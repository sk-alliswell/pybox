//! Where observability rows live.
//!
//! Two tables, both append-only: `requests` (one row per proxied request) and
//! `spans` (gateway spans plus spans received over OTLP). A third, tiny
//! key/value area holds metadata (deliverables, run links, prices, rates).
//!
//! `Memory` keeps the most recent rows in a ring and is always available.
//! The `pybox-gateway-tdb` binary (`tdb-backend/`) persists to Super Shotgun TDB.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Mutex;

use serde_json::Value;

pub type Row = BTreeMap<String, Value>;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Table {
    Requests,
    Spans,
    /// One row per MCP request (`tools/call`, `tools/list`, ...).
    Tools,
}

impl Table {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "requests" => Some(Self::Requests),
            "spans" => Some(Self::Spans),
            "tools" => Some(Self::Tools),
            _ => None,
        }
    }
}

pub fn row_ts(row: &Row) -> i64 {
    row.get("ts").and_then(Value::as_i64).unwrap_or(0)
}

/// Millisecond fields are measured from a nanosecond clock, so they arrive as
/// floats like `412.73391200000003`. Nothing reads below a microsecond, and a
/// float with at most three decimals is stored by TDB as a small integer
/// instead of eight raw bytes, so the writer rounds them once on the way in.
pub fn round_ms(row: &mut Row) {
    for (key, value) in row.iter_mut() {
        if !key.ends_with("_ms") {
            continue;
        }
        if let Some(x) = value.as_f64().filter(|x| value.is_f64() && x.is_finite()) {
            *value = Value::from((x * 1000.0).round() / 1000.0);
        }
    }
}

pub trait Backend: Send + Sync {
    fn name(&self) -> &'static str;
    /// Durably append a batch. Called from the single writer thread.
    fn append(&self, table: Table, rows: Vec<Row>) -> Result<(), String>;
    /// Visit rows with `from_ms <= ts < to_ms`. Order is unspecified.
    fn scan(
        &self,
        table: Table,
        from_ms: i64,
        to_ms: i64,
        visit: &mut dyn FnMut(&Row),
    ) -> Result<(), String>;
    /// Like `scan`, but the visited rows need only carry `ts` and `cols`.
    /// Columnar stores override this to skip decoding everything else.
    fn scan_cols(
        &self,
        table: Table,
        from_ms: i64,
        to_ms: i64,
        cols: &[&str],
        visit: &mut dyn FnMut(&Row),
    ) -> Result<(), String> {
        let _ = cols;
        self.scan(table, from_ms, to_ms, visit)
    }
    fn meta_put(&self, kind: &str, id: &str, value: Value) -> Result<(), String>;
    fn meta_delete(&self, kind: &str, id: &str) -> Result<(), String>;
    fn meta_list(&self, kind: &str) -> Result<Vec<(String, Value)>, String>;
    /// Drop data older than the cutoffs. Returns a short description.
    fn retain(&self, requests_before_ms: i64, spans_before_ms: i64) -> Result<String, String>;
    fn info(&self) -> Value;
}

pub struct Memory {
    cap: usize,
    requests: Mutex<VecDeque<Row>>,
    spans: Mutex<VecDeque<Row>>,
    tools: Mutex<VecDeque<Row>>,
    meta: Mutex<BTreeMap<(String, String), Value>>,
}

impl Memory {
    pub fn new(cap: usize) -> Self {
        Self {
            cap: cap.max(1),
            requests: Mutex::default(),
            spans: Mutex::default(),
            tools: Mutex::default(),
            meta: Mutex::default(),
        }
    }
    fn ring(&self, table: Table) -> &Mutex<VecDeque<Row>> {
        match table {
            Table::Requests => &self.requests,
            Table::Spans => &self.spans,
            Table::Tools => &self.tools,
        }
    }
}

impl Backend for Memory {
    fn name(&self) -> &'static str {
        "memory"
    }
    fn append(&self, table: Table, rows: Vec<Row>) -> Result<(), String> {
        // Spans outnumber requests several to one.
        let cap = match table {
            Table::Requests | Table::Tools => self.cap,
            Table::Spans => self.cap * 4,
        };
        let mut ring = self.ring(table).lock().unwrap();
        for r in rows {
            if ring.len() >= cap {
                ring.pop_front();
            }
            ring.push_back(r);
        }
        Ok(())
    }
    fn scan(
        &self,
        table: Table,
        from_ms: i64,
        to_ms: i64,
        visit: &mut dyn FnMut(&Row),
    ) -> Result<(), String> {
        for r in self.ring(table).lock().unwrap().iter() {
            let ts = row_ts(r);
            if ts >= from_ms && ts < to_ms {
                visit(r);
            }
        }
        Ok(())
    }
    fn meta_put(&self, kind: &str, id: &str, value: Value) -> Result<(), String> {
        self.meta
            .lock()
            .unwrap()
            .insert((kind.into(), id.into()), value);
        Ok(())
    }
    fn meta_delete(&self, kind: &str, id: &str) -> Result<(), String> {
        self.meta.lock().unwrap().remove(&(kind.into(), id.into()));
        Ok(())
    }
    fn meta_list(&self, kind: &str) -> Result<Vec<(String, Value)>, String> {
        Ok(self
            .meta
            .lock()
            .unwrap()
            .iter()
            .filter(|((k, _), _)| k == kind)
            .map(|((_, id), v)| (id.clone(), v.clone()))
            .collect())
    }
    fn retain(&self, requests_before_ms: i64, spans_before_ms: i64) -> Result<String, String> {
        let mut n = 0;
        for (t, cut) in [
            (Table::Requests, requests_before_ms),
            (Table::Tools, requests_before_ms),
            (Table::Spans, spans_before_ms),
        ] {
            let mut ring = self.ring(t).lock().unwrap();
            let before = ring.len();
            ring.retain(|r| row_ts(r) >= cut);
            n += before - ring.len();
        }
        Ok(format!("dropped {n} rows"))
    }
    fn info(&self) -> Value {
        serde_json::json!({
            "backend": "memory",
            "requests": self.requests.lock().unwrap().len(),
            "spans": self.spans.lock().unwrap().len(),
            "tools": self.tools.lock().unwrap().len(),
            "capacity": self.cap,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn round_ms_keeps_three_decimals_and_leaves_other_fields_alone() {
        let mut row: Row = [
            ("ttft_ms", json!(412.733_912_000_000_03)),
            ("total_ms", json!(0.000_4)),
            ("queue_ms", json!(17)),
            ("cost_usd", json!(0.001_234_567_8)),
            ("note_ms", json!("1.23456")),
            ("gone_ms", Value::Null),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();
        round_ms(&mut row);
        assert_eq!(row["ttft_ms"], json!(412.734));
        assert_eq!(row["total_ms"], json!(0.0));
        assert_eq!(row["queue_ms"], json!(17));
        assert_eq!(row["cost_usd"], json!(0.001_234_567_8));
        assert_eq!(row["note_ms"], json!("1.23456"));
        assert_eq!(row["gone_ms"], Value::Null);
    }
}
