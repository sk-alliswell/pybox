//! Super Shotgun TDB backend.
//!
//! TDB is an append-only columnar time-series store: appends go to a WAL and
//! an in-memory buffer, full buffers become immutable columnar segments, and
//! retention deletes whole segment files. That is exactly the shape of the
//! gateway's request rows and spans, so the mapping is direct:
//!
//! * `requests` and `spans` are two TDB series under `<dir>/`.
//! * A row's `ts` is the series timestamp; the rest is the row object.
//! * Aggregations read only the columns they need (`scan_columns`).
//! * Metadata (deliverables, links, prices, rates) is a few thousand small
//!   records that are updated in place, which TDB does not do. It lives in
//!   `<dir>/meta.json`, rewritten atomically on each change.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use serde_json::{json, Value};
use tdb::{Cell, Options, SyncPolicy, Tdb};

use pybox_gateway::obs::store::{row_ts, Backend, Row, Table};

type Meta = BTreeMap<String, BTreeMap<String, Value>>;

pub struct Store {
    dir: PathBuf,
    db: Tdb,
    meta: Mutex<Meta>,
}

fn series(t: Table) -> &'static str {
    match t {
        Table::Requests => "requests",
        Table::Spans => "spans",
        Table::Tools => "tools",
    }
}

fn cell_value(c: &Cell) -> Option<Value> {
    Some(match c {
        Cell::Missing => return None,
        Cell::Null => Value::Null,
        Cell::Bool(b) => json!(b),
        Cell::I64(v) => json!(v),
        Cell::U64(v) => json!(v),
        Cell::F64(v) => json!(v),
        Cell::Str(s) => json!(s),
        Cell::Json(v) => v.clone(),
    })
}

impl Store {
    pub fn open(dir: &Path) -> Result<Self, String> {
        std::fs::create_dir_all(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
        let options = Options {
            // The gateway writes about two batches a second from one thread,
            // so an fsync per batch is cheap and nothing acknowledged is lost.
            sync: SyncPolicy::Always,
            ..Options::default()
        };
        let db = Tdb::open(dir.join("tdb"), options)?;
        let meta = match std::fs::read(dir.join("meta.json")) {
            Ok(bytes) => serde_json::from_slice(&bytes).map_err(|e| format!("meta.json: {e}"))?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Meta::new(),
            Err(e) => return Err(format!("meta.json: {e}")),
        };
        Ok(Self {
            dir: dir.to_path_buf(),
            db,
            meta: Mutex::new(meta),
        })
    }

    fn save_meta(&self, meta: &Meta) -> Result<(), String> {
        let tmp = self.dir.join("meta.json.tmp");
        let write = || -> std::io::Result<()> {
            let f = std::fs::File::create(&tmp)?;
            serde_json::to_writer(&f, meta)?;
            f.sync_all()?;
            std::fs::rename(&tmp, self.dir.join("meta.json"))
        };
        write().map_err(|e| format!("meta.json: {e}"))
    }
}

impl Backend for Store {
    fn name(&self) -> &'static str {
        "tdb"
    }

    fn append(&self, table: Table, rows: Vec<Row>) -> Result<(), String> {
        let rows = rows
            .into_iter()
            .map(|mut row| {
                let ts = row_ts(&row);
                row.remove("ts");
                (ts, row.into_iter().collect())
            })
            .collect();
        self.db.append(series(table), rows).map(|_| ())
    }

    fn scan(
        &self,
        table: Table,
        from_ms: i64,
        to_ms: i64,
        visit: &mut dyn FnMut(&Row),
    ) -> Result<(), String> {
        self.db.scan(series(table), from_ms, to_ms, |ts, obj| {
            let mut row: Row = obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
            row.insert("ts".into(), json!(ts));
            visit(&row);
        })
    }

    fn scan_cols(
        &self,
        table: Table,
        from_ms: i64,
        to_ms: i64,
        cols: &[&str],
        visit: &mut dyn FnMut(&Row),
    ) -> Result<(), String> {
        let wanted: Vec<&str> = cols.iter().copied().filter(|c| *c != "ts").collect();
        self.db
            .scan_columns(series(table), from_ms, to_ms, &wanted, |ts, cells| {
                let mut row = Row::new();
                row.insert("ts".into(), json!(ts));
                for (name, cell) in wanted.iter().zip(cells) {
                    if let Some(v) = cell_value(cell) {
                        row.insert((*name).to_string(), v);
                    }
                }
                visit(&row);
            })
    }

    fn meta_put(&self, kind: &str, id: &str, value: Value) -> Result<(), String> {
        let mut meta = self.meta.lock().unwrap();
        let mut next = meta.clone();
        next.entry(kind.to_string())
            .or_default()
            .insert(id.to_string(), value);
        self.save_meta(&next)?;
        *meta = next;
        Ok(())
    }

    fn meta_delete(&self, kind: &str, id: &str) -> Result<(), String> {
        let mut meta = self.meta.lock().unwrap();
        let mut next = meta.clone();
        if next.get_mut(kind).and_then(|m| m.remove(id)).is_none() {
            return Ok(());
        }
        self.save_meta(&next)?;
        *meta = next;
        Ok(())
    }

    fn meta_list(&self, kind: &str) -> Result<Vec<(String, Value)>, String> {
        let meta = self.meta.lock().unwrap();
        Ok(meta
            .get(kind)
            .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default())
    }

    fn retain(&self, requests_before_ms: i64, spans_before_ms: i64) -> Result<String, String> {
        let r = self.db.drop_before("requests", requests_before_ms)?;
        let s = self.db.drop_before("spans", spans_before_ms)?;
        let t = self.db.drop_before("tools", requests_before_ms)?;
        Ok(format!(
            "dropped {} request segment(s), {} span segment(s), {} tool segment(s), {} bytes",
            r.segments,
            s.segments,
            t.segments,
            r.bytes + s.bytes + t.bytes
        ))
    }

    fn info(&self) -> Value {
        let stat = |name: &str| match self.db.stats(name) {
            Ok(s) => json!({
                "rows": s.rows,
                "buffered_rows": s.buffered_rows,
                "segments": s.segments,
                "segment_bytes": s.segment_bytes,
                "wal_bytes": s.wal_bytes,
                "min_ts": s.min_ts,
                "max_ts": s.max_ts,
                "last_error": s.last_error,
            }),
            Err(e) => json!({ "error": e }),
        };
        json!({
            "backend": "tdb",
            "dir": self.dir.display().to_string(),
            "requests": stat("requests"),
            "spans": stat("spans"),
            "tools": stat("tools"),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(ts: i64, k: &str, n: f64) -> Row {
        [
            ("ts".to_string(), json!(ts)),
            ("k".to_string(), json!(k)),
            ("n".to_string(), json!(n)),
        ]
        .into_iter()
        .collect()
    }

    fn collect(s: &Store, t: Table, from: i64, to: i64) -> Vec<Row> {
        let mut out = Vec::new();
        s.scan(t, from, to, &mut |r| out.push(r.clone())).unwrap();
        out.sort_by_key(row_ts);
        out
    }

    #[test]
    fn rows_and_meta_survive_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let s = Store::open(dir.path()).unwrap();
            let rows = (0..40_000).map(|i| row(1_000 + i, "a", i as f64)).collect();
            s.append(Table::Requests, rows).unwrap();
            s.append(Table::Spans, vec![row(5, "span", 1.0)]).unwrap();
            s.meta_put("price", "m", json!({"input": 1.0})).unwrap();
            s.meta_put("price", "gone", json!(1)).unwrap();
            s.meta_delete("price", "gone").unwrap();
        }
        let s = Store::open(dir.path()).unwrap();
        let got = collect(&s, Table::Requests, 1_010, 1_013);
        assert_eq!(
            got,
            vec![
                row(1_010, "a", 10.0),
                row(1_011, "a", 11.0),
                row(1_012, "a", 12.0)
            ]
        );
        assert_eq!(collect(&s, Table::Requests, 0, i64::MAX).len(), 40_000);
        assert_eq!(collect(&s, Table::Spans, 0, 10), vec![row(5, "span", 1.0)]);
        assert_eq!(
            s.meta_list("price").unwrap(),
            vec![("m".to_string(), json!({"input": 1.0}))]
        );
    }

    #[test]
    fn projection_returns_only_the_asked_columns() {
        let dir = tempfile::tempdir().unwrap();
        let s = Store::open(dir.path()).unwrap();
        // Enough rows that some are in a columnar segment and some buffered.
        let rows = (0..20_000)
            .map(|i| row(i, if i % 2 == 0 { "even" } else { "odd" }, 0.5))
            .collect();
        s.append(Table::Requests, rows).unwrap();
        let (mut count, mut sum) = (0, 0.0);
        s.scan_cols(Table::Requests, 0, 20_000, &["n", "missing"], &mut |r| {
            assert!(r.contains_key("ts") && !r.contains_key("k") && !r.contains_key("missing"));
            count += 1;
            sum += r["n"].as_f64().unwrap();
        })
        .unwrap();
        assert_eq!((count, sum), (20_000, 10_000.0));
    }

    #[test]
    fn retention_hides_old_rows_per_table() {
        let dir = tempfile::tempdir().unwrap();
        let s = Store::open(dir.path()).unwrap();
        for t in [Table::Requests, Table::Spans] {
            s.append(t, (0..100).map(|i| row(i, "x", 0.0)).collect())
                .unwrap();
        }
        s.retain(10, 60).unwrap();
        assert_eq!(collect(&s, Table::Requests, 0, 1_000).len(), 90);
        assert_eq!(collect(&s, Table::Spans, 0, 1_000).len(), 40);
    }

    #[test]
    fn tools_table_reads_empty_before_first_write_and_follows_request_retention() {
        let dir = tempfile::tempdir().unwrap();
        let s = Store::open(dir.path()).unwrap();
        // The gateway scans this table on startup and for every trace view,
        // long before any MCP call has been recorded.
        assert!(collect(&s, Table::Tools, 0, i64::MAX).is_empty());
        s.append(Table::Tools, (0..100).map(|i| row(i, "t", 0.0)).collect())
            .unwrap();
        s.retain(10, 60).unwrap();
        assert_eq!(collect(&s, Table::Tools, 0, 1_000).len(), 90);
        assert!(s.info()["tools"]["rows"].as_u64().is_some());
    }
}

#[cfg(test)]
mod rounding {
    use super::*;
    use pybox_gateway::obs::store::round_ms;

    fn dir_bytes(p: &Path) -> u64 {
        std::fs::read_dir(p)
            .unwrap()
            .map(|e| e.unwrap())
            .map(|e| {
                if e.path().is_dir() {
                    dir_bytes(&e.path())
                } else {
                    e.metadata().unwrap().len()
                }
            })
            .sum()
    }

    /// Latencies as the gateway measures them: nanosecond clock, as float ms.
    fn rows(round: bool) -> Vec<Row> {
        let mut x = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = move || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        (0..20_000i64)
            .map(|i| {
                let mut row = Row::new();
                row.insert("ts".into(), json!(1_700_000_000_000 + i * 50));
                for key in ["ttft_ms", "total_ms", "queue_ms", "gen_ms"] {
                    let ns = next() % 30_000_000_000;
                    let d = std::time::Duration::from_nanos(ns);
                    row.insert(key.into(), json!(d.as_secs_f64() * 1000.0));
                }
                if round {
                    round_ms(&mut row);
                }
                row
            })
            .collect()
    }

    #[test]
    fn rounded_latencies_take_less_space() {
        let size = |round: bool| {
            let d = tempfile::tempdir().unwrap();
            let s = Store::open(d.path()).unwrap();
            s.append(Table::Requests, rows(round)).unwrap();
            s.db.flush("requests").unwrap();
            dir_bytes(&d.path().join("tdb"))
        };
        let (raw, rounded) = (size(false), size(true));
        println!("latency columns, 20,000 rows: raw {raw} B, rounded {rounded} B");
        assert!(rounded * 10 < raw * 7, "raw {raw}, rounded {rounded}");
    }
}
