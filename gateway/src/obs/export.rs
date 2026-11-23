//! Stored span rows as OTLP, for any OpenTelemetry backend.
//!
//! Two ways out: a push exporter that batches the gateway's own spans to an
//! OTLP/HTTP endpoint (`[obs.otlp_export]`), and OTLP/JSON for one trace on
//! `GET /gateway/obs/trace/<id>?format=otlp`.
//!
//! The root span of a model call is stored as `gateway.request`; on the way
//! out it takes the GenAI convention's shape: named
//! `{gen_ai.operation.name} {gen_ai.request.model}` with kind CLIENT.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::time::Duration;

use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::mpsc;

use super::otlp::enc;
use super::store::Row;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OtlpExportConfig {
    /// Full OTLP/HTTP traces URL, e.g. `http://collector:4318/v1/traces`.
    pub endpoint: String,
    /// Extra request headers (authentication for hosted backends).
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    #[serde(default = "batch")]
    pub batch_spans: usize,
    #[serde(default = "interval")]
    pub interval_ms: u64,
}

fn batch() -> usize {
    512
}
fn interval() -> u64 {
    2_000
}

fn unhex(s: &str, n: usize) -> Vec<u8> {
    let b = s.as_bytes();
    if b.len() != n * 2 {
        return Vec::new();
    }
    (0..n)
        .filter_map(|i| {
            u8::from_str_radix(std::str::from_utf8(&b[i * 2..i * 2 + 2]).ok()?, 16).ok()
        })
        .collect()
}

fn text<'a>(row: &'a Row, k: &str) -> &'a str {
    row.get(k).and_then(Value::as_str).unwrap_or("")
}

/// Name and kind (OTLP enum) a stored span is exported with.
fn name_and_kind(row: &Row) -> (String, u64) {
    let name = text(row, "name");
    let attr = |k: &str| {
        row.get("attrs")
            .and_then(|a| a.get(k))
            .and_then(Value::as_str)
            .unwrap_or("")
    };
    if name == "gateway.request" && !attr("gen_ai.operation.name").is_empty() {
        let model = attr("gen_ai.request.model");
        let name = if model.is_empty() {
            attr("gen_ai.operation.name").to_string()
        } else {
            format!("{} {model}", attr("gen_ai.operation.name"))
        };
        return (name, 3);
    }
    let kind = match text(row, "kind") {
        "internal" => 1,
        "server" => 2,
        "client" => 3,
        "producer" => 4,
        "consumer" => 5,
        _ => 0,
    };
    (name.to_string(), kind)
}

fn times_ns(row: &Row) -> (u64, u64) {
    let start_us = row
        .get("start_us")
        .and_then(Value::as_i64)
        .unwrap_or(0)
        .max(0) as u64;
    let dur_ms = row
        .get("duration_ms")
        .and_then(Value::as_f64)
        .unwrap_or(0.0)
        .max(0.0);
    let start = start_us * 1_000;
    (start, start + (dur_ms * 1e6) as u64)
}

fn any_value_pb(v: &Value) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    match v {
        Value::Null => return None,
        Value::String(s) => enc::bytes(&mut out, 1, s.as_bytes()),
        Value::Bool(b) => enc::uint(&mut out, 2, u64::from(*b)),
        Value::Number(n) => match n.as_i64() {
            Some(i) => enc::uint(&mut out, 3, i as u64),
            None => enc::double(&mut out, 4, n.as_f64().unwrap_or(0.0)),
        },
        Value::Array(items) => {
            let mut arr = Vec::new();
            for item in items {
                if let Some(x) = any_value_pb(item) {
                    enc::bytes(&mut arr, 1, &x);
                }
            }
            enc::bytes(&mut out, 5, &arr);
        }
        // Nested maps are rare in span attributes; keep them readable.
        Value::Object(_) => enc::bytes(&mut out, 1, v.to_string().as_bytes()),
    }
    Some(out)
}

fn span_pb(row: &Row) -> Option<Vec<u8>> {
    let (trace, id) = (
        unhex(text(row, "trace_id"), 16),
        unhex(text(row, "span_id"), 8),
    );
    if trace.len() != 16 || id.len() != 8 {
        return None;
    }
    let mut s = Vec::with_capacity(256);
    enc::bytes(&mut s, 1, &trace);
    enc::bytes(&mut s, 2, &id);
    let parent = unhex(text(row, "parent_span_id"), 8);
    if parent.len() == 8 {
        enc::bytes(&mut s, 4, &parent);
    }
    let (name, kind) = name_and_kind(row);
    enc::bytes(&mut s, 5, name.as_bytes());
    enc::uint(&mut s, 6, kind);
    let (start, end) = times_ns(row);
    enc::fixed64(&mut s, 7, start);
    enc::fixed64(&mut s, 8, end);
    if let Some(Value::Object(attrs)) = row.get("attrs") {
        for (k, v) in attrs {
            if let Some(any) = any_value_pb(v) {
                enc::bytes(&mut s, 9, &enc::kv(k, &any));
            }
        }
    }
    if text(row, "status") == "error" {
        let mut status = Vec::new();
        enc::uint(&mut status, 3, 2);
        enc::bytes(&mut s, 15, &status);
    }
    Some(s)
}

/// Group rows by the service (and instance) that produced them.
fn by_resource(spans: &[Row]) -> BTreeMap<(&str, &str), Vec<&Row>> {
    let mut groups: BTreeMap<(&str, &str), Vec<&Row>> = BTreeMap::new();
    for r in spans {
        groups
            .entry((text(r, "service"), text(r, "instance")))
            .or_default()
            .push(r);
    }
    groups
}

/// An `ExportTraceServiceRequest` in protobuf.
pub fn encode_protobuf(spans: &[Row]) -> Vec<u8> {
    let mut out = Vec::new();
    for ((service, instance), rows) in by_resource(spans) {
        let mut resource = Vec::new();
        enc::bytes(&mut resource, 1, &enc::kv_str("service.name", service));
        if !instance.is_empty() {
            enc::bytes(
                &mut resource,
                1,
                &enc::kv_str("service.instance.id", instance),
            );
        }
        let mut scope = Vec::new();
        enc::bytes(&mut scope, 1, b"pybox-gateway");
        enc::bytes(&mut scope, 2, env!("CARGO_PKG_VERSION").as_bytes());
        let mut scope_spans = Vec::new();
        enc::bytes(&mut scope_spans, 1, &scope);
        for row in rows {
            if let Some(s) = span_pb(row) {
                enc::bytes(&mut scope_spans, 2, &s);
            }
        }
        let mut rs = Vec::new();
        enc::bytes(&mut rs, 1, &resource);
        enc::bytes(&mut rs, 2, &scope_spans);
        enc::bytes(&mut out, 1, &rs);
    }
    out
}

fn any_value_json(v: &Value) -> Option<Value> {
    Some(match v {
        Value::Null => return None,
        Value::String(s) => json!({ "stringValue": s }),
        Value::Bool(b) => json!({ "boolValue": b }),
        Value::Number(n) => match n.as_i64() {
            // OTLP/JSON carries 64-bit integers as strings.
            Some(i) => json!({ "intValue": i.to_string() }),
            None => json!({ "doubleValue": n }),
        },
        Value::Array(items) => {
            json!({ "arrayValue": { "values": items.iter().filter_map(any_value_json).collect::<Vec<_>>() } })
        }
        Value::Object(_) => json!({ "stringValue": v.to_string() }),
    })
}

/// The same request in OTLP/JSON.
pub fn encode_json(spans: &[Row]) -> Value {
    let mut resource_spans = Vec::new();
    for ((service, instance), rows) in by_resource(spans) {
        let mut res_attrs =
            vec![json!({ "key": "service.name", "value": { "stringValue": service } })];
        if !instance.is_empty() {
            res_attrs.push(
                json!({ "key": "service.instance.id", "value": { "stringValue": instance } }),
            );
        }
        let spans: Vec<Value> = rows
            .into_iter()
            .filter(|r| text(r, "trace_id").len() == 32 && text(r, "span_id").len() == 16)
            .map(|row| {
                let (name, kind) = name_and_kind(row);
                let (start, end) = times_ns(row);
                let attributes: Vec<Value> = row
                    .get("attrs")
                    .and_then(Value::as_object)
                    .into_iter()
                    .flatten()
                    .filter_map(|(k, v)| Some(json!({ "key": k, "value": any_value_json(v)? })))
                    .collect();
                let mut span = json!({
                    "traceId": text(row, "trace_id"),
                    "spanId": text(row, "span_id"),
                    "name": name,
                    "kind": kind,
                    "startTimeUnixNano": start.to_string(),
                    "endTimeUnixNano": end.to_string(),
                    "attributes": attributes,
                });
                if text(row, "parent_span_id").len() == 16 {
                    span["parentSpanId"] = json!(text(row, "parent_span_id"));
                }
                if text(row, "status") == "error" {
                    span["status"] = json!({ "code": 2 });
                }
                span
            })
            .collect();
        resource_spans.push(json!({
            "resource": { "attributes": res_attrs },
            "scopeSpans": [{
                "scope": { "name": "pybox-gateway", "version": env!("CARGO_PKG_VERSION") },
                "spans": spans,
            }],
        }));
    }
    json!({ "resourceSpans": resource_spans })
}

#[derive(Default)]
pub struct ExportStats {
    pub sent: AtomicU64,
    pub failed: AtomicU64,
    pub dropped: AtomicU64,
}

/// Background OTLP/HTTP push. Never blocks the request path: when the
/// collector is slow or down, spans are dropped and counted.
pub struct Exporter {
    tx: mpsc::Sender<Vec<Row>>,
    pub stats: Arc<ExportStats>,
    endpoint: String,
}

impl Exporter {
    /// Needs a running Tokio runtime.
    pub fn spawn(cfg: &OtlpExportConfig) -> anyhow::Result<Self> {
        let mut headers = reqwest::header::HeaderMap::new();
        for (k, v) in &cfg.headers {
            headers.insert(
                reqwest::header::HeaderName::from_bytes(k.as_bytes())?,
                reqwest::header::HeaderValue::from_str(v)?,
            );
        }
        headers.insert(
            reqwest::header::CONTENT_TYPE,
            reqwest::header::HeaderValue::from_static("application/x-protobuf"),
        );
        let client = reqwest::Client::builder()
            .default_headers(headers)
            .timeout(Duration::from_secs(10))
            .build()?;
        let (tx, mut rx) = mpsc::channel::<Vec<Row>>(1024);
        let stats = Arc::new(ExportStats::default());
        let (st, endpoint) = (stats.clone(), cfg.endpoint.clone());
        let (max, every) = (
            cfg.batch_spans.max(1),
            Duration::from_millis(cfg.interval_ms.max(50)),
        );
        tokio::spawn(async move {
            let mut pending: Vec<Row> = Vec::new();
            let mut tick = tokio::time::interval(every);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                let (closed, ticked) = tokio::select! {
                    got = rx.recv() => match got {
                        Some(mut rows) => { pending.append(&mut rows); (false, false) }
                        None => (true, false),
                    },
                    _ = tick.tick() => (false, true),
                };
                // Send on a full batch, on the timer, and when shutting down.
                if closed || ticked || pending.len() >= max {
                    while !pending.is_empty() {
                        let take = pending.len().min(max);
                        let batch: Vec<Row> = pending.drain(..take).collect();
                        let n = batch.len() as u64;
                        let body = encode_protobuf(&batch);
                        match client.post(&endpoint).body(body).send().await {
                            Ok(r) if r.status().is_success() => {
                                st.sent.fetch_add(n, Relaxed);
                            }
                            Ok(r) => {
                                st.failed.fetch_add(n, Relaxed);
                                tracing::warn!(status = %r.status(), spans = n, "OTLP export rejected");
                            }
                            Err(e) => {
                                st.failed.fetch_add(n, Relaxed);
                                tracing::warn!(error = %e, spans = n, "OTLP export failed");
                            }
                        }
                    }
                }
                if closed {
                    return;
                }
            }
        });
        Ok(Self {
            tx,
            stats,
            endpoint: cfg.endpoint.clone(),
        })
    }

    pub fn offer(&self, spans: Vec<Row>) {
        let n = spans.len() as u64;
        if self.tx.try_send(spans).is_err() {
            self.stats.dropped.fetch_add(n, Relaxed);
        }
    }

    pub fn info(&self) -> Value {
        json!({
            "endpoint": self.endpoint,
            "sent": self.stats.sent.load(Relaxed),
            "failed": self.stats.failed.load(Relaxed),
            "dropped": self.stats.dropped.load(Relaxed),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(name: &str, kind: &str, attrs: Value) -> Row {
        [
            ("ts", json!(1_794_000_000_000i64)),
            ("start_us", json!(1_794_000_000_000_000i64)),
            ("duration_ms", json!(250.0)),
            ("trace_id", json!("ab".repeat(16))),
            ("span_id", json!("01".repeat(8))),
            ("parent_span_id", json!("")),
            ("service", json!("pybox-gateway")),
            ("instance", json!("")),
            ("name", json!(name)),
            ("kind", json!(kind)),
            ("status", json!("ok")),
            ("attrs", attrs),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect()
    }

    #[test]
    fn protobuf_round_trips_through_the_receiver() {
        let spans = vec![row(
            "gateway.request",
            "server",
            json!({
                "gen_ai.operation.name": "chat",
                "gen_ai.request.model": "claude-sonnet-5",
                "gen_ai.usage.input_tokens": 1200,
                "gen_ai.response.time_to_first_chunk": 0.25,
                "gen_ai.request.stream": true,
                "gen_ai.response.finish_reasons": ["end_turn"],
            }),
        )];
        let back = super::super::otlp::decode(&encode_protobuf(&spans)).unwrap();
        assert_eq!(back.len(), 1);
        let s = &back[0];
        // The root span leaves in the GenAI convention's shape.
        assert_eq!(s["name"], "chat claude-sonnet-5");
        assert_eq!(s["kind"], "client");
        assert_eq!(s["service"], "pybox-gateway");
        assert_eq!(s["trace_id"], "ab".repeat(16));
        assert_eq!(s["start_us"], 1_794_000_000_000_000i64);
        assert_eq!(s["duration_ms"], 250.0);
        assert_eq!(s["attrs"]["gen_ai.usage.input_tokens"], 1200);
        assert_eq!(s["attrs"]["gen_ai.response.time_to_first_chunk"], 0.25);
        assert_eq!(s["attrs"]["gen_ai.request.stream"], true);
        assert_eq!(
            s["attrs"]["gen_ai.response.finish_reasons"],
            json!(["end_turn"])
        );
    }

    #[test]
    fn json_uses_otlp_field_names() {
        let mut r = row("gateway.attempt", "client", json!({ "attempt": 1 }));
        r.insert("status".into(), json!("error"));
        r.insert("parent_span_id".into(), json!("02".repeat(8)));
        let v = encode_json(&[r]);
        let span = &v["resourceSpans"][0]["scopeSpans"][0]["spans"][0];
        assert_eq!(span["name"], "gateway.attempt");
        assert_eq!(span["kind"], 3);
        assert_eq!(span["startTimeUnixNano"], "1794000000000000000");
        assert_eq!(span["endTimeUnixNano"], "1794000000250000000");
        assert_eq!(span["parentSpanId"], "02".repeat(8));
        assert_eq!(span["status"]["code"], 2);
        assert_eq!(
            span["attributes"][0],
            json!({ "key": "attempt", "value": { "intValue": "1" } })
        );
        assert_eq!(
            v["resourceSpans"][0]["resource"]["attributes"][0]["value"]["stringValue"],
            "pybox-gateway"
        );
    }
}
