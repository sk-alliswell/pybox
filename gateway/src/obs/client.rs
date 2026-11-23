//! What the caller measured, next to what the gateway measured.
//!
//! The request log is the gateway's view: the first token is timed when it
//! leaves the gateway. The way back to the caller (network, proxies, the
//! caller's own HTTP stack) is invisible from here. A caller that times its
//! requests can send the numbers after each answer:
//!
//! ```text
//! POST /gateway/obs/client      (the agent's own token)
//! {"trace_id": "<x-gateway-trace-id of the answer>",
//!  "ttft_ms": 412.0, "total_ms": 1830.5, "connect_ms": 3.1}
//! ```
//!
//! or several at once as `{"items": [...]}`. Each report becomes a
//! `client.request` span in the request's own trace, carrying both views
//! and their difference (`net_ttft_ms`, `net_total_ms`). The way *to* the
//! gateway is measured by `x-gateway-client-sent-at` on the request itself.
//! `GET /gateway/obs/client?from=&to=` sums the reports up per agent and
//! per model.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};

use super::store::Table;
use super::trace::{new_span_id, span_row};
use crate::{error, App};

/// Requests remembered for matching a later report.
const KEPT: usize = 20_000;

/// The gateway's own numbers for a finished request.
#[derive(Clone, Debug, Default)]
pub struct Seen {
    pub ts_ms: i64,
    pub span_id: String,
    pub agent: String,
    pub model: String,
    pub upstream: String,
    pub ttft_ms: Option<f64>,
    pub total_ms: f64,
}

/// The latest finished requests by trace id.
#[derive(Default)]
pub struct Recent {
    inner: Mutex<(HashMap<String, Seen>, VecDeque<String>)>,
}

impl Recent {
    pub fn note(&self, trace_id: &str, seen: Seen) {
        let mut g = self.inner.lock().unwrap();
        let (map, order) = &mut *g;
        if map.insert(trace_id.to_string(), seen).is_none() {
            order.push_back(trace_id.to_string());
            if order.len() > KEPT {
                if let Some(old) = order.pop_front() {
                    map.remove(&old);
                }
            }
        }
    }
    pub fn get(&self, trace_id: &str) -> Option<Seen> {
        self.inner.lock().unwrap().0.get(trace_id).cloned()
    }
}

#[derive(Deserialize, Clone)]
pub struct Report {
    trace_id: String,
    #[serde(default)]
    ttft_ms: Option<f64>,
    #[serde(default)]
    total_ms: Option<f64>,
    #[serde(default)]
    connect_ms: Option<f64>,
    /// What went wrong on the caller's side, if anything (a timeout, a
    /// dropped connection).
    #[serde(default)]
    error: Option<String>,
}

#[derive(Deserialize)]
#[serde(untagged)]
pub enum Body {
    Many { items: Vec<Report> },
    One(Report),
}

fn sane(v: Option<f64>) -> Option<f64> {
    v.filter(|x| x.is_finite() && (0.0..86_400_000.0).contains(x))
}

/// `POST /gateway/obs/client`.
pub async fn report(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Json(body): Json<Body>,
) -> Response {
    let admin = app.cfg.admin_token.is_some() && app.admin_ok(&headers);
    let agent = app.agent_for(&headers);
    if !admin && agent.is_none() {
        return error(
            StatusCode::UNAUTHORIZED,
            "authentication_error",
            "an agent token or the admin token is required",
        );
    }
    let Some(obs) = app.obs.clone() else {
        return error(
            StatusCode::NOT_FOUND,
            "not_found_error",
            "observability is disabled ([obs] enabled = false)",
        );
    };
    let items = match body {
        Body::Many { items } => items,
        Body::One(r) => vec![r],
    };
    if items.len() > 1000 {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
            "at most 1000 reports at once",
        );
    }
    let who = agent.as_ref().map(|a| a.id.as_str()).unwrap_or("");
    let (mut stored, mut unmatched, mut refused) = (0, 0, 0);
    let mut spans = Vec::new();
    for r in items {
        let (ttft, total) = (sane(r.ttft_ms), sane(r.total_ms));
        if r.trace_id.is_empty() || r.trace_id.len() > 64 || (ttft.is_none() && total.is_none()) {
            refused += 1;
            continue;
        }
        let seen = obs.recent.get(&r.trace_id);
        // An agent reports on its own requests only.
        if let (false, Some(s)) = (admin, &seen) {
            if s.agent != who {
                refused += 1;
                continue;
            }
        }
        let mut attrs = serde_json::Map::new();
        let mut put = |k: &str, v: Option<f64>| {
            if let Some(v) = v {
                attrs.insert(k.into(), json!((v * 1000.0).round() / 1000.0));
            }
        };
        put("client_ttft_ms", ttft);
        put("client_total_ms", total);
        put("client_connect_ms", sane(r.connect_ms));
        if let Some(s) = &seen {
            put("gateway_ttft_ms", s.ttft_ms);
            put("gateway_total_ms", Some(s.total_ms));
            put("net_ttft_ms", ttft.zip(s.ttft_ms).map(|(c, g)| c - g));
            put("net_total_ms", total.map(|c| c - s.total_ms));
        }
        let agent_id = seen.as_ref().map_or(who, |s| s.agent.as_str());
        attrs.insert("agent_id".into(), json!(agent_id));
        if let Some(s) = &seen {
            attrs.insert("model".into(), json!(s.model));
            attrs.insert("upstream".into(), json!(s.upstream));
        }
        attrs.insert("matched".into(), json!(seen.is_some()));
        if let Some(e) = r.error.as_deref().filter(|e| !e.is_empty()) {
            attrs.insert(
                "error".into(),
                json!(e.chars().take(200).collect::<String>()),
            );
        }
        if seen.is_none() {
            unmatched += 1;
        }
        let ts = seen.as_ref().map_or_else(super::now_ms, |s| s.ts_ms);
        let parent = seen.as_ref().map_or("", |s| s.span_id.as_str());
        spans.push(span_row(
            ts,
            0.0,
            total.or(ttft).unwrap_or(0.0),
            &r.trace_id,
            &new_span_id(),
            parent,
            "client.request",
            "client",
            r.error.as_deref().is_none_or(str::is_empty),
            Value::Object(attrs),
        ));
        stored += 1;
    }
    obs.emit_own_spans(spans);
    Json(json!({ "stored": stored, "unmatched": unmatched, "refused": refused })).into_response()
}

#[derive(Deserialize)]
pub struct Window {
    from: i64,
    to: i64,
}

#[derive(Default)]
struct Group {
    n: u64,
    errors: u64,
    cols: BTreeMap<&'static str, Vec<f64>>,
}

const COLS: &[&str] = &[
    "client_ttft_ms",
    "gateway_ttft_ms",
    "net_ttft_ms",
    "client_total_ms",
    "gateway_total_ms",
    "net_total_ms",
    "client_connect_ms",
];

fn quantile(v: &mut [f64], q: f64) -> Option<f64> {
    if v.is_empty() {
        return None;
    }
    v.sort_by(|a, b| a.total_cmp(b));
    let i = ((v.len() - 1) as f64 * q).round() as usize;
    Some((v[i] * 100.0).round() / 100.0)
}

impl Group {
    fn add(&mut self, attrs: &Value, ok: bool) {
        self.n += 1;
        self.errors += u64::from(!ok);
        for c in COLS {
            if let Some(x) = attrs[*c].as_f64() {
                self.cols.entry(c).or_default().push(x);
            }
        }
    }
    fn json(mut self, key: &str, name: &str) -> Value {
        let mut out = serde_json::Map::new();
        out.insert(key.into(), json!(name));
        out.insert("reports".into(), json!(self.n));
        out.insert("errors".into(), json!(self.errors));
        for c in COLS {
            let v = self.cols.entry(c).or_default();
            let stem = c.trim_end_matches("_ms");
            out.insert(format!("{stem}_p50_ms"), json!(quantile(v, 0.5)));
            out.insert(format!("{stem}_p95_ms"), json!(quantile(v, 0.95)));
        }
        Value::Object(out)
    }
}

/// Sum up the `client.request` spans between `from` and `to`.
pub fn summary(backend: &dyn super::store::Backend, from: i64, to: i64) -> Result<Value, String> {
    let mut all = Group::default();
    let mut agents: BTreeMap<String, Group> = BTreeMap::new();
    let mut models: BTreeMap<String, Group> = BTreeMap::new();
    backend.scan(Table::Spans, from, to, &mut |row| {
        if row.get("name").and_then(Value::as_str) != Some("client.request") {
            return;
        }
        let Some(attrs) = row.get("attrs") else {
            return;
        };
        let ok = row.get("status").and_then(Value::as_str) != Some("error");
        all.add(attrs, ok);
        let text = |k: &str| attrs[k].as_str().unwrap_or("").to_string();
        agents.entry(text("agent_id")).or_default().add(attrs, ok);
        if attrs["matched"].as_bool() == Some(true) {
            models.entry(text("model")).or_default().add(attrs, ok);
        }
    })?;
    Ok(json!({
        "from": from,
        "to": to,
        "all": all.json("scope", "all"),
        "agents": agents.into_iter().map(|(k, g)| g.json("agent_id", &k)).collect::<Vec<_>>(),
        "models": models.into_iter().map(|(k, g)| g.json("model", &k)).collect::<Vec<_>>(),
    }))
}

/// `GET /gateway/obs/client?from=&to=` (admin).
pub async fn get_summary(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Query(w): Query<Window>,
) -> Response {
    if !app.admin_ok(&headers) {
        return error(
            StatusCode::UNAUTHORIZED,
            "authentication_error",
            "admin token required",
        );
    }
    let Some(obs) = app.obs.clone() else {
        return error(
            StatusCode::NOT_FOUND,
            "not_found_error",
            "observability is disabled ([obs] enabled = false)",
        );
    };
    match tokio::task::spawn_blocking(move || summary(obs.backend.as_ref(), w.from, w.to)).await {
        Ok(Ok(v)) => Json(v).into_response(),
        Ok(Err(e)) => error(StatusCode::BAD_REQUEST, "invalid_request_error", e),
        Err(e) => error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "api_error",
            e.to_string(),
        ),
    }
}
