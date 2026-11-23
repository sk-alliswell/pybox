#![allow(clippy::result_large_err)] // handlers return the HTTP error response as Err
//! Admin-only HTTP surface for the observability store, plus the OTLP
//! receiver and the dashboard page.

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Path, Query as UrlQuery, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};

use super::insight;
use super::query::{self, Query};
use super::roi::{self, Deliverable, Link, Rate};
use super::store::{Row, Table};
use super::{now_ms, Obs, Price};
use crate::{constant_eq, error, App};

const DASHBOARD: &str = include_str!("dashboard.html");
const ROI_PAGE: &str = include_str!("roi.html");

pub fn routes() -> Router<Arc<App>> {
    Router::new()
        .route("/gateway/dashboard", get(|| async { Html(DASHBOARD) }))
        .route("/gateway/obs/info", get(info))
        .route("/gateway/obs/query", post(run_query))
        .route("/gateway/obs/values", get(values))
        .route("/gateway/obs/rows", post(rows))
        .route("/gateway/obs/export", get(export))
        .route(
            "/gateway/obs/client",
            get(super::client::get_summary).post(super::client::report),
        )
        .route("/gateway/obs/trace/{trace_id}", get(trace))
        .route("/gateway/obs/roi", get(roi_report))
        .route("/gateway/roi", get(|| async { Html(ROI_PAGE) }))
        .route("/gateway/obs/insights", get(insights))
        .route("/gateway/obs/meta/{kind}", get(meta_list).post(meta_put))
        .route(
            "/gateway/obs/meta/{kind}/{id}",
            axum::routing::delete(meta_delete),
        )
        .route("/gateway/otlp/v1/traces", post(otlp))
}

fn unauthorized() -> Response {
    error(
        StatusCode::UNAUTHORIZED,
        "authentication_error",
        "admin token required",
    )
}

fn bad(msg: impl Into<String>) -> Response {
    error(StatusCode::BAD_REQUEST, "invalid_request_error", msg)
}

/// Admin check plus "is observability on".
fn guard(app: &Arc<App>, headers: &HeaderMap) -> Result<Arc<Obs>, Response> {
    if !app.admin_ok(headers) {
        return Err(unauthorized());
    }
    app.obs.clone().ok_or_else(|| {
        error(
            StatusCode::NOT_FOUND,
            "not_found_error",
            "observability is disabled ([obs] enabled = false)",
        )
    })
}

/// Scans can take a while on big windows; keep them off the async workers.
async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> Result<T, String> + Send + 'static,
) -> Result<T, Response> {
    match tokio::task::spawn_blocking(f).await {
        Ok(Ok(v)) => Ok(v),
        Ok(Err(e)) => Err(bad(e)),
        Err(e) => Err(error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "api_error",
            e.to_string(),
        )),
    }
}

async fn info(State(app): State<Arc<App>>, headers: HeaderMap) -> Response {
    match guard(&app, &headers) {
        Ok(obs) => Json(obs.info()).into_response(),
        Err(r) => r,
    }
}

async fn run_query(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Json(q): Json<Query>,
) -> Response {
    let obs = match guard(&app, &headers) {
        Ok(o) => o,
        Err(r) => return r,
    };
    match blocking(move || query::run(obs.backend.as_ref(), &q)).await {
        Ok(v) => Json(v).into_response(),
        Err(r) => r,
    }
}

#[derive(Deserialize)]
struct ValuesParams {
    #[serde(default)]
    table: Option<String>,
    col: String,
    from: i64,
    to: i64,
}

async fn values(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    UrlQuery(p): UrlQuery<ValuesParams>,
) -> Response {
    let obs = match guard(&app, &headers) {
        Ok(o) => o,
        Err(r) => return r,
    };
    let Some(table) = Table::parse(p.table.as_deref().unwrap_or("requests")) else {
        return bad("unknown table");
    };
    match blocking(move || query::values(obs.backend.as_ref(), table, &p.col, p.from, p.to)).await {
        Ok(v) => Json(v).into_response(),
        Err(r) => r,
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RowsQuery {
    #[serde(default)]
    table: Option<String>,
    from: i64,
    to: i64,
    #[serde(default)]
    filter: BTreeMap<String, Vec<Value>>,
    /// Column to sort by, descending. Defaults to `ts`.
    #[serde(default)]
    order_by: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
}

/// Raw rows, newest (or largest by `order_by`) first.
async fn rows(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Json(q): Json<RowsQuery>,
) -> Response {
    let obs = match guard(&app, &headers) {
        Ok(o) => o,
        Err(r) => return r,
    };
    let Some(table) = Table::parse(q.table.as_deref().unwrap_or("requests")) else {
        return bad("unknown table");
    };
    let limit = q.limit.unwrap_or(100).min(2_000);
    let res = blocking(move || {
        let key = q.order_by.unwrap_or_else(|| "ts".into());
        let rank = |r: &Row| r.get(&key).and_then(Value::as_f64).unwrap_or(f64::MIN);
        let mut keep: Vec<Row> = Vec::new();
        obs.backend.scan(table, q.from, q.to, &mut |row| {
            if q.filter
                .iter()
                .all(|(c, allowed)| allowed.iter().any(|a| Some(a) == row.get(c)))
            {
                keep.push(row.clone());
                // Bound memory: trim to the best `limit` now and then.
                if keep.len() >= limit * 4 + 64 {
                    keep.sort_by(|a, b| rank(b).total_cmp(&rank(a)));
                    keep.truncate(limit);
                }
            }
        })?;
        keep.sort_by(|a, b| rank(b).total_cmp(&rank(a)));
        keep.truncate(limit);
        Ok(keep)
    })
    .await;
    match res {
        Ok(v) => Json(json!({ "rows": v })).into_response(),
        Err(r) => r,
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExportParams {
    #[serde(default)]
    table: Option<String>,
    from: i64,
    to: i64,
    /// Comma-separated columns to keep (`ts` always comes along). A replay
    /// needs about a dozen of the sixty; asking for those makes a day of
    /// log several times smaller and faster to read from a columnar store.
    #[serde(default)]
    cols: Option<String>,
}

/// The export reads the window in pieces this long, so memory stays at one
/// piece however long the window is.
const EXPORT_SLICE_MS: i64 = 10 * 60 * 1000;
/// Rows of one piece are sent on in chunks of about this many bytes.
const EXPORT_CHUNK: usize = 256 * 1024;

/// Every row of a time window, oldest first, one JSON object per line.
/// Unlike `rows` there is no cap: this is for taking a day of the log
/// somewhere else (a usage report, a simulator replay). The answer is
/// streamed, ten minutes of log at a time.
async fn export(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    UrlQuery(p): UrlQuery<ExportParams>,
) -> Response {
    let obs = match guard(&app, &headers) {
        Ok(o) => o,
        Err(r) => return r,
    };
    let Some(table) = Table::parse(p.table.as_deref().unwrap_or("requests")) else {
        return bad("unknown table");
    };
    if p.to < p.from {
        return bad("to is before from");
    }
    let cols: Option<Vec<String>> = p.cols.map(|c| {
        c.split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    });
    // The store says nothing about where its rows are, so an open-ended
    // window (`from=0`) would be millions of empty pieces: read such a
    // window in as many pieces as a week has.
    let span = p.to - p.from;
    let step = EXPORT_SLICE_MS.max(span / (7 * 24 * 6)).max(1);
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(4);
    tokio::task::spawn_blocking(move || {
        let mut at = p.from;
        while at < p.to {
            let end = at.saturating_add(step).min(p.to);
            let mut lines: Vec<(i64, String)> = Vec::new();
            let mut visit = |row: &Row| {
                let ts = row.get("ts").and_then(Value::as_i64).unwrap_or(0);
                lines.push((ts, serde_json::to_string(row).unwrap_or_default()));
            };
            let res = match &cols {
                Some(c) => {
                    let c: Vec<&str> = c.iter().map(String::as_str).collect();
                    obs.backend.scan_cols(table, at, end, &c, &mut visit)
                }
                None => obs.backend.scan(table, at, end, &mut visit),
            };
            if let Err(e) = res {
                // The status line is long gone: end the body early so the
                // reader sees a broken transfer instead of a short day.
                let _ = tx.blocking_send(Err(std::io::Error::other(e)));
                return;
            }
            lines.sort_by_key(|l| l.0);
            let mut out = String::with_capacity(EXPORT_CHUNK + 4096);
            for (_, l) in lines {
                out.push_str(&l);
                out.push('\n');
                if out.len() >= EXPORT_CHUNK {
                    let full =
                        std::mem::replace(&mut out, String::with_capacity(EXPORT_CHUNK + 4096));
                    if tx.blocking_send(Ok(Bytes::from(full))).is_err() {
                        return; // the reader went away
                    }
                }
            }
            if !out.is_empty() && tx.blocking_send(Ok(Bytes::from(out))).is_err() {
                return;
            }
            at = end;
        }
    });
    let stream = futures_util::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    });
    (
        [(header::CONTENT_TYPE, "application/x-ndjson")],
        axum::body::Body::from_stream(stream),
    )
        .into_response()
}

#[derive(Deserialize)]
struct TraceParams {
    /// Narrow the scan when the caller knows roughly when the trace ran.
    #[serde(default)]
    around: Option<i64>,
    /// `otlp` returns the trace as OTLP/JSON (`resourceSpans`), ready for any
    /// OpenTelemetry backend.
    #[serde(default)]
    format: Option<String>,
}

/// Every span of a trace (gateway and LiteLLM) plus its request rows.
async fn trace(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Path(trace_id): Path<String>,
    UrlQuery(p): UrlQuery<TraceParams>,
) -> Response {
    let obs = match guard(&app, &headers) {
        Ok(o) => o,
        Err(r) => return r,
    };
    let (from, to) = match p.around {
        Some(t) => (t - 3_600_000, t + 3_600_000),
        None => (now_ms() - 2 * 86_400_000, now_ms() + 60_000),
    };
    let otlp = match p.format.as_deref() {
        None | Some("json") => false,
        Some("otlp") => true,
        Some(other) => return bad(format!("unknown format {other:?} (json | otlp)")),
    };
    let res = blocking(move || {
        let want = json!(trace_id);
        let (mut spans, mut requests, mut tools) = (Vec::new(), Vec::new(), Vec::new());
        obs.backend.scan(Table::Spans, from, to, &mut |r| {
            if r.get("trace_id") == Some(&want) && spans.len() < 5_000 {
                spans.push(r.clone());
            }
        })?;
        obs.backend.scan(Table::Requests, from, to, &mut |r| {
            if r.get("trace_id") == Some(&want) && requests.len() < 1_000 {
                requests.push(r.clone());
            }
        })?;
        spans.sort_by_key(|r| r.get("start_us").and_then(Value::as_i64).unwrap_or(0));
        if otlp {
            return Ok(super::export::encode_json(&spans));
        }
        obs.backend.scan(Table::Tools, from, to, &mut |r| {
            if r.get("trace_id") == Some(&want) && tools.len() < 1_000 {
                tools.push(r.clone());
            }
        })?;
        requests.sort_by_key(|r| r.get("ts").and_then(Value::as_i64).unwrap_or(0));
        tools.sort_by_key(|r| r.get("ts").and_then(Value::as_i64).unwrap_or(0));
        Ok(json!({ "trace_id": trace_id, "spans": spans, "requests": requests, "tools": tools }))
    })
    .await;
    match res {
        Ok(v) => Json(v).into_response(),
        Err(r) => r,
    }
}

#[derive(Deserialize)]
struct Window {
    from: i64,
    to: i64,
}

async fn roi_report(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    UrlQuery(w): UrlQuery<Window>,
) -> Response {
    let obs = match guard(&app, &headers) {
        Ok(o) => o,
        Err(r) => return r,
    };
    match blocking(move || roi::report(obs.backend.as_ref(), w.from, w.to)).await {
        Ok(v) => Json(v).into_response(),
        Err(r) => r,
    }
}

#[derive(Deserialize)]
struct InsightQuery {
    from: i64,
    to: i64,
    tz: Option<f64>,
    day_s: Option<f64>,
    server_hour_usd: Option<f64>,
    slots: Option<f64>,
}

/// Productivity and suggestions per person, team and serving side.
async fn insights(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    UrlQuery(q): UrlQuery<InsightQuery>,
) -> Response {
    let obs = match guard(&app, &headers) {
        Ok(o) => o,
        Err(r) => return r,
    };
    let mut opts = insight::Options::default();
    if let Some(tz) = q.tz {
        opts.tz_hours = tz;
    }
    if let Some(d) = q.day_s.filter(|d| *d >= 24.0) {
        opts.day_s = d;
    }
    opts.server_hour_usd = q.server_hour_usd.filter(|p| *p > 0.0);
    opts.slots = q.slots.filter(|s| *s >= 1.0);
    match blocking(move || insight::report(obs.backend.as_ref(), q.from, q.to, &opts)).await {
        Ok(v) => Json(v).into_response(),
        Err(r) => r,
    }
}

const META_KINDS: &[&str] = &["deliverable", "link", "price", "rate"];

async fn meta_list(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Path(kind): Path<String>,
) -> Response {
    let obs = match guard(&app, &headers) {
        Ok(o) => o,
        Err(r) => return r,
    };
    if !META_KINDS.contains(&kind.as_str()) {
        return bad(format!("unknown kind {kind:?} (one of {META_KINDS:?})"));
    }
    match blocking(move || obs.backend.meta_list(&kind)).await {
        Ok(v) => Json(json!({ "items": v.into_iter().map(|(id, value)| json!({"id": id, "value": value})).collect::<Vec<_>>() })).into_response(),
        Err(r) => r,
    }
}

/// Upsert one metadata item. Body: `{"id": "...", ...fields}`; links take
/// `run_id` and `deliverable_id` instead of an id. Each kind is validated
/// against its type so the ROI report never meets a malformed record.
async fn meta_put(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Path(kind): Path<String>,
    Json(mut body): Json<Value>,
) -> Response {
    let obs = match guard(&app, &headers) {
        Ok(o) => o,
        Err(r) => return r,
    };
    let Some(obj) = body.as_object_mut() else {
        return bad("body must be a JSON object");
    };
    let id = obj
        .remove("id")
        .and_then(|v| v.as_str().map(str::to_string));
    let checked: Result<(String, Value), String> = (|| match kind.as_str() {
        "deliverable" => {
            let d: Deliverable = serde_json::from_value(body.clone()).map_err(|e| e.to_string())?;
            Ok((
                id.ok_or("id is required")?,
                serde_json::to_value(d).unwrap(),
            ))
        }
        "link" => {
            let l: Link = serde_json::from_value(body.clone()).map_err(|e| e.to_string())?;
            if !(l.weight.is_finite() && l.weight >= 0.0) {
                return Err("weight must be a non-negative number".into());
            }
            Ok((
                format!("{}|{}", l.run_id, l.deliverable_id),
                serde_json::to_value(l).unwrap(),
            ))
        }
        "price" => {
            let p: Price = serde_json::from_value(body.clone()).map_err(|e| e.to_string())?;
            Ok((
                id.ok_or("id (model name) is required")?,
                serde_json::to_value(p).unwrap(),
            ))
        }
        "rate" => {
            let r: Rate = serde_json::from_value(body.clone()).map_err(|e| e.to_string())?;
            Ok((
                id.ok_or("id (team id, or * for all) is required")?,
                serde_json::to_value(r).unwrap(),
            ))
        }
        other => Err(format!("unknown kind {other:?} (one of {META_KINDS:?})")),
    })();
    let (id, value) = match checked {
        Ok(v) => v,
        Err(e) => return bad(e),
    };
    if id.is_empty() || id.len() > 300 {
        return bad("id must be 1..=300 characters");
    }
    let out = json!({ "id": id, "value": value });
    match blocking(move || obs.backend.meta_put(&kind, &id, value)).await {
        Ok(()) => Json(out).into_response(),
        Err(r) => r,
    }
}

async fn meta_delete(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Path((kind, id)): Path<(String, String)>,
) -> Response {
    let obs = match guard(&app, &headers) {
        Ok(o) => o,
        Err(r) => return r,
    };
    match blocking(move || obs.backend.meta_delete(&kind, &id)).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(r) => r,
    }
}

/// OTLP/HTTP trace export, protobuf. Point LiteLLM's exporter here:
/// `OTEL_EXPORTER=otlp_http`, `OTEL_ENDPOINT=http://gw/gateway/otlp/v1/traces`,
/// `OTEL_HEADERS=Authorization=Bearer <token>`.
async fn otlp(State(app): State<Arc<App>>, headers: HeaderMap, body: Bytes) -> Response {
    let Some(obs) = app.obs.clone() else {
        return error(
            StatusCode::NOT_FOUND,
            "not_found_error",
            "observability is disabled",
        );
    };
    let authorized = match &obs.cfg.otlp_token {
        Some(want) => {
            let got = headers
                .get(header::AUTHORIZATION)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.strip_prefix("Bearer "))
                .unwrap_or("");
            constant_eq(got.as_bytes(), want.as_bytes())
        }
        None => app.admin_ok(&headers),
    };
    if !authorized {
        return unauthorized();
    }
    let ctype = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if !ctype.starts_with("application/x-protobuf") {
        return error(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "invalid_request_error",
            "send OTLP as application/x-protobuf",
        );
    }
    if headers.contains_key(header::CONTENT_ENCODING) {
        return error(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "invalid_request_error",
            "compressed OTLP is not supported; set OTEL_EXPORTER_OTLP_COMPRESSION=none",
        );
    }
    match super::otlp::decode(&body) {
        Ok(spans) => {
            obs.emit(Table::Spans, spans);
            // An empty ExportTraceServiceResponse.
            (
                [(header::CONTENT_TYPE, "application/x-protobuf")],
                Bytes::new(),
            )
                .into_response()
        }
        Err(e) => bad(format!("malformed OTLP payload: {e}")),
    }
}
