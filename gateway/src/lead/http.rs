//! Admin endpoints for the ledger, cache diagnosis and experiments.

use std::sync::Arc;

use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::json;

use super::engine::Lead;
use super::ledger::Event;
use crate::obs::now_ms;
use crate::{error, App};

pub fn routes() -> Router<Arc<App>> {
    Router::new()
        .route("/gateway/lead/roi", get(roi))
        .route("/gateway/lead/events", get(list_events).post(add_event))
        .route("/gateway/lead/cache", get(cache))
        .route("/gateway/lead/experiment", get(experiment))
}

#[allow(clippy::result_large_err)]
fn guard(app: &Arc<App>, headers: &HeaderMap) -> Result<Arc<Lead>, Response> {
    if !app.admin_ok(headers) {
        return Err(error(
            StatusCode::UNAUTHORIZED,
            "authentication_error",
            "admin token required",
        ));
    }
    app.lead.clone().ok_or_else(|| {
        error(
            StatusCode::NOT_FOUND,
            "not_found_error",
            "lead is disabled in the config",
        )
    })
}

fn bad(msg: String) -> Response {
    error(StatusCode::BAD_REQUEST, "invalid_request_error", msg)
}

#[derive(Deserialize)]
struct Window {
    /// Unix ms. Defaults to the last 30 days.
    from: Option<i64>,
    to: Option<i64>,
    /// Use only what was known at this moment.
    as_of: Option<i64>,
    /// Experiment to report: `cache_points` (default) or `tool_results`.
    name: Option<String>,
}

impl Window {
    fn range(&self) -> (i64, i64) {
        // The ledger works in whole hours; include the current one.
        let to = self.to.unwrap_or_else(|| now_ms() + super::ledger::HOUR_MS);
        (self.from.unwrap_or(to - 30 * 86_400_000), to)
    }
}

async fn roi(State(app): State<Arc<App>>, headers: HeaderMap, Query(w): Query<Window>) -> Response {
    let lead = match guard(&app, &headers) {
        Ok(l) => l,
        Err(r) => return r,
    };
    let (from, to) = w.range();
    match lead.roi(from, to, w.as_of.unwrap_or(i64::MAX)) {
        Ok(v) => Json(v).into_response(),
        Err(e) => bad(e),
    }
}

async fn list_events(State(app): State<Arc<App>>, headers: HeaderMap) -> Response {
    match guard(&app, &headers) {
        Ok(lead) => Json(json!({ "events": lead.events() })).into_response(),
        Err(r) => r,
    }
}

/// Append one deliverable event. `ts` defaults to now.
async fn add_event(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Json(mut body): Json<serde_json::Value>,
) -> Response {
    let lead = match guard(&app, &headers) {
        Ok(l) => l,
        Err(r) => return r,
    };
    let now = now_ms();
    if let Some(o) = body.as_object_mut() {
        o.entry("ts").or_insert(json!(now));
        o.remove("recorded_at");
    }
    let event: Event = match serde_json::from_value(body) {
        Ok(e) => e,
        Err(e) => return bad(e.to_string()),
    };
    match tokio::task::spawn_blocking(move || lead.record_event(event, now)).await {
        Ok(Ok(id)) => Json(json!({ "id": id, "recorded_at": now })).into_response(),
        Ok(Err(e)) => bad(e),
        Err(e) => error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "api_error",
            e.to_string(),
        ),
    }
}

async fn cache(State(app): State<Arc<App>>, headers: HeaderMap) -> Response {
    match guard(&app, &headers) {
        Ok(lead) => Json(lead.cache_breaks()).into_response(),
        Err(r) => r,
    }
}

async fn experiment(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Query(w): Query<Window>,
) -> Response {
    let lead = match guard(&app, &headers) {
        Ok(l) => l,
        Err(r) => return r,
    };
    let (from, to) = w.range();
    let name = w.name.clone().unwrap_or_else(|| "cache_points".into());
    match tokio::task::spawn_blocking(move || lead.experiment(&name, from, to)).await {
        Ok(Ok(v)) => Json(v).into_response(),
        Ok(Err(e)) => bad(e),
        Err(e) => error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "api_error",
            e.to_string(),
        ),
    }
}
