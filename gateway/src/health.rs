//! Health of the model servers behind each upstream.
//!
//! The scheduler already learns from requests: three failures in a row, or
//! one refused connection, take a server out for a while. That leaves two
//! holes. A server that died while idle is found by a caller's request, and
//! a server that is out is tried again with a caller's request. With
//! `health_probe_secs` set the gateway asks each server itself, so both are
//! found out without spending anyone's request.
//!
//! Whatever is learned, by request or by probe, every server that goes out
//! or comes back is written down: in the log, in memory, and with the
//! store's metadata (kind `health_event`) so it outlives a restart.

use std::collections::HashMap;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::{json, Value};

use crate::config::Kind;
use crate::{error, obs, App, Upstream};

/// Events kept with the store's metadata.
const STORED_EVENTS: usize = 1000;
const META_KIND: &str = "health_event";

/// Path asked for after `base`: the configured one, else LiteLLM's
/// liveness path or the model list (bases are written with or without the
/// version segment).
pub fn probe_path(u: &crate::config::UpstreamConfig, base: &str) -> String {
    match &u.health_probe_path {
        Some(p) => p.clone(),
        None if u.kind == Kind::Litellm => "/health/liveliness".into(),
        None if base.ends_with("/v1") || base.ends_with("/openai") => "/models".into(),
        None => "/v1/models".into(),
    }
}

/// How long one probe may take.
fn probe_timeout(every: Duration) -> Duration {
    every.clamp(Duration::from_millis(200), Duration::from_secs(5))
}

/// Ask one server. `Ok` when it answered below 500.
async fn probe(up: &Upstream, base: &str, timeout: Duration) -> Result<(), String> {
    let base = if base.is_empty() { &up.base } else { base };
    let mut req = up
        .client
        .get(format!("{base}{}", probe_path(&up.cfg, base)))
        .timeout(timeout);
    if let Some((k, v)) = &up.auth {
        req = req.header(k, v);
    }
    for (k, v) in &up.extra {
        req = req.header(k, v);
    }
    match req.send().await {
        Ok(r) if r.status().as_u16() < 500 => Ok(()),
        Ok(r) => Err(format!("status {}", r.status().as_u16())),
        Err(e) if e.is_timeout() => Err(format!("no answer within {} ms", timeout.as_millis())),
        Err(e) if e.is_connect() => Err("could not connect".into()),
        Err(e) => Err(e.without_url().to_string()),
    }
}

/// Probe every server of one upstream once.
pub async fn probe_upstream(up: &Upstream) {
    let every = Duration::from_secs(up.cfg.health_probe_secs.max(1));
    let timeout = probe_timeout(every);
    // Out for two rounds: the next probe comes before a request is tried.
    let hold = every * 2 + timeout;
    let targets = up.sched.probe_targets(false);
    let results =
        futures_util::future::join_all(targets.iter().map(|(_, base)| probe(up, base, timeout)))
            .await;
    for ((idx, _), res) in targets.iter().zip(results) {
        match res {
            Ok(()) => up.sched.probe_result(*idx, true, "", hold),
            Err(e) => up.sched.probe_result(*idx, false, &e, hold),
        }
    }
}

fn event_json(upstream: &str, e: &crate::scheduler::HealthEvent) -> Value {
    json!({
        "ts": e.ts_ms,
        "upstream": upstream,
        "server": e.backend,
        "state": e.state,
        "reason": e.reason,
        "down_ms": e.down_ms,
    })
}

/// Copy new events to the store and drop the oldest beyond what is kept.
fn record(app: &App, seen: &mut HashMap<String, u64>) {
    let Some(obs) = &app.obs else { return };
    let mut wrote = false;
    for (name, up) in &app.upstreams {
        let after = seen.get(name).copied().unwrap_or(0);
        for e in up.sched.health_events(after) {
            seen.insert(name.clone(), e.seq);
            let id = format!("{:013}-{name}-{}", e.ts_ms, e.seq);
            if let Err(err) = obs.backend.meta_put(META_KIND, &id, event_json(name, &e)) {
                tracing::warn!(error = %err, "health event not stored");
            }
            wrote = true;
        }
    }
    if !wrote {
        return;
    }
    if let Ok(mut all) = obs.backend.meta_list(META_KIND) {
        if all.len() > STORED_EVENTS {
            all.sort_by(|a, b| a.0.cmp(&b.0));
            let extra = all.len() - STORED_EVENTS;
            for (id, _) in all.into_iter().take(extra) {
                let _ = obs.backend.meta_delete(META_KIND, &id);
            }
        }
    }
}

/// Run the probes and keep the event record, until the app is dropped.
pub fn spawn(app: &Arc<App>) {
    let weak: Weak<App> = Arc::downgrade(app);
    tokio::spawn(async move {
        let mut due: HashMap<String, Instant> = HashMap::new();
        let mut seen: HashMap<String, u64> = HashMap::new();
        let mut tick = tokio::time::interval(Duration::from_millis(500));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            let Some(app) = weak.upgrade() else { return };
            let now = Instant::now();
            let mut round = Vec::new();
            for (name, up) in &app.upstreams {
                if up.cfg.health_probe_secs == 0 {
                    continue;
                }
                if due.get(name).is_none_or(|t| now >= *t) {
                    due.insert(
                        name.clone(),
                        now + Duration::from_secs(up.cfg.health_probe_secs),
                    );
                    round.push(probe_upstream(up));
                }
            }
            futures_util::future::join_all(round).await;
            record(&app, &mut seen);
        }
    });
}

/// `GET /gateway/servers`: every model server with its health, and the
/// record of servers going out and coming back (newest first).
pub async fn servers(State(app): State<Arc<App>>, headers: HeaderMap) -> Response {
    if !app.admin_ok(&headers) {
        return error(
            StatusCode::UNAUTHORIZED,
            "authentication_error",
            "admin token required",
        );
    }
    Json(servers_json(&app)).into_response()
}

pub(crate) fn servers_json(app: &App) -> Value {
    let mut ups = serde_json::Map::new();
    let mut down = 0;
    for (name, up) in &app.upstreams {
        let servers = up.sched.servers();
        down += servers.iter().filter(|s| s.health == "down").count();
        ups.insert(
            name.clone(),
            json!({
                "probe_secs": up.cfg.health_probe_secs,
                "probe_path": probe_path(&up.cfg, &up.base),
                "refuse_when_down": up.cfg.refuse_when_down,
                "servers": servers,
            }),
        );
    }
    let mut events: Vec<Value> = match &app.obs {
        Some(o) => {
            let mut all = o.backend.meta_list(META_KIND).unwrap_or_default();
            all.sort_by(|a, b| b.0.cmp(&a.0));
            all.into_iter().map(|(_, v)| v).collect()
        }
        None => Vec::new(),
    };
    if events.is_empty() {
        for (name, up) in &app.upstreams {
            events.extend(
                up.sched
                    .health_events(0)
                    .iter()
                    .map(|e| event_json(name, e)),
            );
        }
        events.sort_by_key(|e| std::cmp::Reverse(e["ts"].as_i64().unwrap_or(0)));
    }
    events.truncate(200);
    json!({
        "now": obs::now_ms(),
        "servers_down": down,
        "upstreams": ups,
        "events": events,
    })
}

/// The operations page: server health, the gateway's reports and what
/// callers measured, on one screen.
pub const OPS_PAGE: &str = include_str!("ops.html");

/// `GET /gateway/ops/data`: everything the operations page shows.
pub async fn ops_data(State(app): State<Arc<App>>, headers: HeaderMap) -> Response {
    if !app.admin_ok(&headers) {
        return error(
            StatusCode::UNAUTHORIZED,
            "authentication_error",
            "admin token required",
        );
    }
    let servers = servers_json(&app);
    let reports = app.cfg.reports.as_ref().map(crate::reports::listing);
    let client = match app.obs.clone() {
        Some(o) => {
            let now = obs::now_ms();
            tokio::task::spawn_blocking(move || {
                obs::client::summary(o.backend.as_ref(), now - 86_400_000, now + 1000).ok()
            })
            .await
            .ok()
            .flatten()
        }
        None => None,
    };
    Json(json!({ "servers": servers, "reports": reports, "client": client })).into_response()
}
