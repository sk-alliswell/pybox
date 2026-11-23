//! Management console: the web page at `/gateway/console` and the admin API
//! behind it.
//!
//! - upstreams: what each configured API looks like right now
//! - agent keys: edit limits and attribution, rotate a token
//! - models: what every upstream says it serves
//! - live: one sample a second of in-flight, queued and finished work
//! - jobs: batches of requests the gateway runs against itself, so they are
//!   scheduled, limited and logged like any other agent's traffic

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{delete, get, patch, post};
use axum::{Json, Router};
use futures_util::StreamExt;
use rand::Rng;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::config::Kind;
use crate::obs::now_ms;
use crate::{error, key_id, AgentInfo, App};

const PAGE: &str = include_str!("console.html");
/// Fifteen minutes of one-second samples.
const LIVE_SAMPLES: usize = 900;
const MAX_JOBS: usize = 200;
const MAX_JOB_REQUESTS: u64 = 100_000;
const MAX_JOB_CONCURRENCY: usize = 1_000;

#[derive(Default)]
pub struct Console {
    live: Mutex<VecDeque<Value>>,
    sampler_started: AtomicBool,
    jobs: Mutex<Vec<Arc<Job>>>,
    client: reqwest::Client,
}

pub fn routes() -> Router<Arc<App>> {
    Router::new()
        .route("/gateway/console", get(|| async { Html(PAGE) }))
        .route("/gateway/upstreams", get(upstreams))
        .route("/gateway/agents/{id}", patch(patch_agent))
        .route("/gateway/agents/{id}/rotate", post(rotate_agent))
        .route("/gateway/models", get(models))
        .route("/gateway/live", get(live))
        .route("/gateway/jobs", get(list_jobs).post(create_job))
        .route("/gateway/jobs/{id}", delete(cancel_job))
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

fn new_token() -> String {
    let bytes: [u8; 24] = rand::rng().random();
    format!(
        "pgw_{}",
        bytes.iter().map(|b| format!("{b:02x}")).collect::<String>()
    )
}

// ---- upstreams

async fn upstreams(State(app): State<Arc<App>>, headers: HeaderMap) -> Response {
    if !app.admin_ok(&headers) {
        return unauthorized();
    }
    let (_, counters) = app.stats.snapshot();
    let mut list: Vec<Value> = app
        .upstreams
        .values()
        .map(|u| {
            json!({
                "name": u.cfg.name,
                "base_url": u.cfg.base_url,
                "kind": format!("{:?}", u.cfg.kind).to_lowercase(),
                "cluster": u.cfg.cluster.as_deref().unwrap_or(&u.cfg.name),
                "key_alias": u.cfg.key_alias,
                "has_key": u.auth.is_some(),
                "max_concurrency": u.cfg.max_concurrency,
                "rpm": u.cfg.rpm,
                "input_tpm": u.cfg.input_tpm,
                "idle_timeout_secs": u.cfg.idle_timeout_secs,
                "scheduler": u.sched.snapshot(),
                "counters": counters.get(&u.cfg.name).cloned().unwrap_or_default(),
            })
        })
        .collect();
    list.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
    Json(json!({ "upstreams": list })).into_response()
}

// ---- agent keys

/// Absent fields stay as they are; `null` clears an optional one.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AgentPatch {
    #[serde(default)]
    weight: Option<u32>,
    #[serde(default, deserialize_with = "some")]
    max_concurrency: Option<Option<usize>>,
    #[serde(default, deserialize_with = "some")]
    token_budget: Option<Option<u64>>,
    #[serde(default, deserialize_with = "some")]
    project_id: Option<Option<String>>,
    #[serde(default, deserialize_with = "some")]
    team_id: Option<Option<String>>,
    #[serde(default, deserialize_with = "some")]
    user_id: Option<Option<String>>,
    #[serde(default, deserialize_with = "some")]
    session_id: Option<Option<String>>,
}

fn some<'de, T: Deserialize<'de>, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<Option<T>, D::Error> {
    T::deserialize(d).map(Some)
}

async fn patch_agent(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(p): Json<AgentPatch>,
) -> Response {
    if !app.admin_ok(&headers) {
        return unauthorized();
    }
    if p.max_concurrency == Some(Some(0)) {
        return bad("max_concurrency must be at least 1 (or null for no limit)");
    }
    let mut agents = app.agents.write().unwrap();
    let mut updated = None;
    for a in agents.values_mut().filter(|a| a.id == id) {
        let mut n = (**a).clone();
        if let Some(w) = p.weight {
            n.weight = w.max(1);
        }
        if let Some(v) = &p.max_concurrency {
            n.max_concurrency = *v;
        }
        if let Some(v) = &p.token_budget {
            n.token_budget = *v;
        }
        let text = |v: &Option<String>| v.clone().filter(|s| !s.is_empty());
        if let Some(v) = &p.project_id {
            n.project_id = text(v);
        }
        if let Some(v) = &p.team_id {
            n.team_id = text(v);
        }
        if let Some(v) = &p.user_id {
            n.user_id = text(v);
        }
        if let Some(v) = &p.session_id {
            n.session_id = text(v);
        }
        *a = Arc::new(n);
        updated = Some(a.clone());
    }
    drop(agents);
    if updated.is_some() {
        app.save_agents();
    }
    match updated {
        Some(a) => Json(json!({ "agent": *a })).into_response(),
        None => error(StatusCode::NOT_FOUND, "not_found_error", "unknown agent"),
    }
}

/// Replace every token of an agent with one new token, shown only here.
async fn rotate_agent(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    if !app.admin_ok(&headers) {
        return unauthorized();
    }
    let mut agents = app.agents.write().unwrap();
    let Some(old) = agents.values().find(|a| a.id == id).cloned() else {
        return error(StatusCode::NOT_FOUND, "not_found_error", "unknown agent");
    };
    agents.retain(|_, a| a.id != id);
    let token = new_token();
    let info = Arc::new(AgentInfo {
        key_id: key_id(&token),
        ..(*old).clone()
    });
    agents.insert(token.clone(), info.clone());
    drop(agents);
    app.save_agents();
    Json(json!({ "agent": *info, "token": token })).into_response()
}

// ---- models

/// Ask every upstream which models it serves. Slow or failing upstreams are
/// reported next to the ones that answered.
async fn models(State(app): State<Arc<App>>, headers: HeaderMap) -> Response {
    if !app.admin_ok(&headers) {
        return unauthorized();
    }
    let asks = app.upstreams.values().map(|u| async move {
        // Bases are written with or without the version segment.
        let url = if u.base.ends_with("/v1") || u.base.ends_with("/openai") {
            format!("{}/models", u.base)
        } else {
            format!("{}/v1/models", u.base)
        };
        let mut req = u.client.get(&url).timeout(Duration::from_secs(8));
        if let Some((k, v)) = &u.auth {
            req = req.header(k, v);
        }
        if u.cfg.kind == Kind::Anthropic {
            req = req.header("anthropic-version", "2023-06-01");
        }
        for (k, v) in &u.extra {
            req = req.header(k, v);
        }
        let got: Result<Vec<Value>, String> = async {
            let res = req.send().await.map_err(|e| e.to_string())?;
            let status = res.status();
            let body: Value = res.json().await.map_err(|e| e.to_string())?;
            if !status.is_success() {
                return Err(format!("HTTP {}", status.as_u16()));
            }
            Ok(body["data"].as_array().cloned().unwrap_or_default())
        }
        .await;
        (u.cfg.name.clone(), got)
    });
    let (mut list, mut errors) = (Vec::new(), BTreeMap::new());
    for (name, got) in futures_util::future::join_all(asks).await {
        match got {
            Ok(items) => list.extend(items.into_iter().filter_map(|m| {
                let id = m["id"].as_str()?.to_string();
                Some(json!({ "upstream": name, "id": id, "owned_by": m["owned_by"] }))
            })),
            Err(e) => {
                errors.insert(name, e);
            }
        }
    }
    list.sort_by(|a, b| {
        (a["upstream"].as_str(), a["id"].as_str()).cmp(&(b["upstream"].as_str(), b["id"].as_str()))
    });
    Json(json!({ "models": list, "errors": errors })).into_response()
}

// ---- live samples

fn sample(app: &App) -> Value {
    let (agents, counters) = app.stats.snapshot();
    let ups: BTreeMap<&str, Value> = app
        .upstreams
        .iter()
        .map(|(name, u)| {
            let s = u.sched.snapshot();
            let c = counters.get(name).cloned().unwrap_or_default();
            (
                name.as_str(),
                json!({
                    "inflight": s.inflight, "queued": s.queued, "cooldown_ms": s.cooldown_ms,
                    "max_concurrency": s.max_concurrency, "requests": c.requests,
                    "rate_limited": c.rate_limited, "errors": c.errors, "retries": c.retries,
                }),
            )
        })
        .collect();
    let sum = |f: fn(&crate::stats::AgentStats) -> u64| agents.values().map(f).sum::<u64>();
    json!({
        "ts": now_ms(),
        "upstreams": ups,
        // Cumulative since start; the page turns them into rates.
        "requests": sum(|a| a.requests),
        "errors": sum(|a| a.errors),
        "input_tokens": sum(|a| a.input_tokens + a.cache_read_tokens + a.cache_write_tokens),
        "output_tokens": sum(|a| a.output_tokens),
    })
}

/// Sampling starts with the first viewer and stops with the gateway.
fn ensure_sampler(app: &Arc<App>) {
    if app.console.sampler_started.swap(true, Relaxed) {
        return;
    }
    let weak: Weak<App> = Arc::downgrade(app);
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        loop {
            tick.tick().await;
            let Some(app) = weak.upgrade() else { return };
            let s = sample(&app);
            let mut ring = app.console.live.lock().unwrap();
            if ring.len() >= LIVE_SAMPLES {
                ring.pop_front();
            }
            ring.push_back(s);
        }
    });
}

async fn live(State(app): State<Arc<App>>, headers: HeaderMap) -> Response {
    if !app.admin_ok(&headers) {
        return unauthorized();
    }
    ensure_sampler(&app);
    let (agent_stats, _) = app.stats.snapshot();
    let mut agents: BTreeMap<String, Value> = BTreeMap::new();
    for u in app.upstreams.values() {
        for (id, a) in u.sched.snapshot().agents {
            let e = agents
                .entry(id)
                .or_insert_with(|| json!({ "inflight": 0, "queued": 0 }));
            e["inflight"] = json!(e["inflight"].as_u64().unwrap_or(0) + a.inflight as u64);
            e["queued"] = json!(e["queued"].as_u64().unwrap_or(0) + a.queued as u64);
        }
    }
    for (id, v) in agents.iter_mut() {
        if let Some(s) = agent_stats.get(id) {
            v["requests"] = json!(s.requests);
            v["output_tokens"] = json!(s.output_tokens);
        }
    }
    let samples: Vec<Value> = app.console.live.lock().unwrap().iter().cloned().collect();
    Json(json!({ "now": sample(&app), "samples": samples, "agents": agents })).into_response()
}

// ---- jobs

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NewJob {
    #[serde(default)]
    name: String,
    upstream: String,
    model: String,
    /// Used in turn, wrapping around.
    prompts: Vec<String>,
    /// Requests to send in total.
    count: u64,
    /// Requests the job keeps open at once. The upstream's own cap still applies.
    concurrency: usize,
    #[serde(default = "default_max_tokens")]
    max_tokens: u32,
    #[serde(default)]
    system: Option<String>,
    /// Attribution for the job's spend.
    #[serde(default)]
    project_id: Option<String>,
    #[serde(default)]
    team_id: Option<String>,
    #[serde(default)]
    deliverable_id: Option<String>,
    #[serde(default = "one")]
    weight: u32,
}

fn default_max_tokens() -> u32 {
    128
}
fn one() -> u32 {
    1
}

#[derive(Serialize)]
pub struct Job {
    id: String,
    name: String,
    agent_id: String,
    upstream: String,
    model: String,
    count: u64,
    concurrency: usize,
    max_tokens: u32,
    project_id: Option<String>,
    team_id: Option<String>,
    created_ms: i64,
    finished_ms: AtomicU64,
    started: AtomicU64,
    ok: AtomicU64,
    failed: AtomicU64,
    cancelled: AtomicBool,
    /// Most recent failure, to show why a job is going wrong.
    last_error: Mutex<String>,
}

impl Job {
    fn view(&self) -> Value {
        let mut v = serde_json::to_value(self).unwrap_or_default();
        let (ok, failed) = (self.ok.load(Relaxed), self.failed.load(Relaxed));
        let finished = self.finished_ms.load(Relaxed) > 0;
        v["state"] = json!(match (finished, self.cancelled.load(Relaxed)) {
            (true, true) => "cancelled",
            (true, false) => "done",
            (false, true) => "cancelling",
            (false, false) => "running",
        });
        v["inflight"] = json!(self.started.load(Relaxed).saturating_sub(ok + failed));
        v
    }
}

async fn list_jobs(State(app): State<Arc<App>>, headers: HeaderMap) -> Response {
    if !app.admin_ok(&headers) {
        return unauthorized();
    }
    let jobs: Vec<Value> = app
        .console
        .jobs
        .lock()
        .unwrap()
        .iter()
        .rev()
        .map(|j| j.view())
        .collect();
    Json(json!({ "jobs": jobs })).into_response()
}

async fn create_job(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Json(req): Json<NewJob>,
) -> Response {
    if !app.admin_ok(&headers) {
        return unauthorized();
    }
    let Some(up) = app.upstreams.get(&req.upstream) else {
        return bad(format!("unknown upstream {:?}", req.upstream));
    };
    let prompts: Vec<String> = req
        .prompts
        .into_iter()
        .filter(|p| !p.trim().is_empty())
        .collect();
    if prompts.is_empty() || req.model.is_empty() {
        return bad("model and at least one prompt are required");
    }
    if req.count == 0 || req.count > MAX_JOB_REQUESTS {
        return bad(format!("count must be 1..={MAX_JOB_REQUESTS}"));
    }
    if req.concurrency == 0 || req.concurrency > MAX_JOB_CONCURRENCY {
        return bad(format!("concurrency must be 1..={MAX_JOB_CONCURRENCY}"));
    }
    let Ok(listen) = app.cfg.listen.parse::<std::net::SocketAddr>() else {
        return bad("the gateway's listen address is not an IP address and port");
    };
    // Reach ourselves over loopback even when listening on every interface.
    let host = if listen.ip().is_unspecified() {
        std::net::SocketAddr::from(([127, 0, 0, 1], listen.port()))
    } else {
        listen
    };
    let anthropic = up.cfg.kind == Kind::Anthropic;
    let url = format!(
        "http://{host}/{}/v1/{}",
        req.upstream,
        if anthropic {
            "messages"
        } else {
            "chat/completions"
        }
    );

    let id = format!(
        "job-{}",
        rand::rng()
            .random::<[u8; 5]>()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>()
    );
    let name = if req.name.trim().is_empty() {
        id.clone()
    } else {
        req.name.trim().to_string()
    };
    // The job is an agent like any other: its own key, weight and attribution.
    let token = new_token();
    let agent_id = format!("job:{name}");
    app.agents.write().unwrap().insert(
        token.clone(),
        Arc::new(AgentInfo {
            id: agent_id.clone(),
            weight: req.weight.max(1),
            max_concurrency: None,
            token_budget: None,
            project_id: req.project_id.clone().filter(|s| !s.is_empty()),
            team_id: req.team_id.clone().filter(|s| !s.is_empty()),
            user_id: Some("console".into()),
            session_id: Some(id.clone()),
            priority: Some("batch".into()),
            kind: Some("console".into()),
            max_wait_ms: None,
            model_swap: false,
            key_id: key_id(&token),
            mcp_allow: None,
            mcp_deny: Vec::new(),
        }),
    );
    let job = Arc::new(Job {
        id: id.clone(),
        name,
        agent_id,
        upstream: req.upstream.clone(),
        model: req.model.clone(),
        count: req.count,
        concurrency: req.concurrency,
        max_tokens: req.max_tokens,
        project_id: req.project_id,
        team_id: req.team_id,
        created_ms: now_ms(),
        finished_ms: AtomicU64::new(0),
        started: AtomicU64::new(0),
        ok: AtomicU64::new(0),
        failed: AtomicU64::new(0),
        cancelled: AtomicBool::new(false),
        last_error: Mutex::new(String::new()),
    });
    {
        let mut jobs = app.console.jobs.lock().unwrap();
        if jobs.len() >= MAX_JOBS {
            if let Some(i) = jobs.iter().position(|j| j.finished_ms.load(Relaxed) > 0) {
                jobs.remove(i);
            }
        }
        jobs.push(job.clone());
    }

    let weak = Arc::downgrade(&app);
    let client = app.console.client.clone();
    let (j, model, max_tokens, system, deliverable) = (
        job.clone(),
        req.model,
        req.max_tokens,
        req.system,
        req.deliverable_id.filter(|s| !s.is_empty()),
    );
    tokio::spawn(async move {
        let one = |i: u64| {
            let (client, url, token, j) = (client.clone(), url.clone(), token.clone(), j.clone());
            let prompt = prompts[(i % prompts.len() as u64) as usize].clone();
            let mut body = json!({
                "model": model, "max_tokens": max_tokens, "stream": true,
                "messages": [{ "role": "user", "content": prompt }],
            });
            match (&system, anthropic) {
                (Some(s), true) => body["system"] = json!(s),
                (Some(s), false) => {
                    let msgs = body["messages"].as_array_mut().unwrap();
                    msgs.insert(0, json!({ "role": "system", "content": s }));
                }
                _ => {}
            }
            let deliverable = deliverable.clone();
            async move {
                if j.cancelled.load(Relaxed) {
                    return;
                }
                j.started.fetch_add(1, Relaxed);
                let mut r = client
                    .post(&url)
                    .bearer_auth(&token)
                    .header("x-gateway-run-id", &j.id)
                    .header("x-gateway-client-sent-at", now_ms().to_string())
                    .json(&body);
                if anthropic {
                    r = r.header("anthropic-version", "2023-06-01");
                }
                if let Some(d) = &deliverable {
                    r = r.header("x-gateway-deliverable-id", d);
                }
                let outcome: Result<(), String> = async {
                    let res = r.send().await.map_err(|e| e.to_string())?;
                    let status = res.status();
                    if !status.is_success() {
                        let text = res.text().await.unwrap_or_default();
                        let short: String = text.chars().take(300).collect();
                        return Err(format!("HTTP {}: {short}", status.as_u16()));
                    }
                    // Drain the stream; the gateway's own log has the detail.
                    let mut s = res.bytes_stream();
                    while let Some(chunk) = s.next().await {
                        chunk.map_err(|e| e.to_string())?;
                        if j.cancelled.load(Relaxed) {
                            return Err("cancelled".into());
                        }
                    }
                    Ok(())
                }
                .await;
                match outcome {
                    Ok(()) => j.ok.fetch_add(1, Relaxed),
                    Err(e) => {
                        *j.last_error.lock().unwrap() = e;
                        j.failed.fetch_add(1, Relaxed)
                    }
                };
            }
        };
        futures_util::stream::iter(0..j.count)
            .for_each_concurrent(j.concurrency, one)
            .await;
        j.finished_ms.store(now_ms() as u64, Relaxed);
        // The job's key dies with the job.
        if let Some(app) = weak.upgrade() {
            app.agents.write().unwrap().remove(&token);
        }
    });
    (StatusCode::CREATED, Json(json!({ "job": job.view() }))).into_response()
}

async fn cancel_job(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    if !app.admin_ok(&headers) {
        return unauthorized();
    }
    let jobs = app.console.jobs.lock().unwrap();
    match jobs.iter().find(|j| j.id == id) {
        Some(j) => {
            j.cancelled.store(true, Relaxed);
            Json(json!({ "job": j.view() })).into_response()
        }
        None => error(StatusCode::NOT_FOUND, "not_found_error", "unknown job"),
    }
}
