//! pybox-gateway: a host-side LLM API gateway for many local agents.
//!
//! Agents point their SDK `base_url` at `http://127.0.0.1:8787/<upstream>` and
//! use a per-agent gateway token as their API key. The gateway swaps in the
//! real key and forwards the request through a shared, pooled HTTP client,
//! with per-upstream pacing, a shared 429 cooldown, fair queuing, retries and
//! byte-for-byte SSE streaming.

pub mod agents;
pub mod cache;
pub mod cli;
pub mod config;
pub mod console;
pub mod health;
pub mod lead;
pub mod limiter;
pub mod limits;
pub mod loopguard;
pub mod mcp;
pub mod obs;
mod proxy;
pub mod reports;
pub mod scheduler;
pub mod stats;
pub mod usage;

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::{Context, Result};
use axum::extract::{DefaultBodyLimit, Path, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, delete, get, put};
use axum::{Json, Router};
use rand::Rng;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::cache::Cache;
use crate::config::{Config, Kind, UpstreamConfig};
use crate::limiter::RateLimiter;
use crate::limits::{Amount, Denied, LimitRule, Limits, Subject};
use crate::scheduler::Scheduler;
use crate::stats::Stats;

pub struct Upstream {
    pub cfg: UpstreamConfig,
    base: String,
    /// Host and port of `base`, for `server.address` / `server.port`.
    server: (String, Option<u16>),
    auth: Option<(http::HeaderName, HeaderValue)>,
    extra: Vec<(http::HeaderName, HeaderValue)>,
    client: reqwest::Client,
    pub sched: Arc<Scheduler>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct AgentInfo {
    pub id: String,
    pub weight: u32,
    pub max_concurrency: Option<usize>,
    pub token_budget: Option<u64>,
    pub project_id: Option<String>,
    pub team_id: Option<String>,
    pub user_id: Option<String>,
    pub session_id: Option<String>,
    pub priority: Option<String>,
    /// What this agent's requests are (`ide`, `chat`, `batch`, ...): a label
    /// for the request log, not used for scheduling.
    pub kind: Option<String>,
    /// How long this agent's callers can wait for a seat (ms).
    pub max_wait_ms: Option<u64>,
    /// May be sent to a stand-in model from the upstream's `model_groups`.
    pub model_swap: bool,
    /// First 12 hex digits of SHA-256(token): identifies the key in reports
    /// without revealing it.
    pub key_id: String,
    /// MCP tools this agent may use (`server/tool` patterns).
    pub mcp_allow: Option<Vec<String>>,
    pub mcp_deny: Vec<String>,
}

impl AgentInfo {
    pub(crate) fn from_config(a: &config::AgentConfig) -> Self {
        Self {
            id: a.id.clone(),
            weight: a.weight,
            max_concurrency: a.max_concurrency,
            token_budget: a.token_budget,
            project_id: a.project_id.clone(),
            team_id: a.team_id.clone(),
            user_id: a.user_id.clone(),
            session_id: a.session_id.clone(),
            priority: a.priority.clone(),
            kind: a.kind.clone(),
            max_wait_ms: a.max_wait_ms,
            model_swap: a.model_swap,
            key_id: key_id(&a.token),
            mcp_allow: a.mcp_allow.clone(),
            mcp_deny: a.mcp_deny.clone(),
        }
    }
}

pub fn key_id(token: &str) -> String {
    use sha2::{Digest, Sha256};
    Sha256::digest(token.as_bytes())[..6]
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

pub struct App {
    pub cfg: Config,
    pub upstreams: HashMap<String, Upstream>,
    agents: RwLock<HashMap<String, Arc<AgentInfo>>>,
    pub stats: Stats,
    cache: Option<Arc<Cache>>,
    pub obs: Option<Arc<obs::Obs>>,
    pub(crate) console: console::Console,
    pub(crate) loop_guard: Option<loopguard::LoopGuard>,
    pub limits: Arc<Limits>,
    /// MCP servers by name, reachable at `/mcp/<name>`.
    pub mcp: HashMap<String, mcp::Server>,
    pub lead: Option<Arc<lead::engine::Lead>>,
    /// (upstream, model) pairs that rejected a request the gateway had
    /// rewritten, and until when (Unix ms) rewriting stays off for them.
    pub(crate) rewrite_off: std::sync::Mutex<HashMap<(String, String), i64>>,
    /// Requests that can be asked about while they wait, by (agent,
    /// ticket).
    pub(crate) tickets: proxy::Tickets,
}

impl App {
    pub fn new(cfg: Config) -> Result<Arc<Self>> {
        let obs = obs::Obs::new(cfg.obs.clone())?;
        Self::with_obs(cfg, obs)
    }

    /// Build with an observability store chosen by the caller (the
    /// `pybox-gateway-tdb` binary passes a Super Shotgun TDB backend).
    pub fn with_obs(cfg: Config, obs: Option<Arc<obs::Obs>>) -> Result<Arc<Self>> {
        cfg.validate()?;
        let mut upstreams = HashMap::new();
        for u in &cfg.upstreams {
            let key = u.resolve_key()?;
            let auth = match (u.kind, key) {
                (Kind::Anthropic, Some(k)) => Some((
                    http::HeaderName::from_static("x-api-key"),
                    HeaderValue::from_str(&k).context("api key")?,
                )),
                (Kind::Openai | Kind::Litellm, Some(k)) => Some((
                    http::header::AUTHORIZATION,
                    HeaderValue::from_str(&format!("Bearer {k}")).context("api key")?,
                )),
                _ => None,
            };
            let mut extra = Vec::new();
            for (k, v) in &u.headers {
                extra.push((
                    http::HeaderName::from_bytes(k.as_bytes()).context("header name")?,
                    HeaderValue::from_str(v).context("header value")?,
                ));
            }
            // One pooled client per upstream: keep-alive connections are reused
            // across all agents, and HTTP/2 is negotiated via ALPN when offered.
            let client = reqwest::Client::builder()
                .pool_idle_timeout(Duration::from_secs(90))
                .pool_max_idle_per_host(u.max_concurrency.max(4))
                .tcp_keepalive(Duration::from_secs(30))
                .tcp_nodelay(true)
                .connect_timeout(Duration::from_secs(10))
                .read_timeout(Duration::from_secs(u.idle_timeout_secs))
                .http2_adaptive_window(true)
                // Notice a dead multiplexed connection in seconds instead of
                // stalling every stream on it until the read timeout.
                .http2_keep_alive_interval(Duration::from_secs(20))
                .http2_keep_alive_timeout(Duration::from_secs(10))
                .http2_keep_alive_while_idle(true)
                .no_gzip()
                .no_brotli()
                .no_deflate()
                .build()?;
            let limiter = RateLimiter::new(u.rpm, u.input_tpm);
            let backends = if u.backends.is_empty() {
                vec![scheduler::BackendSpec {
                    name: u.name.clone(),
                    // One server: requests go to the upstream's own URL.
                    url: String::new(),
                    limit: u.max_concurrency,
                    adaptive: u.adaptive,
                    models: Vec::new(),
                    prefix_slots: 0,
                }]
            } else {
                u.backends
                    .iter()
                    .map(|b| scheduler::BackendSpec {
                        name: b.name.clone(),
                        url: b.base_url.trim_end_matches('/').to_string(),
                        limit: b.max_concurrency,
                        adaptive: b.adaptive.unwrap_or(u.adaptive),
                        models: b.models.clone(),
                        prefix_slots: b.prefix_slots.unwrap_or(0),
                    })
                    .collect()
            };
            let sched = Scheduler::spawn_opts(
                u.name.clone(),
                backends,
                limiter,
                scheduler::Options {
                    fair: u.policy == "fair",
                    fair_cfg: lead::fair::FairCfg::default(),
                    strict: u.strict_priority,
                    min_share: u.min_share,
                    reserve: u.reserve_slots,
                    spill: u.spill,
                    slowdown: u.adaptive_slowdown,
                    affinity: lead::prefix::Affinity::parse(&u.affinity)
                        .expect("checked when the config was read"),
                    affinity_min_tokens: u.affinity_min_tokens,
                    affinity_wait: std::time::Duration::from_millis(u.affinity_wait_ms),
                    prefix_ttl: std::time::Duration::from_secs(u.prefix_ttl_secs),
                    load_weight: (u.route_by == "cost").then_some(u.load_weight),
                    slot_hold: std::time::Duration::from_millis(u.slot_hold_ms),
                    slot_max_wait: std::time::Duration::from_millis(u.slot_max_wait_ms),
                    batch_group: std::time::Duration::from_millis(u.batch_group_ms),
                    stale_after: std::time::Duration::from_secs(u.stale_after_secs),
                    used_half_life: std::time::Duration::from_secs(u.least_used_half_life_secs),
                    hold: u.hold_batch.then_some(scheduler::Hold {
                        max: u.hold_max,
                        max_bytes: u.hold_max_bytes,
                    }),
                },
            );
            upstreams.insert(
                u.name.clone(),
                Upstream {
                    server: obs::genai::server_of(&u.base_url),
                    base: u.base_url.trim_end_matches('/').to_string(),
                    cfg: u.clone(),
                    auth,
                    extra,
                    client,
                    sched,
                },
            );
        }
        let agents = cfg
            .agents
            .iter()
            .map(|a| (a.token.clone(), Arc::new(AgentInfo::from_config(a))))
            .collect();
        let cache = (cfg.cache.ttl_secs > 0).then(|| {
            Cache::new(
                Duration::from_secs(cfg.cache.ttl_secs),
                cfg.cache.max_entries,
            )
        });
        let loop_guard = (cfg.loop_guard.max_repeats > 0).then(|| {
            loopguard::LoopGuard::new(
                cfg.loop_guard.max_repeats,
                Duration::from_secs(cfg.loop_guard.window_secs),
            )
        });
        let limits = Limits::new(cfg.limits.clone()).map_err(anyhow::Error::msg)?;
        let mut mcp_servers = HashMap::new();
        for m in &cfg.mcp_servers {
            mcp_servers.insert(m.name.clone(), mcp::Server::new(m)?);
        }
        let lead = if cfg.lead.enabled {
            let store = obs.as_ref().map(|o| o.backend.clone());
            Some(
                lead::engine::Lead::new(cfg.lead.clone(), store, obs::now_ms())
                    .map_err(|e| anyhow::anyhow!("lead: {e}"))?,
            )
        } else {
            None
        };
        let app = Arc::new(Self {
            lead,
            rewrite_off: std::sync::Mutex::new(HashMap::new()),
            tickets: proxy::Tickets::default(),
            cfg,
            upstreams,
            agents: RwLock::new(agents),
            stats: Stats::default(),
            cache,
            obs,
            console: console::Console::default(),
            loop_guard,
            limits,
            mcp: mcp_servers,
        });
        app.restore_limits();
        app.restore_agents();
        health::spawn(&app);
        reports::spawn(&app);
        Ok(app)
    }

    /// Bring limit rules set over the API and the usage already spent in the
    /// open windows back from the store, so a restart does not hand everyone
    /// a fresh budget.
    fn restore_limits(&self) {
        let Some(obs) = &self.obs else { return };
        for (name, v) in obs.backend.meta_list("limit").unwrap_or_default() {
            match serde_json::from_value::<LimitRule>(v) {
                Ok(rule) => {
                    if let Err(e) = self.limits.put(rule) {
                        tracing::warn!(limit = %name, error = %e, "stored limit rule skipped");
                    }
                }
                Err(e) => tracing::warn!(limit = %name, error = %e, "stored limit rule skipped"),
            }
        }
        let now = obs::now_ms();
        let Some(from) = self.limits.restore_from_ms(now) else {
            return;
        };
        let text = |r: &obs::store::Row, k: &str| {
            r.get(k)
                .and_then(serde_json::Value::as_str)
                .unwrap_or("")
                .to_string()
        };
        let num = |r: &obs::store::Row, k: &str| {
            r.get(k).and_then(serde_json::Value::as_f64).unwrap_or(0.0)
        };
        let subject = |r: &obs::store::Row| Subject {
            agent: text(r, "agent_id"),
            key: text(r, "gateway_key_id"),
            user: text(r, "user_id"),
            team: text(r, "team_id"),
            project: text(r, "project_id"),
            session: text(r, "session_id"),
            upstream: text(r, "upstream"),
            model: text(r, "model"),
            tool: String::new(),
        };
        let mut n = 0u64;
        let cols = [
            "agent_id",
            "gateway_key_id",
            "user_id",
            "team_id",
            "project_id",
            "session_id",
            "upstream",
            "model",
            "total_tokens",
            "cost_usd",
        ];
        let res = obs.backend.scan_cols(
            obs::store::Table::Requests,
            from,
            now + 1,
            &cols,
            &mut |r| {
                n += 1;
                let amount = Amount {
                    requests: 1.0,
                    tokens: num(r, "total_tokens"),
                    usd: num(r, "cost_usd"),
                    tool_calls: 0.0,
                };
                self.limits
                    .replay(&subject(r), amount, obs::store::row_ts(r), now);
            },
        );
        let tool_cols = [
            "agent_id",
            "gateway_key_id",
            "user_id",
            "team_id",
            "project_id",
            "session_id",
            "server",
            "tool",
            "method",
        ];
        let res = res.and_then(|()| {
            obs.backend.scan_cols(
                obs::store::Table::Tools,
                from,
                now + 1,
                &tool_cols,
                &mut |r| {
                    if text(r, "method") != "tools/call" {
                        return;
                    }
                    n += 1;
                    let mut s = subject(r);
                    s.tool = format!("{}/{}", text(r, "server"), text(r, "tool"));
                    let amount = Amount {
                        tool_calls: 1.0,
                        ..Amount::default()
                    };
                    self.limits.replay(&s, amount, obs::store::row_ts(r), now);
                },
            )
        });
        match res {
            Ok(()) => tracing::info!(rows = n, "limit usage restored from the request log"),
            Err(e) => tracing::warn!(error = %e, "could not restore limit usage"),
        }
    }

    /// Price for a model: set over the API, else the config table.
    pub fn price(&self, model: &str) -> Option<obs::Price> {
        match &self.obs {
            Some(o) => o.price(model),
            None => obs::table_price(&self.cfg.obs.prices, model),
        }
    }

    pub(crate) fn subject(agent: &AgentInfo, upstream: &str, model: &str) -> Subject {
        Subject {
            agent: agent.id.clone(),
            key: agent.key_id.clone(),
            user: agent.user_id.clone().unwrap_or_default(),
            team: agent.team_id.clone().unwrap_or_default(),
            project: agent.project_id.clone().unwrap_or_default(),
            session: agent.session_id.clone().unwrap_or_default(),
            upstream: upstream.to_string(),
            model: model.to_string(),
            tool: String::new(),
        }
    }

    pub fn router(self: &Arc<Self>) -> Router {
        Router::new()
            .route("/gateway/health", get(|| async { "ok" }))
            .route("/gateway/stats", get(stats))
            .route("/gateway/servers", get(health::servers))
            .route(
                "/gateway/ops",
                get(|| async { axum::response::Html(health::OPS_PAGE) }),
            )
            .route("/gateway/ops/data", get(health::ops_data))
            .route("/gateway/reports", get(reports::list))
            .route("/gateway/reports/run", axum::routing::post(reports::run))
            .route("/gateway/reports/{file}", get(reports::file))
            .route("/gateway/metrics", get(metrics))
            .route("/gateway/agents", get(list_agents).post(create_agent))
            .route("/gateway/agents/{id}", delete(delete_agent))
            .route("/gateway/queue/{ticket}", get(proxy::ticket_status))
            .route("/gateway/limits", get(list_limits))
            .route(
                "/gateway/limits/{name}",
                put(put_limit).delete(delete_limit),
            )
            .merge(obs::http::routes())
            .merge(console::routes())
            .route("/mcp/{server}", any(mcp::handle))
            .merge(lead::http::routes())
            .route("/{upstream}/{*rest}", any(proxy::handle))
            .layer(DefaultBodyLimit::max(self.cfg.max_body_bytes))
            .with_state(self.clone())
    }

    fn agent_for(&self, headers: &HeaderMap) -> Option<Arc<AgentInfo>> {
        let token = headers
            .get("x-api-key")
            .and_then(|v| v.to_str().ok())
            .or_else(|| {
                headers
                    .get(http::header::AUTHORIZATION)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.strip_prefix("Bearer "))
            });
        if let Some(t) = token {
            let t = t.trim();
            // A stored agent is kept under the hash of its token; a token
            // shaped like such a name is never taken at its word.
            if !agents::is_key(t) {
                let agents = self.agents.read().unwrap();
                if let Some(a) = agents.get(t).or_else(|| agents.get(&agents::token_key(t))) {
                    return Some(a.clone());
                }
            }
        }
        self.cfg.allow_anonymous.then(|| {
            Arc::new(AgentInfo {
                id: "anonymous".into(),
                weight: 1,
                max_concurrency: None,
                token_budget: None,
                project_id: None,
                team_id: None,
                user_id: None,
                session_id: None,
                priority: None,
                kind: None,
                max_wait_ms: None,
                model_swap: false,
                key_id: "anonymous".into(),
                mcp_allow: None,
                mcp_deny: Vec::new(),
            })
        })
    }

    fn admin_ok(&self, headers: &HeaderMap) -> bool {
        let Some(want) = &self.cfg.admin_token else {
            return true;
        };
        let got = headers
            .get(http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .unwrap_or("");
        constant_eq(got.as_bytes(), want.as_bytes())
    }
}

/// The 429 an agent gets when a limit rule refuses its call.
pub(crate) fn limit_response(d: &Denied) -> Response {
    let mut r = error(
        StatusCode::TOO_MANY_REQUESTS,
        "gateway_limit_exceeded",
        d.message(),
    );
    let secs = d.retry_after().as_secs().max(1);
    let h = r.headers_mut();
    h.insert(http::header::RETRY_AFTER, HeaderValue::from(secs));
    if let Ok(v) = HeaderValue::from_str(&d.rule) {
        h.insert("x-gateway-limit", v);
    }
    r
}

pub(crate) fn constant_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

pub(crate) fn error(status: StatusCode, kind: &str, message: impl Into<String>) -> Response {
    let body = json!({
        "type": "error",
        "error": { "type": kind, "message": message.into(), "source": "pybox-gateway" }
    });
    let mut r = (status, Json(body)).into_response();
    // The caller's own budget or limit: retrying sooner will not help, and
    // neither will more servers.
    if matches!(
        kind,
        "gateway_budget_exceeded" | "gateway_limit_exceeded" | "gateway_loop_detected"
    ) {
        r.headers_mut().insert(
            "x-gateway-reject-reason",
            HeaderValue::from_static("caller_limit"),
        );
    }
    r
}

async fn stats(State(app): State<Arc<App>>, headers: HeaderMap) -> Response {
    if !app.admin_ok(&headers) {
        return error(
            StatusCode::UNAUTHORIZED,
            "authentication_error",
            "admin token required",
        );
    }
    let (agents, counters) = app.stats.snapshot();
    let mut ups = serde_json::Map::new();
    for (name, u) in &app.upstreams {
        ups.insert(
            name.clone(),
            json!({
                "scheduler": u.sched.snapshot(),
                "counters": counters.get(name).cloned().unwrap_or_default(),
            }),
        );
    }
    let obs = app.obs.as_ref().map(|o| o.info());
    let mut mcp = serde_json::Map::new();
    for (name, s) in &app.mcp {
        mcp.insert(name.clone(), json!({ "scheduler": s.sched.snapshot() }));
    }
    let cache = app.lead.as_ref().map(|l| l.cache_breaks());
    Json(json!({
        "upstreams": ups,
        "agents": agents,
        "obs": obs,
        "mcp": { "servers": mcp, "tools": app.stats.mcp_snapshot() },
        "prompt_cache": cache,
    }))
    .into_response()
}

#[derive(Deserialize)]
struct NewAgent {
    id: String,
    #[serde(default)]
    weight: Option<u32>,
    #[serde(default)]
    max_concurrency: Option<usize>,
    #[serde(default)]
    token_budget: Option<u64>,
    #[serde(default)]
    project_id: Option<String>,
    #[serde(default)]
    team_id: Option<String>,
    #[serde(default)]
    user_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    mcp_allow: Option<Vec<String>>,
    #[serde(default)]
    mcp_deny: Vec<String>,
    priority: Option<String>,
    #[serde(default)]
    kind: Option<String>,
    #[serde(default)]
    model_swap: bool,
}

/// Register an agent at runtime (e.g. when pybox starts a sandbox) and
/// return its token. The token is shown only here.
async fn create_agent(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Json(req): Json<NewAgent>,
) -> Response {
    if !app.admin_ok(&headers) {
        return error(
            StatusCode::UNAUTHORIZED,
            "authentication_error",
            "admin token required",
        );
    }
    if req.id.is_empty() || req.id == "anonymous" {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
            "invalid agent id",
        );
    }
    let bytes: [u8; 24] = rand::rng().random();
    let token = format!(
        "pgw_{}",
        bytes.iter().map(|b| format!("{b:02x}")).collect::<String>()
    );
    let info = Arc::new(AgentInfo {
        id: req.id.clone(),
        weight: req.weight.unwrap_or(1).max(1),
        max_concurrency: req.max_concurrency,
        token_budget: req.token_budget,
        project_id: req.project_id,
        team_id: req.team_id,
        user_id: req.user_id,
        session_id: req.session_id,
        priority: req.priority,
        kind: req.kind,
        max_wait_ms: None,
        model_swap: req.model_swap,
        key_id: key_id(&token),
        mcp_allow: req.mcp_allow,
        mcp_deny: req.mcp_deny,
    });
    app.agents
        .write()
        .unwrap()
        .insert(token.clone(), info.clone());
    app.save_agents();
    (
        StatusCode::CREATED,
        Json(json!({ "agent": *info, "token": token })),
    )
        .into_response()
}

async fn list_agents(State(app): State<Arc<App>>, headers: HeaderMap) -> Response {
    if !app.admin_ok(&headers) {
        return error(
            StatusCode::UNAUTHORIZED,
            "authentication_error",
            "admin token required",
        );
    }
    let mut list: Vec<AgentInfo> = app
        .agents
        .read()
        .unwrap()
        .values()
        .map(|a| (**a).clone())
        .collect();
    list.sort_by(|a, b| a.id.cmp(&b.id));
    Json(json!({ "agents": list })).into_response()
}

async fn delete_agent(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    if !app.admin_ok(&headers) {
        return error(
            StatusCode::UNAUTHORIZED,
            "authentication_error",
            "admin token required",
        );
    }
    let mut agents = app.agents.write().unwrap();
    let before = agents.len();
    agents.retain(|_, a| a.id != id);
    let removed = before - agents.len();
    drop(agents);
    if removed > 0 {
        app.save_agents();
    }
    if removed == 0 {
        return error(StatusCode::NOT_FOUND, "not_found_error", "unknown agent");
    }
    Json(json!({ "removed_tokens": removed })).into_response()
}

async fn list_limits(State(app): State<Arc<App>>, headers: HeaderMap) -> Response {
    if !app.admin_ok(&headers) {
        return error(
            StatusCode::UNAUTHORIZED,
            "authentication_error",
            "admin token required",
        );
    }
    Json(app.limits.snapshot(obs::now_ms())).into_response()
}

/// Add or replace a limit rule at runtime. Stored with the observability
/// metadata when a persistent store is configured.
async fn put_limit(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Path(name): Path<String>,
    Json(mut rule): Json<LimitRule>,
) -> Response {
    if !app.admin_ok(&headers) {
        return error(
            StatusCode::UNAUTHORIZED,
            "authentication_error",
            "admin token required",
        );
    }
    rule.name = name;
    if let Err(e) = app.limits.put(rule.clone()) {
        return error(StatusCode::BAD_REQUEST, "invalid_request_error", e);
    }
    if let Some(obs) = &app.obs {
        let stored = serde_json::to_value(&rule).unwrap_or_default();
        if let Err(e) = obs.backend.meta_put("limit", &rule.name, stored) {
            tracing::warn!(error = %e, "limit rule not persisted");
        }
    }
    Json(json!({ "limit": rule })).into_response()
}

async fn delete_limit(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Response {
    if !app.admin_ok(&headers) {
        return error(
            StatusCode::UNAUTHORIZED,
            "authentication_error",
            "admin token required",
        );
    }
    let removed = app.limits.remove(&name);
    if let Some(obs) = &app.obs {
        let _ = obs.backend.meta_delete("limit", &name);
    }
    if !removed {
        return error(StatusCode::NOT_FOUND, "not_found_error", "unknown limit");
    }
    Json(json!({ "removed": name })).into_response()
}

/// Prometheus scrape endpoint: GenAI-convention histograms and counters plus
/// the gateway's current queue and concurrency.
async fn metrics(State(app): State<Arc<App>>, headers: HeaderMap) -> Response {
    if !app.admin_ok(&headers) {
        return error(
            StatusCode::UNAUTHORIZED,
            "authentication_error",
            "admin token required",
        );
    }
    let Some(obs) = &app.obs else {
        return error(
            StatusCode::NOT_FOUND,
            "not_found_error",
            "metrics need [obs] enabled = true",
        );
    };
    let mut gauges = Vec::new();
    let mut names: Vec<&String> = app.upstreams.keys().collect();
    names.sort();
    let snaps: Vec<(&String, scheduler::SchedSnapshot)> = names
        .into_iter()
        .map(|n| (n, app.upstreams[n].sched.snapshot()))
        .collect();
    type Pick = fn(&scheduler::SchedSnapshot) -> f64;
    let series: [(&str, &str, Pick); 4] = [
        (
            "pybox_gateway_inflight",
            "Upstream requests in flight.",
            |s| s.inflight as f64,
        ),
        (
            "pybox_gateway_queued",
            "Requests waiting for an upstream slot.",
            |s| s.queued as f64,
        ),
        (
            "pybox_gateway_max_concurrency",
            "Configured upstream concurrency.",
            |s| s.max_concurrency as f64,
        ),
        (
            "pybox_gateway_cooldown_seconds",
            "Remaining shared pause after a rate-limit response.",
            |s| s.cooldown_ms as f64 / 1000.0,
        ),
    ];
    for (name, help, pick) in series {
        for (up, snap) in &snaps {
            gauges.push((name, help, vec![("upstream", (*up).clone())], pick(snap)));
        }
    }
    for (rule, n) in app.limits.denied_counts() {
        gauges.push((
            "pybox_gateway_limit_denied",
            "Calls refused by a limit rule since start.",
            vec![("rule", rule)],
            n as f64,
        ));
    }
    (
        [(
            http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        obs.metrics.render(&gauges),
    )
        .into_response()
}
