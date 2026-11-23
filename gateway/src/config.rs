//! TOML configuration. See `gateway.example.toml` for an annotated example.

use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{bail, Context, Result};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Listen address. Keep this on loopback; the gateway holds real API keys.
    #[serde(default = "default_listen")]
    pub listen: String,
    /// Bearer token for `/gateway/*` admin endpoints. `None` leaves them open to
    /// anything that can reach the listen address.
    #[serde(default)]
    pub admin_token: Option<String>,
    /// Accept requests without a known agent token (tracked as agent `anonymous`).
    #[serde(default)]
    pub allow_anonymous: bool,
    /// Largest accepted request body.
    #[serde(default = "default_max_body")]
    pub max_body_bytes: usize,
    #[serde(default)]
    pub retry: RetryConfig,
    #[serde(default)]
    pub cache: CacheConfig,
    #[serde(default)]
    pub loop_guard: LoopGuardConfig,
    pub upstreams: Vec<UpstreamConfig>,
    #[serde(default)]
    pub agents: Vec<AgentConfig>,
    /// Request logs, traces and the dashboard.
    #[serde(default)]
    pub obs: crate::obs::ObsConfig,
    /// Request, token, dollar and tool-call limits per scope and window.
    #[serde(default)]
    pub limits: Vec<crate::limits::LimitRule>,
    /// MCP servers reachable at `/mcp/<name>`.
    #[serde(default)]
    pub mcp_servers: Vec<McpServerConfig>,
    /// Run tracking, cache diagnosis and the cost ledger.
    #[serde(default)]
    pub lead: crate::lead::engine::LeadConfig,
    /// Reports the gateway writes by itself every day (off when unset).
    #[serde(default)]
    pub reports: Option<crate::reports::ReportsConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpServerConfig {
    /// Agents use `http://127.0.0.1:8787/mcp/<name>` as the server URL.
    pub name: String,
    /// The server's Streamable HTTP endpoint.
    pub url: String,
    /// Sent as `Authorization: Bearer`; the agent never sees it.
    #[serde(default)]
    pub api_key: Option<String>,
    #[serde(default)]
    pub api_key_env: Option<String>,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    /// `tools/call` requests in flight at once, across all agents.
    #[serde(default = "default_concurrency")]
    pub max_concurrency: usize,
    /// Tool calls started per minute. Unset = unlimited.
    #[serde(default)]
    pub rpm: Option<u32>,
    /// Give up on a call that has produced nothing for this long.
    #[serde(default = "default_call_timeout")]
    pub call_timeout_secs: u64,
    /// Largest tool result passed back to an agent; bigger results are
    /// replaced by a tool error so they cannot flood the model's context.
    #[serde(default = "default_result_bytes")]
    pub max_result_bytes: usize,
    /// Tools agents may see and call (`*` wildcard). Unset = all.
    #[serde(default)]
    pub allow_tools: Option<Vec<String>>,
    /// Tools hidden and refused even when allowed above.
    #[serde(default)]
    pub deny_tools: Vec<String>,
}

fn default_call_timeout() -> u64 {
    120
}
fn default_result_bytes() -> usize {
    1 << 20
}

impl McpServerConfig {
    pub fn resolve_key(&self) -> Result<Option<String>> {
        if let Some(var) = &self.api_key_env {
            return match std::env::var(var) {
                Ok(v) if !v.is_empty() => Ok(Some(v)),
                _ => bail!(
                    "mcp server {:?}: environment variable {var} is not set",
                    self.name
                ),
            };
        }
        Ok(self.api_key.clone())
    }
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    /// Injects `x-api-key`.
    Anthropic,
    /// Injects `Authorization: Bearer`.
    Openai,
    /// A LiteLLM proxy: `Authorization: Bearer <virtual key>`, plus trace
    /// context and attribution headers so LiteLLM's own logs line up.
    Litellm,
    /// Injects nothing (e.g. a local model server).
    None,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpstreamConfig {
    /// First path segment agents use: `http://127.0.0.1:8787/<name>/v1/...`.
    pub name: String,
    pub base_url: String,
    pub kind: Kind,
    #[serde(default)]
    pub api_key: Option<String>,
    /// Environment variable holding the key (preferred over `api_key`).
    #[serde(default)]
    pub api_key_env: Option<String>,
    /// Upstream requests in flight at once, across all agents.
    #[serde(default = "default_concurrency")]
    pub max_concurrency: usize,
    /// Requests per minute (token bucket). Unset = unlimited.
    #[serde(default)]
    pub rpm: Option<u32>,
    /// Estimated input tokens per minute (token bucket). Unset = unlimited.
    #[serde(default)]
    pub input_tpm: Option<u32>,
    /// Requests allowed to wait for a slot. Beyond this the gateway answers
    /// 503 at once instead of queueing without bound. Unset = unbounded.
    #[serde(default)]
    pub max_queue: Option<usize>,
    /// Longest a request may wait for a slot before it gets a 503.
    #[serde(default)]
    pub queue_timeout_secs: Option<u64>,
    /// What a full queue does with a new request: `refuse` it, or `evict`
    /// the newest waiter of the lowest-weight agent when the newcomer's agent
    /// weighs more, so bulk work is pushed out before interactive work.
    #[serde(default = "refuse")]
    pub overflow: String,
    /// Refuse a request at once (503 with the expected wait) when it would
    /// wait longer than `queue_timeout_secs` or its own
    /// `x-gateway-max-wait-ms`, instead of letting it wait and time out.
    #[serde(default)]
    pub admit_by_wait: bool,
    /// Serve the highest-weight agents that have work first. Lower weights
    /// are not refused, they wait for the slots that are left.
    #[serde(default)]
    pub strict_priority: bool,
    /// Under `strict_priority`, the share of seats that still goes to less
    /// important work while it is waiting (0 to 0.5), so a steady stream of
    /// interactive requests cannot keep a batch waiting for ever.
    #[serde(default)]
    pub min_share: f64,
    /// Seats that less important work may not take while more important
    /// work has arrived in the last 30 s. A running request is never
    /// interrupted, so without this an interactive request can wait for a
    /// whole batch request to finish.
    #[serde(default)]
    pub reserve_slots: usize,
    /// Most request-body bytes allowed to wait for a slot. Unset = unbounded.
    #[serde(default)]
    pub max_queue_bytes: Option<usize>,
    /// Sets of models that may stand in for one another. A request from an
    /// agent with `model_swap = true` whose own model has no free server is
    /// sent to a free server of another model in its set, with the model
    /// name rewritten. The answer comes from a different model.
    #[serde(default)]
    pub model_groups: Vec<Vec<String>>,
    /// With `adaptive`: lower a server's limit when first tokens take this
    /// many times longer than at the lowest load seen. 0 = off.
    #[serde(default)]
    pub adaptive_slowdown: f64,
    /// `gen_ai.provider.name` reported for this upstream (e.g. `anthropic`,
    /// `openai`, `gcp.gemini`). Guessed from the kind and model when unset.
    #[serde(default)]
    pub provider: Option<String>,
    /// Abort a response if the upstream sends nothing for this long.
    #[serde(default = "default_idle")]
    pub idle_timeout_secs: u64,
    /// Give up on an upstream request that has not started answering
    /// (response headers) after this long, count it against the server and
    /// try elsewhere. Unset = only `idle_timeout_secs` applies. Set it above
    /// the longest time to first token you expect: a model server keeps
    /// working on a request the gateway has walked away from.
    #[serde(default)]
    pub first_byte_timeout_secs: Option<u64>,
    /// Extra headers added to every upstream request.
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    /// Label grouping several upstream entries (LiteLLM servers) in reports.
    /// Defaults to the upstream name.
    #[serde(default)]
    pub cluster: Option<String>,
    /// Name of the key this upstream authenticates with, for per-key reports
    /// (e.g. the LiteLLM virtual key alias). Never the key itself.
    #[serde(default)]
    pub key_alias: Option<String>,
    /// How waiting requests are ordered: `round_robin` (by request count
    /// per agent) or `fair` (by tokens per team, shortest-served run first,
    /// cache-aware, with routing over `backends`).
    #[serde(default = "round_robin")]
    pub policy: String,
    /// Several servers that serve this upstream (LiteLLM instances, model
    /// servers). Requests go to the least loaded one that serves the model,
    /// preferring the one that holds the run's cache.
    #[serde(default)]
    pub backends: Vec<BackendConfig>,
    /// Find each backend's best concurrency from observed throughput,
    /// between 1 and its `max_concurrency`. Needs the `fair` policy.
    #[serde(default)]
    pub adaptive: bool,
    /// How much busier (as a share of its limit) the run's previous backend
    /// may be than the least loaded one before the run moves.
    #[serde(default = "default_spill")]
    pub spill: f64,
    /// Which server counts as warm for a request when there are several
    /// `backends`: `prefix` (the one whose prefix cache holds the longest
    /// start of the prompt, whoever sent it), `run` (the one that answered
    /// the run's previous turn; needs `[lead]`) or `off` (load only).
    #[serde(default = "default_affinity")]
    pub affinity: String,
    /// With `affinity = "prefix"`: ignore shared prompt starts shorter than
    /// this many (estimated) tokens.
    #[serde(default = "default_affinity_min_tokens")]
    pub affinity_min_tokens: u32,
    /// With `affinity = "prefix"`: a request whose prompt is mostly held by
    /// a server that is full waits up to this long for it instead of going
    /// to a free server that would read the whole prompt again. 0 = never
    /// wait. Worth setting when reading the prompt takes longer than a
    /// typical answer, as on servers without a GPU.
    #[serde(default)]
    pub affinity_wait_ms: u64,
    /// With `affinity = "prefix"`: assume a server has dropped a prompt it
    /// has not been sent for this long. 0 = only newer prompts push it out.
    #[serde(default)]
    pub prefix_ttl_secs: u64,
    /// How a server is chosen among those with room: `load` (least load,
    /// with a tolerance of `spill` for servers holding the prompt) or
    /// `cost` (the least prompt tokens left to read plus `load_weight`
    /// times the prompt tokens the server already has in flight per seat).
    #[serde(default = "default_route_by")]
    pub route_by: String,
    /// With `route_by = "cost"`: how much a token in flight on a server
    /// counts against a token this request would have to read there.
    #[serde(default = "default_load_weight")]
    pub load_weight: f64,
    /// Session admission. A request whose prompt no server holds would push
    /// another prompt out of a server's cache; while every prompt it could
    /// push out was used within this many ms, it waits instead (the
    /// sessions behind those prompts are still running). 0 = off.
    #[serde(default)]
    pub slot_hold_ms: u64,
    /// ...but never longer than this.
    #[serde(default = "default_slot_max_wait")]
    pub slot_max_wait_ms: u64,
    /// Batch requests (`priority = "batch"`) of one caller are served kind
    /// by kind: one that would start cold lets later ones whose prompt a
    /// free server holds go first, until it has waited this many ms. 0 =
    /// arrival order.
    #[serde(default)]
    pub batch_group_ms: u64,
    /// A server with requests in flight and none finished for this long is
    /// treated as full until one finishes. 0 = off.
    #[serde(default)]
    pub stale_after_secs: u64,
    /// Refusing in stages (needs `max_queue`). The least important work
    /// starts to be refused when this share of the queue is taken, more
    /// often the fuller it gets; the most important work only when the
    /// queue is full. Unset = everything is refused only when full.
    #[serde(default)]
    pub shed_from: Option<f64>,
    /// ...and from this share on, the least important work is always
    /// refused (but see `shed_floor`).
    #[serde(default = "default_shed_ceiling")]
    pub shed_ceiling: f64,
    /// Work is never refused early while requests of its importance or less
    /// hold fewer than this share of the queue's places, so batch work is
    /// thinned out under overload but not shut out.
    #[serde(default = "default_shed_floor")]
    pub shed_floor: f64,
    /// Least used first: callers (teams under `policy = "fair"`, agents
    /// otherwise) are ordered by the server time their requests held
    /// lately, per weight, where use this many seconds old counts half.
    /// 0 = take turns. 60 or more is sensible; much shorter forgets too
    /// fast to tell a heavy caller from a light one.
    #[serde(default)]
    pub least_used_half_life_secs: u64,
    /// Ask every server behind this upstream whether it is alive every this
    /// many seconds (0 = never; health is then learned from requests
    /// alone). A server that does not answer is taken out before a caller's
    /// request finds out, and one that is out comes back as soon as a probe
    /// is answered instead of when a request is risked on it.
    #[serde(default)]
    pub health_probe_secs: u64,
    /// Path the probe asks for, after the server's base URL. Any answer
    /// below 500 counts as alive. Unset = `/health/liveliness` for
    /// `kind = "litellm"`, `/v1/models` otherwise.
    #[serde(default)]
    pub health_probe_path: Option<String>,
    /// While every server that could serve a request is out, answer 503
    /// `gateway_servers_down` at once instead of letting it wait in the
    /// queue for one to come back. Requests already waiting stay.
    #[serde(default)]
    pub refuse_when_down: bool,
    /// Hold batch work instead of refusing it. Requests with
    /// `priority = "batch"` take no place in the queue: `max_queue`,
    /// `max_queue_bytes`, staged refusal, eviction, `admit_by_wait` and
    /// `queue_timeout_secs` pass them by. They are sent to a server only
    /// when no other request that could run is waiting, and the caller's
    /// connection stays open meanwhile. Other work keeps its short queue
    /// and is still refused when that is full.
    #[serde(default)]
    pub hold_batch: bool,
    /// Most requests held at once; one more is refused
    /// (`gateway_hold_full`). Each is an open connection.
    #[serde(default = "default_hold_max")]
    pub hold_max: usize,
    /// Most request-body bytes held at once: what holding may cost in
    /// memory.
    #[serde(default = "default_hold_max_bytes")]
    pub hold_max_bytes: usize,
    /// Longest a request is held before it gets a 503
    /// (`gateway_hold_timeout`). A caller's own `x-gateway-max-wait-ms` or
    /// the agent's `max_wait_ms` shortens it.
    #[serde(default = "default_hold_timeout_secs")]
    pub hold_timeout_secs: u64,
    /// Tell streaming callers while they wait for a slot: `sse` (comment
    /// lines, which SSE clients skip) or `event` (`event: gateway.queue`
    /// with JSON). `off` = only callers that ask with
    /// `x-gateway-queue-notice` are told. The header wins either way.
    #[serde(default = "default_queue_notice")]
    pub queue_notice: String,
}

fn default_queue_notice() -> String {
    "off".into()
}

fn default_hold_max() -> usize {
    10_000
}
fn default_hold_max_bytes() -> usize {
    256 << 20
}
fn default_hold_timeout_secs() -> u64 {
    900
}

fn default_shed_ceiling() -> f64 {
    0.75
}
fn default_shed_floor() -> f64 {
    0.1
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackendConfig {
    pub name: String,
    pub base_url: String,
    #[serde(default = "default_concurrency")]
    pub max_concurrency: usize,
    /// Overrides the upstream's `adaptive`.
    #[serde(default)]
    pub adaptive: Option<bool>,
    /// Models this backend serves; empty means all.
    #[serde(default)]
    pub models: Vec<String>,
    /// How many prompts the server keeps in its prefix (KV) cache: the
    /// number of slots of a llama.cpp server, more for servers that share
    /// cached blocks between requests. Defaults to `max_concurrency`.
    #[serde(default)]
    pub prefix_slots: Option<usize>,
}

fn round_robin() -> String {
    "round_robin".into()
}

fn default_spill() -> f64 {
    0.5
}

fn default_affinity() -> String {
    "prefix".into()
}

fn default_affinity_min_tokens() -> u32 {
    64
}

fn default_route_by() -> String {
    "load".into()
}

fn default_load_weight() -> f64 {
    1.0
}

fn default_slot_max_wait() -> u64 {
    10_000
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentConfig {
    pub id: String,
    pub token: String,
    #[serde(default = "one")]
    pub weight: u32,
    #[serde(default)]
    pub max_concurrency: Option<usize>,
    /// Total input+output tokens this agent may consume before getting 429s.
    #[serde(default)]
    pub token_budget: Option<u64>,
    /// Who the agent's spend belongs to. Bound to the token, never taken
    /// from request headers.
    #[serde(default)]
    pub project_id: Option<String>,
    #[serde(default)]
    pub team_id: Option<String>,
    #[serde(default)]
    pub user_id: Option<String>,
    #[serde(default)]
    pub session_id: Option<String>,
    /// MCP tools this agent may use, as `server/tool` patterns (`*`
    /// wildcard). Unset = whatever each server allows.
    #[serde(default)]
    pub mcp_allow: Option<Vec<String>>,
    #[serde(default)]
    pub mcp_deny: Vec<String>,
    /// `interactive` or `batch`: orders this agent's requests within its
    /// team under the `fair` policy. A request can also say so itself with
    /// `x-gateway-priority`.
    #[serde(default)]
    pub priority: Option<String>,
    /// What this agent's requests are, in the operator's own words (`ide`,
    /// `chat`, `batch`, `review`, ...). Written to the request log as
    /// `request_kind` so usage reports and simulator replays can tell kinds
    /// of work apart; scheduling does not look at it. A request can also say
    /// so itself with `x-gateway-request-kind`, which wins.
    #[serde(default)]
    pub kind: Option<String>,
    /// How long this agent's callers can wait for a seat (ms). With
    /// `admit_by_wait` a request expected to wait longer is refused at the
    /// door instead of waiting and timing out. A request can also say so
    /// itself with `x-gateway-max-wait-ms`, which wins.
    #[serde(default)]
    pub max_wait_ms: Option<u64>,
    /// Let the gateway send this agent's requests to a stand-in model from
    /// the upstream's `model_groups` when its own model's servers are full.
    #[serde(default)]
    pub model_swap: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct RetryConfig {
    pub max_retries: u32,
    pub base_ms: u64,
    pub max_ms: u64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            base_ms: 500,
            max_ms: 30_000,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct CacheConfig {
    /// 0 disables the response cache and request coalescing entirely.
    pub ttl_secs: u64,
    pub max_entries: usize,
    pub max_body_bytes: usize,
    /// Cache every eligible request, not only those sending `x-gateway-cache: on`.
    pub default_on: bool,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            ttl_secs: 300,
            max_entries: 1024,
            max_body_bytes: 8 << 20,
            default_on: false,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct LoopGuardConfig {
    /// Refuse an agent's request once it has sent the same one this many
    /// times in a row. 0 turns the guard off.
    pub max_repeats: u32,
    /// A pause this long between identical requests resets the count.
    pub window_secs: u64,
}

impl Default for LoopGuardConfig {
    fn default() -> Self {
        Self {
            max_repeats: 0,
            window_secs: 300,
        }
    }
}

fn default_listen() -> String {
    "127.0.0.1:8787".into()
}
fn default_max_body() -> usize {
    32 << 20
}
fn default_concurrency() -> usize {
    8
}
fn refuse() -> String {
    "refuse".into()
}

fn default_idle() -> u64 {
    300
}
fn one() -> u32 {
    1
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let text =
            std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        let cfg: Config = toml::from_str(&text).context("parsing config")?;
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn validate(&self) -> Result<()> {
        if self.upstreams.is_empty() {
            bail!("at least one [[upstreams]] entry is required");
        }
        if let Some(r) = &self.reports {
            if let Err(e) = r.validate() {
                bail!("{e}");
            }
        }
        let mut names = std::collections::HashSet::new();
        for u in &self.upstreams {
            if u.health_probe_path
                .as_ref()
                .is_some_and(|p| !p.starts_with('/'))
            {
                bail!("upstream {:?}: health_probe_path must start with /", u.name);
            }
            if u.name.is_empty() || u.name.contains('/') || u.name == "gateway" || u.name == "mcp" {
                bail!("invalid upstream name {:?}", u.name);
            }
            if !names.insert(&u.name) {
                bail!("duplicate upstream {:?}", u.name);
            }
            if u.max_concurrency == 0 {
                bail!("upstream {:?}: max_concurrency must be > 0", u.name);
            }
            if let Some(from) = u.shed_from {
                if u.max_queue.is_none() {
                    bail!("upstream {:?}: shed_from needs max_queue", u.name);
                }
                let ok = (0.0..1.0).contains(&from)
                    && u.shed_ceiling > from
                    && u.shed_ceiling <= 1.0
                    && (0.0..=1.0).contains(&u.shed_floor);
                if !ok {
                    bail!(
                        "upstream {:?}: need 0 <= shed_from < shed_ceiling <= 1 and shed_floor in 0..1",
                        u.name
                    );
                }
            }
            if !matches!(u.queue_notice.as_str(), "off" | "sse" | "event") {
                bail!(
                    "upstream {:?}: unknown queue_notice {:?} (off | sse | event)",
                    u.name,
                    u.queue_notice
                );
            }
            if u.hold_batch && (u.hold_max == 0 || u.hold_timeout_secs == 0) {
                bail!(
                    "upstream {:?}: hold_batch needs hold_max and hold_timeout_secs above 0",
                    u.name
                );
            }
            if !matches!(u.overflow.as_str(), "refuse" | "evict") {
                bail!(
                    "upstream {:?}: unknown overflow {:?} (refuse | evict)",
                    u.name,
                    u.overflow
                );
            }
            if !matches!(u.policy.as_str(), "round_robin" | "fair") {
                bail!(
                    "upstream {:?}: unknown policy {:?} (round_robin | fair)",
                    u.name,
                    u.policy
                );
            }
            if !(0.0..=0.5).contains(&u.min_share) {
                bail!("upstream {:?}: min_share must be between 0 and 0.5", u.name);
            }
            if u.adaptive_slowdown != 0.0 && u.adaptive_slowdown < 1.5 {
                bail!(
                    "upstream {:?}: adaptive_slowdown must be 0 (off) or at least 1.5",
                    u.name
                );
            }
            if crate::lead::prefix::Affinity::parse(&u.affinity).is_none() {
                bail!(
                    "upstream {:?}: affinity must be \"prefix\", \"run\" or \"off\"",
                    u.name
                );
            }
            if !matches!(u.route_by.as_str(), "load" | "cost") {
                bail!(
                    "upstream {:?}: route_by must be \"load\" or \"cost\"",
                    u.name
                );
            }
            if !(0.0..=1000.0).contains(&u.load_weight) {
                bail!(
                    "upstream {:?}: load_weight must be between 0 and 1000",
                    u.name
                );
            }
            if !(0.0..=10.0).contains(&u.spill) {
                bail!("upstream {:?}: spill must be between 0 and 10", u.name);
            }
            let mut seen = std::collections::HashSet::new();
            for b in &u.backends {
                if b.name.is_empty() || !seen.insert(&b.name) {
                    bail!(
                        "upstream {:?}: backend names must be unique and not empty",
                        u.name
                    );
                }
                if b.max_concurrency == 0 || b.base_url.is_empty() {
                    bail!(
                        "upstream {:?}: backend {:?} needs base_url and max_concurrency > 0",
                        u.name,
                        b.name
                    );
                }
            }
        }
        if let Err(e) = self.lead.mode() {
            bail!("{e}");
        }
        let mut servers = std::collections::HashSet::new();
        for m in &self.mcp_servers {
            if m.name.is_empty() || m.name.contains('/') {
                bail!("invalid mcp server name {:?}", m.name);
            }
            if !servers.insert(&m.name) {
                bail!("duplicate mcp server {:?}", m.name);
            }
            if m.max_concurrency == 0 {
                bail!("mcp server {:?}: max_concurrency must be > 0", m.name);
            }
            if !(m.url.starts_with("http://") || m.url.starts_with("https://")) {
                bail!("mcp server {:?}: url must be http(s)", m.name);
            }
        }
        let mut limit_names = std::collections::HashSet::new();
        for l in &self.limits {
            let mut l = l.clone();
            l.validate().map_err(anyhow::Error::msg)?;
            if !limit_names.insert(l.name.clone()) {
                bail!("duplicate limit name {:?}", l.name);
            }
        }
        let mut tokens = std::collections::HashSet::new();
        for a in &self.agents {
            if a.token.len() < 8 {
                bail!("agent {:?}: token must be at least 8 characters", a.id);
            }
            if !tokens.insert(&a.token) {
                bail!("agent {:?}: duplicate token", a.id);
            }
        }
        Ok(())
    }
}

impl UpstreamConfig {
    pub fn is_fair(&self) -> bool {
        self.policy == "fair"
    }

    pub fn resolve_key(&self) -> Result<Option<String>> {
        if let Some(var) = &self.api_key_env {
            return match std::env::var(var) {
                Ok(v) if !v.is_empty() => Ok(Some(v)),
                _ => bail!(
                    "upstream {:?}: environment variable {var} is not set",
                    self.name
                ),
            };
        }
        Ok(self.api_key.clone())
    }
}
