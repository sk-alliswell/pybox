//! What a simulation run is made of: hosts, the components on them, the links
//! between them and the clients that drive load. Scenarios are written in
//! TOML (or JSON); every field has a default so a small file is enough.
//!
//! Times are in the unit the field name says (`_ms`, `_s`); rates are per
//! second unless the name says otherwise.

use serde::{Deserialize, Serialize};

fn one() -> usize {
    1
}
fn is_one(n: &usize) -> bool {
    *n == 1
}
fn yes() -> bool {
    true
}
fn f1() -> f64 {
    1.0
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scenario {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub description: String,
    /// The description in Korean, shown by the monitor.
    #[serde(default)]
    pub description_ko: String,
    #[serde(default = "default_seed")]
    pub seed: u64,
    /// Output tokens a model server writes per simulation event after the
    /// first one. 1 follows every token; a larger stride is the same answer
    /// in fewer events, with a caller's leaving noticed and the server's
    /// speed set once a stride. For runs with millions of requests.
    #[serde(default = "default_stride")]
    pub token_stride: u32,
    /// Hard stop. Requests still open then are reported as unfinished.
    #[serde(default = "default_max_s")]
    pub max_s: f64,
    /// How often state is sampled for time series and the live monitor.
    #[serde(default = "default_sample_ms")]
    pub sample_ms: f64,
    #[serde(default)]
    pub net: Net,
    #[serde(default, rename = "host")]
    pub hosts: Vec<Host>,
    #[serde(default)]
    pub gateway: Option<Gateway>,
    #[serde(default, rename = "proxy")]
    pub proxies: Vec<Proxy>,
    #[serde(default, rename = "model_server")]
    pub model_servers: Vec<ModelServer>,
    #[serde(default, rename = "hosted_api")]
    pub hosted_apis: Vec<HostedApi>,
    #[serde(default, rename = "mcp_server")]
    pub mcp_servers: Vec<McpServer>,
    #[serde(default)]
    pub db: Option<Db>,
    #[serde(default, rename = "client")]
    pub clients: Vec<Client>,
    /// Windows that summaries are split by (e.g. warm-up, flood, recovery).
    #[serde(default, rename = "phase")]
    pub phases: Vec<Phase>,
}

fn default_hold_max() -> usize {
    10_000
}
fn default_hold_timeout_s() -> f64 {
    900.0
}
fn default_seed() -> u64 {
    11
}
fn default_stride() -> u32 {
    1
}
fn default_max_s() -> f64 {
    3600.0
}
fn default_sample_ms() -> f64 {
    1000.0
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Phase {
    pub name: String,
    pub from_s: f64,
    pub to_s: f64,
}

/// Latency of one hop between two components.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Net {
    /// One-way latency of a hop that has no `link` entry.
    #[serde(default = "default_hop_ms")]
    pub default_ms: f64,
    #[serde(default, rename = "link")]
    pub links: Vec<Link>,
}

fn default_hop_ms() -> f64 {
    0.2
}

impl Default for Net {
    fn default() -> Self {
        Self {
            default_ms: default_hop_ms(),
            links: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Link {
    /// Component names; `clients` stands for every client.
    pub from: String,
    pub to: String,
    pub latency_ms: f64,
    /// Uniform extra delay in [0, jitter_ms).
    #[serde(default)]
    pub jitter_ms: f64,
}

/// A machine whose cores are shared by the components placed on it. When the
/// threads that want to run exceed the cores, everything on it slows down in
/// proportion.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Host {
    pub name: String,
    pub cores: f64,
    /// Threads that are always busy on this host and are not modelled as a
    /// component (a load generator, another service).
    #[serde(default)]
    pub background_threads: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Gateway {
    #[serde(default = "gateway_name")]
    pub name: String,
    #[serde(default)]
    pub host: Option<String>,
    /// Gateway's own processing per request.
    #[serde(default = "default_gw_overhead")]
    pub overhead_ms: f64,
    /// Times a 429 or connect failure is retried before it reaches the client.
    #[serde(default = "default_gw_retries")]
    pub max_retries: u32,
    #[serde(default)]
    pub memory: GatewayMemory,
    /// Identical gateway processes behind a load balancer. They share
    /// nothing: every caller is pinned to one of them (in turn, by its
    /// position in the client list), and each has its own queue and an
    /// equal share of every upstream's `max_concurrency` and `max_queue`.
    /// An assumption: the real gateway has only been run as one process.
    #[serde(default = "one", skip_serializing_if = "is_one")]
    pub instances: usize,
    /// CPU cores of one instance (0 = its CPU is not modelled).
    #[serde(default)]
    pub cores: f64,
    /// CPU one request costs an instance before it joins the queue. With
    /// `cores`, requests wait for a free core in arrival order.
    #[serde(default)]
    pub cpu_ms_per_request: f64,
    /// Response cache in front of the queue.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache: Option<Cache>,
    #[serde(rename = "upstream")]
    pub upstreams: Vec<Upstream>,
}

fn gateway_name() -> String {
    "gateway".into()
}
fn default_gw_overhead() -> f64 {
    0.03
}
fn default_gw_retries() -> u32 {
    2
}

/// A response cache: a share of requests is answered from it, whole, after
/// a lookup, and never reaches a queue or a server. Which requests repeat is
/// not modelled; the share is given.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Cache {
    #[serde(default = "cache_name")]
    pub name: String,
    /// Share of requests answered from the cache (0 to 1).
    pub hit_rate: f64,
    #[serde(default = "f1")]
    pub lookup_ms: f64,
}

fn cache_name() -> String {
    "cache".into()
}

/// Resident memory of the gateway process as a linear model.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GatewayMemory {
    #[serde(default = "default_mem_base")]
    pub base_mb: f64,
    /// Held while a request waits or runs.
    #[serde(default = "default_mem_open")]
    pub per_open_request_kb: f64,
    /// Kept for every request seen when logs stay in memory (`[obs] backend
    /// = "memory"`); zero when they go to the store.
    #[serde(default = "default_mem_logged")]
    pub per_logged_request_kb: f64,
}

fn default_mem_base() -> f64 {
    10.0
}
fn default_mem_open() -> f64 {
    60.0
}
fn default_mem_logged() -> f64 {
    45.0
}

impl Default for GatewayMemory {
    fn default() -> Self {
        Self {
            base_mb: default_mem_base(),
            per_open_request_kb: default_mem_open(),
            per_logged_request_kb: default_mem_logged(),
        }
    }
}

/// One `[[upstreams]]` entry of the gateway: a concurrency cap, a queue and
/// the servers behind it. Field names follow the gateway's own config.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Upstream {
    pub name: String,
    /// Models this upstream serves. Empty: any model no other upstream lists.
    #[serde(default)]
    pub models: Vec<String>,
    /// Proxies or backends requests are sent to.
    pub targets: Vec<String>,
    /// How a target is chosen when there are several.
    #[serde(default)]
    pub routing: Routing,
    /// What a turn costs in the order of waiting agents: `round_robin`
    /// (the gateway's default: every request counts the same) or `fair`
    /// (a request counts its prompt tokens).
    #[serde(default)]
    pub policy: Policy,
    /// With `affinity`: a prefix gets one more target for every this many
    /// of its requests that are open at once, counted in targets' worth of
    /// slots (1.0 = one more when it alone would fill a target). 0 = one
    /// target only; when that one is full, requests go wherever there is
    /// most room.
    #[serde(default)]
    pub replicate_at: f64,
    /// With `cost`: how much the work already at a target counts against
    /// what the target would have to read of this prompt.
    #[serde(default = "always")]
    pub cost_load_weight: f64,
    /// With `cost`: count tokens (a prompt token and an output token weigh
    /// the same) instead of the time they take.
    #[serde(default)]
    pub cost_in_tokens: bool,
    /// With `prefix`: prompts the gateway believes each target keeps
    /// (its `prefix_slots`). Unset = as many as the target runs at once,
    /// which is the gateway's own default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix_slots: Option<usize>,
    /// With `prefix`: how much busier (share of its seats) a target that
    /// holds the whole prompt may be than the least loaded and still be
    /// chosen.
    #[serde(default = "default_spill")]
    pub spill: f64,
    /// With `prefix`: how long a request waits for the full target that
    /// holds at least half of its prompt while another target is free
    /// (the gateway's `affinity_wait_ms`). 0 = never.
    #[serde(default)]
    pub affinity_wait_s: f64,
    /// Requests sent on at once. May be left out when `headroom` is given.
    #[serde(default)]
    pub max_concurrency: usize,
    /// Keep the servers behind this upstream from being handed more than
    /// they can run: `max_concurrency` becomes this share of their slots
    /// (1.0 = exactly as many as they have, 0.9 = a tenth kept free).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub headroom: Option<f64>,
    #[serde(default)]
    pub max_queue: Option<usize>,
    #[serde(default)]
    pub queue_timeout_s: Option<f64>,
    #[serde(default)]
    pub overflow: Overflow,
    #[serde(default)]
    pub admit_by_wait: bool,
    #[serde(default)]
    pub strict_priority: bool,
    /// Start refusing before the queue is full, lower weights first (the
    /// live gateway's `shed_from`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shed: Option<Shed>,
    /// The live gateway's `hold_batch`, by weight where the gateway goes
    /// by the class "batch": requests of this weight or lower are held,
    /// not refused. They do not count against `max_queue`; staged refusal,
    /// the wait limit and `queue_timeout_s` pass them by, and they are
    /// sent on when there is room (use with `strict_priority` to send
    /// them only when nothing heavier waits, as the gateway does).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hold_weight: Option<u32>,
    /// Most requests held at once; one more is refused. The gateway's
    /// `hold_max`, and its default. Its limit in bytes is not modelled.
    #[serde(default = "default_hold_max")]
    pub hold_max: usize,
    /// Longest a request is held before it is refused. The gateway's
    /// `hold_timeout_secs`, and its default.
    #[serde(default = "default_hold_timeout_s")]
    pub hold_timeout_s: f64,
    /// Not in the live gateway yet: refuse at the door a request whose
    /// estimated wait is longer than the time its caller has left.
    #[serde(default)]
    pub admit_by_deadline: bool,
    /// The live gateway's `least_used_half_life_secs`: order waiting agents by least attained
    /// service (slot time per weight) instead of round robin; use older
    /// than this half-life counts half.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub least_attained_half_life_s: Option<f64>,
    /// Dispatch pacing, requests per second (0 = none).
    #[serde(default)]
    pub max_rps: f64,
    /// Set when the gateway has several instances: which one this copy of
    /// the upstream belongs to (from 0). Not written by hand.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance: Option<usize>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Policy {
    #[default]
    RoundRobin,
    Fair,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Overflow {
    #[default]
    Refuse,
    Evict,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Routing {
    /// Fewest requests in flight; ties go to the first listed.
    #[default]
    LeastInflight,
    /// Fewest requests in flight relative to what the target can run at
    /// once (a server's slots, a proxy's workers), so a small server is not
    /// handed as much as a large one.
    LeastLoaded,
    RoundRobin,
    Random,
    /// Requests that share a prompt prefix (see a client's `prefix`) go to
    /// the same target while it has room, so its prefix cache stays warm;
    /// when it is full, and for requests without a prefix, this is
    /// `least_loaded`.
    Affinity,
    /// One rule for cache and load: the target where the time to read what
    /// is not cached of this prompt, plus the request's own duration times
    /// how full the target is, is least (so a full target costs as much as
    /// doing the request once more). Assumes the router knows what each
    /// server has cached (but not whether that slot is busy), how fast it
    /// reads and writes, and how long the answer will be. With
    /// `cost_in_tokens` it is tokens still to read plus the tokens the
    /// target has in hand per slot.
    Cost,
    /// Prefix routing as the gateway does it (`affinity = "prefix"`). Only
    /// targets with a free seat are looked at. Of those, the one believed
    /// to hold most of the prompt wins unless it is much busier than the
    /// least loaded (`spill`), else the least loaded. What a target holds
    /// is the gateway's own picture: the last `prefix_slots` prompts it
    /// sent there, which need not be what the server really kept. A full
    /// target is not waited for unless `affinity_wait_s` says so.
    Prefix,
}

/// A request-forwarding proxy that burns CPU per request and per streamed
/// chunk on a fixed number of single-threaded workers. LiteLLM Proxy is one.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Proxy {
    pub name: String,
    /// Identical instances this entry stands for, named `name-1` to
    /// `name-N`; a gateway upstream that lists `name` gets all of them.
    #[serde(default = "one", skip_serializing_if = "is_one")]
    pub count: usize,
    #[serde(default)]
    pub host: Option<String>,
    /// Processes; each is one event loop that does one thing at a time.
    #[serde(default = "one")]
    pub workers: usize,
    #[serde(default = "default_proxy_req")]
    pub cpu_ms_per_request: f64,
    #[serde(default = "default_proxy_chunk")]
    pub cpu_ms_per_chunk: f64,
    /// CPU spent after the response has gone out (logging, cost tracking,
    /// callbacks). It does not delay that request but keeps the worker busy.
    #[serde(default)]
    pub cpu_ms_after_request: f64,
    /// Spread of the CPU cost (sigma of a log-normal).
    #[serde(default = "default_proxy_jitter")]
    pub jitter: f64,
    /// How a backend is chosen when several serve the model.
    #[serde(default = "proxy_routing")]
    pub routing: Routing,
    /// With `affinity`: a prefix gets one more target for every this many
    /// of its requests that are open at once, counted in targets' worth of
    /// slots (1.0 = one more when it alone would fill a target). 0 = one
    /// target only; when that one is full, requests go wherever there is
    /// most room.
    #[serde(default)]
    pub replicate_at: f64,
    /// With `cost`: how much the work already at a target counts against
    /// what the target would have to read of this prompt.
    #[serde(default = "always")]
    pub cost_load_weight: f64,
    /// With `cost`: count tokens (a prompt token and an output token weigh
    /// the same) instead of the time they take.
    #[serde(default)]
    pub cost_in_tokens: bool,
    /// Backends this proxy can reach. Empty: all of them.
    #[serde(default)]
    pub backends: Vec<String>,
}

fn default_proxy_req() -> f64 {
    18.0
}
fn default_proxy_chunk() -> f64 {
    0.6
}
fn default_proxy_jitter() -> f64 {
    0.3
}
fn proxy_routing() -> Routing {
    Routing::Random
}

/// A server that generates tokens at a fixed rate on its own hardware.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelServer {
    pub name: String,
    /// Identical servers this entry stands for. They are named `name-1` to
    /// `name-N`, and wherever a gateway upstream or a proxy lists `name` it
    /// gets all of them.
    #[serde(default = "one", skip_serializing_if = "is_one")]
    pub count: usize,
    #[serde(default)]
    pub host: Option<String>,
    pub models: Vec<String>,
    /// Requests generated at the same time; the rest wait in arrival order.
    #[serde(default = "one")]
    pub slots: usize,
    /// Output tokens per second of one request when it runs alone.
    pub tokens_per_s: f64,
    /// How total throughput grows with requests running together:
    /// total = tokens_per_s * n^batch_scaling. 0 = they share one stream's
    /// speed (CPU inference), 1 = each keeps full speed (a GPU below its
    /// knee).
    #[serde(default)]
    pub batch_scaling: f64,
    /// Requests running together beyond which the total stops growing:
    /// total = tokens_per_s * min(n, batch_knee)^batch_scaling, so every
    /// request past the knee slows all of them in proportion. A server
    /// that takes many more requests than its knee (vLLM's default
    /// `max_num_seqs` is 256) keeps its total and gives each caller a
    /// small share of it. Unset: the total grows up to `slots`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub batch_knee: Option<usize>,
    /// Prompt tokens read between two steps of the answers being written
    /// (llama.cpp's `n_batch`, vLLM's token budget for chunked prefill).
    /// While any prompt is being read, every answer on the server waits
    /// that long for each of its tokens: chunk / `prefill_tokens_per_s`.
    /// 0 = reading a prompt does not hold up writing. In the lab's run a
    /// 512-token chunk cut each answer from 24 to 0.4 tokens a second
    /// (`reference/speed-under-load`).
    #[serde(default)]
    pub prefill_chunk_tokens: f64,
    /// Tokens of context (prompts and what has been written) the server
    /// can hold for the requests it runs. A request starts only when its
    /// prompt fits; when the answers outgrow the room, the request that
    /// started last is put back in the queue and later read again from
    /// the start, with what it had written (vLLM's preemption with
    /// recompute). Unset: no limit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_tokens: Option<f64>,
    /// The measured shape of batching, in place of `batch_scaling` and
    /// `batch_knee`: a request among n runs at 1 / (1 + (n - 1) / batch_half)
    /// of its solo speed, so the total rises towards `batch_half` times one
    /// stream and each request keeps slowing. Fits the lab's run within 7%
    /// from 1 to 64 together with 6.36 (`reference/speed-under-load`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub batch_half: Option<f64>,
    /// A measured concurrency sweep, in place of every other batching
    /// field: pairs of (requests running together, one request's share of
    /// its solo speed), ascending. Between two points the share follows a
    /// straight line on log-log axes, from 1 at one request. Past the last
    /// point the share keeps the slope of the last stretch, held between
    /// n^-0.5 and n^-1 (an assumption: the sweep ended there; -1 is a total
    /// that has stopped growing).
    /// `reference/models` builds these from public benchmark rows.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub batch_curve: Vec<[f64; 2]>,
    /// How fast the total falls past the knee: it is multiplied by
    /// (n / batch_knee)^-overload_decay (memory for contexts runs short
    /// and work is redone). 0 = the total holds, which is what published
    /// concurrency sweeps and the lab's own run show while callers wait.
    #[serde(default)]
    pub overload_decay: f64,
    /// Fixed time before the first token (request parsing, sampling setup).
    #[serde(default)]
    pub prefill_ms: f64,
    /// Prompt tokens read per second; 0 = prompt length does not matter.
    #[serde(default)]
    pub prefill_tokens_per_s: f64,
    /// Share of requests whose prompt prefix is already in the server's
    /// cache (KV / prefix cache); their prefill is shorter by
    /// `prompt_cache_saving`.
    #[serde(default)]
    pub prompt_cache_hit_rate: f64,
    #[serde(default = "default_prompt_saving")]
    pub prompt_cache_saving: f64,
    /// Unset: each slot keeps the prompt it ran last, and a request takes
    /// the idle slot whose prompt starts like its own, else the idle slot
    /// used longest ago (llama.cpp). So a prefix whose slot is busy is not
    /// found. Set: the server keeps this many prefixes for all slots
    /// together, least recently used first out (closer to vLLM's block
    /// cache). Only requests from clients with a `prefix` look here.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix_cache_entries: Option<usize>,
    /// Spread of prefill time and token rate per request (log-normal sigma).
    #[serde(default = "default_server_jitter")]
    pub jitter: f64,
    /// What the server does with a request whose caller has gone.
    #[serde(default)]
    pub on_disconnect: OnDisconnect,
    /// Extra time an abandoned request costs beyond its prefill (the first
    /// token and noticing the closed connection).
    #[serde(default)]
    pub abandon_extra_ms: f64,
    /// Threads busy on the host while at least one request runs.
    #[serde(default = "f1")]
    pub threads: f64,
    /// How much of the host's slowdown (more busy threads than cores)
    /// reaches token writing; reading a prompt always gets all of it.
    /// On the lab's CPU servers a prompt was read 2.2 times slower beside
    /// two busy neighbours and tokens were written 1.1 times slower: 0.2.
    #[serde(default = "f1")]
    pub gen_contention: f64,
}

impl ModelServer {
    /// Requests running together at which the server gives its full total:
    /// its knee, or every slot when it has none. What counts as its
    /// capacity; a server that takes more is over it, not fuller.
    pub fn full(&self) -> usize {
        self.batch_knee
            .map_or(self.slots, |k| k.clamp(1, self.slots))
    }

    /// One request's share of its solo speed while `n` run together.
    pub fn share(&self, n: f64) -> f64 {
        let n = n.max(1.0);
        if let Some(share) = curve_share(&self.batch_curve, n) {
            return share;
        }
        if let Some(h) = self.batch_half.filter(|h| *h > 0.0) {
            return 1.0 / (1.0 + (n - 1.0) / h);
        }
        match self.batch_knee.map(|k| k.max(1) as f64) {
            Some(k) if n > k => k.powf(self.batch_scaling) / n * (n / k).powf(-self.overload_decay),
            _ => n.powf(self.batch_scaling - 1.0),
        }
    }
}

/// One request's share of its solo speed among `n`, read off a measured
/// sweep. None when there is no usable sweep.
fn curve_share(curve: &[[f64; 2]], n: f64) -> Option<f64> {
    let pts: Vec<[f64; 2]> = curve
        .iter()
        .copied()
        .filter(|p| p[0] >= 1.0 && p[1] > 0.0)
        .collect();
    let (first, last) = (*pts.first()?, *pts.last()?);
    if n <= first[0] {
        // One request alone always has its full speed.
        if first[0] <= 1.0 {
            return Some(first[1]);
        }
        let t = n.ln() / first[0].ln();
        return Some((t * first[1].ln()).exp());
    }
    if n >= last[0] {
        let slope = match pts.len() {
            1 => -1.0,
            k => {
                let prev = pts[k - 2];
                ((last[1] / prev[1]).ln() / (last[0] / prev[0]).ln()).clamp(-1.0, -0.5)
            }
        };
        return Some(last[1] * (n / last[0]).powf(slope));
    }
    let i = pts.iter().position(|p| p[0] >= n)?;
    let (a, b) = (pts[i - 1], pts[i]);
    if b[0] <= a[0] {
        return Some(b[1]);
    }
    let t = (n.ln() - a[0].ln()) / (b[0].ln() - a[0].ln());
    Some((a[1].ln() + t * (b[1].ln() - a[1].ln())).exp())
}

fn default_prompt_saving() -> f64 {
    0.8
}
fn default_server_jitter() -> f64 {
    0.05
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnDisconnect {
    /// Reads the prompt and produces the first token before it notices
    /// (llama-cpp-python checks the connection after each chunk).
    #[default]
    FirstToken,
    /// Drops the request at once, queued or running.
    Cancel,
    /// Generates the whole answer anyway.
    Finish,
}

/// A provider API: effectively unlimited parallelism, its own latency
/// distribution and rate limits.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostedApi {
    pub name: String,
    pub models: Vec<String>,
    pub ttft_ms: Dist,
    /// Extra time to first token per prompt token.
    #[serde(default)]
    pub ttft_ms_per_prompt_token: f64,
    pub tokens_per_s: Dist,
    /// Requests per minute before 429 (0 = no limit).
    #[serde(default)]
    pub rpm: f64,
    /// Input plus output tokens per minute before 429 (0 = no limit).
    #[serde(default)]
    pub tpm: f64,
    /// Open requests before 429 (0 = no limit).
    #[serde(default)]
    pub max_concurrency: usize,
}

/// Log-normal: `median` and the spread of the log (`sigma`, 0 = constant).
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Dist {
    pub median: f64,
    #[serde(default)]
    pub sigma: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpServer {
    pub name: String,
    #[serde(default)]
    pub host: Option<String>,
    /// Tool calls served at once; the rest wait.
    #[serde(default = "default_mcp_conc")]
    pub concurrency: usize,
    pub latency_ms: Dist,
}

fn default_mcp_conc() -> usize {
    8
}

/// Where the gateway's request log goes. Writes are off the request path, so
/// a slow store shows up as a growing buffer, not as request latency.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Db {
    #[serde(default = "db_name")]
    pub name: String,
    #[serde(default)]
    pub host: Option<String>,
    /// Rows written per request (the request row plus its spans).
    #[serde(default = "default_db_rows")]
    pub rows_per_request: f64,
    /// Rows the store appends per second while writing.
    #[serde(default = "default_db_rate")]
    pub rows_per_s: f64,
    /// Fixed cost of one write batch.
    #[serde(default = "default_db_batch_ms")]
    pub batch_overhead_ms: f64,
    #[serde(default = "default_db_flush")]
    pub flush_interval_ms: f64,
    /// Rows the gateway holds before it starts dropping.
    #[serde(default = "default_db_buffer")]
    pub max_buffer_rows: f64,
}

fn db_name() -> String {
    "tdb".into()
}
fn default_db_rows() -> f64 {
    4.0
}
fn default_db_rate() -> f64 {
    96_000.0
}
fn default_db_batch_ms() -> f64 {
    1.0
}
fn default_db_flush() -> f64 {
    1000.0
}
fn default_db_buffer() -> f64 {
    100_000.0
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Client {
    pub name: String,
    /// Identical callers this entry stands for, named `name-1` to `name-N`.
    /// Each has its own arrival process and its own gateway queue; results
    /// are grouped under `name` unless `class` says otherwise.
    #[serde(default = "one", skip_serializing_if = "is_one")]
    pub count: usize,
    /// Label results are grouped by (`interactive`, `batch`, ...).
    #[serde(default)]
    pub class: Option<String>,
    /// Gateway agent id; requests of one agent share a queue. Defaults to the
    /// client name.
    #[serde(default)]
    pub agent: Option<String>,
    /// Agent weight in the gateway scheduler.
    #[serde(default = "default_weight")]
    pub weight: u32,
    /// Most requests of this agent the gateway runs at once.
    #[serde(default)]
    pub agent_max_concurrency: Option<usize>,
    /// First component requests go to: the gateway, a proxy or a backend.
    pub entry: String,
    /// Each model gets its own arrival process (or its own set of agents).
    pub models: Vec<String>,
    pub arrival: Arrival,
    #[serde(default)]
    pub from_s: f64,
    /// No new requests after this.
    pub to_s: f64,
    #[serde(default = "default_prompt")]
    pub prompt_tokens: f64,
    /// Output tokens per request.
    pub output_tokens: f64,
    /// Spread of output length (log-normal sigma; 0 = always the same).
    #[serde(default)]
    pub output_sigma: f64,
    /// The caller gives up this long after it first sent the request.
    #[serde(default)]
    pub timeout_s: Option<f64>,
    /// Sent as `x-gateway-max-wait-ms`.
    #[serde(default)]
    pub max_wait_ms: Option<f64>,
    /// Come back after `retry-after` on 503 and 429, until the timeout.
    #[serde(default)]
    pub retry: bool,
    /// After a timeout, send the same request again this many times, each
    /// with the full timeout (what client SDKs do by default: twice). The
    /// attempt that timed out counts as a request that gave up and the new
    /// one as a request sent, so a pile-up shows as more requests. When
    /// set, `retry` comes back after a refusal this many times too, not
    /// until the timeout.
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    pub timeout_retries: u32,
    /// Tool use and multi-turn behaviour of closed-loop agents.
    #[serde(default)]
    pub session: Option<Session>,
    /// How much of the prompt repeats, so that a server's prefix (KV) cache
    /// can skip reading it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<Prefix>,
    /// A daily rhythm for `poisson` arrivals: `rate_per_s` is the rate at
    /// the height of the busy hours and falls off around them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rhythm: Option<Rhythm>,
}

/// Refusing in stages, the gateway's `shed_from` / `shed_ceiling` /
/// `shed_floor`. With the queue filled to `from` (a share of `max_queue`)
/// the lowest weight starts to be refused, more often the fuller it gets
/// and always from `low_ceiling`. The highest weight is refused only when
/// the queue is full; weights in between get thresholds in between.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Shed {
    pub from: f64,
    pub low_ceiling: f64,
    /// Work is not refused early while requests of its weight or less hold
    /// fewer than this share of `max_queue` places.
    #[serde(default)]
    pub floor: f64,
}

/// The repeating start of a client's prompts. A request finds it in a
/// server's cache when an earlier request with the same prefix ran on that
/// server and nothing pushed it out since; then only the rest of the prompt
/// is read (see `prompt_cache_saving`).
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Prefix {
    /// Typical share of the prompt that repeats (0 to 1).
    pub share: f64,
    /// Spread of that share between requests (log-normal sigma).
    #[serde(default)]
    pub sigma: f64,
    /// Who repeats it: each caller its own context (`caller`), or everyone
    /// in the group the same template (`group`).
    #[serde(default)]
    pub scope: PrefixScope,
    /// With `scope = "caller"`: the first part of the prompt that is also
    /// the same for everyone in the group (a system prompt, tool
    /// definitions), as a share of the prompt. It is what a request still
    /// finds when its own context is not in the cache.
    #[serde(default)]
    pub group_share: f64,
    /// Who shares the group part: clients that give the same name. Unset =
    /// the client's class.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    /// How often a request continues the caller's previous one. Otherwise
    /// it starts a new conversation, and only the group part repeats.
    #[serde(default = "always")]
    pub follow_up: f64,
}

fn default_spill() -> f64 {
    0.5
}

fn always() -> f64 {
    1.0
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrefixScope {
    #[default]
    Caller,
    Group,
}

/// Busy hours that repeat every `period_s`. The rate is `rate_per_s` times
/// `quiet` away from the busy hours and rises smoothly (a raised cosine
/// `busy_width_s` wide) to `rate_per_s` at `busy_at_s`. `shift_s` moves the
/// whole pattern later, which is how groups are staggered. With `plateau`
/// the busy hours are a working day with a flat top instead, with `bell` a
/// normal curve.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Rhythm {
    pub period_s: f64,
    #[serde(default)]
    pub busy_at_s: f64,
    #[serde(default)]
    pub busy_width_s: f64,
    #[serde(default)]
    pub quiet: f64,
    #[serde(default)]
    pub shift_s: f64,
    /// A working day instead of one bump: the rate starts to rise at the
    /// first time, is full between the second and the third, and is back
    /// down at the fourth (seconds into the period, in that order).
    #[serde(default)]
    pub plateau: Option<[f64; 4]>,
    /// A bell instead: `[centre, sigma]` in seconds. The rate follows a
    /// normal curve around the centre, so groups whose days are shifted
    /// apart add up to a lower peak. Wins over `plateau`.
    #[serde(default)]
    pub bell: Option<[f64; 2]>,
}

impl Rhythm {
    /// Share of the peak rate at time `t_s`, between `quiet` and 1.
    pub fn level(&self, t_s: f64) -> f64 {
        if self.period_s <= 0.0 {
            return 1.0;
        }
        let quiet = self.quiet.clamp(0.0, 1.0);
        if let Some([centre, sigma]) = self.bell {
            let mut d = (t_s - self.shift_s - centre).rem_euclid(self.period_s);
            if d > self.period_s / 2.0 {
                d -= self.period_s;
            }
            let z = d / sigma.max(1e-9);
            return quiet + (1.0 - quiet) * (-0.5 * z * z).exp();
        }
        if let Some([rise, full, until, down]) = self.plateau {
            let t = (t_s - self.shift_s).rem_euclid(self.period_s);
            // A smooth step from 0 at `a` to 1 at `b`.
            let ease = |a: f64, b: f64| {
                let x = ((t - a) / (b - a).max(1e-9)).clamp(0.0, 1.0);
                0.5 * (1.0 - (std::f64::consts::PI * x).cos())
            };
            let up = if t >= full { 1.0 } else { ease(rise, full) };
            let stay = if t <= until {
                1.0
            } else {
                1.0 - ease(until, down)
            };
            return quiet + (1.0 - quiet) * up * stay;
        }
        if self.busy_width_s <= 0.0 {
            return 1.0;
        }
        let mut d = (t_s - self.shift_s - self.busy_at_s).rem_euclid(self.period_s);
        if d > self.period_s / 2.0 {
            d -= self.period_s;
        }
        let bump = if d.abs() < self.busy_width_s / 2.0 {
            0.5 * (1.0 + (std::f64::consts::TAU * d / self.busy_width_s).cos())
        } else {
            0.0
        };
        quiet + (1.0 - quiet) * bump
    }
}

fn default_weight() -> u32 {
    1
}
fn default_prompt() -> f64 {
    30.0
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Arrival {
    /// Open loop: requests keep coming whether or not earlier ones finished.
    Poisson { rate_per_s: f64 },
    /// Open loop at a fixed spacing.
    Uniform { rate_per_s: f64 },
    /// Closed loop: each agent sends, waits for the answer, thinks, repeats.
    Closed {
        agents: usize,
        #[serde(default)]
        think_s: f64,
    },
    /// Requests at recorded times with recorded sizes.
    Trace { rows: Vec<TraceRow> },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TraceRow {
    pub t: f64,
    pub model: String,
    #[serde(default)]
    pub prompt_tokens: Option<f64>,
    #[serde(default)]
    pub output_tokens: Option<f64>,
    /// The start of the prompt this request shares with earlier ones, from
    /// a recorded log: `[key, share, group key, group share, store key]`.
    /// `key` names the longest start some earlier request also had (its
    /// own conversation so far) and `share` how much of the prompt that
    /// is; the group pair is the same for the very first block (a system
    /// prompt everyone of a kind sends). `store key` is what a follow-up
    /// will look for once a server has read this prompt. A key of 0 =
    /// none. Takes the place of the client's `prefix`.
    pub prefix: Option<[f64; 5]>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Session {
    /// Turns before the agent starts over with a short prompt.
    #[serde(default = "one")]
    pub turns: usize,
    /// Prompt tokens added by every turn.
    #[serde(default)]
    pub prompt_growth_tokens: f64,
    /// MCP server called before a turn, with this probability.
    #[serde(default)]
    pub tool: Option<String>,
    #[serde(default = "f1")]
    pub tool_call_probability: f64,
    /// Whether tool calls go through the gateway (they do when it is the
    /// client's entry).
    #[serde(default = "yes")]
    pub tool_via_gateway: bool,
}

impl Scenario {
    pub fn from_toml(text: &str) -> Result<Self, String> {
        let s: Scenario = toml::from_str(text).map_err(|e| e.to_string())?;
        s.ready()
    }

    pub fn from_json(text: &str) -> Result<Self, String> {
        let s: Scenario = serde_json::from_str(text).map_err(|e| e.to_string())?;
        s.ready()
    }

    /// Expand `count` and check the result.
    pub fn ready(mut self) -> Result<Self, String> {
        self.expand()?;
        self.split_gateway()?;
        self.resolve_caps();
        self.check()?;
        Ok(self)
    }

    /// Slots of the model servers an upstream ends at; `None` when one of
    /// its backends has no fixed limit.
    pub fn slots_behind(&self, u: &Upstream) -> Option<usize> {
        let mut slots = 0;
        for b in crate::metrics::backends_behind(self, u) {
            if let Some(m) = self.model_servers.iter().find(|m| m.name == b) {
                slots += m.full();
            } else {
                match self.hosted_apis.iter().find(|h| h.name == b) {
                    Some(h) if h.max_concurrency > 0 => slots += h.max_concurrency,
                    _ => return None,
                }
            }
        }
        Some(slots)
    }

    /// Turn every `headroom` into the concurrency cap it stands for.
    pub fn resolve_caps(&mut self) {
        let Some(g) = &self.gateway else { return };
        let caps: Vec<Option<usize>> = g
            .upstreams
            .iter()
            .map(|u| {
                let h = u.headroom.filter(|h| *h > 0.0)?;
                let slots = self.slots_behind(u)?;
                // An instance hands out its share of the slots.
                let share = if u.instance.is_some() {
                    g.instances.max(1) as f64
                } else {
                    1.0
                };
                Some(((slots as f64 * h / share).floor() as usize).max(1))
            })
            .collect();
        if let Some(g) = &mut self.gateway {
            for (u, cap) in g.upstreams.iter_mut().zip(caps) {
                if let Some(cap) = cap {
                    u.max_concurrency = cap;
                }
            }
        }
    }

    /// Replace every entry with `count = N` by N entries.
    fn expand(&mut self) -> Result<(), String> {
        const MOST: usize = 5000;
        let mut servers = Vec::new();
        let mut fleets: Vec<(String, Vec<String>)> = Vec::new();
        for m in std::mem::take(&mut self.model_servers) {
            if m.count == 0 || m.count > MOST {
                return Err(format!(
                    "model_server {}: count must be 1 to {MOST}",
                    m.name
                ));
            }
            if m.count == 1 {
                servers.push(m);
                continue;
            }
            let names: Vec<String> = (1..=m.count).map(|i| format!("{}-{i}", m.name)).collect();
            for n in &names {
                servers.push(ModelServer {
                    name: n.clone(),
                    count: 1,
                    ..m.clone()
                });
            }
            fleets.push((m.name, names));
        }
        self.model_servers = servers;
        let mut proxies = Vec::new();
        for p in std::mem::take(&mut self.proxies) {
            if p.count == 0 || p.count > MOST {
                return Err(format!("proxy {}: count must be 1 to {MOST}", p.name));
            }
            if p.count == 1 {
                proxies.push(p);
                continue;
            }
            let names: Vec<String> = (1..=p.count).map(|i| format!("{}-{i}", p.name)).collect();
            for n in &names {
                proxies.push(Proxy {
                    name: n.clone(),
                    count: 1,
                    ..p.clone()
                });
            }
            fleets.push((p.name, names));
        }
        self.proxies = proxies;
        let spread = |list: &mut Vec<String>| {
            *list = std::mem::take(list)
                .into_iter()
                .flat_map(|n| match fleets.iter().find(|f| f.0 == n) {
                    Some(f) => f.1.clone(),
                    None => vec![n],
                })
                .collect();
        };
        if let Some(g) = &mut self.gateway {
            g.upstreams.iter_mut().for_each(|u| spread(&mut u.targets));
        }
        self.proxies
            .iter_mut()
            .for_each(|p| spread(&mut p.backends));
        let mut clients = Vec::new();
        for c in std::mem::take(&mut self.clients) {
            if c.count == 0 || c.count > MOST {
                return Err(format!("client {}: count must be 1 to {MOST}", c.name));
            }
            if c.count == 1 {
                clients.push(c);
                continue;
            }
            for i in 1..=c.count {
                clients.push(Client {
                    name: format!("{}-{i}", c.name),
                    class: Some(c.class.clone().unwrap_or_else(|| c.name.clone())),
                    agent: c.agent.as_ref().map(|a| format!("{a}-{i}")),
                    count: 1,
                    ..c.clone()
                });
            }
        }
        self.clients = clients;
        Ok(())
    }

    /// TOML or JSON, whichever the text is.
    pub fn parse(text: &str) -> Result<Self, String> {
        if text.trim_start().starts_with('{') {
            Self::from_json(text)
        } else {
            Self::from_toml(text)
        }
    }

    pub fn backend_names(&self) -> Vec<&str> {
        self.model_servers
            .iter()
            .map(|m| m.name.as_str())
            .chain(self.hosted_apis.iter().map(|h| h.name.as_str()))
            .collect()
    }

    /// Every reference resolves and every number is usable.
    pub fn check(&self) -> Result<(), String> {
        let mut names: Vec<&str> = self.backend_names();
        names.extend(self.proxies.iter().map(|p| p.name.as_str()));
        names.extend(self.mcp_servers.iter().map(|p| p.name.as_str()));
        if let Some(g) = &self.gateway {
            names.push(&g.name);
        }
        if let Some(d) = &self.db {
            names.push(&d.name);
        }
        let mut seen = std::collections::BTreeSet::new();
        for n in &names {
            if n.is_empty() || *n == "clients" {
                return Err(format!("component name {n:?} is not allowed"));
            }
            if !seen.insert(*n) {
                return Err(format!("two components are named {n:?}"));
            }
        }
        let host_ok = |h: &Option<String>, who: &str| match h {
            Some(h) if !self.hosts.iter().any(|x| &x.name == h) => {
                Err(format!("{who}: unknown host {h:?}"))
            }
            _ => Ok(()),
        };
        for h in &self.hosts {
            if h.cores <= 0.0 {
                return Err(format!("host {:?}: cores must be > 0", h.name));
            }
        }
        let backends = self.backend_names();
        let is_target = |t: &str| backends.contains(&t) || self.proxies.iter().any(|p| p.name == t);
        if let Some(g) = &self.gateway {
            host_ok(&g.host, "gateway")?;
            if g.upstreams.is_empty() {
                return Err("gateway: needs at least one upstream".into());
            }
            for u in &g.upstreams {
                if u.max_concurrency == 0 {
                    return Err(format!(
                        "upstream {:?}: max_concurrency must be > 0",
                        u.name
                    ));
                }
                if u.targets.is_empty() {
                    return Err(format!("upstream {:?}: needs a target", u.name));
                }
                for t in &u.targets {
                    if !is_target(t) {
                        return Err(format!("upstream {:?}: unknown target {t:?}", u.name));
                    }
                }
            }
        }
        for p in &self.proxies {
            host_ok(&p.host, &p.name)?;
            if p.workers == 0 {
                return Err(format!("proxy {:?}: workers must be > 0", p.name));
            }
            for b in &p.backends {
                if !backends.contains(&b.as_str()) {
                    return Err(format!("proxy {:?}: unknown backend {b:?}", p.name));
                }
            }
        }
        for m in &self.model_servers {
            host_ok(&m.host, &m.name)?;
            if m.slots == 0 || m.tokens_per_s <= 0.0 {
                return Err(format!(
                    "model server {:?}: slots and tokens_per_s must be > 0",
                    m.name
                ));
            }
        }
        for h in &self.hosted_apis {
            if h.ttft_ms.median < 0.0 || h.tokens_per_s.median <= 0.0 {
                return Err(format!("hosted api {:?}: bad latency numbers", h.name));
            }
        }
        for m in &self.mcp_servers {
            host_ok(&m.host, &m.name)?;
            if m.concurrency == 0 {
                return Err(format!("mcp server {:?}: concurrency must be > 0", m.name));
            }
        }
        if let Some(d) = &self.db {
            host_ok(&d.host, &d.name)?;
            if d.rows_per_s <= 0.0 || d.flush_interval_ms <= 0.0 {
                return Err("db: rows_per_s and flush_interval_ms must be > 0".into());
            }
        }
        if self.clients.is_empty() {
            return Err("no clients: nothing would send a request".into());
        }
        let mut routed = std::collections::HashSet::new();
        for c in &self.clients {
            let entry_is_gateway = self.gateway.as_ref().is_some_and(|g| g.name == c.entry);
            if !entry_is_gateway && !is_target(&c.entry) {
                return Err(format!("client {:?}: unknown entry {:?}", c.name, c.entry));
            }
            if c.models.is_empty() && !matches!(c.arrival, Arrival::Trace { .. }) {
                return Err(format!("client {:?}: needs at least one model", c.name));
            }
            if c.output_tokens < 1.0 {
                return Err(format!("client {:?}: output_tokens must be >= 1", c.name));
            }
            match &c.arrival {
                Arrival::Poisson { rate_per_s } | Arrival::Uniform { rate_per_s } => {
                    if *rate_per_s <= 0.0 {
                        return Err(format!("client {:?}: rate_per_s must be > 0", c.name));
                    }
                }
                Arrival::Closed { agents, think_s } => {
                    if *agents == 0 || *think_s < 0.0 {
                        return Err(format!("client {:?}: bad closed-loop numbers", c.name));
                    }
                }
                Arrival::Trace { .. } => {}
            }
            let mut models: Vec<&str> = c.models.iter().map(String::as_str).collect();
            if let Arrival::Trace { rows } = &c.arrival {
                models.extend(rows.iter().map(|r| r.model.as_str()));
            }
            models.sort_unstable();
            models.dedup();
            for m in models {
                // Callers of one fleet share entry and model: check a route once.
                if routed.insert((c.entry.as_str(), m)) {
                    self.check_route(c, m)?;
                }
            }
            if let Some(tool) = c.session.as_ref().and_then(|s| s.tool.as_ref()) {
                if !self.mcp_servers.iter().any(|m| &m.name == tool) {
                    return Err(format!("client {:?}: unknown tool server {tool:?}", c.name));
                }
            }
        }
        Ok(())
    }

    fn serves(&self, backend: &str, model: &str) -> bool {
        self.model_servers
            .iter()
            .any(|m| m.name == backend && m.models.iter().any(|x| x == model))
            || self
                .hosted_apis
                .iter()
                .any(|h| h.name == backend && h.models.iter().any(|x| x == model))
    }

    /// Whether `node` (a proxy or backend) can get `model` answered.
    pub fn reaches(&self, node: &str, model: &str) -> bool {
        if let Some(p) = self.proxies.iter().find(|p| p.name == node) {
            return self.backend_names().into_iter().any(|b| {
                (p.backends.is_empty() || p.backends.iter().any(|x| x == b))
                    && self.serves(b, model)
            });
        }
        self.serves(node, model)
    }

    /// The gateway upstream that takes `model`.
    pub fn upstream_for(&self, model: &str) -> Option<usize> {
        self.upstream_at(model, 0)
    }

    /// The gateway instance a client is pinned to.
    pub fn instance_of(&self, client: usize) -> usize {
        client % self.gateway.as_ref().map_or(1, |g| g.instances.max(1))
    }

    /// The upstream that takes `model` on one gateway instance.
    pub fn upstream_at(&self, model: &str, instance: usize) -> Option<usize> {
        let ups = &self.gateway.as_ref()?.upstreams;
        let here = |u: &Upstream| u.instance.is_none_or(|i| i == instance);
        ups.iter()
            .position(|u| here(u) && u.models.iter().any(|m| m == model))
            .or_else(|| ups.iter().position(|u| here(u) && u.models.is_empty()))
    }

    /// One copy of every upstream per gateway instance, each with its share
    /// of the cap and of the queue.
    fn split_gateway(&mut self) -> Result<(), String> {
        let Some(g) = &mut self.gateway else {
            return Ok(());
        };
        if g.instances == 0 || g.instances > 256 {
            return Err("gateway: instances must be 1 to 256".into());
        }
        if g.instances == 1 {
            return Ok(());
        }
        let n = g.instances;
        let mut out = Vec::new();
        for u in std::mem::take(&mut g.upstreams) {
            if u.instance.is_some() {
                out.push(u);
                continue;
            }
            for i in 0..n {
                out.push(Upstream {
                    name: format!("{}-{}", u.name, i + 1),
                    max_concurrency: (u.max_concurrency / n).max(1),
                    max_queue: u.max_queue.map(|q| (q / n).max(1)),
                    max_rps: u.max_rps / n as f64,
                    instance: Some(i),
                    ..u.clone()
                });
            }
        }
        g.upstreams = out;
        Ok(())
    }

    fn check_route(&self, c: &Client, model: &str) -> Result<(), String> {
        let fail = |why: String| Err(format!("client {:?}, model {model:?}: {why}", c.name));
        match &self.gateway {
            Some(g) if g.name == c.entry => {
                let Some(u) = self.upstream_for(model) else {
                    return fail("no gateway upstream takes this model".into());
                };
                let u = &g.upstreams[u];
                if let Some(t) = u.targets.iter().find(|t| !self.reaches(t, model)) {
                    return fail(format!(
                        "upstream {:?} target {t:?} cannot serve it",
                        u.name
                    ));
                }
                Ok(())
            }
            _ if self.reaches(&c.entry, model) => Ok(()),
            _ => fail(format!("{:?} cannot serve it", c.entry)),
        }
    }
}

fn is_zero_u32(n: &u32) -> bool {
    *n == 0
}
