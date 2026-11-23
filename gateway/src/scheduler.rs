//! Per-upstream dispatcher: who goes next, on which server, and who is told
//! to come back later.
//!
//! One queue engine serves every policy. Requests wait in
//! [`FairQueue`](crate::lead::fair::FairQueue); a single dispatcher task per
//! upstream grants [`Lease`]s one at a time, so a busy agent cannot starve
//! the others and rate-limit waits are paid once for the whole upstream.
//!
//! - `round_robin` shares turns between agents by request count, in
//!   proportion to their weights.
//! - `fair` shares by tokens between teams, serves the run with the least
//!   service first and lets a request whose provider cache is about to expire
//!   go ahead within a bound.
//!
//! On top of either: strict priority with a guaranteed share for lower
//!   priorities, seats kept free for the most important work, a bounded
//!   queue (by count and by bytes) that pushes out the least important
//!   waiter first, refusal at the door when the wait would be too long, and
//!   routing over several servers by the work they have outstanding.
//!
//! With [`Hold`], batch work is not refused for a full queue: it is held
//! beside the queue and sent on when nothing else that could run is waiting.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::Serialize;
use tokio::sync::{oneshot, Notify};

use crate::lead::adaptive::{route, AdaptiveCfg, BackendState, Warmth};
use crate::lead::fair::{FairCfg, FairQueue, Item};
use crate::lead::prefix::{Affinity, Belief, Chain, PrefixMap};
use crate::limiter::RateLimiter;
use crate::obs::now_ms;

/// Importance of a request: class first (0 interactive, 1 normal, 2
/// batch), then the agent's weight. Smaller is more important.
pub type Rank = (u8, u32);

fn rank_of(class: u8, weight: u32) -> Rank {
    (class, u32::MAX - weight.max(1))
}

/// How long ago a more important request must have arrived for seats to be
/// kept free for its kind.
const RESERVE_MEMORY: Duration = Duration::from_secs(30);
/// Grants older than this do not count towards the measured service rate.
const RATE_WINDOW: Duration = Duration::from_secs(45);
const RATE_WINDOW_SHORT: Duration = Duration::from_secs(15);

pub struct Scheduler {
    pub name: String,
    state: Mutex<State>,
    notify: Notify,
    pub limiter: Mutex<RateLimiter>,
    opts: Options,
    backend_urls: Vec<String>,
}

/// One model server behind an upstream.
#[derive(Clone, Debug)]
pub struct BackendSpec {
    pub name: String,
    pub url: String,
    pub limit: usize,
    /// Find the best concurrency from observed throughput, up to `limit`.
    pub adaptive: bool,
    pub models: Vec<String>,
    /// Prompts the server keeps in its prefix cache (0 = as many as
    /// `limit`).
    pub prefix_slots: usize,
}

/// How an upstream's queue behaves.
#[derive(Clone, Debug)]
pub struct Options {
    /// Share by tokens between teams (`fair`) instead of by request count
    /// between agents (`round_robin`).
    pub fair: bool,
    pub fair_cfg: FairCfg,
    /// Serve the most important work that can run first; the rest gets the
    /// seats that are left.
    pub strict: bool,
    /// Under `strict`, the share of grants that goes to less important work
    /// anyway while it is waiting, so it cannot starve (0..0.5).
    pub min_share: f64,
    /// Seats less important work may not take while more important work has
    /// been arriving, so that work does not wait for a long request to end.
    pub reserve: usize,
    /// How much busier a run's last server may be before the run moves.
    pub spill: f64,
    /// Adaptive limits step down when first tokens take this many times
    /// longer than at the lowest load seen (0 = off).
    pub slowdown: f64,
    /// What counts as a warm server when routing.
    pub affinity: Affinity,
    /// Shared prompt starts shorter than this (estimated tokens) are
    /// ignored.
    pub affinity_min_tokens: u32,
    /// How long a request may wait for the full server that holds most of
    /// its prompt while another server is free.
    pub affinity_wait: Duration,
    /// A server forgets a prompt it has not seen for this long (zero =
    /// only when newer prompts push it out).
    pub prefix_ttl: Duration,
    /// `Some(w)`: pick the server with the least prompt tokens left to read
    /// plus `w` times its prompt tokens in flight per seat.
    pub load_weight: Option<f64>,
    /// A prompt no server holds does not push out a prompt used within
    /// this time; it waits, at most `slot_max_wait`.
    pub slot_hold: Duration,
    pub slot_max_wait: Duration,
    /// A batch request that would start cold lets later batch requests of
    /// the same caller whose prompt is warm go first, until it has waited
    /// this long (zero = keep arrival order).
    pub batch_group: Duration,
    /// A server with requests in flight and none finished for this long
    /// gets no more work until one does (zero = no such rule).
    pub stale_after: Duration,
    /// Least used first between callers: order them by the server time
    /// they held lately, where use this old counts half (zero = take
    /// turns).
    pub used_half_life: Duration,
    /// Hold batch work instead of refusing it.
    pub hold: Option<Hold>,
}

/// Holding batch work (class 2). A held request does not take a place in
/// the queue: `max_queue`, `max_queue_bytes`, staged refusal, eviction and
/// the wait limit at the door pass it by. It gets a seat only when no other
/// request that could run is waiting, whatever the policy and `min_share`
/// say. What is held is bounded on its own, because every held request is an
/// open connection and a request body in memory.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Hold {
    /// Most requests held; one more is refused.
    pub max: usize,
    /// Most request-body bytes held.
    pub max_bytes: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            fair: false,
            fair_cfg: FairCfg::default(),
            strict: false,
            min_share: 0.0,
            reserve: 0,
            spill: 0.5,
            slowdown: 0.0,
            affinity: Affinity::Prefix,
            affinity_min_tokens: 64,
            affinity_wait: Duration::ZERO,
            prefix_ttl: Duration::ZERO,
            load_weight: None,
            slot_hold: Duration::ZERO,
            slot_max_wait: Duration::ZERO,
            batch_group: Duration::ZERO,
            stale_after: Duration::ZERO,
            used_half_life: Duration::ZERO,
            hold: None,
        }
    }
}

/// What the queue needs to know about a request beyond its agent.
#[derive(Clone, Debug, Default)]
pub struct FairTicket {
    /// Who the request is charged to (team, project or agent).
    pub tenant: String,
    pub tenant_weight: f64,
    pub run: String,
    /// Estimated cost in weighted tokens.
    pub cost: f64,
    /// Unix ms at which the provider cache entry this request reads expires.
    pub deadline_ms: Option<i64>,
    pub risk: f64,
    /// 0 interactive, 1 normal, 2 batch.
    pub class: u8,
    pub model: String,
    pub affinity: Option<usize>,
    /// The prompt as cumulative block hashes (see `lead::fingerprint`).
    pub prefix: Option<Chain>,
    /// Models that may stand in for `model` when its servers are full.
    pub alts: Vec<String>,
    /// Backend that just failed this request.
    pub avoid: Option<usize>,
}

struct Waiter {
    tx: oneshot::Sender<Grant>,
    agent: String,
    tenant: String,
    weight: f64,
    run: String,
    /// Cost for rate-limit pacing (estimated input tokens).
    cost: u32,
    rank: Rank,
    bytes: usize,
    /// Held beside the queue (see [`Hold`]).
    held: bool,
}

struct State {
    q: FairQueue,
    waiters: HashMap<u64, Waiter>,
    next_id: u64,
    agent_inflight: HashMap<String, usize>,
    agent_cap: HashMap<String, usize>,
    backends: Vec<BackendState>,
    jumps: u64,
    /// Bytes of request bodies waiting in the queue (held ones apart).
    bytes: usize,
    /// Requests held beside the queue, and the bytes of their bodies.
    held: usize,
    held_bytes: usize,
    /// Requests refused because as many as allowed were held already.
    hold_refused: u64,
    /// When each recent grant was given and how important it was.
    grants: VecDeque<(Instant, Rank)>,
    /// How long a seat is held per request, smoothed (ms); 0 = not measured.
    service_ms: f64,
    /// The most important rank that arrived lately, and when.
    seen: Option<(Rank, Instant)>,
    /// Grants owed to less important work under `strict`.
    credit: f64,
    /// Models paused after a rate-limit answer, until this Unix ms.
    model_cool: HashMap<String, i64>,
    evicted: u64,
    /// Requests refused before the queue was full (`Admission::shed`).
    shed_early: u64,
    /// Every importance that has asked for a place, to tell where one
    /// stands between the least and the most important.
    ranks: std::collections::BTreeSet<Rank>,
    /// Per importance: the share of a refusal owed so far. Refusing every
    /// time this passes one gives the wanted rate without dice.
    shed_owed: HashMap<Rank, f64>,
    /// Which prompts each backend's prefix cache should still hold.
    prefix: PrefixMap,
    aff: AffinityStats,
    /// What is known about each backend's health, beside its failure count.
    health: Vec<Health>,
    /// Servers going out and coming back, newest last.
    events: VecDeque<HealthEvent>,
    event_seq: u64,
}

/// Health of one backend as the gateway has seen it.
#[derive(Clone, Debug, Default)]
struct Health {
    /// When the server was taken out (Unix ms); 0 = it is in.
    down_since_ms: i64,
    /// What was last seen wrong with it.
    last_error: String,
    probes: u64,
    probe_fails: u64,
    /// When it was last probed (Unix ms).
    last_probe_ms: i64,
}

/// A model server going out of rotation or coming back.
#[derive(Debug, Clone, Serialize)]
pub struct HealthEvent {
    pub seq: u64,
    pub ts_ms: i64,
    pub backend: String,
    /// `down` or `up`.
    pub state: &'static str,
    /// What was seen: a failed request, a failed or answered probe.
    pub reason: String,
    /// For `up`: how long the server was out (ms).
    pub down_ms: i64,
}

/// Events kept in memory per upstream.
const EVENTS_KEPT: usize = 256;

impl State {
    /// Compare each backend's failure state with what was last recorded and
    /// note the servers that went out or came back. `why` says what was
    /// just seen wrong; a server comes back here only by answering a
    /// request (a probe that is answered records its own event).
    fn health_sync(&mut self, upstream: &str, now: i64, why: &str) {
        for i in 0..self.backends.len() {
            let (fails, cooling) = {
                let b = &self.backends[i];
                (b.fails, now < b.cool_until_ms)
            };
            let was_down = self.health[i].down_since_ms != 0;
            if !was_down && fails > 0 && cooling {
                self.health[i].down_since_ms = now;
                self.health[i].last_error = why.to_string();
                self.health_event(upstream, i, now, "down", why, 0);
            } else if was_down && fails == 0 {
                let down_ms = now - self.health[i].down_since_ms;
                self.health[i].down_since_ms = 0;
                self.health_event(upstream, i, now, "up", "a request succeeded", down_ms);
            }
        }
    }

    fn health_event(
        &mut self,
        upstream: &str,
        idx: usize,
        now: i64,
        state: &'static str,
        why: &str,
        down_ms: i64,
    ) {
        let backend = self.backends[idx].name.clone();
        if state == "down" {
            tracing::warn!(upstream, backend = %backend, reason = why, "model server taken out");
        } else {
            tracing::info!(upstream, backend = %backend, reason = why, down_ms, "model server back");
        }
        self.event_seq += 1;
        if self.events.len() >= EVENTS_KEPT {
            self.events.pop_front();
        }
        self.events.push_back(HealthEvent {
            seq: self.event_seq,
            ts_ms: now,
            backend,
            state,
            reason: why.to_string(),
            down_ms,
        });
    }

    /// Requests waiting in the queue proper, held ones apart.
    fn in_queue(&self) -> usize {
        self.waiters.len() - self.held
    }

    /// Take a waiter off the books (not out of the queue's order).
    fn unlist(&mut self, id: u64) -> Option<Waiter> {
        let w = self.waiters.remove(&id)?;
        if w.held {
            self.held -= 1;
            self.held_bytes = self.held_bytes.saturating_sub(w.bytes);
        } else {
            self.bytes = self.bytes.saturating_sub(w.bytes);
        }
        Some(w)
    }
}

/// How routing by prompt prefix has gone, over requests whose prompt the
/// gateway could read.
#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
pub struct AffinityStats {
    /// Sent to a server that held the start of the prompt.
    pub warm: u64,
    /// A server held it, but the request went elsewhere (it was full or too
    /// busy).
    pub spilled: u64,
    /// No server held anything of it.
    pub cold: u64,
    /// Batch requests moved ahead of an earlier one of the same caller
    /// because their prompt was warm.
    pub regrouped: u64,
    /// Estimated prompt tokens the chosen servers already held.
    pub warm_tokens: u64,
    /// Estimated prompt tokens of all those requests.
    pub prompt_tokens: u64,
}

enum Step {
    Granted,
    Idle,
    Wait(Duration),
}

/// What a waiting request eventually hears: its seat, or that it was pushed
/// out of a full queue by more important work.
pub type Grant = Result<Lease, Evicted>;

#[derive(Debug)]
pub struct Evicted;

/// Why a request was not let into the queue.
#[derive(Debug, PartialEq)]
pub enum Refused {
    /// The queue is full (by count or bytes) and nothing in it is less
    /// important.
    Full(Outlook),
    /// The queue is filling up and this request is among the less
    /// important ones (`Admission::shed`).
    Shed(Outlook),
    /// It would wait about this long, which is more than it is willing to.
    WaitTooLong(Outlook),
    /// It would be held, and as much as allowed is held already.
    HoldFull(Outlook),
    /// No server behind this upstream serves the model, nor a stand-in.
    NoServer,
    /// Every server that serves the model is out after failures
    /// (`Admission::refuse_down`); the first is due back in about this long.
    ServersDown(Duration),
}

/// Where a request stands in the queue: how many go before it and about how
/// long until it gets a seat.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Outlook {
    pub ahead: usize,
    pub wait: Duration,
    /// The pessimistic end of the estimate.
    pub wait_max: Duration,
}

/// How the queue treats a request at the door.
#[derive(Debug, Clone, Copy, Default)]
pub struct Admission {
    /// Most requests allowed to wait. `None` = unbounded.
    pub max_queue: Option<usize>,
    /// Most request-body bytes allowed to wait. `None` = unbounded.
    pub max_bytes: Option<usize>,
    /// When full, push out the newest of the least important waiters if the
    /// arriving request is more important.
    pub evict: bool,
    /// Refuse at once when the expected wait is longer than this.
    pub max_wait: Option<Duration>,
    /// Start refusing before the queue is full, less important work first.
    pub shed: Option<Shed>,
    /// Refuse at once while every server that could serve the request is
    /// out after failures, instead of letting it wait for one to come back.
    pub refuse_down: bool,
}

/// Refusing in stages. The least important work starts to be refused when
/// the queue is filled to `from` (a share of `max_queue`), more often the
/// fuller it gets, and always from `ceiling` on. The most important work is
/// refused only when the queue is full; work in between gets thresholds in
/// between. Whatever the fill, work is not refused early while requests of
/// its importance or less hold fewer than `floor` (a share of `max_queue`)
/// places, so the least important work keeps a foothold under heavy load.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Shed {
    pub from: f64,
    pub ceiling: f64,
    pub floor: f64,
}

impl Shed {
    /// How likely a request is refused: `pos` is 0 for the least and 1 for
    /// the most important work, `fill` the share of the queue taken.
    pub fn probability(&self, pos: f64, fill: f64) -> f64 {
        if pos >= 1.0 {
            return 0.0;
        }
        let start = self.from + (1.0 - self.from) * pos;
        let ceiling = self.ceiling + (1.0 - self.ceiling) * pos;
        ((fill - start) / (ceiling - start).max(1e-9)).clamp(0.0, 1.0)
    }
}

/// A request's place in the queue, for asking how it is doing.
#[derive(Debug, Clone)]
pub struct Place {
    pub id: u64,
    tenant: String,
    weight: f64,
    rank: Rank,
}

/// Permission to run one upstream request. Dropping it frees the seat.
pub struct Lease {
    sched: Arc<Scheduler>,
    agent: String,
    tenant: String,
    run: String,
    backend: usize,
    est: f64,
    since: Instant,
    model: Option<String>,
    /// Estimated prompt tokens the backend already held, and the prompt's
    /// size, when the prompt was readable.
    warm: Option<(u32, u32)>,
    /// What was believed about the backend's cache when this was routed.
    belief: Option<Belief>,
    /// What the server said it reused and read (prompt tokens).
    cache: Option<(u64, u64)>,
    tokens: f64,
    /// (output tokens, generation ms, ttft ms, ok, real weighted cost)
    outcome: Option<(f64, f64, f64, bool, Option<f64>)>,
}

impl Lease {
    /// Index of the backend this request was routed to.
    pub fn backend(&self) -> Option<usize> {
        (self.sched.backend_urls.len() > 1 || self.sched.routed()).then_some(self.backend)
    }
    /// Base URL to send to, when the upstream lists its servers.
    pub fn backend_url(&self) -> Option<&str> {
        if !self.sched.routed() {
            return None;
        }
        self.sched
            .backend_urls
            .get(self.backend)
            .map(String::as_str)
    }
    pub fn backend_name(&self) -> Option<String> {
        if !self.sched.routed() {
            return None;
        }
        let st = self.sched.state.lock().unwrap();
        st.backends.get(self.backend).map(|b| b.name.clone())
    }
    /// The model to ask for instead of the one requested, when a stand-in
    /// server took the request.
    pub fn model(&self) -> Option<&str> {
        self.model.as_deref()
    }
    /// Estimated prompt tokens the chosen backend's prefix cache already
    /// held, and the size of the prompt. `None` when the prompt was not
    /// readable or the upstream has a single server.
    pub fn warm(&self) -> Option<(u32, u32)> {
        self.warm
    }
    /// Pass on the server's own count of prompt tokens it reused and read,
    /// so the gateway's picture of that server's cache can be corrected.
    pub fn cache_report(&mut self, cached: u64, fresh: u64) {
        self.cache = Some((cached, fresh));
    }
    /// Report how the request went, so the backend's limit, its health and
    /// the tenant's share use real numbers. `real_cost` is in weighted
    /// tokens.
    pub fn record(
        &mut self,
        out_tokens: f64,
        gen_ms: f64,
        ttft_ms: f64,
        ok: bool,
        real_cost: Option<f64>,
    ) {
        self.outcome = Some((out_tokens, gen_ms, ttft_ms, ok, real_cost));
    }
    /// The server could not be reached or answered with a server error
    /// before any byte: counts against its health.
    pub fn failed(&mut self) {
        self.outcome = Some((0.0, 0.0, 0.0, false, None));
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        let now = now_ms();
        let held = self.since.elapsed().as_secs_f64() * 1000.0;
        let fair = self.sched.opts.fair;
        let mut guard = self.sched.state.lock().unwrap();
        let st = &mut *guard;
        st.q.finish(&self.tenant, &self.run);
        st.q.used(&self.tenant, held, now);
        if let Some(b) = st.backends.get_mut(self.backend) {
            b.tokens = (b.tokens - self.tokens).max(0.0);
        }
        if let (Some(was), Some((cached, fresh))) = (self.belief, self.cache) {
            st.prefix.observe(self.backend, was, cached, fresh);
        }
        if let Some((tokens, gen_ms, ttft, ok, real)) = self.outcome {
            if let (true, Some(real)) = (fair, real) {
                st.q.settle(&self.tenant, &self.run, real - self.est);
            }
            if ok {
                st.service_ms = if st.service_ms > 0.0 {
                    st.service_ms + 0.2 * (held - st.service_ms)
                } else {
                    held
                };
            }
            if let Some(b) = st.backends.get_mut(self.backend) {
                b.finish(now, tokens, gen_ms, ttft, ok);
                b.end(now, self.est, held, Some(ok));
                if !ok && now < b.cool_until_ms {
                    // Taken out: assume it comes back with an empty cache.
                    st.prefix.clear(self.backend);
                }
            }
        } else if let Some(b) = st.backends.get_mut(self.backend) {
            // Nothing learned about the server: the caller left, or the
            // request never reached it.
            b.finish(now, 0.0, 0.0, 0.0, true);
            b.end(now, self.est, held, None);
        }
        if let Some(n) = st.agent_inflight.get_mut(&self.agent) {
            *n = n.saturating_sub(1);
            if *n == 0 {
                st.agent_inflight.remove(&self.agent);
            }
        }
        st.health_sync(&self.sched.name, now, "requests failed");
        drop(guard);
        self.sched.notify.notify_one();
    }
}

#[derive(Default)]
pub struct Ticket {
    pub weight: u32,
    pub max_inflight: Option<usize>,
    /// Estimated input tokens, for rate-limit pacing.
    pub cost: u32,
    /// Size of the request body, for the queue's byte cap.
    pub bytes: usize,
    /// Team, run, class, model and cache hints. Unset = an ordinary request
    /// of the agent.
    pub fair: Option<FairTicket>,
}

#[derive(Debug, Serialize)]
pub struct BackendSnapshot {
    pub name: String,
    pub inflight: usize,
    pub limit: usize,
    pub adaptive: bool,
    pub served: u64,
    pub cooling: bool,
    /// Estimated cost of the requests in flight.
    pub work: f64,
    /// Times in a row the server was taken out of rotation.
    pub fails: u32,
    /// Prompts the gateway believes the server's prefix cache holds.
    pub prefix_entries: usize,
    /// How many it is believed to keep (configured, then corrected from
    /// the server's own reports), and how often that was corrected.
    pub prefix_slots: usize,
    pub prefix_corrections: u64,
    /// Requests in flight and nothing finished for `stale_after`.
    pub stale: bool,
    /// `up`, `down` (out after failures) or `trial` (out, and due one
    /// request to see whether it is back).
    pub health: &'static str,
    /// When it went out (Unix ms); 0 = it is in.
    pub down_since_ms: i64,
    /// Milliseconds until it is tried again.
    pub retry_in_ms: i64,
    /// What was last seen wrong with it.
    pub last_error: String,
    pub probes: u64,
    pub probe_fails: u64,
    /// When it was last probed (Unix ms); 0 = never.
    pub last_probe_ms: i64,
}

#[derive(Debug, Serialize)]
pub struct SchedSnapshot {
    pub max_concurrency: usize,
    pub inflight: usize,
    pub queued: usize,
    pub queued_bytes: usize,
    pub cooldown_ms: u64,
    pub agents: HashMap<String, AgentSnapshot>,
    pub policy: &'static str,
    pub backends: Vec<BackendSnapshot>,
    /// Servers out after failures (counts the upstream itself when it has
    /// no backend list).
    pub servers_down: usize,
    /// Requests that went ahead of the fair order to keep a cache entry.
    pub cache_jumps: u64,
    /// Waiting requests pushed out to make room for more important ones.
    pub evicted: u64,
    /// Requests refused before the queue was full (staged refusal).
    pub shed_early: u64,
    /// Batch requests held beside the queue (included in `queued`), and
    /// the bytes of their bodies.
    pub held: usize,
    pub held_bytes: usize,
    /// Batch requests refused because as many as allowed were held.
    pub hold_refused: u64,
    /// Models paused after a rate-limit answer.
    pub models_cooling: Vec<String>,
    /// `off`, `run` or `prefix`.
    pub affinity: &'static str,
    pub prefix_routing: AffinityStats,
}

#[derive(Debug, Serialize)]
pub struct AgentSnapshot {
    pub inflight: usize,
    pub queued: usize,
}

impl Scheduler {
    /// Round robin between agents over one server.
    pub fn spawn(name: String, max_concurrency: usize, limiter: RateLimiter) -> Arc<Self> {
        Self::spawn_with(name, max_concurrency, limiter, false)
    }

    pub fn spawn_with(
        name: String,
        max_concurrency: usize,
        limiter: RateLimiter,
        strict: bool,
    ) -> Arc<Self> {
        let spec = BackendSpec {
            name: name.clone(),
            url: String::new(),
            limit: max_concurrency,
            adaptive: false,
            models: Vec::new(),
            prefix_slots: 0,
        };
        Self::spawn_opts(
            name,
            vec![spec],
            limiter,
            Options {
                strict,
                ..Options::default()
            },
        )
    }

    /// The fair policy over one or more backends.
    pub fn spawn_fair(
        name: String,
        backends: Vec<BackendSpec>,
        limiter: RateLimiter,
        cfg: FairCfg,
        spill: f64,
    ) -> Arc<Self> {
        Self::spawn_opts(
            name,
            backends,
            limiter,
            Options {
                fair: true,
                fair_cfg: cfg,
                spill,
                ..Options::default()
            },
        )
    }

    pub fn spawn_opts(
        name: String,
        backends: Vec<BackendSpec>,
        limiter: RateLimiter,
        opts: Options,
    ) -> Arc<Self> {
        let now = now_ms();
        let states = backends
            .iter()
            .map(|b| {
                let ctl = b.adaptive.then(|| AdaptiveCfg {
                    ttft_gradient: opts.slowdown,
                    ..AdaptiveCfg::new(b.limit)
                });
                let mut s = BackendState::new(&b.name, b.limit, ctl, b.models.clone(), now);
                s.stale_ms = opts.stale_after.as_millis() as i64;
                s
            })
            .collect();
        let slots: Vec<usize> = backends
            .iter()
            .map(|b| {
                if b.prefix_slots > 0 {
                    b.prefix_slots
                } else {
                    b.limit
                }
            })
            .collect();
        let prefix = PrefixMap::new(&slots, opts.prefix_ttl.as_millis() as i64);
        let used_half_life_ms = opts.used_half_life.as_millis() as i64;
        let cfg = if opts.fair {
            FairCfg {
                used_half_life_ms,
                ..opts.fair_cfg
            }
        } else {
            // Round robin is the same queue counting requests instead of
            // tokens, one run per agent, nothing jumping ahead.
            FairCfg {
                tenant_fair: true,
                las: false,
                jump: false,
                starve_ms: i64::MAX,
                used_half_life_ms,
                ..FairCfg::default()
            }
        };
        let s = Arc::new(Self {
            name,
            state: Mutex::new(State {
                q: FairQueue::new(cfg),
                waiters: HashMap::new(),
                next_id: 0,
                agent_inflight: HashMap::new(),
                agent_cap: HashMap::new(),
                backends: states,
                jumps: 0,
                bytes: 0,
                grants: VecDeque::new(),
                service_ms: 0.0,
                seen: None,
                credit: 0.0,
                model_cool: HashMap::new(),
                evicted: 0,
                shed_early: 0,
                held: 0,
                held_bytes: 0,
                hold_refused: 0,
                ranks: std::collections::BTreeSet::new(),
                shed_owed: HashMap::new(),
                prefix,
                aff: AffinityStats::default(),
                health: vec![Health::default(); backends.len()],
                events: VecDeque::new(),
                event_seq: 0,
            }),
            notify: Notify::new(),
            limiter: Mutex::new(limiter),
            opts,
            backend_urls: backends.into_iter().map(|b| b.url).collect(),
        });
        // The dispatcher holds a weak handle so the scheduler can be dropped.
        let weak = Arc::downgrade(&s);
        tokio::spawn(async move { dispatch(weak).await });
        s
    }

    pub fn is_fair(&self) -> bool {
        self.opts.fair
    }

    /// Whether a request of this class (0 interactive, 1 normal, 2 batch)
    /// is held beside the queue instead of taking a place in it.
    pub fn holds(&self, class: u8) -> bool {
        self.opts.hold.is_some() && class >= 2
    }

    /// Whether leases name the server to send to.
    fn routed(&self) -> bool {
        self.backend_urls.iter().any(|u| !u.is_empty())
    }

    /// Keep new work away from a backend for a while (it refused a
    /// connection). Repeated failures keep it out longer; `cool` is the
    /// shortest pause.
    pub fn fail_backend(&self, idx: usize, cool: Duration) {
        let now = now_ms();
        let mut st = self.state.lock().unwrap();
        st.prefix.clear(idx);
        if let Some(b) = st.backends.get_mut(idx) {
            b.fail(now);
            b.cool_until_ms = b.cool_until_ms.max(now + cool.as_millis() as i64);
        }
        st.health_sync(&self.name, now, "could not connect");
        drop(st);
        self.notify.notify_one();
    }

    /// The servers a health probe should ask: `(index, base URL)`, where
    /// the URL is empty for an upstream without a backend list. With
    /// `only_out`, just the ones out after failures.
    pub fn probe_targets(&self, only_out: bool) -> Vec<(usize, String)> {
        let st = self.state.lock().unwrap();
        st.backends
            .iter()
            .enumerate()
            .filter(|(_, b)| !only_out || b.fails > 0)
            .map(|(i, _)| (i, self.backend_urls[i].clone()))
            .collect()
    }

    /// What a health probe found. A server that answers is put back at
    /// once, held to one request until that succeeds; one that does not is
    /// taken out (or kept out) for at least `hold`, so that while probes run
    /// no caller's request is spent on finding out.
    pub fn probe_result(&self, idx: usize, ok: bool, err: &str, hold: Duration) {
        let now = now_ms();
        let mut guard = self.state.lock().unwrap();
        let st = &mut *guard;
        if idx >= st.backends.len() {
            return;
        }
        st.health[idx].probes += 1;
        st.health[idx].last_probe_ms = now;
        if ok {
            let b = &mut st.backends[idx];
            if b.fails > 0 {
                b.cool_until_ms = b.cool_until_ms.min(now);
            }
            let since = st.health[idx].down_since_ms;
            if since != 0 {
                st.health[idx].down_since_ms = 0;
                st.health_event(&self.name, idx, now, "up", "probe answered", now - since);
            }
        } else {
            st.health[idx].probe_fails += 1;
            st.health[idx].last_error = err.to_string();
            if st.backends[idx].fails == 0 {
                st.backends[idx].fail(now);
                st.prefix.clear(idx);
            }
            let b = &mut st.backends[idx];
            b.cool_until_ms = b.cool_until_ms.max(now + hold.as_millis() as i64);
            let why = format!("probe failed: {err}");
            st.health_sync(&self.name, now, &why);
        }
        drop(guard);
        self.notify.notify_one();
    }

    /// Servers that went out or came back after event number `after`.
    pub fn health_events(&self, after: u64) -> Vec<HealthEvent> {
        let st = self.state.lock().unwrap();
        st.events
            .iter()
            .filter(|e| e.seq > after)
            .cloned()
            .collect()
    }

    /// The upstream said to slow down. With one server and no model named
    /// the whole upstream pauses; otherwise only the server, or the model on
    /// it, that said so.
    pub fn slow_down(&self, backend: Option<usize>, model: &str, d: Duration) {
        let now = now_ms();
        let until = now + d.as_millis() as i64;
        let mut st = self.state.lock().unwrap();
        let many = st.backends.len() > 1;
        if let Some(b) = backend.and_then(|i| st.backends.get_mut(i)) {
            if let Some(c) = b.ctl.as_mut() {
                c.overloaded(now);
            }
            if many && model.is_empty() {
                b.pause(now, d.as_millis() as i64);
                drop(st);
                self.notify.notify_one();
                return;
            }
        }
        if !model.is_empty() {
            let e = st.model_cool.entry(model.to_string()).or_insert(0);
            *e = (*e).max(until);
            drop(st);
            self.notify.notify_one();
            return;
        }
        drop(st);
        self.cooldown(d);
    }

    fn warmth<'a>(&self, st: &'a State) -> Warmth<'a> {
        self.warmth_of(st.backends.len(), &st.prefix)
    }

    fn warmth_of<'a>(&self, backends: usize, prefix: &'a PrefixMap) -> Warmth<'a> {
        Warmth {
            mode: self.opts.affinity,
            // One server: nothing to choose between.
            map: (backends > 1).then_some(prefix),
            min_tokens: self.opts.affinity_min_tokens,
            wait_ms: self.opts.affinity_wait.as_millis() as i64,
            load_weight: self.opts.load_weight,
            slot_hold_ms: self.opts.slot_hold.as_millis() as i64,
            slot_max_wait_ms: self.opts.slot_max_wait.as_millis() as i64,
        }
    }

    /// Reorders inside one caller's own queue so a warm cache is used
    /// while it is there. Two cases: batch requests are served kind by
    /// kind (while the next one would start cold, a later one whose prompt
    /// some free server holds goes first); and when the next request is
    /// being held back (session admission, or waiting for its warm server),
    /// a later one that can run warm right now goes first instead of
    /// waiting behind it. Without the second, one caller with several
    /// conversations stalls behind its own held request.
    fn regroup(&self, st: &mut State, now: i64) {
        if !self.wants_prefix() {
            return;
        }
        let limit = self.opts.batch_group.as_millis() as i64;
        let holds = !self.opts.slot_hold.is_zero() || !self.opts.affinity_wait.is_zero();
        if limit <= 0 && !holds {
            return;
        }
        let State {
            q,
            backends,
            prefix,
            model_cool,
            aff,
            ..
        } = st;
        let warmth = self.warmth_of(backends.len(), prefix);
        let cool = |m: &str| model_cool.get(m).is_some_and(|&t| now < t);
        let routed = |it: &Item| route(backends, it, now, self.opts.spill, &warmth, &cool);
        let warm = |it: &Item| routed(it).is_some_and(|r| r.warm > 0);
        if limit > 0 {
            let warm = |it: &Item| it.class == 2 && warm(it);
            let yields = |it: &Item| it.class == 2 && now - it.enq_ms < limit && !warm(it);
            aff.regrouped += q.regroup(64, &yields, &warm) as u64;
        }
        if holds {
            let held = |it: &Item| routed(it).is_none();
            aff.regrouped += q.regroup(64, &held, &warm) as u64;
        }
    }

    /// Whether any server that serves `model` can take work now or will
    /// without being repaired: false when all of them are out after
    /// failures.
    pub fn any_server_up(&self, model: &str) -> bool {
        let now = now_ms();
        let st = self.state.lock().unwrap();
        st.backends
            .iter()
            .any(|b| b.serves(model) && (b.fails == 0 || now >= b.cool_until_ms))
    }

    /// Whether requests to this upstream should carry their prompt chain.
    pub fn wants_prefix(&self) -> bool {
        self.opts.affinity == Affinity::Prefix && self.backend_urls.len() > 1
    }

    fn eligible(&self, st: &State, it: &Item, now: i64) -> bool {
        let used = st.agent_inflight.get(&it.agent).copied().unwrap_or(0);
        if used >= st.agent_cap.get(&it.agent).copied().unwrap_or(usize::MAX) {
            return false;
        }
        let cool = |m: &str| st.model_cool.get(m).is_some_and(|&t| now < t);
        if route(
            &st.backends,
            it,
            now,
            self.opts.spill,
            &self.warmth(st),
            &cool,
        )
        .is_none()
        {
            return false;
        }
        if self.opts.reserve > 0 {
            // Seats are kept back from this request if something more
            // important has been arriving.
            let outranked = st.seen.is_some_and(|(r, at)| {
                r < rank_of(it.class, it.weight) && at.elapsed() < RESERVE_MEMORY
            });
            if outranked {
                let free: usize = st
                    .backends
                    .iter()
                    .filter(|b| now >= b.cool_until_ms)
                    .map(|b| b.limit().saturating_sub(b.inflight))
                    .sum();
                let total: usize = st.backends.iter().map(BackendState::limit).sum();
                // Never keep every seat back.
                if free <= self.opts.reserve.min(total.saturating_sub(1)) {
                    return false;
                }
            }
        }
        true
    }

    /// One dispatch attempt.
    fn step(self: &Arc<Self>) -> Step {
        let now = now_ms();
        let mut guard = self.state.lock().unwrap();
        loop {
            let st = &mut *guard;
            st.model_cool.retain(|_, t| now < *t);
            st.prefix.expire(now);
            self.regroup(st, now);
            let pick = if self.opts.strict {
                let ok = |it: &Item| self.eligible(st, it, now);
                let top =
                    st.q.fronts()
                        .filter(|it| ok(it))
                        .map(|it| rank_of(it.class, it.weight))
                        .min();
                let Some(top) = top else {
                    return Step::Idle;
                };
                let owed = (st.credit >= 1.0)
                    .then(|| {
                        // The guaranteed share is for work in the queue,
                        // not for held work.
                        st.q.peek(now, &|it: &Item| {
                            rank_of(it.class, it.weight) > top && !self.holds(it.class) && ok(it)
                        })
                    })
                    .flatten();
                match owed {
                    Some(p) => Some((p, true)),
                    None => {
                        st.q.peek(now, &|it: &Item| {
                            rank_of(it.class, it.weight) == top && ok(it)
                        })
                        .map(|p| (p, false))
                    }
                }
            } else {
                // Held work goes only when nothing else can.
                self.opts
                    .hold
                    .and_then(|_| {
                        st.q.peek(now, &|it: &Item| {
                            !self.holds(it.class) && self.eligible(st, it, now)
                        })
                    })
                    .or_else(|| st.q.peek(now, &|it: &Item| self.eligible(st, it, now)))
                    .map(|p| (p, false))
            };
            let Some((pick, owed)) = pick else {
                return Step::Idle;
            };
            let Some(w) = st.waiters.get(&pick.id) else {
                st.q.remove(&pick.tenant, &pick.run, pick.id);
                continue;
            };
            if w.tx.is_closed() {
                st.q.remove(&pick.tenant, &pick.run, pick.id);
                st.unlist(pick.id);
                continue;
            }
            let delay = self.limiter.lock().unwrap().delay(w.cost, Instant::now());
            if !delay.is_zero() {
                return Step::Wait(delay);
            }
            let Some(item) = st.q.take(&pick, now) else {
                return Step::Idle;
            };
            let w = st.unlist(pick.id).expect("checked above");
            let cool = |m: &str| st.model_cool.get(m).is_some_and(|&t| now < t);
            let to = route(
                &st.backends,
                &item,
                now,
                self.opts.spill,
                &self.warmth(st),
                &cool,
            )
            .expect("peek accepted it");
            let (b, model) = (to.backend, to.model);
            let mut warm = None;
            let mut belief = None;
            if let (Some(chain), true) = (&item.prefix, self.wants_prefix()) {
                warm = Some((to.warm, to.total));
                if model.is_none() {
                    belief = Some(st.prefix.belief(b, chain));
                }
                st.aff.prompt_tokens += u64::from(to.total);
                if to.warm > 0 {
                    st.aff.warm += 1;
                    st.aff.warm_tokens += u64::from(to.warm);
                } else if to.had_warm {
                    st.aff.spilled += 1;
                } else {
                    st.aff.cold += 1;
                }
                if model.is_none() {
                    // The server will hold this prompt from now on.
                    st.prefix.note(b, chain, now);
                }
            }
            st.backends[b].begin(now, item.cost);
            st.backends[b].tokens += f64::from(item.tokens);
            *st.agent_inflight.entry(item.agent.clone()).or_default() += 1;
            st.jumps += u64::from(pick.jumped);
            let at = Instant::now();
            st.grants.push_back((at, w.rank));
            while st
                .grants
                .front()
                .is_some_and(|(t, _)| at.duration_since(*t) > RATE_WINDOW)
            {
                st.grants.pop_front();
            }
            if self.opts.strict && self.opts.min_share > 0.0 {
                if owed {
                    st.credit -= 1.0;
                } else if st.waiters.values().any(|o| o.rank > w.rank && !o.held) {
                    // Less important work is waiting: it earns part of a grant.
                    let s = self.opts.min_share.clamp(0.0, 0.5);
                    st.credit = (st.credit + s / (1.0 - s)).min(2.0);
                } else {
                    st.credit = 0.0;
                }
            }
            drop(guard);
            self.limiter.lock().unwrap().take(w.cost, Instant::now());
            let lease = Lease {
                sched: self.clone(),
                agent: item.agent,
                tenant: w.tenant,
                run: w.run,
                backend: b,
                est: item.cost,
                since: at,
                model,
                warm,
                belief,
                cache: None,
                tokens: f64::from(item.tokens),
                outcome: None,
            };
            // A lease nobody receives drops here and frees its seat.
            let _ = w.tx.send(Ok(lease));
            return Step::Granted;
        }
    }

    /// Forget waiters whose caller has gone.
    fn sweep(st: &mut State) {
        let dead: Vec<u64> = st
            .waiters
            .iter()
            .filter(|(_, w)| w.tx.is_closed())
            .map(|(id, _)| *id)
            .collect();
        for id in dead {
            if let Some(w) = st.unlist(id) {
                st.q.remove(&w.tenant, &w.run, id);
            }
        }
    }

    /// Queue a request; the receiver resolves when it may run.
    pub fn enqueue(&self, agent: &str, t: Ticket) -> oneshot::Receiver<Grant> {
        self.try_enqueue(agent, t, Admission::default())
            .expect("an unbounded queue accepts every request")
            .0
    }

    /// Like [`enqueue`](Self::enqueue), but may refuse at the door: when the
    /// request would wait longer than it is willing to, or when the queue is
    /// full and nothing less important can make room. Saying no at once keeps
    /// latency bounded when agents send faster than the upstream can serve.
    pub fn try_enqueue(
        &self,
        agent: &str,
        t: Ticket,
        adm: Admission,
    ) -> Result<(oneshot::Receiver<Grant>, Outlook, Place), Refused> {
        let (tx, rx) = oneshot::channel();
        let weight = t.weight.max(1);
        let mut ft = t.fair.unwrap_or_else(|| FairTicket {
            class: 1,
            ..FairTicket::default()
        });
        if !self.opts.fair || ft.tenant.is_empty() {
            // Round robin: every agent is its own tenant and one run, and a
            // request costs one turn.
            ft.tenant = agent.to_string();
            ft.tenant_weight = f64::from(weight);
            // One line per model, so a model that is paused or whose
            // servers are full does not hold up the agent's other requests.
            ft.run = format!("{agent}\u{1}{}", ft.model);
            ft.deadline_ms = None;
            ft.risk = 0.0;
            if !self.opts.fair {
                ft.cost = 1.0;
            } else {
                ft.cost = f64::from(t.cost.max(1));
            }
        }
        let rank = rank_of(ft.class, weight);
        let held = self.holds(ft.class);
        let mut st = self.state.lock().unwrap();
        let st = &mut *st;
        if !std::iter::once(&ft.model)
            .chain(ft.alts.iter())
            .any(|m| st.backends.iter().any(|b| b.serves(m)))
        {
            return Err(Refused::NoServer);
        }
        if adm.refuse_down {
            let now = now_ms();
            let mut back: Option<i64> = None;
            let mut up = false;
            for b in st.backends.iter().filter(|b| {
                std::iter::once(&ft.model)
                    .chain(ft.alts.iter())
                    .any(|m| b.serves(m))
            }) {
                if b.fails == 0 || now >= b.cool_until_ms {
                    up = true;
                    break;
                }
                let left = b.cool_until_ms - now;
                back = Some(back.map_or(left, |x| x.min(left)));
            }
            if !up {
                return Err(Refused::ServersDown(Duration::from_millis(
                    back.unwrap_or(0).max(0) as u64,
                )));
            }
        }
        Self::sweep(st);
        let place = Place {
            id: st.next_id + 1,
            tenant: ft.tenant.clone(),
            weight: ft.tenant_weight.max(1e-9),
            rank,
        };
        let outlook = self.outlook(st, &place, true);
        if let (true, Some(h)) = (held, self.opts.hold) {
            // Something larger than the byte cap on its own is still held
            // when nothing else is; otherwise it could never run.
            if st.held >= h.max || (st.held > 0 && st.held_bytes + t.bytes > h.max_bytes) {
                st.hold_refused += 1;
                return Err(Refused::HoldFull(outlook));
            }
        }
        if !held && adm.max_wait.is_some_and(|limit| outlook.wait > limit) {
            return Err(Refused::WaitTooLong(outlook));
        }
        // Held work is outside the order staged refusal goes by.
        if !held && st.ranks.len() < 64 {
            st.ranks.insert(rank);
        }
        let shed = adm.shed.filter(|_| !held);
        if let (Some(sh), Some(max)) = (shed, adm.max_queue.filter(|m| *m > 0)) {
            // Where this request stands between the least important work
            // seen (0) and the most important (1).
            let below = st.ranks.range(rank..).count().saturating_sub(1);
            let pos = match st.ranks.len() {
                0 | 1 => 1.0,
                n => below as f64 / (n - 1) as f64,
            };
            let fill = st.in_queue() as f64 / max as f64;
            let p = sh.probability(pos, fill);
            let foothold = (sh.floor * max as f64).ceil() as usize;
            let low = st.waiters.values().filter(|w| w.rank >= rank && !w.held);
            if p > 0.0 && low.count() >= foothold {
                let owed = st.shed_owed.entry(rank).or_insert(0.0);
                *owed += p;
                if *owed >= 1.0 {
                    *owed -= 1.0;
                    st.shed_early += 1;
                    return Err(Refused::Shed(outlook));
                }
            }
        }
        loop {
            // Held work takes no place, so it needs no room made.
            if held {
                break;
            }
            let over_count = adm.max_queue.is_some_and(|m| st.in_queue() >= m);
            // A request larger than the cap on its own is still let in when
            // nothing else is waiting; otherwise it could never run.
            let over_bytes = adm
                .max_bytes
                .is_some_and(|m| st.in_queue() > 0 && st.bytes + t.bytes > m);
            if !over_count && !over_bytes {
                break;
            }
            // Make room by dropping the request that has waited least among
            // the least important ones.
            let victim = st
                .waiters
                .iter()
                .filter(|(_, w)| adm.evict && w.rank > rank && !w.held)
                .max_by_key(|(id, w)| (w.rank, **id))
                .map(|(id, _)| *id);
            let Some(id) = victim else {
                return Err(Refused::Full(outlook));
            };
            let w = st.unlist(id).expect("picked above");
            st.q.remove(&w.tenant, &w.run, id);
            st.evicted += 1;
            let _ = w.tx.send(Err(Evicted));
        }
        st.next_id += 1;
        let id = st.next_id;
        st.agent_cap.insert(
            agent.to_string(),
            t.max_inflight.unwrap_or(usize::MAX).max(1),
        );
        st.q.push(
            &ft.tenant,
            ft.tenant_weight.max(1e-9),
            &ft.run,
            Item {
                id,
                cost: ft.cost.max(1.0),
                enq_ms: now_ms(),
                deadline_ms: ft.deadline_ms,
                risk: ft.risk,
                class: ft.class,
                agent: agent.to_string(),
                model: ft.model,
                affinity: ft.affinity,
                prefix: ft.prefix,
                tokens: t.cost.max(1),
                weight,
                alts: ft.alts,
                bytes: t.bytes,
                avoid: ft.avoid,
            },
        );
        if held {
            st.held += 1;
            st.held_bytes += t.bytes;
        } else {
            st.bytes += t.bytes;
        }
        st.waiters.insert(
            id,
            Waiter {
                tx,
                agent: agent.to_string(),
                tenant: ft.tenant,
                weight: place.weight,
                run: ft.run,
                cost: t.cost,
                rank,
                bytes: t.bytes,
                held,
            },
        );
        let now = Instant::now();
        let stale = st
            .seen
            .is_none_or(|(r, at)| rank <= r || now.duration_since(at) >= RESERVE_MEMORY);
        if stale {
            st.seen = Some((rank, now));
        }
        self.notify.notify_one();
        Ok((rx, outlook, place))
    }

    /// What a waiting request is looking at right now.
    pub fn queue_status(&self, place: &Place) -> Outlook {
        let mut st = self.state.lock().unwrap();
        Self::sweep(&mut st);
        self.outlook(&st, place, false)
    }

    /// Roughly how many requests go before this one and how long that takes.
    ///
    /// The count follows the queue's order. The time is that count divided
    /// by the rate at which requests of this importance have actually been
    /// getting seats lately, measured over two windows: the longer one gives
    /// the estimate, the slower of the two the pessimistic end. Before
    /// anything of this importance has been served, the rate is what the
    /// seats can do minus what more important work has been taking.
    fn outlook(&self, st: &State, me: &Place, arriving: bool) -> Outlook {
        let before = |id: u64| arriving || id < me.id;
        // Held work goes after everything in the queue, and work in the
        // queue never waits for held work.
        let me_held = self.holds(me.rank.0);
        let ahead = if self.opts.strict {
            st.waiters
                .iter()
                .filter(|(id, w)| w.rank < me.rank || (w.rank == me.rank && before(**id)))
                .count() as f64
        } else if me_held {
            st.in_queue() as f64
                + st.waiters
                    .iter()
                    .filter(|(id, w)| w.held && before(**id))
                    .count() as f64
        } else {
            // Shared turns: for each of this tenant's turns the other
            // waiting tenants get theirs, in proportion to their weights.
            let own = st
                .waiters
                .iter()
                .filter(|(id, w)| w.tenant == me.tenant && !w.held && before(**id))
                .count() as f64;
            let mut others: HashMap<&str, (f64, f64)> = HashMap::new();
            let queued = st.waiters.values().filter(|w| !w.held);
            for w in queued.filter(|w| w.tenant != me.tenant) {
                let e = others.entry(w.tenant.as_str()).or_insert((0.0, w.weight));
                e.0 += 1.0;
            }
            own + others
                .values()
                .map(|(n, w)| n.min((own + 1.0) * w / me.weight))
                .sum::<f64>()
        };
        let now_wall = now_ms();
        let seats: usize = st.backends.iter().map(BackendState::limit).sum();
        let free = st
            .backends
            .iter()
            .any(|b| b.free(now_wall) && b.limit() - b.inflight > self.reserved_from(st, me.rank));
        if ahead < 0.5 && free {
            return Outlook::default();
        }
        if st.service_ms <= 0.0 {
            // Nothing measured yet: say how many, not how long.
            return Outlook {
                ahead: ahead.round() as usize,
                ..Outlook::default()
            };
        }
        let capacity = seats.max(1) as f64 / (st.service_ms / 1000.0);
        let now = Instant::now();
        let rate_over = |window: Duration| -> f64 {
            let secs = window.as_secs_f64();
            let (mut mine, mut better) = (0.0, 0.0);
            for (at, r) in &st.grants {
                if now.duration_since(*at) > window {
                    continue;
                }
                if !self.opts.strict || *r >= me.rank {
                    mine += 1.0;
                } else {
                    better += 1.0;
                }
            }
            let top = !self.opts.strict || !st.waiters.values().any(|w| w.rank < me.rank);
            if top && better == 0.0 {
                // Nothing goes before this kind of work: every seat serves it.
                return capacity.max(mine / secs);
            }
            let left = (capacity - better / secs).max(0.05 * capacity);
            if mine >= 3.0 {
                // Requests like this one have been getting through at this
                // pace, whatever the model above says.
                mine / secs
            } else {
                left
            }
        };
        let (long, short) = (rate_over(RATE_WINDOW), rate_over(RATE_WINDOW_SHORT));
        let secs = |rate: f64| (ahead + 1.0) / rate.max(1e-6);
        let wait = secs(long);
        let wait_max = wait.max(secs(short));
        let cap = |s: f64| Duration::from_secs_f64(s.clamp(0.0, 86_400.0));
        Outlook {
            ahead: ahead.round() as usize,
            wait: cap(wait),
            wait_max: cap(wait_max),
        }
    }

    /// Seats kept back from a request of this rank right now.
    fn reserved_from(&self, st: &State, rank: Rank) -> usize {
        let outranked = st
            .seen
            .is_some_and(|(r, at)| r < rank && at.elapsed() < RESERVE_MEMORY);
        if outranked {
            self.opts.reserve
        } else {
            0
        }
    }

    /// Pause dispatch for `d` (never shortens an existing pause).
    pub fn cooldown(&self, d: Duration) {
        self.limiter.lock().unwrap().cooldown(d, Instant::now());
        self.notify.notify_one();
    }

    /// Every server behind the upstream with its load and health; an
    /// upstream without a backend list is one server under its own name.
    pub fn servers(&self) -> Vec<BackendSnapshot> {
        let st = self.state.lock().unwrap();
        Self::backend_snaps(&st, now_ms())
    }

    fn backend_snaps(st: &State, now: i64) -> Vec<BackendSnapshot> {
        st.backends
            .iter()
            .enumerate()
            .map(|(i, b)| BackendSnapshot {
                prefix_entries: st.prefix.entries(i),
                prefix_slots: st.prefix.slots(i),
                prefix_corrections: st.prefix.corrections(i),
                stale: b.stale(now),
                name: b.name.clone(),
                inflight: b.inflight,
                limit: b.limit(),
                adaptive: b.ctl.is_some(),
                served: b.served,
                cooling: now < b.cool_until_ms,
                work: b.work,
                fails: b.fails,
                health: if b.fails == 0 {
                    "up"
                } else if now < b.cool_until_ms {
                    "down"
                } else {
                    "trial"
                },
                down_since_ms: st.health[i].down_since_ms,
                retry_in_ms: if b.fails > 0 {
                    (b.cool_until_ms - now).max(0)
                } else {
                    0
                },
                last_error: st.health[i].last_error.clone(),
                probes: st.health[i].probes,
                probe_fails: st.health[i].probe_fails,
                last_probe_ms: st.health[i].last_probe_ms,
            })
            .collect()
    }

    pub fn snapshot(&self) -> SchedSnapshot {
        let now = now_ms();
        let mut st = self.state.lock().unwrap();
        Self::sweep(&mut st);
        let mut agents: HashMap<String, AgentSnapshot> = HashMap::new();
        let blank = || AgentSnapshot {
            inflight: 0,
            queued: 0,
        };
        for (a, n) in &st.agent_inflight {
            agents.entry(a.clone()).or_insert_with(blank).inflight = *n;
        }
        for w in st.waiters.values() {
            agents.entry(w.agent.clone()).or_insert_with(blank).queued += 1;
        }
        let backends = Self::backend_snaps(&st, now);
        let mut models_cooling: Vec<String> = st
            .model_cool
            .iter()
            .filter(|(_, t)| now < **t)
            .map(|(m, _)| m.clone())
            .collect();
        models_cooling.sort();
        let snap = SchedSnapshot {
            max_concurrency: backends.iter().map(|b| b.limit).sum(),
            inflight: backends.iter().map(|b| b.inflight).sum(),
            queued: st.waiters.len(),
            queued_bytes: st.bytes,
            cooldown_ms: 0,
            agents,
            policy: if self.opts.fair {
                "fair"
            } else {
                "round_robin"
            },
            servers_down: backends.iter().filter(|b| b.health == "down").count(),
            // One unnamed server is the upstream itself, not a backend list.
            backends: if self.routed() { backends } else { Vec::new() },
            cache_jumps: st.jumps,
            evicted: st.evicted,
            shed_early: st.shed_early,
            held: st.held,
            held_bytes: st.held_bytes,
            hold_refused: st.hold_refused,
            models_cooling,
            affinity: self.opts.affinity.as_str(),
            prefix_routing: st.aff,
        };
        drop(st);
        SchedSnapshot {
            cooldown_ms: self
                .limiter
                .lock()
                .unwrap()
                .cooldown_remaining(Instant::now())
                .as_millis() as u64,
            ..snap
        }
    }
}

async fn dispatch(weak: std::sync::Weak<Scheduler>) {
    loop {
        let Some(s) = weak.upgrade() else { return };
        match s.step() {
            Step::Granted => {}
            Step::Idle => {
                // Wake on new work or a freed seat; the timeout covers
                // server cooldowns and jump windows, which pass silently.
                let _ = tokio::time::timeout(Duration::from_millis(250), s.notify.notified()).await;
            }
            Step::Wait(d) => {
                drop(s);
                tokio::time::sleep(d).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ticket(weight: u32) -> Ticket {
        Ticket {
            weight,
            ..Ticket::default()
        }
    }

    fn classed(weight: u32, class: u8) -> Ticket {
        Ticket {
            weight,
            fair: Some(FairTicket {
                class,
                ..FairTicket::default()
            }),
            ..Ticket::default()
        }
    }

    async fn lease(rx: oneshot::Receiver<Grant>) -> Lease {
        rx.await.unwrap().unwrap()
    }

    /// Queue `n` requests per agent, run them one at a time and return the
    /// order the agents were served in.
    async fn order(s: &Arc<Scheduler>, agents: &[(&'static str, Ticket, usize)]) -> Vec<String> {
        let (tx, mut out) = tokio::sync::mpsc::unbounded_channel();
        // Hold the only seat so everything queues before anything runs.
        let hold = lease(s.enqueue("hold", ticket(1))).await;
        for (a, t, n) in agents {
            for _ in 0..*n {
                let rx = s.enqueue(
                    a,
                    Ticket {
                        weight: t.weight,
                        fair: t.fair.clone(),
                        ..Ticket::default()
                    },
                );
                let (tx, a) = (tx.clone(), a.to_string());
                tokio::spawn(async move {
                    let l = lease(rx).await;
                    tx.send(a).unwrap();
                    drop(l);
                });
            }
        }
        tokio::task::yield_now().await;
        drop(hold);
        drop(tx);
        let mut got = Vec::new();
        while let Some(a) = out.recv().await {
            got.push(a);
        }
        got
    }

    #[tokio::test]
    async fn round_robin_across_agents() {
        let s = Scheduler::spawn("t".into(), 1, RateLimiter::new(None, None));
        let got = order(&s, &[("a", ticket(1), 3), ("b", ticket(1), 3)]).await;
        // Neither agent ever gets two turns more than the other.
        let mut diff = 0i32;
        for a in &got {
            diff += if a == "a" { 1 } else { -1 };
            assert!(diff.abs() <= 1, "{got:?}");
        }
    }

    #[tokio::test]
    async fn weights_give_proportional_turns() {
        let s = Scheduler::spawn("t".into(), 1, RateLimiter::new(None, None));
        let got = order(&s, &[("a", ticket(3), 9), ("b", ticket(1), 9)]).await;
        let a = got.iter().take(8).filter(|x| *x == "a").count();
        assert_eq!(a, 6, "{got:?}");
    }

    #[tokio::test]
    async fn per_agent_cap_lets_others_through() {
        let s = Scheduler::spawn("t".into(), 4, RateLimiter::new(None, None));
        let capped = || Ticket {
            weight: 1,
            max_inflight: Some(1),
            ..Ticket::default()
        };
        let a1 = lease(s.enqueue("a", capped())).await;
        let mut a2 = s.enqueue("a", capped());
        let b1 = lease(s.enqueue("b", ticket(1))).await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(a2.try_recv().is_err(), "a is at its own cap");
        drop(a1);
        let _a2 = lease(a2).await;
        drop(b1);
    }

    #[tokio::test]
    async fn cancelled_waiters_do_not_consume_slots() {
        let s = Scheduler::spawn("t".into(), 1, RateLimiter::new(None, None));
        let first = lease(s.enqueue("a", ticket(1))).await;
        drop(s.enqueue("a", ticket(1)));
        let third = s.enqueue("b", ticket(1));
        drop(first);
        let _l = lease(third).await;
        assert_eq!(s.snapshot().queued, 0);
    }

    fn cap(max: usize) -> Admission {
        Admission {
            max_queue: Some(max),
            ..Admission::default()
        }
    }

    #[tokio::test]
    async fn bounded_queue_refuses_when_full() {
        let s = Scheduler::spawn("t".into(), 1, RateLimiter::new(None, None));
        let _run = lease(s.enqueue("a", ticket(1))).await;
        let _w1 = s.try_enqueue("a", ticket(1), cap(2)).unwrap();
        let w2 = s.try_enqueue("b", ticket(1), cap(2)).unwrap();
        assert!(matches!(
            s.try_enqueue("c", ticket(1), cap(2)),
            Err(Refused::Full(_))
        ));
        // A waiter that left makes room.
        drop(w2);
        assert!(s.try_enqueue("c", ticket(1), cap(2)).is_ok());
    }

    fn staged(max: usize, floor: f64) -> Admission {
        Admission {
            max_queue: Some(max),
            shed: Some(Shed {
                from: 0.25,
                ceiling: 0.75,
                floor,
            }),
            ..Admission::default()
        }
    }

    #[tokio::test]
    async fn a_filling_queue_refuses_less_important_work_first() {
        let s = Scheduler::spawn("t".into(), 1, RateLimiter::new(None, None));
        let _run = lease(s.enqueue("x", ticket(1))).await;
        let adm = staged(20, 0.0);
        let mut held = Vec::new();
        // Below a quarter full nobody is refused.
        for _ in 0..2 {
            held.push(s.try_enqueue("low", ticket(1), adm).unwrap());
            held.push(s.try_enqueue("high", ticket(10), adm).unwrap());
        }
        // Between the thresholds the less important work is refused some of
        // the time: at half full, every other request.
        while s.snapshot().queued < 10 {
            held.push(s.try_enqueue("high", ticket(10), adm).unwrap());
        }
        let refused = (0..4)
            .filter(|_| {
                let r = s.try_enqueue("low", ticket(1), adm);
                let no = matches!(r, Err(Refused::Shed(_)));
                drop(r); // leave the queue as it was
                no
            })
            .count();
        assert_eq!(refused, 2);
        // From three quarters on, always; the most important work still gets
        // in until the queue is full.
        while s.snapshot().queued < 15 {
            held.push(s.try_enqueue("high", ticket(10), adm).unwrap());
        }
        for _ in 0..3 {
            assert!(matches!(
                s.try_enqueue("low", ticket(1), adm),
                Err(Refused::Shed(_))
            ));
        }
        while s.snapshot().queued < 20 {
            held.push(s.try_enqueue("high", ticket(10), adm).unwrap());
        }
        assert!(matches!(
            s.try_enqueue("high", ticket(10), adm),
            Err(Refused::Full(_))
        ));
        assert_eq!(s.snapshot().shed_early, 5);
    }

    #[tokio::test]
    async fn the_least_important_work_keeps_a_foothold() {
        let s = Scheduler::spawn("t".into(), 1, RateLimiter::new(None, None));
        let _run = lease(s.enqueue("x", ticket(1))).await;
        // A tenth of 20 places: two.
        let adm = staged(20, 0.1);
        let mut held = vec![s.try_enqueue("low", ticket(1), adm).unwrap()];
        while s.snapshot().queued < 17 {
            held.push(s.try_enqueue("high", ticket(10), adm).unwrap());
        }
        // Well past the ceiling, but only one request of its kind waits.
        held.push(s.try_enqueue("low", ticket(1), adm).unwrap());
        assert!(matches!(
            s.try_enqueue("low", ticket(1), adm),
            Err(Refused::Shed(_))
        ));
        // One of them leaves (served or gone): the place is open again.
        held.pop();
        assert!(s.try_enqueue("low", ticket(1), adm).is_ok());
    }

    #[tokio::test]
    async fn the_caller_that_used_least_is_served_first() {
        let run = |half_life: u64| async move {
            let s = Scheduler::spawn_opts(
                "t".into(),
                vec![BackendSpec {
                    name: "b".into(),
                    url: String::new(),
                    limit: 1,
                    adaptive: false,
                    models: Vec::new(),
                    prefix_slots: 0,
                }],
                RateLimiter::new(None, None),
                Options {
                    used_half_life: Duration::from_secs(half_life),
                    ..Options::default()
                },
            );
            let first = lease(s.enqueue("heavy", ticket(1))).await;
            let mut heavy = s.enqueue("heavy", ticket(1));
            tokio::time::sleep(Duration::from_millis(60)).await;
            let mut light = s.enqueue("light", ticket(1));
            drop(first);
            tokio::time::sleep(Duration::from_millis(30)).await;
            (heavy.try_recv().is_ok(), light.try_recv().is_ok())
        };
        // Taking turns: heavy asked first and both have had one turn or none.
        assert_eq!(run(0).await, (true, false));
        // Least used first: heavy just held the server, light has not.
        assert_eq!(run(60).await, (false, true));
    }

    #[tokio::test]
    async fn full_queue_evicts_the_newest_least_important_waiter() {
        let s = Scheduler::spawn("t".into(), 1, RateLimiter::new(None, None));
        let _run = lease(s.enqueue("x", ticket(1))).await;
        let adm = Admission {
            evict: true,
            ..cap(2)
        };
        let (mut old, ..) = s.try_enqueue("bulk", classed(1, 2), adm).unwrap();
        let (new, ..) = s.try_enqueue("bulk", classed(1, 2), adm).unwrap();
        // Work of the same importance does not push anything out.
        assert!(matches!(
            s.try_enqueue("bulk2", classed(1, 2), adm),
            Err(Refused::Full(_))
        ));
        let _chat = s.try_enqueue("chat", classed(1, 0), adm).unwrap();
        assert!(new.await.unwrap().is_err(), "the newest bulk waiter left");
        assert!(old.try_recv().is_err(), "the older one still waits");
        assert_eq!(s.snapshot().evicted, 1);
    }

    #[tokio::test]
    async fn byte_cap_bounds_what_waits() {
        let s = Scheduler::spawn("t".into(), 1, RateLimiter::new(None, None));
        let _run = lease(s.enqueue("x", ticket(1))).await;
        let adm = Admission {
            max_bytes: Some(1000),
            ..Admission::default()
        };
        let big = |bytes| Ticket {
            weight: 1,
            bytes,
            ..Ticket::default()
        };
        // Alone in the queue even an oversized request is let in.
        let _a = s.try_enqueue("a", big(5000), adm).unwrap();
        assert!(matches!(
            s.try_enqueue("b", big(10), adm),
            Err(Refused::Full(_))
        ));
        assert_eq!(s.snapshot().queued_bytes, 5000);
    }

    fn holding(max: usize, max_bytes: usize, strict: bool) -> Arc<Scheduler> {
        let spec = BackendSpec {
            name: "t".into(),
            url: String::new(),
            limit: 1,
            adaptive: false,
            models: Vec::new(),
            prefix_slots: 0,
        };
        Scheduler::spawn_opts(
            "t".into(),
            vec![spec],
            RateLimiter::new(None, None),
            Options {
                strict,
                min_share: if strict { 0.5 } else { 0.0 },
                hold: Some(Hold { max, max_bytes }),
                ..Options::default()
            },
        )
    }

    #[tokio::test]
    async fn batch_work_is_held_beside_a_full_queue() {
        let s = holding(100, 1 << 20, false);
        let _run = lease(s.enqueue("x", ticket(1))).await;
        let adm = Admission {
            evict: true,
            shed: Some(Shed {
                from: 0.0,
                ceiling: 0.5,
                floor: 0.0,
            }),
            max_wait: Some(Duration::ZERO),
            ..cap(1)
        };
        // Held however many wait: no cap, staged refusal or wait limit.
        let held: Vec<_> = (0..5)
            .map(|_| s.try_enqueue("bulk", classed(1, 2), adm).unwrap().0)
            .collect();
        // The queue proper still has its one place, and it is not taken.
        let adm = Admission {
            max_wait: None,
            shed: None,
            ..adm
        };
        let _first = s.try_enqueue("ide0", classed(1, 0), adm).unwrap();
        assert!(matches!(
            s.try_enqueue("chat2", classed(1, 1), adm),
            Err(Refused::Full(_))
        ));
        // More important work does not push held work out.
        assert!(matches!(
            s.try_enqueue("ide", classed(1, 0), adm),
            Err(Refused::Full(_))
        ));
        let snap = s.snapshot();
        assert_eq!((snap.queued, snap.held, snap.evicted), (6, 5, 0));
        drop(held);
        assert_eq!(s.snapshot().held, 0, "callers that left are forgotten");
    }

    #[tokio::test]
    async fn held_work_runs_when_nothing_else_waits() {
        for strict in [false, true] {
            let s = holding(100, 1 << 20, strict);
            // Batch arrives first and in number; chat keeps arriving later.
            let got = order(
                &s,
                &[("bulk", classed(1, 2), 3), ("chat", classed(1, 1), 3)],
            )
            .await;
            assert_eq!(
                got,
                ["chat", "chat", "chat", "bulk", "bulk", "bulk"],
                "strict = {strict}"
            );
        }
    }

    #[tokio::test]
    async fn what_is_held_is_bounded_by_count_and_bytes() {
        let big = |bytes| Ticket {
            bytes,
            ..classed(1, 2)
        };
        let adm = Admission::default();
        let s = holding(2, 1000, false);
        let _run = lease(s.enqueue("x", ticket(1))).await;
        let _a = s.try_enqueue("bulk", big(10), adm).unwrap();
        let _b = s.try_enqueue("bulk", big(10), adm).unwrap();
        assert!(matches!(
            s.try_enqueue("bulk", big(10), adm),
            Err(Refused::HoldFull(_))
        ));
        let s = holding(100, 1000, false);
        let _run = lease(s.enqueue("x", ticket(1))).await;
        // Alone, even an oversized request is held.
        let _a = s.try_enqueue("bulk", big(5000), adm).unwrap();
        assert!(matches!(
            s.try_enqueue("bulk", big(10), adm),
            Err(Refused::HoldFull(_))
        ));
        let snap = s.snapshot();
        assert_eq!(
            (snap.held_bytes, snap.queued_bytes, snap.hold_refused),
            (5000, 0, 1)
        );
        // Other work is not refused for what is held.
        assert!(s.try_enqueue("chat", classed(1, 1), cap(1)).is_ok());
    }

    #[tokio::test]
    async fn held_work_does_not_lengthen_the_wait_others_are_told() {
        let s = holding(100, 1 << 20, false);
        let mut l = lease(s.enqueue("a", ticket(1))).await;
        tokio::time::sleep(Duration::from_millis(60)).await;
        l.record(1.0, 50.0, 10.0, true, None);
        drop(l);
        let _run = lease(s.enqueue("a", ticket(1))).await;
        let adm = Admission::default();
        let held: Vec<_> = (0..20)
            .map(|_| s.try_enqueue("bulk", classed(1, 2), adm).unwrap())
            .collect();
        let (_rx, chat, _) = s.try_enqueue("chat", classed(1, 1), adm).unwrap();
        assert_eq!(chat.ahead, 0);
        // The last held request sees the others and the chat request.
        let (_, _, place) = held.last().unwrap();
        assert_eq!(s.queue_status(place).ahead, 20);
    }

    #[tokio::test]
    async fn refuses_at_once_when_the_wait_would_be_too_long() {
        let s = Scheduler::spawn("t".into(), 1, RateLimiter::new(None, None));
        // Teach it how long a seat is held.
        let mut l = lease(s.enqueue("a", ticket(1))).await;
        tokio::time::sleep(Duration::from_millis(60)).await;
        l.record(1.0, 50.0, 10.0, true, None);
        drop(l);
        let _run = lease(s.enqueue("a", ticket(1))).await;
        let adm = |ms| Admission {
            max_wait: Some(Duration::from_millis(ms)),
            ..Admission::default()
        };
        let mut held = Vec::new();
        for _ in 0..5 {
            held.push(s.try_enqueue("a", ticket(1), Admission::default()).unwrap());
        }
        match s.try_enqueue("b", ticket(1), adm(20)) {
            Err(Refused::WaitTooLong(o)) => {
                assert!(o.wait >= Duration::from_millis(50), "{o:?}");
                assert!(o.wait_max >= o.wait);
            }
            other => panic!("expected a refusal, got {:?}", other.map(|_| ())),
        }
        assert!(s.try_enqueue("b", ticket(1), adm(60_000)).is_ok());
    }

    #[tokio::test]
    async fn strict_priority_serves_more_important_work_first() {
        let s = Scheduler::spawn_with("t".into(), 1, RateLimiter::new(None, None), true);
        let got = order(
            &s,
            &[
                ("bulk", classed(1, 2), 3),
                ("chat", classed(1, 0), 3),
                ("heavy", ticket(4), 2),
            ],
        )
        .await;
        assert_eq!(
            got,
            ["chat", "chat", "chat", "heavy", "heavy", "bulk", "bulk", "bulk"]
        );
    }

    #[tokio::test]
    async fn strict_priority_still_gives_bulk_its_share() {
        let s = Scheduler::spawn_opts(
            "t".into(),
            vec![BackendSpec {
                name: "t".into(),
                url: String::new(),
                limit: 1,
                adaptive: false,
                models: vec![],
                prefix_slots: 0,
            }],
            RateLimiter::new(None, None),
            Options {
                strict: true,
                min_share: 0.25,
                ..Options::default()
            },
        );
        let got = order(
            &s,
            &[("bulk", classed(1, 2), 4), ("chat", classed(1, 0), 12)],
        )
        .await;
        let bulk_early = got.iter().take(12).filter(|a| *a == "bulk").count();
        assert_eq!(
            bulk_early, 3,
            "one grant in four while chat is waiting: {got:?}"
        );
    }

    fn two(limit_a: usize, limit_b: usize, models: [&[&str]; 2], opts: Options) -> Arc<Scheduler> {
        let spec = |name: &str, limit, models: &[&str]| BackendSpec {
            name: name.into(),
            url: format!("http://{name}"),
            limit,
            adaptive: false,
            models: models.iter().map(|m| m.to_string()).collect(),
            prefix_slots: 0,
        };
        Scheduler::spawn_opts(
            "t".into(),
            vec![spec("a", limit_a, models[0]), spec("b", limit_b, models[1])],
            RateLimiter::new(None, None),
            opts,
        )
    }

    #[tokio::test]
    async fn a_seat_is_kept_for_important_work_that_keeps_arriving() {
        let s = two(
            1,
            1,
            [&[], &[]],
            Options {
                strict: true,
                reserve: 1,
                ..Options::default()
            },
        );
        // Nothing important seen yet: bulk may use both seats.
        let b1 = lease(s.enqueue("bulk", classed(1, 2))).await;
        let b2 = lease(s.enqueue("bulk", classed(1, 2))).await;
        drop((b1, b2));
        let chat = lease(s.enqueue("chat", classed(1, 0))).await;
        drop(chat);
        let _b1 = lease(s.enqueue("bulk", classed(1, 2))).await;
        let mut b2 = s.enqueue("bulk", classed(1, 2));
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(b2.try_recv().is_err(), "the last seat is kept free");
        // Important work gets it at once.
        let _chat = lease(s.enqueue("chat", classed(1, 0))).await;
    }

    #[tokio::test]
    async fn cooldown_delays_dispatch() {
        let s = Scheduler::spawn("t".into(), 1, RateLimiter::new(None, None));
        s.cooldown(Duration::from_millis(80));
        let t0 = Instant::now();
        let _l = lease(s.enqueue("a", ticket(1))).await;
        assert!(t0.elapsed() >= Duration::from_millis(70));
    }

    fn fticket(tenant: &str, run: &str, cost: f64, model: &str) -> Ticket {
        Ticket {
            weight: 1,
            cost: cost as u32,
            fair: Some(FairTicket {
                tenant: tenant.into(),
                tenant_weight: 1.0,
                run: run.into(),
                cost,
                class: 1,
                model: model.into(),
                ..FairTicket::default()
            }),
            ..Ticket::default()
        }
    }

    #[tokio::test]
    async fn fair_policy_shares_by_cost_between_tenants() {
        let s = two(
            1,
            1,
            [&["m"], &["other"]],
            Options {
                fair: true,
                ..Options::default()
            },
        );
        let hold = lease(s.enqueue("h", fticket("h", "h", 1.0, "m"))).await;
        let (tx, mut out) = tokio::sync::mpsc::unbounded_channel();
        for (tenant, cost, n) in [("big", 1000.0, 4), ("small", 100.0, 4)] {
            for i in 0..n {
                let rx = s.enqueue(tenant, fticket(tenant, &format!("{tenant}{i}"), cost, "m"));
                let tx = tx.clone();
                tokio::spawn(async move {
                    let l = lease(rx).await;
                    tx.send(tenant).unwrap();
                    drop(l);
                });
            }
        }
        tokio::task::yield_now().await;
        drop(hold);
        let mut got = Vec::new();
        for _ in 0..5 {
            got.push(out.recv().await.unwrap());
        }
        // One big request costs as much as ten small ones, so the small
        // tenant is served out before the big one gets a second turn.
        assert_eq!(got.iter().filter(|t| **t == "small").count(), 4, "{got:?}");
    }

    #[tokio::test]
    async fn requests_go_to_the_server_with_the_least_work() {
        let s = two(4, 4, [&[], &[]], Options::default());
        let big = lease(s.enqueue("a", fticket("a", "a", 1.0, ""))).await;
        assert_eq!(big.backend(), Some(0));
        let next = lease(s.enqueue("a", fticket("a", "a", 1.0, ""))).await;
        assert_eq!(next.backend(), Some(1), "the other server has less to do");
    }

    /// A prompt of 1000-token blocks, as the proxy would hand it over.
    fn prompt(blocks: &[u64]) -> Option<Chain> {
        let mut cum = 7u64;
        Some(
            blocks
                .iter()
                .enumerate()
                .map(|(i, b)| {
                    cum = cum.wrapping_mul(1_000_003).wrapping_add(*b);
                    (cum, 1000 * (i as u32 + 1))
                })
                .collect::<Vec<_>>()
                .into(),
        )
    }

    fn asking(agent: &str, blocks: &[u64]) -> Ticket {
        let mut t = fticket(agent, agent, 1.0, "");
        t.fair.as_mut().unwrap().prefix = prompt(blocks);
        t
    }

    #[tokio::test]
    async fn the_same_prompt_start_goes_to_the_server_that_holds_it() {
        let s = two(4, 4, [&[], &[]], Options::default());
        let a = lease(s.enqueue("a", asking("a", &[1, 2]))).await;
        let b = lease(s.enqueue("b", asking("b", &[5, 6]))).await;
        assert_eq!((a.backend(), b.backend()), (Some(0), Some(1)));
        assert_eq!(b.warm(), Some((0, 2000)), "nobody held it yet");
        drop((a, b));
        // Another caller, same start: the second server, though the first
        // is just as idle and would win on load alone.
        let c = lease(s.enqueue("c", asking("c", &[5, 6, 7]))).await;
        assert_eq!(c.backend(), Some(1));
        assert_eq!(c.warm(), Some((2000, 3000)));
        // And the first prompt's next turn goes back to the first server.
        let d = lease(s.enqueue("a", asking("a", &[1, 2, 3]))).await;
        assert_eq!(d.backend(), Some(0));
        let snap = s.snapshot();
        assert_eq!(snap.affinity, "prefix");
        assert_eq!(
            (snap.prefix_routing.warm, snap.prefix_routing.cold),
            (2, 2),
            "{snap:?}"
        );
        assert_eq!(snap.prefix_routing.warm_tokens, 4000);
        assert_eq!(snap.prefix_routing.prompt_tokens, 10_000);
    }

    #[tokio::test]
    async fn a_busy_warm_server_gives_way_to_an_idle_one() {
        let s = two(4, 4, [&[], &[]], Options::default());
        let a = lease(s.enqueue("a", asking("a", &[1, 2]))).await;
        assert_eq!(a.backend(), Some(0));
        // A quarter of this prompt is held by server 0, which has one of
        // its four seats taken; that is not enough to pass over an idle
        // server.
        let b = lease(s.enqueue("b", asking("b", &[1, 9, 9, 9]))).await;
        assert_eq!(b.backend(), Some(1));
        assert_eq!(s.snapshot().prefix_routing.spilled, 1);
        drop(b);
        // Two thirds held is.
        let c = lease(s.enqueue("c", asking("c", &[1, 2, 3]))).await;
        assert_eq!(c.backend(), Some(0));
        assert_eq!(c.warm(), Some((2000, 3000)));
    }

    #[tokio::test]
    async fn affinity_off_routes_by_load_alone() {
        let s = two(
            4,
            4,
            [&[], &[]],
            Options {
                affinity: Affinity::Off,
                ..Options::default()
            },
        );
        let a = lease(s.enqueue("a", asking("a", &[1, 2]))).await;
        let b = lease(s.enqueue("b", asking("b", &[5, 6]))).await;
        assert_eq!((a.backend(), b.backend()), (Some(0), Some(1)));
        drop((a, b));
        let c = lease(s.enqueue("c", asking("c", &[5, 6, 7]))).await;
        assert_eq!(c.backend(), Some(0));
        assert_eq!(c.warm(), None);
    }

    #[tokio::test]
    async fn a_request_waits_briefly_for_its_warm_server_when_told_to() {
        let opts = |wait| Options {
            affinity_wait: Duration::from_millis(wait),
            ..Options::default()
        };
        let s = two(1, 1, [&[], &[]], opts(5_000));
        let a = lease(s.enqueue("a", asking("a", &[1, 2]))).await;
        assert_eq!(a.backend(), Some(0));
        // Server 1 is free, but server 0 holds two thirds of this prompt.
        let mut next = s.enqueue("b", asking("b", &[1, 2, 3]));
        // Unrelated work does not wait behind it.
        let other = lease(s.enqueue("c", asking("c", &[8]))).await;
        assert_eq!(other.backend(), Some(1));
        drop(other);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(next.try_recv().is_err(), "held for the warm server");
        drop(a);
        assert_eq!(lease(next).await.backend(), Some(0));

        // The wait is bounded.
        let s = two(1, 1, [&[], &[]], opts(100));
        let a = lease(s.enqueue("a", asking("a", &[1, 2]))).await;
        let next = lease(s.enqueue("b", asking("b", &[1, 2, 3]))).await;
        assert_eq!(next.backend(), Some(1));
        assert_eq!(next.warm(), Some((0, 3000)));
        drop(a);
    }

    #[tokio::test]
    async fn a_server_taken_out_is_assumed_to_have_lost_its_cache() {
        let s = two(4, 4, [&[], &[]], Options::default());
        let a = lease(s.enqueue("a", asking("a", &[1, 2]))).await;
        let b = lease(s.enqueue("b", asking("b", &[5, 6]))).await;
        assert_eq!(b.backend(), Some(1));
        drop((a, b));
        s.fail_backend(1, Duration::from_millis(1));
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(s.snapshot().backends[1].prefix_entries, 0);
    }

    #[tokio::test]
    async fn batch_requests_of_one_caller_are_served_kind_by_kind() {
        let s = two(
            1,
            1,
            [&[], &[]],
            Options {
                batch_group: Duration::from_secs(30),
                ..Options::default()
            },
        );
        let batch = |blocks: &[u64]| {
            let mut t = asking("job", blocks);
            t.fair.as_mut().unwrap().class = 2;
            t
        };
        // Both servers are busy with one kind each.
        let a = lease(s.enqueue("job", batch(&[1, 0]))).await;
        let b = lease(s.enqueue("job", batch(&[2, 0]))).await;
        assert_eq!((a.backend(), b.backend()), (Some(0), Some(1)));
        // Queued in this order: a third kind, then one more of each.
        let third = s.enqueue("job", batch(&[3, 1]));
        let more_b = s.enqueue("job", batch(&[2, 1]));
        let more_a = s.enqueue("job", batch(&[1, 1]));
        tokio::task::yield_now().await;
        // Server 1 frees up: the request of its own kind goes first.
        drop(b);
        let got = lease(more_b).await;
        assert_eq!((got.backend(), got.warm()), (Some(1), Some((1000, 2000))));
        drop(a);
        let got_a = lease(more_a).await;
        assert_eq!(got_a.backend(), Some(0));
        drop(got);
        assert_eq!(lease(third).await.backend(), Some(1));
        assert_eq!(s.snapshot().prefix_routing.regrouped, 2);
    }

    #[tokio::test]
    async fn a_callers_warm_turn_passes_its_own_held_request() {
        let s = two(
            1,
            1,
            [&[], &[]],
            Options {
                slot_hold: Duration::from_secs(10),
                slot_max_wait: Duration::from_secs(10),
                ..Options::default()
            },
        );
        // One caller, two conversations, one on each server.
        let a = lease(s.enqueue("app", asking("app", &[1, 0]))).await;
        let b = lease(s.enqueue("app", asking("app", &[2, 0]))).await;
        assert_eq!((a.backend(), b.backend()), (Some(0), Some(1)));
        drop((a, b));
        // A third conversation would push one of them out, so it is held
        // back; the next turn of the first must not wait behind it.
        let mut third = s.enqueue("app", asking("app", &[3, 0]));
        let next = s.enqueue("app", asking("app", &[1, 0, 1]));
        let got = lease(next).await;
        assert_eq!((got.backend(), got.warm()), (Some(0), Some((2000, 3000))));
        assert!(third.try_recv().is_err(), "still held back");
        assert_eq!(s.snapshot().prefix_routing.regrouped, 1);
    }

    #[tokio::test]
    async fn a_model_no_server_has_is_refused_at_once() {
        let s = two(1, 1, [&["qwen"], &["smol"]], Options::default());
        let r = s.try_enqueue("a", fticket("a", "a", 1.0, "gpt"), Admission::default());
        assert!(matches!(r, Err(Refused::NoServer)));
        assert!(s
            .try_enqueue("a", fticket("a", "a", 1.0, "smol"), Admission::default())
            .is_ok());
        assert!(s.any_server_up("qwen"));
        s.fail_backend(0, Duration::from_secs(5));
        assert!(!s.any_server_up("qwen"));
    }

    #[tokio::test]
    async fn a_stand_in_model_takes_work_its_own_servers_cannot() {
        let s = two(1, 1, [&["qwen"], &["smol"]], Options::default());
        let with_alt = |alts: &[&str]| {
            let mut t = fticket("a", "a", 1.0, "qwen");
            t.fair.as_mut().unwrap().alts = alts.iter().map(|m| m.to_string()).collect();
            t
        };
        let first = lease(s.enqueue("a", with_alt(&["smol"]))).await;
        assert_eq!((first.backend(), first.model()), (Some(0), None));
        // qwen's server is full: a request that allows it moves to smol.
        let second = lease(s.enqueue("a", with_alt(&["smol"]))).await;
        assert_eq!((second.backend(), second.model()), (Some(1), Some("smol")));
        drop(second);
        // One that does not allow it waits for its own server.
        let mut third = s.enqueue("a", with_alt(&[]));
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(third.try_recv().is_err());
        drop(first);
        assert_eq!(lease(third).await.backend(), Some(0));
    }

    #[tokio::test]
    async fn a_failing_server_is_taken_out_and_tried_again_carefully() {
        let s = two(2, 2, [&[], &[]], Options::default());
        for _ in 0..3 {
            let mut l = lease(s.enqueue("a", ticket(1))).await;
            if l.backend() == Some(0) {
                l.failed();
            } else {
                l.record(1.0, 1.0, 1.0, true, None);
            }
        }
        // Three failures in a row may take several rounds to collect.
        for _ in 0..6 {
            let mut l = lease(s.enqueue("a", ticket(1))).await;
            if l.backend() == Some(0) {
                l.failed();
            }
        }
        let snap = s.snapshot();
        assert!(snap.backends[0].cooling, "{snap:?}");
        assert_eq!(
            snap.backends[0].limit, 1,
            "one trial request when it returns"
        );
        for _ in 0..4 {
            assert_eq!(lease(s.enqueue("a", ticket(1))).await.backend(), Some(1));
        }
    }

    #[tokio::test]
    async fn a_rate_limited_model_pauses_alone() {
        let s = two(1, 1, [&[], &[]], Options::default());
        s.slow_down(Some(0), "hot", Duration::from_millis(120));
        let t0 = Instant::now();
        let _other = lease(s.enqueue("a", fticket("a", "a", 1.0, "cold"))).await;
        assert!(
            t0.elapsed() < Duration::from_millis(60),
            "other models go on"
        );
        let _hot = lease(s.enqueue("a", fticket("a", "a", 1.0, "hot"))).await;
        assert!(t0.elapsed() >= Duration::from_millis(100));
        assert!(s.snapshot().models_cooling.is_empty());
    }

    #[tokio::test]
    async fn the_wait_estimate_follows_what_bulk_work_actually_gets() {
        let s = Scheduler::spawn_with("t".into(), 1, RateLimiter::new(None, None), true);
        // Serve a mix so both kinds have a measured pace.
        for i in 0..12 {
            let class = if i % 4 == 0 { 2 } else { 0 };
            let mut l = lease(s.enqueue("x", classed(1, class))).await;
            tokio::time::sleep(Duration::from_millis(10)).await;
            l.record(1.0, 5.0, 1.0, true, None);
        }
        let _run = lease(s.enqueue("x", classed(1, 0))).await;
        let mut held = Vec::new();
        for _ in 0..4 {
            held.push(
                s.try_enqueue("bulk", classed(1, 2), Admission::default())
                    .unwrap(),
            );
        }
        let (_rx, chat, _) = s
            .try_enqueue("chat", classed(1, 0), Admission::default())
            .unwrap();
        let bulk = s.queue_status(&held[3].2);
        assert_eq!(chat.ahead, 0);
        assert_eq!(bulk.ahead, 4, "three bulk requests and the chat one");
        // Bulk got one grant in four, so its wait per place is several
        // times the chat request's.
        assert!(bulk.wait > chat.wait * 4, "{bulk:?} {chat:?}");
    }
}
