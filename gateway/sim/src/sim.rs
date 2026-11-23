//! The event loop and the component models.
//!
//! A request is created by a client, walks a path of components (gateway,
//! proxy, backend) and its response chunks walk the same path back. Every
//! component is a small state machine driven by events on one virtual clock,
//! so queueing, timeouts, retries and work done for callers who already left
//! all fall out of the same mechanics the real stack has.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, VecDeque};

use crate::metrics::{Gauge, Metrics};
use crate::rng::Rng;
use crate::scenario::*;
use crate::sched::{Admission, Refused, Sched};

/// Status of a request the caller abandoned at its own timeout.
pub const GAVE_UP: i32 = -1;
/// Status of a request still open when the run was cut off at `max_s`.
pub const UNFINISHED: i32 = -2;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Node {
    Client(usize),
    Gateway,
    Proxy(usize),
    Server(usize),
    Hosted(usize),
    Mcp(usize),
    Db,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum Kind {
    First,
    Last,
    Error { status: i32, retry_ms: f64 },
}

#[derive(Clone, Copy, Debug)]
enum Ev {
    Arrive {
        src: usize,
    },
    AgentWake {
        agent: usize,
    },
    AgentTurn {
        agent: usize,
    },
    Send {
        req: u32,
    },
    At {
        req: u32,
        ep: u32,
        hop: usize,
    },
    /// A response chunk reaches `path[hop]`, or the client when `hop` is -1.
    Resp {
        req: u32,
        ep: u32,
        hop: i32,
        kind: Kind,
    },
    GwTimeout {
        req: u32,
        seq: u32,
    },
    /// A gateway instance has spent its CPU on a request.
    GwReady {
        req: u32,
        ep: u32,
    },
    /// The gateway found the answer in its cache.
    GwCached {
        req: u32,
        ep: u32,
    },
    GwKick {
        up: usize,
    },
    ProxyDone {
        proxy: usize,
        worker: usize,
    },
    Step {
        server: usize,
        slot: usize,
        run: u64,
    },
    HostedTok {
        req: u32,
        ep: u32,
        hop: usize,
    },
    ClientTimeout {
        req: u32,
    },
    McpArrive {
        agent: usize,
        mcp: usize,
        back_ms: f64,
    },
    McpDone {
        agent: usize,
        mcp: usize,
        back_ms: f64,
    },
    DbFlush,
    DbDone {
        rows: f64,
    },
}

struct Timed {
    at: f64,
    seq: u64,
    ev: Ev,
}

impl PartialEq for Timed {
    fn eq(&self, o: &Self) -> bool {
        self.seq == o.seq
    }
}
impl Eq for Timed {}
impl PartialOrd for Timed {
    fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for Timed {
    // Earliest first; ties in the order they were scheduled.
    fn cmp(&self, o: &Self) -> Ordering {
        o.at.total_cmp(&self.at).then(o.seq.cmp(&self.seq))
    }
}

/// One request as its caller sees it, plus where its time went.
#[derive(Clone, Debug)]
pub struct Req {
    pub id: u32,
    pub client: usize,
    pub model: usize,
    pub prompt_tokens: f64,
    pub out_tokens: u32,
    /// When the caller first sent it (ms).
    pub sent_at: f64,
    pub deadline: f64,
    /// 0 for a request as first sent; n for the n-th time the caller sent
    /// it again after a timeout.
    pub resend: u32,
    pub tries: u32,
    /// 0 while open; 200, an error status, [`GAVE_UP`] or [`UNFINISHED`].
    pub status: i32,
    pub first_token_at: Option<f64>,
    pub done_at: Option<f64>,
    /// Time waiting for a gateway slot, over all attempts (ms).
    pub queue_ms: f64,
    /// Wait the gateway announced when the request first joined its queue.
    pub expected_wait_ms: Option<f64>,
    /// How long that first wait in the gateway's queue lasted (ms), and
    /// whether it ended with a seat (not with a refusal or eviction).
    pub waited_ms: Option<f64>,
    pub seated: bool,
    pub first_refusal_at: Option<f64>,
    pub proxy_ms: f64,
    pub backend_queue_ms: f64,
    pub prefill_ms: f64,
    pub gen_ms: f64,
    /// Which repeating prefix the prompt starts with (0 = none) and how
    /// much of the prompt it is.
    pub prefix_key: u64,
    /// What the prompt is remembered under once a server has read it:
    /// the key a follow-up will look for. Same as `prefix_key` unless a
    /// recorded log says otherwise.
    pub store_key: u64,
    pub prefix_share: f64,
    /// The part shared with the whole group (0 = none), within the above.
    pub group_key: u64,
    pub group_share: f64,
    /// Whether the server that ran it had the prefix cached.
    pub prefix_hit: Option<bool>,
    /// Prompt tokens the server did not have to read again.
    pub cached_tokens: f64,
    /// Backend that answered (index into `Sim::backend_names`).
    pub backend: Option<usize>,
    pub(crate) epoch: u32,
    pub(crate) path: Vec<Node>,
    /// The hop that led to each entry of `path`, for counting what flows
    /// back along it.
    hops: Vec<u32>,
    hold: Option<Hold>,
    gw_attempts: u32,
    gw_seq: u32,
    agent: Option<usize>,
    proxy_worker: usize,
    proxy_open: Option<usize>,
    // Stream state at a hosted API.
    left: u32,
    tok_ms: f64,
    started: bool,
    backend_at: f64,
    /// Tokens a model server had written when it put the request back.
    written: u32,
}

impl Req {
    /// Output tokens a second as the caller saw them, from the first token
    /// to `done` (ms). `None` when the answer was a single token or less.
    pub fn gen_tps(&self, done: f64) -> Option<f64> {
        let first = self.first_token_at?;
        (self.out_tokens > 1 && done > first)
            .then(|| f64::from(self.out_tokens - 1) / (done - first) * 1000.0)
    }
}

#[derive(Clone, Copy, Debug)]
struct Hold {
    up: usize,
    enq_at: f64,
    /// The request's first time in this queue: the wait its ticket is about.
    first: bool,
    leased: Option<(f64, usize)>,
}

struct HostSt {
    cores: f64,
    demand: f64,
}

struct UpSt {
    sched: Sched,
    target_open: Vec<usize>,
    /// `prefix` routing: per target, the prompts the gateway believes it
    /// keeps (own key, group key), most recently sent first.
    belief: Vec<VecDeque<(u64, u64)>>,
    rr: usize,
    kick_at: f64,
    pub queue: Gauge,
    pub busy: Gauge,
    pub shed: u64,
    pub done: u64,
}

struct GwSt {
    ups: Vec<UpSt>,
    open: usize,
    logged: u64,
    targets: Vec<Vec<Node>>,
    /// What each target can run at once, for load-relative routing.
    target_cap: Vec<Vec<f64>>,
}

#[derive(Clone, Copy)]
enum JobKind {
    Start,
    Chunk(Kind),
    /// Tokens passing through mid-stream: so many chunks' worth of CPU.
    Mid(u32),
    After,
}

#[derive(Clone, Copy)]
struct Job {
    req: u32,
    ep: u32,
    hop: usize,
    kind: JobKind,
    enq_at: f64,
}

struct Worker {
    queue: VecDeque<Job>,
    busy: Option<Job>,
}

struct ProxySt {
    workers: Vec<Worker>,
    rr: usize,
    open: usize,
    host: Option<usize>,
    backends: Vec<usize>,
    pub queue: Gauge,
    pub busy: Gauge,
    pub done: u64,
    rng: Rng,
}

#[derive(Clone, Copy, PartialEq)]
enum Phase {
    Prefill,
    Gen,
}

/// A target as the router sees it: requests open there, how many it runs at
/// once, and what it is.
struct Spot {
    open: f64,
    cap: f64,
    node: Node,
}

struct Run {
    req: u32,
    ep: u32,
    hop: usize,
    phase: Phase,
    left: u32,
    ghost: bool,
    tok_ms: f64,
    started: f64,
    /// Which run of the slot this is: a step meant for one that was put
    /// back is dropped.
    seq: u64,
    /// Tokens of context it holds.
    ctx: f64,
    /// Started again after being put back: its first token is not the
    /// answer's first.
    resumed: bool,
}

struct ServerSt {
    queue: VecDeque<(u32, u32, usize, f64)>,
    slots: Vec<Option<Run>>,
    active: usize,
    host: Option<usize>,
    pub qlen: Gauge,
    pub busy: Gauge,
    pub wasted_ms: f64,
    pub done: u64,
    pub ghosts: u64,
    pub tokens: u64,
    rng: Rng,
    /// Prompt prefixes in the cache, most recently used first.
    kv: VecDeque<(u64, u64)>,
    /// 0 = every slot keeps its own last prompt (`slot_kv`).
    kv_cap: usize,
    /// Per slot: the caller and group prefix of the prompt it ran last, and
    /// when.
    slot_kv: Vec<(u64, u64, f64)>,
    run_seq: u64,
    /// Slots reading a prompt.
    reading: usize,
    /// Tokens of context held by the running requests.
    ctx: f64,
}

struct HostedSt {
    open: usize,
    rpm_tokens: f64,
    tpm_tokens: f64,
    refilled_at: f64,
    pub busy: Gauge,
    pub done: u64,
    pub limited: u64,
    pub tokens: u64,
    rng: Rng,
}

struct McpSt {
    queue: VecDeque<(usize, f64, f64)>,
    active: usize,
    pub qlen: Gauge,
    pub busy: Gauge,
    pub done: u64,
    pub wait_ms: f64,
    rng: Rng,
}

struct DbSt {
    buffer: f64,
    writing: bool,
    flush_due: bool,
    pub backlog: Gauge,
    pub busy: Gauge,
    pub written: f64,
    pub dropped: f64,
}

struct Source {
    client: usize,
    model: usize,
    rng: Rng,
    /// Next row for trace sources.
    next_row: usize,
}

struct AgentSt {
    client: usize,
    model: usize,
    turn: usize,
    rng: Rng,
}

pub struct Sim {
    pub sc: Scenario,
    pub now: f64,
    heap: BinaryHeap<Timed>,
    seq: u64,
    rng: Rng,
    pub reqs: Vec<Req>,
    pub models: Vec<String>,
    hosts: Vec<HostSt>,
    gw: Option<GwSt>,
    /// When each core of each gateway instance is next free (ms).
    gw_cpu: Vec<Vec<f64>>,
    pub gw_cpu_busy_ms: f64,
    pub gw_cpu_wait_ms: f64,
    pub gw_cpu_seen: u64,
    cache_rng: Rng,
    /// Requests turned away by gradual shedding.
    pub shed_early: u64,
    prefix_rng: Rng,
    /// Clients of one class share a group prefix.
    client_group: Vec<usize>,
    /// Which conversation each client is in (see `Prefix::follow_up`).
    conversation: Vec<u32>,
    /// Unfinished requests per prefix, for `replicate_at`.
    prefix_open: std::collections::HashMap<u64, u32>,
    pub prefix_lookups: u64,
    pub prefix_hits: u64,
    /// Prompt tokens of requests with a prefix, and how many of them a
    /// server did not have to read.
    pub prefix_tokens: f64,
    pub prefix_tokens_saved: f64,
    /// Requests a model server put back for want of context room, and the
    /// tokens (prompt and written) that had to be read again.
    pub preempted: u64,
    pub preempted_tokens: f64,
    pub cache_hits: u64,
    pub cache_lookups: u64,
    proxies: Vec<ProxySt>,
    servers: Vec<ServerSt>,
    hosted: Vec<HostedSt>,
    mcps: Vec<McpSt>,
    db: Option<DbSt>,
    sources: Vec<Source>,
    agents: Vec<AgentSt>,
    client_entry: Vec<Node>,
    pub metrics: Metrics,
    next_sample: f64,
    pub events: u64,
    /// Requests not settled yet, and scheduled events that can still create
    /// work (arrivals, agent turns, retries, tool calls, log writes).
    open: usize,
    pending: usize,
}

fn host_index(sc: &Scenario, h: &Option<String>) -> Option<usize> {
    h.as_ref()
        .and_then(|h| sc.hosts.iter().position(|x| &x.name == h))
}

impl Sim {
    pub fn new(sc: Scenario) -> Result<Self, String> {
        sc.check()?;
        let mut rng = Rng::new(sc.seed);
        let mut models: Vec<String> = Vec::new();
        let mut model_id = |m: &str| match models.iter().position(|x| x == m) {
            Some(i) => i,
            None => {
                models.push(m.to_string());
                models.len() - 1
            }
        };
        for c in &sc.clients {
            for m in &c.models {
                model_id(m);
            }
            if let Arrival::Trace { rows } = &c.arrival {
                for r in rows {
                    model_id(&r.model);
                }
            }
        }
        let node_of = |name: &str| -> Option<Node> {
            if sc.gateway.as_ref().is_some_and(|g| g.name == name) {
                return Some(Node::Gateway);
            }
            if let Some(i) = sc.proxies.iter().position(|p| p.name == name) {
                return Some(Node::Proxy(i));
            }
            if let Some(i) = sc.model_servers.iter().position(|p| p.name == name) {
                return Some(Node::Server(i));
            }
            sc.hosted_apis
                .iter()
                .position(|p| p.name == name)
                .map(Node::Hosted)
        };
        let hosts = sc
            .hosts
            .iter()
            .map(|h| HostSt {
                cores: h.cores,
                demand: h.background_threads,
            })
            .collect();
        let gw = sc.gateway.as_ref().map(|g| GwSt {
            ups: g
                .upstreams
                .iter()
                .map(|u| UpSt {
                    sched: {
                        let s = Sched::new(u.max_concurrency, u.strict_priority, u.max_rps)
                            .holding(u.hold_weight);
                        match u.least_attained_half_life_s {
                            Some(h) => s.least_attained(h),
                            None => s,
                        }
                    },
                    target_open: vec![0; u.targets.len()],
                    belief: vec![VecDeque::new(); u.targets.len()],
                    rr: 0,
                    kick_at: f64::INFINITY,
                    queue: Gauge::default(),
                    busy: Gauge::default(),
                    shed: 0,
                    done: 0,
                })
                .collect(),
            open: 0,
            logged: 0,
            targets: g
                .upstreams
                .iter()
                .map(|u| u.targets.iter().filter_map(|t| node_of(t)).collect())
                .collect(),
            target_cap: g
                .upstreams
                .iter()
                .map(|u| {
                    u.targets
                        .iter()
                        .filter(|t| node_of(t).is_some())
                        .map(|t| {
                            if let Some(m) = sc.model_servers.iter().find(|m| &m.name == t) {
                                m.full() as f64
                            } else if let Some(p) = sc.proxies.iter().find(|p| &p.name == t) {
                                p.workers as f64
                            } else {
                                1.0
                            }
                        })
                        .collect()
                })
                .collect(),
        });
        let gw_cpu = sc.gateway.as_ref().map_or_else(Vec::new, |g| {
            vec![vec![0.0; (g.cores.floor() as usize).max(1)]; g.instances.max(1)]
        });
        let cache_rng = Rng::new(sc.seed ^ 0xCAC4_E000);
        let prefix_rng = Rng::new(sc.seed ^ 0x9AEF_1C00);
        let group_name = |c: &Client| {
            c.prefix
                .as_ref()
                .and_then(|p| p.group.clone())
                .or_else(|| c.class.clone())
                .unwrap_or_else(|| c.name.clone())
        };
        let client_group: Vec<usize> = sc
            .clients
            .iter()
            .map(|c| {
                let g = group_name(c);
                sc.clients
                    .iter()
                    .position(|o| group_name(o) == g)
                    .unwrap_or(0)
            })
            .collect();
        let n_servers = sc.model_servers.len();
        let proxies = sc
            .proxies
            .iter()
            .enumerate()
            .map(|(i, p)| ProxySt {
                workers: (0..p.workers)
                    .map(|_| Worker {
                        queue: VecDeque::new(),
                        busy: None,
                    })
                    .collect(),
                rr: 0,
                open: 0,
                host: host_index(&sc, &p.host),
                backends: sc
                    .backend_names()
                    .iter()
                    .enumerate()
                    .filter(|(_, b)| p.backends.is_empty() || p.backends.iter().any(|x| x == *b))
                    .map(|(i, _)| i)
                    .collect(),
                queue: Gauge::default(),
                busy: Gauge::default(),
                done: 0,
                rng: rng.fork(100 + i as u64),
            })
            .collect();
        let servers = sc
            .model_servers
            .iter()
            .enumerate()
            .map(|(i, m)| ServerSt {
                queue: VecDeque::new(),
                slots: (0..m.slots).map(|_| None).collect(),
                active: 0,
                host: host_index(&sc, &m.host),
                qlen: Gauge::default(),
                busy: Gauge::default(),
                wasted_ms: 0.0,
                done: 0,
                ghosts: 0,
                tokens: 0,
                rng: rng.fork(200 + i as u64),
                kv: VecDeque::new(),
                kv_cap: m.prefix_cache_entries.unwrap_or(0),
                slot_kv: vec![(0, 0, -1.0); m.slots],
                run_seq: 0,
                reading: 0,
                ctx: 0.0,
            })
            .collect();
        let hosted = sc
            .hosted_apis
            .iter()
            .enumerate()
            .map(|(i, h)| HostedSt {
                open: 0,
                rpm_tokens: h.rpm,
                tpm_tokens: h.tpm,
                refilled_at: 0.0,
                busy: Gauge::default(),
                done: 0,
                limited: 0,
                tokens: 0,
                rng: rng.fork(300 + i as u64),
            })
            .collect();
        let mcps = sc
            .mcp_servers
            .iter()
            .enumerate()
            .map(|(i, _)| McpSt {
                queue: VecDeque::new(),
                active: 0,
                qlen: Gauge::default(),
                busy: Gauge::default(),
                done: 0,
                wait_ms: 0.0,
                rng: rng.fork(400 + i as u64),
            })
            .collect();
        let db = sc.db.as_ref().map(|_| DbSt {
            buffer: 0.0,
            writing: false,
            flush_due: false,
            backlog: Gauge::default(),
            busy: Gauge::default(),
            written: 0.0,
            dropped: 0.0,
        });
        let client_entry = sc
            .clients
            .iter()
            .map(|c| node_of(&c.entry).expect("checked"))
            .collect();
        let metrics = Metrics::new(&sc, n_servers);
        let mut sim = Sim {
            now: 0.0,
            heap: BinaryHeap::new(),
            seq: 0,
            rng,
            reqs: Vec::new(),
            models,
            hosts,
            gw,
            proxies,
            servers,
            hosted,
            mcps,
            db,
            sources: Vec::new(),
            gw_cpu,
            gw_cpu_busy_ms: 0.0,
            gw_cpu_wait_ms: 0.0,
            gw_cpu_seen: 0,
            cache_rng,
            shed_early: 0,
            prefix_rng,
            conversation: vec![0; client_group.len()],
            prefix_open: std::collections::HashMap::new(),
            client_group,
            prefix_lookups: 0,
            prefix_hits: 0,
            prefix_tokens: 0.0,
            prefix_tokens_saved: 0.0,
            preempted: 0,
            preempted_tokens: 0.0,
            cache_hits: 0,
            cache_lookups: 0,
            agents: Vec::new(),
            client_entry,
            metrics,
            next_sample: sc.sample_ms,
            events: 0,
            open: 0,
            pending: 0,
            sc,
        };
        sim.start_clients();
        Ok(sim)
    }

    fn model_index(&self, m: &str) -> usize {
        self.models.iter().position(|x| x == m).expect("interned")
    }

    fn start_clients(&mut self) {
        for ci in 0..self.sc.clients.len() {
            let c = self.sc.clients[ci].clone();
            let from = c.from_s * 1000.0;
            match &c.arrival {
                Arrival::Poisson { .. } | Arrival::Uniform { .. } => {
                    for m in &c.models {
                        let src = self.sources.len();
                        let model = self.model_index(m);
                        let mut rng = self.rng.fork(1000 + src as u64);
                        // Independent callers do not all start on the same
                        // instant: a Poisson source waits one gap first.
                        let first = match c.arrival {
                            Arrival::Poisson { rate_per_s } => rng.exp(1000.0 / rate_per_s),
                            _ => 0.0,
                        };
                        self.sources.push(Source {
                            client: ci,
                            model,
                            rng,
                            next_row: 0,
                        });
                        self.push(from + first, Ev::Arrive { src });
                    }
                }
                Arrival::Trace { rows } => {
                    let src = self.sources.len();
                    let rng = self.rng.fork(1000 + src as u64);
                    self.sources.push(Source {
                        client: ci,
                        model: 0,
                        rng,
                        next_row: 0,
                    });
                    if let Some(first) = rows.first() {
                        self.push(first.t * 1000.0, Ev::Arrive { src });
                    }
                }
                Arrival::Closed { agents, think_s } => {
                    for m in &c.models {
                        let model = self.model_index(m);
                        for _ in 0..*agents {
                            let agent = self.agents.len();
                            let mut rng = self.rng.fork(5000 + agent as u64);
                            // Agents do not all start on the same instant.
                            let offset = rng.range(0.0, (think_s * 1000.0).max(200.0));
                            self.agents.push(AgentSt {
                                client: ci,
                                model,
                                turn: 0,
                                rng,
                            });
                            self.push(from + offset, Ev::AgentWake { agent });
                        }
                    }
                }
            }
        }
    }

    fn makes_work(ev: &Ev) -> bool {
        matches!(
            ev,
            Ev::Arrive { .. }
                | Ev::AgentWake { .. }
                | Ev::AgentTurn { .. }
                | Ev::Send { .. }
                | Ev::McpArrive { .. }
                | Ev::McpDone { .. }
                | Ev::DbFlush
                | Ev::DbDone { .. }
        )
    }

    fn push(&mut self, at: f64, ev: Ev) {
        if Self::makes_work(&ev) {
            self.pending += 1;
        }
        self.seq += 1;
        self.heap.push(Timed {
            at: at.max(self.now),
            seq: self.seq,
            ev,
        });
    }

    pub fn node_name(&self, n: Node) -> &str {
        match n {
            Node::Client(c) => &self.sc.clients[c].name,
            Node::Gateway => &self.sc.gateway.as_ref().expect("has gateway").name,
            Node::Proxy(i) => &self.sc.proxies[i].name,
            Node::Server(i) => &self.sc.model_servers[i].name,
            Node::Hosted(i) => &self.sc.hosted_apis[i].name,
            Node::Mcp(i) => &self.sc.mcp_servers[i].name,
            Node::Db => &self.sc.db.as_ref().expect("has db").name,
        }
    }

    /// One-way latency between two components right now.
    fn lat(&mut self, a: Node, b: Node) -> f64 {
        let name = |s: &Sim, n: Node| -> String {
            match n {
                Node::Client(_) => "clients".to_string(),
                n => s.node_name(n).to_string(),
            }
        };
        let (na, nb) = (name(self, a), name(self, b));
        let client_name = |s: &Sim, n: Node| match n {
            Node::Client(c) => Some(s.sc.clients[c].name.clone()),
            _ => None,
        };
        let (ca, cb) = (client_name(self, a), client_name(self, b));
        let is = |x: &str, n: &str, c: &Option<String>| x == n || c.as_deref() == Some(x);
        let link = self.sc.net.links.iter().find(|l| {
            (is(&l.from, &na, &ca) && is(&l.to, &nb, &cb))
                || (is(&l.from, &nb, &cb) && is(&l.to, &na, &ca))
        });
        match link {
            Some(l) => {
                let (base, jitter) = (l.latency_ms, l.jitter_ms);
                base + if jitter > 0.0 {
                    self.rng.range(0.0, jitter)
                } else {
                    0.0
                }
            }
            None => self.sc.net.default_ms,
        }
    }

    fn host_speed(&self, h: Option<usize>) -> f64 {
        match h {
            Some(h) => {
                let h = &self.hosts[h];
                (h.cores / h.demand.max(1e-9)).min(1.0)
            }
            None => 1.0,
        }
    }

    fn host_demand(&mut self, h: Option<usize>, delta: f64) {
        if let Some(h) = h {
            self.hosts[h].demand = (self.hosts[h].demand + delta).max(0.0);
        }
    }

    fn live(&self, req: u32, ep: u32) -> bool {
        let r = &self.reqs[req as usize];
        r.status == 0 && r.epoch == ep
    }

    /// Whether anything is left to simulate.
    pub fn finished(&self) -> bool {
        // Timers of requests that are already settled do not keep a run alive.
        self.heap.is_empty()
            || (self.open == 0 && self.pending == 0)
            || self.now >= self.sc.max_s * 1000.0
    }

    /// Run until virtual time `to_ms` (or the end of the scenario).
    pub fn advance(&mut self, to_ms: f64) {
        let end = to_ms.min(self.sc.max_s * 1000.0);
        loop {
            let next = self.heap.peek().map_or(f64::INFINITY, |t| t.at);
            // Samples are taken on the way, at their own times.
            while self.next_sample <= next.min(end) {
                self.now = self.next_sample;
                self.sample();
                self.next_sample += self.sc.sample_ms;
            }
            if next > end {
                break;
            }
            let t = self.heap.pop().expect("peeked");
            self.now = t.at;
            self.events += 1;
            self.handle(t.ev);
        }
        if end > self.now && !self.heap.is_empty() {
            self.now = end;
        }
    }

    /// Run to the end and close the books.
    pub fn run(&mut self) {
        while !self.finished() {
            let next = self.heap.peek().map_or(self.now, |t| t.at);
            self.advance(next.max(self.now) + 60_000.0);
        }
        self.close();
    }

    /// Mark what is still open as unfinished and take a last sample.
    pub fn close(&mut self) {
        for r in &mut self.reqs {
            if r.status == 0 {
                r.status = UNFINISHED;
                r.done_at = Some(self.now);
            }
        }
        self.sample();
    }

    fn handle(&mut self, ev: Ev) {
        if Self::makes_work(&ev) {
            self.pending -= 1;
        }
        match ev {
            Ev::Arrive { src } => self.arrive(src),
            Ev::AgentWake { agent } => self.agent_wake(agent),
            Ev::AgentTurn { agent } => {
                let (ci, model) = (self.agents[agent].client, self.agents[agent].model);
                self.new_request(ci, model, None, None, Some(agent), None);
            }
            Ev::Send { req } => self.send(req),
            Ev::At { req, ep, hop } => self.at(req, ep, hop),
            Ev::Resp { req, ep, hop, kind } => self.resp(req, ep, hop, kind),
            Ev::GwTimeout { req, seq } => self.gw_timeout(req, seq),
            Ev::GwReady { req, ep } => self.gw_admit(req, ep),
            Ev::GwCached { req, ep } => {
                if self.live(req, ep) {
                    self.emit(req, ep, 0, Kind::First);
                    self.emit(req, ep, 0, Kind::Last);
                    self.gw_log();
                }
            }
            Ev::GwKick { up } => {
                if let Some(g) = self.gw.as_mut() {
                    g.ups[up].kick_at = f64::INFINITY;
                }
                self.gw_dispatch(up);
            }
            Ev::ProxyDone { proxy, worker } => self.proxy_done(proxy, worker),
            Ev::Step { server, slot, run } => self.server_step(server, slot, run),
            Ev::HostedTok { req, ep, hop } => self.hosted_tok(req, ep, hop),
            Ev::ClientTimeout { req } => self.client_timeout(req),
            Ev::McpArrive {
                agent,
                mcp,
                back_ms,
            } => self.mcp_arrive(agent, mcp, back_ms),
            Ev::McpDone {
                agent,
                mcp,
                back_ms,
            } => self.mcp_done(agent, mcp, back_ms),
            Ev::DbFlush => self.db_flush(),
            Ev::DbDone { rows } => self.db_done(rows),
        }
    }

    // ---- clients ----

    fn arrive(&mut self, src: usize) {
        let ci = self.sources[src].client;
        let c = &self.sc.clients[ci];
        let to = c.to_s * 1000.0;
        if self.now >= to {
            return;
        }
        match &c.arrival {
            Arrival::Poisson { rate_per_s } => {
                let model = self.sources[src].model;
                let gap = self.sources[src].rng.exp(1000.0 / rate_per_s);
                // With a daily rhythm, arrivals come at the peak rate and
                // are kept with the share of it that holds right now.
                let keep = match c.rhythm {
                    Some(r) => self.sources[src].rng.unit() < r.level(self.now / 1000.0),
                    None => true,
                };
                if keep {
                    self.new_request(ci, model, None, None, None, None);
                }
                self.push(self.now + gap, Ev::Arrive { src });
            }
            Arrival::Uniform { rate_per_s } => {
                let model = self.sources[src].model;
                let gap = 1000.0 / rate_per_s;
                self.new_request(ci, model, None, None, None, None);
                self.push(self.now + gap, Ev::Arrive { src });
            }
            Arrival::Trace { rows } => {
                let i = self.sources[src].next_row;
                let row = rows[i].clone();
                let next = rows.get(i + 1).map(|r| r.t * 1000.0);
                self.sources[src].next_row += 1;
                let model = self.model_index(&row.model);
                self.new_request(
                    ci,
                    model,
                    row.prompt_tokens,
                    row.output_tokens,
                    None,
                    row.prefix,
                );
                if let Some(t) = next {
                    self.push(t, Ev::Arrive { src });
                }
            }
            Arrival::Closed { .. } => {}
        }
    }

    fn new_request(
        &mut self,
        ci: usize,
        model: usize,
        prompt: Option<f64>,
        out: Option<f64>,
        agent: Option<usize>,
        recorded_prefix: Option<[f64; 5]>,
    ) {
        let c = &self.sc.clients[ci];
        let turn = agent.map_or(0, |a| self.agents[a].turn) as f64;
        let growth = c.session.as_ref().map_or(0.0, |s| s.prompt_growth_tokens);
        let prompt_tokens = prompt.unwrap_or(c.prompt_tokens + growth * turn);
        let out_tokens = match out {
            Some(o) => o,
            None if c.output_sigma > 0.0 => {
                let (median, sigma) = (c.output_tokens, c.output_sigma);
                match agent {
                    Some(a) => self.agents[a].rng.lognormal(median, sigma),
                    None => self.rng.lognormal(median, sigma),
                }
            }
            None => c.output_tokens,
        }
        .round()
        .max(1.0) as u32;
        let c = &self.sc.clients[ci];
        let mut recorded_store = 0.0;
        let (prefix_key, prefix_share, group_key, group_share) = match (recorded_prefix, &c.prefix)
        {
            // Keys from a log are kept apart per model like generated ones.
            (Some([k, share, gk, gshare, store]), _) => {
                recorded_store = store;
                let key = |k: f64| {
                    if k == 0.0 {
                        0
                    } else {
                        (k.to_bits() ^ (model as u64) << 1).wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1
                    }
                };
                (
                    key(k),
                    share.clamp(0.0, 1.0),
                    key(gk),
                    gshare.clamp(0.0, 1.0),
                )
            }
            (None, Some(p)) => {
                let mix = |who: u64, scope: u64| {
                    (who << 21 | (model as u64) << 1 | scope).wrapping_mul(0x9E37_79B9_7F4A_7C15)
                        | 1
                };
                let share = if p.sigma > 0.0 {
                    self.prefix_rng.lognormal(p.share, p.sigma)
                } else {
                    p.share
                }
                .clamp(0.0, 1.0);
                let group = mix(self.client_group[ci] as u64 + 1, 1);
                match p.scope {
                    PrefixScope::Group => (0, 0.0, group, share),
                    PrefixScope::Caller => {
                        if p.follow_up < 1.0 && self.prefix_rng.unit() >= p.follow_up {
                            self.conversation[ci] += 1;
                        }
                        let who = (self.conversation[ci] as u64) << 24 | (ci as u64 + 1);
                        let g = p.group_share.clamp(0.0, share);
                        (mix(who, 0), share, if g > 0.0 { group } else { 0 }, g)
                    }
                }
            }
            (None, None) => (0, 0.0, 0, 0.0),
        };
        let route_key = if prefix_key != 0 {
            prefix_key
        } else {
            group_key
        };
        if route_key != 0 {
            *self.prefix_open.entry(route_key).or_insert(0) += 1;
        }
        let c = &self.sc.clients[ci];
        let id = self.reqs.len() as u32;
        let deadline = c.timeout_s.map_or(f64::INFINITY, |t| self.now + t * 1000.0);
        self.reqs.push(Req {
            id,
            client: ci,
            model,
            prompt_tokens,
            out_tokens,
            sent_at: self.now,
            deadline,
            resend: 0,
            tries: 0,
            status: 0,
            first_token_at: None,
            done_at: None,
            queue_ms: 0.0,
            expected_wait_ms: None,
            waited_ms: None,
            seated: false,
            first_refusal_at: None,
            proxy_ms: 0.0,
            backend_queue_ms: 0.0,
            prefill_ms: 0.0,
            gen_ms: 0.0,
            prefix_key,
            store_key: if recorded_store != 0.0 {
                (recorded_store.to_bits() ^ (model as u64) << 1).wrapping_mul(0x9E37_79B9_7F4A_7C15)
                    | 1
            } else {
                prefix_key
            },
            prefix_share,
            group_key,
            group_share,
            prefix_hit: None,
            cached_tokens: 0.0,
            backend: None,
            epoch: 0,
            path: Vec::with_capacity(3),
            hops: Vec::with_capacity(3),
            hold: None,
            gw_attempts: 0,
            gw_seq: 0,
            agent,
            proxy_worker: 0,
            proxy_open: None,
            left: 0,
            tok_ms: 0.0,
            started: false,
            backend_at: 0.0,
            written: 0,
        });
        self.metrics.sent(ci);
        self.open += 1;
        if deadline.is_finite() {
            self.push(deadline, Ev::ClientTimeout { req: id });
        }
        self.send(id);
    }

    fn send(&mut self, req: u32) {
        let r = &mut self.reqs[req as usize];
        if r.status != 0 {
            return;
        }
        r.epoch += 1;
        r.tries += 1;
        r.gw_attempts = 0;
        r.started = false;
        let (ep, ci) = (r.epoch, r.client);
        let entry = self.client_entry[ci];
        let d = self.lat(Node::Client(ci), entry);
        let h = self.metrics.hop(Node::Client(ci), entry);
        let r = &mut self.reqs[req as usize];
        r.path.clear();
        r.path.push(entry);
        r.hops.clear();
        r.hops.push(h);
        self.push(self.now + d, Ev::At { req, ep, hop: 0 });
    }

    fn agent_wake(&mut self, agent: usize) {
        let (ci, model) = (self.agents[agent].client, self.agents[agent].model);
        let c = &self.sc.clients[ci];
        if self.now >= c.to_s * 1000.0 {
            return;
        }
        // A tool call before the turn, if the session has one.
        if let Some(s) = &c.session {
            if let Some(tool) = &s.tool {
                let p = s.tool_call_probability;
                let via_gw = s.tool_via_gateway && self.client_entry[ci] == Node::Gateway;
                let mcp = self
                    .sc
                    .mcp_servers
                    .iter()
                    .position(|m| &m.name == tool)
                    .expect("checked");
                if self.agents[agent].rng.unit() < p {
                    let (there, back) = if via_gw {
                        let over = self.sc.gateway.as_ref().map_or(0.0, |g| g.overhead_ms);
                        let a = self.lat(Node::Client(ci), Node::Gateway);
                        let b = self.lat(Node::Gateway, Node::Mcp(mcp));
                        self.metrics.hop(Node::Client(ci), Node::Gateway);
                        self.metrics.hop(Node::Gateway, Node::Mcp(mcp));
                        (a + b + over, a + b)
                    } else {
                        let a = self.lat(Node::Client(ci), Node::Mcp(mcp));
                        self.metrics.hop(Node::Client(ci), Node::Mcp(mcp));
                        (a, a)
                    };
                    self.push(
                        self.now + there,
                        Ev::McpArrive {
                            agent,
                            mcp,
                            back_ms: back,
                        },
                    );
                    return;
                }
            }
        }
        self.new_request(ci, model, None, None, Some(agent), None);
    }

    fn agent_next(&mut self, agent: usize, ok: bool) {
        let ci = self.agents[agent].client;
        let c = &self.sc.clients[ci];
        let Arrival::Closed { think_s, .. } = c.arrival else {
            return;
        };
        let turns = c.session.as_ref().map_or(1, |s| s.turns.max(1));
        let a = &mut self.agents[agent];
        a.turn = if ok { (a.turn + 1) % turns } else { a.turn };
        let mut think = if think_s > 0.0 {
            a.rng.exp(think_s * 1000.0)
        } else {
            0.0
        };
        if !ok {
            // A caller that was refused does not spin; it backs off.
            think = think.max(1000.0);
        }
        self.push(self.now + think.max(1.0), Ev::AgentWake { agent });
    }

    fn client_timeout(&mut self, req: u32) {
        if self.reqs[req as usize].status != 0 {
            return;
        }
        self.finish(req, GAVE_UP);
        self.abandon(req);
        self.resend(req);
    }

    /// A caller whose request timed out sends it again, if it is set to
    /// (`timeout_retries`). What it left behind keeps its server slot until
    /// the server notices, so the same prompt can be in the system twice.
    fn resend(&mut self, req: u32) {
        let old = &self.reqs[req as usize];
        let c = &self.sc.clients[old.client];
        let Some(timeout) = c.timeout_s else { return };
        if old.agent.is_some() || old.resend >= c.timeout_retries {
            return;
        }
        let id = self.reqs.len() as u32;
        let deadline = self.now + timeout * 1000.0;
        let new = Req {
            id,
            sent_at: self.now,
            deadline,
            resend: old.resend + 1,
            tries: 0,
            status: 0,
            first_token_at: None,
            done_at: None,
            queue_ms: 0.0,
            expected_wait_ms: None,
            waited_ms: None,
            seated: false,
            first_refusal_at: None,
            proxy_ms: 0.0,
            backend_queue_ms: 0.0,
            prefill_ms: 0.0,
            gen_ms: 0.0,
            prefix_hit: None,
            cached_tokens: 0.0,
            backend: None,
            epoch: 0,
            path: Vec::with_capacity(3),
            hops: Vec::with_capacity(3),
            hold: None,
            gw_attempts: 0,
            gw_seq: 0,
            proxy_worker: 0,
            proxy_open: None,
            left: 0,
            tok_ms: 0.0,
            started: false,
            backend_at: 0.0,
            written: 0,
            ..old.clone()
        };
        let (ci, route_key) = (
            new.client,
            if new.prefix_key != 0 {
                new.prefix_key
            } else {
                new.group_key
            },
        );
        if route_key != 0 {
            *self.prefix_open.entry(route_key).or_insert(0) += 1;
        }
        self.reqs.push(new);
        self.metrics.sent(ci);
        self.open += 1;
        self.push(deadline, Ev::ClientTimeout { req: id });
        self.send(id);
    }

    /// The caller closed its connection: the gateway lets go at once; proxies
    /// and backends find out when they next look.
    fn abandon(&mut self, req: u32) {
        let Some(h) = self.reqs[req as usize].hold else {
            return;
        };
        if h.leased.is_some() {
            self.gw_release(req);
        } else {
            let agent = self.agent_id(req);
            let g = self.gw.as_mut().expect("held by a gateway");
            g.ups[h.up].sched.remove(&agent, req);
            let q = g.ups[h.up].sched.queued() as f64;
            g.ups[h.up].queue.set(self.now, q);
            g.open -= 1;
            let r = &mut self.reqs[req as usize];
            if h.first && h.leased.is_none() {
                r.waited_ms = Some(self.now - h.enq_at);
            }
            r.hold = None;
            self.gw_log();
        }
    }

    fn finish(&mut self, req: u32, status: i32) {
        let r = &mut self.reqs[req as usize];
        if r.status != 0 {
            return;
        }
        r.status = status;
        r.done_at = Some(self.now);
        let route_key = if r.prefix_key != 0 {
            r.prefix_key
        } else {
            r.group_key
        };
        if let Some(n) = self.prefix_open.get_mut(&route_key) {
            *n = n.saturating_sub(1);
            if *n == 0 {
                self.prefix_open.remove(&route_key);
            }
        }
        let r = &mut self.reqs[req as usize];
        self.open -= 1;
        let (agent, ci) = (r.agent, r.client);
        let ttft = r.first_token_at.map(|t| t - r.sent_at);
        let (gen, out) = (r.gen_tps(self.now), r.out_tokens);
        self.metrics.finished(ci, status, ttft, gen, out, self.now);
        if let Some(a) = agent {
            self.agent_next(a, status == 200);
        }
    }

    fn agent_id(&self, req: u32) -> String {
        let c = &self.sc.clients[self.reqs[req as usize].client];
        c.agent.clone().unwrap_or_else(|| c.name.clone())
    }

    // ---- routing ----

    fn at(&mut self, req: u32, ep: u32, hop: usize) {
        match self.reqs[req as usize].path[hop] {
            Node::Gateway => {
                // An instance with modelled CPU works through requests on
                // its cores in arrival order before they can queue.
                let (cores, cpu, host) = {
                    let g = self.sc.gateway.as_ref().expect("gateway");
                    (g.cores, g.cpu_ms_per_request, g.host.clone())
                };
                if cores > 0.0 && cpu > 0.0 {
                    let inst = self.sc.instance_of(self.reqs[req as usize].client);
                    let host = host.and_then(|h| self.sc.hosts.iter().position(|x| x.name == h));
                    let dur = cpu / self.host_speed(host);
                    let free = &mut self.gw_cpu[inst];
                    let core = (0..free.len())
                        .min_by(|a, b| free[*a].total_cmp(&free[*b]))
                        .unwrap_or(0);
                    let start = free[core].max(self.now);
                    free[core] = start + dur;
                    self.gw_cpu_busy_ms += dur;
                    self.gw_cpu_wait_ms += start - self.now;
                    self.gw_cpu_seen += 1;
                    self.push(start + dur, Ev::GwReady { req, ep });
                } else {
                    self.gw_admit(req, ep);
                }
            }
            Node::Proxy(p) => {
                // The proxy pays for parsing even if the caller has gone.
                let st = &mut self.proxies[p];
                st.rr = (st.rr + 1) % st.workers.len();
                let w = st.rr;
                st.open += 1;
                let r = &mut self.reqs[req as usize];
                r.proxy_worker = w;
                r.proxy_open = Some(p);
                self.proxy_push(
                    p,
                    w,
                    Job {
                        req,
                        ep,
                        hop,
                        kind: JobKind::Start,
                        enq_at: self.now,
                    },
                );
            }
            Node::Server(s) => {
                self.reqs[req as usize].backend_at = self.now;
                self.servers[s].queue.push_back((req, ep, hop, self.now));
                let n = self.servers[s].queue.len() as f64;
                self.servers[s].qlen.set(self.now, n);
                self.server_start(s);
            }
            Node::Hosted(h) => self.hosted_arrive(req, ep, hop, h),
            Node::Client(_) | Node::Mcp(_) | Node::Db => {}
        }
    }

    /// Send a chunk from `path[hop]` one step back toward the caller.
    fn emit(&mut self, req: u32, ep: u32, hop: usize, kind: Kind) {
        let r = &self.reqs[req as usize];
        let from = r.path[hop];
        let to = if hop == 0 {
            Node::Client(r.client)
        } else {
            r.path[hop - 1]
        };
        let d = self.lat(from, to);
        self.push(
            self.now + d,
            Ev::Resp {
                req,
                ep,
                hop: hop as i32 - 1,
                kind,
            },
        );
    }

    /// A chunk between the first and the last: it only costs the proxy CPU.
    fn emit_mid(&mut self, req: u32, ep: u32, hop: usize, n: u32) {
        if hop == 0 {
            return;
        }
        if let Node::Proxy(p) = self.reqs[req as usize].path[hop - 1] {
            let w = self.reqs[req as usize].proxy_worker;
            self.proxy_push(
                p,
                w,
                Job {
                    req,
                    ep,
                    hop: hop - 1,
                    kind: JobKind::Mid(n),
                    enq_at: self.now,
                },
            );
        }
    }

    fn resp(&mut self, req: u32, ep: u32, hop: i32, kind: Kind) {
        if hop < 0 {
            return self.client_resp(req, ep, kind);
        }
        let hop = hop as usize;
        match self.reqs[req as usize].path[hop] {
            Node::Gateway => self.gw_resp(req, ep, kind),
            Node::Proxy(p) => {
                let w = self.reqs[req as usize].proxy_worker;
                self.proxy_push(
                    p,
                    w,
                    Job {
                        req,
                        ep,
                        hop,
                        kind: JobKind::Chunk(kind),
                        enq_at: self.now,
                    },
                );
            }
            _ => {}
        }
    }

    fn client_resp(&mut self, req: u32, ep: u32, kind: Kind) {
        if !self.live(req, ep) {
            return;
        }
        match kind {
            Kind::First => {
                let r = &mut self.reqs[req as usize];
                r.first_token_at.get_or_insert(self.now);
            }
            Kind::Last => {
                let r = &mut self.reqs[req as usize];
                r.first_token_at.get_or_insert(self.now);
                self.finish(req, 200);
            }
            Kind::Error { status, retry_ms } => {
                let ci = self.reqs[req as usize].client;
                // With `timeout_retries` set, a refusal is retried that many
                // times too (an SDK's retry count covers both).
                let c = &self.sc.clients[ci];
                let retry = c.retry
                    && matches!(status, 503 | 429)
                    && (c.timeout_retries == 0
                        || self.reqs[req as usize].tries <= c.timeout_retries);
                if retry {
                    let r = &mut self.reqs[req as usize];
                    r.first_refusal_at.get_or_insert(self.now);
                    let wait = retry_ms.max(100.0) * self.rng.range(0.8, 1.2);
                    self.push(self.now + wait, Ev::Send { req });
                } else {
                    self.finish(req, status);
                }
            }
        }
    }

    // ---- gateway ----

    /// A request is at the gateway and has had its CPU: answer it from the
    /// cache or let it queue.
    fn gw_admit(&mut self, req: u32, ep: u32) {
        if !self.live(req, ep) {
            return;
        }
        let cache = self
            .sc
            .gateway
            .as_ref()
            .and_then(|g| g.cache.as_ref())
            .map(|c| (c.hit_rate, c.lookup_ms));
        if let Some((hit_rate, lookup_ms)) = cache {
            // A request that was refused and came back is the same request:
            // it missed the first time.
            let r = &self.reqs[req as usize];
            if r.tries == 1 && r.gw_attempts == 0 && hit_rate > 0.0 {
                self.cache_lookups += 1;
                if self.cache_rng.unit() < hit_rate {
                    self.cache_hits += 1;
                    self.push(self.now + lookup_ms.max(0.0), Ev::GwCached { req, ep });
                    return;
                }
            }
        }
        self.gw.as_mut().expect("gateway").open += 1;
        self.gw_enqueue(req);
    }

    fn gw_enqueue(&mut self, req: u32) {
        let now = self.now;
        let r = &self.reqs[req as usize];
        let c = &self.sc.clients[r.client];
        let (weight, cap, max_wait) = (c.weight, c.agent_max_concurrency, c.max_wait_ms);
        let model = &self.models[r.model];
        let up = self
            .sc
            .upstream_at(model, self.sc.instance_of(r.client))
            .expect("checked");
        let ucfg = &self.sc.gateway.as_ref().expect("gateway").upstreams[up];
        // Held work waits by the hold's own limit, not the queue's.
        let held = ucfg.hold_weight.is_some_and(|h| weight <= h);
        let limit = if held {
            Some(ucfg.hold_timeout_s)
        } else {
            ucfg.queue_timeout_s
        };
        let patience = match (limit.map(|s| s * 1000.0), max_wait) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        // Time left for waiting once the request's own run is taken off.
        let own = self.gw.as_ref().expect("gateway").ups[up]
            .sched
            .service_estimate(weight);
        let left = (r.deadline - now - own).max(0.0);
        let by_deadline = ucfg.admit_by_deadline && left.is_finite();
        let adm = Admission {
            max_queue: ucfg.max_queue,
            evict: ucfg.overflow == Overflow::Evict,
            max_wait_ms: match (patience.filter(|_| ucfg.admit_by_wait), by_deadline) {
                (Some(p), true) => Some(p.min(left)),
                (None, true) => Some(left),
                (p, false) => p,
            },
            hold_weight: ucfg.hold_weight,
            hold_max: Some(ucfg.hold_max),
        };
        // Gradual shedding: the fuller the queue, the likelier a low weight
        // is turned away.
        let (shed, queue_cap) = (
            ucfg.shed,
            ucfg.max_queue.unwrap_or(ucfg.max_concurrency).max(1),
        );
        if let Some(sh) = shed.filter(|_| ucfg.hold_weight.is_none_or(|h| weight > h)) {
            let g = self.gw.as_mut().expect("gateway");
            if g.ups[up]
                .sched
                .shed(weight, sh.from, sh.low_ceiling, sh.floor, queue_cap)
            {
                self.shed_early += 1;
                self.gw_shed(req, up, 1000.0);
                return;
            }
        }
        let agent = self.agent_id(req);
        // What the gateway can tell before the answer: the size of the prompt.
        let cost = match ucfg.policy {
            Policy::RoundRobin => 1.0,
            Policy::Fair => r.prompt_tokens.max(1.0),
        };
        let first = r.tries == 1 && r.gw_attempts == 0;
        let g = self.gw.as_mut().expect("gateway");
        let u = &mut g.ups[up];
        match u
            .sched
            .try_enqueue(now, &agent, weight, cap, req, cost, adm)
        {
            Err(Refused::Full | Refused::HoldFull) => self.gw_shed(req, up, 1000.0),
            Err(Refused::WaitTooLong(o)) => {
                if first {
                    self.reqs[req as usize].expected_wait_ms = Some(o.wait_ms);
                }
                let over = (o.wait_ms - patience.unwrap_or(0.0)).max(0.0);
                self.gw_shed(req, up, (over / 1000.0).floor().max(1.0) * 1000.0);
            }
            Ok((o, evicted)) => {
                u.queue.set(now, u.sched.queued() as f64);
                let r = &mut self.reqs[req as usize];
                r.gw_seq += 1;
                r.hold = Some(Hold {
                    up,
                    enq_at: now,
                    first,
                    leased: None,
                });
                if first {
                    r.expected_wait_ms = Some(o.wait_ms);
                }
                let seq = r.gw_seq;
                if let Some(p) = patience {
                    self.push(now + p, Ev::GwTimeout { req, seq });
                }
                if let Some(v) = evicted {
                    let pushed = &mut self.reqs[v as usize];
                    if let Some(h) = pushed.hold.take().filter(|h| h.first) {
                        pushed.waited_ms = Some(now - h.enq_at);
                    }
                    let (va, vw) = (
                        self.agent_id(v),
                        self.sc.clients[self.reqs[v as usize].client].weight,
                    );
                    let g = self.gw.as_ref().expect("gateway");
                    let o = g.ups[up].sched.queue_status(now, &va, vw);
                    self.gw_shed(v, up, (o.wait_ms / 1000.0).floor().max(1.0) * 1000.0);
                }
                self.gw_dispatch(up);
            }
        }
    }

    /// Answer 503 with `retry-after`.
    fn gw_shed(&mut self, req: u32, up: usize, retry_ms: f64) {
        let g = self.gw.as_mut().expect("gateway");
        g.ups[up].shed += 1;
        g.open -= 1;
        self.gw_log();
        let ep = self.reqs[req as usize].epoch;
        self.emit(
            req,
            ep,
            0,
            Kind::Error {
                status: 503,
                retry_ms,
            },
        );
    }

    fn gw_timeout(&mut self, req: u32, seq: u32) {
        let r = &self.reqs[req as usize];
        let Some(h) = r.hold else { return };
        if r.status != 0 || r.gw_seq != seq || h.leased.is_some() {
            return;
        }
        let agent = self.agent_id(req);
        let g = self.gw.as_mut().expect("gateway");
        g.ups[h.up].sched.remove(&agent, req);
        let q = g.ups[h.up].sched.queued() as f64;
        g.ups[h.up].queue.set(self.now, q);
        let r = &mut self.reqs[req as usize];
        r.queue_ms += self.now - h.enq_at;
        if h.first {
            r.waited_ms = Some(self.now - h.enq_at);
        }
        r.hold = None;
        self.gw_shed(req, h.up, 1000.0);
        self.gw_dispatch(h.up);
    }

    fn gw_dispatch(&mut self, up: usize) {
        loop {
            let now = self.now;
            let g = self.gw.as_mut().expect("gateway");
            let u = &mut g.ups[up];
            let prefix =
                self.sc.gateway.as_ref().expect("gateway").upstreams[up].routing == Routing::Prefix;
            let granted = if prefix {
                // A request may be waiting for a warm target instead of a
                // free one: the scheduler passes its agent over meanwhile.
                let mut sched = std::mem::replace(&mut u.sched, Sched::new(0, false, 0.0));
                let retry = std::cell::Cell::new(f64::INFINITY);
                let got = sched.grant_where(now, &|req| match self.prefix_hold(up, req) {
                    Some(until) => {
                        retry.set(retry.get().min(until));
                        false
                    }
                    None => true,
                });
                self.gw.as_mut().expect("gateway").ups[up].sched = sched;
                match got {
                    Err(None) if retry.get().is_finite() => Err(Some(retry.get())),
                    other => other,
                }
            } else {
                u.sched.grant(now)
            };
            let g = self.gw.as_mut().expect("gateway");
            let u = &mut g.ups[up];
            let req = match granted {
                Ok(r) => r,
                Err(Some(at)) => {
                    if at < u.kick_at {
                        u.kick_at = at;
                        self.push(at, Ev::GwKick { up });
                    }
                    return;
                }
                Err(None) => return,
            };
            u.queue.set(now, u.sched.queued() as f64);
            u.busy.set(now, u.sched.inflight() as f64);
            // Pick where it goes.
            let ucfg = &self.sc.gateway.as_ref().expect("gateway").upstreams[up];
            let n = g.targets[up].len();
            let t = match ucfg.routing {
                Routing::LeastInflight => (0..n).min_by_key(|i| u.target_open[*i]).unwrap_or(0),
                Routing::LeastLoaded | Routing::Affinity | Routing::Cost => {
                    let spots: Vec<Spot> = (0..n)
                        .map(|i| Spot {
                            open: u.target_open[i] as f64,
                            cap: g.target_cap[up][i],
                            node: g.targets[up][i],
                        })
                        .collect();
                    self.choose(
                        ucfg.routing,
                        ucfg.replicate_at,
                        ucfg.cost_load_weight * if ucfg.cost_in_tokens { -1.0 } else { 1.0 },
                        req,
                        &spots,
                    )
                }
                Routing::Prefix => self.prefix_choose(up, req).unwrap_or(0),
                Routing::RoundRobin => {
                    u.rr = (u.rr + 1) % n;
                    u.rr
                }
                Routing::Random => self.rng.below(n),
            };
            if prefix {
                self.prefix_note(up, t, req);
            }
            let g = self.gw.as_mut().expect("gateway");
            g.ups[up].target_open[t] += 1;
            let target = g.targets[up][t];
            let over = self.sc.gateway.as_ref().expect("gateway").overhead_ms;
            let r = &mut self.reqs[req as usize];
            let h = r.hold.as_mut().expect("queued requests are held");
            r.queue_ms += now - h.enq_at;
            if h.first {
                r.waited_ms = Some(now - h.enq_at);
                r.seated = true;
            }
            h.leased = Some((now, t));
            r.path.truncate(1);
            r.path.push(target);
            let ep = r.epoch;
            let h = self.metrics.hop(Node::Gateway, target);
            r.hops.truncate(1);
            r.hops.push(h);
            let d = self.lat(Node::Gateway, target);
            self.push(now + over + d, Ev::At { req, ep, hop: 1 });
        }
    }

    /// The share of a request's prompt the gateway believes target `t`
    /// holds: its own context if a prompt with it was sent there and not
    /// pushed out of the picture since, else the group's common start.
    fn prefix_believed(&self, up: usize, t: usize, req: u32) -> f64 {
        let r = &self.reqs[req as usize];
        let held = &self.gw.as_ref().expect("gateway").ups[up].belief[t];
        // The gateway does not steer for a shared start under 64 tokens.
        let worth = |share: f64| {
            if share * r.prompt_tokens >= 64.0 {
                share
            } else {
                0.0
            }
        };
        if r.prefix_key != 0 && held.iter().any(|e| e.0 == r.prefix_key) {
            worth(r.prefix_share)
        } else if r.group_key != 0 && held.iter().any(|e| e.1 == r.group_key) {
            worth(r.group_share)
        } else {
            0.0
        }
    }

    /// `prefix` routing: the target with a free seat this request goes to.
    fn prefix_choose(&self, up: usize, req: u32) -> Option<usize> {
        let g = self.gw.as_ref().expect("gateway");
        let u = &g.ups[up];
        let spill = self.sc.gateway.as_ref().expect("gateway").upstreams[up].spill;
        let load = |i: usize| u.target_open[i] as f64 / g.target_cap[up][i];
        let free: Vec<usize> = (0..u.target_open.len())
            .filter(|i| (u.target_open[*i] as f64) < g.target_cap[up][*i])
            .collect();
        let least = free
            .iter()
            .copied()
            .min_by(|a, b| load(*a).total_cmp(&load(*b)))?;
        let warm = free
            .iter()
            .map(|i| (self.prefix_believed(up, *i, req), *i))
            .filter(|(share, i)| *share > 0.0 && load(*i) <= load(least) + spill * share)
            .max_by(|a, b| a.0.total_cmp(&b.0).then(load(b.1).total_cmp(&load(a.1))));
        Some(warm.map_or(least, |(_, i)| i))
    }

    /// `affinity_wait_s`: `Some(until)` while this request should stay in
    /// the queue because a full target holds at least half of its prompt
    /// and more than the free one it would get.
    fn prefix_hold(&self, up: usize, req: u32) -> Option<f64> {
        let wait =
            self.sc.gateway.as_ref().expect("gateway").upstreams[up].affinity_wait_s * 1000.0;
        if wait <= 0.0 {
            return None;
        }
        let until = self.reqs[req as usize].hold.as_ref()?.enq_at + wait;
        if self.now >= until {
            return None;
        }
        let g = self.gw.as_ref().expect("gateway");
        let u = &g.ups[up];
        let got = self
            .prefix_choose(up, req)
            .map_or(0.0, |t| self.prefix_believed(up, t, req));
        (0..u.target_open.len())
            .any(|i| {
                let share = self.prefix_believed(up, i, req);
                share > got && share >= 0.5 && u.target_open[i] as f64 >= g.target_cap[up][i]
            })
            .then_some(until)
    }

    /// The gateway's picture after sending a request to target `t`: the
    /// server continues in the slot of the most similar prompt it holds,
    /// which then holds this one; otherwise the prompt sent longest ago
    /// makes room.
    fn prefix_note(&mut self, up: usize, t: usize, req: u32) {
        let r = &self.reqs[req as usize];
        let (key, gkey, gshare, store) = (r.prefix_key, r.group_key, r.group_share, r.store_key);
        if store == 0 && gkey == 0 {
            return;
        }
        let cap = self.sc.gateway.as_ref().expect("gateway").upstreams[up].prefix_slots;
        let g = self.gw.as_mut().expect("gateway");
        let cap = cap.unwrap_or(g.target_cap[up][t] as usize).max(1);
        let held = &mut g.ups[up].belief[t];
        let same = held
            .iter()
            .position(|e| key != 0 && e.0 == key)
            .or_else(|| {
                held.iter()
                    .position(|e| gkey != 0 && gshare >= 0.1 && e.1 == gkey)
            });
        if let Some(i) = same {
            held.remove(i);
        }
        held.push_front((store, gkey));
        held.truncate(cap);
    }

    /// Free the slot a request held and let the next one go.
    fn gw_release(&mut self, req: u32) {
        let Some(h) = self.reqs[req as usize].hold.take() else {
            return;
        };
        let Some((since, t)) = h.leased else { return };
        let agent = self.agent_id(req);
        let weight = self.sc.clients[self.reqs[req as usize].client].weight;
        let now = self.now;
        let g = self.gw.as_mut().expect("gateway");
        let u = &mut g.ups[h.up];
        u.sched.attain(now, &agent, now - since);
        u.sched.release(&agent, weight, now - since);
        u.target_open[t] -= 1;
        u.busy.set(now, u.sched.inflight() as f64);
        u.done += 1;
        g.open -= 1;
        self.gw_log();
        self.gw_dispatch(h.up);
    }

    fn gw_resp(&mut self, req: u32, ep: u32, kind: Kind) {
        if !self.live(req, ep) {
            return;
        }
        match kind {
            Kind::First => self.emit(req, ep, 0, kind),
            Kind::Last => {
                self.gw_release(req);
                self.emit(req, ep, 0, kind);
            }
            Kind::Error { status, retry_ms } => {
                let max = self.sc.gateway.as_ref().expect("gateway").max_retries;
                let retriable = status == 429 || status >= 500;
                let r = &mut self.reqs[req as usize];
                if retriable && r.gw_attempts < max && !r.started {
                    r.gw_attempts += 1;
                    let up = r.hold.map(|h| h.up);
                    // Give the slot back, pause the whole upstream on a rate
                    // limit, and queue again.
                    if let (Some(up), true) = (up, status == 429) {
                        let until = self.now + retry_ms;
                        self.gw.as_mut().expect("gateway").ups[up]
                            .sched
                            .cooldown(until);
                    }
                    self.gw_release(req);
                    self.gw.as_mut().expect("gateway").open += 1;
                    self.gw_enqueue(req);
                } else {
                    self.gw_release(req);
                    self.emit(req, ep, 0, kind);
                }
            }
        }
    }

    /// A request left the gateway: one more log entry for memory and the store.
    fn gw_log(&mut self) {
        if let Some(g) = self.gw.as_mut() {
            g.logged += 1;
        }
        let Some(cfg) = self.sc.db.as_ref() else {
            return;
        };
        let (rows, max, interval) = (
            cfg.rows_per_request,
            cfg.max_buffer_rows,
            cfg.flush_interval_ms,
        );
        let now = self.now;
        let db = self.db.as_mut().expect("db state");
        let room = (max - db.buffer).max(0.0);
        db.buffer += rows.min(room);
        db.dropped += (rows - room).max(0.0);
        db.backlog.set(now, db.buffer);
        if !db.flush_due {
            db.flush_due = true;
            self.push(now + interval, Ev::DbFlush);
        }
    }

    pub fn gateway_memory_mb(&self) -> f64 {
        match (&self.gw, &self.sc.gateway) {
            (Some(g), Some(cfg)) => {
                let logged = if self.sc.db.is_some() {
                    0.0
                } else {
                    g.logged as f64
                };
                cfg.memory.base_mb
                    + (g.open as f64 * cfg.memory.per_open_request_kb
                        + logged * cfg.memory.per_logged_request_kb)
                        / 1024.0
            }
            _ => 0.0,
        }
    }

    // ---- proxy ----

    fn proxy_push(&mut self, p: usize, w: usize, job: Job) {
        self.proxies[p].workers[w].queue.push_back(job);
        self.proxy_gauge(p);
        self.proxy_run(p, w);
    }

    fn proxy_gauge(&mut self, p: usize) {
        let st = &mut self.proxies[p];
        let q: usize = st.workers.iter().map(|w| w.queue.len()).sum();
        let b = st.workers.iter().filter(|w| w.busy.is_some()).count();
        st.queue.set(self.now, q as f64);
        st.busy.set(self.now, b as f64);
    }

    fn proxy_run(&mut self, p: usize, w: usize) {
        if self.proxies[p].workers[w].busy.is_some() {
            return;
        }
        let Some(job) = self.proxies[p].workers[w].queue.pop_front() else {
            return;
        };
        let cfg = &self.sc.proxies[p];
        let base = match job.kind {
            JobKind::Start => cfg.cpu_ms_per_request,
            JobKind::After => cfg.cpu_ms_after_request,
            JobKind::Mid(n) => cfg.cpu_ms_per_chunk * f64::from(n),
            JobKind::Chunk(_) => cfg.cpu_ms_per_chunk,
        };
        let jitter = cfg.jitter;
        let host = self.proxies[p].host;
        self.host_demand(host, 1.0);
        let cpu = base * self.proxies[p].rng.lognormal(1.0, jitter);
        let dur = cpu / self.host_speed(host);
        self.proxies[p].workers[w].busy = Some(job);
        self.proxy_gauge(p);
        self.push(
            self.now + dur,
            Ev::ProxyDone {
                proxy: p,
                worker: w,
            },
        );
    }

    fn proxy_done(&mut self, p: usize, w: usize) {
        let job = self.proxies[p].workers[w].busy.take().expect("was busy");
        let host = self.proxies[p].host;
        self.host_demand(host, -1.0);
        self.proxy_gauge(p);
        let live = self.live(job.req, job.ep);
        match job.kind {
            JobKind::Start => {
                if live {
                    self.reqs[job.req as usize].proxy_ms += self.now - job.enq_at;
                    self.proxy_forward(p, job);
                } else {
                    self.proxy_close(job.req);
                }
            }
            JobKind::Chunk(kind) => {
                if kind != Kind::First {
                    self.proxy_close(job.req);
                    self.proxies[p].done += 1;
                    if self.sc.proxies[p].cpu_ms_after_request > 0.0 {
                        let after = Job {
                            kind: JobKind::After,
                            enq_at: self.now,
                            ..job
                        };
                        self.proxies[p].workers[w].queue.push_back(after);
                    }
                }
                if live {
                    self.reqs[job.req as usize].proxy_ms += self.now - job.enq_at;
                    self.emit(job.req, job.ep, job.hop, kind);
                }
            }
            JobKind::Mid(_) | JobKind::After => {}
        }
        self.proxy_run(p, w);
    }

    fn proxy_close(&mut self, req: u32) {
        if let Some(p) = self.reqs[req as usize].proxy_open.take() {
            self.proxies[p].open = self.proxies[p].open.saturating_sub(1);
        }
    }

    fn backend_node(&self, b: usize) -> Node {
        let n = self.sc.model_servers.len();
        if b < n {
            Node::Server(b)
        } else {
            Node::Hosted(b - n)
        }
    }

    fn backend_load(&self, b: usize) -> usize {
        match self.backend_node(b) {
            Node::Server(s) => self.servers[s].queue.len() + self.servers[s].active,
            Node::Hosted(h) => self.hosted[h].open,
            _ => 0,
        }
    }

    /// Requests a backend runs at once.
    fn backend_cap(&self, b: usize) -> f64 {
        match self.backend_node(b) {
            Node::Server(s) => self.sc.model_servers[s].full() as f64,
            Node::Hosted(h) => match self.sc.hosted_apis[h].max_concurrency {
                0 => 64.0,
                n => n as f64,
            },
            _ => 1.0,
        }
    }

    /// Output tokens each model server has produced so far.
    pub fn server_tokens(&self) -> Vec<u64> {
        self.servers.iter().map(|s| s.tokens).collect()
    }

    /// Where a request goes under the routings that look at load or cache.
    fn choose(
        &self,
        routing: Routing,
        replicate_at: f64,
        w: f64,
        req: u32,
        spots: &[Spot],
    ) -> usize {
        let n = spots.len();
        let load = |i: usize| spots[i].open / spots[i].cap;
        let least = || {
            (0..n)
                .min_by(|a, b| load(*a).total_cmp(&load(*b)))
                .unwrap_or(0)
        };
        let r = &self.reqs[req as usize];
        let key = if r.prefix_key != 0 {
            r.prefix_key
        } else {
            r.group_key
        };
        match routing {
            Routing::Affinity if key != 0 => {
                let first = (key >> 20) as usize % n;
                let open = self.prefix_open.get(&key).copied().unwrap_or(0) as f64;
                let homes = if replicate_at > 0.0 {
                    (1 + (open / (replicate_at * spots[first].cap)) as usize).min(n)
                } else {
                    1
                };
                (0..homes)
                    .map(|i| (first + i) % n)
                    .filter(|i| load(*i) < 1.0)
                    .min_by(|a, b| load(*a).total_cmp(&load(*b)))
                    .unwrap_or_else(least)
            }
            Routing::Cost => {
                // A negative weight asks for the count of tokens instead of
                // the time they take.
                let (w, in_tokens) = (w.abs(), w < 0.0);
                let cost = |i: usize| match spots[i].node {
                    Node::Server(s) => {
                        let cfg = &self.sc.model_servers[s];
                        let found = self.server_has(s, r);
                        let unread = r.prompt_tokens
                            * (1.0 - found * cfg.prompt_cache_saving.clamp(0.0, 1.0));
                        if in_tokens || cfg.prefill_tokens_per_s <= 0.0 {
                            let (read, write) = self.server_pending_tokens(s);
                            unread + w * (read + write) / spots[i].cap
                        } else {
                            // A full target costs as much as doing the
                            // request once more.
                            let own = r.prompt_tokens / cfg.prefill_tokens_per_s
                                + f64::from(r.out_tokens) / cfg.tokens_per_s;
                            unread / cfg.prefill_tokens_per_s + w * load(i) * own
                        }
                    }
                    // Nothing is cached at a proxy: only load tells them apart.
                    _ => load(i),
                };
                (0..n)
                    .min_by(|a, b| {
                        cost(*a)
                            .total_cmp(&cost(*b))
                            .then(load(*a).total_cmp(&load(*b)))
                    })
                    .unwrap_or(0)
            }
            _ => least(),
        }
    }

    /// The share of a request's prompt a server has cached, as a router
    /// that remembers what it sent there would believe: it does not know
    /// whether the slot holding it is busy.
    fn server_has(&self, s: usize, r: &Req) -> f64 {
        let st = &self.servers[s];
        let has = |k: u64, own: bool| {
            k != 0
                && if st.kv_cap == 0 {
                    st.slot_kv
                        .iter()
                        .any(|x| if own { x.0 == k } else { x.1 == k })
                } else {
                    st.kv.iter().any(|e| if own { e.0 == k } else { e.1 == k })
                }
        };
        if has(r.prefix_key, true) {
            r.prefix_share
        } else if has(r.group_key, false) {
            r.group_share
        } else {
            0.0
        }
    }

    /// Prompt tokens a server still has to read and output tokens it still
    /// has to write for what it holds.
    fn server_pending_tokens(&self, s: usize) -> (f64, f64) {
        let st = &self.servers[s];
        let (mut read, mut write) = (0.0, 0.0);
        for q in &st.queue {
            let r = &self.reqs[q.0 as usize];
            read += r.prompt_tokens;
            write += f64::from(r.out_tokens);
        }
        for run in st.slots.iter().flatten() {
            if matches!(run.phase, Phase::Prefill) {
                read += self.reqs[run.req as usize].prompt_tokens;
            }
            write += f64::from(run.left);
        }
        (read, write)
    }

    fn backend_serves(&self, b: usize, model: &str) -> bool {
        match self.backend_node(b) {
            Node::Server(s) => self.sc.model_servers[s].models.iter().any(|m| m == model),
            Node::Hosted(h) => self.sc.hosted_apis[h].models.iter().any(|m| m == model),
            _ => false,
        }
    }

    fn proxy_forward(&mut self, p: usize, job: Job) {
        let model = self.models[self.reqs[job.req as usize].model].clone();
        let cands: Vec<usize> = self.proxies[p]
            .backends
            .iter()
            .copied()
            .filter(|b| self.backend_serves(*b, &model))
            .collect();
        let pick = match self.sc.proxies[p].routing {
            Routing::LeastInflight => cands
                .iter()
                .copied()
                .min_by_key(|b| self.backend_load(*b))
                .expect("checked"),
            Routing::LeastLoaded | Routing::Affinity | Routing::Cost | Routing::Prefix => {
                let spots: Vec<Spot> = cands
                    .iter()
                    .map(|b| Spot {
                        open: self.backend_load(*b) as f64,
                        cap: self.backend_cap(*b),
                        node: self.backend_node(*b),
                    })
                    .collect();
                let cfg = &self.sc.proxies[p];
                cands[self.choose(
                    cfg.routing,
                    cfg.replicate_at,
                    cfg.cost_load_weight * if cfg.cost_in_tokens { -1.0 } else { 1.0 },
                    job.req,
                    &spots,
                )]
            }
            Routing::RoundRobin => {
                let st = &mut self.proxies[p];
                st.rr = st.rr.wrapping_add(1);
                cands[st.rr % cands.len()]
            }
            Routing::Random => cands[self.proxies[p].rng.below(cands.len())],
        };
        let target = self.backend_node(pick);
        let r = &mut self.reqs[job.req as usize];
        r.path.truncate(job.hop + 1);
        r.path.push(target);
        let h = self.metrics.hop(Node::Proxy(p), target);
        r.hops.truncate(job.hop + 1);
        r.hops.push(h);
        let d = self.lat(Node::Proxy(p), target);
        self.push(
            self.now + d,
            Ev::At {
                req: job.req,
                ep: job.ep,
                hop: job.hop + 1,
            },
        );
    }

    // ---- model server ----

    fn server_speed(&self, s: usize) -> f64 {
        let cfg = &self.sc.model_servers[s];
        let n = self.servers[s].active.max(1) as f64;
        self.host_speed(self.servers[s].host) * cfg.share(n)
    }

    fn server_start(&mut self, s: usize) {
        loop {
            let st = &self.servers[s];
            if st.active >= st.slots.len() || st.queue.is_empty() {
                break;
            }
            // The next request starts only when its context fits.
            let need = {
                let r = &self.reqs[self.servers[s].queue[0].0 as usize];
                r.prompt_tokens + f64::from(r.written)
            };
            if let Some(room) = self.sc.model_servers[s].context_tokens {
                if st.active > 0 && st.ctx + need > room {
                    break;
                }
            }
            let (req, ep, hop, enq) = self.servers[s].queue.pop_front().expect("non-empty");
            let n = self.servers[s].queue.len() as f64;
            self.servers[s].qlen.set(self.now, n);
            let cfg = &self.sc.model_servers[s];
            let ghost = !self.live(req, ep);
            if ghost && cfg.on_disconnect == OnDisconnect::Cancel {
                self.proxy_close(req);
                continue;
            }
            let (jitter, tps) = (cfg.jitter, cfg.tokens_per_s);
            let mut prefill = cfg.prefill_ms;
            // A prompt whose start is in the cache is read only from where
            // it differs.
            let (store, key, share, gkey, gshare, tokens) = {
                let r = &self.reqs[req as usize];
                (
                    r.store_key,
                    r.prefix_key,
                    r.prefix_share,
                    r.group_key,
                    r.group_share,
                    r.prompt_tokens,
                )
            };
            let mut read = 1.0;
            let mut slot = None;
            if store != 0 || key != 0 || gkey != 0 {
                let now = self.now;
                let st = &mut self.servers[s];
                let found = if st.kv_cap == 0 {
                    // llama.cpp: the idle slot whose prompt starts most like
                    // this one (at least a tenth of it), else the idle slot
                    // used longest ago.
                    let idle = |i: &usize| st.slots[*i].is_none();
                    let own = (0..st.slots.len())
                        .filter(idle)
                        .find(|i| key != 0 && st.slot_kv[*i].0 == key);
                    let same_group = (0..st.slots.len())
                        .filter(idle)
                        .filter(|i| gkey != 0 && gshare >= 0.1 && st.slot_kv[*i].1 == gkey)
                        .max_by(|a, b| st.slot_kv[*a].2.total_cmp(&st.slot_kv[*b].2));
                    let oldest = (0..st.slots.len())
                        .filter(idle)
                        .min_by(|a, b| st.slot_kv[*a].2.total_cmp(&st.slot_kv[*b].2));
                    let (i, found) = match (own, same_group) {
                        (Some(i), _) => (i, share),
                        (None, Some(i)) => (i, gshare),
                        (None, None) => (oldest.expect("free"), 0.0),
                    };
                    st.slot_kv[i] = (store, gkey, now);
                    slot = Some(i);
                    found
                } else {
                    // The server keeps this many prompts. A prompt
                    // continues where the most similar one it holds left
                    // off, and takes that one's place; otherwise the one
                    // used longest ago makes room.
                    let own = st.kv.iter().position(|e| key != 0 && e.0 == key);
                    let group = st
                        .kv
                        .iter()
                        .position(|e| gkey != 0 && gshare >= 0.1 && e.1 == gkey);
                    if let Some(i) = own.or(group) {
                        st.kv.remove(i);
                    }
                    st.kv.push_front((store, gkey));
                    st.kv.truncate(st.kv_cap);
                    let (own, group) = (own.is_some(), group.is_some());
                    if own {
                        share
                    } else if group {
                        gshare
                    } else {
                        0.0
                    }
                };
                let saved = found * cfg.prompt_cache_saving.clamp(0.0, 1.0);
                read = 1.0 - saved;
                let hit = found > 0.0;
                self.reqs[req as usize].prefix_hit = Some(hit);
                self.reqs[req as usize].cached_tokens = tokens * saved;
                self.prefix_lookups += 1;
                self.prefix_hits += hit as u64;
                self.prefix_tokens += tokens;
                self.prefix_tokens_saved += tokens * saved;
                let client = self.reqs[req as usize].client;
                self.metrics.prefix_lookup(client, hit);
            }
            let written = self.reqs[req as usize].written;
            if cfg.prefill_tokens_per_s > 0.0 {
                // What was written before it was put back is read again.
                prefill += (self.reqs[req as usize].prompt_tokens * read + f64::from(written))
                    / cfg.prefill_tokens_per_s
                    * 1000.0;
            } else {
                prefill *= read;
            }
            if cfg.prompt_cache_hit_rate > 0.0
                && self.servers[s].rng.unit() < cfg.prompt_cache_hit_rate
            {
                prefill *= 1.0 - cfg.prompt_cache_saving.clamp(0.0, 1.0);
            }
            if ghost && cfg.on_disconnect == OnDisconnect::FirstToken {
                prefill += cfg.abandon_extra_ms;
            }
            let threads = cfg.threads;
            let st = &mut self.servers[s];
            let prefill = prefill * st.rng.lognormal(1.0, jitter * 2.0);
            let tok_ms = 1000.0 / tps * st.rng.lognormal(1.0, jitter);
            let slot =
                slot.unwrap_or_else(|| st.slots.iter().position(Option::is_none).expect("free"));
            st.slots[slot] = Some(Run {
                req,
                ep,
                hop,
                phase: Phase::Prefill,
                left: self.reqs[req as usize].out_tokens - written,
                ghost,
                tok_ms,
                started: self.now,
                seq: st.run_seq + 1,
                ctx: need,
                resumed: written > 0,
            });
            st.run_seq += 1;
            st.reading += 1;
            st.ctx += need;
            let run = st.run_seq;
            st.active += 1;
            let active = st.active;
            st.busy.set(self.now, active as f64);
            let host = st.host;
            if active == 1 {
                self.host_demand(host, threads);
            }
            if !ghost {
                let r = &mut self.reqs[req as usize];
                r.backend_queue_ms += self.now - enq;
                r.backend = Some(s);
            }
            // Read in chunks between the answers' steps, a prompt is not
            // slowed by them; it slows them.
            // Prompts being read at once share the reading speed (as
            // many as are being read when this one starts).
            let speed = if self.sc.model_servers[s].prefill_chunk_tokens > 0.0 {
                self.host_speed(self.servers[s].host) / self.servers[s].reading.max(1) as f64
            } else {
                self.server_speed(s)
            };
            self.push(
                self.now + prefill / speed,
                Ev::Step {
                    server: s,
                    slot,
                    run,
                },
            );
        }
    }

    fn server_free(&mut self, s: usize, slot: usize, wasted: bool) {
        let run = self.servers[s].slots[slot].take().expect("running");
        let threads = self.sc.model_servers[s].threads;
        let st = &mut self.servers[s];
        st.active -= 1;
        st.ctx = (st.ctx - run.ctx).max(0.0);
        if run.phase == Phase::Prefill {
            st.reading -= 1;
        }
        let active = st.active;
        st.busy.set(self.now, active as f64);
        let host = st.host;
        if wasted {
            st.wasted_ms += self.now - run.started;
            st.ghosts += 1;
            self.proxy_close(run.req);
        } else {
            st.done += 1;
        }
        if active == 0 {
            self.host_demand(host, -threads);
        }
        self.server_start(s);
    }

    fn server_step(&mut self, s: usize, slot: usize, seq: u64) {
        let mode = self.sc.model_servers[s].on_disconnect;
        let (req, ep, hop, phase, ghost, started, resumed) = {
            // The run this step was meant for may have been put back.
            let Some(r) = self.servers[s].slots[slot]
                .as_ref()
                .filter(|r| r.seq == seq)
            else {
                return;
            };
            (r.req, r.ep, r.hop, r.phase, r.ghost, r.started, r.resumed)
        };
        // The connection is checked each time a chunk is written.
        let gone = !ghost && !self.live(req, ep);
        if (ghost || gone) && mode != OnDisconnect::Finish {
            return self.server_free(s, slot, true);
        }
        let ghost = ghost || gone;
        // After the first token an answer advances `token_stride` tokens an
        // event: the same time and the same counts, with the connection
        // checked and the speed set once a stride.
        let stride = if phase == Phase::Gen {
            self.sc.token_stride.max(1)
        } else {
            1
        };
        let run = self.servers[s].slots[slot].as_mut().expect("running");
        run.ghost = ghost;
        let k = stride.min(run.left);
        run.left -= k;
        run.phase = Phase::Gen;
        run.ctx += f64::from(k);
        let (left, tok_ms) = (run.left, run.tok_ms);
        let st = &mut self.servers[s];
        st.tokens += u64::from(k);
        st.ctx += f64::from(k);
        if phase == Phase::Prefill {
            st.reading -= 1;
        }
        if !ghost {
            self.metrics.token_back(&self.reqs[req as usize].hops, k);
            self.metrics.token_out(self.reqs[req as usize].client, k);
            if phase == Phase::Prefill && !resumed {
                self.reqs[req as usize].prefill_ms = self.now - started;
                self.reqs[req as usize].started = true;
                self.emit(req, ep, hop, Kind::First);
            }
            if left == 0 {
                let r = &mut self.reqs[req as usize];
                r.gen_ms = self.now - started - r.prefill_ms;
                self.emit(req, ep, hop, Kind::Last);
            } else if phase == Phase::Gen || resumed {
                self.emit_mid(req, ep, hop, k);
            }
        }
        if left == 0 {
            return self.server_free(s, slot, ghost);
        }
        self.server_make_room(s, slot);
        // Writing tokens feels less of a crowded host than reading does.
        let cfg = &self.sc.model_servers[s];
        let n = self.servers[s].active.max(1) as f64;
        let host = 1.0 - cfg.gen_contention * (1.0 - self.host_speed(self.servers[s].host));
        let speed = host * cfg.share(n);
        let next = self.sc.token_stride.max(1).min(left);
        // While a prompt is being read, a chunk of it goes between every
        // two steps of the answers.
        let stall = if self.servers[s].reading > 0 && cfg.prefill_tokens_per_s > 0.0 {
            cfg.prefill_chunk_tokens.max(0.0) / cfg.prefill_tokens_per_s * 1000.0
        } else {
            0.0
        };
        self.push(
            self.now + (tok_ms / speed + stall) * f64::from(next),
            Ev::Step {
                server: s,
                slot,
                run: seq,
            },
        );
    }

    /// The answers have outgrown the room for context: the request that
    /// started last (not the one in `keep`) goes back to the head of the
    /// queue and will be read again from the start.
    fn server_make_room(&mut self, s: usize, keep: usize) {
        let Some(room) = self.sc.model_servers[s].context_tokens else {
            return;
        };
        while self.servers[s].ctx > room {
            let st = &self.servers[s];
            let victim = (0..st.slots.len())
                .filter(|i| *i != keep)
                .filter_map(|i| st.slots[i].as_ref().map(|r| (i, r.started, r.seq)))
                .max_by(|a, b| a.1.total_cmp(&b.1).then(a.2.cmp(&b.2)))
                .map(|x| x.0);
            let Some(v) = victim else { return };
            let st = &mut self.servers[s];
            let run = st.slots[v].take().expect("running");
            st.active -= 1;
            st.ctx = (st.ctx - run.ctx).max(0.0);
            if run.phase == Phase::Prefill {
                st.reading -= 1;
            }
            let active = st.active;
            st.busy.set(self.now, active as f64);
            st.queue.push_front((run.req, run.ep, run.hop, self.now));
            let n = st.queue.len() as f64;
            st.qlen.set(self.now, n);
            let r = &mut self.reqs[run.req as usize];
            r.written = r.out_tokens - run.left;
            self.preempted += 1;
            self.preempted_tokens += run.ctx;
        }
    }

    // ---- hosted api ----

    fn hosted_arrive(&mut self, req: u32, ep: u32, hop: usize, h: usize) {
        if !self.live(req, ep) {
            self.proxy_close(req);
            return;
        }
        let now = self.now;
        let cfg = &self.sc.hosted_apis[h];
        let st = &mut self.hosted[h];
        let dt_min = (now - st.refilled_at) / 60_000.0;
        st.refilled_at = now;
        st.rpm_tokens = (st.rpm_tokens + cfg.rpm * dt_min).min(cfg.rpm);
        st.tpm_tokens = (st.tpm_tokens + cfg.tpm * dt_min).min(cfg.tpm);
        let r = &self.reqs[req as usize];
        let cost = r.prompt_tokens + r.out_tokens as f64;
        let mut retry_ms: f64 = 0.0;
        if cfg.rpm > 0.0 && st.rpm_tokens < 1.0 {
            retry_ms = retry_ms.max((1.0 - st.rpm_tokens) / cfg.rpm * 60_000.0);
        }
        if cfg.tpm > 0.0 && st.tpm_tokens < cost {
            retry_ms = retry_ms.max((cost - st.tpm_tokens) / cfg.tpm * 60_000.0);
        }
        if cfg.max_concurrency > 0 && st.open >= cfg.max_concurrency {
            retry_ms = retry_ms.max(1000.0);
        }
        if retry_ms > 0.0 {
            st.limited += 1;
            self.emit(
                req,
                ep,
                hop,
                Kind::Error {
                    status: 429,
                    retry_ms: retry_ms.max(1000.0),
                },
            );
            return;
        }
        if cfg.rpm > 0.0 {
            st.rpm_tokens -= 1.0;
        }
        if cfg.tpm > 0.0 {
            st.tpm_tokens -= cost;
        }
        st.open += 1;
        st.busy.set(now, st.open as f64);
        let ttft = st.rng.lognormal(cfg.ttft_ms.median, cfg.ttft_ms.sigma)
            + cfg.ttft_ms_per_prompt_token * r.prompt_tokens;
        let tps = st
            .rng
            .lognormal(cfg.tokens_per_s.median, cfg.tokens_per_s.sigma);
        let r = &mut self.reqs[req as usize];
        r.left = r.out_tokens;
        r.tok_ms = 1000.0 / tps.max(1e-6);
        r.backend_at = now;
        r.backend = Some(self.sc.model_servers.len() + h);
        self.push(now + ttft, Ev::HostedTok { req, ep, hop });
    }

    fn hosted_tok(&mut self, req: u32, ep: u32, hop: usize) {
        let Node::Hosted(h) = self.reqs[req as usize].path[hop] else {
            return;
        };
        let now = self.now;
        if !self.live(req, ep) {
            // Providers stop generating when the stream is closed.
            let st = &mut self.hosted[h];
            st.open -= 1;
            st.busy.set(now, st.open as f64);
            self.proxy_close(req);
            return;
        }
        let r = &mut self.reqs[req as usize];
        let first = !r.started;
        r.started = true;
        r.left -= 1;
        let (left, tok_ms, at) = (r.left, r.tok_ms, r.backend_at);
        if first {
            r.prefill_ms = now - at;
        }
        if left == 0 {
            r.gen_ms = now - at - r.prefill_ms;
        }
        self.hosted[h].tokens += 1;
        self.metrics.token_back(&self.reqs[req as usize].hops, 1);
        self.metrics.token_out(self.reqs[req as usize].client, 1);
        if first {
            self.emit(req, ep, hop, Kind::First);
        }
        if left == 0 {
            let st = &mut self.hosted[h];
            st.open -= 1;
            st.done += 1;
            st.busy.set(now, st.open as f64);
            self.emit(req, ep, hop, Kind::Last);
        } else {
            if !first {
                self.emit_mid(req, ep, hop, 1);
            }
            self.push(now + tok_ms, Ev::HostedTok { req, ep, hop });
        }
    }

    // ---- mcp ----

    fn mcp_arrive(&mut self, agent: usize, mcp: usize, back_ms: f64) {
        self.mcps[mcp].queue.push_back((agent, back_ms, self.now));
        self.mcp_start(mcp);
    }

    fn mcp_start(&mut self, mcp: usize) {
        let cfg = &self.sc.mcp_servers[mcp];
        let (cap, lat) = (cfg.concurrency, cfg.latency_ms);
        while self.mcps[mcp].active < cap {
            let Some((agent, back_ms, enq)) = self.mcps[mcp].queue.pop_front() else {
                break;
            };
            let st = &mut self.mcps[mcp];
            st.active += 1;
            st.wait_ms += self.now - enq;
            let dur = st.rng.lognormal(lat.median, lat.sigma);
            self.push(
                self.now + dur,
                Ev::McpDone {
                    agent,
                    mcp,
                    back_ms,
                },
            );
        }
        let st = &mut self.mcps[mcp];
        let (q, a) = (st.queue.len() as f64, st.active as f64);
        st.qlen.set(self.now, q);
        st.busy.set(self.now, a);
    }

    fn mcp_done(&mut self, agent: usize, mcp: usize, back_ms: f64) {
        self.mcps[mcp].active -= 1;
        self.mcps[mcp].done += 1;
        self.mcp_start(mcp);
        // The tool result is back with the agent after the return trip; it
        // then sends its turn. Reuse the wake with the tool step skipped.
        self.push(self.now + back_ms, Ev::AgentTurn { agent });
    }

    // ---- log store ----

    fn db_flush(&mut self) {
        let cfg = self.sc.db.as_ref().expect("db");
        let (rate, over) = (cfg.rows_per_s, cfg.batch_overhead_ms);
        let host = host_index(&self.sc, &cfg.host);
        let speed = self.host_speed(host);
        let db = self.db.as_mut().expect("db");
        db.flush_due = false;
        if db.writing || db.buffer <= 0.0 {
            return;
        }
        let rows = db.buffer;
        db.writing = true;
        db.busy.set(self.now, 1.0);
        let dur = (over + rows / rate * 1000.0) / speed;
        self.push(self.now + dur, Ev::DbDone { rows });
    }

    fn db_done(&mut self, rows: f64) {
        let interval = self.sc.db.as_ref().expect("db").flush_interval_ms;
        let now = self.now;
        let db = self.db.as_mut().expect("db");
        db.writing = false;
        db.buffer = (db.buffer - rows).max(0.0);
        db.written += rows;
        db.busy.set(now, 0.0);
        db.backlog.set(now, db.buffer);
        if db.buffer > 0.0 && !db.flush_due {
            db.flush_due = true;
            // A store that cannot keep up writes back to back.
            let wait = if db.buffer >= rows { 0.0 } else { interval };
            self.push(now + wait, Ev::DbFlush);
        }
    }

    // ---- sampling ----

    fn sample(&mut self) {
        let now = self.now;
        let mut nodes = Vec::new();
        if let (Some(g), Some(cfg)) = (self.gw.as_mut(), self.sc.gateway.as_ref()) {
            for (u, ucfg) in g.ups.iter_mut().zip(&cfg.upstreams) {
                nodes.push(crate::metrics::NodeSample {
                    id: format!("{}/{}", cfg.name, ucfg.name),
                    kind: "upstream",
                    cap: ucfg.max_concurrency as f64,
                    queue: u.sched.queued() as f64,
                    busy: u.sched.inflight() as f64,
                    busy_area: u.busy.take(now),
                    queue_area: u.queue.take(now),
                    done: u.done,
                    wasted_ms: 0.0,
                    held: ucfg.hold_weight.map_or(0, |h| u.sched.held(h)) as f64,
                    extra: u.shed as f64,
                });
            }
        }
        if let Some(c) = self.sc.gateway.as_ref().and_then(|g| g.cache.as_ref()) {
            // done = answers from the cache, extra = lookups.
            nodes.push(crate::metrics::NodeSample {
                id: c.name.clone(),
                kind: "cache",
                cap: 0.0,
                queue: 0.0,
                busy: 0.0,
                busy_area: 0.0,
                queue_area: 0.0,
                done: self.cache_hits,
                wasted_ms: 0.0,
                held: 0.0,
                extra: self.cache_lookups as f64,
            });
        }
        for (p, cfg) in self.proxies.iter_mut().zip(&self.sc.proxies) {
            nodes.push(crate::metrics::NodeSample {
                id: cfg.name.clone(),
                kind: "proxy",
                cap: cfg.workers as f64,
                queue: p.workers.iter().map(|w| w.queue.len()).sum::<usize>() as f64,
                busy: p.workers.iter().filter(|w| w.busy.is_some()).count() as f64,
                busy_area: p.busy.take(now),
                queue_area: p.queue.take(now),
                done: p.done,
                wasted_ms: 0.0,
                held: 0.0,
                extra: p.open as f64,
            });
        }
        for (s, cfg) in self.servers.iter_mut().zip(&self.sc.model_servers) {
            nodes.push(crate::metrics::NodeSample {
                id: cfg.name.clone(),
                kind: "model_server",
                cap: cfg.full() as f64,
                queue: s.queue.len() as f64,
                busy: s.active as f64,
                busy_area: s.busy.take(now),
                queue_area: s.qlen.take(now),
                done: s.done,
                wasted_ms: s.wasted_ms,
                held: 0.0,
                extra: s.tokens as f64,
            });
        }
        for (h, cfg) in self.hosted.iter_mut().zip(&self.sc.hosted_apis) {
            nodes.push(crate::metrics::NodeSample {
                id: cfg.name.clone(),
                kind: "hosted_api",
                cap: cfg.max_concurrency as f64,
                queue: 0.0,
                busy: h.open as f64,
                busy_area: h.busy.take(now),
                queue_area: 0.0,
                done: h.done,
                wasted_ms: 0.0,
                held: 0.0,
                extra: h.limited as f64,
            });
        }
        for (m, cfg) in self.mcps.iter_mut().zip(&self.sc.mcp_servers) {
            nodes.push(crate::metrics::NodeSample {
                id: cfg.name.clone(),
                kind: "mcp_server",
                cap: cfg.concurrency as f64,
                queue: m.queue.len() as f64,
                busy: m.active as f64,
                busy_area: m.busy.take(now),
                queue_area: m.qlen.take(now),
                done: m.done,
                wasted_ms: 0.0,
                held: 0.0,
                extra: m.wait_ms,
            });
        }
        if let (Some(d), Some(cfg)) = (self.db.as_mut(), self.sc.db.as_ref()) {
            nodes.push(crate::metrics::NodeSample {
                id: cfg.name.clone(),
                kind: "db",
                cap: 1.0,
                queue: d.buffer,
                busy: if d.writing { 1.0 } else { 0.0 },
                busy_area: d.busy.take(now),
                queue_area: d.backlog.take(now),
                done: d.written as u64,
                wasted_ms: 0.0,
                held: 0.0,
                extra: d.dropped,
            });
        }
        let hosts = self
            .hosts
            .iter()
            .zip(&self.sc.hosts)
            .map(|(h, cfg)| (cfg.name.clone(), h.demand, h.cores))
            .collect();
        let mem = self.gateway_memory_mb();
        let names: std::collections::HashMap<Node, String> = self
            .metrics
            .hop_nodes()
            .into_iter()
            .map(|n| (n, self.node_name(n).to_string()))
            .collect();
        self.metrics
            .sample(now, nodes, hosts, mem, &names, &self.sc);
    }

    pub fn backend_names(&self) -> Vec<String> {
        self.sc
            .backend_names()
            .iter()
            .map(|s| s.to_string())
            .collect()
    }
}
