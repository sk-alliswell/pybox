//! What a run records while it goes: per-component load over time, per-class
//! outcomes, and a running diagnosis of where the bottleneck is.

use std::collections::VecDeque;

use serde::Serialize;

use crate::scenario::Scenario;
use crate::sim::{Node, GAVE_UP};

/// A piecewise-constant value whose time integral is tracked.
#[derive(Clone, Debug, Default)]
pub struct Gauge {
    v: f64,
    t: f64,
    area: f64,
    pub max: f64,
}

impl Gauge {
    pub fn set(&mut self, now: f64, v: f64) {
        self.area += self.v * (now - self.t);
        self.t = now;
        self.v = v;
        self.max = self.max.max(v);
    }

    /// Integral since the last call.
    pub fn take(&mut self, now: f64) -> f64 {
        self.area += self.v * (now - self.t);
        self.t = now;
        std::mem::take(&mut self.area)
    }
}

/// State of one component at a sample, handed over by the simulator.
pub struct NodeSample {
    pub id: String,
    pub kind: &'static str,
    pub cap: f64,
    pub queue: f64,
    pub busy: f64,
    pub busy_area: f64,
    pub queue_area: f64,
    pub done: u64,
    pub wasted_ms: f64,
    /// Of `queue`: requests that are held to be sent on later.
    pub held: f64,
    /// Kind-specific counter: refusals (upstream), open requests (proxy),
    /// tokens (model server), 429s (hosted API), dropped rows (db).
    pub extra: f64,
}

#[derive(Clone, Debug, Serialize)]
pub struct NodeFrame {
    pub id: String,
    pub kind: &'static str,
    pub cap: f64,
    /// Waiting right now.
    pub queue: f64,
    /// Running right now.
    pub busy: f64,
    /// Share of capacity in use over the last interval (0 to 1).
    pub util: f64,
    pub mean_queue: f64,
    /// Completions per second over the last interval.
    pub rate: f64,
    /// Share of the interval spent on work nobody was waiting for.
    pub wasted: f64,
    /// Of `queue`: requests held to be sent on later (gateway upstreams).
    pub held: f64,
    /// Change of the kind-specific counter over the interval.
    pub extra: f64,
}

#[derive(Clone, Debug, Serialize)]
pub struct LinkFrame {
    pub from: String,
    pub to: String,
    /// Requests per second sent over this hop in the last interval.
    pub rate: f64,
    /// Output tokens per second that came back over it.
    pub tokens: f64,
}

/// A caller is served on time when the first token comes within this many
/// seconds. One threshold for every kind of request: a way to compare runs,
/// not a promise about any one of them.
pub const ON_TIME_S: f64 = 10.0;

#[derive(Clone, Debug, Default, Serialize)]
pub struct ClassFrame {
    pub name: String,
    pub sent: u64,
    pub ok: u64,
    /// Answers whose first token came within [`ON_TIME_S`].
    pub on_time: u64,
    pub gave_up: u64,
    pub refused: u64,
    pub errors: u64,
    pub open: u64,
    /// Over answers of the last ten seconds (s); null when there were none.
    pub ttft_p50: Option<f64>,
    pub ttft_p95: Option<f64>,
    /// Output tokens a second one caller saw while its answer was written:
    /// the median over answers of the last ten seconds.
    pub gen_tps_p50: Option<f64>,
    /// Answers per second over the last interval.
    pub rate_ok: f64,
    pub tokens_per_s: f64,
    /// Output tokens of the answers that were finished, since the start:
    /// what callers got to use. `tokens_per_s` also counts what was written
    /// for answers the caller did not wait out.
    pub tokens_ok: u64,
    /// Requests with a repeating prompt prefix that reached a model server,
    /// and how many found the prefix in its cache (since the start).
    pub prefix_lookups: u64,
    pub prefix_hits: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct HostFrame {
    pub name: String,
    pub demand: f64,
    pub cores: f64,
}

/// One thing the diagnosis noticed.
#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct Finding {
    /// `saturated`, `queue_behind`, `concurrency_cap`, `dispatch_paused`,
    /// `unmanaged_queue`,
    /// `wasted_work`, `proxy_cpu`, `rate_limited`, `tool_server`,
    /// `log_store`, `host_cpu`, `idle_capacity`.
    pub code: &'static str,
    pub node: String,
    /// The component that causes it, when that is a different one.
    pub cause: Option<String>,
    /// The number behind it: utilisation, queue length, share.
    pub value: f64,
    /// True for the finding that limits throughput or latency most.
    pub primary: bool,
    /// Share of the run during which this held (1 in a live frame).
    pub share: f64,
}

#[derive(Clone, Debug, Serialize)]
pub struct Frame {
    /// Seconds since the start.
    pub t: f64,
    pub nodes: Vec<NodeFrame>,
    pub links: Vec<LinkFrame>,
    pub classes: Vec<ClassFrame>,
    pub hosts: Vec<HostFrame>,
    pub gateway_mem_mb: f64,
    pub findings: Vec<Finding>,
}

#[derive(Default)]
struct ClassSt {
    name: String,
    sent: u64,
    ok: u64,
    on_time: u64,
    gave_up: u64,
    refused: u64,
    errors: u64,
    recent: VecDeque<(f64, f64)>,
    recent_gen: VecDeque<(f64, f64)>,
    ok_mark: u64,
    /// Output tokens that reached the caller, counted as they arrive.
    tokens: u64,
    tokens_mark: u64,
    tokens_ok: u64,
    prefix_lookups: u64,
    prefix_hits: u64,
}

/// Aggregate load of one component, over an interval or the whole run.
#[derive(Clone, Debug, Default, Serialize)]
pub struct NodeLoad {
    pub id: String,
    pub kind: &'static str,
    pub cap: f64,
    pub util: f64,
    pub mean_queue: f64,
    pub max_queue: f64,
    pub done: u64,
    pub wasted: f64,
    pub extra: f64,
}

struct Hop {
    from: Node,
    to: Node,
    sent: u64,
    sent0: u64,
    tokens: u64,
    tokens0: u64,
}

impl Hop {
    fn default_at(from: Node, to: Node) -> Hop {
        Hop {
            from,
            to,
            sent: 0,
            sent0: 0,
            tokens: 0,
            tokens0: 0,
        }
    }
}

pub struct Metrics {
    class_of: Vec<usize>,
    classes: Vec<ClassSt>,
    hops: Vec<Hop>,
    hop_ix: std::collections::HashMap<(Node, Node), u32>,
    prev: Vec<(u64, f64, f64)>,
    smooth: Vec<NodeLoad>,
    totals: Vec<(f64, f64, f64)>,
    last_t: f64,
    sampled: bool,
    /// Per finding seen: time it held, value x time, time it was primary.
    seen: Vec<(Finding, f64, f64, f64)>,
    pub frames: Vec<Frame>,
    pub gateway_mem_peak_mb: f64,
    /// Keep every frame (the monitor and reports want them) or only the last.
    pub keep_frames: bool,
}

impl Metrics {
    pub fn new(sc: &Scenario, _servers: usize) -> Self {
        let mut classes: Vec<ClassSt> = Vec::new();
        let class_of = sc
            .clients
            .iter()
            .map(|c| {
                let name = c.class.clone().unwrap_or_else(|| c.name.clone());
                match classes.iter().position(|x| x.name == name) {
                    Some(i) => i,
                    None => {
                        classes.push(ClassSt {
                            name,
                            ..ClassSt::default()
                        });
                        classes.len() - 1
                    }
                }
            })
            .collect();
        Self {
            class_of,
            classes,
            hops: Vec::new(),
            hop_ix: std::collections::HashMap::new(),
            prev: Vec::new(),
            smooth: Vec::new(),
            totals: Vec::new(),
            last_t: 0.0,
            sampled: false,
            seen: Vec::new(),
            frames: Vec::new(),
            gateway_mem_peak_mb: 0.0,
            keep_frames: true,
        }
    }

    pub fn class_name(&self, client: usize) -> &str {
        &self.classes[self.class_of[client]].name
    }

    pub fn sent(&mut self, client: usize) {
        self.classes[self.class_of[client]].sent += 1;
    }

    /// Count a request sent from one component to the next; returns the
    /// hop's index for [`Metrics::token_back`].
    pub fn hop(&mut self, from: Node, to: Node) -> u32 {
        let hops = &mut self.hops;
        let i = *self.hop_ix.entry((from, to)).or_insert_with(|| {
            hops.push(Hop::default_at(from, to));
            hops.len() as u32 - 1
        });
        self.hops[i as usize].sent += 1;
        i
    }

    /// One output token travelled back over these hops to the caller.
    pub fn token_back(&mut self, hops: &[u32], n: u32) {
        for h in hops {
            self.hops[*h as usize].tokens += u64::from(n);
        }
    }

    /// Output tokens reached a caller of this client's class. Counted as
    /// they arrive, not when the answer ends: a long answer booked whole
    /// at its end would show as a spike in tokens a second.
    pub fn token_out(&mut self, client: usize, n: u32) {
        self.classes[self.class_of[client]].tokens += u64::from(n);
    }

    /// Every component a hop starts or ends at.
    pub fn hop_nodes(&self) -> std::collections::HashSet<Node> {
        self.hops.iter().flat_map(|h| [h.from, h.to]).collect()
    }

    pub fn prefix_lookup(&mut self, client: usize, hit: bool) {
        let c = &mut self.classes[self.class_of[client]];
        c.prefix_lookups += 1;
        c.prefix_hits += u64::from(hit);
    }

    pub fn finished(
        &mut self,
        client: usize,
        status: i32,
        ttft_ms: Option<f64>,
        gen_tps: Option<f64>,
        out_tokens: u32,
        now: f64,
    ) {
        let c = &mut self.classes[self.class_of[client]];
        match status {
            200 => {
                c.ok += 1;
                c.tokens_ok += u64::from(out_tokens);
                if let Some(g) = gen_tps {
                    c.recent_gen.push_back((now, g));
                }
                if let Some(t) = ttft_ms {
                    if t <= ON_TIME_S * 1000.0 {
                        c.on_time += 1;
                    }
                    c.recent.push_back((now, t / 1000.0));
                }
            }
            GAVE_UP => c.gave_up += 1,
            503 | 429 => c.refused += 1,
            _ => c.errors += 1,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn sample(
        &mut self,
        now: f64,
        nodes: Vec<NodeSample>,
        hosts: Vec<(String, f64, f64)>,
        mem: f64,
        names: &std::collections::HashMap<Node, String>,
        sc: &Scenario,
    ) {
        let dt = now - self.last_t;
        if dt <= 0.0 && self.sampled {
            return;
        }
        self.sampled = true;
        let dt = dt.max(1e-9);
        self.last_t = now;
        self.gateway_mem_peak_mb = self.gateway_mem_peak_mb.max(mem);
        self.prev.resize(nodes.len(), (0, 0.0, 0.0));
        self.totals.resize(nodes.len(), (0.0, 0.0, 0.0));
        if self.smooth.len() != nodes.len() {
            self.smooth = nodes
                .iter()
                .map(|n| NodeLoad {
                    id: n.id.clone(),
                    kind: n.kind,
                    cap: n.cap,
                    ..NodeLoad::default()
                })
                .collect();
        }
        // Smoothing over roughly the last ten seconds.
        let alpha = (dt / 10_000.0).min(1.0);
        let mut frames = Vec::with_capacity(nodes.len());
        for (i, n) in nodes.into_iter().enumerate() {
            let (done0, wasted0, extra0) = self.prev[i];
            self.prev[i] = (n.done, n.wasted_ms, n.extra);
            // Components without a limit report load as a count, not a share.
            let cap = if n.cap > 0.0 { n.cap } else { 1.0 };
            // A server that takes more requests than it runs at full speed
            // is full, not more than full.
            let util = (n.busy_area / dt / cap).min(1.0);
            let mean_queue = n.queue_area / dt;
            // Work is booked when it ends, so one interval can hold more than its length.
            let wasted = ((n.wasted_ms - wasted0) / dt / cap).min(1.0);
            let extra = n.extra - extra0;
            self.totals[i].0 += n.busy_area;
            self.totals[i].1 += n.queue_area;
            self.totals[i].2 = self.totals[i].2.max(n.queue);
            let s = &mut self.smooth[i];
            s.util += alpha * (util - s.util);
            s.mean_queue += alpha * (mean_queue - s.mean_queue);
            s.wasted += alpha * (wasted - s.wasted);
            s.extra += alpha * (extra / dt * 1000.0 - s.extra);
            s.max_queue = n.queue;
            frames.push(NodeFrame {
                id: n.id,
                kind: n.kind,
                cap: n.cap,
                queue: n.queue,
                busy: n.busy,
                util,
                mean_queue,
                rate: (n.done - done0) as f64 / dt * 1000.0,
                wasted,
                held: n.held,
                extra,
            });
        }
        let name = |n: Node| -> String { names.get(&n).cloned().unwrap_or_default() };
        let links = self
            .hops
            .iter_mut()
            .map(|h| {
                let rate = (h.sent - h.sent0) as f64 / dt * 1000.0;
                let tokens = (h.tokens - h.tokens0) as f64 / dt * 1000.0;
                h.sent0 = h.sent;
                h.tokens0 = h.tokens;
                LinkFrame {
                    from: name(h.from),
                    to: name(h.to),
                    rate,
                    tokens,
                }
            })
            .collect();
        let classes = self
            .classes
            .iter_mut()
            .map(|c| {
                while c.recent.front().is_some_and(|x| x.0 < now - 10_000.0) {
                    c.recent.pop_front();
                }
                while c.recent_gen.front().is_some_and(|x| x.0 < now - 10_000.0) {
                    c.recent_gen.pop_front();
                }
                let mut v: Vec<f64> = c.recent.iter().map(|x| x.1).collect();
                v.sort_by(f64::total_cmp);
                let mut g: Vec<f64> = c.recent_gen.iter().map(|x| x.1).collect();
                g.sort_by(f64::total_cmp);
                let f = ClassFrame {
                    name: c.name.clone(),
                    sent: c.sent,
                    ok: c.ok,
                    on_time: c.on_time,
                    gave_up: c.gave_up,
                    refused: c.refused,
                    errors: c.errors,
                    open: c.sent - c.ok - c.gave_up - c.refused - c.errors,
                    ttft_p50: pct(&v, 50.0),
                    ttft_p95: pct(&v, 95.0),
                    gen_tps_p50: pct(&g, 50.0),
                    rate_ok: (c.ok - c.ok_mark) as f64 / dt * 1000.0,
                    tokens_per_s: (c.tokens - c.tokens_mark) as f64 / dt * 1000.0,
                    tokens_ok: c.tokens_ok,
                    prefix_lookups: c.prefix_lookups,
                    prefix_hits: c.prefix_hits,
                };
                c.ok_mark = c.ok;
                c.tokens_mark = c.tokens;
                f
            })
            .collect();
        let host_frames: Vec<HostFrame> = hosts
            .into_iter()
            .map(|(name, demand, cores)| HostFrame {
                name,
                demand,
                cores,
            })
            .collect();
        let findings = diagnose(&self.smooth, &host_frames, sc);
        for f in &findings {
            let i = match self
                .seen
                .iter()
                .position(|s| s.0.code == f.code && s.0.node == f.node && s.0.cause == f.cause)
            {
                Some(i) => i,
                None => {
                    self.seen.push((f.clone(), 0.0, 0.0, 0.0));
                    self.seen.len() - 1
                }
            };
            let s = &mut self.seen[i];
            s.1 += dt;
            s.2 += f.value * dt;
            if f.primary {
                s.3 += dt;
            }
        }
        let frame = Frame {
            t: now / 1000.0,
            nodes: frames,
            links,
            classes,
            hosts: host_frames,
            gateway_mem_mb: mem,
            findings,
        };
        if !self.keep_frames {
            self.frames.clear();
        }
        self.frames.push(frame);
    }

    /// What the running diagnosis found over the whole run: everything that
    /// held for at least a tenth of it, longest first. The primary finding
    /// is the one that was primary for the longest.
    pub fn run_findings(&self) -> Vec<Finding> {
        let t = self.last_t.max(1e-9);
        // The most direct limit; among equals, the one that held longest.
        let lead = self
            .seen
            .iter()
            .filter(|s| rank(&s.0) < 9 && s.1 / t >= 0.1)
            .min_by(|a, b| rank(&a.0).cmp(&rank(&b.0)).then(b.1.total_cmp(&a.1)))
            .map(|s| (s.0.code, s.0.node.clone()));
        let mut out: Vec<Finding> = self
            .seen
            .iter()
            .filter(|s| s.1 / t >= 0.1)
            .map(|s| Finding {
                value: s.2 / s.1,
                share: s.1 / t,
                primary: lead
                    .as_ref()
                    .is_some_and(|l| l.0 == s.0.code && l.1 == s.0.node),
                ..s.0.clone()
            })
            .collect();
        out.sort_by(|a, b| b.primary.cmp(&a.primary).then(b.share.total_cmp(&a.share)));
        out
    }

    /// Load of every component over the whole run.
    pub fn whole_run(&self) -> Vec<NodeLoad> {
        let t = self.last_t.max(1e-9);
        self.smooth
            .iter()
            .enumerate()
            .map(|(i, s)| {
                let cap = if s.cap > 0.0 { s.cap } else { 1.0 };
                let (done, wasted, extra) = self.prev[i];
                NodeLoad {
                    id: s.id.clone(),
                    kind: s.kind,
                    cap: s.cap,
                    util: self.totals[i].0 / t / cap,
                    mean_queue: self.totals[i].1 / t,
                    max_queue: self.totals[i].2,
                    done,
                    wasted: wasted / t / cap,
                    extra,
                }
            })
            .collect()
    }
}

/// Percentile with linear interpolation over a sorted slice.
pub fn pct(sorted: &[f64], p: f64) -> Option<f64> {
    if sorted.is_empty() {
        return None;
    }
    let pos = p / 100.0 * (sorted.len() - 1) as f64;
    let lo = pos.floor() as usize;
    let hi = (lo + 1).min(sorted.len() - 1);
    Some(sorted[lo] + (sorted[hi] - sorted[lo]) * (pos - lo as f64))
}

/// Backends a gateway upstream ends at, through proxies.
pub(crate) fn backends_behind(sc: &Scenario, upstream: &crate::scenario::Upstream) -> Vec<String> {
    let mut out = Vec::new();
    let serves =
        |b: &str| upstream.models.is_empty() || upstream.models.iter().any(|m| sc.reaches(b, m));
    for t in &upstream.targets {
        match sc.proxies.iter().find(|p| &p.name == t) {
            Some(p) => {
                for b in sc.backend_names() {
                    if (p.backends.is_empty() || p.backends.iter().any(|x| x == b)) && serves(b) {
                        out.push(b.to_string());
                    }
                }
            }
            None => out.push(t.clone()),
        }
    }
    out.sort();
    out.dedup();
    out
}

/// How directly a finding limits the system: lower is more direct, 9 is a
/// symptom or a side effect.
fn rank(f: &Finding) -> u8 {
    match f.code {
        "saturated" => 0,
        "rate_limited" => 1,
        "proxy_cpu" => 2,
        "concurrency_cap" | "dispatch_paused" => 3,
        "tool_server" => 4,
        "log_store" => 5,
        _ => 9,
    }
}

/// Read the load of every component and say where the limit is.
///
/// The rule of thumb it encodes: a queue is a symptom, and the cause is the
/// busiest thing downstream of it. Requests wait in the gateway because a
/// model server is full, not because the gateway is slow.
pub fn diagnose(load: &[NodeLoad], hosts: &[HostFrame], sc: &Scenario) -> Vec<Finding> {
    const FULL: f64 = 0.9;
    let mut out: Vec<Finding> = Vec::new();
    let mut add = |code, node: &str, cause: Option<String>, value| {
        out.push(Finding {
            code,
            node: node.to_string(),
            cause,
            value,
            primary: false,
            share: 1.0,
        });
    };
    let by_id: std::collections::HashMap<&str, &NodeLoad> =
        load.iter().map(|n| (n.id.as_str(), n)).collect();
    let get = |id: &str| by_id.get(id).copied();
    for n in load {
        match n.kind {
            "model_server" => {
                if n.util >= FULL {
                    add("saturated", &n.id, None, n.util);
                }
                if n.wasted >= 0.1 {
                    add("wasted_work", &n.id, None, n.wasted);
                }
            }
            "proxy" if n.util >= 0.8 => add("proxy_cpu", &n.id, None, n.util),
            "hosted_api" if n.extra > 0.01 => add("rate_limited", &n.id, None, n.extra),
            "mcp_server" if n.util >= FULL || n.mean_queue >= 1.0 => {
                add("tool_server", &n.id, None, n.util);
            }
            "db" if n.util >= FULL || n.extra > 0.0 => add("log_store", &n.id, None, n.util),
            _ => {}
        }
    }
    let gateway = sc.gateway.as_ref();
    let mut managed: Vec<String> = Vec::new();
    if let Some(g) = gateway {
        for u in &g.upstreams {
            let id = format!("{}/{}", g.name, u.name);
            let behind = backends_behind(sc, u);
            managed.extend(behind.iter().cloned());
            let Some(n) = get(&id) else { continue };
            if n.mean_queue < 1.0 && n.util < 0.95 {
                continue;
            }
            // What this queue waits for: a full model server behind it, a
            // proxy out of CPU on the way, or a provider's rate limit.
            let full = behind
                .iter()
                .filter_map(|b| get(b))
                .filter(|b| b.kind == "model_server" && b.util >= FULL)
                .max_by(|a, b| a.util.total_cmp(&b.util));
            let proxy = u
                .targets
                .iter()
                .filter_map(|t| get(t))
                .find(|p| p.kind == "proxy" && p.util >= 0.8);
            let limited = behind
                .iter()
                .filter_map(|b| get(b))
                .find(|b| b.kind == "hosted_api" && b.extra > 0.01);
            match full.or(proxy).or(limited) {
                Some(b) => add("queue_behind", &id, Some(b.id.clone()), n.mean_queue),
                None if n.mean_queue >= 1.0 && n.util >= FULL => {
                    add("concurrency_cap", &id, None, n.mean_queue);
                }
                // Waiting with slots free: dispatch is paused by a 429
                // cooldown or by the upstream's own pacing.
                None if n.mean_queue >= 1.0 => add("dispatch_paused", &id, None, n.mean_queue),
                None => {}
            }
            // Capacity that could have taken this queue but is not wired to it.
            if n.mean_queue >= 1.0 {
                for s in load
                    .iter()
                    .filter(|s| s.kind == "model_server" && s.util < 0.2 && !behind.contains(&s.id))
                {
                    add("idle_capacity", &s.id, Some(id.clone()), 1.0 - s.util);
                }
            }
        }
    }
    for n in load.iter().filter(|n| n.kind == "model_server") {
        // Work queued inside a server cannot be reordered, refused or dropped
        // when its caller leaves.
        let deep = if managed.contains(&n.id) { 4.0 } else { 1.0 };
        if n.mean_queue >= deep {
            add("unmanaged_queue", &n.id, None, n.mean_queue);
        }
    }
    for h in hosts {
        if h.demand > h.cores * 1.05 {
            add("host_cpu", &h.name, None, h.demand / h.cores);
        }
    }
    // The one that matters most: a saturated resource first, then the rest.
    if let Some(best) = out
        .iter()
        .enumerate()
        .filter(|(_, f)| rank(f) < 9)
        .min_by(|a, b| {
            rank(a.1)
                .cmp(&rank(b.1))
                .then(b.1.value.total_cmp(&a.1.value))
        })
        .map(|(i, _)| i)
    {
        out[best].primary = true;
    }
    out
}

/// Frames as the monitor takes them: every frame keeps its time, the
/// per-class numbers and one queue length and one utilisation per node (in
/// the order of `Frame::nodes`); the whole frame rides along as `full` only
/// where `full` says so. A long run of a large system would otherwise be
/// tens of megabytes of node rows nobody looks at.
pub fn compact(frames: &[Frame], full: impl Fn(usize) -> bool) -> serde_json::Value {
    let r = |v: f64, k: f64| (v * k).round() / k;
    frames
        .iter()
        .enumerate()
        .map(|(i, f)| {
            let mut v = serde_json::json!({
                "t": f.t,
                "classes": f.classes,
                "q": f.nodes.iter().map(|n| r(n.queue, 10.0)).collect::<Vec<_>>(),
                "u": f.nodes.iter().map(|n| r(n.util.min(1.0), 1000.0)).collect::<Vec<_>>(),
            });
            if full(i) {
                v["full"] = serde_json::to_value(f).expect("plain data");
            }
            v
        })
        .collect()
}
