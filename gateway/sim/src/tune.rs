//! Looking for gateway settings that do best on one number at a time.
//!
//! A run is scored on several numbers (errors, tokens produced, how long
//! callers wait, how much better the high-priority callers fare, how much
//! piles up at the servers). The search tries settings, scores every try on
//! all of them and keeps, for each number, the settings that did best on it.
//! The first half of the tries are drawn at random; the rest take turns
//! changing one or two settings of the best so far for one number.
//!
//! What it finds is the best of what it tried in the simulator, with the
//! simulator's assumptions; it is not a guarantee about a real system.

use serde::Serialize;

use crate::rng::Rng;
use crate::scenario::{Overflow, Routing, Scenario};
use crate::sim::{Sim, UNFINISHED};

/// One number a run is scored on.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct Metric {
    pub key: &'static str,
    pub unit: &'static str,
    pub higher_is_better: bool,
}

pub const METRICS: [Metric; 6] = [
    Metric {
        key: "error_rate",
        unit: "%",
        higher_is_better: false,
    },
    Metric {
        key: "tokens_per_s",
        unit: "tok/s",
        higher_is_better: true,
    },
    Metric {
        key: "wait_max_s",
        unit: "s",
        higher_is_better: false,
    },
    Metric {
        key: "wait_mean_s",
        unit: "s",
        higher_is_better: false,
    },
    Metric {
        key: "priority_gain",
        unit: "x",
        higher_is_better: true,
    },
    Metric {
        key: "server_backlog",
        unit: "req",
        higher_is_better: false,
    },
];

/// The numbers of one run, in the order of [`METRICS`]. `None` where a
/// number does not exist (no answers, or every caller has the same weight).
pub type Scores = [Option<f64>; 6];

/// Score a finished run.
///
/// - `error_rate`: share of requests that did not get an answer (refused,
///   failed, or the caller gave up), in percent.
/// - `tokens_per_s`: output tokens of answered requests per second of run.
/// - `wait_max_s`, `wait_mean_s`: time from sending to the first token, or
///   to learning there will be none.
/// - `priority_gain`: mean wait of the callers with the lowest weight
///   divided by that of the callers with the highest.
/// - `server_backlog`: requests waiting at model servers beyond their
///   slots, averaged over the run and summed over servers.
pub fn scores(sim: &Sim) -> Scores {
    let (mut sent, mut ok, mut tokens) = (0u64, 0u64, 0u64);
    let (mut wait_sum, mut wait_max) = (0.0f64, 0.0f64);
    let weights: Vec<u32> = sim.sc.clients.iter().map(|c| c.weight).collect();
    let (lo, hi) = (
        weights.iter().copied().min().unwrap_or(1),
        weights.iter().copied().max().unwrap_or(1),
    );
    let (mut lo_sum, mut lo_n, mut hi_sum, mut hi_n) = (0.0, 0u64, 0.0, 0u64);
    for r in &sim.reqs {
        if r.status == 0 || r.status == UNFINISHED {
            continue;
        }
        sent += 1;
        let answered = r.status == 200 && r.first_token_at.is_some();
        if answered {
            ok += 1;
            tokens += u64::from(r.out_tokens);
        }
        let until = match r.first_token_at {
            Some(t) if answered => t,
            _ => r.done_at.unwrap_or(sim.now),
        };
        let wait = (until - r.sent_at).max(0.0) / 1000.0;
        wait_sum += wait;
        wait_max = wait_max.max(wait);
        let w = weights.get(r.client).copied().unwrap_or(1);
        if w == lo {
            lo_sum += wait;
            lo_n += 1;
        }
        if w == hi {
            hi_sum += wait;
            hi_n += 1;
        }
    }
    let some = sent > 0;
    let secs = (sim.now / 1000.0).max(1e-9);
    let backlog: f64 = sim
        .metrics
        .whole_run()
        .iter()
        .filter(|n| n.kind == "model_server")
        .map(|n| n.mean_queue)
        .sum();
    [
        some.then(|| 100.0 * (sent - ok) as f64 / sent as f64),
        Some(tokens as f64 / secs),
        some.then_some(wait_max),
        some.then(|| wait_sum / sent as f64),
        (lo != hi && lo_n > 0 && hi_n > 0 && hi_sum > 0.0)
            .then(|| (lo_sum / lo_n as f64) / (hi_sum / hi_n as f64)),
        Some(backlog),
    ]
}

/// Settings the search may change. They apply to every gateway upstream
/// (and `proxy_routing` to every proxy) alike.
#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
pub struct Knobs {
    /// Share of the servers' slots the gateway hands out at once.
    pub headroom: f64,
    pub gateway_routing: Routing,
    pub proxy_routing: Routing,
    pub strict_priority: bool,
    /// Gateway queue length as a multiple of the concurrency cap.
    pub queue_factor: f64,
    /// Longest a request waits in the gateway queue (0 = no limit).
    pub queue_timeout_s: f64,
    /// Refuse at once when the expected wait is over the limit.
    pub admit_by_wait: bool,
    pub evict: bool,
}

const HEADROOM: [f64; 7] = [0.5, 0.7, 0.85, 1.0, 1.25, 1.5, 2.0];
const ROUTING: [Routing; 5] = [
    Routing::Affinity,
    Routing::LeastLoaded,
    Routing::LeastInflight,
    Routing::RoundRobin,
    Routing::Random,
];
const QUEUE_FACTOR: [f64; 5] = [0.25, 0.5, 1.0, 2.0, 4.0];
const QUEUE_TIMEOUT: [f64; 5] = [0.0, 2.0, 5.0, 15.0, 60.0];

impl Knobs {
    fn random(rng: &mut Rng) -> Self {
        Knobs {
            headroom: HEADROOM[rng.below(HEADROOM.len())],
            gateway_routing: ROUTING[rng.below(ROUTING.len())],
            proxy_routing: ROUTING[rng.below(ROUTING.len())],
            strict_priority: rng.below(2) == 1,
            queue_factor: QUEUE_FACTOR[rng.below(QUEUE_FACTOR.len())],
            queue_timeout_s: QUEUE_TIMEOUT[rng.below(QUEUE_TIMEOUT.len())],
            admit_by_wait: rng.below(2) == 1,
            evict: rng.below(2) == 1,
        }
    }

    /// The same settings with one of them changed.
    fn nudged(mut self, rng: &mut Rng) -> Self {
        match rng.below(8) {
            0 => self.headroom = HEADROOM[rng.below(HEADROOM.len())],
            1 => self.gateway_routing = ROUTING[rng.below(ROUTING.len())],
            2 => self.proxy_routing = ROUTING[rng.below(ROUTING.len())],
            3 => self.strict_priority = !self.strict_priority,
            4 => self.queue_factor = QUEUE_FACTOR[rng.below(QUEUE_FACTOR.len())],
            5 => self.queue_timeout_s = QUEUE_TIMEOUT[rng.below(QUEUE_TIMEOUT.len())],
            6 => self.admit_by_wait = !self.admit_by_wait,
            _ => self.evict = !self.evict,
        }
        self
    }

    /// Write the settings into a scenario.
    pub fn apply(&self, sc: &mut Scenario) {
        for p in &mut sc.proxies {
            p.routing = self.proxy_routing;
        }
        if let Some(g) = &mut sc.gateway {
            for u in &mut g.upstreams {
                u.headroom = Some(self.headroom);
                u.routing = self.gateway_routing;
                u.strict_priority = self.strict_priority;
                u.queue_timeout_s = (self.queue_timeout_s > 0.0).then_some(self.queue_timeout_s);
                u.admit_by_wait = self.admit_by_wait;
                u.overflow = if self.evict {
                    Overflow::Evict
                } else {
                    Overflow::Refuse
                };
            }
        }
        sc.resolve_caps();
        if let Some(g) = &mut sc.gateway {
            for u in &mut g.upstreams {
                let q = (u.max_concurrency as f64 * self.queue_factor).round() as usize;
                u.max_queue = Some(q.max(1));
            }
        }
    }
}

/// One try: the settings (`None` = the scenario as given) and its scores.
#[derive(Clone, Debug, Serialize)]
pub struct Try {
    pub knobs: Option<Knobs>,
    pub scores: Scores,
}

#[derive(Clone, Debug, Serialize)]
pub struct Preset {
    pub metric: &'static str,
    /// Index into the tries; 0 is the scenario as given.
    pub pick: usize,
    pub knobs: Option<Knobs>,
    pub scores: Scores,
}

#[derive(Clone, Debug, Serialize)]
pub struct State {
    pub done: usize,
    pub total: usize,
    pub seeds: usize,
    pub metrics: Vec<Metric>,
    pub baseline: Option<Scores>,
    pub best: Vec<Preset>,
}

pub struct Tuner {
    base: Scenario,
    seeds: usize,
    total: usize,
    rng: Rng,
    tries: Vec<Try>,
    best: [usize; 6],
}

fn better(m: usize, a: &Scores, b: &Scores) -> bool {
    match (a[m], b[m]) {
        (Some(x), Some(y)) if (x - y).abs() > 1e-9 * (x.abs() + y.abs() + 1.0) => {
            if METRICS[m].higher_is_better {
                x > y
            } else {
                x < y
            }
        }
        // Equal on this number: fewer errors, then shorter waits.
        (Some(_), Some(_)) => match (a[0], b[0], a[3], b[3]) {
            (Some(x), Some(y), _, _) if (x - y).abs() > 1e-9 => x < y,
            (_, _, Some(x), Some(y)) => x < y - 1e-9,
            _ => false,
        },
        (Some(_), None) => true,
        _ => false,
    }
}

impl Tuner {
    /// `sc` must be ready to run. `tries` counts the scenario as given too.
    pub fn new(sc: Scenario, tries: usize, seeds: usize) -> Self {
        let rng = Rng::new(sc.seed ^ 0x7E57_5EED);
        Tuner {
            base: sc,
            seeds: seeds.max(1),
            total: tries.max(2),
            rng,
            tries: Vec::new(),
            best: [0; 6],
        }
    }

    pub fn finished(&self) -> bool {
        self.tries.len() >= self.total
    }

    /// The scenario with the settings of a try written into it.
    pub fn scenario(&self, pick: usize) -> Option<Scenario> {
        let t = self.tries.get(pick)?;
        let mut sc = self.base.clone();
        if let Some(k) = &t.knobs {
            k.apply(&mut sc);
        }
        Some(sc)
    }

    fn eval(&self, knobs: Option<Knobs>) -> Result<Scores, String> {
        let mut sum = [0.0; 6];
        let mut n = [0u32; 6];
        for i in 0..self.seeds {
            let mut sc = self.base.clone();
            sc.seed = self.base.seed.wrapping_add(i as u64 * 7919);
            if let Some(k) = &knobs {
                k.apply(&mut sc);
            }
            let mut sim = Sim::new(sc)?;
            sim.run();
            for (m, v) in scores(&sim).into_iter().enumerate() {
                if let Some(v) = v {
                    sum[m] += v;
                    n[m] += 1;
                }
            }
        }
        let mut out = [None; 6];
        for m in 0..6 {
            if n[m] > 0 {
                out[m] = Some(sum[m] / f64::from(n[m]));
            }
        }
        Ok(out)
    }

    /// Run one more try. Returns whether the search is over.
    pub fn step(&mut self) -> Result<bool, String> {
        if self.finished() {
            return Ok(true);
        }
        let i = self.tries.len();
        let explore = 1 + (self.total - 1) / 2;
        let knobs = if i == 0 {
            None
        } else {
            let mut pick = Knobs::random(&mut self.rng);
            for _ in 0..40 {
                pick = if i < explore {
                    Knobs::random(&mut self.rng)
                } else {
                    // Improve on the best for one number, taking turns.
                    let m = (i - explore) % METRICS.len();
                    let from = self.tries[self.best[m]]
                        .knobs
                        .unwrap_or_else(|| Knobs::random(&mut self.rng));
                    let mut k = from.nudged(&mut self.rng);
                    if self.rng.below(3) == 0 {
                        k = k.nudged(&mut self.rng);
                    }
                    k
                };
                if !self.tries.iter().any(|t| t.knobs == Some(pick)) {
                    break;
                }
            }
            Some(pick)
        };
        let scores = self.eval(knobs)?;
        self.tries.push(Try { knobs, scores });
        for m in 0..METRICS.len() {
            if better(m, &scores, &self.tries[self.best[m]].scores) {
                self.best[m] = i;
            }
        }
        Ok(self.finished())
    }

    pub fn state(&self) -> State {
        State {
            done: self.tries.len(),
            total: self.total,
            seeds: self.seeds,
            metrics: METRICS.to_vec(),
            baseline: self.tries.first().map(|t| t.scores),
            best: if self.tries.is_empty() {
                Vec::new()
            } else {
                METRICS
                    .iter()
                    .enumerate()
                    .map(|(m, d)| {
                        let t = &self.tries[self.best[m]];
                        Preset {
                            metric: d.key,
                            pick: self.best[m],
                            knobs: t.knobs,
                            scores: t.scores,
                        }
                    })
                    .collect()
            },
        }
    }
}

/// Run a whole search.
pub fn tune(sc: Scenario, tries: usize, seeds: usize) -> Result<Tuner, String> {
    let mut t = Tuner::new(sc, tries, seeds);
    while !t.step()? {}
    Ok(t)
}

/// How much of the model servers' capacity a run left unused, by group of
/// like-named servers (`gpu-small-3` belongs to `gpu-small`).
#[derive(Clone, Debug, Default, Serialize)]
pub struct IdlePool {
    pub pool: String,
    pub models: Vec<String>,
    pub servers: usize,
    pub slots: usize,
    /// Share of slot time nothing ran in.
    pub idle_share: f64,
    /// Slots times seconds nothing ran in.
    pub idle_slot_s: f64,
    /// Servers that never ran anything.
    pub never_used: usize,
    pub tokens_made: u64,
    /// Output tokens the group makes with every slot busy for the whole
    /// run (tokens_per_s * slots^batch_scaling per server).
    pub tokens_at_full: f64,
    /// `tokens_at_full - tokens_made`: an upper bound on what the idle time
    /// could still produce, since real work does not arrive evenly.
    pub tokens_spare: f64,
}

pub fn idle(sim: &Sim) -> Vec<IdlePool> {
    let secs = sim.now / 1000.0;
    let made = sim.server_tokens();
    let load = sim.metrics.whole_run();
    let mut out: Vec<IdlePool> = Vec::new();
    for (i, m) in sim.sc.model_servers.iter().enumerate() {
        let pool = match m.name.rsplit_once('-') {
            Some((head, n)) if !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()) => head,
            _ => m.name.as_str(),
        };
        let at = match out.iter().position(|p| p.pool == pool) {
            Some(at) => at,
            None => {
                out.push(IdlePool {
                    pool: pool.to_string(),
                    models: m.models.clone(),
                    ..IdlePool::default()
                });
                out.len() - 1
            }
        };
        let util = load
            .iter()
            .find(|n| n.kind == "model_server" && n.id == m.name)
            .map_or(0.0, |n| n.util.clamp(0.0, 1.0));
        let p = &mut out[at];
        p.servers += 1;
        p.slots += m.full();
        p.idle_slot_s += (1.0 - util) * m.full() as f64 * secs;
        p.never_used += usize::from(made.get(i).copied().unwrap_or(0) == 0);
        p.tokens_made += made.get(i).copied().unwrap_or(0);
        p.tokens_at_full += m.tokens_per_s * (m.full() as f64).powf(m.batch_scaling) * secs;
    }
    for p in &mut out {
        p.idle_share = if p.slots > 0 && secs > 0.0 {
            p.idle_slot_s / (p.slots as f64 * secs)
        } else {
            0.0
        };
        p.tokens_spare = (p.tokens_at_full - p.tokens_made as f64).max(0.0);
    }
    out
}
