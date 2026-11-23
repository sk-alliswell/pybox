//! A discrete-event simulator for the scheduler.
//!
//! It replays agent runs (several turns each, with think or tool time in
//! between) against model servers whose total throughput saturates at a knee,
//! and a provider prompt cache that follows the published rules. The policy
//! code under test is the same `FairQueue`, `Adaptive` and `choose` the live
//! dispatcher uses, so results here describe the shipped logic.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, VecDeque};

use super::adaptive::{choose, AdaptiveCfg, BackendState};
use super::cachemodel::{CacheRules, ProviderCache, Weights};
use super::fair::{FairCfg, FairQueue, Item, Pick};
use super::fingerprint::Ttl;
use super::json::hash;

#[derive(Clone, Copy, Debug)]
pub enum Limit {
    Fixed(usize),
    Adaptive(usize),
}

#[derive(Clone, Debug)]
pub struct SimBackend {
    /// Requests in flight beyond which total throughput stops growing.
    pub knee: usize,
    /// Tokens per second of one request below the knee.
    pub tps: f64,
    /// Change in total throughput per request beyond the knee (e.g. -0.03).
    pub decay: f64,
    pub limit: Limit,
}

impl SimBackend {
    fn total_tps(&self, n: usize) -> f64 {
        let over = n.saturating_sub(self.knee) as f64;
        self.tps * n.min(self.knee) as f64 * (1.0 + self.decay * over).max(0.2)
    }
}

#[derive(Clone, Debug)]
pub struct Turn {
    pub new_in: u32,
    pub out: u32,
    /// Time between the previous response and this request.
    pub gap_ms: i64,
}

#[derive(Clone, Debug)]
pub struct SimRun {
    pub tenant: usize,
    pub start_ms: i64,
    /// Tokens in tools plus system prompt.
    pub base_in: u32,
    /// Identity of the system prompt (runs sharing it share cache).
    pub sys: u64,
    pub turns: Vec<Turn>,
}

#[derive(Clone, Debug, Default)]
pub struct Workload {
    pub tenants: Vec<(String, f64)>,
    pub runs: Vec<SimRun>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Policy {
    Fcfs,
    /// Round robin over runs by request count: what the gateway did before.
    Wrr,
    Vtc,
    Las,
    FairNoJump,
    Fair,
}

impl Policy {
    pub fn name(self) -> &'static str {
        match self {
            Policy::Fcfs => "fcfs",
            Policy::Wrr => "round_robin",
            Policy::Vtc => "vtc",
            Policy::Las => "run_las",
            Policy::FairNoJump => "fair_no_jump",
            Policy::Fair => "fair",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Route {
    /// Least loaded with cache affinity, decided at dispatch time.
    Smart,
    /// Round robin fixed at arrival, blind to load.
    Blind,
}

#[derive(Clone, Debug)]
pub struct SimCfg {
    pub backends: Vec<SimBackend>,
    pub policy: Policy,
    pub route: Route,
    pub cache: bool,
    pub ttl: Ttl,
    /// How much faster the server reads prompt tokens than it writes output.
    pub prefill_speedup: f64,
    pub weights: Weights,
    pub fair: FairCfg,
    pub spill: f64,
    pub rules: CacheRules,
}

impl SimCfg {
    pub fn new(backends: Vec<SimBackend>, policy: Policy) -> Self {
        Self {
            backends,
            policy,
            route: Route::Smart,
            cache: true,
            ttl: Ttl::M5,
            prefill_speedup: 20.0,
            weights: Weights::default(),
            fair: FairCfg::default(),
            spill: 0.5,
            rules: CacheRules::default(),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct SimOut {
    pub makespan_ms: f64,
    /// Per run: (tenant, time from first request to last response).
    pub run_ms: Vec<(usize, f64)>,
    /// Largest difference in weighted service between two tenants while
    /// both had requests waiting.
    pub fairness_gap: f64,
    pub largest_request: f64,
    /// Tokens re-sent uncached because the entry expired while queued.
    pub expiry_lost: u64,
    /// Tokens re-sent uncached because the request went to another server.
    pub spill_lost: u64,
    pub cache_read: u64,
    pub cache_write: u64,
    pub fresh_in: u64,
    /// Total spend in base-input-token units.
    pub cost_units: f64,
    pub tokens_out: u64,
    pub requests: u64,
    /// Time with a request waiting while a server that could take it had a
    /// free seat.
    pub idle_queued_ms: f64,
    pub final_limits: Vec<usize>,
    /// Time-weighted average concurrency limit per backend.
    pub mean_limits: Vec<f64>,
    pub served: Vec<u64>,
    pub max_wait_ms: f64,
    pub mean_wait_ms: f64,
    pub jumps: u64,
    pub affinity_kept: u64,
    pub affinity_total: u64,
}

impl SimOut {
    pub fn tokens_per_s(&self) -> f64 {
        self.tokens_out as f64 / (self.makespan_ms / 1000.0)
    }
    pub fn run_stats(&self, tenant: Option<usize>) -> (f64, f64, f64) {
        let mut v: Vec<f64> = self
            .run_ms
            .iter()
            .filter(|(t, _)| tenant.is_none_or(|x| x == *t))
            .map(|r| r.1)
            .collect();
        if v.is_empty() {
            return (0.0, 0.0, 0.0);
        }
        v.sort_by(f64::total_cmp);
        let mean = v.iter().sum::<f64>() / v.len() as f64;
        let q = |p: f64| v[((v.len() - 1) as f64 * p).round() as usize];
        (mean, q(0.5), q(0.99))
    }
}

/// Deterministic generator (splitmix64).
pub struct Rng(pub u64);

impl Rng {
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    pub fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
    pub fn range(&mut self, lo: f64, hi: f64) -> f64 {
        lo + (hi - lo) * self.unit()
    }
    pub fn normal(&mut self) -> f64 {
        let (a, b) = (self.unit().max(1e-12), self.unit());
        (-2.0 * a.ln()).sqrt() * (std::f64::consts::TAU * b).cos()
    }
}

#[derive(Default)]
struct Wrr {
    ring: VecDeque<String>,
    queues: HashMap<String, VecDeque<Item>>,
}

enum Q {
    Fair(FairQueue),
    Wrr(Wrr),
}

impl Q {
    fn push(&mut self, tenant: &str, weight: f64, run: &str, item: Item) {
        match self {
            Q::Fair(q) => q.push(tenant, weight, run, item),
            Q::Wrr(w) => {
                let q = w.queues.entry(run.to_string()).or_default();
                if q.is_empty() {
                    w.ring.push_back(run.to_string());
                }
                q.push_back(item);
            }
        }
    }
    fn peek(
        &self,
        tenant_of: &dyn Fn(&str) -> String,
        now: i64,
        ok: &dyn Fn(&Item) -> bool,
    ) -> Option<Pick> {
        match self {
            Q::Fair(q) => q.peek(now, ok),
            Q::Wrr(w) => w.ring.iter().find_map(|run| {
                let it = w.queues.get(run)?.front()?;
                ok(it).then(|| Pick {
                    tenant: tenant_of(run),
                    run: run.clone(),
                    id: it.id,
                    cost: it.cost,
                    jumped: false,
                })
            }),
        }
    }
    fn take(&mut self, p: &Pick, now: i64) -> Option<Item> {
        match self {
            Q::Fair(q) => q.take(p, now),
            Q::Wrr(w) => {
                let q = w.queues.get_mut(&p.run)?;
                let it = q.pop_front()?;
                let pos = w.ring.iter().position(|r| *r == p.run)?;
                w.ring.remove(pos);
                if !q.is_empty() {
                    w.ring.push_back(p.run.clone());
                }
                Some(it)
            }
        }
    }
    fn settle(&mut self, tenant: &str, run: &str, delta: f64) {
        if let Q::Fair(q) = self {
            q.settle(tenant, run, delta);
        }
    }
    fn finish(&mut self, tenant: &str, run: &str) {
        if let Q::Fair(q) = self {
            q.finish(tenant, run);
        }
    }
}

struct Pending {
    run: usize,
    turn: usize,
    would_read: u32,
    arrived: f64,
}

struct Active {
    run: usize,
    turn: usize,
    backend: usize,
    remaining: f64,
    started: f64,
    prefill_ms: f64,
}

struct RunState {
    chain: Vec<(u64, u32)>,
    last_dispatch: f64,
    backend: Option<usize>,
    first_arrival: f64,
}

pub fn run(wl: &Workload, cfg: &SimCfg) -> SimOut {
    let nb = cfg.backends.len();
    let mut backends: Vec<BackendState> = cfg
        .backends
        .iter()
        .enumerate()
        .map(|(i, b)| match b.limit {
            Limit::Fixed(n) => BackendState::new(&format!("b{i}"), n, None, vec![], 0),
            Limit::Adaptive(max) => BackendState::new(
                &format!("b{i}"),
                max,
                Some(AdaptiveCfg::new(max)),
                vec![],
                0,
            ),
        })
        .collect();
    let fair_cfg = |tenant_fair, las, jump| FairCfg {
        tenant_fair,
        las,
        jump,
        ..cfg.fair
    };
    let mut q = match cfg.policy {
        Policy::Wrr => Q::Wrr(Wrr::default()),
        Policy::Fcfs => Q::Fair(FairQueue::new(fair_cfg(false, false, false))),
        Policy::Vtc => Q::Fair(FairQueue::new(fair_cfg(true, false, false))),
        Policy::Las => Q::Fair(FairQueue::new(fair_cfg(false, true, false))),
        Policy::FairNoJump => Q::Fair(FairQueue::new(fair_cfg(true, true, false))),
        Policy::Fair => Q::Fair(FairQueue::new(fair_cfg(true, true, cfg.cache))),
    };
    let mut caches: Vec<ProviderCache> = (0..nb).map(|_| ProviderCache::default()).collect();
    let mut runs: Vec<RunState> = wl
        .runs
        .iter()
        .map(|r| RunState {
            chain: vec![(hash(r.sys, b"sys"), r.base_in)],
            last_dispatch: -1.0,
            backend: None,
            first_arrival: r.start_ms as f64,
        })
        .collect();
    let mut arrivals: BinaryHeap<Reverse<(i64, usize, usize)>> = wl
        .runs
        .iter()
        .enumerate()
        .map(|(i, r)| Reverse((r.start_ms, i, 0)))
        .collect();
    let nt = wl.tenants.len();
    let mut waiting = vec![0usize; nt];
    let mut service = vec![0f64; nt];
    let mut pair_base: HashMap<(usize, usize), (f64, f64)> = HashMap::new();
    let mut pending: HashMap<u64, Pending> = HashMap::new();
    let mut active: Vec<Active> = Vec::new();
    let mut out = SimOut {
        mean_limits: vec![0.0; nb],
        ..SimOut::default()
    };
    let (mut t, mut next_id, mut rr, mut wait_sum) = (0f64, 0u64, 0usize, 0f64);
    let tenant_of = |run: &str| wl.runs[run.parse::<usize>().unwrap()].tenant.to_string();
    let w = cfg.weights;

    loop {
        // Dispatch everything that can go now.
        loop {
            let now = t as i64;
            let ok = |it: &Item| match cfg.route {
                Route::Smart => choose(&backends, it, now, cfg.spill).is_some(),
                Route::Blind => it.affinity.is_some_and(|a| backends[a].free(now)),
            };
            let Some(pick) = q.peek(&tenant_of, now, &ok) else {
                break;
            };
            let b = match cfg.route {
                Route::Smart => {
                    let run_idx: usize = pick.run.parse().unwrap();
                    let probe = Item {
                        affinity: runs[run_idx].backend,
                        ..Item::default()
                    };
                    choose(&backends, &probe, now, cfg.spill).unwrap()
                }
                Route::Blind => 0,
            };
            let Some(item) = q.take(&pick, now) else {
                break;
            };
            let b = if cfg.route == Route::Blind {
                item.affinity.unwrap()
            } else {
                b
            };
            let p = pending.remove(&item.id).unwrap();
            let tenant = wl.runs[p.run].tenant;
            let weight = wl.tenants[tenant].1;
            // Fairness bookkeeping, before and after the charge.
            let turn = &wl.runs[p.run].turns[p.turn];
            let total_in = runs[p.run].chain.last().unwrap().1;
            let real = f64::from(total_in) + f64::from(turn.out) * w.output;
            for other in 0..nt {
                if other == tenant {
                    continue;
                }
                let key = (tenant.min(other), tenant.max(other));
                if waiting[tenant] > 0 && waiting[other] > 0 {
                    pair_base
                        .entry(key)
                        .or_insert((service[key.0], service[key.1]));
                } else {
                    pair_base.remove(&key);
                }
            }
            waiting[tenant] -= 1;
            service[tenant] += real / weight;
            out.largest_request = out.largest_request.max(real / weight);
            for (key, base) in &pair_base {
                let gap = ((service[key.0] - base.0) - (service[key.1] - base.1)).abs();
                out.fairness_gap = out.fairness_gap.max(gap);
            }
            q.settle(&pick.tenant, &pick.run, real - item.cost);
            if pick.jumped {
                out.jumps += 1;
            }
            // Cache and service time.
            let chain = &runs[p.run].chain;
            let last = chain.len() - 1;
            let u = if cfg.cache {
                caches[b].apply(chain, &[0, last], cfg.ttl, now, cfg.rules)
            } else {
                super::cachemodel::CacheUse {
                    fresh: total_in,
                    ..Default::default()
                }
            };
            if p.would_read > u.read {
                let lost = u64::from(p.would_read - u.read);
                if runs[p.run].backend.is_some_and(|prev| prev != b) {
                    out.spill_lost += lost;
                } else {
                    out.expiry_lost += lost;
                }
            }
            if let Some(prev) = runs[p.run].backend {
                out.affinity_total += 1;
                out.affinity_kept += u64::from(prev == b);
            }
            out.cache_read += u64::from(u.read);
            out.cache_write += u64::from(u.write);
            out.fresh_in += u64::from(u.fresh);
            out.cost_units += w.input_cost(u, cfg.ttl) + f64::from(turn.out) * w.output;
            let prefill = f64::from(u.fresh + u.write) / cfg.prefill_speedup
                + f64::from(u.read) / (cfg.prefill_speedup * 10.0);
            let waited = t - p.arrived;
            wait_sum += waited;
            out.max_wait_ms = out.max_wait_ms.max(waited);
            out.requests += 1;
            runs[p.run].last_dispatch = t;
            runs[p.run].backend = Some(b);
            backends[b].start(now);
            active.push(Active {
                run: p.run,
                turn: p.turn,
                backend: b,
                remaining: prefill + f64::from(turn.out),
                started: t,
                prefill_ms: 0.0,
            });
            let n = backends[b].inflight;
            let rate = cfg.backends[b].total_tps(n) / n as f64;
            active.last_mut().unwrap().prefill_ms = prefill / rate * 1000.0;
        }

        // Next event: an arrival or a completion.
        let rate_of = |b: usize, backends: &[BackendState]| {
            let n = backends[b].inflight.max(1);
            cfg.backends[b].total_tps(n) / n as f64
        };
        let next_done = active
            .iter()
            .map(|a| t + a.remaining / rate_of(a.backend, &backends) * 1000.0)
            .min_by(f64::total_cmp);
        let next_arr = arrivals.peek().map(|Reverse(a)| a.0 as f64);
        let Some(next) = [next_done, next_arr]
            .into_iter()
            .flatten()
            .min_by(f64::total_cmp)
        else {
            break;
        };
        let next = next.max(t);
        let dt = next - t;
        for a in &mut active {
            a.remaining -= rate_of(a.backend, &backends) * dt / 1000.0;
        }
        for (i, b) in backends.iter().enumerate() {
            out.mean_limits[i] += b.limit() as f64 * dt;
        }
        if !pending.is_empty() && backends.iter().any(|b| b.free(t as i64)) {
            out.idle_queued_ms += dt;
        }
        t = next;
        let now = t as i64;

        let mut i = 0;
        while i < active.len() {
            if active[i].remaining > 1e-6 {
                i += 1;
                continue;
            }
            let a = active.swap_remove(i);
            let r = &wl.runs[a.run];
            let turn = &r.turns[a.turn];
            backends[a.backend].finish(
                now,
                f64::from(turn.out),
                (t - a.started - a.prefill_ms).max(1.0),
                a.prefill_ms,
                true,
            );
            q.finish(&r.tenant.to_string(), &a.run.to_string());
            out.tokens_out += u64::from(turn.out);
            if a.turn + 1 < r.turns.len() {
                let gap = r.turns[a.turn + 1].gap_ms;
                arrivals.push(Reverse((now + gap, a.run, a.turn + 1)));
            } else {
                out.run_ms.push((r.tenant, t - runs[a.run].first_arrival));
            }
        }
        while arrivals.peek().is_some_and(|Reverse(a)| a.0 as f64 <= t) {
            let Reverse((_, ri, ti)) = arrivals.pop().unwrap();
            let r = &wl.runs[ri];
            let turn = &r.turns[ti];
            let st = &mut runs[ri];
            if ti > 0 {
                let prev_out = r.turns[ti - 1].out;
                let (h, tok) = *st.chain.last().unwrap();
                st.chain.push((hash(h, &[1, ti as u8]), tok + prev_out));
            }
            let (h, tok) = *st.chain.last().unwrap();
            st.chain
                .push((hash(h ^ ri as u64, &[2, ti as u8]), tok + turn.new_in));
            let total_in = tok + turn.new_in;
            let last = st.chain.len() - 1;
            let home = st.backend.unwrap_or(0);
            let would = if cfg.cache {
                caches[home]
                    .peek(&st.chain, &[0, last], now, cfg.rules)
                    .read
            } else {
                0
            };
            let pred_out = if ti > 0 { r.turns[ti - 1].out } else { 500 };
            let deadline = (cfg.cache && ti > 0 && st.last_dispatch >= 0.0)
                .then(|| st.last_dispatch as i64 + cfg.ttl.ms());
            let affinity = match cfg.route {
                Route::Smart => st.backend,
                Route::Blind => {
                    rr += 1;
                    Some(rr % nb)
                }
            };
            next_id += 1;
            let item = Item {
                id: next_id,
                cost: f64::from(total_in) + f64::from(pred_out) * w.output,
                enq_ms: now,
                deadline_ms: deadline,
                risk: f64::from(tok) * (w.write(cfg.ttl) - w.read),
                class: 1,
                affinity,
                ..Item::default()
            };
            pending.insert(
                next_id,
                Pending {
                    run: ri,
                    turn: ti,
                    would_read: would,
                    arrived: t,
                },
            );
            waiting[r.tenant] += 1;
            q.push(
                &r.tenant.to_string(),
                wl.tenants[r.tenant].1,
                &ri.to_string(),
                item,
            );
        }
    }
    out.makespan_ms = t;
    out.mean_wait_ms = if out.requests > 0 {
        wait_sum / out.requests as f64
    } else {
        0.0
    };
    out.final_limits = backends.iter().map(BackendState::limit).collect();
    for m in &mut out.mean_limits {
        *m /= t.max(1.0);
    }
    out.served = backends.iter().map(|b| b.served).collect();
    out
}

/// Three tenants on one provider with four seats: a heavy tenant with long
/// runs and large prompts, a tenant with many short runs, and a tenant whose
/// agents wait on tools for almost the cache lifetime between calls.
pub fn mixed_workload(seed: u64) -> Workload {
    let mut g = Rng(seed);
    let mut wl = Workload {
        tenants: vec![
            ("heavy".into(), 1.0),
            ("short".into(), 1.0),
            ("toolwait".into(), 1.0),
        ],
        runs: Vec::new(),
    };
    for _ in 0..6 {
        let turns = (0..12)
            .map(|_| Turn {
                new_in: 3000,
                out: g.range(1500.0, 2500.0) as u32,
                gap_ms: g.range(1000.0, 5000.0) as i64,
            })
            .collect();
        wl.runs.push(SimRun {
            tenant: 0,
            start_ms: g.range(0.0, 30_000.0) as i64,
            base_in: 40_000,
            sys: 1,
            turns,
        });
    }
    for _ in 0..24 {
        let turns = (0..3)
            .map(|_| Turn {
                new_in: 500,
                out: g.range(200.0, 400.0) as u32,
                gap_ms: g.range(2000.0, 10_000.0) as i64,
            })
            .collect();
        wl.runs.push(SimRun {
            tenant: 1,
            start_ms: g.range(0.0, 1_200_000.0) as i64,
            base_in: 6000,
            sys: 2,
            turns,
        });
    }
    for _ in 0..8 {
        let turns = (0..8)
            .map(|_| Turn {
                new_in: 1500,
                out: g.range(300.0, 600.0) as u32,
                gap_ms: g.range(240_000.0, 290_000.0) as i64,
            })
            .collect();
        wl.runs.push(SimRun {
            tenant: 2,
            start_ms: g.range(0.0, 120_000.0) as i64,
            base_in: 30_000,
            sys: 3,
            turns,
        });
    }
    wl
}

/// A steady flood of similar requests from one tenant: more work than the
/// servers can take, so the concurrency limit is what decides throughput.
pub fn flood_workload(seed: u64, runs: usize, turns: usize) -> Workload {
    let mut g = Rng(seed);
    let mut wl = Workload {
        tenants: vec![("t".into(), 1.0)],
        runs: Vec::new(),
    };
    for _ in 0..runs {
        let ts = (0..turns)
            .map(|_| Turn {
                new_in: 300,
                out: g.range(200.0, 400.0) as u32,
                gap_ms: g.range(100.0, 500.0) as i64,
            })
            .collect();
        wl.runs.push(SimRun {
            tenant: 0,
            start_ms: g.range(0.0, 2000.0) as i64,
            base_in: 2000,
            sys: 9,
            turns: ts,
        });
    }
    wl
}

/// Requests of very uneven length, so assigning them to servers without
/// looking at load leaves one busy and another idle.
pub fn uneven_workload(seed: u64) -> Workload {
    let mut g = Rng(seed);
    let mut wl = Workload {
        tenants: vec![("t".into(), 1.0)],
        runs: Vec::new(),
    };
    for i in 0..60 {
        let long = g.unit() < 0.15;
        let ts = (0..4)
            .map(|_| Turn {
                new_in: 400,
                out: if long {
                    g.range(1500.0, 2500.0) as u32
                } else {
                    g.range(60.0, 200.0) as u32
                },
                gap_ms: g.range(200.0, 1500.0) as i64,
            })
            .collect();
        wl.runs.push(SimRun {
            tenant: 0,
            start_ms: i * 10_000 + g.range(0.0, 8000.0) as i64,
            base_in: 3000,
            sys: 5,
            turns: ts,
        });
    }
    wl
}

#[cfg(test)]
mod tests {
    use super::*;

    fn api(seats: usize) -> Vec<SimBackend> {
        vec![SimBackend {
            knee: 1000,
            tps: 60.0,
            decay: 0.0,
            limit: Limit::Fixed(seats),
        }]
    }

    #[test]
    fn every_run_finishes_under_every_policy() {
        let wl = mixed_workload(1);
        let n: usize = wl.runs.iter().map(|r| r.turns.len()).sum();
        for p in [
            Policy::Fcfs,
            Policy::Wrr,
            Policy::Vtc,
            Policy::Las,
            Policy::FairNoJump,
            Policy::Fair,
        ] {
            let o = run(&wl, &SimCfg::new(api(4), p));
            assert_eq!(o.run_ms.len(), wl.runs.len(), "{}", p.name());
            assert_eq!(o.requests as usize, n);
            assert!(o.cache_read > 0 && o.makespan_ms > 0.0);
        }
    }

    #[test]
    fn simulation_is_deterministic() {
        let wl = mixed_workload(7);
        let a = run(&wl, &SimCfg::new(api(4), Policy::Fair));
        let b = run(&wl, &SimCfg::new(api(4), Policy::Fair));
        assert_eq!(a.makespan_ms, b.makespan_ms);
        assert_eq!(a.cost_units, b.cost_units);
        assert_eq!(a.fairness_gap, b.fairness_gap);
    }
}
