//! The queueing policy, as a plain data structure with no clock or I/O so
//! the live dispatcher and the simulator run exactly the same code.
//!
//! Three ideas are combined:
//! - between tenants, a weighted virtual token counter (VTC): the tenant
//!   that has received the least weighted service goes next, and a tenant
//!   returning from idle is lifted to the current floor so it cannot spend
//!   credit it saved up while away;
//! - within a tenant, least attained service per run (Autellix's program
//!   level scheduling), so short runs are not stuck behind long ones;
//! - a request whose provider cache entry is about to expire may jump the
//!   queue, but only while its tenant stays within a fixed slack of the
//!   fair order, which keeps the fairness bound finite.

use std::collections::{HashMap, VecDeque};

#[derive(Clone, Copy, Debug)]
pub struct FairCfg {
    /// Fair sharing between tenants (off: one shared queue).
    pub tenant_fair: bool,
    /// Least attained service between runs (off: first come first served).
    pub las: bool,
    /// Let requests with an expiring cache entry go first.
    pub jump: bool,
    /// How far (in weighted tokens per unit weight) a tenant may run ahead
    /// of the fair order through jumps.
    pub slack: f64,
    /// A request counts as urgent this long before its cache entry expires.
    pub jump_window_ms: i64,
    /// Ignore cache entries worth less than this many weighted tokens.
    pub min_risk: f64,
    /// A run whose oldest request waited this long goes first in its tenant.
    pub starve_ms: i64,
    /// Least used first between tenants: order them by the server time
    /// their requests held lately (per unit weight), where use this many
    /// ms old counts half. A caller that has used little goes straight to
    /// the front instead of taking its turn in the round. 0 = off.
    pub used_half_life_ms: i64,
}

impl Default for FairCfg {
    fn default() -> Self {
        Self {
            tenant_fair: true,
            las: true,
            jump: true,
            slack: 200_000.0,
            jump_window_ms: 20_000,
            min_risk: 2_000.0,
            starve_ms: 60_000,
            used_half_life_ms: 0,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct Item {
    pub id: u64,
    /// Estimated cost in weighted tokens.
    pub cost: f64,
    pub enq_ms: i64,
    /// When the cache entry this request would read expires.
    pub deadline_ms: Option<i64>,
    /// Weighted tokens lost if that entry expires first.
    pub risk: f64,
    /// 0 interactive, 1 normal, 2 background.
    pub class: u8,
    pub agent: String,
    pub model: String,
    /// Backend whose prefix cache is warm for this run.
    pub affinity: Option<usize>,
    /// The prompt as cumulative block hashes, to find the server whose
    /// prefix cache holds most of it.
    pub prefix: Option<super::prefix::Chain>,
    /// Estimated prompt tokens.
    pub tokens: u32,
    /// Importance inside a class (the agent's weight); higher goes first
    /// where the scheduler orders strictly by priority.
    pub weight: u32,
    /// Other models that may serve this request when no server for `model`
    /// has room.
    pub alts: Vec<String>,
    /// Size of the request body held while it waits.
    pub bytes: usize,
    /// Backend that just failed this request: go elsewhere if anything
    /// else has room.
    pub avoid: Option<usize>,
}

#[derive(Default)]
struct RunQ {
    attained: f64,
    q: VecDeque<Item>,
    inflight: u32,
}

struct Tenant {
    weight: f64,
    /// Weighted service received, divided by weight.
    counter: f64,
    runs: HashMap<String, RunQ>,
    waiting: usize,
    /// Server time held (ms), decayed to `used_at`.
    used: f64,
    used_at: i64,
}

impl Tenant {
    fn used_now(&self, now: i64, half_life_ms: i64) -> f64 {
        if self.used <= 0.0 || half_life_ms <= 0 {
            return 0.0;
        }
        let age = (now - self.used_at).max(0) as f64;
        self.used * 0.5f64.powf(age / half_life_ms as f64)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Pick {
    pub tenant: String,
    pub run: String,
    pub id: u64,
    pub cost: f64,
    pub jumped: bool,
}

pub struct FairQueue {
    pub cfg: FairCfg,
    tenants: HashMap<String, Tenant>,
    /// Counter of the last tenant served: where idle tenants rejoin.
    floor: f64,
    wait_ewma: f64,
    /// Smoothed server time one request holds (ms), charged for requests
    /// still in flight under least-used-first.
    held_ewma: f64,
    len: usize,
}

impl FairQueue {
    pub fn new(cfg: FairCfg) -> Self {
        Self {
            cfg,
            tenants: HashMap::new(),
            floor: 0.0,
            wait_ewma: 0.0,
            held_ewma: 0.0,
            len: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
    pub fn waiting(&self, tenant: &str) -> usize {
        self.tenants.get(tenant).map_or(0, |t| t.waiting)
    }
    pub fn counter(&self, tenant: &str) -> f64 {
        self.tenants.get(tenant).map_or(0.0, |t| t.counter)
    }

    /// The request at the head of every run: the ones `peek` chooses from.
    pub fn fronts(&self) -> impl Iterator<Item = &Item> {
        self.tenants
            .values()
            .flat_map(|t| t.runs.values().filter_map(|r| r.q.front()))
    }

    /// Every waiting request with its tenant and run.
    pub fn waiting_items(&self) -> impl Iterator<Item = (&str, &str, &Item)> {
        self.tenants.iter().flat_map(|(tn, t)| {
            t.runs
                .iter()
                .flat_map(move |(rn, r)| r.q.iter().map(move |i| (tn.as_str(), rn.as_str(), i)))
        })
    }

    fn min_backlogged(&self, except: Option<&str>) -> Option<f64> {
        self.tenants
            .iter()
            .filter(|(n, t)| t.waiting > 0 && Some(n.as_str()) != except)
            .map(|(_, t)| t.counter)
            .min_by(f64::total_cmp)
    }

    pub fn push(&mut self, tenant: &str, weight: f64, run: &str, item: Item) {
        let lift = self.min_backlogged(Some(tenant)).unwrap_or(self.floor);
        let t = self
            .tenants
            .entry(tenant.to_string())
            .or_insert_with(|| Tenant {
                weight: 1.0,
                counter: lift,
                runs: HashMap::new(),
                waiting: 0,
                used: 0.0,
                used_at: 0,
            });
        t.weight = weight.max(1e-9);
        if t.waiting == 0 {
            t.counter = t.counter.max(lift);
        }
        t.waiting += 1;
        t.runs.entry(run.to_string()).or_default().q.push_back(item);
        self.len += 1;
    }

    /// The request that should go next among those `ok` accepts.
    pub fn peek(&self, now: i64, ok: &dyn Fn(&Item) -> bool) -> Option<Pick> {
        let c = &self.cfg;
        if c.jump {
            let window = c.jump_window_ms.max((1.5 * self.wait_ewma) as i64);
            let floor = self.min_backlogged(None).unwrap_or(self.floor);
            let mut best: Option<(i64, u64, &str, &str, f64)> = None;
            for (tn, t) in &self.tenants {
                for (rn, r) in &t.runs {
                    let Some(it) = r.q.front() else { continue };
                    let Some(d) = it.deadline_ms else { continue };
                    if d <= now || d - now > window || it.risk < c.min_risk {
                        continue;
                    }
                    if t.counter + it.cost / t.weight - floor > c.slack {
                        continue;
                    }
                    if !ok(it) {
                        continue;
                    }
                    if best.is_none_or(|b| (d, it.id) < (b.0, b.1)) {
                        best = Some((d, it.id, tn, rn, it.cost));
                    }
                }
            }
            if let Some((_, id, tn, rn, cost)) = best {
                return Some(Pick {
                    tenant: tn.to_string(),
                    run: rn.to_string(),
                    id,
                    cost,
                    jumped: true,
                });
            }
        }
        // (tenant key, run key) ordering; smaller goes first.
        type Key = (f64, u8, u8, f64, i64, u64);
        let mut best: Option<(Key, &str, &str, f64)> = None;
        for (tn, t) in &self.tenants {
            if t.waiting == 0 {
                continue;
            }
            let tkey = if c.used_half_life_ms > 0 {
                // What it has used lately plus what it is using right now.
                let inflight: u32 = t.runs.values().map(|r| r.inflight).sum();
                (t.used_now(now, c.used_half_life_ms) + f64::from(inflight) * self.held_ewma)
                    / t.weight
            } else if c.tenant_fair {
                t.counter
            } else {
                0.0
            };
            if best.as_ref().is_some_and(|b| tkey > b.0 .0) {
                continue;
            }
            for (rn, r) in &t.runs {
                let Some(it) = r.q.front() else { continue };
                if !ok(it) {
                    continue;
                }
                let starving = now - it.enq_ms >= c.starve_ms;
                let attained = if c.las && !starving { r.attained } else { 0.0 };
                let key: Key = (
                    tkey,
                    it.class,
                    u8::from(!starving),
                    attained,
                    it.enq_ms,
                    it.id,
                );
                let better = best.as_ref().is_none_or(|b| {
                    key.0
                        .total_cmp(&b.0 .0)
                        .then(key.1.cmp(&b.0 .1))
                        .then(key.2.cmp(&b.0 .2))
                        .then(key.3.total_cmp(&b.0 .3))
                        .then(key.4.cmp(&b.0 .4))
                        .then(key.5.cmp(&b.0 .5))
                        .is_lt()
                });
                if better {
                    best = Some((key, tn, rn, it.cost));
                }
            }
        }
        best.map(|(key, tn, rn, cost)| Pick {
            tenant: tn.to_string(),
            run: rn.to_string(),
            id: key.5,
            cost,
            jumped: false,
        })
    }

    /// Dequeue a picked request and charge its estimated cost.
    /// Let a later request of the same run go first: for every run whose
    /// next request `yields` (it would have to start cold, say), the first
    /// of the `depth` requests behind it that is `better` moves to the
    /// front. Only the order inside one run's own queue changes, so no
    /// other run or tenant is affected. Returns how many were moved.
    pub fn regroup(
        &mut self,
        depth: usize,
        yields: &dyn Fn(&Item) -> bool,
        better: &dyn Fn(&Item) -> bool,
    ) -> usize {
        let mut moved = 0;
        for t in self.tenants.values_mut() {
            for r in t.runs.values_mut() {
                if r.q.len() < 2 || !r.q.front().is_some_and(yields) {
                    continue;
                }
                let found = r.q.iter().take(depth + 1).skip(1).position(better);
                if let Some(j) = found {
                    let it = r.q.remove(j + 1).expect("position is in range");
                    r.q.push_front(it);
                    moved += 1;
                }
            }
        }
        moved
    }

    pub fn take(&mut self, p: &Pick, now: i64) -> Option<Item> {
        let t = self.tenants.get_mut(&p.tenant)?;
        let r = t.runs.get_mut(&p.run)?;
        if r.q.front().map(|i| i.id) != Some(p.id) {
            return None;
        }
        let it = r.q.pop_front()?;
        r.inflight += 1;
        r.attained += it.cost;
        self.floor = self.floor.max(t.counter);
        t.counter += it.cost / t.weight;
        t.waiting -= 1;
        self.len -= 1;
        let waited = (now - it.enq_ms).max(0) as f64;
        self.wait_ewma = 0.9 * self.wait_ewma + 0.1 * waited;
        Some(it)
    }

    /// Drop a queued request (its client went away).
    pub fn remove(&mut self, tenant: &str, run: &str, id: u64) -> bool {
        let Some(t) = self.tenants.get_mut(tenant) else {
            return false;
        };
        let Some(r) = t.runs.get_mut(run) else {
            return false;
        };
        let before = r.q.len();
        r.q.retain(|i| i.id != id);
        let gone = before - r.q.len();
        t.waiting -= gone;
        self.len -= gone;
        if r.q.is_empty() && r.inflight == 0 {
            t.runs.remove(run);
        }
        gone > 0
    }

    /// Correct the charge once the real cost is known (`delta` may be negative).
    pub fn settle(&mut self, tenant: &str, run: &str, delta: f64) {
        if let Some(t) = self.tenants.get_mut(tenant) {
            t.counter = (t.counter + delta / t.weight).max(0.0);
            if let Some(r) = t.runs.get_mut(run) {
                r.attained = (r.attained + delta).max(0.0);
            }
        }
    }

    /// A dispatched request finished.
    pub fn finish(&mut self, tenant: &str, run: &str) {
        if let Some(t) = self.tenants.get_mut(tenant) {
            if let Some(r) = t.runs.get_mut(run) {
                r.inflight = r.inflight.saturating_sub(1);
            }
        }
    }

    /// A request of `tenant` held a server for `held_ms`.
    pub fn used(&mut self, tenant: &str, held_ms: f64, now: i64) {
        let half = self.cfg.used_half_life_ms;
        if half <= 0 {
            return;
        }
        self.held_ewma = if self.held_ewma > 0.0 {
            self.held_ewma + 0.2 * (held_ms - self.held_ewma)
        } else {
            held_ms
        };
        if let Some(t) = self.tenants.get_mut(tenant) {
            t.used = t.used_now(now, half) + held_ms.max(0.0);
            t.used_at = now;
        }
    }

    /// Server time `tenant` has used lately (ms, decayed).
    pub fn used_now(&self, tenant: &str, now: i64) -> f64 {
        self.tenants
            .get(tenant)
            .map_or(0.0, |t| t.used_now(now, self.cfg.used_half_life_ms))
    }

    /// Forget a run that has ended (its attained service no longer matters).
    pub fn end_run(&mut self, tenant: &str, run: &str) {
        if let Some(t) = self.tenants.get_mut(tenant) {
            if t.runs
                .get(run)
                .is_some_and(|r| r.q.is_empty() && r.inflight == 0)
            {
                t.runs.remove(run);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(id: u64, cost: f64, enq: i64) -> Item {
        Item {
            id,
            cost,
            enq_ms: enq,
            class: 1,
            ..Item::default()
        }
    }
    fn drain(q: &mut FairQueue, now: i64) -> Vec<u64> {
        let mut out = Vec::new();
        while let Some(p) = q.peek(now, &|_| true) {
            out.push(q.take(&p, now).unwrap().id);
        }
        out
    }

    #[test]
    fn tenants_share_by_tokens_not_by_request_count() {
        let mut q = FairQueue::new(FairCfg {
            jump: false,
            ..FairCfg::default()
        });
        // Tenant a sends big requests, b small ones.
        for i in 0..3 {
            q.push("a", 1.0, "ra", item(i, 1000.0, 0));
        }
        for i in 10..20 {
            q.push("b", 1.0, "rb", item(i, 100.0, 0));
        }
        let order = drain(&mut q, 0);
        // After a's first 1000 tokens, b gets ten small requests before a goes again.
        assert_eq!(order[0], 0);
        assert_eq!(&order[1..11], &(10..20).collect::<Vec<u64>>()[..]);
        assert!((q.counter("a") - 3000.0).abs() < 1e-9 && (q.counter("b") - 1000.0).abs() < 1e-9);
    }

    #[test]
    fn weights_scale_the_share() {
        let mut q = FairQueue::new(FairCfg {
            jump: false,
            ..FairCfg::default()
        });
        for i in 0..40 {
            q.push("a", 3.0, "ra", item(i, 100.0, 0));
            q.push("b", 1.0, "rb", item(100 + i, 100.0, 0));
        }
        let first: Vec<u64> = (0..20)
            .map(|_| {
                let p = q.peek(0, &|_| true).unwrap();
                q.take(&p, 0).unwrap().id
            })
            .collect();
        let a = first.iter().filter(|i| **i < 100).count();
        assert_eq!(a, 15);
    }

    #[test]
    fn idle_tenant_is_lifted_on_return() {
        let mut q = FairQueue::new(FairCfg {
            jump: false,
            ..FairCfg::default()
        });
        for i in 0..50 {
            q.push("busy", 1.0, "r", item(i, 100.0, 0));
        }
        for _ in 0..40 {
            let p = q.peek(0, &|_| true).unwrap();
            q.take(&p, 0);
        }
        // "late" arrives with no history. It must not get 40 turns in a row.
        for i in 100..120 {
            q.push("late", 1.0, "r", item(i, 100.0, 1));
        }
        let next: Vec<u64> = (0..6)
            .map(|_| {
                let p = q.peek(1, &|_| true).unwrap();
                q.take(&p, 1).unwrap().id
            })
            .collect();
        let late = next.iter().filter(|i| **i >= 100).count();
        assert!((2..=4).contains(&late), "{next:?}");
    }

    #[test]
    fn short_runs_overtake_long_ones_inside_a_tenant() {
        let mut q = FairQueue::new(FairCfg {
            jump: false,
            ..FairCfg::default()
        });
        for i in 0..5 {
            q.push("t", 1.0, "long", item(i, 500.0, 0));
        }
        let p = q.peek(0, &|_| true).unwrap();
        q.take(&p, 0);
        q.push("t", 1.0, "short", item(50, 100.0, 5));
        assert_eq!(q.peek(5, &|_| true).unwrap().run, "short");
        // Unless the long run has been starving.
        let mut q2 = FairQueue::new(FairCfg {
            jump: false,
            starve_ms: 1000,
            ..FairCfg::default()
        });
        q2.push("t", 1.0, "long", item(1, 500.0, 0));
        q2.settle("t", "long", 5000.0);
        q2.push("t", 1.0, "short", item(2, 100.0, 1500));
        assert_eq!(q2.peek(1500, &|_| true).unwrap().run, "long");
    }

    #[test]
    fn expiring_cache_jumps_within_slack() {
        let cfg = FairCfg {
            slack: 1500.0,
            jump_window_ms: 10_000,
            min_risk: 100.0,
            ..FairCfg::default()
        };
        let mut q = FairQueue::new(cfg);
        for i in 0..5 {
            q.push("a", 1.0, "ra", item(i, 1000.0, 0));
        }
        q.push("b", 1.0, "rb", item(10, 1000.0, 0));
        q.settle("b", "rb", 300.0); // b is behind a in the fair order
        let urgent = |id| Item {
            deadline_ms: Some(8_000),
            risk: 5000.0,
            ..item(id, 1000.0, 0)
        };
        q.push("b", 1.0, "rb2", urgent(11));
        let p = q.peek(0, &|_| true).unwrap();
        assert!(p.jumped && p.id == 11);
        q.take(&p, 0);
        // b is now 1300 ahead of a: a second jump would exceed the slack.
        q.push("b", 1.0, "rb3", urgent(12));
        let p = q.peek(0, &|_| true).unwrap();
        assert!(!p.jumped && p.tenant == "a");
        // An entry that already expired is not worth jumping for.
        let mut q3 = FairQueue::new(cfg);
        q3.push("a", 1.0, "ra", item(1, 10.0, 0));
        q3.push(
            "b",
            1.0,
            "rb",
            Item {
                deadline_ms: Some(5),
                risk: 5000.0,
                ..item(2, 10.0, 1)
            },
        );
        assert!(!q3.peek(10, &|_| true).unwrap().jumped);
    }

    #[test]
    fn the_caller_that_used_least_goes_first() {
        let mut q = FairQueue::new(FairCfg {
            used_half_life_ms: 60_000,
            ..FairCfg::default()
        });
        // A heavy caller has been holding servers; a light one has not.
        for id in 1..=3 {
            q.push("heavy", 1.0, "h", item(id, 1.0, 0));
        }
        let p = q.peek(0, &|_| true).unwrap();
        q.take(&p, 0);
        q.finish("heavy", "h");
        q.used("heavy", 8_000.0, 1_000);
        q.push("light", 1.0, "l", item(10, 1.0, 1_000));
        // Under round robin the heavy caller's next request would be due;
        // here the light caller goes straight to the front.
        assert_eq!(q.peek(1_000, &|_| true).unwrap().tenant, "light");
        let p = q.peek(1_000, &|_| true).unwrap();
        q.take(&p, 1_000);
        // Its request in flight counts as use: with nothing finished for it
        // yet, it is charged the usual holding time (8 s), about the same as
        // heavy's decayed 8 s, so it does not take every seat either.
        q.push("light", 1.0, "l", item(11, 1.0, 1_000));
        assert_eq!(q.peek(2_000, &|_| true).unwrap().tenant, "heavy");
    }

    #[test]
    fn old_use_is_forgotten_and_weight_buys_a_larger_share() {
        let mut q = FairQueue::new(FairCfg {
            used_half_life_ms: 10_000,
            ..FairCfg::default()
        });
        q.push("a", 1.0, "a", item(1, 1.0, 0));
        q.push("b", 1.0, "b", item(2, 1.0, 0));
        q.used("a", 4_000.0, 0);
        q.used("b", 1_000.0, 0);
        assert!(
            (q.used_now("a", 10_000) - 2_000.0).abs() < 1.0,
            "half after one half-life"
        );
        assert_eq!(q.peek(0, &|_| true).unwrap().tenant, "b");
        // Five times the weight outweighs four times the use.
        q.push("a", 5.0, "a", item(3, 1.0, 0));
        assert_eq!(q.peek(0, &|_| true).unwrap().tenant, "a");
    }

    #[test]
    fn ineligible_items_are_skipped_and_removal_works() {
        let mut q = FairQueue::new(FairCfg::default());
        q.push(
            "a",
            1.0,
            "r1",
            Item {
                model: "big".into(),
                ..item(1, 10.0, 0)
            },
        );
        q.push(
            "a",
            1.0,
            "r2",
            Item {
                model: "small".into(),
                ..item(2, 10.0, 1)
            },
        );
        assert_eq!(q.peek(2, &|i| i.model != "big").unwrap().id, 2);
        assert!(q.remove("a", "r1", 1));
        assert_eq!((q.len(), q.waiting("a")), (1, 1));
        assert!(!q.remove("a", "r1", 1));
    }
}
