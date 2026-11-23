//! The gateway's per-upstream dispatcher in virtual time.
//!
//! Follows `gateway/src/scheduler.rs` and `gateway/src/lead/fair.rs` as they
//! are on main after PR #10: per-agent FIFO queues served in the order of a
//! weighted virtual counter (the agent that has been given the least work
//! per unit weight goes next; one that was idle rejoins at the current
//! floor), strict priority, the bounded queue with eviction, refusing in
//! stages, least used first, and the wait estimate behind `admit_by_wait`.
//! The clock is passed in and iteration order is fixed, so runs are
//! reproducible. Not followed: classes inside one agent, runs inside a
//! tenant, cache-expiry jumps, seats reserved for important work, and the
//! `fair` (per team) policy.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

pub type ReqId = u32;

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Outlook {
    pub ahead: usize,
    pub wait_ms: f64,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Refused {
    Full,
    /// It would be held, and as many as allowed are held already.
    HoldFull,
    WaitTooLong(Outlook),
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Admission {
    pub max_queue: Option<usize>,
    pub evict: bool,
    pub max_wait_ms: Option<f64>,
    /// Requests of this weight or lower are held beside the queue: they
    /// neither count against `max_queue` nor are refused for it.
    pub hold_weight: Option<u32>,
    /// Most requests held at once (`None` = no limit).
    pub hold_max: Option<usize>,
}

struct AgentQueue {
    /// Waiting requests with their cost (estimated tokens).
    waiting: VecDeque<(ReqId, f64)>,
    inflight: usize,
    max_inflight: usize,
    weight: u32,
}

pub struct Sched {
    max_concurrency: usize,
    strict: bool,
    queues: BTreeMap<String, AgentQueue>,
    /// Work given to each agent so far, per unit weight.
    counter: BTreeMap<String, f64>,
    /// Counter of the agent served last: where idle agents rejoin.
    floor: f64,
    /// Weights seen, to place a request between least and most important.
    ranks: BTreeSet<u32>,
    /// Refusals owed per weight by staged refusal (see [`Sched::shed`]).
    shed_owed: BTreeMap<u32, f64>,
    /// Weights up to this are held beside the queue (see [`Sched::holding`]).
    hold: Option<u32>,
    /// Smoothed slot time per agent weight (ms).
    service_ms: BTreeMap<u32, f64>,
    /// Per weight: last arrival and smoothed gap between arrivals (ms).
    arrivals: BTreeMap<u32, (f64, f64)>,
    inflight: usize,
    /// Dispatch is paused until this time (429 cooldown, pacing).
    pub paused_until: f64,
    min_gap_ms: f64,
    /// Not in the live scheduler yet: serve the agent that has had the
    /// least slot time per weight, forgetting old use with this half-life.
    las_half_life_ms: Option<f64>,
    /// Per agent: slot time used (ms, decayed) and when it was last decayed.
    attained: BTreeMap<String, (f64, f64)>,
    /// Smoothed slot time of one request (ms), charged for requests still
    /// running under least used first.
    held_ms: f64,
    clock: f64,
}

impl Sched {
    pub fn new(max_concurrency: usize, strict: bool, max_rps: f64) -> Self {
        Self {
            max_concurrency,
            strict,
            queues: BTreeMap::new(),
            counter: BTreeMap::new(),
            floor: 0.0,
            ranks: BTreeSet::new(),
            shed_owed: BTreeMap::new(),
            hold: None,
            service_ms: BTreeMap::new(),
            arrivals: BTreeMap::new(),
            inflight: 0,
            paused_until: 0.0,
            min_gap_ms: if max_rps > 0.0 { 1000.0 / max_rps } else { 0.0 },
            las_half_life_ms: None,
            attained: BTreeMap::new(),
            held_ms: 0.0,
            clock: 0.0,
        }
    }

    /// Order by least attained service instead of round robin.
    /// Requests of weight `hold` or lower wait beside the queue: staged
    /// refusal does not count them.
    pub fn holding(mut self, hold: Option<u32>) -> Self {
        self.hold = hold;
        self
    }

    pub fn least_attained(mut self, half_life_s: f64) -> Self {
        self.las_half_life_ms = Some(half_life_s.max(0.001) * 1000.0);
        self
    }

    fn attained_now(&self, agent: &str) -> f64 {
        let Some(half) = self.las_half_life_ms else {
            return 0.0;
        };
        self.attained
            .get(agent)
            .map_or(0.0, |(v, at)| v * 0.5f64.powf((self.clock - at) / half))
    }

    /// Charge `held_ms` of slot time to `agent`.
    pub fn attain(&mut self, now: f64, agent: &str, held_ms: f64) {
        if self.las_half_life_ms.is_none() {
            return;
        }
        self.clock = self.clock.max(now);
        self.held_ms = if self.held_ms > 0.0 {
            self.held_ms + 0.2 * (held_ms - self.held_ms)
        } else {
            held_ms
        };
        let v = self.attained_now(agent) + held_ms;
        self.attained.insert(agent.to_string(), (v, self.clock));
        if self.attained.len() > 4096 {
            let clock = self.clock;
            let half = self.las_half_life_ms.unwrap_or(1.0);
            self.attained.retain(|_, (_, at)| clock - *at < half * 10.0);
        }
    }

    fn pick_least_attained(&self, ok: &dyn Fn(ReqId) -> bool) -> Option<String> {
        let ready = |q: &&AgentQueue| {
            q.inflight < q.max_inflight && q.waiting.front().is_some_and(|w| ok(w.0))
        };
        let top = if self.strict {
            self.queues.values().filter(ready).map(|q| q.weight).max()
        } else {
            None
        };
        self.queues
            .iter()
            .filter(|(_, q)| ready(q) && top.is_none_or(|t| q.weight >= t))
            .map(|(id, q)| {
                // What it has used lately plus what it is using right now.
                let used = self.attained_now(id) + q.inflight as f64 * self.held_ms;
                (used / f64::from(q.weight.max(1)), q.waiting[0].0, id)
            })
            .min_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)))
            .map(|(_, _, id)| id.clone())
    }

    pub fn inflight(&self) -> usize {
        self.inflight
    }

    pub fn queued(&self) -> usize {
        self.queues.values().map(|q| q.waiting.len()).sum()
    }

    /// Waiting requests of weight `hold` or lower.
    pub fn held(&self, hold: u32) -> usize {
        self.queues
            .values()
            .filter(|q| q.weight <= hold)
            .map(|q| q.waiting.len())
            .sum()
    }

    pub fn max_concurrency(&self) -> usize {
        self.max_concurrency
    }

    fn service(&self, weight: u32) -> f64 {
        self.service_ms.get(&weight).copied().unwrap_or_else(|| {
            let n = self.service_ms.len().max(1) as f64;
            self.service_ms.values().sum::<f64>() / n
        })
    }

    /// Smoothed time a request of this weight holds a slot (ms); 0 before
    /// any has finished.
    pub fn service_estimate(&self, weight: u32) -> f64 {
        if self.service_ms.is_empty() {
            0.0
        } else {
            self.service(weight.max(1))
        }
    }

    fn heavier_load(&self, weight: u32, now: f64) -> f64 {
        self.arrivals
            .iter()
            .filter(|(w, (last, gap))| **w > weight && now - last < gap * 10.0)
            .map(|(w, (_, gap))| self.service(*w) / gap.max(1.0))
            .sum::<f64>()
            / self.max_concurrency as f64
    }

    /// Where a request of `agent` with `own` of the agent's requests in front
    /// of it stands.
    pub fn outlook(&self, now: f64, agent: &str, weight: u32, own: f64) -> Outlook {
        let weight = weight.max(1);
        if self.queued() == 0 && self.inflight < self.max_concurrency {
            return Outlook::default();
        }
        let own_ms = self.service(weight);
        let (mut n, mut ms) = (own, own * own_ms);
        let slowdown = if self.strict {
            for (id, q) in &self.queues {
                if q.weight >= weight && id != agent {
                    n += q.waiting.len() as f64;
                    ms += q.waiting.len() as f64 * self.service(q.weight);
                }
            }
            1.0 / (1.0 - self.heavier_load(weight, now)).max(0.1)
        } else {
            for (id, q) in &self.queues {
                if id != agent && !q.waiting.is_empty() {
                    let turns =
                        (own * f64::from(q.weight) / f64::from(weight)).min(q.waiting.len() as f64);
                    n += turns;
                    ms += turns * self.service(q.weight);
                }
            }
            1.0
        };
        Outlook {
            ahead: n.round() as usize,
            wait_ms: (ms + own_ms) * slowdown / self.max_concurrency as f64,
        }
    }

    /// What the oldest waiting request of `agent` is looking at.
    pub fn queue_status(&self, now: f64, agent: &str, weight: u32) -> Outlook {
        self.outlook(now, agent, weight, 0.0)
    }

    /// Queue a request. Returns its outlook and the request that was pushed
    /// out to make room, if any.
    #[allow(clippy::too_many_arguments)]
    pub fn try_enqueue(
        &mut self,
        now: f64,
        agent: &str,
        weight: u32,
        max_inflight: Option<usize>,
        req: ReqId,
        cost: f64,
        adm: Admission,
    ) -> Result<(Outlook, Option<ReqId>), Refused> {
        let weight = weight.max(1);
        let own = self.queues.get(agent).map_or(0, |q| q.waiting.len()) as f64;
        let outlook = self.outlook(now, agent, weight, own);
        let held = adm.hold_weight.is_some_and(|h| weight <= h);
        if !held && adm.max_wait_ms.is_some_and(|limit| outlook.wait_ms > limit) {
            return Err(Refused::WaitTooLong(outlook));
        }
        if held {
            let n = adm.hold_weight.map_or(0, |h| self.held(h));
            if adm.hold_max.is_some_and(|max| n >= max) {
                return Err(Refused::HoldFull);
            }
        }
        let mut evicted = None;
        let counted = self.queued() - adm.hold_weight.map_or(0, |h| self.held(h));
        if !held && adm.max_queue.is_some_and(|max| counted >= max) {
            let victim = self
                .queues
                .iter()
                .filter(|(id, q)| {
                    adm.evict && q.weight < weight && id.as_str() != agent && !q.waiting.is_empty()
                })
                .min_by_key(|(_, q)| (q.weight, std::cmp::Reverse(q.waiting.len())))
                .map(|(id, _)| id.clone());
            let Some(victim) = victim else {
                return Err(Refused::Full);
            };
            evicted = self
                .queues
                .get_mut(&victim)
                .and_then(|q| q.waiting.pop_back())
                .map(|(r, _)| r);
        }
        // An agent with nothing waiting rejoins where the others are, so it
        // cannot spend credit saved up while it was away.
        if self.queues.get(agent).is_none_or(|q| q.waiting.is_empty()) {
            let lift = self
                .queues
                .iter()
                .filter(|(id, q)| id.as_str() != agent && !q.waiting.is_empty())
                .filter_map(|(id, _)| self.counter.get(id).copied())
                .reduce(f64::min)
                .unwrap_or(self.floor);
            let c = self.counter.entry(agent.to_string()).or_insert(lift);
            *c = c.max(lift);
        }
        let q = self
            .queues
            .entry(agent.to_string())
            .or_insert_with(|| AgentQueue {
                waiting: VecDeque::new(),
                inflight: 0,
                max_inflight: usize::MAX,
                weight: 1,
            });
        q.weight = weight;
        q.max_inflight = max_inflight.unwrap_or(usize::MAX).max(1);
        q.waiting.push_back((req, cost.max(1e-9)));
        self.arrivals
            .entry(weight)
            .and_modify(|(last, gap)| {
                *gap += 0.1 * (now - *last - *gap);
                *last = now;
            })
            .or_insert((now, 60_000.0));
        Ok((outlook, evicted))
    }

    /// Take a waiting request out (its caller hung up or its wait ran out).
    pub fn remove(&mut self, agent: &str, req: ReqId) -> bool {
        let Some(q) = self.queues.get_mut(agent) else {
            return false;
        };
        let Some(pos) = q.waiting.iter().position(|r| r.0 == req) else {
            return false;
        };
        q.waiting.remove(pos);
        true
    }

    fn pick(&self, ok: &dyn Fn(ReqId) -> bool) -> Option<String> {
        let ready = |q: &&AgentQueue| {
            q.inflight < q.max_inflight && q.waiting.front().is_some_and(|w| ok(w.0))
        };
        let top = if self.strict {
            self.queues.values().filter(ready).map(|q| q.weight).max()
        } else {
            None
        };
        self.queues
            .iter()
            .filter(|(_, q)| ready(q) && top.is_none_or(|t| q.weight >= t))
            .map(|(id, q)| {
                let c = self.counter.get(id).copied().unwrap_or(self.floor);
                // On a tie the gateway serves the more urgent class first;
                // the weight stands in for the class here.
                (c, std::cmp::Reverse(q.weight), q.waiting[0].0, id)
            })
            .min_by(|a, b| a.0.total_cmp(&b.0).then((a.1, a.2).cmp(&(b.1, b.2))))
            .map(|(.., id)| id.clone())
    }

    /// Refusing in stages, as the gateway does it: should a request of this
    /// weight be refused now, before the queue is full? The least important
    /// weight seen starts to be refused at `from` (a share of `max_queue`)
    /// and always from `ceiling`; the most important only when the queue is
    /// full; weights between get thresholds between, by their place in the
    /// order. Not while requests of its weight or less hold fewer than
    /// `floor` of the places. The share refused is exact, not drawn.
    pub fn shed(&mut self, weight: u32, from: f64, ceiling: f64, floor: f64, max: usize) -> bool {
        let weight = weight.max(1);
        if self.ranks.len() < 64 {
            self.ranks.insert(weight);
        }
        let below = self.ranks.range(..weight).count();
        let pos = match self.ranks.len() {
            0 | 1 => 1.0,
            n => below as f64 / (n - 1) as f64,
        };
        if pos >= 1.0 || max == 0 {
            return false;
        }
        // Held requests are beside the queue: they do not make it fuller.
        let fill = (self.queued() - self.hold.map_or(0, |h| self.held(h))) as f64 / max as f64;
        let start = from + (1.0 - from) * pos;
        let top = ceiling + (1.0 - ceiling) * pos;
        let p = ((fill - start) / (top - start).max(1e-9)).clamp(0.0, 1.0);
        let foothold = (floor * max as f64).ceil() as usize;
        let held: usize = self
            .queues
            .values()
            .filter(|q| q.weight <= weight)
            .map(|q| q.waiting.len())
            .sum();
        if p <= 0.0 || held < foothold {
            return false;
        }
        let owed = self.shed_owed.entry(weight).or_insert(0.0);
        *owed += p;
        if *owed >= 1.0 {
            *owed -= 1.0;
            return true;
        }
        false
    }

    /// Give the next request a slot if one is free. Returns the request, or
    /// the time dispatch may be tried again when it is only paused.
    pub fn grant(&mut self, now: f64) -> Result<ReqId, Option<f64>> {
        self.grant_where(now, &|_| true)
    }

    /// Like [`grant`](Self::grant), but an agent whose next request `ok`
    /// turns down is passed over this time (the request is waiting for
    /// something other than a slot).
    pub fn grant_where(
        &mut self,
        now: f64,
        ok: &dyn Fn(ReqId) -> bool,
    ) -> Result<ReqId, Option<f64>> {
        if self.inflight >= self.max_concurrency {
            return Err(None);
        }
        if now < self.paused_until {
            return Err((self.queued() > 0).then_some(self.paused_until));
        }
        self.clock = self.clock.max(now);
        let picked = if self.las_half_life_ms.is_some() {
            self.pick_least_attained(ok)
        } else {
            self.pick(ok)
        };
        let Some(agent) = picked else {
            return Err(None);
        };
        let q = self.queues.get_mut(&agent).expect("picked above");
        let (req, cost) = q.waiting.pop_front().expect("picked a non-empty queue");
        q.inflight += 1;
        let weight = q.weight;
        let c = self.counter.entry(agent).or_insert(self.floor);
        self.floor = self.floor.max(*c);
        *c += cost / f64::from(weight.max(1));
        if self.counter.len() > 4096 {
            let (floor, queues) = (self.floor, &self.queues);
            self.counter
                .retain(|id, c| *c > floor || queues.contains_key(id));
        }
        self.inflight += 1;
        if self.min_gap_ms > 0.0 {
            self.paused_until = self.paused_until.max(now + self.min_gap_ms);
        }
        Ok(req)
    }

    /// The slot of a request of `agent` is free again after `held_ms`.
    pub fn release(&mut self, agent: &str, weight: u32, held_ms: f64) {
        let weight = weight.max(1);
        let s = self.service_ms.entry(weight).or_insert(held_ms);
        *s += 0.2 * (held_ms - *s);
        self.inflight = self.inflight.saturating_sub(1);
        if let Some(q) = self.queues.get_mut(agent) {
            q.inflight = q.inflight.saturating_sub(1);
            if q.inflight == 0 && q.waiting.is_empty() {
                self.queues.remove(agent);
            }
        }
    }

    pub fn cooldown(&mut self, until: f64) {
        self.paused_until = self.paused_until.max(until);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn order(s: &mut Sched, n: usize) -> Vec<ReqId> {
        (0..n).map(|_| s.grant(0.0).unwrap()).collect()
    }

    #[test]
    fn round_robin_across_agents() {
        let mut s = Sched::new(100, false, 0.0);
        for (i, a) in ["a", "a", "a", "b", "b", "b"].iter().enumerate() {
            s.try_enqueue(0.0, a, 1, None, i as u32, 1.0, Admission::default())
                .unwrap();
        }
        assert_eq!(order(&mut s, 6), [0, 3, 1, 4, 2, 5]);
    }

    #[test]
    fn weights_give_more_turns() {
        let mut s = Sched::new(100, false, 0.0);
        for i in 0..6 {
            s.try_enqueue(0.0, "heavy", 3, None, i, 1.0, Admission::default())
                .unwrap();
        }
        for i in 10..12 {
            s.try_enqueue(0.0, "light", 1, None, i, 1.0, Admission::default())
                .unwrap();
        }
        // Three turns of the heavy agent for one of the light one.
        assert_eq!(order(&mut s, 8), [0, 10, 1, 2, 3, 11, 4, 5]);
    }

    #[test]
    fn strict_priority_serves_heaviest_first() {
        let mut s = Sched::new(100, true, 0.0);
        s.try_enqueue(0.0, "batch", 1, None, 1, 1.0, Admission::default())
            .unwrap();
        s.try_enqueue(0.0, "batch", 1, None, 2, 1.0, Admission::default())
            .unwrap();
        s.try_enqueue(0.0, "inter", 4, None, 3, 1.0, Admission::default())
            .unwrap();
        assert_eq!(order(&mut s, 3), [3, 1, 2]);
    }

    #[test]
    fn cap_is_respected_and_release_frees() {
        let mut s = Sched::new(1, false, 0.0);
        s.try_enqueue(0.0, "a", 1, None, 1, 1.0, Admission::default())
            .unwrap();
        s.try_enqueue(0.0, "a", 1, None, 2, 1.0, Admission::default())
            .unwrap();
        assert_eq!(s.grant(0.0), Ok(1));
        assert_eq!(s.grant(0.0), Err(None));
        s.release("a", 1, 500.0);
        assert_eq!(s.grant(1.0), Ok(2));
    }

    #[test]
    fn full_queue_refuses_or_evicts_lighter_work() {
        let adm = |evict| Admission {
            max_queue: Some(2),
            evict,
            max_wait_ms: None,
            hold_weight: None,
            hold_max: None,
        };
        let mut s = Sched::new(1, false, 0.0);
        s.try_enqueue(0.0, "batch", 1, None, 1, 1.0, adm(false))
            .unwrap();
        s.try_enqueue(0.0, "batch", 1, None, 2, 1.0, adm(false))
            .unwrap();
        assert_eq!(
            s.try_enqueue(0.0, "inter", 4, None, 3, 1.0, adm(false)),
            Err(Refused::Full)
        );
        let (_, evicted) = s
            .try_enqueue(0.0, "inter", 4, None, 3, 1.0, adm(true))
            .unwrap();
        assert_eq!(evicted, Some(2));
        // Equal weight never evicts.
        assert_eq!(
            s.try_enqueue(0.0, "batch2", 1, None, 4, 1.0, adm(true)),
            Err(Refused::Full)
        );
    }

    #[test]
    fn wait_estimate_refuses_at_the_door() {
        let mut s = Sched::new(1, false, 0.0);
        s.try_enqueue(0.0, "a", 1, None, 1, 1.0, Admission::default())
            .unwrap();
        s.grant(0.0).unwrap();
        s.release("a", 1, 2000.0);
        for i in 2..6 {
            s.try_enqueue(1.0, "a", 1, None, i, 1.0, Admission::default())
                .unwrap();
        }
        s.grant(1.0).unwrap();
        // Three ahead plus the running one, two seconds each.
        let o = s.outlook(1.0, "a", 1, 3.0);
        assert_eq!(o.ahead, 3);
        assert!((o.wait_ms - 8000.0).abs() < 1.0, "{o:?}");
        let adm = Admission {
            max_wait_ms: Some(5000.0),
            ..Admission::default()
        };
        assert!(matches!(
            s.try_enqueue(1.0, "a", 1, None, 9, 1.0, adm),
            Err(Refused::WaitTooLong(_))
        ));
    }

    #[test]
    fn an_agent_back_from_idle_does_not_jump_ahead_on_saved_credit() {
        let mut s = Sched::new(100, false, 0.0);
        for i in 0..4 {
            s.try_enqueue(0.0, "busy", 1, None, i, 1.0, Admission::default())
                .unwrap();
        }
        assert_eq!(order(&mut s, 3), [0, 1, 2]);
        // `late` was never served, yet it gets one turn, not three.
        for i in 10..13 {
            s.try_enqueue(0.0, "late", 1, None, i, 1.0, Admission::default())
                .unwrap();
        }
        for i in 4..6 {
            s.try_enqueue(0.0, "busy", 1, None, i, 1.0, Admission::default())
                .unwrap();
        }
        assert_eq!(order(&mut s, 4), [3, 10, 4, 11]);
    }

    #[test]
    fn staged_refusal_turns_the_least_important_away_first_and_keeps_a_foothold() {
        let mut s = Sched::new(1, false, 0.0);
        let fill = |s: &mut Sched, agent: &str, weight, from: u32, n: u32| {
            for i in from..from + n {
                s.try_enqueue(0.0, agent, weight, None, i, 1.0, Admission::default())
                    .unwrap();
            }
        };
        // Three weights seen; a queue of 20 filled to 60% by the top weight.
        fill(&mut s, "ide", 4, 0, 12);
        fill(&mut s, "chat", 2, 100, 0);
        s.shed(2, 0.25, 0.75, 0.0, 20);
        s.shed(1, 0.25, 0.75, 0.0, 20);
        // Lowest weight: thresholds 25% to 75%, so at 60% it is refused 7
        // times in 10; the middle weight starts at 62.5% and is let in.
        let low = (0..10).filter(|_| s.shed(1, 0.25, 0.75, 0.0, 20)).count();
        assert_eq!(low, 7);
        assert!(!(0..10).any(|_| s.shed(2, 0.25, 0.75, 0.0, 20)));
        assert!(!(0..10).any(|_| s.shed(4, 0.25, 0.75, 0.0, 20)));
        // With a floor of two places the lowest weight is let in until it
        // holds them.
        assert!(!s.shed(1, 0.25, 0.75, 0.1, 20));
        fill(&mut s, "batch", 1, 200, 2);
        assert!((0..3).any(|_| s.shed(1, 0.25, 0.75, 0.1, 20)));
    }

    #[test]
    fn removed_waiter_is_skipped() {
        let mut s = Sched::new(1, false, 0.0);
        s.try_enqueue(0.0, "a", 1, None, 1, 1.0, Admission::default())
            .unwrap();
        s.try_enqueue(0.0, "a", 1, None, 2, 1.0, Admission::default())
            .unwrap();
        assert!(s.remove("a", 1));
        assert_eq!(s.grant(0.0), Ok(2));
    }

    #[test]
    fn held_requests_wait_beside_the_queue() {
        let mut s = Sched::new(1, true, 0.0);
        let adm = Admission {
            max_queue: Some(1),
            evict: false,
            max_wait_ms: None,
            hold_weight: Some(1),
            hold_max: Some(3),
        };
        s.try_enqueue(0.0, "a", 4, None, 1, 1.0, adm).unwrap();
        assert_eq!(s.grant(0.0).unwrap(), 1);
        // Low weight is held however many wait, and takes no room.
        for r in 2..5 {
            s.try_enqueue(0.0, "b", 1, None, r, 1.0, adm).unwrap();
        }
        assert_eq!(s.held(1), 3);
        // As many as allowed are held: one more is refused.
        assert!(matches!(
            s.try_enqueue(0.0, "b", 1, None, 9, 1.0, adm),
            Err(Refused::HoldFull)
        ));
        s.try_enqueue(0.0, "a", 4, None, 5, 1.0, adm).unwrap();
        assert!(matches!(
            s.try_enqueue(0.0, "c", 4, None, 6, 1.0, adm),
            Err(Refused::Full)
        ));
        // Strict priority: the held ones go when nothing heavier waits.
        s.release("a", 4, 1.0);
        assert_eq!(s.grant(0.0).unwrap(), 5);
    }
}
