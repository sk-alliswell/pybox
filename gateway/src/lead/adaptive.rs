//! Backends: how many requests each model server should run at once, and
//! which server a request should go to.
//!
//! The model server does the batching. What the gateway controls is how many
//! requests it lets in. Too few and the server idles between tokens; too many
//! and every request slows down without the total going up. `Adaptive` finds
//! the knee by hill climbing on observed tokens per second.

use super::fair::Item;
use super::prefix::{Affinity, PrefixMap};

#[derive(Clone, Copy, Debug)]
pub struct AdaptiveCfg {
    pub min: usize,
    pub max: usize,
    pub start: usize,
    /// A step must raise throughput by at least this fraction to be kept.
    pub gain: f64,
    /// Completed requests needed before a decision.
    pub window: u32,
    /// Look again at neighbouring limits after this many quiet windows.
    pub reprobe: u32,
    /// Step down when time to first token exceeds this (0 = no guard).
    pub ttft_budget_ms: f64,
    /// Step down when time to first token is this many times the lowest
    /// seen (0 = no guard).
    pub ttft_gradient: f64,
}

impl AdaptiveCfg {
    pub fn new(max: usize) -> Self {
        Self {
            min: 1,
            max: max.max(1),
            start: max.clamp(1, 2),
            gain: 0.05,
            window: 6,
            reprobe: 30,
            ttft_budget_ms: 0.0,
            ttft_gradient: 0.0,
        }
    }
}

/// Concurrency limit for one backend, adjusted from what it delivers.
#[derive(Clone, Debug)]
pub struct Adaptive {
    pub cfg: AdaptiveCfg,
    pub limit: usize,
    /// Smoothed throughput (tokens/s) seen at each limit; NaN = not tried.
    thr: Vec<f64>,
    // Current window.
    n: u32,
    sum_rate: f64,
    sum_ttft: f64,
    busy_ms: f64,
    span_ms: f64,
    last_ms: i64,
    errors: u32,
    quiet: u32,
    /// Completions to ignore after a change: they ran under the old limit.
    skip: u32,
    pub changes: u32,
    /// Lowest window-average time to first token seen (ms); 0 = none yet.
    base_ttft: f64,
}

impl Adaptive {
    pub fn new(cfg: AdaptiveCfg, now: i64) -> Self {
        Self {
            limit: cfg.start.clamp(cfg.min, cfg.max),
            thr: vec![f64::NAN; cfg.max + 2],
            cfg,
            n: 0,
            sum_rate: 0.0,
            sum_ttft: 0.0,
            busy_ms: 0.0,
            span_ms: 0.0,
            last_ms: now,
            errors: 0,
            quiet: 0,
            skip: 0,
            changes: 0,
            base_ttft: 0.0,
        }
    }

    /// Call whenever the number of requests in flight changes, with the
    /// count that held since the previous call.
    pub fn tick(&mut self, now: i64, inflight_before: usize) {
        let dt = (now - self.last_ms).max(0) as f64;
        self.busy_ms += dt * inflight_before.min(self.limit) as f64;
        self.span_ms += dt;
        self.last_ms = now;
    }

    /// A request finished. `tokens` over `gen_ms` (first token to last) is
    /// its own generation speed.
    /// The server said it is over its limit (429 or 529): halve at once,
    /// whatever throughput looked like.
    pub fn overloaded(&mut self, now: i64) {
        let to = (self.limit / 2).max(self.cfg.min);
        self.thr[self.limit] = f64::NAN;
        self.set(to, now);
    }

    pub fn done(&mut self, now: i64, tokens: f64, gen_ms: f64, ttft_ms: f64, ok: bool) {
        if !ok {
            self.errors += 1;
        } else if self.skip > 0 {
            self.skip -= 1;
            if self.skip == 0 {
                self.busy_ms = 0.0;
                self.span_ms = 0.0;
            }
        } else if gen_ms > 0.0 && tokens > 0.0 {
            self.n += 1;
            self.sum_rate += tokens / (gen_ms / 1000.0);
            self.sum_ttft += ttft_ms;
        }
        if self.errors >= 2 {
            let to = (self.limit / 2).max(self.cfg.min);
            self.set(to, now);
            return;
        }
        if self.n >= self.cfg.window.max(2 * self.limit as u32) {
            self.decide(now);
        }
    }

    fn set(&mut self, to: usize, now: i64) {
        if to != self.limit {
            self.skip = self.limit as u32;
            self.limit = to;
            self.changes += 1;
            self.quiet = 0;
        }
        self.n = 0;
        self.sum_rate = 0.0;
        self.sum_ttft = 0.0;
        self.busy_ms = 0.0;
        self.span_ms = 0.0;
        self.errors = 0;
        self.last_ms = now;
    }

    fn decide(&mut self, now: i64) {
        let l = self.limit;
        let avg_inflight = if self.span_ms > 0.0 {
            self.busy_ms / self.span_ms
        } else {
            0.0
        };
        // The limit only teaches us something when it was actually binding.
        if avg_inflight < 0.8 * l as f64 {
            self.set(l, now);
            return;
        }
        let rate = self.sum_rate / f64::from(self.n);
        let total = rate * avg_inflight;
        let ttft = self.sum_ttft / f64::from(self.n);
        if ttft > 0.0 && (self.base_ttft <= 0.0 || ttft < self.base_ttft) {
            self.base_ttft = ttft;
        }
        self.thr[l] = if self.thr[l].is_nan() {
            total
        } else {
            0.6 * self.thr[l] + 0.4 * total
        };
        let g = 1.0 + self.cfg.gain;
        let (here, below, above) = (self.thr[l], self.thr[l - 1], self.thr[l + 1]);
        // Too slow: over the configured budget, or first tokens take several
        // times longer than they did at the lowest load seen (queueing inside
        // the server, which more requests can only make worse). The second
        // test is what stops the climb on servers whose throughput never
        // flattens, such as a proxy in front of a hosted API.
        let too_slow = (self.cfg.ttft_budget_ms > 0.0 && ttft > self.cfg.ttft_budget_ms)
            || (self.cfg.ttft_gradient > 0.0
                && self.base_ttft > 0.0
                && ttft > self.base_ttft * self.cfg.ttft_gradient);
        let to = if too_slow && l > self.cfg.min {
            l - 1
        } else if l > self.cfg.min && !below.is_nan() && here < below * g {
            // The last step up did not pay for itself.
            l - 1
        } else if l < self.cfg.max && !too_slow && (above.is_nan() || above >= here * g) {
            l + 1
        } else if l > self.cfg.min && below.is_nan() {
            // Going up did not help and lower was never tried: the same
            // throughput with fewer requests in flight means less waiting.
            l - 1
        } else {
            l
        };
        if to == l {
            self.quiet += 1;
            if self.quiet >= self.cfg.reprobe {
                // Conditions drift: forget the neighbours and look again.
                self.thr[l + 1] = f64::NAN;
                if l > self.cfg.min {
                    self.thr[l - 1] = f64::NAN;
                }
                self.quiet = 0;
            }
        }
        self.set(to, now);
    }
}

/// One model server behind an upstream.
#[derive(Clone, Debug)]
pub struct BackendState {
    pub name: String,
    pub inflight: usize,
    pub fixed_limit: usize,
    pub ctl: Option<Adaptive>,
    /// Do not send new work before this time (after failures).
    pub cool_until_ms: i64,
    /// Models this backend serves; empty = any.
    pub models: Vec<String>,
    pub served: u64,
    /// Estimated cost of the requests in flight (weighted tokens, or
    /// requests where the queue counts requests).
    pub work: f64,
    /// Smoothed service time per unit of cost (ms); 0 = not measured yet.
    pub slow: f64,
    /// Times in a row this server was taken out; sets how long it stays out
    /// and holds it to one trial request until that succeeds.
    pub fails: u32,
    /// Failed requests in a row since the last good one.
    pub errs: u32,
    /// Estimated prompt tokens of the requests in flight.
    pub tokens: f64,
    /// When a request last started on an idle server or finished (ms).
    pub progress_ms: i64,
    /// With requests in flight and nothing finished for this long, the
    /// server takes no more work (0 = never).
    pub stale_ms: i64,
}

/// Failed requests in a row before a server is taken out.
const EJECT_AFTER: u32 = 3;
const COOL_BASE_MS: i64 = 10_000;
const COOL_MAX_MS: i64 = 120_000;

impl BackendState {
    pub fn new(
        name: &str,
        limit: usize,
        adaptive: Option<AdaptiveCfg>,
        models: Vec<String>,
        now: i64,
    ) -> Self {
        Self {
            name: name.to_string(),
            inflight: 0,
            fixed_limit: limit.max(1),
            ctl: adaptive.map(|c| Adaptive::new(c, now)),
            cool_until_ms: 0,
            models,
            served: 0,
            work: 0.0,
            slow: 0.0,
            fails: 0,
            errs: 0,
            tokens: 0.0,
            progress_ms: now,
            stale_ms: 0,
        }
    }
    /// Requests are in flight and none has finished for too long: whatever
    /// the gateway knows about this server is out of date.
    pub fn stale(&self, now: i64) -> bool {
        self.stale_ms > 0 && self.inflight > 0 && now - self.progress_ms > self.stale_ms
    }
    pub fn limit(&self) -> usize {
        if self.fails > 0 {
            // Back from a failure: one request at a time until one succeeds.
            return 1;
        }
        self.ctl.as_ref().map_or(self.fixed_limit, |c| c.limit)
    }
    pub fn load(&self) -> f64 {
        self.inflight as f64 / self.limit() as f64
    }
    pub fn serves(&self, model: &str) -> bool {
        self.models.is_empty() || model.is_empty() || self.models.iter().any(|m| m == model)
    }
    pub fn free(&self, now: i64) -> bool {
        self.inflight < self.limit() && now >= self.cool_until_ms && !self.stale(now)
    }
    pub fn start(&mut self, now: i64) {
        if let Some(c) = self.ctl.as_mut() {
            c.tick(now, self.inflight);
        }
        if self.inflight == 0 {
            self.progress_ms = now;
        }
        self.inflight += 1;
        self.served += 1;
    }
    pub fn finish(&mut self, now: i64, tokens: f64, service_ms: f64, ttft_ms: f64, ok: bool) {
        if let Some(c) = self.ctl.as_mut() {
            c.tick(now, self.inflight);
            c.done(now, tokens, service_ms, ttft_ms, ok);
        }
        self.progress_ms = now;
        self.inflight = self.inflight.saturating_sub(1);
    }

    /// Like [`start`](Self::start), and counts the request's estimated cost
    /// as work this server still has to do.
    pub fn begin(&mut self, now: i64, cost: f64) {
        self.start(now);
        self.work += cost;
    }

    /// A request that [`begin`](Self::begin) started is over. `ok` is
    /// `None` when nothing was learned about the server (the caller left
    /// before it answered).
    pub fn end(&mut self, now: i64, cost: f64, held_ms: f64, ok: Option<bool>) {
        self.work = (self.work - cost).max(0.0);
        match ok {
            Some(true) => {
                self.fails = 0;
                self.errs = 0;
                if cost > 0.0 && held_ms > 0.0 {
                    let per = held_ms / cost;
                    self.slow = if self.slow > 0.0 {
                        0.8 * self.slow + 0.2 * per
                    } else {
                        per
                    };
                }
            }
            Some(false) => {
                self.errs += 1;
                if self.errs >= EJECT_AFTER || self.fails > 0 {
                    self.fail(now);
                }
            }
            None => {}
        }
    }

    /// Take the server out: 10 s the first time, doubling while it keeps
    /// failing, at most two minutes.
    pub fn fail(&mut self, now: i64) -> i64 {
        self.fails = (self.fails + 1).min(16);
        self.errs = 0;
        let cool = (COOL_BASE_MS << (self.fails - 1).min(4)).min(COOL_MAX_MS);
        self.cool_until_ms = self.cool_until_ms.max(now + cool);
        cool
    }

    /// The server asked for a pause (429): honour it without counting a
    /// failure, and let the adaptive limit come down.
    pub fn pause(&mut self, now: i64, ms: i64) {
        self.cool_until_ms = self.cool_until_ms.max(now + ms);
        if let Some(c) = self.ctl.as_mut() {
            c.overloaded(now);
        }
    }
}

/// How long the work a backend already has would take it, in comparable
/// units: outstanding cost times how slow the server has been, spread over
/// its seats. Servers not measured yet count as average.
fn backlog(backends: &[BackendState], i: usize) -> f64 {
    let b = &backends[i];
    let slow = if b.slow > 0.0 {
        b.slow
    } else {
        let known: Vec<f64> = backends
            .iter()
            .filter(|b| b.slow > 0.0)
            .map(|b| b.slow)
            .collect();
        if known.is_empty() {
            1.0
        } else {
            known.iter().sum::<f64>() / known.len() as f64
        }
    };
    b.work * slow / b.limit() as f64
}

/// What routing knows about warm prefix caches.
#[derive(Clone, Copy)]
pub struct Warmth<'a> {
    pub mode: Affinity,
    /// The picture of each backend's cache; without it only the run's
    /// previous backend counts as warm.
    pub map: Option<&'a PrefixMap>,
    /// A shared start shorter than this (estimated tokens) is not worth
    /// steering for.
    pub min_tokens: u32,
    /// How long a request may wait for the full server that holds most of
    /// its prompt, while another server is free (0 = never).
    pub wait_ms: i64,
    /// `Some(w)`: choose the server with the least prompt tokens left to
    /// read plus `w` times the prompt tokens it already has in flight per
    /// seat, instead of least load with a tolerance for warm servers.
    pub load_weight: Option<f64>,
    /// A prompt no server holds does not push out one that was used within
    /// this time (0 = it may).
    pub slot_hold_ms: i64,
    /// ...unless it has waited this long.
    pub slot_max_wait_ms: i64,
}

impl Warmth<'_> {
    /// Run affinity only, as before prefix routing existed.
    pub const RUN: Warmth<'static> = Warmth {
        mode: Affinity::Run,
        map: None,
        min_tokens: 0,
        wait_ms: 0,
        load_weight: None,
        slot_hold_ms: 0,
        slot_max_wait_ms: 0,
    };

    /// (tokens backend `i` can reuse, tokens in the prompt). A run's
    /// previous backend without a prompt to compare counts as all of it.
    fn of(&self, it: &Item, i: usize) -> (u32, u32) {
        match (self.mode, self.map, it.prefix.as_deref()) {
            (Affinity::Off, _, _) => (0, 0),
            (Affinity::Prefix, Some(map), Some(chain)) => {
                let total = chain.last().map_or(0, |c| c.1);
                let w = map.warm(i, chain);
                if w >= self.min_tokens.max(1) {
                    (w, total)
                } else {
                    (0, total)
                }
            }
            _ if it.affinity == Some(i) => (1, 1),
            _ => (0, 0),
        }
    }
}

/// Where a request goes.
#[derive(Clone, Debug, PartialEq)]
pub struct Routed {
    pub backend: usize,
    /// The stand-in model that serves it, if not its own.
    pub model: Option<String>,
    /// Estimated prompt tokens the chosen backend can reuse, and the
    /// prompt's size. Both 0 when nothing is known about the prompt.
    pub warm: u32,
    pub total: u32,
    /// Some backend held a usable prefix (whether or not it was chosen).
    pub had_warm: bool,
}

/// Pick a backend with a free seat for this request.
///
/// Among the servers that serve the model, the one with the least work
/// outstanding wins (cost of the requests in flight, weighted by how slow the
/// server has been), then the one with the smallest share of its seats in
/// use. A server that already holds the start of the prompt is preferred
/// while it has room and is not much busier than that one: the tolerated
/// difference in load is `spill` (0..1) times the share of the prompt the
/// server holds, and of several warm servers the one holding most wins. A
/// full server never holds a request back while another one is free, unless
/// `wait_ms` says a short wait for it is acceptable.
pub fn choose(backends: &[BackendState], it: &Item, now: i64, spill: f64) -> Option<usize> {
    choose_for(backends, it, &it.model, &Warmth::RUN, now, spill)
}

fn choose_for(
    backends: &[BackendState],
    it: &Item,
    model: &str,
    warmth: &Warmth,
    now: i64,
    spill: f64,
) -> Option<usize> {
    if it.avoid.is_some() {
        // A retry goes to a different server when one has room.
        let other = choose_from(backends, it, model, warmth, it.avoid, now, spill);
        if other.is_some() {
            return other;
        }
    }
    choose_from(backends, it, model, warmth, None, now, spill)
}

fn choose_from(
    backends: &[BackendState],
    it: &Item,
    model: &str,
    warmth: &Warmth,
    skip: Option<usize>,
    now: i64,
    spill: f64,
) -> Option<usize> {
    let mut least: Option<(f64, f64, usize, usize)> = None;
    for (i, b) in backends.iter().enumerate() {
        if !b.free(now) || !b.serves(model) || skip == Some(i) {
            continue;
        }
        let key = (backlog(backends, i), b.load(), b.inflight, i);
        if least.is_none_or(|l| {
            key.0
                .total_cmp(&l.0)
                .then(key.1.total_cmp(&l.1))
                .then(key.2.cmp(&l.2))
                .then(key.3.cmp(&l.3))
                .is_lt()
        }) {
            least = Some(key);
        }
    }
    let (_, min_load, _, idx) = least?;
    if let Some(w) = warmth.load_weight {
        // One number per server: what it would have to read of this prompt,
        // plus what it is already busy reading for others.
        let size = f64::from(it.tokens.max(1));
        let mut best: Option<(f64, usize)> = None;
        for (i, b) in backends.iter().enumerate() {
            if !b.free(now) || !b.serves(model) || skip == Some(i) {
                continue;
            }
            let (warm, total) = warmth.of(it, i);
            let unread = if total == 0 {
                size
            } else {
                size * (1.0 - f64::from(warm) / f64::from(total.max(warm)))
            };
            let score = unread + w * b.tokens / b.limit() as f64;
            // Ties go to the run's own server, then the less busy one.
            let better = best.is_none_or(|(s, j)| {
                score < s
                    || (score == s
                        && (it.affinity == Some(i)
                            || (it.affinity != Some(j) && b.inflight < backends[j].inflight)))
            });
            if better {
                best = Some((score, i));
            }
        }
        return best.map(|(_, i)| i);
    }
    // The warmest server that is not too much busier than the least loaded.
    let mut best: Option<(u32, f64, usize)> = None;
    for (i, b) in backends.iter().enumerate() {
        if !b.free(now) || !b.serves(model) || skip == Some(i) {
            continue;
        }
        let (warm, total) = warmth.of(it, i);
        if warm == 0 {
            continue;
        }
        let share = f64::from(warm) / f64::from(total.max(warm));
        if b.load() > min_load + spill * share {
            continue;
        }
        // Equally warm: the run's own last server (it also holds the
        // answer it gave), then the less busy one.
        let own = it.affinity == Some(i);
        if best.is_none_or(|(w, l, j)| {
            warm > w || (warm == w && (own || (it.affinity != Some(j) && b.load() < l)))
        }) {
            best = Some((warm, b.load(), i));
        }
    }
    Some(best.map_or(idx, |(_, _, i)| i))
}

/// The request should wait a little: the server holding most of its prompt
/// is full, the one it would get instead holds less, and it has not waited
/// `wait_ms` yet.
fn hold(backends: &[BackendState], it: &Item, warmth: &Warmth, chosen: usize, now: i64) -> bool {
    if warmth.wait_ms <= 0 || it.avoid.is_some() || now - it.enq_ms >= warmth.wait_ms {
        return false;
    }
    let got = warmth.of(it, chosen).0;
    backends.iter().enumerate().any(|(i, b)| {
        let (warm, total) = warmth.of(it, i);
        warm > got
            && u64::from(warm) * 2 >= u64::from(total)
            && b.serves(&it.model)
            && b.fails == 0
            && now >= b.cool_until_ms
            && b.inflight >= b.limit()
    })
}

/// Session admission: a prompt nobody holds has to push some other prompt
/// out of a server's cache. While every prompt it could push out was used
/// within `slot_hold_ms`, the sessions behind them are still running and
/// would each have to be read again, so the newcomer waits (at most
/// `slot_max_wait_ms`) until one of them goes quiet. Returns the server to
/// use, which may be another free one whose oldest prompt is idle.
fn settle(
    backends: &[BackendState],
    it: &Item,
    warmth: &Warmth,
    chosen: usize,
    now: i64,
) -> Option<usize> {
    let (Some(map), Some(chain)) = (warmth.map, it.prefix.as_deref()) else {
        return Some(chosen);
    };
    if warmth.slot_hold_ms <= 0
        || warmth.mode != Affinity::Prefix
        || it.avoid.is_some()
        || warmth.of(it, chosen).0 > 0
        || now - it.enq_ms >= warmth.slot_max_wait_ms
    {
        return Some(chosen);
    }
    let quiet = |i: usize| {
        map.victim_idle(i, chain, now)
            .is_none_or(|idle| idle >= warmth.slot_hold_ms)
    };
    if quiet(chosen) {
        return Some(chosen);
    }
    backends
        .iter()
        .enumerate()
        .filter(|(i, b)| b.free(now) && b.serves(&it.model) && quiet(*i))
        .min_by(|a, b| a.1.load().total_cmp(&b.1.load()))
        .map(|(i, _)| i)
}

/// Like [`choose`], but when no server for the request's own model has room
/// (or `skip` says the model is paused), a server for one of its stand-in
/// models may take it. `None` when the request has to wait.
pub fn route(
    backends: &[BackendState],
    it: &Item,
    now: i64,
    spill: f64,
    warmth: &Warmth,
    skip: &dyn Fn(&str) -> bool,
) -> Option<Routed> {
    let had_warm = (0..backends.len()).any(|i| warmth.of(it, i).0 > 0);
    if !skip(&it.model) {
        if let Some(i) = choose_for(backends, it, &it.model, warmth, now, spill) {
            if hold(backends, it, warmth, i, now) {
                return None;
            }
            let i = settle(backends, it, warmth, i, now)?;
            let (warm, total) = warmth.of(it, i);
            return Some(Routed {
                backend: i,
                model: None,
                warm,
                total,
                had_warm,
            });
        }
        if warmth.wait_ms > 0 && had_warm && now - it.enq_ms < warmth.wait_ms {
            // Its own servers are only full for now: no stand-in yet.
            return None;
        }
    }
    it.alts
        .iter()
        .filter(|m| m.as_str() != it.model && !skip(m))
        .find_map(|m| {
            // Another model's server holds nothing of this prompt.
            choose_for(
                backends,
                it,
                m,
                &Warmth {
                    mode: Affinity::Off,
                    ..*warmth
                },
                now,
                spill,
            )
            .map(|i| Routed {
                backend: i,
                model: Some(m.clone()),
                warm: 0,
                total: 0,
                had_warm,
            })
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A server whose total throughput grows up to `knee` requests and then
    /// stays flat (slightly worse) however many more are let in.
    fn total_tps(n: usize, knee: usize) -> f64 {
        let over = n.saturating_sub(knee) as f64;
        30.0 * n.min(knee) as f64 * (1.0 - 0.03 * over)
    }

    fn converge(knee: usize, max: usize) -> usize {
        let mut a = Adaptive::new(AdaptiveCfg::new(max), 0);
        let mut now = 0i64;
        let mut seen = [0u32; 18];
        for i in 0..900 {
            if i >= 300 {
                seen[a.limit] += 1;
            }
            let n = a.limit;
            // Saturated: n requests each producing 300 tokens.
            let per = total_tps(n, knee) / n as f64;
            let service_ms = 300.0 / per * 1000.0;
            a.tick(now, n);
            now += (service_ms / n as f64) as i64;
            a.tick(now, n);
            a.done(now, 300.0, service_ms, 100.0, true);
        }
        // Where it spent most of its time once settled.
        (0..seen.len()).max_by_key(|&l| seen[l]).unwrap()
    }

    #[test]
    fn finds_the_knee() {
        for knee in [1, 2, 4, 8] {
            let got = converge(knee, 16);
            assert_eq!(got, knee, "knee {knee}");
        }
    }

    #[test]
    fn errors_halve_the_limit_and_idle_windows_change_nothing() {
        let mut a = Adaptive::new(
            AdaptiveCfg {
                start: 8,
                ..AdaptiveCfg::new(16)
            },
            0,
        );
        a.done(1, 0.0, 0.0, 0.0, false);
        a.done(2, 0.0, 0.0, 0.0, false);
        assert_eq!(a.limit, 4);
        // Demand below the limit: nothing is learned.
        let mut now = 10;
        for _ in 0..50 {
            a.tick(now, 1);
            now += 1000;
            a.tick(now, 1);
            a.done(now, 100.0, 1000.0, 50.0, true);
        }
        assert_eq!(a.limit, 4);
    }

    #[test]
    fn routing_spills_from_a_full_server_and_respects_affinity() {
        let mut bs = vec![
            BackendState::new("a", 2, None, vec![], 0),
            BackendState::new("b", 2, None, vec![], 0),
        ];
        let it = |aff| Item {
            affinity: aff,
            ..Item::default()
        };
        // Affinity wins while the difference in load is small.
        bs[0].inflight = 1;
        assert_eq!(choose(&bs, &it(Some(0)), 0, 0.5), Some(0));
        // A full server never blocks: spill to the idle one.
        bs[0].inflight = 2;
        assert_eq!(choose(&bs, &it(Some(0)), 0, 0.5), Some(1));
        // No affinity: least loaded.
        bs[0].inflight = 1;
        assert_eq!(choose(&bs, &it(None), 0, 0.5), Some(1));
        // Everything full, cooling down or serving another model: wait.
        bs[1].inflight = 2;
        bs[0].inflight = 2;
        assert_eq!(choose(&bs, &it(None), 0, 0.5), None);
        bs[0].inflight = 0;
        bs[0].cool_until_ms = 100;
        assert_eq!(choose(&bs, &it(None), 50, 0.5), None);
        bs[0].models = vec!["m1".into()];
        assert_eq!(
            choose(
                &bs,
                &Item {
                    model: "m2".into(),
                    ..Item::default()
                },
                200,
                0.5
            ),
            None
        );
    }

    /// 1000 tokens per block.
    fn chain(blocks: &[u64]) -> crate::lead::prefix::Chain {
        let mut cum = 7u64;
        blocks
            .iter()
            .enumerate()
            .map(|(i, b)| {
                cum = cum.wrapping_mul(1_000_003).wrapping_add(*b);
                (cum, 1000 * (i as u32 + 1))
            })
            .collect::<Vec<_>>()
            .into()
    }

    #[test]
    fn routing_prefers_the_server_holding_most_of_the_prompt_within_reason() {
        let mut bs: Vec<BackendState> = ["a", "b", "c"]
            .iter()
            .map(|n| BackendState::new(n, 4, None, vec![], 0))
            .collect();
        let mut map = PrefixMap::new(&[4, 4, 4], 0);
        map.note(1, &chain(&[1, 2]), 0);
        map.note(2, &chain(&[1, 2, 3, 4]), 0);
        let warmth = |min_tokens, wait_ms| Warmth {
            mode: Affinity::Prefix,
            map: Some(&map),
            min_tokens,
            wait_ms,
            ..Warmth::RUN
        };
        let it = |blocks: &[u64]| Item {
            prefix: Some(chain(blocks)),
            ..Item::default()
        };
        let go = |bs: &[BackendState], it: &Item, w: &Warmth| {
            route(bs, it, 0, 0.5, w, &|_| false).map(|r| (r.backend, r.warm, r.had_warm))
        };
        let w = warmth(64, 0);
        // Two servers hold a start of it: the longer one wins.
        assert_eq!(go(&bs, &it(&[1, 2, 3, 4, 5]), &w), Some((2, 4000, true)));
        assert_eq!(go(&bs, &it(&[1, 2, 9]), &w), Some((1, 2000, true)));
        // Nothing held: least loaded, as before.
        assert_eq!(go(&bs, &it(&[7]), &w), Some((0, 0, false)));
        // The warm server may be busier by `spill` times the share it
        // holds: 4 of 5 blocks tolerate 0.4, so one seat in four is fine
        // and two are not; the server holding 2 of 5 is next (0.2 < 0.25).
        bs[2].inflight = 1;
        assert_eq!(go(&bs, &it(&[1, 2, 3, 4, 5]), &w).unwrap().0, 2);
        bs[2].inflight = 2;
        assert_eq!(go(&bs, &it(&[1, 2, 3, 4, 5]), &w).unwrap().0, 1);
        bs[1].inflight = 1;
        assert_eq!(go(&bs, &it(&[1, 2, 3, 4, 5]), &w), Some((0, 0, true)));
        // A start shorter than the minimum is not worth steering for.
        bs[1].inflight = 0;
        bs[2].inflight = 0;
        assert_eq!(go(&bs, &it(&[1, 2, 9]), &warmth(2001, 0)).unwrap().0, 0);
        // With no map, or without a prompt, only the run's last server counts.
        let run = Item {
            affinity: Some(1),
            ..Item::default()
        };
        assert_eq!(go(&bs, &run, &w).unwrap().0, 1);
        let off = Warmth {
            mode: Affinity::Off,
            ..w
        };
        assert_eq!(go(&bs, &it(&[1, 2, 3, 4, 5]), &off), Some((0, 0, false)));

        // Waiting: the warm server is full and the request is new.
        bs[2].inflight = 4;
        let held = warmth(64, 1000);
        assert_eq!(go(&bs, &it(&[1, 2, 3, 4, 5]), &held), None);
        // Less than half of the prompt held anywhere full: do not wait.
        assert_eq!(
            go(&bs, &it(&[1, 2, 3, 4, 5, 6, 7, 8, 9]), &held).unwrap().0,
            1
        );
        // Waited long enough, or the server is out: take what is free.
        let late = Item {
            enq_ms: -1000,
            ..it(&[1, 2, 3, 4, 5])
        };
        assert_eq!(go(&bs, &late, &held).unwrap().0, 1);
        bs[2].cool_until_ms = 10;
        assert_eq!(go(&bs, &it(&[1, 2, 3, 4, 5]), &held).unwrap().0, 1);
    }

    #[test]
    fn cost_routing_weighs_tokens_to_read_against_tokens_in_flight() {
        let mut bs: Vec<BackendState> = ["a", "b"]
            .iter()
            .map(|n| BackendState::new(n, 4, None, vec![], 0))
            .collect();
        let mut map = PrefixMap::new(&[4, 4], 0);
        map.note(1, &chain(&[1, 2, 3]), 0);
        let cost = |w| Warmth {
            mode: Affinity::Prefix,
            map: Some(&map),
            min_tokens: 64,
            load_weight: Some(w),
            ..Warmth::RUN
        };
        // 4000 tokens, 3000 of them held by b: a would read 4000, b 1000.
        let it = Item {
            prefix: Some(chain(&[1, 2, 3, 4])),
            tokens: 4000,
            ..Item::default()
        };
        let go = |bs: &[BackendState], w: f64| {
            route(bs, &it, 0, 0.5, &cost(w), &|_| false).map(|r| r.backend)
        };
        assert_eq!(go(&bs, 1.0), Some(1));
        // b already has 8000 tokens in flight on its four seats (2000 per
        // seat): 1000 + 2000 is still less than 4000.
        bs[1].inflight = 2;
        bs[1].tokens = 8000.0;
        assert_eq!(go(&bs, 1.0), Some(1));
        // At 16000 in flight it is not, unless load is weighted down.
        bs[1].tokens = 16_000.0;
        assert_eq!(go(&bs, 1.0), Some(0));
        assert_eq!(go(&bs, 0.5), Some(1));
        // A prompt nobody holds goes where fewer tokens are in flight.
        let cold = Item {
            prefix: Some(chain(&[9])),
            tokens: 1000,
            ..Item::default()
        };
        bs[0].inflight = 1;
        bs[0].tokens = 20_000.0;
        assert_eq!(
            route(&bs, &cold, 0, 0.5, &cost(1.0), &|_| false).map(|r| r.backend),
            Some(1)
        );
    }

    #[test]
    fn a_new_prompt_waits_rather_than_push_out_one_still_in_use() {
        let bs: Vec<BackendState> = ["a", "b"]
            .iter()
            .map(|n| BackendState::new(n, 1, None, vec![], 0))
            .collect();
        let mut map = PrefixMap::new(&[1, 1], 0);
        map.note(0, &chain(&[1]), 1000);
        map.note(1, &chain(&[2]), 4000);
        let it = |blocks: &[u64], enq_ms| Item {
            prefix: Some(chain(blocks)),
            enq_ms,
            ..Item::default()
        };
        let go = |map: &PrefixMap, it: &Item, now| {
            let w = Warmth {
                mode: Affinity::Prefix,
                map: Some(map),
                min_tokens: 64,
                slot_hold_ms: 5000,
                slot_max_wait_ms: 8000,
                ..Warmth::RUN
            };
            route(&bs, it, now, 0.5, &w, &|_| false).map(|r| r.backend)
        };
        // Both servers are free, but both prompts were used in the last 5 s.
        assert_eq!(go(&map, &it(&[9], 5000), 5000), None);
        // A prompt one of them holds is not held back.
        assert_eq!(go(&map, &it(&[2, 3], 5000), 5000), Some(1));
        // Server a's prompt has now been idle for 5 s: its slot may go.
        assert_eq!(go(&map, &it(&[9], 5000), 6000), Some(0));
        // And nobody waits longer than the limit.
        map.note(0, &chain(&[1]), 12_000);
        map.note(1, &chain(&[2]), 12_000);
        assert_eq!(go(&map, &it(&[9], 5000), 12_500), None);
        assert_eq!(go(&map, &it(&[9], 5000), 13_000), Some(0));
    }

    #[test]
    fn a_server_with_nothing_finishing_takes_no_more_work() {
        let mut b = BackendState::new("a", 4, None, vec![], 0);
        b.stale_ms = 1000;
        assert!(b.free(5000), "idle is not stale");
        b.start(5000);
        assert!(b.free(5900));
        assert!(!b.free(6100) && b.stale(6100));
        b.start(5500);
        b.finish(6200, 10.0, 100.0, 50.0, true);
        assert!(b.free(6300), "something finished: it is alive");
    }
}
