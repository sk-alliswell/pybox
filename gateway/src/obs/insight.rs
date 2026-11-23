//! Who spends what, what comes out of it, and what to change.
//!
//! One pass over request rows plus the deliverable metadata gives three
//! views, each ending in suggestions that carry the numbers they rest on:
//!
//! * person: spend, accepted work, where money and time are lost;
//! * team: the same added up, plus concentration, spread and peak overlap;
//! * serving: per server and per hour of day, load against capacity.
//!
//! Spend is the billed `cost_usd` when rows carry one. Self-hosted models
//! bill nothing per request, so with `server_hour_usd` set each request is
//! charged for the server time it held (`total_ms - queue_ms`, divided by the
//! server's slots) and the rest of the fleet's price shows up as idle cost.
//!
//! A deliverable's value is split between the people who spent on it, in
//! proportion to that spend. Every threshold lives in [`limits`] and is
//! returned with the report so the page can show what a rule compared.

use std::collections::{BTreeMap, HashMap, HashSet};

use serde::Serialize;
use serde_json::{json, Value};

use super::roi::{Deliverable, Link, Rate};
use super::store::{Backend, Table};

/// Thresholds the rules compare against. Fixed guesses, not fitted.
pub mod limits {
    /// Below this many requests a person or team gets numbers but no advice.
    pub const MIN_REQUESTS: u64 = 20;
    /// An hour is pressed when queue wait p95 passes this, or refusals pass
    /// `PRESSED_REFUSED`, or offered work passes capacity.
    pub const PRESSED_QUEUE_P95_MS: f64 = 2000.0;
    pub const PRESSED_REFUSED: f64 = 0.01;
    /// An hour is idle when offered work is under this share of capacity.
    pub const IDLE_BUSY: f64 = 0.30;
    /// Capacity suggestions aim for this share at the busiest hour.
    pub const TARGET_BUSY: f64 = 0.80;
    /// Cache reuse under this fraction of the comparison group's is low.
    pub const CACHE_GAP: f64 = 0.6;
    /// Median input per request above this multiple of the team's is long.
    pub const CONTEXT_FACTOR: f64 = 2.0;
    pub const WASTE_SHARE: f64 = 0.15;
    /// A small request: at most this much output and input.
    pub const SMALL_OUT: f64 = 300.0;
    pub const SMALL_IN: f64 = 2000.0;
    /// A model counts as expensive at this multiple of the cheapest one.
    pub const PRICE_FACTOR: f64 = 3.0;
    pub const SMALL_ON_BIG_SHARE: f64 = 0.20;
    pub const UNATTRIBUTED_SHARE: f64 = 0.5;
    pub const BATCH_PEAK_SHARE: f64 = 0.3;
    pub const REJECTED_SHARE: f64 = 0.25;
    pub const TOP_USER_SHARE: f64 = 0.5;
    pub const SPREAD: f64 = 3.0;
    /// A first token later than this is slow for someone waiting.
    pub const SLOW_FIRST_TOKEN_MS: f64 = 10_000.0;
    pub const SLOW_SHARE: f64 = 0.10;
    pub const IMBALANCE: f64 = 0.3;
    pub const SPEED_DROP: f64 = 0.7;
    pub const ERROR_RATE: f64 = 0.05;
    pub const STALL_MS: f64 = 5000.0;
    pub const SLOW_PROMPT_MS: f64 = 3000.0;
}

const DAY_MS: i64 = 86_400_000;

#[derive(Debug, Clone)]
pub struct Options {
    /// Offset of local time from UTC, in hours, for the hour-of-day view.
    pub tz_hours: f64,
    /// Length of a day in seconds. Lab runs squeeze a day into minutes;
    /// with a short day, hours count from the start of the window.
    pub day_s: f64,
    /// Price of one model server for an hour. Used when rows carry no cost.
    pub server_hour_usd: Option<f64>,
    /// Requests a server works on at once. Default: the most seen at once.
    pub slots: Option<f64>,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            tz_hours: 9.0,
            day_s: 86_400.0,
            server_hour_usd: None,
            slots: None,
        }
    }
}

#[derive(Serialize, Clone, Debug)]
pub struct Suggestion {
    /// person | team | serving | setup
    pub scope: &'static str,
    pub target: String,
    pub team: String,
    pub id: &'static str,
    pub title: String,
    /// What was seen, with the numbers compared.
    pub detail: String,
    pub action: String,
    /// Spend in this window the change could have kept, when it can be put
    /// in dollars. Negative for a suggestion that costs money.
    pub saving_usd: Option<f64>,
    /// measured: the saving is spend already seen. estimate: it assumes the
    /// change reaches the comparison level.
    pub basis: &'static str,
    /// Orders suggestions that have no dollar figure (0 to 1).
    pub weight: f64,
}

struct R {
    ts: i64,
    user: String,
    team: String,
    session: String,
    model: String,
    batch: bool,
    backend: String,
    failed: bool,
    error: bool,
    refused: bool,
    queue: f64,
    ttft: Option<f64>,
    total: f64,
    input: f64,
    output: f64,
    cached: f64,
    cost: f64,
    tps: Option<f64>,
    gap: Option<f64>,
    /// (deliverable, share of this request)
    targets: Vec<(String, f64)>,
}

impl R {
    fn service(&self) -> f64 {
        (self.total - self.queue).max(0.0)
    }
    fn small(&self) -> bool {
        self.output <= limits::SMALL_OUT && self.input <= limits::SMALL_IN
    }
}

fn text(v: Option<&Value>) -> String {
    v.and_then(Value::as_str).unwrap_or("").to_string()
}
fn num(v: Option<&Value>) -> f64 {
    v.and_then(Value::as_f64).unwrap_or(0.0)
}
fn flag(v: Option<&Value>) -> bool {
    v.and_then(Value::as_bool).unwrap_or(false)
}
fn r2(x: f64) -> f64 {
    (x * 100.0).round() / 100.0
}
fn r4(x: f64) -> f64 {
    (x * 10_000.0).round() / 10_000.0
}

fn quantile(v: &mut [f64], q: f64) -> Option<f64> {
    if v.is_empty() {
        return None;
    }
    v.sort_by(|a, b| a.total_cmp(b));
    let i = ((v.len() - 1) as f64 * q).round() as usize;
    Some(v[i])
}

fn median_of(mut v: Vec<f64>) -> Option<f64> {
    quantile(&mut v, 0.5)
}

// Text helpers for the Korean sentences.
fn pc(x: f64) -> String {
    format!("{:.0}%", x * 100.0)
}
fn commas(x: f64) -> String {
    let s = format!("{:.0}", x.abs());
    let mut out = String::new();
    for (i, c) in s.chars().enumerate() {
        if i > 0 && (s.len() - i) % 3 == 0 {
            out.push(',');
        }
        out.push(c);
    }
    if x < 0.0 {
        format!("-{out}")
    } else {
        out
    }
}
fn usd(x: f64) -> String {
    if x.abs() >= 100.0 {
        format!("${}", commas(x))
    } else {
        format!("${x:.2}")
    }
}
fn secs(ms: f64) -> String {
    format!("{:.1}초", ms / 1000.0)
}
fn hour_ranges(hours: &[usize]) -> String {
    let mut out: Vec<String> = Vec::new();
    let mut i = 0;
    while i < hours.len() {
        let mut j = i;
        while j + 1 < hours.len() && hours[j + 1] == hours[j] + 1 {
            j += 1;
        }
        out.push(format!("{}~{}시", hours[i], hours[j] + 1));
        i = j + 1;
    }
    out.join(", ")
}

#[derive(Default)]
struct Agg {
    requests: u64,
    errors: u64,
    aborts: u64,
    refused: u64,
    cost: f64,
    fail_cost: f64,
    no_target_cost: f64,
    service_ms: f64,
    input: f64,
    output: f64,
    cached: f64,
    inputs: Vec<f64>,
    waits: Vec<f64>,
    slow: u64,
    sessions: HashSet<String>,
    active: HashSet<i64>,
    batch: u64,
    batch_pressed: u64,
    pressed: u64,
    small_big_cost: f64,
    small_big_saving: f64,
    small_big_n: u64,
    by_deliverable: HashMap<String, f64>,
    by_user: HashMap<String, f64>,
}

struct Ctx<'a> {
    pressed: [bool; 24],
    /// Spend per token by model, where spend is known.
    price: HashMap<String, f64>,
    cheapest: Option<(String, f64)>,
    hour_ms: i64,
    hour_of: &'a dyn Fn(i64) -> usize,
}

impl Agg {
    fn add(&mut self, r: &R, cx: &Ctx) {
        self.requests += 1;
        self.errors += u64::from(r.error);
        self.aborts += u64::from(r.failed && !r.error);
        self.refused += u64::from(r.refused);
        self.cost += r.cost;
        self.service_ms += r.service();
        self.input += r.input;
        self.output += r.output;
        self.cached += r.cached;
        if !r.failed {
            self.inputs.push(r.input);
        }
        if !r.session.is_empty() {
            self.sessions.insert(r.session.clone());
        }
        self.active.insert(r.ts.div_euclid(cx.hour_ms));
        let pressed = cx.pressed[(cx.hour_of)(r.ts)];
        self.pressed += u64::from(pressed);
        if r.batch {
            self.batch += 1;
            self.batch_pressed += u64::from(pressed);
        } else if !r.refused {
            // Someone is waiting for this one.
            let wait = r.ttft.unwrap_or(r.queue);
            self.waits.push(wait);
            self.slow += u64::from(wait > limits::SLOW_FIRST_TOKEN_MS);
        }
        // Spend lands in exactly one of: failed, a deliverable, nobody's.
        if r.failed {
            self.fail_cost += r.cost;
        } else if r.targets.is_empty() {
            self.no_target_cost += r.cost;
        } else {
            for (d, share) in &r.targets {
                *self.by_deliverable.entry(d.clone()).or_default() += r.cost * share;
            }
        }
        *self.by_user.entry(r.user.clone()).or_default() += r.cost;
        if let (Some((_, low)), Some(p)) = (&cx.cheapest, cx.price.get(&r.model)) {
            if r.small() && !r.failed && *p >= low * limits::PRICE_FACTOR {
                self.small_big_cost += r.cost;
                self.small_big_saving += r.cost * (1.0 - low / p);
                self.small_big_n += 1;
            }
        }
    }
    fn cache_share(&self) -> f64 {
        self.cached / self.input.max(1.0)
    }
    fn fail_rate(&self) -> f64 {
        (self.errors + self.aborts) as f64 / self.requests.max(1) as f64
    }
}

struct Meta {
    deliverables: BTreeMap<String, Deliverable>,
    links: HashMap<String, Vec<(String, f64)>>,
    rates: HashMap<String, f64>,
}

fn load_meta(backend: &dyn Backend) -> Result<Meta, String> {
    let deliverables = backend
        .meta_list("deliverable")?
        .into_iter()
        .filter_map(|(id, v)| Some((id, serde_json::from_value(v).ok()?)))
        .collect();
    let mut links: HashMap<String, Vec<(String, f64)>> = HashMap::new();
    for (_, v) in backend.meta_list("link")? {
        if let Ok(l) = serde_json::from_value::<Link>(v) {
            if l.weight > 0.0 {
                links
                    .entry(l.run_id)
                    .or_default()
                    .push((l.deliverable_id, l.weight));
            }
        }
    }
    let rates = backend
        .meta_list("rate")?
        .into_iter()
        .filter_map(|(id, v)| Some((id, serde_json::from_value::<Rate>(v).ok()?.hourly_usd)))
        .collect();
    Ok(Meta {
        deliverables,
        links,
        rates,
    })
}

impl Meta {
    fn rate(&self, team: &str) -> Option<f64> {
        self.rates
            .get(team)
            .or_else(|| self.rates.get("*"))
            .copied()
    }
    fn value(&self, d: &Deliverable) -> f64 {
        if d.status != "accepted" {
            return 0.0;
        }
        d.value_usd
            .or_else(|| Some(d.hours_saved? * self.rate(&d.team_id)?))
            .unwrap_or(0.0)
    }
}

/// What a group's spend on deliverables came to.
#[derive(Default)]
struct Outcome {
    value: f64,
    accepted: f64,
    touched: usize,
    useful: f64,
    rejected: f64,
    unknown: f64,
}

fn outcome(a: &Agg, meta: &Meta, all: &HashMap<String, f64>) -> Outcome {
    let mut o = Outcome::default();
    for (id, mine) in &a.by_deliverable {
        let Some(d) = meta.deliverables.get(id) else {
            o.unknown += mine;
            continue;
        };
        o.touched += 1;
        let share = mine / all.get(id).copied().unwrap_or(0.0).max(f64::MIN_POSITIVE);
        match d.status.as_str() {
            "rejected" | "abandoned" => o.rejected += mine,
            "accepted" => {
                o.useful += mine;
                o.accepted += share;
                o.value += meta.value(d) * share;
            }
            _ => o.useful += mine,
        }
    }
    o
}

const COLS: [&str; 28] = [
    "team_id",
    "user_id",
    "claimed_user_id",
    "agent_id",
    "session_id",
    "run_id",
    "deliverable_id",
    "model",
    "priority",
    "backend",
    "upstream_ip",
    "status",
    "is_error",
    "client_abort",
    "queue_ms",
    "ttft_ms",
    "total_ms",
    "input_tokens",
    "output_tokens",
    "cache_read_tokens",
    "prefix_warm_tokens",
    "cost_usd",
    "output_tps",
    "max_chunk_gap_ms",
    "gen_ms",
    "kind",
    "user",
    "team",
];

fn read_rows(backend: &dyn Backend, from: i64, to: i64, meta: &Meta) -> Result<Vec<R>, String> {
    let mut rows = Vec::new();
    backend.scan_cols(Table::Requests, from, to, &COLS, &mut |row| {
        let first = |keys: &[&str]| {
            keys.iter()
                .map(|k| text(row.get(*k)))
                .find(|s| !s.is_empty())
                .unwrap_or_default()
        };
        let status = num(row.get("status")) as i64;
        let error = flag(row.get("is_error")) || status >= 500 || status == 429;
        let abort = flag(row.get("client_abort"));
        let direct = text(row.get("deliverable_id"));
        let targets = if !direct.is_empty() {
            vec![(direct, 1.0)]
        } else {
            let ls = meta
                .links
                .get(&text(row.get("run_id")))
                .cloned()
                .unwrap_or_default();
            let sum: f64 = ls.iter().map(|l| l.1).sum();
            ls.into_iter().map(|(d, w)| (d, w / sum)).collect()
        };
        let pos = |k: &str| row.get(k).and_then(Value::as_f64).filter(|x| *x > 0.0);
        rows.push(R {
            ts: row.get("ts").and_then(Value::as_i64).unwrap_or(0),
            user: first(&["user_id", "claimed_user_id", "user", "agent_id"]),
            team: first(&["team_id", "team"]),
            session: text(row.get("session_id")),
            model: text(row.get("model")),
            batch: first(&["priority", "kind"]) == "batch",
            backend: first(&["backend", "upstream_ip"]),
            failed: error || abort,
            error,
            refused: status == 429 || status == 503,
            queue: num(row.get("queue_ms")),
            ttft: pos("ttft_ms"),
            total: num(row.get("total_ms")),
            input: num(row.get("input_tokens")),
            output: num(row.get("output_tokens")),
            // Local servers report no cached tokens; the gateway's own count
            // of the prefix the chosen server already held stands in.
            cached: num(row.get("cache_read_tokens")).max(num(row.get("prefix_warm_tokens"))),
            cost: num(row.get("cost_usd")),
            tps: pos("output_tps"),
            gap: pos("max_chunk_gap_ms"),
            targets,
        });
    })?;
    rows.sort_by_key(|r| r.ts);
    Ok(rows)
}

/// Most requests a backend worked on at once.
fn peak_concurrency(rows: &[R]) -> HashMap<String, f64> {
    let mut events: HashMap<&str, Vec<(i64, i32)>> = HashMap::new();
    for r in rows.iter().filter(|r| !r.backend.is_empty()) {
        let start = r.ts + r.queue as i64;
        let end = (r.ts + r.total as i64).max(start + 1);
        let e = events.entry(&r.backend).or_default();
        e.push((start, 1));
        e.push((end, -1));
    }
    events
        .into_iter()
        .map(|(b, mut e)| {
            e.sort();
            let (mut now, mut top) = (0, 0);
            for (_, d) in e {
                now += d;
                top = top.max(now);
            }
            (b.to_string(), f64::from(top.max(1)))
        })
        .collect()
}

#[derive(Default)]
struct Hour {
    requests: u64,
    batch: u64,
    refused: u64,
    offered_ms: f64,
    queue: Vec<f64>,
    tps: Vec<f64>,
}

#[derive(Default)]
struct Server {
    requests: u64,
    errors: u64,
    service_ms: f64,
    input: f64,
    cached: f64,
    queue: Vec<f64>,
    prompt: Vec<f64>,
    tps: Vec<f64>,
    gap: Vec<f64>,
    models: HashSet<String>,
}

pub fn report(backend: &dyn Backend, from: i64, to: i64, opts: &Options) -> Result<Value, String> {
    let meta = load_meta(backend)?;
    let mut rows = read_rows(backend, from, to, &meta)?;
    let window_ms = (to - from).max(1);
    let day_ms = ((opts.day_s * 1000.0) as i64).max(24);
    let hour_ms = (day_ms / 24).max(1);
    let real_day = day_ms == DAY_MS;
    let tz_ms = (opts.tz_hours * 3_600_000.0) as i64;
    let hour_of = move |ts: i64| -> usize {
        let t = if real_day { ts + tz_ms } else { ts - from };
        (t.rem_euclid(day_ms) / hour_ms).min(23) as usize
    };
    // How many times each hour of the day falls inside the window.
    let mut occ = [0.0f64; 24];
    let steps = window_ms / hour_ms + 1;
    if steps <= 24 * 400 {
        let mut t = from;
        while t < to {
            // Step to the next hour boundary so partial hours count partly.
            let into = if real_day { t + tz_ms } else { t - from }.rem_euclid(hour_ms);
            let next = (t + hour_ms - into).min(to);
            occ[hour_of(t)] += (next - t) as f64 / hour_ms as f64;
            t = next;
        }
    } else {
        occ = [window_ms as f64 / day_ms as f64; 24];
    }

    // ----- serving: capacity and load -----
    let seen = peak_concurrency(&rows);
    let slots_of = |b: &str| {
        opts.slots
            .unwrap_or_else(|| seen.get(b).copied().unwrap_or(1.0))
    };
    let mut servers: BTreeMap<String, Server> = BTreeMap::new();
    let mut hours: Vec<Hour> = (0..24).map(|_| Hour::default()).collect();
    for r in &rows {
        let h = &mut hours[hour_of(r.ts)];
        h.requests += 1;
        h.batch += u64::from(r.batch);
        h.refused += u64::from(r.refused);
        h.offered_ms += r.service();
        h.queue.push(r.queue);
        if let Some(t) = r.tps {
            h.tps.push(t);
        }
        if r.backend.is_empty() {
            continue;
        }
        let s = servers.entry(r.backend.clone()).or_default();
        s.requests += 1;
        s.errors += u64::from(r.error);
        s.service_ms += r.service();
        s.input += r.input;
        s.cached += r.cached;
        s.queue.push(r.queue);
        if let Some(t) = r.ttft {
            s.prompt.push((t - r.queue).max(0.0));
        }
        if let Some(t) = r.tps {
            s.tps.push(t);
        }
        if let Some(g) = r.gap {
            s.gap.push(g);
        }
        s.models.insert(r.model.clone());
    }
    let total_slots: f64 = servers.keys().map(|b| slots_of(b)).sum::<f64>().max(1.0);
    let slots_per_server = total_slots / servers.len().max(1) as f64;

    // ----- spend basis -----
    let billed: f64 = rows.iter().map(|r| r.cost).sum();
    let basis = match (billed > 0.0, opts.server_hour_usd) {
        (true, _) => "billed",
        (false, Some(_)) => "server_time",
        _ => "none",
    };
    let mut fleet_cost = None;
    if basis == "server_time" {
        let price = opts.server_hour_usd.unwrap_or(0.0);
        for r in rows.iter_mut().filter(|r| !r.backend.is_empty()) {
            let slots = opts
                .slots
                .unwrap_or_else(|| seen.get(&r.backend).copied().unwrap_or(1.0));
            r.cost = r.service() / 3.6e6 * price / slots;
        }
        fleet_cost = Some(servers.len() as f64 * window_ms as f64 / 3.6e6 * price);
    } else if basis == "none" {
        // No money at all: shares still work on server time.
        for r in rows.iter_mut() {
            r.cost = r.service() / 1000.0;
        }
    }
    let money = basis != "none";

    let mut hour_rows = Vec::new();
    let mut pressed = [false; 24];
    let mut idle = [false; 24];
    let mut hour_util = [0.0f64; 24];
    for (i, h) in hours.iter_mut().enumerate() {
        let capacity = total_slots * hour_ms as f64 * occ[i];
        let util = if capacity > 0.0 {
            h.offered_ms / capacity
        } else {
            0.0
        };
        let q95 = quantile(&mut h.queue, 0.95).unwrap_or(0.0);
        let refused = h.refused as f64 / h.requests.max(1) as f64;
        hour_util[i] = util;
        pressed[i] = h.requests > 0
            && (q95 > limits::PRESSED_QUEUE_P95_MS
                || refused > limits::PRESSED_REFUSED
                || util > 1.0);
        idle[i] = occ[i] > 0.0 && util < limits::IDLE_BUSY && !pressed[i];
        hour_rows.push(json!({
            "hour": i, "requests": h.requests, "batch": h.batch, "util": r4(util),
            "queue_p95_ms": r2(q95), "refused": r4(refused),
            "tps_p50": quantile(&mut h.tps, 0.5).map(r2),
            "state": if pressed[i] { "pressed" } else if idle[i] { "idle" } else { "normal" },
        }));
    }

    // ----- model prices -----
    let mut per_model: BTreeMap<String, (u64, f64, f64, f64, f64)> = BTreeMap::new();
    for r in &rows {
        let m = per_model.entry(r.model.clone()).or_default();
        m.0 += 1;
        m.1 += r.cost;
        m.2 += r.input + r.output;
        m.3 += u64::from(r.small() && !r.failed) as f64;
        m.4 += r.service();
    }
    let price: HashMap<String, f64> = per_model
        .iter()
        .filter(|(_, m)| basis == "billed" && m.1 > 0.0 && m.2 > 0.0)
        .map(|(k, m)| (k.clone(), m.1 / m.2))
        .collect();
    let cheapest = price
        .iter()
        .min_by(|a, b| a.1.total_cmp(b.1))
        .map(|(k, v)| (k.clone(), *v))
        .filter(|_| price.len() >= 2);
    let cx = Ctx {
        pressed,
        price,
        cheapest,
        hour_ms,
        hour_of: &hour_of,
    };

    // ----- people and teams -----
    let mut people: BTreeMap<(String, String), Agg> = BTreeMap::new();
    let mut teams: BTreeMap<String, Agg> = BTreeMap::new();
    let mut total = Agg::default();
    for r in &rows {
        people
            .entry((r.team.clone(), r.user.clone()))
            .or_default()
            .add(r, &cx);
        teams.entry(r.team.clone()).or_default().add(r, &cx);
        total.add(r, &cx);
    }
    let all_d = total.by_deliverable.clone();
    let have_deliverables = !meta.deliverables.is_empty();
    let pressed_hours: Vec<usize> = (0..24).filter(|i| pressed[*i]).collect();
    let idle_hours: Vec<usize> = (0..24).filter(|i| idle[*i]).collect();
    let mut advice: Vec<Suggestion> = Vec::new();

    // Shared numbers for a person, a team or everything.
    let base = |a: &mut Agg, o: &Outcome, team: &str| -> Value {
        let cost = a.cost.max(f64::MIN_POSITIVE);
        let waste = a.fail_cost + o.rejected;
        let nobody = a.no_target_cost + o.unknown;
        let wait_h = a.waits.iter().sum::<f64>() / 3.6e6;
        let interactive = a.waits.len().max(1) as f64;
        json!({
            "requests": a.requests,
            "few": a.requests < limits::MIN_REQUESTS,
            "cost_usd": money.then(|| r4(a.cost)),
            "server_s": r2(a.service_ms / 1000.0),
            "input_tokens": a.input, "output_tokens": a.output,
            "sessions": a.sessions.len(), "active_hours": a.active.len(),
            "cache_share": r4(a.cache_share()),
            "input_p50": quantile(&mut a.inputs, 0.5),
            "fail_rate": r4(a.fail_rate()),
            "wait_p50_ms": quantile(&mut a.waits, 0.5).map(r2),
            "wait_p95_ms": quantile(&mut a.waits, 0.95).map(r2),
            "wait_hours": r4(wait_h),
            "wait_cost_usd": meta.rate(team).map(|r| r2(wait_h * r)),
            "slow_share": r4(a.slow as f64 / interactive),
            "batch": a.batch, "batch_pressed": a.batch_pressed,
            "pressed_share": r4(a.pressed as f64 / a.requests.max(1) as f64),
            "deliverables": o.touched,
            "accepted": r2(o.accepted),
            "value_usd": r2(o.value),
            "roi": (money && a.cost > 0.0 && have_deliverables).then(|| r2((o.value - a.cost) / a.cost)),
            "cost_per_accepted": (money && o.accepted >= 0.05).then(|| r2(a.cost / o.accepted)),
            "useful_share": r4(o.useful / cost),
            "waste_share": r4(waste / cost),
            "unattributed_share": r4(nobody / cost),
            "waste_usd": money.then(|| r4(waste)),
            "small_on_big_usd": money.then(|| r4(a.small_big_cost)),
        })
    };

    // ----- person rules -----
    let mut people_out = Vec::new();
    let mut per_team_cpa: HashMap<String, Vec<(String, f64)>> = HashMap::new();
    for ((team, user), a) in people.iter_mut() {
        let o = outcome(a, &meta, &all_d);
        let t = &teams[team];
        let team_input_p50 = median_of(t.inputs.clone());
        let mut v = base(a, &o, team);
        let mut mine: Vec<Suggestion> = Vec::new();
        let mut push = |id,
                        title: String,
                        detail: String,
                        action: &str,
                        saving: Option<f64>,
                        basis,
                        weight: f64| {
            mine.push(Suggestion {
                scope: "person",
                target: user.clone(),
                team: team.clone(),
                id,
                title,
                detail,
                action: action.to_string(),
                saving_usd: saving.filter(|s| money && *s > 0.0).map(r2),
                basis,
                weight,
            });
        };
        let in_share = a.input / (a.input + a.output).max(1.0);
        if a.requests >= limits::MIN_REQUESTS {
            let (own, grp) = (a.cache_share(), t.cache_share());
            if grp >= 0.1 && own < grp * limits::CACHE_GAP {
                push(
                    "cache_low",
                    "프롬프트 앞부분이 재사용되지 않습니다".into(),
                    format!("입력 토큰 중 캐시로 다시 읽은 비율이 {}입니다. 팀 전체는 {}입니다.", pc(own), pc(grp)),
                    "시스템 프롬프트와 도구 정의를 요청마다 같은 순서로 맨 앞에 두고, 시각이나 난수처럼 매번 바뀌는 값은 뒤로 옮기세요.",
                    Some(a.cost * in_share * (grp - own) * 0.9),
                    "estimate",
                    (grp - own).min(1.0),
                );
            }
            if let (Some(own), Some(grp)) = (median_of(a.inputs.clone()), team_input_p50) {
                if grp > 0.0 && own > grp * limits::CONTEXT_FACTOR {
                    push(
                        "context_long",
                        "요청 하나에 싣는 입력이 깁니다".into(),
                        format!("요청당 입력 토큰 중앙값이 {}개입니다. 팀 중앙값은 {}개입니다.", commas(own), commas(grp)),
                        "세션이 길어지면 요약하고 새 세션으로 넘어가고, 지난 도구 결과는 빼고 보내세요. 게이트웨이의 도구 결과 정리(tool_results)를 켜는 방법도 있습니다.",
                        Some(a.cost * in_share * (1.0 - grp / own)),
                        "estimate",
                        (1.0 - grp / own).min(1.0),
                    );
                }
            }
            if a.fail_rate() > limits::WASTE_SHARE {
                push(
                    "failed_requests",
                    "끝까지 받지 못한 요청이 많습니다".into(),
                    format!(
                        "요청 {}건 중 오류 {}건, 중간에 끊은 요청 {}건으로 {}입니다.",
                        commas(a.requests as f64), a.errors, a.aborts, pc(a.fail_rate())
                    ),
                    "끊은 요청이 많으면 대기 시간이나 max_tokens를 확인하고, 오류가 많으면 같은 요청을 되풀이하는 재시도 루프가 없는지 보세요.",
                    Some(a.fail_cost),
                    "measured",
                    a.fail_rate(),
                );
            }
            if money && a.small_big_cost > a.cost * limits::SMALL_ON_BIG_SHARE {
                let low = cx.cheapest.as_ref().map(|c| c.0.as_str()).unwrap_or("");
                push(
                    "small_on_big",
                    "짧은 요청을 비싼 모델로 보내고 있습니다".into(),
                    format!(
                        "입력 {}토큰 이하, 출력 {}토큰 이하인 요청 {}건이 비싼 모델로 갔고, 쓴 비용의 {}({})입니다.",
                        commas(limits::SMALL_IN), commas(limits::SMALL_OUT), a.small_big_n,
                        pc(a.small_big_cost / a.cost), usd(a.small_big_cost)
                    ),
                    &format!("자동 완성, 분류, 짧은 질문은 {low} 같은 작은 모델로 보내세요. 답의 품질이 유지되는지는 표본으로 확인해야 합니다."),
                    Some(a.small_big_saving),
                    "estimate",
                    a.small_big_cost / a.cost,
                );
            }
            let nobody = (a.no_target_cost + o.unknown) / a.cost.max(f64::MIN_POSITIVE);
            if have_deliverables && nobody > limits::UNATTRIBUTED_SHARE {
                push(
                    "unattributed",
                    "어떤 산출물을 위한 사용인지 알 수 없습니다".into(),
                    format!("쓴 비용의 {}가 산출물과 연결되지 않았습니다.", pc(nobody)),
                    "에이전트 런타임이 x-gateway-run-id와 x-gateway-deliverable-id 헤더를 보내도록 설정하세요. 연결되지 않은 사용은 성과로 잡히지 않습니다.",
                    None,
                    "measured",
                    nobody,
                );
            }
            if o.rejected > a.cost * limits::REJECTED_SHARE {
                push(
                    "rejected_work",
                    "반려되거나 버려진 산출물에 쓴 비중이 큽니다".into(),
                    format!("쓴 비용의 {}({})가 반려 또는 중단된 산출물로 갔습니다.", pc(o.rejected / a.cost), usd(o.rejected)),
                    "작업을 크게 돌리기 전에 범위와 기준을 먼저 확인받고, 작은 단위로 나눠 중간 검토를 받으세요.",
                    Some(o.rejected),
                    "measured",
                    o.rejected / a.cost,
                );
            }
            if a.batch_pressed >= 5
                && a.batch_pressed as f64 > a.batch as f64 * limits::BATCH_PEAK_SHARE
                && !idle_hours.is_empty()
            {
                push(
                    "batch_at_peak",
                    "급하지 않은 일괄 작업이 붐비는 시간에 돌고 있습니다".into(),
                    format!(
                        "batch 요청 {}건 중 {}건이 대기가 길어지는 시간대({})에 들어왔습니다.",
                        a.batch, a.batch_pressed, hour_ranges(&pressed_hours)
                    ),
                    &format!("이 작업을 한가한 시간대({})로 예약하세요. 본인과 동료의 대화형 요청 대기가 함께 줄어듭니다.", hour_ranges(&idle_hours)),
                    None,
                    "measured",
                    a.batch_pressed as f64 / a.batch.max(1) as f64 * 0.5,
                );
            }
        }
        if let Some(c) = v["cost_per_accepted"].as_f64() {
            if a.requests >= limits::MIN_REQUESTS && o.accepted >= 0.5 {
                per_team_cpa
                    .entry(team.clone())
                    .or_default()
                    .push((user.clone(), c));
            }
        }
        let obj = v.as_object_mut().unwrap();
        obj.insert("user".into(), json!(user));
        obj.insert("team".into(), json!(team));
        obj.insert("team_cache_share".into(), json!(r4(t.cache_share())));
        obj.insert("team_input_p50".into(), json!(team_input_p50));
        obj.insert("suggestions".into(), json!(mine));
        advice.extend(mine);
        people_out.push(v);
    }

    // ----- team rules -----
    let mut teams_out = Vec::new();
    let team_cpa: Vec<f64> = teams
        .values()
        .filter_map(|a| {
            let o = outcome(a, &meta, &all_d);
            (o.accepted >= 0.5).then(|| a.cost / o.accepted)
        })
        .collect();
    let cpa_teams = team_cpa.len();
    let cpa_median = median_of(team_cpa);
    for (team, a) in teams.iter_mut() {
        let o = outcome(a, &meta, &all_d);
        let mut v = base(a, &o, team);
        let mut mine: Vec<Suggestion> = Vec::new();
        let mut push = |id,
                        title: String,
                        detail: String,
                        action: String,
                        saving: Option<f64>,
                        basis,
                        weight: f64| {
            mine.push(Suggestion {
                scope: "team",
                target: team.clone(),
                team: team.clone(),
                id,
                title,
                detail,
                action,
                saving_usd: saving.filter(|s| money && *s > 0.0).map(r2),
                basis,
                weight,
            });
        };
        let top = a
            .by_user
            .iter()
            .max_by(|x, y| x.1.total_cmp(y.1))
            .map(|(u, c)| (u.clone(), c / a.cost.max(f64::MIN_POSITIVE)));
        let users = a.by_user.len();
        if a.requests >= limits::MIN_REQUESTS {
            if let Some((u, share)) = &top {
                if users >= 3 && *share > limits::TOP_USER_SHARE {
                    push(
                        "concentrated",
                        "사용이 한 사람에게 몰려 있습니다".into(),
                        format!("{users}명 중 {u} 한 사람이 팀 사용량의 {}를 씁니다.", pc(*share)),
                        "이 사람의 작업이 자동화된 일괄 작업이면 batch로 표시해 뒤로 미루고, 붐빌 때 다른 팀원이 밀리지 않도록 사용자별 한도나 적게 쓴 사람 먼저(least_used) 줄 세우기를 켜세요.".into(),
                        None,
                        "measured",
                        *share * 0.6,
                    );
                }
            }
            let (roi, judged) = (v["roi"].as_f64(), o.touched >= 3);
            if let (Some(roi), true) = (roi, judged) {
                if roi < 0.0 {
                    push(
                        "roi_negative",
                        "쓴 비용이 인정된 가치보다 큽니다".into(),
                        format!("비용 {} 대비 승인된 산출물의 가치가 {}입니다.", usd(a.cost), usd(o.value)),
                        "가치가 등록되지 않은 산출물이 있는지 먼저 확인하고, 반려가 많은 작업 유형은 범위를 줄이거나 작은 모델로 바꾸세요.".into(),
                        None,
                        "measured",
                        0.7,
                    );
                }
            }
            if let (Some(c), Some(m)) = (v["cost_per_accepted"].as_f64(), cpa_median) {
                if cpa_teams >= 3 && c > m * 2.0 {
                    push(
                        "cost_per_accepted_high",
                        "승인된 산출물 하나에 드는 비용이 다른 팀보다 큽니다".into(),
                        format!("승인 1건당 {}입니다. 팀들의 중앙값은 {}입니다.", usd(c), usd(m)),
                        "산출물 종류가 달라서인지 먼저 보고, 같다면 승인당 비용이 낮은 팀의 프롬프트와 작업 단위를 가져오세요.".into(),
                        Some(a.cost - m * o.accepted),
                        "estimate",
                        0.5,
                    );
                }
            }
            let nobody = (a.no_target_cost + o.unknown) / a.cost.max(f64::MIN_POSITIVE);
            if have_deliverables && nobody > limits::UNATTRIBUTED_SHARE {
                push(
                    "unattributed",
                    "팀 사용량의 절반 이상이 산출물과 연결되지 않습니다".into(),
                    format!("비용의 {}가 어떤 산출물에도 연결되지 않아 ROI에 잡히지 않습니다.", pc(nobody)),
                    "팀이 쓰는 에이전트 런타임에서 실행 ID와 산출물 ID 헤더를 보내게 하고, PR 병합이나 리뷰 승인 시점에 산출물 상태를 등록하세요.".into(),
                    None,
                    "measured",
                    nobody,
                );
            }
            if a.fail_cost + o.rejected > a.cost * limits::REJECTED_SHARE {
                let w = a.fail_cost + o.rejected;
                push(
                    "waste_high",
                    "버려진 사용이 큽니다".into(),
                    format!(
                        "비용의 {}({})가 실패하거나 끊긴 요청({})과 반려 또는 중단된 산출물({})로 갔습니다.",
                        pc(w / a.cost), usd(w), usd(a.fail_cost), usd(o.rejected)
                    ),
                    "반려 사유를 유형별로 모아 가장 잦은 것부터 작업 지시서에 반영하세요.".into(),
                    Some(w),
                    "measured",
                    w / a.cost,
                );
            }
            let (own, all) = (a.cache_share(), total.cache_share());
            if all >= 0.1 && own < all * limits::CACHE_GAP {
                push(
                    "cache_low",
                    "팀의 프롬프트 재사용이 낮습니다".into(),
                    format!("캐시로 다시 읽은 입력 비율이 {}입니다. 전체는 {}입니다.", pc(own), pc(all)),
                    "팀 공용 시스템 프롬프트와 도구 정의를 한 벌로 맞추세요. 사람마다 조금씩 다른 머리말을 쓰면 서버가 앞부분을 다시 계산합니다.".into(),
                    Some(a.cost * (a.input / (a.input + a.output).max(1.0)) * (all - own) * 0.9),
                    "estimate",
                    all - own,
                );
            }
            let pressed_share = a.pressed as f64 / a.requests as f64;
            if !pressed_hours.is_empty()
                && a.batch_pressed >= 10
                && a.batch_pressed as f64 > a.batch as f64 * limits::BATCH_PEAK_SHARE
            {
                push(
                    "stagger",
                    "붐비는 시간대에 일괄 작업이 겹칩니다".into(),
                    format!(
                        "팀 요청의 {}가 대기가 길어지는 시간대({})에 들어오고, 그중 batch가 {}건입니다.",
                        pc(pressed_share), hour_ranges(&pressed_hours), a.batch_pressed
                    ),
                    if idle_hours.is_empty() {
                        "일괄 작업을 batch로 표시하고 게이트웨이의 hold_batch로 뒤로 미루세요.".to_string()
                    } else {
                        format!("일괄 작업을 한가한 시간대({})로 옮기세요. 서버를 늘리지 않고 대기를 줄이는 가장 싼 방법입니다.", hour_ranges(&idle_hours))
                    },
                    None,
                    "measured",
                    a.batch_pressed as f64 / a.requests as f64,
                );
            }
            let slow = a.slow as f64 / a.waits.len().max(1) as f64;
            if slow > limits::SLOW_SHARE {
                let wait_h = a.waits.iter().sum::<f64>() / 3.6e6;
                push(
                    "waiting",
                    "사람이 기다리는 시간이 깁니다".into(),
                    format!(
                        "대화형 요청의 {}가 첫 토큰까지 {} 넘게 걸렸고, 기다린 시간을 더하면 {:.1}시간입니다.",
                        pc(slow), secs(limits::SLOW_FIRST_TOKEN_MS), wait_h
                    ),
                    "모델 서빙 탭의 제안(서버 증설, 일괄 작업 미루기)이 이 팀에 가장 큰 효과를 냅니다.".into(),
                    meta.rate(team).map(|r| wait_h * r),
                    "estimate",
                    slow,
                );
            }
            if let Some(list) = per_team_cpa.get(team) {
                let lo = list.iter().min_by(|x, y| x.1.total_cmp(&y.1));
                let hi = list.iter().max_by(|x, y| x.1.total_cmp(&y.1));
                if let (Some(lo), Some(hi)) = (lo, hi) {
                    if list.len() >= 3 && hi.1 > lo.1 * limits::SPREAD {
                        push(
                            "spread",
                            "팀 안에서 승인당 비용 차이가 큽니다".into(),
                            format!("승인 1건당 비용이 {}는 {}, {}는 {}입니다.", lo.0, usd(lo.1), hi.0, usd(hi.1)),
                            format!("{}의 프롬프트, 모델 선택, 작업 단위를 팀에 공유하세요. 맡은 일의 난이도 차이일 수 있으니 순위로 쓰지는 마세요.", lo.0),
                            None,
                            "measured",
                            0.4,
                        );
                    }
                }
            }
        }
        let obj = v.as_object_mut().unwrap();
        obj.insert("team".into(), json!(team));
        obj.insert("users".into(), json!(users));
        obj.insert("top_user".into(), json!(top.as_ref().map(|t| t.0.clone())));
        obj.insert(
            "top_user_share".into(),
            json!(top.as_ref().map(|t| r4(t.1))),
        );
        obj.insert("rate_usd".into(), json!(meta.rate(team)));
        obj.insert("suggestions".into(), json!(mine));
        advice.extend(mine);
        teams_out.push(v);
    }

    // ----- serving rules -----
    let mut serving: Vec<Suggestion> = Vec::new();
    let mut push = |id,
                    title: &str,
                    detail: String,
                    action: String,
                    saving: Option<f64>,
                    basis,
                    weight: f64| {
        serving.push(Suggestion {
            scope: "serving",
            target: "모델 서빙".into(),
            team: String::new(),
            id,
            title: title.to_string(),
            detail,
            action,
            saving_usd: saving.filter(|s| money && *s > 0.0).map(r2),
            basis,
            weight,
        });
    };
    let n_servers = servers.len();
    let mut server_rows = Vec::new();
    // (models served, server, share of the window it worked)
    let mut busy: Vec<(String, String, f64)> = Vec::new();
    for (name, s) in servers.iter_mut() {
        let slots = slots_of(name);
        let share = s.service_ms / (slots * window_ms as f64);

        let err = s.errors as f64 / s.requests.max(1) as f64;
        let prompt_p50 = quantile(&mut s.prompt, 0.5);
        let gap_p95 = quantile(&mut s.gap, 0.95);
        let cache = s.cached / s.input.max(1.0);
        let mut models: Vec<&String> = s.models.iter().collect();
        models.sort();
        let key: Vec<&str> = models.iter().map(|m| m.as_str()).collect();
        busy.push((key.join(","), name.clone(), share));
        if s.requests >= limits::MIN_REQUESTS && err > limits::ERROR_RATE {
            push(
                "server_errors",
                "오류가 잦은 서버가 있습니다",
                format!("{name} 서버의 요청 {}건 중 {}가 오류로 끝났습니다.", s.requests, pc(err)),
                "이 서버의 메모리와 로그를 확인하고, 게이트웨이의 서버 상태 확인(health)이 이 서버를 빼고 있는지 보세요.".into(),
                None,
                "measured",
                err.min(1.0),
            );
        }
        if let Some(g) = gap_p95.filter(|g| *g > limits::STALL_MS) {
            push(
                "stalls",
                "답이 나오다 멈추는 구간이 깁니다",
                format!("{name} 서버에서 토큰 사이 가장 긴 멈춤의 p95가 {}입니다.", secs(g)),
                "한 서버에 동시에 넣는 요청 수를 줄이거나, 긴 프롬프트 처리가 생성 중인 요청을 막지 않도록 서버의 배치 설정을 확인하세요.".into(),
                None,
                "measured",
                0.4,
            );
        }
        server_rows.push(json!({
            "backend": name, "models": models, "requests": s.requests, "slots": slots,
            "busy_share": r4(share), "queue_p95_ms": quantile(&mut s.queue, 0.95).map(r2),
            "prompt_p50_ms": prompt_p50.map(r2), "tps_p50": quantile(&mut s.tps, 0.5).map(r2),
            "error_rate": r4(err), "cache_share": r4(cache), "stall_p95_ms": gap_p95.map(r2),
        }));
    }
    let peak = (0..24)
        .max_by(|a, b| hour_util[*a].total_cmp(&hour_util[*b]))
        .unwrap_or(0);
    let wait_all = total.waits.iter().sum::<f64>() / 3.6e6;
    let refused_all = total.refused as f64 / total.requests.max(1) as f64;
    let idle_slot_hours: f64 = idle_hours
        .iter()
        .map(|h| (1.0 - hour_util[*h]) * total_slots * occ[*h] * hour_ms as f64 / 3.6e6)
        .sum();
    let batch_pressed: u64 = pressed_hours.iter().map(|h| hours[*h].batch).sum();
    let reqs_pressed: u64 = pressed_hours.iter().map(|h| hours[*h].requests).sum();
    if !pressed_hours.is_empty() && n_servers > 0 {
        let need = (hour_util[peak] * n_servers as f64 / limits::TARGET_BUSY).ceil();
        let extra = need - n_servers as f64;
        let batch_share = batch_pressed as f64 / reqs_pressed.max(1) as f64;
        if batch_share >= 0.15 {
            push(
                "hold_batch",
                "붐비는 시간대의 일괄 작업을 뒤로 미루세요",
                format!(
                    "대기가 길어지는 시간대({}) 요청의 {}가 batch입니다. 같은 기간 한가한 시간대에 슬롯 기준 {:.0}시간이 비어 있습니다.",
                    hour_ranges(&pressed_hours), pc(batch_share), idle_slot_hours
                ),
                "게이트웨이에서 hold_batch = true로 batch를 붙잡아 두었다가 대화형 요청이 없을 때 보내세요. 서버를 늘리기 전에 해 볼 일입니다.".into(),
                None,
                "measured",
                0.8,
            );
        }
        if extra >= 1.0 {
            push(
                "add_servers",
                "가장 붐비는 시간에 서버가 모자랍니다",
                format!(
                    "{}시대에 들어온 일이 처리 능력의 {}입니다. 대기 p95는 {}입니다. 이 기간에 사람이 첫 토큰을 기다린 시간은 모두 {:.1}시간입니다{}.",
                    peak, pc(hour_util[peak]), secs(hour_rows[peak]["queue_p95_ms"].as_f64().unwrap_or(0.0)),
                    wait_all, meta.rate("*").map(|r| format!("(시간당 단가로 {})", usd(wait_all * r))).unwrap_or_default()
                ),
                format!(
                    "이 시간대 사용률을 {} 아래로 두려면 서버가 {:.0}대 필요합니다(지금 {}대, {:.0}대 추가). 일괄 작업을 먼저 옮기면 필요한 대수가 줄어듭니다.",
                    pc(limits::TARGET_BUSY), need, n_servers, extra
                ),
                opts.server_hour_usd.map(|p| -extra * p * window_ms as f64 / 3.6e6),
                "estimate",
                0.9,
            );
        } else {
            push(
                "pressed_hours",
                "평균으로는 여유가 있는데 몰릴 때 대기가 깁니다",
                format!(
                    "{} 시간대에 대기 p95가 {}를 넘거나 거절이 나옵니다. 가장 붐비는 {}시대의 사용률은 {}입니다.",
                    hour_ranges(&pressed_hours), secs(limits::PRESSED_QUEUE_P95_MS), peak, pc(hour_util[peak])
                ),
                "짧게 몰리는 부하입니다. 사용자별 동시 요청 한도와 적게 쓴 사람 먼저(least_used) 줄 세우기를 켜고, 대기 예상 시간을 알려 주는 대기표(queue_notice)를 쓰세요.".into(),
                None,
                "measured",
                0.6,
            );
        }
    }
    if refused_all > limits::PRESSED_REFUSED {
        push(
            "refusals",
            "거절되는 요청이 있습니다",
            format!("요청의 {}({}건)가 429 또는 503으로 거절됐습니다.", pc(refused_all), total.refused),
            "거절을 batch부터 단계적으로 하고(staged rejection) 대화형 요청에는 최소 처리량을 보장하세요. 거절 응답에 다시 시도할 시각을 넣으면 재시도 폭주가 줄어듭니다.".into(),
            None,
            "measured",
            0.7,
        );
    }
    if idle_slot_hours > 0.0 && !idle_hours.is_empty() {
        let idle_cost = opts
            .server_hour_usd
            .map(|p| idle_slot_hours / slots_per_server * p);
        push(
            "idle_capacity",
            "서버가 비어 있는 시간이 깁니다",
            format!(
                "{} 시간대의 사용률이 {} 아래입니다. 이 기간에 슬롯 기준 {:.0}시간이 비었습니다.",
                hour_ranges(&idle_hours), pc(limits::IDLE_BUSY), idle_slot_hours
            ),
            "이 시간대에 일괄 작업, 평가, 색인 작업을 예약해 채우세요. 채울 일이 없으면 이 시간대에 서버 일부를 내리는 일정을 두세요.".into(),
            idle_cost,
            "estimate",
            0.5,
        );
    }
    // Only servers that carry the same models can share work.
    let mut groups: BTreeMap<&str, Vec<(&str, f64)>> = BTreeMap::new();
    for (models, name, share) in &busy {
        groups.entry(models).or_default().push((name, *share));
    }
    for (models, list) in groups {
        let lo = list.iter().min_by(|a, b| a.1.total_cmp(&b.1)).unwrap();
        let hi = list.iter().max_by(|a, b| a.1.total_cmp(&b.1)).unwrap();
        if list.len() >= 2 && hi.1 - lo.1 > limits::IMBALANCE && !pressed_hours.is_empty() {
            push(
                "imbalance",
                "같은 모델을 올린 서버 사이에 일이 고르게 가지 않습니다",
                format!(
                    "{models} 모델을 올린 서버 중 {}는 시간의 {}를 일했고 {}는 {}만 일했습니다.",
                    hi.0, pc(hi.1), lo.0, pc(lo.1)
                ),
                "게이트웨이의 부하 기준 분배를 확인하세요. 접두부 캐시 분배(affinity = \"prefix\")가 한쪽으로 몰고 있다면 따뜻한 서버를 기다리는 한도를 낮추세요.".into(),
                None,
                "measured",
                hi.1 - lo.1,
            );
        }
    }
    let mut prompt_all: Vec<f64> = rows
        .iter()
        .filter_map(|r| Some((r.ttft? - r.queue).max(0.0)))
        .collect();
    let prompt_p50 = quantile(&mut prompt_all, 0.5);
    if let Some(p) = prompt_p50.filter(|p| *p > limits::SLOW_PROMPT_MS && total.cache_share() < 0.3)
    {
        push(
            "prompt_slow",
            "줄을 서지 않아도 첫 토큰이 늦습니다",
            format!(
                "서버에 들어간 뒤 첫 토큰까지의 중앙값이 {}이고, 입력 토큰 중 서버가 기억하고 있던 비율은 {}입니다.",
                secs(p), pc(total.cache_share())
            ),
            "같은 대화의 다음 요청을 같은 서버로 보내는 접두부 캐시 분배를 켜고, 서버가 기억하는 프롬프트 수(슬롯 캐시)를 늘리세요. 프롬프트 처리 속도 자체가 느리면 더 작은 양자화나 더 빠른 GPU가 필요합니다.".into(),
            None,
            "measured",
            0.6,
        );
    }
    let mut tps_pressed: Vec<f64> = Vec::new();
    let mut tps_calm: Vec<f64> = Vec::new();
    for r in &rows {
        if let Some(t) = r.tps {
            if pressed[hour_of(r.ts)] {
                tps_pressed.push(t);
            } else {
                tps_calm.push(t);
            }
        }
    }
    if tps_pressed.len() >= 20 && tps_calm.len() >= 20 {
        let (p, c) = (
            median_of(tps_pressed).unwrap(),
            median_of(tps_calm).unwrap(),
        );
        if p < c * limits::SPEED_DROP {
            push(
                "speed_drop",
                "붐빌 때 생성 속도가 떨어집니다",
                format!("요청 하나의 생성 속도 중앙값이 한가할 때 초당 {c:.1}토큰, 붐빌 때 초당 {p:.1}토큰입니다."),
                "한 서버에 동시에 넣는 요청 수가 많습니다. 게이트웨이의 서버별 동시 처리 한도를 낮추거나 적응형 동시성(adaptive)을 켜서, 처리량이 더 늘지 않는 지점에서 멈추게 하세요.".into(),
                None,
                "measured",
                1.0 - p / c,
            );
        }
    }
    if money && total.small_big_cost > total.cost * 0.1 {
        let low = cx
            .cheapest
            .as_ref()
            .map(|c| c.0.clone())
            .unwrap_or_default();
        push(
            "route_small",
            "짧은 요청을 작은 모델로 보내는 규칙을 두세요",
            format!(
                "짧은 요청 {}건이 비싼 모델로 갔고 전체 비용의 {}({})입니다.",
                total.small_big_n, pc(total.small_big_cost / total.cost), usd(total.small_big_cost)
            ),
            format!("게이트웨이에서 입력과 출력이 작은 요청을 {low}로 보내는 규칙을 두고, 표본으로 답의 품질을 비교하세요."),
            Some(total.small_big_saving),
            "estimate",
            total.small_big_cost / total.cost,
        );
    }
    let mut setup: Vec<Suggestion> = Vec::new();
    let mut need = |id, title: &str, detail: &str, action: &str| {
        setup.push(Suggestion {
            scope: "setup",
            target: "측정".into(),
            team: String::new(),
            id,
            title: title.to_string(),
            detail: detail.to_string(),
            action: action.to_string(),
            saving_usd: None,
            basis: "measured",
            weight: 0.3,
        });
    };
    if !rows.is_empty() {
        if basis == "none" {
            need("no_cost", "비용을 알 수 없습니다", "요청에 비용이 없고 서버 시간당 가격도 주어지지 않았습니다. 비중은 서버 사용 시간으로 계산했습니다.", "자체 서버라면 서버 한 대의 시간당 가격(server_hour_usd)을 넣고, API라면 모델 가격표(price)를 등록하세요.");
        }
        if !have_deliverables {
            need("no_deliverables", "산출물이 등록되지 않아 ROI를 계산할 수 없습니다", "이 기간에 등록된 산출물이 없습니다. 비용과 낭비는 보이지만 무엇을 얻었는지는 알 수 없습니다.", "PR 병합, 보고서 승인 같은 시점에 /gateway/obs/meta/deliverable로 산출물과 상태를 등록하고, 요청에 x-gateway-deliverable-id 헤더를 보내세요.");
        } else if meta.rates.is_empty() && meta.deliverables.values().any(|d| d.value_usd.is_none())
        {
            need("no_rate", "산출물의 가치를 돈으로 바꿀 기준이 없습니다", "절약 시간만 있는 산출물이 있는데 팀의 시간당 단가가 등록되지 않았습니다.", "/gateway/obs/meta/rate에 팀별 시간당 단가를 등록하세요. 기다린 시간의 비용도 같은 단가로 계산합니다.");
        }
        if rows.iter().all(|r| r.user.is_empty()) {
            need(
                "no_user",
                "누가 보낸 요청인지 알 수 없습니다",
                "요청에 사용자 ID가 없습니다.",
                "API 키를 사람마다 발급하거나 x-gateway-user-id 헤더를 보내세요.",
            );
        }
    }
    advice.extend(serving.iter().cloned());
    advice.extend(setup.iter().cloned());
    advice.sort_by(|a, b| {
        let key = |s: &Suggestion| (s.saving_usd.filter(|x| *x > 0.0).unwrap_or(0.0), s.weight);
        let (ka, kb) = (key(a), key(b));
        kb.0.total_cmp(&ka.0).then(kb.1.total_cmp(&ka.1))
    });

    let by_cost = |a: &Value, b: &Value| {
        let k = |v: &Value| {
            v["cost_usd"]
                .as_f64()
                .unwrap_or(0.0)
                .max(v["server_s"].as_f64().unwrap_or(0.0) * 1e-9)
        };
        k(b).total_cmp(&k(a))
    };
    people_out.sort_by(by_cost);
    teams_out.sort_by(by_cost);
    let o = outcome(&total, &meta, &all_d);
    let total_v = base(&mut total, &o, "*");
    let saving: f64 = advice
        .iter()
        .filter_map(|s| s.saving_usd)
        .filter(|x| *x > 0.0)
        .sum();
    let models: Vec<Value> = per_model
        .iter()
        .map(|(m, v)| {
            json!({
                "model": m, "requests": v.0, "cost_usd": money.then(|| r4(v.1)), "tokens": v.2,
                "small_share": r4(v.3 / v.0.max(1) as f64), "server_s": r2(v.4 / 1000.0),
                "usd_per_mtok": cx.price.get(m).map(|p| r2(p * 1e6)),
            })
        })
        .collect();
    let allocated: f64 = if basis == "server_time" {
        total.cost
    } else {
        0.0
    };
    Ok(json!({
        "window": {"from": from, "to": to, "hours": r2(window_ms as f64 / 3.6e6), "day_s": opts.day_s, "tz_hours": opts.tz_hours},
        "cost_basis": basis,
        "total": total_v,
        "saving_usd": money.then(|| r2(saving)),
        "people": people_out,
        "teams": teams_out,
        "serving": {
            "servers": server_rows, "hours": hour_rows, "models": models,
            "slots": total_slots, "slots_assumed": opts.slots.is_none(),
            "pressed_hours": pressed_hours, "idle_hours": idle_hours,
            "idle_slot_hours": r2(idle_slot_hours),
            "fleet_cost_usd": fleet_cost.map(r2),
            "idle_cost_usd": fleet_cost.map(|f| r2((f - allocated).max(0.0))),
            "refused": r4(refused_all),
            "prompt_p50_ms": prompt_p50.map(r2),
            "suggestions": serving,
        },
        "setup": setup,
        "suggestions": advice,
        "limits": {
            "min_requests": limits::MIN_REQUESTS, "pressed_queue_p95_ms": limits::PRESSED_QUEUE_P95_MS,
            "pressed_refused": limits::PRESSED_REFUSED, "idle_busy": limits::IDLE_BUSY,
            "target_busy": limits::TARGET_BUSY, "cache_gap": limits::CACHE_GAP,
            "context_factor": limits::CONTEXT_FACTOR, "waste_share": limits::WASTE_SHARE,
            "small_out": limits::SMALL_OUT, "small_in": limits::SMALL_IN,
            "price_factor": limits::PRICE_FACTOR, "small_on_big_share": limits::SMALL_ON_BIG_SHARE,
            "unattributed_share": limits::UNATTRIBUTED_SHARE, "batch_peak_share": limits::BATCH_PEAK_SHARE,
            "rejected_share": limits::REJECTED_SHARE, "top_user_share": limits::TOP_USER_SHARE,
            "spread": limits::SPREAD, "slow_first_token_ms": limits::SLOW_FIRST_TOKEN_MS,
            "slow_share": limits::SLOW_SHARE, "imbalance": limits::IMBALANCE,
            "speed_drop": limits::SPEED_DROP, "error_rate": limits::ERROR_RATE,
            "stall_ms": limits::STALL_MS, "slow_prompt_ms": limits::SLOW_PROMPT_MS,
        },
    }))
}

#[cfg(test)]
mod tests {
    use super::super::store::{Memory, Row};
    use super::*;

    const HOUR: i64 = 3_600_000;

    struct Req {
        ts: i64,
        user: &'static str,
        team: &'static str,
        deliverable: &'static str,
        cost: f64,
        input: f64,
        cached: f64,
        queue: f64,
        batch: bool,
        abort: bool,
        backend: &'static str,
    }

    fn base(ts: i64, user: &'static str) -> Req {
        Req {
            ts,
            user,
            team: "t1",
            deliverable: "",
            cost: 1.0,
            input: 1000.0,
            cached: 700.0,
            queue: 10.0,
            batch: false,
            abort: false,
            backend: "s1",
        }
    }

    fn row(r: &Req) -> Row {
        [
            ("ts", json!(r.ts)),
            ("user_id", json!(r.user)),
            ("team_id", json!(r.team)),
            ("deliverable_id", json!(r.deliverable)),
            ("cost_usd", json!(r.cost)),
            ("input_tokens", json!(r.input)),
            ("output_tokens", json!(100)),
            ("cache_read_tokens", json!(r.cached)),
            ("queue_ms", json!(r.queue)),
            ("ttft_ms", json!(r.queue + 500.0)),
            ("total_ms", json!(r.queue + 2000.0)),
            (
                "priority",
                json!(if r.batch { "batch" } else { "interactive" }),
            ),
            ("client_abort", json!(r.abort)),
            ("backend", json!(r.backend)),
            ("status", json!(200)),
            ("model", json!("m")),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect()
    }

    fn run(reqs: &[Req], meta: &[(&str, &str, Value)], opts: &Options) -> Value {
        let m = Memory::new(reqs.len() + 1);
        m.append(Table::Requests, reqs.iter().map(row).collect())
            .unwrap();
        for (kind, id, v) in meta {
            m.meta_put(kind, id, v.clone()).unwrap();
        }
        // One UTC day; tz 0 keeps hour numbers equal to the test's hours.
        report(&m, 0, DAY_MS, opts).unwrap()
    }

    fn utc() -> Options {
        Options {
            tz_hours: 0.0,
            ..Options::default()
        }
    }

    fn find<'a>(list: &'a Value, key: &str, name: &str) -> &'a Value {
        list.as_array()
            .unwrap()
            .iter()
            .find(|v| v[key] == name)
            .unwrap_or_else(|| panic!("no {name}"))
    }

    fn ids(v: &Value) -> Vec<&str> {
        v["suggestions"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s["id"].as_str().unwrap())
            .collect()
    }

    #[test]
    fn splits_value_by_spend_and_classes_add_up() {
        let mut reqs = Vec::new();
        for i in 0..30 {
            // ann: 30 on d1 (accepted). bob: 10 on d1, 10 on d2 (rejected), 10 on nothing.
            reqs.push(Req {
                deliverable: "d1",
                ..base(9 * HOUR + i, "ann")
            });
            reqs.push(Req {
                deliverable: ["d1", "d2", ""][(i % 3) as usize],
                ..base(9 * HOUR + i, "bob")
            });
        }
        let meta = [
            (
                "deliverable",
                "d1",
                json!({"team_id": "t1", "status": "accepted", "hours_saved": 4.0}),
            ),
            (
                "deliverable",
                "d2",
                json!({"team_id": "t1", "status": "rejected", "value_usd": 900.0}),
            ),
            ("rate", "*", json!({"hourly_usd": 100.0})),
        ];
        let r = run(&reqs, &meta, &utc());
        let (ann, bob) = (
            find(&r["people"], "user", "ann"),
            find(&r["people"], "user", "bob"),
        );
        // d1 is worth 400 and cost 40: ann paid 30 of it, bob 10.
        assert_eq!(ann["value_usd"], 300.0);
        assert_eq!(bob["value_usd"], 100.0);
        assert_eq!(ann["accepted"], 0.75);
        assert_eq!(ann["roi"], 9.0);
        assert_eq!(ann["useful_share"], 1.0);
        for p in [ann, bob, &r["total"]] {
            let sum = ["useful_share", "waste_share", "unattributed_share"]
                .iter()
                .map(|k| p[*k].as_f64().unwrap())
                .sum::<f64>();
            assert!((sum - 1.0).abs() < 1e-3, "{sum}");
        }
        assert!(ids(bob).contains(&"rejected_work"));
        assert!(ids(ann).is_empty(), "{:?}", ids(ann));
        assert_eq!(r["total"]["value_usd"], 400.0);
        assert_eq!(r["cost_basis"], "billed");
    }

    #[test]
    fn person_rules_compare_with_the_team() {
        let mut reqs = Vec::new();
        for i in 0..40 {
            for u in ["a", "b", "c"] {
                reqs.push(base(9 * HOUR + i, u));
            }
            reqs.push(Req {
                cached: 50.0,
                ..base(9 * HOUR + i, "nocache")
            });
            reqs.push(Req {
                input: 9000.0,
                cached: 6300.0,
                ..base(9 * HOUR + i, "long")
            });
            reqs.push(Req {
                abort: i % 2 == 0,
                ..base(9 * HOUR + i, "quits")
            });
        }
        // Too few requests: numbers but no advice.
        reqs.push(Req {
            cached: 0.0,
            abort: true,
            ..base(9 * HOUR, "new")
        });
        let r = run(&reqs, &[], &utc());
        let of = |u: &str| ids(find(&r["people"], "user", u));
        assert_eq!(of("nocache"), ["cache_low"]);
        assert_eq!(of("long"), ["context_long"]);
        assert_eq!(of("quits"), ["failed_requests"]);
        assert!(of("a").is_empty());
        assert!(of("new").is_empty());
        assert_eq!(find(&r["people"], "user", "new")["few"], true);
        let quits = find(&r["people"], "user", "quits");
        assert_eq!(quits["suggestions"][0]["saving_usd"], 20.0);
        assert_eq!(quits["suggestions"][0]["basis"], "measured");
        // No deliverables registered: said once, not per person.
        assert_eq!(r["setup"][0]["id"], "no_deliverables");
    }

    #[test]
    fn serving_finds_pressed_and_idle_hours() {
        let mut reqs = Vec::new();
        // 13:00 is queued and full of batch work, 03:00 is nearly empty.
        for i in 0..60 {
            reqs.push(Req {
                queue: 6000.0,
                batch: i % 2 == 0,
                ..base(13 * HOUR + i * 1000, "a")
            });
        }
        for i in 0..5 {
            reqs.push(base(3 * HOUR + i * 60_000, "b"));
        }
        let r = run(
            &reqs,
            &[],
            &Options {
                slots: Some(1.0),
                ..utc()
            },
        );
        let s = &r["serving"];
        assert_eq!(s["pressed_hours"], json!([13]));
        assert!(s["idle_hours"].as_array().unwrap().contains(&json!(3)));
        assert_eq!(s["hours"][13]["state"], "pressed");
        let got = ids(s);
        assert!(got.contains(&"hold_batch"), "{got:?}");
        assert!(got.contains(&"idle_capacity"), "{got:?}");
        assert!(ids(find(&r["people"], "user", "a")).contains(&"batch_at_peak"));
        // 60 requests of 2 s in one hour on one slot: 120 s of 3,600.
        assert_eq!(s["hours"][13]["util"], 0.0333);
    }

    #[test]
    fn server_time_prices_requests_without_a_bill() {
        let reqs: Vec<Req> = (0..30)
            .map(|i| Req {
                cost: 0.0,
                backend: if i % 3 == 0 { "s2" } else { "s1" },
                ..base(9 * HOUR + i * 5000, "a")
            })
            .collect();
        let none = run(&reqs, &[], &utc());
        assert_eq!(none["cost_basis"], "none");
        assert!(none["total"]["cost_usd"].is_null());
        assert_eq!(none["setup"][0]["id"], "no_cost");
        let opts = Options {
            server_hour_usd: Some(3.6),
            slots: Some(1.0),
            ..utc()
        };
        let r = run(&reqs, &[], &opts);
        assert_eq!(r["cost_basis"], "server_time");
        // 30 requests of 2 s at $3.60 an hour is 6 cents; two servers for a day cost $172.80.
        assert_eq!(r["total"]["cost_usd"], 0.06);
        assert_eq!(r["serving"]["fleet_cost_usd"], 172.8);
        assert_eq!(r["serving"]["idle_cost_usd"], 172.74);
    }
}
