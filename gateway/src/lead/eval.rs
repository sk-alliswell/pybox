//! The scorecard: every claim about the three areas as a criterion with a
//! target, a measured value and a baseline. `gateway-eval` prints it and
//! `tests/lead_eval.rs` fails the build when a criterion stops passing.
//!
//! Scheduling criteria run on the simulator (which drives the same queue,
//! routing and concurrency code as the live scheduler). Cache criteria run
//! real request bodies through the real rewrite path, with the provider's
//! cache played by an independent model that sees only the bytes sent.

use std::collections::HashMap;
use std::time::Instant;

use axum::body::Bytes;
use serde_json::{Map, Value};

use super::cachectl;
use super::cachemodel::{CacheRules, CacheUse, ProviderCache, Weights};
use super::engine::{Done, Lead, LeadConfig};
use super::experiment::{verdict, ArmStats, Guard};
use super::fair::{FairCfg, FairQueue, Item};
use super::fingerprint::{fingerprint, Ttl};
use super::ledger::{Entry, Event, Fact, Ledger, HOUR_MS};
use super::runs::{AgentStats, RunTracker};
use super::sim::{
    flood_workload, mixed_workload, run, uneven_workload, Limit, Policy, Rng, Route, SimBackend,
    SimCfg, SimRun, Turn, Workload,
};
use crate::obs::Price;

#[derive(Debug, Clone, serde::Serialize)]
pub struct Criterion {
    pub id: &'static str,
    pub area: &'static str,
    pub name: &'static str,
    pub target: String,
    pub measured: String,
    pub baseline: String,
    pub pass: bool,
    /// Wall-clock measurement: only meaningful in a release build.
    pub timing: bool,
}

fn crit(
    id: &'static str,
    area: &'static str,
    name: &'static str,
    target: impl Into<String>,
    measured: impl Into<String>,
    baseline: impl Into<String>,
    pass: bool,
) -> Criterion {
    Criterion {
        id,
        area,
        name,
        target: target.into(),
        measured: measured.into(),
        baseline: baseline.into(),
        pass,
        timing: false,
    }
}

// ---------------------------------------------------------------- bodies

const WORDS: &[&str] = &[
    "agent", "build", "cache", "deploy", "error", "file", "graph", "index", "json", "kernel",
    "latency", "merge", "node", "output", "parse", "queue", "route", "schema", "token", "update",
    "value", "worker", "yield", "zone",
];

fn words(g: &mut Rng, bytes: usize) -> String {
    let mut s = String::with_capacity(bytes + 8);
    while s.len() < bytes {
        s.push_str(WORDS[(g.next() % WORDS.len() as u64) as usize]);
        s.push(' ');
    }
    s
}

/// An Anthropic-format conversation that grows turn by turn.
#[derive(Clone)]
pub struct Conv {
    pub model: String,
    pub tools: Vec<String>,
    pub system: String,
    pub msgs: Vec<String>,
    /// Extra top-level members, e.g. `,"temperature":0.2`.
    pub opts: String,
    /// The client sets its own cache markers.
    pub client_cc: bool,
    n: u32,
}

impl Conv {
    pub fn new(g: &mut Rng, tools: usize, tool_bytes: usize, system_bytes: usize) -> Self {
        let tools = (0..tools)
            .map(|i| {
                format!(
                    r#"{{"name":"tool_{i}","description":"{}","input_schema":{{"type":"object","properties":{{}}}}}}"#,
                    words(g, tool_bytes)
                )
            })
            .collect();
        Self {
            model: "claude-test".into(),
            tools,
            system: words(g, system_bytes),
            msgs: Vec::new(),
            opts: String::new(),
            client_cc: false,
            n: 0,
        }
    }
    pub fn user(&mut self, g: &mut Rng, bytes: usize) {
        self.n += 1;
        let text = format!("task {:x} {}", g.next(), words(g, bytes));
        self.msgs.push(format!(
            r#"{{"role":"user","content":[{{"type":"text","text":"{text}"}}]}}"#
        ));
    }
    /// The model's reply with a tool call, then the tool's result.
    pub fn tool_turn(&mut self, g: &mut Rng, reply_bytes: usize, result_bytes: usize) {
        self.n += 1;
        let id = format!("tu_{}", self.n);
        self.msgs.push(format!(
            r#"{{"role":"assistant","content":[{{"type":"text","text":"{}"}},{{"type":"tool_use","id":"{id}","name":"tool_0","input":{{}}}}]}}"#,
            words(g, reply_bytes)
        ));
        self.msgs.push(format!(
            r#"{{"role":"user","content":[{{"type":"tool_result","tool_use_id":"{id}","content":"{}"}}]}}"#,
            words(g, result_bytes)
        ));
    }
    /// Serialize. With `fuzz`, random whitespace goes between tokens.
    pub fn render(&self, mut fuzz: Option<&mut Rng>) -> Bytes {
        let mut sp = || -> &'static str {
            match fuzz.as_mut().map(|g| g.next() % 6) {
                Some(0) => " ",
                Some(1) => "\n  ",
                Some(2) => "\t",
                _ => "",
            }
        };
        let mut s = String::new();
        s.push_str(sp());
        s.push('{');
        s.push_str(sp());
        s.push_str(&format!(
            r#""model":{}"{}","max_tokens":1024{}"#,
            sp(),
            self.model,
            self.opts
        ));
        if self.client_cc {
            s.push_str(r#","cache_control":{"type":"ephemeral"}"#);
        }
        if !self.tools.is_empty() {
            s.push_str(&format!(r#",{}"tools"{}:{}["#, sp(), sp(), sp()));
            for (i, t) in self.tools.iter().enumerate() {
                if i > 0 {
                    s.push(',');
                    s.push_str(sp());
                }
                s.push_str(t);
            }
            s.push_str(sp());
            s.push(']');
        }
        let cc = if self.client_cc {
            r#","cache_control":{"type":"ephemeral"}"#
        } else {
            ""
        };
        s.push_str(&format!(
            r#",{}"system":{}[{}{{"type":"text","text":"{}"{cc}{}}}{}]"#,
            sp(),
            sp(),
            sp(),
            self.system,
            sp(),
            sp()
        ));
        s.push_str(&format!(r#",{}"messages":{}["#, sp(), sp()));
        for (i, m) in self.msgs.iter().enumerate() {
            if i > 0 {
                s.push(',');
                s.push_str(sp());
            }
            s.push_str(m);
        }
        s.push_str(sp());
        s.push(']');
        s.push_str(sp());
        s.push('}');
        s.push_str(sp());
        Bytes::from(s)
    }
}

/// The provider's cache, seeing only the bytes that were sent.
fn provider(
    cache: &mut ProviderCache,
    body: &[u8],
    now: i64,
    rules: CacheRules,
) -> (CacheUse, Ttl) {
    let fp = fingerprint(body, true).expect("valid body");
    let chain: Vec<(u64, u32)> = fp.blocks.iter().map(|b| (b.cum, b.tok)).collect();
    let ttl = fp.ttl();
    (cache.apply(&chain, &fp.breakpoints(), ttl, now, rules), ttl)
}

pub struct Req {
    pub ts: i64,
    pub agent: String,
    pub body: Bytes,
    pub out: u32,
    /// Which part of the day this request belongs to.
    pub kind: &'static str,
}

/// A compressed working day of agent traffic, with the habits seen in real
/// logs: agents that set no cache markers, a support bot with one shared
/// prompt, small one-off calls, runs that wait minutes on tools, clients
/// that manage their own cache, and an agent with a timestamp in its system
/// prompt.
pub fn day_workload(seed: u64, scale: usize) -> Vec<Req> {
    let mut g = Rng(seed);
    let mut reqs = Vec::new();
    let mut push = |ts: i64, agent: &str, c: &Conv, out: u32, kind: &'static str| {
        reqs.push(Req {
            ts,
            agent: agent.to_string(),
            body: c.render(None),
            out,
            kind,
        });
    };
    // Coding agents: many turns, large tools and system prompt, short gaps.
    let coder = Conv::new(&mut g, 12, 1600, 14_000);
    for r in 0..6 * scale {
        let mut c = coder.clone();
        let mut t = g.range(0.0, 1_500_000.0) as i64;
        c.user(&mut g, 600);
        for _ in 0..14 {
            push(t, &format!("coder-{}", r % 3), &c, 500, "coding agent");
            c.tool_turn(&mut g, 400, 3000);
            t += g.range(4000.0, 30_000.0) as i64;
        }
    }
    // Support bot: one request per conversation, one shared prompt.
    let bot = Conv::new(&mut g, 2, 800, 10_000);
    for _ in 0..60 * scale {
        let mut c = bot.clone();
        c.user(&mut g, 400);
        push(
            g.range(0.0, 1_800_000.0) as i64,
            "support",
            &c,
            250,
            "support bot",
        );
    }
    // Small one-off calls: nothing worth caching.
    for _ in 0..40 * scale {
        let mut c = Conv::new(&mut g, 0, 0, 600);
        c.user(&mut g, 500);
        push(
            g.range(0.0, 1_800_000.0) as i64,
            "oneoff",
            &c,
            150,
            "small one-off",
        );
    }
    // Runs that wait six to nine minutes on tools between turns.
    let waiter = Conv::new(&mut g, 6, 1200, 16_000);
    for r in 0..4 * scale {
        let mut c = waiter.clone();
        let mut t = g.range(0.0, 600_000.0) as i64;
        c.user(&mut g, 500);
        for _ in 0..8 {
            push(t, &format!("waiter-{}", r % 2), &c, 400, "long tool waits");
            c.tool_turn(&mut g, 300, 2000);
            t += g.range(360_000.0, 540_000.0) as i64;
        }
    }
    // Clients that already place cache markers themselves.
    let mut managed = Conv::new(&mut g, 8, 1200, 12_000);
    managed.client_cc = true;
    for _ in 0..3 * scale {
        let mut c = managed.clone();
        let mut t = g.range(0.0, 1_500_000.0) as i64;
        c.user(&mut g, 500);
        for _ in 0..10 {
            push(t, "managed", &c, 400, "client-managed cache");
            c.tool_turn(&mut g, 300, 2500);
            t += g.range(4000.0, 20_000.0) as i64;
        }
    }
    // A timestamp at the top of the system prompt: nothing can be cached.
    let stamped = Conv::new(&mut g, 8, 1500, 12_000);
    for _ in 0..2 * scale {
        let mut c = stamped.clone();
        let mut t = g.range(0.0, 1_500_000.0) as i64;
        c.user(&mut g, 500);
        for _ in 0..8 {
            c.system = format!(
                "now 2026-11-12T08:{:02}:{:02}Z {}",
                (t / 60_000) % 60,
                (t / 1000) % 60,
                stamped.system
            );
            push(t, "stamped", &c, 400, "timestamp in system");
            c.tool_turn(&mut g, 300, 2500);
            t += g.range(4000.0, 20_000.0) as i64;
        }
    }
    reqs.sort_by_key(|r| r.ts);
    reqs
}

#[derive(Default, Clone, Copy)]
pub struct Cost {
    /// Billed input in base-input-token units (cache reads count 0.1,
    /// writes 1.25 or 2).
    pub as_sent: f64,
    pub litellm_style: f64,
    pub ours: f64,
    pub raw_in: f64,
    pub out: f64,
}

pub struct DayResult {
    pub total: Cost,
    pub by_kind: Vec<(&'static str, Cost)>,
    /// Sum of the engine's own `gateway_saved_usd` column.
    pub claimed_saved_usd: f64,
    /// The same saving from the independent provider model.
    pub actual_saved_usd: f64,
    pub max_pred_err: f64,
    pub causes: HashMap<String, u64>,
}

const PRICE: Price = Price {
    input: 3.0,
    output: 15.0,
    cache_read: Some(0.3),
    cache_write: Some(3.75),
};

/// Replay a day three ways: as the clients sent it, with markers placed the
/// way LiteLLM's injection does (system prompt and last message, five
/// minutes), and through the gateway with cache points on.
pub fn replay_day(reqs: &[Req]) -> DayResult {
    let rules = CacheRules::default();
    let w = Weights::default();
    let lead = Lead::new(
        LeadConfig {
            cache_points: "on".into(),
            ..LeadConfig::default()
        },
        None,
        0,
    )
    .expect("config");
    let (mut p_sent, mut p_ours, mut p_lite) = (
        ProviderCache::default(),
        ProviderCache::default(),
        ProviderCache::default(),
    );
    let mut total = Cost::default();
    let mut kinds: Vec<(&'static str, Cost)> = Vec::new();
    let (mut claimed, mut actual, mut max_err) = (0.0, 0.0, 0.0f64);
    let mut causes: HashMap<String, u64> = HashMap::new();
    let usd = |u: CacheUse, ttl: Ttl| {
        let cw = PRICE.input * w.write(ttl);
        (f64::from(u.fresh) * PRICE.input
            + f64::from(u.read) * PRICE.cache_read.unwrap()
            + f64::from(u.write) * cw)
            / 1e6
    };
    for r in reqs {
        let (sent_use, sent_ttl) = provider(&mut p_sent, &r.body, r.ts, rules);
        let fp = fingerprint(&r.body, true).expect("valid");
        let chain: Vec<(u64, u32)> = fp.blocks.iter().map(|b| (b.cum, b.tok)).collect();
        let lite_use = if fp.client_breakpoints() {
            sent_use
        } else {
            let mut bps = Vec::new();
            if fp.n_system > 0 {
                bps.push(fp.n_tools + fp.n_system - 1);
            }
            bps.push(chain.len() - 1);
            p_lite.apply(&chain, &bps, Ttl::M5, r.ts, rules)
        };
        let prep = lead
            .prepare("up", true, true, &r.agent, None, &r.body, r.ts)
            .expect("understood");
        let (our_use, our_ttl) = provider(&mut p_ours, &prep.body, r.ts, rules);
        let cause = prep
            .dims
            .iter()
            .find(|d| d.0 == "cache_cause")
            .and_then(|d| d.1.as_str())
            .unwrap_or("")
            .to_string();
        *causes.entry(cause).or_default() += 1;
        // Hand the provider's numbers back, as a response's usage would.
        let mut row = Map::new();
        lead.finish(
            Some(&prep.fin),
            &Done {
                ts: r.ts,
                project: "p",
                team: "t",
                run: &prep.run_id,
                deliverable: "",
                fresh: u64::from(our_use.fresh),
                cache_read: u64::from(our_use.read),
                cache_write: u64::from(our_use.write),
                total_in: u64::from(fp.total_tokens()),
                output: u64::from(r.out),
                cost_usd: usd(our_use, our_ttl) + f64::from(r.out) * PRICE.output / 1e6,
                failed: false,
                price: Some(Price {
                    cache_write: Some(PRICE.input * w.write(our_ttl)),
                    ..PRICE
                }),
                backend: None,
            },
            &mut row,
        );
        claimed += row
            .get("gateway_saved_usd")
            .and_then(Value::as_f64)
            .unwrap_or(0.0);
        actual += usd(sent_use, sent_ttl) - usd(our_use, our_ttl);
        max_err = max_err.max(
            row.get("cache_pred_err")
                .and_then(Value::as_f64)
                .unwrap_or(0.0),
        );
        let c = Cost {
            as_sent: w.input_cost(sent_use, sent_ttl),
            litellm_style: w.input_cost(lite_use, Ttl::M5),
            ours: w.input_cost(our_use, our_ttl),
            raw_in: f64::from(fp.total_tokens()),
            out: f64::from(r.out),
        };
        let slot = match kinds.iter_mut().find(|k| k.0 == r.kind) {
            Some(k) => &mut k.1,
            None => {
                kinds.push((r.kind, Cost::default()));
                &mut kinds.last_mut().unwrap().1
            }
        };
        for t in [&mut total, slot] {
            t.as_sent += c.as_sent;
            t.litellm_style += c.litellm_style;
            t.ours += c.ours;
            t.raw_in += c.raw_in;
            t.out += c.out;
        }
    }
    DayResult {
        total,
        by_kind: kinds,
        claimed_saved_usd: claimed,
        actual_saved_usd: actual,
        max_pred_err: max_err,
        causes,
    }
}

// ------------------------------------------------------------- criteria

fn pct(x: f64) -> String {
    format!("{:.1}%", x * 100.0)
}

fn percentile(v: &mut [f64], p: f64) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    v.sort_by(f64::total_cmp);
    v[((v.len() - 1) as f64 * p).round() as usize]
}

/// F1: runs are told apart without any header.
fn run_inference(out: &mut Vec<Criterion>) {
    let mut g = Rng(11);
    let base: Vec<Conv> = (0..4).map(|_| Conv::new(&mut g, 6, 800, 6000)).collect();
    // (time, agent, true run, body)
    let mut reqs: Vec<(i64, String, usize, Bytes)> = Vec::new();
    let runs = 200;
    for r in 0..runs {
        let mut c = base[r % 4].clone();
        let agent = format!("agent-{}", r % 5);
        let mut t = g.range(0.0, 600_000.0) as i64;
        c.user(&mut g, 300);
        for _ in 0..(3 + g.next() % 10) {
            reqs.push((t, agent.clone(), r, c.render(None)));
            c.tool_turn(&mut g, 200, 800);
            t += g.range(500.0, 20_000.0) as i64;
        }
    }
    reqs.sort_by_key(|r| r.0);
    let mut tr = RunTracker::default();
    let mut ids: HashMap<usize, String> = HashMap::new();
    let mut owner: HashMap<String, usize> = HashMap::new();
    let mut right = 0;
    for (t, agent, truth, body) in &reqs {
        let fp = fingerprint(body, true).unwrap();
        let id = format!("{agent}/{}", tr.observe(agent, None, &fp, *t).run_id);
        let same_as_before = ids.entry(*truth).or_insert_with(|| id.clone()) == &id;
        let not_shared = *owner.entry(id).or_insert(*truth) == *truth;
        right += usize::from(same_as_before && not_shared);
    }
    let acc = right as f64 / reqs.len() as f64;
    out.push(crit(
        "F1",
        "common",
        "Requests grouped into the right run with no header",
        ">= 99%",
        format!(
            "{} of {} requests, {runs} interleaved runs",
            pct(acc),
            reqs.len()
        ),
        "today: no run id unless the client sends one",
        acc >= 0.99,
    ));
}

fn random_ledger(g: &mut Rng, entries: usize, days: i64) -> Ledger {
    let mut l = Ledger::default();
    let runs = (entries / 20).max(10);
    // A run's requests fall within a few hours of its start.
    let starts: Vec<i64> = (0..runs)
        .map(|_| (g.unit() * ((days * 24 - 3) as f64)) as i64)
        .collect();
    for _ in 0..entries {
        let run = g.next() as usize % runs;
        l.add(&Entry {
            ts: (starts[run] + (g.next() % 3) as i64) * HOUR_MS + 5,
            run: format!("r{run}"),
            deliverable: if run % 7 == 0 {
                format!("d{}", run % 50)
            } else {
                String::new()
            },
            project: format!("p{}", run % 6),
            team: format!("t{}", run % 3),
            cost: g.range(0.0001, 0.05),
            failed: g.unit() < 0.03,
            tokens: 1000,
            ..Entry::default()
        });
    }
    for i in 0..runs {
        let fact = match g.next() % 6 {
            0 | 1 => Fact::Link {
                run_id: format!("r{i}"),
                deliverable_id: format!("d{}", g.next() % 50),
                weight: g.range(0.5, 2.0),
            },
            2 => Fact::RunEnd {
                run_id: format!("r{i}"),
                outcome: ["ok", "failed", "abandoned", "shared"][(g.next() % 4) as usize].into(),
            },
            3 => Fact::Status {
                id: format!("d{}", g.next() % 50),
                status: ["accepted", "rejected", "abandoned", "reopened", "accepted"]
                    [(g.next() % 5) as usize]
                    .into(),
            },
            4 => Fact::Value {
                id: format!("d{}", g.next() % 50),
                usd_low: Some(5.0),
                usd_high: Some(g.range(5.0, 500.0)),
                hours_low: None,
                hours_high: None,
            },
            _ => continue,
        };
        let ts = (g.unit() * (days * 24) as f64) as i64 * HOUR_MS;
        l.record(Event {
            ts,
            recorded_at: ts + 1000,
            fact,
        });
    }
    l
}

fn ledger(out: &mut Vec<Criterion>, quick: bool) {
    let mut g = Rng(21);
    let days = 90;
    let l = random_ledger(&mut g, 20_000, days);
    let none = HashMap::new();
    let nodef = HashMap::new();
    let to = days * 24 * HOUR_MS;
    let r = l.report(0, to, i64::MAX, &none, &nodef);
    let sum_of = |v: &Value| {
        ["deliverable", "shared", "waste", "unattributed"]
            .iter()
            .map(|k| v["classes"][k].as_f64().unwrap())
            .sum::<f64>()
    };
    let total = l.total();
    let diff = (sum_of(&r["total"]) - total).abs() / total;
    let by_project: f64 = r["by_project"].as_array().unwrap().iter().map(sum_of).sum();
    let diff_p = (by_project - total).abs() / total;
    out.push(crit(
        "A1",
        "roi",
        "Four cost classes add up to total spend, overall and per project",
        "difference <= 0.001%",
        format!(
            "{:.5}% overall, {:.5}% per project (20,000 requests, ${total:.2})",
            diff * 100.0,
            diff_p * 100.0
        ),
        "today: one \"unattributed\" bucket, failed and rejected work counted as normal spend",
        diff <= 1e-5 && diff_p <= 1e-5,
    ));

    // A2: a report as of a moment does not change when late events arrive.
    let mut l2 = random_ledger(&mut Rng(22), 5000, days);
    let as_of = to / 2;
    let before = l2.report(0, to, as_of, &none, &nodef);
    for i in 0..500 {
        l2.record(Event {
            ts: (g.unit() * as_of as f64) as i64,
            recorded_at: as_of + 1 + i,
            fact: Fact::Status {
                id: format!("d{}", g.next() % 50),
                status: "rejected".into(),
            },
        });
    }
    let after = l2.report(0, to, as_of, &none, &nodef);
    let now = l2.report(0, to, i64::MAX, &none, &nodef);
    out.push(crit(
        "A2",
        "roi",
        "A report as of a past moment is unchanged after 500 late events",
        "identical",
        if before == after {
            "identical; the current report did change"
        } else {
            "changed"
        },
        "today: reports are recomputed from mutable records, so old numbers drift",
        before == after && now != after,
    ));

    // A3: ROI is a range, and absent when nobody stated a value.
    let rows = r["deliverables"].as_array().unwrap();
    let mut bad = 0;
    let mut with_value = 0;
    for d in rows {
        let (lo, hi) = (d["roi_low"].as_f64(), d["roi_high"].as_f64());
        let valued = !d["value_low_usd"].is_null();
        with_value += usize::from(valued);
        let ok = match (valued, lo, hi) {
            (true, Some(l), Some(h)) => l <= h && d["status"] == "accepted",
            (false, None, None) => true,
            // Valued but no spend in the window: no ratio.
            (true, None, None) => d["cost_usd"].as_f64() == Some(0.0),
            _ => false,
        };
        bad += usize::from(!ok);
    }
    out.push(crit(
        "A3",
        "roi",
        "ROI is a low-high range, shown only for accepted deliverables with a value",
        "0 violations",
        format!(
            "{bad} violations in {} deliverables ({with_value} with a value)",
            rows.len()
        ),
        "today: a single ROI number, zero value when none is stated",
        bad == 0 && with_value > 0,
    ));

    // A4: report speed from the rollup.
    let n = if quick { 200_000 } else { 1_000_000 };
    let big = random_ledger(&mut Rng(23), n, days);
    let mut ms: Vec<f64> = (0..5)
        .map(|_| {
            let t = Instant::now();
            let v = big.report(0, to, i64::MAX, &none, &nodef);
            std::hint::black_box(v);
            t.elapsed().as_secs_f64() * 1000.0
        })
        .collect();
    let med = percentile(&mut ms, 0.5);
    let limit = if quick { 60.0 } else { 300.0 };
    let mut c = crit(
        "A4",
        "roi",
        "90-day report over the request history",
        format!("<= {limit:.0} ms for {n} requests"),
        format!("{med:.0} ms ({} rollup cells)", big.cells()),
        "today: every report rescans the request rows",
        med <= limit,
    );
    c.timing = true;
    out.push(c);
}

fn api(seats: usize) -> Vec<SimBackend> {
    vec![SimBackend {
        knee: 1000,
        tps: 60.0,
        decay: 0.0,
        limit: Limit::Fixed(seats),
    }]
}

/// Interactive users next to a batch job on the same seats.
pub fn interactive_batch_workload(seed: u64) -> Workload {
    let mut g = Rng(seed);
    let mut wl = Workload {
        tenants: vec![("interactive".into(), 1.0), ("batch".into(), 1.0)],
        runs: Vec::new(),
    };
    for _ in 0..80 {
        let turns = (0..3)
            .map(|_| Turn {
                new_in: 400,
                out: g.range(80.0, 200.0) as u32,
                gap_ms: g.range(3000.0, 15_000.0) as i64,
            })
            .collect();
        wl.runs.push(SimRun {
            tenant: 0,
            start_ms: g.range(0.0, 480_000.0) as i64,
            base_in: 4000,
            sys: 1,
            turns,
        });
    }
    // The batch arrives at once, one minute in.
    for _ in 0..300 {
        wl.runs.push(SimRun {
            tenant: 1,
            start_ms: 60_000 + g.range(0.0, 5000.0) as i64,
            base_in: 6000,
            sys: 2,
            turns: vec![Turn {
                new_in: 2000,
                out: g.range(500.0, 900.0) as u32,
                gap_ms: 0,
            }],
        });
    }
    wl
}

fn scheduling(out: &mut Vec<Criterion>, quick: bool) {
    let seeds: &[u64] = if quick { &[1, 2] } else { &[1, 2, 3, 4, 5] };
    let n = seeds.len() as f64;
    let (mut gap_f, mut gap_r, mut bound) = (0.0f64, 0.0f64, 0.0f64);
    let (mut p50_f, mut p50_r, mut mean_f, mut mean_r) = (0.0, 0.0, 0.0, 0.0);
    let (mut lost_f, mut lost_r, mut span_f, mut span_r) = (0.0, 0.0, 0.0, 0.0);
    let (mut wait_f, mut wait_r) = (0.0f64, 0.0f64);
    let mut fair_ok = true;
    for &s in seeds {
        let wl = mixed_workload(s);
        let f = run(&wl, &SimCfg::new(api(4), Policy::Fair));
        let r = run(&wl, &SimCfg::new(api(4), Policy::Wrr));
        let b = 2.0 * (f.largest_request + FairCfg::default().slack);
        fair_ok &= f.fairness_gap <= b && f.fairness_gap < r.fairness_gap;
        gap_f = gap_f.max(f.fairness_gap);
        gap_r = gap_r.max(r.fairness_gap);
        bound = bound.max(b);
        p50_f += f.run_stats(Some(1)).1 / n;
        p50_r += r.run_stats(Some(1)).1 / n;
        mean_f += f.run_stats(None).0 / n;
        mean_r += r.run_stats(None).0 / n;
        lost_f += f.expiry_lost as f64 / n;
        lost_r += r.expiry_lost as f64 / n;
        span_f += f.makespan_ms / n;
        span_r += r.makespan_ms / n;
        wait_f = wait_f.max(f.max_wait_ms);
        wait_r = wait_r.max(r.max_wait_ms);
    }
    let base = "round robin by request count (current scheduler)";
    out.push(crit(
        "B1",
        "scheduling",
        "Service difference between teams with work waiting stays bounded",
        "<= 2 x (largest request + slack), and below round robin",
        format!(
            "{:.0}k weighted tokens (bound {:.0}k)",
            gap_f / 1e3,
            bound / 1e3
        ),
        format!("{:.0}k with {base}", gap_r / 1e3),
        fair_ok,
    ));
    out.push(crit(
        "B2",
        "scheduling",
        "Short runs finish sooner and no run pays for it on average",
        "short-run median <= 70% of round robin; mean run time not worse",
        format!(
            "short-run median {:.0} s, mean run {:.0} s",
            p50_f / 1e3,
            mean_f / 1e3
        ),
        format!("{:.0} s and {:.0} s with {base}", p50_r / 1e3, mean_r / 1e3),
        p50_f <= 0.7 * p50_r && mean_f <= mean_r,
    ));
    out.push(crit(
        "B3",
        "scheduling",
        "Tokens re-sent uncached because the cache expired in the queue",
        "<= 50% of round robin",
        format!("{:.1}k tokens per run of the workload", lost_f / 1e3),
        format!("{:.1}k with {base}", lost_r / 1e3),
        lost_f <= 0.5 * lost_r,
    ));
    out.push(crit(
        "B4",
        "scheduling",
        "No throughput is given up, and nothing starves",
        "time to finish all work <= 101% of round robin; longest wait <= 150%",
        format!(
            "{:.0} s to finish, longest wait {:.0} s",
            span_f / 1e3,
            wait_f / 1e3
        ),
        format!(
            "{:.0} s and {:.0} s with {base}",
            span_r / 1e3,
            wait_r / 1e3
        ),
        span_f <= 1.01 * span_r && wait_f <= 1.5 * wait_r,
    ));

    // B5: interactive requests next to a batch.
    let (mut i_f, mut i_r, mut b_f, mut b_r) = (Vec::new(), Vec::new(), 0.0, 0.0);
    for &s in seeds {
        let wl = interactive_batch_workload(s);
        let f = run(&wl, &SimCfg::new(api(8), Policy::Fair));
        let r = run(&wl, &SimCfg::new(api(8), Policy::Wrr));
        i_f.push(f.run_stats(Some(0)));
        i_r.push(r.run_stats(Some(0)));
        b_f += f.makespan_ms / n;
        b_r += r.makespan_ms / n;
    }
    let avg = |v: &[(f64, f64, f64)], k: usize| {
        v.iter().map(|x| [x.0, x.1, x.2][k]).sum::<f64>() / v.len() as f64
    };
    out.push(crit(
        "B5",
        "scheduling",
        "Interactive runs while a 300-request batch is running (headline: latency -50%)",
        "interactive run time p99 and mean <= 50% of round robin; batch finishes within 105%",
        format!(
            "mean {:.1} s, p99 {:.1} s; batch done in {:.0} s",
            avg(&i_f, 0) / 1e3,
            avg(&i_f, 2) / 1e3,
            b_f / 1e3
        ),
        format!(
            "mean {:.1} s, p99 {:.1} s; batch done in {:.0} s with {base}",
            avg(&i_r, 0) / 1e3,
            avg(&i_r, 2) / 1e3,
            b_r / 1e3
        ),
        avg(&i_f, 2) <= 0.5 * avg(&i_r, 2)
            && avg(&i_f, 0) <= 0.5 * avg(&i_r, 0)
            && b_f <= 1.05 * b_r,
    ));

    // B6: cost of one scheduling decision with a deep queue.
    let mut q = FairQueue::new(FairCfg::default());
    let item = |id: u64, now: i64| Item {
        id,
        cost: 1000.0 + (id % 7) as f64 * 300.0,
        enq_ms: now,
        deadline_ms: (id % 5 == 0).then_some(now + 30_000),
        risk: 5000.0,
        class: 1,
        ..Item::default()
    };
    for r in 0..1000u64 {
        q.push(&format!("t{}", r % 50), 1.0, &format!("r{r}"), item(r, 0));
    }
    let mut ns = Vec::with_capacity(20_000);
    let mut id = 1000u64;
    for step in 0..20_000i64 {
        let t = Instant::now();
        let p = q.peek(step, &|_| true).expect("backlog");
        let it = q.take(&p, step).expect("item");
        q.finish(&p.tenant, &p.run);
        id += 1;
        q.push(&p.tenant, 1.0, &p.run, item(id, step));
        std::hint::black_box(it);
        ns.push(t.elapsed().as_nanos() as f64);
    }
    let p99 = percentile(&mut ns, 0.99) / 1000.0;
    let mut c = crit(
        "B6",
        "scheduling",
        "Cost of one scheduling decision with 1,000 runs waiting",
        "p99 <= 50 microseconds",
        format!(
            "p99 {p99:.1} us, median {:.1} us",
            percentile(&mut ns, 0.5) / 1000.0
        ),
        "a model call takes hundreds of milliseconds",
        p99 <= 50.0,
    );
    c.timing = true;
    out.push(c);

    // B7: the concurrency limit finds the server's capacity by itself.
    let mut ok = true;
    let mut parts = Vec::new();
    let mut base_parts = Vec::new();
    for knee in [2usize, 4, 8] {
        let wl = flood_workload(5, 60, 20);
        let server = |limit| {
            vec![SimBackend {
                knee,
                tps: 30.0,
                decay: -0.03,
                limit,
            }]
        };
        let tput = |limit| run(&wl, &SimCfg::new(server(limit), Policy::Fair));
        let best = (1..=16)
            .map(|l| tput(Limit::Fixed(l)).tokens_per_s())
            .fold(0.0, f64::max);
        let a = tput(Limit::Adaptive(16));
        let (one, sixteen) = (
            tput(Limit::Fixed(1)).tokens_per_s(),
            tput(Limit::Fixed(16)).tokens_per_s(),
        );
        let share = a.tokens_per_s() / best;
        ok &= share >= 0.95 && a.final_limits[0].abs_diff(knee) <= 1;
        parts.push(format!(
            "capacity {knee}: settles at {}, {} of best",
            a.final_limits[0],
            pct(share)
        ));
        base_parts.push(format!(
            "capacity {knee}: {} / {}",
            pct(one / best),
            pct(sixteen / best)
        ));
    }
    out.push(crit(
        "B7",
        "scheduling",
        "Adaptive concurrency finds each server's capacity from throughput alone",
        "settles within 1 of the best limit, >= 95% of the best fixed limit's throughput",
        parts.join("; "),
        format!(
            "fixed limit 1 / 16, share of best: {}",
            base_parts.join("; ")
        ),
        ok,
    ));

    // B8, B9: several servers, uneven work.
    let two = || {
        vec![
            SimBackend {
                knee: 4,
                tps: 40.0,
                decay: -0.03,
                limit: Limit::Fixed(4),
            },
            SimBackend {
                knee: 4,
                tps: 40.0,
                decay: -0.03,
                limit: Limit::Fixed(4),
            },
        ]
    };
    let (
        mut idle_s,
        mut idle_b,
        mut kept,
        mut total,
        mut spill_s,
        mut spill_b,
        mut run_s,
        mut run_b,
    ) = (0.0, 0.0, 0u64, 0u64, 0u64, 0u64, 0.0, 0.0);
    for &s in seeds {
        let wl = uneven_workload(s);
        let smart = run(&wl, &SimCfg::new(two(), Policy::Fair));
        let mut cfg = SimCfg::new(two(), Policy::Fair);
        cfg.route = Route::Blind;
        let blind = run(&wl, &cfg);
        idle_s += smart.idle_queued_ms / smart.makespan_ms / n;
        idle_b += blind.idle_queued_ms / blind.makespan_ms / n;
        kept += smart.affinity_kept;
        total += smart.affinity_total;
        spill_s += smart.spill_lost;
        spill_b += blind.spill_lost;
        run_s += smart.run_stats(None).0 / n;
        run_b += blind.run_stats(None).0 / n;
    }
    out.push(crit(
        "B8",
        "scheduling",
        "No server sits idle while requests it could serve are waiting",
        "<= 1% of the time; mean run time not worse than blind rotation",
        format!("{} of the time, mean run {:.1} s", pct(idle_s), run_s / 1e3),
        format!(
            "{} and {:.1} s when servers are taken in turn without looking at load",
            pct(idle_b),
            run_b / 1e3
        ),
        idle_s <= 0.01 && run_s <= run_b,
    ));
    let keep = kept as f64 / total.max(1) as f64;
    out.push(crit(
        "B9",
        "scheduling",
        "A run stays on the server that holds its cache unless that server is overloaded",
        ">= 90% of follow-up turns; cache tokens lost to moving <= 25% of blind rotation",
        format!(
            "{} of turns, {:.0}k tokens re-sent",
            pct(keep),
            spill_s as f64 / 1e3
        ),
        format!(
            "{:.0}k tokens re-sent with blind rotation",
            spill_b as f64 / 1e3
        ),
        keep >= 0.9 && (spill_s as f64) <= 0.25 * spill_b as f64,
    ));

    // B10: servers of different capacity, limits unknown in advance.
    let wl = flood_workload(9, 80, 20);
    let mixed = |a, b| {
        vec![
            SimBackend {
                knee: 2,
                tps: 30.0,
                decay: -0.03,
                limit: a,
            },
            SimBackend {
                knee: 8,
                tps: 30.0,
                decay: -0.03,
                limit: b,
            },
        ]
    };
    let ideal = run(
        &wl,
        &SimCfg::new(mixed(Limit::Fixed(2), Limit::Fixed(8)), Policy::Fair),
    )
    .tokens_per_s();
    let adaptive = run(
        &wl,
        &SimCfg::new(
            mixed(Limit::Adaptive(16), Limit::Adaptive(16)),
            Policy::Fair,
        ),
    );
    let same = run(
        &wl,
        &SimCfg::new(mixed(Limit::Fixed(5), Limit::Fixed(5)), Policy::Fair),
    )
    .tokens_per_s();
    let share = adaptive.tokens_per_s() / ideal;
    out.push(crit(
        "B10",
        "scheduling",
        "A small and a large server together, capacities not configured",
        ">= 93% of the throughput with hand-tuned limits",
        format!(
            "{} (limits settled at {:?})",
            pct(share),
            adaptive.final_limits
        ),
        format!(
            "{} with the same fixed limit of 5 on both",
            pct(same / ideal)
        ),
        share >= 0.93,
    ));
}

fn strip_cc(v: &mut Value) {
    match v {
        Value::Object(m) => {
            m.remove("cache_control");
            m.values_mut().for_each(strip_cc);
        }
        Value::Array(a) => a.iter_mut().for_each(strip_cc),
        _ => {}
    }
}

fn tokens(out: &mut Vec<Criterion>, quick: bool) {
    // C1: cause of a cache break, on labelled cases.
    let mut g = Rng(31);
    let mut right = 0;
    let mut total = 0;
    let mut wrong: Vec<String> = Vec::new();
    let variants = if quick { 8 } else { 25 };
    type Mutate = fn(&mut Conv, &mut Rng);
    let cases: &[(&str, &str, i64, Mutate)] = &[
        ("ok", "on", 10_000, |_, _| {}),
        ("tools_changed", "on", 10_000, |c, g| {
            let extra = Conv::new(g, 1, 300, 10)
                .tools
                .remove(0)
                .replace("tool_0", "tool_new");
            c.tools.push(extra);
        }),
        ("tools_reordered", "on", 10_000, |c, _| c.tools.swap(0, 2)),
        ("system_changed", "on", 10_000, |c, g| {
            c.system = words(g, 6000)
        }),
        ("system_volatile", "on", 10_000, |c, g| {
            c.system = format!(
                "time 2026-11-12T{:02}:{:02}:00Z {}",
                g.next() % 24,
                g.next() % 60,
                &c.system[26..]
            );
        }),
        ("history_edited", "on", 10_000, |c, g| {
            c.msgs[1] = format!(
                r#"{{"role":"assistant","content":[{{"type":"text","text":"summary {}"}}]}}"#,
                words(g, 200)
            );
        }),
        ("model_changed", "on", 10_000, |c, _| {
            c.model = "claude-other".into()
        }),
        ("options_changed", "on", 10_000, |c, g| {
            c.opts = format!(r#","temperature":0.{}"#, 1 + g.next() % 8)
        }),
        ("ttl_expired", "on", 400_000, |_, _| {}),
        ("no_breakpoint", "shadow", 10_000, |_, _| {}),
    ];
    for (expect, mode, gap, mutate) in cases {
        for v in 0..variants {
            let lead = Lead::new(
                LeadConfig {
                    cache_points: (*mode).into(),
                    ..LeadConfig::default()
                },
                None,
                0,
            )
            .unwrap();
            let mut c = Conv::new(&mut g, 4 + v % 4, 900, 7000);
            c.system = format!("time 2026-11-12T00:00:00Z {}", c.system);
            c.user(&mut g, 400);
            c.tool_turn(&mut g, 300, 1200);
            let run = Some("run-1");
            lead.prepare("up", true, true, "a", run, &c.render(None), 0);
            c.tool_turn(&mut g, 300, 1200);
            lead.prepare("up", true, true, "a", run, &c.render(None), 5000);
            mutate(&mut c, &mut g);
            c.tool_turn(&mut g, 300, 1200);
            let p = lead
                .prepare("up", true, true, "a", run, &c.render(None), 5000 + gap)
                .unwrap();
            let got = p
                .dims
                .iter()
                .find(|d| d.0 == "cache_cause")
                .unwrap()
                .1
                .as_str()
                .unwrap()
                .to_string();
            total += 1;
            if got == *expect {
                right += 1;
            } else if wrong.len() < 4 {
                wrong.push(format!("{expect} read as {got}"));
            }
        }
    }
    // A prompt too short to cache.
    for _ in 0..variants {
        let lead = Lead::new(LeadConfig::default(), None, 0).unwrap();
        let mut c = Conv::new(&mut g, 0, 0, 400);
        c.user(&mut g, 200);
        lead.prepare("up", true, true, "a", Some("r"), &c.render(None), 0);
        c.tool_turn(&mut g, 100, 200);
        let p = lead
            .prepare("up", true, true, "a", Some("r"), &c.render(None), 3000)
            .unwrap();
        total += 1;
        right += usize::from(
            p.dims
                .iter()
                .any(|d| d.0 == "cache_cause" && d.1 == "below_min"),
        );
    }
    let acc = right as f64 / total as f64;
    out.push(crit(
        "C1",
        "tokens",
        "The cause of a cache break is named correctly (11 kinds of labelled cases)",
        ">= 95%",
        format!(
            "{} of {total} cases{}",
            pct(acc),
            if wrong.is_empty() {
                String::new()
            } else {
                format!(" ({})", wrong.join(", "))
            }
        ),
        "today: only the cache-read token count is visible, not why it dropped",
        acc >= 0.95,
    ));

    // C2: the rewrite changes nothing but the markers.
    let n = if quick { 400 } else { 3000 };
    let (mut ok, mut changed) = (0, 0);
    let stats = AgentStats {
        gaps: 0,
        long_gaps: 0,
        runs: 1,
        turns: 6,
    };
    for i in 0..n {
        let mut c = Conv::new(&mut g, i % 5, 700 + (i % 3) * 600, 300 + (i % 7) * 1500);
        c.system.push_str(r#" quote \" brace } bracket ] é \\ end"#);
        c.user(&mut g, 300);
        for _ in 0..(i % 4) {
            c.tool_turn(&mut g, 200, 900);
        }
        let body = c.render(Some(&mut g));
        let fp = fingerprint(&body, true).unwrap();
        let plan = cachectl::plan(
            &body,
            &fp,
            1 + (i % 3) as u32,
            stats,
            CacheRules::default(),
            Weights::default(),
            i % 11 == 0,
        );
        let sent = cachectl::apply(&body, &plan.inserts);
        // Cutting the inserted text back out must give the original bytes.
        let mut back = sent.to_vec();
        let mut exact = true;
        for (off, text) in &plan.inserts {
            // Earlier inserts are already cut, so offsets are the original's.
            let at = *off;
            exact &= back.get(at..at + text.len()) == Some(text.as_bytes());
            if exact {
                back.drain(at..at + text.len());
            }
        }
        let same_json = match (
            serde_json::from_slice::<Value>(&sent),
            serde_json::from_slice::<Value>(&body),
        ) {
            (Ok(mut a), Ok(b)) => {
                strip_cc(&mut a);
                a == b
            }
            _ => false,
        };
        changed += usize::from(!plan.inserts.is_empty());
        ok += usize::from(exact && back == body.as_ref() && same_json);
    }
    out.push(crit(
        "C2",
        "tokens",
        "Adding cache markers leaves every other byte of the request as the client sent it",
        "100% of fuzzed bodies",
        format!("{ok} of {n} ({changed} had markers added)"),
        "LiteLLM re-serializes the request when it injects cache markers",
        ok == n && changed > n / 3,
    ));

    // C3, C4, H1: a day of traffic three ways.
    let reqs = day_workload(41, if quick { 1 } else { 3 });
    let d = replay_day(&reqs);
    let t = d.total;
    let no_regress = d.by_kind.iter().all(|(_, c)| c.ours <= c.as_sent * 1.001);
    let kinds: Vec<String> = d
        .by_kind
        .iter()
        .map(|(k, c)| format!("{k} {}", pct(c.ours / c.as_sent)))
        .collect();
    out.push(crit(
        "C3",
        "tokens",
        "Billed input cost of a day of mixed agent traffic (headline: token spend <= 50%)",
        "<= 50% of the cost as sent; no kind of traffic costs more; not above LiteLLM-style injection",
        format!("{} of as-sent ({})", pct(t.ours / t.as_sent), kinds.join(", ")),
        format!("LiteLLM-style injection (system + last message, 5 min): {}", pct(t.litellm_style / t.as_sent)),
        t.ours <= 0.5 * t.as_sent && no_regress && t.ours <= t.litellm_style * 1.001,
    ));
    let acct =
        (d.claimed_saved_usd - d.actual_saved_usd).abs() / d.actual_saved_usd.abs().max(1e-9);
    out.push(crit(
        "C4",
        "tokens",
        "The saving the gateway reports equals the saving an independent provider model sees",
        "within 1%; predicted cache reads within 1% of tokens",
        format!(
            "reported ${:.4}, independent ${:.4} ({} apart); worst prediction error {}",
            d.claimed_saved_usd,
            d.actual_saved_usd,
            pct(acct),
            pct(d.max_pred_err)
        ),
        "today: cache savings are reported, but not what the gateway itself changed",
        acct <= 0.01 && d.max_pred_err <= 0.01,
    ));

    // C5: the experiment verdict.
    let trials = if quick { 300 } else { 2000 };
    let mut g = Rng(51);
    let sample = |g: &mut Rng, scale: f64, fail: f64| {
        let mut a = ArmStats::default();
        for _ in 0..200 {
            a.add(
                scale * (0.6 * g.normal()).exp(),
                g.unit() < fail,
                8.0 + 2.0 * g.normal(),
            );
        }
        a
    };
    let (mut false_pos, mut power, mut stopped) = (0, 0, 0);
    for _ in 0..trials {
        let (c, t) = (sample(&mut g, 1.0, 0.02), sample(&mut g, 1.0, 0.02));
        false_pos += usize::from(!matches!(
            verdict(&c, &t, Guard::default()).decision,
            "inconclusive"
        ));
        let t = sample(&mut g, 0.8, 0.02);
        power += usize::from(verdict(&c, &t, Guard::default()).decision == "adopt");
        let t = sample(&mut g, 0.7, 0.12);
        stopped += usize::from(verdict(&c, &t, Guard::default()).decision == "stop_guardrail");
    }
    let (fp, pw, st) = (
        false_pos as f64 / trials as f64,
        power as f64 / trials as f64,
        stopped as f64 / trials as f64,
    );
    out.push(crit(
        "C5",
        "tokens",
        "Experiment verdicts at 200 runs per arm",
        "false alarm <= 8% with no real difference; a 20% saving found >= 80%; a cheaper arm that fails more is stopped >= 90%",
        format!("false alarm {}, detection {}, stopped {}", pct(fp), pct(pw), pct(st)),
        "today: changes are adopted on before/after averages",
        fp <= 0.08 && pw >= 0.8 && st >= 0.9,
    ));

    // C6: time added to a request.
    let mut big = Conv::new(&mut g, 20, 2000, 30_000);
    big.user(&mut g, 500);
    while big.render(None).len() < 200_000 {
        big.tool_turn(&mut g, 400, 4000);
    }
    let body = big.render(None);
    let lead = Lead::new(
        LeadConfig {
            cache_points: "on".into(),
            ..LeadConfig::default()
        },
        None,
        0,
    )
    .unwrap();
    let mut ms: Vec<f64> = (0..200)
        .map(|i| {
            let t = Instant::now();
            std::hint::black_box(lead.prepare("up", true, true, "a", Some("r"), &body, i * 1000));
            t.elapsed().as_secs_f64() * 1000.0
        })
        .collect();
    let (med, p99) = (percentile(&mut ms, 0.5), percentile(&mut ms, 0.99));
    let mut c = crit(
        "C6",
        "tokens",
        "Time the analysis and rewrite add to a 200 KB request",
        "median <= 1 ms",
        format!("median {med:.2} ms, p99 {p99:.2} ms ({} KB, {} blocks)", body.len() / 1000, fingerprint(&body, true).unwrap().blocks.len()),
        "LiteLLM's own overhead per request is 20 ms idle and up to 285 ms under load (measured in the console lab)",
        med <= 1.0,
    );
    c.timing = true;
    out.push(c);
}

/// Totals of replaying long tool-heavy runs through the gateway.
#[derive(Default, Clone, Copy)]
pub struct TrimRun {
    /// Input tokens actually sent.
    pub sent_tokens: f64,
    /// Input tokens the clients sent.
    pub client_tokens: f64,
    /// Billed input in base-token units, provider cache included.
    pub billed: f64,
    /// Turns on which the cleared boundary moved.
    pub advances: u64,
    pub turns: u64,
    pub invalid: u64,
}

/// Long coding runs with a tool result every turn, replayed with the given
/// `cache_points` and `tool_results` modes. The provider cache is the
/// independent model; `client_cache` makes the clients set their own markers.
pub fn replay_long_runs(
    seed: u64,
    runs: usize,
    turns: usize,
    cache: &str,
    trim: &str,
    client_cache: bool,
) -> TrimRun {
    let rules = CacheRules::default();
    let w = Weights::default();
    let lead = Lead::new(
        LeadConfig {
            cache_points: cache.into(),
            tool_results: trim.into(),
            ..LeadConfig::default()
        },
        None,
        0,
    )
    .expect("config");
    let mut provider_cache = ProviderCache::default();
    let mut g = Rng(seed);
    let base = Conv::new(&mut g, 10, 1500, 12_000);
    let mut reqs: Vec<(i64, usize, Bytes)> = Vec::new();
    for r in 0..runs {
        let mut c = base.clone();
        c.client_cc = client_cache;
        let mut t = g.range(0.0, 600_000.0) as i64;
        c.user(&mut g, 500);
        for _ in 0..turns {
            reqs.push((t, r, c.render(None)));
            let size = g.range(2000.0, 9000.0) as usize;
            c.tool_turn(&mut g, 300, size);
            t += g.range(3000.0, 20_000.0) as i64;
        }
    }
    reqs.sort_by_key(|r| r.0);
    let mut out = TrimRun::default();
    for (ts, run, body) in &reqs {
        let id = format!("run-{run}");
        let p = lead
            .prepare("up", true, true, "coder", Some(&id), body, *ts)
            .expect("understood");
        let (used, ttl) = provider(&mut provider_cache, &p.body, *ts, rules);
        out.billed += w.input_cost(used, ttl);
        out.sent_tokens += f64::from(fingerprint(&p.body, true).unwrap().total_tokens());
        out.client_tokens += f64::from(fingerprint(body, true).unwrap().total_tokens());
        out.turns += 1;
        out.advances += u64::from(p.dims.iter().any(|d| d.0 == "trim_advanced" && d.1 == true));
        // What was sent must still be a well-formed conversation: every
        // tool result answers a tool call, and the last three are intact.
        let (sent, orig): (Value, Value) = match (
            serde_json::from_slice(&p.body),
            serde_json::from_slice(body),
        ) {
            (Ok(a), Ok(b)) => (a, b),
            _ => {
                out.invalid += 1;
                continue;
            }
        };
        let results = |v: &Value| -> Vec<Value> {
            v["messages"]
                .as_array()
                .map(|m| {
                    m.iter()
                        .flat_map(|x| x["content"].as_array().cloned().unwrap_or_default())
                        .filter(|c| c["type"] == "tool_result")
                        .collect()
                })
                .unwrap_or_default()
        };
        let (a, b) = (results(&sent), results(&orig));
        let ids_kept = a.len() == b.len()
            && a.iter()
                .zip(&b)
                .all(|(x, y)| x["tool_use_id"] == y["tool_use_id"]);
        let tail_kept = a.iter().rev().take(3).eq(b.iter().rev().take(3));
        let (mut s2, mut o2) = (sent.clone(), orig.clone());
        strip_cc(&mut s2);
        strip_cc(&mut o2);
        let rest_kept = s2["system"] == o2["system"] && s2["tools"] == o2["tools"];
        out.invalid += u64::from(!(ids_kept && tail_kept && rest_kept));
    }
    out
}

fn trimming(out: &mut Vec<Criterion>, quick: bool) {
    let (runs, turns) = if quick { (4, 30) } else { (10, 40) };
    let go = |cache: &str, trim: &str, client: bool| {
        replay_long_runs(61, runs, turns, cache, trim, client)
    };
    // No provider cache at all (what the console lab's day mostly is).
    let plain = go("off", "off", false);
    let plain_trim = go("off", "on", false);
    // With cache points on.
    let cached = go("on", "off", false);
    let cached_trim = go("on", "on", false);
    // Clients that manage their own cache.
    let own = go("off", "off", true);
    let own_trim = go("off", "on", true);
    let share = plain_trim.sent_tokens / plain.sent_tokens;
    out.push(crit(
        "D1",
        "tokens",
        "Input tokens sent on long tool-heavy runs with old tool results cleared (headline: token use <= 50%)",
        "<= 50% of what the clients sent",
        format!(
            "{} ({:.2}M of {:.2}M tokens, {runs} runs x {turns} turns)",
            pct(share),
            plain_trim.sent_tokens / 1e6,
            plain.sent_tokens / 1e6
        ),
        "today: every old tool result is re-sent on every turn",
        share <= 0.5,
    ));
    let (c1, c2) = (
        cached_trim.billed / cached.billed,
        own_trim.billed / own.billed,
    );
    out.push(crit(
        "D2",
        "tokens",
        "Clearing never costs more than it saves when a prompt cache is in use",
        "billed input with clearing <= without, for gateway-placed and client-placed cache markers",
        format!(
            "{} of cache-only cost with gateway markers, {} with client markers; boundary moved on {} of turns",
            pct(c1),
            pct(c2),
            pct(cached_trim.advances as f64 / cached_trim.turns as f64)
        ),
        format!(
            "clearing on every turn would rewrite the cached prompt each time; cache alone is already {} of uncached cost",
            pct(cached.billed / plain.billed)
        ),
        c1 <= 1.0 && c2 <= 1.0,
    ));
    let invalid = plain_trim.invalid + cached_trim.invalid + own_trim.invalid;
    let checked = plain_trim.turns + cached_trim.turns + own_trim.turns;
    out.push(crit(
        "D3",
        "tokens",
        "Cleared requests stay well-formed: tool call ids, the last three results, tools and system untouched",
        "0 violations",
        format!("{invalid} in {checked} requests"),
        "not applicable",
        invalid == 0 && plain.invalid + cached.invalid == 0,
    ));
}

/// Run every criterion. `quick` uses smaller workloads (for tests).
pub fn scorecard(quick: bool) -> Vec<Criterion> {
    let mut out = Vec::new();
    run_inference(&mut out);
    ledger(&mut out, quick);
    scheduling(&mut out, quick);
    tokens(&mut out, quick);
    trimming(&mut out, quick);
    out
}

pub fn markdown(cs: &[Criterion]) -> String {
    let mut s = String::from("| id | area | criterion | target | measured | baseline | result |\n|---|---|---|---|---|---|---|\n");
    for c in cs {
        s.push_str(&format!(
            "| {} | {} | {} | {} | {} | {} | {} |\n",
            c.id,
            c.area,
            c.name,
            c.target,
            c.measured,
            c.baseline,
            if c.pass { "pass" } else { "FAIL" }
        ));
    }
    s
}
