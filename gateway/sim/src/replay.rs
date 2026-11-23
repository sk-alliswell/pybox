//! Replay a gateway log: the requests the gateway recorded, sent again at
//! the recorded times into a simulated copy of the system, and what the
//! simulator makes of them set beside what the log says happened.
//!
//! The log (`GET /gateway/obs/export`) gives, per request: when it came,
//! from which agent, what kind of work, for which model, how long the
//! prompt and the answer were, and which start of the prompt it shared
//! with others (`prefix_chain`). The base scenario gives the system: the
//! gateway's settings and the servers. Server speed and callers' patience
//! can be read off the same log (`fit`), so a replay needs no separate
//! calibration run.
//!
//! Two uses. Against the same settings the log was made with, the
//! comparison says how far the simulator is from that system. Against
//! changed settings, the simulated side is the forecast for that day under
//! the change.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::metrics::pct;
use crate::scenario::{Arrival, Client, Scenario, TraceRow};
use crate::usage::{rows_from_sim, Row};
use crate::Sim;

/// A log row with the columns only a replay reads.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct LogRow {
    #[serde(flatten)]
    pub row: Row,
    /// `hash:tokens` per kept prompt block, tokens cumulative.
    pub prefix_chain: String,
    /// Time the request ran on its server before the first token, where the
    /// log has it apart from the wait at the gateway.
    pub model_ttft_ms: Option<f64>,
    pub gen_ms: Option<f64>,
}

pub fn parse_log(text: &str) -> Result<Vec<LogRow>, String> {
    text.lines()
        .filter(|l| !l.trim().is_empty())
        .enumerate()
        .map(|(i, l)| {
            let mut r: LogRow =
                serde_json::from_str(l).map_err(|e| format!("line {}: {e}", i + 1))?;
            if r.row.kind.is_empty() {
                r.row.kind = std::mem::take(&mut r.row.priority);
            }
            Ok(r)
        })
        .collect()
}

/// What was read off the log to stand in for a calibration run.
#[derive(Clone, Debug, Default, Serialize)]
pub struct Fit {
    /// Per model: fixed time before the first token (ms), prompt tokens
    /// read per second (0 = prompt length made no visible difference),
    /// output tokens per second, and the rows it rests on.
    pub models: BTreeMap<String, ModelFit>,
    /// Per agent: how long its callers waited before giving up (s), where
    /// any did.
    pub patience_s: BTreeMap<String, f64>,
}

#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct ModelFit {
    pub prefill_ms: f64,
    pub prefill_tokens_per_s: f64,
    pub tokens_per_s: f64,
    pub rows: usize,
}

fn median(mut v: Vec<f64>) -> Option<f64> {
    v.retain(|x| x.is_finite());
    v.sort_by(f64::total_cmp);
    pct(&v, 50.0)
}

/// Rows the simulator is asked to reproduce: everything that reached the
/// scheduler. Answers from the gateway's own cache and requests turned
/// away for what they were (bad request, no key, over budget) never did.
fn replayed(r: &LogRow) -> bool {
    let s = r.row.status;
    r.row.gateway_cache != "hit" && !((400..500).contains(&s) && s != 429)
}

pub fn fit(rows: &[LogRow]) -> Fit {
    let mut out = Fit::default();
    let mut by_model: BTreeMap<&str, Vec<&LogRow>> = BTreeMap::new();
    for r in rows
        .iter()
        .filter(|r| r.row.ok() && r.row.attempts.unwrap_or(1) >= 1)
    {
        by_model.entry(&r.row.model).or_default().push(r);
    }
    for (model, rs) in by_model {
        // Time on the server before the first token against the prompt
        // tokens it had to read: a line through the faster half of the
        // rows at each size, because the slower ones waited inside the
        // server or beside a busy neighbour.
        let mut pts: Vec<(f64, f64)> = rs
            .iter()
            .filter_map(|r| {
                let first = r
                    .model_ttft_ms
                    .or(r.row.ttft_ms.map(|t| t - r.row.queue_ms))?;
                let fresh = (r.row.input_tokens - r.row.cache_read_tokens).max(0.0);
                (first > 0.0).then_some((fresh, first))
            })
            .collect();
        pts.sort_by(|a, b| a.1.total_cmp(&b.1));
        pts.truncate((pts.len() * 3 / 4).max(1));
        let n = pts.len() as f64;
        let (mx, my) = (
            pts.iter().map(|p| p.0).sum::<f64>() / n,
            pts.iter().map(|p| p.1).sum::<f64>() / n,
        );
        let sxx: f64 = pts.iter().map(|p| (p.0 - mx).powi(2)).sum();
        let sxy: f64 = pts.iter().map(|p| (p.0 - mx) * (p.1 - my)).sum();
        let slope = if sxx > 0.0 { sxy / sxx } else { 0.0 };
        let (prefill_ms, read_rate) = if slope > 1e-6 && my - slope * mx >= 0.0 {
            (my - slope * mx, 1000.0 / slope)
        } else {
            (
                median(pts.iter().map(|p| p.1).collect()).unwrap_or(0.0),
                0.0,
            )
        };
        let rate = median(
            rs.iter()
                .filter_map(|r| {
                    let gen = r.gen_ms.or_else(|| Some(r.row.total_ms - r.row.ttft_ms?))?;
                    (gen > 0.0 && r.row.output_tokens > 1.0)
                        .then(|| (r.row.output_tokens - 1.0) / gen * 1000.0)
                })
                .collect(),
        );
        out.models.insert(
            model.to_string(),
            ModelFit {
                prefill_ms,
                prefill_tokens_per_s: read_rate,
                tokens_per_s: rate.unwrap_or(0.0),
                rows: rs.len(),
            },
        );
    }
    let mut left: BTreeMap<&str, Vec<f64>> = BTreeMap::new();
    for r in rows
        .iter()
        .filter(|r| r.row.client_abort && r.row.status == 0)
    {
        left.entry(&r.row.agent_id)
            .or_default()
            .push(r.row.total_ms / 1000.0);
    }
    for (agent, v) in left {
        if let Some(m) = median(v) {
            out.patience_s.insert(agent.to_string(), m);
        }
    }
    out
}

/// The kept blocks of a `prefix_chain`: (key, cumulative tokens). A key
/// travels as a float, so 52 bits of the hash are kept, and never 0.
fn blocks(chain: &str) -> Vec<(u64, f64)> {
    chain
        .split(',')
        .filter_map(|b| {
            let (h, t) = b.split_once(':')?;
            let h = u64::from_str_radix(h, 16).ok()? & ((1 << 52) - 1) | 1;
            Some((h, t.parse::<f64>().ok()?))
        })
        .collect()
}

/// Each request's shared start in the simulator's terms, from the chains
/// in time order: the longest start some earlier request also had is what
/// it can find in a cache (`key`, `share`); its first block is the group
/// part; its last block is what a follow-up will look for (`store`).
/// Shares are of the chain's own token estimate, which is in the same
/// unit as the block sizes.
fn prefixes(rows: &[&LogRow]) -> Vec<Option<[f64; 5]>> {
    let mut order: Vec<usize> = (0..rows.len()).collect();
    order.sort_by(|a, b| rows[*a].row.ts.total_cmp(&rows[*b].row.ts));
    let mut seen = std::collections::HashSet::new();
    let mut out = vec![None; rows.len()];
    for i in order {
        let bs = blocks(&rows[i].prefix_chain);
        let (Some(first), Some(last)) = (bs.first(), bs.last()) else {
            continue;
        };
        let total = last.1.max(1.0);
        let shared = bs.iter().rev().find(|b| seen.contains(&b.0));
        let (key, share) = match shared {
            Some(b) if b.0 != first.0 || bs.len() == 1 => (b.0 as f64, b.1 / total),
            _ => (0.0, 0.0),
        };
        let group = if bs.len() > 1 {
            (first.0 as f64, first.1 / total)
        } else {
            (0.0, 0.0)
        };
        out[i] = Some([key, share, group.0, group.1, last.0 as f64]);
        seen.extend(bs.iter().map(|b| b.0));
    }
    out
}

/// The base scenario with its clients replaced by the log's agents, each
/// sending what it sent at the times it sent it. The template for an
/// agent's weight, timeout and the like is the base's client with the same
/// `agent` (or name), else the one whose `class` is the kind of work, else
/// the one named `*`.
pub fn scenario(base: &Scenario, rows: &[LogRow], fit: Option<&Fit>) -> Result<Scenario, String> {
    let rows: Vec<&LogRow> = rows.iter().filter(|r| replayed(r)).collect();
    let t0 = rows.iter().map(|r| r.row.ts).fold(f64::INFINITY, f64::min);
    if !t0.is_finite() {
        return Err("the log has no request to replay".into());
    }
    let mut sc = base.clone();
    let entry = sc
        .gateway
        .as_ref()
        .map(|g| g.name.clone())
        .ok_or("the base scenario needs a gateway")?;
    let template = |agent: &str, kind: &str| {
        base.clients
            .iter()
            .find(|c| c.agent.as_deref().unwrap_or(&c.name) == agent)
            .or_else(|| {
                base.clients
                    .iter()
                    .find(|c| c.class.as_deref() == Some(kind) || c.name == kind)
            })
            .or_else(|| base.clients.iter().find(|c| c.name == "*"))
    };
    let shared = prefixes(&rows);
    type Sent<'a> = Vec<(&'a LogRow, Option<[f64; 5]>)>;
    let mut by_agent: BTreeMap<(&str, &str), Sent> = BTreeMap::new();
    for (r, p) in rows.iter().zip(shared) {
        by_agent
            .entry((&r.row.agent_id, &r.row.kind))
            .or_default()
            .push((r, p));
    }
    let mut clients = Vec::new();
    let mut last = 0.0f64;
    for ((agent, kind), mut rs) in by_agent {
        rs.sort_by(|a, b| a.0.row.ts.total_cmp(&b.0.row.ts));
        let out_typical = median(
            rs.iter()
                .filter(|r| r.0.row.ok())
                .map(|r| r.0.row.output_tokens)
                .collect(),
        )
        .unwrap_or(64.0);
        let prompt_typical = median(
            rs.iter()
                .map(|r| r.0.row.input_tokens)
                .filter(|t| *t > 0.0)
                .collect(),
        )
        .unwrap_or(256.0);
        let trace: Vec<TraceRow> = rs
            .iter()
            .map(|(r, prefix)| TraceRow {
                t: (r.row.ts - t0) / 1000.0,
                model: r.row.model.clone(),
                prompt_tokens: (r.row.input_tokens > 0.0).then_some(r.row.input_tokens),
                output_tokens: (r.row.ok() && r.row.output_tokens > 0.0)
                    .then_some(r.row.output_tokens),
                prefix: *prefix,
            })
            .collect();
        last = last.max(trace.last().map_or(0.0, |r| r.t));
        let name = if kind.is_empty() {
            agent.to_string()
        } else {
            format!("{agent} ({kind})")
        };
        let mut c: Client = match template(agent, kind) {
            Some(t) => t.clone(),
            None => serde_json::from_value(serde_json::json!({
                "name": "", "entry": entry, "models": [], "to_s": 0.0,
                "arrival": {"kind": "trace", "rows": []},
                "output_tokens": 1.0,
            }))
            .map_err(|e| e.to_string())?,
        };
        c.name = name;
        c.count = 1;
        c.agent = Some(agent.to_string());
        c.class = Some(if kind.is_empty() { "all" } else { kind }.to_string());
        c.models = Vec::new();
        c.from_s = 0.0;
        c.to_s = f64::MAX;
        c.prompt_tokens = prompt_typical;
        c.output_tokens = out_typical;
        c.output_sigma = 0.0;
        c.prefix = None;
        c.session = None;
        c.rhythm = None;
        if c.timeout_s.is_none() {
            c.timeout_s = fit.and_then(|f| f.patience_s.get(agent).copied());
        }
        c.arrival = Arrival::Trace { rows: trace };
        clients.push(c);
    }
    sc.clients = clients;
    sc.phases.clear();
    // Long enough for the last request to be answered or given up on.
    sc.max_s = last + 900.0;
    if let Some(f) = fit {
        for s in &mut sc.model_servers {
            let Some(m) = s.models.iter().find_map(|m| f.models.get(m)) else {
                continue;
            };
            if m.tokens_per_s > 0.0 {
                s.tokens_per_s = m.tokens_per_s;
            }
            s.prefill_ms = m.prefill_ms;
            s.prefill_tokens_per_s = m.prefill_tokens_per_s;
        }
    }
    sc.ready()
}

/// One kind of work, summarised the same way for the log and for a run.
#[derive(Clone, Debug, Default, Serialize)]
pub struct Summary {
    pub requests: f64,
    pub answered: f64,
    pub gave_up: f64,
    pub refused: f64,
    /// Seconds.
    pub ttft_p50: Option<f64>,
    pub ttft_p95: Option<f64>,
    /// Wait at the gateway, answered requests, seconds.
    pub wait_p50: Option<f64>,
    pub wait_p95: Option<f64>,
    /// Prompt tokens the servers did not read again, share of all prompt
    /// tokens of answered requests.
    pub reused: Option<f64>,
}

pub const METRICS: [&str; 9] = [
    "requests", "answered", "gave_up", "refused", "ttft_p50", "ttft_p95", "wait_p50", "wait_p95",
    "reused",
];

impl Summary {
    pub fn get(&self, k: &str) -> Option<f64> {
        match k {
            "requests" => Some(self.requests),
            "answered" => Some(self.answered),
            "gave_up" => Some(self.gave_up),
            "refused" => Some(self.refused),
            "ttft_p50" => self.ttft_p50,
            "ttft_p95" => self.ttft_p95,
            "wait_p50" => self.wait_p50,
            "wait_p95" => self.wait_p95,
            "reused" => self.reused,
            _ => None,
        }
    }
}

pub fn summarise<'a>(rows: impl Iterator<Item = &'a Row>) -> BTreeMap<String, Summary> {
    let mut groups: BTreeMap<String, Vec<&Row>> = BTreeMap::new();
    for r in rows {
        let kind = if r.kind.is_empty() { "all" } else { &r.kind };
        groups.entry(kind.to_string()).or_default().push(r);
        groups.entry("(everything)".into()).or_default().push(r);
    }
    groups
        .into_iter()
        .map(|(k, rs)| {
            let n = rs.len() as f64;
            let share = |f: &dyn Fn(&Row) -> bool| rs.iter().filter(|r| f(r)).count() as f64 / n;
            let sorted = |f: &dyn Fn(&Row) -> Option<f64>| {
                let mut v: Vec<f64> = rs.iter().filter(|r| r.ok()).filter_map(|r| f(r)).collect();
                v.sort_by(f64::total_cmp);
                v
            };
            let ttft = sorted(&|r| r.ttft_ms.map(|t| t / 1000.0));
            let wait = sorted(&|r| Some(r.queue_ms / 1000.0));
            let (read, input) = rs.iter().filter(|r| r.ok()).fold((0.0, 0.0), |a, r| {
                (a.0 + r.cache_read_tokens, a.1 + r.input_tokens)
            });
            let s = Summary {
                requests: n,
                answered: share(&|r| r.ok()),
                gave_up: share(&|r| r.client_abort && !(r.status == 429 || r.status == 503)),
                refused: share(&|r| r.status == 429 || r.status == 503),
                ttft_p50: pct(&ttft, 50.0),
                ttft_p95: pct(&ttft, 95.0),
                wait_p50: pct(&wait, 50.0),
                wait_p95: pct(&wait, 95.0),
                reused: (input > 0.0).then(|| read / input),
            };
            (k, s)
        })
        .collect()
}

/// A number over the seeds: mean, the range seen, and the half-width of
/// the 95% interval for the mean (Student's t).
#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct Spread {
    pub mean: f64,
    pub min: f64,
    pub max: f64,
    pub ci95: f64,
    pub n: usize,
}

pub fn spread(v: &[f64]) -> Option<Spread> {
    if v.is_empty() {
        return None;
    }
    let n = v.len();
    let mean = v.iter().sum::<f64>() / n as f64;
    let sd = if n > 1 {
        (v.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (n - 1) as f64).sqrt()
    } else {
        0.0
    };
    // Two-sided 95% t quantiles for 1..=30 degrees of freedom.
    const T: [f64; 30] = [
        12.706, 4.303, 3.182, 2.776, 2.571, 2.447, 2.365, 2.306, 2.262, 2.228, 2.201, 2.179, 2.160,
        2.145, 2.131, 2.120, 2.110, 2.101, 2.093, 2.086, 2.080, 2.074, 2.069, 2.064, 2.060, 2.056,
        2.052, 2.048, 2.045, 2.042,
    ];
    let t = match n {
        1 => 0.0,
        n if n <= 31 => T[n - 2],
        _ => 1.96,
    };
    Some(Spread {
        mean,
        min: v.iter().copied().fold(f64::INFINITY, f64::min),
        max: v.iter().copied().fold(f64::NEG_INFINITY, f64::max),
        ci95: t * sd / (n as f64).sqrt(),
        n,
    })
}

#[derive(Clone, Debug, Serialize)]
pub struct Line {
    pub kind: String,
    pub metric: &'static str,
    pub logged: Option<f64>,
    pub simulated: Option<Spread>,
}

#[derive(Clone, Debug, Serialize)]
pub struct Comparison {
    pub name: String,
    pub rows: usize,
    /// Rows left out: answered from the gateway's cache or turned away for
    /// what they were.
    pub skipped: usize,
    pub span_s: f64,
    pub seeds: usize,
    pub fit: Option<Fit>,
    pub lines: Vec<Line>,
    /// Wall-clock seconds one seed took.
    pub seconds_per_seed: f64,
}

/// Run the replay `seeds` times and set it beside the log.
pub fn compare(
    base: &Scenario,
    rows: &[LogRow],
    seeds: usize,
    use_fit: bool,
    clock: &dyn Fn() -> f64,
) -> Result<Comparison, String> {
    let f = use_fit.then(|| fit(rows));
    let sc = scenario(base, rows, f.as_ref())?;
    let kept: Vec<&Row> = rows
        .iter()
        .filter(|r| replayed(r))
        .map(|r| &r.row)
        .collect();
    let logged = summarise(kept.iter().copied());
    let mut runs: Vec<BTreeMap<String, Summary>> = Vec::new();
    let started = clock();
    for seed in 1..=seeds.max(1) {
        let mut one = sc.clone();
        one.seed = base.seed.wrapping_add(seed as u64);
        let mut sim = Sim::new(one)?;
        sim.run();
        // In a run the class is the kind.
        let mut out = rows_from_sim(&sim);
        for r in &mut out {
            if r.kind.is_empty() {
                r.kind = std::mem::take(&mut r.team_id);
            }
        }
        runs.push(summarise(out.iter()));
    }
    let seconds_per_seed = (clock() - started) / seeds.max(1) as f64;
    let mut lines = Vec::new();
    for (kind, l) in &logged {
        for m in METRICS {
            let v: Vec<f64> = runs
                .iter()
                .filter_map(|r| r.get(kind).and_then(|s| s.get(m)))
                .collect();
            lines.push(Line {
                kind: kind.clone(),
                metric: m,
                logged: l.get(m),
                simulated: spread(&v),
            });
        }
    }
    let span = kept.iter().map(|r| r.ts).fold(f64::NEG_INFINITY, f64::max)
        - kept.iter().map(|r| r.ts).fold(f64::INFINITY, f64::min);
    Ok(Comparison {
        name: base.name.clone(),
        rows: kept.len(),
        skipped: rows.len() - kept.len(),
        span_s: span / 1000.0,
        seeds: seeds.max(1),
        fit: f,
        lines,
        seconds_per_seed,
    })
}

fn show(metric: &str, v: f64) -> String {
    match metric {
        "requests" => format!("{v:.0}"),
        "answered" | "gave_up" | "refused" | "reused" => format!("{:.1}%", v * 100.0),
        _ => format!("{v:.2} s"),
    }
}

/// The comparison as a Markdown table: logged, simulated (mean with its
/// 95% interval and the range over seeds), and the gap.
pub fn markdown(c: &Comparison) -> String {
    let mut out = format!(
        "# Replay of {}\n\n{} requests over {:.1} h, {} seed(s), {:.2} s of computing per seed.",
        c.name,
        c.rows,
        c.span_s / 3600.0,
        c.seeds,
        c.seconds_per_seed
    );
    if c.skipped > 0 {
        out += &format!(
            " {} rows left out (answered from the gateway's cache, or turned away as bad requests).",
            c.skipped
        );
    }
    out += "\n\n";
    if let Some(f) = &c.fit {
        out += "Read off the log: ";
        let models: Vec<String> = f
            .models
            .iter()
            .map(|(m, x)| {
                format!(
                    "{m} {:.0} ms before the first token{}, {:.1} tokens/s ({} rows)",
                    x.prefill_ms,
                    if x.prefill_tokens_per_s > 0.0 {
                        format!(" plus {:.0} prompt tokens/s", x.prefill_tokens_per_s)
                    } else {
                        String::new()
                    },
                    x.tokens_per_s,
                    x.rows
                )
            })
            .collect();
        out += &models.join("; ");
        if !f.patience_s.is_empty() {
            let p: Vec<String> = f
                .patience_s
                .iter()
                .map(|(a, s)| format!("{a} {s:.0} s"))
                .collect();
            out += &format!(". Callers gave up after: {}", p.join(", "));
        }
        out += ".\n\n";
    }
    out += "| kind | metric | logged | simulated | ± 95% | range over seeds | gap |\n|---|---|---|---|---|---|---|\n";
    for l in &c.lines {
        if l.metric == "requests" && l.kind != "(everything)" && l.simulated.is_none() {
            continue;
        }
        let logged = l.logged.map_or("–".into(), |v| show(l.metric, v));
        let (sim, ci, range, gap) = match (&l.simulated, l.logged) {
            (Some(s), logged) => {
                let share = matches!(l.metric, "answered" | "gave_up" | "refused" | "reused");
                let ci = if share {
                    format!("{:.1} pt", s.ci95 * 100.0)
                } else if l.metric == "requests" {
                    String::new()
                } else {
                    format!("{:.2} s", s.ci95)
                };
                let gap = match logged {
                    _ if l.metric == "requests" => String::new(),
                    Some(v) if share => format!("{:+.1} pt", (s.mean - v) * 100.0),
                    // A percentage of next to nothing says nothing.
                    Some(v) if v >= 0.1 => format!("{:+.0}%", (s.mean - v) / v * 100.0),
                    Some(v) => format!("{:+.2} s", s.mean - v),
                    _ => String::new(),
                };
                (
                    show(l.metric, s.mean),
                    ci,
                    format!("{} to {}", show(l.metric, s.min), show(l.metric, s.max)),
                    gap,
                )
            }
            (None, _) => ("–".into(), String::new(), String::new(), String::new()),
        };
        out += &format!(
            "| {} | {} | {logged} | {sim} | {ci} | {range} | {gap} |\n",
            l.kind, l.metric
        );
    }
    out
}
