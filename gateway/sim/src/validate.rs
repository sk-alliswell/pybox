//! Predicted against measured: replay a measured experiment and report the
//! same numbers for both, with the error.

use serde::{Deserialize, Serialize};

use crate::report::{parse_jsonl, records, summarize, Record, Summary};
use crate::scenario::{Arrival, Scenario, TraceRow};
use crate::sim::Sim;

#[derive(Clone, Debug, Serialize)]
pub struct Metric {
    pub name: String,
    /// `share` (0 to 1, error in percentage points), `seconds`, `count` or
    /// `rate` (error relative to the measured value).
    pub unit: &'static str,
    pub measured: Option<f64>,
    /// Mean over the seeds.
    pub predicted: Option<f64>,
    pub predicted_min: Option<f64>,
    pub predicted_max: Option<f64>,
    /// Percentage points for shares, percent of measured otherwise.
    pub error: Option<f64>,
}

#[derive(Clone, Debug, Serialize)]
pub struct Comparison {
    pub scenario: String,
    pub description: String,
    pub seeds: usize,
    /// `measured arrivals` or `poisson`.
    pub arrivals: &'static str,
    pub metrics: Vec<Metric>,
}

/// One measured number of a reference run.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Measured {
    pub name: String,
    pub unit: String,
    pub value: Option<f64>,
}

/// When one class of callers sent its requests in a measured run.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClassArrivals {
    pub class: String,
    /// (seconds from the start of the run, index into `Reference::models`).
    pub sent: Vec<(f64, usize)>,
}

/// What a measured run leaves behind for the accuracy check: when each
/// request was sent and for which model, and the numbers the run produced.
/// It is derived from the client log and holds none of its rows, so it can
/// live in the repository when the log cannot.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Reference {
    pub scenario: String,
    pub models: Vec<String>,
    pub arrivals: Vec<ClassArrivals>,
    pub measured: Vec<Measured>,
}

impl Reference {
    pub fn from_records(sc: &Scenario, rows: &[Record], queue_max: Option<f64>) -> Reference {
        let mut models: Vec<String> = Vec::new();
        let mut arrivals: Vec<ClassArrivals> = Vec::new();
        for r in rows {
            let m = match models.iter().position(|m| *m == r.model) {
                Some(i) => i,
                None => {
                    models.push(r.model.clone());
                    models.len() - 1
                }
            };
            let i = match arrivals.iter().position(|a| a.class == r.cls) {
                Some(i) => i,
                None => {
                    arrivals.push(ClassArrivals {
                        class: r.cls.clone(),
                        sent: Vec::new(),
                    });
                    arrivals.len() - 1
                }
            };
            arrivals[i].sent.push((r.t, m));
        }
        for a in &mut arrivals {
            a.sent.sort_by(|x, y| x.0.total_cmp(&y.0));
        }
        Reference {
            scenario: sc.name.clone(),
            models,
            arrivals,
            measured: numbers(rows, sc, queue_max)
                .into_iter()
                .map(|n| Measured {
                    name: n.name.to_string(),
                    unit: n.unit.to_string(),
                    // Rounded so the file reads back as the same number.
                    value: n.value.map(|v| (v * 1e6).round() / 1e6),
                })
                .collect(),
        }
    }

    pub fn from_log(sc: &Scenario, log: &str, samples: Option<&str>) -> Result<Reference, String> {
        Ok(Reference::from_records(
            sc,
            &parse_jsonl(log)?,
            samples.and_then(measured_queue_max),
        ))
    }
}

/// Replace every open-loop client by the arrivals a measured run recorded
/// for its class, so the model is tested on the same input as the real run.
pub fn with_measured_arrivals(sc: &Scenario, measured: &Reference) -> Scenario {
    let mut sc = sc.clone();
    let mut seen: Vec<String> = Vec::new();
    sc.clients.retain_mut(|c| {
        let class = c.class.clone().unwrap_or_else(|| c.name.clone());
        if matches!(c.arrival, Arrival::Closed { .. }) {
            return true;
        }
        // Several clients of one class (phases of one workload) collapse
        // into the first.
        if seen.contains(&class) {
            return false;
        }
        seen.push(class.clone());
        let rows: Vec<TraceRow> = measured
            .arrivals
            .iter()
            .filter(|a| a.class == class)
            .flat_map(|a| &a.sent)
            .map(|(t, m)| TraceRow {
                t: *t,
                model: measured.models[*m].clone(),
                prompt_tokens: None,
                output_tokens: None,
                prefix: None,
            })
            .collect();
        c.from_s = 0.0;
        c.to_s = f64::MAX;
        c.arrival = Arrival::Trace { rows };
        true
    });
    sc
}

struct Numbers {
    name: &'static str,
    unit: &'static str,
    value: Option<f64>,
}

fn numbers(rows: &[Record], sc: &Scenario, queue_max: Option<f64>) -> Vec<Numbers> {
    let phase = |name: &str| sc.phases.iter().find(|p| p.name == name);
    let class = |cls: &str, ph: Option<&str>| -> Summary {
        let p = ph.and_then(phase);
        summarize(
            rows.iter()
                .filter(|r| r.cls == cls && p.is_none_or(|p| r.t >= p.from_s && r.t < p.to_s)),
        )
    };
    let mut out = Vec::new();
    let mut push = |name, unit, value| out.push(Numbers { name, unit, value });
    let has = |cls: &str| rows.iter().any(|r| r.cls == cls);
    let flood = phase("flood").map(|_| "flood");
    if has("interactive") {
        let (f, a) = (class("interactive", flood), class("interactive", None));
        if flood.is_some() {
            push("interactive answered, flood", "share", f.ok_share);
            push("interactive TTFT p50, flood", "seconds", f.ttft_p50);
            push("interactive TTFT p95, flood", "seconds", f.ttft_p95);
        }
        push("interactive answered, whole run", "share", a.ok_share);
        push("interactive TTFT p50, whole run", "seconds", a.ttft_p50);
        push("interactive total p50, whole run", "seconds", a.total_p50);
    }
    if has("batch") {
        let b = class("batch", None);
        push("batch answered", "share", b.ok_share);
        push("batch TTFT p50", "seconds", b.ttft_p50);
        push("batch TTFT p95", "seconds", b.ttft_p95);
    }
    let all = summarize(rows.iter());
    if let Some(p) = phase("flood") {
        let n = rows
            .iter()
            .filter(|r| r.ok() && r.t >= p.from_s && r.t < p.to_s)
            .count();
        push(
            "completed per minute, flood",
            "rate",
            Some(n as f64 / ((p.to_s - p.from_s) / 60.0)),
        );
    }
    push("gave up at own timeout", "count", Some(all.gave_up as f64));
    push("refused (503)", "count", Some(all.refused as f64));
    push("requests that retried", "count", Some(all.retried as f64));
    let last = rows
        .iter()
        .filter(|r| r.ok())
        .map(|r| r.t + r.total)
        .fold(0.0, f64::max);
    push("last answer at", "seconds", Some(last));
    if queue_max.is_some() {
        push("longest gateway queue", "count", queue_max);
    }
    out
}

/// Longest gateway queue in a `*.samples.jsonl` file from `lab/saturate.py`.
pub fn measured_queue_max(samples: &str) -> Option<f64> {
    samples
        .lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .filter_map(|v| v.get("queued").and_then(serde_json::Value::as_f64))
        .fold(None, |m: Option<f64>, q| Some(m.map_or(q, |m| m.max(q))))
}

pub fn compare(
    sc: &Scenario,
    measured: &Reference,
    seeds: usize,
    poisson: bool,
) -> Result<Comparison, String> {
    let base = if poisson {
        sc.clone()
    } else {
        with_measured_arrivals(sc, measured)
    };
    let with_queue = measured
        .measured
        .iter()
        .any(|m| m.name == "longest gateway queue");
    let mut runs: Vec<Vec<Numbers>> = Vec::new();
    for i in 0..seeds.max(1) {
        let mut s = base.clone();
        s.seed = sc.seed + i as u64;
        let mut sim = Sim::new(s)?;
        sim.metrics.keep_frames = false;
        let mut queue_max: f64 = 0.0;
        // Sample the queue the way the lab does: once a second.
        while !sim.finished() {
            sim.advance(sim.now + 1000.0);
            if let Some(f) = sim.metrics.frames.last() {
                let q: f64 = f
                    .nodes
                    .iter()
                    .filter(|n| n.kind == "upstream")
                    .map(|n| n.queue)
                    .sum();
                queue_max = queue_max.max(q);
            }
        }
        sim.close();
        runs.push(numbers(&records(&sim), sc, with_queue.then_some(queue_max)));
    }
    let metrics = measured
        .measured
        .iter()
        .map(|w| {
            let found: Vec<&Numbers> = runs
                .iter()
                .filter_map(|r| r.iter().find(|n| n.name == w.name))
                .collect();
            let unit = match w.unit.as_str() {
                "share" => "share",
                "seconds" => "seconds",
                "count" => "count",
                _ => "rate",
            };
            let vals: Vec<f64> = found.iter().filter_map(|n| n.value).collect();
            let predicted =
                (!vals.is_empty()).then(|| vals.iter().sum::<f64>() / vals.len() as f64);
            let error = match (w.value, predicted) {
                (Some(m), Some(p)) if unit == "share" => Some((p - m) * 100.0),
                (Some(m), Some(p)) if m.abs() > 1e-9 => Some((p - m) / m * 100.0),
                (Some(_), Some(p)) => Some(if p.abs() < 1e-9 { 0.0 } else { f64::INFINITY }),
                _ => None,
            };
            Metric {
                name: w.name.clone(),
                unit,
                measured: w.value,
                predicted,
                predicted_min: vals.iter().copied().reduce(f64::min),
                predicted_max: vals.iter().copied().reduce(f64::max),
                error,
            }
        })
        .collect();
    Ok(Comparison {
        scenario: sc.name.clone(),
        description: sc.description.clone(),
        seeds: seeds.max(1),
        arrivals: if poisson {
            "poisson"
        } else {
            "measured arrivals"
        },
        metrics,
    })
}

pub fn compare_text(
    sc: &Scenario,
    measured: &str,
    samples: Option<&str>,
    seeds: usize,
    poisson: bool,
) -> Result<Comparison, String> {
    compare(
        sc,
        &Reference::from_log(sc, measured, samples)?,
        seeds,
        poisson,
    )
}

/// The error of one number in the run the baseline was taken from.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BaselineEntry {
    pub scenario: String,
    pub metric: String,
    pub unit: String,
    /// Percentage points for shares, percent of measured otherwise.
    pub error: f64,
}

/// The accuracy the simulator had when the baseline was written. A change
/// to the model may not make any number worse than this by more than the
/// allowance.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Baseline {
    pub seeds: usize,
    pub entries: Vec<BaselineEntry>,
}

/// Allowance on top of the baseline error: percentage points for shares,
/// percent for the rest. It covers the last-digit differences between
/// platforms' math libraries, which a queue near saturation amplifies.
pub const ALLOW_SHARE_POINTS: f64 = 2.0;
pub const ALLOW_PERCENT: f64 = 4.0;

/// Whether a number says anything about accuracy: small counts move by
/// tens of percent on one request either way.
fn scored(m: &Metric) -> Option<f64> {
    let e = m.error.filter(|e| e.is_finite())?;
    (m.unit != "count" || m.measured.is_some_and(|v| v >= 20.0)).then_some(e)
}

impl Baseline {
    pub fn from(all: &[Comparison]) -> Baseline {
        Baseline {
            seeds: all.first().map_or(0, |c| c.seeds),
            entries: all
                .iter()
                .flat_map(|c| {
                    c.metrics.iter().filter_map(|m| {
                        Some(BaselineEntry {
                            scenario: c.scenario.clone(),
                            metric: m.name.clone(),
                            unit: m.unit.to_string(),
                            error: (scored(m)? * 100.0).round() / 100.0,
                        })
                    })
                })
                .collect(),
        }
    }

    /// (numbers that got worse than the baseline allows, numbers that got
    /// better by more than the allowance). A scored number the baseline does
    /// not know counts as worse: the baseline has to be written again when
    /// the set of numbers changes.
    pub fn check(&self, all: &[Comparison]) -> (Vec<String>, Vec<String>) {
        let (mut worse, mut better) = (Vec::new(), Vec::new());
        for c in all {
            for m in &c.metrics {
                let Some(e) = scored(m) else { continue };
                let Some(b) = self
                    .entries
                    .iter()
                    .find(|b| b.scenario == c.scenario && b.metric == m.name)
                else {
                    worse.push(format!("{}: {} is not in the baseline", c.scenario, m.name));
                    continue;
                };
                let (allow, unit) = if m.unit == "share" {
                    (ALLOW_SHARE_POINTS, " points")
                } else {
                    (ALLOW_PERCENT, "%")
                };
                let line = format!(
                    "{}: {} is off by {:+.1}{unit}, the baseline is {:+.1}{unit}",
                    c.scenario, m.name, e, b.error
                );
                if e.abs() > b.error.abs() + allow {
                    worse.push(line);
                } else if e.abs() < b.error.abs() - allow {
                    better.push(line);
                }
            }
        }
        (worse, better)
    }
}

impl Comparison {
    pub fn text(&self) -> String {
        use std::fmt::Write;
        let mut o = String::new();
        let _ = writeln!(
            o,
            "{} ({}, {} seeds)",
            self.scenario, self.arrivals, self.seeds
        );
        let _ = writeln!(
            o,
            "  {:<36}{:>10}{:>11}{:>19}{:>10}",
            "metric", "measured", "predicted", "range over seeds", "error"
        );
        for m in &self.metrics {
            let show = |v: Option<f64>| match (v, m.unit) {
                (None, _) => "-".to_string(),
                (Some(v), "share") => format!("{:.0}%", v * 100.0),
                (Some(v), "count") => format!("{v:.0}"),
                (Some(v), _) => format!("{v:.2}"),
            };
            let err = match (m.error, m.unit) {
                (None, _) => "-".to_string(),
                (Some(e), "share") => format!("{e:+.0} pt"),
                (Some(e), _) if e.is_finite() => format!("{e:+.0}%"),
                _ => "n/a".to_string(),
            };
            let _ = writeln!(
                o,
                "  {:<36}{:>10}{:>11}{:>19}{:>10}",
                m.name,
                show(m.measured),
                show(m.predicted),
                format!("{} .. {}", show(m.predicted_min), show(m.predicted_max)),
                err
            );
        }
        o
    }
}
