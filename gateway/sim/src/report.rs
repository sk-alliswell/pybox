//! Turning a finished run, or a measured log, into the same summary numbers
//! so the two can be put side by side.

use serde::{Deserialize, Serialize};

use crate::metrics::{pct, Finding, Frame, NodeLoad};
use crate::sim::{Sim, GAVE_UP};

/// One request as a client log records it. The field names are the ones
/// `lab/saturate.py` writes, so measured logs parse into this directly.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Record {
    /// Seconds from the start of the run to when the caller sent it.
    pub t: f64,
    pub cls: String,
    pub model: String,
    /// 200, an HTTP error, or -1 when the caller gave up.
    pub status: i32,
    #[serde(default)]
    pub queue_ms: f64,
    /// Seconds to the first token; absent when there was none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttft: Option<f64>,
    /// Seconds until the caller had its answer or stopped waiting.
    pub total: f64,
    #[serde(default = "one")]
    pub tries: u32,
    /// Sent again by the caller after a timeout (0 = as first sent).
    #[serde(default)]
    pub resend: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_ms: Option<f64>,
    /// How long the first wait in the gateway's queue lasted (ms). Absent
    /// in client logs, where `queue_ms` of a request tried once stands in.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub waited_ms: Option<f64>,
    /// Whether that wait ended with a seat.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seated: Option<bool>,
    /// Output tokens a second as the caller saw them, from the first token
    /// to the last; absent for answers of one token and for failures.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gen_tps: Option<f64>,
}

fn one() -> u32 {
    1
}

impl Record {
    pub fn ok(&self) -> bool {
        self.status == 200 && self.ttft.is_some()
    }
}

/// Parse a JSON-lines client log. Unknown fields are ignored.
pub fn parse_jsonl(text: &str) -> Result<Vec<Record>, String> {
    text.lines()
        .filter(|l| !l.trim().is_empty())
        .enumerate()
        .map(|(i, l)| serde_json::from_str(l).map_err(|e| format!("line {}: {e}", i + 1)))
        .collect()
}

pub fn records(sim: &Sim) -> Vec<Record> {
    sim.reqs
        .iter()
        .map(|r| {
            let done = r.done_at.unwrap_or(sim.now);
            Record {
                t: r.sent_at / 1000.0,
                cls: sim.metrics.class_name(r.client).to_string(),
                model: sim.models[r.model].clone(),
                status: r.status,
                queue_ms: r.queue_ms,
                ttft: r.first_token_at.map(|t| (t - r.sent_at) / 1000.0),
                total: (done - r.sent_at) / 1000.0,
                tries: r.tries,
                resend: r.resend,
                expected_ms: r.expected_wait_ms,
                waited_ms: r.waited_ms,
                seated: r.waited_ms.map(|_| r.seated),
                gen_tps: (r.status == 200).then(|| r.gen_tps(done)).flatten(),
            }
        })
        .collect()
}

/// The numbers `lab/analyze_sat.py` reports for one group of requests.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct Summary {
    pub sent: usize,
    pub ok: usize,
    pub ok_share: Option<f64>,
    /// Answers whose first token came within `metrics::ON_TIME_S`.
    #[serde(default)]
    pub on_time: usize,
    pub gave_up: usize,
    pub refused: usize,
    pub other_errors: usize,
    pub ttft_p50: Option<f64>,
    pub ttft_p95: Option<f64>,
    pub ttft_p99: Option<f64>,
    pub total_p50: Option<f64>,
    pub total_p95: Option<f64>,
    /// How long a caller that got nothing waited before it knew.
    pub fail_wait_p50: Option<f64>,
    pub retried: usize,
    /// Requests the caller sent again after a timeout.
    #[serde(default)]
    pub resent: usize,
    pub queue_ms_p50: Option<f64>,
    pub queue_ms_p95: Option<f64>,
    /// Output tokens a second one caller saw while its answer was written:
    /// the median answer, and the slow end (5th percentile).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gen_tps_p50: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gen_tps_p5: Option<f64>,
    /// Waiting tickets: requests whose first wait at the gateway lasted
    /// [`TICKET_AFTER_MS`] or more, the point from which the gateway tells
    /// a waiting caller where it stands.
    #[serde(default)]
    pub tickets: usize,
    /// Of those that got a seat: the wait they were told and the wait they
    /// had (s, medians), and the median gap between the two.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ticket_told_p50: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ticket_wait_p50: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ticket_off_p50: Option<f64>,
    /// Share of all tickets that waited more than twice what they were
    /// told and at least [`TICKET_SLACK_S`] longer, seat or no seat.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ticket_late: Option<f64>,
}

/// The live gateway sends its first notice after this long in the queue.
pub const TICKET_AFTER_MS: f64 = 250.0;
/// A ticket is late when the wait was over twice the told one and this much
/// longer (s).
pub const TICKET_SLACK_S: f64 = 2.0;

fn sorted(mut v: Vec<f64>) -> Vec<f64> {
    v.sort_by(f64::total_cmp);
    v
}

pub fn summarize<'a>(rows: impl Iterator<Item = &'a Record>) -> Summary {
    let rows: Vec<&Record> = rows.collect();
    let ok: Vec<&&Record> = rows.iter().filter(|r| r.ok()).collect();
    let ttft = sorted(ok.iter().filter_map(|r| r.ttft).collect());
    let total = sorted(ok.iter().map(|r| r.total).collect());
    let queue = sorted(ok.iter().map(|r| r.queue_ms).collect());
    let gen = sorted(ok.iter().filter_map(|r| r.gen_tps).collect());
    // (told s, waited s, seated) of every request that held a ticket.
    let tickets: Vec<(f64, f64, bool)> = rows
        .iter()
        .filter_map(|r| {
            let told = r.expected_ms?;
            let waited = r.waited_ms.or((r.tries == 1).then_some(r.queue_ms))?;
            let seated = r.seated.unwrap_or(r.status == 200);
            (waited >= TICKET_AFTER_MS).then_some((told / 1000.0, waited / 1000.0, seated))
        })
        .collect();
    let seated: Vec<&(f64, f64, bool)> = tickets.iter().filter(|t| t.2).collect();
    let late = tickets
        .iter()
        .filter(|(told, waited, _)| *waited > told * 2.0 && waited - told >= TICKET_SLACK_S)
        .count();
    let fail = sorted(
        rows.iter()
            .filter(|r| r.status != 200)
            .map(|r| r.total)
            .collect(),
    );
    Summary {
        sent: rows.len(),
        ok: ok.len(),
        ok_share: (!rows.is_empty()).then(|| ok.len() as f64 / rows.len() as f64),
        on_time: ok
            .iter()
            .filter(|r| r.ttft.is_some_and(|t| t <= crate::metrics::ON_TIME_S))
            .count(),
        gave_up: rows.iter().filter(|r| r.status == GAVE_UP).count(),
        refused: rows
            .iter()
            .filter(|r| matches!(r.status, 503 | 429))
            .count(),
        other_errors: rows
            .iter()
            .filter(|r| !matches!(r.status, 200 | GAVE_UP | 503 | 429))
            .count(),
        ttft_p50: pct(&ttft, 50.0),
        ttft_p95: pct(&ttft, 95.0),
        ttft_p99: pct(&ttft, 99.0),
        total_p50: pct(&total, 50.0),
        total_p95: pct(&total, 95.0),
        fail_wait_p50: pct(&fail, 50.0),
        retried: rows.iter().filter(|r| r.tries > 1).count(),
        resent: rows.iter().filter(|r| r.resend > 0).count(),
        queue_ms_p50: pct(&queue, 50.0),
        queue_ms_p95: pct(&queue, 95.0),
        gen_tps_p50: pct(&gen, 50.0),
        gen_tps_p5: pct(&gen, 5.0),
        tickets: tickets.len(),
        ticket_told_p50: pct(&sorted(seated.iter().map(|t| t.0).collect()), 50.0),
        ticket_wait_p50: pct(&sorted(seated.iter().map(|t| t.1).collect()), 50.0),
        ticket_off_p50: pct(
            &sorted(seated.iter().map(|t| (t.0 - t.1).abs()).collect()),
            50.0,
        ),
        ticket_late: (!tickets.is_empty()).then(|| late as f64 / tickets.len() as f64),
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct ClassSummary {
    pub class: String,
    /// `all`, or the name of a `[[phase]]`.
    pub phase: String,
    #[serde(flatten)]
    pub summary: Summary,
}

/// Summaries per class, for the whole run and for each phase (by send time).
pub fn by_class(rows: &[Record], phases: &[crate::scenario::Phase]) -> Vec<ClassSummary> {
    let mut classes: Vec<&str> = Vec::new();
    for r in rows {
        if !classes.contains(&r.cls.as_str()) {
            classes.push(&r.cls);
        }
    }
    let mut out = Vec::new();
    for c in classes {
        out.push(ClassSummary {
            class: c.to_string(),
            phase: "all".into(),
            summary: summarize(rows.iter().filter(|r| r.cls == c)),
        });
        for p in phases {
            out.push(ClassSummary {
                class: c.to_string(),
                phase: p.name.clone(),
                summary: summarize(
                    rows.iter()
                        .filter(|r| r.cls == c && r.t >= p.from_s && r.t < p.to_s),
                ),
            });
        }
    }
    out
}

/// Where the time of answered requests went, as averages per class (ms).
#[derive(Clone, Debug, Default, Serialize)]
pub struct Stages {
    pub class: String,
    pub requests: usize,
    pub gateway_queue_ms: f64,
    pub proxy_ms: f64,
    pub backend_queue_ms: f64,
    pub prefill_ms: f64,
    pub generation_ms: f64,
    /// Network hops and retry pauses: whatever the rest does not explain.
    pub other_ms: f64,
    pub total_ms: f64,
}

#[derive(Clone, Debug, Serialize)]
pub struct Report {
    pub name: String,
    pub seed: u64,
    /// Virtual seconds until the last request was settled.
    pub duration_s: f64,
    pub events: u64,
    pub requests: usize,
    pub classes: Vec<ClassSummary>,
    pub stages: Vec<Stages>,
    pub nodes: Vec<NodeLoad>,
    pub findings: Vec<Finding>,
    pub gateway_mem_peak_mb: f64,
    /// Completions per minute per phase, all classes.
    pub completed_per_min: Vec<(String, f64)>,
    /// CPU of the gateway instances, when the scenario models it: share of
    /// their cores in use, and the mean wait for a core (ms).
    pub gateway_cpu: Option<(f64, f64)>,
    /// Per class: requests with a repeating prompt prefix that reached a
    /// model server, and how many found it in the server's cache.
    pub prefix_cache: Vec<(String, u64, u64)>,
    /// Prompt tokens of requests with a prefix that reached a model server,
    /// and how many of them the server did not have to read.
    pub prefix_tokens: (f64, f64),
    /// Requests a model server put back for want of context room, and the
    /// tokens it had to read again for them.
    #[serde(default)]
    pub preempted: (u64, f64),
    /// Requests turned away by gradual shedding (`shed`).
    pub shed_early: u64,
    /// Unused model-server capacity by group of servers.
    pub idle: Vec<crate::tune::IdlePool>,
    /// The run on the numbers the settings search uses.
    pub scores: crate::tune::Scores,
    /// Sampled state over time; left out of JSON unless asked for.
    #[serde(skip)]
    pub frames: Vec<Frame>,
    #[serde(skip)]
    pub records: Vec<Record>,
}

impl Report {
    pub fn from_sim(sim: &Sim) -> Self {
        let rows = records(sim);
        let classes = by_class(&rows, &sim.sc.phases);
        let mut stages: Vec<Stages> = Vec::new();
        for r in sim.reqs.iter().filter(|r| r.status == 200) {
            let class = sim.metrics.class_name(r.client);
            let i = match stages.iter().position(|s| s.class == class) {
                Some(i) => i,
                None => {
                    stages.push(Stages {
                        class: class.to_string(),
                        ..Stages::default()
                    });
                    stages.len() - 1
                }
            };
            let s = &mut stages[i];
            s.requests += 1;
            s.gateway_queue_ms += r.queue_ms;
            s.proxy_ms += r.proxy_ms;
            s.backend_queue_ms += r.backend_queue_ms;
            s.prefill_ms += r.prefill_ms;
            s.generation_ms += r.gen_ms;
            s.total_ms += r.done_at.unwrap_or(sim.now) - r.sent_at;
        }
        for s in &mut stages {
            let n = s.requests.max(1) as f64;
            for v in [
                &mut s.gateway_queue_ms,
                &mut s.proxy_ms,
                &mut s.backend_queue_ms,
                &mut s.prefill_ms,
                &mut s.generation_ms,
                &mut s.total_ms,
            ] {
                *v /= n;
            }
            s.other_ms = (s.total_ms
                - s.gateway_queue_ms
                - s.proxy_ms
                - s.backend_queue_ms
                - s.prefill_ms
                - s.generation_ms)
                .max(0.0);
        }
        let nodes = sim.metrics.whole_run();
        let findings = sim.metrics.run_findings();
        let completed_per_min = sim
            .sc
            .phases
            .iter()
            .map(|p| {
                let n = rows
                    .iter()
                    .filter(|r| r.ok() && r.t >= p.from_s && r.t < p.to_s)
                    .count();
                (p.name.clone(), n as f64 / ((p.to_s - p.from_s) / 60.0))
            })
            .collect();
        let last_done = sim
            .reqs
            .iter()
            .filter_map(|r| r.done_at)
            .fold(0.0, f64::max);
        Report {
            name: sim.sc.name.clone(),
            seed: sim.sc.seed,
            duration_s: last_done / 1000.0,
            events: sim.events,
            requests: rows.len(),
            classes,
            stages,
            nodes,
            findings,
            gateway_mem_peak_mb: sim.metrics.gateway_mem_peak_mb,
            completed_per_min,
            prefix_tokens: (sim.prefix_tokens, sim.prefix_tokens_saved),
            preempted: (sim.preempted, sim.preempted_tokens),
            shed_early: sim.shed_early,
            prefix_cache: sim
                .metrics
                .frames
                .last()
                .map(|f| {
                    f.classes
                        .iter()
                        .filter(|c| c.prefix_lookups > 0)
                        .map(|c| (c.name.clone(), c.prefix_lookups, c.prefix_hits))
                        .collect()
                })
                .unwrap_or_default(),
            gateway_cpu: sim
                .sc
                .gateway
                .as_ref()
                .filter(|g| g.cores > 0.0 && g.cpu_ms_per_request > 0.0 && sim.now > 0.0)
                .map(|g| {
                    let cores = g.cores.floor().max(1.0) * g.instances.max(1) as f64;
                    (
                        sim.gw_cpu_busy_ms / (cores * sim.now),
                        sim.gw_cpu_wait_ms / sim.gw_cpu_seen.max(1) as f64,
                    )
                }),
            idle: crate::tune::idle(sim),
            scores: crate::tune::scores(sim),
            frames: sim.metrics.frames.clone(),
            records: rows,
        }
    }

    /// A plain-text summary for the terminal.
    pub fn text(&self) -> String {
        use std::fmt::Write;
        let mut o = String::new();
        let f = |v: Option<f64>| v.map_or("-".to_string(), |v| format!("{v:.2}"));
        let _ = writeln!(
            o,
            "{}: {} requests, {:.0} s simulated, {} events (seed {})",
            self.name, self.requests, self.duration_s, self.events, self.seed
        );
        let _ = writeln!(
            o,
            "\n{:<14}{:<12}{:>6}{:>6}{:>7}{:>8}{:>8}{:>9}{:>9}{:>9}{:>9}",
            "class",
            "phase",
            "sent",
            "ok",
            "ok %",
            "gave up",
            "refused",
            "ttft p50",
            "ttft p95",
            "total p50",
            "total p95"
        );
        for c in &self.classes {
            let s = &c.summary;
            let _ = writeln!(
                o,
                "{:<14}{:<12}{:>6}{:>6}{:>7}{:>8}{:>8}{:>9}{:>9}{:>9}{:>9}",
                c.class,
                c.phase,
                s.sent,
                s.ok,
                s.ok_share
                    .map_or("-".into(), |v| format!("{:.0}", v * 100.0)),
                s.gave_up,
                s.refused,
                f(s.ttft_p50),
                f(s.ttft_p95),
                f(s.total_p50),
                f(s.total_p95)
            );
        }
        let _ = writeln!(o, "\nwhere the time of answered requests went (mean ms)");
        let _ = writeln!(
            o,
            "{:<14}{:>10}{:>9}{:>10}{:>9}{:>10}{:>8}{:>9}",
            "class", "gw queue", "proxy", "srv queue", "prefill", "generate", "other", "total"
        );
        for s in &self.stages {
            let _ = writeln!(
                o,
                "{:<14}{:>10.0}{:>9.0}{:>10.0}{:>9.0}{:>10.0}{:>8.0}{:>9.0}",
                s.class,
                s.gateway_queue_ms,
                s.proxy_ms,
                s.backend_queue_ms,
                s.prefill_ms,
                s.generation_ms,
                s.other_ms,
                s.total_ms
            );
        }
        let _ = writeln!(
            o,
            "\n{:<22}{:<14}{:>6}{:>8}{:>11}{:>10}{:>9}",
            "component", "kind", "cap", "util %", "mean queue", "max queue", "wasted %"
        );
        for n in &self.nodes {
            let _ = writeln!(
                o,
                "{:<22}{:<14}{:>6}{:>8}{:>11.1}{:>10.0}{:>9.0}",
                n.id,
                n.kind,
                if n.cap > 0.0 {
                    format!("{:.0}", n.cap)
                } else {
                    "-".into()
                },
                if n.cap > 0.0 {
                    format!("{:.0}", n.util * 100.0)
                } else {
                    "-".into()
                },
                n.mean_queue,
                n.max_queue,
                n.wasted * 100.0
            );
        }
        if self.gateway_mem_peak_mb > 0.0 {
            let _ = writeln!(
                o,
                "\ngateway memory peak: {:.0} MB",
                self.gateway_mem_peak_mb
            );
        }
        let _ = writeln!(o, "\ndiagnosis");
        if self.findings.is_empty() {
            let _ = writeln!(o, "  no component is near its limit");
        }
        for x in &self.findings {
            let _ = writeln!(
                o,
                "  {} {} ({:.0}% of the run)",
                if x.primary { "*" } else { " " },
                explain(x),
                x.share * 100.0
            );
        }
        o
    }
}

/// One finding as a sentence.
pub fn explain(f: &Finding) -> String {
    let cause = f.cause.as_deref().unwrap_or("?");
    match f.code {
        "saturated" => format!(
            "{} is the bottleneck: {:.0}% busy",
            f.node,
            f.value * 100.0
        ),
        "queue_behind" => format!(
            "{} holds {:.0} requests on average, waiting for {}",
            f.node, f.value, cause
        ),
        "concurrency_cap" => format!(
            "{} queues {:.0} requests on average while its backends have room: its concurrency cap is the limit",
            f.node, f.value
        ),
        "dispatch_paused" => format!(
            "{} holds {:.0} requests on average with slots free: dispatch is paused (rate-limit cooldown or pacing)",
            f.node, f.value
        ),
        "unmanaged_queue" => format!(
            "{} has {:.0} requests queued inside it, where they cannot be reordered, refused or cancelled",
            f.node, f.value
        ),
        "wasted_work" => format!(
            "{} spends {:.0}% of its time on requests whose caller already left",
            f.node,
            f.value * 100.0
        ),
        "proxy_cpu" => format!(
            "{} is short of CPU: its workers are busy {:.0}% of the time",
            f.node,
            f.value * 100.0
        ),
        "rate_limited" => format!("{} answers 429 {:.1} times a second", f.node, f.value),
        "tool_server" => format!("{} (tool server) is at {:.0}% and queueing", f.node, f.value * 100.0),
        "log_store" => format!("{} cannot keep up with the log volume", f.node),
        "host_cpu" => format!(
            "host {} wants {:.1}x the cores it has; everything on it slows down",
            f.node, f.value
        ),
        "idle_capacity" => format!(
            "{} is idle {:.0}% of the time while {} has a backlog it is not allowed to take",
            f.node,
            f.value * 100.0,
            cause
        ),
        other => format!("{other}: {}", f.node),
    }
}
