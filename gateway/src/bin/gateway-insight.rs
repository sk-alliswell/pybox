//! Productivity and improvement report from exported request rows.
//!
//!     gateway-insight ROWS.jsonl [--meta META.json] [--tz 9] [--day-s N]
//!         [--server-hour-usd X] [--slots N] [--json OUT] [--html OUT]
//!     gateway-insight --demo [--rows OUT.jsonl] [--meta-out OUT.json] ...
//!
//! ROWS is what `/gateway/obs/export` writes (one request per line). META
//! holds `{"deliverable": {id: {...}}, "link": {...}, "rate": {...}}`, the
//! same objects `/gateway/obs/meta/*` stores. `--demo` makes up a week of
//! traffic with known problems planted in it, to show the page; nothing in
//! it is measured. `--html` writes the dashboard page with the report inside.

use std::collections::BTreeMap;
use std::f64::consts::PI;

use pybox_gateway::obs::insight::{self, Options};
use pybox_gateway::obs::store::{Backend, Memory, Row, Table};
use serde_json::{json, Value};

const PAGE: &str = include_str!("../obs/roi.html");

fn die(msg: impl AsRef<str>) -> ! {
    eprintln!("gateway-insight: {}", msg.as_ref());
    std::process::exit(2)
}

struct Lcg(u64);
impl Lcg {
    fn f(&mut self) -> f64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        ((self.0 >> 11) as f64) / ((1u64 << 53) as f64)
    }
    fn range(&mut self, lo: f64, hi: f64) -> f64 {
        lo + (hi - lo) * self.f()
    }
    fn exp(&mut self, mean: f64) -> f64 {
        -mean * (1.0 - self.f()).ln()
    }
    /// Hour of day drawn from a bell around `centre`.
    fn hour(&mut self, centre: f64, sigma: f64) -> f64 {
        let n = (-2.0 * (1.0 - self.f()).ln()).sqrt() * (2.0 * PI * self.f()).cos();
        (centre + sigma * n).clamp(0.0, 23.99)
    }
}

/// A made-up working week: 3 teams of 12, two large-model servers and one
/// small-model server, with a handful of habits planted so every rule has
/// something to find.
fn demo() -> (Vec<Row>, Value) {
    let mut g = Lcg(20261118);
    // Monday 2026-11-09 00:00 KST.
    let week0: i64 = 1_794_150_000_000;
    let teams = ["platform", "product", "research"];
    let (big, small) = ("large-32b", "small-4b");
    // $ per million tokens: (input, output). Example prices, not quotes.
    let price = |m: &str| if m == big { (0.60, 2.40) } else { (0.06, 0.24) };
    let mut rows = Vec::new();
    let mut deliverables = serde_json::Map::new();
    let kinds = ["pr", "report", "doc", "ticket"];
    for team in teams {
        for u in 1..=12 {
            let user = format!("{team}-{u}");
            let who = (team, u);
            // Planted habits.
            let no_cache = who == ("platform", 3);
            let long_context = who == ("product", 2);
            let quitter = who == ("platform", 6);
            let big_for_small = who == ("product", 5) || who == ("product", 9);
            let batch_runner = who == ("research", 1);
            let rejected_a_lot = who == ("product", 7);
            let untagged = who == ("research", 4) || team == "platform" && u >= 10;
            let mine: Vec<String> = (0..4)
                .map(|i| {
                    let id = format!("{team}-{u}-d{i}");
                    let roll = g.f();
                    let bad = if rejected_a_lot {
                        0.6
                    } else if team == "product" {
                        0.22
                    } else {
                        0.1
                    };
                    let status = if roll < bad {
                        "rejected"
                    } else if roll < bad + 0.2 {
                        "in_progress"
                    } else {
                        "accepted"
                    };
                    deliverables.insert(
                        id.clone(),
                        json!({
                            "project_id": format!("{team}-main"), "team_id": team,
                            "kind": kinds[(g.f() * 4.0) as usize % 4],
                            "title": format!("{user} 작업 {}", i + 1), "status": status,
                            "hours_saved": (g.range(1.0, 7.0) * 2.0).round() / 2.0,
                            "completed_at": week0 + 4 * 86_400_000,
                        }),
                    );
                    id
                })
                .collect();
            let per_day = if batch_runner {
                1500.0
            } else {
                g.range(180.0, 420.0)
            };
            for day in 0..5 {
                let n = (per_day * g.range(0.8, 1.2)) as usize;
                let mut turn = 0u32;
                let mut session = 0u32;
                let mut clock: Vec<f64> = (0..n)
                    .map(|_| g.hour(if team == "research" { 14.5 } else { 13.0 }, 2.6))
                    .collect();
                clock.sort_by(|a, b| a.total_cmp(b));
                for h in clock {
                    let ts = week0 + day * 86_400_000 + (h * 3_600_000.0) as i64;
                    if turn == 0 || g.f() < 0.07 {
                        session += 1;
                        turn = 0;
                    }
                    turn += 1;
                    let batch = batch_runner && g.f() < 0.9 || g.f() < 0.08;
                    let tiny = g.f() < if big_for_small { 0.75 } else { 0.3 };
                    let model = if tiny && !big_for_small || g.f() < 0.1 {
                        small
                    } else {
                        big
                    };
                    let mut input = if tiny {
                        g.range(200.0, 1800.0)
                    } else {
                        1500.0 + f64::from(turn) * g.range(300.0, 800.0)
                    };
                    if long_context {
                        input = input * 3.2 + 6000.0;
                    }
                    let mut output = if tiny {
                        g.range(20.0, 250.0)
                    } else {
                        g.exp(520.0) + 60.0
                    };
                    let reuse = if no_cache {
                        0.06
                    } else if turn == 1 {
                        0.3
                    } else {
                        g.range(0.6, 0.9)
                    };
                    let cached = (input * reuse).round();
                    // The afternoon is short of servers: queueing and slower tokens.
                    let load = (-((h - 13.5) / 2.6).powi(2) / 2.0).exp();
                    let queue = if load > 0.75 {
                        g.exp(4200.0 * load)
                    } else {
                        g.exp(150.0)
                    };
                    let tps = if model == big { 34.0 } else { 90.0 } * (1.0 - 0.45 * load.powi(2));
                    let prompt = 250.0 + (input - cached) / if model == big { 2.2 } else { 6.0 };
                    let abort =
                        !batch && (g.f() < if quitter { 0.3 } else { 0.02 } || queue > 25_000.0);
                    let error = g.f() < 0.008;
                    if abort || error {
                        output = (output * g.f() * 0.5).round();
                    }
                    let gen = output / tps * 1000.0;
                    let backend = if model == small {
                        "s3"
                    } else if g.f() < 0.68 {
                        "s1"
                    } else {
                        "s2"
                    };
                    let (pi, po) = price(model);
                    let cost = ((input - cached) * pi + cached * pi * 0.1 + output * po) / 1e6;
                    let tagged = !untagged && g.f() < 0.8;
                    let row = json!({
                        "ts": ts, "team_id": team, "user_id": user, "agent_id": format!("{user}-agent"),
                        "session_id": format!("{user}-{day}-{session}"), "run_id": format!("{user}-{day}-{session}"),
                        "deliverable_id": if tagged { mine[(session as usize + day as usize) % 4].clone() } else { String::new() },
                        "model": model, "priority": if batch { "batch" } else { "interactive" },
                        "backend": backend, "status": if error { 502 } else { 200 }, "is_error": error,
                        "client_abort": abort && !error, "queue_ms": queue, "ttft_ms": queue + prompt,
                        "gen_ms": gen, "total_ms": queue + prompt + gen, "input_tokens": input.round(),
                        "output_tokens": output.round(), "cache_read_tokens": cached, "cost_usd": cost,
                        "output_tps": tps * g.range(0.9, 1.1), "max_chunk_gap_ms": g.exp(180.0),
                    });
                    rows.push(serde_json::from_value::<Row>(row).unwrap());
                }
            }
        }
    }
    let meta = json!({
        "deliverable": deliverables,
        "rate": {"*": {"hourly_usd": 60.0}},
    });
    (rows, meta)
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut opts = Options::default();
    let mut path = None;
    let (mut meta_path, mut json_out, mut html_out, mut rows_out, mut meta_out) =
        (None, None, None, None, None);
    let mut is_demo = false;
    let mut label = String::new();
    let mut i = 0;
    while i < args.len() {
        let arg = args[i].clone();
        let mut val = || {
            i += 1;
            args.get(i)
                .cloned()
                .unwrap_or_else(|| die("missing value for an option"))
        };
        let numf = |s: String| -> f64 {
            s.parse()
                .unwrap_or_else(|_| die(format!("not a number: {s}")))
        };
        match arg.as_str() {
            "--demo" => is_demo = true,
            "--meta" => meta_path = Some(val()),
            "--json" => json_out = Some(val()),
            "--html" => html_out = Some(val()),
            "--rows" => rows_out = Some(val()),
            "--meta-out" => meta_out = Some(val()),
            "--label" => label = val(),
            "--tz" => opts.tz_hours = numf(val()),
            "--day-s" => opts.day_s = numf(val()),
            "--server-hour-usd" => opts.server_hour_usd = Some(numf(val())),
            "--slots" => opts.slots = Some(numf(val())),
            a if a.starts_with("--") => die(format!("unknown option {a}")),
            a => path = Some(a.to_string()),
        }
        i += 1;
    }
    let (rows, meta): (Vec<Row>, Value) = if is_demo {
        demo()
    } else {
        let path = path.unwrap_or_else(|| die("give ROWS.jsonl or --demo"));
        let body = std::fs::read_to_string(&path).unwrap_or_else(|e| die(format!("{path}: {e}")));
        let rows = body
            .lines()
            .filter(|l| !l.trim().is_empty())
            .enumerate()
            .map(|(n, l)| {
                serde_json::from_str(l).unwrap_or_else(|e| die(format!("{path}:{}: {e}", n + 1)))
            })
            .collect();
        let meta = meta_path.map_or(json!({}), |p| {
            let body = std::fs::read_to_string(&p).unwrap_or_else(|e| die(format!("{p}: {e}")));
            serde_json::from_str(&body).unwrap_or_else(|e| die(format!("{p}: {e}")))
        });
        (rows, meta)
    };
    if rows.is_empty() {
        die("no rows");
    }
    if let Some(p) = &rows_out {
        let lines: Vec<String> = rows.iter().map(|r| json!(r).to_string()).collect();
        std::fs::write(p, lines.join("\n") + "\n").unwrap_or_else(|e| die(format!("{p}: {e}")));
    }
    if let Some(p) = &meta_out {
        std::fs::write(p, serde_json::to_string_pretty(&meta).unwrap())
            .unwrap_or_else(|e| die(format!("{p}: {e}")));
    }
    let num = |r: &Row, k: &str| r.get(k).and_then(Value::as_f64).unwrap_or(0.0);
    let from = rows.iter().map(|r| num(r, "ts") as i64).min().unwrap();
    let to = rows
        .iter()
        .map(|r| (num(r, "ts") + num(r, "total_ms")) as i64)
        .max()
        .unwrap()
        + 1;
    // Whole days when the data is a real calendar: idle nights then count.
    let day = 86_400_000;
    let (from, to) = if opts.day_s == 86_400.0 {
        let tz = (opts.tz_hours * 3.6e6) as i64;
        (
            (from + tz).div_euclid(day) * day - tz,
            ((to + tz - 1).div_euclid(day) + 1) * day - tz,
        )
    } else {
        (from, to)
    };
    let store = Memory::new(rows.len() + 1);
    store.append(Table::Requests, rows).unwrap();
    if let Some(kinds) = meta.as_object() {
        for (kind, items) in kinds {
            let items: BTreeMap<String, Value> =
                serde_json::from_value(items.clone()).unwrap_or_default();
            for (id, v) in items {
                store.meta_put(kind, &id, v).unwrap();
            }
        }
    }
    let mut report = insight::report(&store, from, to, &opts).unwrap_or_else(|e| die(e));
    let source = if is_demo {
        "예시 데이터 (생성한 값, 실측 아님)".to_string()
    } else if label.is_empty() {
        "내려받은 게이트웨이 로그".to_string()
    } else {
        label
    };
    report["source"] = json!(source);
    report["generated"] = json!(is_demo);
    if let Some(p) = &json_out {
        std::fs::write(p, serde_json::to_string_pretty(&report).unwrap())
            .unwrap_or_else(|e| die(format!("{p}: {e}")));
    }
    if let Some(p) = &html_out {
        // "</" must not end the script block early.
        let data = report.to_string().replace("</", "<\\/");
        let page = PAGE.replace("/*__INSIGHT__*/null", &data);
        std::fs::write(p, page).unwrap_or_else(|e| die(format!("{p}: {e}")));
    }
    let t = &report["total"];
    println!(
        "{source}: 요청 {}건, 사람 {}명, 팀 {}개, 제안 {}건, 비용 기준 {}",
        t["requests"],
        report["people"].as_array().map_or(0, Vec::len),
        report["teams"].as_array().map_or(0, Vec::len),
        report["suggestions"].as_array().map_or(0, Vec::len),
        report["cost_basis"].as_str().unwrap_or(""),
    );
    for s in report["suggestions"]
        .as_array()
        .into_iter()
        .flatten()
        .take(12)
    {
        println!(
            "- [{} {}] {} {}",
            s["scope"].as_str().unwrap_or(""),
            s["target"].as_str().unwrap_or(""),
            s["title"].as_str().unwrap_or(""),
            s["saving_usd"]
                .as_f64()
                .map_or(String::new(), |x| format!("(${x:.2})")),
        );
    }
}
