//! Command line for the simulator.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use gwsim::load::load;
use gwsim::report::{parse_jsonl, Report};
use gwsim::{calibrate, usage, validate, Sim};

const USAGE: &str = "\
gwsim: simulate the gateway stack instead of running it

  gwsim run SCENARIO.toml [--seed N] [--json OUT] [--records OUT.jsonl] [--frames OUT.json]
            [--rows OUT.jsonl] [--usage OUT.md]
      Run a scenario and print latency, throughput, queues and the bottleneck.
      --rows writes one line per request with the gateway log's column names;
      --usage writes the usage report on them.
  gwsim usage ROWS.jsonl [--report OUT.md] [--json OUT] [--source TEXT] [--day-s N] [--day-start MS] [--tz HOURS]
              [--slots MODEL=N,...] [--simulated]
      Usage patterns from request rows, simulated or exported from the
      gateway's `requests` table: by team and kind, by hour of day, idle
      servers, heavy callers, cache reuse.
  gwsim replay LOG.jsonl --base SCENARIO.toml [--fit] [--seeds N] [--markdown OUT.md] [--json OUT] [--scenario OUT.json]
      Send the requests of a gateway log (`/gateway/obs/export`) again, at
      the recorded times, through the system the base scenario describes,
      and set the result beside what the log says happened. --fit reads
      server speed and callers' patience off the log.
  gwsim check SCENARIO.toml
      Parse and validate a scenario without running it.
  gwsim validate SCENARIO.toml --measured LOG.jsonl [--samples S.jsonl] [--seeds N] [--poisson]
      Replay a measured run and print predicted against measured.
  gwsim validate-all MANIFEST.toml (--data DIR | --reference DIR) [--seeds N] [--json OUT] [--markdown OUT]
                     [--write-reference DIR] [--baseline FILE] [--write-baseline FILE]
      Do that for every experiment a manifest lists, from the measured logs in
      --data or from the reference files derived from them (--write-reference).
      --baseline fails when a number is further off than the baseline allows.
  gwsim calibrate LOG.jsonl --tokens interactive=24,batch=64
      Fit model-server parameters from a measured log.
  gwsim tune SCENARIO.toml [--tries N] [--seeds N] [--json OUT] [--write DIR]
      Try gateway settings and keep, for each of several numbers (errors,
      tokens per second, longest and mean wait, priority gain, server
      backlog), the settings that did best on it. --write saves each as a
      scenario file.
  gwsim resolve SCENARIO.toml
      Print the scenario as JSON with `extends` applied.
  gwsim monitor --wasm GWSIM.wasm --out monitor.html [--validation V.json] SCENARIO.toml...
      Build the self-contained live monitor page.
";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match run(&args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("gwsim: {e}");
            ExitCode::FAILURE
        }
    }
}

struct Args {
    pos: Vec<String>,
    opts: BTreeMap<String, String>,
}

fn parse(args: &[String], flags: &[&str]) -> Result<Args, String> {
    let (mut pos, mut opts) = (Vec::new(), BTreeMap::new());
    let mut it = args.iter();
    while let Some(a) = it.next() {
        match a.strip_prefix("--") {
            Some(k) if flags.contains(&k) => {
                opts.insert(k.to_string(), "true".to_string());
            }
            Some(k) => {
                let v = it.next().ok_or(format!("--{k} needs a value"))?;
                opts.insert(k.to_string(), v.clone());
            }
            None => pos.push(a.clone()),
        }
    }
    Ok(Args { pos, opts })
}

impl Args {
    fn num<T: std::str::FromStr>(&self, key: &str) -> Result<Option<T>, String> {
        self.opts
            .get(key)
            .map(|v| v.parse().map_err(|_| format!("--{key}: bad number {v:?}")))
            .transpose()
    }
    fn first(&self) -> Result<&str, String> {
        self.pos
            .first()
            .map(String::as_str)
            .ok_or_else(|| "missing file argument\n\n".to_string() + USAGE)
    }
}

fn read(path: impl AsRef<Path>) -> Result<String, String> {
    let path = path.as_ref();
    std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))
}

fn write(path: &str, text: &str) -> Result<(), String> {
    std::fs::write(path, text).map_err(|e| format!("{path}: {e}"))
}

fn run(args: &[String]) -> Result<(), String> {
    let Some(cmd) = args.first() else {
        print!("{USAGE}");
        return Ok(());
    };
    let rest = &args[1..];
    match cmd.as_str() {
        "run" => cmd_run(parse(rest, &[])?),
        "usage" => cmd_usage(parse(rest, &["simulated"])?),
        "replay" => cmd_replay(parse(rest, &["fit"])?),
        "check" => {
            let a = parse(rest, &[])?;
            let sc = load(a.first()?)?;
            println!("{}: ok ({} clients)", sc.name, sc.clients.len());
            Ok(())
        }
        "resolve" => {
            let a = parse(rest, &[])?;
            let sc = load(a.first()?)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&sc).map_err(|e| e.to_string())?
            );
            Ok(())
        }
        "validate" => cmd_validate(parse(rest, &["poisson"])?),
        "validate-all" => cmd_validate_all(parse(rest, &["poisson"])?),
        "calibrate" => cmd_calibrate(parse(rest, &[])?),
        "monitor" => cmd_monitor(parse(rest, &[])?),
        "tune" => cmd_tune(parse(rest, &[])?),
        "-h" | "--help" | "help" => {
            print!("{USAGE}");
            Ok(())
        }
        other => Err(format!("unknown command {other:?}\n\n{USAGE}")),
    }
}

fn cmd_run(a: Args) -> Result<(), String> {
    let mut sc = load(a.first()?)?;
    if let Some(seed) = a.num("seed")? {
        sc.seed = seed;
    }
    if let Some(n) = a.num::<usize>("seeds")? {
        return run_seeds(sc, n.max(1), a.num("jobs")?);
    }
    let started = std::time::Instant::now();
    let mut sim = Sim::new(sc)?;
    sim.run();
    let report = Report::from_sim(&sim);
    print!("{}", report.text());
    println!(
        "\nsimulated {:.0} s in {:.0} ms of real time",
        report.duration_s,
        started.elapsed().as_secs_f64() * 1000.0
    );
    let json = |v: &dyn erased::Json| v.json();
    if let Some(p) = a.opts.get("json") {
        write(p, &json(&report))?;
    }
    if let Some(p) = a.opts.get("frames") {
        write(p, &json(&report.frames))?;
    }
    if let Some(p) = a.opts.get("records") {
        let lines: Vec<String> = report.records.iter().map(|r| json(r)).collect();
        write(p, &(lines.join("\n") + "\n"))?;
    }
    if a.opts.contains_key("rows") || a.opts.contains_key("usage") {
        let rows = usage::rows_from_sim(&sim);
        if let Some(p) = a.opts.get("rows") {
            let lines: Vec<String> = rows.iter().map(|r| json(r)).collect();
            write(p, &(lines.join("\n") + "\n"))?;
        }
        if let Some(p) = a.opts.get("usage") {
            let ctx = usage::Context::from_scenario(&sim.sc);
            write(p, &usage::report(&usage::analyze(&rows, &ctx)))?;
        }
    }
    Ok(())
}

/// The same scenario under `n` seeds, several at a time: per class the
/// mean of each number, the half-width of its 95% interval and the range.
fn run_seeds(sc: gwsim::Scenario, n: usize, jobs: Option<usize>) -> Result<(), String> {
    use gwsim::replay::{spread, Spread};
    let jobs = jobs
        .or_else(|| std::thread::available_parallelism().ok().map(usize::from))
        .unwrap_or(1)
        .clamp(1, n);
    let started = std::time::Instant::now();
    let next = std::sync::atomic::AtomicUsize::new(0);
    let reports = std::sync::Mutex::new(Vec::new());
    std::thread::scope(|scope| {
        for _ in 0..jobs {
            scope.spawn(|| loop {
                let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if i >= n {
                    break;
                }
                let mut one = sc.clone();
                one.seed = sc.seed.wrapping_add(i as u64);
                let r = Sim::new(one).map(|mut sim| {
                    sim.run();
                    let mut r = Report::from_sim(&sim);
                    r.frames.clear();
                    r.records.clear();
                    r
                });
                reports.lock().unwrap().push(r);
            });
        }
    });
    let reports: Vec<Report> = reports
        .into_inner()
        .unwrap()
        .into_iter()
        .collect::<Result<_, _>>()?;
    let wall = started.elapsed().as_secs_f64();
    let events: u64 = reports.iter().map(|r| r.events).sum();
    println!(
        "{}: {n} seeds, {jobs} at a time, {:.1} s of real time ({:.2} million events a second)\n",
        sc.name,
        wall,
        events as f64 / wall / 1e6
    );
    println!("| class | number | mean | ± 95% | range |\n|---|---|---|---|---|");
    let classes: Vec<String> = reports[0]
        .classes
        .iter()
        .filter(|c| c.phase == "all")
        .map(|c| c.class.clone())
        .collect();
    for class in classes {
        let get = |f: &dyn Fn(&gwsim::report::Summary) -> Option<f64>| -> Option<Spread> {
            let v: Vec<f64> = reports
                .iter()
                .filter_map(|r| {
                    r.classes
                        .iter()
                        .find(|c| c.class == class && c.phase == "all")
                        .and_then(|c| f(&c.summary))
                })
                .collect();
            spread(&v)
        };
        let share = |name: &str, s: Option<Spread>| {
            if let Some(s) = s {
                println!(
                    "| {class} | {name} | {:.1}% | {:.1} pt | {:.1}% to {:.1}% |",
                    s.mean * 100.0,
                    s.ci95 * 100.0,
                    s.min * 100.0,
                    s.max * 100.0
                );
            }
        };
        let secs = |name: &str, s: Option<Spread>| {
            if let Some(s) = s {
                println!(
                    "| {class} | {name} | {:.2} s | {:.2} s | {:.2} s to {:.2} s |",
                    s.mean, s.ci95, s.min, s.max
                );
            }
        };
        share("answered", get(&|s| s.ok_share));
        share(
            "gave up",
            get(&|s| (s.sent > 0).then(|| s.gave_up as f64 / s.sent as f64)),
        );
        share(
            "refused",
            get(&|s| (s.sent > 0).then(|| s.refused as f64 / s.sent as f64)),
        );
        secs("first token p50", get(&|s| s.ttft_p50));
        secs("first token p95", get(&|s| s.ttft_p95));
        secs("first token p99", get(&|s| s.ttft_p99));
    }
    Ok(())
}

fn cmd_replay(a: Args) -> Result<(), String> {
    let rows = gwsim::replay::parse_log(&read(a.first()?)?)?;
    let base = load(a.opts.get("base").ok_or("replay: --base SCENARIO.toml")?)?;
    let seeds = a.num("seeds")?.unwrap_or(5);
    let t0 = std::time::Instant::now();
    let clock = move || t0.elapsed().as_secs_f64();
    if let Some(p) = a.opts.get("scenario") {
        let f = a
            .opts
            .contains_key("fit")
            .then(|| gwsim::replay::fit(&rows));
        let sc = gwsim::replay::scenario(&base, &rows, f.as_ref())?;
        write(p, &serde_json::to_string(&sc).map_err(|e| e.to_string())?)?;
    }
    let c = gwsim::replay::compare(&base, &rows, seeds, a.opts.contains_key("fit"), &clock)?;
    let text = gwsim::replay::markdown(&c);
    match a.opts.get("markdown") {
        Some(p) => write(p, &text)?,
        None => print!("{text}"),
    }
    if let Some(p) = a.opts.get("json") {
        write(
            p,
            &serde_json::to_string_pretty(&c).map_err(|e| e.to_string())?,
        )?;
    }
    Ok(())
}

fn cmd_usage(a: Args) -> Result<(), String> {
    let rows = usage::parse_rows(&read(a.first()?)?)?;
    let mut ctx = usage::Context {
        source: a.opts.get("source").cloned().unwrap_or_default(),
        simulated: a.opts.contains_key("simulated"),
        ..usage::Context::default()
    };
    if let Some(d) = a.num("day-s")? {
        ctx.day_s = d;
    }
    if let Some(h) = a.num("tz")? {
        ctx.tz_hours = h;
    }
    if let Some(t) = a.num("day-start")? {
        ctx.day_start_ms = t;
    }
    for pair in a.opts.get("slots").into_iter().flat_map(|s| s.split(',')) {
        let (m, n) = pair
            .split_once('=')
            .ok_or(format!("--slots: expected MODEL=N, got {pair:?}"))?;
        let n: f64 = n
            .parse()
            .map_err(|_| format!("--slots: bad number {n:?}"))?;
        ctx.slots.insert(m.to_string(), n);
    }
    let analysis = usage::analyze(&rows, &ctx);
    let text = usage::report(&analysis);
    match a.opts.get("report") {
        Some(p) => write(p, &text)?,
        None => print!("{text}"),
    }
    if let Some(p) = a.opts.get("json") {
        write(
            p,
            &serde_json::to_string_pretty(&analysis).map_err(|e| e.to_string())?,
        )?;
    }
    Ok(())
}

mod erased {
    pub trait Json {
        fn json(&self) -> String;
    }
    impl<T: serde::Serialize> Json for T {
        fn json(&self) -> String {
            serde_json::to_string(self).expect("plain data serialises")
        }
    }
}

fn cmd_validate(a: Args) -> Result<(), String> {
    let sc = load(a.first()?)?;
    let measured = read(
        a.opts
            .get("measured")
            .ok_or("--measured LOG.jsonl is required")?,
    )?;
    let samples = a.opts.get("samples").map(read).transpose()?;
    let c = validate::compare_text(
        &sc,
        &measured,
        samples.as_deref(),
        a.num("seeds")?.unwrap_or(20),
        a.opts.contains_key("poisson"),
    )?;
    print!("{}", c.text());
    Ok(())
}

#[derive(serde::Deserialize)]
struct Manifest {
    experiment: Vec<Experiment>,
}

#[derive(serde::Deserialize)]
struct Experiment {
    scenario: String,
    measured: String,
    #[serde(default)]
    samples: Option<String>,
    /// `calibration` when the scenario's parameters were fitted on this log,
    /// `held-out` when they were not.
    role: String,
}

fn cmd_validate_all(a: Args) -> Result<(), String> {
    let manifest_path = PathBuf::from(a.first()?);
    let dir = manifest_path
        .parent()
        .unwrap_or(Path::new("."))
        .to_path_buf();
    let data = a.opts.get("data").map(PathBuf::from);
    let reference = a.opts.get("reference").map(PathBuf::from);
    if data.is_some() == reference.is_some() {
        return Err("give one of --data DIR and --reference DIR".into());
    }
    let m: Manifest = toml::from_str(&read(&manifest_path)?).map_err(|e| e.to_string())?;
    let seeds = a.num("seeds")?.unwrap_or(20);
    let mut all = Vec::new();
    for e in &m.experiment {
        let sc = load(&dir.join(&e.scenario).to_string_lossy())?;
        let file = format!("{}.json", e.scenario.trim_end_matches(".toml"));
        let measured = match (&data, &reference) {
            (Some(data), _) => {
                let samples = match &e.samples {
                    Some(s) => Some(read(data.join(s))?),
                    None => None,
                };
                validate::Reference::from_log(
                    &sc,
                    &read(data.join(&e.measured))?,
                    samples.as_deref(),
                )?
            }
            (None, Some(dir)) => {
                let p = dir.join(&file);
                serde_json::from_str(&read(&p)?).map_err(|e| format!("{}: {e}", p.display()))?
            }
            (None, None) => unreachable!("checked above"),
        };
        if let Some(out) = a.opts.get("write-reference") {
            write(
                &Path::new(out).join(&file).to_string_lossy(),
                &(serde_json::to_string(&measured).map_err(|e| e.to_string())? + "\n"),
            )?;
        }
        let c = validate::compare(&sc, &measured, seeds, a.opts.contains_key("poisson"))?;
        println!("[{}] {}", e.role, c.text());
        all.push((e.role.clone(), c));
    }
    println!("{}", summary_line(&all));
    if let Some(p) = a.opts.get("json") {
        let v: Vec<serde_json::Value> = all
            .iter()
            .map(|(role, c)| {
                let mut v = serde_json::to_value(c).expect("plain data");
                v["role"] = serde_json::Value::String(role.clone());
                v
            })
            .collect();
        write(
            p,
            &serde_json::to_string_pretty(&v).map_err(|e| e.to_string())?,
        )?;
    }
    if let Some(p) = a.opts.get("markdown") {
        write(p, &markdown(&all))?;
    }
    let runs: Vec<validate::Comparison> = all.iter().map(|(_, c)| c.clone()).collect();
    if let Some(p) = a.opts.get("write-baseline") {
        let b = validate::Baseline::from(&runs);
        write(
            p,
            &(serde_json::to_string_pretty(&b).map_err(|e| e.to_string())? + "\n"),
        )?;
    }
    if let Some(p) = a.opts.get("baseline") {
        let b: validate::Baseline =
            serde_json::from_str(&read(p)?).map_err(|e| format!("{p}: {e}"))?;
        if b.seeds != seeds {
            return Err(format!(
                "{p} was written with --seeds {}, this run used {seeds}",
                b.seeds
            ));
        }
        let (worse, better) = b.check(&runs);
        for l in &better {
            println!("better than the baseline: {l}");
        }
        if !worse.is_empty() {
            return Err(format!(
                "accuracy fell below the baseline:\n  {}",
                worse.join("\n  ")
            ));
        }
        println!(
            "accuracy is within the baseline ({} numbers)",
            b.entries.len()
        );
    }
    Ok(())
}

/// Absolute errors over the held-out experiments: (points for shares,
/// percent for the rest). Counts under 20 and shares that are 100% on both
/// sides say nothing about accuracy and are left out.
fn held_out_errors(all: &[(String, validate::Comparison)]) -> (Vec<f64>, Vec<f64>) {
    let (mut shares, mut others) = (Vec::new(), Vec::new());
    for (_, c) in all.iter().filter(|(role, _)| role == "held-out") {
        for m in &c.metrics {
            let (Some(e), Some(measured), Some(predicted)) = (m.error, m.measured, m.predicted)
            else {
                continue;
            };
            if !e.is_finite() {
                continue;
            }
            match m.unit {
                "share" if measured < 1.0 || predicted < 1.0 => shares.push(e.abs()),
                "share" => {}
                "count" if measured < 20.0 => {}
                _ => others.push(e.abs()),
            }
        }
    }
    shares.sort_by(f64::total_cmp);
    others.sort_by(f64::total_cmp);
    (shares, others)
}

fn summary_line(all: &[(String, validate::Comparison)]) -> String {
    let (shares, others) = held_out_errors(all);
    let q = |v: &[f64], p: f64| gwsim::metrics::pct(v, p).unwrap_or(f64::NAN);
    format!(
        "held-out experiments: {}. Success shares ({} numbers): median error {:.1} points, 90th percentile {:.1}, worst {:.1}. Latency, throughput and counts ({} numbers): median error {:.1}%, 90th percentile {:.1}%, worst {:.1}%.",
        all.iter().filter(|(r, _)| r == "held-out").count(),
        shares.len(),
        q(&shares, 50.0),
        q(&shares, 90.0),
        q(&shares, 100.0),
        others.len(),
        q(&others, 50.0),
        q(&others, 90.0),
        q(&others, 100.0),
    )
}

fn markdown(all: &[(String, validate::Comparison)]) -> String {
    let mut o = format!("{}\n\n", summary_line(all));
    for (role, c) in all {
        o.push_str(&format!(
            "### {} ({role})\n\n{}\n\n| metric | measured | predicted | range over {} seeds | error |\n|---|---|---|---|---|\n",
            c.scenario, c.description, c.seeds
        ));
        for m in &c.metrics {
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
            o.push_str(&format!(
                "| {} | {} | {} | {} to {} | {} |\n",
                m.name,
                show(m.measured),
                show(m.predicted),
                show(m.predicted_min),
                show(m.predicted_max),
                err
            ));
        }
        o.push('\n');
    }
    o
}

fn cmd_calibrate(a: Args) -> Result<(), String> {
    let path = a.first()?;
    let rows = parse_jsonl(&read(path)?)?;
    let mut tokens = BTreeMap::new();
    for pair in a
        .opts
        .get("tokens")
        .ok_or("--tokens class=N[,class=N] is required (output tokens per class)")?
        .split(',')
    {
        let (k, v) = pair.split_once('=').ok_or("--tokens: expected class=N")?;
        tokens.insert(
            k.to_string(),
            v.parse::<f64>()
                .map_err(|_| format!("--tokens: bad number {v:?}"))?,
        );
    }
    let fits = calibrate::fit(&rows, &tokens);
    print!("{}", calibrate::to_toml(&fits, path));
    Ok(())
}

fn cmd_tune(a: Args) -> Result<(), String> {
    let sc = load(a.first()?)?;
    let tries = a.num("tries")?.unwrap_or(40);
    let seeds = a.num("seeds")?.unwrap_or(1);
    let started = std::time::Instant::now();
    let mut t = gwsim::tune::Tuner::new(sc, tries, seeds);
    while !t.step()? {
        let s = t.state();
        eprint!("\rtry {}/{}", s.done, s.total);
    }
    eprintln!();
    let s = t.state();
    let show = |v: Option<f64>| v.map_or("-".to_string(), |v| format!("{v:.2}"));
    let line = |name: &str, sc: &gwsim::tune::Scores| {
        let cells: Vec<String> = sc.iter().map(|v| format!("{:>14}", show(*v))).collect();
        println!("{name:<16}{}", cells.join(""));
    };
    let head: Vec<String> = s.metrics.iter().map(|m| format!("{:>14}", m.key)).collect();
    println!("{:<16}{}", "best for", head.join(""));
    if let Some(b) = &s.baseline {
        line("(as given)", b);
    }
    for p in &s.best {
        line(p.metric, &p.scores);
    }
    println!();
    for p in &s.best {
        match &p.knobs {
            Some(k) => println!(
                "{}: {}",
                p.metric,
                serde_json::to_string(k).map_err(|e| e.to_string())?
            ),
            None => println!("{}: the scenario as given", p.metric),
        }
        if let Some(dir) = a.opts.get("write") {
            let sc = t.scenario(p.pick).ok_or("no such try")?;
            std::fs::create_dir_all(dir).map_err(|e| format!("{dir}: {e}"))?;
            write(
                &format!("{dir}/{}.json", p.metric),
                &serde_json::to_string_pretty(&sc).map_err(|e| e.to_string())?,
            )?;
        }
    }
    println!(
        "\n{} tries of {} run(s) in {:.1} s. Best of what was tried, in the simulator.",
        s.total,
        s.seeds,
        started.elapsed().as_secs_f64()
    );
    if let Some(p) = a.opts.get("json") {
        write(p, &serde_json::to_string(&s).map_err(|e| e.to_string())?)?;
    }
    Ok(())
}

fn cmd_monitor(a: Args) -> Result<(), String> {
    let wasm = std::fs::read(a.opts.get("wasm").ok_or("--wasm FILE is required")?)
        .map_err(|e| format!("wasm: {e}"))?;
    let out = a.opts.get("out").ok_or("--out FILE is required")?;
    let mut scenarios = Vec::new();
    for p in &a.pos {
        let sc = load(p)?;
        // A recorded run, replayed where the browser cannot run the engine.
        let mut sim = Sim::new(sc.clone())?;
        sim.run();
        let report = Report::from_sim(&sim);
        // Whole frames often enough to animate, thinned for large systems.
        let size = report
            .frames
            .first()
            .map_or(0, |f| f.nodes.len() + f.links.len());
        let last = report.frames.len().saturating_sub(1);
        let every = size
            .div_ceil(20)
            .max(report.frames.len().div_ceil(120))
            .max(1);
        let frames = gwsim::metrics::compact(&report.frames, |i| i % every == 0 || i == last);
        let mut recording = serde_json::json!({ "frames": frames, "report": report });
        round(&mut recording);
        scenarios.push(serde_json::json!({
            "name": sc.name,
            "description": sc.description,
            "source": serde_json::to_value(&sc).map_err(|e| e.to_string())?,
            "recording": recording,
        }));
    }
    if scenarios.is_empty() {
        return Err("give at least one scenario".into());
    }
    let validation = match a.opts.get("validation") {
        Some(p) => read(p)?,
        None => "[]".into(),
    };
    let html = include_str!("../../web/monitor.html")
        .replace("/*__WASM_BASE64__*/", &base64(&wasm))
        .replace(
            "/*__SCENARIOS__*/[]",
            &serde_json::to_string(&scenarios)
                .map_err(|e| e.to_string())?
                .replace("</", "<\\/"),
        )
        .replace("/*__VALIDATION__*/[]", &validation.replace("</", "<\\/"));
    write(out, &html)?;
    println!(
        "{out}: {} scenarios, {} KB",
        scenarios.len(),
        html.len() / 1024
    );
    Ok(())
}

/// Three decimals are plenty for a chart and shrink the page a lot.
fn round(v: &mut serde_json::Value) {
    match v {
        serde_json::Value::Number(n) => {
            if let Some(f) = n.as_f64().filter(|f| f.fract() != 0.0) {
                *v = serde_json::json!((f * 1000.0).round() / 1000.0);
            }
        }
        serde_json::Value::Array(a) => a.iter_mut().for_each(round),
        serde_json::Value::Object(o) => o.values_mut().for_each(round),
        _ => {}
    }
}

fn base64(data: &[u8]) -> String {
    const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut o = String::with_capacity(data.len().div_ceil(3) * 4);
    for c in data.chunks(3) {
        let n = u32::from(c[0]) << 16
            | u32::from(*c.get(1).unwrap_or(&0)) << 8
            | u32::from(*c.get(2).unwrap_or(&0));
        o.push(T[(n >> 18) as usize & 63] as char);
        o.push(T[(n >> 12) as usize & 63] as char);
        o.push(if c.len() > 1 {
            T[(n >> 6) as usize & 63] as char
        } else {
            '='
        });
        o.push(if c.len() > 2 {
            T[n as usize & 63] as char
        } else {
            '='
        });
    }
    o
}
