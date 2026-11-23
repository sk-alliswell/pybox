//! The scenario files that ship with the crate, and (when the measured logs
//! are at hand) the accuracy of the replays.

use std::path::{Path, PathBuf};

use gwsim::load::load;
use gwsim::validate;

fn dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("scenarios")
}

#[test]
fn every_shipped_scenario_loads_and_runs() {
    let mut n = 0;
    for e in std::fs::read_dir(dir()).unwrap() {
        let p = e.unwrap().path();
        let name = p.file_name().unwrap().to_string_lossy().into_owned();
        if !name.ends_with(".toml") || name == "validation.toml" {
            continue;
        }
        let mut sc = load(&p.to_string_lossy()).unwrap_or_else(|e| panic!("{name}: {e}"));
        // The fleets take long in a debug build: their first minute will do.
        if sc.clients.len() > 100 {
            sc.max_s = if sc.clients.len() > 500 { 10.0 } else { 60.0 };
        }
        let report = gwsim::run(sc).unwrap_or_else(|e| panic!("{name}: {e}"));
        assert!(report.requests > 0, "{name} sent nothing");
        n += 1;
    }
    assert!(n >= 11);
}

#[test]
fn extends_merges_named_tables() {
    let sc = load(&dir().join("sat-smart.toml").to_string_lossy()).unwrap();
    let g = sc.gateway.unwrap();
    assert_eq!(g.upstreams.len(), 2);
    assert!(g
        .upstreams
        .iter()
        .all(|u| u.strict_priority && u.max_concurrency == 2));
    let inter = sc.clients.iter().find(|c| c.name == "interactive").unwrap();
    // Weight from the variant, retry from round 2, rate from round 1.
    assert_eq!(
        (inter.weight, inter.retry, inter.timeout_s),
        (4, true, Some(15.0))
    );
    assert_eq!(sc.model_servers[0].tokens_per_s, 23.21);
}

const RUNS: [&str; 7] = [
    "sat-direct",
    "sat-plain",
    "sat-bounded",
    "sat-tight",
    "sat-direct-patient",
    "sat-bounded-patient",
    "sat-smart",
];
const HELD_OUT: [&str; 5] = [
    "sat-plain",
    "sat-bounded",
    "sat-tight",
    "sat-bounded-patient",
    "sat-smart",
];

fn reference(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("reference")
        .join(format!("{name}.json"))
}

fn json<T: serde::de::DeserializeOwned>(name: &str) -> T {
    let p = reference(name);
    serde_json::from_str(&std::fs::read_to_string(&p).unwrap())
        .unwrap_or_else(|e| panic!("{}: {e}", p.display()))
}

fn replay(name: &str, change: impl Fn(&mut gwsim::Scenario), seeds: usize) -> validate::Comparison {
    let mut sc = load(&dir().join(format!("{name}.toml")).to_string_lossy()).unwrap();
    change(&mut sc);
    validate::compare(&sc, &json::<validate::Reference>(name), seeds, false).unwrap()
}

/// The accuracy gate: replay every measured run from its reference file and
/// fail when a number is further from the measurement than the baseline
/// allows. After a change that moves the numbers on purpose, write the
/// baseline again (see README, "Keeping the accuracy") and say why in the
/// commit.
#[test]
fn replays_are_as_close_to_the_measurements_as_the_baseline() {
    let baseline: validate::Baseline = json("baseline");
    let all: Vec<validate::Comparison> = RUNS
        .iter()
        .map(|n| replay(n, |_| {}, baseline.seeds))
        .collect();
    let (worse, better) = baseline.check(&all);
    for l in &better {
        eprintln!("better than the baseline: {l}");
    }
    assert!(
        worse.is_empty(),
        "accuracy fell below the baseline:\n  {}",
        worse.join("\n  ")
    );

    // The baseline itself may not drift: whatever it says, the runs the
    // model was not fitted on have to stay this close.
    let (mut shares, mut others) = (Vec::new(), Vec::new());
    for c in all
        .iter()
        .filter(|c| HELD_OUT.contains(&c.scenario.as_str()))
    {
        for m in &c.metrics {
            let Some(e) = m.error.filter(|e| e.is_finite()) else {
                continue;
            };
            match m.unit {
                "share" => shares.push(e.abs()),
                "count" if m.measured.unwrap_or(0.0) < 20.0 => {}
                _ => others.push(e.abs()),
            }
        }
    }
    let median = |v: &mut Vec<f64>| {
        v.sort_by(f64::total_cmp);
        v[v.len() / 2]
    };
    let worst = shares.iter().copied().fold(0.0, f64::max);
    assert!(worst <= 10.0, "a success share is off by {worst:.1} points");
    let (s, o) = (median(&mut shares), median(&mut others));
    assert!(s <= 3.0, "median success share error {s:.1} points");
    assert!(o <= 5.0, "median latency/throughput/count error {o:.1}%");
}

/// The gate has to notice a model that is wrong: a server a fifth slower
/// than the one that was measured.
#[test]
fn the_baseline_catches_a_wrong_model() {
    let baseline: validate::Baseline = json("baseline");
    let slow = replay(
        "sat-bounded",
        |sc| {
            for m in &mut sc.model_servers {
                m.tokens_per_s *= 0.8;
            }
        },
        baseline.seeds,
    );
    let (worse, _) = baseline.check(&[slow]);
    assert!(!worse.is_empty());
}

/// The reference files are what `validate-all --write-reference` derives
/// from the logs; with the logs at hand (`GWSIM_DATA`), check they still are.
#[test]
fn reference_files_match_the_logs() {
    let Ok(data) = std::env::var("GWSIM_DATA") else {
        return;
    };
    for name in RUNS {
        let sc = load(&dir().join(format!("{name}.toml")).to_string_lossy()).unwrap();
        let read = |f: String| std::fs::read_to_string(Path::new(&data).join(f));
        let kept: validate::Reference = json(name);
        let samples = kept
            .measured
            .iter()
            .any(|m| m.name == "longest gateway queue")
            .then(|| read(format!("{name}.samples.jsonl")).unwrap());
        let fresh = validate::Reference::from_log(
            &sc,
            &read(format!("{name}.jsonl")).unwrap(),
            samples.as_deref(),
        )
        .unwrap();
        assert_eq!(
            serde_json::to_string(&fresh).unwrap(),
            serde_json::to_string(&kept).unwrap(),
            "{name}"
        );
    }
}
