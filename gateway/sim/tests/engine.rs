//! The engine against queueing theory and against its own invariants.

use gwsim::report::Report;
use gwsim::scenario::Scenario;
use gwsim::sim::{Sim, GAVE_UP};

fn run(toml: &str) -> (Sim, Report) {
    let mut sim = Sim::new(Scenario::from_toml(toml).expect("scenario")).expect("sim");
    sim.run();
    let r = Report::from_sim(&sim);
    (sim, r)
}

const ONE_SERVER: &str = r#"
[net]
default_ms = 0
[[model_server]]
name = "srv"
models = ["m"]
slots = 1
tokens_per_s = 10
jitter = 0
"#;

#[test]
fn single_server_wait_matches_md1() {
    // Poisson arrivals at 0.7/s, constant 1 s service: Pollaczek-Khinchine
    // gives a mean wait of rho * S / (2 * (1 - rho)) = 1.1667 s.
    let (sim, _) = run(&format!(
        r#"max_s = 40000
{ONE_SERVER}
[[client]]
name = "c"
entry = "srv"
models = ["m"]
arrival = {{ kind = "poisson", rate_per_s = 0.7 }}
to_s = 30000
output_tokens = 11
"#
    ));
    let n = sim.reqs.len() as f64;
    let wait = sim.reqs.iter().map(|r| r.backend_queue_ms).sum::<f64>() / n / 1000.0;
    assert!((wait - 1.1667).abs() < 0.06, "mean wait {wait}");
    let busy = sim.reqs.iter().map(|r| r.gen_ms).sum::<f64>() / 1000.0 / 30000.0;
    assert!((busy - 0.7).abs() < 0.02, "utilisation {busy}");
}

#[test]
fn closed_loop_throughput_is_capacity() {
    // Eight agents with no think time on a one-slot server that needs 0.5 s
    // per answer: two answers a second, each waiting behind seven others.
    let (_, r) = run(&format!(
        r#"{ONE_SERVER}
[[client]]
name = "c"
entry = "srv"
models = ["m"]
arrival = {{ kind = "closed", agents = 8 }}
to_s = 500
output_tokens = 6
"#
    ));
    let s = &r.classes[0].summary;
    assert!((s.ok as f64 / 500.0 - 2.0).abs() < 0.03, "{} answers", s.ok);
    let total = s.total_p50.unwrap();
    assert!((total - 4.0).abs() < 0.1, "response time {total}");
}

const OVERLOAD: &str = r#"
seed = 3
[[proxy]]
name = "litellm"
[[model_server]]
name = "srv"
models = ["m"]
slots = 1
tokens_per_s = 20
prefill_ms = 200
[[client]]
name = "inter"
class = "interactive"
entry = "ENTRY"
models = ["m"]
arrival = { kind = "poisson", rate_per_s = 2.0 }
to_s = 120
output_tokens = 24
timeout_s = 10
weight = 4
"#;

fn overload(entry: &str, extra: &str) -> (Sim, Report) {
    run(&format!("{}{extra}", OVERLOAD.replace("ENTRY", entry)))
}

const GATEWAY: &str = r#"
[gateway]
[[gateway.upstream]]
name = "up"
targets = ["litellm"]
max_concurrency = 2
"#;

#[test]
fn every_request_is_settled_exactly_once() {
    for (entry, extra) in [
        ("litellm", String::new()),
        ("gateway", GATEWAY.to_string()),
        (
            "gateway",
            format!("{GATEWAY}max_queue = 5\nqueue_timeout_s = 3\n"),
        ),
    ] {
        let (sim, r) = overload(entry, &extra);
        assert!(sim
            .reqs
            .iter()
            .all(|q| q.status != 0 && q.done_at.is_some()));
        let s = &r.classes[0].summary;
        assert_eq!(s.ok + s.gave_up + s.refused + s.other_errors, s.sent);
        assert!(s.sent > 150);
        // Nobody waits past their own timeout.
        assert!(r.records.iter().all(|q| q.total <= 10.0 + 1e-6));
        assert!(r
            .records
            .iter()
            .filter(|q| q.status == GAVE_UP)
            .all(|q| (q.total - 10.0).abs() < 1e-6));
    }
}

#[test]
fn same_seed_same_run_and_other_seed_differs() {
    let a = overload("gateway", GATEWAY).1;
    let b = overload("gateway", GATEWAY).1;
    let key = |r: &Report| -> Vec<(i32, u64)> {
        r.records
            .iter()
            .map(|q| (q.status, (q.total * 1e6) as u64))
            .collect()
    };
    assert_eq!(key(&a), key(&b));
    let c = run(&format!(
        "{}{GATEWAY}",
        OVERLOAD
            .replace("ENTRY", "gateway")
            .replace("seed = 3", "seed = 4")
    ))
    .1;
    assert_ne!(key(&a), key(&c));
}

#[test]
fn gateway_never_exceeds_its_concurrency_cap() {
    let (sim, _) = overload("gateway", GATEWAY);
    for f in &sim.metrics.frames {
        let up = f.nodes.iter().find(|n| n.kind == "upstream").unwrap();
        assert!(up.busy <= 2.0 && up.util <= 1.0 + 1e-9);
    }
}

#[test]
fn work_for_callers_who_left_depends_on_the_server() {
    let wasted = |mode: &str| {
        let text = OVERLOAD.replace("ENTRY", "litellm").replace(
            "prefill_ms = 200",
            &format!("prefill_ms = 200\non_disconnect = \"{mode}\""),
        );
        let (_, r) = run(&text);
        (
            r.nodes.iter().find(|n| n.id == "srv").unwrap().wasted,
            r.classes[0].summary.ok,
        )
    };
    let (finish, ok_finish) = wasted("finish");
    let (first, ok_first) = wasted("first_token");
    let (cancel, ok_cancel) = wasted("cancel");
    // Most callers here give up before the first token, so the last two
    // differ by less than the run-to-run noise.
    assert!(
        finish > first && finish > cancel && first > cancel * 0.97,
        "{finish} {first} {cancel}"
    );
    // The less a server wastes, the more callers it answers in time.
    assert!(
        ok_cancel >= ok_first && ok_first > ok_finish,
        "{ok_cancel} {ok_first} {ok_finish}"
    );
}

#[test]
fn bounded_queue_answers_more_than_direct() {
    let direct = overload("litellm", "").1.classes[0].summary.ok;
    let bounded = overload(
        "gateway",
        &format!("{GATEWAY}max_queue = 6\nqueue_timeout_s = 4\n"),
    )
    .1;
    let s = &bounded.classes[0].summary;
    assert!(s.ok > direct * 2, "direct {direct}, bounded {}", s.ok);
    assert!(s.refused > 0);
    // Refusals come quickly, not at the caller's own timeout.
    assert!(s.fail_wait_p50.unwrap() <= 4.0 + 1e-6);
}

#[test]
fn diagnosis_names_the_full_server_not_the_gateway() {
    let (_, r) = overload("gateway", GATEWAY);
    let primary = r.findings.iter().find(|f| f.primary).expect("a bottleneck");
    assert_eq!((primary.code, primary.node.as_str()), ("saturated", "srv"));
    assert!(r
        .findings
        .iter()
        .any(|f| f.code == "queue_behind" && f.cause.as_deref() == Some("srv")));
    // Straight at the server the queue is inside it, out of anyone's control.
    let (_, r) = overload("litellm", "");
    assert!(r.findings.iter().any(|f| f.code == "unmanaged_queue"));
    assert!(r.findings.iter().any(|f| f.code == "wasted_work"));
}

#[test]
fn rate_limit_is_retried_and_reported() {
    let (sim, r) = run(r#"
[gateway]
[[gateway.upstream]]
name = "cloud"
targets = ["api"]
max_concurrency = 50
[[hosted_api]]
name = "api"
models = ["m"]
ttft_ms = { median = 300, sigma = 0.2 }
tokens_per_s = { median = 100 }
rpm = 60
[[client]]
name = "c"
entry = "gateway"
models = ["m"]
arrival = { kind = "poisson", rate_per_s = 3 }
to_s = 120
output_tokens = 20
timeout_s = 30
retry = true
"#);
    // The provider admits a burst of 60, then 60 a minute for the 150 s in
    // which callers are still trying.
    let ok = r.classes[0].summary.ok as f64;
    assert!((170.0..=215.0).contains(&ok), "{ok} answered");
    assert!(sim.reqs.iter().any(|q| q.status == GAVE_UP));
    assert!(r
        .findings
        .iter()
        .any(|f| f.code == "rate_limited" && f.node == "api"));
}

#[test]
fn slow_tool_server_and_log_store_show_up() {
    let (_, r) = run(r#"
[gateway]
[[gateway.upstream]]
name = "cloud"
targets = ["api"]
max_concurrency = 50
[[hosted_api]]
name = "api"
models = ["m"]
ttft_ms = { median = 100 }
tokens_per_s = { median = 200 }
[[mcp_server]]
name = "tools"
concurrency = 1
latency_ms = { median = 1000 }
[db]
rows_per_request = 10
rows_per_s = 5
max_buffer_rows = 200
[[client]]
name = "agents"
entry = "gateway"
models = ["m"]
arrival = { kind = "closed", agents = 10, think_s = 0.1 }
to_s = 200
output_tokens = 20
session = { turns = 5, tool = "tools" }
"#);
    // One tool call a second is all the tool server does, so that is the
    // rate of turns too.
    let ok = r.classes[0].summary.ok as f64;
    assert!((ok / 200.0 - 1.0).abs() < 0.08, "{ok} turns");
    assert!(r
        .findings
        .iter()
        .any(|f| f.code == "tool_server" && f.primary));
    assert!(r.findings.iter().any(|f| f.code == "log_store"));
    let db = r.nodes.iter().find(|n| n.kind == "db").unwrap();
    assert!(db.extra > 0.0, "rows should have been dropped");
}

#[test]
fn proxy_out_of_cpu_is_found() {
    let (_, r) = run(r#"
[[proxy]]
name = "litellm"
cpu_ms_per_request = 40
cpu_ms_after_request = 40
[[hosted_api]]
name = "api"
models = ["m"]
ttft_ms = { median = 200 }
tokens_per_s = { median = 100 }
[[client]]
name = "c"
entry = "litellm"
models = ["m"]
arrival = { kind = "poisson", rate_per_s = 14 }
to_s = 120
output_tokens = 10
"#);
    let p = r.findings.iter().find(|f| f.primary).expect("a bottleneck");
    assert_eq!((p.code, p.node.as_str()), ("proxy_cpu", "litellm"));
    // 14 requests a second need 1.2 CPU-seconds a second from one worker.
    assert!(r.stages[0].proxy_ms > 1000.0);
}

#[test]
fn shared_host_slows_everything_on_it() {
    let scenario = |cores: u32| {
        format!(
            r#"
[[host]]
name = "box"
cores = {cores}
[[model_server]]
name = "a"
host = "box"
models = ["m"]
tokens_per_s = 10
jitter = 0
threads = 2
[[model_server]]
name = "b"
host = "box"
models = ["n"]
tokens_per_s = 10
jitter = 0
threads = 2
[[client]]
name = "c"
entry = "a"
models = ["m"]
arrival = {{ kind = "closed", agents = 1 }}
to_s = 100
output_tokens = 11
[[client]]
name = "d"
entry = "b"
models = ["n"]
arrival = {{ kind = "closed", agents = 1 }}
to_s = 100
output_tokens = 11
"#
        )
    };
    let fast = run(&scenario(4)).1.classes[0].summary.ok as f64;
    let slow = run(&scenario(2)).1.classes[0].summary.ok as f64;
    assert!((fast - 100.0).abs() <= 2.0, "{fast}");
    assert!((slow - 50.0).abs() <= 2.0, "{slow}");
}

#[test]
fn bad_scenarios_are_refused_with_a_reason() {
    let err = |t: &str| Scenario::from_toml(t).unwrap_err();
    assert!(err("").contains("no clients"));
    let base = "[[model_server]]\nname = \"s\"\nmodels = [\"m\"]\ntokens_per_s = 5\n";
    let client = "[[client]]\nname = \"c\"\nentry = \"ENTRY\"\nmodels = [\"MODEL\"]\narrival = { kind = \"poisson\", rate_per_s = 1 }\nto_s = 1\noutput_tokens = 1\n";
    let e = err(&format!(
        "{base}{}",
        client.replace("ENTRY", "nowhere").replace("MODEL", "m")
    ));
    assert!(e.contains("unknown entry"), "{e}");
    let e = err(&format!(
        "{base}{}",
        client.replace("ENTRY", "s").replace("MODEL", "other")
    ));
    assert!(e.contains("cannot serve"), "{e}");
    let e = err(&format!("{base}typo = 1\n"));
    assert!(e.contains("typo"), "{e}");
}

#[test]
fn count_stands_for_that_many_identical_nodes() {
    let sc = Scenario::from_toml(
        r#"
max_s = 120
[gateway]
[[gateway.upstream]]
name = "pool"
targets = ["edge"]
routing = "least_inflight"
max_concurrency = 64

[[proxy]]
name = "edge"
count = 2
backends = ["gpu"]

[[model_server]]
name = "gpu"
count = 3
models = ["m"]
slots = 2
tokens_per_s = 50

[[client]]
name = "team"
count = 5
entry = "gateway"
models = ["m"]
arrival = { kind = "poisson", rate_per_s = 0.5 }
to_s = 60
output_tokens = 20
"#,
    )
    .expect("scenario");
    let names = |v: Vec<&str>| v.join(" ");
    assert_eq!(
        names(sc.model_servers.iter().map(|m| m.name.as_str()).collect()),
        "gpu-1 gpu-2 gpu-3"
    );
    let g = sc.gateway.as_ref().unwrap();
    assert_eq!(g.upstreams[0].targets, ["edge-1", "edge-2"]);
    assert_eq!(sc.proxies[1].backends, ["gpu-1", "gpu-2", "gpu-3"]);
    assert_eq!(sc.clients.len(), 5);
    assert!(sc
        .clients
        .iter()
        .all(|c| c.class.as_deref() == Some("team")));

    let mut sim = Sim::new(sc).expect("sim");
    sim.run();
    let report = gwsim::Report::from_sim(&sim);
    // One class for the five callers, and every server took part.
    assert_eq!(
        report.classes.iter().filter(|c| c.phase == "all").count(),
        1
    );
    let busy = |id: &str| report.nodes.iter().find(|n| n.id == id).unwrap().util;
    assert!(busy("gpu-1") > 0.0 && busy("gpu-2") > 0.0 && busy("gpu-3") > 0.0);
}

/// Tokens flow back over every hop the request went out on.
#[test]
fn answers_travel_back_over_the_hops_of_the_request() {
    let mut sim = Sim::new(
        Scenario::from_toml(
            r#"
max_s = 200
[gateway]
[[gateway.upstream]]
name = "up"
targets = ["proxy"]
max_concurrency = 8

[[proxy]]
name = "proxy"

[[model_server]]
name = "server"
models = ["m"]
slots = 4
tokens_per_s = 100

[[client]]
name = "caller"
entry = "gateway"
models = ["m"]
arrival = { kind = "uniform", rate_per_s = 1.0 }
to_s = 50
output_tokens = 30
"#,
        )
        .expect("scenario"),
    )
    .expect("sim");
    sim.run();
    let frames = &sim.metrics.frames;
    let total = |from: &str, to: &str| -> f64 {
        frames
            .windows(2)
            .map(|w| {
                let dt = w[1].t - w[0].t;
                w[1].links
                    .iter()
                    .filter(|l| l.from == from && l.to == to)
                    .map(|l| l.tokens * dt)
                    .sum::<f64>()
            })
            .sum()
    };
    let answered = sim.reqs.iter().filter(|r| r.status == 200).count() as f64;
    assert!(answered >= 45.0);
    for (from, to) in [
        ("caller", "gateway"),
        ("gateway", "proxy"),
        ("proxy", "server"),
    ] {
        let tokens = total(from, to);
        assert!(
            (tokens - answered * 30.0).abs() <= 30.0,
            "{from} to {to}: {tokens} tokens for {answered} answers"
        );
    }
}

const MIXED: &str = r#"
name = "mixed"
max_s = 200
[gateway]
[[gateway.upstream]]
name = "m"
models = ["m"]
targets = ["px"]
routing = "least_loaded"
headroom = 1.0
max_queue = 500

[[proxy]]
name = "px"
count = 2
workers = 4
routing = "ROUTING"

[[model_server]]
name = "big"
count = 4
models = ["m"]
slots = 8
tokens_per_s = 60
batch_scaling = 0.85

[[model_server]]
name = "tiny"
count = 4
models = ["m"]
slots = 1
tokens_per_s = 30

[[client]]
name = "c"
count = 40
entry = "gateway"
models = ["m"]
arrival = { kind = "poisson", rate_per_s = 0.4 }
to_s = 120
output_tokens = 150
timeout_s = 120
"#;

#[test]
fn headroom_sets_the_cap_from_the_slots_behind() {
    let sc = Scenario::parse(&MIXED.replace("ROUTING", "least_loaded")).unwrap();
    let u = &sc.gateway.as_ref().unwrap().upstreams[0];
    assert_eq!(u.max_concurrency, 4 * 8 + 4);
}

#[test]
fn load_relative_routing_keeps_small_servers_from_overloading() {
    let backlog = |routing: &str| {
        let sc = Scenario::parse(&MIXED.replace("ROUTING", routing)).unwrap();
        let mut sim = gwsim::Sim::new(sc).unwrap();
        sim.run();
        gwsim::tune::scores(&sim)[5].unwrap()
    };
    let (by_count, by_load) = (backlog("least_inflight"), backlog("least_loaded"));
    assert!(by_load < 0.5, "requests waited inside servers: {by_load}");
    assert!(by_count > 4.0 * by_load.max(0.1), "{by_count} vs {by_load}");
}

#[test]
fn idle_capacity_adds_up() {
    let sc = Scenario::parse(&MIXED.replace("ROUTING", "least_loaded")).unwrap();
    let mut sim = gwsim::Sim::new(sc).unwrap();
    sim.run();
    let idle = gwsim::tune::idle(&sim);
    assert_eq!(idle.len(), 2);
    for p in &idle {
        assert_eq!(p.servers, 4);
        assert!((0.0..=1.0).contains(&p.idle_share), "{p:?}");
        assert!(p.tokens_made as f64 <= p.tokens_at_full * 1.1, "{p:?}");
    }
}

#[test]
fn search_never_ends_worse_than_it_started() {
    let mut sc = Scenario::parse(&MIXED.replace("ROUTING", "random")).unwrap();
    sc.max_s = 150.0;
    let t = gwsim::tune::tune(sc, 8, 1).unwrap();
    let s = t.state();
    let base = s.baseline.unwrap();
    for (m, p) in s.best.iter().enumerate() {
        let (Some(got), Some(was)) = (p.scores[m], base[m]) else {
            continue;
        };
        if s.metrics[m].higher_is_better {
            assert!(got >= was - 1e-9, "{}: {got} < {was}", p.metric);
        } else {
            assert!(got <= was + 1e-9, "{}: {got} > {was}", p.metric);
        }
        // The scenario it hands back runs.
        gwsim::Sim::new(t.scenario(p.pick).unwrap()).unwrap();
    }
}

#[test]
fn rhythm_moves_the_busy_hours() {
    let r = gwsim::scenario::Rhythm {
        period_s: 240.0,
        busy_at_s: 60.0,
        busy_width_s: 80.0,
        quiet: 0.1,
        shift_s: 0.0,
        plateau: None,
        bell: None,
    };
    assert!((r.level(60.0) - 1.0).abs() < 1e-9);
    assert!((r.level(180.0) - 0.1).abs() < 1e-9);
    assert!((r.level(300.0) - 1.0).abs() < 1e-9, "repeats every period");
    let late = gwsim::scenario::Rhythm { shift_s: 60.0, ..r };
    assert!((late.level(120.0) - 1.0).abs() < 1e-9);

    // A working day: rising from 8, full from 10 to 17, down by 22.
    let h = 10.0;
    let day = gwsim::scenario::Rhythm {
        plateau: Some([8.0 * h, 10.0 * h, 17.0 * h, 22.0 * h]),
        ..r
    };
    assert!((day.level(3.0 * h) - 0.1).abs() < 1e-9, "night");
    assert!(
        day.level(9.0 * h) > 0.1 && day.level(9.0 * h) < 1.0,
        "rising"
    );
    assert!((day.level(10.0 * h) - 1.0).abs() < 1e-9);

    // A bell around 13:30 with a sigma of three hours: two halves six hours
    // apart peak lower than both together.
    let bell = gwsim::scenario::Rhythm {
        bell: Some([13.5 * h, 3.0 * h]),
        ..r
    };
    assert!((bell.level(13.5 * h) - 1.0).abs() < 1e-9);
    assert!((bell.level(10.5 * h) - bell.level(16.5 * h)).abs() < 1e-9);
    let (early, late) = (
        gwsim::scenario::Rhythm {
            shift_s: -3.0 * h,
            ..bell
        },
        gwsim::scenario::Rhythm {
            shift_s: 3.0 * h,
            ..bell
        },
    );
    let peak = (0..240)
        .map(|i| 0.5 * (early.level(i as f64) + late.level(i as f64)))
        .fold(0.0, f64::max);
    assert!(peak < 0.75, "staggered halves peak at {peak}");
    assert!((day.level(16.9 * h) - 1.0).abs() < 1e-9);
    assert!(day.level(20.0 * h) < day.level(18.0 * h), "falling");
    assert!((day.level(23.0 * h) - 0.1).abs() < 1e-9);
    assert!(
        (day.level(240.0 + 12.0 * h) - 1.0).abs() < 1e-9,
        "the next day"
    );

    // Arrivals follow it: most requests are sent in the busy stretch.
    let text = MIXED.replace("ROUTING", "least_loaded").replace(
        "to_s = 120",
        "to_s = 240\nrhythm = { period_s = 240, busy_at_s = 60, busy_width_s = 80, quiet = 0.05 }",
    );
    let mut sc = Scenario::parse(&text).unwrap();
    sc.max_s = 400.0;
    let mut sim = gwsim::Sim::new(sc).unwrap();
    sim.run();
    let busy = sim
        .reqs
        .iter()
        .filter(|r| (20_000.0..100_000.0).contains(&r.sent_at))
        .count();
    assert!(
        busy * 10 > sim.reqs.len() * 7,
        "{busy} of {}",
        sim.reqs.len()
    );
}

#[test]
fn gateway_cpu_and_cache_change_what_callers_see() {
    let run = |extra: &str| {
        let text = MIXED
            .replace("ROUTING", "least_loaded")
            .replace("[gateway]\n", &format!("[gateway]\n{extra}\n"));
        let mut sim = gwsim::Sim::new(Scenario::parse(&text).unwrap()).unwrap();
        sim.run();
        let s = gwsim::tune::scores(&sim);
        (
            s[3].unwrap(),
            Report::from_sim(&sim),
            sim.cache_hits,
            sim.reqs.len(),
        )
    };
    let (plain, rep, hits, _) = run("");
    assert!(rep.gateway_cpu.is_none());
    assert_eq!(hits, 0);
    // One slow core: requests wait for the gateway itself.
    let (slow, rep, _, _) = run("cores = 1\ncpu_ms_per_request = 55");
    let (util, wait) = rep.gateway_cpu.unwrap();
    assert!(util > 0.5 && wait > 10.0, "util {util}, wait {wait}");
    assert!(slow > plain + 0.05, "{slow} vs {plain}");
    // Half the requests come from the cache: they never reach a server.
    let (cached, _, hits, n) = run("cache = { hit_rate = 0.5, lookup_ms = 2 }");
    assert!(
        hits * 10 > n as u64 * 4 && hits * 10 < n as u64 * 6,
        "{hits} of {n}"
    );
    assert!(cached < plain, "{cached} vs {plain}");
}

#[test]
fn gateway_instances_split_the_cap_and_pin_callers() {
    let text = MIXED
        .replace("ROUTING", "least_loaded")
        .replace("[gateway]\n", "[gateway]\ninstances = 3\n");
    let sc = Scenario::parse(&text).unwrap();
    let ups = &sc.gateway.as_ref().unwrap().upstreams;
    assert_eq!(ups.len(), 3);
    assert!(ups.iter().all(|u| u.max_concurrency == 12));
    assert_eq!(sc.upstream_at("m", sc.instance_of(4)), Some(1));
    // Parsing what was written out again changes nothing.
    let again = Scenario::parse(&serde_json::to_string(&sc).unwrap()).unwrap();
    assert_eq!(again.gateway.as_ref().unwrap().upstreams.len(), 3);
    let mut sim = gwsim::Sim::new(sc).unwrap();
    sim.run();
    assert!(sim.reqs.iter().filter(|r| r.status == 200).count() > 100);
}

#[test]
fn prompt_cache_shortens_the_wait_for_the_first_token() {
    let run = |extra: &str| {
        let text = MIXED.replace("ROUTING", "least_loaded").replace(
            "tokens_per_s = 60",
            &format!("tokens_per_s = 60\nprefill_ms = 400\n{extra}"),
        );
        let mut sim = gwsim::Sim::new(Scenario::parse(&text).unwrap()).unwrap();
        sim.run();
        gwsim::tune::scores(&sim)[3].unwrap()
    };
    assert!(run("prompt_cache_hit_rate = 0.9") < run("") - 0.1);
}

const PREFIX: &str = r#"
seed = 5
[[proxy]]
name = "litellm"
routing = "ROUTING"
[[model_server]]
name = "srv"
count = 4
models = ["m"]
slots = 4
tokens_per_s = 60
prefill_ms = 20
prefill_tokens_per_s = 2000
batch_scaling = 1
prompt_cache_saving = 0.95
prefix_cache_entries = 8
[[client]]
name = "dev"
count = 24
entry = "litellm"
models = ["m"]
arrival = { kind = "poisson", rate_per_s = 0.05 }
to_s = 600
prompt_tokens = 4000
output_tokens = 40
prefix = { share = 0.9, SCOPE }
"#;

#[test]
fn prefix_cache_hits_come_from_where_requests_are_sent() {
    let run = |routing: &str, scope: &str| {
        let text = PREFIX.replace("ROUTING", routing).replace("SCOPE", scope);
        let mut sim = gwsim::Sim::new(Scenario::parse(&text).unwrap()).unwrap();
        sim.run();
        let rate = sim.prefix_hits as f64 / sim.prefix_lookups as f64;
        (rate, gwsim::tune::scores(&sim)[3].unwrap())
    };
    // 24 callers with their own context, 8 cache entries on each of four
    // servers: spreading by load finds a caller's context on the chosen
    // server now and then, sending a caller to the same server nearly
    // always, and the first token comes sooner.
    let (spread, wait_spread) = run("least_loaded", "scope = \"caller\"");
    let (sticky, wait_sticky) = run("affinity", "scope = \"caller\"");
    assert!(spread < 0.6, "{spread}");
    assert!(sticky > spread + 0.15, "{sticky} vs {spread}");
    assert!(
        wait_sticky < wait_spread,
        "{wait_sticky} vs {wait_spread}, hits {sticky} vs {spread}"
    );
    // One template for everyone is warm on every server either way.
    let (shared, _) = run("least_loaded", "scope = \"group\"");
    assert!(shared > 0.95, "{shared}");
}

const POLICY: &str = r#"
seed = 7
[gateway]
[[gateway.upstream]]
name = "up"
targets = ["srv"]
routing = "ROUTING"
headroom = 1.0
max_queue = 40
EXTRA
[[model_server]]
name = "srv"
count = 4
models = ["m"]
slots = 2
tokens_per_s = 60
batch_scaling = 1
prefill_ms = 20
prefill_tokens_per_s = 2000
prompt_cache_saving = 0.95
[[client]]
name = "dev"
count = 16
weight = 4
entry = "gateway"
models = ["m"]
arrival = { kind = "poisson", rate_per_s = RATE }
to_s = 300
prompt_tokens = 4000
output_tokens = 60
timeout_s = 120
prefix = { share = 0.8, group_share = 0.4, group = "dev", follow_up = 0.6 }
[[client]]
name = "bulk"
count = 6
weight = 1
entry = "gateway"
models = ["m"]
arrival = { kind = "poisson", rate_per_s = BULK }
to_s = 300
prompt_tokens = 1000
output_tokens = 60
timeout_s = 120
prefix = { share = 0.5, scope = "group" }
"#;

fn policy(routing: &str, extra: &str, rate: f64, bulk: f64) -> (Sim, Report) {
    run(&POLICY
        .replace("ROUTING", routing)
        .replace("EXTRA", extra)
        .replace("RATE", &rate.to_string())
        .replace("BULK", &bulk.to_string()))
}

fn class<'a>(r: &'a Report, name: &str) -> &'a gwsim::report::Summary {
    &r.classes
        .iter()
        .find(|c| c.class == name && c.phase == "all")
        .unwrap()
        .summary
}

#[test]
fn slot_cache_misses_what_a_busy_slot_holds_and_cost_routing_reads_least() {
    let reuse = |routing: &str, extra: &str| {
        let (sim, _) = policy(routing, extra, 0.08, 0.3);
        sim.prefix_tokens_saved / sim.prefix_tokens
    };
    let (spread, sticky, cost) = (
        reuse("least_loaded", ""),
        reuse("affinity", ""),
        reuse("cost", ""),
    );
    // Nothing can save more than every prompt finding its whole prefix.
    assert!(cost < 0.8 * 0.95, "{cost}");
    assert!(
        cost > spread && cost > sticky,
        "cost {cost}, spread {spread}, affinity {sticky}"
    );
    // A server-wide cache also finds prefixes whose slot is busy.
    let (sim, _) = run(&POLICY
        .replace("ROUTING", "cost")
        .replace("EXTRA", "")
        .replace("RATE", "0.08")
        .replace("BULK", "0.3")
        .replace("slots = 2", "slots = 2\nprefix_cache_entries = 2"));
    let shared = sim.prefix_tokens_saved / sim.prefix_tokens;
    assert!(shared > cost, "shared {shared}, per slot {cost}");
}

#[test]
fn gradual_shedding_turns_low_weights_away_first() {
    let (_, full) = policy("least_loaded", "", 0.2, 3.0);
    let (sim, shed) = policy(
        "least_loaded",
        "shed = { from = 0.25, low_ceiling = 0.75 }",
        0.2,
        3.0,
    );
    assert!(sim.shed_early > 0);
    let (dev, bulk) = (class(&shed, "dev"), class(&shed, "bulk"));
    // The top weight is only refused by a full queue, which shedding
    // keeps from filling.
    assert!(dev.refused <= class(&full, "dev").refused);
    assert!(dev.ok_share.unwrap() > class(&full, "dev").ok_share.unwrap());
    assert!(bulk.ok_share.unwrap() < class(&full, "bulk").ok_share.unwrap());
}

#[test]
fn least_attained_service_serves_light_users_sooner() {
    let heavy = r#"
[[client]]
name = "heavy"
count = 2
weight = 4
entry = "gateway"
models = ["m"]
arrival = { kind = "poisson", rate_per_s = 1.5 }
to_s = 300
prompt_tokens = 4000
output_tokens = 60
timeout_s = 120
"#;
    let go = |extra: &str| {
        run(&format!(
            "{}{heavy}",
            POLICY
                .replace("ROUTING", "least_loaded")
                .replace("EXTRA", extra)
                .replace("RATE", "0.08")
                .replace("BULK", "0.3")
        ))
        .1
    };
    let (rr, las) = (go(""), go("least_attained_half_life_s = 60"));
    assert!(
        class(&las, "dev").ttft_p95.unwrap() < class(&rr, "dev").ttft_p95.unwrap(),
        "{:?} {:?}",
        class(&las, "dev").ttft_p95,
        class(&rr, "dev").ttft_p95
    );
    // What it does to the heavy callers depends on the load: taking turns
    // by tokens already holds their long prompts back, so nothing is
    // asserted about them.
}

const KNEE: &str = r#"
seed = 3
[[model_server]]
name = "srv"
models = ["m"]
slots = SLOTS
tokens_per_s = 40
batch_scaling = 0.85
EXTRA
jitter = 0
[[client]]
name = "dev"
entry = "srv"
models = ["m"]
arrival = { kind = "closed", agents = 64 }
to_s = 300
prompt_tokens = 100
output_tokens = 200
"#;

#[test]
fn past_its_knee_a_server_keeps_its_total_and_each_caller_gets_less() {
    let go = |slots: &str, extra: &str| {
        let (sim, rep) = run(&KNEE.replace("SLOTS", slots).replace("EXTRA", extra));
        let tokens: f64 = sim
            .reqs
            .iter()
            .filter(|r| r.status == 200)
            .map(|r| f64::from(r.out_tokens))
            .sum();
        (tokens, class(&rep, "dev").gen_tps_p50.unwrap())
    };
    // Eight at a time, the rest wait outside: each answer is written at the
    // speed of a batch of eight.
    let (at_knee, fast) = go("8", "");
    assert!((fast - 40.0 * 8f64.powf(-0.15)).abs() < 1.5, "{fast}");
    // All 64 let in, the knee still at eight: about the same total, and
    // each caller sees an eighth of the speed.
    let (crowded, slow) = go("64", "batch_knee = 8");
    assert!(
        (crowded / at_knee - 1.0).abs() < 0.08,
        "{crowded} {at_knee}"
    );
    assert!((slow / fast - 0.125).abs() < 0.02, "{slow} {fast}");
    // With a decay the total falls too: (64 / 8)^-0.2 = 0.66.
    let (decayed, _) = go("64", "batch_knee = 8\noverload_decay = 0.2");
    assert!(
        (decayed / crowded - 0.66).abs() < 0.06,
        "{decayed} {crowded}"
    );
}

#[test]
fn measured_batching_shape_is_reproduced() {
    // reference/speed-under-load/results.json: tokens a second of each of
    // n sequences decoded together.
    let measured = [
        (1.0, 44.21),
        (2.0, 35.75),
        (4.0, 32.27),
        (8.0, 20.52),
        (16.0, 12.92),
        (32.0, 7.63),
        (64.0, 4.05),
    ];
    let text = KNEE
        .replace("SLOTS", "64")
        .replace("EXTRA", "batch_half = 6.36");
    let sc = Scenario::parse(&text).unwrap();
    for (n, each) in measured {
        let got = 44.21 * sc.model_servers[0].share(n);
        assert!((got / each - 1.0).abs() < 0.08, "{n}: {got} vs {each}");
    }
}

#[test]
fn a_measured_sweep_is_followed_point_by_point() {
    // reference/models/inferencex.json: gpt-oss-120b on two H100 with
    // vLLM, 1,024-token prompts and answers, tokens a second of each of n
    // callers. The catalogue stores the shares of the speed alone.
    let solo = 230.4;
    let measured = [
        (4.0, 193.9),
        (8.0, 160.2),
        (16.0, 122.1),
        (32.0, 90.6),
        (64.0, 66.6),
    ];
    let curve: Vec<String> = measured
        .iter()
        .map(|(n, each)| format!("[{n}, {}]", each / solo))
        .collect();
    let text = KNEE
        .replace("SLOTS", "256")
        .replace("EXTRA", &format!("batch_curve = [{}]", curve.join(", ")));
    let sc = Scenario::parse(&text).unwrap();
    let srv = &sc.model_servers[0];
    for (n, each) in measured {
        assert!((solo * srv.share(n) - each).abs() < 0.01, "{n}");
    }
    // Alone it has its full speed, and between two points it lies between.
    assert!((srv.share(1.0) - 1.0).abs() < 1e-9);
    let mid = solo * srv.share(24.0);
    assert!(mid < 122.1 && mid > 90.6, "{mid}");
    // Past the sweep the last slope goes on, no flatter than n^-0.5 (the
    // last stretch here is n^-0.44) and no steeper than n^-1.
    let past = srv.share(128.0) / srv.share(64.0);
    assert!((past - 0.5f64.sqrt()).abs() < 1e-9, "{past}");
    let flat = Scenario::parse(
        &KNEE
            .replace("SLOTS", "256")
            .replace("EXTRA", "batch_curve = [[1, 1.0], [32, 0.25], [48, 0.1]]"),
    )
    .unwrap();
    let srv = &flat.model_servers[0];
    assert!((srv.share(96.0) / srv.share(48.0) - 0.5).abs() < 1e-9);
}

const CROWD: &str = r#"
seed = 4
[[model_server]]
name = "srv"
models = ["m"]
slots = 8
tokens_per_s = 40
batch_scaling = 0.85
prefill_tokens_per_s = 200
EXTRA
jitter = 0
[[client]]
name = "dev"
entry = "srv"
models = ["m"]
arrival = { kind = "closed", agents = 8 }
to_s = 600
prompt_tokens = PROMPT
output_tokens = OUT
"#;

#[test]
fn reading_a_prompt_holds_up_the_answers_beside_it() {
    let go = |extra: &str| {
        let text = CROWD
            .replace("EXTRA", extra)
            .replace("PROMPT", "2000")
            .replace("OUT", "60");
        let (_, rep) = run(&text);
        let c = class(&rep, "dev");
        (c.gen_tps_p50.unwrap(), c.ok)
    };
    let (free, done_free) = go("");
    // A 512-token chunk takes 2.6 s to read, and an answer gets one token
    // a chunk while any prompt is being read. The callers fall into step
    // (they read together, then write together), so not every token of an
    // answer is held up.
    let (stalled, done_stalled) = go("prefill_chunk_tokens = 512");
    assert!(stalled < free * 0.5, "{stalled} {free}");
    assert!(done_stalled < done_free, "{done_stalled} {done_free}");
}

#[test]
fn answers_that_outgrow_the_room_put_the_last_request_back() {
    let go = |extra: &str| {
        let text = CROWD
            .replace("EXTRA", extra)
            .replace("PROMPT", "1000")
            .replace("OUT", "600");
        let (sim, rep) = run(&text);
        let short = sim
            .reqs
            .iter()
            .filter(|r| r.status == 200)
            .any(|r| r.first_token_at.is_none());
        assert!(!short);
        (sim.preempted, class(&rep, "dev").ok, sim.preempted_tokens)
    };
    let (none, done, _) = go("context_tokens = 100000");
    assert_eq!(none, 0);
    // Room for the eight prompts and a little of each answer: they start
    // together, outgrow it, and the last ones are read again.
    let (put_back, done_tight, redone) = go("context_tokens = 9000");
    assert!(put_back > 0 && redone > 1000.0, "{put_back} {redone}");
    assert!(done_tight < done, "{done_tight} {done}");
    assert!(done_tight as f64 > done as f64 * 0.3, "{done_tight} {done}");
}

#[test]
fn waiting_tickets_are_counted_and_checked_against_the_wait() {
    // Light load: nobody waits a quarter of a second, so nobody holds a
    // ticket.
    let (_, r) = run(&format!(
        "{}{GATEWAY}",
        OVERLOAD
            .replace("ENTRY", "gateway")
            .replace("rate_per_s = 2.0", "rate_per_s = 0.05")
    ));
    let s = class(&r, "interactive");
    assert_eq!(s.tickets, 0);
    assert!(s.ticket_told_p50.is_none() && s.ticket_late.is_none());

    // Steady overload on a bounded queue: those let in wait, are told how
    // long, and the telling is about right once the queue has settled.
    let (sim, r) = overload("gateway", &format!("{GATEWAY}max_queue = 6\n"));
    let s = class(&r, "interactive");
    let held: Vec<_> = sim
        .reqs
        .iter()
        .filter(|q| q.waited_ms.is_some_and(|w| w >= 250.0) && q.expected_wait_ms.is_some())
        .collect();
    assert!(s.tickets > 20, "{} tickets", s.tickets);
    assert_eq!(s.tickets, held.len());
    assert!(sim.reqs.iter().all(|q| q.waited_ms.is_some() || !q.seated));
    let (told, waited) = (s.ticket_told_p50.unwrap(), s.ticket_wait_p50.unwrap());
    assert!(
        told > waited * 0.5 && told < waited * 2.0,
        "told {told} s, waited {waited} s"
    );
    assert!(s.ticket_off_p50.unwrap() < waited);
    assert!(s.ticket_late.unwrap() < 0.25, "{:?} late", s.ticket_late);
}

#[test]
fn a_caller_that_times_out_sends_again_and_adds_to_the_pile() {
    // The overloaded server of `OVERLOAD`, callers that give up after 10 s.
    // Sending again after a timeout (client SDKs do, twice by default) puts
    // more requests on a server that already cannot keep up.
    let (_, once) = overload("litellm", "");
    let (sim, again) = run_with_resend(2);
    let (a, b) = (&once.classes[0].summary, &again.classes[0].summary);
    assert_eq!(a.resent, 0);
    assert!(b.resent > 0 && b.sent == a.sent + b.resent, "{b:?}");
    // Every attempt is settled once, none is sent more than twice again,
    // and each attempt has the full timeout of its own.
    assert_eq!(b.ok + b.gave_up + b.refused + b.other_errors, b.sent);
    assert!(sim.reqs.iter().all(|q| q.resend <= 2));
    assert!(again.records.iter().all(|q| q.total <= 10.0 + 1e-6));
    // More attempts time out than before: the server did not get faster.
    assert!(b.gave_up > a.gave_up, "{} {}", a.gave_up, b.gave_up);
}

fn run_with_resend(n: u32) -> (Sim, Report) {
    run(&OVERLOAD.replace("ENTRY", "litellm").replace(
        "timeout_s = 10",
        &format!("timeout_s = 10\ntimeout_retries = {n}"),
    ))
}
