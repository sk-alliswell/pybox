//! The usage analysis reads simulated runs and gateway log rows alike.

use gwsim::usage::{analyze, findings, parse_rows, report, rows_from_sim, Context};
use gwsim::{Scenario, Sim};

/// Two teams whose busy hours coincide on servers that are too few at the
/// peak and idle the rest of the day.
const DAY: &str = r#"
name = "day"
seed = 3
max_s = 2500

[[model_server]]
name = "gpu"
models = ["m"]
slots = 6
prefill_ms = 200
tokens_per_s = 60

[[gateway.upstream]]
name = "m"
models = ["m"]
targets = ["gpu"]
max_concurrency = 6
max_queue = 6

[[client]]
name = "a~ide"
class = "a · ide"
count = 20
entry = "gateway"
models = ["m"]
arrival = { kind = "poisson", rate_per_s = 0.15 }
to_s = 2400
prompt_tokens = 500
output_tokens = 120
rhythm = { period_s = 2400, busy_at_s = 1300, busy_width_s = 600, quiet = 0.05 }

[[client]]
name = "b~batch"
class = "b · batch"
count = 10
entry = "gateway"
models = ["m"]
arrival = { kind = "poisson", rate_per_s = 0.1 }
to_s = 2400
prompt_tokens = 300
output_tokens = 60
rhythm = { period_s = 2400, busy_at_s = 1300, busy_width_s = 600, quiet = 0.05 }
"#;

#[test]
fn a_simulated_day_shows_when_it_is_pressed_and_when_idle() {
    let sc = Scenario::from_toml(DAY).unwrap();
    let ctx = Context::from_scenario(&sc);
    assert_eq!(ctx.day_s, 2400.0);
    assert_eq!(ctx.slots["m"], 6.0);
    let mut sim = Sim::new(sc).unwrap();
    sim.run();
    let rows = rows_from_sim(&sim);
    assert_eq!(rows.len(), sim.reqs.len());
    let a = analyze(&rows, &ctx);

    let names: Vec<(&str, &str)> = a
        .groups
        .iter()
        .map(|g| (g.team.as_str(), g.kind.as_str()))
        .collect();
    assert_eq!(names, [("a", "ide"), ("b", "batch")]);
    assert_eq!(a.callers, 30, "20 + 10 people, by `team-N`");
    assert!(a.missing.contains(&"cache_read_tokens".to_string()));

    // The busy hours are around 13 and both teams share them.
    assert!(a.pressed_hours.contains(&13), "{:?}", a.pressed_hours);
    assert!(a.quiet_hours.contains(&2) && !a.quiet_hours.contains(&13));
    assert!(a.coincidence > 0.85, "{}", a.coincidence);
    assert!(a.refused_share > 0.02);
    let used = |h: usize| a.hours[h].used.unwrap();
    assert!(used(13) > 0.8 && used(2) < 0.2, "{} {}", used(13), used(2));

    let text = findings(&a).join("\n");
    assert!(text.contains("같은 시간대에 몰립니다"), "{text}");
    assert!(text.contains("batch"), "{text}");
    let md = report(&a);
    assert!(md.contains("시뮬레이션 결과입니다") && md.contains("| a | ide |"));

    // The rows survive a round trip through the file format.
    let lines: Vec<String> = rows
        .iter()
        .map(|r| serde_json::to_string(r).unwrap())
        .collect();
    let again = analyze(&parse_rows(&lines.join("\n")).unwrap(), &ctx);
    assert_eq!(again.requests, a.requests);
    assert_eq!(again.pressed_hours, a.pressed_hours);
}

#[test]
fn gateway_log_rows_are_read_as_they_are() {
    // Rows as `/gateway/obs/rows` returns them: epoch ms, more columns than
    // the analysis uses, and no request kind.
    let base = 1_794_528_000_000.0; // 2026-11-13 00:00 UTC
    let mut lines = Vec::new();
    for i in 0..40 {
        // One request a minute from 01:00 UTC, each holding a server 30 s.
        let ts = base + 3_600_000.0 + f64::from(i) * 60_000.0;
        lines.push(format!(
            r#"{{"ts":{ts},"trace_id":"t{i}","status":200,"is_error":false,"attempts":1,"provider":"openai","queue_ms":0,"ttft_ms":400,"total_ms":30000,"input_tokens":1000,"output_tokens":200,"cache_read_tokens":600,"team_id":"core","user_id":"u{}","model":"m","upstream":"m","backend":"s{}","upstream_ip":"10.0.0.1","priority":"interactive","claimed_user_id":"x","stream":true,"cost_usd":0.0}}"#,
            i % 4,
            i % 2
        ));
    }
    lines.push(format!(
        r#"{{"ts":{},"status":429,"team_id":"core","user_id":"u0","model":"m","queue_ms":0,"total_ms":2}}"#,
        base + 3_700_000.0
    ));
    let rows = parse_rows(&lines.join("\n")).unwrap();
    assert_eq!(rows[0].backend, "s0");
    assert_eq!(rows[0].kind, "interactive");
    let ctx = Context {
        tz_hours: 9.0,
        ..Context::default()
    };
    let a = analyze(&rows, &ctx);
    assert_eq!(a.requests, 41);
    assert_eq!(a.callers, 4);
    assert!(a.missing.is_empty(), "{:?}", a.missing);
    assert!((a.cache_read_share - 0.6).abs() < 1e-9);
    // 01:00 UTC is 10:00 in Seoul.
    assert_eq!(a.busy_hours["core"].0, 10);
    assert!(a.hours[10].requests_per_h > 30.0 && a.hours[1].requests_per_h == 0.0);
    // Half a request runs at once; with no slot count the room left is
    // against the most seen at once.
    let m = &a.models[0];
    assert!(m.slots.is_none() && m.running_peak == 1.0);
    let md = report(&a);
    assert!(!md.contains("시뮬레이션") && md.contains("슬롯 수를 받지 못해"));
}

#[test]
fn waiting_callers_beside_idle_servers_are_called_an_outage() {
    // Hours 3 to 5: callers wait a minute and leave, nothing runs.
    let mut lines = Vec::new();
    for i in 0..600 {
        let ts = f64::from(i) * 36_000.0; // 100 an hour over six hours
        let hour = i / 100;
        lines.push(if hour >= 3 {
            format!(
                r#"{{"ts":{ts},"status":0,"client_abort":true,"attempts":0,"queue_ms":0,"total_ms":60000,"team_id":"a","user_id":"u","model":"m"}}"#
            )
        } else {
            format!(
                r#"{{"ts":{ts},"status":200,"attempts":1,"queue_ms":0,"ttft_ms":500,"total_ms":60000,"input_tokens":100,"output_tokens":10,"team_id":"a","user_id":"u","model":"m"}}"#
            )
        });
    }
    let ctx = Context {
        slots: [("m".to_string(), 4.0)].into(),
        ..Context::default()
    };
    let a = analyze(&parse_rows(&lines.join("\n")).unwrap(), &ctx);
    // A caller that left while queued held no server, and its wait is the
    // time it stayed even though the old log wrote 0.
    assert!(a.hours[1].used.unwrap() > 0.3 && a.hours[4].used.unwrap() < 0.05);
    assert_eq!(a.hours[4].queue_p95_s, Some(60.0));
    assert!(findings(&a)[0].contains("장애 모양"), "{:?}", findings(&a));
}
