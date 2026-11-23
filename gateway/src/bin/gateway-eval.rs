//! Prints the scorecard for the three lead areas.
//!
//!     cargo run --release --bin gateway-eval [-- --json] [--quick]

use pybox_gateway::lead::eval;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let has = |f: &str| args.iter().any(|a| a == f);
    let cs = eval::scorecard(has("--quick"));
    if has("--json") {
        println!("{}", serde_json::to_string_pretty(&cs).unwrap());
    } else {
        print!("{}", eval::markdown(&cs));
        let d = eval::replay_day(&eval::day_workload(41, 3));
        println!(
            "\nDay replay, billed input in base-token units (as sent / LiteLLM-style / gateway):"
        );
        for (k, c) in &d.by_kind {
            println!(
                "  {k:24} {:>10.0} {:>10.0} {:>10.0}",
                c.as_sent, c.litellm_style, c.ours
            );
        }
        let t = d.total;
        println!(
            "  {:24} {:>10.0} {:>10.0} {:>10.0}",
            "total", t.as_sent, t.litellm_style, t.ours
        );
        println!(
            "  raw input tokens {:.0}, output tokens {:.0}",
            t.raw_in, t.out
        );
        let mut causes: Vec<_> = d.causes.iter().collect();
        causes.sort();
        println!("  cache state per request: {causes:?}");
    }
    let failed = cs.iter().filter(|c| !c.pass).count();
    if cfg!(debug_assertions) {
        eprintln!("debug build: timing criteria are not meaningful");
    }
    std::process::exit(i32::from(failed > 0));
}
