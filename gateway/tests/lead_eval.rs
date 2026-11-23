//! The scorecard must keep passing. Timing criteria only count in release
//! builds (`cargo run --release --bin gateway-eval` prints the full card).

use pybox_gateway::lead::eval;

#[test]
fn every_criterion_passes() {
    let cs = eval::scorecard(true);
    assert!(cs.len() >= 20);
    let failed: Vec<String> = cs
        .iter()
        .filter(|c| !c.pass && !(c.timing && cfg!(debug_assertions)))
        .map(|c| format!("{} {}: {} (target {})", c.id, c.name, c.measured, c.target))
        .collect();
    assert!(failed.is_empty(), "{failed:#?}");
}
