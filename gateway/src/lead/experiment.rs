//! Run-level experiments: does a change really lower cost without hurting
//! the work?
//!
//! Whole runs are assigned to an arm by a hash of the run id, so a run never
//! mixes arms and the split needs no stored state. The verdict compares mean
//! cost per run (Welch's test, two-sided 5%) and is blocked by guardrails:
//! a higher failure rate or more turns per run in the treated arm
//! (one-sided 1%) stops the experiment whatever the cost says.
//!
//! The verdict is meant to be read once, at the planned sample size. Reading
//! it repeatedly while data arrives raises the false-positive rate.

use serde::Serialize;

use super::fingerprint::Fingerprint;
use super::json::hash;

/// True when the run is in the treated arm.
pub fn treated(experiment: &str, run_id: &str, fraction: f64) -> bool {
    let h = hash(hash(0x5eed, experiment.as_bytes()), run_id.as_bytes());
    // Top 53 bits as a uniform number in [0, 1).
    ((h >> 11) as f64 / (1u64 << 53) as f64) < fraction
}

#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct ArmStats {
    pub runs: u64,
    pub cost_sum: f64,
    pub cost_sq: f64,
    pub failures: u64,
    pub turns_sum: f64,
    pub turns_sq: f64,
}

impl ArmStats {
    pub fn add(&mut self, cost: f64, failed: bool, turns: f64) {
        self.runs += 1;
        self.cost_sum += cost;
        self.cost_sq += cost * cost;
        self.failures += u64::from(failed);
        self.turns_sum += turns;
        self.turns_sq += turns * turns;
    }
    fn mean_var(&self, sum: f64, sq: f64) -> (f64, f64) {
        let n = self.runs as f64;
        if self.runs < 2 {
            return (if self.runs == 1 { sum } else { 0.0 }, 0.0);
        }
        let mean = sum / n;
        (mean, ((sq - n * mean * mean) / (n - 1.0)).max(0.0))
    }
    pub fn cost(&self) -> (f64, f64) {
        self.mean_var(self.cost_sum, self.cost_sq)
    }
    pub fn turns(&self) -> (f64, f64) {
        self.mean_var(self.turns_sum, self.turns_sq)
    }
    pub fn failure_rate(&self) -> f64 {
        if self.runs == 0 {
            0.0
        } else {
            self.failures as f64 / self.runs as f64
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Guard {
    /// Runs per arm before any verdict.
    pub min_runs: u64,
    /// Two-sided z for the cost difference (1.96 = 5%).
    pub z_cost: f64,
    /// One-sided z for the guardrails (2.326 = 1%).
    pub z_guard: f64,
}

impl Default for Guard {
    fn default() -> Self {
        Self {
            min_runs: 30,
            z_cost: 1.96,
            z_guard: 2.326,
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Verdict {
    /// adopt | reject | stop_guardrail | inconclusive | need_more_runs
    pub decision: &'static str,
    pub reason: String,
    /// Treated minus control, as a share of control cost per run.
    pub effect: f64,
    pub ci_low: f64,
    pub ci_high: f64,
    pub z: f64,
}

pub fn verdict(control: &ArmStats, treat: &ArmStats, g: Guard) -> Verdict {
    let (mc, vc) = control.cost();
    let (mt, vt) = treat.cost();
    let (nc, nt) = (control.runs as f64, treat.runs as f64);
    let mut v = Verdict {
        decision: "need_more_runs",
        reason: format!(
            "{} and {} runs, {} per arm needed",
            control.runs, treat.runs, g.min_runs
        ),
        effect: 0.0,
        ci_low: 0.0,
        ci_high: 0.0,
        z: 0.0,
    };
    if control.runs < g.min_runs || treat.runs < g.min_runs {
        return v;
    }
    // Guardrails first: a cheaper run that fails more is not a saving.
    let (pc, pt) = (control.failure_rate(), treat.failure_rate());
    let pooled = (control.failures + treat.failures) as f64 / (nc + nt);
    let se_p = (pooled * (1.0 - pooled) * (1.0 / nc + 1.0 / nt)).sqrt();
    if se_p > 0.0 && (pt - pc) / se_p > g.z_guard {
        v.decision = "stop_guardrail";
        v.reason = format!(
            "failure rate rose from {:.1}% to {:.1}%",
            pc * 100.0,
            pt * 100.0
        );
        return v;
    }
    let ((tc, tvc), (tt, tvt)) = (control.turns(), treat.turns());
    let se_t = (tvc / nc + tvt / nt).sqrt();
    if se_t > 0.0 && (tt - tc) / se_t > g.z_guard {
        v.decision = "stop_guardrail";
        v.reason = format!("turns per run rose from {tc:.1} to {tt:.1}");
        return v;
    }
    let se = (vc / nc + vt / nt).sqrt();
    if mc <= 0.0 || se <= 0.0 {
        v.decision = "inconclusive";
        v.reason = "no cost variation to compare".into();
        return v;
    }
    let diff = mt - mc;
    v.z = diff / se;
    v.effect = diff / mc;
    v.ci_low = (diff - g.z_cost * se) / mc;
    v.ci_high = (diff + g.z_cost * se) / mc;
    (v.decision, v.reason) = if v.z < -g.z_cost {
        (
            "adopt",
            format!("cost per run fell by {:.1}%", -v.effect * 100.0),
        )
    } else if v.z > g.z_cost {
        (
            "reject",
            format!("cost per run rose by {:.1}%", v.effect * 100.0),
        )
    } else {
        ("inconclusive", "the difference is within noise".into())
    };
    v
}

/// What clearing old tool results would do to this request, without doing
/// it. Returns (tokens removed, net input tokens saved on this request in
/// base-token units). Removing a block rewrites everything after it once,
/// so the net can be negative on the turn where the clearing happens.
pub fn shadow_clear(fp: &Fingerprint, keep_last: usize, read_w: f64, write_w: f64) -> (u32, f64) {
    let results: Vec<usize> = fp
        .blocks
        .iter()
        .enumerate()
        .filter(|(_, b)| b.tool_result)
        .map(|(i, _)| i)
        .collect();
    if results.len() <= keep_last {
        return (0, 0.0);
    }
    // Block token counts are cumulative.
    let own = |i: usize| fp.blocks[i].tok - if i == 0 { 0 } else { fp.blocks[i - 1].tok };
    let cut = &results[..results.len() - keep_last];
    let removed: u32 = cut.iter().map(|&i| own(i)).sum();
    let first = cut[0];
    let before = if first == 0 {
        0
    } else {
        fp.blocks[first - 1].tok
    };
    let kept_after = fp.total_tokens() - before - removed;
    // Today the removed and kept tokens after the first cut are cache reads.
    // After clearing, the kept ones are written once and the removed ones
    // are gone.
    let net = f64::from(removed) * read_w - f64::from(kept_after) * (write_w - read_w);
    (removed, net)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn assignment_is_stable_and_close_to_the_fraction() {
        let n = 20_000;
        let hits = (0..n)
            .filter(|i| treated("exp", &format!("run-{i}"), 0.3))
            .count();
        assert!((hits as f64 / n as f64 - 0.3).abs() < 0.015, "{hits}");
        assert_eq!(treated("exp", "run-7", 0.3), treated("exp", "run-7", 0.3));
        // A different experiment splits the same runs differently.
        let both = (0..n)
            .filter(|i| treated("a", &format!("r{i}"), 0.5) && treated("b", &format!("r{i}"), 0.5))
            .count();
        assert!((both as f64 / n as f64 - 0.25).abs() < 0.02);
    }

    fn arm(n: u64, cost: f64, fail_every: u64) -> ArmStats {
        let mut a = ArmStats::default();
        for i in 0..n {
            let jitter = 0.8 + 0.4 * ((i * 37 % 100) as f64 / 100.0);
            a.add(
                cost * jitter,
                fail_every > 0 && i % fail_every == 0,
                10.0 + (i % 5) as f64,
            );
        }
        a
    }

    #[test]
    fn verdicts() {
        let g = Guard::default();
        assert_eq!(
            verdict(&arm(10, 1.0, 0), &arm(10, 0.5, 0), g).decision,
            "need_more_runs"
        );
        let v = verdict(&arm(200, 1.0, 0), &arm(200, 0.8, 0), g);
        assert_eq!(v.decision, "adopt");
        assert!(v.ci_high < 0.0 && (v.effect + 0.2).abs() < 0.01);
        assert_eq!(
            verdict(&arm(200, 1.0, 0), &arm(200, 1.2, 0), g).decision,
            "reject"
        );
        assert_eq!(
            verdict(&arm(200, 1.0, 0), &arm(200, 1.0, 0), g).decision,
            "inconclusive"
        );
        // Cheaper but failing more: stopped.
        assert_eq!(
            verdict(&arm(200, 1.0, 50), &arm(200, 0.5, 5), g).decision,
            "stop_guardrail"
        );
    }
}
