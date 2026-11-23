//! Fitting model-server parameters from a measured client log.
//!
//! Works on logs of requests sent straight at a single-slot server (through
//! a proxy or not), where the server's own queue is first in, first out:
//! - token rate from how generation time grows with output length;
//! - prefill from the gap between one answer ending and the next one's
//!   first token while the server stays busy;
//! - what an abandoned request costs, from how that gap grows with the
//!   number of abandoned requests that were in line between the two.

use std::collections::BTreeMap;

use serde::Serialize;

use crate::metrics::pct;
use crate::report::Record;

#[derive(Clone, Debug, Serialize)]
pub struct ServerFit {
    pub model: String,
    pub answered: usize,
    pub tokens_per_s: f64,
    /// Spread of generation time between requests (sigma of the log).
    pub jitter: f64,
    pub prefill_ms: Option<f64>,
    /// Cost of an abandoned request beyond the prefill.
    pub abandon_extra_ms: Option<f64>,
    /// Scatter of the gaps the prefill fit could not explain (ms).
    pub prefill_residual_ms: Option<f64>,
    pub gaps_used: usize,
}

fn median(mut v: Vec<f64>) -> Option<f64> {
    v.sort_by(f64::total_cmp);
    pct(&v, 50.0)
}

/// `out_tokens`: output length per class (the client's `max_tokens`).
pub fn fit(rows: &[Record], out_tokens: &BTreeMap<String, f64>) -> Vec<ServerFit> {
    let mut models: Vec<&str> = rows.iter().map(|r| r.model.as_str()).collect();
    models.sort_unstable();
    models.dedup();
    models
        .into_iter()
        .map(|m| fit_model(m, rows, out_tokens))
        .collect()
}

fn fit_model(model: &str, rows: &[Record], out_tokens: &BTreeMap<String, f64>) -> ServerFit {
    let mut mine: Vec<&Record> = rows.iter().filter(|r| r.model == model).collect();
    mine.sort_by(|a, b| a.t.total_cmp(&b.t));
    // Generation time (first to last token) by output length.
    let mut by_len: Vec<(f64, f64, Vec<f64>)> = Vec::new();
    for (cls, n) in out_tokens {
        let g: Vec<f64> = mine
            .iter()
            .filter(|r| &r.cls == cls && r.ok())
            .map(|r| r.total - r.ttft.unwrap_or(0.0))
            .filter(|g| *g > 0.0)
            .collect();
        if let Some(med) = median(g.clone()) {
            by_len.push((*n, med, g));
        }
    }
    by_len.sort_by(|a, b| a.0.total_cmp(&b.0));
    // Least squares through the origin: generation = (tokens - 1) / rate.
    let num: f64 = by_len.iter().map(|x| (x.0 - 1.0) * (x.0 - 1.0)).sum();
    let den: f64 = by_len.iter().map(|x| (x.0 - 1.0) * x.1).sum();
    let tokens_per_s = if den > 0.0 { num / den } else { f64::NAN };
    let mut logs = Vec::new();
    for (_, med, g) in &by_len {
        logs.extend(g.iter().map(|x| (x / med).ln()));
    }
    let jitter = if logs.len() > 1 {
        (logs.iter().map(|x| x * x).sum::<f64>() / logs.len() as f64).sqrt()
    } else {
        0.0
    };
    // Gaps between consecutive answers while the server stayed busy:
    // gap = prefill + abandoned_in_between * abandon_cost.
    let (mut xs, mut ys) = (Vec::new(), Vec::new());
    let mut prev: Option<(usize, &Record)> = None;
    for (i, r) in mine.iter().enumerate() {
        if !r.ok() {
            continue;
        }
        if let Some((j, p)) = prev {
            let busy = p.t + p.total > r.t;
            let between = (i - j - 1) as f64;
            let gap = (r.t + r.ttft.unwrap_or(0.0)) - (p.t + p.total);
            if busy && gap > 0.0 {
                xs.push(between);
                ys.push(gap * 1000.0);
            }
        }
        prev = Some((i, r));
    }
    // Medians, so a request that was abandoned halfway through its answer
    // (and cost much more than a prefill) does not pull the fit.
    let (mut prefill, mut extra, mut resid) = (None, None, None);
    let clean: Vec<f64> = xs
        .iter()
        .zip(&ys)
        .filter(|(x, _)| **x == 0.0)
        .map(|(_, y)| *y)
        .collect();
    if clean.len() >= 5 {
        let p = median(clean.clone()).expect("non-empty");
        prefill = Some(p);
        resid = median(clean.iter().map(|y| (y - p).abs()).collect());
        let per: Vec<f64> = xs
            .iter()
            .zip(&ys)
            .filter(|(x, _)| **x >= 1.0)
            .map(|(x, y)| (y - p) / x)
            .collect();
        if per.len() >= 5 {
            extra = median(per).map(|c| c - p);
        }
    }
    ServerFit {
        model: model.to_string(),
        answered: mine.iter().filter(|r| r.ok()).count(),
        tokens_per_s,
        jitter,
        prefill_ms: prefill,
        abandon_extra_ms: extra,
        prefill_residual_ms: resid,
        gaps_used: xs.len(),
    }
}

/// The fit as `[[model_server]]` tables to paste into a scenario.
pub fn to_toml(fits: &[ServerFit], source: &str) -> String {
    let mut o = String::new();
    for f in fits {
        o.push_str(&format!(
            "# fitted from {source}: {} answers, {} busy gaps, residual {} ms\n[[model_server]]\nname = \"{}\"\nmodels = [\"{}\"]\nslots = 1\ntokens_per_s = {:.2}\njitter = {:.3}\n",
            f.answered,
            f.gaps_used,
            f.prefill_residual_ms.map_or("-".into(), |v| format!("{v:.0}")),
            f.model,
            f.model,
            f.tokens_per_s,
            f.jitter
        ));
        if let Some(p) = f.prefill_ms {
            o.push_str(&format!("prefill_ms = {p:.0}\n"));
        }
        if let Some(e) = f.abandon_extra_ms {
            o.push_str(&format!(
                "on_disconnect = \"first_token\"\nabandon_extra_ms = {:.0}\n",
                e.max(0.0)
            ));
        }
        o.push('\n');
    }
    o
}
