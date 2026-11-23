//! Why did this request miss the provider's prompt cache?
//!
//! Compares a request's fingerprint with the previous request of the same
//! run and names the first reason the cached prefix could not be reused.

use super::cachemodel::CacheRules;
use super::fingerprint::Fingerprint;
use super::runs::RunInfo;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Cause {
    /// First request of a run: nothing to reuse yet.
    First,
    /// The previous prefix is reusable.
    Ok,
    ModelChanged,
    ToolsChanged,
    /// Same tool definitions in a different order.
    ToolsReordered,
    SystemChanged,
    /// The system prompt differs only in digits or id-like strings.
    SystemVolatile,
    /// An earlier message was edited or removed.
    HistoryEdited,
    OptionsChanged,
    /// The request carries no cache breakpoint at all.
    NoBreakpoint,
    /// More time passed than the cache entry lives.
    TtlExpired,
    /// The prefix is shorter than the provider's minimum.
    BelowMin,
    /// The breakpoint moved more than the lookback window past the last one.
    LookbackExceeded,
}

impl Cause {
    pub fn as_str(self) -> &'static str {
        match self {
            Cause::First => "first",
            Cause::Ok => "ok",
            Cause::ModelChanged => "model_changed",
            Cause::ToolsChanged => "tools_changed",
            Cause::ToolsReordered => "tools_reordered",
            Cause::SystemChanged => "system_changed",
            Cause::SystemVolatile => "system_volatile",
            Cause::HistoryEdited => "history_edited",
            Cause::OptionsChanged => "options_changed",
            Cause::NoBreakpoint => "no_breakpoint",
            Cause::TtlExpired => "ttl_expired",
            Cause::BelowMin => "below_min",
            Cause::LookbackExceeded => "lookback_exceeded",
        }
    }
    pub fn is_break(self) -> bool {
        !matches!(self, Cause::First | Cause::Ok)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Diagnosis {
    pub cause: Cause,
    /// Tokens that were (or could have been) read from cache and were not.
    pub lost_tokens: u32,
    /// Tokens this request shares with the previous one.
    pub shared_tokens: u32,
}

pub fn diagnose(
    prev: Option<&RunInfo>,
    fp: &Fingerprint,
    bps: &[usize],
    now: i64,
    rules: CacheRules,
) -> Diagnosis {
    let d = |cause, lost_tokens, shared_tokens| Diagnosis {
        cause,
        lost_tokens,
        shared_tokens,
    };
    let Some(p) = prev else {
        return d(Cause::First, 0, 0);
    };
    let k = p
        .chain
        .iter()
        .zip(&fp.blocks)
        .take_while(|(a, b)| a.0 == b.cum)
        .count();
    let shared = if k == 0 { 0 } else { p.chain[k - 1].1 };
    let warm = p.warm_tokens();
    // What the previous request cached, or could have with a breakpoint.
    let potential = if warm > 0 {
        warm
    } else {
        p.chain.last().map_or(0, |c| c.1)
    };
    let lost_after = |keep: u32| potential.saturating_sub(keep);
    if p.model != fp.model {
        return d(Cause::ModelChanged, potential, shared);
    }
    if k < p.chain.len() {
        let cause = if k < p.n_tools.max(fp.n_tools) {
            if p.tools_set == fp.tools_set && p.n_tools == fp.n_tools {
                Cause::ToolsReordered
            } else {
                Cause::ToolsChanged
            }
        } else if k < p.n_tools + p.n_system.max(fp.n_system) {
            if p.sys_masked == fp.sys_masked {
                Cause::SystemVolatile
            } else {
                Cause::SystemChanged
            }
        } else {
            Cause::HistoryEdited
        };
        return d(cause, lost_after(shared), shared);
    }
    if p.opts != fp.opts {
        let fixed = fp.static_tokens().min(shared);
        return d(Cause::OptionsChanged, lost_after(fixed), shared);
    }
    if shared < rules.min_tokens {
        return d(Cause::BelowMin, 0, shared);
    }
    if bps.is_empty() {
        return d(Cause::NoBreakpoint, shared, shared);
    }
    if p.bps.is_empty() {
        // Nothing was written last time, so this request is the first write.
        return d(Cause::NoBreakpoint, shared, shared);
    }
    if now > p.expires_ms() {
        return d(Cause::TtlExpired, warm, shared);
    }
    let reachable = bps.iter().any(|&bp| {
        let lo = (bp + 1).saturating_sub(rules.lookback);
        p.bps.iter().any(|&q| q >= lo && q <= bp)
    });
    if !reachable {
        return d(Cause::LookbackExceeded, warm, shared);
    }
    d(Cause::Ok, 0, shared)
}
