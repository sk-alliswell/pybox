//! Cache breakpoint placement: decide where a request should be cached and
//! for how long, and add the markers by splicing bytes into the body.
//!
//! The body is never parsed and re-serialized. Everything outside the
//! inserted bytes stays byte-identical, so the gateway cannot be the thing
//! that breaks a cache prefix.

use bytes::Bytes;

use super::cachemodel::{CacheRules, Weights};
use super::fingerprint::{BKind, Fingerprint, Ttl};
use super::json::ws;
use super::runs::AgentStats;

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Plan {
    /// (byte offset, text) pairs, ascending by offset.
    pub inserts: Vec<(usize, String)>,
    /// Block indexes that will carry a breakpoint after the inserts.
    pub bps: Vec<usize>,
    pub ttl: Ttl,
    pub why: &'static str,
}

impl Plan {
    fn skip(why: &'static str) -> Self {
        Self {
            why,
            ..Self::default()
        }
    }
    pub fn is_noop(&self) -> bool {
        self.inserts.is_empty()
    }
}

/// Pay the one hour write premium only when long gaps are common enough.
///
/// Per turn, a five minute cache that expires costs a rewrite of the prefix
/// (`write_5m - read` per token) with probability `long_gap_rate`. The one
/// hour cache costs `write_1h - write_5m` more on the tokens each turn adds,
/// plus the same premium once on the starting prefix.
pub fn choose_ttl(stats: AgentStats, prefix: f64, per_turn: f64, w: Weights) -> Ttl {
    if stats.gaps < 8 {
        return Ttl::M5;
    }
    let turns = stats.turns_per_run().max(1.0);
    let miss_cost = stats.long_gap_rate() * (w.write_5m - w.read) * prefix;
    let premium = (w.write_1h - w.write_5m) * (per_turn + prefix / turns);
    if miss_cost > premium {
        Ttl::H1
    } else {
        Ttl::M5
    }
}

fn marker(ttl: Ttl) -> &'static str {
    match ttl {
        Ttl::M5 => r#","cache_control":{"type":"ephemeral"}"#,
        Ttl::H1 => r#","cache_control":{"type":"ephemeral","ttl":"1h"}"#,
    }
}

/// Offset just before the closing brace of the object ending at `end`, or
/// `None` for an empty object (a leading comma would be invalid there).
fn before_close(body: &[u8], start: usize, end: usize) -> Option<usize> {
    let close = end - 1;
    (body[close] == b'}' && ws(body, start + 1) != close).then_some(close)
}

pub fn plan(
    body: &[u8],
    fp: &Fingerprint,
    turn: u32,
    stats: AgentStats,
    rules: CacheRules,
    w: Weights,
    volatile_system: bool,
) -> Plan {
    if fp.implicit_cache {
        return Plan::skip("implicit_cache");
    }
    if fp.client_breakpoints() {
        return Plan::skip("client_managed");
    }
    let total = fp.total_tokens();
    if total < rules.min_tokens {
        return Plan::skip("below_min");
    }
    // A system prompt that changes every request (a timestamp at the top)
    // makes everything after the tools uncacheable: mark the tools only.
    let n_static = if volatile_system {
        fp.n_tools
    } else {
        fp.n_tools + fp.n_system
    };
    let static_tok = if n_static == 0 {
        0
    } else {
        fp.blocks[n_static - 1].tok
    };
    let per_turn = if turn > 1 {
        f64::from(total - static_tok) / f64::from(turn)
    } else {
        f64::from(total - static_tok)
    };
    let ttl = choose_ttl(stats, f64::from(static_tok), per_turn, w);
    let mut p = Plan {
        ttl,
        why: "placed",
        ..Plan::default()
    };
    // The fixed part (tools and system) is shared by every run of the agent.
    if static_tok >= rules.min_tokens {
        let at = (0..n_static)
            .rev()
            .find(|&i| fp.blocks[i].obj)
            .filter(|&i| {
                // Everything after the breakpoint within the fixed part would be
                // left out, so only accept the last fixed block or a system
                // block followed by nothing but a plain-string remainder.
                i + 1 == n_static
                    || fp.blocks[i + 1..n_static]
                        .iter()
                        .all(|b| b.kind == BKind::System && !b.obj)
            });
        if let Some(i) = at.filter(|&i| fp.blocks[i].tok >= rules.min_tokens) {
            let b = &fp.blocks[i];
            if let Some(off) = before_close(body, b.start, b.end) {
                p.inserts.push((off, marker(ttl).to_string()));
                p.bps.push(i);
            }
        }
    }
    // From the second turn on, or for agents that usually take several
    // turns, cache the conversation too. A one-shot run would only pay the
    // write premium on it.
    let multi_turn = turn > 1 || stats.turns_per_run() >= 2.0;
    if multi_turn && total > static_tok && !volatile_system {
        if let Some(off) = before_close(body, 0, fp.top_end + 1) {
            p.inserts.push((off, marker(ttl).to_string()));
            let last = fp.blocks.len() - 1;
            if !p.bps.contains(&last) {
                p.bps.push(last);
            }
        }
    }
    if p.inserts.is_empty() {
        return Plan::skip(if volatile_system {
            "volatile_system"
        } else if multi_turn {
            "no_anchor"
        } else {
            "single_turn"
        });
    }
    p.inserts.sort_by_key(|i| i.0);
    p
}

/// Add an OpenAI-style cache routing key when the client sent none.
pub fn cache_key_insert(body: &[u8], fp: &Fingerprint, run_id: &str) -> Option<(usize, String)> {
    if !fp.implicit_cache || fp.has_cache_key {
        return None;
    }
    let safe: String = run_id
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
        .take(64)
        .collect();
    if safe.is_empty() {
        return None;
    }
    let off = before_close(body, ws(body, 0), fp.top_end + 1)?;
    Some((off, format!(r#","prompt_cache_key":"{safe}""#)))
}

pub fn apply(body: &Bytes, inserts: &[(usize, String)]) -> Bytes {
    if inserts.is_empty() {
        return body.clone();
    }
    let extra: usize = inserts.iter().map(|i| i.1.len()).sum();
    let mut out = Vec::with_capacity(body.len() + extra);
    let mut at = 0;
    for (off, text) in inserts {
        out.extend_from_slice(&body[at..*off]);
        out.extend_from_slice(text.as_bytes());
        at = *off;
    }
    out.extend_from_slice(&body[at..]);
    Bytes::from(out)
}

#[cfg(test)]
mod tests {
    use super::super::fingerprint::fingerprint;
    use super::*;

    fn big(n: usize) -> String {
        "lorem ipsum ".repeat(n)
    }
    fn rules() -> CacheRules {
        CacheRules::default()
    }

    fn body(sys_obj: bool, turns: usize) -> String {
        let sys = if sys_obj {
            format!(r#"[{{"type":"text","text":"{}"}}]"#, big(600))
        } else {
            format!(r#""{}""#, big(600))
        };
        let msgs: Vec<String> = (0..turns)
            .map(|i| {
                format!(
                    r#"{{"role":"{}","content":[{{"type":"text","text":"t{i} {}"}}]}}"#,
                    if i % 2 == 0 { "user" } else { "assistant" },
                    big(50)
                )
            })
            .collect();
        format!(
            r#"{{"model":"m","tools":[{{"name":"a","description":"{}"}}],"system":{sys},"messages":[{}]}}"#,
            big(100),
            msgs.join(",")
        )
    }

    #[test]
    fn first_turn_caches_only_the_fixed_part() {
        let b = body(true, 1);
        let fp = fingerprint(b.as_bytes(), true).unwrap();
        let p = plan(
            b.as_bytes(),
            &fp,
            1,
            AgentStats::default(),
            rules(),
            Weights::default(),
            false,
        );
        assert_eq!(p.bps, [1]);
        let out = apply(&Bytes::from(b.clone()), &p.inserts);
        let fp2 = fingerprint(&out, true).unwrap();
        assert_eq!(fp2.breakpoints(), [1]);
        assert_eq!(fp2.tail(), fp.tail());
        serde_json::from_slice::<serde_json::Value>(&out).unwrap();
        // Removing the inserted text gives back the original bytes.
        let s = String::from_utf8(out.to_vec()).unwrap();
        assert_eq!(s.replacen(&p.inserts[0].1, "", 1), b);
    }

    #[test]
    fn later_turns_also_cache_the_conversation() {
        let b = body(true, 3);
        let fp = fingerprint(b.as_bytes(), true).unwrap();
        let p = plan(
            b.as_bytes(),
            &fp,
            2,
            AgentStats::default(),
            rules(),
            Weights::default(),
            false,
        );
        assert_eq!(p.bps, [1, 4]);
        let out = apply(&Bytes::from(b), &p.inserts);
        let fp2 = fingerprint(&out, true).unwrap();
        assert_eq!(fp2.breakpoints(), [1, 4]);
        assert!(fp2.auto_cc.is_some());
    }

    #[test]
    fn string_system_falls_back_and_clients_are_left_alone() {
        let b = body(false, 3);
        let fp = fingerprint(b.as_bytes(), true).unwrap();
        let p = plan(
            b.as_bytes(),
            &fp,
            2,
            AgentStats::default(),
            rules(),
            Weights::default(),
            false,
        );
        // No object to hang the fixed breakpoint on: conversation only.
        assert_eq!(p.bps, [4]);
        let managed = body(true, 3).replacen(
            "{\"model\"",
            "{\"cache_control\":{\"type\":\"ephemeral\"},\"model\"",
            1,
        );
        let fp = fingerprint(managed.as_bytes(), true).unwrap();
        assert_eq!(
            plan(
                managed.as_bytes(),
                &fp,
                2,
                AgentStats::default(),
                rules(),
                Weights::default(),
                false
            )
            .why,
            "client_managed"
        );
        let tiny = r#"{"model":"m","messages":[{"role":"user","content":"hi"}]}"#;
        let fp = fingerprint(tiny.as_bytes(), true).unwrap();
        assert_eq!(
            plan(
                tiny.as_bytes(),
                &fp,
                1,
                AgentStats::default(),
                rules(),
                Weights::default(),
                false
            )
            .why,
            "below_min"
        );
    }

    #[test]
    fn one_hour_only_when_long_gaps_pay_for_it() {
        let w = Weights::default();
        let often = AgentStats {
            gaps: 100,
            long_gaps: 40,
            runs: 10,
            turns: 110,
        };
        let rare = AgentStats {
            gaps: 100,
            long_gaps: 2,
            runs: 10,
            turns: 110,
        };
        assert_eq!(choose_ttl(often, 20_000.0, 1_000.0, w), Ttl::H1);
        assert_eq!(choose_ttl(rare, 20_000.0, 1_000.0, w), Ttl::M5);
        assert_eq!(
            choose_ttl(AgentStats::default(), 20_000.0, 1_000.0, w),
            Ttl::M5
        );
    }

    #[test]
    fn openai_requests_get_a_cache_key() {
        let o = r#"{"model":"gpt","messages":[{"role":"user","content":"u"}]}"#;
        let fp = fingerprint(o.as_bytes(), false).unwrap();
        let ins = cache_key_insert(o.as_bytes(), &fp, "run 1/α").unwrap();
        let out = apply(&Bytes::from(o), &[ins]);
        let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["prompt_cache_key"], "run1");
        let fp2 = fingerprint(&out, false).unwrap();
        assert!(fp2.has_cache_key && fp2.tail() == fp.tail());
    }
}
