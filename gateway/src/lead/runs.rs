//! Run tracking: which requests belong to the same piece of agent work.
//!
//! A run is named by `x-gateway-run-id` when the runtime sends it. Otherwise
//! it is inferred: the next turn of a conversation starts with exactly the
//! blocks of the turn before, so its cumulative hash at that depth equals the
//! previous request's final hash.

use std::collections::{HashMap, VecDeque};

use super::fingerprint::{BKind, Fingerprint, Ttl};

#[derive(Clone, Debug)]
pub struct RunInfo {
    pub id: String,
    pub inferred: bool,
    pub agent: String,
    /// (cumulative hash, cumulative tokens) of the last request.
    pub chain: Vec<(u64, u32)>,
    pub n_tools: usize,
    pub n_system: usize,
    pub tools_set: u64,
    pub sys_masked: u64,
    pub model: u64,
    pub opts: u64,
    /// Block indexes the last request cached at.
    pub bps: Vec<usize>,
    pub ttl: Ttl,
    pub first_ms: i64,
    pub last_ms: i64,
    pub turns: u32,
    /// Smoothed output tokens per call, to predict the next one.
    pub out_ewma: f64,
    /// Backend that served the last call (its prefix cache is warm).
    pub backend: Option<usize>,
    tails: VecDeque<u64>,
    first: u64,
    /// How many of the run's oldest tool results are being cleared.
    pub trim_boundary: usize,
}

impl RunInfo {
    pub fn warm_tokens(&self) -> u32 {
        self.bps
            .iter()
            .max()
            .and_then(|&i| self.chain.get(i))
            .map_or(0, |c| c.1)
    }
    pub fn expires_ms(&self) -> i64 {
        self.last_ms + self.ttl.ms()
    }
}

/// Request-gap statistics for one agent, used to choose a cache lifetime.
#[derive(Clone, Copy, Debug, Default)]
pub struct AgentStats {
    pub gaps: u32,
    /// Gaps longer than 5 minutes but within an hour.
    pub long_gaps: u32,
    pub runs: u32,
    pub turns: u32,
}

impl AgentStats {
    pub fn long_gap_rate(&self) -> f64 {
        if self.gaps == 0 {
            0.0
        } else {
            f64::from(self.long_gaps) / f64::from(self.gaps)
        }
    }
    pub fn turns_per_run(&self) -> f64 {
        if self.runs == 0 {
            1.0
        } else {
            f64::from(self.turns) / f64::from(self.runs)
        }
    }
}

pub struct Observed {
    pub run_id: String,
    pub inferred: bool,
    /// The run as it was before this request, if it existed.
    pub prev: Option<RunInfo>,
}

#[derive(Default)]
pub struct RunTracker {
    runs: HashMap<String, RunInfo>,
    by_tail: HashMap<(String, u64), String>,
    /// Inferred runs by their first message, for when the prefix before
    /// the conversation changed (a timestamp in the system prompt, say).
    by_first: HashMap<(String, u64), String>,
    agents: HashMap<String, AgentStats>,
    seq: u64,
    calls: u64,
}

const IDLE_MS: i64 = 2 * 3_600_000;
const TAILS: usize = 4;

impl RunTracker {
    pub fn len(&self) -> usize {
        self.runs.len()
    }
    pub fn is_empty(&self) -> bool {
        self.runs.is_empty()
    }
    pub fn get(&self, id: &str) -> Option<&RunInfo> {
        self.runs.get(id)
    }
    pub fn get_mut(&mut self, id: &str) -> Option<&mut RunInfo> {
        self.runs.get_mut(id)
    }
    pub fn agent(&self, agent: &str) -> AgentStats {
        self.agents.get(agent).copied().unwrap_or_default()
    }
    pub fn runs(&self) -> impl Iterator<Item = &RunInfo> {
        self.runs.values()
    }

    /// Record a request and say which run it belongs to.
    pub fn observe(
        &mut self,
        agent: &str,
        header_run: Option<&str>,
        fp: &Fingerprint,
        now: i64,
    ) -> Observed {
        self.calls += 1;
        if self.calls % 1024 == 0 {
            self.prune(now);
        }
        let first_msg = fp.blocks.iter().position(|b| b.kind == BKind::Msg);
        let found = match header_run {
            Some(id) => Some((format!("{agent}\u{1}{id}"), false)),
            None => first_msg.and_then(|lo| {
                (lo..fp.blocks.len())
                    .rev()
                    .find_map(|i| self.by_tail.get(&(agent.to_string(), fp.blocks[i].cum)))
                    .or_else(|| {
                        // Same opening message and a longer conversation.
                        let key = self.by_first.get(&(agent.to_string(), fp.first_msg))?;
                        let r = self.runs.get(key)?;
                        let had = r.chain.len() - (r.n_tools + r.n_system).min(r.chain.len());
                        (fp.blocks.len() - lo > had && now - r.last_ms <= IDLE_MS).then_some(key)
                    })
                    .map(|id| (id.clone(), true))
            }),
        };
        let (key, inferred) = found.unwrap_or_else(|| {
            self.seq += 1;
            (format!("{agent}\u{1}inf-{:x}-{:x}", now, self.seq), true)
        });
        let public = key.split('\u{1}').nth(1).unwrap_or(&key).to_string();
        let prev = self.runs.get(&key).cloned();
        let st = self.agents.entry(agent.to_string()).or_default();
        st.turns += 1;
        match &prev {
            Some(p) => {
                let gap = now - p.last_ms;
                st.gaps += 1;
                if gap > Ttl::M5.ms() && gap <= Ttl::H1.ms() {
                    st.long_gaps += 1;
                }
            }
            None => st.runs += 1,
        }
        let run = self.runs.entry(key.clone()).or_insert_with(|| RunInfo {
            id: public.clone(),
            inferred,
            agent: agent.to_string(),
            chain: Vec::new(),
            n_tools: 0,
            n_system: 0,
            tools_set: 0,
            sys_masked: 0,
            model: 0,
            opts: 0,
            bps: Vec::new(),
            ttl: Ttl::M5,
            first_ms: now,
            last_ms: now,
            turns: 0,
            out_ewma: 0.0,
            backend: None,
            tails: VecDeque::new(),
            first: fp.first_msg,
            trim_boundary: 0,
        });
        if inferred && run.turns == 0 {
            self.by_first
                .insert((agent.to_string(), fp.first_msg), key.clone());
        }
        run.chain = fp.blocks.iter().map(|b| (b.cum, b.tok)).collect();
        run.n_tools = fp.n_tools;
        run.n_system = fp.n_system;
        run.tools_set = fp.tools_set;
        run.sys_masked = fp.sys_masked;
        run.model = fp.model;
        run.opts = fp.opts;
        run.last_ms = now;
        run.turns += 1;
        let tail = fp.tail();
        if !run.tails.contains(&tail) {
            run.tails.push_back(tail);
            self.by_tail.insert((agent.to_string(), tail), key.clone());
            if run.tails.len() > TAILS {
                if let Some(old) = run.tails.pop_front() {
                    self.by_tail.remove(&(agent.to_string(), old));
                }
            }
        }
        Observed {
            run_id: public,
            inferred,
            prev,
        }
    }

    /// Record where the request that was actually sent will be cached.
    pub fn set_cache(&mut self, agent: &str, run: &str, bps: Vec<usize>, ttl: Ttl) {
        if let Some(r) = self.runs.get_mut(&format!("{agent}\u{1}{run}")) {
            r.bps = bps;
            r.ttl = ttl;
        }
    }

    pub fn note_output(&mut self, agent: &str, run: &str, out_tokens: u64, backend: Option<usize>) {
        if let Some(r) = self.runs.get_mut(&format!("{agent}\u{1}{run}")) {
            let o = out_tokens as f64;
            r.out_ewma = if r.out_ewma == 0.0 {
                o
            } else {
                0.7 * r.out_ewma + 0.3 * o
            };
            if backend.is_some() {
                r.backend = backend;
            }
        }
    }

    pub fn lookup(&self, agent: &str, run: &str) -> Option<&RunInfo> {
        self.runs.get(&format!("{agent}\u{1}{run}"))
    }

    fn prune(&mut self, now: i64) {
        let dead: Vec<String> = self
            .runs
            .iter()
            .filter(|(_, r)| now - r.last_ms > IDLE_MS)
            .map(|(k, _)| k.clone())
            .collect();
        for k in dead {
            if let Some(r) = self.runs.remove(&k) {
                for t in r.tails {
                    self.by_tail.remove(&(r.agent.clone(), t));
                }
                if self.by_first.get(&(r.agent.clone(), r.first)) == Some(&k) {
                    self.by_first.remove(&(r.agent.clone(), r.first));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::fingerprint::fingerprint;
    use super::*;

    fn body(sys: &str, turns: &[&str]) -> String {
        let msgs: Vec<String> = turns
            .iter()
            .enumerate()
            .map(|(i, t)| {
                let role = if i % 2 == 0 { "user" } else { "assistant" };
                format!(r#"{{"role":"{role}","content":[{{"type":"text","text":"{t}"}}]}}"#)
            })
            .collect();
        format!(
            r#"{{"model":"m","system":"{sys}","messages":[{}]}}"#,
            msgs.join(",")
        )
    }

    #[test]
    fn infers_runs_from_conversation_growth() {
        let mut t = RunTracker::default();
        let f = |s: &str| fingerprint(s.as_bytes(), true).unwrap();
        let a1 = t.observe("box", None, &f(&body("S", &["task A"])), 0);
        let b1 = t.observe("box", None, &f(&body("S", &["task B"])), 10);
        assert_ne!(a1.run_id, b1.run_id);
        assert!(a1.inferred && a1.prev.is_none());
        let a2 = t.observe("box", None, &f(&body("S", &["task A", "ok", "next"])), 20);
        let b2 = t.observe("box", None, &f(&body("S", &["task B", "sure", "go"])), 30);
        assert_eq!(
            (a2.run_id.as_str(), b2.run_id.as_str()),
            (a1.run_id.as_str(), b1.run_id.as_str())
        );
        assert_eq!(a2.prev.unwrap().turns, 1);
        // Another agent with the same conversation is a different run.
        let c = t.observe("other", None, &f(&body("S", &["task A", "ok", "next"])), 40);
        assert_ne!(c.run_id, a1.run_id);
        assert_eq!(t.len(), 3);
    }

    #[test]
    fn header_wins_and_gaps_are_counted() {
        let mut t = RunTracker::default();
        let f = |s: &str| fingerprint(s.as_bytes(), true).unwrap();
        let a = t.observe("box", Some("r1"), &f(&body("S", &["x"])), 0);
        assert!(!a.inferred);
        // History rewritten by the agent: the header still ties it together.
        let b = t.observe(
            "box",
            Some("r1"),
            &f(&body("S", &["summary", "ok", "y"])),
            400_000,
        );
        assert_eq!(b.run_id, "r1");
        assert!(b.prev.is_some());
        let s = t.agent("box");
        assert_eq!((s.runs, s.turns, s.gaps, s.long_gaps), (1, 2, 1, 1));
        t.prune(400_000 + IDLE_MS + 1);
        assert!(t.is_empty());
    }
}
