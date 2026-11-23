//! In-memory counters exposed at `GET /gateway/stats`.

use std::collections::HashMap;
use std::sync::Mutex;

use serde::Serialize;

use crate::usage::Usage;

#[derive(Debug, Default, Clone, Serialize)]
pub struct AgentStats {
    pub requests: u64,
    pub streams: u64,
    pub errors: u64,
    pub retries: u64,
    pub cache_hits: u64,
    pub client_aborts: u64,
    /// Requests refused by a limit rule.
    pub limited: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_read_tokens: u64,
    pub cache_write_tokens: u64,
    pub last_status: u16,
    pub avg_queue_ms: f64,
    pub avg_ttfb_ms: f64,
    #[serde(skip)]
    queue_ms_sum: f64,
    #[serde(skip)]
    ttfb_ms_sum: f64,
    #[serde(skip)]
    samples: u64,
}

impl AgentStats {
    pub fn tokens_used(&self) -> u64 {
        self.input_tokens + self.output_tokens + self.cache_read_tokens + self.cache_write_tokens
    }
}

#[derive(Debug, Default, Clone, Serialize)]
pub struct UpstreamStats {
    pub requests: u64,
    pub retries: u64,
    pub rate_limited: u64,
    pub errors: u64,
    /// Requests refused because the queue was full or the wait too long.
    pub shed: u64,
    /// Rewritten requests the upstream turned down and the gateway re-sent
    /// as the client wrote them.
    pub rewrite_rejected: u64,
    pub status: HashMap<u16, u64>,
}

/// Per `server/tool`, or `server/<method>` for requests that are not tool calls.
#[derive(Debug, Default, Clone, Serialize)]
pub struct McpStats {
    pub calls: u64,
    pub errors: u64,
    /// Refused by the allow/deny policy.
    pub denied: u64,
    /// Refused by a limit rule.
    pub limited: u64,
    pub avg_ms: f64,
    #[serde(skip)]
    ms_sum: f64,
}

#[derive(Default)]
pub struct Stats {
    agents: Mutex<HashMap<String, AgentStats>>,
    upstreams: Mutex<HashMap<String, UpstreamStats>>,
    mcp: Mutex<HashMap<String, McpStats>>,
}

impl Stats {
    pub fn agent<R>(&self, id: &str, f: impl FnOnce(&mut AgentStats) -> R) -> R {
        let mut agents = self.agents.lock().unwrap();
        // Look up first: the entry API would allocate the key on every call.
        if !agents.contains_key(id) {
            agents.insert(id.to_string(), AgentStats::default());
        }
        f(agents.get_mut(id).unwrap())
    }

    pub fn upstream(&self, name: &str, f: impl FnOnce(&mut UpstreamStats)) {
        let mut ups = self.upstreams.lock().unwrap();
        if !ups.contains_key(name) {
            ups.insert(name.to_string(), UpstreamStats::default());
        }
        f(ups.get_mut(name).unwrap())
    }

    pub fn mcp(&self, server: &str, tool: &str, status: &str, duration_ms: f64) {
        let key = format!("{server}/{tool}");
        let mut all = self.mcp.lock().unwrap();
        // Tool names come from agents; do not let them grow this without bound.
        if all.len() >= 2_000 && !all.contains_key(&key) {
            return;
        }
        let s = all.entry(key).or_default();
        s.calls += 1;
        match status {
            "ok" => {}
            "denied" => s.denied += 1,
            "limited" => s.limited += 1,
            _ => s.errors += 1,
        }
        s.ms_sum += duration_ms;
        s.avg_ms = s.ms_sum / s.calls as f64;
    }

    pub fn mcp_snapshot(&self) -> HashMap<String, McpStats> {
        self.mcp.lock().unwrap().clone()
    }

    pub fn tokens_used(&self, id: &str) -> u64 {
        self.agents
            .lock()
            .unwrap()
            .get(id)
            .map(AgentStats::tokens_used)
            .unwrap_or(0)
    }

    pub fn record_timing(&self, id: &str, queue_ms: f64, ttfb_ms: f64) {
        self.agent(id, |a| {
            a.queue_ms_sum += queue_ms;
            a.ttfb_ms_sum += ttfb_ms;
            a.samples += 1;
            a.avg_queue_ms = a.queue_ms_sum / a.samples as f64;
            a.avg_ttfb_ms = a.ttfb_ms_sum / a.samples as f64;
        })
    }

    pub fn record_usage(&self, id: &str, u: Usage) {
        self.agent(id, |a| {
            a.input_tokens += u.input;
            a.output_tokens += u.output;
            a.cache_read_tokens += u.cache_read;
            a.cache_write_tokens += u.cache_write;
        })
    }

    pub fn snapshot(&self) -> (HashMap<String, AgentStats>, HashMap<String, UpstreamStats>) {
        (
            self.agents.lock().unwrap().clone(),
            self.upstreams.lock().unwrap().clone(),
        )
    }
}
