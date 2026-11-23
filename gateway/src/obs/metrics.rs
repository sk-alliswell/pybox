//! In-process metrics, exposed in Prometheus text format at
//! `GET /gateway/metrics`.
//!
//! Model-call and tool-call metrics follow the OpenTelemetry GenAI and MCP
//! semantic conventions (names, units, attributes and bucket boundaries),
//! translated to Prometheus naming the way an OpenTelemetry Collector does:
//! dots become underscores, a `_seconds` unit suffix on durations, `_total`
//! on counters. Gateway-specific series use the `pybox_gateway_` prefix.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::Mutex;

/// The convention's advisory boundaries for duration histograms, in seconds.
const SECONDS: [f64; 14] = [
    0.01, 0.02, 0.04, 0.08, 0.16, 0.32, 0.64, 1.28, 2.56, 5.12, 10.24, 20.48, 40.96, 81.92,
];
/// The convention's advisory boundaries for per-operation token counts.
const TOKENS: [f64; 14] = [
    1.0, 4.0, 16.0, 64.0, 256.0, 1024.0, 4096.0, 16384.0, 65536.0, 262144.0, 1048576.0, 4194304.0,
    16777216.0, 67108864.0,
];
/// Label sets kept per metric. Models and tools are client-chosen strings, so
/// growth has to stop somewhere; later series fold into `_OTHER`.
const MAX_SERIES: usize = 512;

type Labels = Vec<(&'static str, String)>;

#[derive(Clone)]
struct Hist {
    bounds: &'static [f64; 14],
    counts: [u64; 15],
    sum: f64,
    n: u64,
}

impl Hist {
    fn new(bounds: &'static [f64; 14]) -> Self {
        Self {
            bounds,
            counts: [0; 15],
            sum: 0.0,
            n: 0,
        }
    }
    fn observe(&mut self, x: f64) {
        let i = self.bounds.partition_point(|b| *b < x);
        self.counts[i] += 1;
        self.sum += x;
        self.n += 1;
    }
}

#[derive(Default)]
struct Registry {
    hists: BTreeMap<&'static str, BTreeMap<Labels, Hist>>,
    counters: BTreeMap<&'static str, BTreeMap<Labels, f64>>,
}

fn capped<V>(
    series: &mut BTreeMap<Labels, V>,
    mut labels: Labels,
    make: impl FnOnce() -> V,
) -> &mut V {
    if !series.contains_key(&labels) && series.len() >= MAX_SERIES {
        // Keep low-cardinality labels, fold the client-chosen ones.
        for (k, v) in &mut labels {
            if matches!(
                *k,
                "gen_ai_request_model" | "gen_ai_response_model" | "gen_ai_tool_name" | "agent"
            ) {
                *v = "_OTHER".into();
            }
        }
    }
    series.entry(labels).or_insert_with(make)
}

#[derive(Default)]
pub struct Metrics {
    reg: Mutex<Registry>,
}

/// One finished model call.
#[derive(Debug, Default)]
pub struct InferenceSample<'a> {
    pub operation: &'a str,
    pub provider: &'a str,
    pub request_model: &'a str,
    pub response_model: &'a str,
    pub server_address: &'a str,
    pub server_port: Option<u16>,
    pub error_type: Option<&'a str>,
    pub duration_s: f64,
    /// Streaming calls that produced output only.
    pub time_to_first_chunk_s: Option<f64>,
    pub queue_s: f64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_read_tokens: u64,
    pub cache_write_tokens: u64,
    pub cost_usd: f64,
    pub upstream: &'a str,
}

impl Registry {
    fn hist(&mut self, name: &'static str, bounds: &'static [f64; 14], labels: Labels, x: f64) {
        if !x.is_finite() || x < 0.0 {
            return;
        }
        capped(self.hists.entry(name).or_default(), labels, || {
            Hist::new(bounds)
        })
        .observe(x);
    }

    fn count(&mut self, name: &'static str, labels: Labels, by: f64) {
        if by <= 0.0 || !by.is_finite() {
            return;
        }
        *capped(self.counters.entry(name).or_default(), labels, || 0.0) += by;
    }
}

impl Metrics {
    pub fn inference(&self, s: &InferenceSample) {
        // One lock for the whole sample: this runs once per request.
        let mut reg = self.reg.lock().unwrap();
        let mut base: Labels = vec![
            ("gen_ai_operation_name", s.operation.to_string()),
            ("gen_ai_provider_name", s.provider.to_string()),
            ("gen_ai_request_model", s.request_model.to_string()),
        ];
        if !s.response_model.is_empty() {
            base.push(("gen_ai_response_model", s.response_model.to_string()));
        }
        if !s.server_address.is_empty() {
            base.push(("server_address", s.server_address.to_string()));
            if let Some(p) = s.server_port {
                base.push(("server_port", p.to_string()));
            }
        }
        let mut with_error = base.clone();
        if let Some(e) = s.error_type {
            with_error.push(("error_type", e.to_string()));
        }
        reg.hist(
            "gen_ai_client_inference_duration_seconds",
            &SECONDS,
            with_error.clone(),
            s.duration_s,
        );
        if let Some(t) = s.time_to_first_chunk_s {
            reg.hist(
                "gen_ai_client_inference_time_to_first_chunk_seconds",
                &SECONDS,
                base.clone(),
                t,
            );
        }
        if s.input_tokens + s.output_tokens > 0 {
            reg.hist(
                "gen_ai_client_inference_operation_input_tokens",
                &TOKENS,
                with_error.clone(),
                s.input_tokens as f64,
            );
            reg.hist(
                "gen_ai_client_inference_operation_output_tokens",
                &TOKENS,
                with_error,
                s.output_tokens as f64,
            );
            // The gateway does not split usage by modality.
            let mut usage = base.clone();
            usage.push(("gen_ai_token_modality", "unknown".into()));
            for (name, n) in [
                (
                    "gen_ai_client_inference_usage_input_tokens_total",
                    s.input_tokens,
                ),
                (
                    "gen_ai_client_inference_usage_output_tokens_total",
                    s.output_tokens,
                ),
                (
                    "gen_ai_client_inference_usage_cache_read_input_tokens_total",
                    s.cache_read_tokens,
                ),
                (
                    "gen_ai_client_inference_usage_cache_write_input_tokens_total",
                    s.cache_write_tokens,
                ),
            ] {
                reg.count(name, usage.clone(), n as f64);
            }
        }
        let own: Labels = vec![
            ("upstream", s.upstream.to_string()),
            ("gen_ai_request_model", s.request_model.to_string()),
        ];
        reg.count("pybox_gateway_cost_usd_total", own, s.cost_usd);
        reg.hist(
            "pybox_gateway_queue_wait_seconds",
            &SECONDS,
            vec![("upstream", s.upstream.to_string())],
            s.queue_s,
        );
    }

    /// One MCP request relayed for an agent.
    pub fn tool(
        &self,
        server_address: &str,
        method: &str,
        tool: &str,
        error_type: Option<&str>,
        duration_s: f64,
    ) {
        let mut reg = self.reg.lock().unwrap();
        let mut labels: Labels = vec![("mcp_method_name", method.to_string())];
        if !tool.is_empty() {
            labels.push(("gen_ai_tool_name", tool.to_string()));
            labels.push(("gen_ai_operation_name", "execute_tool".into()));
        }
        if !server_address.is_empty() {
            labels.push(("server_address", server_address.to_string()));
        }
        if let Some(e) = error_type {
            labels.push(("error_type", e.to_string()));
        }
        reg.hist(
            "mcp_client_operation_duration_seconds",
            &SECONDS,
            labels,
            duration_s,
        );
    }

    /// A request the gateway refused itself, by reason.
    pub fn refused(&self, reason: &'static str, scope: &str) {
        self.reg.lock().unwrap().count(
            "pybox_gateway_refused_total",
            vec![("reason", reason.to_string()), ("scope", scope.to_string())],
            1.0,
        );
    }

    /// Prometheus text exposition of everything recorded, followed by
    /// `gauges` (current values the caller reads elsewhere).
    pub fn render(&self, gauges: &[(&str, &str, Labels, f64)]) -> String {
        let reg = self.reg.lock().unwrap();
        let mut out = String::with_capacity(8192);
        for (name, series) in &reg.counters {
            let _ = writeln!(out, "# TYPE {name} counter");
            for (labels, v) in series {
                let _ = writeln!(out, "{name}{} {v}", fmt_labels(labels, None));
            }
        }
        for (name, series) in &reg.hists {
            let _ = writeln!(out, "# TYPE {name} histogram");
            for (labels, h) in series {
                let mut cum = 0u64;
                for (i, bound) in h.bounds.iter().enumerate() {
                    cum += h.counts[i];
                    let le = bound.to_string();
                    let _ = writeln!(out, "{name}_bucket{} {cum}", fmt_labels(labels, Some(&le)));
                }
                let _ = writeln!(
                    out,
                    "{name}_bucket{} {}",
                    fmt_labels(labels, Some("+Inf")),
                    h.n
                );
                let _ = writeln!(out, "{name}_sum{} {}", fmt_labels(labels, None), h.sum);
                let _ = writeln!(out, "{name}_count{} {}", fmt_labels(labels, None), h.n);
            }
        }
        let mut last = "";
        for (name, help, labels, v) in gauges {
            if *name != last {
                let _ = writeln!(out, "# HELP {name} {help}");
                let _ = writeln!(out, "# TYPE {name} gauge");
                last = name;
            }
            let _ = writeln!(out, "{name}{} {v}", fmt_labels(labels, None));
        }
        out
    }
}

fn fmt_labels(labels: &Labels, le: Option<&str>) -> String {
    if labels.is_empty() && le.is_none() {
        return String::new();
    }
    let mut s = String::from("{");
    for (i, (k, v)) in labels.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        s.push_str(k);
        s.push_str("=\"");
        for c in v.chars() {
            match c {
                '\\' => s.push_str("\\\\"),
                '"' => s.push_str("\\\""),
                '\n' => s.push_str("\\n"),
                c => s.push(c),
            }
        }
        s.push('"');
    }
    if let Some(le) = le {
        if !labels.is_empty() {
            s.push(',');
        }
        let _ = write!(s, "le=\"{le}\"");
    }
    s.push('}');
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_convention_names_and_cumulative_buckets() {
        let m = Metrics::default();
        for (dur, err) in [(0.5, None), (3.0, None), (0.015, Some("429"))] {
            m.inference(&InferenceSample {
                operation: "chat",
                provider: "anthropic",
                request_model: "claude-sonnet-5",
                error_type: err,
                duration_s: dur,
                time_to_first_chunk_s: err.is_none().then_some(0.2),
                input_tokens: if err.is_none() { 1000 } else { 0 },
                output_tokens: if err.is_none() { 50 } else { 0 },
                cache_read_tokens: 800,
                upstream: "llm",
                cost_usd: 0.01,
                ..InferenceSample::default()
            });
        }
        m.tool("mcp.example.com", "tools/call", "search", None, 0.03);
        let text = m.render(&[(
            "pybox_gateway_inflight",
            "Calls in flight.",
            vec![("upstream", "llm".into())],
            3.0,
        )]);
        let ok = r#"gen_ai_operation_name="chat",gen_ai_provider_name="anthropic",gen_ai_request_model="claude-sonnet-5""#;
        assert!(text.contains(&format!(
            "gen_ai_client_inference_duration_seconds_bucket{{{ok},le=\"0.64\"}} 1"
        )));
        assert!(text.contains(&format!(
            "gen_ai_client_inference_duration_seconds_bucket{{{ok},le=\"+Inf\"}} 2"
        )));
        assert!(text.contains(&format!(
            "gen_ai_client_inference_duration_seconds_count{{{ok},error_type=\"429\"}} 1"
        )));
        assert!(text.contains(&format!(
            "gen_ai_client_inference_usage_input_tokens_total{{{ok},gen_ai_token_modality=\"unknown\"}} 2000"
        )));
        assert!(text.contains(&format!(
            "gen_ai_client_inference_time_to_first_chunk_seconds_count{{{ok}}} 2"
        )));
        assert!(text.contains(
            "mcp_client_operation_duration_seconds_bucket{mcp_method_name=\"tools/call\",gen_ai_tool_name=\"search\",gen_ai_operation_name=\"execute_tool\",server_address=\"mcp.example.com\",le=\"0.04\"} 1"
        ));
        assert!(text.contains("pybox_gateway_inflight{upstream=\"llm\"} 3"));
        assert!(text.contains("# TYPE gen_ai_client_inference_duration_seconds histogram"));
    }

    #[test]
    fn series_growth_is_capped() {
        let m = Metrics::default();
        for i in 0..(MAX_SERIES + 50) {
            m.inference(&InferenceSample {
                operation: "chat",
                provider: "openai",
                request_model: &format!("model-{i}"),
                duration_s: 0.1,
                upstream: "llm",
                ..InferenceSample::default()
            });
        }
        let reg = m.reg.lock().unwrap();
        let n = reg.hists["gen_ai_client_inference_duration_seconds"].len();
        assert!(n <= MAX_SERIES + 1, "{n}");
    }
}
