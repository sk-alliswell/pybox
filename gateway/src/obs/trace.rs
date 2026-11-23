//! Per-request trace context: ids, timings, and the final request row.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use axum::http::HeaderMap;
use rand::Rng;
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};

use super::store::{Row, Table};
use super::{now_ms, Obs};
use crate::usage::{ResponseMeta, Usage};
use crate::AgentInfo;

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

pub fn new_span_id() -> String {
    hex(&rand::rng().random::<[u8; 8]>())
}

fn new_trace_id() -> String {
    hex(&rand::rng().random::<[u8; 16]>())
}

/// `00-<trace>-<parent>-<flags>` → (trace id, parent span id).
pub fn parse_traceparent(v: &str) -> Option<(String, String)> {
    let mut it = v.trim().split('-');
    let (ver, trace, parent, _flags) = (it.next()?, it.next()?, it.next()?, it.next()?);
    let ok = |s: &str, n: usize| {
        s.len() == n && s.bytes().all(|b| b.is_ascii_hexdigit()) && s.bytes().any(|b| b != b'0')
    };
    (ver.len() == 2 && ver != "ff" && ok(trace, 32) && ok(parent, 16))
        .then(|| (trace.to_ascii_lowercase(), parent.to_ascii_lowercase()))
}

/// Trace and parent span for a call: the caller's `traceparent` when it sent
/// one, else an id derived from its run so a run's model and tool calls line
/// up without a tracing SDK in the agent, else a fresh trace.
pub(crate) fn trace_ids(headers: &HeaderMap, agent: &AgentInfo) -> (String, String) {
    let run_id = header(headers, "x-gateway-run-id").unwrap_or("");
    match header(headers, "traceparent").and_then(parse_traceparent) {
        Some(tp) => tp,
        None if !run_id.is_empty() => {
            let mut h = Sha256::new();
            h.update(agent.session_id.as_deref().unwrap_or(&agent.id));
            h.update([0]);
            h.update(run_id);
            (hex(&h.finalize()[..16]), String::new())
        }
        None => (new_trace_id(), String::new()),
    }
}

pub(crate) fn header<'a>(h: &'a HeaderMap, name: &str) -> Option<&'a str> {
    h.get(name)?
        .to_str()
        .ok()
        .map(str::trim)
        .filter(|s| !s.is_empty() && s.len() <= 200)
}

/// What the response side learned, handed to [`ReqCtx::finish`].
#[derive(Default)]
pub struct Outcome {
    pub status: u16,
    pub usage: Usage,
    pub complete: bool,
    pub resp_bytes: u64,
    pub chunks: u32,
    pub first_byte: Option<Instant>,
    pub first_token: Option<Instant>,
    pub max_chunk_gap_ms: f64,
    pub error_type: Option<String>,
    /// Token counts are the gateway's estimate: the stream ended before the
    /// upstream reported usage.
    pub usage_estimated: bool,
    pub meta: ResponseMeta,
}

pub struct ReqCtx {
    obs: Arc<Obs>,
    pub t0: Instant,
    ts_ms: i64,
    pub trace_id: String,
    pub root_id: String,
    parent_id: String,
    dims: Map<String, Value>,
    spans: Vec<Row>,
    model: String,
    anthropic_format: bool,
    stream: bool,
    // Filled in along the way by the proxy path.
    pub queue_ms: f64,
    pub retry_wait_ms: f64,
    pub upstream_ttfb_ms: f64,
    pub pre_ms: Option<f64>,
    pub attempts: u32,
    pub cache: &'static str,
    pub upstream_ip: String,
    /// `ip:port` of the LiteLLM server that answered.
    pub upstream_instance: String,
    /// Key alias from config, used when LiteLLM does not name the key.
    pub litellm_key_default: String,
    pub upstream_http: String,
    pub upstream_req_bytes: u64,
    pub litellm: Map<String, Value>,
    /// The attempt whose response is being relayed: (span id, start, attrs).
    /// Closed when the request finishes so it covers the whole stream, like
    /// the LiteLLM server span parented to it.
    pub open_attempt: Option<(String, Instant, Value)>,
    /// `gen_ai.operation.name`, from the request path.
    operation: &'static str,
    /// `gen_ai.provider.name` and the upstream's address, set by the proxy.
    pub provider: String,
    pub server_address: String,
    pub server_port: Option<u16>,
    pub max_tokens: Option<u64>,
    /// Cost ledger and cache accounting, when enabled.
    pub lead: Option<Arc<crate::lead::engine::Lead>>,
    pub lead_fin: Option<crate::lead::engine::Fin>,
    /// Index of the backend that served the request (fair policy).
    pub backend: Option<usize>,
    done: bool,
}

impl ReqCtx {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        obs: &Arc<Obs>,
        agent: &AgentInfo,
        upstream: &str,
        cluster: &str,
        path: &str,
        headers: &HeaderMap,
        peer: Option<SocketAddr>,
        client_http: &str,
        model: &str,
        stream: bool,
        body_len: usize,
    ) -> Self {
        let run_id = header(headers, "x-gateway-run-id").unwrap_or("");
        let (trace_id, parent_id) = trace_ids(headers, agent);
        let ts_ms = now_ms();
        let client_to_gw_ms = header(headers, "x-gateway-client-sent-at")
            .and_then(|v| v.parse::<i64>().ok())
            // Microseconds since the epoch; tolerate milliseconds.
            .map(|v| {
                if v > 100_000_000_000_000 {
                    v as f64 / 1000.0
                } else {
                    v as f64
                }
            })
            .map(|sent_ms| ts_ms as f64 - sent_ms)
            .filter(|d| (-1000.0..600_000.0).contains(d));
        let anthropic_format = path.contains("/messages");
        let mut dims = Map::new();
        let mut put = |k: &str, v: Value| {
            dims.insert(k.to_string(), v);
        };
        put(
            "project_id",
            json!(agent.project_id.as_deref().unwrap_or("")),
        );
        put("team_id", json!(agent.team_id.as_deref().unwrap_or("")));
        put("user_id", json!(agent.user_id.as_deref().unwrap_or("")));
        put("agent_id", json!(agent.id));
        put(
            "session_id",
            json!(agent.session_id.as_deref().unwrap_or("")),
        );
        put("run_id", json!(run_id));
        put(
            "deliverable_id",
            json!(header(headers, "x-gateway-deliverable-id").unwrap_or("")),
        );
        // Client-claimed end user, kept apart from the token-bound user.
        put(
            "claimed_user_id",
            json!(header(headers, "x-gateway-user-id").unwrap_or("")),
        );
        put("gateway_key_id", json!(agent.key_id));
        put("upstream", json!(upstream));
        put("litellm_cluster", json!(cluster));
        put(
            "client_ip",
            json!(peer.map(|p| p.ip().to_string()).unwrap_or_default()),
        );
        put(
            "client_conn",
            json!(peer.map(|p| p.to_string()).unwrap_or_default()),
        );
        put("client_http", json!(client_http));
        put(
            "api_format",
            json!(if anthropic_format {
                "anthropic"
            } else {
                "openai"
            }),
        );
        put("path", json!(path.split('?').next().unwrap_or("")));
        put("model", json!(model));
        put("stream", json!(stream));
        put("client_req_bytes", json!(body_len));
        if let Some(d) = client_to_gw_ms {
            put("client_to_gw_ms", json!(d));
        }
        Self {
            obs: obs.clone(),
            t0: Instant::now(),
            ts_ms,
            trace_id,
            root_id: new_span_id(),
            parent_id,
            dims,
            spans: Vec::new(),
            model: model.to_string(),
            anthropic_format,
            stream,
            queue_ms: 0.0,
            retry_wait_ms: 0.0,
            upstream_ttfb_ms: 0.0,
            pre_ms: None,
            attempts: 0,
            cache: "off",
            upstream_ip: String::new(),
            upstream_instance: String::new(),
            litellm_key_default: String::new(),
            upstream_http: String::new(),
            upstream_req_bytes: 0,
            litellm: Map::new(),
            open_attempt: None,
            operation: super::genai::operation(path),
            provider: String::new(),
            server_address: String::new(),
            server_port: None,
            max_tokens: None,
            lead: None,
            lead_fin: None,
            backend: None,
            done: false,
        }
    }

    fn ms_since_start(&self, at: Instant) -> f64 {
        at.saturating_duration_since(self.t0).as_secs_f64() * 1000.0
    }

    pub fn traceparent(&self, span_id: &str) -> String {
        format!("00-{}-{}-01", self.trace_id, span_id)
    }

    /// Set a column of the request row.
    pub fn put(&mut self, key: &str, v: Value) {
        self.dims.insert(key.to_string(), v);
    }

    pub fn dim(&self, key: &str) -> &str {
        self.dims.get(key).and_then(Value::as_str).unwrap_or("")
    }

    /// Record a finished child span of the root.
    #[allow(clippy::too_many_arguments)]
    pub fn span(
        &mut self,
        id: String,
        name: &str,
        kind: &str,
        start: Instant,
        end: Instant,
        ok: bool,
        attrs: Value,
    ) {
        let start_ms = self.ms_since_start(start);
        self.spans.push(span_row(
            self.ts_ms,
            start_ms,
            self.ms_since_start(end) - start_ms,
            &self.trace_id,
            &id,
            &self.root_id,
            name,
            kind,
            ok,
            attrs,
        ));
    }

    /// Pull the LiteLLM response headers worth keeping.
    pub fn absorb_response_headers(&mut self, h: &HeaderMap) {
        let text = |k: &str| header(h, k).map(str::to_string);
        let num = |k: &str| {
            header(h, k)
                .and_then(|v| v.parse::<f64>().ok())
                .filter(|x| x.is_finite())
        };
        self.litellm.clear();
        if let Some(v) = text("x-litellm-call-id") {
            self.litellm.insert("litellm_call_id".into(), json!(v));
        }
        if let Some(v) = text("x-litellm-model-id") {
            self.litellm.insert("litellm_model_id".into(), json!(v));
        }
        if let Some(v) = text("x-litellm-model-api-base") {
            self.litellm.insert("provider_api_base".into(), json!(v));
        }
        if let Some(v) = text("x-litellm-key-alias").or_else(|| text("x-litellm-key-name")) {
            self.litellm.insert("litellm_key".into(), json!(v));
        }
        if let Some(v) = num("x-litellm-response-cost") {
            self.litellm.insert("litellm_cost_usd".into(), json!(v));
        }
        if let Some(v) = num("x-litellm-overhead-duration-ms") {
            self.litellm.insert("litellm_overhead_ms".into(), json!(v));
        }
        if let Some(v) = num("x-litellm-response-duration-ms") {
            self.litellm.insert("litellm_duration_ms".into(), json!(v));
        }
        if let Some(v) = num("x-litellm-attempted-retries") {
            self.litellm.insert("litellm_retries".into(), json!(v));
        }
        if let Some(v) = num("x-litellm-attempted-fallbacks") {
            self.litellm.insert("litellm_fallbacks".into(), json!(v));
        }
        // LiteLLM sets x-litellm-cache-key when its response cache answered.
        self.litellm.insert(
            "litellm_cache_hit".into(),
            json!(h.contains_key("x-litellm-cache-key")),
        );
    }

    pub fn finish(&mut self, o: Outcome) {
        if std::mem::replace(&mut self.done, true) {
            return;
        }
        let end = Instant::now();
        let total_ms = self.ms_since_start(end);
        if let Some((id, start, attrs)) = self.open_attempt.take() {
            self.span(
                id,
                "gateway.attempt",
                "client",
                start,
                end,
                o.status != 0 && o.status < 400,
                attrs,
            );
        }
        let mut row = std::mem::take(&mut self.dims);
        let mut put = |k: &str, v: Value| {
            row.insert(k.to_string(), v);
        };
        put("ts", json!(self.ts_ms));
        put("trace_id", json!(self.trace_id));
        put("span_id", json!(self.root_id));
        put("status", json!(o.status));
        let is_error = o.status == 0 || o.status >= 400;
        put("is_error", json!(is_error));
        if let Some(e) = &o.error_type {
            put("error_type", json!(e));
        }
        put("attempts", json!(self.attempts));
        put("operation", json!(self.operation));
        put("provider", json!(self.provider));
        for (k, v) in [
            ("response_model", &o.meta.model),
            ("response_id", &o.meta.id),
            ("finish_reason", &o.meta.finish_reason),
        ] {
            if !v.is_empty() {
                put(k, json!(v));
            }
        }
        // Ended without a complete response and without an upstream error.
        put("client_abort", json!(!o.complete && o.error_type.is_none()));
        put("gateway_cache", json!(self.cache));
        put("upstream_ip", json!(self.upstream_ip));
        put("litellm_instance", json!(self.upstream_instance));
        put("litellm_key", json!(self.litellm_key_default));
        put("upstream_http", json!(self.upstream_http));
        put("upstream_req_bytes", json!(self.upstream_req_bytes));
        put("upstream_resp_bytes", json!(o.resp_bytes));
        put("client_resp_bytes", json!(o.resp_bytes));
        put("chunks", json!(o.chunks));

        // Timings.
        // A caller that left before any attempt spent all its time waiting
        // for a place; the grant that would have recorded the wait never came.
        let queue_ms = if self.attempts == 0 && o.status == 0 {
            (total_ms - self.pre_ms.unwrap_or(0.0)).max(self.queue_ms)
        } else {
            self.queue_ms
        };
        put("queue_ms", json!(queue_ms));
        put("retry_wait_ms", json!(self.retry_wait_ms));
        put("gw_overhead_ms", json!(self.pre_ms.unwrap_or(0.0)));
        put("upstream_ttfb_ms", json!(self.upstream_ttfb_ms));
        put("total_ms", json!(total_ms));
        if self.stream {
            if let Some(first) = o.first_token.or(o.first_byte) {
                let ttft = self.ms_since_start(first);
                let gen_ms = (total_ms - ttft).max(0.0);
                put("ttft_ms", json!(ttft));
                // Time to first token with gateway queueing taken out.
                put(
                    "model_ttft_ms",
                    json!((ttft - self.queue_ms - self.retry_wait_ms).max(0.0)),
                );
                put("gen_ms", json!(gen_ms));
                if o.usage.output > 1 && gen_ms > 0.0 {
                    put(
                        "output_tps",
                        json!(o.usage.output as f64 / (gen_ms / 1000.0)),
                    );
                }
                put("max_chunk_gap_ms", json!(o.max_chunk_gap_ms));
            }
        }

        let u = o.usage;
        let billed = super::bill(self.obs.price(&self.model), u, self.anthropic_format);
        let (fresh, total_in) = (billed.fresh_input, billed.total_input);
        put("input_tokens", json!(total_in));
        put("fresh_input_tokens", json!(fresh));
        put("output_tokens", json!(u.output));
        put("cache_read_tokens", json!(u.cache_read));
        put("cache_write_tokens", json!(u.cache_write));
        put("total_tokens", json!(total_in + u.output));
        put("prompt_cache_hit", json!(u.cache_read > 0));
        if o.usage_estimated {
            put("usage_estimated", json!(true));
        }

        // Cost: what LiteLLM reported, else the price table.
        let reported = self
            .litellm
            .get("litellm_cost_usd")
            .and_then(Value::as_f64)
            .filter(|c| *c > 0.0);
        let price = self.obs.price(&self.model);
        let (cost_usd, source) = if self.cache == "hit" || self.cache == "shared" {
            (0.0, "gateway_cache")
        } else if let Some(c) = reported {
            (c, "litellm")
        } else if let Some(c) = billed.cost_usd {
            (c, "price_table")
        } else {
            (0.0, "unknown")
        };
        put("cost_usd", json!(cost_usd));
        put("cost_source", json!(source));
        if let Some(saved) = billed.cache_saved_usd {
            put("cache_saved_usd", json!(saved));
        }
        if let Some(lead) = self.lead.take() {
            let text = |k: &str| row.get(k).and_then(Value::as_str).unwrap_or("").to_string();
            let (project, team, run, deliverable) = (
                text("project_id"),
                text("team_id"),
                text("run_id"),
                text("deliverable_id"),
            );
            lead.finish(
                self.lead_fin.as_ref(),
                &crate::lead::engine::Done {
                    ts: self.ts_ms,
                    project: &project,
                    team: &team,
                    run: &run,
                    deliverable: &deliverable,
                    fresh,
                    cache_read: u.cache_read,
                    cache_write: u.cache_write,
                    total_in,
                    output: u.output,
                    cost_usd,
                    failed: is_error || (!o.complete && o.error_type.is_none()),
                    price,
                    backend: self.backend,
                },
                &mut row,
            );
        }
        for (k, v) in std::mem::take(&mut self.litellm) {
            row.insert(k, v);
        }

        // The root span carries the GenAI convention's attributes, plus the
        // gateway's own under `pybox.`.
        let text = |k: &str| row.get(k).and_then(Value::as_str).unwrap_or("");
        let status_text = o.status.to_string();
        let error_type = if is_error {
            Some(match o.error_type.as_deref() {
                Some("upstream_status") | None if o.status != 0 => status_text.as_str(),
                Some(e) => e,
                None => "_OTHER",
            })
        } else {
            None
        };
        let ttft_ms = row.get("ttft_ms").and_then(Value::as_f64);
        let cost_usd = row.get("cost_usd").and_then(Value::as_f64).unwrap_or(0.0);
        let mut attrs = super::genai::inference_attrs(&super::genai::Inference {
            operation: self.operation,
            provider: &self.provider,
            request_model: &self.model,
            response_model: &o.meta.model,
            response_id: &o.meta.id,
            finish_reason: &o.meta.finish_reason,
            stream: self.stream,
            max_tokens: self.max_tokens,
            input_tokens: total_in,
            output_tokens: u.output,
            cache_read_tokens: u.cache_read,
            cache_write_tokens: u.cache_write,
            ttft_ms,
            error_type,
            server_address: &self.server_address,
            server_port: self.server_port,
            conversation_id: text("session_id"),
            agent_id: text("agent_id"),
        });
        if o.status != 0 {
            attrs.insert("http.response.status_code".into(), json!(o.status));
        }
        for (k, col) in [
            ("pybox.project_id", "project_id"),
            ("pybox.team_id", "team_id"),
            ("pybox.run_id", "run_id"),
            ("pybox.deliverable_id", "deliverable_id"),
        ] {
            if !text(col).is_empty() {
                attrs.insert(k.into(), json!(text(col)));
            }
        }
        attrs.insert("pybox.queue_ms".into(), json!(self.queue_ms));
        attrs.insert("pybox.attempts".into(), json!(self.attempts));
        attrs.insert("pybox.gateway_cache".into(), json!(self.cache));
        attrs.insert("pybox.cost_usd".into(), json!(cost_usd));
        if row.get("client_abort") == Some(&json!(true)) {
            attrs.insert("pybox.client_abort".into(), json!(true));
        }
        self.obs
            .metrics
            .inference(&super::metrics::InferenceSample {
                operation: self.operation,
                provider: &self.provider,
                request_model: &self.model,
                response_model: &o.meta.model,
                server_address: &self.server_address,
                server_port: self.server_port,
                error_type,
                duration_s: total_ms / 1000.0,
                time_to_first_chunk_s: ttft_ms.filter(|_| !is_error).map(|ms| ms / 1000.0),
                queue_s: self.queue_ms / 1000.0,
                input_tokens: total_in,
                output_tokens: u.output,
                cache_read_tokens: u.cache_read,
                cache_write_tokens: u.cache_write,
                cost_usd,
                upstream: text("upstream"),
            });

        let root = span_row(
            self.ts_ms,
            0.0,
            total_ms,
            &self.trace_id,
            &self.root_id,
            &self.parent_id,
            "gateway.request",
            "server",
            !is_error,
            Value::Object(attrs),
        );
        let mut spans = std::mem::take(&mut self.spans);
        spans.push(root);
        let cell = |k: &str| row.get(k).and_then(Value::as_str).unwrap_or("").to_string();
        self.obs.recent.note(
            &self.trace_id,
            super::client::Seen {
                ts_ms: self.ts_ms,
                span_id: self.root_id.clone(),
                agent: cell("agent_id"),
                model: cell("model"),
                upstream: cell("upstream"),
                ttft_ms: row.get("ttft_ms").and_then(Value::as_f64),
                total_ms,
            },
        );
        self.obs
            .emit(Table::Requests, vec![row.into_iter().collect()]);
        self.obs.emit_own_spans(spans);
    }
}

impl Drop for ReqCtx {
    /// A request dropped before any outcome (client went away while queued).
    fn drop(&mut self) {
        self.finish(Outcome::default());
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn span_row(
    base_ts_ms: i64,
    start_ms: f64,
    duration_ms: f64,
    trace: &str,
    id: &str,
    parent: &str,
    name: &str,
    kind: &str,
    ok: bool,
    attrs: Value,
) -> Row {
    let start_us = base_ts_ms * 1000 + (start_ms * 1000.0) as i64;
    [
        ("ts", json!(start_us / 1000)),
        ("start_us", json!(start_us)),
        ("duration_ms", json!(duration_ms.max(0.0))),
        ("trace_id", json!(trace)),
        ("span_id", json!(id)),
        ("parent_span_id", json!(parent)),
        ("service", json!("pybox-gateway")),
        ("instance", json!("")),
        ("name", json!(name)),
        ("kind", json!(kind)),
        ("status", json!(if ok { "ok" } else { "error" })),
        ("attrs", attrs),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v))
    .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn traceparent_parsing() {
        let (t, p) =
            parse_traceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01").unwrap();
        assert_eq!(t, "4bf92f3577b34da6a3ce929d0e0e4736");
        assert_eq!(p, "00f067aa0ba902b7");
        for bad in [
            "",
            "00-abc-def-01",
            "00-00000000000000000000000000000000-00f067aa0ba902b7-01",
            "ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7",
        ] {
            assert!(parse_traceparent(bad).is_none(), "{bad}");
        }
    }
}
