//! MCP tool traffic through the gateway.
//!
//! Agents point their MCP client at `http://127.0.0.1:8787/mcp/<server>`
//! (Streamable HTTP) with their gateway token. The gateway holds the real
//! credential and applies, per call:
//!
//! - which tools an agent may see and call (server allow/deny lists plus the
//!   agent's own `server/tool` patterns); hidden tools are also removed from
//!   `tools/list` so the model never learns about them,
//! - limit rules on tool calls (per agent, team, tool pattern, window),
//! - a per-server concurrency cap with the same fair queue model calls use,
//! - a ceiling on result size, so one oversized result cannot flood a
//!   model's context,
//!
//! and records every request as a row in the `tools` table, a span named per
//! the OpenTelemetry MCP convention (`tools/call <tool>`) and a
//! `mcp.client.operation.duration` sample. Tool calls of a run share the
//! trace of its model calls.
//!
//! Refusals a model can act on (rate limit, oversized result) come back as a
//! tool result with `isError: true`, which MCP clients pass to the model.
//! Calling a tool the agent is not allowed to use is a protocol error, the
//! same one a server gives for an unknown tool.

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::{Body, Bytes};
use axum::extract::{ConnectInfo, Path, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use futures_util::{Stream, StreamExt};
use serde_json::{json, Map, Value};

use crate::config::McpServerConfig;
use crate::limiter::RateLimiter;
use crate::limits::{glob, Amount};
use crate::obs::store::{Row, Table};
use crate::obs::trace::{header, new_span_id, span_row, trace_ids};
use crate::obs::{now_ms, Obs};
use crate::scheduler::{Lease, Scheduler, Ticket};
use crate::{error, AgentInfo, App};

/// Headers relayed to the MCP server; everything else stays at the gateway.
const FORWARD: &[&str] = &[
    "accept",
    "content-type",
    "mcp-session-id",
    "mcp-protocol-version",
    "last-event-id",
];
/// One SSE event may be this much larger than the result cap before the
/// stream is cut (progress notifications and framing ride along).
const EVENT_SLACK: usize = 64 << 10;

pub struct Server {
    pub cfg: McpServerConfig,
    client: reqwest::Client,
    auth: Option<HeaderValue>,
    extra: Vec<(HeaderName, HeaderValue)>,
    /// Host and port, for `server.address` / `server.port`.
    address: (String, Option<u16>),
    pub sched: Arc<Scheduler>,
}

impl Server {
    pub fn new(cfg: &McpServerConfig) -> anyhow::Result<Self> {
        let auth = match cfg.resolve_key()? {
            Some(k) => Some(HeaderValue::from_str(&format!("Bearer {k}"))?),
            None => None,
        };
        let mut extra = Vec::new();
        for (k, v) in &cfg.headers {
            extra.push((
                HeaderName::from_bytes(k.as_bytes())?,
                HeaderValue::from_str(v)?,
            ));
        }
        let client = reqwest::Client::builder()
            .pool_idle_timeout(Duration::from_secs(90))
            .tcp_keepalive(Duration::from_secs(30))
            .tcp_nodelay(true)
            .connect_timeout(Duration::from_secs(10))
            .read_timeout(Duration::from_secs(cfg.call_timeout_secs.max(1)))
            .build()?;
        Ok(Self {
            client,
            auth,
            extra,
            address: crate::obs::genai::server_of(&cfg.url),
            sched: Scheduler::spawn(
                format!("mcp:{}", cfg.name),
                cfg.max_concurrency,
                RateLimiter::new(cfg.rpm, None),
            ),
            cfg: cfg.clone(),
        })
    }

    /// May `agent` see and call `tool` on this server?
    pub fn allows(&self, agent: &AgentInfo, tool: &str) -> bool {
        let any = |list: &[String], text: &str| list.iter().any(|p| glob(p, text));
        if self.cfg.allow_tools.as_ref().is_some_and(|l| !any(l, tool))
            || any(&self.cfg.deny_tools, tool)
        {
            return false;
        }
        let full = format!("{}/{tool}", self.cfg.name);
        agent.mcp_allow.as_ref().is_none_or(|l| any(l, &full)) && !any(&agent.mcp_deny, &full)
    }

    fn request(&self, method: Method, headers: &HeaderMap) -> reqwest::RequestBuilder {
        let mut out = HeaderMap::new();
        for name in FORWARD {
            if let Some(v) = headers.get(*name) {
                out.insert(HeaderName::from_static(name), v.clone());
            }
        }
        for (k, v) in &self.extra {
            out.insert(k.clone(), v.clone());
        }
        if let Some(v) = &self.auth {
            out.insert(http::header::AUTHORIZATION, v.clone());
        }
        self.client.request(method, &self.cfg.url).headers(out)
    }
}

fn rpc_error(id: &Value, code: i64, message: impl Into<String>) -> Response {
    Json(json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": { "code": code, "message": message.into() },
    }))
    .into_response()
}

/// A tool result the model gets to read.
fn tool_error(id: &Value, text: impl Into<String>) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": { "content": [{ "type": "text", "text": text.into() }], "isError": true },
    })
}

/// Bookkeeping for one JSON-RPC request relayed to a server.
struct Call {
    obs: Option<Arc<Obs>>,
    app: Arc<App>,
    server: String,
    address: (String, Option<u16>),
    method: String,
    tool: String,
    id: Value,
    t0: Instant,
    ts_ms: i64,
    trace_id: String,
    span_id: String,
    parent_id: String,
    dims: Map<String, Value>,
    queue_ms: f64,
    req_bytes: usize,
    resp_bytes: usize,
    session: String,
    protocol: String,
    done: bool,
}

impl Call {
    /// `status` is `ok` or a short reason; `rpc_code` the JSON-RPC error code
    /// when the server (or the gateway) answered with one.
    fn finish(&mut self, status: &str, rpc_code: Option<i64>) {
        if std::mem::replace(&mut self.done, true) {
            return;
        }
        let duration_ms = self.t0.elapsed().as_secs_f64() * 1000.0;
        let what = if self.tool.is_empty() {
            &self.method
        } else {
            &self.tool
        };
        self.app.stats.mcp(&self.server, what, status, duration_ms);
        let Some(obs) = self.obs.take() else { return };
        let is_error = status != "ok";
        // Convention: the JSON-RPC code when there is one, else a short class.
        let error_type = is_error.then(|| rpc_code.map_or(status.to_string(), |c| c.to_string()));
        obs.metrics.tool(
            &self.address.0,
            &self.method,
            &self.tool,
            error_type.as_deref(),
            duration_ms / 1000.0,
        );

        let mut row: Row = std::mem::take(&mut self.dims).into_iter().collect();
        let mut put = |k: &str, v: Value| {
            row.insert(k.to_string(), v);
        };
        put("ts", json!(self.ts_ms));
        put("trace_id", json!(self.trace_id));
        put("span_id", json!(self.span_id));
        put("server", json!(self.server));
        put("method", json!(self.method));
        put("tool", json!(self.tool));
        put("status", json!(status));
        put("is_error", json!(is_error));
        if let Some(c) = rpc_code {
            put("rpc_code", json!(c));
        }
        put("duration_ms", json!(duration_ms));
        put("queue_ms", json!(self.queue_ms));
        put("req_bytes", json!(self.req_bytes));
        put("resp_bytes", json!(self.resp_bytes));
        put("mcp_session_id", json!(self.session));

        let mut attrs = Map::new();
        let mut a = |k: &str, v: Value| {
            attrs.insert(k.to_string(), v);
        };
        a("mcp.method.name", json!(self.method));
        if !self.tool.is_empty() {
            a("gen_ai.tool.name", json!(self.tool));
            a("gen_ai.operation.name", json!("execute_tool"));
        }
        a(
            "jsonrpc.request.id",
            json!(match &self.id {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            }),
        );
        if !self.session.is_empty() {
            a("mcp.session.id", json!(self.session));
        }
        if !self.protocol.is_empty() {
            a("mcp.protocol.version", json!(self.protocol));
        }
        a("network.transport", json!("tcp"));
        a("network.protocol.name", json!("http"));
        a("server.address", json!(self.address.0));
        if let Some(p) = self.address.1 {
            a("server.port", json!(p));
        }
        if let Some(e) = &error_type {
            a("error.type", json!(e));
        }
        if let Some(c) = rpc_code {
            a("rpc.response.status_code", json!(c.to_string()));
        }
        for (k, col) in [
            ("gen_ai.agent.id", "agent_id"),
            ("gen_ai.conversation.id", "session_id"),
            ("pybox.project_id", "project_id"),
            ("pybox.team_id", "team_id"),
            ("pybox.run_id", "run_id"),
            ("pybox.deliverable_id", "deliverable_id"),
        ] {
            if let Some(Value::String(s)) = row.get(col).filter(|v| v.as_str() != Some("")) {
                a(k, json!(s));
            }
        }
        a("pybox.mcp.server", json!(self.server));
        a("pybox.mcp.status", json!(status));
        a("pybox.queue_ms", json!(self.queue_ms));
        let name = if self.tool.is_empty() {
            self.method.clone()
        } else {
            format!("{} {}", self.method, self.tool)
        };
        let span = span_row(
            self.ts_ms,
            0.0,
            duration_ms,
            &self.trace_id,
            &self.span_id,
            &self.parent_id,
            &name,
            "client",
            !is_error,
            Value::Object(attrs),
        );
        obs.emit(Table::Tools, vec![row]);
        obs.emit_own_spans(vec![span]);
    }
}

impl Drop for Call {
    /// The agent went away before the server answered.
    fn drop(&mut self) {
        self.finish("aborted", None);
    }
}

/// What to do with the server's answer to the request being tracked.
struct Reply<'a> {
    server: &'a Server,
    agent: &'a AgentInfo,
    method: &'a str,
}

impl Reply<'_> {
    /// Apply policy to the response message. Returns the replacement when it
    /// changed, the status to record and the JSON-RPC error code if any.
    fn apply(&self, msg: &mut Value, raw_len: usize) -> (bool, &'static str, Option<i64>) {
        if let Some(err) = msg.get("error").filter(|e| !e.is_null()) {
            return (false, "rpc_error", err.get("code").and_then(Value::as_i64));
        }
        match self.method {
            "tools/list" => {
                let Some(tools) = msg
                    .get_mut("result")
                    .and_then(|r| r.get_mut("tools"))
                    .and_then(Value::as_array_mut)
                else {
                    return (false, "ok", None);
                };
                let before = tools.len();
                tools.retain(|t| {
                    t.get("name")
                        .and_then(Value::as_str)
                        .is_some_and(|n| self.server.allows(self.agent, n))
                });
                (tools.len() != before, "ok", None)
            }
            "tools/call" => {
                let max = self.server.cfg.max_result_bytes;
                if raw_len > max {
                    let id = msg.get("id").cloned().unwrap_or(Value::Null);
                    *msg = tool_error(
                        &id,
                        format!(
                            "The tool result was {raw_len} bytes, over this gateway's limit of {max} bytes, and was not delivered. Ask for less: a narrower query, fewer fields, or one page at a time."
                        ),
                    );
                    return (true, "too_large", None);
                }
                let failed = msg
                    .get("result")
                    .and_then(|r| r.get("isError"))
                    .and_then(Value::as_bool)
                    == Some(true);
                (false, if failed { "tool_error" } else { "ok" }, None)
            }
            _ => (false, "ok", None),
        }
    }
}

fn same_id(a: &Value, b: &Value) -> bool {
    a == b
}

pub async fn handle(
    State(app): State<Arc<App>>,
    Path(name): Path<String>,
    peer: Option<Extension<ConnectInfo<SocketAddr>>>,
    method: Method,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let Some(server) = app.mcp.get(&name) else {
        return error(
            StatusCode::NOT_FOUND,
            "not_found_error",
            format!("unknown mcp server {name:?}"),
        );
    };
    let Some(agent) = app.agent_for(&headers) else {
        return error(
            StatusCode::UNAUTHORIZED,
            "authentication_error",
            "unknown gateway token",
        );
    };
    if method != Method::POST {
        // GET opens the server-to-client stream, DELETE ends the session.
        return if method == Method::GET || method == Method::DELETE {
            passthrough(server, method, &headers, body).await
        } else {
            error(
                StatusCode::METHOD_NOT_ALLOWED,
                "invalid_request_error",
                "MCP endpoints accept POST, GET and DELETE",
            )
        };
    }
    let mut msg: Value = match serde_json::from_slice(&body) {
        Ok(Value::Object(m)) => Value::Object(m),
        Ok(Value::Array(_)) => {
            return (
                StatusCode::BAD_REQUEST,
                rpc_error(
                    &Value::Null,
                    -32600,
                    "JSON-RPC batches are not supported; send one message per request",
                ),
            )
                .into_response()
        }
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                rpc_error(&Value::Null, -32700, "body is not a JSON-RPC message"),
            )
                .into_response()
        }
    };
    let rpc_method = msg
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let id = msg.get("id").cloned().unwrap_or(Value::Null);
    if rpc_method.is_empty() || id.is_null() {
        // Notifications and the client's replies to server requests: nothing
        // to police or to wait for.
        return passthrough(server, method, &headers, body).await;
    }
    let tool = if rpc_method == "tools/call" {
        msg.get("params")
            .and_then(|p| p.get("name"))
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string()
    } else {
        String::new()
    };

    let (trace_id, parent_id) = match msg
        .get("params")
        .and_then(|p| p.get("_meta"))
        .and_then(|m| m.get("traceparent"))
        .and_then(Value::as_str)
        .and_then(crate::obs::trace::parse_traceparent)
    {
        // MCP carries trace context in the message, not only in HTTP headers.
        Some(tp) if header(&headers, "traceparent").is_none() => tp,
        _ => trace_ids(&headers, &agent),
    };
    let mut dims = Map::new();
    for (k, v) in [
        ("project_id", agent.project_id.as_deref().unwrap_or("")),
        ("team_id", agent.team_id.as_deref().unwrap_or("")),
        ("user_id", agent.user_id.as_deref().unwrap_or("")),
        ("agent_id", agent.id.as_str()),
        ("session_id", agent.session_id.as_deref().unwrap_or("")),
        ("run_id", header(&headers, "x-gateway-run-id").unwrap_or("")),
        (
            "deliverable_id",
            header(&headers, "x-gateway-deliverable-id").unwrap_or(""),
        ),
        ("gateway_key_id", agent.key_id.as_str()),
    ] {
        dims.insert(k.to_string(), json!(v));
    }
    dims.insert(
        "client_ip".into(),
        json!(peer
            .map(|Extension(ConnectInfo(a))| a.ip().to_string())
            .unwrap_or_default()),
    );
    let mut call = Call {
        obs: app.obs.clone(),
        app: app.clone(),
        server: name.clone(),
        address: server.address.clone(),
        method: rpc_method.clone(),
        tool: tool.clone(),
        id: id.clone(),
        t0: Instant::now(),
        ts_ms: now_ms(),
        trace_id,
        span_id: new_span_id(),
        parent_id,
        dims,
        queue_ms: 0.0,
        req_bytes: body.len(),
        resp_bytes: 0,
        session: header(&headers, "mcp-session-id").unwrap_or("").to_string(),
        protocol: header(&headers, "mcp-protocol-version")
            .unwrap_or("")
            .to_string(),
        done: false,
    };

    let mut lease = None;
    let mut body = body;
    if rpc_method == "tools/call" {
        if tool.is_empty() || !server.allows(&agent, &tool) {
            call.finish("denied", Some(-32602));
            if let Some(o) = &app.obs {
                o.metrics.refused("mcp_policy", &name);
            }
            return rpc_error(&id, -32602, format!("Unknown tool: {tool}"));
        }
        let mut subject = App::subject(&agent, "", "");
        subject.tool = format!("{name}/{tool}");
        let one_call = Amount {
            tool_calls: 1.0,
            ..Amount::default()
        };
        // Tool-call rules count the call at admission; nothing to settle.
        if let Err(denied) = app
            .limits
            .admit(&subject, one_call, Amount::default(), now_ms())
        {
            call.finish("limited", None);
            if let Some(o) = &app.obs {
                o.metrics.refused("mcp_limit", &name);
            }
            return Json(tool_error(
                &id,
                format!(
                    "Tool call refused by the gateway: {}. Do not retry before then.",
                    denied.message()
                ),
            ))
            .into_response();
        }
        let queued = Instant::now();
        let rx = server.sched.enqueue(
            &agent.id,
            Ticket {
                weight: agent.weight,
                max_inflight: None,
                cost: 0,
                ..Ticket::default()
            },
        );
        let Ok(Ok(l)) = rx.await else {
            call.finish("shutting_down", None);
            return error(
                StatusCode::SERVICE_UNAVAILABLE,
                "overloaded_error",
                "gateway shutting down",
            );
        };
        call.queue_ms = queued.elapsed().as_secs_f64() * 1000.0;
        lease = Some(l);
        // Continue the trace inside the server, the way the MCP convention
        // carries it: in `params._meta`.
        if app.obs.is_some() {
            if let Some(params) = msg.get_mut("params").and_then(Value::as_object_mut) {
                let meta = params.entry("_meta").or_insert_with(|| json!({}));
                if let Some(meta) = meta.as_object_mut() {
                    meta.insert(
                        "traceparent".into(),
                        json!(format!("00-{}-{}-01", call.trace_id, call.span_id)),
                    );
                    if let Ok(b) = serde_json::to_vec(&msg) {
                        body = Bytes::from(b);
                    }
                }
            }
        }
    }

    let resp = match server
        .request(Method::POST, &headers)
        .header(
            "traceparent",
            format!("00-{}-{}-01", call.trace_id, call.span_id),
        )
        .body(body)
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => {
            let kind = if e.is_timeout() {
                "timeout"
            } else {
                "upstream_error"
            };
            call.finish(kind, None);
            tracing::warn!(server = %name, error = %e, "mcp request failed");
            return if rpc_method == "tools/call" {
                Json(tool_error(
                    &id,
                    format!("The tool server {name:?} could not be reached ({kind})."),
                ))
                .into_response()
            } else {
                error(
                    StatusCode::BAD_GATEWAY,
                    "api_connection_error",
                    format!("mcp server {name:?} unreachable: {e}"),
                )
            };
        }
    };
    let status = resp.status();
    let mut out_headers = HeaderMap::new();
    for h in ["content-type", "mcp-session-id", "www-authenticate"] {
        if let Some(v) = resp.headers().get(h) {
            out_headers.insert(HeaderName::from_static(h), v.clone());
        }
    }
    if call.session.is_empty() {
        if let Some(s) = header(resp.headers(), "mcp-session-id") {
            call.session = s.to_string();
        }
    }
    if let Ok(v) = HeaderValue::from_str(&call.trace_id) {
        out_headers.insert("x-gateway-trace-id", v);
    }
    let ctype = header(resp.headers(), "content-type").unwrap_or("");
    let cap = server.cfg.max_result_bytes + EVENT_SLACK;

    if !status.is_success() {
        let bytes = resp.bytes().await.unwrap_or_default();
        call.resp_bytes = bytes.len();
        call.finish(&format!("http_{}", status.as_u16()), None);
        return (status, out_headers, bytes).into_response();
    }
    if ctype.starts_with("text/event-stream") {
        let stream = SseRelay {
            inner: Box::pin(resp.bytes_stream()),
            pending: Vec::new(),
            out: VecDeque::new(),
            call,
            lease,
            app: app.clone(),
            server: name,
            agent: agent.clone(),
            method: rpc_method,
            id,
            cap,
            answered: false,
            ended: false,
        };
        return (status, out_headers, Body::from_stream(stream.into_stream())).into_response();
    }
    // A single JSON response. Read it with a ceiling so a huge result is
    // refused without being buffered whole.
    let mut buf = Vec::new();
    let mut s = resp.bytes_stream();
    let mut too_big = false;
    while let Some(chunk) = s.next().await {
        match chunk {
            Ok(c) => {
                call.resp_bytes += c.len();
                if buf.len() + c.len() > cap && rpc_method == "tools/call" {
                    too_big = true;
                    // Keep counting, stop storing.
                    continue;
                }
                if !too_big {
                    buf.extend_from_slice(&c);
                }
            }
            Err(e) => {
                call.finish(
                    if e.is_timeout() {
                        "timeout"
                    } else {
                        "upstream_error"
                    },
                    None,
                );
                return error(
                    StatusCode::BAD_GATEWAY,
                    "api_connection_error",
                    e.to_string(),
                );
            }
        }
    }
    drop(lease);
    let reply = Reply {
        server,
        agent: &agent,
        method: &rpc_method,
    };
    if too_big {
        let mut m = json!({ "id": id });
        let (_, st, _) = reply.apply(&mut m, call.resp_bytes);
        call.finish(st, None);
        out_headers.insert(
            http::header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        return (status, out_headers, Json(m)).into_response();
    }
    match serde_json::from_slice::<Value>(&buf) {
        Ok(mut m) if m.is_object() => {
            let (changed, st, code) = reply.apply(&mut m, buf.len());
            call.finish(st, code);
            if changed {
                (status, out_headers, Json(m)).into_response()
            } else {
                (status, out_headers, Bytes::from(buf)).into_response()
            }
        }
        _ => {
            call.finish("ok", None);
            (status, out_headers, Bytes::from(buf)).into_response()
        }
    }
}

/// Relay without inspection: notifications, the listening stream, session
/// teardown.
async fn passthrough(
    server: &Server,
    method: Method,
    headers: &HeaderMap,
    body: Bytes,
) -> Response {
    let resp = match server.request(method, headers).body(body).send().await {
        Ok(r) => r,
        Err(e) => {
            return error(
                StatusCode::BAD_GATEWAY,
                "api_connection_error",
                format!("mcp server {:?} unreachable: {e}", server.cfg.name),
            )
        }
    };
    let status = resp.status();
    let mut out = HeaderMap::new();
    for h in ["content-type", "mcp-session-id", "www-authenticate"] {
        if let Some(v) = resp.headers().get(h) {
            out.insert(HeaderName::from_static(h), v.clone());
        }
    }
    (status, out, Body::from_stream(resp.bytes_stream())).into_response()
}

type Upstream = std::pin::Pin<Box<dyn Stream<Item = reqwest::Result<Bytes>> + Send>>;

/// An SSE response relayed event by event. Events other than the answer to
/// the tracked request (progress, logging, server requests) pass through
/// byte for byte; the answer goes through [`Reply::apply`].
struct SseRelay {
    inner: Upstream,
    pending: Vec<u8>,
    out: VecDeque<Bytes>,
    call: Call,
    lease: Option<Lease>,
    app: Arc<App>,
    server: String,
    agent: Arc<AgentInfo>,
    method: String,
    id: Value,
    cap: usize,
    answered: bool,
    ended: bool,
}

/// Index just past the blank line that ends the first complete event.
fn event_end(buf: &[u8]) -> Option<usize> {
    let lf = memchr::memmem::find(buf, b"\n\n").map(|i| i + 2);
    let crlf = memchr::memmem::find(buf, b"\r\n\r\n").map(|i| i + 4);
    match (lf, crlf) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    }
}

impl SseRelay {
    fn answer(&mut self, status: &str, code: Option<i64>) {
        self.answered = true;
        self.lease.take();
        self.call.finish(status, code);
    }

    fn event(&mut self, raw: &[u8]) {
        if self.answered {
            self.out.push_back(Bytes::copy_from_slice(raw));
            return;
        }
        let mut data = Vec::new();
        for line in raw.split(|&b| b == b'\n') {
            let line = line.strip_suffix(b"\r").unwrap_or(line);
            if let Some(d) = line.strip_prefix(b"data:") {
                if !data.is_empty() {
                    data.push(b'\n');
                }
                data.extend_from_slice(d.strip_prefix(b" ").unwrap_or(d));
            }
        }
        let parsed = serde_json::from_slice::<Value>(&data).ok();
        let Some(mut msg) = parsed.filter(|m| {
            m.get("id").is_some_and(|i| same_id(i, &self.id))
                && (m.get("result").is_some() || m.get("error").is_some())
        }) else {
            self.out.push_back(Bytes::copy_from_slice(raw));
            return;
        };
        let app = self.app.clone();
        let Some(server) = app.mcp.get(&self.server) else {
            self.out.push_back(Bytes::copy_from_slice(raw));
            return;
        };
        let (changed, status, code) = Reply {
            server,
            agent: &self.agent,
            method: &self.method,
        }
        .apply(&mut msg, data.len());
        if changed {
            // Keep the event's other fields (`event:`, `id:`), swap the data.
            let mut ev = Vec::with_capacity(raw.len());
            for line in raw.split_inclusive(|&b| b == b'\n') {
                let bare = line.strip_suffix(b"\n").unwrap_or(line);
                let bare = bare.strip_suffix(b"\r").unwrap_or(bare);
                if bare.is_empty() || bare.starts_with(b"data:") {
                    continue;
                }
                ev.extend_from_slice(line);
            }
            ev.extend_from_slice(b"data: ");
            ev.extend_from_slice(&serde_json::to_vec(&msg).unwrap_or_default());
            ev.extend_from_slice(b"\n\n");
            self.out.push_back(Bytes::from(ev));
        } else {
            self.out.push_back(Bytes::copy_from_slice(raw));
        }
        self.answer(status, code);
    }

    fn push(&mut self, chunk: &[u8]) {
        self.call.resp_bytes += chunk.len();
        self.pending.extend_from_slice(chunk);
        while let Some(end) = event_end(&self.pending) {
            let raw: Vec<u8> = self.pending.drain(..end).collect();
            self.event(&raw);
        }
        if !self.answered && self.pending.len() > self.cap {
            // An event this large can only be the oversized result.
            let ev = format!(
                "event: message\ndata: {}\n\n",
                tool_error(
                    &self.id,
                    format!(
                        "The tool result was over this gateway's limit of {} bytes and was not delivered. Ask for less: a narrower query, fewer fields, or one page at a time.",
                        self.cap - EVENT_SLACK
                    )
                )
            );
            self.pending.clear();
            self.out.push_back(Bytes::from(ev));
            self.answer("too_large", None);
            self.ended = true;
        }
    }

    fn into_stream(self) -> impl Stream<Item = std::io::Result<Bytes>> + Send {
        futures_util::stream::unfold(self, |mut st| async move {
            loop {
                if let Some(b) = st.out.pop_front() {
                    return Some((Ok(b), st));
                }
                if st.ended {
                    if !st.answered {
                        st.answer("no_response", None);
                    }
                    return None;
                }
                match st.inner.next().await {
                    Some(Ok(chunk)) => st.push(&chunk),
                    Some(Err(e)) => {
                        let kind = if e.is_timeout() {
                            "timeout"
                        } else {
                            "upstream_error"
                        };
                        if !st.answered {
                            st.answer(kind, None);
                        }
                        st.ended = true;
                        return Some((Err(std::io::Error::other(e)), st));
                    }
                    None => {
                        st.ended = true;
                        if !st.pending.is_empty() {
                            let rest = std::mem::take(&mut st.pending);
                            st.out.push_back(Bytes::from(rest));
                        }
                    }
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finds_event_boundaries_in_both_line_endings() {
        assert_eq!(event_end(b"data: 1\n\ndata: 2\n\n"), Some(9));
        assert_eq!(event_end(b"data: 1\r\n\r\nrest"), Some(11));
        assert_eq!(event_end(b"data: 1\n"), None);
    }
}
