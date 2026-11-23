//! End-to-end checks of the controls added on top of the proxy: windowed
//! token and dollar limits, load shedding, GenAI-convention telemetry and
//! MCP tool traffic.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::Router;
use futures_util::StreamExt;
use pybox_gateway::config::Config;
use pybox_gateway::App;
use serde_json::{json, Value};

#[derive(Default)]
struct Mock {
    hits: AtomicUsize,
    mcp_calls: Mutex<Vec<Value>>,
    mcp_auth: Mutex<Vec<String>>,
    otlp: Mutex<Vec<Bytes>>,
}

/// Anthropic-format messages: 100 input and 50 output tokens every time.
async fn messages(State(m): State<Arc<Mock>>, body: Bytes) -> Response {
    m.hits.fetch_add(1, SeqCst);
    let v: Value = serde_json::from_slice(&body).unwrap();
    tokio::time::sleep(Duration::from_millis(v["delay_ms"].as_u64().unwrap_or(0))).await;
    if v["stream"] == true {
        let sse = concat!(
            "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_abc\",\"model\":\"claude-test-20260101\",\"usage\":{\"input_tokens\":100,\"output_tokens\":1}}}\n\n",
            "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"hello\"}}\n\n",
            "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":50}}\n\n",
            "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
        );
        return Response::builder()
            .header("content-type", "text/event-stream")
            .body(Body::from(sse))
            .unwrap();
    }
    axum::Json(json!({
        "id": "msg_abc", "model": "claude-test-20260101", "stop_reason": "end_turn",
        "content": [{"type": "text", "text": "hi"}],
        "usage": {"input_tokens": 100, "output_tokens": 50}
    }))
    .into_response()
}

/// A small MCP server speaking Streamable HTTP.
async fn mcp(State(m): State<Arc<Mock>>, headers: HeaderMap, body: Bytes) -> Response {
    let v: Value = serde_json::from_slice(&body).unwrap();
    m.mcp_auth.lock().unwrap().push(
        headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string(),
    );
    m.mcp_calls.lock().unwrap().push(v.clone());
    let id = v["id"].clone();
    let result = |r: Value| json!({"jsonrpc": "2.0", "id": id, "result": r});
    let text = |s: &str| json!({"content": [{"type": "text", "text": s}], "isError": false});
    match v["method"].as_str().unwrap_or("") {
        "initialize" => (
            [("mcp-session-id", "sess-mcp-1")],
            axum::Json(result(json!({"protocolVersion": "2025-06-18", "capabilities": {}}))),
        )
            .into_response(),
        "notifications/initialized" => StatusCode::ACCEPTED.into_response(),
        "tools/list" => axum::Json(result(json!({"tools": [
            {"name": "search"}, {"name": "delete_repo"}, {"name": "big"},
            {"name": "stream_search"}, {"name": "stream_big"},
        ]})))
        .into_response(),
        "tools/call" => match v["params"]["name"].as_str().unwrap_or("") {
            "search" => axum::Json(result(text("3 results"))).into_response(),
            "big" => axum::Json(result(text(&"x".repeat(5000)))).into_response(),
            name @ ("stream_search" | "stream_big") => {
                let payload = if name == "stream_big" {
                    "y".repeat(5000)
                } else {
                    "streamed".into()
                };
                let events = vec![
                    format!(
                        "event: message\ndata: {}\n\n",
                        json!({"jsonrpc": "2.0", "method": "notifications/progress", "params": {"progress": 1}})
                    ),
                    format!("event: message\nid: 7\ndata: {}\n\n", result(text(&payload))),
                ];
                let s = futures_util::stream::iter(events).then(|e| async move {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    Ok::<_, std::io::Error>(Bytes::from(e))
                });
                Response::builder()
                    .header("content-type", "text/event-stream")
                    .body(Body::from_stream(s))
                    .unwrap()
            }
            other => axum::Json(json!({"jsonrpc": "2.0", "id": id, "error": {"code": -32602, "message": format!("Unknown tool: {other}")}})).into_response(),
        },
        _ => axum::Json(result(json!({}))).into_response(),
    }
}

async fn collector(State(m): State<Arc<Mock>>, body: Bytes) -> Response {
    m.otlp.lock().unwrap().push(body);
    ([("content-type", "application/x-protobuf")], Bytes::new()).into_response()
}

async fn serve(router: Router) -> SocketAddr {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            l,
            router.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap()
    });
    addr
}

struct Env {
    gw: String,
    app: Arc<App>,
    mock: Arc<Mock>,
    http: reqwest::Client,
}

/// `{up}` in `config` is replaced by the mock's address.
async fn setup(config: &str) -> Env {
    let mock = Arc::new(Mock::default());
    let up = serve(
        Router::new()
            .route("/v1/messages", post(messages))
            .route("/mcp", post(mcp))
            .route("/v1/traces", post(collector))
            .with_state(mock.clone()),
    )
    .await;
    let toml = format!(
        r#"
admin_token = "admin-secret"
{}
[[agents]]
id = "a1"
token = "agent-one-token"
team_id = "growth"
project_id = "apollo"
session_id = "sess-1"

[[agents]]
id = "a2"
token = "agent-two-token"
team_id = "infra"
mcp_deny = ["srv/search"]
"#,
        config.replace("{up}", &up.to_string())
    );
    let cfg: Config = toml::from_str(&toml).unwrap();
    let app = App::new(cfg).unwrap();
    let gw = serve(app.router()).await;
    Env {
        gw: format!("http://{gw}"),
        app,
        mock,
        http: reqwest::Client::new(),
    }
}

const UPSTREAM: &str = r#"
[[upstreams]]
name = "llm"
base_url = "http://{up}"
kind = "anthropic"
api_key = "real-key"
max_concurrency = 16
"#;

impl Env {
    async fn call(&self, token: &str, body: Value) -> reqwest::Response {
        self.http
            .post(format!("{}/llm/v1/messages", self.gw))
            .header("x-api-key", token)
            .json(&body)
            .send()
            .await
            .unwrap()
    }
    async fn admin(&self, method: reqwest::Method, path: &str, body: Option<Value>) -> Value {
        let mut req = self
            .http
            .request(method, format!("{}{}", self.gw, path))
            .bearer_auth("admin-secret");
        if let Some(b) = body {
            req = req.json(&b);
        }
        let resp = req.send().await.unwrap();
        assert!(resp.status().is_success(), "{path}: {}", resp.status());
        let text = resp.text().await.unwrap();
        serde_json::from_str(&text).unwrap_or(Value::String(text))
    }
    async fn rpc(&self, token: &str, msg: Value, headers: &[(&str, &str)]) -> reqwest::Response {
        let mut req = self
            .http
            .post(format!("{}/mcp/srv", self.gw))
            .bearer_auth(token)
            .header("accept", "application/json, text/event-stream")
            .json(&msg);
        for (k, v) in headers {
            req = req.header(*k, *v);
        }
        req.send().await.unwrap()
    }
    async fn tool(&self, token: &str, name: &str) -> Value {
        let r = self
            .rpc(
                token,
                json!({"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": name, "arguments": {}}}),
                &[],
            )
            .await;
        assert_eq!(r.status(), 200);
        let text = r.text().await.unwrap();
        // SSE or plain JSON: take the message that carries the answer.
        let json_part = text
            .lines()
            .filter_map(|l| l.strip_prefix("data: "))
            .rfind(|d| d.contains("\"id\""))
            .unwrap_or(&text);
        serde_json::from_str(json_part).unwrap()
    }
}

fn msg(model: &str) -> Value {
    json!({"model": model, "max_tokens": 100, "messages": [{"role": "user", "content": "hi"}]})
}

#[tokio::test]
async fn token_limit_per_team_rejects_with_retry_after() {
    let e = setup(&format!(
        r#"
[[limits]]
name = "team-daily"
scope = "team"
window = "day"
tokens = 400
{UPSTREAM}"#
    ))
    .await;
    // 150 tokens a call: three fit (the third starts under the cap), then stop.
    for _ in 0..3 {
        assert_eq!(
            e.call("agent-one-token", msg("claude-test")).await.status(),
            200
        );
    }
    let denied = e.call("agent-one-token", msg("claude-test")).await;
    assert_eq!(denied.status(), 429);
    assert_eq!(denied.headers()["x-gateway-limit"], "team-daily");
    let retry: u64 = denied.headers()["retry-after"]
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    assert!((1..=86_400).contains(&retry));
    let body: Value = denied.json().await.unwrap();
    assert_eq!(body["error"]["type"], "gateway_limit_exceeded");
    assert_eq!(e.mock.hits.load(SeqCst), 3, "refused before the upstream");
    // The other team has its own budget.
    assert_eq!(
        e.call("agent-two-token", msg("claude-test")).await.status(),
        200
    );

    let state = e.admin(reqwest::Method::GET, "/gateway/limits", None).await;
    let rule = &state["limits"][0];
    assert_eq!(rule["denied"], 1);
    let growth = rule["usage"]
        .as_array()
        .unwrap()
        .iter()
        .find(|u| u["value"] == "growth")
        .unwrap();
    assert_eq!(growth["tokens"], 450.0);
    assert_eq!(growth["requests"], 3.0);
}

#[tokio::test]
async fn strict_dollar_limit_holds_under_concurrency() {
    // A call costs 100 * 10/1M + 50 * 100/1M = 0.006 USD and may cost up to
    // about 0.0102 (max_tokens = 100), which is what a strict rule reserves.
    let e = Arc::new(
        setup(&format!(
            r#"
[obs.prices."claude-test"]
input = 10.0
output = 100.0

[[limits]]
scope = "agent"
window = "day"
usd = 0.02
strict = true
{UPSTREAM}"#
        ))
        .await,
    );
    let mut burst = Vec::new();
    for _ in 0..5 {
        let e = e.clone();
        burst.push(tokio::spawn(async move {
            let mut m = msg("claude-test");
            m["delay_ms"] = json!(200);
            e.call("agent-one-token", m).await.status().as_u16()
        }));
    }
    let mut ok = 0;
    for t in burst {
        if t.await.unwrap() == 200 {
            ok += 1;
        }
    }
    assert_eq!(ok, 1, "a second concurrent call could have passed 0.02 USD");
    // The first settled at 0.006, so one more fits, and then no more.
    assert_eq!(
        e.call("agent-one-token", msg("claude-test")).await.status(),
        200
    );
    assert_eq!(
        e.call("agent-one-token", msg("claude-test")).await.status(),
        429
    );
    let state = e.admin(reqwest::Method::GET, "/gateway/limits", None).await;
    let usd = state["limits"][0]["usage"][0]["usd"].as_f64().unwrap();
    assert!((usd - 0.012).abs() < 1e-9, "{usd}");
    assert_eq!(state["limits"][0]["rule"]["name"], "agent-day");
}

#[tokio::test]
async fn limits_can_be_managed_at_runtime() {
    let e = setup(UPSTREAM).await;
    assert_eq!(e.call("agent-one-token", msg("m")).await.status(), 200);
    e.admin(
        reqwest::Method::PUT,
        "/gateway/limits/one-a-minute",
        Some(json!({"scope": "agent", "match": "a1", "window": "1m", "requests": 1})),
    )
    .await;
    assert_eq!(e.call("agent-one-token", msg("m")).await.status(), 200);
    assert_eq!(e.call("agent-one-token", msg("m")).await.status(), 429);
    assert_eq!(
        e.call("agent-two-token", msg("m")).await.status(),
        200,
        "rule names a1 only"
    );
    e.admin(
        reqwest::Method::DELETE,
        "/gateway/limits/one-a-minute",
        None,
    )
    .await;
    assert_eq!(e.call("agent-one-token", msg("m")).await.status(), 200);
    let bad = e
        .http
        .put(format!("{}/gateway/limits/x", e.gw))
        .bearer_auth("admin-secret")
        .json(&json!({"scope": "agent", "window": "1m"}))
        .send()
        .await
        .unwrap();
    assert_eq!(bad.status(), 400, "a rule needs at least one cap");
}

#[tokio::test]
async fn full_queue_and_long_waits_are_shed() {
    let e = Arc::new(
        setup(
            r#"
[[upstreams]]
name = "llm"
base_url = "http://{up}"
kind = "anthropic"
api_key = "real-key"
max_concurrency = 1
max_queue = 1
queue_timeout_secs = 1
"#,
        )
        .await,
    );
    let slow = |ms: u64| {
        let e = e.clone();
        tokio::spawn(async move {
            let mut m = msg("m");
            m["delay_ms"] = json!(ms);
            let r = e.call("agent-one-token", m).await;
            (
                r.status().as_u16(),
                r.headers().get("retry-after").is_some(),
            )
        })
    };
    // One runs for 1.4 s, one waits; the third finds the queue full.
    let running = slow(1400);
    tokio::time::sleep(Duration::from_millis(100)).await;
    let waiting = slow(0);
    tokio::time::sleep(Duration::from_millis(100)).await;
    let (status, retry) = slow(0).await.unwrap();
    assert_eq!((status, retry), (503, true), "queue full");
    // The waiter gives up after a second, before the slot frees.
    assert_eq!(waiting.await.unwrap(), (503, true), "queue timeout");
    assert_eq!(running.await.unwrap().0, 200);
    assert_eq!(e.mock.hits.load(SeqCst), 1);
    let stats = e.admin(reqwest::Method::GET, "/gateway/stats", None).await;
    assert_eq!(stats["upstreams"]["llm"]["counters"]["shed"], 2);
}

#[tokio::test]
async fn telemetry_follows_the_genai_conventions() {
    let e = setup(&format!(
        r#"
[obs.otlp_export]
endpoint = "http://{{up}}/v1/traces"
interval_ms = 50
{UPSTREAM}"#
    ))
    .await;
    let mut m = msg("claude-test");
    m["stream"] = json!(true);
    let resp = e.call("agent-one-token", m).await;
    let trace_id = resp.headers()["x-gateway-trace-id"]
        .to_str()
        .unwrap()
        .to_string();
    resp.text().await.unwrap();
    let obs = e.app.obs.as_ref().unwrap();
    obs.flush().await;

    let t = e
        .admin(
            reqwest::Method::GET,
            &format!("/gateway/obs/trace/{trace_id}"),
            None,
        )
        .await;
    let row = &t["requests"][0];
    assert_eq!(row["provider"], "anthropic");
    assert_eq!(row["operation"], "chat");
    assert_eq!(row["response_model"], "claude-test-20260101");
    assert_eq!(row["finish_reason"], "end_turn");
    let root = t["spans"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["name"] == "gateway.request")
        .unwrap();
    let a = &root["attrs"];
    assert_eq!(a["gen_ai.operation.name"], "chat");
    assert_eq!(a["gen_ai.provider.name"], "anthropic");
    assert_eq!(a["gen_ai.request.model"], "claude-test");
    assert_eq!(a["gen_ai.response.model"], "claude-test-20260101");
    assert_eq!(a["gen_ai.response.id"], "msg_abc");
    assert_eq!(a["gen_ai.response.finish_reasons"], json!(["end_turn"]));
    assert_eq!(a["gen_ai.usage.input_tokens"], 100);
    assert_eq!(a["gen_ai.usage.output_tokens"], 50);
    assert_eq!(a["gen_ai.request.max_tokens"], 100);
    assert_eq!(a["gen_ai.request.stream"], true);
    assert_eq!(a["gen_ai.conversation.id"], "sess-1");
    assert_eq!(a["server.address"], "127.0.0.1");
    assert!(a["gen_ai.response.time_to_first_chunk"].as_f64().unwrap() > 0.0);
    assert_eq!(a["pybox.project_id"], "apollo");

    // The same trace as OTLP/JSON, root span in the convention's shape.
    let otlp = e
        .admin(
            reqwest::Method::GET,
            &format!("/gateway/obs/trace/{trace_id}?format=otlp"),
            None,
        )
        .await;
    let spans = otlp["resourceSpans"][0]["scopeSpans"][0]["spans"]
        .as_array()
        .unwrap();
    let chat = spans
        .iter()
        .find(|s| s["name"] == "chat claude-test")
        .unwrap();
    assert_eq!(chat["kind"], 3);
    assert_eq!(chat["traceId"], trace_id);

    // Pushed to the collector as protobuf.
    let mut pushed = Vec::new();
    for _ in 0..50 {
        pushed = e.mock.otlp.lock().unwrap().clone();
        if !pushed.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let decoded: Vec<_> = pushed
        .iter()
        .flat_map(|b| pybox_gateway::obs::otlp::decode(b).unwrap())
        .collect();
    let chat = decoded
        .iter()
        .find(|s| s["name"] == "chat claude-test")
        .unwrap();
    assert_eq!(chat["trace_id"], trace_id);
    assert_eq!(chat["attrs"]["gen_ai.usage.output_tokens"], 50);
    assert!(decoded.iter().any(|s| s["name"] == "gateway.attempt"));

    let metrics = e
        .admin(reqwest::Method::GET, "/gateway/metrics", None)
        .await;
    let text = metrics.as_str().unwrap();
    let labels = r#"gen_ai_operation_name="chat",gen_ai_provider_name="anthropic",gen_ai_request_model="claude-test",gen_ai_response_model="claude-test-20260101",server_address="127.0.0.1""#;
    assert!(
        text.contains(&format!(
            "gen_ai_client_inference_duration_seconds_count{{{labels},"
        )),
        "{text}"
    );
    assert!(
        text.contains("gen_ai_client_inference_usage_output_tokens_total{"),
        "{text}"
    );
    assert!(text.contains("gen_ai_client_inference_time_to_first_chunk_seconds_bucket{"));
    assert!(text.contains("pybox_gateway_inflight{upstream=\"llm\"} 0"));
    let anon = e
        .http
        .get(format!("{}/gateway/metrics", e.gw))
        .send()
        .await
        .unwrap();
    assert_eq!(anon.status(), 401);
}

const MCP: &str = r#"
[[mcp_servers]]
name = "srv"
url = "http://{up}/mcp"
api_key = "real-mcp-key"
deny_tools = ["delete_*"]
max_result_bytes = 1000

[[limits]]
name = "stream-calls"
scope = "agent"
window = "1m"
tool = "srv/stream_s*"
tool_calls = 1
"#;

#[tokio::test]
async fn mcp_policy_hides_and_blocks_tools() {
    let e = setup(&format!("{MCP}{UPSTREAM}")).await;
    let init = e
        .rpc(
            "agent-one-token",
            json!({"jsonrpc": "2.0", "id": 0, "method": "initialize", "params": {}}),
            &[],
        )
        .await;
    assert_eq!(init.headers()["mcp-session-id"], "sess-mcp-1");
    let note = e
        .rpc(
            "agent-one-token",
            json!({"jsonrpc": "2.0", "method": "notifications/initialized"}),
            &[],
        )
        .await;
    assert_eq!(note.status(), 202);

    let names = |v: Value| -> Vec<String> {
        v["result"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["name"].as_str().unwrap().to_string())
            .collect()
    };
    let list = |token: &'static str| {
        let e = &e;
        async move {
            e.rpc(
                token,
                json!({"jsonrpc": "2.0", "id": 2, "method": "tools/list"}),
                &[],
            )
            .await
            .json::<Value>()
            .await
            .unwrap()
        }
    };
    // The server-wide deny list hides delete_repo from everyone; a2 is also
    // denied search.
    assert_eq!(
        names(list("agent-one-token").await),
        ["search", "big", "stream_search", "stream_big"]
    );
    assert_eq!(
        names(list("agent-two-token").await),
        ["big", "stream_search", "stream_big"]
    );

    let before = e.mock.mcp_calls.lock().unwrap().len();
    for (token, tool) in [
        ("agent-one-token", "delete_repo"),
        ("agent-two-token", "search"),
    ] {
        let refused = e.tool(token, tool).await;
        assert_eq!(refused["error"]["code"], -32602, "{tool}");
    }
    assert_eq!(
        e.mock.mcp_calls.lock().unwrap().len(),
        before,
        "never reached the server"
    );

    let ok = e.tool("agent-one-token", "search").await;
    assert_eq!(ok["result"]["content"][0]["text"], "3 results");
    // The gateway's credential, never the agent's token.
    assert!(e
        .mock
        .mcp_auth
        .lock()
        .unwrap()
        .iter()
        .all(|a| a == "Bearer real-mcp-key"));
    // Trace context continues into the server through params._meta.
    let last = e.mock.mcp_calls.lock().unwrap().last().unwrap().clone();
    assert!(last["params"]["_meta"]["traceparent"]
        .as_str()
        .unwrap()
        .starts_with("00-"));

    let unauth = e
        .rpc(
            "nope",
            json!({"jsonrpc": "2.0", "id": 9, "method": "tools/list"}),
            &[],
        )
        .await;
    assert_eq!(unauth.status(), 401);
}

#[tokio::test]
async fn mcp_results_are_capped_limited_and_recorded() {
    let e = setup(&format!("{MCP}{UPSTREAM}")).await;
    // Oversized results come back as a tool error the model can act on.
    for tool in ["big", "stream_big"] {
        let v = e.tool("agent-one-token", tool).await;
        assert_eq!(v["result"]["isError"], true, "{tool}");
        let text = v["result"]["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("limit of 1000 bytes"), "{text}");
    }
    // A streamed call: the progress notification passes through untouched.
    let r = e
        .rpc(
            "agent-one-token",
            json!({"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "stream_search"}}),
            &[("x-gateway-run-id", "run-9")],
        )
        .await;
    let trace_id = r.headers()["x-gateway-trace-id"]
        .to_str()
        .unwrap()
        .to_string();
    let body = r.text().await.unwrap();
    assert!(
        body.contains("notifications/progress") && body.contains("streamed"),
        "{body}"
    );
    assert!(body.contains("id: 7"), "event ids survive");
    // One stream_s* call a minute per agent: the next is refused in-band.
    let limited = e.tool("agent-one-token", "stream_search").await;
    assert_eq!(limited["result"]["isError"], true);
    assert!(limited["result"]["content"][0]["text"]
        .as_str()
        .unwrap()
        .contains("stream-calls"));
    assert_eq!(
        e.tool("agent-two-token", "stream_search").await["result"]["isError"],
        false
    );

    // A model call of the same run lands in the same trace as the tool call.
    let model_call = e
        .http
        .post(format!("{}/llm/v1/messages", e.gw))
        .header("x-api-key", "agent-one-token")
        .header("x-gateway-run-id", "run-9")
        .json(&msg("m"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        model_call.headers()["x-gateway-trace-id"].to_str().unwrap(),
        trace_id
    );

    e.app.obs.as_ref().unwrap().flush().await;
    let t = e
        .admin(
            reqwest::Method::GET,
            &format!("/gateway/obs/trace/{trace_id}"),
            None,
        )
        .await;
    assert_eq!(t["requests"].as_array().unwrap().len(), 1);
    let tool_row = &t["tools"][0];
    assert_eq!(
        (
            &tool_row["server"],
            &tool_row["tool"],
            &tool_row["status"],
            &tool_row["agent_id"]
        ),
        (
            &json!("srv"),
            &json!("stream_search"),
            &json!("ok"),
            &json!("a1")
        )
    );
    let span = t["spans"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["name"] == "tools/call stream_search")
        .expect("span named per the MCP convention");
    assert_eq!(span["attrs"]["mcp.method.name"], "tools/call");
    assert_eq!(span["attrs"]["gen_ai.tool.name"], "stream_search");
    assert_eq!(span["attrs"]["gen_ai.operation.name"], "execute_tool");

    // Aggregates over the tools table use the same query API as requests.
    let q = e
        .admin(
            reqwest::Method::POST,
            "/gateway/obs/query",
            Some(json!({"table": "tools", "from": 0, "to": 9_000_000_000_000i64,
                "filter": {"method": ["tools/call"]}, "group_by": ["tool", "status"], "metrics": ["count"]})),
        )
        .await;
    let count = |tool: &str, status: &str| {
        q["rows"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["tool"] == tool && r["status"] == status)
            .map(|r| r["count"].as_f64().unwrap())
    };
    assert_eq!(count("big", "too_large"), Some(1.0), "{q}");
    assert_eq!(count("stream_search", "limited"), Some(1.0), "{q}");
    assert_eq!(count("stream_search", "ok"), Some(2.0), "{q}");

    let stats = e.admin(reqwest::Method::GET, "/gateway/stats", None).await;
    assert_eq!(stats["mcp"]["tools"]["srv/stream_search"]["limited"], 1);
    let metrics = e
        .admin(reqwest::Method::GET, "/gateway/metrics", None)
        .await;
    assert!(metrics.as_str().unwrap().contains(
        "mcp_client_operation_duration_seconds_count{mcp_method_name=\"tools/call\",gen_ai_tool_name=\"stream_search\",gen_ai_operation_name=\"execute_tool\",server_address=\"127.0.0.1\"} 2"
    ));
}

#[tokio::test]
async fn limit_usage_and_runtime_rules_survive_a_restart() {
    use pybox_gateway::obs::store::{Backend, Memory};
    use pybox_gateway::obs::Obs;

    let mock = Arc::new(Mock::default());
    let up = serve(
        Router::new()
            .route("/v1/messages", post(messages))
            .with_state(mock.clone()),
    )
    .await;
    let toml = format!(
        r#"
admin_token = "admin-secret"
[[limits]]
name = "team-daily"
scope = "team"
window = "day"
tokens = 400
[[upstreams]]
name = "llm"
base_url = "http://{up}"
kind = "anthropic"
api_key = "real-key"
[[agents]]
id = "a1"
token = "agent-one-token"
team_id = "growth"
"#
    );
    // One store, two gateway lifetimes.
    let store: Arc<dyn Backend> = Arc::new(Memory::new(1000));
    let start = |store: Arc<dyn Backend>| {
        let cfg: Config = toml::from_str(&toml).unwrap();
        let obs = Obs::with_backend(cfg.obs.clone(), store).unwrap();
        App::with_obs(cfg, Some(obs)).unwrap()
    };
    let http = reqwest::Client::new();
    let call = |gw: SocketAddr| {
        let http = http.clone();
        async move {
            http.post(format!("http://{gw}/llm/v1/messages"))
                .header("x-api-key", "agent-one-token")
                .json(&msg("claude-test"))
                .send()
                .await
                .unwrap()
                .status()
                .as_u16()
        }
    };

    let first = start(store.clone());
    let gw1 = serve(first.router()).await;
    for _ in 0..3 {
        assert_eq!(call(gw1).await, 200);
    }
    let put = http
        .put(format!("http://{gw1}/gateway/limits/added-live"))
        .bearer_auth("admin-secret")
        .json(&json!({"scope": "agent", "window": "1h", "requests": 1000}))
        .send()
        .await
        .unwrap();
    assert_eq!(put.status(), 200);
    first.obs.as_ref().unwrap().flush().await;

    let second = start(store);
    let gw2 = serve(second.router()).await;
    assert_eq!(
        call(gw2).await,
        429,
        "the 450 tokens spent before the restart still count"
    );
    let state: Value = http
        .get(format!("http://{gw2}/gateway/limits"))
        .bearer_auth("admin-secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let names: Vec<&str> = state["limits"]
        .as_array()
        .unwrap()
        .iter()
        .map(|l| l["rule"]["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["team-daily", "added-live"]);
    assert_eq!(state["limits"][0]["usage"][0]["tokens"], 450.0);
    assert_eq!(mock.hits.load(SeqCst), 3);
}
