//! End-to-end checks of request logging, tracing and ROI against a mock
//! LiteLLM proxy.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::HeaderMap;
use axum::response::Response;
use axum::routing::post;
use axum::Router;
use futures_util::StreamExt;
use pybox_gateway::config::Config;
use pybox_gateway::App;
use serde_json::{json, Value};

#[derive(Default)]
struct Seen {
    headers: Mutex<Vec<HeaderMap>>,
}

/// OpenAI-format chat completions the way LiteLLM serves them: 80 ms to the
/// first content chunk, then two more 40 ms apart, usage last.
async fn chat(State(seen): State<Arc<Seen>>, headers: HeaderMap, body: Bytes) -> Response {
    seen.headers.lock().unwrap().push(headers);
    let v: Value = serde_json::from_slice(&body).unwrap();
    let usage = json!({"prompt_tokens": 1000, "completion_tokens": 30, "prompt_tokens_details": {"cached_tokens": 800}});
    if v["stream"] != true {
        return Response::builder()
            .header("content-type", "application/json")
            .header("x-litellm-call-id", "call-json")
            .header("x-litellm-response-cost", "0.0125")
            .header("x-litellm-cache-key", "abc")
            .body(Body::from(
                json!({"choices": [{"message": {"content": "hi"}}], "usage": usage}).to_string(),
            ))
            .unwrap();
    }
    assert_eq!(
        v["stream_options"]["include_usage"], true,
        "gateway asks for usage on streams"
    );
    let events = vec![
        (
            0,
            json!({"choices": [{"delta": {"role": "assistant", "content": ""}}]}),
        ),
        (80, json!({"choices": [{"delta": {"content": "one"}}]})),
        (40, json!({"choices": [{"delta": {"content": " two"}}]})),
        (40, json!({"choices": [{"delta": {"content": " three"}}]})),
        (0, json!({"choices": [], "usage": usage})),
    ];
    let s = futures_util::stream::iter(events).then(|(delay, ev)| async move {
        tokio::time::sleep(Duration::from_millis(delay)).await;
        Ok::<_, std::io::Error>(Bytes::from(format!("data: {ev}\n\n")))
    });
    Response::builder()
        .header("content-type", "text/event-stream")
        .header("x-litellm-call-id", "call-stream")
        .header("x-litellm-model-id", "deploy-1")
        .header("x-litellm-overhead-duration-ms", "3.5")
        .body(Body::from_stream(s))
        .unwrap()
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
    seen: Arc<Seen>,
    http: reqwest::Client,
}

async fn setup() -> Env {
    let seen = Arc::new(Seen::default());
    let up = serve(
        Router::new()
            .route("/v1/chat/completions", post(chat))
            .with_state(seen.clone()),
    )
    .await;
    let toml = format!(
        r#"
admin_token = "admin-secret"

[obs.prices."gpt-test"]
input = 2.0
output = 10.0
cache_read = 0.2

[[upstreams]]
name = "llm"
base_url = "http://{up}"
kind = "litellm"
api_key = "sk-virtual"
key_alias = "team-growth"
cluster = "seoul"

[[agents]]
id = "writer-1"
token = "agent-one-token"
project_id = "apollo"
team_id = "growth"
user_id = "dana"
session_id = "sess-9"
"#
    );
    let cfg: Config = toml::from_str(&toml).unwrap();
    let app = App::new(cfg).unwrap();
    let gw = serve(app.router()).await;
    Env {
        gw: format!("http://{gw}"),
        app,
        seen,
        http: reqwest::Client::new(),
    }
}

impl Env {
    async fn chat(&self, stream: bool, headers: &[(&str, &str)]) -> reqwest::Response {
        let mut req = self
            .http
            .post(format!("{}/llm/v1/chat/completions", self.gw))
            .bearer_auth("agent-one-token")
            .json(&json!({"model": "gpt-test", "stream": stream, "messages": []}));
        for (k, v) in headers {
            req = req.header(*k, *v);
        }
        req.send().await.unwrap()
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
        resp.json().await.unwrap_or(Value::Null)
    }
    async fn flush(&self) {
        self.app.obs.as_ref().unwrap().flush().await;
    }
}

const FAR: i64 = 9_000_000_000_000;

#[tokio::test]
async fn stream_is_logged_with_timings_attribution_cache_and_cost() {
    let e = setup().await;
    let resp = e
        .chat(
            true,
            &[
                ("x-gateway-run-id", "run-7"),
                ("x-gateway-deliverable-id", "pr-42"),
                ("x-gateway-request-kind", "Code Review!"),
            ],
        )
        .await;
    let trace_id = resp.headers()["x-gateway-trace-id"]
        .to_str()
        .unwrap()
        .to_string();
    let body = resp.text().await.unwrap();
    assert!(
        body.contains(" three") && !body.contains("prompt_tokens"),
        "usage-only chunk stays hidden"
    );
    e.flush().await;

    let rows = e
        .admin(
            reqwest::Method::POST,
            "/gateway/obs/rows",
            Some(json!({"from": 0, "to": FAR})),
        )
        .await;
    let r = &rows["rows"][0];
    assert_eq!(r["priority"], "normal", "the kind of work is a column");
    // The whole window, one row per line and no cap.
    let export = e
        .http
        .get(format!("{}/gateway/obs/export?from=0&to={FAR}", e.gw))
        .bearer_auth("admin-secret")
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let lines: Vec<Value> = export
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(lines.len(), 1);
    assert_eq!(lines[0]["trace_id"], r["trace_id"]);
    assert_eq!(
        lines[0]["request_kind"], "codereview",
        "the caller's own word for the work, cut down to a label"
    );
    // Only the columns asked for.
    let narrow = e
        .http
        .get(format!(
            "{}/gateway/obs/export?from=0&to={FAR}&cols=model,request_kind",
            e.gw
        ))
        .bearer_auth("admin-secret")
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let narrow: Value = serde_json::from_str(narrow.trim()).unwrap();
    assert_eq!(narrow["model"], "gpt-test");
    assert_eq!(narrow["request_kind"], "codereview");
    assert!(narrow["ts"].is_i64());
    // Attribution comes from the token, not from the request.
    for (k, v) in [
        ("project_id", "apollo"),
        ("team_id", "growth"),
        ("user_id", "dana"),
        ("agent_id", "writer-1"),
        ("session_id", "sess-9"),
        ("run_id", "run-7"),
        ("deliverable_id", "pr-42"),
        ("litellm_cluster", "seoul"),
        ("litellm_key", "team-growth"),
        ("model", "gpt-test"),
        ("api_format", "openai"),
        ("client_ip", "127.0.0.1"),
        ("upstream_ip", "127.0.0.1"),
        ("litellm_call_id", "call-stream"),
        ("litellm_model_id", "deploy-1"),
        ("cost_source", "price_table"),
        ("trace_id", trace_id.as_str()),
    ] {
        assert_eq!(r[k], v, "{k}");
    }
    assert_eq!(r["gateway_key_id"].as_str().unwrap().len(), 12);
    assert_eq!(
        (
            r["status"].as_u64(),
            r["stream"].as_bool(),
            r["client_abort"].as_bool()
        ),
        (Some(200), Some(true), Some(false))
    );
    // Tokens: 1000 prompt tokens of which 800 were served from the prompt cache.
    assert_eq!(
        (
            r["input_tokens"].as_u64(),
            r["fresh_input_tokens"].as_u64(),
            r["cache_read_tokens"].as_u64(),
            r["output_tokens"].as_u64()
        ),
        (Some(1000), Some(200), Some(800), Some(30))
    );
    // 200 fresh * $2 + 800 cached * $0.2 + 30 out * $10, per million.
    assert!((r["cost_usd"].as_f64().unwrap() - 0.00086).abs() < 1e-9);
    assert!((r["cache_saved_usd"].as_f64().unwrap() - 0.00144).abs() < 1e-9);
    assert_eq!(r["litellm_overhead_ms"], 3.5);
    // Timings: first content after ~80 ms, generation ~80 ms more.
    let (ttft, gen_ms, total) = (
        r["ttft_ms"].as_f64().unwrap(),
        r["gen_ms"].as_f64().unwrap(),
        r["total_ms"].as_f64().unwrap(),
    );
    assert!((70.0..400.0).contains(&ttft), "ttft {ttft}");
    assert!((70.0..400.0).contains(&gen_ms), "gen {gen_ms}");
    assert!((ttft + gen_ms - total).abs() < 1.0);
    assert!(
        r["upstream_ttfb_ms"].as_f64().unwrap() < ttft,
        "headers arrive before the first token"
    );
    assert!(r["max_chunk_gap_ms"].as_f64().unwrap() >= 35.0);
    assert!(r["output_tps"].as_f64().unwrap() > 0.0);

    // LiteLLM got trace context and attribution headers, and never our own.
    let h = e.seen.headers.lock().unwrap()[0].clone();
    let tp = h["traceparent"].to_str().unwrap();
    assert!(tp.starts_with(&format!("00-{trace_id}-")), "{tp}");
    assert_eq!(h["x-litellm-trace-id"], "sess-9");
    assert_eq!(
        h["x-litellm-tags"],
        "project:apollo,team:growth,agent:writer-1"
    );
    assert_eq!(h["authorization"], "Bearer sk-virtual");
    assert!(!h.contains_key("x-gateway-run-id"));

    // Spans: root, queue and attempt; the attempt is LiteLLM's parent.
    let t = e
        .admin(
            reqwest::Method::GET,
            &format!("/gateway/obs/trace/{trace_id}"),
            None,
        )
        .await;
    let spans = t["spans"].as_array().unwrap();
    let by_name = |n: &str| {
        spans
            .iter()
            .find(|s| s["name"] == n)
            .unwrap_or_else(|| panic!("no span {n}"))
    };
    let (root, attempt) = (by_name("gateway.request"), by_name("gateway.attempt"));
    by_name("gateway.queue");
    assert_eq!(attempt["parent_span_id"], root["span_id"]);
    assert_eq!(tp.split('-').nth(2).unwrap(), attempt["span_id"]);
    assert!(
        attempt["duration_ms"].as_f64().unwrap() >= 150.0,
        "attempt span covers the stream"
    );
    assert_eq!(t["requests"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn run_id_groups_calls_into_one_trace_and_traceparent_is_continued() {
    let e = setup().await;
    let a = e.chat(false, &[("x-gateway-run-id", "run-1")]).await;
    let b = e.chat(false, &[("x-gateway-run-id", "run-1")]).await;
    let c = e
        .chat(
            false,
            &[(
                "traceparent",
                "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            )],
        )
        .await;
    let id = |r: &reqwest::Response| {
        r.headers()["x-gateway-trace-id"]
            .to_str()
            .unwrap()
            .to_string()
    };
    assert_eq!(id(&a), id(&b));
    assert_eq!(id(&c), "4bf92f3577b34da6a3ce929d0e0e4736");
    e.flush().await;
    let t = e
        .admin(
            reqwest::Method::GET,
            "/gateway/obs/trace/4bf92f3577b34da6a3ce929d0e0e4736",
            None,
        )
        .await;
    let root = t["spans"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["name"] == "gateway.request")
        .unwrap();
    assert_eq!(root["parent_span_id"], "00f067aa0ba902b7");
    // Non-streaming: LiteLLM's reported cost and cache hit are taken as-is.
    let r = &t["requests"][0];
    assert_eq!(
        (
            r["cost_usd"].as_f64(),
            r["cost_source"].as_str(),
            r["litellm_cache_hit"].as_bool()
        ),
        (Some(0.0125), Some("litellm"), Some(true))
    );
    assert!(
        r.get("ttft_ms").is_none(),
        "no first-token time without a stream"
    );
}

#[tokio::test]
async fn aggregates_roi_and_otlp_ingest() {
    let e = setup().await;
    for _ in 0..3 {
        e.chat(true, &[("x-gateway-run-id", "run-a")])
            .await
            .bytes()
            .await
            .unwrap();
    }
    e.chat(true, &[("x-gateway-deliverable-id", "doc-1")])
        .await
        .bytes()
        .await
        .unwrap();
    e.flush().await;

    let q = e.admin(reqwest::Method::POST, "/gateway/obs/query", Some(json!({
        "from": 0, "to": FAR, "group_by": ["project_id", "team_id"],
        "metrics": ["count", "sum:cost_usd", "p95:ttft_ms", "ratio:cache_read_tokens/input_tokens", "uniq:run_id"]}))).await;
    let g = &q["rows"][0];
    assert_eq!(
        (
            g["project_id"].as_str(),
            g["count"].as_u64(),
            g["uniq:run_id"].as_u64()
        ),
        // "run-a" from the header, plus the run inferred for the request
        // that named none.
        (Some("apollo"), Some(4), Some(2))
    );
    assert_eq!(g["ratio:cache_read_tokens/input_tokens"], 0.8);
    assert!(g["p95:ttft_ms"].as_f64().unwrap() > 70.0);
    assert!((g["sum:cost_usd"].as_f64().unwrap() - 0.00344).abs() < 1e-9);

    // Register what the spend produced, then ask for ROI.
    for (kind, body) in [
        (
            "deliverable",
            json!({"id": "doc-1", "project_id": "apollo", "team_id": "growth", "kind": "doc", "title": "Launch brief", "status": "accepted", "hours_saved": 0.5}),
        ),
        (
            "deliverable",
            json!({"id": "pr-9", "project_id": "apollo", "team_id": "growth", "kind": "pr", "status": "in_progress"}),
        ),
        ("link", json!({"run_id": "run-a", "deliverable_id": "pr-9"})),
        ("rate", json!({"id": "growth", "hourly_usd": 80.0})),
    ] {
        e.admin(
            reqwest::Method::POST,
            &format!("/gateway/obs/meta/{kind}"),
            Some(body),
        )
        .await;
    }
    let roi = e
        .admin(
            reqwest::Method::GET,
            &format!("/gateway/obs/roi?from=0&to={FAR}"),
            None,
        )
        .await;
    // The productivity view reads the same rows and metadata.
    let ins = e
        .admin(
            reqwest::Method::GET,
            &format!("/gateway/obs/insights?from=0&to={FAR}&tz=9"),
            None,
        )
        .await;
    assert_eq!(ins["cost_basis"], "billed");
    assert_eq!(ins["total"]["value_usd"], 40.0);
    assert_eq!(ins["total"]["requests"], roi["total"]["requests"]);
    assert!(ins["people"].as_array().is_some_and(|p| !p.is_empty()));
    assert_eq!(ins["serving"]["hours"].as_array().unwrap().len(), 24);
    let d = |id: &str| {
        roi["deliverables"]
            .as_array()
            .unwrap()
            .iter()
            .find(|d| d["deliverable_id"] == id)
            .unwrap()
            .clone()
    };
    assert!((d("doc-1")["cost_usd"].as_f64().unwrap() - 0.00086).abs() < 1e-9);
    assert_eq!(d("doc-1")["value_usd"], 40.0);
    assert!((d("pr-9")["cost_usd"].as_f64().unwrap() - 0.00258).abs() < 1e-9);
    assert_eq!(d("pr-9")["value_usd"], 0.0);
    let p = &roi["projects"][0];
    assert_eq!(
        (
            p["name"].as_str(),
            p["unattributed_usd"].as_f64(),
            p["value_usd"].as_f64()
        ),
        (Some("apollo"), Some(0.0), Some(40.0))
    );
    // Malformed metadata is refused.
    let bad = e
        .http
        .post(format!("{}/gateway/obs/meta/deliverable", e.gw))
        .bearer_auth("admin-secret")
        .json(&json!({"id": "x", "valeu_usd": 1}))
        .send()
        .await
        .unwrap();
    assert_eq!(bad.status(), 400);

    // Everything above needs the admin token.
    let anon = e
        .http
        .post(format!("{}/gateway/obs/query", e.gw))
        .json(&json!({"from": 0, "to": 1}))
        .send()
        .await
        .unwrap();
    assert_eq!(anon.status(), 401);
    let otlp_anon = e
        .http
        .post(format!("{}/gateway/otlp/v1/traces", e.gw))
        .header("content-type", "application/x-protobuf")
        .body(vec![])
        .send()
        .await
        .unwrap();
    assert_eq!(otlp_anon.status(), 401);
    let page = e
        .http
        .get(format!("{}/gateway/dashboard", e.gw))
        .send()
        .await
        .unwrap();
    assert!(page.status().is_success());
}
