//! What the gateway does when the servers behind it misbehave: errors,
//! silence, streams cut off, requests they will not take. Each test stands
//! up mock model servers with one fault switched on.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU16, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::Router;
use pybox_gateway::config::Config;
use pybox_gateway::App;
use serde_json::{json, Value};

#[derive(Default)]
struct Mock {
    bodies: Mutex<Vec<String>>,
    hits: AtomicUsize,
    /// Answer every request with this status (0 = behave).
    status: AtomicU16,
    /// Never answer.
    hang: std::sync::atomic::AtomicBool,
    /// Cut a streamed answer off after the first chunk.
    cut_stream: std::sync::atomic::AtomicBool,
    /// Answer 400 to a body containing this (an unknown parameter).
    rejects: Mutex<Option<&'static str>>,
    /// Answer 429 to this model until the given hit count has been reached.
    limited_model: Mutex<Option<&'static str>>,
    limited_left: AtomicU64,
    delay_ms: AtomicU64,
}

async fn chat(State(m): State<Arc<Mock>>, body: Bytes) -> Response {
    let text = String::from_utf8(body.to_vec()).unwrap();
    m.hits.fetch_add(1, Ordering::SeqCst);
    m.bodies.lock().unwrap().push(text.clone());
    if m.hang.load(Ordering::SeqCst) {
        std::future::pending::<()>().await;
    }
    let code = m.status.load(Ordering::SeqCst);
    if code != 0 {
        return (StatusCode::from_u16(code).unwrap(), "broken").into_response();
    }
    let unknown = *m.rejects.lock().unwrap();
    if unknown.is_some_and(|p| text.contains(p)) {
        return (
            StatusCode::BAD_REQUEST,
            r#"{"error":{"message":"unsupported parameter"}}"#,
        )
            .into_response();
    }
    let v: Value = serde_json::from_str(&text).unwrap();
    let model = v["model"].as_str().unwrap_or("");
    let limited = *m.limited_model.lock().unwrap();
    if limited == Some(model) && m.limited_left.load(Ordering::SeqCst) > 0 {
        m.limited_left.fetch_sub(1, Ordering::SeqCst);
        return (
            StatusCode::TOO_MANY_REQUESTS,
            [("retry-after-ms", "400")],
            "slow down",
        )
            .into_response();
    }
    tokio::time::sleep(Duration::from_millis(m.delay_ms.load(Ordering::SeqCst))).await;
    if v["stream"] == true {
        let first = format!(
            "data: {}\n\n",
            json!({"choices": [{"index": 0, "delta": {"content": "hello"}}]})
        );
        let last = format!(
            "data: {}\n\ndata: [DONE]\n\n",
            json!({"choices": [], "usage": {"prompt_tokens": 10, "completion_tokens": 2, "total_tokens": 12}})
        );
        let cut = m.cut_stream.load(Ordering::SeqCst);
        let stream = async_stream(first, last, cut);
        return Response::builder()
            .header(header::CONTENT_TYPE, "text/event-stream")
            .body(Body::from_stream(stream))
            .unwrap();
    }
    let total = (text.len() / 4) as u64;
    axum::Json(json!({
        "model": model,
        "choices": [{"index": 0, "message": {"role": "assistant", "content": "ok"}, "finish_reason": "stop"}],
        "usage": {"prompt_tokens": total, "completion_tokens": 20, "total_tokens": total + 20}
    }))
    .into_response()
}

fn async_stream(
    first: String,
    last: String,
    cut: bool,
) -> impl futures_util::Stream<Item = Result<Bytes, std::io::Error>> {
    futures_util::stream::unfold(0u8, move |step| {
        let (first, last) = (first.clone(), last.clone());
        async move {
            match step {
                0 => Some((Ok(Bytes::from(first)), 1)),
                1 => {
                    tokio::time::sleep(Duration::from_millis(30)).await;
                    if cut {
                        Some((Err(std::io::Error::other("connection lost")), 2))
                    } else {
                        Some((Ok(Bytes::from(last)), 2))
                    }
                }
                _ => None,
            }
        }
    })
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

async fn mock() -> (Arc<Mock>, SocketAddr) {
    let m = Arc::new(Mock::default());
    let addr = serve(
        Router::new()
            .route("/v1/chat/completions", post(chat))
            .with_state(m.clone()),
    )
    .await;
    (m, addr)
}

struct Env {
    gw: String,
    http: reqwest::Client,
    _app: Arc<App>,
}

const HEAD: &str = r#"
admin_token = "admin-secret"

[retry]
max_retries = 3
base_ms = 10
max_ms = 50
"#;

const AGENTS: &str = r#"
[[agents]]
id = "coder"
token = "agent-one-token"

[[agents]]
id = "sorter"
token = "agent-two-token"
model_swap = true
"#;

impl Env {
    async fn new(toml: String) -> Self {
        let cfg: Config = toml::from_str(&toml).unwrap();
        let app = App::new(cfg).unwrap();
        let gw = serve(app.router()).await;
        Self {
            gw: format!("http://{gw}"),
            http: reqwest::Client::new(),
            _app: app,
        }
    }
    async fn chat(&self, token: &str, model: &str, stream: bool) -> reqwest::Response {
        static RUN: AtomicU64 = AtomicU64::new(0);
        // Every call is its own run: a run otherwise stays with the server
        // that answered it last.
        let run = format!("run-{}", RUN.fetch_add(1, Ordering::SeqCst));
        self.http
            .post(format!("{}/llm/v1/chat/completions", self.gw))
            .bearer_auth(token)
            .header("x-gateway-run-id", run)
            .json(&json!({
                "model": model, "max_tokens": 50, "stream": stream,
                "messages": [{"role": "user", "content": "hello there"}],
            }))
            .send()
            .await
            .unwrap()
    }
    async fn admin(&self, path: &str) -> Value {
        self.http
            .get(format!("{}{}", self.gw, path))
            .bearer_auth("admin-secret")
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap()
    }
    async fn sched(&self) -> Value {
        self.admin("/gateway/stats").await["upstreams"]["llm"]["scheduler"].clone()
    }
    async fn rows(&self) -> Vec<Value> {
        // Rows are written when the response body ends.
        tokio::time::sleep(Duration::from_millis(60)).await;
        self._app.obs.as_ref().unwrap().flush().await;
        let v: Value = self
            .http
            .post(format!("{}/gateway/obs/rows", self.gw))
            .bearer_auth("admin-secret")
            .json(&json!({"from": 0, "to": 9_000_000_000_000i64, "limit": 100}))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        v["rows"].as_array().cloned().unwrap_or_default()
    }
}

fn two_backends(a: SocketAddr, b: SocketAddr, extra: &str, models: [&str; 2]) -> String {
    format!(
        r#"{HEAD}
[[upstreams]]
name = "llm"
base_url = "http://{a}"
kind = "openai"
api_key = "sk-real"
{extra}

[[upstreams.backends]]
name = "a"
base_url = "http://{a}"
max_concurrency = 2
models = [{}]

[[upstreams.backends]]
name = "b"
base_url = "http://{b}"
max_concurrency = 2
models = [{}]
{AGENTS}"#,
        models[0], models[1]
    )
}

#[tokio::test]
async fn a_server_that_keeps_failing_is_taken_out_and_nobody_notices() {
    let (a, addr_a) = mock().await;
    let (b, addr_b) = mock().await;
    a.status.store(503, Ordering::SeqCst);
    let e = Env::new(two_backends(addr_a, addr_b, "", ["", ""])).await;
    for _ in 0..12 {
        let r = e.chat("agent-one-token", "m", false).await;
        assert_eq!(r.status(), 200, "the caller never sees the broken server");
    }
    let s = e.sched().await;
    assert_eq!(s["backends"][0]["cooling"], true, "{s}");
    // Three failures take it out; a retry goes to the other server at once
    // rather than back to the one that just failed.
    assert_eq!(a.hits.load(Ordering::SeqCst), 3, "{s}");
    assert_eq!(b.hits.load(Ordering::SeqCst), 12);
}

#[tokio::test]
async fn a_server_that_never_answers_is_given_up_on() {
    let (a, addr_a) = mock().await;
    let (b, addr_b) = mock().await;
    a.hang.store(true, Ordering::SeqCst);
    let e = Env::new(two_backends(
        addr_a,
        addr_b,
        "first_byte_timeout_secs = 1",
        ["", ""],
    ))
    .await;
    let t0 = Instant::now();
    let r = e.chat("agent-one-token", "m", false).await;
    assert_eq!(r.status(), 200);
    let took = t0.elapsed();
    assert!(
        took >= Duration::from_millis(900) && took < Duration::from_secs(3),
        "one timeout, then the other server: {took:?}"
    );
    assert_eq!(b.hits.load(Ordering::SeqCst), 1);
    // The seat on the silent server came back.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(e.sched().await["inflight"], 0);
}

#[tokio::test]
async fn a_stream_cut_off_midway_is_reported_and_frees_its_seat() {
    let (a, addr) = mock().await;
    a.cut_stream.store(true, Ordering::SeqCst);
    let e = Env::new(format!(
        r#"{HEAD}
[[upstreams]]
name = "llm"
base_url = "http://{addr}"
kind = "openai"
api_key = "sk-real"
max_concurrency = 1
{AGENTS}"#
    ))
    .await;
    let r = e.chat("agent-one-token", "m", true).await;
    assert_eq!(r.status(), 200, "the answer had already started");
    // What arrived is passed on; then the body ends with an error, not a
    // clean close, so the client knows it is incomplete.
    let body = r.bytes().await;
    assert!(body.is_err() || !String::from_utf8_lossy(&body.unwrap()).contains("[DONE]"));
    let rows = e.rows().await;
    assert_eq!(
        rows[0]["error_type"], "upstream_stream_error",
        "{}",
        rows[0]
    );
    assert_eq!(e.sched().await["inflight"], 0);
    // It is not retried (bytes had reached the caller) and the next request runs.
    assert_eq!(a.hits.load(Ordering::SeqCst), 1);
    a.cut_stream.store(false, Ordering::SeqCst);
    let ok = e.chat("agent-one-token", "m", true).await;
    assert!(ok.text().await.unwrap().contains("[DONE]"));
}

#[tokio::test]
async fn one_rate_limited_model_does_not_hold_up_the_others() {
    let (a, addr) = mock().await;
    *a.limited_model.lock().unwrap() = Some("hot");
    a.limited_left.store(1, Ordering::SeqCst);
    let e = Arc::new(
        Env::new(format!(
            r#"{HEAD}
[[upstreams]]
name = "llm"
base_url = "http://{addr}"
kind = "openai"
api_key = "sk-real"
max_concurrency = 4
{AGENTS}"#
        ))
        .await,
    );
    let hot = {
        let e = e.clone();
        tokio::spawn(async move {
            let t0 = Instant::now();
            let r = e.chat("agent-one-token", "hot", false).await;
            (r.status(), t0.elapsed())
        })
    };
    tokio::time::sleep(Duration::from_millis(80)).await;
    let t0 = Instant::now();
    let cold = e.chat("agent-one-token", "cold", false).await;
    assert_eq!(cold.status(), 200);
    assert!(
        t0.elapsed() < Duration::from_millis(200),
        "the other model is not paused: {:?}",
        t0.elapsed()
    );
    let (status, took) = hot.await.unwrap();
    assert_eq!(status, 200);
    assert!(
        took >= Duration::from_millis(380),
        "waited out its pause: {took:?}"
    );
}

#[tokio::test]
async fn a_stand_in_model_serves_an_agent_that_allows_it() {
    let (a, addr_a) = mock().await;
    let (b, addr_b) = mock().await;
    a.delay_ms.store(300, Ordering::SeqCst);
    let e = Arc::new(
        Env::new(two_backends(
            addr_a,
            addr_b,
            r#"model_groups = [["qwen", "smol"]]"#,
            [r#""qwen""#, r#""smol""#],
        ))
        .await,
    );
    // Fill qwen's two seats, then one more request for qwen from each agent.
    let mut held = Vec::new();
    for _ in 0..2 {
        let e = e.clone();
        held.push(tokio::spawn(async move {
            e.chat("agent-one-token", "qwen", false).await.status()
        }));
    }
    tokio::time::sleep(Duration::from_millis(60)).await;
    let t0 = Instant::now();
    let moved = e.chat("agent-two-token", "qwen", false).await;
    assert_eq!(moved.status(), 200);
    assert!(
        t0.elapsed() < Duration::from_millis(150),
        "did not wait for qwen"
    );
    assert_eq!(b.hits.load(Ordering::SeqCst), 1);
    let sent: Value = serde_json::from_str(&b.bodies.lock().unwrap()[0]).unwrap();
    assert_eq!(
        sent["model"], "smol",
        "asked the stand-in for its own model"
    );
    // An agent that did not ask for stand-ins waits for its own model.
    let stayed = e.chat("agent-one-token", "qwen", false).await;
    assert_eq!(stayed.status(), 200);
    assert_eq!(b.hits.load(Ordering::SeqCst), 1);
    for h in held {
        assert_eq!(h.await.unwrap(), 200);
    }
    let rows = e.rows().await;
    assert_eq!(
        rows.iter()
            .filter(|r| r["model_swapped_to"] == "smol")
            .count(),
        1
    );
}

#[tokio::test]
async fn a_rewrite_the_upstream_rejects_is_undone_and_switched_off() {
    let (a, addr) = mock().await;
    *a.rejects.lock().unwrap() = Some("prompt_cache_key");
    let e = Env::new(format!(
        r#"{HEAD}
[lead]
cache_points = "on"

[[upstreams]]
name = "llm"
base_url = "http://{addr}"
kind = "openai"
api_key = "sk-real"
{AGENTS}"#
    ))
    .await;
    let long = "Follow the house rules. ".repeat(600);
    let ask = |n: usize| {
        json!({
            "model": "gpt-4o-mini", "max_tokens": 50,
            "messages": [{"role": "system", "content": long}, {"role": "user", "content": format!("question {n}")}],
        })
    };
    let send = |n: usize| {
        e.http
            .post(format!("{}/llm/v1/chat/completions", e.gw))
            .bearer_auth("agent-one-token")
            .header("x-gateway-run-id", "run-1")
            .json(&ask(n))
            .send()
    };
    for n in 0..4 {
        let r = send(n).await.unwrap();
        assert_eq!(r.status(), 200, "request {n} reaches the model as written");
    }
    let bodies = a.bodies.lock().unwrap().clone();
    let rewritten = bodies
        .iter()
        .filter(|b| b.contains("prompt_cache_key"))
        .count();
    assert_eq!(rewritten, 1, "tried once, then left alone");
    assert_eq!(bodies.len(), 5, "one rejected attempt plus four answers");
    let stats = e.admin("/gateway/stats").await;
    assert_eq!(
        stats["upstreams"]["llm"]["counters"]["rewrite_rejected"], 1,
        "{stats}"
    );
    let rows = e.rows().await;
    assert_eq!(
        rows.iter().filter(|r| r["rewrite"] == "rejected").count(),
        1
    );
    assert!(rows.iter().any(|r| r["rewrite"] == "off"));
}

#[tokio::test]
async fn a_genuinely_bad_request_is_passed_back_once() {
    let (a, addr) = mock().await;
    a.status.store(400, Ordering::SeqCst);
    let e = Env::new(format!(
        r#"{HEAD}
[[upstreams]]
name = "llm"
base_url = "http://{addr}"
kind = "openai"
api_key = "sk-real"
{AGENTS}"#
    ))
    .await;
    let r = e.chat("agent-one-token", "m", false).await;
    assert_eq!(r.status(), 400);
    assert_eq!(a.hits.load(Ordering::SeqCst), 1, "a 400 is not retried");
}
