//! End-to-end tests against a local mock upstream.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst};
use std::sync::Arc;
use std::time::{Duration, Instant};

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
    active: AtomicUsize,
    max_active: AtomicUsize,
    flaky_left: AtomicUsize,
    stream_dropped: AtomicBool,
    last_key: std::sync::Mutex<String>,
}

struct ActiveGuard(Arc<Mock>);
impl ActiveGuard {
    fn new(m: &Arc<Mock>) -> Self {
        let now = m.active.fetch_add(1, SeqCst) + 1;
        m.max_active.fetch_max(now, SeqCst);
        Self(m.clone())
    }
}
impl Drop for ActiveGuard {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, SeqCst);
    }
}

struct DropFlag(Arc<Mock>);
impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.stream_dropped.store(true, SeqCst);
    }
}

const SSE: &str = concat!(
    "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":11,\"output_tokens\":1}}}\n\n",
    "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"hello\"}}\n\n",
    "event: message_delta\ndata: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":7}}\n\n",
    "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
);

async fn messages(State(m): State<Arc<Mock>>, headers: HeaderMap, body: Bytes) -> Response {
    m.hits.fetch_add(1, SeqCst);
    let key = headers
        .get("x-api-key")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    *m.last_key.lock().unwrap() = key.clone();
    if key != "real-key" {
        return (StatusCode::UNAUTHORIZED, "bad key").into_response();
    }
    let guard = ActiveGuard::new(&m);
    let v: Value = serde_json::from_slice(&body).unwrap();
    let delay = v["delay_ms"].as_u64().unwrap_or(0);
    tokio::time::sleep(Duration::from_millis(delay)).await;
    if v["stream"] == true {
        // Send the SSE in small pieces to exercise chunk-boundary parsing.
        let chunks: Vec<Bytes> = SSE
            .as_bytes()
            .chunks(13)
            .map(Bytes::copy_from_slice)
            .collect();
        let s = futures_util::stream::iter(chunks).then(move |c| async move {
            tokio::time::sleep(Duration::from_millis(2)).await;
            Ok::<_, std::io::Error>(c)
        });
        let s = s.chain(futures_util::stream::once(async move {
            drop(guard);
            Ok(Bytes::new())
        }));
        return Response::builder()
            .header("content-type", "text/event-stream")
            .body(Body::from_stream(s))
            .unwrap();
    }
    drop(guard);
    axum::Json(json!({"content":[{"type":"text","text":"hi"}],"usage":{"input_tokens":5,"output_tokens":3}}))
        .into_response()
}

async fn flaky(State(m): State<Arc<Mock>>) -> Response {
    m.hits.fetch_add(1, SeqCst);
    let mut left = m.flaky_left.load(SeqCst);
    let mut fail = false;
    while left > 0 {
        match m
            .flaky_left
            .compare_exchange(left, left - 1, SeqCst, SeqCst)
        {
            Ok(_) => {
                fail = true;
                break;
            }
            Err(now) => left = now,
        }
    }
    if fail {
        return Response::builder()
            .status(429)
            .header("retry-after-ms", "100")
            .body(Body::from("slow down"))
            .unwrap();
    }
    axum::Json(json!({"ok":true,"usage":{"input_tokens":1,"output_tokens":1}})).into_response()
}

async fn endless(State(m): State<Arc<Mock>>) -> Response {
    let flag = DropFlag(m.clone());
    let s = futures_util::stream::unfold(flag, |flag| async move {
        tokio::time::sleep(Duration::from_millis(20)).await;
        Some((
            Ok::<_, std::io::Error>(Bytes::from_static(b"data: {\"tick\":1}\n\n")),
            flag,
        ))
    });
    Response::builder()
        .header("content-type", "text/event-stream")
        .body(Body::from_stream(s))
        .unwrap()
}

async fn serve(router: Router) -> SocketAddr {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(l, router).await.unwrap() });
    addr
}

struct Env {
    gw: String,
    mock: Arc<Mock>,
    http: reqwest::Client,
}

async fn setup(extra: &str) -> Env {
    let mock = Arc::new(Mock::default());
    let up = serve(
        Router::new()
            .route("/v1/messages", post(messages))
            .route("/v1/flaky", post(flaky))
            .route("/v1/endless", post(endless))
            .with_state(mock.clone()),
    )
    .await;
    let toml = format!(
        r#"
admin_token = "admin-secret"
[retry]
base_ms = 20
{extra}
[[upstreams]]
name = "anthropic"
base_url = "http://{up}"
kind = "anthropic"
api_key = "real-key"
max_concurrency = 2

[[agents]]
id = "a1"
token = "agent-one-token"

[[agents]]
id = "a2"
token = "agent-two-token"
token_budget = 10
"#
    );
    let cfg: Config = toml::from_str(&toml).unwrap();
    let app = App::new(cfg).unwrap();
    let gw = serve(app.router()).await;
    Env {
        gw: format!("http://{gw}"),
        mock,
        http: reqwest::Client::new(),
    }
}

impl Env {
    fn post(&self, path: &str, token: &str, body: Value) -> reqwest::RequestBuilder {
        self.http
            .post(format!("{}{}", self.gw, path))
            .header("x-api-key", token)
            .header("anthropic-version", "2023-06-01")
            .json(&body)
    }

    async fn stats(&self) -> Value {
        self.http
            .get(format!("{}/gateway/stats", self.gw))
            .bearer_auth("admin-secret")
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap()
    }
}

#[tokio::test]
async fn swaps_agent_token_for_real_key() {
    let e = setup("").await;
    let r = e
        .post("/anthropic/v1/messages", "nope-nope-nope", json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 401);
    assert_eq!(
        e.mock.hits.load(SeqCst),
        0,
        "unauthenticated requests never reach upstream"
    );

    let r = e
        .post(
            "/anthropic/v1/messages",
            "agent-one-token",
            json!({"model":"m"}),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(*e.mock.last_key.lock().unwrap(), "real-key");
}

#[tokio::test]
async fn streams_sse_unchanged_and_counts_usage() {
    let e = setup("").await;
    let r = e
        .post(
            "/anthropic/v1/messages",
            "agent-one-token",
            json!({"model":"m","stream":true}),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert!(r.headers()["content-type"]
        .to_str()
        .unwrap()
        .starts_with("text/event-stream"));
    assert_eq!(r.headers()["x-gateway-attempts"], "1");
    for h in ["x-gateway-queue-ms", "x-gateway-upstream-ms"] {
        let v: f64 = r.headers()[h].to_str().unwrap().parse().unwrap();
        assert!(v >= 0.0, "{h}");
    }
    let body = r.text().await.unwrap();
    assert_eq!(body, SSE);
    tokio::time::sleep(Duration::from_millis(50)).await;
    let s = e.stats().await;
    assert_eq!(s["agents"]["a1"]["input_tokens"], 11);
    assert_eq!(s["agents"]["a1"]["output_tokens"], 7);
    assert_eq!(s["agents"]["a1"]["streams"], 1);
}

#[tokio::test]
async fn retries_429_with_shared_cooldown() {
    let e = setup("").await;
    e.mock.flaky_left.store(2, SeqCst);
    let t0 = Instant::now();
    let r = e
        .post("/anthropic/v1/flaky", "agent-one-token", json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200, "client never sees the transient 429s");
    assert_eq!(r.headers()["x-gateway-attempts"], "3");
    assert!(
        t0.elapsed() >= Duration::from_millis(190),
        "honours retry-after-ms"
    );
    assert_eq!(e.mock.hits.load(SeqCst), 3);
    let s = e.stats().await;
    assert_eq!(s["upstreams"]["anthropic"]["counters"]["rate_limited"], 2);
    assert_eq!(s["agents"]["a1"]["retries"], 2);
}

#[tokio::test]
async fn gives_up_after_max_retries_and_passes_429_through() {
    let e = setup("max_retries = 1").await;
    e.mock.flaky_left.store(5, SeqCst);
    let r = e
        .post("/anthropic/v1/flaky", "agent-one-token", json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 429);
    assert_eq!(e.mock.hits.load(SeqCst), 2);
}

#[tokio::test]
async fn caps_upstream_concurrency_across_agents() {
    let e = setup("").await;
    let mut hs = Vec::new();
    for i in 0..10 {
        let req = e.post(
            "/anthropic/v1/messages",
            if i % 2 == 0 {
                "agent-one-token"
            } else {
                "agent-two-token"
            },
            json!({"delay_ms": 40, "n": i}),
        );
        hs.push(tokio::spawn(
            async move { req.send().await.unwrap().status() },
        ));
    }
    for h in hs {
        assert_eq!(h.await.unwrap(), 200);
    }
    assert_eq!(e.mock.hits.load(SeqCst), 10);
    assert!(
        e.mock.max_active.load(SeqCst) <= 2,
        "max_concurrency = 2 was exceeded"
    );
}

#[tokio::test]
async fn coalesces_identical_cacheable_requests() {
    let e = setup("").await;
    let mut hs = Vec::new();
    for _ in 0..5 {
        let req = e
            .post(
                "/anthropic/v1/messages",
                "agent-one-token",
                json!({"delay_ms": 80, "temperature": 0}),
            )
            .header("x-gateway-cache", "on");
        hs.push(tokio::spawn(async move {
            let r = req.send().await.unwrap();
            let how = r.headers()["x-gateway-cache"].to_str().unwrap().to_string();
            (how, r.text().await.unwrap())
        }));
    }
    let mut kinds = Vec::new();
    for h in hs {
        let (how, body) = h.await.unwrap();
        assert!(body.contains("\"hi\""));
        kinds.push(how);
    }
    assert_eq!(
        e.mock.hits.load(SeqCst),
        1,
        "one upstream call for five requests"
    );
    assert_eq!(kinds.iter().filter(|k| *k == "miss").count(), 1);

    let r = e
        .post(
            "/anthropic/v1/messages",
            "agent-one-token",
            json!({"delay_ms": 80, "temperature": 0}),
        )
        .header("x-gateway-cache", "on")
        .send()
        .await
        .unwrap();
    assert_eq!(r.headers()["x-gateway-cache"], "hit");
    assert_eq!(e.mock.hits.load(SeqCst), 1);
}

#[tokio::test]
async fn client_disconnect_cancels_upstream_and_frees_slot() {
    let e = setup("").await;
    let r = e
        .post(
            "/anthropic/v1/endless",
            "agent-one-token",
            json!({"stream": true}),
        )
        .send()
        .await
        .unwrap();
    let mut s = r.bytes_stream();
    let first = s.next().await.unwrap().unwrap();
    assert!(first.starts_with(b"data:"));
    drop(s);
    let deadline = Instant::now() + Duration::from_secs(2);
    while !e.mock.stream_dropped.load(SeqCst) {
        assert!(
            Instant::now() < deadline,
            "upstream stream was not cancelled"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let st = e.stats().await;
    assert_eq!(st["upstreams"]["anthropic"]["scheduler"]["inflight"], 0);
    assert_eq!(st["agents"]["a1"]["client_aborts"], 1);
}

#[tokio::test]
async fn enforces_token_budget() {
    let e = setup("").await;
    // a2 has a 10-token budget; one call uses 8.
    let r = e
        .post("/anthropic/v1/messages", "agent-two-token", json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let _ = r.bytes().await;
    let r = e
        .post("/anthropic/v1/messages", "agent-two-token", json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let _ = r.bytes().await;
    let r = e
        .post("/anthropic/v1/messages", "agent-two-token", json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 429);
    let v: Value = r.json().await.unwrap();
    assert_eq!(v["error"]["type"], "gateway_budget_exceeded");
}

#[tokio::test]
async fn registers_agents_at_runtime() {
    let e = setup("").await;
    let r = e
        .http
        .post(format!("{}/gateway/agents", e.gw))
        .json(&json!({"id": "box-7"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 401, "admin token required");

    let v: Value = e
        .http
        .post(format!("{}/gateway/agents", e.gw))
        .bearer_auth("admin-secret")
        .json(&json!({"id": "box-7", "weight": 2}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let token = v["token"].as_str().unwrap().to_string();
    let r = e
        .post("/anthropic/v1/messages", &token, json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);

    let r = e
        .http
        .delete(format!("{}/gateway/agents/box-7", e.gw))
        .bearer_auth("admin-secret")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let r = e
        .post("/anthropic/v1/messages", &token, json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 401, "revoked token is rejected");
}
