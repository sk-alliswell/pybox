//! Token-saving behaviour: replaying cached streams, stopping request loops,
//! and accounting for streams the client cancelled.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::sync::Arc;
use std::time::Duration;

use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::response::Response;
use axum::routing::post;
use axum::Router;
use futures_util::StreamExt;
use pybox_gateway::config::Config;
use pybox_gateway::App;
use serde_json::{json, Value};

async fn serve(router: Router) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            router.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap()
    });
    addr
}

/// Three content chunks 60 ms apart, then usage.
async fn chat(State(calls): State<Arc<AtomicUsize>>, _body: Bytes) -> Response {
    let n = calls.fetch_add(1, SeqCst) + 1;
    let events = vec![
        json!({"choices": [{"delta": {"content": format!("answer {n}")}}]}),
        json!({"choices": [{"delta": {"content": " part two"}}]}),
        json!({"choices": [{"delta": {"content": " part three"}}]}),
        json!({"choices": [], "usage": {"prompt_tokens": 40, "completion_tokens": 6}}),
    ];
    let s = futures_util::stream::iter(events).then(|ev| async move {
        tokio::time::sleep(Duration::from_millis(60)).await;
        Ok::<_, std::io::Error>(Bytes::from(format!("data: {ev}\n\n")))
    });
    Response::builder()
        .header("content-type", "text/event-stream")
        .body(Body::from_stream(s))
        .unwrap()
}

async fn gateway(extra: &str) -> (String, Arc<AtomicUsize>, Arc<App>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let upstream = serve(
        Router::new()
            .route("/v1/chat/completions", post(chat))
            .with_state(calls.clone()),
    )
    .await;
    let cfg: Config = toml::from_str(&format!(
        r#"
admin_token = "admin-secret"
{extra}
[[upstreams]]
name = "mock"
base_url = "http://{upstream}"
kind = "litellm"
[[agents]]
id = "a1"
token = "agent-token-1"
[[agents]]
id = "a2"
token = "agent-token-2"
"#
    ))
    .unwrap();
    let app = App::new(cfg).unwrap();
    let addr = serve(app.router()).await;
    (format!("http://{addr}"), calls, app)
}

fn ask(base: &str, token: &str, question: &str) -> reqwest::RequestBuilder {
    reqwest::Client::new()
        .post(format!("{base}/mock/v1/chat/completions"))
        .bearer_auth(token)
        .json(
            &json!({"model": "tiny", "stream": true, "stream_options": {"include_usage": true},
            "messages": [{"role": "user", "content": question}]}),
        )
}

async fn rows(base: &str) -> Vec<Value> {
    let v: Value = reqwest::Client::new()
        .post(format!("{base}/gateway/obs/rows"))
        .bearer_auth("admin-secret")
        .json(&json!({"from": 0, "to": 9_999_999_999_999i64}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    v["rows"].as_array().unwrap().clone()
}

#[tokio::test]
async fn a_repeated_stream_is_replayed_from_the_cache() {
    let (base, calls, app) = gateway("[cache]\nttl_secs = 60\ndefault_on = true").await;
    let first = ask(&base, "agent-token-1", "q").send().await.unwrap();
    assert_eq!(first.headers()["x-gateway-cache"], "miss");
    let first = first.text().await.unwrap();
    assert!(first.contains("answer 1") && first.contains("part three"));

    // Another agent asking the same thing gets the same bytes, as SSE, without an upstream call.
    let again = ask(&base, "agent-token-2", "q").send().await.unwrap();
    assert_eq!(again.headers()["x-gateway-cache"], "hit");
    assert_eq!(again.headers()["content-type"], "text/event-stream");
    assert_eq!(again.text().await.unwrap(), first);
    assert_eq!(calls.load(SeqCst), 1);

    // A different question is a different entry; opting out skips the cache.
    ask(&base, "agent-token-1", "other")
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let off = ask(&base, "agent-token-1", "q")
        .header("x-gateway-cache", "off")
        .send()
        .await
        .unwrap();
    assert!(off.headers().get("x-gateway-cache").is_none());
    off.text().await.unwrap();
    assert_eq!(calls.load(SeqCst), 3);

    app.obs.as_ref().unwrap().flush().await;
    let rows = rows(&base).await;
    let hit = rows.iter().find(|r| r["gateway_cache"] == "hit").unwrap();
    assert_eq!(hit["agent_id"], "a2");
    assert_eq!(hit["cost_usd"], 0.0);
    assert_eq!(hit["input_tokens"], 0);
}

#[tokio::test]
async fn a_cancelled_stream_is_not_cached_and_its_prompt_is_estimated() {
    let (base, calls, app) = gateway("[cache]\nttl_secs = 60\ndefault_on = true").await;
    let res = ask(&base, "agent-token-1", &"long question ".repeat(100))
        .send()
        .await
        .unwrap();
    let mut body = res.bytes_stream();
    body.next().await.unwrap().unwrap(); // first chunk, then hang up
    drop(body);
    tokio::time::sleep(Duration::from_millis(400)).await;

    app.obs.as_ref().unwrap().flush().await;
    let rows_now = rows(&base).await;
    let r = &rows_now[0];
    assert_eq!(r["client_abort"], true, "{r}");
    assert_eq!(r["usage_estimated"], true);
    // About 1,500 bytes of request at four bytes a token.
    let est = r["input_tokens"].as_u64().unwrap();
    assert!((300..500).contains(&est), "{est}");

    // The half-delivered answer must not be served to the next caller.
    let again = ask(&base, "agent-token-1", &"long question ".repeat(100))
        .send()
        .await
        .unwrap();
    assert_eq!(again.headers()["x-gateway-cache"], "miss");
    assert!(again.text().await.unwrap().contains("part three"));
    assert_eq!(calls.load(SeqCst), 2);
}

#[tokio::test]
async fn an_agent_repeating_itself_is_stopped() {
    let (base, calls, app) = gateway("[loop_guard]\nmax_repeats = 3").await;
    for _ in 0..3 {
        let res = ask(&base, "agent-token-1", "retry deploy")
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 200);
        res.text().await.unwrap();
    }
    for _ in 0..2 {
        let res = ask(&base, "agent-token-1", "retry deploy")
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 429);
        let v: Value = res.json().await.unwrap();
        assert_eq!(v["error"]["type"], "gateway_loop_detected");
    }
    // Other agents and other requests go through; so does the old one after a change.
    assert_eq!(
        ask(&base, "agent-token-2", "retry deploy")
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    assert_eq!(
        ask(&base, "agent-token-1", "something else")
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    assert_eq!(
        ask(&base, "agent-token-1", "retry deploy")
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    assert_eq!(calls.load(SeqCst), 6);

    app.obs.as_ref().unwrap().flush().await;
    let blocked = rows(&base)
        .await
        .iter()
        .filter(|r| r["error_type"] == "gateway_loop_detected")
        .count();
    assert_eq!(blocked, 2);
}

#[tokio::test]
async fn cached_repeats_do_not_trip_the_loop_guard() {
    let (base, calls, _app) =
        gateway("[cache]\nttl_secs = 60\ndefault_on = true\n[loop_guard]\nmax_repeats = 2").await;
    for _ in 0..6 {
        let res = ask(&base, "agent-token-1", "faq").send().await.unwrap();
        assert_eq!(res.status(), 200);
        res.text().await.unwrap();
    }
    assert_eq!(calls.load(SeqCst), 1);
}
