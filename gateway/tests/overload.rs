//! What a caller sees when the upstream is saturated: where it stands in the
//! queue, an immediate refusal when the wait would be too long, and bulk work
//! making room for interactive work.

use std::net::SocketAddr;
use std::time::Duration;

use axum::body::Body;
use axum::response::Response;
use axum::routing::post;
use axum::Router;
use pybox_gateway::config::Config;
use pybox_gateway::App;
use serde_json::json;

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

/// One slot, and every answer takes 300 ms.
async fn gateway(extra: &str) -> String {
    let upstream = serve(Router::new().route(
        "/v1/chat/completions",
        post(|| async {
            tokio::time::sleep(Duration::from_millis(300)).await;
            let sse = format!(
                "data: {}\n\ndata: [DONE]\n\n",
                json!({"choices": [{"delta": {"content": "hi"}}]})
            );
            Response::builder()
                .header("content-type", "text/event-stream")
                .body(Body::from(sse))
                .unwrap()
        }),
    ))
    .await;
    let cfg: Config = toml::from_str(&format!(
        r#"
listen = "127.0.0.1:0"
admin_token = "admin"
[[upstreams]]
name = "slow"
base_url = "http://{upstream}"
kind = "litellm"
max_concurrency = 1
{extra}
[[agents]]
id = "chat"
token = "chat-token"
weight = 4
[[agents]]
id = "batch"
token = "batch-token"
[[agents]]
id = "bulk"
token = "bulk-token"
priority = "batch"
[[agents]]
id = "ide"
token = "ide-token"
max_wait_ms = 200
"#
    ))
    .unwrap();
    let addr = serve(App::new(cfg).unwrap().router()).await;
    format!("http://{addr}/slow/v1/chat/completions")
}

fn call(url: &str, token: &str) -> reqwest::RequestBuilder {
    reqwest::Client::new()
        .post(url)
        .bearer_auth(token)
        .json(&json!({"model": "m", "stream": true, "messages": []}))
}

async fn spawn_calls(url: &str, token: &str, n: usize) -> Vec<tokio::task::JoinHandle<u16>> {
    let mut v = Vec::new();
    for _ in 0..n {
        let req = call(url, token);
        v.push(tokio::spawn(async move {
            let res = req.send().await.unwrap();
            let status = res.status().as_u16();
            let _ = res.bytes().await;
            status
        }));
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
    v
}

#[tokio::test]
async fn a_wait_that_is_too_long_is_refused_at_once_with_the_outlook() {
    let url = gateway("admit_by_wait = true").await;
    // The first call teaches the gateway how long a slot is held.
    assert_eq!(call(&url, "chat-token").send().await.unwrap().status(), 200);
    let waiting = spawn_calls(&url, "chat-token", 4).await;
    let t0 = std::time::Instant::now();
    let res = call(&url, "chat-token")
        .header("x-gateway-max-wait-ms", "200")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 503);
    assert!(t0.elapsed() < Duration::from_millis(150), "said no at once");
    assert_eq!(res.headers()["x-gateway-queue-ahead"], "3");
    let wait: u64 = res.headers()["x-gateway-expected-wait-ms"]
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    assert!((900..2000).contains(&wait), "{wait}");
    assert!(res.headers().contains_key("retry-after"));
    assert!(res.text().await.unwrap().contains("3 ahead"));
    // Everyone who was let in is answered, and told where it stood.
    for h in waiting {
        assert_eq!(h.await.unwrap(), 200);
    }
    let res = call(&url, "chat-token").send().await.unwrap();
    assert_eq!(res.headers()["x-gateway-queue-ahead"], "0");
}

#[tokio::test]
async fn an_agents_own_patience_is_used_when_the_request_states_none() {
    let url = gateway("admit_by_wait = true").await;
    assert_eq!(call(&url, "chat-token").send().await.unwrap().status(), 200);
    let waiting = spawn_calls(&url, "chat-token", 4).await;
    // Configured to wait 200 ms at most: refused at the door.
    let res = call(&url, "ide-token").send().await.unwrap();
    assert_eq!(res.status(), 503);
    assert_eq!(res.headers()["x-gateway-reject-reason"], "saturated");
    // What the request says itself wins over the agent's setting.
    let res = call(&url, "ide-token")
        .header("x-gateway-max-wait-ms", "60000")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    for h in waiting {
        assert_eq!(h.await.unwrap(), 200);
    }
}

#[tokio::test]
async fn bulk_work_makes_room_and_waits_behind_interactive_work() {
    let url = gateway("max_queue = 2\noverflow = \"evict\"\nstrict_priority = true").await;
    let batch = spawn_calls(&url, "batch-token", 3).await; // one running, two waiting
    let chat = spawn_calls(&url, "chat-token", 1).await;
    let mut statuses = Vec::new();
    for h in batch {
        statuses.push(h.await.unwrap());
    }
    // The newest waiting batch request gave up its place.
    assert_eq!(statuses, [200, 200, 503]);
    assert_eq!(chat.into_iter().next().unwrap().await.unwrap(), 200);
}

#[tokio::test]
async fn a_queued_stream_is_told_where_it_stands() {
    let url = gateway("").await;
    let _busy = spawn_calls(&url, "batch-token", 3).await;
    for (mode, marker) in [
        ("sse", ": gateway queued ahead="),
        (
            "event",
            "event: gateway.queue\ndata: {\"status\":\"queued\",\"ahead\":",
        ),
    ] {
        let res = call(&url, "chat-token")
            .header("x-gateway-queue-notice", mode)
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 200);
        assert_eq!(res.headers()["x-gateway-queued"], "1");
        let text = res.text().await.unwrap();
        assert!(text.starts_with(marker), "{text}");
        assert!(text.contains("expected_wait_ms"), "{text}");
        assert!(text.contains("dispatched"), "{text}");
        assert!(text.ends_with("data: [DONE]\n\n"), "{text}");
    }
    // Not queued: the ordinary response, untouched.
    tokio::time::sleep(Duration::from_millis(700)).await;
    let res = call(&url, "chat-token")
        .header("x-gateway-queue-notice", "sse")
        .send()
        .await
        .unwrap();
    assert!(!res.headers().contains_key("x-gateway-queued"));
    assert!(res.text().await.unwrap().starts_with("data: "));
}

/// The scheduler's counters for the upstream, from `/gateway/stats`.
async fn sched_stat(url: &str, key: &str) -> u64 {
    let stats = url.replace("/slow/v1/chat/completions", "/gateway/stats");
    let v: serde_json::Value = reqwest::Client::new()
        .get(stats)
        .bearer_auth("admin")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    v["upstreams"]["slow"]["scheduler"][key].as_u64().unwrap()
}

#[tokio::test]
async fn batch_is_held_while_other_work_is_refused_when_full() {
    let url = gateway("max_queue = 1\nhold_batch = true").await;
    let first = spawn_calls(&url, "chat-token", 1).await; // running
    let batch = spawn_calls(&url, "bulk-token", 4).await; // all held
    let chat = spawn_calls(&url, "chat-token", 2).await; // one waits, one refused
    assert_eq!(sched_stat(&url, "held").await, 4);
    let mut statuses = Vec::new();
    for h in first.into_iter().chain(chat) {
        statuses.push(h.await.unwrap());
    }
    assert_eq!(statuses, [200, 200, 503]);
    // The chat request that waited went before the batch requests that
    // came earlier; none of those was refused.
    assert!(sched_stat(&url, "held").await >= 3);
    for h in batch {
        assert_eq!(h.await.unwrap(), 200);
    }
}

#[tokio::test]
async fn a_held_request_ends_at_its_limit_or_when_its_caller_leaves() {
    let url = gateway("hold_batch = true\nhold_timeout_secs = 1\nhold_max = 2").await;
    // Chat keeps the only slot busy for longer than the hold limit.
    let chat = spawn_calls(&url, "chat-token", 6).await;
    let leaver = spawn_calls(&url, "bulk-token", 1).await;
    let stayer = call(&url, "bulk-token").send();
    let (res, ()) = tokio::join!(stayer, async {
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(sched_stat(&url, "held").await, 2);
        // A third is refused: as many as allowed are held.
        let full = call(&url, "bulk-token").send().await.unwrap();
        assert_eq!(full.status(), 503);
        assert!(full.text().await.unwrap().contains("held already"));
        // One caller hangs up: its request is forgotten.
        leaver[0].abort();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(sched_stat(&url, "held").await, 1);
    });
    // The other is told no after the hold limit, not the queue's.
    let res = res.unwrap();
    assert_eq!(res.status(), 503);
    assert_eq!(res.headers()["x-gateway-reject-reason"], "saturated");
    assert_eq!(sched_stat(&url, "held").await, 0);
    for h in chat {
        assert_eq!(h.await.unwrap(), 200);
    }
}

#[tokio::test]
async fn a_waiting_request_has_a_ticket_that_can_be_asked_about() {
    let url = gateway("hold_batch = true\nqueue_notice = \"sse\"").await;
    let ask = |ticket: &str, token: &str| {
        let at = url.replace("/slow/v1/chat/completions", "/gateway/queue/") + ticket;
        let req = reqwest::Client::new().get(at).bearer_auth(token);
        async move {
            let res = req.send().await.unwrap();
            let status = res.status().as_u16();
            (status, res.json::<serde_json::Value>().await.unwrap())
        }
    };
    let _busy = spawn_calls(&url, "chat-token", 4).await;
    // Not streaming: nothing can be said in the answer, so the caller names
    // a ticket and asks from outside.
    let plain = reqwest::Client::new()
        .post(&url)
        .bearer_auth("chat-token")
        .header("x-gateway-ticket", "job-7")
        .json(&json!({"model": "m", "messages": []}))
        .send();
    let plain = tokio::spawn(plain);
    tokio::time::sleep(Duration::from_millis(100)).await;
    let (status, t) = ask("job-7", "chat-token").await;
    assert_eq!((status, t["status"].as_str()), (200, Some("queued")), "{t}");
    assert!(t["ahead"].as_u64().unwrap() >= 3, "{t}");
    assert!(
        t["expected_wait_ms"].is_u64() && t["waited_ms"].is_u64(),
        "{t}"
    );
    // Another agent cannot ask about it.
    assert_eq!(ask("job-7", "ide-token").await.0, 404);
    // The upstream's setting tells a streaming caller without it asking,
    // and a held request says so.
    let res = call(&url, "bulk-token").send().await.unwrap();
    assert_eq!(res.status(), 200);
    let ticket = res.headers()["x-gateway-ticket"]
        .to_str()
        .unwrap()
        .to_string();
    let (status, t) = ask(&ticket, "bulk-token").await;
    assert_eq!((status, t["status"].as_str()), (200, Some("held")), "{t}");
    let text = res.text().await.unwrap();
    assert!(text.starts_with(": gateway held ahead="), "{text}");
    assert!(text.contains(&format!("ticket={ticket}")), "{text}");
    assert!(text.ends_with("data: [DONE]\n\n"), "{text}");
    // Tickets end with their requests.
    assert_eq!(plain.await.unwrap().unwrap().status(), 200);
    assert_eq!(ask("job-7", "chat-token").await.0, 404);
    assert_eq!(ask(&ticket, "bulk-token").await.0, 404);
    // The header turns the upstream's setting off for one call.
    let _busy = spawn_calls(&url, "chat-token", 2).await;
    let res = call(&url, "chat-token")
        .header("x-gateway-queue-notice", "off")
        .send()
        .await
        .unwrap();
    assert!(!res.headers().contains_key("x-gateway-queued"));
}
