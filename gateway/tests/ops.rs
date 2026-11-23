//! Keeping the gateway running day to day: server health probes, agents
//! that survive a restart, reports written by the gateway itself, and
//! timings reported by callers.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::{Body, Bytes};
use axum::response::Response;
use axum::routing::{get, post};
use axum::{Json, Router};
use pybox_gateway::config::Config;
use pybox_gateway::obs::store::{Backend, Memory};
use pybox_gateway::obs::Obs;
use pybox_gateway::App;
use serde_json::{json, Value};

async fn chat(body: Bytes) -> Response {
    let v: Value = serde_json::from_slice(&body).unwrap();
    let usage = json!({"prompt_tokens": 12, "completion_tokens": 3});
    if v["stream"] == true {
        let sse = format!(
            "data: {}\n\ndata: {}\n\ndata: [DONE]\n\n",
            json!({"choices": [{"delta": {"content": "hi"}}]}),
            json!({"choices": [], "usage": usage}),
        );
        return Response::builder()
            .header("content-type", "text/event-stream")
            .body(Body::from(sse))
            .unwrap();
    }
    let out = json!({"choices": [{"message": {"content": "hi"}}], "usage": usage});
    Response::builder()
        .header("content-type", "application/json")
        .body(Body::from(out.to_string()))
        .unwrap()
}

fn model_server() -> Router {
    Router::new()
        .route("/v1/chat/completions", post(chat))
        .route(
            "/v1/models",
            get(|| async { Json(json!({"data": [{"id": "tiny"}]})) }),
        )
}

async fn serve_on(listener: tokio::net::TcpListener, router: Router) {
    tokio::spawn(async move {
        axum::serve(
            listener,
            router.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap()
    });
}

async fn serve(router: Router) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    serve_on(listener, router).await;
    addr
}

/// A port nothing listens on (until the test binds it again).
async fn free_port() -> SocketAddr {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    l.local_addr().unwrap()
}

fn admin() -> reqwest::Client {
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert("authorization", "Bearer admin-secret".parse().unwrap());
    reqwest::Client::builder()
        .default_headers(headers)
        .build()
        .unwrap()
}

async fn start(cfg: Config, store: Option<Arc<dyn Backend>>) -> (String, Arc<App>) {
    let app = match store {
        Some(b) => {
            let obs = Obs::with_backend(cfg.obs.clone(), b).unwrap();
            App::with_obs(cfg, Some(obs)).unwrap()
        }
        None => App::new(cfg).unwrap(),
    };
    let addr = serve(app.router()).await;
    (format!("http://{addr}"), app)
}

async fn ask(base: &str, upstream: &str, token: &str, stream: bool) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("{base}/{upstream}/v1/chat/completions"))
        .bearer_auth(token)
        .json(&json!({"model": "tiny", "stream": stream, "messages": [{"role": "user", "content": "hi"}]}))
        .send()
        .await
        .unwrap()
}

async fn servers(base: &str) -> Value {
    admin()
        .get(format!("{base}/gateway/servers"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

/// Poll until `f` holds, at most `secs`.
async fn until(secs: u64, base: &str, f: impl Fn(&Value) -> bool) -> Value {
    let end = Instant::now() + Duration::from_secs(secs);
    loop {
        let v = servers(base).await;
        if f(&v) {
            return v;
        }
        assert!(Instant::now() < end, "never held: {v}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn health_of(v: &Value, upstream: &str, server: &str) -> String {
    v["upstreams"][upstream]["servers"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["name"] == server)
        .unwrap()["health"]
        .as_str()
        .unwrap()
        .to_string()
}

#[tokio::test]
async fn probes_find_a_dead_server_and_bring_it_back() {
    let live = serve(model_server()).await;
    let dead = free_port().await;
    let cfg: Config = toml::from_str(&format!(
        r#"
admin_token = "admin-secret"
[retry]
max_retries = 0
[[upstreams]]
name = "pool"
base_url = "http://{live}"
kind = "none"
max_concurrency = 4
policy = "fair"
health_probe_secs = 1
refuse_when_down = true
[[upstreams.backends]]
name = "a"
base_url = "http://{live}"
max_concurrency = 2
[[upstreams.backends]]
name = "b"
base_url = "http://{dead}"
max_concurrency = 2
[[upstreams]]
name = "alone"
base_url = "http://{dead}"
kind = "none"
max_concurrency = 2
health_probe_secs = 1
refuse_when_down = true
[[agents]]
id = "a1"
token = "token-a1"
"#
    ))
    .unwrap();
    let (base, _app) = start(cfg, None).await;

    // No request has been sent: the probe alone finds the dead server.
    let v = until(5, &base, |v| {
        health_of(v, "pool", "b") == "down" && health_of(v, "alone", "alone") == "down"
    })
    .await;
    assert_eq!(health_of(&v, "pool", "a"), "up");
    assert_eq!(v["servers_down"], 2);
    let ev = v["events"].as_array().unwrap();
    assert!(
        ev.iter()
            .any(|e| e["server"] == "b" && e["state"] == "down" && e["upstream"] == "pool"),
        "{ev:?}"
    );

    // Every request goes to the live server; none is spent on the dead one.
    for _ in 0..6 {
        let res = ask(&base, "pool", "token-a1", false).await;
        assert_eq!(res.status(), 200);
        assert_eq!(res.headers()["x-gateway-backend"], "a");
        assert_eq!(res.headers()["x-gateway-attempts"], "1");
    }

    // An upstream whose only server is out refuses at the door.
    let t = Instant::now();
    let res = ask(&base, "alone", "token-a1", false).await;
    assert_eq!(res.status(), 503);
    assert_eq!(res.headers()["x-gateway-reject-reason"], "no_server");
    assert!(res.headers().contains_key("retry-after"));
    assert!(t.elapsed() < Duration::from_secs(1), "{:?}", t.elapsed());
    assert!(res.text().await.unwrap().contains("every server"));

    // The server comes up: the next probe puts it back, no request needed.
    let listener = tokio::net::TcpListener::bind(dead).await.unwrap();
    serve_on(listener, model_server()).await;
    let v = until(6, &base, |v| {
        health_of(v, "pool", "b") != "down" && health_of(v, "alone", "alone") != "down"
    })
    .await;
    assert_eq!(v["servers_down"], 0);
    let ev = v["events"].as_array().unwrap();
    let up = ev
        .iter()
        .find(|e| e["server"] == "b" && e["state"] == "up")
        .unwrap_or_else(|| panic!("{ev:?}"));
    assert!(up["down_ms"].as_i64().unwrap() > 0);
    let res = ask(&base, "alone", "token-a1", false).await;
    assert_eq!(res.status(), 200);
    // One good request and it counts as healthy again.
    assert_eq!(health_of(&servers(&base).await, "alone", "alone"), "up");
}

#[tokio::test]
async fn without_refuse_when_down_requests_still_wait() {
    let dead = free_port().await;
    let cfg: Config = toml::from_str(&format!(
        r#"
admin_token = "admin-secret"
[retry]
max_retries = 0
[[upstreams]]
name = "alone"
base_url = "http://{dead}"
kind = "none"
max_concurrency = 2
health_probe_secs = 1
queue_timeout_secs = 1
[[agents]]
id = "a1"
token = "token-a1"
"#
    ))
    .unwrap();
    let (base, _app) = start(cfg, None).await;
    until(5, &base, |v| health_of(v, "alone", "alone") == "down").await;
    let t = Instant::now();
    let res = ask(&base, "alone", "token-a1", false).await;
    assert_eq!(res.status(), 503);
    assert_eq!(res.headers()["x-gateway-reject-reason"], "no_server");
    assert!(
        t.elapsed() >= Duration::from_millis(900),
        "{:?}",
        t.elapsed()
    );
}

fn agents_cfg(upstream: SocketAddr) -> Config {
    toml::from_str(&format!(
        r#"
admin_token = "admin-secret"
[[upstreams]]
name = "mock"
base_url = "http://{upstream}"
kind = "none"
max_concurrency = 4
[[agents]]
id = "keep"
token = "token-keep"
team_id = "old-team"
[[agents]]
id = "drop"
token = "token-drop"
[[agents]]
id = "turn"
token = "token-turn"
"#
    ))
    .unwrap()
}

#[tokio::test]
async fn agents_changed_at_runtime_survive_a_restart() {
    let upstream = serve(model_server()).await;
    let store: Arc<dyn Backend> = Arc::new(Memory::new(1000));
    let (base, app) = start(agents_cfg(upstream), Some(store.clone())).await;
    let c = admin();

    let made: Value = c
        .post(format!("{base}/gateway/agents"))
        .json(&json!({"id": "fresh", "team_id": "t9", "weight": 3}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let fresh = made["token"].as_str().unwrap().to_string();
    let res = c
        .patch(format!("{base}/gateway/agents/keep"))
        .json(&json!({"team_id": "new-team"}))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    let res = c
        .delete(format!("{base}/gateway/agents/drop"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    let turned: Value = c
        .post(format!("{base}/gateway/agents/turn/rotate"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let turned = turned["token"].as_str().unwrap().to_string();

    // Tokens are not in the store, only their hashes.
    let stored = format!(
        "{:?}{:?}",
        store.meta_list("agent").unwrap(),
        store.meta_list("agent_revoked").unwrap()
    );
    for secret in [
        fresh.as_str(),
        turned.as_str(),
        "token-drop",
        "token-turn",
        "token-keep",
    ] {
        assert!(!stored.contains(secret), "{secret} in {stored}");
    }
    drop(app);

    // The same config and the same store, as after a restart.
    let (base, _app) = start(agents_cfg(upstream), Some(store.clone())).await;
    for (token, want) in [
        (fresh.as_str(), 200),
        (turned.as_str(), 200),
        ("token-keep", 200),
        ("token-drop", 401),
        ("token-turn", 401),
    ] {
        let res = ask(&base, "mock", token, false).await;
        assert_eq!(res.status(), want, "{token}");
    }
    // A stored name is not a token.
    let name = store.meta_list("agent").unwrap()[0].0.clone();
    assert!(name.starts_with("sha256:"));
    assert_eq!(ask(&base, "mock", &name, false).await.status(), 401);

    let list: Value = c
        .get(format!("{base}/gateway/agents"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let by_id = |id: &str| {
        list["agents"]
            .as_array()
            .unwrap()
            .iter()
            .find(|a| a["id"] == id)
            .cloned()
    };
    assert_eq!(by_id("keep").unwrap()["team_id"], "new-team");
    assert_eq!(by_id("fresh").unwrap()["weight"], 3);
    assert_eq!(by_id("fresh").unwrap()["team_id"], "t9");
    assert!(by_id("drop").is_none());
    assert!(by_id("turn").is_some());

    // Removing a runtime agent removes its stored entry too.
    c.delete(format!("{base}/gateway/agents/fresh"))
        .send()
        .await
        .unwrap();
    let (base, _app) = start(agents_cfg(upstream), Some(store)).await;
    assert_eq!(ask(&base, "mock", &fresh, false).await.status(), 401);
}

#[tokio::test]
async fn the_gateway_writes_its_own_report() {
    let upstream = serve(model_server()).await;
    let dir = std::env::temp_dir().join(format!("gw-reports-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let cfg: Config = toml::from_str(&format!(
        r#"
admin_token = "admin-secret"
[[upstreams]]
name = "mock"
base_url = "http://{upstream}"
kind = "none"
max_concurrency = 4
[[agents]]
id = "a1"
token = "token-a1"
team_id = "team-a"
user_id = "ann"
[reports]
dir = {dir:?}
at = "00:00"
tz_hours = 0.0
"#,
        dir = dir.display().to_string()
    ))
    .unwrap();
    let (base, app) = start(cfg, None).await;
    for _ in 0..5 {
        assert_eq!(ask(&base, "mock", "token-a1", true).await.status(), 200);
    }
    app.obs.as_ref().unwrap().flush().await;
    let today =
        pybox_gateway::reports::day_name(pybox_gateway::obs::now_ms().div_euclid(86_400_000));
    let c = admin();
    let made: Value = c
        .post(format!("{base}/gateway/reports/run?day={today}"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(made["file"], format!("roi-day-{today}.html"));
    assert_eq!(made["requests"], 5, "{made}");

    let page = std::fs::read_to_string(dir.join(format!("roi-day-{today}.html"))).unwrap();
    assert!(page.contains("\"first_day\""), "report not embedded");
    assert!(!page.contains("/*__INSIGHT__*/null"));

    let list: Value = c
        .get(format!("{base}/gateway/reports"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(list["reports"][0]["file"], format!("roi-day-{today}.html"));

    // A browser opens it with the token in the address; nothing without.
    let anon = reqwest::Client::new();
    let url = format!("{base}/gateway/reports/roi-day-{today}.html");
    assert_eq!(anon.get(&url).send().await.unwrap().status(), 401);
    let res = anon
        .get(format!("{url}?token=admin-secret"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    assert!(res.headers()["content-type"]
        .to_str()
        .unwrap()
        .starts_with("text/html"));
    for bad in ["gateway.toml", "roi-day-2026-13-01.html", "..%2Fx.html"] {
        let res = c
            .get(format!("{base}/gateway/reports/{bad}"))
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 404, "{bad}");
    }
    // The schedule itself: yesterday's report is due and gets written.
    let yesterday =
        pybox_gateway::reports::day_name(pybox_gateway::obs::now_ms().div_euclid(86_400_000) - 1);
    let end = Instant::now() + Duration::from_secs(5);
    while !dir.join(format!("roi-day-{yesterday}.html")).exists() {
        assert!(Instant::now() < end, "scheduled report not written");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn callers_report_what_they_measured() {
    let upstream = serve(model_server()).await;
    let cfg: Config = toml::from_str(&format!(
        r#"
admin_token = "admin-secret"
[[upstreams]]
name = "mock"
base_url = "http://{upstream}"
kind = "none"
max_concurrency = 4
[[agents]]
id = "a1"
token = "token-a1"
[[agents]]
id = "a2"
token = "token-a2"
"#
    ))
    .unwrap();
    let (base, app) = start(cfg, None).await;
    let from = pybox_gateway::obs::now_ms() - 1000;
    let res = ask(&base, "mock", "token-a1", true).await;
    let trace = res.headers()["x-gateway-trace-id"]
        .to_str()
        .unwrap()
        .to_string();
    res.bytes().await.unwrap();
    app.obs.as_ref().unwrap().flush().await;

    let anon = reqwest::Client::new();
    let url = format!("{base}/gateway/obs/client");
    let body = json!({"trace_id": trace, "ttft_ms": 5000.0, "total_ms": 6000.0, "connect_ms": 2.5});
    assert_eq!(
        anon.post(&url).json(&body).send().await.unwrap().status(),
        401
    );
    // Another agent cannot report on this request.
    let v: Value = anon
        .post(&url)
        .bearer_auth("token-a2")
        .json(&body)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(v, json!({"stored": 0, "unmatched": 0, "refused": 1}));
    let v: Value = anon
        .post(&url)
        .bearer_auth("token-a1")
        .json(
            &json!({"items": [body, {"trace_id": "unknown", "total_ms": 10.0}, {"trace_id": "x"}]}),
        )
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(v, json!({"stored": 2, "unmatched": 1, "refused": 1}));
    app.obs.as_ref().unwrap().flush().await;

    let to = pybox_gateway::obs::now_ms() + 1000;
    let s: Value = admin()
        .get(format!("{url}?from={from}&to={to}"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(s["all"]["reports"], 2, "{s}");
    let m = &s["models"][0];
    assert_eq!(m["model"], "tiny");
    assert_eq!(m["client_ttft_p50_ms"], 5000.0);
    // The gateway saw the first token in milliseconds; the rest is the way back.
    let net = m["net_ttft_p50_ms"].as_f64().unwrap();
    assert!((4000.0..5000.0).contains(&net), "{net}");
    // The report sits in the request's own trace.
    let t: Value = admin()
        .get(format!("{base}/gateway/obs/trace/{trace}"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(t.to_string().contains("client.request"), "{t}");

    // The operations page gets all of it in one call.
    let d: Value = admin()
        .get(format!("{base}/gateway/ops/data"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(d["client"]["all"]["reports"], 2);
    assert_eq!(d["servers"]["servers_down"], 0);
    assert!(d["reports"].is_null());
    let page = anon
        .get(format!("{base}/gateway/ops"))
        .send()
        .await
        .unwrap();
    assert!(page.text().await.unwrap().contains("/*__OPS__*/null"));
    assert_eq!(
        anon.get(format!("{base}/gateway/ops/data"))
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
}
