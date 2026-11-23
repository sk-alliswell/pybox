//! The lead areas on the live request path, against mock model servers.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::State;
use axum::routing::post;
use axum::{Json, Router};
use pybox_gateway::config::Config;
use pybox_gateway::App;
use serde_json::{json, Value};

#[derive(Default)]
struct Server {
    bodies: Mutex<Vec<String>>,
    delay_ms: u64,
}

/// Anthropic-format server that reports cache reads the way the provider
/// would: whatever was marked before is read back.
async fn messages(State(s): State<Arc<Server>>, body: Bytes) -> Json<Value> {
    let text = String::from_utf8(body.to_vec()).unwrap();
    let marked = text.contains("cache_control");
    let n = {
        let mut b = s.bodies.lock().unwrap();
        b.push(text.clone());
        b.len()
    };
    tokio::time::sleep(Duration::from_millis(s.delay_ms)).await;
    let total = (text.len() / 4) as u64;
    let read = if marked && n > 1 { total * 8 / 10 } else { 0 };
    Json(json!({
        "content": [{"type": "text", "text": "ok"}],
        "usage": {"input_tokens": total - read, "output_tokens": 20,
                   "cache_read_input_tokens": read, "cache_creation_input_tokens": 0}
    }))
}

/// OpenAI-format server that reports no cached tokens, as local model
/// servers do.
async fn chat(State(s): State<Arc<Server>>, body: Bytes) -> Json<Value> {
    let text = String::from_utf8(body.to_vec()).unwrap();
    let total = (text.len() / 4) as u64;
    s.bodies.lock().unwrap().push(text);
    Json(json!({
        "choices": [{"index": 0, "message": {"role": "assistant", "content": "ok"}, "finish_reason": "stop"}],
        "usage": {"prompt_tokens": total, "completion_tokens": 20, "total_tokens": total + 20}
    }))
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

async fn model_server(delay_ms: u64) -> (Arc<Server>, SocketAddr) {
    let s = Arc::new(Server {
        delay_ms,
        ..Server::default()
    });
    let addr = serve(
        Router::new()
            .route("/v1/messages", post(messages))
            .route("/v1/chat/completions", post(chat))
            .with_state(s.clone()),
    )
    .await;
    (s, addr)
}

fn conversation(turns: usize) -> Value {
    let filler = "word ".repeat(1200);
    let mut messages = vec![
        json!({"role": "user", "content": [{"type": "text", "text": "fix the failing test"}]}),
    ];
    for i in 0..turns {
        messages.push(json!({"role": "assistant", "content": [{"type": "text", "text": format!("step {i}")}]}));
        messages.push(json!({"role": "user", "content": [{"type": "text", "text": format!("result {i} {filler}")}]}));
    }
    json!({
        "model": "claude-test", "max_tokens": 100,
        "system": [{"type": "text", "text": format!("You are a coding agent. {filler}")}],
        "messages": messages,
    })
}

struct Env {
    gw: String,
    app: Arc<App>,
    http: reqwest::Client,
}

impl Env {
    async fn new(toml: String) -> Self {
        let cfg: Config = toml::from_str(&toml).unwrap();
        let app = App::new(cfg).unwrap();
        let gw = serve(app.router()).await;
        Self {
            gw: format!("http://{gw}"),
            app,
            http: reqwest::Client::new(),
        }
    }
    async fn send(&self, token: &str, body: &Value, headers: &[(&str, &str)]) -> reqwest::Response {
        let mut req = self
            .http
            .post(format!("{}/llm/v1/messages", self.gw))
            .header("x-api-key", token)
            .json(body);
        for (k, v) in headers {
            req = req.header(*k, *v);
        }
        let resp = req.send().await.unwrap();
        assert!(resp.status().is_success(), "{}", resp.status());
        resp
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
        resp.json().await.unwrap()
    }
    async fn rows(&self) -> Vec<Value> {
        self.app.obs.as_ref().unwrap().flush().await;
        let v = self
            .admin(
                reqwest::Method::POST,
                "/gateway/obs/rows",
                Some(json!({"from": 0, "to": 9_000_000_000_000i64, "limit": 100})),
            )
            .await;
        let mut rows = v["rows"].as_array().unwrap().clone();
        rows.sort_by_key(|r| r["run_turn"].as_u64());
        rows
    }
}

const AGENTS: &str = r#"
[[agents]]
id = "coder"
token = "agent-one-token"
project_id = "apollo"
team_id = "growth"
"#;

#[tokio::test]
async fn cache_points_runs_and_the_ledger_work_end_to_end() {
    let (server, addr) = model_server(0).await;
    let e = Env::new(format!(
        r#"
admin_token = "admin-secret"

[lead]
cache_points = "on"

[obs.prices."claude-test"]
input = 3.0
output = 15.0
cache_read = 0.3
cache_write = 3.75

[[upstreams]]
name = "llm"
base_url = "http://{addr}"
kind = "anthropic"
api_key = "sk-real"
{AGENTS}"#
    ))
    .await;
    for turns in 0..3 {
        e.send("agent-one-token", &conversation(turns), &[]).await;
    }
    // The client's bytes arrive unchanged apart from the markers.
    let bodies = server.bodies.lock().unwrap().clone();
    for (turns, sent) in bodies.iter().enumerate() {
        let mut v: Value = serde_json::from_str(sent).unwrap();
        assert_eq!(
            v["system"][0]["cache_control"]["type"], "ephemeral",
            "turn {turns}"
        );
        v["system"][0]
            .as_object_mut()
            .unwrap()
            .remove("cache_control");
        if turns > 0 {
            assert_eq!(v["cache_control"]["type"], "ephemeral");
        }
        v.as_object_mut().unwrap().remove("cache_control");
        assert_eq!(v, conversation(turns));
    }

    let rows = e.rows().await;
    assert_eq!(rows.len(), 3);
    let run = rows[0]["run_id"].as_str().unwrap();
    assert!(run.starts_with("inf-"), "{run}");
    for (i, r) in rows.iter().enumerate() {
        assert_eq!(r["run_id"], run, "one run without any header");
        assert_eq!(
            (r["run_inferred"].as_bool(), r["run_turn"].as_u64()),
            (Some(true), Some(i as u64 + 1))
        );
        assert_eq!(r["lead_applied"], true);
    }
    assert_eq!(
        (
            rows[0]["cache_cause"].as_str(),
            rows[2]["cache_cause"].as_str()
        ),
        (Some("first"), Some("ok"))
    );
    assert!(rows[2]["gateway_saved_usd"].as_f64().unwrap() > 0.0);
    assert!(
        rows[2]["no_gateway_input_usd"].as_f64().unwrap() > rows[2]["cost_usd"].as_f64().unwrap()
    );

    // Deliverable events turn spend into a cost class and an ROI range.
    let roi = e
        .admin(reqwest::Method::GET, "/gateway/lead/roi", None)
        .await;
    let spent = roi["total"]["cost_usd"].as_f64().unwrap();
    assert!(spent > 0.0);
    assert_eq!(
        roi["total"]["classes"]["unattributed"].as_f64(),
        Some(spent)
    );
    for ev in [
        json!({"type": "deliverable", "id": "pr-7", "project_id": "apollo", "team_id": "growth", "kind": "pr"}),
        json!({"type": "link", "run_id": run, "deliverable_id": "pr-7"}),
        json!({"type": "status", "id": "pr-7", "status": "accepted"}),
        json!({"type": "value", "id": "pr-7", "usd_low": 20.0, "usd_high": 60.0}),
    ] {
        e.admin(reqwest::Method::POST, "/gateway/lead/events", Some(ev))
            .await;
    }
    let bad = e
        .http
        .post(format!("{}/gateway/lead/events", e.gw))
        .bearer_auth("admin-secret")
        .json(&json!({"type": "status", "id": "pr-7", "status": "shipped"}))
        .send()
        .await
        .unwrap();
    assert_eq!(bad.status(), 400);
    let roi = e
        .admin(reqwest::Method::GET, "/gateway/lead/roi", None)
        .await;
    let c = &roi["total"]["classes"];
    assert_eq!(
        (c["deliverable"].as_f64(), c["unattributed"].as_f64()),
        (Some(spent), Some(0.0))
    );
    let d = &roi["deliverables"][0];
    assert_eq!(
        (d["deliverable_id"].as_str(), d["status"].as_str()),
        (Some("pr-7"), Some("accepted"))
    );
    assert!(d["roi_low"].as_f64().unwrap() < d["roi_high"].as_f64().unwrap());
    let stats = e.admin(reqwest::Method::GET, "/gateway/stats", None).await;
    assert_eq!(stats["prompt_cache"]["mode"], "on");
}

#[tokio::test]
async fn shadow_mode_measures_without_touching_the_request() {
    let (server, addr) = model_server(0).await;
    let e = Env::new(format!(
        r#"
admin_token = "admin-secret"

[obs.prices."claude-test"]
input = 3.0
output = 15.0
cache_read = 0.3
cache_write = 3.75

[[upstreams]]
name = "llm"
base_url = "http://{addr}"
kind = "anthropic"
api_key = "sk-real"
{AGENTS}"#
    ))
    .await;
    for turns in 0..3 {
        e.send("agent-one-token", &conversation(turns), &[]).await;
    }
    assert!(server
        .bodies
        .lock()
        .unwrap()
        .iter()
        .all(|b| !b.contains("cache_control")));
    let rows = e.rows().await;
    assert_eq!(rows[2]["cache_cause"], "no_breakpoint");
    assert_eq!(rows[2]["lead_applied"], false);
    assert!(rows[2]["shadow_saving_usd"].as_f64().unwrap() > 0.0);
    assert!(rows[2]["cache_lost_usd"].as_f64().unwrap() > 0.0);
    let breaks = e
        .admin(reqwest::Method::GET, "/gateway/lead/cache", None)
        .await;
    assert_eq!(breaks["breaks"][0]["cause"], "no_breakpoint");
}

#[tokio::test]
async fn requests_go_to_the_idle_server_and_runs_stay_put() {
    let (a, addr_a) = model_server(300).await;
    let (b, addr_b) = model_server(300).await;
    let e = Env::new(format!(
        r#"
admin_token = "admin-secret"

[[upstreams]]
name = "llm"
base_url = "http://{addr_a}"
kind = "anthropic"
api_key = "sk-real"
policy = "fair"

[[upstreams.backends]]
name = "a"
base_url = "http://{addr_a}"
max_concurrency = 2

[[upstreams.backends]]
name = "b"
base_url = "http://{addr_b}"
max_concurrency = 2
{AGENTS}"#
    ))
    .await;
    // Four requests at once over two servers with two seats each: nobody
    // waits behind a busy server while the other has room.
    let started = std::time::Instant::now();
    let e = Arc::new(e);
    let mut tasks = Vec::new();
    for i in 0..4 {
        let e = e.clone();
        tasks.push(tokio::spawn(async move {
            let run = format!("run-{i}");
            e.send(
                "agent-one-token",
                &conversation(0),
                &[("x-gateway-run-id", &run)],
            )
            .await;
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    assert!(
        started.elapsed() < Duration::from_millis(550),
        "{:?}",
        started.elapsed()
    );
    let (na, nb) = (
        a.bodies.lock().unwrap().len(),
        b.bodies.lock().unwrap().len(),
    );
    assert_eq!((na, nb), (2, 2));
    // The next turn of a run returns to the server that served it.
    let rows = e.rows().await;
    let home = rows.iter().find(|r| r["run_id"] == "run-2").unwrap()["backend"]
        .as_str()
        .unwrap()
        .to_string();
    e.send(
        "agent-one-token",
        &conversation(1),
        &[("x-gateway-run-id", "run-2")],
    )
    .await;
    let rows = e.rows().await;
    let again: Vec<&str> = rows
        .iter()
        .filter(|r| r["run_id"] == "run-2")
        .map(|r| r["backend"].as_str().unwrap())
        .collect();
    assert_eq!(again, vec![home.as_str(), home.as_str()]);
    let stats = e.admin(reqwest::Method::GET, "/gateway/stats", None).await;
    let sched = &stats["upstreams"]["llm"]["scheduler"];
    assert_eq!(sched["policy"], "fair");
    assert_eq!(sched["backends"].as_array().unwrap().len(), 2);
}

fn tool_run(results: usize) -> Value {
    let big = "line of tool output ".repeat(150);
    let mut messages =
        vec![json!({"role": "user", "content": [{"type": "text", "text": "find the bug"}]})];
    for i in 0..results {
        messages.push(json!({"role": "assistant", "content": [
            {"type": "tool_use", "id": format!("tu_{i}"), "name": "read_file", "input": {"n": i}}]}));
        messages.push(json!({"role": "user", "content": [
            {"type": "tool_result", "tool_use_id": format!("tu_{i}"), "content": format!("{i}: {big}")}]}));
    }
    json!({"model": "claude-test", "max_tokens": 100, "system": "You are a coding agent.", "messages": messages})
}

#[tokio::test]
async fn old_tool_results_are_cleared_only_when_asked() {
    let (server, addr) = model_server(0).await;
    let e = Env::new(format!(
        r#"
admin_token = "admin-secret"

[lead]
cache_points = "off"
tool_results = "on"
keep_tool_results = 2

[[upstreams]]
name = "llm"
base_url = "http://{addr}"
kind = "anthropic"
api_key = "sk-real"
{AGENTS}"#
    ))
    .await;
    for n in 1..=6 {
        e.send(
            "agent-one-token",
            &tool_run(n),
            &[("x-gateway-run-id", "r1")],
        )
        .await;
    }
    let bodies = server.bodies.lock().unwrap().clone();
    let last: Value = serde_json::from_str(bodies.last().unwrap()).unwrap();
    let results: Vec<&Value> = last["messages"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|m| m["content"].get(0))
        .filter(|c| c["type"] == "tool_result")
        .collect();
    assert_eq!(results.len(), 6);
    for (i, r) in results.iter().enumerate() {
        let text = r["content"].as_str().unwrap();
        assert_eq!(r["tool_use_id"], format!("tu_{i}"));
        assert_eq!(
            text.starts_with("[tool result cleared by the gateway"),
            i < 4,
            "result {i}"
        );
    }
    assert!(bodies.last().unwrap().len() * 2 < tool_run(6).to_string().len());
    let rows = e.rows().await;
    let row = rows.iter().max_by_key(|r| r["run_turn"].as_u64()).unwrap();
    assert_eq!(row["trim_cleared"], 4);
    assert!(row["gateway_saved_tokens"].as_f64().unwrap() > 1000.0);
}

/// A server that reports no cached tokens gets no credit for a cache the
/// gateway only assumed: clearing starts once that has been seen, and then
/// moves in steps rather than on every turn.
#[tokio::test]
async fn clearing_does_not_wait_for_a_cache_the_upstream_never_shows() {
    let (server, addr) = model_server(0).await;
    let e = Env::new(format!(
        r#"
admin_token = "admin-secret"

[lead]
cache_points = "off"
tool_results = "on"
keep_tool_results = 2

[[upstreams]]
name = "llm"
base_url = "http://{addr}"
kind = "openai"
{AGENTS}"#
    ))
    .await;
    let big = "line of tool output ".repeat(150);
    let mut messages = vec![json!({"role": "user", "content": "find the bug"})];
    for i in 0..12 {
        messages.push(json!({"role": "assistant", "content": "", "tool_calls": [
            {"id": format!("c{i}"), "type": "function", "function": {"name": "read_file", "arguments": "{}"}}]}));
        messages.push(json!({"role": "tool", "tool_call_id": format!("c{i}"), "content": format!("{i}: {big}")}));
        let resp = e
            .http
            .post(format!("{}/llm/v1/chat/completions", e.gw))
            .bearer_auth("agent-one-token")
            .header("x-gateway-run-id", "r1")
            .json(&json!({"model": "local", "messages": messages}))
            .send()
            .await
            .unwrap();
        assert!(resp.status().is_success(), "{}", resp.status());
    }
    let bodies = server.bodies.lock().unwrap().clone();
    let cleared = |b: &String| b.matches("[tool result cleared by the gateway").count();
    // Nothing is cleared before the upstream has shown how it caches.
    assert_eq!(cleared(&bodies[3]), 0);
    let last = cleared(bodies.last().unwrap());
    assert!((8..=10).contains(&last), "{last} cleared on the last turn");
    // The boundary moved in steps: fewer distinct levels than turns.
    let mut levels: Vec<usize> = bodies.iter().map(cleared).collect();
    assert!(levels.windows(2).all(|w| w[0] <= w[1]));
    levels.dedup();
    assert!(levels.len() <= 6, "{levels:?}");
    let v: Value = serde_json::from_str(bodies.last().unwrap()).unwrap();
    assert_eq!(v["messages"].as_array().unwrap().len(), 25);
}

/// A prompt with its own long system text and one question.
fn ask(system: &str, question: &str) -> Value {
    json!({
        "model": "claude-test", "max_tokens": 100,
        "system": [{"type": "text", "text": format!("{system} {}", "word ".repeat(1200))}],
        "messages": [{"role": "user", "content": [{"type": "text", "text": question}]}],
    })
}

async fn two_servers(extra: &str) -> Env {
    let (_, addr_a) = model_server(200).await;
    let (_, addr_b) = model_server(200).await;
    Env::new(format!(
        r#"
admin_token = "admin-secret"

[[upstreams]]
name = "llm"
base_url = "http://{addr_a}"
kind = "anthropic"
api_key = "sk-real"
{extra}

[[upstreams.backends]]
name = "a"
base_url = "http://{addr_a}"
max_concurrency = 2

[[upstreams.backends]]
name = "b"
base_url = "http://{addr_b}"
max_concurrency = 2
{AGENTS}
[[agents]]
id = "reviewer"
token = "agent-two-token"
"#
    ))
    .await
}

/// Which server answered, and what the gateway says it already held.
fn landed(r: &reqwest::Response) -> (String, String) {
    let h = |k: &str| {
        r.headers()
            .get(k)
            .map(|v| v.to_str().unwrap().to_string())
            .unwrap_or_default()
    };
    (h("x-gateway-backend"), h("x-gateway-prefix-warm"))
}

#[tokio::test]
async fn prompts_that_start_the_same_go_to_the_same_server() {
    // No lead engine and the plain round-robin queue: routing by prefix
    // needs neither.
    let e = Arc::new(two_servers("").await);
    // Two different system prompts arrive together and take one server each.
    let first = {
        let e = e.clone();
        tokio::spawn(
            async move { landed(&e.send("agent-one-token", &ask("alpha", "q1"), &[]).await) },
        )
    };
    tokio::time::sleep(Duration::from_millis(50)).await;
    let (b2, warm2) = landed(&e.send("agent-two-token", &ask("beta", "q1"), &[]).await);
    let (b1, warm1) = first.await.unwrap();
    assert_ne!(b1, b2);
    assert!(
        warm1.starts_with("0/") && warm2.starts_with("0/"),
        "{warm1} {warm2}"
    );
    // Later questions under each system prompt, from the other agent, land
    // where that prompt is held, whichever server load alone would pick.
    for _ in 0..2 {
        let (b, warm) = landed(&e.send("agent-two-token", &ask("alpha", "q2"), &[]).await);
        assert_eq!(b, b1);
        assert!(!warm.starts_with("0/"), "{warm}");
        let (b, _) = landed(&e.send("agent-one-token", &ask("beta", "q3"), &[]).await);
        assert_eq!(b, b2);
    }
    let stats = e.admin(reqwest::Method::GET, "/gateway/stats", None).await;
    let sched = &stats["upstreams"]["llm"]["scheduler"];
    assert_eq!(sched["affinity"], "prefix");
    assert_eq!(sched["prefix_routing"]["warm"], 4);
    assert_eq!(sched["prefix_routing"]["cold"], 2);
    let rows = e.rows().await;
    assert!(rows
        .iter()
        .any(|r| r["prefix_warm_tokens"].as_u64().unwrap_or(0) > 1000));

    // Switched off, an idle pair of servers always gives the first one.
    let e = two_servers("affinity = \"off\"").await;
    for (system, token) in [
        ("alpha", "agent-one-token"),
        ("beta", "agent-two-token"),
        ("beta", "agent-one-token"),
    ] {
        let (b, warm) = landed(&e.send(token, &ask(system, "q"), &[]).await);
        assert_eq!((b.as_str(), warm.as_str()), ("a", ""));
    }
}

#[tokio::test]
async fn a_refusal_says_whether_retrying_can_help() {
    let (_, addr) = model_server(0).await;
    let e = Env::new(format!(
        r#"
admin_token = "admin-secret"

[[upstreams]]
name = "llm"
base_url = "http://{addr}"
kind = "anthropic"
api_key = "sk-real"

[[upstreams.backends]]
name = "a"
base_url = "http://{addr}"
models = ["claude-test"]

[[upstreams.backends]]
name = "b"
base_url = "http://{addr}"
models = ["claude-test"]

[[agents]]
id = "coder"
token = "agent-one-token"
token_budget = 1
"#
    ))
    .await;
    let reason = |r: &reqwest::Response| {
        r.headers()
            .get("x-gateway-reject-reason")
            .map(|v| v.to_str().unwrap().to_string())
    };
    // No server lists this model: nothing to wait for.
    let mut other = ask("alpha", "q");
    other["model"] = json!("some-other-model");
    let r = e
        .http
        .post(format!("{}/llm/v1/messages", e.gw))
        .header("x-api-key", "agent-one-token")
        .json(&other)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 503);
    assert_eq!(reason(&r).as_deref(), Some("no_server"));
    // One answered request uses up the budget: the next refusal is the
    // caller's own limit.
    let ok = e.send("agent-one-token", &ask("alpha", "q"), &[]).await;
    assert_eq!(reason(&ok), None);
    let r = e
        .http
        .post(format!("{}/llm/v1/messages", e.gw))
        .header("x-api-key", "agent-one-token")
        .json(&ask("alpha", "q"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 429);
    assert_eq!(reason(&r).as_deref(), Some("caller_limit"));
}
