//! The management console's admin API: key edits, model listing, live
//! samples, and a job run end to end against a mock upstream.

use std::net::SocketAddr;
use std::time::Duration;

use axum::body::{Body, Bytes};
use axum::response::Response;
use axum::routing::{get, post};
use axum::{Json, Router};
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

async fn chat(body: Bytes) -> Response {
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["stream"], true);
    let usage = json!({"prompt_tokens": 12, "completion_tokens": 3});
    let sse = format!(
        "data: {}\n\ndata: {}\n\ndata: [DONE]\n\n",
        json!({"choices": [{"delta": {"content": "hi"}}]}),
        json!({"choices": [], "usage": usage}),
    );
    Response::builder()
        .header("content-type", "text/event-stream")
        .body(Body::from(sse))
        .unwrap()
}

/// A gateway on a real port (jobs call it over loopback) in front of a mock.
async fn gateway() -> (String, reqwest::Client) {
    let upstream = serve(
        Router::new()
            .route("/v1/chat/completions", post(chat))
            .route(
                "/v1/models",
                get(|| async { Json(json!({"data": [{"id": "tiny", "owned_by": "mock"}]})) }),
            ),
    )
    .await;
    // Reserve a port, then hand it to the gateway through its config.
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = probe.local_addr().unwrap();
    drop(probe);
    let cfg: Config = toml::from_str(&format!(
        r#"
listen = "{addr}"
admin_token = "admin-secret"
[[upstreams]]
name = "mock"
base_url = "http://{upstream}"
kind = "litellm"
api_key = "sk-up"
max_concurrency = 4
[[agents]]
id = "a1"
token = "agent-token-1"
team_id = "old-team"
"#
    ))
    .unwrap();
    let app = App::new(cfg).unwrap();
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    let router = app.router();
    tokio::spawn(async move {
        axum::serve(
            listener,
            router.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap()
    });
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert("authorization", "Bearer admin-secret".parse().unwrap());
    let client = reqwest::Client::builder()
        .default_headers(headers)
        .build()
        .unwrap();
    (format!("http://{addr}"), client)
}

#[tokio::test]
async fn admin_token_is_required() {
    let (base, _) = gateway().await;
    let anon = reqwest::Client::new();
    for path in [
        "/gateway/upstreams",
        "/gateway/models",
        "/gateway/live",
        "/gateway/jobs",
    ] {
        let res = anon.get(format!("{base}{path}")).send().await.unwrap();
        assert_eq!(res.status(), 401, "{path}");
    }
    let res = anon
        .post(format!("{base}/gateway/agents/a1/rotate"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 401);
    // The page itself carries no data and asks for the token in the browser.
    let page = anon
        .get(format!("{base}/gateway/console"))
        .send()
        .await
        .unwrap();
    assert_eq!(page.status(), 200);
}

#[tokio::test]
async fn keys_can_be_edited_and_rotated() {
    let (base, admin) = gateway().await;
    let r: Value = admin
        .patch(format!("{base}/gateway/agents/a1"))
        .json(&json!({"weight": 3, "token_budget": 1000, "team_id": "new-team", "max_concurrency": 2}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(r["agent"]["weight"], 3);
    assert_eq!(r["agent"]["token_budget"], 1000);
    assert_eq!(r["agent"]["team_id"], "new-team");
    // null clears; absent fields keep their value.
    let r: Value = admin
        .patch(format!("{base}/gateway/agents/a1"))
        .json(&json!({"token_budget": null}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(r["agent"]["token_budget"], Value::Null);
    assert_eq!(r["agent"]["weight"], 3);
    assert_eq!(r["agent"]["max_concurrency"], 2);
    let res = admin
        .patch(format!("{base}/gateway/agents/nobody"))
        .json(&json!({"weight": 2}))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 404);

    let r: Value = admin
        .post(format!("{base}/gateway/agents/a1/rotate"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let new_token = r["token"].as_str().unwrap().to_string();
    assert_eq!(r["agent"]["team_id"], "new-team");
    let call = |token: String| {
        let base = base.clone();
        async move {
            reqwest::Client::new()
                .post(format!("{base}/mock/v1/chat/completions"))
                .bearer_auth(token)
                .json(&json!({"model": "tiny", "stream": true, "messages": []}))
                .send()
                .await
                .unwrap()
                .status()
        }
    };
    assert_eq!(call("agent-token-1".into()).await, 401);
    assert_eq!(call(new_token).await, 200);
}

#[tokio::test]
async fn models_upstreams_and_live() {
    let (base, admin) = gateway().await;
    let get = |path: &'static str| {
        let (admin, base) = (admin.clone(), base.clone());
        async move {
            admin
                .get(format!("{base}{path}"))
                .send()
                .await
                .unwrap()
                .json::<Value>()
                .await
                .unwrap()
        }
    };
    let m = get("/gateway/models").await;
    assert_eq!(m["models"][0]["id"], "tiny");
    assert_eq!(m["models"][0]["upstream"], "mock");
    let u = get("/gateway/upstreams").await;
    assert_eq!(u["upstreams"][0]["max_concurrency"], 4);
    assert_eq!(u["upstreams"][0]["has_key"], true);
    assert!(!u.to_string().contains("sk-up"), "keys never leave");
    get("/gateway/live").await;
    tokio::time::sleep(Duration::from_millis(1200)).await;
    let l = get("/gateway/live").await;
    assert!(!l["samples"].as_array().unwrap().is_empty());
    assert_eq!(l["now"]["upstreams"]["mock"]["max_concurrency"], 4);
}

#[tokio::test]
async fn job_runs_through_the_gateway_and_is_logged() {
    let (base, admin) = gateway().await;
    let bad = admin
        .post(format!("{base}/gateway/jobs"))
        .json(&json!({"upstream": "nope", "model": "tiny", "prompts": ["x"], "count": 1, "concurrency": 1}))
        .send()
        .await
        .unwrap();
    assert_eq!(bad.status(), 400);
    let r: Value = admin
        .post(format!("{base}/gateway/jobs"))
        .json(
            &json!({"name": "nightly", "upstream": "mock", "model": "tiny", "prompts": ["a", "b"],
            "count": 9, "concurrency": 3, "project_id": "p1", "team_id": "t1"}),
        )
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let id = r["job"]["id"].as_str().unwrap().to_string();
    let mut job = Value::Null;
    for _ in 0..100 {
        let list: Value = admin
            .get(format!("{base}/gateway/jobs"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        job = list["jobs"][0].clone();
        if job["state"] == "done" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(job["state"], "done", "{job}");
    assert_eq!(job["ok"], 9);
    assert_eq!(job["failed"], 0);

    // Its requests carry the job id as run id and the job's attribution.
    tokio::time::sleep(Duration::from_millis(700)).await;
    let q: Value = admin
        .post(format!("{base}/gateway/obs/query"))
        .json(&json!({"from": 0, "to": 9_999_999_999_999i64, "filter": {"run_id": [id]},
            "group_by": ["agent_id", "project_id", "team_id"], "metrics": ["count", "sum:output_tokens"]}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(q["rows"][0]["count"], 9, "{q}");
    assert_eq!(q["rows"][0]["agent_id"], "job:nightly");
    assert_eq!(q["rows"][0]["project_id"], "p1");
    assert_eq!(q["rows"][0]["sum:output_tokens"], 27.0);

    // The job's key is gone once it finishes.
    let agents: Value = admin
        .get(format!("{base}/gateway/agents"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(!agents.to_string().contains("job:nightly"));
}
