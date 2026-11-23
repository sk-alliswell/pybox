//! Live smoke test against a real provider API through the gateway.
//!
//!   ANTHROPIC_API_KEY=... cargo run --release --example live -- [agents] [model]
//!   GEMINI_API_KEY=...    cargo run --release --example live -- 10 gemini-2.5-flash-lite
//!
//! Models starting with `gemini` go through an `openai`-kind upstream pointed
//! at Gemini's OpenAI-compatible endpoint; anything else uses Anthropic.
//! `LIVE_BASE_URL` overrides the upstream base URL and `LIVE_KEY_ENV` the
//! variable holding the key (`GATEWAY_ANTHROPIC_KEY` is picked up
//! automatically for Anthropic when set).
//!
//! Starts the gateway in-process, registers N agents, has each send one
//! streaming and one non-streaming request with a tiny max_tokens, then
//! prints per-request timings and the gateway's stats. Costs a few cents at most.

use std::time::Instant;

use futures_util::StreamExt;
use pybox_gateway::config::Config;
use pybox_gateway::App;
use serde_json::{json, Value};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let agents: usize = args.next().map(|a| a.parse()).transpose()?.unwrap_or(10);
    let model = args.next().unwrap_or_else(|| "claude-haiku-5-5".into());
    let gemini = model.starts_with("gemini");
    let (name, kind, key_env, default_base, path) = if gemini {
        (
            "gemini",
            "openai",
            "GEMINI_API_KEY",
            "https://generativelanguage.googleapis.com/v1beta/openai",
            "/chat/completions",
        )
    } else {
        (
            "anthropic",
            "anthropic",
            "ANTHROPIC_API_KEY",
            "https://api.anthropic.com",
            "/v1/messages",
        )
    };
    // Claude Code environments reserve ANTHROPIC_API_KEY, so allow another name.
    let key_env = std::env::var("LIVE_KEY_ENV").unwrap_or_else(|_| {
        if !gemini && std::env::var_os("GATEWAY_ANTHROPIC_KEY").is_some() {
            "GATEWAY_ANTHROPIC_KEY".into()
        } else {
            key_env.into()
        }
    });
    let base = std::env::var("LIVE_BASE_URL").unwrap_or_else(|_| default_base.into());

    let cfg: Config = toml::from_str(&format!(
        r#"
admin_token = "live-admin"
[[upstreams]]
name = "{name}"
base_url = "{base}"
kind = "{kind}"
api_key_env = "{key_env}"
max_concurrency = 4
rpm = 50
"#
    ))?;
    let app = App::new(cfg)?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let gw = format!("http://{}", listener.local_addr()?);
    let router = app.router();
    tokio::spawn(async move { axum::serve(listener, router).await });

    let http = reqwest::Client::new();
    let mut tokens = Vec::new();
    for i in 0..agents {
        let v: Value = http
            .post(format!("{gw}/gateway/agents"))
            .bearer_auth("live-admin")
            .json(&json!({"id": format!("agent-{i}")}))
            .send()
            .await?
            .json()
            .await?;
        tokens.push(v["token"].as_str().unwrap().to_string());
    }

    let t0 = Instant::now();
    let mut handles = Vec::new();
    for (i, token) in tokens.into_iter().enumerate() {
        let (http, gw, model) = (http.clone(), gw.clone(), model.clone());
        handles.push(tokio::spawn(async move {
            let mut out = Vec::new();
            for stream in [true, false] {
                let start = Instant::now();
                let prompt = format!("Reply with the number {i} and nothing else.");
                let mut body = json!({
                    "model": model,
                    "max_tokens": 16,
                    "stream": stream,
                    "messages": [{"role": "user", "content": prompt}]
                });
                if gemini && stream {
                    // OpenAI-style streams only report usage when asked.
                    body["stream_options"] = json!({"include_usage": true});
                }
                let mut req = http.post(format!("{gw}/{name}{path}"));
                req = if gemini {
                    req.bearer_auth(&token)
                } else {
                    req.header("x-api-key", &token)
                        .header("anthropic-version", "2023-06-01")
                };
                let r = req.json(&body).send().await;
                let line = match r {
                    Ok(r) => {
                        let status = r.status();
                        let mut first = None;
                        let mut bytes = 0usize;
                        let mut body = Vec::new();
                        let mut s = r.bytes_stream();
                        while let Some(chunk) = s.next().await {
                            let chunk = chunk.map_err(|e| e.to_string());
                            match chunk {
                                Ok(c) => {
                                    first.get_or_insert(start.elapsed());
                                    bytes += c.len();
                                    if body.len() < 600 {
                                        body.extend_from_slice(&c);
                                    }
                                }
                                Err(e) => return vec![format!("agent-{i} stream error: {e}")],
                            }
                        }
                        let snippet = if status.is_success() { String::new() } else {
                            format!(" body={}", String::from_utf8_lossy(&body))
                        };
                        format!(
                            "agent-{i:<2} {:<6} {status} first_byte={:>5}ms total={:>5}ms bytes={bytes}{snippet}",
                            if stream { "stream" } else { "json" },
                            first.unwrap_or_default().as_millis(),
                            start.elapsed().as_millis(),
                        )
                    }
                    Err(e) => format!("agent-{i} request error: {e}"),
                };
                out.push(line);
            }
            out
        }));
    }
    for h in handles {
        for line in h.await? {
            println!("{line}");
        }
    }
    println!("wall time: {}ms", t0.elapsed().as_millis());
    let stats: Value = http
        .get(format!("{gw}/gateway/stats"))
        .bearer_auth("live-admin")
        .send()
        .await?
        .json()
        .await?;
    println!("{}", serde_json::to_string_pretty(&stats)?);
    Ok(())
}
