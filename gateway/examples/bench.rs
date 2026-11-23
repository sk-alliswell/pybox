//! Local serving benchmark: how much CPU and latency the gateway adds per
//! streamed request, with no provider or network in the way.
//!
//!   cargo build --release
//!   cargo run --release --example bench -- target/release/pybox-gateway \
//!       [--format openai|anthropic] [--conc 64] [--requests 4000] \
//!       [--chunks 200] [--gap-ms 0] [--prompt-kb 8] [--obs on|off]
//!
//! Starts a mock model server in this process, launches the given gateway
//! binary as a child, and drives streaming requests first straight at the
//! mock and then through the gateway. Reports throughput, the latency the
//! gateway adds, and the gateway process's CPU time per request (from
//! `/proc/<pid>/stat`, so Linux only). Prints one JSON object.

use std::net::SocketAddr;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::header::CONTENT_TYPE;
use axum::response::Response;
use axum::routing::post;
use axum::serve::ListenerExt;
use axum::Router;
use futures_util::StreamExt;
use serde_json::json;

#[derive(Clone)]
struct Opts {
    gateway: String,
    anthropic: bool,
    conc: usize,
    requests: usize,
    chunks: usize,
    gap_ms: u64,
    prompt_kb: usize,
    obs: bool,
}

struct Mock {
    chunks: Vec<Bytes>,
    gap: Duration,
}

/// What OpenAI sends with `stream_options.include_usage`: `"usage":null` on
/// every chunk, the real usage on a final chunk with no choices.
fn openai_chunks(n: usize) -> Vec<Bytes> {
    let mut out = Vec::with_capacity(n + 3);
    out.push(Bytes::from(
        "data: {\"id\":\"chatcmpl-bench\",\"object\":\"chat.completion.chunk\",\"created\":1794000000,\"model\":\"bench-model\",\"system_fingerprint\":\"fp_0\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"\"},\"logprobs\":null,\"finish_reason\":null}],\"usage\":null}\n\n",
    ));
    for i in 0..n {
        out.push(Bytes::from(format!(
            "data: {{\"id\":\"chatcmpl-bench\",\"object\":\"chat.completion.chunk\",\"created\":1794000000,\"model\":\"bench-model\",\"system_fingerprint\":\"fp_0\",\"choices\":[{{\"index\":0,\"delta\":{{\"content\":\" tok{i}\"}},\"logprobs\":null,\"finish_reason\":null}}],\"usage\":null}}\n\n"
        )));
    }
    out.push(Bytes::from(format!(
        "data: {{\"id\":\"chatcmpl-bench\",\"object\":\"chat.completion.chunk\",\"created\":1794000000,\"model\":\"bench-model\",\"choices\":[],\"usage\":{{\"prompt_tokens\":2000,\"completion_tokens\":{n},\"total_tokens\":{}}}}}\n\n",
        2000 + n
    )));
    out.push(Bytes::from("data: [DONE]\n\n"));
    out
}

fn anthropic_chunks(n: usize) -> Vec<Bytes> {
    let mut out = Vec::with_capacity(n + 5);
    out.push(Bytes::from(
        "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_bench\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"bench-model\",\"content\":[],\"usage\":{\"input_tokens\":2000,\"cache_read_input_tokens\":0,\"output_tokens\":1}}}\n\n",
    ));
    out.push(Bytes::from(
        "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
    ));
    for i in 0..n {
        out.push(Bytes::from(format!(
            "event: content_block_delta\ndata: {{\"type\":\"content_block_delta\",\"index\":0,\"delta\":{{\"type\":\"text_delta\",\"text\":\" tok{i}\"}}}}\n\n"
        )));
    }
    out.push(Bytes::from(format!(
        "event: content_block_stop\ndata: {{\"type\":\"content_block_stop\",\"index\":0}}\n\nevent: message_delta\ndata: {{\"type\":\"message_delta\",\"delta\":{{\"stop_reason\":\"end_turn\"}},\"usage\":{{\"output_tokens\":{n}}}}}\n\n"
    )));
    out.push(Bytes::from(
        "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
    ));
    out
}

async fn serve_stream(State(m): State<Arc<Mock>>, _body: Bytes) -> Response {
    let gap = m.gap;
    let chunks = m.chunks.clone();
    let s = futures_util::stream::iter(chunks).then(move |c| async move {
        if !gap.is_zero() {
            tokio::time::sleep(gap).await;
        } else {
            // One write per chunk, like a real token stream.
            tokio::task::yield_now().await;
        }
        Ok::<_, std::io::Error>(c)
    });
    let mut r = Response::new(Body::from_stream(s));
    r.headers_mut()
        .insert(CONTENT_TYPE, "text/event-stream".parse().unwrap());
    r
}

/// (utime + stime) of a process in milliseconds, and its peak RSS in kB.
fn proc_cpu_ms(pid: u32) -> (f64, u64) {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).unwrap_or_default();
    let after = stat.rsplit_once(')').map(|(_, r)| r).unwrap_or("");
    let f: Vec<&str> = after.split_whitespace().collect();
    let ticks: f64 = [11, 12]
        .iter()
        .filter_map(|&i| f.get(i)?.parse::<f64>().ok())
        .sum();
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).unwrap_or_default();
    let hwm = status
        .lines()
        .find_map(|l| l.strip_prefix("VmHWM:"))
        .and_then(|v| v.split_whitespace().next()?.parse().ok())
        .unwrap_or(0);
    // USER_HZ is 100 on every Linux this runs on.
    (ticks * 10.0, hwm)
}

struct Run {
    secs: f64,
    ttfb_ms: Vec<f64>,
    total_ms: Vec<f64>,
    bytes: u64,
}

async fn drive(url: String, key: &'static str, o: &Opts) -> anyhow::Result<Run> {
    let client = reqwest::Client::builder()
        .pool_max_idle_per_host(o.conc)
        .tcp_nodelay(true)
        .build()?;
    let prompt = "lorem ipsum dolor sit amet ".repeat(o.prompt_kb * 1024 / 27);
    let body = Bytes::from(serde_json::to_vec(&json!({
        "model": "bench-model",
        "stream": true,
        "max_tokens": 1024,
        "messages": [{"role": "user", "content": prompt}],
    }))?);
    let per = o.requests / o.conc;
    let t0 = Instant::now();
    let mut tasks = Vec::new();
    for _ in 0..o.conc {
        let (client, url, body) = (client.clone(), url.clone(), body.clone());
        tasks.push(tokio::spawn(async move {
            let mut out = Vec::with_capacity(per);
            let mut bytes = 0u64;
            for _ in 0..per {
                let start = Instant::now();
                let resp = client
                    .post(&url)
                    .header("authorization", format!("Bearer {key}"))
                    .header("x-api-key", key)
                    .header("content-type", "application/json")
                    .body(body.clone())
                    .send()
                    .await?;
                anyhow::ensure!(resp.status().is_success(), "status {}", resp.status());
                let mut s = resp.bytes_stream();
                let mut first = None;
                while let Some(c) = s.next().await {
                    let c = c?;
                    first.get_or_insert_with(|| start.elapsed());
                    bytes += c.len() as u64;
                }
                out.push((
                    first.unwrap_or_default().as_secs_f64() * 1e3,
                    start.elapsed().as_secs_f64() * 1e3,
                ));
            }
            Ok::<_, anyhow::Error>((out, bytes))
        }));
    }
    let mut run = Run {
        secs: 0.0,
        ttfb_ms: Vec::new(),
        total_ms: Vec::new(),
        bytes: 0,
    };
    for t in tasks {
        let (rows, bytes) = t.await??;
        run.bytes += bytes;
        for (a, b) in rows {
            run.ttfb_ms.push(a);
            run.total_ms.push(b);
        }
    }
    run.secs = t0.elapsed().as_secs_f64();
    run.ttfb_ms.sort_by(f64::total_cmp);
    run.total_ms.sort_by(f64::total_cmp);
    Ok(run)
}

fn pct(v: &[f64], p: f64) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    v[((v.len() - 1) as f64 * p).round() as usize]
}

struct Kill(Child);
impl Drop for Kill {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn parse() -> anyhow::Result<Opts> {
    let mut a = std::env::args().skip(1);
    let mut o = Opts {
        gateway: String::new(),
        anthropic: false,
        conc: 64,
        requests: 4000,
        chunks: 200,
        gap_ms: 0,
        prompt_kb: 8,
        obs: true,
    };
    while let Some(k) = a.next() {
        let mut val = || a.next().ok_or_else(|| anyhow::anyhow!("{k} needs a value"));
        match k.as_str() {
            "--format" => o.anthropic = val()? == "anthropic",
            "--conc" => o.conc = val()?.parse()?,
            "--requests" => o.requests = val()?.parse()?,
            "--chunks" => o.chunks = val()?.parse()?,
            "--gap-ms" => o.gap_ms = val()?.parse()?,
            "--prompt-kb" => o.prompt_kb = val()?.parse()?,
            "--obs" => o.obs = val()? != "off",
            _ if o.gateway.is_empty() => o.gateway = k,
            other => anyhow::bail!("unknown argument {other:?}"),
        }
    }
    anyhow::ensure!(
        !o.gateway.is_empty(),
        "usage: bench <gateway binary> [options]"
    );
    Ok(o)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let o = parse()?;
    let chunks = if o.anthropic {
        anthropic_chunks(o.chunks)
    } else {
        openai_chunks(o.chunks)
    };
    let mock = Arc::new(Mock {
        chunks,
        gap: Duration::from_millis(o.gap_ms),
    });
    let router = Router::new()
        .route("/v1/chat/completions", post(serve_stream))
        .route("/v1/messages", post(serve_stream))
        .layer(axum::extract::DefaultBodyLimit::max(64 << 20))
        .with_state(mock);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let mock_addr: SocketAddr = listener.local_addr()?;
    let listener = listener.tap_io(|tcp| {
        let _ = tcp.set_nodelay(true);
    });
    tokio::spawn(async move { axum::serve(listener, router).await });

    let gw_port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0")?;
        l.local_addr()?.port()
    };
    let dir = std::env::temp_dir().join(format!("pgw-bench-{}", std::process::id()));
    std::fs::create_dir_all(&dir)?;
    let cfg = dir.join("gateway.toml");
    std::fs::write(
        &cfg,
        format!(
            r#"listen = "127.0.0.1:{gw_port}"
admin_token = "bench-admin"

[cache]
ttl_secs = 0

[[upstreams]]
name = "up"
base_url = "http://{mock_addr}"
kind = "{kind}"
api_key = "real-key"
max_concurrency = 100000

[obs]
enabled = {obs}
backend = "memory"

[[agents]]
id = "bench"
token = "bench-agent-token"
"#,
            kind = if o.anthropic { "anthropic" } else { "openai" },
            obs = o.obs,
        ),
    )?;
    let child = Kill(
        Command::new(&o.gateway)
            .arg("--config")
            .arg(&cfg)
            .env("RUST_LOG", "warn")
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()?,
    );
    let pid = child.0.id();
    let health = format!("http://127.0.0.1:{gw_port}/gateway/health");
    let probe = reqwest::Client::new();
    let mut up = false;
    for _ in 0..100 {
        if probe.get(&health).send().await.is_ok() {
            up = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    anyhow::ensure!(up, "gateway did not start");

    let path = if o.anthropic {
        "/v1/messages"
    } else {
        "/v1/chat/completions"
    };
    // Warm both paths, then measure.
    let warm = Opts {
        requests: o.conc * 2,
        ..o.clone()
    };
    drive(format!("http://{mock_addr}{path}"), "real-key", &warm).await?;
    drive(
        format!("http://127.0.0.1:{gw_port}/up{path}"),
        "bench-agent-token",
        &warm,
    )
    .await?;

    let direct = drive(format!("http://{mock_addr}{path}"), "real-key", &o).await?;
    let (cpu0, _) = proc_cpu_ms(pid);
    let via = drive(
        format!("http://127.0.0.1:{gw_port}/up{path}"),
        "bench-agent-token",
        &o,
    )
    .await?;
    let (cpu1, hwm_kb) = proc_cpu_ms(pid);

    let n = via.total_ms.len() as f64;
    let cpu_ms = cpu1 - cpu0;
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "format": if o.anthropic { "anthropic" } else { "openai" },
            "requests": n,
            "concurrency": o.conc,
            "chunks_per_response": mock_chunks(&o),
            "chunk_gap_ms": o.gap_ms,
            "prompt_kb": o.prompt_kb,
            "direct": {
                "req_per_s": direct.total_ms.len() as f64 / direct.secs,
                "ttfb_p50_ms": pct(&direct.ttfb_ms, 0.5),
                "ttfb_p99_ms": pct(&direct.ttfb_ms, 0.99),
                "total_p50_ms": pct(&direct.total_ms, 0.5),
                "total_p99_ms": pct(&direct.total_ms, 0.99),
            },
            "gateway": {
                "req_per_s": n / via.secs,
                "mb_per_s": via.bytes as f64 / 1e6 / via.secs,
                "ttfb_p50_ms": pct(&via.ttfb_ms, 0.5),
                "ttfb_p99_ms": pct(&via.ttfb_ms, 0.99),
                "total_p50_ms": pct(&via.total_ms, 0.5),
                "total_p99_ms": pct(&via.total_ms, 0.99),
                "cpu_ms_total": cpu_ms,
                "cpu_us_per_request": cpu_ms * 1000.0 / n,
                "cpu_us_per_chunk": cpu_ms * 1000.0 / (n * mock_chunks(&o) as f64),
                "cpu_cores_used": cpu_ms / 1000.0 / via.secs,
                "peak_rss_mb": hwm_kb as f64 / 1024.0,
            },
            "added": {
                "ttfb_p50_ms": pct(&via.ttfb_ms, 0.5) - pct(&direct.ttfb_ms, 0.5),
                "ttfb_p99_ms": pct(&via.ttfb_ms, 0.99) - pct(&direct.ttfb_ms, 0.99),
            },
        }))?
    );
    drop(child);
    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}

fn mock_chunks(o: &Opts) -> usize {
    o.chunks + if o.anthropic { 4 } else { 3 }
}
