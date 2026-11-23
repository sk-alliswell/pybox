//! A stand-in fleet for runs too large for the lab's real model servers:
//! many model servers that only keep time, and thousands of callers.
//!
//! The gateway under test is the real one. What is emulated is everything
//! around it, cheaply enough that one small machine carries tens of servers
//! and thousands of callers:
//!
//!   fleet serve   N model servers on consecutive ports. Each runs `slots`
//!                 requests at once and queues the rest, keeps the last
//!                 prompt of every slot (a request continues in the idle
//!                 slot whose prompt starts most like its own, llama.cpp's
//!                 rule), takes `prefill_ms` plus the unread prompt tokens
//!                 at `read_tps` before the first token, then writes
//!                 `max_tokens` tokens at `tps` shared by what runs
//!                 together (total = tps * n^scaling). It notices a caller
//!                 that left when it next writes a chunk. Answers carry
//!                 `usage` with the cached prompt tokens.
//!   fleet config  A gateway config for those servers: one upstream, prefix
//!                 routing, one agent per team and kind of work.
//!   fleet drive   Callers: teams of people with ide / chat / batch work,
//!                 conversations that grow turn by turn, busy hours once a
//!                 (shortened) day, a timeout per kind. Writes one line per
//!                 request.
//!
//! The servers' timing is the simulator's own server model, on purpose: a
//! run through this fleet tests the gateway and the simulator's copy of it
//! at a size the lab cannot reach with real models, not the model of a
//! model server (that is what the local-model runs are for).
//!
//!   cargo run --release --example fleet -- serve --servers 16 --port 9300
//!   cargo run --release --example fleet -- config --servers 16 --port 9300 > fleet.toml
//!   cargo run --release --example fleet -- drive --users 2000 --days 3 --day-s 600 --out client.jsonl

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures_util::StreamExt;
use serde_json::{json, Value};

// ---------- small deterministic helpers ----------

/// splitmix64: every random draw here comes from a seed and a counter, so a
/// run can be repeated.
fn mix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        mix(self.0)
    }
    /// Uniform in (0, 1).
    fn unit(&mut self) -> f64 {
        ((self.next() >> 11) as f64 + 0.5) / (1u64 << 53) as f64
    }
    fn exp(&mut self, rate: f64) -> f64 {
        -self.unit().ln() / rate
    }
    fn normal(&mut self) -> f64 {
        let (a, b) = (self.unit(), self.unit());
        (-2.0 * a.ln()).sqrt() * (std::f64::consts::TAU * b).cos()
    }
    fn lognormal(&mut self, sigma: f64) -> f64 {
        (sigma * self.normal()).exp()
    }
}

fn hash(s: &str) -> u64 {
    s.bytes().fold(0xcbf2_9ce4_8422_2325, |h, b| {
        (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3)
    })
}

/// About `tokens` tokens of filler text (4 bytes a token), the same for the
/// same seed.
fn text(seed: &str, tokens: usize) -> String {
    let mut r = Rng(hash(seed));
    let mut out = String::with_capacity(tokens * 4 + 8);
    while out.len() < tokens * 4 {
        let n = 3 + (r.next() % 6) as usize;
        for _ in 0..n {
            out.push((b'a' + (r.next() % 26) as u8) as char);
        }
        out.push(' ');
    }
    out
}

fn tokens_of(s: &str) -> u32 {
    (s.len() / 4) as u32 + 4
}

struct Args(HashMap<String, String>);
impl Args {
    fn parse(rest: &[String]) -> Self {
        let mut m = HashMap::new();
        let mut i = 0;
        while i < rest.len() {
            let k = rest[i].trim_start_matches("--").to_string();
            let v = rest.get(i + 1).cloned().unwrap_or_default();
            m.insert(k, v);
            i += 2;
        }
        Self(m)
    }
    fn num(&self, k: &str, default: f64) -> f64 {
        self.0
            .get(k)
            .and_then(|v| v.parse().ok())
            .unwrap_or(default)
    }
    fn text(&self, k: &str, default: &str) -> String {
        self.0
            .get(k)
            .cloned()
            .unwrap_or_else(|| default.to_string())
    }
}

// ---------- the servers ----------

#[derive(Clone, Copy)]
struct Speed {
    slots: usize,
    tps: f64,
    scaling: f64,
    prefill_ms: f64,
    read_tps: f64,
    jitter: f64,
}

struct ServerState {
    /// Per slot: the prompt it read last, as cumulative (hash, tokens) per
    /// message, and when it was last used.
    held: Vec<(Vec<(u64, u32)>, u64)>,
    busy: Vec<bool>,
    waiting: VecDeque<tokio::sync::oneshot::Sender<()>>,
    active: usize,
    clock: u64,
    draws: u64,
}

struct Server {
    speed: Speed,
    model: String,
    seed: u64,
    st: Mutex<ServerState>,
}

fn chain(messages: &[Value]) -> Vec<(u64, u32)> {
    let (mut h, mut t) = (0u64, 0u32);
    messages
        .iter()
        .map(|m| {
            let c = m["content"].as_str().unwrap_or("");
            h = mix(h ^ hash(c) ^ hash(m["role"].as_str().unwrap_or("")));
            t += tokens_of(c);
            (h, t)
        })
        .collect()
}

fn common(a: &[(u64, u32)], b: &[(u64, u32)]) -> usize {
    a.iter().zip(b).take_while(|(x, y)| x.0 == y.0).count()
}

impl Server {
    /// Wait for a slot, then take the idle one that suits this prompt.
    /// Returns (slot, prompt tokens it already holds).
    async fn seat(&self, prompt: &[(u64, u32)]) -> (usize, u32) {
        let wait = {
            let mut st = self.st.lock().unwrap();
            if st.active < self.speed.slots {
                st.active += 1;
                None
            } else {
                let (tx, rx) = tokio::sync::oneshot::channel();
                st.waiting.push_back(tx);
                Some(rx)
            }
        };
        if let Some(rx) = wait {
            // The slot is handed over by the request that leaves it.
            let _ = rx.await;
        }
        let mut st = self.st.lock().unwrap();
        let total = prompt.last().map_or(1, |p| p.1.max(1));
        let idle = |st: &ServerState, i: &usize| !st.busy[*i];
        let depth = |st: &ServerState, i: usize| common(&st.held[i].0, prompt);
        let best = (0..self.speed.slots)
            .filter(|i| idle(&st, i))
            .max_by_key(|i| (depth(&st, *i), std::cmp::Reverse(st.held[*i].1)))
            .expect("a slot is free");
        let d = depth(&st, best);
        let warm = if d > 0 { prompt[d - 1].1 } else { 0 };
        let (slot, warm) = if f64::from(warm) / f64::from(total) >= 0.10 {
            (best, warm)
        } else {
            let oldest = (0..self.speed.slots)
                .filter(|i| idle(&st, i))
                .min_by_key(|i| st.held[*i].1)
                .expect("a slot is free");
            (oldest, 0)
        };
        st.busy[slot] = true;
        st.clock += 1;
        st.held[slot] = (prompt.to_vec(), st.clock);
        (slot, warm)
    }

    fn leave(&self, slot: usize) {
        let mut st = self.st.lock().unwrap();
        st.busy[slot] = false;
        // Hand the seat to the next in line, or give it back.
        while let Some(tx) = st.waiting.pop_front() {
            if tx.send(()).is_ok() {
                return;
            }
        }
        st.active -= 1;
    }

    fn draw(&self, sigma: f64) -> f64 {
        let mut st = self.st.lock().unwrap();
        st.draws += 1;
        Rng(self.seed ^ mix(st.draws)).lognormal(sigma)
    }

    fn running(&self) -> usize {
        self.st
            .lock()
            .unwrap()
            .busy
            .iter()
            .filter(|b| **b)
            .count()
            .max(1)
    }
}

async fn chat(State(s): State<Arc<Server>>, Json(body): Json<Value>) -> Response {
    let messages = body["messages"].as_array().cloned().unwrap_or_default();
    let prompt = chain(&messages);
    let total = prompt.last().map_or(0, |p| p.1);
    let out = body["max_tokens"].as_u64().unwrap_or(64).max(1);
    let stream = body["stream"].as_bool().unwrap_or(false);
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(64);
    let model = s.model.clone();
    tokio::spawn(async move {
        let (slot, warm) = s.seat(&prompt).await;
        let sp = s.speed;
        let read = f64::from(total - warm.min(total));
        let prefill = (sp.prefill_ms + read / sp.read_tps * 1000.0) * s.draw(sp.jitter * 2.0);
        let pace = s.draw(sp.jitter);
        tokio::time::sleep(Duration::from_secs_f64(prefill / 1000.0)).await;
        let id = format!("chatcmpl-{slot}-{}", s.draw(1.0).to_bits());
        let frame = |v: Value| Ok(Bytes::from(format!("data: {v}\n\n")));
        let usage = json!({"prompt_tokens": total, "completion_tokens": out, "total_tokens": u64::from(total) + out,
            "prompt_tokens_details": {"cached_tokens": warm.min(total.saturating_sub(1))}});
        if !stream {
            let n = s.running() as f64;
            let ms = out as f64 * 1000.0 / sp.tps * n.powf(1.0 - sp.scaling) * pace;
            tokio::time::sleep(Duration::from_secs_f64(ms / 1000.0)).await;
            let v = json!({"id": id, "object": "chat.completion", "model": model, "usage": usage,
                "choices": [{"index": 0, "finish_reason": "stop", "message": {"role": "assistant", "content": "ok ".repeat(out as usize)}}]});
            let _ = tx.send(Ok(Bytes::from(v.to_string()))).await;
            s.leave(slot);
            return;
        }
        for i in 0..out {
            let delta = json!({"id": id, "object": "chat.completion.chunk", "model": model,
                "choices": [{"index": 0, "delta": {"content": "ok "}, "finish_reason": null}]});
            // The caller is checked for when a chunk is written, so one
            // that left during the prompt still cost its reading.
            if tx.send(frame(delta)).await.is_err() {
                s.leave(slot);
                return;
            }
            if i + 1 < out {
                let n = s.running() as f64;
                let ms = 1000.0 / sp.tps * n.powf(1.0 - sp.scaling) * pace;
                tokio::time::sleep(Duration::from_secs_f64(ms / 1000.0)).await;
            }
        }
        let stop = json!({"id": id, "object": "chat.completion.chunk", "model": model,
            "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]});
        let last = json!({"id": id, "object": "chat.completion.chunk", "model": model, "choices": [], "usage": usage});
        let _ = tx.send(frame(stop)).await;
        let _ = tx.send(frame(last)).await;
        let _ = tx.send(Ok(Bytes::from("data: [DONE]\n\n"))).await;
        s.leave(slot);
    });
    let body = Body::from_stream(futures_util::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    }));
    let kind = if stream {
        "text/event-stream"
    } else {
        "application/json"
    };
    ([(axum::http::header::CONTENT_TYPE, kind)], body).into_response()
}

async fn serve(a: Args) {
    let n = a.num("servers", 16.0) as usize;
    let port = a.num("port", 9300.0) as u16;
    let speed = Speed {
        slots: a.num("slots", 4.0) as usize,
        tps: a.num("tps", 40.0),
        scaling: a.num("scaling", 0.85),
        prefill_ms: a.num("prefill-ms", 80.0),
        read_tps: a.num("read-tps", 3000.0),
        jitter: a.num("jitter", 0.05),
    };
    let model = a.text("model", "fleet-small");
    let mut tasks = Vec::new();
    for i in 0..n {
        let s = Arc::new(Server {
            speed,
            model: model.clone(),
            seed: mix(i as u64 + 1),
            st: Mutex::new(ServerState {
                held: vec![(Vec::new(), 0); speed.slots],
                busy: vec![false; speed.slots],
                waiting: VecDeque::new(),
                active: 0,
                clock: 0,
                draws: 0,
            }),
        });
        let m = model.clone();
        let app = Router::new()
            .route("/v1/chat/completions", post(chat))
            .route(
                "/v1/models",
                get(move || async move {
                    Json(json!({"object": "list", "data": [{"id": m, "object": "model"}]}))
                }),
            )
            .with_state(s);
        let l = tokio::net::TcpListener::bind(("127.0.0.1", port + i as u16))
            .await
            .expect("port is free");
        tasks.push(tokio::spawn(async move { axum::serve(l, app).await }));
    }
    eprintln!("{n} servers on 127.0.0.1:{port}..{}", port as usize + n - 1);
    for t in tasks {
        let _ = t.await;
    }
}

// ---------- what the callers do ----------

/// (name, people out of every 100, hour the team is busiest, requests a
/// minute from one person at the busiest: ide, chat, batch)
const TEAMS: [(&str, f64, f64, [f64; 3]); 3] = [
    ("platform", 40.0, 13.0, [1.2, 0.3, 0.05]),
    ("research", 30.0, 14.5, [0.4, 0.4, 0.5]),
    ("product", 30.0, 11.5, [0.8, 0.4, 0.08]),
];
/// (name, gateway weight, priority class, caller's timeout s, system prompt
/// tokens, tokens a turn adds, answer tokens, turns before starting over)
type Kind = (&'static str, u32, &'static str, f64, usize, usize, u64, u32);
const KINDS: [Kind; 3] = [
    ("ide", 4, "interactive", 30.0, 900, 300, 60, 6),
    ("chat", 2, "", 60.0, 500, 200, 80, 8),
    ("batch", 1, "batch", 120.0, 600, 400, 40, 1),
];

fn config(a: Args) {
    let n = a.num("servers", 16.0) as usize;
    let port = a.num("port", 9300.0) as u16;
    let slots = a.num("slots", 4.0) as usize;
    let queue = a.num("max-queue", (n * slots * 3) as f64) as usize;
    println!("# Written by `fleet config`: {n} stand-in servers, {slots} requests each.");
    println!(
        "listen = \"127.0.0.1:{}\"\nadmin_token = \"fleet-admin\"\n",
        a.num("listen", 8788.0) as u16
    );
    println!(
        "[obs]\nbackend = \"{}\"\ndir = \"obs-fleet\"\n",
        a.text("obs", "tdb")
    );
    println!(
        "[[upstreams]]\nname = \"fleet\"\nbase_url = \"http://127.0.0.1:{port}\"\nkind = \"none\""
    );
    println!(
        "affinity = \"{}\"\nmax_queue = {queue}",
        a.text("affinity", "prefix")
    );
    if a.text("shed", "on") == "on" {
        println!("shed_from = 0.25\nshed_ceiling = 0.75\nshed_floor = 0.1");
    }
    let wait = a.num("affinity-wait-ms", 0.0);
    if wait > 0.0 {
        println!("affinity_wait_ms = {wait}");
    }
    println!();
    for i in 0..n {
        println!("[[upstreams.backends]]\nname = \"s{}\"\nbase_url = \"http://127.0.0.1:{}\"\nmax_concurrency = {slots}\nprefix_slots = {slots}\n", i + 1, port as usize + i);
    }
    for (team, ..) in TEAMS {
        for (kind, weight, class, ..) in KINDS {
            println!("[[agents]]\nid = \"{team}-{kind}\"\ntoken = \"fleet-{team}-{kind}\"\nteam_id = \"{team}\"\nkind = \"{kind}\"\nweight = {weight}");
            if !class.is_empty() {
                println!("priority = \"{class}\"");
            }
            println!();
        }
    }
}

/// Share of the busiest rate at this hour of the day: a raised cosine
/// `width` hours wide around `busy_at`, `quiet` outside it.
fn shape(hour: f64, busy_at: f64, width: f64, quiet: f64) -> f64 {
    let d = ((hour - busy_at + 12.0).rem_euclid(24.0) - 12.0).abs();
    if d >= width {
        return quiet;
    }
    quiet + (1.0 - quiet) * 0.5 * (1.0 + (std::f64::consts::PI * d / width).cos())
}

struct Job {
    at: f64,
    team: usize,
    kind: usize,
    user: usize,
}

/// A person's conversation of one kind: which one, and how far along.
#[derive(Default, Clone, Copy)]
struct Conv {
    id: u32,
    turn: u32,
}

fn messages(seed: u64, team: &str, kind: usize, user: usize, c: Conv) -> Vec<Value> {
    let (name, _, _, _, sys, add, _, _) = KINDS[kind];
    let mut m =
        vec![json!({"role": "system", "content": text(&format!("{seed}/system/{name}"), sys)})];
    for t in 0..=c.turn {
        let who = format!("{seed}/{team}/{user}/{name}/{}/{t}", c.id);
        m.push(json!({"role": "user", "content": text(&who, add)}));
        if t < c.turn {
            m.push(json!({"role": "assistant", "content": text(&format!("{who}/answer"), 40)}));
        }
    }
    m
}

async fn drive(a: Args) {
    let users = a.num("users", 2000.0) as usize;
    let days = a.num("days", 3.0);
    let day_s = a.num("day-s", 600.0);
    let load = a.num("load", 1.0);
    let quiet = a.num("quiet", 0.1);
    let width = a.num("width", 6.0);
    let seed = a.num("seed", 1.0) as u64;
    let gateway = a.text("gateway", "http://127.0.0.1:8788");
    let model = a.text("model", "fleet-small");
    let out_path = a.text("out", "client.jsonl");
    // Who sends what when: thinning a Poisson stream at the busiest rate.
    let mut rng = Rng(mix(seed));
    let mut jobs = Vec::new();
    let mut first = 0;
    for (ti, (_, share, busy_at, rates)) in TEAMS.iter().enumerate() {
        let n = ((users as f64) * share / 100.0).round() as usize;
        for (ki, per_min) in rates.iter().enumerate() {
            // A day shortened to `day_s` keeps its requests per person per
            // real minute: the day is busy for fewer minutes.
            let top = n as f64 * per_min / 60.0 * load;
            let mut at = 0.0;
            loop {
                at += rng.exp(top);
                if at >= day_s * days {
                    break;
                }
                if rng.unit() < shape(at / day_s * 24.0 % 24.0, *busy_at, width, quiet) {
                    // A few people send much more than the rest.
                    let u = (rng.unit().powi(2) * n as f64) as usize;
                    jobs.push(Job {
                        at,
                        team: ti,
                        kind: ki,
                        user: first + u.min(n - 1),
                    });
                }
            }
        }
        first += n;
    }
    jobs.sort_by(|a, b| a.at.total_cmp(&b.at));
    let client = reqwest::Client::builder()
        .pool_max_idle_per_host(0)
        .build()
        .expect("client");
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    let writer = tokio::task::spawn_blocking(move || {
        use std::io::Write;
        let mut f = std::io::BufWriter::new(std::fs::File::create(out_path).expect("out file"));
        while let Some(line) = rx.blocking_recv() {
            let _ = writeln!(f, "{line}");
        }
    });
    let convs: Arc<Mutex<HashMap<(usize, usize), Conv>>> = Arc::default();
    let t0 = Instant::now();
    let start_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    println!(
        "{}",
        json!({"start_ms": start_ms as u64, "day_s": day_s, "days": days, "users": users, "requests": jobs.len()})
    );
    let mut tasks = Vec::with_capacity(jobs.len());
    for (n, j) in jobs.into_iter().enumerate() {
        tokio::time::sleep_until((t0 + Duration::from_secs_f64(j.at)).into()).await;
        let (team, kind) = (TEAMS[j.team].0, KINDS[j.kind]);
        // The conversation as it stands; it moves on when this is answered.
        let conv = *convs.lock().unwrap().entry((j.user, j.kind)).or_default();
        let body = json!({"model": model, "stream": true, "max_tokens": kind.6,
            "messages": messages(seed, team, j.kind, j.user, conv)});
        let req = client
            .post(format!("{gateway}/fleet/v1/chat/completions"))
            .bearer_auth(format!("fleet-{team}-{}", kind.0))
            .header("x-gateway-user-id", format!("{team}-{}", j.user))
            .json(&body);
        let (tx, convs) = (tx.clone(), convs.clone());
        tasks.push(tokio::spawn(async move {
            let sent = Instant::now();
            let mut first: Option<f64> = None;
            let mut status = 0u16;
            let run = async {
                let r = req.send().await.map_err(|e| e.to_string())?;
                status = r.status().as_u16();
                if status != 200 {
                    return Ok::<bool, String>(false);
                }
                let mut s = r.bytes_stream();
                let mut done = false;
                while let Some(chunk) = s.next().await {
                    let chunk = chunk.map_err(|e| e.to_string())?;
                    if first.is_none() && memchr::memmem::find(&chunk, b"\"content\"").is_some() {
                        first = Some(sent.elapsed().as_secs_f64());
                    }
                    done |= memchr::memmem::find(&chunk, b"[DONE]").is_some();
                }
                Ok(done)
            };
            let res = tokio::time::timeout(Duration::from_secs_f64(kind.3), run).await;
            let (done, gave_up, error) = match res {
                Ok(Ok(d)) => (d, false, None),
                Ok(Err(e)) => (false, false, Some(e)),
                Err(_) => (false, true, None),
            };
            if done {
                // Next time this person continues, or starts over.
                let mut c = convs.lock().unwrap();
                let c = c.entry((j.user, j.kind)).or_default();
                if c.id == conv.id && c.turn == conv.turn {
                    if c.turn + 1 >= kind.7 {
                        *c = Conv { id: c.id + 1, turn: 0 };
                    } else {
                        c.turn += 1;
                    }
                }
            }
            let _ = tx.send(
                json!({"n": n, "at": j.at, "team": team, "kind": kind.0, "user": j.user, "status": status,
                    "done": done, "gave_up": gave_up, "error": error, "ttft": first,
                    "total": sent.elapsed().as_secs_f64(), "turn": conv.turn})
                .to_string(),
            );
        }));
    }
    for t in tasks {
        let _ = t.await;
    }
    drop(tx);
    let _ = writer.await;
    let end_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    println!("{}", json!({"end_ms": end_ms as u64}));
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let rest = Args::parse(args.get(1..).unwrap_or(&[]));
    match args.first().map(String::as_str) {
        Some("serve") => serve(rest).await,
        Some("config") => config(rest),
        Some("drive") => drive(rest).await,
        _ => eprintln!("usage: fleet serve|config|drive [--option value ...] (see the top of examples/fleet.rs)"),
    }
}
