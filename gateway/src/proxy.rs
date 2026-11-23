//! The forwarding path: authenticate, queue, send with retries, stream back.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use axum::body::{Body, Bytes};
use axum::extract::{ConnectInfo, Path, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, Uri, Version};
use axum::response::Response;
use axum::Extension;
use futures_util::{Stream, StreamExt};
use rand::Rng;
use serde::Deserialize;

use crate::cache::{self, Cached, Leader, Lookup};
use crate::config::Kind;
use crate::lead::cachemodel::Weights;
use crate::limits::{Amount, Reservation};
use crate::obs::now_ms;
use crate::obs::trace::{new_span_id, Outcome, ReqCtx};
use crate::scheduler::{
    Admission, FairTicket, Lease, Outlook, Place, Refused, Scheduler, Shed, Ticket,
};
use crate::usage::{Usage, UsageParser};
use crate::{error, limit_response, AgentInfo, App, Upstream};

/// Never forwarded in either direction.
const HOP_BY_HOP: &[&str] = &[
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "host",
    "content-length",
];

/// Request headers the gateway owns.
const STRIP_REQUEST: &[&str] = &["authorization", "x-api-key", "accept-encoding"];

#[derive(Deserialize)]
struct Peek {
    #[serde(default)]
    stream: bool,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    stream_options: Option<StreamOptions>,
    // Output caps, by API flavour. Used to reserve budget before the call.
    #[serde(default)]
    max_tokens: Option<u64>,
    #[serde(default)]
    max_completion_tokens: Option<u64>,
    #[serde(default)]
    max_output_tokens: Option<u64>,
}

#[derive(Deserialize)]
struct StreamOptions {
    #[serde(default)]
    include_usage: Option<bool>,
}

fn http_name(v: Version) -> &'static str {
    match v {
        Version::HTTP_2 => "h2",
        Version::HTTP_3 => "h3",
        Version::HTTP_10 => "http/1.0",
        _ => "http/1.1",
    }
}

fn ms_between(a: Instant, b: Instant) -> f64 {
    b.saturating_duration_since(a).as_secs_f64() * 1000.0
}

#[allow(clippy::too_many_arguments)] // axum extractors
/// What a request is waiting behind, shared with the task that keeps a
/// queued streaming caller informed.
#[derive(Default)]
pub struct QueueWatch(Mutex<Option<Waiting>>);

struct Waiting {
    sched: Arc<Scheduler>,
    place: Place,
    since: Instant,
    /// Held beside the queue (`hold_batch`) instead of waiting in it.
    held: bool,
}

/// Where a waiting request stands: its waiting ticket.
struct Standing {
    outlook: Outlook,
    waited: Duration,
    held: bool,
}

impl Standing {
    fn status(&self) -> &'static str {
        if self.held {
            "held"
        } else {
            "queued"
        }
    }
}

impl QueueWatch {
    fn set(&self, w: Option<Waiting>) {
        *self.0.lock().unwrap() = w;
    }

    /// Requests ahead, expected wait from now and time waited so far, while
    /// the request is waiting.
    fn status(&self) -> Option<Standing> {
        let g = self.0.lock().unwrap();
        let w = g.as_ref()?;
        Some(Standing {
            outlook: w.sched.queue_status(&w.place),
            waited: w.since.elapsed(),
            held: w.held,
        })
    }
}

/// Requests that can be asked about while they wait, by (agent, ticket).
#[derive(Default)]
pub struct Tickets(Mutex<std::collections::HashMap<(String, String), Arc<QueueWatch>>>);

/// Takes the ticket off the list when its request ends, however it ends.
struct TicketGuard {
    app: Arc<App>,
    key: (String, String),
    watch: Arc<QueueWatch>,
}

impl Drop for TicketGuard {
    fn drop(&mut self) {
        let mut g = self.app.tickets.0.lock().unwrap();
        // A caller may reuse a ticket name; only take our own entry out.
        if g.get(&self.key)
            .is_some_and(|w| Arc::ptr_eq(w, &self.watch))
        {
            g.remove(&self.key);
        }
    }
}

/// A ticket name a caller may choose: short and plain.
fn ticket_name(v: &str) -> Option<&str> {
    let ok = |c: char| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.');
    ((1..=64).contains(&v.len()) && v.chars().all(ok)).then_some(v)
}

/// `GET /gateway/queue/{ticket}` with the agent's own token: where the
/// request with that ticket stands. For callers that cannot be told in the
/// response itself (not streaming), and for whoever watches on their behalf.
pub async fn ticket_status(
    State(app): State<Arc<App>>,
    Path(ticket): Path<String>,
    headers: HeaderMap,
) -> Response {
    let Some(agent) = app.agent_for(&headers) else {
        return error(
            StatusCode::UNAUTHORIZED,
            "authentication_error",
            "unknown gateway token",
        );
    };
    let watch = app
        .tickets
        .0
        .lock()
        .unwrap()
        .get(&(agent.id.clone(), ticket.clone()))
        .cloned();
    let Some(watch) = watch else {
        return error(
            StatusCode::NOT_FOUND,
            "not_found_error",
            "no request of this agent with that ticket is in the gateway (it has ended, or has not arrived)",
        );
    };
    let body = match watch.status() {
        Some(s) => serde_json::json!({
            "ticket": ticket,
            "status": s.status(),
            "reason": "saturated",
            "ahead": s.outlook.ahead,
            "expected_wait_ms": s.outlook.wait.as_millis() as u64,
            "expected_wait_max_ms": s.outlook.wait_max.as_millis() as u64,
            "waited_ms": s.waited.as_millis() as u64,
        }),
        // Not waiting: before the queue, or already with a server.
        None => serde_json::json!({"ticket": ticket, "status": "running"}),
    };
    axum::response::IntoResponse::into_response(axum::Json(body))
}

#[allow(clippy::too_many_arguments)]
pub async fn handle(
    state: State<Arc<App>>,
    path: Path<(String, String)>,
    peer: Option<Extension<ConnectInfo<std::net::SocketAddr>>>,
    version: Version,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    // A streaming caller is told while it waits for a slot when it asks
    // (`x-gateway-queue-notice`) or the upstream says so (`queue_notice`):
    // `sse` (comment lines any SSE client skips) or `event`
    // (`event: gateway.queue` with a JSON body). The header wins.
    let notice = headers
        .get("x-gateway-queue-notice")
        .and_then(|v| v.to_str().ok())
        .or_else(|| {
            let up = state.upstreams.get(&path.0 .0)?;
            Some(up.cfg.queue_notice.as_str())
        })
        .filter(|v| matches!(*v, "sse" | "event"))
        .map(|v| v == "event");
    let streams = || serde_json::from_slice::<Peek>(&body).is_ok_and(|p| p.stream);
    let notice = notice.filter(|_| streams());
    // Any caller can name a ticket to ask about its request from outside.
    let named = headers
        .get("x-gateway-ticket")
        .and_then(|v| ticket_name(v.to_str().ok()?))
        .map(str::to_string);
    let agent = (notice.is_some() || named.is_some())
        .then(|| state.agent_for(&headers))
        .flatten();
    let Some(agent) = agent else {
        return serve(state, path, peer, version, method, uri, headers, body, None).await;
    };
    let watch = Arc::new(QueueWatch::default());
    let ticket = named.unwrap_or_else(new_span_id);
    let guard = TicketGuard {
        app: state.0.clone(),
        key: (agent.id.clone(), ticket.clone()),
        watch: watch.clone(),
    };
    state
        .tickets
        .0
        .lock()
        .unwrap()
        .insert(guard.key.clone(), watch.clone());
    let served = serve(
        state,
        path,
        peer,
        version,
        method,
        uri,
        headers,
        body,
        Some(watch.clone()),
    );
    let mut fut: Pin<Box<dyn Future<Output = Response> + Send>> = Box::pin(async move {
        let _listed = guard;
        served.await
    });
    let Some(as_event) = notice else {
        return fut.await;
    };
    // Until it has actually sat in the queue for a moment, the request keeps
    // its ordinary response: status, headers and all.
    loop {
        tokio::select! {
            resp = &mut fut => return resp,
            () = tokio::time::sleep(Duration::from_millis(250)) => {
                if watch.status().is_some() {
                    break;
                }
            }
        }
    }
    queue_notice_stream(fut, watch, as_event, ticket)
}

/// Answer a queued streaming request now, tell it where it stands every
/// couple of seconds, then pass the real stream through. An upstream or
/// gateway error can no longer change the status line, so it arrives as an
/// `event: error` instead.
fn queue_notice_stream(
    fut: Pin<Box<dyn Future<Output = Response> + Send>>,
    watch: Arc<QueueWatch>,
    as_event: bool,
    ticket: String,
) -> Response {
    enum St {
        Waiting(
            Pin<Box<dyn Future<Output = Response> + Send>>,
            tokio::time::Interval,
        ),
        Body(axum::body::BodyDataStream),
    }
    let ticket_header = HeaderValue::from_str(&ticket).ok();
    let line = Arc::new(move |status: &str, o: Outlook, waited: Duration| {
        let (ahead, wait, waited) = (o.ahead, o.wait.as_millis(), waited.as_millis());
        let wait_max = o.wait_max.as_millis();
        if as_event {
            format!(
                "event: gateway.queue\ndata: {{\"status\":\"{status}\",\"ahead\":{ahead},\"expected_wait_ms\":{wait},\"waited_ms\":{waited},\"expected_wait_max_ms\":{wait_max},\"ticket\":\"{ticket}\"}}\n\n"
            )
        } else {
            format!(
                ": gateway {status} ahead={ahead} expected_wait_ms={wait} waited_ms={waited} expected_wait_max_ms={wait_max} ticket={ticket}\n\n"
            )
        }
    });
    let started = Instant::now();
    let st = St::Waiting(fut, tokio::time::interval(Duration::from_secs(2)));
    let stream = futures_util::stream::unfold(Some(st), move |st| {
        let (watch, line) = (watch.clone(), line.clone());
        async move {
            match st? {
                St::Waiting(mut fut, mut tick) => {
                    tokio::select! {
                        resp = &mut fut => {
                            if resp.status() == StatusCode::OK {
                                let l = line("dispatched", Outlook::default(), started.elapsed());
                                let body = resp.into_body().into_data_stream();
                                return Some((Ok(Bytes::from(l)), Some(St::Body(body))));
                            }
                            let body = axum::body::to_bytes(resp.into_body(), 1 << 20)
                                .await
                                .unwrap_or_default();
                            let text = String::from_utf8_lossy(&body).replace('\n', " ");
                            Some((Ok(Bytes::from(format!("event: error\ndata: {text}\n\n"))), None))
                        }
                        _ = tick.tick() => {
                            let l = match watch.status() {
                                Some(s) => line(s.status(), s.outlook, s.waited),
                                None => line("running", Outlook::default(), started.elapsed()),
                            };
                            Some((Ok(Bytes::from(l)), Some(St::Waiting(fut, tick))))
                        }
                    }
                }
                St::Body(mut body) => match body.next().await {
                    Some(Ok(b)) => Some((Ok(b), Some(St::Body(body)))),
                    Some(Err(e)) => Some((Err(e), None)),
                    None => None,
                },
            }
        }
    });
    let mut resp = Response::new(Body::from_stream(stream));
    let h = resp.headers_mut();
    h.insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("text/event-stream"),
    );
    h.insert(
        http::header::CACHE_CONTROL,
        HeaderValue::from_static("no-cache"),
    );
    h.insert(
        HeaderName::from_static("x-gateway-queued"),
        HeaderValue::from_static("1"),
    );
    if let Some(v) = ticket_header {
        h.insert(HeaderName::from_static("x-gateway-ticket"), v);
    }
    resp
}

#[allow(clippy::too_many_arguments)]
async fn serve(
    State(app): State<Arc<App>>,
    Path((name, rest)): Path<(String, String)>,
    peer: Option<Extension<ConnectInfo<std::net::SocketAddr>>>,
    version: Version,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    mut body: Bytes,
    watch: Option<Arc<QueueWatch>>,
) -> Response {
    let Some(up) = app.upstreams.get(&name) else {
        return error(
            StatusCode::NOT_FOUND,
            "not_found_error",
            format!("unknown upstream {name:?}"),
        );
    };
    let Some(agent) = app.agent_for(&headers) else {
        return error(
            StatusCode::UNAUTHORIZED,
            "authentication_error",
            "unknown gateway token",
        );
    };
    let path_and_query = match uri.query() {
        Some(q) => format!("/{rest}?{q}"),
        None => format!("/{rest}"),
    };
    let peek = serde_json::from_slice::<Peek>(&body).ok();
    let is_stream = peek.as_ref().is_some_and(|p| p.stream);
    let mut ctx = app.obs.as_ref().map(|obs| {
        ReqCtx::new(
            obs,
            &agent,
            &name,
            up.cfg.cluster.as_deref().unwrap_or(&name),
            &path_and_query,
            &headers,
            peer.map(|Extension(ConnectInfo(a))| a),
            http_name(version),
            peek.as_ref().and_then(|p| p.model.as_deref()).unwrap_or(""),
            is_stream,
            body.len(),
        )
    });
    if let Some(budget) = agent.token_budget {
        if app.stats.tokens_used(&agent.id) >= budget {
            if let Some(c) = ctx.as_mut() {
                c.finish(Outcome {
                    status: 429,
                    error_type: Some("gateway_budget_exceeded".into()),
                    ..Outcome::default()
                });
            }
            return error(
                StatusCode::TOO_MANY_REQUESTS,
                "gateway_budget_exceeded",
                format!("agent {:?} has used its token budget of {budget}", agent.id),
            );
        }
    }

    let url = format!("{}{}", up.base, path_and_query);
    // OpenAI-style streams report usage only when asked. Ask on the agent's
    // behalf so budgets and TPM pacing see real counts, and hide the extra
    // usage-only chunk from clients that did not request it.
    let mut strip_usage_chunk = false;
    let openai_stream = match up.cfg.kind {
        Kind::Openai => true,
        // LiteLLM also serves Anthropic-format routes; leave those bodies alone.
        Kind::Litellm => rest.ends_with("chat/completions"),
        _ => false,
    };
    if is_stream && openai_stream {
        let has_options = peek.as_ref().is_some_and(|p| p.stream_options.is_some());
        let already = peek
            .as_ref()
            .and_then(|p| p.stream_options.as_ref())
            .and_then(|o| o.include_usage)
            == Some(true);
        let rewritten = if already {
            None
        } else if has_options {
            request_stream_usage(&body)
        } else {
            splice_stream_usage(&body)
        };
        if let Some(b) = rewritten {
            body = b;
            strip_usage_chunk = true;
        }
    }
    // Fingerprint the prompt: which run this is, why its cache broke, where
    // cache markers belong. In `on` and `experiment` modes the markers are
    // spliced into the body; nothing else in it changes.
    let anthropic = rest.ends_with("messages");
    let chat = method == Method::POST && (anthropic || rest.ends_with("chat/completions"));
    let mut hints = None;
    // The prompt as cumulative block hashes, for routing by prefix cache.
    let mut prefix: Option<crate::lead::prefix::Chain> = None;
    // The request as the client sent it, kept while a rewritten one is tried.
    let mut plain_body: Option<Bytes> = None;
    if let (Some(lead), true) = (&app.lead, chat) {
        let run = headers
            .get("x-gateway-run-id")
            .and_then(|v| v.to_str().ok())
            .map(str::trim)
            .filter(|s| !s.is_empty() && s.len() <= 200);
        let model = peek.as_ref().and_then(|p| p.model.as_deref()).unwrap_or("");
        let markers_ok = accepts_cache_markers(up.cfg.kind, anthropic, model);
        // A model that turned down a rewritten request recently gets
        // requests as the client wrote them for a while.
        let rewrite_off = app
            .rewrite_off
            .lock()
            .unwrap()
            .get(&(name.clone(), model.to_string()))
            .is_some_and(|until| now_ms() < *until);
        if let Some(p) = lead.prepare(
            &name,
            anthropic,
            markers_ok,
            &agent.id,
            run,
            &body,
            now_ms(),
        ) {
            if p.body != body {
                if rewrite_off {
                    if let Some(c) = ctx.as_mut() {
                        c.put("rewrite", serde_json::json!("off"));
                    }
                } else {
                    plain_body = Some(std::mem::replace(&mut body, p.body));
                }
            }
            if let Some(c) = ctx.as_mut() {
                if p.inferred {
                    c.put("run_id", serde_json::json!(p.run_id));
                }
                for (k, v) in p.dims {
                    c.put(k, v);
                }
                c.lead_fin = Some(p.fin);
            }
            prefix = Some(p.prefix);
            hints = Some((p.run_id, p.cost, p.deadline_ms, p.risk, p.affinity));
        }
    }
    if prefix.is_none() && chat && up.sched.wants_prefix() {
        // No lead engine: the prompt is still needed to route by prefix.
        prefix = crate::lead::fingerprint::fingerprint(&body, anthropic)
            .map(|f| f.blocks.iter().map(|b| (b.cum, b.tok)).collect());
    }
    if let Some(c) = ctx.as_mut() {
        c.lead = app.lead.clone();
    }
    // Rough input-token estimate (~4 bytes/token) for TPM pacing; corrected
    // with the real count once the response reports usage.
    let cost = (body.len() / 4) as u32;
    let priority = headers
        .get("x-gateway-priority")
        .and_then(|v| v.to_str().ok())
        .or(agent.priority.as_deref());
    if let Some(c) = ctx.as_mut() {
        // The kind of work, for usage reports: who waits for the answer.
        let kind = match priority {
            Some("interactive") => "interactive",
            Some("batch") => "batch",
            _ => "normal",
        };
        c.put("priority", kind.into());
        // What the caller calls this work. Free text from outside: keep
        // only a short label.
        let label = headers
            .get("x-gateway-request-kind")
            .and_then(|v| v.to_str().ok())
            .or(agent.kind.as_deref())
            .map(request_kind)
            .unwrap_or_default();
        c.put("request_kind", label.into());
        if let Some(chain) = &prefix {
            c.put("prefix_chain", prefix_column(chain).into());
        }
    }
    let fair = Some({
        let (run, est, deadline_ms, risk, affinity) = hints.unwrap_or_else(|| {
            let est = f64::from(cost) + 500.0 * Weights::default().output;
            (agent.id.clone(), est, None, 0.0, None)
        });
        FairTicket {
            // Spend is shared out between teams, then projects, then agents.
            tenant: agent
                .team_id
                .clone()
                .or_else(|| agent.project_id.clone())
                .unwrap_or_else(|| agent.id.clone()),
            tenant_weight: f64::from(agent.weight.max(1)),
            run: format!("{}\u{1}{run}", agent.id),
            cost: est,
            deadline_ms,
            risk,
            class: match priority {
                Some("interactive") => 0,
                Some("batch") => 2,
                _ => 1,
            },
            model: peek
                .as_ref()
                .and_then(|p| p.model.clone())
                .unwrap_or_default(),
            affinity,
            prefix: prefix.filter(|_| up.sched.wants_prefix()),
            alts: stand_ins(
                &up.cfg.model_groups,
                agent.model_swap,
                peek.as_ref().and_then(|p| p.model.as_deref()),
            ),
            avoid: None,
        }
    });

    let model = peek.as_ref().and_then(|p| p.model.as_deref()).unwrap_or("");
    let reservation = if app.limits.is_empty() {
        None
    } else {
        // Reserve what the call is expected to use; the response settles it.
        let price = app.price(model);
        let max_out = peek
            .as_ref()
            .and_then(|p| {
                p.max_tokens
                    .or(p.max_completion_tokens)
                    .or(p.max_output_tokens)
            })
            .unwrap_or(0) as f64;
        let estimate = Amount {
            requests: 1.0,
            tokens: f64::from(cost),
            usd: price.map_or(0.0, |p| f64::from(cost) * p.input / 1e6),
            tool_calls: 0.0,
        };
        let strict_extra = Amount {
            tokens: max_out,
            usd: price.map_or(0.0, |p| max_out * p.output / 1e6),
            ..Amount::default()
        };
        match app.limits.admit(
            &App::subject(&agent, &name, model),
            estimate,
            strict_extra,
            crate::obs::now_ms(),
        ) {
            Ok(r) => Some(r),
            Err(denied) => {
                app.stats.agent(&agent.id, |a| a.limited += 1);
                if let Some(c) = ctx.as_mut() {
                    c.finish(Outcome {
                        status: 429,
                        error_type: Some("gateway_limit_exceeded".into()),
                        ..Outcome::default()
                    });
                }
                return limit_response(&denied);
            }
        }
    };

    let mut fwd = forward_headers(&headers, up);
    if let (Kind::Litellm, Some(c)) = (up.cfg.kind, ctx.as_ref()) {
        // Lets LiteLLM's own spend logs group and tag the same way we do.
        let session = [c.dim("session_id"), c.dim("run_id"), c.trace_id.as_str()]
            .into_iter()
            .find(|s| !s.is_empty())
            .unwrap_or("")
            .to_string();
        let tags: Vec<String> = [
            ("project", "project_id"),
            ("team", "team_id"),
            ("agent", "agent_id"),
        ]
        .into_iter()
        .filter(|(_, k)| !c.dim(k).is_empty() && !c.dim(k).contains(','))
        .map(|(label, k)| format!("{label}:{}", c.dim(k)))
        .collect();
        for (k, v) in [
            ("x-litellm-trace-id", session),
            ("x-litellm-tags", tags.join(",")),
        ] {
            if let (false, Ok(v)) = (v.is_empty(), HeaderValue::from_str(&v)) {
                fwd.insert(HeaderName::from_static(k), v);
            }
        }
    }
    if let (Some(alias), Some(c)) = (&up.cfg.key_alias, ctx.as_mut()) {
        c.litellm_key_default = alias.clone();
    }
    if let Some(c) = ctx.as_mut() {
        c.provider = crate::obs::genai::provider(
            up.cfg.kind,
            up.cfg.provider.as_deref(),
            &up.base,
            peek.as_ref().and_then(|p| p.model.as_deref()).unwrap_or(""),
        );
        (c.server_address, c.server_port) = up.server.clone();
        c.max_tokens = peek.as_ref().and_then(|p| {
            p.max_tokens
                .or(p.max_completion_tokens)
                .or(p.max_output_tokens)
        });
    }
    app.stats.agent(&agent.id, |a| {
        a.requests += 1;
        if is_stream {
            a.streams += 1;
        }
    });

    let req = Outgoing {
        app: &app,
        up,
        agent: &agent,
        method,
        url,
        path: path_and_query.clone(),
        headers: fwd,
        body,
        cost,
        fair,
        anthropic,
        ctx,
        reservation,
        model: model.to_string(),
        anthropic_format: path_and_query.contains("/messages"),
        max_wait: headers
            .get("x-gateway-max-wait-ms")
            .and_then(|v| v.to_str().ok()?.parse().ok())
            .or(agent.max_wait_ms)
            .map(Duration::from_millis),
        watch,
        plain_body,
        fell_back: false,
    };

    let cache_mode = headers
        .get("x-gateway-cache")
        .and_then(|v| v.to_str().ok())
        .map(str::to_ascii_lowercase);
    let wants_cache = match cache_mode.as_deref() {
        Some("on") => true,
        Some("off") => false,
        _ => app.cfg.cache.default_on,
    };
    let mut req = req;
    let key = cache::key(&name, &path_and_query, &req.body);
    // Refuse a request the agent keeps repeating. Checked only on the way
    // upstream, so repeats answered from the cache never count.
    let looping = |ctx: &mut Option<ReqCtx>| -> Option<Response> {
        let n = app
            .loop_guard
            .as_ref()?
            .check(&agent.id, key, Instant::now())?;
        if let Some(c) = ctx.as_mut() {
            c.finish(Outcome {
                status: 429,
                error_type: Some("gateway_loop_detected".into()),
                ..Outcome::default()
            });
        }
        app.stats.agent(&agent.id, |a| a.errors += 1);
        Some(error(
            StatusCode::TOO_MANY_REQUESTS,
            "gateway_loop_detected",
            format!(
                "agent {:?} sent the same request {n} times in a row; change the request or wait {}s",
                agent.id, app.cfg.loop_guard.window_secs
            ),
        ))
    };
    if let (Some(c), true) = (&app.cache, wants_cache) {
        let served = |how: &'static str, hit: &Cached, ctx: &mut Option<ReqCtx>| {
            app.stats.agent(&agent.id, |a| a.cache_hits += 1);
            if let Some(c) = ctx.as_mut() {
                c.cache = how;
                c.finish(Outcome {
                    status: hit.status.as_u16(),
                    complete: true,
                    resp_bytes: hit.body.len() as u64,
                    ..Outcome::default()
                });
            }
            cached_response(hit, how)
        };
        match c.lookup(key) {
            Lookup::Hit(hit) => return served("hit", &hit, &mut req.ctx),
            Lookup::Wait(rx) => {
                if let Some(shared) = cache::wait(rx).await {
                    return served("shared", &shared, &mut req.ctx);
                }
                // The leader failed; fall through and try ourselves.
            }
            Lookup::Leader(leader) => {
                if let Some(refused) = looping(&mut req.ctx) {
                    return refused;
                }
                if let Some(c) = req.ctx.as_mut() {
                    c.cache = "miss";
                }
                let limit = app.cfg.cache.max_body_bytes;
                return match req.send().await {
                    // A stream goes to the client as it arrives and is kept
                    // on the side; later identical requests get it replayed.
                    Ok(sent) if is_stream => sent.stream(strip_usage_chunk, Some((leader, limit))),
                    Ok(sent) => {
                        let (resp, cached) = sent.buffer(&limit).await;
                        leader.complete(cached);
                        resp
                    }
                    Err(resp) => *resp,
                };
            }
        }
    }

    if let Some(refused) = looping(&mut req.ctx) {
        return refused;
    }
    match req.send().await {
        Ok(sent) => sent.stream(strip_usage_chunk, None),
        Err(resp) => *resp,
    }
}

/// Whether the model behind a request takes the cache parameters the
/// gateway can add. A proxy such as LiteLLM serves many providers on one
/// route and most of them reject parameters they do not know: Gemini
/// answers 400 to `prompt_cache_key`.
fn accepts_cache_markers(kind: Kind, anthropic_format: bool, model: &str) -> bool {
    let m = model.to_ascii_lowercase();
    let name = m.rsplit('/').next().unwrap_or(&m);
    if anthropic_format {
        // `cache_control` markers: Claude models on the Messages API.
        kind == Kind::Anthropic || name.starts_with("claude")
    } else {
        // `prompt_cache_key`: OpenAI's own models only.
        let openai = ["gpt-", "o1", "o3", "o4", "chatgpt"]
            .iter()
            .any(|p| name.starts_with(p));
        openai && (kind == Kind::Openai || !m.contains('/') || m.starts_with("openai/"))
    }
}

/// Set `stream_options.include_usage = true` unless the client already did.
/// Returns the rewritten body, or `None` when nothing changed.
fn request_stream_usage(body: &[u8]) -> Option<Bytes> {
    let mut v: serde_json::Value = serde_json::from_slice(body).ok()?;
    let obj = v.as_object_mut()?;
    let opts = obj
        .entry("stream_options")
        .or_insert_with(|| serde_json::json!({}));
    let opts = opts.as_object_mut()?;
    if opts.get("include_usage").and_then(|b| b.as_bool()) == Some(true) {
        return None;
    }
    opts.insert("include_usage".into(), true.into());
    serde_json::to_vec(&v).ok().map(Bytes::from)
}

/// The common case of [`request_stream_usage`]: the body has no
/// `stream_options` key, so the option can be spliced in right after the
/// opening brace without parsing and re-serialising a prompt that may be
/// megabytes long. Every other byte of the body is forwarded untouched.
fn splice_stream_usage(body: &[u8]) -> Option<Bytes> {
    const INSERT: &[u8] = br#""stream_options":{"include_usage":true},"#;
    let open = body.iter().position(|b| !b.is_ascii_whitespace())?;
    if body[open] != b'{' {
        return None;
    }
    let mut out = Vec::with_capacity(body.len() + INSERT.len());
    out.extend_from_slice(&body[..=open]);
    out.extend_from_slice(INSERT);
    out.extend_from_slice(&body[open + 1..]);
    Some(Bytes::from(out))
}

/// Removes OpenAI usage-only SSE events (`"choices": []` plus `usage`) from a
/// stream, passing every other byte through unchanged. Used only when the
/// gateway itself turned on `include_usage`.
#[derive(Default)]
struct UsageChunkFilter {
    pending: Vec<u8>,
    skip_blank: bool,
}

impl UsageChunkFilter {
    fn feed(&mut self, chunk: &Bytes) -> Bytes {
        // Nearly every chunk is whole events with no usage in them: hand the
        // same buffer on without copying or parsing.
        if self.pending.is_empty()
            && !self.skip_blank
            && chunk.last() == Some(&b'\n')
            && !crate::usage::has_usage(chunk)
        {
            return chunk.clone();
        }
        self.pending.extend_from_slice(chunk);
        let Some(last_nl) = self.pending.iter().rposition(|&b| b == b'\n') else {
            return Bytes::new();
        };
        let rest = self.pending.split_off(last_nl + 1);
        let complete = std::mem::replace(&mut self.pending, rest);
        let mut out = Vec::with_capacity(complete.len());
        for line in complete.split_inclusive(|&b| b == b'\n') {
            let content = line.strip_suffix(b"\n").unwrap_or(line);
            let content = content.strip_suffix(b"\r").unwrap_or(content);
            if self.skip_blank {
                self.skip_blank = false;
                if content.is_empty() {
                    continue;
                }
            }
            if is_usage_only(content) {
                self.skip_blank = true;
                continue;
            }
            out.extend_from_slice(line);
        }
        Bytes::from(out)
    }

    fn flush(&mut self) -> Bytes {
        Bytes::from(std::mem::take(&mut self.pending))
    }
}

fn is_usage_only(line: &[u8]) -> bool {
    let Some(data) = line.strip_prefix(b"data:") else {
        return false;
    };
    if !crate::usage::has_usage(data) {
        return false;
    }
    let Ok(v) = serde_json::from_slice::<serde_json::Value>(data.trim_ascii()) else {
        return false;
    };
    v.get("usage").is_some_and(|u| !u.is_null())
        && v.get("choices")
            .and_then(|c| c.as_array())
            .is_some_and(|c| c.is_empty())
}

fn forward_headers(incoming: &HeaderMap, up: &Upstream) -> HeaderMap {
    let mut out = HeaderMap::with_capacity(incoming.len() + 2);
    for (k, v) in incoming {
        let n = k.as_str();
        if HOP_BY_HOP.contains(&n) || STRIP_REQUEST.contains(&n) || n.starts_with("x-gateway-") {
            continue;
        }
        out.append(k.clone(), v.clone());
    }
    for (k, v) in &up.extra {
        out.insert(k.clone(), v.clone());
    }
    if let Some((k, v)) = &up.auth {
        out.insert(k.clone(), v.clone());
    }
    out
}

fn response_headers(h: &HeaderMap) -> HeaderMap {
    let mut out = HeaderMap::with_capacity(h.len());
    for (k, v) in h {
        if !HOP_BY_HOP.contains(&k.as_str()) {
            out.append(k.clone(), v.clone());
        }
    }
    out
}

fn cached_response(c: &Cached, how: &'static str) -> Response {
    let mut r = Response::new(Body::from(c.body.clone()));
    *r.status_mut() = c.status;
    *r.headers_mut() = c.headers.clone();
    r.headers_mut()
        .insert("x-gateway-cache", HeaderValue::from_static(how));
    r
}

struct Outgoing<'a> {
    app: &'a Arc<App>,
    up: &'a Upstream,
    agent: &'a Arc<AgentInfo>,
    method: Method,
    url: String,
    headers: HeaderMap,
    body: Bytes,
    cost: u32,
    ctx: Option<ReqCtx>,
    reservation: Option<Reservation>,
    model: String,
    anthropic_format: bool,
    /// Path and query, appended to the chosen backend's base URL.
    path: String,
    fair: Option<FairTicket>,
    anthropic: bool,
    /// Longest the caller said it will wait for a slot.
    max_wait: Option<Duration>,
    watch: Option<Arc<QueueWatch>>,
    /// The body before the gateway rewrote it, to fall back on when the
    /// upstream turns the rewritten one down.
    plain_body: Option<Bytes>,
    fell_back: bool,
}

struct Sent {
    sent_at: Instant,
    anthropic: bool,
    app: Arc<App>,
    upstream: String,
    agent: String,
    cost: u32,
    resp: reqwest::Response,
    lease: Lease,
    /// Time spent waiting for a slot, summed over attempts.
    queue_ms: f64,
    /// Where the request stood when it joined the queue.
    outlook: Outlook,
    /// Time from sending the final attempt to the upstream's response headers.
    upstream_ms: f64,
    attempts: u32,
    ctx: Option<ReqCtx>,
    reservation: Option<Reservation>,
    model: String,
    anthropic_format: bool,
    /// Cost LiteLLM reported for the call, when it did.
    reported_cost: Option<f64>,
}

/// Other models of the same group, when this agent allows a stand-in.
fn stand_ins(groups: &[Vec<String>], allowed: bool, model: Option<&str>) -> Vec<String> {
    let Some(model) = model.filter(|_| allowed) else {
        return Vec::new();
    };
    groups
        .iter()
        .filter(|g| g.iter().any(|m| m == model))
        .flatten()
        .filter(|m| m.as_str() != model)
        .cloned()
        .collect()
}

/// The request with its top-level `"model"` replaced; every other byte stays
/// as the client sent it.
fn swap_model(body: &Bytes, model: &str) -> Option<Bytes> {
    use crate::lead::json::{members, ws};
    let start = ws(body, 0);
    let (_, value) = members(body, start)?
        .into_iter()
        .find(|(k, _)| &body[k.clone()] == b"model")?;
    let quoted = serde_json::to_string(model).ok()?;
    let mut out = Vec::with_capacity(body.len() + quoted.len());
    out.extend_from_slice(&body[..value.start]);
    out.extend_from_slice(quoted.as_bytes());
    out.extend_from_slice(&body[value.end..]);
    Some(Bytes::from(out))
}

fn retry_after(h: &HeaderMap) -> Option<Duration> {
    let num = |name: &str| h.get(name)?.to_str().ok()?.trim().parse::<f64>().ok();
    let secs = num("retry-after-ms")
        .map(|ms| ms / 1000.0)
        .or_else(|| num("retry-after"))?;
    (secs.is_finite() && secs >= 0.0).then(|| Duration::from_secs_f64(secs.min(120.0)))
}

impl Outgoing<'_> {
    fn backoff(&self, attempt: u32) -> Duration {
        let r = &self.app.cfg.retry;
        let exp = r
            .base_ms
            .saturating_mul(1u64 << attempt.min(16))
            .min(r.max_ms);
        let jitter: f64 = rand::rng().random_range(0.5..1.0);
        Duration::from_millis((exp as f64 * jitter) as u64)
    }

    /// Queue for a slot and send, retrying transient failures. Retries happen
    /// only before any response byte reaches the client, so they are invisible
    /// to it. Rate-limit responses pause the whole upstream, not just this
    /// request, so other agents stop hammering the same limit.
    async fn send(mut self) -> Result<Sent, Box<Response>> {
        let max = self.app.cfg.retry.max_retries;
        let name = &self.up.cfg.name;
        let mut attempt = 0u32;
        let mut queue_total = 0.0;
        let mut outlook = Outlook::default();
        // The server that failed the previous attempt.
        let mut avoid: Option<usize> = None;
        loop {
            let queued_at = Instant::now();
            if let Some(c) = self.ctx.as_mut() {
                c.pre_ms.get_or_insert(ms_between(c.t0, queued_at));
            }
            // The caller's own patience, if it stated one, bounds the wait
            // as well as the upstream's setting.
            // Held batch work has its own, longer limit instead of the
            // queue's.
            let held = self
                .up
                .sched
                .holds(self.fair.as_ref().map_or(1, |f| f.class));
            let limit = if held {
                Some(self.up.cfg.hold_timeout_secs)
            } else {
                self.up.cfg.queue_timeout_secs
            };
            let patience = match (limit.map(Duration::from_secs), self.max_wait) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (a, b) => a.or(b),
            };
            if let (true, Some(c)) = (held, self.ctx.as_mut()) {
                c.put("held", true.into());
            }
            let entered = self.up.sched.try_enqueue(
                &self.agent.id,
                Ticket {
                    weight: self.agent.weight,
                    max_inflight: self.agent.max_concurrency,
                    cost: self.cost,
                    bytes: self.body.len(),
                    fair: self.fair.clone().map(|mut f| {
                        f.avoid = avoid;
                        f
                    }),
                },
                Admission {
                    max_queue: self.up.cfg.max_queue,
                    max_bytes: self.up.cfg.max_queue_bytes,
                    evict: self.up.cfg.overflow == "evict",
                    max_wait: patience.filter(|_| self.up.cfg.admit_by_wait),
                    shed: self.up.cfg.shed_from.map(|from| Shed {
                        from,
                        ceiling: self.up.cfg.shed_ceiling,
                        floor: self.up.cfg.shed_floor,
                    }),
                    refuse_down: self.up.cfg.refuse_when_down,
                },
            );
            let (rx, seen, place) = match entered {
                Ok(v) => v,
                Err(Refused::Full(o)) => {
                    return Err(self.shed("gateway_queue_full", "queue is full", o, 1));
                }
                Err(Refused::Shed(o)) => {
                    return Err(self.shed(
                        "gateway_shed_early",
                        "the queue is filling up and more important work goes first",
                        o,
                        2,
                    ));
                }
                Err(Refused::HoldFull(o)) => {
                    return Err(self.shed(
                        "gateway_hold_full",
                        "as many batch requests as allowed are held already",
                        o,
                        30,
                    ));
                }
                Err(Refused::NoServer) => {
                    return Err(self.shed(
                        "gateway_no_server",
                        "no server behind it serves this model",
                        Outlook::default(),
                        30,
                    ));
                }
                Err(Refused::ServersDown(back)) => {
                    return Err(self.shed(
                        "gateway_servers_down",
                        "every server for this model is out; none has answered lately",
                        Outlook::default(),
                        back.as_secs().clamp(1, 30),
                    ));
                }
                Err(Refused::WaitTooLong(o)) => {
                    // Come back when the queue has had time to shrink to what
                    // this caller can wait for.
                    let over = o.wait.saturating_sub(patience.unwrap_or_default());
                    let retry = over.as_secs().max(1);
                    return Err(self.shed(
                        "gateway_wait_too_long",
                        "the expected wait is longer than this request allows",
                        o,
                        retry,
                    ));
                }
            };
            if attempt == 0 {
                outlook = seen;
            }
            if let Some(w) = &self.watch {
                w.set(Some(Waiting {
                    sched: self.up.sched.clone(),
                    place: place.clone(),
                    since: queued_at,
                    held,
                }));
            }
            let granted = match patience {
                Some(limit) => match tokio::time::timeout(limit, rx).await {
                    Ok(r) => r,
                    // Dropping the receiver takes the request out of the queue.
                    Err(_) => {
                        let o = self.up.sched.queue_status(&place);
                        // Waiting on servers that are all out is not the
                        // same as waiting in a long queue.
                        let model = self.fair.as_ref().map_or("", |f| f.model.as_str());
                        let kind = if !self.up.sched.any_server_up(model) {
                            "gateway_servers_down"
                        } else if held {
                            "gateway_hold_timeout"
                        } else {
                            "gateway_queue_timeout"
                        };
                        return Err(self.shed(kind, "waited too long for a slot", o, 1));
                    }
                },
                None => rx.await,
            };
            if let Some(w) = &self.watch {
                w.set(None);
            }
            let lease = match granted {
                Ok(Ok(lease)) => lease,
                Ok(Err(_evicted)) => {
                    let o = self.up.sched.queue_status(&place);
                    return Err(self.shed(
                        "gateway_queue_evicted",
                        "the queue is full and this request made room for higher-priority work",
                        o,
                        o.wait.as_secs().max(1),
                    ));
                }
                Err(_) => {
                    if let Some(c) = self.ctx.as_mut() {
                        c.finish(Outcome {
                            status: 503,
                            error_type: Some("gateway_shutting_down".into()),
                            ..Outcome::default()
                        });
                    }
                    return Err(Box::new(error(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "overloaded_error",
                        "gateway shutting down",
                    )));
                }
            };
            let queue_ms = queued_at.elapsed().as_secs_f64() * 1000.0;
            queue_total += queue_ms;
            let sent_at = Instant::now();
            let attempt_id = new_span_id();
            let mut headers = self.headers.clone();
            // Under the fair policy the lease says which backend to use.
            let url = match lease.backend_url() {
                Some(base) => format!("{base}{}", self.path),
                None => self.url.clone(),
            };
            if let Some(c) = self.ctx.as_mut() {
                c.backend = lease.backend();
                if let Some(b) = lease.backend_name() {
                    c.put("backend", serde_json::json!(b));
                }
                if let Some((warm, total)) = lease.warm() {
                    c.put("prefix_warm_tokens", serde_json::json!(warm));
                    c.put("prefix_prompt_tokens", serde_json::json!(total));
                }
                c.queue_ms += queue_ms;
                c.attempts = attempt + 1;
                c.upstream_req_bytes = self.body.len() as u64;
                c.span(
                    new_span_id(),
                    "gateway.queue",
                    "internal",
                    queued_at,
                    sent_at,
                    true,
                    serde_json::json!({ "attempt": attempt + 1 }),
                );
                // LiteLLM continues our trace under this attempt.
                if self.up.cfg.kind == Kind::Litellm {
                    if let Ok(v) = HeaderValue::from_str(&c.traceparent(&attempt_id)) {
                        headers.insert(HeaderName::from_static("traceparent"), v);
                    }
                }
            }
            // A stand-in server took the request: ask it for its own model.
            let swapped = lease.model().and_then(|m| {
                let b = swap_model(&self.body, m)?;
                if let Some(c) = self.ctx.as_mut() {
                    c.put("model_swapped_to", serde_json::json!(m));
                }
                Some(b)
            });
            let mut lease = lease;
            let sending = self
                .up
                .client
                .request(self.method.clone(), &url)
                .headers(headers)
                .body(swapped.unwrap_or_else(|| self.body.clone()))
                .send();
            // (what went wrong, could not connect, ran out of time)
            let res: Result<reqwest::Response, (String, bool, bool)> =
                match self.up.cfg.first_byte_timeout_secs {
                    Some(secs) => {
                        match tokio::time::timeout(Duration::from_secs(secs), sending).await {
                            Ok(r) => r.map_err(|e| (e.to_string(), e.is_connect(), e.is_timeout())),
                            // Dropping the request closes it at the server.
                            Err(_) => Err((format!("no response within {secs} s"), false, true)),
                        }
                    }
                    None => sending
                        .await
                        .map_err(|e| (e.to_string(), e.is_connect(), e.is_timeout())),
                };
            let answered_at = Instant::now();
            self.app.stats.upstream(name, |u| u.requests += 1);
            match res {
                Ok(resp) => {
                    let status = resp.status();
                    let code = status.as_u16();
                    self.app
                        .stats
                        .upstream(name, |u| *u.status.entry(code).or_default() += 1);
                    let rate_limited = code == 429 || code == 529;
                    if rate_limited {
                        self.app.stats.upstream(name, |u| u.rate_limited += 1);
                    }
                    let hint = resp
                        .headers()
                        .get("x-should-retry")
                        .and_then(|v| v.to_str().ok())
                        .map(|v| v == "true");
                    let retryable =
                        hint.unwrap_or(matches!(code, 408 | 429 | 500 | 502 | 503 | 504 | 529));
                    let attempt_attrs = serde_json::json!({
                        "attempt": attempt + 1,
                        "status": code,
                        "upstream": name,
                        "upstream_ip": resp.remote_addr().map(|a| a.to_string()),
                        "http": http_name(resp.version()),
                        "ttfb_ms": ms_between(sent_at, answered_at),
                    });
                    // The upstream turned down a request the gateway had
                    // rewritten: send what the client wrote instead, once.
                    if matches!(code, 400 | 422) && !self.fell_back {
                        if let Some(plain) = self.plain_body.take() {
                            drop(resp);
                            drop(lease);
                            self.body = plain;
                            self.fell_back = true;
                            self.app.stats.upstream(name, |u| u.rewrite_rejected += 1);
                            if let Some(c) = self.ctx.as_mut() {
                                c.put("rewrite", serde_json::json!("rejected"));
                                c.lead_fin = None;
                                c.span(
                                    attempt_id,
                                    "gateway.attempt",
                                    "client",
                                    sent_at,
                                    answered_at,
                                    false,
                                    attempt_attrs,
                                );
                            }
                            continue;
                        }
                    }
                    if self.fell_back && status.is_success() {
                        // The plain request went through, so the rewrite was
                        // the problem: leave this model alone for ten minutes.
                        tracing::warn!(upstream = %name, model = %self.model,
                            "upstream rejected a rewritten request; rewriting is off for this model for 10 min");
                        self.app
                            .rewrite_off
                            .lock()
                            .unwrap()
                            .insert((name.clone(), self.model.clone()), now_ms() + 600_000);
                    }
                    if retryable && attempt < max {
                        let wait =
                            retry_after(resp.headers()).unwrap_or_else(|| self.backoff(attempt));
                        drop(resp);
                        if !rate_limited && code >= 500 {
                            lease.failed();
                        }
                        let backend = lease.backend();
                        if !rate_limited {
                            avoid = backend;
                        }
                        drop(lease);
                        self.note_retry(code, wait);
                        if let Some(c) = self.ctx.as_mut() {
                            c.span(
                                attempt_id,
                                "gateway.attempt",
                                "client",
                                sent_at,
                                answered_at,
                                false,
                                attempt_attrs,
                            );
                        }
                        if rate_limited {
                            // Shared pause for whoever said so (the model,
                            // the server, or the whole upstream); our
                            // re-queue waits for it with everyone else.
                            self.up.sched.slow_down(backend, &self.model, wait);
                        } else {
                            tokio::time::sleep(wait).await;
                            if let Some(c) = self.ctx.as_mut() {
                                c.retry_wait_ms += wait.as_secs_f64() * 1000.0;
                                c.span(
                                    new_span_id(),
                                    "gateway.backoff",
                                    "internal",
                                    answered_at,
                                    Instant::now(),
                                    true,
                                    serde_json::json!({ "after_status": code }),
                                );
                            }
                        }
                        attempt += 1;
                        continue;
                    }
                    if !status.is_success() {
                        self.app.stats.agent(&self.agent.id, |a| a.errors += 1);
                    }
                    self.app
                        .stats
                        .agent(&self.agent.id, |a| a.last_status = code);
                    let upstream_ms = ms_between(sent_at, answered_at);
                    self.app
                        .stats
                        .record_timing(&self.agent.id, queue_ms, upstream_ms);
                    if let Some(c) = self.ctx.as_mut() {
                        c.upstream_ttfb_ms = upstream_ms;
                        c.upstream_ip = resp
                            .remote_addr()
                            .map(|a| a.ip().to_string())
                            .unwrap_or_default();
                        c.upstream_instance = resp
                            .remote_addr()
                            .map(|a| a.to_string())
                            .unwrap_or_default();
                        c.upstream_http = http_name(resp.version()).into();
                        c.absorb_response_headers(resp.headers());
                        c.open_attempt = Some((attempt_id, sent_at, attempt_attrs));
                    }
                    let reported_cost = resp
                        .headers()
                        .get("x-litellm-response-cost")
                        .and_then(|v| v.to_str().ok())
                        .and_then(|v| v.trim().parse::<f64>().ok())
                        .filter(|c| c.is_finite() && *c > 0.0);
                    return Ok(Sent {
                        reservation: self.reservation.take(),
                        model: std::mem::take(&mut self.model),
                        anthropic_format: self.anthropic_format,
                        reported_cost,
                        ctx: self.ctx.take(),
                        queue_ms: queue_total,
                        outlook,
                        upstream_ms,
                        attempts: attempt + 1,
                        app: self.app.clone(),
                        upstream: name.clone(),
                        agent: self.agent.id.clone(),
                        cost: self.cost,
                        resp,
                        lease,
                        sent_at,
                        anthropic: self.anthropic,
                    });
                }
                Err((e, is_connect, is_timeout)) => {
                    avoid = lease.backend();
                    if let (true, Some(b)) = (is_connect, lease.backend()) {
                        // Send new work elsewhere while this one is down.
                        self.up.sched.fail_backend(b, Duration::from_secs(10));
                    } else {
                        lease.failed();
                    }
                    drop(lease);
                    self.app.stats.upstream(name, |u| u.errors += 1);
                    let kind = if is_timeout {
                        "timeout"
                    } else if is_connect {
                        "connect"
                    } else {
                        "transport"
                    };
                    if let Some(c) = self.ctx.as_mut() {
                        c.span(attempt_id, "gateway.attempt", "client", sent_at, answered_at, false, serde_json::json!({ "attempt": attempt + 1, "upstream": name, "error": kind }));
                    }
                    if (is_connect || is_timeout) && attempt < max {
                        let wait = self.backoff(attempt);
                        self.note_retry(0, wait);
                        tokio::time::sleep(wait).await;
                        if let Some(c) = self.ctx.as_mut() {
                            c.retry_wait_ms += wait.as_secs_f64() * 1000.0;
                        }
                        attempt += 1;
                        continue;
                    }
                    self.app.stats.agent(&self.agent.id, |a| {
                        a.errors += 1;
                        a.last_status = 502;
                    });
                    tracing::warn!(upstream = %name, error = %e, "upstream request failed");
                    if let Some(c) = self.ctx.as_mut() {
                        c.finish(Outcome {
                            status: 502,
                            error_type: Some(format!("upstream_{kind}")),
                            ..Outcome::default()
                        });
                    }
                    return Err(Box::new(error(
                        StatusCode::BAD_GATEWAY,
                        "api_connection_error",
                        format!("upstream {name:?} unreachable: {e}"),
                    )));
                }
            }
        }
    }

    /// Refuse a request the upstream queue cannot take. 503 with a short
    /// `retry-after` is what SDKs back off and retry on.
    fn shed(&mut self, kind: &str, why: &str, o: Outlook, retry_secs: u64) -> Box<Response> {
        let name = &self.up.cfg.name;
        self.app.stats.upstream(name, |u| u.shed += 1);
        self.app.stats.agent(&self.agent.id, |a| {
            a.errors += 1;
            a.last_status = 503;
        });
        if let Some(c) = self.ctx.as_mut() {
            c.finish(Outcome {
                status: 503,
                error_type: Some(kind.into()),
                ..Outcome::default()
            });
        }
        // Servers that are out are not a long queue: say so.
        let message = if kind == "gateway_servers_down" && o.ahead == 0 {
            format!("upstream {name:?} cannot serve this now: {why}")
        } else {
            format!(
                "upstream {name:?} is overloaded: {why} ({} ahead, about {:.1} s)",
                o.ahead,
                o.wait.as_secs_f64()
            )
        };
        let mut r = error(StatusCode::SERVICE_UNAVAILABLE, "overloaded_error", message);
        // Callers told the same moment all return at that moment; spread
        // them, and never ask for less than the queue is expected to take
        // to make room (capped so a caller is not sent away for minutes).
        let base = (retry_secs as f64).max(o.wait.as_secs_f64().min(30.0) / 2.0);
        let retry_secs = (base * rand::rng().random_range(1.0..1.5)).ceil() as u64;
        let h = r.headers_mut();
        // Why it was refused, in three words a caller can branch on:
        // `saturated` is worth retrying after `retry-after`, `no_server`
        // only once somebody has fixed or added a server.
        let reason = match kind {
            "gateway_no_server" | "gateway_servers_down" => "no_server",
            _ => "saturated",
        };
        for (k, v) in [
            ("x-gateway-reject-reason", reason.to_string()),
            ("retry-after", retry_secs.to_string()),
            ("x-gateway-queue-ahead", o.ahead.to_string()),
            ("x-gateway-expected-wait-ms", o.wait.as_millis().to_string()),
            (
                "x-gateway-expected-wait-max-ms",
                o.wait_max.as_millis().to_string(),
            ),
        ] {
            if let Ok(v) = HeaderValue::from_str(&v) {
                h.insert(HeaderName::from_static(k), v);
            }
        }
        Box::new(r)
    }

    fn note_retry(&self, code: u16, wait: Duration) {
        tracing::info!(
            upstream = %self.up.cfg.name,
            agent = %self.agent.id,
            status = code,
            wait_ms = wait.as_millis() as u64,
            "retrying"
        );
        self.app
            .stats
            .upstream(&self.up.cfg.name, |u| u.retries += 1);
        self.app.stats.agent(&self.agent.id, |a| a.retries += 1);
    }
}

impl Sent {
    /// Per-request timing headers so clients can tell gateway queueing apart
    /// from upstream latency.
    fn timing_headers(&self, h: &mut HeaderMap) {
        let mut put = |name: &'static str, v: String| {
            if let Ok(v) = HeaderValue::from_str(&v) {
                h.insert(HeaderName::from_static(name), v);
            }
        };
        put("x-gateway-queue-ms", format!("{:.1}", self.queue_ms));
        put("x-gateway-queue-ahead", self.outlook.ahead.to_string());
        put(
            "x-gateway-expected-wait-ms",
            self.outlook.wait.as_millis().to_string(),
        );
        put("x-gateway-upstream-ms", format!("{:.1}", self.upstream_ms));
        put("x-gateway-attempts", self.attempts.to_string());
        if let Some(b) = self.lease.backend_name() {
            put("x-gateway-backend", b.to_string());
        }
        // Estimated prompt tokens the server already held / in the prompt.
        if let Some((warm, total)) = self.lease.warm() {
            put("x-gateway-prefix-warm", format!("{warm}/{total}"));
        }
        // A stand-in model answered: say which, the body's own `model` may not.
        if let Some(m) = self.lease.model() {
            put("x-gateway-model", m.to_string());
        }
        if let Some(c) = &self.ctx {
            put("x-gateway-trace-id", c.trace_id.clone());
        }
    }

    fn finalizer(&mut self) -> impl FnOnce(Usage, bool) + Send + 'static {
        let (app, upstream, agent, cost) = (
            self.app.clone(),
            self.upstream.clone(),
            self.agent.clone(),
            self.cost,
        );
        let reservation = self.reservation.take();
        let (model, anthropic_format, reported) = (
            std::mem::take(&mut self.model),
            self.anthropic_format,
            self.reported_cost,
        );
        move |u: Usage, complete: bool| {
            app.stats.record_usage(&agent, u);
            if let Some(r) = reservation {
                let billed = crate::obs::bill(app.price(&model), u, anthropic_format);
                r.settle(
                    billed.total_input + u.output,
                    reported.or(billed.cost_usd).unwrap_or(0.0),
                    crate::obs::now_ms(),
                );
            }
            if !complete {
                app.stats.agent(&agent, |a| a.client_aborts += 1);
            }
            if u.input > 0 {
                if let Some(up) = app.upstreams.get(&upstream) {
                    up.sched
                        .limiter
                        .lock()
                        .unwrap()
                        .correct_tokens(cost, u.input);
                }
            }
        }
    }

    /// Pass the body through as it arrives. Holding the lease inside the body
    /// keeps the slot busy until the stream ends; if the client disconnects
    /// the body is dropped, which closes the upstream request at once.
    fn stream(mut self, strip_usage_chunk: bool, keep: Option<(Leader, usize)>) -> Response {
        let status = self.resp.status();
        let mut headers = response_headers(self.resp.headers());
        let capture = keep.map(|(leader, limit)| {
            headers.insert(
                HeaderName::from_static("x-gateway-cache"),
                HeaderValue::from_static("miss"),
            );
            Capture {
                leader,
                limit,
                headers: headers.clone(),
                body: Vec::new(),
            }
        });
        self.timing_headers(&mut headers);
        let sse = headers
            .get(http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.starts_with("text/event-stream"));
        let done = Box::new(self.finalizer());
        let tapped = Tapped {
            inner: Box::pin(self.resp.bytes_stream()),
            parser: UsageParser::new(sse),
            filter: (sse && strip_usage_chunk).then(UsageChunkFilter::default),
            lease: Some(self.lease),
            done: Some(done),
            ctx: self.ctx.take(),
            capture,
            est_input: u64::from(self.cost),
            status: status.as_u16(),
            bytes: 0,
            chunks: 0,
            first_byte: None,
            first_token: None,
            last_chunk: None,
            max_gap_ms: 0.0,
            sent_at: self.sent_at,
            anthropic: self.anthropic,
        };
        let mut r = Response::new(Body::from_stream(tapped));
        *r.status_mut() = status;
        *r.headers_mut() = headers;
        r
    }

    /// Read the whole body (for the cache path).
    async fn buffer(mut self, limit: &usize) -> (Response, Option<Arc<Cached>>) {
        let mut ctx = self.ctx.take();
        let status = self.resp.status();
        let mut headers = response_headers(self.resp.headers());
        let mut timing = HeaderMap::new();
        self.timing_headers(&mut timing);
        let done = self.finalizer();
        let mut lease = self.lease;
        let (sent_at, anthropic) = (self.sent_at, self.anthropic);
        let body = match self.resp.bytes().await {
            Ok(b) => b,
            Err(e) => {
                lease.record(0.0, 0.0, 0.0, false, None);
                drop(lease);
                done(Usage::default(), false);
                if let Some(c) = ctx.as_mut() {
                    c.finish(Outcome {
                        status: status.as_u16(),
                        error_type: Some("upstream_body_error".into()),
                        ..Outcome::default()
                    });
                }
                return (
                    error(
                        StatusCode::BAD_GATEWAY,
                        "api_connection_error",
                        e.to_string(),
                    ),
                    None,
                );
            }
        };
        let mut p = UsageParser::new(false);
        p.feed(&body);
        let usage = p.finish();
        lease.cache_report(usage.cache_read, fresh_input(usage, anthropic));
        lease.record(
            usage.output as f64,
            ms_between(sent_at, Instant::now()),
            0.0,
            status.as_u16() < 500,
            real_cost(usage, anthropic),
        );
        drop(lease);
        done(usage, true);
        if let Some(c) = ctx.as_mut() {
            c.finish(Outcome {
                meta: p.meta().clone(),
                status: status.as_u16(),
                usage,
                complete: true,
                resp_bytes: body.len() as u64,
                chunks: 1,
                error_type: (!status.is_success()).then(|| "upstream_status".into()),
                ..Outcome::default()
            });
        }
        headers.insert(
            HeaderName::from_static("x-gateway-cache"),
            HeaderValue::from_static("miss"),
        );
        let cached = (body.len() <= *limit).then(|| {
            Arc::new(Cached {
                status,
                headers: headers.clone(),
                body: body.clone(),
            })
        });
        headers.extend(timing);
        let mut r = Response::new(Body::from(body));
        *r.status_mut() = status;
        *r.headers_mut() = headers;
        (r, cached)
    }
}

type ByteStream = Pin<Box<dyn Stream<Item = reqwest::Result<Bytes>> + Send>>;

/// A streamed response being kept for the cache while it is relayed.
struct Capture {
    leader: Leader,
    limit: usize,
    headers: HeaderMap,
    body: Vec<u8>,
}

struct Tapped {
    inner: ByteStream,
    parser: UsageParser,
    filter: Option<UsageChunkFilter>,
    lease: Option<Lease>,
    done: Option<Box<dyn FnOnce(Usage, bool) + Send>>,
    ctx: Option<ReqCtx>,
    capture: Option<Capture>,
    /// Input tokens guessed from the request size, for streams that end
    /// before the upstream reports usage.
    est_input: u64,
    status: u16,
    bytes: u64,
    chunks: u32,
    first_byte: Option<Instant>,
    first_token: Option<Instant>,
    last_chunk: Option<Instant>,
    max_gap_ms: f64,
    sent_at: Instant,
    anthropic: bool,
}

/// What a request really cost, in the scheduler's weighted tokens.
fn real_cost(u: Usage, anthropic: bool) -> Option<f64> {
    let w = Weights::default();
    let fresh = if anthropic {
        u.input
    } else {
        u.input.saturating_sub(u.cache_read)
    };
    (u.input + u.output > 0).then_some(
        fresh as f64
            + u.cache_read as f64 * w.read
            + u.cache_write as f64 * w.write_5m
            + u.output as f64 * w.output,
    )
}

/// Prompt tokens the server read rather than reused. OpenAI-style usage
/// counts reused tokens inside the prompt total; Anthropic-style does not.
fn fresh_input(u: Usage, anthropic: bool) -> u64 {
    if anthropic {
        u.input
    } else {
        u.input.saturating_sub(u.cache_read)
    }
}

impl Tapped {
    fn finish(&mut self, complete: bool, stream_error: bool) {
        let mut usage = self.parser.finish();
        if let Some(mut lease) = self.lease.take() {
            let now = Instant::now();
            // Generation speed is measured from the first token, so prompt
            // processing and queueing inside the server do not blur it.
            let first = self.first_token.or(self.first_byte);
            if complete && !stream_error {
                lease.cache_report(usage.cache_read, fresh_input(usage, self.anthropic));
            }
            lease.record(
                usage.output as f64,
                ms_between(first.unwrap_or(self.sent_at), now),
                first.map_or(0.0, |f| ms_between(self.sent_at, f)),
                self.status < 500 && !stream_error,
                real_cost(usage, self.anthropic),
            );
        }
        // A client that hangs up mid-stream never sees the usage chunk, and
        // neither do we, but the provider still bills the prompt.
        let estimated = !complete
            && !stream_error
            && self.status < 400
            && self.first_byte.is_some()
            && usage.input == 0
            && usage.output == 0;
        if estimated {
            usage.input = self.est_input;
            // What was generated before the caller left is billed too.
            usage.output = self.parser.output_estimate();
        }
        if let Some(cap) = self.capture.take() {
            let whole = complete && !stream_error && self.status == 200;
            cap.leader.complete(whole.then(|| {
                Arc::new(Cached {
                    status: StatusCode::OK,
                    headers: cap.headers,
                    body: Bytes::from(cap.body),
                })
            }));
        }
        if let Some(f) = self.done.take() {
            f(usage, complete);
        }
        if let Some(mut c) = self.ctx.take() {
            c.finish(Outcome {
                meta: self.parser.meta().clone(),
                status: self.status,
                usage,
                complete,
                resp_bytes: self.bytes,
                chunks: self.chunks,
                first_byte: self.first_byte,
                first_token: self.first_token,
                max_chunk_gap_ms: self.max_gap_ms,
                error_type: if stream_error {
                    Some("upstream_stream_error".into())
                } else {
                    (self.status >= 400).then(|| "upstream_status".into())
                },
                usage_estimated: estimated,
            });
        }
    }

    /// Remember what was sent to the client, up to the cache's size limit.
    fn keep(&mut self, sent: &Bytes) {
        if let Some(cap) = self.capture.as_mut() {
            if cap.body.len() + sent.len() > cap.limit {
                // Too big to cache; dropping the leader releases any waiters.
                self.capture = None;
            } else {
                cap.body.extend_from_slice(sent);
            }
        }
    }

    /// Timing bookkeeping for one upstream body chunk.
    fn observe(&mut self, len: usize) {
        if self.ctx.is_none() {
            return;
        }
        let now = Instant::now();
        self.bytes += len as u64;
        self.chunks += 1;
        self.first_byte.get_or_insert(now);
        if let Some(prev) = self.last_chunk.replace(now) {
            self.max_gap_ms = self.max_gap_ms.max(ms_between(prev, now));
        }
        if self.first_token.is_none() && self.parser.content_seen() {
            self.first_token = Some(now);
        }
    }
}

impl Stream for Tapped {
    type Item = reqwest::Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        loop {
            match this.inner.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(b))) => {
                    this.parser.feed(&b);
                    this.observe(b.len());
                    let Some(f) = this.filter.as_mut() else {
                        this.keep(&b);
                        return Poll::Ready(Some(Ok(b)));
                    };
                    let out = f.feed(&b);
                    if !out.is_empty() {
                        this.keep(&out);
                        return Poll::Ready(Some(Ok(out)));
                    }
                    // Whole chunk held back or filtered; read on.
                }
                Poll::Ready(Some(Err(e))) => {
                    this.finish(false, true);
                    return Poll::Ready(Some(Err(e)));
                }
                Poll::Ready(None) => {
                    if let Some(rest) = this.filter.as_mut().map(UsageChunkFilter::flush) {
                        if !rest.is_empty() {
                            this.keep(&rest);
                            return Poll::Ready(Some(Ok(rest)));
                        }
                    }
                    this.finish(true, false);
                    return Poll::Ready(None);
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl Drop for Tapped {
    fn drop(&mut self) {
        self.finish(false, false);
    }
}

/// A request-kind label as it goes into the log: lower case, letters,
/// digits, `-` and `_`, at most 32 characters.
fn request_kind(raw: &str) -> String {
    raw.trim()
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '_')
        .take(32)
        .map(|c| c.to_ascii_lowercase())
        .collect()
}

/// The prompt's block chain for the request log, `hash:tokens` per block
/// (tokens cumulative): the first block and the last three. Enough for a
/// replay to know which requests share a start and how long it is, without
/// keeping any of the prompt.
fn prefix_column(chain: &[(u64, u32)]) -> String {
    let keep = |i: usize| i == 0 || i + 3 >= chain.len();
    chain
        .iter()
        .enumerate()
        .filter(|(i, _)| keep(*i))
        .map(|(_, (h, t))| format!("{h:016x}:{t}"))
        .collect::<Vec<_>>()
        .join(",")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_parameters_only_go_to_models_that_take_them() {
        // OpenAI format through LiteLLM: only OpenAI's own models.
        for (model, ok) in [
            ("gpt-4o-mini", true),
            ("openai/gpt-4.1", true),
            ("o3-mini", true),
            ("gemini-2.5-flash-lite", false),
            ("gemini/gemini-2.5-flash", false),
            ("claude-haiku-4-5", false),
            ("azure/gpt-4o", false),
            ("qwen2.5-coder", false),
            ("", false),
        ] {
            assert_eq!(
                accepts_cache_markers(Kind::Litellm, false, model),
                ok,
                "{model}"
            );
        }
        assert!(!accepts_cache_markers(Kind::Openai, false, "local-llama"));
        // Anthropic format: Claude models, or the Anthropic API itself.
        assert!(accepts_cache_markers(
            Kind::Litellm,
            true,
            "anthropic/claude-sonnet-4-5"
        ));
        assert!(!accepts_cache_markers(
            Kind::Litellm,
            true,
            "gemini-2.5-pro"
        ));
        assert!(accepts_cache_markers(Kind::Anthropic, true, "anything"));

        // A model that would reject them gets its bytes back untouched.
        let lead = crate::lead::engine::Lead::new(
            crate::lead::engine::LeadConfig {
                cache_points: "on".into(),
                ..Default::default()
            },
            None,
            0,
        )
        .unwrap();
        let body = Bytes::from(
            r#"{"model":"gemini-2.5-flash-lite","messages":[{"role":"user","content":"hi"}]}"#,
        );
        let kept = lead
            .prepare("up", false, false, "a", None, &body, 0)
            .unwrap();
        assert_eq!(kept.body, body);
        let keyed = lead
            .prepare("up", false, true, "a", None, &body, 1)
            .unwrap();
        assert!(std::str::from_utf8(&keyed.body)
            .unwrap()
            .contains("prompt_cache_key"));
    }

    #[test]
    fn injects_include_usage_once() {
        let b = request_stream_usage(br#"{"model":"m","stream":true}"#).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
        assert_eq!(v["stream_options"]["include_usage"], true);
        assert!(request_stream_usage(&b).is_none());
        let b =
            request_stream_usage(br#"{"stream":true,"stream_options":{"include_usage":false}}"#)
                .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
        assert_eq!(v["stream_options"]["include_usage"], true);
    }

    #[test]
    fn splices_include_usage_without_touching_the_rest() {
        let body = br#" {"model":"m","stream":true,"messages":[{"role":"user","content":"hi"}]}"#;
        let b = splice_stream_usage(body).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
        assert_eq!(v["stream_options"]["include_usage"], true);
        assert_eq!(v["messages"][0]["content"], "hi");
        assert!(b.ends_with(&body[2..]));
        assert!(splice_stream_usage(b"[1]").is_none());
    }

    #[test]
    fn filter_drops_only_usage_chunk_across_splits() {
        let body = concat!(
            "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}],\"usage\":null}\n\n",
            "data: {\"choices\":[{\"delta\":{}}],\"usage\":{\"prompt_tokens\":2}}\n\n",
            "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":2,\"completion_tokens\":1}}\n\n",
            "data: [DONE]\n\n",
        );
        let expected = concat!(
            "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}],\"usage\":null}\n\n",
            "data: {\"choices\":[{\"delta\":{}}],\"usage\":{\"prompt_tokens\":2}}\n\n",
            "data: [DONE]\n\n",
        );
        for size in [1, 5, 13, body.len()] {
            let mut f = UsageChunkFilter::default();
            let mut out = Vec::new();
            for c in body.as_bytes().chunks(size) {
                out.extend_from_slice(&f.feed(&Bytes::copy_from_slice(c)));
            }
            out.extend_from_slice(&f.flush());
            assert_eq!(
                String::from_utf8(out).unwrap(),
                expected,
                "chunk size {size}"
            );
        }
    }

    #[test]
    fn a_request_kind_is_cut_down_to_a_short_label() {
        assert_eq!(request_kind(" IDE "), "ide");
        assert_eq!(request_kind("code review\n{x}"), "codereviewx");
        assert_eq!(request_kind(&"a".repeat(100)).len(), 32);
    }

    #[test]
    fn the_logged_chain_keeps_the_first_block_and_the_last_three() {
        let chain: Vec<(u64, u32)> = (1..=6).map(|i| (i, i as u32 * 100)).collect();
        assert_eq!(
            prefix_column(&chain),
            "0000000000000001:100,0000000000000004:400,0000000000000005:500,0000000000000006:600"
        );
        assert_eq!(
            prefix_column(&chain[..2]),
            "0000000000000001:100,0000000000000002:200"
        );
    }
}
