//! Ties the pieces to the request path: what happens before a request is
//! queued (`prepare`) and after it finishes (`finish`).

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use axum::body::Bytes;
use serde::Deserialize;
use serde_json::{json, Map, Value};

use super::cachectl;
use super::cachemodel::{CacheRules, CacheUse, ProviderCache, Weights};
use super::diagnose::{diagnose, Cause};
use super::experiment::{self, shadow_clear, treated, ArmStats, Guard};
use super::fingerprint::{fingerprint, Ttl};
use super::ledger::{Entry, Event, HoursRange, Ledger};
use super::runs::RunTracker;
use super::trim::{self, TrimCfg};
use crate::obs::store::{Backend, Table};
use crate::obs::Price;

pub const CACHE_EXPERIMENT: &str = "cache_points";
pub const TRIM_EXPERIMENT: &str = "tool_results";
const EVENT_KIND: &str = "roi_event";

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct LeadConfig {
    /// Fingerprint requests, infer runs, diagnose cache breaks and keep the
    /// cost ledger. Costs a fraction of a millisecond per request.
    pub enabled: bool,
    /// What to do with prompt-cache breakpoints the client did not set:
    /// `off` (nothing), `shadow` (measure what adding them would save),
    /// `on` (add them), `experiment` (add them for a share of runs and
    /// compare cost per run).
    pub cache_points: String,
    /// Shortest prefix the provider will cache.
    pub min_cache_tokens: u32,
    /// Share of runs treated in `experiment` mode.
    pub experiment_fraction: f64,
    /// What to do with old tool results in long runs: `off`, `shadow`
    /// (estimate the saving), `on` (replace all but the most recent with a
    /// one-line note), `experiment` (do so for a share of runs). `on`
    /// changes what the model sees.
    pub tool_results: String,
    /// Most recent tool results always sent in full.
    pub keep_tool_results: usize,
    /// Tool results smaller than this are left alone.
    pub min_tool_result_bytes: usize,
    /// Days of request rows folded into the cost ledger at startup.
    pub rollup_days: u32,
    /// Worth of an accepted deliverable of a kind when nobody stated one.
    pub value_defaults: HashMap<String, HoursRange>,
}

impl Default for LeadConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            cache_points: "shadow".into(),
            min_cache_tokens: 1024,
            experiment_fraction: 0.5,
            tool_results: "shadow".into(),
            keep_tool_results: 3,
            min_tool_result_bytes: 512,
            rollup_days: 90,
            value_defaults: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Off,
    Shadow,
    On,
    Experiment,
}

impl LeadConfig {
    pub fn trim_mode(&self) -> Result<Mode, String> {
        match self.tool_results.as_str() {
            "off" => Ok(Mode::Off),
            "shadow" => Ok(Mode::Shadow),
            "on" => Ok(Mode::On),
            "experiment" => Ok(Mode::Experiment),
            o => Err(format!(
                "unknown lead.tool_results {o:?} (off | shadow | on | experiment)"
            )),
        }
    }

    pub fn mode(&self) -> Result<Mode, String> {
        if !(0.0..=1.0).contains(&self.experiment_fraction) {
            return Err("lead.experiment_fraction must be between 0 and 1".into());
        }
        self.trim_mode()?;
        match self.cache_points.as_str() {
            "off" => Ok(Mode::Off),
            "shadow" => Ok(Mode::Shadow),
            "on" => Ok(Mode::On),
            "experiment" => Ok(Mode::Experiment),
            o => Err(format!(
                "unknown lead.cache_points {o:?} (off | shadow | on | experiment)"
            )),
        }
    }
}

/// Per-upstream models of the provider's prompt cache.
#[derive(Default)]
struct Caches {
    /// As the requests were really sent.
    sent: ProviderCache,
    /// As they would have gone without the gateway touching them.
    base: ProviderCache,
    /// As they would have gone with the gateway's breakpoints on every one.
    plan: ProviderCache,
    /// Cached input tokens the model predicted and the upstream reported,
    /// both decayed. A local server reports none, and its cache does not
    /// survive another run using the slot, so the prediction is not trusted
    /// beyond what the upstream has shown.
    read_pred: f64,
    read_seen: f64,
}

impl Caches {
    /// Share of predicted cache reads the upstream really reported, 1.0
    /// until there is enough to judge.
    fn effect(&self) -> f64 {
        if self.read_pred < 4000.0 {
            1.0
        } else {
            (self.read_seen / self.read_pred).clamp(0.0, 1.0)
        }
    }
}

#[derive(Default, Clone, Copy)]
struct CauseAgg {
    requests: u64,
    lost_tokens: u64,
    lost_usd: f64,
}

#[derive(Default)]
struct Inner {
    runs: RunTracker,
    caches: HashMap<String, Caches>,
    causes: BTreeMap<(String, &'static str), CauseAgg>,
    /// Per agent: (follow-up turns, turns whose system prompt had changed),
    /// both decayed so old behaviour fades.
    system_churn: HashMap<String, (f64, f64)>,
}

/// What `prepare` learned, carried to `finish`.
pub struct Fin {
    pub agent: String,
    pub run: String,
    upstream: String,
    ttl_base: Ttl,
    sent: CacheUse,
    base: CacheUse,
    plan: Option<(CacheUse, Ttl)>,
    cause: Cause,
    lost_tokens: u32,
    est_tokens: u32,
    /// Input tokens of the request as the client sent it.
    orig_tokens: u32,
    applied: bool,
}

pub struct Prepared {
    /// The body to send: the client's bytes, with cache markers spliced in
    /// when the mode says so. Nothing else is touched.
    pub body: Bytes,
    pub run_id: String,
    pub inferred: bool,
    /// When the cache entry this request would read expires.
    pub deadline_ms: Option<i64>,
    /// Weighted tokens lost if it expires first.
    pub risk: f64,
    /// Backend that served the run's previous turn.
    pub affinity: Option<usize>,
    /// The prompt as sent, as cumulative block hashes, for routing to the
    /// server whose prefix cache holds most of it.
    pub prefix: super::prefix::Chain,
    /// Expected cost in weighted tokens.
    pub cost: f64,
    pub dims: Vec<(&'static str, Value)>,
    pub fin: Fin,
}

/// Numbers from the finished request that `finish` needs.
pub struct Done<'a> {
    pub ts: i64,
    pub project: &'a str,
    pub team: &'a str,
    pub run: &'a str,
    pub deliverable: &'a str,
    pub fresh: u64,
    pub cache_read: u64,
    pub cache_write: u64,
    pub total_in: u64,
    pub output: u64,
    pub cost_usd: f64,
    pub failed: bool,
    pub price: Option<Price>,
    pub backend: Option<usize>,
}

pub struct Lead {
    pub cfg: LeadConfig,
    mode: Mode,
    trim_mode: Mode,
    rules: CacheRules,
    weights: Weights,
    inner: Mutex<Inner>,
    ledger: Mutex<Ledger>,
    store: Option<Arc<dyn Backend>>,
    seq: AtomicU64,
}

fn text(v: Option<&Value>) -> &str {
    v.and_then(Value::as_str).unwrap_or("")
}

impl Lead {
    pub fn new(
        cfg: LeadConfig,
        store: Option<Arc<dyn Backend>>,
        now: i64,
    ) -> Result<Arc<Self>, String> {
        let mode = cfg.mode()?;
        let trim_mode = cfg.trim_mode()?;
        let mut ledger = Ledger::default();
        if let Some(b) = &store {
            let mut events: Vec<(String, Event)> = b
                .meta_list(EVENT_KIND)?
                .into_iter()
                .filter_map(|(id, v)| Some((id, serde_json::from_value(v).ok()?)))
                .collect();
            events.sort_by(|a, b| a.0.cmp(&b.0));
            for (_, e) in events {
                ledger.record(e);
            }
            const COLS: [&str; 10] = [
                "cost_usd",
                "total_tokens",
                "project_id",
                "team_id",
                "run_id",
                "deliverable_id",
                "is_error",
                "client_abort",
                "cache_lost_usd",
                "gateway_saved_usd",
            ];
            let from = now - i64::from(cfg.rollup_days) * 86_400_000;
            b.scan_cols(Table::Requests, from, now + 1, &COLS, &mut |row| {
                let num = |k: &str| row.get(k).and_then(Value::as_f64).unwrap_or(0.0);
                let flag = |k: &str| row.get(k).and_then(Value::as_bool).unwrap_or(false);
                ledger.add(&Entry {
                    ts: crate::obs::store::row_ts(row),
                    run: text(row.get("run_id")).into(),
                    deliverable: text(row.get("deliverable_id")).into(),
                    project: text(row.get("project_id")).into(),
                    team: text(row.get("team_id")).into(),
                    cost: num("cost_usd"),
                    failed: flag("is_error") || flag("client_abort"),
                    tokens: num("total_tokens") as u64,
                    cache_lost: num("cache_lost_usd"),
                    saved: num("gateway_saved_usd"),
                });
            })?;
        }
        Ok(Arc::new(Self {
            rules: CacheRules {
                min_tokens: cfg.min_cache_tokens,
                ..CacheRules::default()
            },
            weights: Weights::default(),
            mode,
            trim_mode,
            cfg,
            inner: Mutex::default(),
            ledger: Mutex::new(ledger),
            store,
            seq: AtomicU64::new(0),
        }))
    }

    pub fn mode(&self) -> Mode {
        self.mode
    }

    /// Look at a request before it is queued. `None` when the body is not a
    /// chat request the gateway understands; such requests pass untouched.
    #[allow(clippy::too_many_arguments)]
    pub fn prepare(
        &self,
        upstream: &str,
        anthropic: bool,
        // The model behind this request accepts the cache markers (or
        // `prompt_cache_key`). Others reject unknown parameters.
        markers_ok: bool,
        agent: &str,
        header_run: Option<&str>,
        body: &Bytes,
        now: i64,
    ) -> Option<Prepared> {
        let orig = fingerprint(body, anthropic)?;
        let orig_chain: Vec<(u64, u32)> = orig.blocks.iter().map(|b| (b.cum, b.tok)).collect();
        let mut guard = self.inner.lock().unwrap();
        let st = &mut *guard;
        // The run is recognised from the conversation as the client sent it.
        let seen = st.runs.observe(agent, header_run, &orig, now);
        let prev = seen.prev;
        let turn = prev.as_ref().map_or(1, |p| p.turns + 1);
        let stats = st.runs.agent(agent);
        let (follow, churn) = st.system_churn.get(agent).copied().unwrap_or((0.0, 0.0));
        let volatile = follow >= 2.0 && churn / follow >= 0.5;

        // Old tool results. The boundary of what is cleared moves only when
        // the tokens saved on later turns pay for rewriting the prompt once.
        let tcfg = TrimCfg {
            keep_last: self.cfg.keep_tool_results,
            min_bytes: self.cfg.min_tool_result_bytes,
        };
        let results = trim::tool_results(body, &orig);
        let trim_arm = match self.trim_mode {
            Mode::Experiment if !results.is_empty() => {
                if treated(TRIM_EXPERIMENT, &seen.run_id, self.cfg.experiment_fraction) {
                    "treated"
                } else {
                    "control"
                }
            }
            _ => "",
        };
        let trimming = self.trim_mode == Mode::On || trim_arm == "treated";
        let limit = trim::limit(results.len(), tcfg);
        let mut boundary = prev.as_ref().map_or(0, |p| p.trim_boundary).min(limit);
        let mut advanced = false;
        let mut trimmed: Option<(Bytes, super::fingerprint::Fingerprint, usize)> = None;
        if trimming && limit > 0 {
            let cut = |b: usize| -> Option<(Bytes, super::fingerprint::Fingerprint, usize)> {
                let (bytes, n) = trim::clear(body, &results, b, tcfg);
                (n > 0).then(|| fingerprint(&bytes, anthropic).map(|f| (bytes, f, n)))?
            };
            let stay = if boundary > 0 { cut(boundary) } else { None };
            if limit > boundary {
                if let Some(next) = cut(limit) {
                    // Where the cache would be read and written this turn.
                    let mut bps = orig.breakpoints();
                    if let Some(p) = &prev {
                        bps.extend(p.bps.iter().copied().filter(|i| *i < orig.blocks.len()));
                        if !p.bps.is_empty() {
                            bps.push(orig.blocks.len() - 1);
                        }
                    }
                    let ttl = prev.as_ref().map_or(Ttl::M5, |p| p.ttl);
                    let w = self.weights;
                    let caches = st.caches.entry(upstream.to_string()).or_default();
                    // What a cached token costs here, going by how much of
                    // the predicted caching this upstream has reported.
                    let effect = caches.effect();
                    let read_w = effect * w.read + (1.0 - effect);
                    let cache = &caches.sent;
                    let price = |f: &super::fingerprint::Fingerprint| {
                        let chain: Vec<(u64, u32)> =
                            f.blocks.iter().map(|b| (b.cum, b.tok)).collect();
                        let u = cache.peek(&chain, &bps, now, self.rules);
                        w.input_cost(u, ttl) + f64::from(u.read) * (read_w - w.read)
                    };
                    let now_stay = price(stay.as_ref().map_or(&orig, |s| &s.1));
                    let now_next = price(&next.1);
                    let fewer = f64::from(
                        stay.as_ref().map_or(&orig, |s| &s.1).total_tokens()
                            - next.1.total_tokens(),
                    );
                    // A run that has lasted this long tends to last a while
                    // longer; count on half as many turns again, and ask
                    // for twice the rewrite cost back, since a run that
                    // stops early never repays it.
                    let later_turns = f64::from(turn.clamp(1, 40)) / 2.0;
                    let per_turn = fewer * if bps.is_empty() { 1.0 } else { read_w };
                    // Where no caching shows, the server may still keep a
                    // prefix cache it does not report; move in steps so a
                    // rewrite is not forced on every turn.
                    let step = if effect < 0.5 {
                        tcfg.keep_last.max(1)
                    } else {
                        1
                    };
                    if limit - boundary >= step
                        && 2.0 * (now_next - now_stay) < per_turn * later_turns
                    {
                        boundary = limit;
                        advanced = true;
                        trimmed = Some(next);
                    }
                }
            }
            if !advanced {
                trimmed = stay;
            }
            if let Some(r) = st.runs.get_mut(&format!("{agent}\u{1}{}", seen.run_id)) {
                r.trim_boundary = boundary;
            }
        }
        let cleared_results = trimmed.as_ref().map_or(0, |t| t.2);
        let (trim_body, fp) = match trimmed {
            Some((b, f, _)) => (Some(b), f),
            None => (None, orig.clone()),
        };
        let body = trim_body.as_ref().unwrap_or(body);
        let chain: Vec<(u64, u32)> = fp.blocks.iter().map(|b| (b.cum, b.tok)).collect();
        let plan = if !markers_ok {
            cachectl::Plan {
                why: "unsupported_model",
                ..cachectl::Plan::default()
            }
        } else if anthropic {
            cachectl::plan(body, &fp, turn, stats, self.rules, self.weights, volatile)
        } else {
            let mut p = cachectl::Plan {
                why: if fp.has_cache_key {
                    "client_managed"
                } else {
                    "cache_key"
                },
                ..cachectl::Plan::default()
            };
            if let Some(ins) =
                cachectl::cache_key_insert(body, &fp, &seen.run_id).filter(|_| !fp.has_cache_key)
            {
                p.inserts.push(ins);
            }
            p
        };
        let has_plan = !plan.inserts.is_empty();
        let arm = match self.mode {
            Mode::Experiment if has_plan && anthropic => {
                if treated(CACHE_EXPERIMENT, &seen.run_id, self.cfg.experiment_fraction) {
                    "treated"
                } else {
                    "control"
                }
            }
            _ => "",
        };
        let applied = has_plan && (self.mode == Mode::On || arm == "treated");
        let client_bps = fp.breakpoints();
        let ttl_base = fp.ttl();
        let (sent_bps, ttl_sent) = if applied && anthropic {
            let mut b = client_bps.clone();
            b.extend(plan.bps.iter().copied().filter(|i| !client_bps.contains(i)));
            (b, plan.ttl)
        } else {
            (client_bps.clone(), ttl_base)
        };
        // Whether the client kept its own prefix stable.
        let dx = diagnose(prev.as_ref(), &orig, &sent_bps, now, self.rules);
        if prev.is_some() {
            let e = st.system_churn.entry(agent.to_string()).or_default();
            let changed = matches!(dx.cause, Cause::SystemVolatile | Cause::SystemChanged);
            *e = (e.0 * 0.95 + 1.0, e.1 * 0.95 + f64::from(u8::from(changed)));
        }
        let caches = st.caches.entry(upstream.to_string()).or_default();
        let sent = caches
            .sent
            .apply(&chain, &sent_bps, ttl_sent, now, self.rules);
        let base = caches
            .base
            .apply(&orig_chain, &client_bps, ttl_base, now, self.rules);
        let planned = (anthropic && has_plan).then(|| {
            (
                caches
                    .plan
                    .apply(&chain, &plan.bps, plan.ttl, now, self.rules),
                plan.ttl,
            )
        });
        st.runs
            .set_cache(agent, &seen.run_id, sent_bps.clone(), ttl_sent);
        drop(guard);

        // Scheduling hints: the entry this turn reads, and what losing it costs.
        let warm = prev.as_ref().map_or(0, |p| p.warm_tokens());
        let deadline_ms = prev
            .as_ref()
            .filter(|_| warm > 0 && sent.read > 0)
            .map(|p| p.expires_ms());
        let w = self.weights;
        let risk = f64::from(sent.read) * (w.write(ttl_sent) - w.read);
        let out_est = prev.as_ref().map_or(500.0, |p| p.out_ewma.max(1.0));
        let cost = w.input_cost(sent, ttl_sent) + out_est * w.output;
        let (cleared, clear_net) = if self.trim_mode == Mode::Shadow {
            shadow_clear(&orig, self.cfg.keep_tool_results, w.read, w.write(ttl_sent))
        } else {
            (0, 0.0)
        };

        let mut dims: Vec<(&'static str, Value)> = vec![
            ("run_inferred", json!(seen.inferred)),
            ("run_turn", json!(turn)),
            ("cache_cause", json!(dx.cause.as_str())),
            ("cache_lost_tokens", json!(dx.lost_tokens)),
            ("lead_plan", json!(plan.why)),
            ("lead_applied", json!(applied)),
            ("lead_arm", json!(arm)),
            ("prompt_blocks", json!(fp.blocks.len())),
        ];
        if !trim_arm.is_empty() {
            dims.push(("trim_arm", json!(trim_arm)));
        }
        if cleared_results > 0 {
            dims.push(("trim_cleared", json!(cleared_results)));
            dims.push(("trim_advanced", json!(advanced)));
        }
        if cleared > 0 {
            dims.push(("shadow_clear_tokens", json!(cleared)));
            dims.push(("shadow_clear_net_tokens", json!(clear_net.round())));
        }
        let body = if applied {
            cachectl::apply(body, &plan.inserts)
        } else {
            body.clone()
        };
        Some(Prepared {
            body,
            inferred: seen.inferred,
            deadline_ms,
            risk,
            affinity: prev.as_ref().and_then(|p| p.backend),
            prefix: chain.as_slice().into(),
            cost,
            dims,
            fin: Fin {
                agent: agent.to_string(),
                run: seen.run_id.clone(),
                upstream: upstream.to_string(),
                ttl_base,
                sent,
                base,
                plan: planned,
                cause: dx.cause,
                lost_tokens: dx.lost_tokens,
                est_tokens: chain.last().map_or(0, |c| c.1),
                orig_tokens: orig.total_tokens(),
                applied: applied || cleared_results > 0,
            },
            run_id: seen.run_id,
        })
    }

    /// Add cost columns to the request row and fold it into the ledger.
    /// Called for every request, with `fin` when `prepare` understood it.
    pub fn finish(&self, fin: Option<&Fin>, d: &Done, row: &mut Map<String, Value>) {
        let mut put = |k: &str, v: Value| {
            row.insert(k.to_string(), v);
        };
        let (mut lost_usd, mut saved_usd) = (0.0, 0.0);
        if let Some(f) = fin {
            // The fingerprint counts tokens roughly; scale to the real count.
            let scale = if f.est_tokens > 0 && d.total_in > 0 {
                d.total_in as f64 / f64::from(f.est_tokens)
            } else {
                1.0
            };
            let w = self.weights;
            let units = |u: CacheUse, ttl: Ttl| w.input_cost(u, ttl) * scale;
            let base_units = units(f.base, f.ttl_base);
            put(
                "pred_cache_read_tokens",
                json!((f64::from(f.sent.read) * scale).round()),
            );
            if d.total_in > 0 && !d.failed {
                let err = (f64::from(f.sent.read) * scale - d.cache_read as f64).abs()
                    / d.total_in as f64;
                put("cache_pred_err", json!((err * 1e4).round() / 1e4));
            }
            if let Some(p) = d.price {
                let per = p.input / 1e6;
                let (cr, cw) = (
                    p.cache_read.unwrap_or(p.input) / 1e6,
                    p.cache_write.unwrap_or(p.input) / 1e6,
                );
                let actual_in =
                    d.fresh as f64 * per + d.cache_read as f64 * cr + d.cache_write as f64 * cw;
                put(
                    "list_cost_usd",
                    json!(d.total_in as f64 * per + d.output as f64 * p.output / 1e6),
                );
                let base_in = base_units * per;
                put("no_gateway_input_usd", json!(base_in));
                if f.applied && !d.failed {
                    saved_usd = base_in - actual_in;
                }
                if let Some((plan, ttl)) = f.plan.filter(|_| !f.applied) {
                    put(
                        "shadow_saving_usd",
                        json!((base_units - units(plan, ttl)) * per),
                    );
                }
                if f.cause.is_break() {
                    lost_usd = f64::from(f.lost_tokens) * scale * (per - cr).max(0.0);
                }
            }
            if f.orig_tokens > f.est_tokens {
                put(
                    "gateway_saved_tokens",
                    json!((f64::from(f.orig_tokens - f.est_tokens) * scale).round()),
                );
            }
            put("gateway_saved_usd", json!(saved_usd));
            put("cache_lost_usd", json!(lost_usd));
            let mut st = self.inner.lock().unwrap();
            st.runs.note_output(&f.agent, &f.run, d.output, d.backend);
            if d.total_in > 0 && !d.failed {
                let c = st.caches.entry(f.upstream.clone()).or_default();
                c.read_pred = c.read_pred * 0.98 + f64::from(f.sent.read) * scale;
                c.read_seen = c.read_seen * 0.98 + d.cache_read as f64;
            }
            if f.cause.is_break() {
                let a = st
                    .causes
                    .entry((f.agent.clone(), f.cause.as_str()))
                    .or_default();
                a.requests += 1;
                a.lost_tokens += (f64::from(f.lost_tokens) * scale) as u64;
                a.lost_usd += lost_usd;
            }
        }
        self.ledger.lock().unwrap().add(&Entry {
            ts: d.ts,
            run: d.run.to_string(),
            deliverable: d.deliverable.to_string(),
            project: d.project.to_string(),
            team: d.team.to_string(),
            cost: d.cost_usd,
            failed: d.failed,
            tokens: d.total_in + d.output,
            cache_lost: lost_usd,
            saved: saved_usd,
        });
    }

    /// Append a deliverable event. Returns its id.
    pub fn record_event(&self, mut e: Event, now: i64) -> Result<String, String> {
        e.validate()?;
        e.recorded_at = now;
        let id = format!("{now:013}-{:06}", self.seq.fetch_add(1, Ordering::Relaxed));
        if let Some(b) = &self.store {
            b.meta_put(
                EVENT_KIND,
                &id,
                serde_json::to_value(&e).map_err(|e| e.to_string())?,
            )?;
        }
        self.ledger.lock().unwrap().record(e);
        Ok(id)
    }

    pub fn events(&self) -> Vec<Event> {
        self.ledger.lock().unwrap().events().to_vec()
    }

    pub fn roi(&self, from: i64, to: i64, as_of: i64) -> Result<Value, String> {
        let rates: HashMap<String, f64> = match &self.store {
            Some(b) => b
                .meta_list("rate")?
                .into_iter()
                .filter_map(|(id, v)| Some((id, v.get("hourly_usd")?.as_f64()?)))
                .collect(),
            None => HashMap::new(),
        };
        Ok(self
            .ledger
            .lock()
            .unwrap()
            .report(from, to, as_of, &rates, &self.cfg.value_defaults))
    }

    /// Why prompt caches broke, per agent, since the gateway started.
    pub fn cache_breaks(&self) -> Value {
        let st = self.inner.lock().unwrap();
        let mut rows: Vec<Value> = st
            .causes
            .iter()
            .map(|((agent, cause), a)| {
                json!({ "agent_id": agent, "cause": cause, "requests": a.requests,
                        "lost_tokens": a.lost_tokens, "lost_usd": a.lost_usd })
            })
            .collect();
        rows.sort_by(|a, b| b["lost_tokens"].as_u64().cmp(&a["lost_tokens"].as_u64()));
        json!({ "mode": self.cfg.cache_points, "active_runs": st.runs.len(), "breaks": rows })
    }

    /// Compare the arms of the cache-point experiment from request rows.
    /// `name` is `cache_points` or `tool_results`.
    pub fn experiment(&self, name: &str, from: i64, to: i64) -> Result<Value, String> {
        let (column, mode) = match name {
            CACHE_EXPERIMENT => ("lead_arm", &self.cfg.cache_points),
            TRIM_EXPERIMENT => ("trim_arm", &self.cfg.tool_results),
            o => {
                return Err(format!(
                    "unknown experiment {o:?} (cache_points | tool_results)"
                ))
            }
        };
        let Some(b) = &self.store else {
            return Err("no request store".into());
        };
        // run -> (arm, cost, failed, turns)
        let mut runs: HashMap<String, (bool, f64, bool, f64)> = HashMap::new();
        let cols = ["run_id", column, "cost_usd", "is_error", "agent_id"];
        b.scan_cols(Table::Requests, from, to, &cols, &mut |row| {
            let arm = text(row.get(column));
            if arm.is_empty() {
                return;
            }
            let key = format!(
                "{}\u{1}{}",
                text(row.get("agent_id")),
                text(row.get("run_id"))
            );
            let r = runs
                .entry(key)
                .or_insert((arm == "treated", 0.0, false, 0.0));
            r.1 += row.get("cost_usd").and_then(Value::as_f64).unwrap_or(0.0);
            r.2 |= row
                .get("is_error")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            r.3 += 1.0;
        })?;
        let (mut control, mut treat) = (ArmStats::default(), ArmStats::default());
        for (is_treated, cost, failed, turns) in runs.into_values() {
            if is_treated { &mut treat } else { &mut control }.add(cost, failed, turns);
        }
        let v = experiment::verdict(&control, &treat, Guard::default());
        Ok(json!({
            "experiment": name, "mode": mode,
            "control": { "runs": control.runs, "cost_per_run_usd": control.cost().0, "failure_rate": control.failure_rate(), "turns_per_run": control.turns().0 },
            "treated": { "runs": treat.runs, "cost_per_run_usd": treat.cost().0, "failure_rate": treat.failure_rate(), "turns_per_run": treat.turns().0 },
            "verdict": v,
        }))
    }
}
