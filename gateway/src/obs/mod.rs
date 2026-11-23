//! Request logs, traces and cost attribution.
//!
//! The proxy path builds one request row and a handful of spans per call and
//! hands them to a bounded queue; a dedicated thread batches them into the
//! storage backend. Nothing on the request path waits for storage: when the
//! queue is full, rows are dropped and counted.

pub mod client;
pub mod export;
pub mod genai;
pub mod http;
pub mod insight;
pub mod metrics;
pub mod otlp;
pub mod query;
pub mod roi;
pub mod store;
pub mod trace;

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::mpsc::{sync_channel, RecvTimeoutError, SyncSender, TrySendError};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Result};
use serde::Deserialize;
use serde_json::{json, Value};

use store::{Backend, Memory, Row, Table};

const FLUSH_EVERY: Duration = Duration::from_millis(500);
const FLUSH_ROWS: usize = 256;
const DAY_MS: i64 = 86_400_000;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObsConfig {
    #[serde(default = "yes")]
    pub enabled: bool,
    /// `memory` keeps recent rows in RAM. `tdb` persists to Super Shotgun TDB
    /// under `dir`; it is provided by the `pybox-gateway-tdb` binary.
    #[serde(default = "memory")]
    pub backend: String,
    #[serde(default = "obs_dir")]
    pub dir: String,
    /// Rows kept by the memory backend.
    #[serde(default = "memory_rows")]
    pub memory_rows: usize,
    #[serde(default = "queue")]
    pub queue: usize,
    #[serde(default = "d90")]
    pub retention_days: u32,
    #[serde(default = "d14")]
    pub span_retention_days: u32,
    /// Bearer token LiteLLM uses to export spans to `/gateway/otlp/v1/traces`.
    /// Falls back to the admin token when unset.
    #[serde(default)]
    pub otlp_token: Option<String>,
    /// USD per million tokens, keyed by model name or prefix. Used when the
    /// upstream did not report a cost for the call.
    #[serde(default)]
    pub prices: BTreeMap<String, Price>,
    /// Push the gateway's spans to an OpenTelemetry backend.
    #[serde(default)]
    pub otlp_export: Option<export::OtlpExportConfig>,
}

fn yes() -> bool {
    true
}
fn memory() -> String {
    "memory".into()
}
fn obs_dir() -> String {
    "obs".into()
}
fn memory_rows() -> usize {
    50_000
}
fn queue() -> usize {
    8_192
}
fn d90() -> u32 {
    90
}
fn d14() -> u32 {
    14
}

impl Default for ObsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            backend: memory(),
            dir: obs_dir(),
            memory_rows: memory_rows(),
            queue: queue(),
            retention_days: 90,
            span_retention_days: 14,
            otlp_token: None,
            prices: BTreeMap::new(),
            otlp_export: None,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Deserialize, serde::Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Price {
    #[serde(default)]
    pub input: f64,
    #[serde(default)]
    pub output: f64,
    /// Defaults to `input` when omitted (no cache discount).
    #[serde(default)]
    pub cache_read: Option<f64>,
    #[serde(default)]
    pub cache_write: Option<f64>,
}

/// Price for `model` from a config table: exact name, else the longest
/// matching prefix (or `provider/<name>` suffix).
pub fn table_price(prices: &BTreeMap<String, Price>, model: &str) -> Option<Price> {
    prices.get(model).copied().or_else(|| {
        prices
            .iter()
            .filter(|(k, _)| model.starts_with(k.as_str()) || model.ends_with(&format!("/{k}")))
            .max_by_key(|(k, _)| k.len())
            .map(|(_, p)| *p)
    })
}

/// Token totals and price-table cost of one call.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Billed {
    /// Input tokens that were not served from the provider's prompt cache.
    pub fresh_input: u64,
    /// All input tokens, cached ones included.
    pub total_input: u64,
    /// `None` when the model has no price.
    pub cost_usd: Option<f64>,
    /// What prompt caching saved against paying full input price.
    pub cache_saved_usd: Option<f64>,
}

/// OpenAI-style `prompt_tokens` already include cached tokens;
/// Anthropic-style `input_tokens` do not.
pub fn bill(price: Option<Price>, u: crate::usage::Usage, anthropic_format: bool) -> Billed {
    let (fresh, total_in) = if anthropic_format {
        (u.input, u.input + u.cache_read + u.cache_write)
    } else {
        (u.input.saturating_sub(u.cache_read), u.input)
    };
    let priced = price.map(|p| {
        let (cr, cw) = (
            p.cache_read.unwrap_or(p.input),
            p.cache_write.unwrap_or(p.input),
        );
        let cost = (fresh as f64 * p.input
            + u.output as f64 * p.output
            + u.cache_read as f64 * cr
            + u.cache_write as f64 * cw)
            / 1e6;
        (cost, u.cache_read as f64 * (p.input - cr) / 1e6)
    });
    Billed {
        fresh_input: fresh,
        total_input: total_in,
        cost_usd: priced.map(|p| p.0),
        cache_saved_usd: priced.map(|p| p.1),
    }
}

enum Msg {
    Rows(Table, Vec<Row>),
    Flush(SyncSender<()>),
}

pub struct Obs {
    pub cfg: ObsConfig,
    pub backend: Arc<dyn Backend>,
    tx: SyncSender<Msg>,
    dropped: AtomicU64,
    written: Arc<AtomicU64>,
    write_errors: Arc<AtomicU64>,
    pub metrics: metrics::Metrics,
    exporter: Option<export::Exporter>,
    /// The latest finished requests, for matching what callers report.
    pub recent: client::Recent,
}

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64)
}

impl Obs {
    pub fn new(cfg: ObsConfig) -> Result<Option<Arc<Self>>> {
        if !cfg.enabled {
            return Ok(None);
        }
        let backend: Arc<dyn Backend> = match cfg.backend.as_str() {
            "memory" => Arc::new(Memory::new(cfg.memory_rows)),
            "tdb" => bail!(
                "obs.backend = \"tdb\" needs the pybox-gateway-tdb binary (gateway/tdb-backend), which links Super Shotgun TDB"
            ),
            other => bail!("unknown obs.backend {other:?} (memory | tdb)"),
        };
        Self::with_backend(cfg, backend).map(Some)
    }

    pub fn with_backend(cfg: ObsConfig, backend: Arc<dyn Backend>) -> Result<Arc<Self>> {
        let (tx, rx) = sync_channel::<Msg>(cfg.queue.max(16));
        let written = Arc::new(AtomicU64::new(0));
        let write_errors = Arc::new(AtomicU64::new(0));
        let (b, w, e) = (backend.clone(), written.clone(), write_errors.clone());
        let (keep_req, keep_span) = (
            i64::from(cfg.retention_days) * DAY_MS,
            i64::from(cfg.span_retention_days) * DAY_MS,
        );
        std::thread::Builder::new()
            .name("obs-writer".into())
            .spawn(move || {
                let mut pending: [(Table, Vec<Row>); 3] = [
                    (Table::Requests, Vec::new()),
                    (Table::Spans, Vec::new()),
                    (Table::Tools, Vec::new()),
                ];
                let mut last_flush = Instant::now();
                let mut last_retain: Option<Instant> = None;
                let flush = |pending: &mut [(Table, Vec<Row>); 3]| {
                    for (table, rows) in pending.iter_mut() {
                        if rows.is_empty() {
                            continue;
                        }
                        let n = rows.len() as u64;
                        match b.append(*table, std::mem::take(rows)) {
                            Ok(()) => {
                                w.fetch_add(n, Relaxed);
                            }
                            Err(err) => {
                                e.fetch_add(n, Relaxed);
                                tracing::warn!(error = %err, rows = n, "obs write failed");
                            }
                        }
                    }
                };
                loop {
                    let wait = FLUSH_EVERY.saturating_sub(last_flush.elapsed());
                    let mut waiter = None;
                    match rx.recv_timeout(wait) {
                        Ok(Msg::Rows(t, mut rows)) => {
                            let i = pending.iter().position(|p| p.0 == t).unwrap_or(0);
                            let slot = &mut pending[i].1;
                            rows.iter_mut().for_each(store::round_ms);
                            slot.append(&mut rows);
                        }
                        Ok(Msg::Flush(done)) => waiter = Some(done),
                        Err(RecvTimeoutError::Timeout) => {}
                        Err(RecvTimeoutError::Disconnected) => {
                            flush(&mut pending);
                            return;
                        }
                    }
                    let total: usize = pending.iter().map(|p| p.1.len()).sum();
                    if waiter.is_some()
                        || total >= FLUSH_ROWS
                        || last_flush.elapsed() >= FLUSH_EVERY
                    {
                        flush(&mut pending);
                        last_flush = Instant::now();
                    }
                    if let Some(done) = waiter {
                        let _ = done.send(());
                    }
                    if last_retain.is_none_or(|t| t.elapsed() > Duration::from_secs(3600)) {
                        last_retain = Some(Instant::now());
                        let now = now_ms();
                        match b.retain(now - keep_req, now - keep_span) {
                            Ok(what) => tracing::debug!(%what, "obs retention"),
                            Err(err) => tracing::warn!(error = %err, "obs retention failed"),
                        }
                    }
                }
            })?;
        let exporter = match &cfg.otlp_export {
            Some(e) if tokio::runtime::Handle::try_current().is_ok() => {
                tracing::info!(endpoint = %e.endpoint, "exporting spans over OTLP/HTTP");
                Some(export::Exporter::spawn(e)?)
            }
            Some(_) => {
                tracing::warn!("obs.otlp_export needs a Tokio runtime; export is off");
                None
            }
            None => None,
        };
        tracing::info!(backend = backend.name(), "observability store ready");
        Ok(Arc::new(Self {
            cfg,
            backend,
            tx,
            dropped: AtomicU64::new(0),
            written,
            write_errors,
            metrics: metrics::Metrics::default(),
            exporter,
            recent: client::Recent::default(),
        }))
    }

    /// Queue rows for the writer. Never blocks.
    pub fn emit(&self, table: Table, rows: Vec<Row>) {
        if rows.is_empty() {
            return;
        }
        let n = rows.len() as u64;
        match self.tx.try_send(Msg::Rows(table, rows)) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => {
                self.dropped.fetch_add(n, Relaxed);
            }
        }
    }

    /// Store spans the gateway itself produced, and hand them to the OTLP
    /// exporter when one is configured. Spans received from elsewhere go
    /// through [`emit`](Self::emit) only, so nothing is exported twice.
    pub fn emit_own_spans(&self, spans: Vec<Row>) {
        if let Some(x) = &self.exporter {
            x.offer(spans.clone());
        }
        self.emit(Table::Spans, spans);
    }

    /// Wait until everything queued so far is stored. For tests and shutdown.
    pub async fn flush(&self) {
        let (done, wait) = sync_channel(1);
        if self.tx.send(Msg::Flush(done)).is_ok() {
            let _ = tokio::task::spawn_blocking(move || wait.recv_timeout(Duration::from_secs(10)))
                .await;
        }
    }

    pub fn price(&self, model: &str) -> Option<Price> {
        if let Some(p) = self.stored_price(model) {
            return Some(p);
        }
        table_price(&self.cfg.prices, model)
    }

    /// Prices set at runtime through the API win over the config file.
    fn stored_price(&self, model: &str) -> Option<Price> {
        let list = self.backend.meta_list("price").ok()?;
        list.into_iter()
            .find(|(id, _)| id == model)
            .and_then(|(_, v)| serde_json::from_value(v).ok())
    }

    pub fn info(&self) -> Value {
        json!({
            "store": self.backend.info(),
            "written": self.written.load(Relaxed),
            "dropped": self.dropped.load(Relaxed),
            "write_errors": self.write_errors.load(Relaxed),
            "retention_days": self.cfg.retention_days,
            "span_retention_days": self.cfg.span_retention_days,
            "otlp_export": self.exporter.as_ref().map(export::Exporter::info),
        })
    }
}
