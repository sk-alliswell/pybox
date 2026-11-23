//! Reports the gateway writes by itself.
//!
//! With `[reports] dir` set, once a day at `at` (local time, `tz_hours`
//! from UTC) the gateway runs the ROI and productivity report
//! ([`crate::obs::insight`]) over the day that just ended and writes it as
//! a self-contained page and as JSON: `roi-day-YYYY-MM-DD.html|json`. On
//! Mondays it also writes the seven days before as `roi-week-…`, named by
//! the week's first day. A day that is missing when the gateway starts is
//! written then, so a machine that was off at `at` does not skip it.
//!
//! The files can be opened straight from the folder, or through
//! `GET /gateway/reports` (list) and `GET /gateway/reports/{file}`.

use std::path::{Path as FsPath, PathBuf};
use std::sync::{Arc, Weak};
use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::obs::insight;
use crate::{error, obs, App};

const PAGE: &str = include_str!("obs/roi.html");
const DAY_MS: i64 = 86_400_000;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReportsConfig {
    /// Folder the reports are written to. Relative paths are taken from
    /// where the gateway runs.
    pub dir: String,
    /// Local time of day the report for the day before is written.
    #[serde(default = "default_at")]
    pub at: String,
    /// Offset of local time from UTC, in hours.
    #[serde(default = "default_tz")]
    pub tz_hours: f64,
    /// Also write a report over the last seven days every Monday.
    #[serde(default = "yes")]
    pub weekly: bool,
    /// Reports older than this many days are deleted. 0 = keep all.
    #[serde(default = "default_keep")]
    pub keep_days: u32,
    /// Passed to the report: price of one model server for an hour, used
    /// when the log carries no cost.
    #[serde(default)]
    pub server_hour_usd: Option<f64>,
    /// Passed to the report: requests a server works on at once.
    #[serde(default)]
    pub slots: Option<f64>,
}

fn default_at() -> String {
    "07:00".into()
}
fn default_tz() -> f64 {
    9.0
}
fn yes() -> bool {
    true
}
fn default_keep() -> u32 {
    90
}

impl ReportsConfig {
    /// Minutes after local midnight, or why `at` cannot be read.
    pub fn at_minutes(&self) -> Result<i64, String> {
        let bad = || format!("reports.at {:?}: expected HH:MM", self.at);
        let (h, m) = self.at.split_once(':').ok_or_else(bad)?;
        let h: i64 = h.parse().map_err(|_| bad())?;
        let m: i64 = m.parse().map_err(|_| bad())?;
        if !(0..24).contains(&h) || !(0..60).contains(&m) {
            return Err(bad());
        }
        Ok(h * 60 + m)
    }
    pub fn validate(&self) -> Result<(), String> {
        self.at_minutes()?;
        if self.dir.trim().is_empty() {
            return Err("reports.dir is empty".into());
        }
        if !(-12.0..=14.0).contains(&self.tz_hours) {
            return Err("reports.tz_hours must be between -12 and 14".into());
        }
        Ok(())
    }
    fn tz_ms(&self) -> i64 {
        (self.tz_hours * 3_600_000.0) as i64
    }
}

/// Days since 1970-01-01 of a civil date (proleptic Gregorian).
pub fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// The civil date of a day number.
pub fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (yoe + era * 400 + i64::from(m <= 2), m, d)
}

pub fn day_name(day: i64) -> String {
    let (y, m, d) = civil_from_days(day);
    format!("{y:04}-{m:02}-{d:02}")
}

pub fn parse_day(s: &str) -> Option<i64> {
    let mut it = s.split('-');
    let y: i64 = it.next()?.parse().ok()?;
    let m: i64 = it.next()?.parse().ok()?;
    let d: i64 = it.next()?.parse().ok()?;
    if it.next().is_some() || !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return None;
    }
    let day = days_from_civil(y, m, d);
    (civil_from_days(day) == (y, m, d)).then_some(day)
}

/// 0 = Monday … 6 = Sunday.
fn weekday(day: i64) -> i64 {
    (day + 3).rem_euclid(7)
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Span {
    Day,
    Week,
}

impl Span {
    fn name(self) -> &'static str {
        match self {
            Span::Day => "day",
            Span::Week => "week",
        }
    }
    fn days(self) -> i64 {
        match self {
            Span::Day => 1,
            Span::Week => 7,
        }
    }
}

fn file_stem(span: Span, first_day: i64) -> String {
    format!("roi-{}-{}", span.name(), day_name(first_day))
}

/// Which reports are due at `now_ms`: the day before the latest local day
/// whose `at` has passed, and on a Monday the week before it.
pub fn due(cfg: &ReportsConfig, now_ms: i64) -> Vec<(Span, i64)> {
    let at = cfg.at_minutes().unwrap_or(0);
    let local = now_ms + cfg.tz_ms() - at * 60_000;
    // The local day whose report time has passed.
    let today = local.div_euclid(DAY_MS);
    let mut out = vec![(Span::Day, today - 1)];
    if cfg.weekly && weekday(today) == 0 {
        out.push((Span::Week, today - 7));
    }
    out
}

/// Run the report over `span` starting at local day `first_day` and write
/// it. Returns a summary of what was written.
pub fn write(
    backend: &dyn obs::store::Backend,
    cfg: &ReportsConfig,
    span: Span,
    first_day: i64,
) -> Result<Value, String> {
    let from = first_day * DAY_MS - cfg.tz_ms();
    let to = from + span.days() * DAY_MS;
    let opts = insight::Options {
        tz_hours: cfg.tz_hours,
        server_hour_usd: cfg.server_hour_usd.filter(|p| *p > 0.0),
        slots: cfg.slots.filter(|s| *s >= 1.0),
        ..insight::Options::default()
    };
    let mut report = insight::report(backend, from, to, &opts)?;
    // Servers that went out during the window, for the record.
    let mut outages: Vec<Value> = backend
        .meta_list("health_event")
        .unwrap_or_default()
        .into_iter()
        .map(|(_, v)| v)
        .filter(|v| v["ts"].as_i64().is_some_and(|t| t >= from && t < to))
        .collect();
    outages.sort_by_key(|v| v["ts"].as_i64().unwrap_or(0));
    report["outages"] = json!(outages);
    report["written_ms"] = json!(obs::now_ms());
    report["span"] = json!(span.name());
    report["first_day"] = json!(day_name(first_day));
    let dir = PathBuf::from(&cfg.dir);
    std::fs::create_dir_all(&dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    let stem = file_stem(span, first_day);
    let text = report.to_string();
    let page = PAGE.replace("/*__INSIGHT__*/null", &text.replace("</", "<\\/"));
    for (ext, body) in [("json", &text), ("html", &page)] {
        let path = dir.join(format!("{stem}.{ext}"));
        // Write beside and rename, so a reader never sees half a file.
        let tmp = dir.join(format!(".{stem}.{ext}.tmp"));
        std::fs::write(&tmp, body).map_err(|e| format!("{}: {e}", tmp.display()))?;
        std::fs::rename(&tmp, &path).map_err(|e| format!("{}: {e}", path.display()))?;
    }
    Ok(summary(&stem, &report))
}

fn summary(stem: &str, report: &Value) -> Value {
    let titles: Vec<&str> = report["suggestions"]
        .as_array()
        .map(|a| {
            a.iter()
                .chain(
                    report["serving"]["suggestions"]
                        .as_array()
                        .into_iter()
                        .flatten(),
                )
                .filter_map(|s| s["title"].as_str())
                .collect()
        })
        .unwrap_or_default();
    json!({
        "file": format!("{stem}.html"),
        "json": format!("{stem}.json"),
        "span": report["span"],
        "first_day": report["first_day"],
        "requests": report["total"]["requests"],
        "spend_usd": report["total"]["spend_usd"],
        "suggestions": titles,
        "outages": report["outages"].as_array().map_or(0, Vec::len),
        "written_ms": report["written_ms"],
    })
}

fn prune(cfg: &ReportsConfig, today: i64) {
    if cfg.keep_days == 0 {
        return;
    }
    let Ok(dir) = std::fs::read_dir(&cfg.dir) else {
        return;
    };
    for e in dir.flatten() {
        let name = e.file_name().to_string_lossy().into_owned();
        let Some((_, day)) = parse_name(&name) else {
            continue;
        };
        if today - day > i64::from(cfg.keep_days) {
            let _ = std::fs::remove_file(e.path());
        }
    }
}

/// `roi-day-2026-11-21.html` → (day or week, day number).
fn parse_name(name: &str) -> Option<(Span, i64)> {
    let stem = name
        .strip_suffix(".html")
        .or_else(|| name.strip_suffix(".json"))?;
    let rest = stem.strip_prefix("roi-")?;
    let (span, day) = rest.split_once('-')?;
    let span = match span {
        "day" => Span::Day,
        "week" => Span::Week,
        _ => return None,
    };
    Some((span, parse_day(day)?))
}

/// Write whatever is due and not written yet.
fn run_due(app: &App, cfg: &ReportsConfig) {
    let Some(o) = &app.obs else { return };
    let now = obs::now_ms();
    for (span, first) in due(cfg, now) {
        let path = FsPath::new(&cfg.dir).join(format!("{}.html", file_stem(span, first)));
        if path.exists() {
            continue;
        }
        match write(o.backend.as_ref(), cfg, span, first) {
            Ok(s) => tracing::info!(
                file = %path.display(),
                requests = %s["requests"],
                "report written"
            ),
            Err(e) => {
                tracing::warn!(file = %path.display(), error = %e, "report not written");
                return;
            }
        }
        prune(cfg, (now + cfg.tz_ms()).div_euclid(DAY_MS));
    }
}

/// Keep the reports coming until the app is dropped.
pub fn spawn(app: &Arc<App>) {
    let Some(cfg) = app.cfg.reports.clone() else {
        return;
    };
    if app.obs.is_none() {
        tracing::warn!("[reports] needs the request log ([obs] enabled = true); no reports");
        return;
    }
    let weak: Weak<App> = Arc::downgrade(app);
    tokio::spawn(async move {
        loop {
            let Some(app) = weak.upgrade() else { return };
            let c = cfg.clone();
            // Scans are blocking work.
            let _ = tokio::task::spawn_blocking(move || run_due(&app, &c)).await;
            tokio::time::sleep(Duration::from_secs(30)).await;
        }
    });
}

#[derive(Deserialize, Default)]
pub struct Auth {
    /// The admin token, for opening a report in a browser, which cannot
    /// send an `Authorization` header.
    #[serde(default)]
    token: Option<String>,
    #[serde(default)]
    day: Option<String>,
    #[serde(default)]
    span: Option<String>,
}

fn allowed(app: &App, headers: &HeaderMap, q: &Auth) -> bool {
    if app.admin_ok(headers) {
        return true;
    }
    let Some(t) = &q.token else { return false };
    let mut h = HeaderMap::new();
    match HeaderValue::from_str(&format!("Bearer {t}")) {
        Ok(v) => {
            h.insert(header::AUTHORIZATION, v);
            app.admin_ok(&h)
        }
        Err(_) => false,
    }
}

fn refuse() -> Response {
    error(
        StatusCode::UNAUTHORIZED,
        "authentication_error",
        "admin token required (header, or ?token=)",
    )
}

#[allow(clippy::result_large_err)]
fn config(app: &App) -> Result<&ReportsConfig, Response> {
    app.cfg.reports.as_ref().ok_or_else(|| {
        error(
            StatusCode::NOT_FOUND,
            "not_found_error",
            "reports are off (set [reports] dir)",
        )
    })
}

/// `GET /gateway/reports`: the reports written so far, newest first.
pub async fn list(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Query(q): Query<Auth>,
) -> Response {
    if !allowed(&app, &headers, &q) {
        return refuse();
    }
    let cfg = match config(&app) {
        Ok(c) => c,
        Err(r) => return r,
    };
    Json(listing(cfg)).into_response()
}

/// The reports in the folder, newest first, with the schedule.
pub fn listing(cfg: &ReportsConfig) -> Value {
    let mut items: Vec<(i64, Value)> = Vec::new();
    if let Ok(dir) = std::fs::read_dir(&cfg.dir) {
        for e in dir.flatten() {
            let name = e.file_name().to_string_lossy().into_owned();
            if !name.ends_with(".json") {
                continue;
            }
            let Some((_, day)) = parse_name(&name) else {
                continue;
            };
            let report: Value = std::fs::read(e.path())
                .ok()
                .and_then(|b| serde_json::from_slice(&b).ok())
                .unwrap_or_default();
            items.push((day, summary(name.trim_end_matches(".json"), &report)));
        }
    }
    items.sort_by(|a, b| {
        b.0.cmp(&a.0)
            .then_with(|| a.1["span"].as_str().cmp(&b.1["span"].as_str()))
    });
    json!({
        "dir": cfg.dir,
        "at": cfg.at,
        "tz_hours": cfg.tz_hours,
        "weekly": cfg.weekly,
        "reports": items.into_iter().map(|(_, v)| v).collect::<Vec<_>>(),
    })
}

/// `GET /gateway/reports/{file}`: one report, as the page or as JSON.
pub async fn file(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Path(name): Path<String>,
    Query(q): Query<Auth>,
) -> Response {
    if !allowed(&app, &headers, &q) {
        return refuse();
    }
    let cfg = match config(&app) {
        Ok(c) => c,
        Err(r) => return r,
    };
    // Only names this module writes: nothing else in the folder, no paths.
    if parse_name(&name).is_none() {
        return error(StatusCode::NOT_FOUND, "not_found_error", "no such report");
    }
    match std::fs::read(FsPath::new(&cfg.dir).join(&name)) {
        Ok(body) => {
            let kind = if name.ends_with(".html") {
                "text/html; charset=utf-8"
            } else {
                "application/json"
            };
            ([(header::CONTENT_TYPE, kind)], body).into_response()
        }
        Err(_) => error(StatusCode::NOT_FOUND, "not_found_error", "no such report"),
    }
}

/// `POST /gateway/reports/run[?day=YYYY-MM-DD][&span=day|week]`: write a
/// report now. Without `day`: the latest day that is due.
pub async fn run(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Query(q): Query<Auth>,
) -> Response {
    if !allowed(&app, &headers, &q) {
        return refuse();
    }
    let cfg = match config(&app) {
        Ok(c) => c.clone(),
        Err(r) => return r,
    };
    let Some(o) = app.obs.clone() else {
        return error(
            StatusCode::NOT_FOUND,
            "not_found_error",
            "observability is disabled ([obs] enabled = false)",
        );
    };
    let bad = |m: &str| error(StatusCode::BAD_REQUEST, "invalid_request_error", m);
    let span = match q.span.as_deref() {
        None | Some("day") => Span::Day,
        Some("week") => Span::Week,
        Some(_) => return bad("span must be day or week"),
    };
    let first = match q.day.as_deref() {
        Some(d) => match parse_day(d) {
            Some(d) => d,
            None => return bad("day must be YYYY-MM-DD"),
        },
        None => due(&cfg, obs::now_ms())[0].1 - (span.days() - 1),
    };
    match tokio::task::spawn_blocking(move || write(o.backend.as_ref(), &cfg, span, first)).await {
        Ok(Ok(v)) => Json(v).into_response(),
        Ok(Err(e)) => bad(&e),
        Err(e) => error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "api_error",
            e.to_string(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(at: &str, tz: f64) -> ReportsConfig {
        ReportsConfig {
            dir: "x".into(),
            at: at.into(),
            tz_hours: tz,
            weekly: true,
            keep_days: 90,
            server_hour_usd: None,
            slots: None,
        }
    }

    #[test]
    fn dates_round_trip() {
        assert_eq!(days_from_civil(1970, 1, 1), 0);
        assert_eq!(day_name(days_from_civil(2026, 11, 22)), "2026-11-22");
        assert_eq!(day_name(days_from_civil(2024, 2, 29)), "2024-02-29");
        assert_eq!(parse_day("2026-11-22"), Some(days_from_civil(2026, 11, 22)));
        assert_eq!(parse_day("2026-02-30"), None);
        assert_eq!(parse_day("../x"), None);
        // 2026-11-23 is a Monday.
        assert_eq!(weekday(days_from_civil(2026, 11, 23)), 0);
        for z in [-800_000, -1, 0, 59, 20_000, 30_000] {
            let (y, m, d) = civil_from_days(z);
            assert_eq!(days_from_civil(y, m, d), z);
        }
    }

    #[test]
    fn due_follows_local_time() {
        let c = cfg("07:00", 9.0);
        let utc = |y, m, d, h: i64, min: i64| {
            days_from_civil(y, m, d) * DAY_MS + h * 3_600_000 + min * 60_000
        };
        // 2026-11-22 06:59 in Seoul (21:59 UTC the day before): the 21st is
        // not due yet, the 20th is the latest.
        let d = due(&c, utc(2026, 11, 21, 21, 59));
        assert_eq!(d, vec![(Span::Day, days_from_civil(2026, 11, 20))]);
        // 07:00: the 21st.
        let d = due(&c, utc(2026, 11, 21, 22, 0));
        assert_eq!(d, vec![(Span::Day, days_from_civil(2026, 11, 21))]);
        // Monday the 23rd, 07:00 local: Sunday's day and the week from the 16th.
        let d = due(&c, utc(2026, 11, 22, 22, 0));
        assert_eq!(
            d,
            vec![
                (Span::Day, days_from_civil(2026, 11, 22)),
                (Span::Week, days_from_civil(2026, 11, 16)),
            ]
        );
    }

    #[test]
    fn names_and_times() {
        assert!(cfg("7:5", 9.0).at_minutes().is_ok());
        assert!(cfg("24:00", 9.0).validate().is_err());
        assert!(cfg("noon", 9.0).validate().is_err());
        let day = days_from_civil(2026, 11, 21);
        assert_eq!(file_stem(Span::Day, day), "roi-day-2026-11-21");
        assert_eq!(
            parse_name("roi-day-2026-11-21.html"),
            Some((Span::Day, day))
        );
        assert_eq!(
            parse_name("roi-week-2026-11-21.json"),
            Some((Span::Week, day))
        );
        assert_eq!(parse_name("roi-day-2026-11-21.html.tmp"), None);
        assert_eq!(parse_name("../gateway.toml"), None);
    }
}
