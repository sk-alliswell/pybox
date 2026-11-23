//! Windowed limits on requests, tokens, dollars and tool calls.
//!
//! A rule names a scope (agent, key, user, team, project, session or the whole
//! gateway), a window and one or more caps. Each distinct scope value gets its
//! own counter, so `scope = "team"` with `usd = 50` and `window = "day"` gives
//! every team 50 USD a day.
//!
//! Admission reserves an estimate (input tokens from the body size, and with
//! `strict = true` also the request's `max_tokens`) and the response settles
//! it with the real usage. Counting only finished calls, as gateways commonly
//! do, lets N concurrent requests all pass a nearly exhausted budget; the
//! reservation closes that gap.
//!
//! Rolling windows (`"1m"`, `"1h"`, `"7d"`) use the two-bucket sliding-window
//! approximation: the previous bucket counts in proportion to how much of it
//! still overlaps the window. Calendar windows (`"day"`, `"month"`, UTC) reset
//! at the boundary, which is what a budget is expected to do.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

const DAY_MS: i64 = 86_400_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Scope {
    Global,
    Agent,
    Key,
    User,
    Team,
    Project,
    Session,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Window {
    /// Sliding window of this many milliseconds.
    Rolling(i64),
    Day,
    Month,
}

impl Window {
    pub fn parse(s: &str) -> Result<Self, String> {
        match s {
            "day" => return Ok(Self::Day),
            "month" => return Ok(Self::Month),
            _ => {}
        }
        let (num, unit) = s.split_at(s.len().saturating_sub(1));
        let n: i64 = num
            .parse()
            .map_err(|_| format!("invalid window {s:?} (e.g. 30s, 1m, 1h, 7d, day, month)"))?;
        let unit_ms = match unit {
            "s" => 1_000,
            "m" => 60_000,
            "h" => 3_600_000,
            "d" => DAY_MS,
            _ => return Err(format!("invalid window unit in {s:?} (s, m, h, d)")),
        };
        if n <= 0 || n > 400 * DAY_MS / unit_ms {
            return Err(format!("window {s:?} out of range"));
        }
        Ok(Self::Rolling(n * unit_ms))
    }

    /// Start and length of the bucket containing `now_ms`.
    fn bucket(self, now_ms: i64) -> (i64, i64) {
        match self {
            Self::Rolling(w) => (now_ms.div_euclid(w) * w, w),
            Self::Day => (now_ms.div_euclid(DAY_MS) * DAY_MS, DAY_MS),
            Self::Month => {
                let (y, m) = year_month(now_ms.div_euclid(DAY_MS));
                let start = days_from_civil(y, m, 1);
                let (ny, nm) = if m == 12 { (y + 1, 1) } else { (y, m + 1) };
                let end = days_from_civil(ny, nm, 1);
                (start * DAY_MS, (end - start) * DAY_MS)
            }
        }
    }

    fn rolling(self) -> bool {
        matches!(self, Self::Rolling(_))
    }

    /// How far back usage still matters at `now_ms`.
    pub fn horizon_ms(self, now_ms: i64) -> i64 {
        let (start, len) = self.bucket(now_ms);
        if self.rolling() {
            start - len
        } else {
            start
        }
    }
}

// Civil-date arithmetic (Howard Hinnant's algorithms), days since 1970-01-01.
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn year_month(days: i64) -> (i64, i64) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (yoe + era * 400 + i64::from(m <= 2), m)
}

impl<'de> Deserialize<'de> for Window {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        Self::parse(&s).map_err(serde::de::Error::custom)
    }
}

impl Serialize for Window {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let text = match *self {
            Self::Day => "day".to_string(),
            Self::Month => "month".to_string(),
            Self::Rolling(ms) if ms % DAY_MS == 0 => format!("{}d", ms / DAY_MS),
            Self::Rolling(ms) if ms % 3_600_000 == 0 => format!("{}h", ms / 3_600_000),
            Self::Rolling(ms) if ms % 60_000 == 0 => format!("{}m", ms / 60_000),
            Self::Rolling(ms) => format!("{}s", ms / 1000),
        };
        s.serialize_str(&text)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct LimitRule {
    /// Shown in errors and reports. Defaults to `<scope>-<window>`.
    #[serde(default)]
    pub name: String,
    pub scope: Scope,
    /// Apply only to this scope value (e.g. one team). Unset: every value
    /// gets its own counter with the same caps.
    #[serde(default, rename = "match", skip_serializing_if = "Option::is_none")]
    pub only: Option<String>,
    /// Count only calls to models matching this pattern (`*` wildcard).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Count only calls through this upstream.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream: Option<String>,
    /// Count only MCP tool calls matching `server/tool` (`*` wildcard).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool: Option<String>,
    pub window: Window,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requests: Option<u64>,
    /// Input (cached included) plus output tokens.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usd: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_calls: Option<u64>,
    /// Also reserve the request's `max_tokens` up front, so the cap can never
    /// be exceeded. Without it only the input estimate is reserved and the
    /// cap can be passed by at most the output of the calls in flight.
    #[serde(default)]
    pub strict: bool,
}

impl LimitRule {
    pub fn validate(&mut self) -> Result<(), String> {
        if self.name.is_empty() {
            self.name = format!(
                "{}-{}",
                serde_json::to_value(self.scope)
                    .ok()
                    .and_then(|v| v.as_str().map(str::to_string))
                    .unwrap_or_default(),
                serde_json::to_value(self.window)
                    .ok()
                    .and_then(|v| v.as_str().map(str::to_string))
                    .unwrap_or_default()
            );
        }
        if self.name.len() > 64 || self.name.contains('/') {
            return Err(format!("limit {:?}: invalid name", self.name));
        }
        if self.requests.is_none()
            && self.tokens.is_none()
            && self.usd.is_none()
            && self.tool_calls.is_none()
        {
            return Err(format!(
                "limit {:?}: set at least one of requests, tokens, usd, tool_calls",
                self.name
            ));
        }
        if self.usd.is_some_and(|u| !u.is_finite() || u < 0.0) {
            return Err(format!("limit {:?}: usd must be >= 0", self.name));
        }
        if self.tool.is_some()
            && (self.tokens.is_some() || self.usd.is_some() || self.model.is_some())
        {
            return Err(format!(
                "limit {:?}: a tool rule can cap tool_calls only",
                self.name
            ));
        }
        Ok(())
    }
}

/// `*` matches any run of characters; everything else is literal.
pub fn glob(pattern: &str, text: &str) -> bool {
    let mut parts = pattern.split('*');
    let first = parts.next().unwrap_or("");
    let Some(mut rest) = text.strip_prefix(first) else {
        return false;
    };
    let mut parts = parts.peekable();
    if parts.peek().is_none() {
        return rest.is_empty();
    }
    while let Some(p) = parts.next() {
        if parts.peek().is_none() {
            // The final literal must sit at the end.
            return p.is_empty() || rest.ends_with(p);
        }
        match rest.find(p) {
            Some(i) => rest = &rest[i + p.len()..],
            None => return false,
        }
    }
    true
}

/// Who and what a call is, for matching rules.
#[derive(Debug, Clone, Default)]
pub struct Subject {
    pub agent: String,
    pub key: String,
    pub user: String,
    pub team: String,
    pub project: String,
    pub session: String,
    pub upstream: String,
    pub model: String,
    /// `server/tool` for MCP tool calls; empty for model calls.
    pub tool: String,
}

impl Subject {
    fn value(&self, scope: Scope) -> &str {
        match scope {
            Scope::Global => "*",
            Scope::Agent => &self.agent,
            Scope::Key => &self.key,
            Scope::User => &self.user,
            Scope::Team => &self.team,
            Scope::Project => &self.project,
            Scope::Session => &self.session,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Amount {
    pub requests: f64,
    pub tokens: f64,
    pub usd: f64,
    pub tool_calls: f64,
}

impl Amount {
    fn add(&mut self, o: Amount, sign: f64) {
        self.requests += sign * o.requests;
        self.tokens += sign * o.tokens;
        self.usd += sign * o.usd;
        self.tool_calls += sign * o.tool_calls;
    }
    fn scaled(self, k: f64) -> Amount {
        Amount {
            requests: self.requests * k,
            tokens: self.tokens * k,
            usd: self.usd * k,
            tool_calls: self.tool_calls * k,
        }
    }
    fn clamp(&mut self) {
        // Floating-point settle/release pairs can leave a tiny negative.
        for v in [
            &mut self.requests,
            &mut self.tokens,
            &mut self.usd,
            &mut self.tool_calls,
        ] {
            if *v < 1e-9 {
                *v = 0.0;
            }
        }
    }
}

#[derive(Debug, Default)]
struct Counter {
    start: i64,
    cur: Amount,
    prev: Amount,
    reserved: Amount,
}

impl Counter {
    fn roll(&mut self, w: Window, now_ms: i64) -> (i64, i64) {
        let (start, len) = w.bucket(now_ms);
        if start != self.start {
            // The bucket right before the current one is the only history a
            // sliding window needs.
            let prev_start = w.bucket(start - 1).0;
            self.prev = if self.start == prev_start {
                self.cur
            } else {
                Amount::default()
            };
            self.cur = Amount::default();
            self.start = start;
        }
        (start, len)
    }

    fn used(&self, w: Window, start: i64, len: i64, now_ms: i64) -> Amount {
        let mut u = self.cur;
        if w.rolling() {
            let frac = ((now_ms - start) as f64 / len as f64).clamp(0.0, 1.0);
            u.add(self.prev.scaled(1.0 - frac), 1.0);
        }
        u
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Denied {
    pub rule: String,
    pub scope: Scope,
    pub value: String,
    pub metric: &'static str,
    pub limit: f64,
    pub used: f64,
    pub retry_after_secs: f64,
}

impl Denied {
    pub fn message(&self) -> String {
        let unit = if self.metric == "usd" { "USD " } else { "" };
        format!(
            "limit {:?} reached for {:?} {:?}: {unit}{:.4} of {unit}{:.4} {} used; retry in {:.0}s",
            self.rule,
            self.scope,
            self.value,
            self.used,
            self.limit,
            self.metric,
            self.retry_after_secs.ceil()
        )
    }

    pub fn retry_after(&self) -> Duration {
        Duration::from_secs_f64(self.retry_after_secs.max(0.0))
    }
}

type Key = (String, String);

#[derive(Default)]
pub struct Limits {
    rules: RwLock<Vec<LimitRule>>,
    counters: Mutex<HashMap<Key, Counter>>,
    denied: Mutex<HashMap<String, u64>>,
}

/// Budget held for a call in flight. Settle it with what the call really
/// used; dropping it unsettled counts the request but no tokens or dollars.
pub struct Reservation {
    limits: Arc<Limits>,
    held: Vec<(Key, Window)>,
    estimate: Amount,
    done: bool,
}

impl Reservation {
    pub fn settle(mut self, tokens: u64, usd: f64, now_ms: i64) {
        self.finish(Some((tokens as f64, usd, now_ms)));
    }

    fn finish(&mut self, actual: Option<(f64, f64, i64)>) {
        if std::mem::replace(&mut self.done, true) || self.held.is_empty() {
            return;
        }
        let mut counters = self.limits.counters.lock().unwrap();
        for (key, w) in &self.held {
            if let Some(c) = counters.get_mut(key) {
                c.reserved.add(self.estimate, -1.0);
                c.reserved.clamp();
                if let Some((tokens, usd, now_ms)) = actual {
                    // Usage lands in the window open when the call ends.
                    c.roll(*w, now_ms);
                    c.cur.tokens += tokens;
                    c.cur.usd += usd;
                }
            }
        }
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.finish(None);
    }
}

impl Limits {
    pub fn new(mut rules: Vec<LimitRule>) -> Result<Arc<Self>, String> {
        let mut seen = std::collections::HashSet::new();
        for r in &mut rules {
            r.validate()?;
            if !seen.insert(r.name.clone()) {
                return Err(format!("duplicate limit name {:?}", r.name));
            }
        }
        Ok(Arc::new(Self {
            rules: RwLock::new(rules),
            ..Self::default()
        }))
    }

    pub fn is_empty(&self) -> bool {
        self.rules.read().unwrap().is_empty()
    }

    pub fn rules(&self) -> Vec<LimitRule> {
        self.rules.read().unwrap().clone()
    }

    /// Add or replace a rule by name. Replacing keeps its counters when the
    /// window is unchanged.
    pub fn put(&self, mut rule: LimitRule) -> Result<(), String> {
        rule.validate()?;
        let mut rules = self.rules.write().unwrap();
        match rules.iter_mut().find(|r| r.name == rule.name) {
            Some(old) => {
                if old.window != rule.window || old.scope != rule.scope {
                    self.counters
                        .lock()
                        .unwrap()
                        .retain(|(name, _), _| *name != rule.name);
                }
                *old = rule;
            }
            None => rules.push(rule),
        }
        Ok(())
    }

    pub fn remove(&self, name: &str) -> bool {
        let mut rules = self.rules.write().unwrap();
        let before = rules.len();
        rules.retain(|r| r.name != name);
        self.counters.lock().unwrap().retain(|(n, _), _| n != name);
        rules.len() != before
    }

    fn applies(rule: &LimitRule, s: &Subject) -> bool {
        let value = s.value(rule.scope);
        if value.is_empty() {
            return false;
        }
        if rule.only.as_deref().is_some_and(|m| m != value) {
            return false;
        }
        if rule.upstream.as_deref().is_some_and(|u| u != s.upstream) {
            return false;
        }
        match (&rule.tool, s.tool.is_empty()) {
            (Some(p), false) => glob(p, &s.tool),
            (Some(_), true) => false,
            // Rules without a tool pattern see model calls, plus tool calls
            // when they cap tool_calls.
            (None, false) => rule.tool_calls.is_some(),
            (None, true) => rule.model.as_deref().is_none_or(|p| glob(p, &s.model)),
        }
    }

    /// Check every matching rule and, when all pass, reserve `estimate`
    /// against them. `estimate.requests` / `tool_calls` are counted at once.
    pub fn admit(
        self: &Arc<Self>,
        subject: &Subject,
        estimate: Amount,
        strict_extra: Amount,
        now_ms: i64,
    ) -> Result<Reservation, Denied> {
        let rules = self.rules.read().unwrap();
        if rules.is_empty() {
            return Ok(Reservation {
                limits: self.clone(),
                held: Vec::new(),
                estimate: Amount::default(),
                done: true,
            });
        }
        let mut counters = self.counters.lock().unwrap();
        let matching: Vec<&LimitRule> =
            rules.iter().filter(|r| Self::applies(r, subject)).collect();
        // First pass: would any rule be exceeded?
        for rule in &matching {
            let key = (rule.name.clone(), subject.value(rule.scope).to_string());
            let c = counters.entry(key).or_default();
            let (start, len) = c.roll(rule.window, now_ms);
            let mut used = c.used(rule.window, start, len, now_ms);
            used.add(c.reserved, 1.0);
            let mut want = estimate;
            if rule.strict {
                want.add(strict_extra, 1.0);
            }
            let checks: [(&'static str, Option<f64>, f64, f64); 4] = [
                (
                    "requests",
                    rule.requests.map(|v| v as f64),
                    used.requests,
                    want.requests,
                ),
                (
                    "tokens",
                    rule.tokens.map(|v| v as f64),
                    used.tokens,
                    want.tokens,
                ),
                ("usd", rule.usd, used.usd, want.usd),
                (
                    "tool_calls",
                    rule.tool_calls.map(|v| v as f64),
                    used.tool_calls,
                    want.tool_calls,
                ),
            ];
            for (metric, limit, used_m, want_m) in checks {
                let Some(limit) = limit else { continue };
                // A spent budget blocks even a zero-cost estimate.
                let over = used_m + want_m > limit + 1e-9 || (want_m == 0.0 && used_m >= limit);
                if !over {
                    continue;
                }
                let end = start + len;
                let retry_ms = if rule.window.rolling() {
                    let prev = match metric {
                        "requests" => c.prev.requests,
                        "tokens" => c.prev.tokens,
                        "usd" => c.prev.usd,
                        _ => c.prev.tool_calls,
                    };
                    let need = used_m + want_m - limit;
                    let frac = (now_ms - start) as f64 / len as f64;
                    if prev * (1.0 - frac) >= need && prev > 0.0 {
                        (need / prev * len as f64).ceil() as i64
                    } else {
                        end - now_ms
                    }
                } else {
                    end - now_ms
                };
                let d = Denied {
                    rule: rule.name.clone(),
                    scope: rule.scope,
                    value: subject.value(rule.scope).to_string(),
                    metric,
                    limit,
                    used: used_m,
                    retry_after_secs: (retry_ms.max(1000)) as f64 / 1000.0,
                };
                *self
                    .denied
                    .lock()
                    .unwrap()
                    .entry(d.rule.clone())
                    .or_default() += 1;
                return Err(d);
            }
        }
        // Second pass: hold the estimate. Requests and tool calls are final.
        let mut held = Vec::with_capacity(matching.len());
        let mut reserve = estimate;
        reserve.requests = 0.0;
        reserve.tool_calls = 0.0;
        // One reservation size for every rule keeps settle symmetric; strict
        // rules already verified the larger amount fits.
        let mut strict_any = false;
        for rule in &matching {
            strict_any |= rule.strict;
        }
        if strict_any {
            reserve.add(
                Amount {
                    tokens: strict_extra.tokens,
                    usd: strict_extra.usd,
                    ..Amount::default()
                },
                1.0,
            );
        }
        for rule in &matching {
            let key = (rule.name.clone(), subject.value(rule.scope).to_string());
            if let Some(c) = counters.get_mut(&key) {
                c.cur.requests += estimate.requests;
                c.cur.tool_calls += estimate.tool_calls;
                c.reserved.add(reserve, 1.0);
            }
            held.push((key, rule.window));
        }
        Ok(Reservation {
            limits: self.clone(),
            held,
            estimate: reserve,
            done: false,
        })
    }

    /// Replay a finished call from the request log (startup restore).
    pub fn replay(&self, subject: &Subject, amount: Amount, ts_ms: i64, now_ms: i64) {
        let rules = self.rules.read().unwrap();
        let mut counters = self.counters.lock().unwrap();
        for rule in rules.iter().filter(|r| Self::applies(r, subject)) {
            let (start, len) = rule.window.bucket(now_ms);
            let (ts_start, _) = rule.window.bucket(ts_ms);
            let c = counters
                .entry((rule.name.clone(), subject.value(rule.scope).to_string()))
                .or_default();
            c.start = start;
            if ts_start == start {
                c.cur.add(amount, 1.0);
            } else if rule.window.rolling() && ts_start == start - len {
                c.prev.add(amount, 1.0);
            }
        }
    }

    /// How far back `replay` needs the log, if any rule exists.
    pub fn restore_from_ms(&self, now_ms: i64) -> Option<i64> {
        self.rules
            .read()
            .unwrap()
            .iter()
            .map(|r| r.window.horizon_ms(now_ms))
            .min()
    }

    pub fn snapshot(&self, now_ms: i64) -> Value {
        let rules = self.rules.read().unwrap();
        let mut counters = self.counters.lock().unwrap();
        let denied = self.denied.lock().unwrap();
        let mut out = Vec::new();
        for rule in rules.iter() {
            let mut usage = Vec::new();
            for ((name, value), c) in counters.iter_mut() {
                if name != &rule.name {
                    continue;
                }
                let (start, len) = c.roll(rule.window, now_ms);
                let u = c.used(rule.window, start, len, now_ms);
                usage.push(json!({
                    "value": value,
                    "requests": u.requests.round(),
                    "tokens": u.tokens.round(),
                    "usd": u.usd,
                    "tool_calls": u.tool_calls.round(),
                    "reserved_tokens": c.reserved.tokens.round(),
                    "reserved_usd": c.reserved.usd,
                    "resets_in_secs": if rule.window.rolling() { Value::Null } else { json!((start + len - now_ms) / 1000) },
                }));
            }
            usage.sort_by(|a, b| a["value"].as_str().cmp(&b["value"].as_str()));
            out.push(json!({
                "rule": rule,
                "denied": denied.get(&rule.name).copied().unwrap_or(0),
                "usage": usage,
            }));
        }
        // Idle counters with nothing left in the window can go.
        counters.retain(|_, c| {
            c.cur != Amount::default()
                || c.prev != Amount::default()
                || c.reserved != Amount::default()
        });
        json!({ "limits": out })
    }

    pub fn denied_counts(&self) -> HashMap<String, u64> {
        self.denied.lock().unwrap().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rule(toml_text: &str) -> LimitRule {
        toml::from_str(toml_text).unwrap()
    }

    fn subj(agent: &str, team: &str) -> Subject {
        Subject {
            agent: agent.into(),
            team: team.into(),
            model: "claude-sonnet-5".into(),
            upstream: "llm".into(),
            ..Subject::default()
        }
    }

    fn req(tokens: f64, usd: f64) -> Amount {
        Amount {
            requests: 1.0,
            tokens,
            usd,
            tool_calls: 0.0,
        }
    }

    #[test]
    fn window_parsing_and_calendar_buckets() {
        assert_eq!(Window::parse("1m").unwrap(), Window::Rolling(60_000));
        assert_eq!(Window::parse("7d").unwrap(), Window::Rolling(7 * DAY_MS));
        assert!(Window::parse("0m").is_err());
        assert!(Window::parse("1w").is_err());
        // 2026-11-12T07:00:00Z
        let now = 1_794_466_800_000i64;
        let (start, len) = Window::Month.bucket(now);
        assert_eq!(start, days_from_civil(2026, 11, 1) * DAY_MS);
        assert_eq!(len, 30 * DAY_MS);
        // February of a leap year, and the December to January roll-over.
        let feb = days_from_civil(2028, 2, 10) * DAY_MS;
        assert_eq!(Window::Month.bucket(feb).1, 29 * DAY_MS);
        let dec = days_from_civil(2026, 12, 31) * DAY_MS;
        let (s, l) = Window::Month.bucket(dec);
        assert_eq!(s + l, days_from_civil(2027, 1, 1) * DAY_MS);
        assert_eq!(year_month(days_from_civil(2026, 11, 12)), (2026, 11));
    }

    #[test]
    fn glob_patterns() {
        assert!(glob("claude-*", "claude-sonnet-5"));
        assert!(glob("*", "anything"));
        assert!(glob("github/*_issue", "github/create_issue"));
        assert!(glob("a*b*c", "aXXbYYc"));
        assert!(!glob("a*b*c", "aXXbYY"));
        assert!(!glob("claude-*", "gpt-5"));
        assert!(glob("exact", "exact"));
        assert!(!glob("exact", "exactly"));
    }

    #[test]
    fn each_scope_value_has_its_own_budget() {
        let l = Limits::new(vec![rule(
            "scope = \"team\"\nwindow = \"day\"\ntokens = 1000",
        )])
        .unwrap();
        let now = 1_794_466_800_000;
        l.admit(
            &subj("a", "growth"),
            req(100.0, 0.0),
            Amount::default(),
            now,
        )
        .unwrap()
        .settle(900, 0.0, now);
        let d = l
            .admit(
                &subj("b", "growth"),
                req(200.0, 0.0),
                Amount::default(),
                now,
            )
            .err()
            .expect("growth is over");
        assert_eq!((d.metric, d.value.as_str()), ("tokens", "growth"));
        assert!(d.retry_after_secs > 3600.0);
        // Another team is unaffected, and so is the next day.
        l.admit(&subj("c", "infra"), req(200.0, 0.0), Amount::default(), now)
            .unwrap();
        l.admit(
            &subj("b", "growth"),
            req(200.0, 0.0),
            Amount::default(),
            now + DAY_MS,
        )
        .unwrap();
    }

    #[test]
    fn reservations_stop_concurrent_overshoot() {
        let l = Limits::new(vec![rule(
            "scope = \"agent\"\nwindow = \"day\"\nusd = 1.2\nstrict = true",
        )])
        .unwrap();
        let now = 1_794_466_800_000;
        let extra = Amount {
            usd: 0.3,
            ..Amount::default()
        };
        // Each call may cost up to 0.1 + 0.3; three fit under 1.2, a fourth does not.
        let held: Vec<_> = (0..3)
            .map(|_| l.admit(&subj("a", ""), req(0.0, 0.1), extra, now).unwrap())
            .collect();
        assert!(l.admit(&subj("a", ""), req(0.0, 0.1), extra, now).is_err());
        // They finish cheaper than reserved, which frees room again.
        for r in held {
            r.settle(10, 0.05, now);
        }
        l.admit(&subj("a", ""), req(0.0, 0.1), extra, now).unwrap();
    }

    #[test]
    fn dropped_reservation_releases_tokens_but_counts_the_request() {
        let l = Limits::new(vec![rule(
            "scope = \"agent\"\nwindow = \"1h\"\ntokens = 100\nrequests = 2",
        )])
        .unwrap();
        let now = 1_794_466_800_000;
        drop(
            l.admit(&subj("a", ""), req(90.0, 0.0), Amount::default(), now)
                .unwrap(),
        );
        l.admit(&subj("a", ""), req(90.0, 0.0), Amount::default(), now)
            .unwrap()
            .settle(10, 0.0, now);
        let d = l
            .admit(&subj("a", ""), req(1.0, 0.0), Amount::default(), now)
            .err()
            .unwrap();
        assert_eq!(d.metric, "requests");
    }

    #[test]
    fn sliding_window_decays_the_previous_bucket() {
        let l = Limits::new(vec![rule(
            "scope = \"agent\"\nwindow = \"1m\"\nrequests = 10",
        )])
        .unwrap();
        let t0 = 1_794_466_800_000; // on a minute boundary
        for _ in 0..10 {
            l.admit(
                &subj("a", ""),
                req(0.0, 0.0),
                Amount::default(),
                t0 + 30_000,
            )
            .unwrap();
        }
        assert!(l
            .admit(
                &subj("a", ""),
                req(0.0, 0.0),
                Amount::default(),
                t0 + 59_000
            )
            .is_err());
        // 15 s into the next minute 75% of the old bucket still counts: 7.5 used.
        let mut ok = 0;
        for _ in 0..10 {
            if l.admit(
                &subj("a", ""),
                req(0.0, 0.0),
                Amount::default(),
                t0 + 75_000,
            )
            .is_ok()
            {
                ok += 1;
            }
        }
        assert_eq!(ok, 2);
    }

    #[test]
    fn tool_rules_see_only_matching_tool_calls() {
        let l = Limits::new(vec![rule(
            "scope = \"agent\"\nwindow = \"1m\"\ntool = \"github/create_*\"\ntool_calls = 1",
        )])
        .unwrap();
        let now = 1_794_466_800_000;
        let call = Amount {
            tool_calls: 1.0,
            ..Amount::default()
        };
        let tool = |name: &str| Subject {
            agent: "a".into(),
            tool: name.into(),
            ..Subject::default()
        };
        l.admit(&tool("github/create_issue"), call, Amount::default(), now)
            .unwrap();
        assert!(l
            .admit(&tool("github/create_pr"), call, Amount::default(), now)
            .is_err());
        l.admit(&tool("github/search"), call, Amount::default(), now)
            .unwrap();
        // Model calls are not tool calls.
        l.admit(&subj("a", ""), req(5.0, 0.0), Amount::default(), now)
            .unwrap();
    }

    #[test]
    fn replay_restores_usage_into_the_right_bucket() {
        let l = Limits::new(vec![rule(
            "scope = \"project\"\nwindow = \"month\"\nusd = 10",
        )])
        .unwrap();
        let now = 1_794_466_800_000;
        let s = Subject {
            project: "apollo".into(),
            ..Subject::default()
        };
        l.replay(&s, req(0.0, 9.5), now - 5 * DAY_MS, now);
        l.replay(&s, req(0.0, 100.0), now - 40 * DAY_MS, now); // last month: ignored
        assert!(l.admit(&s, req(0.0, 1.0), Amount::default(), now).is_err());
        l.admit(&s, req(0.0, 0.4), Amount::default(), now).unwrap();
        assert_eq!(
            l.restore_from_ms(now),
            Some(days_from_civil(2026, 11, 1) * DAY_MS)
        );
    }
}
