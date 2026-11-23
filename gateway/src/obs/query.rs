//! Aggregation over observability rows: filter, group, time-bucket, and
//! compute counts, sums, means and exact quantiles.
//!
//! The store only appends and scans a time range, so this small engine is
//! what the dashboard queries run on. It asks the store for just the columns
//! a query touches, which a columnar backend answers without decoding rows.

use std::collections::{BTreeMap, HashMap};

use serde::Deserialize;
use serde_json::{json, Map, Value};

use super::store::{row_ts, Backend, Row, Table};

/// Values kept per (group, metric) for quantiles before reservoir sampling
/// takes over. 20k samples put p99 within a fraction of a percent.
const MAX_SAMPLES: usize = 20_000;
const MAX_GROUPS: usize = 5_000;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Query {
    #[serde(default = "requests")]
    pub table: String,
    pub from: i64,
    pub to: i64,
    /// `{"project_id": ["a", "b"], "status": [200]}`: a row passes when each
    /// named column equals one of its listed values.
    #[serde(default)]
    pub filter: BTreeMap<String, Vec<Value>>,
    #[serde(default)]
    pub group_by: Vec<String>,
    /// Bucket width in ms. Adds a leading `bucket` key to every group.
    #[serde(default)]
    pub bucket_ms: Option<i64>,
    /// `count`, `sum:col`, `avg:col`, `min:col`, `max:col`, `p50:col`,
    /// `p95:col`, `p99:col`, `uniq:col`, `ratio:num/den` (sum over sum),
    /// `rate:col` (share of rows where col is true or non-zero) and `hist:col`
    /// (counts in log-scale bins; edges come back as `hist_bounds`).
    #[serde(default = "count_only")]
    pub metrics: Vec<String>,
    /// Metric to sort groups by, descending. Bucketed queries sort by time.
    #[serde(default)]
    pub order_by: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
}

fn requests() -> String {
    "requests".into()
}
fn count_only() -> Vec<String> {
    vec!["count".into()]
}

#[derive(Clone, Debug)]
enum Metric {
    Count,
    Sum(String),
    Avg(String),
    Min(String),
    Max(String),
    Quantile(f64, String),
    Uniq(String),
    Ratio(String, String),
    Rate(String),
    Hist(String),
}

fn parse_metric(s: &str) -> Result<Metric, String> {
    if s == "count" {
        return Ok(Metric::Count);
    }
    let (op, col) = s
        .split_once(':')
        .ok_or_else(|| format!("bad metric {s:?}"))?;
    if col.is_empty() {
        return Err(format!("bad metric {s:?}"));
    }
    let col = col.to_string();
    Ok(match op {
        "sum" => Metric::Sum(col),
        "avg" => Metric::Avg(col),
        "min" => Metric::Min(col),
        "max" => Metric::Max(col),
        "uniq" => Metric::Uniq(col),
        "rate" => Metric::Rate(col),
        "hist" => Metric::Hist(col),
        "ratio" => {
            let (n, d) = col
                .split_once('/')
                .ok_or_else(|| format!("bad metric {s:?}"))?;
            Metric::Ratio(n.into(), d.into())
        }
        p if p.starts_with('p') => {
            let q: f64 = p[1..].parse().map_err(|_| format!("bad metric {s:?}"))?;
            if !(0.0..=100.0).contains(&q) {
                return Err(format!("bad metric {s:?}"));
            }
            Metric::Quantile(q / 100.0, col)
        }
        _ => return Err(format!("bad metric {s:?}")),
    })
}

fn num(v: Option<&Value>) -> Option<f64> {
    match v? {
        Value::Number(n) => n.as_f64(),
        Value::Bool(b) => Some(f64::from(u8::from(*b))),
        _ => None,
    }
}

fn truthy(v: Option<&Value>) -> bool {
    match v {
        Some(Value::Bool(b)) => *b,
        Some(Value::Number(n)) => n.as_f64().is_some_and(|x| x != 0.0),
        Some(Value::String(s)) => !s.is_empty(),
        _ => false,
    }
}

fn key_text(v: Option<&Value>) -> String {
    match v {
        Some(Value::String(s)) => s.clone(),
        Some(Value::Null) | None => String::new(),
        Some(other) => other.to_string(),
    }
}

#[derive(Default)]
struct Acc {
    n: u64,
    sum: f64,
    sum2: f64,
    min: f64,
    max: f64,
    samples: Vec<f32>,
    seen: u64,
    uniq: std::collections::HashSet<String>,
    hist: Vec<u64>,
}

/// `hist:col` counts values into fixed log-scale bins, eight per decade from
/// 0.1 upwards, so histograms and heatmaps need no second pass to pick a range.
pub const HIST_BINS: usize = 64;
const HIST_PER_DECADE: f64 = 8.0;

pub fn hist_bin(x: f64) -> usize {
    let i = ((x.max(0.1).log10() + 1.0) * HIST_PER_DECADE).floor();
    (i.max(0.0) as usize).min(HIST_BINS - 1)
}

/// Lower edge of every bin, plus the upper edge of the last one.
pub fn hist_bounds() -> Vec<f64> {
    (0..=HIST_BINS)
        .map(|i| {
            let b = 10f64.powf(i as f64 / HIST_PER_DECADE - 1.0);
            (b * 1e4).round() / 1e4
        })
        .collect()
}

impl Acc {
    fn add(&mut self, x: f64) {
        if self.n == 0 {
            self.min = x;
            self.max = x;
        } else {
            self.min = self.min.min(x);
            self.max = self.max.max(x);
        }
        self.n += 1;
        self.sum += x;
    }
    /// Reservoir sampling keeps memory bounded on very large groups.
    fn sample(&mut self, x: f64) {
        self.seen += 1;
        if self.samples.len() < MAX_SAMPLES {
            self.samples.push(x as f32);
        } else {
            // Cheap deterministic LCG; statistical quality is ample here.
            let r = self
                .seen
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407)
                >> 33;
            let slot = (r % self.seen) as usize;
            if slot < MAX_SAMPLES {
                self.samples[slot] = x as f32;
            }
        }
    }
}

pub fn quantile(sorted: &[f32], q: f64) -> Option<f64> {
    if sorted.is_empty() {
        return None;
    }
    // Linear interpolation between closest ranks.
    let pos = q * (sorted.len() - 1) as f64;
    let (lo, hi) = (pos.floor() as usize, pos.ceil() as usize);
    let frac = pos - lo as f64;
    Some(f64::from(sorted[lo]) * (1.0 - frac) + f64::from(sorted[hi]) * frac)
}

fn round(x: f64) -> Value {
    if !x.is_finite() {
        return Value::Null;
    }
    // Six significant decimals are enough for display and keep JSON small.
    json!((x * 1e6).round() / 1e6)
}

pub fn run(backend: &dyn Backend, q: &Query) -> Result<Value, String> {
    let table = Table::parse(&q.table).ok_or_else(|| format!("unknown table {:?}", q.table))?;
    if q.to <= q.from {
        return Err("`to` must be after `from`".into());
    }
    if q.bucket_ms
        .is_some_and(|b| b <= 0 || (q.to - q.from) / b > 5_000)
    {
        return Err("bucket_ms would produce more than 5000 buckets".into());
    }
    let metrics: Vec<Metric> = q
        .metrics
        .iter()
        .map(|m| parse_metric(m))
        .collect::<Result<_, _>>()?;
    if metrics.is_empty() {
        return Err("at least one metric is required".into());
    }
    let mut groups: HashMap<(i64, Vec<String>), Vec<Acc>> = HashMap::new();
    let (mut scanned, mut matched, mut overflow) = (0u64, 0u64, false);
    let mut cols: Vec<&str> = q.filter.keys().map(String::as_str).collect();
    cols.extend(q.group_by.iter().map(String::as_str));
    for m in &metrics {
        match m {
            Metric::Count => {}
            Metric::Sum(c)
            | Metric::Avg(c)
            | Metric::Min(c)
            | Metric::Max(c)
            | Metric::Quantile(_, c)
            | Metric::Uniq(c)
            | Metric::Hist(c)
            | Metric::Rate(c) => cols.push(c),
            Metric::Ratio(n, d) => cols.extend([n.as_str(), d.as_str()]),
        }
    }
    cols.sort_unstable();
    cols.dedup();
    backend.scan_cols(table, q.from, q.to, &cols, &mut |row: &Row| {
        scanned += 1;
        for (col, allowed) in &q.filter {
            let v = row.get(col).unwrap_or(&Value::Null);
            if !allowed.iter().any(|a| a == v) {
                return;
            }
        }
        matched += 1;
        let bucket = q.bucket_ms.map_or(0, |b| row_ts(row).div_euclid(b) * b);
        let key: Vec<String> = q.group_by.iter().map(|c| key_text(row.get(c))).collect();
        if groups.len() >= MAX_GROUPS && !groups.contains_key(&(bucket, key.clone())) {
            overflow = true;
            return;
        }
        let accs = groups
            .entry((bucket, key))
            .or_insert_with(|| metrics.iter().map(|_| Acc::default()).collect());
        for (m, a) in metrics.iter().zip(accs.iter_mut()) {
            match m {
                Metric::Count => a.n += 1,
                Metric::Sum(c) | Metric::Avg(c) | Metric::Min(c) | Metric::Max(c) => {
                    if let Some(x) = num(row.get(c)) {
                        a.add(x);
                    }
                }
                Metric::Quantile(_, c) => {
                    if let Some(x) = num(row.get(c)) {
                        a.sample(x);
                    }
                }
                Metric::Hist(c) => {
                    if let Some(x) = num(row.get(c)) {
                        if a.hist.is_empty() {
                            a.hist = vec![0; HIST_BINS];
                        }
                        a.hist[hist_bin(x)] += 1;
                    }
                }
                Metric::Uniq(c) => {
                    let k = key_text(row.get(c));
                    if !k.is_empty() {
                        a.uniq.insert(k);
                    }
                }
                Metric::Ratio(n, d) => {
                    a.sum += num(row.get(n)).unwrap_or(0.0);
                    a.sum2 += num(row.get(d)).unwrap_or(0.0);
                }
                Metric::Rate(c) => {
                    a.n += 1;
                    if truthy(row.get(c)) {
                        a.sum += 1.0;
                    }
                }
            }
        }
    })?;

    let mut rows: Vec<(i64, Vec<String>, Vec<Value>)> = groups
        .into_iter()
        .map(|((bucket, key), mut accs)| {
            let vals = metrics
                .iter()
                .zip(accs.iter_mut())
                .map(|(m, a)| match m {
                    Metric::Count => json!(a.n),
                    Metric::Sum(_) => round(a.sum),
                    Metric::Avg(_) if a.n > 0 => round(a.sum / a.n as f64),
                    Metric::Min(_) if a.n > 0 => round(a.min),
                    Metric::Max(_) if a.n > 0 => round(a.max),
                    Metric::Quantile(p, _) => {
                        a.samples.sort_unstable_by(|x, y| x.total_cmp(y));
                        quantile(&a.samples, *p).map_or(Value::Null, round)
                    }
                    Metric::Uniq(_) => json!(a.uniq.len()),
                    Metric::Hist(_) if !a.hist.is_empty() => json!(a.hist),
                    Metric::Ratio(..) if a.sum2 != 0.0 => round(a.sum / a.sum2),
                    Metric::Rate(_) if a.n > 0 => round(a.sum / a.n as f64),
                    _ => Value::Null,
                })
                .collect();
            (bucket, key, vals)
        })
        .collect();

    let order_idx = q
        .order_by
        .as_ref()
        .and_then(|o| q.metrics.iter().position(|m| m == o));
    rows.sort_by(|a, b| {
        a.0.cmp(&b.0).then_with(|| match order_idx {
            Some(i) => {
                let (x, y) = (
                    a.2[i].as_f64().unwrap_or(f64::MIN),
                    b.2[i].as_f64().unwrap_or(f64::MIN),
                );
                y.total_cmp(&x)
            }
            None => a.1.cmp(&b.1),
        })
    });
    if let (Some(limit), None) = (q.limit, q.bucket_ms) {
        rows.truncate(limit);
    }

    let out: Vec<Value> = rows
        .into_iter()
        .map(|(bucket, key, vals)| {
            let mut m = Map::new();
            if q.bucket_ms.is_some() {
                m.insert("bucket".into(), json!(bucket));
            }
            for (c, k) in q.group_by.iter().zip(key) {
                m.insert(c.clone(), json!(k));
            }
            for (name, v) in q.metrics.iter().zip(vals) {
                m.insert(name.clone(), v);
            }
            Value::Object(m)
        })
        .collect();
    let mut res =
        json!({ "rows": out, "scanned": scanned, "matched": matched, "truncated": overflow });
    if metrics.iter().any(|m| matches!(m, Metric::Hist(_))) {
        res["hist_bounds"] = json!(hist_bounds());
    }
    Ok(res)
}

/// Distinct values of a column, most frequent first: feeds dashboard filters.
pub fn values(
    backend: &dyn Backend,
    table: Table,
    col: &str,
    from: i64,
    to: i64,
) -> Result<Value, String> {
    let mut counts: HashMap<String, u64> = HashMap::new();
    backend.scan_cols(table, from, to, &[col], &mut |row| {
        let k = key_text(row.get(col));
        if !k.is_empty() && (counts.len() < MAX_GROUPS || counts.contains_key(&k)) {
            *counts.entry(k).or_default() += 1;
        }
    })?;
    let mut v: Vec<_> = counts.into_iter().collect();
    v.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    Ok(json!(v
        .into_iter()
        .map(|(k, n)| json!({"value": k, "count": n}))
        .collect::<Vec<_>>()))
}

#[cfg(test)]
mod tests {
    use super::super::store::Memory;
    use super::*;

    fn seed() -> Memory {
        let m = Memory::new(10_000);
        let rows = (0..1000i64)
            .map(|i| {
                [
                    ("ts".to_string(), json!(i * 1000)),
                    (
                        "team".to_string(),
                        json!(if i % 4 == 0 { "a" } else { "b" }),
                    ),
                    ("ttft_ms".to_string(), json!(i as f64)),
                    ("cost".to_string(), json!(0.5)),
                    ("in".to_string(), json!(100)),
                    ("cached".to_string(), json!(if i % 2 == 0 { 50 } else { 0 })),
                    ("err".to_string(), json!(i % 10 == 0)),
                    ("user".to_string(), json!(format!("u{}", i % 7))),
                ]
                .into_iter()
                .collect()
            })
            .collect();
        m.append(Table::Requests, rows).unwrap();
        m
    }

    fn q(v: Value) -> Query {
        serde_json::from_value(v).unwrap()
    }

    #[test]
    fn groups_sums_and_quantiles() {
        let m = seed();
        let r = run(&m, &q(json!({"from": 0, "to": 1_000_000, "group_by": ["team"],
            "metrics": ["count", "sum:cost", "p50:ttft_ms", "p95:ttft_ms", "uniq:user", "ratio:cached/in", "rate:err"],
            "order_by": "count"}))).unwrap();
        let rows = r["rows"].as_array().unwrap();
        assert_eq!(rows[0]["team"], "b");
        assert_eq!(rows[0]["count"], 750);
        assert_eq!(rows[1]["count"], 250);
        assert_eq!(rows[1]["sum:cost"], 125.0);
        assert_eq!(rows[1]["uniq:user"], 7);
        // team a = multiples of 4: 0,4,...,996 -> median 498, all even -> cached ratio 0.5.
        assert_eq!(rows[1]["p50:ttft_ms"], 498.0);
        assert_eq!(rows[1]["ratio:cached/in"], 0.5);
        assert!((rows[1]["p95:ttft_ms"].as_f64().unwrap() - 946.2).abs() < 0.01);
        // multiples of 20 among multiples of 4 -> 1 in 5.
        assert_eq!(rows[1]["rate:err"], 0.2);
    }

    #[test]
    fn buckets_filters_and_window() {
        let m = seed();
        let r = run(
            &m,
            &q(json!({"from": 100_000, "to": 400_000, "bucket_ms": 100_000,
            "filter": {"team": ["a"]}, "metrics": ["count", "avg:ttft_ms"]})),
        )
        .unwrap();
        let rows = r["rows"].as_array().unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0]["bucket"], 100_000);
        assert_eq!(rows[0]["count"], 25);
        assert_eq!(rows[0]["avg:ttft_ms"], 148.0);
        assert_eq!(r["matched"], 75);
        assert!(run(&m, &q(json!({"from": 0, "to": 10, "metrics": ["nope:x"]}))).is_err());
        // ttft 0..999 ms: one value below 0.1, everything else spread over the bins.
        let h = run(
            &m,
            &q(json!({"from": 0, "to": 1_000_000, "metrics": ["hist:ttft_ms"]})),
        )
        .unwrap();
        let bins = h["rows"][0]["hist:ttft_ms"].as_array().unwrap();
        assert_eq!(bins.len(), HIST_BINS);
        assert_eq!(bins.iter().map(|b| b.as_u64().unwrap()).sum::<u64>(), 1000);
        let bounds = h["hist_bounds"].as_array().unwrap();
        assert_eq!(bounds.len(), HIST_BINS + 1);
        // 100..=133 ms land in the bin whose lower edge is 100.
        assert_eq!(bounds[hist_bin(100.0)], 100.0);
        assert_eq!(bins[hist_bin(100.0)], 34);
        assert!(run(
            &m,
            &q(json!({"from": 0, "to": 1_000_000_000, "bucket_ms": 1}))
        )
        .is_err());
    }
}
