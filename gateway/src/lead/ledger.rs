//! Cost ledger: every dollar the gateway sees lands in exactly one class.
//!
//! * `deliverable`: spent on a deliverable that is still alive (work in
//!   progress, finished, or rework after acceptance).
//! * `shared`: runs marked as shared overhead (evals, indexing, warmups).
//! * `waste`: failed and aborted requests, runs that ended failed or
//!   abandoned, and deliverables that were rejected or abandoned.
//! * `unattributed`: everything nobody claimed.
//!
//! The four always add up to the total, so the report can be reconciled
//! against the bill. Facts about deliverables arrive as append-only events
//! with two times: `ts` (when it happened) and `recorded_at` (when the
//! gateway learned it). A report "as of" a moment uses only what was known
//! then, so it never changes afterwards, even when late events arrive.
//!
//! Requests are folded into an hourly rollup as they finish, so a report
//! does not rescan request rows.

use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

pub const HOUR_MS: i64 = 3_600_000;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Fact {
    /// Register or update what a deliverable is.
    Deliverable {
        id: String,
        #[serde(default)]
        project_id: String,
        #[serde(default)]
        team_id: String,
        /// pr | report | doc | dataset | ticket | ...
        #[serde(default)]
        kind: String,
        #[serde(default)]
        title: String,
        #[serde(default)]
        url: String,
    },
    /// in_progress | accepted | rejected | abandoned | reopened
    Status { id: String, status: String },
    /// What the deliverable is worth, as a range. Dollars win over hours.
    Value {
        id: String,
        #[serde(default)]
        usd_low: Option<f64>,
        #[serde(default)]
        usd_high: Option<f64>,
        #[serde(default)]
        hours_low: Option<f64>,
        #[serde(default)]
        hours_high: Option<f64>,
    },
    /// A run worked on a deliverable. Weight 0 removes the link.
    Link {
        run_id: String,
        deliverable_id: String,
        #[serde(default = "one")]
        weight: f64,
    },
    /// ok | failed | abandoned | shared
    RunEnd { run_id: String, outcome: String },
}

fn one() -> f64 {
    1.0
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Event {
    /// When it happened (Unix ms).
    pub ts: i64,
    /// When the gateway learned it. Set by the gateway.
    #[serde(default)]
    pub recorded_at: i64,
    #[serde(flatten)]
    pub fact: Fact,
}

impl Event {
    pub fn validate(&self) -> Result<(), String> {
        let finite = |x: &Option<f64>| x.is_none_or(|v| v.is_finite() && v >= 0.0);
        match &self.fact {
            Fact::Deliverable { id, .. } if id.is_empty() => Err("id is empty".into()),
            Fact::Status { id, status } => {
                if id.is_empty() {
                    return Err("id is empty".into());
                }
                match status.as_str() {
                    "in_progress" | "accepted" | "rejected" | "abandoned" | "reopened" => Ok(()),
                    s => Err(format!("unknown status {s:?}")),
                }
            }
            Fact::Value {
                id,
                usd_low,
                usd_high,
                hours_low,
                hours_high,
            } => {
                if id.is_empty() {
                    return Err("id is empty".into());
                }
                if ![usd_low, usd_high, hours_low, hours_high]
                    .into_iter()
                    .all(finite)
                {
                    return Err("value must be a finite number, zero or more".into());
                }
                let ordered = |lo: &Option<f64>, hi: &Option<f64>| match (lo, hi) {
                    (Some(l), Some(h)) => l <= h,
                    _ => true,
                };
                if !ordered(usd_low, usd_high) || !ordered(hours_low, hours_high) {
                    return Err("low is above high".into());
                }
                Ok(())
            }
            Fact::Link {
                run_id,
                deliverable_id,
                weight,
            } => {
                if run_id.is_empty() || deliverable_id.is_empty() {
                    return Err("run_id and deliverable_id are required".into());
                }
                if !weight.is_finite() || *weight < 0.0 {
                    return Err("weight must be zero or more".into());
                }
                Ok(())
            }
            Fact::RunEnd { run_id, outcome } => {
                if run_id.is_empty() {
                    return Err("run_id is empty".into());
                }
                match outcome.as_str() {
                    "ok" | "failed" | "abandoned" | "shared" => Ok(()),
                    s => Err(format!("unknown outcome {s:?}")),
                }
            }
            _ => Ok(()),
        }
    }
}

/// Default worth of a deliverable kind, in hours of work it replaces.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct HoursRange {
    pub hours_low: f64,
    pub hours_high: f64,
}

/// What the ledger keeps from one finished request.
#[derive(Debug, Clone, Default)]
pub struct Entry {
    pub ts: i64,
    pub run: String,
    pub deliverable: String,
    pub project: String,
    pub team: String,
    pub cost: f64,
    /// The request failed or was cut off: its cost bought nothing.
    pub failed: bool,
    pub tokens: u64,
    /// Dollars spent re-sending input that a cache entry would have covered.
    pub cache_lost: f64,
    /// Dollars the gateway's own changes saved on this request.
    pub saved: f64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Key {
    hour: i64,
    run: String,
    deliverable: String,
    project: String,
    team: String,
}

#[derive(Debug, Clone, Copy, Default)]
struct Cell {
    cost: f64,
    failed_cost: f64,
    requests: u64,
    tokens: u64,
    cache_lost: f64,
    saved: f64,
}

#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct Classes {
    pub deliverable: f64,
    pub shared: f64,
    pub waste: f64,
    pub unattributed: f64,
}

impl Classes {
    pub fn total(&self) -> f64 {
        self.deliverable + self.shared + self.waste + self.unattributed
    }
}

#[derive(Default)]
struct Group {
    classes: Classes,
    wip: f64,
    finished: f64,
    rework: f64,
    requests: u64,
    tokens: u64,
    cache_lost: f64,
    saved: f64,
    value_low: f64,
    value_high: f64,
    valued_cost: f64,
    accepted: u64,
    deliverables: u64,
}

#[derive(Default, Clone)]
struct DState {
    project: String,
    team: String,
    kind: String,
    title: String,
    url: String,
    registered: bool,
    status: String,
    first_accepted: Option<i64>,
    accepted_at: Option<i64>,
    usd: Option<(f64, f64)>,
    hours: Option<(f64, f64)>,
}

#[derive(Default)]
struct DSpend {
    cost: f64,
    waste: f64,
    rework: f64,
    requests: f64,
    runs: std::collections::BTreeSet<String>,
}

#[derive(Default)]
pub struct Ledger {
    cells: HashMap<Key, Cell>,
    events: Vec<Event>,
    total: f64,
    entries: u64,
}

fn r6(x: f64) -> f64 {
    (x * 1e6).round() / 1e6
}

fn roi(value: f64, cost: f64) -> Option<f64> {
    (cost > 0.0).then(|| r6((value - cost) / cost))
}

impl Ledger {
    pub fn total(&self) -> f64 {
        self.total
    }
    pub fn entries(&self) -> u64 {
        self.entries
    }
    pub fn cells(&self) -> usize {
        self.cells.len()
    }
    pub fn events(&self) -> &[Event] {
        &self.events
    }

    pub fn add(&mut self, e: &Entry) {
        if !e.cost.is_finite() {
            return;
        }
        let c = self
            .cells
            .entry(Key {
                hour: e.ts.div_euclid(HOUR_MS),
                run: e.run.clone(),
                deliverable: e.deliverable.clone(),
                project: e.project.clone(),
                team: e.team.clone(),
            })
            .or_default();
        c.cost += e.cost;
        if e.failed {
            c.failed_cost += e.cost;
        }
        c.requests += 1;
        c.tokens += e.tokens;
        c.cache_lost += e.cache_lost;
        c.saved += e.saved;
        self.total += e.cost;
        self.entries += 1;
    }

    /// Append an event. Events are never changed or removed.
    pub fn record(&mut self, e: Event) {
        self.events.push(e);
    }

    /// Drop rollup hours older than `before_ms` (request rows stay in the store).
    pub fn retain(&mut self, before_ms: i64) {
        let h = before_ms.div_euclid(HOUR_MS);
        self.cells.retain(|k, _| k.hour >= h);
    }

    /// Spend in `[from, to)` by class, with what was known at `as_of`.
    /// Rollup hours are whole, so `from` and `to` round down to the hour.
    pub fn report(
        &self,
        from: i64,
        to: i64,
        as_of: i64,
        rates: &HashMap<String, f64>,
        defaults: &HashMap<String, HoursRange>,
    ) -> Value {
        // Replay what was known at `as_of`, in the order it happened.
        let mut known: Vec<&Event> = self
            .events
            .iter()
            .filter(|e| e.recorded_at <= as_of)
            .collect();
        known.sort_by_key(|e| (e.ts, e.recorded_at));
        let mut ds: BTreeMap<String, DState> = BTreeMap::new();
        let mut links: HashMap<&str, Vec<(&str, f64)>> = HashMap::new();
        let mut ends: HashMap<&str, &str> = HashMap::new();
        for e in known {
            match &e.fact {
                Fact::Deliverable {
                    id,
                    project_id,
                    team_id,
                    kind,
                    title,
                    url,
                } => {
                    let d = ds.entry(id.clone()).or_default();
                    d.registered = true;
                    for (dst, src) in [
                        (&mut d.project, project_id),
                        (&mut d.team, team_id),
                        (&mut d.kind, kind),
                        (&mut d.title, title),
                        (&mut d.url, url),
                    ] {
                        if !src.is_empty() {
                            dst.clone_from(src);
                        }
                    }
                }
                Fact::Status { id, status } => {
                    let d = ds.entry(id.clone()).or_default();
                    if status == "accepted" {
                        d.first_accepted.get_or_insert(e.ts);
                        d.accepted_at = Some(e.ts);
                    }
                    d.status = if status == "reopened" {
                        "in_progress".into()
                    } else {
                        status.clone()
                    };
                }
                Fact::Value {
                    id,
                    usd_low,
                    usd_high,
                    hours_low,
                    hours_high,
                } => {
                    let d = ds.entry(id.clone()).or_default();
                    let range = |lo: &Option<f64>, hi: &Option<f64>| match (lo, hi) {
                        (Some(l), Some(h)) => Some((*l, *h)),
                        (Some(x), None) | (None, Some(x)) => Some((*x, *x)),
                        (None, None) => None,
                    };
                    d.usd = range(usd_low, usd_high);
                    d.hours = range(hours_low, hours_high);
                }
                Fact::Link {
                    run_id,
                    deliverable_id,
                    weight,
                } => {
                    let l = links.entry(run_id).or_default();
                    l.retain(|x| x.0 != deliverable_id);
                    if *weight > 0.0 {
                        l.push((deliverable_id, *weight));
                    }
                }
                Fact::RunEnd { run_id, outcome } => {
                    ends.insert(run_id, outcome);
                }
            }
        }

        let (h0, h1) = (from.div_euclid(HOUR_MS), to.div_euclid(HOUR_MS));
        let mut total = Group::default();
        let mut by_project: BTreeMap<String, Group> = BTreeMap::new();
        let mut by_team: BTreeMap<String, Group> = BTreeMap::new();
        let mut spend: BTreeMap<&str, DSpend> = BTreeMap::new();
        for (k, c) in &self.cells {
            if k.hour < h0 || k.hour >= h1 {
                continue;
            }
            let mut cl = Classes::default();
            let (mut wip, mut finished, mut rework) = (0.0, 0.0, 0.0);
            let good = c.cost - c.failed_cost;
            cl.waste += c.failed_cost;
            let direct = [(k.deliverable.as_str(), 1.0)];
            let targets: &[(&str, f64)] = if !k.deliverable.is_empty() {
                &direct
            } else {
                links.get(k.run.as_str()).map_or(&[], Vec::as_slice)
            };
            let wsum: f64 = targets.iter().map(|t| t.1).sum();
            if targets.is_empty() || wsum <= 0.0 {
                match ends.get(k.run.as_str()).copied() {
                    Some("shared") => cl.shared += good,
                    Some("failed" | "abandoned") => cl.waste += good,
                    _ => cl.unattributed += good,
                }
            } else {
                let hour_ms = k.hour * HOUR_MS;
                for (id, w) in targets {
                    let share = w / wsum;
                    let part = good * share;
                    let d = ds.get(*id);
                    let s = spend.entry(id).or_default();
                    s.requests += c.requests as f64 * share;
                    s.waste += c.failed_cost * share;
                    if !k.run.is_empty() {
                        s.runs.insert(k.run.clone());
                    }
                    let status = d.map_or("", |d| d.status.as_str());
                    if matches!(status, "rejected" | "abandoned") {
                        cl.waste += part;
                        s.waste += part;
                        continue;
                    }
                    cl.deliverable += part;
                    s.cost += part;
                    // Work in an hour that starts after the first acceptance
                    // is rework.
                    if d.and_then(|d| d.first_accepted)
                        .is_some_and(|t| hour_ms > t)
                    {
                        rework += part;
                        s.rework += part;
                    } else if status == "accepted" {
                        finished += part;
                    } else {
                        wip += part;
                    }
                }
            }
            for g in [
                &mut total,
                by_project.entry(k.project.clone()).or_default(),
                by_team.entry(k.team.clone()).or_default(),
            ] {
                g.classes.deliverable += cl.deliverable;
                g.classes.shared += cl.shared;
                g.classes.waste += cl.waste;
                g.classes.unattributed += cl.unattributed;
                g.wip += wip;
                g.finished += finished;
                g.rework += rework;
                g.requests += c.requests;
                g.tokens += c.tokens;
                g.cache_lost += c.cache_lost;
                g.saved += c.saved;
            }
        }

        // Deliverables: those with spend in the window or accepted inside it.
        let mut rows = Vec::new();
        let ids: Vec<&str> = ds
            .keys()
            .map(String::as_str)
            .chain(spend.keys().copied().filter(|id| !ds.contains_key(*id)))
            .collect();
        for id in ids {
            let d = ds.get(id).cloned().unwrap_or_default();
            let s = spend.remove(id);
            let in_window = d.accepted_at.is_some_and(|t| t >= from && t < to);
            if s.is_none() && !in_window {
                continue;
            }
            let s = s.unwrap_or_default();
            let cost = s.cost + s.waste;
            let rate = rates.get(&d.team).or_else(|| rates.get("*")).copied();
            let value: Option<(f64, f64, &str)> = if d.status != "accepted" {
                None
            } else if let Some((l, h)) = d.usd {
                Some((l, h, "stated_usd"))
            } else if let (Some((l, h)), Some(r)) = (d.hours, rate) {
                Some((l * r, h * r, "stated_hours"))
            } else if let (Some(def), Some(r)) = (defaults.get(&d.kind), rate) {
                Some((def.hours_low * r, def.hours_high * r, "kind_default"))
            } else {
                None
            };
            for g in [
                &mut total,
                by_project.entry(d.project.clone()).or_default(),
                by_team.entry(d.team.clone()).or_default(),
            ] {
                g.deliverables += 1;
                g.accepted += u64::from(d.status == "accepted");
                if let Some((l, h, _)) = value {
                    g.value_low += l;
                    g.value_high += h;
                    g.valued_cost += cost;
                }
            }
            rows.push(json!({
                "deliverable_id": id, "project_id": d.project, "team_id": d.team, "kind": d.kind,
                "title": d.title, "url": d.url, "registered": d.registered,
                "status": if d.status.is_empty() { "in_progress" } else { d.status.as_str() },
                "accepted_at": d.accepted_at,
                "cost_usd": r6(cost), "waste_usd": r6(s.waste), "rework_usd": r6(s.rework),
                "requests": r6(s.requests), "runs": s.runs.len(),
                "value_low_usd": value.map(|v| r6(v.0)), "value_high_usd": value.map(|v| r6(v.1)),
                "value_source": value.map(|v| v.2),
                "roi_low": value.and_then(|v| roi(v.0, cost)),
                "roi_high": value.and_then(|v| roi(v.1, cost)),
            }));
        }
        let cost_of = |v: &Value| v["cost_usd"].as_f64().unwrap_or(0.0);
        rows.sort_by(|a, b| cost_of(b).total_cmp(&cost_of(a)));

        let group = |g: &Group| {
            let t = g.classes.total();
            let valued = g.value_high > 0.0 || g.value_low > 0.0;
            json!({
                "cost_usd": r6(t),
                "classes": {
                    "deliverable": r6(g.classes.deliverable), "shared": r6(g.classes.shared),
                    "waste": r6(g.classes.waste), "unattributed": r6(g.classes.unattributed),
                },
                "deliverable_split": { "wip": r6(g.wip), "finished": r6(g.finished), "rework": r6(g.rework) },
                "requests": g.requests, "tokens": g.tokens,
                "cache_lost_usd": r6(g.cache_lost), "gateway_saved_usd": r6(g.saved),
                "deliverables": g.deliverables, "accepted": g.accepted,
                "value_low_usd": valued.then(|| r6(g.value_low)),
                "value_high_usd": valued.then(|| r6(g.value_high)),
                // Value over all spend of the group, not only the valued part.
                "roi_low": valued.then(|| roi(g.value_low, t)).flatten(),
                "roi_high": valued.then(|| roi(g.value_high, t)).flatten(),
                "cost_per_accepted_usd": (g.accepted > 0).then(|| r6(t / g.accepted as f64)),
            })
        };
        let named = |m: &BTreeMap<String, Group>, key: &str| -> Vec<Value> {
            m.iter()
                .map(|(k, g)| {
                    let mut v = group(g);
                    v[key] = json!(k);
                    v
                })
                .collect()
        };
        let sum = total.classes.total();
        json!({
            "from": h0 * HOUR_MS, "to": h1 * HOUR_MS, "as_of": as_of,
            "total": group(&total),
            // Every dollar is in exactly one class.
            "reconciliation": { "classes_sum_usd": r6(sum), "attributed_share":
                (sum > 0.0).then(|| r6(1.0 - total.classes.unattributed / sum)) },
            "by_project": named(&by_project, "project_id"),
            "by_team": named(&by_team, "team_id"),
            "deliverables": rows,
        })
    }

    /// Sum of all cells in the window, for reconciliation checks.
    pub fn window_total(&self, from: i64, to: i64) -> f64 {
        let (h0, h1) = (from.div_euclid(HOUR_MS), to.div_euclid(HOUR_MS));
        self.cells
            .iter()
            .filter(|(k, _)| k.hour >= h0 && k.hour < h1)
            .map(|(_, c)| c.cost)
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(ts: i64, recorded_at: i64, fact: Fact) -> Event {
        Event {
            ts,
            recorded_at,
            fact,
        }
    }
    fn entry(ts: i64, run: &str, cost: f64) -> Entry {
        Entry {
            ts,
            run: run.into(),
            project: "p".into(),
            team: "t".into(),
            cost,
            ..Entry::default()
        }
    }
    fn status(id: &str, s: &str) -> Fact {
        Fact::Status {
            id: id.into(),
            status: s.into(),
        }
    }
    fn link(run: &str, d: &str) -> Fact {
        Fact::Link {
            run_id: run.into(),
            deliverable_id: d.into(),
            weight: 1.0,
        }
    }

    #[test]
    fn every_dollar_lands_in_one_class() {
        let mut l = Ledger::default();
        let h = HOUR_MS;
        l.add(&entry(0, "r1", 1.0)); // linked, accepted later
        l.add(&entry(h, "r1", 0.5)); // same
        l.add(&entry(5 * h, "r1", 0.25)); // after acceptance: rework
        l.add(&Entry {
            failed: true,
            ..entry(0, "r1", 0.1)
        });
        l.add(&entry(0, "r2", 2.0)); // rejected deliverable
        l.add(&entry(0, "r3", 3.0)); // shared
        l.add(&entry(0, "r4", 4.0)); // failed run
        l.add(&entry(0, "r5", 5.0)); // nobody claimed it
        l.add(&Entry {
            deliverable: "d3".into(),
            ..entry(0, "", 6.0)
        }); // direct, in progress
        for (i, f) in [
            link("r1", "d1"),
            status("d1", "accepted"),
            link("r2", "d2"),
            status("d2", "rejected"),
            Fact::RunEnd {
                run_id: "r3".into(),
                outcome: "shared".into(),
            },
            Fact::RunEnd {
                run_id: "r4".into(),
                outcome: "failed".into(),
            },
            Fact::Value {
                id: "d1".into(),
                usd_low: Some(10.0),
                usd_high: Some(30.0),
                hours_low: None,
                hours_high: None,
            },
        ]
        .into_iter()
        .enumerate()
        {
            l.record(ev(2 * h + i as i64, 2 * h, f));
        }
        let r = l.report(0, 10 * h, i64::MAX, &HashMap::new(), &HashMap::new());
        let c = &r["total"]["classes"];
        assert_eq!(c["deliverable"], 7.75);
        assert_eq!(c["shared"], 3.0);
        assert_eq!(c["waste"], 6.1);
        assert_eq!(c["unattributed"], 5.0);
        assert!(
            (r["reconciliation"]["classes_sum_usd"].as_f64().unwrap() - l.total()).abs() < 1e-9
        );
        let split = &r["total"]["deliverable_split"];
        assert_eq!(
            (
                split["wip"].as_f64(),
                split["finished"].as_f64(),
                split["rework"].as_f64()
            ),
            (Some(6.0), Some(1.5), Some(0.25))
        );
        let d1 = r["deliverables"]
            .as_array()
            .unwrap()
            .iter()
            .find(|d| d["deliverable_id"] == "d1")
            .unwrap();
        assert_eq!(d1["cost_usd"], 1.85);
        assert!((d1["roi_low"].as_f64().unwrap() - (10.0 - 1.85) / 1.85).abs() < 1e-5);
        assert!(d1["roi_high"].as_f64().unwrap() > d1["roi_low"].as_f64().unwrap());
        // No value, no ROI.
        let d3 = r["deliverables"]
            .as_array()
            .unwrap()
            .iter()
            .find(|d| d["deliverable_id"] == "d3")
            .unwrap();
        assert!(d3["roi_low"].is_null() && d3["value_low_usd"].is_null());
    }

    #[test]
    fn a_report_as_of_a_moment_never_changes() {
        let mut l = Ledger::default();
        l.add(&entry(0, "r1", 1.0));
        l.record(ev(10, 10, link("r1", "d1")));
        let before = l.report(0, HOUR_MS, 100, &HashMap::new(), &HashMap::new());
        // Learned later, about an earlier time.
        l.record(ev(5, 200, status("d1", "rejected")));
        let again = l.report(0, HOUR_MS, 100, &HashMap::new(), &HashMap::new());
        assert_eq!(before, again);
        let now = l.report(0, HOUR_MS, 300, &HashMap::new(), &HashMap::new());
        assert_eq!(now["total"]["classes"]["waste"], 1.0);
    }

    #[test]
    fn value_falls_back_to_hours_then_kind_defaults() {
        let mut l = Ledger::default();
        l.add(&Entry {
            deliverable: "a".into(),
            ..entry(0, "", 10.0)
        });
        l.add(&Entry {
            deliverable: "b".into(),
            ..entry(0, "", 10.0)
        });
        let reg = |id: &str| Fact::Deliverable {
            id: id.into(),
            project_id: "p".into(),
            team_id: "t".into(),
            kind: "pr".into(),
            title: String::new(),
            url: String::new(),
        };
        for f in [
            reg("a"),
            reg("b"),
            status("a", "accepted"),
            status("b", "accepted"),
            Fact::Value {
                id: "a".into(),
                usd_low: None,
                usd_high: None,
                hours_low: Some(1.0),
                hours_high: Some(2.0),
            },
        ] {
            l.record(ev(1, 1, f));
        }
        let rates = HashMap::from([("*".to_string(), 100.0)]);
        let defaults = HashMap::from([(
            "pr".to_string(),
            HoursRange {
                hours_low: 0.5,
                hours_high: 4.0,
            },
        )]);
        let r = l.report(0, HOUR_MS, i64::MAX, &rates, &defaults);
        let get = |id: &str| {
            r["deliverables"]
                .as_array()
                .unwrap()
                .iter()
                .find(|d| d["deliverable_id"] == id)
                .unwrap()
                .clone()
        };
        assert_eq!(
            (
                get("a")["value_low_usd"].as_f64(),
                get("a")["value_high_usd"].as_f64()
            ),
            (Some(100.0), Some(200.0))
        );
        assert_eq!(get("b")["value_source"], "kind_default");
        assert_eq!(get("b")["value_high_usd"], 400.0);
        assert_eq!(r["total"]["roi_low"].as_f64(), Some(6.5));
    }
}
