//! Spend attribution and ROI.
//!
//! Cost comes from request rows. Value comes from deliverables registered
//! through the API by whatever knows what the work produced (the agent
//! runtime, CI, a reviewer). A request's cost goes to:
//!
//! 1. the deliverable named on the request (`x-gateway-deliverable-id`), else
//! 2. the deliverables its run is linked to, split by link weight, else
//! 3. nothing ("unattributed").
//!
//! Value counts only for accepted deliverables: `value_usd` when given,
//! otherwise `hours_saved` times the team's hourly rate.

use std::collections::{BTreeMap, HashMap, HashSet};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::store::{Backend, Table};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Deliverable {
    #[serde(default)]
    pub project_id: String,
    #[serde(default)]
    pub team_id: String,
    /// pr | report | doc | dataset | ticket | ...
    #[serde(default)]
    pub kind: String,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub url: String,
    /// in_progress | accepted | rejected | abandoned
    #[serde(default = "in_progress")]
    pub status: String,
    #[serde(default)]
    pub value_usd: Option<f64>,
    #[serde(default)]
    pub hours_saved: Option<f64>,
    #[serde(default)]
    pub completed_at: Option<i64>,
}

fn in_progress() -> String {
    "in_progress".into()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Link {
    pub run_id: String,
    pub deliverable_id: String,
    #[serde(default = "one")]
    pub weight: f64,
}

fn one() -> f64 {
    1.0
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Rate {
    pub hourly_usd: f64,
}

#[derive(Default)]
struct Spend {
    cost: f64,
    requests: f64,
    tokens: f64,
    runs: HashSet<String>,
}

#[derive(Default)]
struct Group {
    cost: f64,
    attributed: f64,
    requests: u64,
    tokens: u64,
    value: f64,
    deliverables: u64,
    accepted: u64,
}

fn text(v: Option<&Value>) -> String {
    v.and_then(Value::as_str).unwrap_or("").to_string()
}

fn round(x: f64) -> f64 {
    (x * 1e6).round() / 1e6
}

pub fn report(backend: &dyn Backend, from: i64, to: i64) -> Result<Value, String> {
    let deliverables: BTreeMap<String, Deliverable> = backend
        .meta_list("deliverable")?
        .into_iter()
        .filter_map(|(id, v)| Some((id, serde_json::from_value(v).ok()?)))
        .collect();
    let mut links: HashMap<String, Vec<(String, f64)>> = HashMap::new();
    for (_, v) in backend.meta_list("link")? {
        if let Ok(l) = serde_json::from_value::<Link>(v) {
            if l.weight > 0.0 {
                links
                    .entry(l.run_id)
                    .or_default()
                    .push((l.deliverable_id, l.weight));
            }
        }
    }
    let rates: HashMap<String, f64> = backend
        .meta_list("rate")?
        .into_iter()
        .filter_map(|(id, v)| Some((id, serde_json::from_value::<Rate>(v).ok()?.hourly_usd)))
        .collect();

    let mut by_deliverable: HashMap<String, Spend> = HashMap::new();
    let mut by_project: BTreeMap<String, Group> = BTreeMap::new();
    let mut by_team: BTreeMap<String, Group> = BTreeMap::new();
    let mut total = Group::default();
    const COLS: [&str; 6] = [
        "cost_usd",
        "total_tokens",
        "project_id",
        "team_id",
        "run_id",
        "deliverable_id",
    ];
    backend.scan_cols(Table::Requests, from, to, &COLS, &mut |row| {
        let cost = row.get("cost_usd").and_then(Value::as_f64).unwrap_or(0.0);
        let tokens = row.get("total_tokens").and_then(Value::as_u64).unwrap_or(0);
        let (project, team) = (text(row.get("project_id")), text(row.get("team_id")));
        let run = text(row.get("run_id"));
        let direct = text(row.get("deliverable_id"));
        let targets: Vec<(String, f64)> = if !direct.is_empty() {
            vec![(direct, 1.0)]
        } else {
            let ls = links.get(&run).cloned().unwrap_or_default();
            let sum: f64 = ls.iter().map(|l| l.1).sum();
            ls.into_iter().map(|(d, w)| (d, w / sum)).collect()
        };
        let attributed = if targets.is_empty() { 0.0 } else { cost };
        for (d, share) in targets {
            let s = by_deliverable.entry(d).or_default();
            s.cost += cost * share;
            s.requests += share;
            s.tokens += tokens as f64 * share;
            if !run.is_empty() {
                s.runs.insert(run.clone());
            }
        }
        for g in [
            by_project.entry(project).or_default(),
            by_team.entry(team).or_default(),
            &mut total,
        ] {
            g.cost += cost;
            g.attributed += attributed;
            g.requests += 1;
            g.tokens += tokens;
        }
    })?;

    // A deliverable appears in the report when it has spend in the window
    // or was completed inside it.
    let mut rows = Vec::new();
    for (id, d) in &deliverables {
        let spend = by_deliverable.remove(id);
        let in_window = d.completed_at.is_some_and(|t| t >= from && t < to);
        if spend.is_none() && !in_window {
            continue;
        }
        let spend = spend.unwrap_or_default();
        let value = if d.status == "accepted" {
            d.value_usd
                .or_else(|| {
                    Some(d.hours_saved? * rates.get(&d.team_id).or_else(|| rates.get("*"))?)
                })
                .unwrap_or(0.0)
        } else {
            0.0
        };
        for g in [
            by_project.entry(d.project_id.clone()).or_default(),
            by_team.entry(d.team_id.clone()).or_default(),
            &mut total,
        ] {
            g.value += value;
            g.deliverables += 1;
            g.accepted += u64::from(d.status == "accepted");
        }
        rows.push(json!({
            "deliverable_id": id, "project_id": d.project_id, "team_id": d.team_id, "kind": d.kind,
            "title": d.title, "url": d.url, "status": d.status, "completed_at": d.completed_at,
            "cost_usd": round(spend.cost), "requests": round(spend.requests), "tokens": spend.tokens.round(),
            "runs": spend.runs.len(), "value_usd": round(value),
            "roi": (spend.cost > 0.0).then(|| round((value - spend.cost) / spend.cost)),
        }));
    }
    // Spend pointing at deliverable ids nobody registered.
    let mut unknown: Vec<Value> = by_deliverable
        .into_iter()
        .map(|(id, s)| json!({"deliverable_id": id, "cost_usd": round(s.cost), "requests": round(s.requests)}))
        .collect();
    unknown.sort_by(|a, b| {
        b["cost_usd"]
            .as_f64()
            .partial_cmp(&a["cost_usd"].as_f64())
            .unwrap()
    });
    rows.sort_by(|a, b| {
        b["cost_usd"]
            .as_f64()
            .partial_cmp(&a["cost_usd"].as_f64())
            .unwrap()
    });

    let group = |name: &str, g: &Group| {
        json!({
            "name": name, "cost_usd": round(g.cost), "attributed_usd": round(g.attributed),
            "unattributed_usd": round(g.cost - g.attributed), "requests": g.requests, "tokens": g.tokens,
            "value_usd": round(g.value), "deliverables": g.deliverables, "accepted": g.accepted,
            "cost_per_accepted": (g.accepted > 0).then(|| round(g.cost / g.accepted as f64)),
            "roi": (g.cost > 0.0).then(|| round((g.value - g.cost) / g.cost)),
        })
    };
    let sorted = |m: &BTreeMap<String, Group>| {
        let mut v: Vec<Value> = m.iter().map(|(k, g)| group(k, g)).collect();
        v.sort_by(|a, b| {
            b["cost_usd"]
                .as_f64()
                .partial_cmp(&a["cost_usd"].as_f64())
                .unwrap()
        });
        v
    };
    Ok(json!({
        "total": group("", &total),
        "projects": sorted(&by_project),
        "teams": sorted(&by_team),
        "deliverables": rows,
        "unregistered": unknown,
    }))
}

#[cfg(test)]
mod tests {
    use super::super::store::{Memory, Row};
    use super::*;

    fn req(ts: i64, project: &str, run: &str, deliverable: &str, cost: f64) -> Row {
        [
            ("ts", json!(ts)),
            ("project_id", json!(project)),
            ("team_id", json!("t1")),
            ("run_id", json!(run)),
            ("deliverable_id", json!(deliverable)),
            ("cost_usd", json!(cost)),
            ("total_tokens", json!(1000)),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect()
    }

    #[test]
    fn attributes_cost_and_computes_roi() {
        let m = Memory::new(100);
        m.append(
            Table::Requests,
            vec![
                req(10, "p1", "r1", "", 2.0), // linked run, split 3:1 between d1 and d2
                req(11, "p1", "r1", "", 2.0),
                req(12, "p1", "r2", "d1", 1.0), // direct
                req(13, "p1", "r3", "", 5.0),   // unattributed
                req(14, "p2", "r4", "ghost", 1.0),
                req(999, "p1", "r1", "", 100.0), // outside window
            ],
        )
        .unwrap();
        let put = |kind: &str, id: &str, v: Value| m.meta_put(kind, id, v).unwrap();
        put(
            "deliverable",
            "d1",
            json!({"project_id": "p1", "team_id": "t1", "status": "accepted", "hours_saved": 2.0}),
        );
        put(
            "deliverable",
            "d2",
            json!({"project_id": "p1", "team_id": "t1", "status": "rejected", "value_usd": 500.0}),
        );
        put(
            "link",
            "r1|d1",
            json!({"run_id": "r1", "deliverable_id": "d1", "weight": 3.0}),
        );
        put(
            "link",
            "r1|d2",
            json!({"run_id": "r1", "deliverable_id": "d2"}),
        );
        put("rate", "t1", json!({"hourly_usd": 50.0}));
        let r = report(&m, 0, 100).unwrap();
        let d: HashMap<String, &Value> = r["deliverables"]
            .as_array()
            .unwrap()
            .iter()
            .map(|d| (d["deliverable_id"].as_str().unwrap().to_string(), d))
            .collect();
        assert_eq!(d["d1"]["cost_usd"], 4.0); // 3 from the split + 1 direct
        assert_eq!(d["d1"]["value_usd"], 100.0);
        assert_eq!(d["d1"]["roi"], 24.0);
        assert_eq!(d["d1"]["runs"], 2);
        assert_eq!(d["d2"]["cost_usd"], 1.0);
        assert_eq!(d["d2"]["value_usd"], 0.0); // rejected work earns nothing
        let p1 = r["projects"]
            .as_array()
            .unwrap()
            .iter()
            .find(|p| p["name"] == "p1")
            .unwrap();
        assert_eq!(p1["cost_usd"], 10.0);
        assert_eq!(p1["unattributed_usd"], 5.0);
        assert_eq!(p1["value_usd"], 100.0);
        assert_eq!(p1["roi"], 9.0);
        assert_eq!(p1["cost_per_accepted"], 10.0);
        assert_eq!(r["unregistered"][0]["deliverable_id"], "ghost");
        assert_eq!(r["total"]["cost_usd"], 11.0);
    }
}
