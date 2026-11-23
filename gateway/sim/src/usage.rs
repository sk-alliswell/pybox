//! Usage patterns from request rows, and a report on them.
//!
//! The input is one row per request with the column names of the gateway's
//! `requests` table, so the same analysis reads a simulated run today and
//! the real log later. Only the columns in [`Row`] are used; others are
//! ignored, missing ones default.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use serde::{Deserialize, Serialize};

use crate::metrics::pct;
use crate::sim::{Sim, GAVE_UP};

/// One request. Field names are the gateway's `requests` columns, except
/// `kind` (the gateway calls it `priority`) and `backend` (`upstream_ip`).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct Row {
    /// When the request arrived, ms (epoch for a real log, from 0 for a
    /// simulated run).
    pub ts: f64,
    pub team_id: String,
    pub user_id: String,
    /// Who the caller says it is (`x-gateway-user-id`), when the key does
    /// not say.
    pub claimed_user_id: String,
    pub agent_id: String,
    pub session_id: String,
    pub project_id: String,
    /// What sort of request it is (ide, chat, batch, ...).
    #[serde(alias = "request_kind")]
    pub kind: String,
    /// The gateway's own word for the kind (interactive, normal, batch);
    /// used when `kind` is empty.
    pub priority: String,
    pub model: String,
    pub upstream: String,
    /// The model server that ran it.
    pub backend: String,
    /// HTTP status; 0 when there was no response.
    pub status: i32,
    /// The caller went away before the answer was complete.
    pub client_abort: bool,
    /// Times it was sent to a server; 0 = it never left the gateway.
    pub attempts: Option<u32>,
    pub queue_ms: f64,
    pub ttft_ms: Option<f64>,
    pub total_ms: f64,
    pub input_tokens: f64,
    pub output_tokens: f64,
    /// Input tokens the server did not have to read again.
    pub cache_read_tokens: f64,
    /// `hit` when the gateway answered from its own cache.
    pub gateway_cache: String,
}

impl Row {
    pub fn ok(&self) -> bool {
        (200..400).contains(&self.status) && !self.client_abort
    }
    fn refused(&self) -> bool {
        self.status == 429 || self.status == 503
    }
    fn gave_up(&self) -> bool {
        self.client_abort || self.status == 0
    }
    /// The time it held a server, ms.
    fn served_ms(&self) -> f64 {
        if self.refused() || self.gateway_cache == "hit" || self.attempts == Some(0) {
            0.0
        } else {
            (self.total_ms - self.queue_ms).max(0.0)
        }
    }
    /// The time it waited for a place, ms. Older gateway logs record no
    /// wait for a caller that left while queued.
    fn waited_ms(&self) -> f64 {
        if self.attempts == Some(0) && self.gave_up() {
            self.queue_ms.max(self.total_ms)
        } else {
            self.queue_ms
        }
    }
    fn who(&self) -> &str {
        [
            &self.user_id,
            &self.claimed_user_id,
            &self.agent_id,
            &self.session_id,
        ]
        .into_iter()
        .find(|s| !s.is_empty())
        .map_or("", String::as_str)
    }
}

/// Parse JSON lines; unknown columns are ignored.
pub fn parse_rows(text: &str) -> Result<Vec<Row>, String> {
    text.lines()
        .filter(|l| !l.trim().is_empty())
        .enumerate()
        .map(|(i, l)| {
            let mut r: Row = serde_json::from_str(l).map_err(|e| format!("line {}: {e}", i + 1))?;
            if r.kind.is_empty() {
                r.kind = std::mem::take(&mut r.priority);
            }
            Ok(r)
        })
        .collect()
}

/// A finished run as rows. A class named `team · kind` becomes those two
/// columns; a client named `team~kind-7` is user `team-7`.
pub fn rows_from_sim(sim: &Sim) -> Vec<Row> {
    let backends = sim.backend_names();
    sim.reqs
        .iter()
        .map(|r| {
            let c = &sim.sc.clients[r.client];
            let class = sim.metrics.class_name(r.client);
            let (team, kind) = class.split_once(" · ").unwrap_or((class, ""));
            let user = match c.name.split_once('~') {
                Some((t, rest)) => match rest.rsplit_once('-') {
                    Some((_, n)) if n.chars().all(|x| x.is_ascii_digit()) => format!("{t}-{n}"),
                    _ => t.to_string(),
                },
                None => c.name.clone(),
            };
            let done = r.done_at.unwrap_or(sim.now);
            Row {
                ts: r.sent_at,
                team_id: team.to_string(),
                user_id: user,
                agent_id: c.agent.clone().unwrap_or_else(|| c.name.clone()),
                kind: kind.to_string(),
                model: sim.models[r.model].clone(),
                backend: r
                    .backend
                    .and_then(|b| backends.get(b).cloned())
                    .unwrap_or_default(),
                status: if r.status == GAVE_UP { 0 } else { r.status },
                client_abort: r.status == GAVE_UP,
                attempts: Some(if r.backend.is_some() {
                    r.tries.max(1)
                } else {
                    0
                }),
                queue_ms: r.queue_ms,
                ttft_ms: r.first_token_at.map(|t| t - r.sent_at),
                total_ms: done - r.sent_at,
                input_tokens: r.prompt_tokens,
                output_tokens: if r.status == 200 {
                    f64::from(r.out_tokens)
                } else {
                    0.0
                },
                cache_read_tokens: r.cached_tokens,
                ..Row::default()
            }
        })
        .collect()
}

/// What the analysis has to be told, because the rows do not say it.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Context {
    /// Where the rows come from, for the report's first line.
    pub source: String,
    /// True when the rows are a simulated run.
    pub simulated: bool,
    /// Length of a day in the rows' time (a simulated day is often
    /// shortened).
    pub day_s: f64,
    /// Hours to add to the timestamps to get local time.
    pub tz_hours: f64,
    /// A timestamp (ms) at which a day starts, for shortened days whose
    /// hours do not line up with the clock.
    #[serde(default)]
    pub day_start_ms: f64,
    /// Requests each model's servers run at once, all servers together.
    pub slots: BTreeMap<String, f64>,
}

impl Default for Context {
    fn default() -> Self {
        Self {
            source: String::new(),
            simulated: false,
            day_s: 86_400.0,
            tz_hours: 0.0,
            day_start_ms: 0.0,
            slots: BTreeMap::new(),
        }
    }
}

impl Context {
    /// Slots and day length as a scenario has them.
    pub fn from_scenario(sc: &crate::Scenario) -> Self {
        let mut slots = BTreeMap::new();
        for s in &sc.model_servers {
            for m in &s.models {
                *slots.entry(m.clone()).or_insert(0.0) += (s.slots * s.count.max(1)) as f64;
            }
        }
        let day_s = sc
            .clients
            .iter()
            .find_map(|c| c.rhythm.map(|r| r.period_s))
            .unwrap_or(86_400.0);
        Self {
            source: format!("시뮬레이션 ({})", sc.name),
            simulated: true,
            day_s,
            tz_hours: 0.0,
            day_start_ms: 0.0,
            slots,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct Group {
    pub team: String,
    pub kind: String,
    pub requests: usize,
    pub ok: usize,
    pub refused: usize,
    pub gave_up: usize,
    pub errors: usize,
    pub ttft_p50_s: Option<f64>,
    pub ttft_p95_s: Option<f64>,
    pub queue_p95_s: Option<f64>,
    pub input_tokens: f64,
    pub output_tokens: f64,
    pub cache_read_tokens: f64,
    /// Share of all server time (0 to 1).
    pub served_share: f64,
    /// Time between one caller's requests of this kind, median (s).
    pub gap_p50_s: Option<f64>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct Hour {
    pub hour: usize,
    /// Requests per real hour of that hour of day, per team.
    pub by_team: BTreeMap<String, f64>,
    pub requests_per_h: f64,
    /// Requests running at once, mean, per model.
    pub running: BTreeMap<String, f64>,
    /// Share of the slots in use, all models together; none without slots.
    pub used: Option<f64>,
    pub queue_p95_s: Option<f64>,
    pub refused_share: f64,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct ModelUse {
    pub model: String,
    pub slots: Option<f64>,
    pub running_mean: f64,
    pub running_peak: f64,
    /// Share of slot time not used; against the peak when slots are unknown.
    pub idle_share: f64,
    /// Unused slot-hours per day.
    pub idle_slot_hours_per_day: f64,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct Caller {
    pub who: String,
    pub team: String,
    pub requests: usize,
    pub served_share: f64,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct Analysis {
    pub context: Option<Context>,
    pub span_s: f64,
    pub days: f64,
    pub requests: usize,
    pub ok_share: f64,
    pub refused_share: f64,
    pub gave_up_share: f64,
    pub input_tokens: f64,
    pub output_tokens: f64,
    pub cache_read_share: f64,
    pub groups: Vec<Group>,
    pub hours: Vec<Hour>,
    pub models: Vec<ModelUse>,
    /// Per team: the hour it is busiest and the hours it is at half that
    /// or more.
    pub busy_hours: BTreeMap<String, (usize, Vec<usize>)>,
    /// Busiest three hours of all teams together over the sum of each
    /// team's own busiest three hours: 1 = everyone peaks together.
    pub coincidence: f64,
    /// Hours of day with under 30% of the slots in use.
    pub quiet_hours: Vec<usize>,
    /// Hours of day whose queue wait p95 is over 2 s or that refuse over 1%.
    pub pressed_hours: Vec<usize>,
    pub callers: usize,
    pub top_callers: Vec<Caller>,
    /// Share of server time used by the busiest tenth of callers.
    pub top_tenth_share: f64,
    /// Columns that were empty in every row, so parts of this are blank.
    pub missing: Vec<String>,
}

fn sorted(mut v: Vec<f64>) -> Vec<f64> {
    v.sort_by(f64::total_cmp);
    v
}

pub fn analyze(rows: &[Row], ctx: &Context) -> Analysis {
    let mut a = Analysis {
        context: Some(ctx.clone()),
        requests: rows.len(),
        ..Analysis::default()
    };
    if rows.is_empty() {
        return a;
    }
    let t0 = rows.iter().map(|r| r.ts).fold(f64::INFINITY, f64::min);
    let t1 = rows.iter().map(|r| r.ts + r.total_ms).fold(t0, f64::max);
    a.span_s = ((t1 - t0) / 1000.0).max(1.0);
    a.days = a.span_s / ctx.day_s;
    let n = rows.len() as f64;
    a.ok_share = rows.iter().filter(|r| r.ok()).count() as f64 / n;
    a.refused_share = rows.iter().filter(|r| r.refused()).count() as f64 / n;
    a.gave_up_share = rows.iter().filter(|r| r.gave_up()).count() as f64 / n;
    a.input_tokens = rows.iter().map(|r| r.input_tokens).sum();
    a.output_tokens = rows.iter().map(|r| r.output_tokens).sum();
    a.cache_read_share =
        rows.iter().map(|r| r.cache_read_tokens).sum::<f64>() / a.input_tokens.max(1.0);
    for (name, empty) in [
        ("team_id", rows.iter().all(|r| r.team_id.is_empty())),
        ("kind", rows.iter().all(|r| r.kind.is_empty())),
        ("backend", rows.iter().all(|r| r.backend.is_empty())),
        (
            "cache_read_tokens",
            rows.iter().all(|r| r.cache_read_tokens == 0.0),
        ),
        ("user_id", rows.iter().all(|r| r.who().is_empty())),
    ] {
        if empty {
            a.missing.push(name.to_string());
        }
    }

    // By team and kind.
    let served_all: f64 = rows.iter().map(Row::served_ms).sum::<f64>().max(1.0);
    let mut keys: Vec<(&str, &str)> = rows
        .iter()
        .map(|r| (r.team_id.as_str(), r.kind.as_str()))
        .collect();
    keys.sort_unstable();
    keys.dedup();
    for (team, kind) in keys {
        let mine: Vec<&Row> = rows
            .iter()
            .filter(|r| r.team_id == team && r.kind == kind)
            .collect();
        let ok: Vec<&&Row> = mine.iter().filter(|r| r.ok()).collect();
        let ttft = sorted(
            ok.iter()
                .filter_map(|r| r.ttft_ms)
                .map(|x| x / 1000.0)
                .collect(),
        );
        let queue = sorted(ok.iter().map(|r| r.queue_ms / 1000.0).collect());
        let mut by_caller: BTreeMap<&str, Vec<f64>> = BTreeMap::new();
        for r in mine.iter().filter(|r| !r.who().is_empty()) {
            by_caller.entry(r.who()).or_default().push(r.ts);
        }
        let gaps = sorted(
            by_caller
                .into_values()
                .flat_map(|ts| {
                    let ts = sorted(ts);
                    ts.windows(2)
                        .map(|w| (w[1] - w[0]) / 1000.0)
                        .collect::<Vec<_>>()
                })
                .collect(),
        );
        let refused = mine.iter().filter(|r| r.refused()).count();
        let gave_up = mine.iter().filter(|r| r.gave_up()).count();
        a.groups.push(Group {
            team: team.to_string(),
            kind: kind.to_string(),
            requests: mine.len(),
            ok: ok.len(),
            refused,
            gave_up,
            errors: mine.len() - ok.len() - refused - gave_up,
            ttft_p50_s: pct(&ttft, 50.0),
            ttft_p95_s: pct(&ttft, 95.0),
            queue_p95_s: pct(&queue, 95.0),
            input_tokens: mine.iter().map(|r| r.input_tokens).sum(),
            output_tokens: mine.iter().map(|r| r.output_tokens).sum(),
            cache_read_tokens: mine.iter().map(|r| r.cache_read_tokens).sum(),
            served_share: mine.iter().map(|r| r.served_ms()).sum::<f64>() / served_all,
            gap_p50_s: pct(&gaps, 50.0),
        });
    }

    // By hour of day. `seen[h]` is how long the rows cover that hour.
    let hour_ms = ctx.day_s * 1000.0 / 24.0;
    let shift = ctx.tz_hours * hour_ms - ctx.day_start_ms;
    let hour_of = |t: f64| (((t + shift) / hour_ms).floor().rem_euclid(24.0)) as usize;
    let mut seen = [0.0f64; 24];
    let mut t = t0;
    while t < t1 {
        let end = (((t + shift) / hour_ms).floor() + 1.0) * hour_ms - shift;
        let end = end.min(t1).max(t + 1.0);
        seen[hour_of(t)] += end - t;
        t = end;
    }
    let mut running: Vec<BTreeMap<String, f64>> = vec![BTreeMap::new(); 24];
    for r in rows.iter().filter(|r| r.served_ms() > 0.0) {
        let (mut s, e) = (r.ts + r.queue_ms, r.ts + r.total_ms);
        while s < e {
            let end = ((((s + shift) / hour_ms).floor() + 1.0) * hour_ms - shift)
                .min(e)
                .max(s + 1e-6);
            *running[hour_of(s)].entry(r.model.clone()).or_insert(0.0) += end - s;
            s = end;
        }
    }
    let total_slots: f64 = ctx.slots.values().sum();
    for h in 0..24 {
        if seen[h] <= 0.0 {
            a.hours.push(Hour {
                hour: h,
                ..Hour::default()
            });
            continue;
        }
        let mine: Vec<&Row> = rows.iter().filter(|r| hour_of(r.ts) == h).collect();
        // Rates are per hour of the rows' own day, so a shortened simulated
        // day reads like a real one.
        let per_h = hour_ms / seen[h];
        let mut by_team = BTreeMap::new();
        for r in &mine {
            *by_team.entry(r.team_id.clone()).or_insert(0.0) += per_h;
        }
        let run: BTreeMap<String, f64> = running[h]
            .iter()
            .map(|(m, ms)| (m.clone(), ms / seen[h]))
            .collect();
        let queue = sorted(mine.iter().map(|r| r.waited_ms() / 1000.0).collect());
        a.hours.push(Hour {
            hour: h,
            requests_per_h: mine.len() as f64 * per_h,
            by_team,
            used: (total_slots > 0.0).then(|| run.values().sum::<f64>() / total_slots),
            running: run,
            queue_p95_s: pct(&queue, 95.0),
            refused_share: mine.iter().filter(|r| r.refused()).count() as f64
                / mine.len().max(1) as f64,
        });
    }

    // Per model: how much runs at once, and how much room is left.
    let mut models: Vec<&str> = rows.iter().map(|r| r.model.as_str()).collect();
    models.sort_unstable();
    models.dedup();
    for m in models {
        let mut ev: Vec<(f64, f64)> = Vec::new();
        let mut busy_ms = 0.0;
        for r in rows.iter().filter(|r| r.model == m && r.served_ms() > 0.0) {
            ev.push((r.ts + r.queue_ms, 1.0));
            ev.push((r.ts + r.total_ms, -1.0));
            busy_ms += r.served_ms();
        }
        ev.sort_by(|x, y| x.0.total_cmp(&y.0).then(x.1.total_cmp(&y.1)));
        let (mut cur, mut peak) = (0.0f64, 0.0f64);
        for (_, d) in ev {
            cur += d;
            peak = peak.max(cur);
        }
        let mean = busy_ms / (a.span_s * 1000.0);
        let slots = ctx.slots.get(m).copied();
        let cap = slots.unwrap_or(peak).max(1.0);
        a.models.push(ModelUse {
            model: m.to_string(),
            slots,
            running_mean: mean,
            running_peak: peak,
            idle_share: (1.0 - mean / cap).clamp(0.0, 1.0),
            idle_slot_hours_per_day: (cap - mean).max(0.0) * 24.0,
        });
    }

    // When each team is busy, and whether they are busy together.
    let mut teams: Vec<&str> = rows.iter().map(|r| r.team_id.as_str()).collect();
    teams.sort_unstable();
    teams.dedup();
    for t in &teams {
        let rate = |h: &Hour| h.by_team.get(*t).copied().unwrap_or(0.0);
        let Some(peak) = a
            .hours
            .iter()
            .max_by(|x, y| rate(x).total_cmp(&rate(y)))
            .filter(|h| rate(h) > 0.0)
        else {
            continue;
        };
        let busy = a
            .hours
            .iter()
            .filter(|h| rate(h) >= rate(peak) * 0.5)
            .map(|h| h.hour)
            .collect();
        a.busy_hours.insert(t.to_string(), (peak.hour, busy));
    }
    // Over three-hour windows, so that a team with few requests does not
    // look out of step because its single busiest hour fell one hour off.
    let window_peak = |rate: &dyn Fn(&Hour) -> f64| {
        (0..24)
            .map(|h| (0..3).map(|d| rate(&a.hours[(h + d) % 24])).sum::<f64>())
            .fold(0.0, f64::max)
    };
    let own_peaks: f64 = teams
        .iter()
        .map(|t| window_peak(&|h: &Hour| h.by_team.get(*t).copied().unwrap_or(0.0)))
        .sum();
    let all_peak = window_peak(&|h: &Hour| h.requests_per_h);
    a.coincidence = if own_peaks > 0.0 {
        all_peak / own_peaks
    } else {
        0.0
    };
    let covered = |h: &&Hour| seen[h.hour] > 0.0;
    let peak_running = a
        .hours
        .iter()
        .map(|h| h.running.values().sum::<f64>())
        .fold(0.0, f64::max)
        .max(1e-9);
    a.quiet_hours = a
        .hours
        .iter()
        .filter(covered)
        .filter(|h| {
            h.used
                .unwrap_or(h.running.values().sum::<f64>() / peak_running)
                < 0.3
        })
        .map(|h| h.hour)
        .collect();
    a.pressed_hours = a
        .hours
        .iter()
        .filter(covered)
        .filter(|h| h.queue_p95_s.is_some_and(|q| q > 2.0) || h.refused_share > 0.01)
        .map(|h| h.hour)
        .collect();

    // Who uses the servers.
    let mut by: BTreeMap<&str, (f64, usize, &str)> = BTreeMap::new();
    for r in rows.iter().filter(|r| !r.who().is_empty()) {
        let e = by.entry(r.who()).or_insert((0.0, 0, &r.team_id));
        e.0 += r.served_ms();
        e.1 += 1;
    }
    let mut callers: Vec<Caller> = by
        .into_iter()
        .map(|(who, (ms, n, team))| Caller {
            who: who.to_string(),
            team: team.to_string(),
            requests: n,
            served_share: ms / served_all,
        })
        .collect();
    callers.sort_by(|x, y| y.served_share.total_cmp(&x.served_share));
    a.callers = callers.len();
    let tenth = callers.len().div_ceil(10);
    a.top_tenth_share = callers.iter().take(tenth).map(|c| c.served_share).sum();
    callers.truncate(5);
    a.top_callers = callers;
    a
}

fn pc(x: f64) -> String {
    format!("{:.0}%", x * 100.0)
}

fn num(x: f64) -> String {
    let s = format!("{:.0}", x.abs());
    let mut out = String::new();
    for (i, c) in s.chars().enumerate() {
        if i > 0 && (s.len() - i) % 3 == 0 {
            out.push(',');
        }
        out.push(c);
    }
    if x < 0.0 {
        format!("-{out}")
    } else {
        out
    }
}

fn secs(x: Option<f64>) -> String {
    x.map_or("–".into(), |v| format!("{v:.2}"))
}

/// Hours as ranges: `[9, 10, 11, 14]` is `9~12시, 14~15시`.
fn hours(list: &[usize]) -> String {
    if list.is_empty() {
        return "없음".into();
    }
    let mut out = Vec::new();
    let mut i = 0;
    while i < list.len() {
        let mut j = i;
        while j + 1 < list.len() && list[j + 1] == list[j] + 1 {
            j += 1;
        }
        out.push(format!("{}~{}시", list[i], list[j] + 1));
        i = j + 1;
    }
    out.join(", ")
}

/// What stands out, with the number behind each line. Rules, not a model:
/// each line says what it compared.
pub fn findings(a: &Analysis) -> Vec<String> {
    let mut out = Vec::new();
    let teams = a.busy_hours.len();
    if teams >= 2 && a.coincidence >= 0.85 && !a.pressed_hours.is_empty() {
        out.push(format!(
            "팀들이 같은 시간대에 몰립니다. 전체가 가장 바쁜 세 시간의 요청량이 각 팀의 가장 바쁜 세 시간을 더한 값의 {}입니다(100%면 완전히 겹침). 대기가 길어지는 시간대는 {}이고, 한가한 시간대는 {}입니다. 팀별 주 사용 시간을 어긋나게 하면 같은 서버로 이 시간대의 대기를 줄일 수 있습니다.",
            pc(a.coincidence),
            hours(&a.pressed_hours),
            hours(&a.quiet_hours)
        ));
    } else if !a.pressed_hours.is_empty() {
        out.push(format!(
            "{}에 대기 시간 p95가 2초를 넘거나 거절이 1%를 넘습니다. 팀들의 최대 사용 시간대는 이미 어긋나 있어(겹침 {}), 시차 조정보다 이 시간대의 용량이나 우선순위 조정이 맞습니다.",
            hours(&a.pressed_hours),
            pc(a.coincidence)
        ));
    }
    let stalled: Vec<usize> = a
        .hours
        .iter()
        .filter(|h| a.pressed_hours.contains(&h.hour))
        .filter(|h| h.used.is_some_and(|u| u < 0.3))
        .map(|h| h.hour)
        .collect();
    if stalled.len() >= 2 {
        out.insert(
            0,
            format!(
                "**{}에는 요청이 밀리는데 서버는 슬롯의 30%도 쓰지 않았습니다.** 부하가 아니라 장애 모양입니다. 모델 서버가 죽었거나, 게이트웨이가 살아 있는 서버로 요청을 보내지 못한 시간대인지 먼저 확인해야 합니다.",
                hours(&stalled)
            ),
        );
    }
    let idle: f64 = a.models.iter().map(|m| m.idle_slot_hours_per_day).sum();
    if idle > 0.0 && !a.quiet_hours.is_empty() {
        let batch: f64 = a
            .groups
            .iter()
            .filter(|g| g.kind.contains("batch"))
            .map(|g| g.served_share)
            .sum();
        let mut s = format!(
            "서버가 하루에 슬롯 기준 {}시간만큼 비어 있습니다({} 사용률 30% 미만).",
            num(idle),
            hours(&a.quiet_hours)
        );
        if batch > 0.0 {
            let _ = write!(
                s,
                " 급하지 않은 batch 요청이 서버 시간의 {}를 쓰고 있으니, 이 요청을 한가한 시간대로 미루는 것이 첫 후보입니다.",
                pc(batch)
            );
        }
        out.push(s);
    }
    if a.pressed_hours.is_empty() && a.models.iter().any(|m| m.slots.is_some()) {
        let peak_used = a.hours.iter().filter_map(|h| h.used).fold(0.0, f64::max);
        if peak_used < 0.7 {
            let per: Vec<String> = a
                .models
                .iter()
                .filter_map(|m| {
                    m.slots
                        .map(|n| format!("{} {:.0}/{}", m.model, m.running_peak, num(n)))
                })
                .collect();
            out.push(format!(
                "가장 바쁜 시간대에도 슬롯의 {}만 쓰고 대기가 없습니다. 순간 최대 동시 처리는 {}입니다. 지금 요청량이라면 서버가 남습니다.",
                pc(peak_used),
                per.join(", ")
            ));
        }
    }
    if a.callers >= 10 && a.top_tenth_share >= 0.4 {
        out.push(format!(
            "호출자 상위 10%가 서버 시간의 {}를 씁니다. 가장 많이 쓰는 호출자는 {}({})입니다. 과부하 때 이들이 나머지의 대기를 늘리므로, 적게 쓴 사람 먼저 내보내는 줄 세우기나 호출자별 한도가 효과를 볼 조건입니다.",
            pc(a.top_tenth_share),
            a.top_callers.first().map_or("", |c| c.who.as_str()),
            a.top_callers
                .first()
                .map_or(String::new(), |c| pc(c.served_share)),
        ));
    }
    if !a.missing.iter().any(|m| m == "cache_read_tokens") {
        let low: Vec<String> = a
            .groups
            .iter()
            .filter(|g| g.input_tokens > 0.02 * a.input_tokens)
            .filter(|g| g.cache_read_tokens / g.input_tokens.max(1.0) < 0.25)
            .map(|g| {
                format!(
                    "{} {} ({})",
                    g.team,
                    g.kind,
                    pc(g.cache_read_tokens / g.input_tokens.max(1.0))
                )
            })
            .collect();
        if !low.is_empty() {
            out.push(format!(
                "입력 토큰 중 캐시로 다시 읽지 않은 비율은 전체 {}입니다. 25%에 못 미치는 묶음: {}. 공개 트레이스에서는 첫 요청도 프롬프트의 42~57%가 다른 요청과 겹쳤으므로, 이보다 낮으면 분배가 캐시를 흩고 있거나 서버가 기억하는 앞부분 수가 모자란 것입니다.",
                pc(a.cache_read_share),
                low.join(", ")
            ));
        }
    }
    if a.refused_share + a.gave_up_share > 0.02 {
        let worst = a.groups.iter().filter(|g| g.requests >= 20).max_by(|x, y| {
            let f = |g: &Group| (g.refused + g.gave_up) as f64 / g.requests as f64;
            f(x).total_cmp(&f(y))
        });
        if let Some(g) = worst {
            out.push(format!(
                "응답을 받지 못한 요청이 {}입니다(거절 {}, 호출자 포기 {}). 가장 심한 묶음은 {} {}로 {}건 중 {}건입니다.",
                pc(a.refused_share + a.gave_up_share),
                pc(a.refused_share),
                pc(a.gave_up_share),
                g.team,
                g.kind,
                num(g.requests as f64),
                num((g.refused + g.gave_up) as f64)
            ));
        }
    }
    if out.is_empty() {
        out.push("눈에 띄는 쏠림이나 포화가 없습니다.".into());
    }
    out
}

/// The report, in Korean Markdown.
pub fn report(a: &Analysis) -> String {
    let ctx = a.context.clone().unwrap_or_default();
    let mut s = String::new();
    let _ = writeln!(s, "# LLM 게이트웨이 사용 패턴 보고서\n");
    let _ = writeln!(
        s,
        "출처: {}. 요청 {}건, 관측 구간은 하루의 {:.1}배입니다.{}\n",
        if ctx.source.is_empty() {
            "요청 로그"
        } else {
            &ctx.source
        },
        num(a.requests as f64),
        a.days,
        if ctx.simulated {
            format!(
                " **실제 사용 기록이 아니라 시뮬레이션 결과입니다.** 하루를 {}초로 줄여 돌렸고, 시간대는 그 하루를 24로 나눈 것입니다.",
                num(ctx.day_s)
            )
        } else if (ctx.day_s - 86_400.0).abs() > 1.0 {
            format!(
                " 하루를 {}초로 줄여 넣은 부하이고, 시간대는 그 하루를 24로 나눈 것입니다.",
                num(ctx.day_s)
            )
        } else {
            String::new()
        }
    );
    let _ = writeln!(s, "## 요약\n");
    for f in findings(a) {
        let _ = writeln!(s, "- {f}");
    }
    let _ = writeln!(s, "\n## 전체 수치\n");
    let _ = writeln!(s, "| 항목 | 값 |\n|---|---|");
    let _ = writeln!(s, "| 응답받은 요청 | {} |", pc(a.ok_share));
    let _ = writeln!(s, "| 거절 (429, 503) | {} |", pc(a.refused_share));
    let _ = writeln!(s, "| 호출자가 포기 | {} |", pc(a.gave_up_share));
    let _ = writeln!(s, "| 입력 토큰 | {} |", num(a.input_tokens));
    let _ = writeln!(s, "| 출력 토큰 | {} |", num(a.output_tokens));
    let _ = writeln!(
        s,
        "| 캐시로 다시 읽지 않은 입력 토큰 | {} |",
        pc(a.cache_read_share)
    );
    let _ = writeln!(s, "| 호출자 수 | {} |", num(a.callers as f64));

    let _ = writeln!(s, "\n## 팀과 요청 유형별\n");
    let _ = writeln!(
        s,
        "| 팀 | 유형 | 요청 | 응답률 | 첫 토큰 p50 / p95 (초) | 대기 p95 (초) | 서버 시간 비중 | 캐시 재사용 | 같은 호출자의 요청 간격 p50 (초) | 출력 토큰 |\n|---|---|---|---|---|---|---|---|---|---|"
    );
    for g in &a.groups {
        let _ = writeln!(
            s,
            "| {} | {} | {} | {} | {} / {} | {} | {} | {} | {} | {} |",
            if g.team.is_empty() { "–" } else { &g.team },
            if g.kind.is_empty() { "–" } else { &g.kind },
            num(g.requests as f64),
            pc(g.ok as f64 / g.requests.max(1) as f64),
            secs(g.ttft_p50_s),
            secs(g.ttft_p95_s),
            secs(g.queue_p95_s),
            pc(g.served_share),
            pc(g.cache_read_tokens / g.input_tokens.max(1.0)),
            g.gap_p50_s.map_or("–".into(), |x| format!("{x:.0}")),
            num(g.output_tokens)
        );
    }

    let _ = writeln!(s, "\n## 시간대별\n");
    if (ctx.day_s - 86_400.0).abs() > 1.0 {
        let _ = writeln!(
            s,
            "여기서 한 시간은 줄인 하루의 1/24({}초)이고, 시간당 요청은 그 한 칸에 들어온 건수입니다.\n",
            num(ctx.day_s / 24.0)
        );
    }
    let teams: Vec<&String> = a.busy_hours.keys().collect();
    let _ = write!(s, "| 시 | 시간당 요청 |");
    for t in &teams {
        let _ = write!(s, " {t} |");
    }
    let _ = writeln!(s, " 슬롯 사용률 | 대기 p95 (초) | 거절 |");
    let _ = writeln!(s, "|---|---|{}---|---|---|", "---|".repeat(teams.len()));
    for h in &a.hours {
        let _ = write!(s, "| {} | {} |", h.hour, num(h.requests_per_h));
        for t in &teams {
            let _ = write!(s, " {} |", num(h.by_team.get(*t).copied().unwrap_or(0.0)));
        }
        let _ = writeln!(
            s,
            " {} | {} | {} |",
            h.used.map_or("–".into(), pc),
            secs(h.queue_p95_s),
            pc(h.refused_share)
        );
    }
    let _ = writeln!(s);
    for (t, (peak, busy)) in &a.busy_hours {
        let _ = writeln!(
            s,
            "- {}: 가장 바쁜 시간 {}시, 그 절반 이상인 시간대 {}",
            if t.is_empty() {
                "(팀 표시 없음)"
            } else {
                t
            },
            peak,
            hours(busy)
        );
    }

    let _ = writeln!(s, "\n## 서버 여유\n");
    let _ = writeln!(
        s,
        "| 모델 | 슬롯 | 동시 처리 평균 | 최대 | 비어 있는 비율 | 하루에 비는 슬롯·시간 |\n|---|---|---|---|---|---|"
    );
    for m in &a.models {
        let _ = writeln!(
            s,
            "| {} | {} | {:.1} | {:.0} | {} | {} |",
            m.model,
            m.slots.map_or("모름 (관측 최대치 기준)".into(), num),
            m.running_mean,
            m.running_peak,
            pc(m.idle_share),
            num(m.idle_slot_hours_per_day)
        );
    }
    let _ = writeln!(
        s,
        "\n사용률 30% 미만인 시간대: {}. 대기 p95가 2초를 넘거나 거절이 1%를 넘는 시간대: {}.",
        hours(&a.quiet_hours),
        hours(&a.pressed_hours)
    );

    let _ = writeln!(s, "\n## 많이 쓰는 호출자\n");
    let _ = writeln!(
        s,
        "호출자 상위 10%가 서버 시간의 {}를 씁니다.\n",
        pc(a.top_tenth_share)
    );
    let _ = writeln!(
        s,
        "| 호출자 | 팀 | 요청 | 서버 시간 비중 |\n|---|---|---|---|"
    );
    for c in &a.top_callers {
        let _ = writeln!(
            s,
            "| {} | {} | {} | {:.1}% |",
            c.who,
            c.team,
            num(c.requests as f64),
            c.served_share * 100.0
        );
    }

    let _ = writeln!(s, "\n## 이 보고서를 읽을 때\n");
    if ctx.simulated {
        let _ = writeln!(
            s,
            "- 시뮬레이션입니다. 인원, 요청 빈도, 집중 시간대는 구성 설정에 넣은 값이고, 서버 속도는 가정입니다. 프롬프트 길이와 반복 비율만 공개 트레이스에서 가져왔습니다."
        );
        let _ = writeln!(
            s,
            "- 하루를 줄여 돌렸기 때문에 요청 하나의 길이는 그대로인데 시간대는 짧습니다. 시간대 간 비교는 의미가 있지만 대기 시간의 절대값은 실제 하루와 다릅니다."
        );
    }
    if !a.missing.is_empty() {
        let _ = writeln!(
            s,
            "- 로그에 값이 없는 열: {}. 해당 표는 비어 있거나 한 묶음으로 나옵니다.",
            a.missing.join(", ")
        );
    }
    if ctx.slots.is_empty() {
        let _ = writeln!(
            s,
            "- 서버 슬롯 수를 받지 못해, 여유는 관측된 최대 동시 처리 대비로 계산했습니다. 실제 여유는 이보다 큽니다."
        );
    }
    let _ = writeln!(
        s,
        "- 요약의 문장은 규칙으로 고른 것입니다. 각 문장에 비교한 값을 적었습니다."
    );
    s
}
