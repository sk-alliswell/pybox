# ROI dashboard: productivity and suggestions

Page: `/gateway/roi` (admin token). Data: `GET /gateway/obs/insights?from=MS&to=MS[&tz=9][&server_hour_usd=X][&slots=N][&day_s=N]`.
Code: `src/obs/insight.rs` (report and rules), `src/obs/roi.html` (page), `src/bin/gateway-insight.rs` (same report from an exported log).

The existing `/gateway/obs/roi` answers "what did each deliverable cost and return". This adds "who spent it, where it was lost, and what to change", for three readers:

| Reader | What they see | Suggestions about |
|---|---|---|
| Person | spend, accepted deliverables (shared ones split by spend), cost per accepted, where the spend went, cache reuse and input per request against the team, failed requests, time spent waiting | prompt cache, long context, failed or abandoned requests, small requests on an expensive model, untagged use, rejected work, batch work at busy hours |
| Team | the same added up, plus the largest user's share | concentration on one person, negative ROI, cost per accepted against other teams, untagged use, waste, cache reuse, batch work at busy hours, waiting, spread inside the team |
| Serving | per hour of day: offered work against capacity, queue p95; per server and per model | holding batch work, servers needed at the peak, refusals, idle capacity, uneven load between servers with the same model, slow first token without a queue, speed drop under load, errors, stalls, routing small requests to a small model |

## How the numbers are made

- **Spend.** `cost_usd` from the rows. When rows carry none (self-hosted models) and `server_hour_usd` is given, a request is charged `(total_ms - queue_ms) x price / slots`; the rest of the fleet's price is reported as idle cost. With neither, shares are computed on server time and no dollar figures are shown.
- **Where spend went.** Each request lands in one class: useful (deliverable accepted or in progress), waste (failed or aborted request, deliverable rejected or abandoned), unattributed. The three shares add up to 1.
- **Value.** Accepted deliverables only: `value_usd`, else `hours_saved` x the team's hourly rate. A deliverable several people spent on is split in proportion to their spend, for people and for teams. (`/gateway/obs/roi` assigns by the deliverable's own team; the two differ only when one team spends on another team's deliverable.)
- **Waiting.** For requests that are not `batch`, time to first token is added up and, with an hourly rate, priced. It assumes a person waited; unattended agent calls make it too large.
- **Capacity.** Slots per server are the largest number of requests seen at once unless `slots` is given. An hour is pressed when queue p95 passes 2 s, refusals pass 1%, or offered work passes capacity; idle below 30%.
- **Savings.** `measured` is spend already in the log (failed requests, rejected work). `estimate` assumes the person reaches the team's level (cache reuse, input size) or the cheapest model's price. Suggestions overlap, so their sum is an upper bound.

Every threshold is in `insight::limits`, returned in the report and listed on the page's last tab. They are fixed guesses, not fitted to data. People and teams with fewer than 20 requests get numbers but no suggestions.

## From a log file

    cargo run --release --bin gateway-insight -- ROWS.jsonl --meta META.json --html roi.html --json roi.json
    cargo run --release --bin gateway-insight -- --demo --slots 2 --html roi-demo.html

`ROWS.jsonl` is the output of `/gateway/obs/export`. `--demo` generates a made-up week (3 teams of 12, 3 servers) with known habits planted, to show the page; nothing in it is measured. For lab runs that squeeze a day into minutes, pass `--day-s`.

## Checked on

- The 2026-11-14 local run (`lab/USAGE.md`, 513 requests, 3 `slot_server.py` servers, 30-minute day): pressed hours 10 to 16, batch work at the peak, idle capacity, and "no deliverables registered". The log has no cost, deliverables or sessions, so ROI and the cost rules had nothing to work on there.
- The generated week: each planted habit is found on the person it was planted on.

## Not done

- Suggestions are rules over one window; there is no before/after tracking of whether a suggestion was followed.
- No request content is read, so "duplicate requests" and answer quality are not judged.
- The hour-of-day view folds all days together; weekdays and weekends are not separated.
- The emulator (`sim/`) does not produce deliverables, so it cannot feed the ROI columns yet.
