# Usage report on the real log

The usage report (`gwsim usage`, see `sim/README.md`) run end to end on the
gateway's own log, 2026-11-14: a day-shaped load against the real gateway
with its log in TDB, the log taken out through `/gateway/obs/export`, the
report built from it. Local models only.

## The run

- Three `slot_server.py` servers (Qwen2.5-0.5B, one request at a time, two
  cache slots each), `pybox-gateway-tdb` with `lab/gateway.day.toml`: one
  upstream, prefix routing, staged refusal, one agent per team and kind so
  the log carries `team_id` and `priority`.
- `lab/day_drive.py`: three teams, 24 people, ide / chat / batch, one bump a
  day, a day shortened to 30 minutes (an "hour" is 75 s), 513 requests at a
  peak of 0.9 arrivals per second.
- `lab/usage_report.sh OUT FROM_MS TO_MS` exports the window and writes
  `report.md` and `report.json`.

```sh
lab/prefix_stack.sh slots 2
(cd tdb-backend && cargo build --release) && tdb-backend/target/release/pybox-gateway-tdb --config lab/gateway.day.toml &
python3 lab/day_drive.py --out client.jsonl --day-s 1800 --peak 0.9 --quiet 0.1   # prints start_ms / end_ms
ADMIN_TOKEN=lab-agent-admin TZ_HOURS=0 DAY_S=1800 DAY_START_MS=<start_ms> SLOTS=qwen-0.5b=3 \
  lab/usage_report.sh out <start_ms> <end_ms>
```

The report from that run is `sim/reference/usage/REAL-DAY.md`. It agrees
with the driver's own record (512 of 513 answered, one ide caller gave up)
and finds what was put in: the three teams busy in the same hours (98%
overlap), waits over 2 s from 9 to 16 with the servers 75 to 84% used,
under 30% used outside 9 to 19, the first person of each team using 41% of
the server time, batch reusing less of its prompt than the other kinds.

## What broke on the real log, and the fixes

| What | Fix |
|---|---|
| A real row has both `backend` and `upstream_ip`; the reader took them for the same column and refused the file | `sim/src/usage.rs`: only `backend` is read |
| The request kind is in `priority` (interactive, normal, batch), not `kind` | read as the kind when `kind` is empty |
| Who sent it is in `claimed_user_id` (`x-gateway-user-id`) when the key names no user | used after `user_id` |
| A day shortened for a test does not start on the hour, so hours of day were arbitrary | `--day-start MS` |
| A caller that left while queued was logged with `queue_ms` 0 and counted as holding a server for its whole wait: 745% of the slots "in use" at the peak | gateway (`obs/trace.rs`): such a row now carries the time it waited; report: a row with `attempts` 0 holds no server, and its wait is its whole time for logs written before the fix |
| `slot_server.py` crashed (segfault inside llama.cpp) as soon as callers hung up mid-answer | one worker thread holds the model for a whole answer |

The last one was found by the first attempt at this run (peak 1.5 arrivals
per second): all three servers died within a minute of the first client
timeouts and nothing was answered for the rest of the day. The report on
that log is `sim/reference/usage/REAL-DAY-OUTAGE.md`; its first line is
the new finding for this shape, callers waiting beside idle servers.

Seen in that run and not changed: with every server gone the gateway kept
queueing and refusing in stages; no row says the servers were unreachable
(statuses were 0 with `client_abort`, and 503 `gateway_shed_early`). The
callers found out by timing out.

## A regular report

What a scheduled run needs is all in `lab/usage_report.sh`: the gateway's
address and admin token, the `gwsim` binary, the number of slots per model
(`SLOTS`, otherwise spare capacity is measured against the most seen at
once), the time zone, and somewhere to put `report.md`. It has to run where
the gateway's admin API can be reached. Nothing is scheduled.

Not measured: a real 24-hour day (the hour tables then need no
`DAY_S` / `DAY_START_MS`), export of a large day (the endpoint builds the
whole answer in memory; 880 rows here), more than one model.
