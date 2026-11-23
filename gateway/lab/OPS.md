# Day-to-day operation on local model servers

`lab/ops_check.py` with `lab/gateway.ops.toml`: three single-slot llama.cpp
servers (Qwen2.5-0.5B, one thread each, 4 vCPU) behind one upstream of
`pybox-gateway-tdb` (log in TDB), two callers sending short streaming chats
without pause (16 output tokens, about 1.2 s each). One run per row,
2026-11-22. No hosted API was called.

    LAB=~/lab python lab/ops_check.py --out ~/lab/ops

## One server dies under load and comes back 20 s later

| | probes off | probes every 2 s |
|---|---|---|
| requests answered | 76 of 76 | 62 of 62 |
| requests that needed a second attempt while it was out | 2 | 1 |
| gateway took it out after | 0.24 s | 0.14 s |
| back in use after the server was ready | 12.4 s | 4.4 s |
| record of the return | "a request succeeded" | "probe answered" |

No caller saw a failure either way: a refused connection is retried on
another server. Under steady load the dead server is found by a request
before the next probe is due, so the time to notice is the same. The
difference is on the way back. Without probes the server waits out its pause
(10 s, then 20 s, doubling to two minutes) and the next caller's request is
the test, which cost one more retried request here; with probes it is back
within one probe interval plus the time for a request to be routed to it.
The run with probes sent fewer requests in the same script because it was in
use again sooner and the script ends 5 s after that.

Not shown by this run: a server that dies while idle. The integration test
`probes_find_a_dead_server_and_bring_it_back` covers it (out before any
request, no request spent on it).

## Every server dies

| | |
|---|---|
| all three marked out after the kill (probes every 2 s, no traffic) | 1.5 s |
| what a caller gets | 503, `x-gateway-reject-reason: no_server`, `retry-after`, in 1 to 2 ms |
| gateway restarted while all were out: agent made over the API | still accepted (503 `no_server`, not 401), listed |
| outage record after that restart | 8 events kept (TDB store metadata) |
| all three back in rotation after the servers answered again | 0.7 s |
| first six requests after that | all 200, first attempt |

Before `refuse_when_down` the same caller waited `queue_timeout_secs`
(20 s here) for the same answer.

## Caller-side timings and the report

Both callers and the runtime agent reported every answer to
`POST /gateway/obs/client` (124 reports):

| | caller | gateway | difference |
|---|---|---|---|
| time to first token, median | 136 ms | 135 ms | 1.5 ms |
| time to first token, 95% | 201 ms | 195 ms | 5.8 ms |
| whole answer, median | 1,239 ms | 1,237 ms | 2.0 ms |

Caller and gateway are on one machine here, so the way back is a
millisecond or two; the point of the run is that the two views line up and
the difference is measured, not that it is small.

`POST /gateway/reports/run` wrote `roi-day-2026-11-22.html` and `.json` over
131 requests with 11 outage lines; the schedule itself had already written
the (empty) report for the day before at start, since it was missing.
