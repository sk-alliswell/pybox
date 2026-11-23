# Predicted against measured

How close the simulator gets to runs that were actually made. Regenerate with

```sh
gwsim validate-all scenarios/validation.toml --data DIR_WITH_SAT_LOGS --seeds 20 --markdown out.md
```

The measured runs are the saturation experiments of `gateway/lab` (PR #3):
`lab/saturate.py` sends about four times what two single-slot local model
servers can serve, straight at LiteLLM or through the gateway in different
configurations. Logs: project files `gateway-console/saturation/`.

> 2026-11-14: the scheduler model now follows the gateway on main (token-counter
> order). The tables below were made before that. With it the held-out runs
> are at 0.8 points median (worst 4.6) for shares and 2.0% median (p90 11.5%,
> worst 29.9%) for the rest; interactive time to first token during the flood
> of `sat-tight` and `sat-bounded-patient` moved 6 to 9% lower.
> `reference/baseline.json` holds the current numbers, and
> `reference/overload/RESULTS.md` the comparison with the overload runs.

## How the test is set up

- **Two calibration runs, five held-out runs.** The measurements come from two
  sessions (the machine was about 15% faster in the second). For each session
  the model-server numbers (tokens per second, prefill, cost of an abandoned
  request) were fitted with `gwsim calibrate` on the run that had no gateway.
  The other runs of the session are predicted by changing only what the
  experiment changed: the gateway's queue settings and the callers' behaviour.
  Nothing was tuned on a held-out run.
- **Same input.** Requests arrive at the times the measured log recorded, so
  the comparison tests the model of the system, not the luck of the arrival
  process. Prediction is the mean over 20 seeds (which vary service times).
- **Same arithmetic.** Both sides are summarised by one function, a port of
  `lab/analyze_sat.py`.
- **Error** is percentage points for shares and percent of the measured value
  otherwise. Each measured run was made once, so the measured numbers carry
  noise of their own (see the last section for how much).

- **Kept honest in CI.** The arrivals and measured numbers of each run are in
  `reference/`, and `cargo test` fails when a prediction moves further from
  its measurement than `reference/baseline.json` allows (README, "Keeping the
  accuracy"). Open gaps are tracked in `KNOWN-GAPS.md`.

## Result

With measured arrivals: Held-out experiments: 5. Success shares (13 numbers): median error 1.2 points, 90th percentile 4.7, worst 7.1. Latency, throughput and counts (49 numbers): median error 1.6%, 90th percentile 11.9%, worst 30.5%.

What the model gets right on runs it was not fitted to: who gets answered
(12% of interactive requests straight at LiteLLM against 98% with the smart
queue: predicted 12% and 98%), how long the answered ones waited, how many
were refused or gave up, how many retried (498 measured, 494 predicted), the
longest gateway queue (248, 56, 8, 60, 294 measured; 246, 58, 8, 60, 288
predicted) and when the backlog was finally gone. In the smart-queue run it
also reproduces one server finishing early and idling while the other still
had a backlog: last answers at 453 s and 628 s measured, 447 s and 638 s
simulated.

Where it is off, and why:

- **Interactive latency near saturation** (`sat-smart`, TTFT p50 during the flood -20%). With
  strict priority, interactive traffic alone uses about 97% of one server, and
  at that load a 3% error in service time moves the wait by tens of percent.
  The number is reported, not tuned away.
- **Whole-run interactive latency in `sat-plain`** (-31%). The measured run
  recovered more slowly after the flood than the model does; the per-phase
  numbers during the flood are within 5%.
- **Short queue** (`sat-tight`): 5 to 6 points fewer interactive answers than
  measured, the largest miss on a success share.
- **Small counts** (7 callers gave up in `sat-smart`, 5 predicted) are noisy on
  both sides and are left out of the summary line.

Not covered by any measured replay yet (see README, "What is assumed"):
LiteLLM's CPU saturation, hosted APIs, MCP servers, the log store, host CPU
contention beyond one fitted number, adaptive concurrency and priority classes. The gateway's
memory growth under a retry storm (390 MB measured) is not modelled.

## Per experiment

### sat-direct (calibration)

No gateway: callers send straight to LiteLLM, about 4x what the two model servers can serve.

| metric | measured | predicted | range over 20 seeds | error |
|---|---|---|---|---|
| interactive answered, flood | 4% | 1% | 1% to 2% | -2 pt |
| interactive TTFT p50, flood | 13.46 | 12.56 | 9.03 to 13.95 | -7% |
| interactive TTFT p95, flood | 14.11 | 13.68 | 13.15 to 14.41 | -3% |
| interactive answered, whole run | 13% | 11% | 11% to 12% | -1 pt |
| interactive TTFT p50, whole run | 1.56 | 1.76 | 1.42 to 2.47 | +12% |
| interactive total p50, whole run | 2.50 | 2.72 | 2.31 to 3.37 | +8% |
| batch answered | 25% | 27% | 26% to 27% | +2 pt |
| batch TTFT p50 | 45.56 | 48.80 | 47.38 to 49.74 | +7% |
| batch TTFT p95 | 83.10 | 86.86 | 86.45 to 87.34 | +5% |
| completed per minute, flood | 41.50 | 41.55 | 40.50 to 42.50 | +0% |
| gave up at own timeout | 518 | 518 | 516 to 520 | -0% |
| refused (503) | 0 | 0 | 0 to 0 | +0% |
| requests that retried | 0 | 0 | 0 to 0 | +0% |
| last answer at | 207.23 | 221.24 | 218.44 to 221.71 | +7% |

### sat-plain (held-out)

Gateway with an unbounded queue, round robin between the two agents.

| metric | measured | predicted | range over 20 seeds | error |
|---|---|---|---|---|
| interactive answered, flood | 10% | 10% | 8% to 12% | -0 pt |
| interactive TTFT p50, flood | 12.98 | 12.43 | 11.72 to 12.94 | -4% |
| interactive TTFT p95, flood | 14.07 | 13.98 | 13.81 to 14.16 | -1% |
| interactive answered, whole run | 32% | 33% | 31% to 34% | +1 pt |
| interactive TTFT p50, whole run | 10.61 | 7.37 | 6.28 to 7.77 | -31% |
| interactive total p50, whole run | 11.57 | 8.27 | 7.06 to 8.71 | -29% |
| batch answered | 23% | 23% | 22% to 25% | +0 pt |
| batch TTFT p50 | 47.17 | 47.44 | 45.83 to 49.35 | +1% |
| batch TTFT p95 | 85.78 | 85.27 | 83.63 to 86.63 | -1% |
| completed per minute, flood | 45.50 | 45.83 | 44.00 to 48.50 | +1% |
| gave up at own timeout | 455 | 452 | 445 to 457 | -1% |
| refused (503) | 0 | 0 | 0 to 0 | +0% |
| requests that retried | 0 | 0 | 0 to 0 | +0% |
| last answer at | 225.12 | 224.78 | 224.06 to 227.52 | -0% |
| longest gateway queue | 248 | 246 | 246 to 246 | -1% |

### sat-bounded (held-out)

Gateway queue capped at 30 with a 12 s wait limit; interactive agent weight 4.

| metric | measured | predicted | range over 20 seeds | error |
|---|---|---|---|---|
| interactive answered, flood | 62% | 60% | 59% to 62% | -2 pt |
| interactive TTFT p50, flood | 11.96 | 11.69 | 11.34 to 12.24 | -2% |
| interactive TTFT p95, flood | 13.60 | 13.57 | 13.37 to 13.72 | -0% |
| interactive answered, whole run | 74% | 72% | 71% to 73% | -2 pt |
| interactive TTFT p50, whole run | 10.73 | 10.77 | 10.36 to 11.33 | +0% |
| interactive total p50, whole run | 11.60 | 11.72 | 11.24 to 12.28 | +1% |
| batch answered | 12% | 12% | 12% to 13% | -0 pt |
| batch TTFT p50 | 12.18 | 12.45 | 12.08 to 12.76 | +2% |
| batch TTFT p95 | 13.68 | 13.54 | 13.21 to 13.66 | -1% |
| completed per minute, flood | 84.50 | 82.15 | 80.50 to 84.00 | -3% |
| gave up at own timeout | 8 | 12 | 8 to 14 | +46% |
| refused (503) | 338 | 342 | 339 to 344 | +1% |
| requests that retried | 0 | 0 | 0 to 0 | +0% |
| last answer at | 204.88 | 205.71 | 205.17 to 206.17 | +0% |
| longest gateway queue | 56 | 58 | 57 to 59 | +3% |

### sat-tight (held-out)

Gateway queue capped at 4 with a 4 s wait limit; interactive agent weight 4.

| metric | measured | predicted | range over 20 seeds | error |
|---|---|---|---|---|
| interactive answered, flood | 38% | 31% | 27% to 37% | -7 pt |
| interactive TTFT p50, flood | 4.74 | 4.82 | 4.45 to 5.20 | +2% |
| interactive TTFT p95, flood | 7.17 | 7.28 | 6.71 to 7.62 | +2% |
| interactive answered, whole run | 58% | 53% | 51% to 57% | -5 pt |
| interactive TTFT p50, whole run | 3.52 | 3.54 | 3.33 to 3.80 | +0% |
| interactive total p50, whole run | 4.30 | 4.51 | 4.29 to 4.65 | +5% |
| batch answered | 20% | 20% | 19% to 21% | +0 pt |
| batch TTFT p50 | 4.96 | 5.02 | 4.75 to 5.38 | +1% |
| batch TTFT p95 | 6.94 | 7.21 | 6.72 to 7.58 | +4% |
| completed per minute, flood | 67.00 | 60.95 | 59.00 to 65.00 | -9% |
| gave up at own timeout | 0 | 0 | 0 to 0 | +0% |
| refused (503) | 382 | 397 | 388 to 401 | +4% |
| requests that retried | 0 | 0 | 0 to 0 | +0% |
| last answer at | 200.29 | 201.77 | 201.20 to 202.55 | +1% |
| longest gateway queue | 8 | 8 | 8 to 8 | +0% |

### sat-direct-patient (calibration)

No gateway, patient batch callers (10 min).

| metric | measured | predicted | range over 20 seeds | error |
|---|---|---|---|---|
| interactive answered, flood | 1% | 1% | 1% to 1% | -0 pt |
| interactive TTFT p50, flood | 12.41 | 12.02 | 11.24 to 12.79 | -3% |
| interactive TTFT p95, flood | 12.56 | 12.49 | 11.98 to 13.29 | -1% |
| interactive answered, whole run | 12% | 12% | 12% to 12% | -0 pt |
| interactive TTFT p50, whole run | 0.95 | 1.02 | 0.91 to 1.17 | +7% |
| interactive total p50, whole run | 1.74 | 1.83 | 1.62 to 2.09 | +5% |
| batch answered | 100% | 100% | 100% to 100% | +0 pt |
| batch TTFT p50 | 133.47 | 135.44 | 132.91 to 137.26 | +1% |
| batch TTFT p95 | 338.65 | 343.77 | 339.58 to 349.43 | +2% |
| completed per minute, flood | 154.50 | 154.50 | 154.50 to 154.50 | +0% |
| gave up at own timeout | 283 | 283 | 283 to 283 | +0% |
| refused (503) | 0 | 0 | 0 to 0 | +0% |
| requests that retried | 0 | 0 | 0 to 0 | +0% |
| last answer at | 510.17 | 518.10 | 513.98 to 522.64 | +2% |

### sat-bounded-patient (held-out)

Queue 30 / 12 s with weights; callers retry on 503 and batch is patient.

| metric | measured | predicted | range over 20 seeds | error |
|---|---|---|---|---|
| interactive answered, flood | 49% | 50% | 44% to 53% | +1 pt |
| interactive TTFT p50, flood | 9.71 | 9.66 | 8.86 to 10.16 | -1% |
| interactive TTFT p95, flood | 13.68 | 13.46 | 13.13 to 13.94 | -2% |
| interactive answered, whole run | 47% | 50% | 48% to 53% | +3 pt |
| interactive TTFT p50, whole run | 8.25 | 8.06 | 7.39 to 8.73 | -2% |
| interactive total p50, whole run | 9.12 | 8.89 | 8.30 to 9.67 | -3% |
| batch answered | 100% | 100% | 100% to 100% | +0 pt |
| batch TTFT p50 | 181.79 | 168.64 | 158.82 to 180.62 | -7% |
| batch TTFT p95 | 353.49 | 337.42 | 321.98 to 366.23 | -5% |
| completed per minute, flood | 182.00 | 183.50 | 177.50 to 187.00 | +1% |
| gave up at own timeout | 178 | 168 | 160 to 177 | -6% |
| refused (503) | 0 | 0 | 0 to 0 | +0% |
| requests that retried | 498 | 494 | 487 to 501 | -1% |
| last answer at | 472.56 | 461.77 | 448.99 to 474.30 | -2% |
| longest gateway queue | 60 | 60 | 60 to 60 | +0% |

### sat-smart (held-out)

Everything is admitted and held: strict priority, eviction of batch when full, refusal by expected wait.

| metric | measured | predicted | range over 20 seeds | error |
|---|---|---|---|---|
| interactive answered, flood | 96% | 97% | 96% to 98% | +1 pt |
| interactive TTFT p50, flood | 5.17 | 4.15 | 3.53 to 5.28 | -20% |
| interactive TTFT p95, flood | 10.90 | 9.62 | 8.94 to 10.70 | -12% |
| interactive answered, whole run | 98% | 98% | 98% to 99% | +1 pt |
| interactive TTFT p50, whole run | 4.46 | 3.80 | 3.44 to 4.20 | -15% |
| interactive total p50, whole run | 5.26 | 4.59 | 4.22 to 5.02 | -13% |
| batch answered | 100% | 100% | 100% to 100% | +0 pt |
| batch TTFT p50 | 261.73 | 255.40 | 252.95 to 258.03 | -2% |
| batch TTFT p95 | 454.75 | 457.59 | 452.63 to 461.95 | +1% |
| completed per minute, flood | 243.00 | 244.10 | 243.00 to 244.50 | +0% |
| gave up at own timeout | 7 | 5 | 4 to 7 | -31% |
| refused (503) | 0 | 0 | 0 to 0 | +0% |
| requests that retried | 11 | 9 | 6 to 13 | -20% |
| last answer at | 628.30 | 634.48 | 629.72 to 638.09 | +1% |
| longest gateway queue | 294 | 288 | 285 to 290 | -2% |


## Without the measured arrivals

The same scenarios with their own Poisson arrivals at the nominal rates, i.e.
what one would predict before running anything: held-out experiments: 5. Success shares (13 numbers): median error 1.4 points, 90th percentile 4.3, worst 5.4. Latency, throughput and counts (49 numbers): median error 4.1%, 90th percentile 10.0%, worst 12.8%.

The range over seeds is wide here, and that is information: with interactive
traffic this close to capacity, two real runs of the same experiment would
differ about as much. In `sat-smart` the interactive TTFT p50 during the flood
ranges from 3.4 s to 8.1 s over 20 seeds (measured once: 5.2 s).
