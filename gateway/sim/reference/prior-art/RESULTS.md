# Policies from the prior-art survey, tried in the simulator

Nothing here was run on real servers. The numbers say which way a policy
pushes and roughly how hard, under the assumptions listed at the end.
`experiments.py` reruns everything (3 seeds each, 10 simulated minutes);
`report.py` prints the tables below from `results.json`.

## What repeats in real prompts

`trace_repeat.py` over the Qwen-Bailian traces (two hours each, production,
anonymised; prompts are given as hashes of 16-token blocks, so the common
prefix of two prompts can be counted exactly). Result in
`qwen-bailian-repeat.json`.

| trace | used for | requests | input tokens p50 / mean | output tokens p50 / mean | has a previous turn in the trace | prefix shared with that turn, when there is one | prefix shared with any earlier request |
|---|---|---|---|---|---|---|---|
| coder | ide | 43,011 | 4,540 / 5,748 | 469 / 808 | 39% | 69% | 52% (first turns: 42%) |
| to-C chat | chat | 43,058 | 1,046 / 2,331 | 376 / 430 | 46% | 67% | 59% (first turns: 52%) |
| to-B API | batch | 172,800 | 574 / 915 | 39 / 92 | 0% | – | 57% |

Two things differ from what the setup page assumed (ide 90%, chat 70% of the
caller's own context; batch 40% of one template):

- Most of what repeats is shared between callers (system prompts, tool
  definitions, templates), not private to a conversation. A first turn
  already finds 42 to 52% of its prompt in what others sent.
- Fewer than half of the requests continue a conversation seen in the trace.
  The trace is a sample, so this is a lower bound.

The scenario format now has both layers (`prefix.share` with `follow_up`
for the caller's context, `prefix.group_share` for the shared part), and the
setup page starts from these numbers.

## Findings

Revised 2026-11-14. Findings 3 and 4 and all tables were first written with
a queue that took turns request by request (the gateway before its
scheduler was unified). The gateway takes turns by tokens per unit weight,
and the simulator now does the same (`reference/overload` compares it with
measurements). Under that order the baseline is already kinder to light and
important callers, so both policies gain less than first reported: the
earlier text had ide TTFT p95 falling from 13.2 s to 5 s under least
attained service, and ide TTFT p50 from 7.0 s to 6.2 s under shedding.

1. **One cost function for cache and load: port it, counted in time.** With
   the llama.cpp slot cache, `cost` left 21% of prompt tokens unread at
   normal load against 10% for least-loaded and 14% for prefix affinity, with
   the same or shorter waits; with the load weight at 0.25, 31% and an ide
   TTFT p50 of 2.5 s against 3.6 s. Counting plain tokens (a prompt token
   weighs as much as an output token) reuses most (37%) but balances load
   worse: at 1.5x load it answered 93% where least-loaded and time-based
   cost answered 97%. Prompt tokens are read about 30 times faster than
   output tokens are written, so they should not weigh the same.
2. **Hot-prefix replication threshold: not worth a separate mechanism.**
   `replicate_at` moved reuse by one or two points and nothing else, also
   when one template carried most of the batch traffic. The cost function
   already sends a popular prefix to every server that holds it.
3. **Gradual shedding with per-priority ceilings: worth porting, as an
   alternative to eviction.** At 1.5x load, shedding from a quarter of the
   queue kept ide and chat at 100% answered (96 to 97% with
   refuse-when-full); batch fell from 96% to 83% answered, and the batch
   requests that were answered waited half as long (TTFT p50 4.9 s to
   2.5 s). It did not make ide or chat faster (TTFT p50 4.1 s and 1.8 s
   either way). Evicting when full protects the top weight equally but
   lets the queue fill first (ide TTFT p95 20 s against 15 s). At 2x load
   every variant starves batch (10 to 26% answered): the thresholds need a
   floor for the lowest weight. `from = 0.25, low_ceiling = 0.75` was the
   best trade in these runs.
4. **Least attained service with decay: a smaller gain than first
   reported.** With three callers sending ten times the others' rate at
   1.5x load, everyone else's ide TTFT p95 fell from 10.0 s to 5.1 s
   (half-life 60 s or 600 s; 6.8 s with 10 s); their median did not move
   (4.1 s) and chat barely (p95 2.7 s to 2.3 s). The heavy callers are held
   back either way (about half their requests time out), because taking
   turns by tokens already charges them for their volume. Throughput did
   not change. At normal load it changes little.
5. **Refusing what cannot meet its deadline: not yet.** It refused about 10
   of some 2,400 requests that went on to time out. Two reasons: most
   misses come from the answer's own length, which is not known at the door,
   and the wait estimate for low weights is too low. Gradual shedding did the same job better (batch requests that
   waited out their timeout for nothing at 2x load: 1,852 down to 385). Fix the estimate first.
6. **The server cache model matters more than the routing.** A server that
   keeps 16 prefixes for all its slots left 42 to 45% unread whatever the
   routing; llama.cpp's one-prompt-per-slot left 10 to 21%. On llama.cpp the
   lever is more slots per server or fewer distinct prefixes per server
   (session admission), not a better hash.

## Tables

### routing load=1.0

| setting | prompt tokens not read % | tokens/s | batch answered % | batch TTFT p50 / p95 s | chat answered % | chat TTFT p50 / p95 s | ide answered % | ide TTFT p50 / p95 s |
|---|---|---|---|---|---|---|---|---|
| random | 8.7 | 787 | 98.7 | 3.60 / 24.14 | 98.5 | 1.81 / 13.64 | 98.4 | 4.16 / 18.69 |
| least_loaded | 10.4 | 808 | 100.0 | 0.67 / 0.83 | 100.0 | 1.57 / 1.87 | 99.9 | 3.57 / 4.42 |
| affinity | 14.1 | 808 | 100.0 | 0.66 / 0.83 | 100.0 | 1.55 / 1.90 | 99.9 | 3.49 / 4.54 |
| cost | 21.1 | 808 | 100.0 | 0.50 / 0.80 | 100.0 | 1.47 / 1.83 | 99.9 | 3.32 / 4.34 |
| affinity replicate_at=0.25 | 15.8 | 808 | 100.0 | 0.65 / 0.83 | 100.0 | 1.53 / 1.90 | 99.9 | 3.53 / 4.48 |
| affinity replicate_at=0.5 | 16.0 | 808 | 100.0 | 0.66 / 0.84 | 100.0 | 1.52 / 1.90 | 99.9 | 3.40 / 4.48 |
| affinity replicate_at=1.0 | 14.0 | 808 | 100.0 | 0.66 / 0.83 | 100.0 | 1.53 / 1.93 | 99.9 | 3.49 / 4.47 |
| cost load_weight=0.25 | 30.6 | 808 | 100.0 | 0.43 / 0.90 | 100.0 | 1.11 / 1.80 | 99.9 | 2.53 / 4.26 |
| cost load_weight=4.0 | 18.3 | 810 | 100.0 | 0.59 / 0.80 | 100.0 | 1.48 / 1.88 | 100.0 | 3.39 / 4.36 |
| cost counted in tokens | 36.6 | 810 | 100.0 | 0.45 / 0.85 | 100.0 | 0.99 / 3.73 | 100.0 | 2.52 / 5.64 |

### routing load=1.0 hot-template

| setting | prompt tokens not read % | tokens/s | batch answered % | batch TTFT p50 / p95 s | chat answered % | chat TTFT p50 / p95 s | ide answered % | ide TTFT p50 / p95 s |
|---|---|---|---|---|---|---|---|---|
| random | 21.1 | 848 | 93.6 | 7.46 / 33.79 | 93.2 | 1.91 / 13.46 | 93.5 | 4.33 / 15.60 |
| least_loaded | 23.6 | 898 | 100.0 | 0.44 / 0.80 | 100.0 | 1.58 / 1.92 | 100.0 | 3.62 / 4.52 |
| affinity | 24.9 | 896 | 100.0 | 0.43 / 0.79 | 100.0 | 1.60 / 1.95 | 100.0 | 3.64 / 4.53 |
| cost | 28.5 | 896 | 100.0 | 0.42 / 0.78 | 100.0 | 1.51 / 1.88 | 100.0 | 3.42 / 4.47 |
| affinity replicate_at=0.25 | 24.7 | 896 | 100.0 | 0.43 / 0.79 | 100.0 | 1.62 / 1.96 | 100.0 | 3.68 / 4.55 |
| affinity replicate_at=0.5 | 26.1 | 893 | 100.0 | 0.43 / 0.80 | 100.0 | 1.57 / 1.92 | 99.9 | 3.61 / 4.56 |
| affinity replicate_at=1.0 | 25.5 | 893 | 100.0 | 0.44 / 0.80 | 100.0 | 1.57 / 1.93 | 99.9 | 3.63 / 4.54 |
| cost load_weight=0.25 | 34.3 | 896 | 100.0 | 0.41 / 0.78 | 100.0 | 1.28 / 1.88 | 100.0 | 2.65 / 4.35 |
| cost load_weight=4.0 | 27.3 | 896 | 100.0 | 0.42 / 0.78 | 100.0 | 1.49 / 1.88 | 100.0 | 3.50 / 4.49 |
| cost counted in tokens | 41.1 | 898 | 100.0 | 0.42 / 0.86 | 100.0 | 1.03 / 3.79 | 100.0 | 2.52 / 5.28 |

### routing load=1.5

| setting | prompt tokens not read % | tokens/s | batch answered % | batch TTFT p50 / p95 s | chat answered % | chat TTFT p50 / p95 s | ide answered % | ide TTFT p50 / p95 s |
|---|---|---|---|---|---|---|---|---|
| random | 8.4 | 877 | 69.0 | 19.37 / 64.96 | 68.0 | 2.20 / 19.47 | 68.1 | 5.00 / 25.91 |
| least_loaded | 6.6 | 1223 | 96.6 | 4.84 / 29.90 | 96.4 | 1.82 / 3.10 | 96.2 | 4.14 / 13.04 |
| affinity | 6.2 | 1226 | 96.5 | 4.69 / 29.47 | 96.7 | 1.83 / 2.78 | 96.3 | 4.15 / 13.07 |
| cost | 11.7 | 1221 | 95.9 | 4.94 / 31.41 | 96.9 | 1.78 / 3.07 | 96.2 | 4.12 / 13.42 |
| affinity replicate_at=0.25 | 6.8 | 1221 | 96.6 | 4.42 / 29.71 | 96.9 | 1.80 / 2.86 | 96.4 | 4.16 / 12.70 |
| affinity replicate_at=0.5 | 6.8 | 1225 | 96.4 | 4.54 / 29.03 | 96.6 | 1.82 / 2.95 | 96.9 | 4.12 / 12.63 |
| affinity replicate_at=1.0 | 6.6 | 1222 | 96.3 | 4.55 / 29.52 | 97.3 | 1.83 / 2.77 | 96.3 | 4.13 / 12.71 |
| cost load_weight=0.25 | 20.5 | 1210 | 95.7 | 6.36 / 32.50 | 95.6 | 1.74 / 4.96 | 95.3 | 3.98 / 13.63 |
| cost load_weight=4.0 | 7.4 | 1226 | 96.4 | 4.66 / 30.49 | 95.9 | 1.80 / 2.86 | 96.8 | 4.13 / 13.27 |
| cost counted in tokens | 22.2 | 1163 | 91.4 | 9.29 / 42.45 | 90.7 | 2.22 / 6.57 | 91.3 | 4.37 / 14.96 |

### routing load=1.5 hot-template

| setting | prompt tokens not read % | tokens/s | batch answered % | batch TTFT p50 / p95 s | chat answered % | chat TTFT p50 / p95 s | ide answered % | ide TTFT p50 / p95 s |
|---|---|---|---|---|---|---|---|---|
| random | 20.8 | 889 | 63.1 | 11.40 / 47.93 | 62.0 | 1.90 / 10.72 | 62.1 | 4.42 / 16.08 |
| least_loaded | 17.2 | 1242 | 87.7 | 5.58 / 26.93 | 86.9 | 1.79 / 2.23 | 87.8 | 4.11 / 7.36 |
| affinity | 17.0 | 1241 | 86.5 | 5.90 / 26.68 | 88.2 | 1.80 / 2.26 | 87.1 | 4.12 / 7.90 |
| cost | 19.6 | 1245 | 87.2 | 5.94 / 25.62 | 87.8 | 1.79 / 2.25 | 87.0 | 4.08 / 7.81 |
| affinity replicate_at=0.25 | 17.4 | 1244 | 87.7 | 5.60 / 26.45 | 87.8 | 1.78 / 2.24 | 87.5 | 4.11 / 7.86 |
| affinity replicate_at=0.5 | 17.4 | 1245 | 87.6 | 5.72 / 26.67 | 88.0 | 1.78 / 2.25 | 87.0 | 4.13 / 7.68 |
| affinity replicate_at=1.0 | 17.1 | 1248 | 87.1 | 5.56 / 27.11 | 87.6 | 1.78 / 2.24 | 86.9 | 4.12 / 7.41 |
| cost load_weight=0.25 | 28.3 | 1232 | 85.5 | 6.23 / 27.43 | 86.9 | 1.77 / 4.56 | 86.9 | 3.90 / 8.48 |
| cost load_weight=4.0 | 17.3 | 1242 | 87.3 | 5.73 / 26.60 | 87.6 | 1.79 / 2.25 | 87.8 | 4.11 / 7.52 |
| cost counted in tokens | 34.5 | 1192 | 83.7 | 7.17 / 30.58 | 83.5 | 2.06 / 5.01 | 83.3 | 3.73 / 9.49 |

### routing load=1.0 shared-cache

| setting | prompt tokens not read % | tokens/s | batch answered % | batch TTFT p50 / p95 s | chat answered % | chat TTFT p50 / p95 s | ide answered % | ide TTFT p50 / p95 s |
|---|---|---|---|---|---|---|---|---|
| least_loaded | 41.8 | 813 | 100.0 | 0.40 / 0.71 | 100.0 | 0.90 / 1.51 | 100.0 | 2.33 / 2.87 |
| affinity | 44.3 | 808 | 100.0 | 0.40 / 0.68 | 100.0 | 0.90 / 1.40 | 99.9 | 2.34 / 2.90 |
| cost | 44.7 | 808 | 100.0 | 0.39 / 0.61 | 100.0 | 0.89 / 1.30 | 99.9 | 2.29 / 2.81 |

### assumed vs traced shares

| setting | prompt tokens not read % | tokens/s | batch answered % | batch TTFT p50 / p95 s | chat answered % | chat TTFT p50 / p95 s | ide answered % | ide TTFT p50 / p95 s |
|---|---|---|---|---|---|---|---|---|
| assumed least_loaded | 11.4 | 808 | 100.0 | 0.49 / 0.73 | 100.0 | 1.59 / 1.91 | 99.9 | 3.76 / 4.46 |
| assumed cost | 14.1 | 810 | 100.0 | 0.48 / 0.72 | 100.0 | 1.57 / 1.89 | 100.0 | 3.75 / 4.50 |
| traced least_loaded | 10.4 | 808 | 100.0 | 0.67 / 0.83 | 100.0 | 1.57 / 1.87 | 99.9 | 3.57 / 4.42 |
| traced cost | 21.1 | 808 | 100.0 | 0.50 / 0.80 | 100.0 | 1.47 / 1.83 | 99.9 | 3.32 / 4.34 |

### shedding load=1.5

| setting | prompt tokens not read % | tokens/s | batch answered % | batch TTFT p50 / p95 s | chat answered % | chat TTFT p50 / p95 s | ide answered % | ide TTFT p50 / p95 s |
|---|---|---|---|---|---|---|---|---|
| refuse when full | 11.7 | 1221 | 95.9 | 4.94 / 31.41 | 96.9 | 1.78 / 3.07 | 96.2 | 4.12 / 13.42 |
| evict lower weight when full | 11.5 | 1233 | 85.9 | 8.02 / 33.37 | 100.0 | 1.82 / 4.03 | 99.9 | 4.19 / 20.39 |
| shed from=0.25 ceiling=0.75 | 12.1 | 1225 | 82.7 | 2.53 / 19.07 | 100.0 | 1.80 / 3.48 | 99.9 | 4.09 / 15.09 |
| shed from=0.5 ceiling=0.9 | 11.8 | 1228 | 84.3 | 3.60 / 27.66 | 100.0 | 1.80 / 3.36 | 99.9 | 4.15 / 17.17 |
| shed from=0.5 ceiling=1.0 | 11.8 | 1231 | 84.6 | 3.98 / 28.55 | 100.0 | 1.80 / 3.70 | 99.9 | 4.14 / 17.72 |
| shed from=0.75 ceiling=1.0 | 11.8 | 1229 | 85.7 | 5.14 / 35.46 | 100.0 | 1.81 / 3.84 | 99.8 | 4.17 / 19.24 |
| shed from=0.1 ceiling=0.5 | 12.2 | 1221 | 80.9 | 1.62 / 11.54 | 100.0 | 1.78 / 2.85 | 99.9 | 4.05 / 12.01 |

### shedding load=2.0

| setting | prompt tokens not read % | tokens/s | batch answered % | batch TTFT p50 / p95 s | chat answered % | chat TTFT p50 / p95 s | ide answered % | ide TTFT p50 / p95 s |
|---|---|---|---|---|---|---|---|---|
| refuse when full | 10.6 | 1439 | 73.5 | 11.20 / 45.09 | 74.4 | 1.81 / 3.18 | 73.5 | 4.20 / 14.29 |
| evict lower weight when full | 16.4 | 1538 | 24.6 | 4.38 / 32.22 | 96.5 | 2.21 / 11.87 | 91.2 | 24.17 / 86.46 |
| shed from=0.25 ceiling=0.75 | 20.2 | 1572 | 12.9 | 4.03 / 37.03 | 98.3 | 2.58 / 12.02 | 95.8 | 20.43 / 81.46 |
| shed from=0.5 ceiling=0.9 | 19.0 | 1563 | 16.2 | 4.61 / 48.60 | 97.7 | 2.37 / 11.29 | 94.7 | 21.47 / 83.04 |
| shed from=0.5 ceiling=1.0 | 16.6 | 1555 | 21.3 | 5.39 / 55.13 | 96.1 | 2.21 / 10.95 | 92.6 | 21.35 / 82.60 |
| shed from=0.75 ceiling=1.0 | 15.3 | 1544 | 26.1 | 8.34 / 54.70 | 93.3 | 2.13 / 11.31 | 91.0 | 20.11 / 81.81 |
| shed from=0.1 ceiling=0.5 | 21.3 | 1584 | 9.9 | 2.55 / 24.77 | 99.3 | 2.62 / 11.35 | 96.9 | 18.39 / 77.55 |

### deadline load=1.5

| setting | prompt tokens not read % | tokens/s | batch answered % | batch TTFT p50 / p95 s | chat answered % | chat TTFT p50 / p95 s | ide answered % | ide TTFT p50 / p95 s |
|---|---|---|---|---|---|---|---|---|
| wait in queue | 14.4 | 805 | 100.0 | 0.74 / 4.06 | 99.6 | 1.65 / 2.10 | 74.7 | 3.73 / 4.77 |
| refuse if the wait is longer than the time left | 14.4 | 805 | 100.0 | 0.74 / 4.06 | 99.6 | 1.65 / 2.10 | 74.7 | 3.73 / 4.77 |
| gradual shedding only | 14.4 | 805 | 100.0 | 0.74 / 4.06 | 99.6 | 1.65 / 2.10 | 74.7 | 3.73 / 4.77 |
| both | 14.4 | 805 | 100.0 | 0.74 / 4.06 | 99.6 | 1.65 / 2.10 | 74.7 | 3.73 / 4.77 |

### deadline load=2.0

| setting | prompt tokens not read % | tokens/s | batch answered % | batch TTFT p50 / p95 s | chat answered % | chat TTFT p50 / p95 s | ide answered % | ide TTFT p50 / p95 s |
|---|---|---|---|---|---|---|---|---|
| wait in queue | 11.3 | 989 | 61.1 | 34.39 / 43.73 | 99.3 | 1.87 / 5.41 | 58.8 | 4.25 / 12.03 |
| refuse if the wait is longer than the time left | 11.1 | 991 | 61.1 | 34.34 / 43.67 | 99.3 | 1.87 / 5.68 | 59.0 | 4.23 / 12.08 |
| gradual shedding only | 11.7 | 1011 | 65.6 | 20.36 / 41.61 | 99.4 | 1.85 / 5.46 | 58.1 | 4.13 / 11.81 |
| both | 11.6 | 1016 | 65.4 | 20.79 / 41.63 | 99.5 | 1.85 / 5.83 | 58.5 | 4.17 / 12.07 |

### attained service load=1.0

| setting | prompt tokens not read % | tokens/s | batch answered % | batch TTFT p50 / p95 s | chat answered % | chat TTFT p50 / p95 s | ide answered % | ide TTFT p50 / p95 s | ide-heavy answered % | ide-heavy TTFT p50 / p95 s |
|---|---|---|---|---|---|---|---|---|---|---|
| round robin | 16.3 | 1048 | 100.0 | 0.69 / 2.65 | 100.0 | 1.63 / 2.11 | 99.9 | 3.56 / 4.67 | 99.9 | 3.74 / 6.99 |
| least attained, half-life 10s | 16.4 | 1048 | 100.0 | 0.69 / 1.85 | 100.0 | 1.60 / 2.07 | 99.9 | 3.59 / 4.69 | 99.9 | 3.74 / 8.65 |
| least attained, half-life 60s | 16.2 | 1048 | 100.0 | 0.69 / 2.15 | 100.0 | 1.60 / 2.04 | 99.9 | 3.59 / 4.69 | 99.9 | 3.72 / 9.31 |
| least attained, half-life 600s | 16.8 | 1048 | 100.0 | 0.69 / 2.25 | 100.0 | 1.60 / 2.02 | 99.9 | 3.54 / 4.59 | 99.9 | 3.71 / 9.34 |

### attained service load=1.5

| setting | prompt tokens not read % | tokens/s | batch answered % | batch TTFT p50 / p95 s | chat answered % | chat TTFT p50 / p95 s | ide answered % | ide TTFT p50 / p95 s | ide-heavy answered % | ide-heavy TTFT p50 / p95 s |
|---|---|---|---|---|---|---|---|---|---|---|
| round robin | 12.3 | 1235 | 84.6 | 2.54 / 16.41 | 83.8 | 1.83 / 2.70 | 84.6 | 4.09 / 10.03 | 48.6 | 81.11 / 111.67 |
| least attained, half-life 10s | 12.5 | 1237 | 86.3 | 1.34 / 17.79 | 84.8 | 1.80 / 2.43 | 84.1 | 4.03 / 6.77 | 49.3 | 87.60 / 111.50 |
| least attained, half-life 60s | 12.7 | 1245 | 86.3 | 1.26 / 17.47 | 86.0 | 1.80 / 2.29 | 85.1 | 3.97 / 5.12 | 45.7 | 84.35 / 110.32 |
| least attained, half-life 600s | 12.8 | 1242 | 86.5 | 1.20 / 16.50 | 85.8 | 1.79 / 2.23 | 85.4 | 3.96 / 5.01 | 45.5 | 83.58 / 112.76 |

## Assumptions

- Workload: 60 ide, 40 chat and 40 batch callers (8 templates) at 1.2, 0.8
  and 6 requests a minute each; lengths and repeat shares from the traces
  above, every request of a type the same prompt length. The number of
  people and their rates are made up. A cloud API's users are not an
  in-house team's agents.
- Servers: 12 with 4 slots, 60 tokens/s per stream, n^0.85 scaling, prompts
  read at 1,800 tokens/s, a cached prefix saves 93% of its reading time
  (measured on a small CPU model, `reference/kv-cache`). Not GPU numbers.
- The gateway sends straight to the servers with as many requests in flight
  as there are slots.
- `cost` knows what each server has cached, its speeds and the length of the
  answer. A real gateway has to estimate all three.
- Session-level admission and prefix-grouped batch dispatch (both in the
  gateway work) are not in the simulator.
