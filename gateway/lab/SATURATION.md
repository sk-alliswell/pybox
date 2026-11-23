# Saturation: four times more than the models can serve

`saturate.py` sends open-loop traffic at the two local models (no hosted API is called):
20 s of interactive requests only, 120 s with a batch flood on top (about 4x capacity in
total), then 60 s of interactive only. Interactive callers give up after 15 s, batch
callers after 90 s. `analyze_sat.py` compares runs. Numbers below are from one 4 vCPU machine.

| during the flood | direct to LiteLLM | gateway, unbounded queue | queue 30 / 12 s, weights | same + fair policy | queue 4 / 4 s, weights | same + fair policy |
|---|---|---|---|---|---|---|
| interactive answered | 4% | 10% | 62% | 72% | 38% | 18% |
| interactive TTFT p50 (answered) | 13.5 s | 13.0 s | 12.0 s | 10.8 s | 4.7 s | 4.7 s |
| completed per minute, all classes | 41.5 | 45.5 | 84.5 | 89.5 | 67.0 | 57.0 |
| callers that hung until their own timeout | 439 | 419 | 8 | 8 | 0 | 0 |
| refused with 503 | 0 | 0 | 333 | 323 | 367 | 396 |
| median wait before a refusal | 15 s / 90 s | 15 s / 90 s | 12 s | 12 s | 0 s | 0 s |
| longest gateway queue | - | 248 | 56 | 58 | 8 | 8 |
| gateway memory (start to peak) | - | 13 to 54 MB | 10 to 45 MB | 10 to 45 MB | 10 to 40 MB | 9 to 38 MB |
| interactive back to normal after the flood | not within 60 s | not within 60 s | about 40 s | over 60 s | about 20 s | about 40 s |

The fair-policy columns were run on a local merge with the `gateway-lead` branch (PR #5).

What it shows:

- **Direct, the servers collapse.** Every request goes straight into the model server's own
  queue. Callers give up, but the server still generates their answers, so after the first
  40 s almost nothing useful came out, and it took four more minutes after the load stopped
  for the servers to work through requests nobody was waiting for.
- **Holding the queue in the gateway is cheap.** 248 waiting requests cost about 40 MB, and
  a request whose caller hangs up leaves the queue without ever reaching the model.
- **An unbounded queue alone does not help.** The queue is first in, first out, so under
  overload the request being served is always the oldest one, about to time out.
- **Bounding the queue doubles useful output.** With a cap and a wait limit the model only
  works on requests somebody still wants: 85 to 90 completions a minute instead of 42.
- **The cap trades waiting for refusals.** Queue 30 / 12 s answers more but everything that
  is answered waited about 11 s. Queue 4 / 4 s answers in under 5 s and recovers in 20 s,
  but refuses more.

Gaps this exposed:

1. The queue cap is blind to class. When the queue is full of batch work an interactive
   request is refused at the door, which is why the short queue with the fair policy answered
   only 18% of interactive requests. Overflow should evict or refuse batch first.
2. First in, first out is the wrong order under overload. Serving the newest request of a
   class first, or refusing on arrival when the expected wait exceeds the caller's deadline,
   would keep answered requests fast.
3. A refusal after `queue_timeout_secs` costs the caller the whole wait. Admission by
   estimated wait would say no at once.
4. There is no cap in bytes; only a count. Large prompts would need one.
5. Limits are per upstream. Here interactive traffic alone was already near capacity, and no
   queue order fixes that; it needs more capacity or a smaller model for that class.

## Second round: keep the work instead of refusing it

The gaps above were closed in the scheduler (`overflow = "evict"`, `strict_priority`,
`admit_by_wait`, the caller's own `x-gateway-max-wait-ms`), and every caller is now told how
many requests are ahead of it and how long that should take. `gateway.sat-smart.toml` turns
all of it on. The traffic is the same, with two changes on the caller side that the goal
"everything succeeds" needs: batch callers are patient (`--patient`, up to 10 minutes; a
job that only has to finish), and a caller that gets 503 comes back after `retry-after`
(`--retry`), as SDKs do. Interactive callers still give up after 15 s.

| whole run | direct to LiteLLM | queue 30 / 12 s, weights (before) | smart queue |
|---|---|---|---|
| interactive answered | 38 of 321 (12%) | 161 of 339 (47%) | 302 of 309 (98%) |
| interactive TTFT p50 during the flood | 12.4 s | 9.7 s | 5.2 s |
| batch answered | 306 of 306 | 261 of 261 | 305 of 305 |
| batch wait p50 / p95 | 133 s / 339 s | 182 s / 353 s | 262 s / 455 s |
| requests that had to retry | 0 | 410 | 11 |
| longest gateway queue | - | 60 | 294 |
| gateway memory peak | - | 390 MB | 74 MB |

- 607 of 614 requests succeeded. The 7 that did not were interactive requests in the first
  20 s of the flood, while both slots were still held by batch requests that had already
  started; a running request is never interrupted.
- Nothing was thrown away to get there: batch work was held (294 requests at the peak,
  74 MB) and ran in the slots interactive work left free, so it finished about two minutes
  later than when sent straight to the model servers. That is the price.
- 100% is not reachable at this load without more capacity: interactive traffic alone needs
  almost all of it, so its TTFT is 5 s here against 1 s when idle.
- With a short queue and retries (the middle column) batch work also finishes, but by
  hammering the gateway every second: 410 requests retried, and interactive work lost.

How good was the announced wait? Compared with the wait that followed, for requests that
waited more than half a second: interactive, median error 19% (median wait 3.9 s). Batch,
poor: off by a factor of two (median wait 260 s), because how long deferred work waits
depends on interactive traffic that has not arrived yet. The 2 s notices on a queued stream
are recomputed each time, so they converge as the request moves up.

## Third round: the unified scheduler, and a stand-in model

Five runs on one machine (4 vCPU, both models on CPU), same load as above with
patient batch callers and retries, each kept in its own experiment folder
(`experiment.py`, see README). One run per setting: arrivals differ by a few
percent between runs, so differences under about 10% are noise.

| whole run | previous scheduler, weights | unified, same settings | + min_share 0.15, reserve 1 | one upstream, stand-in model for batch |
|---|---|---|---|---|
| interactive answered | 329 of 329 | 331 of 334 | 316 of 316 | 322 of 328 |
| interactive TTFT p50 / p95 during the flood | 4.8 / 11.1 s | 4.0 / 10.7 s | 5.3 / 10.3 s | 3.9 / 11.2 s |
| batch answered | 298 of 298 | 291 of 291 | 311 of 311 | 301 of 301 |
| batch wait in queue p50 / p95 | 244 / 441 s | 240 / 387 s | 242 / 453 s | 239 / 327 s |
| everything done after | 613 s | 558 s | 629 s | 485 s |
| one server idle while the other still had a queue | 203 s | 125 s | 219 s | 2 s |
| batch requests answered by the other model | 0 | 0 | 0 | 136 |
| gateway memory, peak | 73 MB | 74 MB | 76 MB | 68 MB |

- The unified scheduler does what the previous one did under the same settings: no
  regression, and overload handling now also works with the fair policy and with
  several servers behind one upstream, which it could not before.
- `min_share` and `reserve_slots` made no measurable difference here. Interactive
  work was already answered and batch already finished; they are guards for loads
  where the top class alone exceeds capacity, which this test does not produce.
- Letting batch be answered by either model is what moved the numbers: both servers
  stayed busy to the end, the last request was done 21% sooner than with the
  previous scheduler (13% sooner than unified without it) and the slowest batch waits
  shrank by a quarter. Interactive requests are never moved.
- Not solved: the wait announced to batch work. It assumes interactive traffic keeps
  arriving at the current pace and came out two to four times too long (median error
  1.4 to 2.7 times the real wait; the previous estimate's was 1.7). For interactive
  work the median error was 12% to 21% (previous 15%), and 55% on the shared
  upstream, where the estimate does not yet split the queue between the two servers.
- An earlier run of the previous build without weights answered only 41 of 328
  interactive requests: that build knows classes only by weight. It is kept in the
  index, marked invalid.

## A day on modelled hosted providers

`day-base` and `day-v4` (realistic.py, 8 minutes each) with the hosted models
answered by `mock_cloud.py`, so nothing was billed and the token and latency
numbers describe the gateway against a model of the providers, not a bill. Both
days are valid: no errors, every persona answered.

| | out of the box | v4 settings |
|---|---|---|
| fresh tokens for the baseline's amount of work | 3.69 M | 3.07 M (-17%) |
| interactive TTFT p50 / p95 | 0.41 / 1.04 s | 0.34 / 0.77 s (-18% / -25%) |
| interactive TTFT p95 while the batch runs | 1.21 s | 0.81 s |
| support, fresh tokens per answer | 296 | 64 |
| runaway agent, fresh tokens | 512 k | 3 k |
| IDE, fresh tokens per turn | 4,334 | 4,404 |

The saving comes from the response cache (repeated support questions, the
runaway loop) and the latency from the batch limit and fair scheduling. IDE
turns are now two thirds of what is left and did not move: the two variants
that defeat the provider cache (a timestamp at the top of the system prompt, a
Claude prompt without a cache marker sent through LiteLLM) are still uncached.
That is where the rest of the way to -50% has to come from.
