# Lead areas: criteria and measurements

Three areas where the gateway aims to be ahead of the products it was
compared with: deliverable-based ROI, fair scheduling per agent run, and
token savings that are measured rather than assumed. Every claim is a
criterion with a target, a measured value and a baseline, checked by
`cargo run --release --bin gateway-eval` and by `tests/lead_eval.rs`.

## How to read the numbers

- Scheduling criteria (B*) run on a discrete-event simulator. It drives the
  same queue (`lead/fair.rs`), routing and concurrency code
  (`lead/adaptive.rs`) as the live scheduler; the model servers are
  simulated. The baseline is the gateway's own round-robin scheduler.
- Cache criteria (C*) send generated Anthropic-format request bodies
  through the real analysis and rewrite code. The provider's cache is an
  independent model that sees only the bytes sent and applies the published
  rules (5 min / 1 h lifetime refreshed on read, 1,024-token minimum,
  20-block lookback, writes 1.25x / 2x, reads 0.1x). No real provider was
  called.
- The day of traffic in C3 is constructed. How much cache points save
  depends on how many clients already cache; on a day where they all do,
  the saving is zero.
- Timing criteria are release-build wall-clock on the development machine.

## Scorecard

| id | area | criterion | target | measured | baseline | result |
|---|---|---|---|---|---|---|
| F1 | common | Requests grouped into the right run with no header | >= 99% | 100.0% of 1484 requests, 200 interleaved runs | today: no run id unless the client sends one | pass |
| A1 | roi | Four cost classes add up to total spend, overall and per project | difference <= 0.001% | 0.00000% overall, 0.00000% per project (20,000 requests, $502.56) | today: one "unattributed" bucket, failed and rejected work counted as normal spend | pass |
| A2 | roi | A report as of a past moment is unchanged after 500 late events | identical | identical; the current report did change | today: reports are recomputed from mutable records, so old numbers drift | pass |
| A3 | roi | ROI is a low-high range, shown only for accepted deliverables with a value | 0 violations | 0 violations in 50 deliverables (22 with a value) | today: a single ROI number, zero value when none is stated | pass |
| A4 | roi | 90-day report over the request history | <= 300 ms for 1000000 requests | 109 ms (149801 rollup cells) | today: every report rescans the request rows | pass |
| B1 | scheduling | Service difference between teams with work waiting stays bounded | <= 2 x (largest request + slack), and below round robin | 226k weighted tokens (bound 623k) | 2320k with round robin by request count (current scheduler) | pass |
| B2 | scheduling | Short runs finish sooner and no run pays for it on average | short-run median <= 70% of round robin; mean run time not worse | short-run median 56 s, mean run 605 s | 133 s and 655 s with round robin by request count (current scheduler) | pass |
| B3 | scheduling | Tokens re-sent uncached because the cache expired in the queue | <= 50% of round robin | 8.0k tokens per run of the workload | 32.3k with round robin by request count (current scheduler) | pass |
| B4 | scheduling | No throughput is given up, and nothing starves | time to finish all work <= 101% of round robin; longest wait <= 150% | 2135 s to finish, longest wait 92 s | 2222 s and 65 s with round robin by request count (current scheduler) | pass |
| B5 | scheduling | Interactive runs while a 300-request batch is running (headline: latency -50%) | interactive run time p99 and mean <= 50% of round robin; batch finishes within 105% | mean 37.1 s, p99 58.0 s; batch done in 666 s | mean 356.6 s, p99 562.5 s; batch done in 661 s with round robin by request count (current scheduler) | pass |
| B6 | scheduling | Cost of one scheduling decision with 1,000 runs waiting | p99 <= 50 microseconds | p99 10.3 us, median 4.5 us | a model call takes hundreds of milliseconds | pass |
| B7 | scheduling | Adaptive concurrency finds each server's capacity from throughput alone | settles within 1 of the best limit, >= 95% of the best fixed limit's throughput | capacity 2: settles at 2, 96.4% of best; capacity 4: settles at 4, 97.9% of best; capacity 8: settles at 8, 95.1% of best | fixed limit 1 / 16, share of best: capacity 2: 50.0% / 58.2%; capacity 4: 25.0% / 64.2%; capacity 8: 12.6% / 76.3% | pass |
| B8 | scheduling | No server sits idle while requests it could serve are waiting | <= 1% of the time; mean run time not worse than blind rotation | 0.0% of the time, mean run 43.7 s | 3.9% and 46.0 s when servers are taken in turn without looking at load | pass |
| B9 | scheduling | A run stays on the server that holds its cache unless that server is overloaded | >= 90% of follow-up turns; cache tokens lost to moving <= 25% of blind rotation | 100.0% of turns, 0k tokens re-sent | 376k tokens re-sent with blind rotation | pass |
| B10 | scheduling | A small and a large server together, capacities not configured | >= 93% of the throughput with hand-tuned limits | 95.9% (limits settled at [1, 8]) | 61.3% with the same fixed limit of 5 on both | pass |
| C1 | tokens | The cause of a cache break is named correctly (11 kinds of labelled cases) | >= 95% | 100.0% of 275 cases | today: only the cache-read token count is visible, not why it dropped | pass |
| C2 | tokens | Adding cache markers leaves every other byte of the request as the client sent it | 100% of fuzzed bodies | 3000 of 3000 (2586 had markers added) | LiteLLM re-serializes the request when it injects cache markers | pass |
| C3 | tokens | Billed input cost of a day of mixed agent traffic (headline: token spend <= 50%) | <= 50% of the cost as sent; no kind of traffic costs more; not above LiteLLM-style injection | 25.0% of as-sent (support bot 13.9%, client-managed cache 100.0%, small one-off 100.0%, long tool waits 25.5%, timestamp in system 74.1%, coding agent 16.9%) | LiteLLM-style injection (system + last message, 5 min): 31.4% | pass |
| C4 | tokens | The saving the gateway reports equals the saving an independent provider model sees | within 1%; predicted cache reads within 1% of tokens | reported $12.6156, independent $12.6156 (0.0% apart); worst prediction error 0.3% | today: cache savings are reported, but not what the gateway itself changed | pass |
| C5 | tokens | Experiment verdicts at 200 runs per arm | false alarm <= 8% with no real difference; a 20% saving found >= 80%; a cheaper arm that fails more is stopped >= 90% | false alarm 6.7%, detection 89.6%, stopped 97.0% | today: changes are adopted on before/after averages | pass |
| C6 | tokens | Time the analysis and rewrite add to a 200 KB request | median <= 1 ms | median 0.89 ms, p99 1.12 ms (201 KB, 106 blocks) | LiteLLM's own overhead per request is 20 ms idle and up to 285 ms under load (measured in the console lab) | pass |
| D1 | tokens | Input tokens sent on long tool-heavy runs with old tool results cleared (headline: token use <= 50%) | <= 50% of what the clients sent | 36.9% (5.41M of 14.65M tokens, 10 runs x 40 turns) | today: every old tool result is re-sent on every turn | pass |
| D2 | tokens | Clearing never costs more than it saves when a prompt cache is in use | billed input with clearing <= without, for gateway-placed and client-placed cache markers | 77.6% of cache-only cost with gateway markers, 77.6% with client markers; boundary moved on 11.2% of turns | clearing on every turn would rewrite the cached prompt each time; cache alone is already 14.7% of uncached cost | pass |
| D3 | tokens | Cleared requests stay well-formed: tool call ids, the last three results, tools and system untouched | 0 violations | 0 in 1200 requests | not applicable | pass |

```
Day replay, billed input in base-token units (as sent / LiteLLM-style / gateway):
  support bot                  551113      81840      76827
  client-managed cache         157754     157754     157754
  small one-off                 35271      35271      35271
  long tool waits              788304     336981     201398
  timestamp in system          427202     534002     316359
  coding agent                3650617     616751     617439
  total                       5610261    1762599    1405048
  raw input tokens 6265895, output tokens 282600
  cache state per request: [("first", 345), ("ok", 373), ("system_volatile", 42), ("ttl_expired", 26)]
```

## Not done yet

- The cache criteria have not been run against a real provider (hosted
  calls are not being spent on). Tool-result clearing has been run on a
  local model, see below.
  The first live check should be `cache_points = "experiment"` on real
  traffic, read once at the planned sample size.
- Output tokens are charged to a run when the request is admitted and
  corrected when it ends, not while it streams.
- Clearing old tool results (`tool_results = "on"` or `"experiment"`) changes
  what the model sees. Whether answers stay as good has not been measured;
  the default is `"shadow"`, which only estimates. Summarising (compaction)
  is not done.
- Deliverable events are kept in the store's metadata, not in a TDB table.
- Run inference falls back to the first message when the system prompt
  changes every request; two runs of one agent that open with the identical
  message and overlap in time can be merged.
- The gateway itself is one process; only the servers behind it are spread.

## Found on the first real run

With `cache_points = "on"` behind LiteLLM, the gateway added
`prompt_cache_key` to every OpenAI-format request. Gemini rejects unknown
parameters, so all of those requests failed with 400, and Claude models
reached through the OpenAI-format route failed too. Cache parameters are now
added only for models that take them: `prompt_cache_key` for OpenAI's own
models, `cache_control` for Claude models on the Messages API. Other
requests are forwarded untouched and their rows say
`lead_plan = "unsupported_model"`.

## Tool-result clearing on a local model

`demo/long_runs.py` drives tool-heavy runs (3 runs x 14 turns, about 260
tokens per tool result) through the gateway to a single-slot llama.cpp
server (Qwen2.5-0.5B, 4 vCPU), with `tool_results` off and on
(`keep_tool_results = 3`). Prompt tokens are what the server counted.

| pattern | clearing | prompt tokens | mean turn | p95 turn | wall |
|---|---|---|---|---|---|
| one run after another | off | 79,650 | 1.89 s | 2.31 s | 79 s |
| one run after another | on | 52,119 (-35%) | 1.96 s | 3.91 s | 82 s |
| runs taking turns | off | 79,650 | 7.74 s | 14.67 s | 325 s |
| runs taking turns | on | 51,705 (-35%) | 4.13 s | 5.97 s | 173 s |

When runs take turns on one slot, the server's prefix cache is lost on every
request and the whole prompt is processed again, so a shorter prompt is
directly less work: turns take 47% less time. When one run has the slot to
itself the server keeps its prefix cache, so the tokens saved were nearly
free already and each step of clearing costs one re-read (the p95).

The first attempt cleared nothing. The cost rule assumed an OpenAI-format
upstream caches prefixes the way OpenAI does and waited for the rewrite to
pay off; a local server reports no cached tokens. The rule now weighs the
predicted cache by what the upstream has actually reported
(`cached_tokens` seen against predicted, per upstream), and where none
shows it moves the boundary in steps of `keep_tool_results` so a server
with an unreported prefix cache is not made to re-read on every turn. The
first run through a fresh gateway is therefore not cleared until about
4,000 predicted cached tokens have gone unreported; longer runs than these
save more (D1).
