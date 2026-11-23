# pybox-gateway

Host-side LLM API gateway for running many local agents (10-20+ pybox
sandboxes) against Anthropic/OpenAI-compatible APIs. Standalone crate: it has
its own `[workspace]` and `Cargo.lock`, so the pybox controller build is
unaffected. Design rationale (Korean): [DESIGN.md](DESIGN.md).

## Run

```sh
cargo build --release --locked
cp gateway.example.toml gateway.toml   # edit limits, admin_token, agents
ANTHROPIC_API_KEY=... ./target/release/pybox-gateway --config gateway.toml
```

Point an agent's SDK at the gateway and give it a gateway token, not a real key:

```python
anthropic.Anthropic(base_url="http://127.0.0.1:8787/anthropic", api_key="<agent token>")
openai.OpenAI(base_url="http://127.0.0.1:8787/openai/v1", api_key="<agent token>")
```

## What it does

- One pooled HTTP client per upstream (keep-alive, HTTP/2 via ALPN).
- Per-upstream `max_concurrency`, `rpm` and `input_tpm` token buckets.
- Weighted round-robin across agents, optional per-agent `max_concurrency`.
- Retries 408/429/5xx/529 and connect errors before any byte reaches the
  agent; honours `retry-after-ms`/`retry-after`/`x-should-retry`. A 429 pauses
  the whole upstream, so the other agents stop hitting the same limit.
- SSE passed through byte-for-byte; a client disconnect cancels the upstream.
- Token usage parsed from responses (stream or not) per agent; optional
  per-agent `token_budget`.
- Opt-in cache + request coalescing for identical non-streaming requests
  (`x-gateway-cache: on`).
- Windowed limits on requests, tokens, dollars and tool calls per agent, key,
  user, team, project or session (see [Limits](#limits)).
- Load shedding: `max_queue` and `queue_timeout_secs` per upstream turn an
  overload into a quick 503 with `retry-after` instead of an unbounded wait.
- Overload, beyond a plain cap: `overflow = "evict"` lets a full queue push out the
  newest waiter of the lowest-weight agent for a heavier one (bulk before
  interactive); `strict_priority = true` serves the heaviest agents first and
  leaves bulk work waiting instead of refusing it; `admit_by_wait = true`
  answers 503 at once, with `retry-after`, when the expected wait is longer
  than `queue_timeout_secs` or the caller's own `x-gateway-max-wait-ms`.
- Every caller is told where it stood: `x-gateway-queue-ahead` and
  `x-gateway-expected-wait-ms` on answers and refusals.
- A waiting ticket for requests that wait (see [Waiting tickets](#waiting-tickets)):
  a streaming caller is told every 2 s how many go before it and about how
  long that takes, and any caller can ask `GET /gateway/queue/{ticket}`.
- MCP servers behind the gateway with per-agent tool policy (see [MCP](#mcp-tool-traffic)).
- Telemetry in the OpenTelemetry GenAI conventions (see [OpenTelemetry](#opentelemetry)).
- Timing headers on every forwarded response: `x-gateway-queue-ms` (time
  spent waiting for an upstream slot, summed over retries),
  `x-gateway-upstream-ms` (final attempt's time to response headers) and
  `x-gateway-attempts`.

## Admin API (`Authorization: Bearer <admin_token>`)

| Method | Path | |
|---|---|---|
| GET | `/gateway/health` | liveness (no auth) |
| GET | `/gateway/stats` | queues, in-flight, cooldown, per-agent counters and tokens |
| GET | `/gateway/queue/{ticket}` | where a waiting request stands (the agent's own token) |
| POST | `/gateway/agents` | `{"id","weight"?,"max_concurrency"?,"token_budget"?}` → `{"token"}` |
| GET | `/gateway/agents` | registered agents (no tokens) |
| DELETE | `/gateway/agents/{id}` | revoke an agent's tokens |
| GET | `/gateway/limits` | limit rules with current usage, reservations and refusals |
| PUT | `/gateway/limits/{name}` | add or replace a rule (same fields as `[[limits]]`) |
| DELETE | `/gateway/limits/{name}` | remove a rule |
| GET | `/gateway/metrics` | Prometheus text: GenAI/MCP convention metrics plus queue gauges |
| GET | `/gateway/servers` | every model server with its health, and servers going out and coming back |
| GET | `/gateway/reports` | reports the gateway wrote; `/gateway/reports/{file}` opens one |
| POST | `/gateway/reports/run` | write a report now (`?day=YYYY-MM-DD`, `&span=day\|week`) |
| POST | `/gateway/obs/client` | a caller reports the timings it measured (the agent's own token) |

## Limits

```toml
[[limits]]
name = "team-monthly-budget"
scope = "team"          # global | agent | key | user | team | project | session
window = "month"        # "30s" "1m" "1h" "7d" slide; "day" and "month" reset on the UTC boundary
usd = 500.0             # also: tokens, requests, tool_calls
strict = true
```

Each scope value gets its own counter (every team gets 500 USD here); `match`
narrows a rule to one value, `model` and `upstream` to some calls, `tool` to
MCP tool calls. A refused call gets 429 `gateway_limit_exceeded` with
`retry-after` and `x-gateway-limit: <rule>` before anything is sent upstream.

Admission reserves an estimate of the call (input tokens from the body size;
with `strict`, the request's `max_tokens` too) and the response settles it
with real usage, so concurrent calls cannot all slip past a nearly spent
budget. Dollars come from LiteLLM's reported cost when present, else from
`[obs.prices]`. After a restart, usage in the open windows is rebuilt from
the request log, and rules set over the API are kept with the store's metadata.

## MCP tool traffic

```toml
[[mcp_servers]]
name = "github"
url = "https://api.githubcopilot.com/mcp/"
api_key_env = "GITHUB_MCP_TOKEN"
deny_tools = ["delete_*"]
max_result_bytes = 262144
```

Agents use `http://127.0.0.1:8787/mcp/github` (Streamable HTTP) with their
gateway token. The gateway injects the credential and, per agent, hides
disallowed tools from `tools/list` and refuses calls to them, applies
`tool_calls` limit rules, caps `tools/call` concurrency with the same fair
queue as model calls, and replaces results over `max_result_bytes` with a
tool error the model can act on. Every MCP request is a row in the `tools`
table (query it with `"table": "tools"`), a span, and a
`mcp.client.operation.duration` sample; tool calls that send the run's
`x-gateway-run-id` share a trace with its model calls. One server per path;
merging several servers behind one endpoint is not implemented.

## OpenTelemetry

Telemetry follows the OpenTelemetry GenAI and MCP semantic conventions as of
2026-11 (still marked Development upstream, so names can move again).

- The root span of each model call carries `gen_ai.operation.name`,
  `gen_ai.provider.name`, request and response model, `gen_ai.usage.*`
  (including `cache_read.input_tokens` and `cache_write.input_tokens`),
  finish reasons and `gen_ai.response.time_to_first_chunk`; gateway-only
  facts sit under `pybox.*`.
- Export: `[obs.otlp_export]` pushes the gateway's spans as OTLP/HTTP
  protobuf; `GET /gateway/obs/trace/{id}?format=otlp` returns one trace as
  OTLP/JSON. The root span leaves as `chat <model>` with kind CLIENT.
- Ingest: spans received on `/gateway/otlp/v1/traces` have older and vendor
  attribute names (`gen_ai.system`, `gen_ai.usage.prompt_tokens`, OpenLLMetry
  `llm.*`, OpenInference `llm.token_count.*`) folded into the current ones.
- `GET /gateway/metrics`: `gen_ai.client.inference.duration`,
  `...time_to_first_chunk`, `...operation.input_tokens/output_tokens`, the
  `...usage.*` token counters and `mcp.client.operation.duration`, with the
  conventions' attributes and bucket boundaries, in Prometheus naming.
  Not emitted: per-chunk timing, modality splits (reported as `unknown`),
  and prompt or completion content.

## Observability and ROI

Every proxied request becomes one row (who, which key, which LiteLLM server, tokens, cost, cache use, and where
the time went) plus trace spans. LiteLLM continues the gateway's `traceparent` and can export its own spans to
`POST /gateway/otlp/v1/traces`, so one trace shows gateway queueing, each attempt, and LiteLLM's router and provider call.

- ROI dashboard at `/gateway/roi`: per person, per team and for the serving side, what was spent, what came of it,
  and suggestions with the numbers they rest on (`GET /gateway/obs/insights`). See [ROI.md](ROI.md).
- Dashboard at `/gateway/dashboard` (asks for the admin token): ROI by project, team and deliverable; token generation
  time and time to first token by user, session, agent or model; per-hop delay; prompt (KV) cache, LiteLLM response
  cache and gateway cache; per key, LiteLLM cluster, server and IP; trace waterfall.
- Storage: `backend = "memory"` by default. `pybox-gateway-tdb` (in `tdb-backend/`, needs access to the
  super-shotgun repo) stores to Super Shotgun TDB, an append-only columnar time-series store; set `backend = "tdb"`.
- Request headers agents may send: `x-gateway-run-id` (groups requests into one trace), `x-gateway-deliverable-id`,
  `x-gateway-user-id`, `x-gateway-client-sent-at` (unix ms, measures client to gateway delay), `traceparent`.
  `x-gateway-request-kind` names the kind of work (`ide`, `chat`, `batch`, ...; an agent's `kind` setting is the
  default) and is logged as `request_kind`; scheduling does not look at it.
- ROI inputs: `POST /gateway/obs/meta/deliverable` `{"id","project_id","team_id","kind","title","url","status","value_usd"|"hours_saved"}`,
  `POST /gateway/obs/meta/link` `{"run_id","deliverable_id"}`, `POST /gateway/obs/meta/rate` `{"id":"<team or *>","hourly_usd"}`.
  Only `accepted` deliverables count as value.
- Stored rows keep millisecond fields (`*_ms`) to three decimals (one microsecond); the writer rounds them once so
  TDB stores them as small integers (latency columns measured 44% smaller).
- Query API: `POST /gateway/obs/query` (`group_by`, `bucket_ms`, `filter`, metrics such as `count`, `sum:cost_usd`,
  `p95:ttft_ms`, `rate:is_error`, `uniq:agent_id`), `/gateway/obs/rows` (at most 2,000 rows), `/gateway/obs/trace/{trace_id}`, `/gateway/obs/roi`.
- Whole log for a window: `GET /gateway/obs/export?from=<ms>&to=<ms>[&table=requests|spans][&cols=a,b,c]` returns
  every row, oldest first, one JSON object per line, with no row limit: it is streamed ten minutes of log at a time, and
  `cols` keeps only the named columns. For usage reports and simulator replays (`gwsim replay`, see `sim/README.md`).
  Each request row carries `request_kind` (the caller's word for the work), `priority` (`interactive`, `normal` or
  `batch`) and, where the prompt was fingerprinted (prefix routing or `[lead]`), `prefix_chain`: the hashes and
  token counts of the prompt's first block and last three, which say which requests share a start without keeping
  any of the prompt.

Try it with two real LiteLLM proxies and a mock model server:

```sh
python3 demo/mock_provider.py 9101 &
for p in 4000 4001; do docker run -d --network host -v $PWD/demo/litellm.yaml:/app/config.yaml:ro \
  -e OTEL_EXPORTER=otlp_http -e OTEL_ENDPOINT=http://127.0.0.1:8787/gateway/otlp/v1/traces \
  -e "OTEL_HEADERS=Authorization=Bearer demo-otlp" -e OTEL_SERVICE_NAME=litellm-$p \
  ghcr.io/berriai/litellm:main-stable --config /app/config.yaml --port $p; done
(cd tdb-backend && cargo build --release) && tdb-backend/target/release/pybox-gateway-tdb --config demo/gateway.demo.toml &
python3 demo/drive.py 120      # then open http://127.0.0.1:8787/gateway/dashboard (token: demo-admin)
```

## Runs, prompt cache, fair scheduling, cost ledger

The gateway fingerprints each chat request (tools, system prompt, messages)
without re-serializing it, and uses that for four things. Design and
measurements: [LEAD.md](LEAD.md).

- **Runs.** Requests are grouped into agent runs by `x-gateway-run-id`, or
  by conversation growth when no header is sent (`run_id`, `run_inferred`,
  `run_turn` columns).
- **Prompt cache.** Each request row says whether the provider cache held
  and, if not, why (`cache_cause`: `no_breakpoint`, `system_volatile`,
  `tools_changed`, `ttl_expired`, ...) and what it cost (`cache_lost_usd`).
  `[lead] cache_points` decides what to do about missing cache markers:
  `shadow` only measures (`shadow_saving_usd`), `on` splices them in
  (`gateway_saved_usd`), `experiment` does so for a share of runs and
  `GET /gateway/lead/experiment` gives the verdict. Per-agent causes:
  `GET /gateway/lead/cache`.
- **Old tool results.** `[lead] tool_results = "on"` replaces all but the
  most recent tool results of a long run with a one-line note
  (`trim_cleared`, `gateway_saved_tokens`). This changes what the model
  sees, so the default only estimates it; `experiment` clears for a share of
  runs and `GET /gateway/lead/experiment?name=tool_results` compares cost,
  failures and turns per run. What is cleared moves forward only when the
  tokens saved later outweigh rewriting the cached prompt once.
- **Fair scheduling.** `policy = "fair"` on an upstream shares capacity by
  tokens between teams, serves the run with the least service first, lets a
  request whose cache is about to expire go ahead within a bound, and orders
  `interactive` before `batch` inside a team (`priority` on the agent or
  `x-gateway-priority`). With `[[upstreams.backends]]` requests go to the
  least loaded server, staying on the one that holds the run's cache unless
  it is overloaded; `adaptive = true` finds each server's best concurrency
  from observed throughput. `/gateway/stats` shows per-backend load.
- **Cost ledger.** `POST /gateway/lead/events` appends facts about
  deliverables (`deliverable`, `link`, `status`, `value`, `run_end`);
  `GET /gateway/lead/roi?from=&to=&as_of=` puts every dollar in one of
  `deliverable`, `shared`, `waste`, `unattributed`, splits deliverable spend
  into work in progress, finished and rework, and gives ROI as a low-high
  range. `as_of` reproduces a past report exactly.

`cargo run --release --bin gateway-eval` prints the scorecard these claims
are held to.
## One scheduler, overload and failing servers

Round robin and fair scheduling run on the same queue; round robin is the
fair queue counting requests instead of tokens. So everything below works
with either policy, alone or combined with `[[upstreams.backends]]`.

- **Starvation guard.** `strict_priority = true` serves the most important
  class first. `min_share = 0.15` still gives the classes below it that share
  of grants, and `reserve_slots = 1` keeps a slot free of less important work
  while more important requests have been arriving, so a long batch answer
  does not stand in front of the next interactive one.
- **Queue limits.** `max_queue` counts requests, `max_queue_bytes` their
  bodies. With `overflow = "evict"` the newest, least important waiter makes
  room.
- **Refusing in stages.** With `shed_from = 0.25` the least important work
  (by priority class, then agent weight) starts to be refused when a quarter
  of `max_queue` is taken, more often the fuller the queue gets, and always
  from `shed_ceiling` (0.75) on; the most important work is refused only
  when the queue is full, work in between at thresholds in between. The
  refusal comes at once (`gateway_shed_early`, reason `saturated`) instead
  of after a wait that ends in a timeout. `shed_floor = 0.1` keeps a tenth
  of the queue's places open to each level of importance whatever the fill,
  so batch work is thinned out under overload, not shut out. Off unless
  `shed_from` is set. `/gateway/stats` counts `shed_early`.
- **Holding batch work.** With `hold_batch = true`, requests of
  `priority = "batch"` are not refused for a full queue. They are held
  beside it and sent to a server only when no other request that could run
  is waiting (whatever the policy and `min_share` say), so they fill the
  time the servers would stand idle. They take no place in the queue:
  `max_queue`, `max_queue_bytes`, staged refusal, eviction, `admit_by_wait`
  and `queue_timeout_secs` pass them by, and they do not lengthen the wait
  other callers are told. Other work keeps its short queue and is still
  refused when that is full. The caller's connection stays open while its
  request is held, so batch callers need a timeout as long as they are
  willing to wait. What is held is bounded on its own:
  - `hold_max` (10,000 requests) and `hold_max_bytes` (256 MiB of request
    bodies); beyond either a batch request is refused at once
    (`gateway_hold_full`, `retry-after` 30 s or more).
  - `hold_timeout_secs` (900): a request held this long gets a 503
    (`gateway_hold_timeout`). `x-gateway-max-wait-ms` or the agent's
    `max_wait_ms` shortens it.
  - A caller that hangs up is forgotten at the next look at the queue; its
    request is never sent.
  - Held requests are in memory only: a gateway restart drops them, as it
    drops the queue.

  Each held request is an open connection, so the process needs a file
  limit above `hold_max` plus the usual traffic (`ulimit -n`), and
  `hold_max_bytes` of memory to spare. `reserve_slots` still matters: a
  batch request that got a seat is not interrupted when interactive work
  arrives. `/gateway/stats` reports `held`, `held_bytes` and
  `hold_refused`; request rows of held work carry `held = true`.
- **Least used first.** `least_used_half_life_secs = 60` orders callers
  (teams under `policy = "fair"`, agents otherwise) by the server time their
  requests held lately, per weight, with older use counting half per
  half-life, instead of giving every waiting caller a turn. A caller that
  sends little goes straight to the front; one that floods waits behind
  everyone else. 0 = take turns. Measured numbers for both: `lab/OVERLOAD.md`.
- **Each caller's own patience.** `admit_by_wait` refuses at the door what
  would wait longer than the caller can: `x-gateway-max-wait-ms` on the
  request, else the agent's `max_wait_ms`, else the upstream's
  `queue_timeout_secs`. One limit for all callers helps little when they
  differ; set it per agent (`lab/OVERLOAD.md`).
- **Wait estimate.** The wait announced in `x-gateway-expected-wait-ms` (and
  `-max-ms`) comes from the rate at which requests of that class were actually
  granted in the last 45 s, and a refusal's `retry-after` is spread out so
  refused callers do not all return at once.
- **Routing.** Among servers that serve the model the request goes to the one
  with the least outstanding work per slot, not the fewest requests.
- **Same prompt start, same server.** A model server reads only the part of
  a prompt that differs from one it still holds in its prefix (KV) cache, so
  with several `backends` the gateway remembers which prompts it sent to
  which server and sends a request to the one holding the longest start of
  its prompt, whoever sent the earlier one (`affinity = "prefix"`, the
  default; it needs neither `[lead]` nor the fair policy). The warm server
  may be busier than the least loaded one by `spill` times the share of the
  prompt it holds; past that, or when it is full, the request goes by load as
  before. `affinity_wait_ms` lets a request wait that long for a full server
  holding most of its prompt instead of having a free one read it all again.
  `prefix_slots` on a backend says how many prompts the server keeps
  (default: its `max_concurrency`). The response carries
  `x-gateway-prefix-warm: <held>/<prompt>` in estimated tokens, the request
  log `prefix_warm_tokens` and `prefix_prompt_tokens`, and `/gateway/stats`
  counts warm, spilled and cold requests per upstream (`prefix_routing`).
  This is the gateway's picture of the servers' caches, not a report from
  them. `affinity = "run"` keeps the earlier behaviour (only the next turn of
  a recognised run returns to its server), `"off"` routes by load alone.
- **One cost instead of two rules.** `route_by = "cost"` picks the server
  with the least prompt tokens left to read plus `load_weight` times the
  prompt tokens it already has in flight per seat, so a warm but busy server
  and a cold idle one are compared in the same unit. The default stays
  `"load"` (least load, with the `spill` tolerance).
- **The picture is checked against the servers.** A remembered prompt is
  replaced the way llama.cpp picks a slot: a new prompt continues in the slot
  whose prompt shares the longest start with it (at least a tenth of the new
  prompt), otherwise the slot used longest ago goes. When a server's answer
  reports cached prompt tokens (`usage.prompt_tokens_details.cached_tokens`,
  `cache_read_input_tokens`), the count is compared with what was expected:
  a miss where a hit was expected lowers the number of prompts that server is
  believed to keep, a hit on a prompt thought to be gone raises it
  (`prefix_slots` and `prefix_corrections` per backend in `/gateway/stats`).
  A server that never reports cached tokens is left as configured.
- **Session admission.** With more kinds of prompt than cache slots, every
  new kind pushes out one that is about to be needed again. `slot_hold_ms`
  makes a request whose prompt no server holds wait while every prompt it
  could push out was used within that time, at most `slot_max_wait_ms`
  (default 10 s). Requests whose prompt is held are never delayed by it,
  also not by a held request of the same caller ahead of them. Measured on
  conversations with pauses: with two conversations per slot reuse went
  from 13% to 58% and half of the turns started in under a second; a
  conversation without a slot waits its turn (`lab/PREFIX.md`, third round).
- **Batch work kind by kind.** With `batch_group_ms`, a waiting `batch`
  request that would start cold lets later batch requests of the same caller
  go first when a free server holds their prompt, until it has waited that
  long. Only the order inside one caller's own queue changes.
- **Why a request was refused.** Refusals carry `x-gateway-reject-reason`:
  `caller_limit` (the caller's own budget, limit rule or loop guard; 429),
  `saturated` (queue full, wait too long; 503, worth retrying after
  `retry-after`) or `no_server` (no server lists the model, refused at once,
  or every server for it is out after failures). `stale_after_secs` treats a
  server with requests in flight and nothing finished for that long as full
  until something finishes.
- **Stand-in models.** `model_groups = [["a", "b"]]` declares models that may
  answer for each other. Only agents registered with `model_swap: true` are
  moved, only when a server of the other model has room and theirs has not,
  and the response says so in `x-gateway-model`.
- **Failing servers.** Three failures in a row (5xx, connect error, no first
  byte within `first_byte_timeout_secs`) take a server out for 10 s, doubling
  to 120 s; it comes back with one trial request. A 429 pauses only that model
  on that server for its `retry-after`. A retry avoids the server that just
  failed.
- **Rewrites that a provider rejects.** If a request the gateway changed
  (cache points, trimming) comes back 400 or 422, the client's own body is
  sent once. If that works, rewriting is switched off for that upstream and
  model for 10 minutes (`rewrite_rejected` in the stats, `rewrite` in the log).
- **Cancelled streams.** When the client hangs up before usage arrives, input
  and output tokens are estimated from what was sent, so the cost is not lost.

## Day-to-day operation

Four things that matter once the gateway runs unattended. All are off until
configured, except that agents are kept whenever a persistent store is.
The page `/gateway/ops` (asks for the admin token) shows them on one screen:
servers and their health, the record of outages, the reports written, and
what callers measured over the last 24 hours (`GET /gateway/ops/data`).

### Server health

```toml
[[upstreams]]
name = "local"
health_probe_secs = 5        # ask every server behind it whether it is alive
# health_probe_path = "/health"   # default /v1/models (LiteLLM: /health/liveliness)
refuse_when_down = true      # 503 at once while every server for the model is out
```

Without probes the gateway learns about a server from callers' requests:
three failures in a row (or one refused connection) take it out, and after
the pause the next caller's request is the test. With `health_probe_secs`
the gateway asks each server itself (any answer below 500 counts as alive):
a server that died while idle is out before a request reaches it, a server
that is out stays out until a probe is answered, and then gets work again at
once, one request at a time until one succeeds.

With `refuse_when_down`, a request whose every possible server is out is
answered 503 (`x-gateway-reject-reason: no_server`, log `error_type =
gateway_servers_down`) instead of waiting in the queue; requests already
waiting stay, since a restart is often over within their patience.

`GET /gateway/servers` shows each server (`up`, `down`, or `trial` while one
request tests it), when it went out, what was last seen wrong and when it
will be tried again, and the record of servers going out and coming back with
how long each was out. The record is kept with the store's metadata (kind
`health_event`, the latest 1,000), so with the TDB store it outlives a
restart; `/gateway/stats` has `servers_down` per upstream.

### Agents kept across restarts

Agents created, edited (`PATCH`), rotated or removed over the admin API are
written to the store's metadata and applied again at start, on top of the
config file: a removed or rotated config token stays refused, an edited
config agent keeps its edit. Only the SHA-256 of a token is stored, never the
token. This needs a persistent store (`obs.backend = "tdb"`); with the memory
store nothing outlives the process, as before. Console jobs are left out:
their keys die with the job.

### Reports written by the gateway

```toml
[reports]
dir = "reports"       # setting this turns it on
at = "07:00"          # local time; the report covers the day before
tz_hours = 9
# weekly = true       # every Monday also the seven days before
# keep_days = 90
# server_hour_usd = 1.2   # passed to the ROI report when the log has no cost
```

Every day at `at` the gateway runs the ROI and productivity report
([ROI.md](ROI.md)) over the day that ended and writes
`roi-day-YYYY-MM-DD.html` (the `/gateway/roi` page with the numbers inside,
openable straight from the folder) and `.json`; on Mondays also
`roi-week-…`. Servers that went out during the window are added under
`outages`. A report that is missing when the gateway starts is written then.
`GET /gateway/reports` lists them; a browser can open
`/gateway/reports/roi-day-2026-11-21.html?token=<admin token>`.

### What the caller measured

The request log times the first token as it leaves the gateway. A caller
that times its own requests can send what it saw after each answer:

```sh
curl -X POST http://127.0.0.1:8787/gateway/obs/client -H "authorization: Bearer <agent token>" \
  -d '{"trace_id": "<x-gateway-trace-id of the answer>", "ttft_ms": 412, "total_ms": 1830, "connect_ms": 3}'
```

(or `{"items": [...]}`, up to 1,000). Each report becomes a `client.request`
span in the request's own trace with both views and their difference
(`net_ttft_ms`, `net_total_ms`: the way back to the caller). An agent can
report on its own requests only; the gateway matches against its latest
20,000 requests. `GET /gateway/obs/client?from=&to=` gives p50 and p95 of
each per agent and per model. The way *to* the gateway is
`x-gateway-client-sent-at` on the request, as before.

## Waiting tickets

A request that has to wait is not left waiting in silence. The gateway
tells its caller that the servers are busy, how many requests go before it
and about how long that will take, the way a numbered ticket does.

| Caller | What it gets |
|---|---|
| Streaming, notice on | The answer starts at once with status 200 and `x-gateway-ticket`. Every 2 s while it waits: `: gateway queued ahead=12 expected_wait_ms=8300 waited_ms=2000 expected_wait_max_ms=9100 ticket=c04b17db6e915f08`, then `: gateway dispatched ...` and the model's stream. |
| Not streaming | Nothing can be said inside the answer. The caller names a ticket (`x-gateway-ticket: job-7`) and it, or whoever watches for it, asks `GET /gateway/queue/job-7`. |
| Refused | 503 with `retry-after`, `x-gateway-queue-ahead` and `x-gateway-expected-wait-ms`, as before. |

- **Turning it on.** Per upstream: `queue_notice = "sse"` (comment lines)
  or `"event"` (`event: gateway.queue` with the same fields as JSON).
  Default `off`. A call's `x-gateway-queue-notice: sse | event | off`
  wins over the upstream's setting.
- **What a plain client sees.** With `sse` nothing changes: lines that
  start with `:` are comments in the SSE format and clients skip them (the
  OpenAI Python SDK was checked against local models). `event` is for
  clients that want to show the ticket; a client that does not know the
  event type may stumble over it, so it is not the default. A request that
  gets a slot within 250 ms is answered as if there were no notice.
- **The one thing that changes.** Once the notice has started, the status
  line is already 200. If the request is refused later (its wait ran out,
  it was pushed out), that arrives as `event: error` with the usual JSON
  body instead of a 503. Callers that rely on the 503 to retry should leave
  the notice off or send `x-gateway-queue-notice: off`.
- **Status.** `queued` (in the queue), `held` (batch work held beside it
  with `hold_batch`), `running` (with a server), `dispatched` (last line
  before the model's stream).
- **The ticket.** `x-gateway-ticket` on the request names it (letters,
  digits, `-`, `_`, `.`; up to 64); otherwise the gateway makes one for
  streaming callers that are told. `GET /gateway/queue/{ticket}` with the
  same agent's token answers
  `{"ticket","status","reason":"saturated","ahead","expected_wait_ms","expected_wait_max_ms","waited_ms"}`
  while the request waits, `{"status":"running"}` once a server has it, and
  404 when it has ended. An agent sees only its own tickets.
- **The estimate.** The same one that refuses at the door: the requests
  that go before this one, divided by the rate at which requests of its
  importance have been getting seats lately. `expected_wait_max_ms` is the
  pessimistic end. Before the gateway has seen a request finish it says
  how many are ahead and 0 for the time. For held batch work the estimate
  assumes traffic stays as it is, so it is rough: held work moves when
  other work stops arriving.

## Management console

`/gateway/console` (asks for the admin token) is the page for running the gateway day to day. The dashboard above
answers "what did the spend buy"; the console answers "what is happening and who may do what".

| Tab | What it is for |
|---|---|
| Live | in-flight and queued requests per upstream, requests and output tokens per second, 429 cooldowns, agents active right now (one sample a second, last 15 minutes) |
| Jobs | start a batch (model, prompts, count, concurrency, weight, project/team), watch and cancel it; history of every run (`x-gateway-run-id`) with its requests |
| APIs and keys | each upstream's limits, load and latency; create, edit, rotate and revoke agent keys; token budget use |
| Models | what every upstream says it serves, joined with measured TTFT, speed, errors and cost per model; price table editing |
| Tokens | tokens per minute in and out, by model, by agent/team/project/key; budget burn |
| Latency | p50/p90/p95/p99 over time for any timing column, log-scale histogram, time × latency heatmap, breakdown by any dimension, slowest requests |
| Explorer | any metric × any dimension × any bucket, with a filter and an overlay of the previous period |

Admin API added for it (all behind the admin token):

| Method | Path | |
|---|---|---|
| GET | `/gateway/upstreams` | config (never the key), scheduler state and counters per upstream |
| PATCH | `/gateway/agents/{id}` | change `weight`, `max_concurrency`, `token_budget`, `project_id`, `team_id`, `user_id`, `session_id`; `null` clears |
| POST | `/gateway/agents/{id}/rotate` | replace the agent's token; the new one is returned once |
| GET | `/gateway/models` | `/v1/models` of every upstream, merged |
| GET | `/gateway/live` | one-second samples of the last 15 minutes |
| GET, POST | `/gateway/jobs` | list jobs; start one: `{"upstream","model","prompts":[...],"count","concurrency","max_tokens"?,"name"?,"weight"?,"project_id"?,"team_id"?,"deliverable_id"?}` |
| DELETE | `/gateway/jobs/{id}` | cancel |

A job calls the gateway's own listen address with a key made for it, so its requests are queued, limited, retried and
logged exactly like an agent's, under run id = job id and agent `job:<name>`. Keys created or edited at runtime and the
job list live in memory and are gone after a restart; the request log is not.

The query API also takes `hist:<column>`: counts in fixed log-scale bins (eight per decade), with the bin edges
returned as `hist_bounds`. With `bucket_ms` that is a heatmap.

[lab/](lab/README.md) stands up LiteLLM with two small local models and three cheap hosted ones and drives it with
and without the gateway.

## Develop

```sh
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked      # unit tests + end-to-end tests against a mock upstream
```

Local serving benchmark (mock model server in-process, no network; see
[BENCH.md](BENCH.md)):

```sh
cargo build --release --locked --example bench
target/release/examples/bench target/release/pybox-gateway --format openai
```

Live smoke test (real API, tiny `max_tokens`, a few cents at most):

```sh
ANTHROPIC_API_KEY=... cargo run --release --example live -- 10 claude-haiku-5-5
```

Multi-provider streaming load test with resource sampling and an HTML
timeline report (real APIs, costs about a dollar per 5-minute run; see
[loadtest/RESULTS.md](loadtest/RESULTS.md)):

```sh
cargo build --release --locked
python3 loadtest/run.py --out /tmp/lt      # needs aiohttp; keys in the gateway's env
python3 loadtest/report.py loadtest/report.html /tmp/lt
```

TLS trusts both the bundled webpki roots and the OS store (`SSL_CERT_FILE` is
honoured), so it works behind TLS-inspecting corporate proxies. CI uses only
the local mock.
