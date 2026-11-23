# gwsim: simulate the gateway stack instead of running it

A discrete-event simulator for the whole request path:

```
clients/agents ─> gateway ─> LiteLLM-style proxy ─> model servers / hosted APIs
       │             └─> log store (TDB)
       └─> MCP tool servers
```

Describe the components and the load in a TOML file, run it, and get latency
percentiles, throughput, queue lengths, where the time of a request went and
which component is the bottleneck. A 10-minute experiment takes about 10 ms.
There is a live monitor page that runs the same engine in the browser.

How far to trust it: on five measured saturation runs it was **not** fitted to,
success shares are predicted within 1.2 percentage points (median; worst 7.1)
and latency, throughput and counts within 1.6% (median; 90th percentile 12%,
worst 31%). Details, including where it is off, are in
[VALIDATION.md](VALIDATION.md). Parts that no measurement has checked yet are
listed under [What is assumed](#what-is-measured-and-what-is-assumed).

## Use it

```sh
cd gateway/sim
cargo run --release -- run scenarios/sat-smart.toml          # one run, report on the terminal
cargo run --release -- run scenarios/sat-smart.toml --json out.json --records out.jsonl --frames frames.json
cargo run --release -- validate scenarios/sat-smart.toml --measured sat-smart.jsonl --samples sat-smart.samples.jsonl
cargo run --release -- validate-all scenarios/validation.toml --reference reference   # predicted against measured
cargo run --release -- calibrate sat-direct.jsonl --tokens interactive=24,batch=64
./build-monitor.sh target/monitor.html [DIR_WITH_SAT_LOGS]   # needs: rustup target add wasm32-unknown-unknown
```

The report names the bottleneck in words. For the run that sends four times
capacity straight at LiteLLM:

```
* smol-srv is the bottleneck: 100% busy (90% of the run)
  qwen-srv has 98 requests queued inside it, where they cannot be reordered, refused or cancelled (90% of the run)
  qwen-srv is the bottleneck: 100% busy (88% of the run)
  smol-srv has 89 requests queued inside it, where they cannot be reordered, refused or cancelled (88% of the run)
  qwen-srv spends 79% of its time on requests whose caller already left (51% of the run)
  smol-srv spends 71% of its time on requests whose caller already left (39% of the run)
```

### The monitor

`build-monitor.sh` writes one self-contained HTML file: the engine compiled to
WebAssembly, the scenarios, and the predicted-against-measured table. Open it
in a browser; no server is needed. It shows the components with traffic moving
between them in both directions (requests towards the servers on one lane,
the tokens of the answers back to the callers on the other), load and queue per component, the bottleneck
highlighted and explained, time series, and where the time of answered
requests went. A column with more than six nodes is drawn as one tile per
node, grouped by name, so a fleet of hundreds stays readable. Load
multiplier, seed and speed are controls; the scenario itself can be edited as
JSON and rerun in place. If the browser cannot run WebAssembly it replays
recorded runs.

The page opens on a guided setup ("시작하기"): five questions, one a step,
each with a few ready-made answers (how many people, what kind of work, how
many servers relative to the demand, how the gateway treats requests, how
long a period). The last step shows what was chosen and starts the run; when
the run ends the report tab opens with a verdict, the results by kind of
request, what was full, rule-based advice and the same as Markdown to copy. Every
answer only fills in the detailed setup page ("상세 설정"), where each value
can still be changed.

The page is a fixed frame with a menu on the left; nothing moves while a run
plays, and what does not fit scrolls inside its own panel.

- **Setup** comes first: groups of callers (how many, which model, how often,
  how long an answer, priority), pools of model servers (servers, GPUs per
  server, GPU class, requests a replica runs together), LiteLLM instances,
  and how the gateway distributes and queues (each choice says what it is
  good for and which one is the usual default). A bar per model shows demand
  against capacity before anything runs. Model and GPU speeds there are
  assumed sizing numbers.
- **Live**: the picture can be zoomed (wheel), dragged, and a group opened by
  double click; tiles large enough on screen say what they are running. Each
  group has a bar of how many of its nodes are at which load, and a line with
  how many are full or idle. Load is coloured in four steps (under 50%, 75%,
  90%, above). Clicking a group or a card shows its numbers and emphasises
  its line in the charts; clicking a chart legend picks it in the picture.
- **Idle capacity**: per pool, how much of the slot time nothing ran in, when
  (a strip over the run), the tokens it could still have made (an upper
  bound), and what that suggests.
- **Settings search**: `gwsim tune` in the browser (see below).

Traffic is drawn so that one element of flow covers what is under it with an
alpha of 0.01; n of them on the same spot come to 1 - 0.99^n. With WebGL the
count per pixel is added up in a float buffer and turned into colour in a
second pass; the 2D canvas path counts per piece of wire. A streak on screen
stands for several elements (its weight), which gives the same picture as
drawing them one by one.

How it stays smooth with thousands of nodes and dots:

- The engine runs in a worker, so simulating never delays a frame. If the
  browser refuses the worker it runs on the page's thread within a few
  milliseconds per frame, and the virtual clock slows down instead of the
  picture.
- Traffic is drawn as nearly transparent streaks that slide along a stretch
  of their wire, so one request is a faint trace and a busy wire builds up to
  solid colour. They are drawn with WebGL2: a streak is uploaded once, with
  its wire and start time, and the vertex shader moves it; a frame uploads
  only the streaks born in it. Integrated graphics are enough. Without WebGL2,
  or where WebGL is a software rasteriser, the page draws fewer streaks on a
  2D canvas (`?renderer=2d` and `?renderer=gl` force one or the other,
  `?worker=off` and `?wasm=off` force the other fallbacks). If frames get
  slow the page lowers the number of streaks rather than stutter.
- Wires and tiles change only when a sample arrives, so they live on their own
  canvas and are redrawn then, not every frame. The engine sends every sample
  as a few numbers per node group and the whole state only as often as the
  page redraws.

Measured in headless Chromium without a GPU (4 cores, so the 2D canvas path)
on `example-fleet-xl`: 60 frames a second with about 1,800 streaks, 2 to 4 ms
per panel refresh (ten a second). The WebGL path has not been timed on a real
GPU; under a software rasteriser it is slower than the 2D path, which is why
the page avoids it there.

The gateway's own console (`/gateway/console`, PR #3) shows real traffic. The
monitor is a separate page on purpose: it needs no running gateway, and PR #3
is still changing. Both use the same vocabulary (upstreams, queue, in flight),
so folding one into the other later is a UI job, not a model job.

## Writing a scenario

Every scenario in `scenarios/` is a working example; `lab-round1.toml` is the
most complete. A minimal one:

```toml
[gateway]
[[gateway.upstream]]
name = "local"
targets = ["litellm"]
max_concurrency = 2
max_queue = 30
queue_timeout_s = 12

[[proxy]]
name = "litellm"

[[model_server]]
name = "qwen"
models = ["qwen-0.5b"]
tokens_per_s = 20
prefill_ms = 290

[[client]]
name = "chat"
class = "interactive"
entry = "gateway"              # or "litellm", or "qwen": where callers connect
models = ["qwen-0.5b"]
arrival = { kind = "poisson", rate_per_s = 0.8 }
to_s = 200
output_tokens = 24
timeout_s = 15
```

| Table | What it is | Main knobs |
|---|---|---|
| `[gateway]`, `[[gateway.upstream]]` | The gateway's dispatcher per upstream; field names follow `gateway.toml` | `max_concurrency`, `max_queue`, `queue_timeout_s`, `overflow`, `admit_by_wait`, `strict_priority`, `targets`, `routing` |
| `[[proxy]]` | LiteLLM-style proxy: single-threaded workers | `workers`, `cpu_ms_per_request`, `cpu_ms_per_chunk`, `cpu_ms_after_request` |
| `[[model_server]]` | A server generating at a fixed token rate | `slots`, `tokens_per_s`, `batch_scaling`, `prefill_ms`, `on_disconnect` |
| `[[hosted_api]]` | A provider: latency distributions and rate limits | `ttft_ms`, `tokens_per_s`, `rpm`, `tpm` |
| `[[mcp_server]]` | A tool server called by agents before a turn | `concurrency`, `latency_ms` |
| `[db]` | Where the gateway's request log goes | `rows_per_request`, `rows_per_s`, `max_buffer_rows` |
| `[[host]]` | A machine whose cores several components share | `cores` |
| `[net]`, `[[net.link]]` | One-way latency per hop | `default_ms`, `latency_ms`, `jitter_ms` |
| `[[client]]` | Who sends load | `arrival` (`poisson`, `uniform`, `closed`, `trace`), `weight`, `timeout_s`, `retry`, `max_wait_ms`, `session` |
| `[[phase]]` | Time windows the summary is split by | `from_s`, `to_s` |

`extends = "base.toml"` loads another file first; tables merge key by key and
arrays of named tables merge entry by entry on `name`, so a variant states only
what it changes (see `sat-*.toml`). Unknown keys are errors, and so are
references that do not resolve or a model nothing can serve.

`count = N` on a `[[client]]`, `[[proxy]]` or `[[model_server]]` stands for N
identical ones named `name-1` to `name-N`. A gateway upstream or proxy that
lists `name` gets all of them, and callers are grouped under `name` in the
results. `example-fleet.toml` (240 callers, 48 servers) and
`example-fleet-xl.toml` (1,200 callers, 240 servers) are written this way. The
engine does the first in about 2 s and the second in about 16 s for 520
simulated seconds (about 3 million events a second). Their numbers are assumed:
the accuracy check covers two model servers, and a fleet is an extrapolation
from parts that were checked until a run on several real servers says more
(see `KNOWN-GAPS.md`).

## The model

- **Clients.** Open-loop arrivals (Poisson, uniform or a recorded trace) or
  closed-loop agents (send, wait, think, repeat; optional multi-turn sessions
  with growing prompts and a tool call before each turn). A caller gives up at
  its own timeout and, if told to, comes back after `retry-after` on 503/429.
- **Gateway.** `src/sched.rs` is a port of `gateway/src/scheduler.rs`:
  per-agent FIFO queues ordered by a weighted token counter (the agent
  that has been granted the fewest prompt tokens per weight goes next),
  strict priority, least used first, staged refusal, the bounded queue with
  eviction, the wait estimate behind `admit_by_wait`, cooldown on
  429. A slot is held until the response ends or the caller hangs up. Several
  targets per upstream are balanced by requests in flight.
- **Proxy.** Each worker is one event loop: request handling and every
  streamed chunk queue for the same CPU.
- **Model server.** `slots` requests generate at once, the rest wait in
  arrival order inside the server. Time to answer = prefill + tokens / rate.
  A request whose caller has gone still costs its prefill and first token
  before the server notices (`on_disconnect = "first_token"`, how
  llama-cpp-python behaves); `cancel` and `finish` model other servers.
- **Hosted API.** No queue of its own; log-normal time to first token and
  token rate; token buckets for RPM/TPM answer 429 with a retry time.
- **MCP server, log store, hosts, links.** A bounded-concurrency queue; a
  batched writer off the request path; proportional slowdown when threads
  exceed cores; fixed latency plus jitter.
- **Diagnosis.** A queue is treated as a symptom: for every gateway queue the
  diagnosis looks downstream for the full server, the proxy out of CPU or the
  rate limit that causes it, and separately reports work done for callers who
  left, queues inside servers, and idle capacity a backlog is not wired to.

## What is measured and what is assumed

| Part | Status |
|---|---|
| Gateway scheduler | Ported from the live code; queue lengths, refusals and retries match on held-out runs |
| Model-server rate, prefill, cost of abandoned requests | Fitted from measured logs by `gwsim calibrate`, per measurement session |
| Setup page models (memory, speed under concurrency) | Published measurements where they exist, else an estimate checked against them; per model in `reference/models/RESULTS.md`. Not measured by us on a GPU |
| Gateway overhead (0.03 ms) | Measured |
| Proxy CPU costs | Derived from one aggregate (227 CPU-s for 3,365 requests); the 14x growth of LiteLLM's overhead under load is reproduced only qualitatively (`example-litellm-peak.toml`) and is **not validated** |
| Gateway memory | Linear model with rough constants; the 390 MB peak under a retry storm is not reproduced |
| Hosted API latency and limits | Assumed; whatever the scenario says |
| MCP servers | Assumed; never measured |
| Log store | 96,000 rows/s is TDB's benchmark number; not measured with the gateway attached |
| Host CPU contention | Assumed; off in the lab replays because the fitted rates already include it |
| Network | Fixed latency and jitter; no loss, no bandwidth limit |

Not modelled at all: adaptive concurrency and priority classes, the gateway
response cache and provider prompt caching, backend failures and mid-stream
drops. Experiments that turn on those still have to be run for real.

Every gap, with what would close it, is listed in [KNOWN-GAPS.md](KNOWN-GAPS.md).

## Not overloading the servers

Two settings keep a server from being handed more than it runs at once:

- `headroom = 1.0` on a gateway upstream sets its `max_concurrency` to that
  share of the slots of the servers behind it, so the excess waits in the
  gateway, where priorities apply, and not inside a server.
- `routing = "least_loaded"` (gateway upstream or proxy) picks the target
  with the fewest requests in flight relative to what it can run. With
  servers of different sizes, `least_inflight` fills the small ones first.

The proxy is assumed to know each backend's load, as LiteLLM does when it is
given per-deployment limits; with several proxies each sees all of it, which
is kinder than reality.

## Busy hours and staggering

A `poisson` client can follow a daily rhythm:

    rhythm = { period_s = 1440, busy_at_s = 780, busy_width_s = 540, quiet = 0.1, shift_s = 60 }

`rate_per_s` is then the rate at the height of the busy hours; away from them
it is `quiet` times that. `shift_s` moves a group's day later, so groups can
be staggered. The setup page has this as "busy hours" with a shift in hours
per group, and a button that runs the same day twice, aligned and staggered,
and puts errors, tokens, waits, backlog and idle time side by side. A day is
usually shortened (an hour to a minute); requests keep their real length, so
congestion passes faster than in a real day and the comparison is about
overlap, not absolute numbers.

## The gateway itself, its cache and the log store

There is one gateway. `[gateway]` takes `cores` and `cpu_ms_per_request`:
every request first waits for a free core for that long, and the report says
how busy the cores were. `cache = { hit_rate, lookup_ms }` answers that share
of requests from the gateway without reaching a server. The log store is the
`[db]` node. In the monitor's setup page these are one panel, and the
picture shows the cache and the store hanging off the gateway.

The setup page describes callers as teams of people. Request types (ide,
chat, batch by default) say which model, how long an answer and how patient
the caller is; each team has a headcount and a rate per person for every
type, so one team can be batch-heavy and another chat-heavy. Teams only split
the picture and the tables; the servers do not know about them.

## The prefix (KV) cache

The hit rate is a result, not an input. A client says how much of its prompt
repeats:

```toml
prefix = { share = 0.69, follow_up = 0.39, group_share = 0.42, group = "ide" }
```

`share` of the prompt is the caller's own running context, found again only
by a request that continues the previous one (`follow_up` of them); the
first `group_share` is the same for everyone who names the same `group` (a
system prompt, tool definitions). `scope = "group"` is a prompt that only
has the shared part. The setup page's defaults come from public traces, see
`reference/prior-art/RESULTS.md`.

A model server keeps prompts the way llama.cpp does: each slot holds the
prompt it ran last, a request takes the idle slot that starts like its own
prompt (own context first, then the shared part), else the idle slot used
longest ago. A prefix in a busy slot is not found. `prefix_cache_entries = N`
switches to one cache of N prefixes for all slots (closer to vLLM). A
request whose prefix is found reads only the rest:
`prompt_tokens x (1 - found x prompt_cache_saving) / prefill_tokens_per_s`.

So what is found depends on where requests go:

- `routing = "affinity"`: a stable target per prefix while it has a free
  slot; `replicate_at` gives a busy prefix more targets.
- `routing = "prefix"`: the gateway's own rule (`affinity = "prefix"`,
  `gateway/src/lead`). Only targets with a free seat are candidates. The
  gateway keeps its own record of the last `prefix_slots` prompts it sent to
  each target and picks the one it believes holds most of this prompt,
  unless that target is fuller than the emptiest by more than `spill`.
  With `affinity_wait_s` a request whose warm target is full waits up to
  that long for it before taking a cold one. This is the one to use when a
  result should carry over to the real gateway.
- `routing = "cost"`: the target where the time to read what is not cached
  plus the request's duration times the target's fill is least;
  `cost_load_weight` trades the two, `cost_in_tokens` counts tokens instead.

The report has `prefix_tokens` (prompt tokens, and how many were not read)
and lookups and hits per class. Measured (`reference/kv-cache`, a small
model on CPU): reading time by prompt length and what a cached prefix saves
(86 to 99%). `prompt_cache_hit_rate` is the older fixed-share form.

## Policies first tried here

Three gateway policies were tried here before they were built
(`reference/prior-art/RESULTS.md`). Two are in the gateway since PR #10 and
the simulator now follows that code; `reference/overload/RESULTS.md`
compares it with the measured runs.

- `shed = { from = 0.25, low_ceiling = 0.75, floor = 0.1 }` on an upstream:
  refuse the lowest weight first as the queue fills, each higher weight
  later, the top weight only when it is full. `floor` is the share of the
  queue the lowest weight keeps regardless.
- `least_attained_half_life_s = 60`: serve the caller that has used the
  least slot time per weight, with older use decaying.
- `admit_by_deadline = true` (simulator only): refuse at the door when the
  estimated wait plus a typical run is longer than the caller's timeout.

## Usage report

`gwsim usage` reads one line per request and writes a report on how the
servers are used: by team and request kind, by hour of day, when the
servers are idle or pressed, whether the teams' busy hours coincide, who
uses most, and how much of the input was served from the prefix cache. The
summary lines are rules over those tables; each states the numbers it
compared. The report is in Korean.

The input uses the column names of the gateway's `requests` table (`ts`,
`team_id`, `user_id`, `agent_id`, `session_id`, `model`, `status`,
`client_abort`, `queue_ms`, `ttft_ms`, `total_ms`, `input_tokens`,
`output_tokens`, `cache_read_tokens`, `gateway_cache`, `upstream_ip`).
Other columns are ignored and missing ones are reported as missing, so the
same command reads a simulated run and the real log.

```sh
# from a simulated run
gwsim run SCENARIO.toml --rows rows.jsonl --usage report.md

# from the gateway's log
curl -H "authorization: Bearer $ADMIN_TOKEN" "$GATEWAY/gateway/obs/export?from=$FROM_MS&to=$TO_MS" -o rows.jsonl
gwsim usage rows.jsonl --tz 9 --slots small=192,large=128 --report report.md
```

`--slots` is what the servers of each model run at once; without it the
room left is measured against the most seen running at once, which
understates it. `--day-s` and `--day-start` are for runs whose day was shortened.

The gateway's rows say what kind a request is in `priority` (interactive,
normal, batch) and who sent it in `user_id` or `claimed_user_id`; both are
read. `lab/usage_report.sh` exports a window through
`/gateway/obs/export` and runs the report; `lab/USAGE.md` is the account of
running it on a real log (TDB, local models) and `reference/usage/` holds
the reports: `REPORT.md` from a simulated day, `REAL-DAY.md` and
`REAL-DAY-OUTAGE.md` from the gateway's log.

## Replaying a gateway log

`gwsim replay` takes the gateway's own request log and sends the same
requests, at the same times, into a simulated copy of the system:

```
curl -H "authorization: Bearer $ADMIN" \
  "$GATEWAY/gateway/obs/export?from=$FROM_MS&to=$TO_MS&cols=agent_id,request_kind,priority,model,status,client_abort,attempts,queue_ms,ttft_ms,model_ttft_ms,gen_ms,total_ms,input_tokens,output_tokens,cache_read_tokens,gateway_cache,prefix_chain" > day.jsonl
gwsim replay day.jsonl --base reference/replay/lab-day.toml --fit --seeds 10 [--markdown OUT.md] [--json OUT] [--scenario OUT.json]
```

- The log gives who sent what when: agent, kind of work (`request_kind`,
  from the agent's `kind` setting or `x-gateway-request-kind`; else the
  `priority` class), model, prompt and answer length, and `prefix_chain`
  (which start of the prompt the request shares with others, as hashes).
- `--base` gives the system: gateway settings and servers. Its clients are
  templates (weight, timeout) matched by agent, then by kind (`class`),
  then `*`; the replay fills in the requests.
- `--fit` reads server speed (time before the first token against prompt
  tokens read, tokens per second) and callers' patience off the same log,
  so no separate calibration run is needed. Without it the base's numbers
  are used, which is what a what-if on other hardware wants.
- The output sets the log beside the simulation per kind of work:
  answered, gave up, refused, time to first token, wait at the gateway,
  prompt tokens reused; for the simulated side the mean over the seeds,
  its 95% interval and the range.

With the base as the system really was, the gaps say how far the simulator
is from it. With a changed base (more servers, another policy), the
simulated column is the forecast for that same day. The export has no row
limit and is streamed ten minutes of log at a time; `cols` keeps it small.
`--scenario` writes the generated scenario, which the monitor can open.

## Several seeds, and how large a run can be

```sh
gwsim run scenarios/example-fleet.toml --seeds 10 [--jobs 4]
```

runs the scenario with seeds 1 to N on `--jobs` threads and prints, per
class, the mean, a 95% interval for the mean (Student's t) and the range.
One seed is one possible day; differences between two settings that are
smaller than the interval are not differences.

Every output token is an event by default, which is what limits the size of
a run. `token_stride = 8` at the top of a scenario lets a model server write
eight tokens per event after the first one: the same durations and token
counts, with a caller's leaving noticed and the server's speed set once per
stride. Time and memory by size are in `reference/scale/RESULTS.md`.

## Settings search

    gwsim tune scenarios/example-fleet.toml --tries 40 [--seeds 2] [--write DIR]

tries gateway settings (headroom, routing at the gateway and at the proxies,
strict or weighted priority, queue length, queue timeout, admission by
expected wait, refuse or evict) and keeps, for each of six numbers, the
settings that did best on it: error rate, output tokens per second, longest
and mean wait of a caller, how much sooner high-priority callers are served
than low-priority ones, and requests backed up inside model servers. The
first half of the tries are random, the second half change one or two
settings of the best so far for one number, taking turns. Every try is
scored on all six, so the table shows what each choice costs elsewhere.

It is a search inside the simulator: the best of what was tried, under the
model's assumptions, at one seed unless `--seeds` says more.

`report.idle` (and the `idle` list in `--json`) has the unused capacity per
pool of model servers.

## Keeping the accuracy

The measured logs are not in the repository, but what the accuracy check needs
from them is: `reference/<run>.json` holds when each request of a measured run
was sent and for which model, and the numbers the run produced.
`reference/baseline.json` holds how far the simulator was from each of those
numbers when the baseline was written.

`cargo test` replays every run and fails when

- a number is further from the measurement than the baseline by more than
  2 points (success shares) or 4% (the rest), or
- over the runs the model was not fitted on, the median error passes 3 points
  or 5%, or any success share is off by more than 10 points.

So a change to the model cannot quietly make it less accurate, and the
baseline cannot be loosened past the second rule. After a change that moves
the numbers on purpose, write the baseline again and say why in the commit:

```
gwsim validate-all scenarios/validation.toml --reference reference --seeds 10 \
    --write-baseline reference/baseline.json
```

Two more sets of measured runs are replayed by script instead of by the
gate, because their scenarios are generated from the logs:
`reference/overload/replay.py` (11 overload and fairness runs) and
`reference/prefix/replay.py` (8 prefix-cache runs). Run them after a change
to the scheduler or the cache model and compare with the `RESULTS.md` there.

To add a measured run: write its scenario, add it to
`scenarios/validation.toml`, and derive the reference files from the logs with
`--data DIR --write-reference reference` in place of `--reference reference`.

## Layout

```
src/scenario.rs   scenario format and checks
src/sim.rs        event loop and component models
src/sched.rs      the gateway scheduler in virtual time
src/metrics.rs    sampling, time series, diagnosis
src/report.rs     summaries shared by simulated and measured runs
src/replay.rs     a gateway log sent again through a simulated system
src/calibrate.rs  fitting server parameters from a log
src/validate.rs   predicted against measured
src/wasm.rs       browser exports
web/monitor.html  the monitor page (template)
reference/        measured runs reduced to what the accuracy check needs, and its baseline
scenarios/        lab replays (sat-*), examples (example-*), validation manifest
```

Tests: `cargo test` checks the engine against queueing theory (M/D/1 wait,
closed-loop throughput), invariants (every request settled once, caps never
exceeded, same seed same run), the diagnosis, and the accuracy of the replays
against the measured runs (see "Keeping the accuracy"). With `GWSIM_DATA`
pointing at the measured logs it also checks that `reference/` still matches
them.

### With and without the gateway

A setup built in the page ("시작하기" or "상세 설정") runs twice at once: the
scenario as set, and the same callers, requests, seed and servers with no
gateway (callers send to LiteLLM, which spreads requests at random and
neither limits nor orders them). The "비교" tab shows the two one above the
other on shared scales. The report waits for both and adds what the gateway
changed, how far peak demand is over the servers, and what to do about it:
replicas and GPUs to add with their cost, what to hold back in the busy
hours, and what moving teams' hours would do. Those proposals are computed
from request lengths and server speeds, not simulated.

GPU types (L40S to B200) carry a market price and an assumed speed against
an H100; hosted API prices are list prices. Both are in `GPUS` and `APIS`
in `web/monitor.html` with their sources. The guided setup sizes servers at
0.8 times peak demand by default. A screen smaller than 1180 by 620 shows
the whole page scaled down.

### The day as a bell, and two shifts

`rhythm.bell = [centre_s, sigma_s]` makes the busy hours a normal curve, so
groups whose days are shifted apart add up to a lower peak (a flat-topped
`plateau` does not). The page uses it by default (busiest at 13:30, sigma
three hours). The guided setup has an experiment step: without and with the
gateway, or before and after two shifts, where half of every team works
three hours earlier and half three hours later with the same requests a
day. The compare tab can switch between the two and change the gap.

### On-time answers, holding, charts

- Every class reports `on_time`: answers whose first token came within
  `metrics::ON_TIME_S` (10 s). Late answers count as answered but not as on
  time, which is what makes a run that refuses comparable with one that
  lets callers wait.
- `hold_weight` on an upstream models the live gateway's `hold_batch`:
  requests of that weight or lower are held beside the queue instead of
  refused, and with `strict_priority` are sent on only when nothing heavier
  waits. `hold_max` (10,000) and `hold_timeout_s` (900) bound it as in the
  gateway. Two differences remain: the gateway holds by the class "batch"
  where the simulator goes by weight, and the gateway's limit in bytes
  (`hold_max_bytes`) is not modelled. The page's default policy holds the
  lightest kind of request. Nodes report `held`.
- The page draws its line charts with uPlot 1.6.32 (MIT), inlined in
  `web/monitor.html` so the page stays one file.

### Waiting tickets

The live gateway tells a waiting caller how many go before it and about how
long that takes (`queue_notice`, `GET /gateway/queue/{ticket}`). The
simulator keeps the same estimate for every request that joins a gateway
queue and reports, per class, how it turned out: `tickets` (requests whose
first wait lasted 250 ms or more, the point of the gateway's first notice),
`ticket_told_p50` and `ticket_wait_p50` (the wait told and the wait had, of
those that got a seat), `ticket_off_p50` (the median gap) and `ticket_late`
(share that waited over twice the told wait and 2 s longer). Records carry
`expected_ms`, `waited_ms` and `seated`. A client log with `expected_ms`
gives the same numbers, with `queue_ms` of a request tried once as the wait.

Only the estimate at the moment of joining is kept; the gateway repeats its
notice every 2 s with a new one. Against the measured saturation runs
(`reference/ticket/RESULTS.md`) the interactive ticket agrees (off by about
half a second, measured and simulated); what batch work is told does not:
the measured gateway told it two to four times the wait it had, the
simulator about 1.2 times. Both assume the present pace lasts, and in those
runs the flood ended while batch work waited.

### Speed under load

What one caller sees while an answer is written is not what the servers
write in total. `reference/speed-under-load` holds two runs on a small
local model (`batch.py`, `waiting.py`, `results.json`):

- Decoded together, 1 to 64 sequences: each one's speed fell from 44 to
  4 tokens a second while the total rose from 44 to 260 and all but
  stopped after 32. `each = solo / (1 + (n - 1) / 6.36)` fits every row
  within 7%; a model server takes that shape with `batch_half = 6.36`.
- Callers waiting outside a server that runs one request at a time, 1 to
  48 of them: the total stayed at 23 to 24 tokens a second. With callers
  that give up after 8 s it fell to 4.8, because the server goes on
  writing for callers that have left.

In the engine: `batch_knee` (the total stops growing there and every
further request slows all of them), `overload_decay` (the total falls past
the knee; 0 by default, since neither run nor the published sweeps show
it), `batch_half` (the measured shape). A server's capacity, for load and
for routing, is its knee. Every class reports `gen_tps_p50` in frames and
`gen_tps_p50` / `gen_tps_p5` in the report: output tokens a second from
the first token to the last, as the caller saw them. Tokens a second per
class count tokens as they arrive.

Two losses under load, both off unless set:

- `prefill_chunk_tokens`: while any prompt is being read, every answer on
  the server waits chunk / `prefill_tokens_per_s` for each of its tokens,
  and the prompt is read at its own speed. Measured (`stall.py`): with 8
  answers being written, a 512-token chunk a step cut each from 24 to 0.4
  tokens a second and a 64-token chunk to 2.3; the step took the step alone
  plus the time to read the chunk. Sarathi-Serve (arXiv 2403.02310) reports
  the same on GPUs: stalls of seconds in vLLM, and one decoded token
  costing about what 128 prompt tokens cost.
- `context_tokens`: a request starts only when its prompt fits; when the
  answers outgrow the room, the request that started last goes back to
  the head of the queue and is read again with what it had written
  (vLLM's preemption with recompute). The report counts `preempted`.
  Mechanism from vLLM's documentation; no measurement of ours.

The page sets a chunk of 128 tokens (at its prompt speed that is a stall
of about three steps, roughly a 512-token chunk on a GPU by the paper's
conversion; assumed) and room for twice the context of the best batch
(assumed), and counts the stall when it sizes servers. In the default run (one seed; without the
gateway 39.5% on time and 17.5 tokens a second for an ide caller) a
gateway sending a server 1x its best batch gave 52.7% and 33.3, 1.5x
52.6% and 22.2, 2x 55.3% and 17.0, 3x 40.0% and 17.0: the page's default
is 1x. `reference/speed-under-load/sweep.md` has the model against rising
load.

On the page a server lets in four times its best batch (`srvAdmit`, an
assumption; vLLM's default limit is 256), so without the gateway answers
start sooner and are written slowly, and with it they wait in the
gateway's queue and are written at full speed. Assumed, not measured: the
factor of four. Past the last point of a model's sweep the curve follows
the public sweeps that go further (see "Real models").
The numbers in this section up to here were run with the page's earlier
round-number models ("8B class at 90 tokens a second") and are not what the
page shows now; see the next section.

### Real models

The setup page's model list is 20 open models that are downloaded most in
November 2026, in their latest versions, with numbers that are published
measurements where there are any (`reference/models`, and its RESULTS.md for
every table):

- **Memory.** The vLLM project's stated minimum for the checkpoint, else the
  published weight size times 1.19 (the median of 21 recipes is 1.20). A
  replica takes as many GPUs as hold that at 95% full, or as many as the
  sweep it is taken from ran on.
- **Speed under concurrency.** A model server takes a measured sweep as
  `batch_curve = [[n, share of the solo speed], ...]` and follows it point
  by point. The sweeps are InferenceX's public rows (SemiAnalysis; H100,
  H200 and B200, vLLM or SGLang): 1,024-token prompts and answers where
  they exist, replays of coding-agent traces for the newest models (there n
  counts users). One model has a single write-up's sweep.
- **A model nobody has swept** gets an estimate: milliseconds per token =
  0.08 x layers (0.10 for a sparse model) + GB of weights one token touches
  / (GPUs x memory bandwidth in TB/s). Against 22 published single-caller
  speeds it is off by 6% at the median for dense models (worst 11%) and 15%
  for sparse ones (worst 61%, DeepSeek V4 Pro). Its curve is borrowed from
  Llama 3.3 70B or gpt-oss 120B.
- **Requests a server is planned for** (`batch_knee`, the page's "적정 동시
  처리"): the most that still leave each one 30 tokens a second. Most
  sweeps stop at 64 together, often with each caller still well over that,
  so the curve is carried on: each doubling past the last measured point
  takes the median slope of the 335 single-turn sweeps at that count (the
  total grows 32% to 128, 23% to 256, 9% to 512), never gentler than the
  sweep's own last segment. The planned count goes no further than four
  times the last measured one (not past it at all for coding-agent sweeps),
  and the page's list says how far each was measured.
- **Why the gateway side can show less server output.** A server puts out
  more in total the more it serves at once, and each caller gets less. The
  gateway sends a server its planned count and queues or refuses the rest;
  without it the server takes four times that. So at the busiest time the
  side without the gateway has the higher total and the slower answers
  (default run: 13,453 against 12,585 tokens a second at the highest, 20.6
  against 34.1 a second for one IDE caller). Before the curve was carried
  on, the gateway's limit sat at the end of the sweep (64 for gpt-oss
  120B, where each caller still had 67 a second), which made the gap wider
  than a real setup would: 13,367 against 11,467, and 291 refused against
  35 now.
- **What the comparison should be read on.** A server without the gateway
  starts every answer at once and writes all of them slowly, so "first
  token within 10 s" flatters it. The table also gives the time to the
  whole answer and the share of server time spent on callers who had left.
  Default setup, one seed, without / with the gateway:

  | load | whole answer p50, IDE | whole answer p50, chat | first token within 10 s | answered | server time on callers who left |
  |---|---:|---:|---:|---:|---:|
  | 1x | 22.3 / 18.5 s | 6.8 / 6.4 s | 85.9 / 82.7% | 100 / 99.5% | 0.0 / 0.1% |
  | 1.5x | 52.3 / 24.9 s | 36.4 / 11.2 s | 36.1 / 51.8% | 86.1 / 85.8% | 10.1 / 0.3% |
  | 2x | 50.7 / 29.2 s | 35.8 / 12.4 s | 23.1 / 40.8% | 59.0 / 74.5% | 17.8 / 0.4% |

  At the load the setup was sized for there is little for a gateway to
  win; past it the whole answer comes in half the time or less. Whole-answer
  times count answered requests only. The table above is without resends.
- **Situations (the guide's step 7).** Three things a real deployment has
  and the comparison did not:
  - *Callers send again after a timeout* (`timeout_retries` on a client;
    the page's "시간 초과 후 다시 보내기"). Client SDKs do this twice by
    default (public documentation); that a person waits that long is
    assumed. The guide turns it on for every situation. A refusal is
    retried the same number of times.
  - *A heavy batch job nobody planned for* lands on the large model after
    the servers were sized: 4,000 tokens in, 800 out per request. The size
    is assumed.
  - *Long agent runs*: the gateway clears old tool results and sends 35%
    less prompt for IDE requests; without the gateway the whole prompt
    goes. 35% is what the real gateway did against a local server
    (`gateway/LEAD.md`: 79,650 to 52,119 prompt tokens over 40-turn runs);
    that IDE traffic looks like those runs is assumed, and the re-read a
    cleared prompt costs the server's prefix cache is not modelled.

  Default setup with resends, one seed, without / with the gateway:

  | situation, load | answered | whole answer p50, IDE | whole answer p50, chat | server time on callers who left | sent again |
  |---|---:|---:|---:|---:|---:|
  | plain, 1x | 100 / 99.6% | 22.2 / 18.8 s | 6.8 / 6.4 s | 0.0 / 0.0% | 1 / 45 |
  | plain, 1.5x | 56.4 / 85.2% | 53.8 / 25.2 s | 36.5 / 11.7 s | 24.1 / 0.2% | 11,020 / 267 |
  | plain, 2x | 25.3 / 73.1% | 39.7 / 26.8 s | 35.6 / 12.4 s | 41.2 / 0.5% | 32,294 / 552 |
  | heavy batch, 1x | 100 / 99.6% | 21.8 / 18.6 s | 28.3 / 9.0 s | 0.0 / 0.1% | 2 / 41 |
  | heavy batch, 1.5x | 37.3 / 85.0% | 55.4 / 25.5 s | 34.6 / 11.5 s | 30.2 / 0.2% | 18,759 / 330 |
  | agent runs, 1x | 100 / 99.8% | 22.2 / 17.8 s | 6.8 / 6.5 s | 0.0 / 0.0% | 1 / 27 |

  With agent runs one answer costs 4,425 tokens (in and out) without the
  gateway and 3,287 with (-26%); the IDE prompt goes from 5,700 to 3,705.
  Halving token use needs more than this one mechanism at this share of
  traffic. Without the gateway, callers that send again are what breaks
  an overloaded server: at 2x a quarter of the requests are answered and
  two fifths of the server's time goes to callers who have left.
- **Tokens written, running total.** The comparison has a chart of the
  output tokens written so far and how many of them are in answers that
  were finished (`tokens_ok` on a class frame), and the totals in the
  table. Plain situation with resends, millions of tokens, without / with:
  written 6.94 / 6.85 at 1x, 11.20 / 8.53 at 1.5x, 7.16 / 9.61 at 2x; in
  finished answers 6.93 / 6.84, 7.72 / 8.48, 5.85 / 9.51. Without the
  gateway an overloaded server writes more and delivers less: 31% of what
  it wrote at 1.5x was for answers nobody waited out.
- **Ours.** Ten 4-bit files measured on this project's 4-core box
  (`measure_local.py`): memory held, reading and writing speed, and the
  memory one token of context costs against the figure computed from the
  model's structure.

Nothing in the list was run by us on a GPU, and the sweeps differ in
engine, precision and speculative decoding, so they are not a ranking.
`python3 reference/models/build_catalog.py --page` rebuilds the list in
`web/monitor.html` from the JSON files beside it.

The compare tab and the report also show output tokens a second for each
environment (live, over the day, average and peak). The guided setup has a
queue length step for requests people wait for: 0.25, 2 or 8 times what the
servers run at once, or no limit (`max_queue` left out, nothing refused).
Held requests (`hold_weight`) are outside that limit either way.
