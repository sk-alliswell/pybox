# Routing by prompt prefix: what it does on real model servers

2026-11-13. Three llama.cpp servers (llama-cpp-python, qwen2.5-0.5b q4_k_m, one
thread each, one request at a time, one cache slot: each keeps the KV state
of the last prompt it read) behind one gateway upstream, on a 4-vCPU machine.
`prefix_drive.py` sends 0.3 requests/s for 300 s (99 requests). Each request
belongs to one of G groups; a group has its own system prompt of about 615
tokens and a short question after it. The arrival schedule is seeded, so the
three arms get the same requests at the same times, and every run starts
with cold caches. One run per cell.

Arms: `affinity = "off"` (load only), `"prefix"`, and `"prefix"` with
`affinity_wait_ms = 3000`.

| groups | arm | prompt tokens reused (server logs) | gateway's own estimate | time to first token: mean / p50 / p95 | total time, mean | queue wait, mean |
|---|---|---|---|---|---|---|
| 3 | off | 28% | - | 7.54 / 6.80 / 17.47 s | 9.17 s | 2.32 s |
| 3 | prefix | 56% | 56% | 3.45 / 0.43 / 11.53 s | 4.94 s | 0.67 s |
| 3 | prefix + wait 3 s | 94% | 95% | 0.57 / 0.20 / 2.57 s | 2.02 s | 0.18 s |
| 6 | off | 15% | - | 8.60 / 8.11 / 16.65 s | 10.09 s | 2.53 s |
| 6 | prefix | 21% | 20% | 6.25 / 6.27 / 14.23 s | 7.63 s | 1.22 s |
| 6 | prefix + wait 3 s | 34% | 33% | 6.64 / 5.91 / 18.19 s | 8.16 s | 2.00 s |

"Reused" is what the servers print themselves (`N prefix-match hit, remaining
M prompt tokens to eval`), summed over the run. The gateway's estimate is the
share of prompt tokens it believed the chosen server held when it routed
(`x-gateway-prefix-warm`); it never sees the servers' logs.

What this shows:

- With as many kinds of prompt as cache slots (3 groups, 3 slots), sending
  the same start to the same server doubles the reuse and halves the mean
  time to first token; letting a request wait up to 3 s for its server
  brings reuse to 94% and the mean time to first token from 7.5 s to 0.6 s.
  Without the wait a request that finds its server busy goes to a free one,
  reads the whole prompt there and overwrites that server's slot, which is
  why plain prefix routing stops at 56%.
- With twice as many kinds of prompt as slots (6 groups), there is little
  to keep: reuse goes from 15% to 21% (34% with the wait), mean time to
  first token improves by about a quarter, and the wait makes the tail
  worse (p95 18.2 s against 14.2 s). The policy cannot create cache the
  servers do not have room for.
- The gateway's picture of the caches matched the servers' own numbers
  within one point in all four runs, so `prefix_routing` in
  `/gateway/stats` can be read as the reuse rate on servers of this kind.

Limits: one run per cell (no noise bars); a tiny CPU model where reading a
600-token prompt (about 5 s with three servers sharing the machine) costs
far more than answering, which flatters the wait; single-slot servers; no
GPU server, no LiteLLM in between, one prompt shape. A server that keeps
many prefixes (shared cached blocks) should behave like the 3-group rows for
more groups, with `prefix_slots` set to match; that is not measured.

Reproduce:

```sh
export LAB=~/lab; PY=$LAB/venv/bin/python
for p in 9211 9212 9213; do $PY -m llama_cpp.server --model $LAB/models/qwen2.5-0.5b-instruct-q4_k_m.gguf \
  --model_alias qwen-0.5b --port $p --n_ctx 4096 --n_threads 1 --interrupt_requests false >$LAB/logs/kv-$p.log 2>&1 & done
python3 lab/experiment.py run prefix-on-g3 --kind prefix --config lab/gateway.prefix-on.toml \
  --drive "$PY lab/prefix_drive.py --out {dir}/requests.jsonl --groups 3 --rate 0.3 --seconds 300 --salt prefix-on-g3" \
  --analyze "$PY lab/analyze_prefix.py {dir}"
```

## Second round (2026-11-13): spread, wait values, busier load, more slots, LiteLLM, and the five follow-up options

Same machine and model. Every row is one 300 s run (about 90 to 150
requests); "reuse" is the share of prompt tokens the servers did not have to
read, from their own logs. Logs: one folder per run under
`gateway-experiments/` (names as in the tables). Each run uses its own
prompt texts (`--salt`), so two rows differ by content as well as by policy.

### How much one run can be trusted

| 3 prompt kinds, 3 single-slot servers | reuse | TTFT mean | TTFT p95 |
|---|---|---|---|
| load only, seed 1 / seed 2 | 28% / 35% | 7.5 / 4.9 s | 17.5 / 10.5 s |
| prefix, seed 1 / seed 2 | 56% / 80% | 3.5 / 1.0 s | 11.5 / 5.7 s |
| prefix + wait 1 s | 80% | 1.3 s | 7.1 s |
| prefix + wait 3 s, seed 1 / seed 2 | 94% / 92% | 0.6 / 0.5 s | 2.6 / 3.8 s |
| prefix + wait 6 s | 88% | 0.9 s | 5.2 s |

| 6 prompt kinds, 3 single-slot servers | reuse | TTFT mean | TTFT p95 |
|---|---|---|---|
| load only, seed 1 / seed 2 | 15% / 27% | 8.6 / 4.9 s | 16.6 / 9.0 s |
| prefix, seed 1 / seed 2 | 21% / 44% | 6.2 / 3.3 s | 14.2 / 10.2 s |
| prefix + wait 1 s | 29% | 5.0 s | 13.1 s |
| prefix + wait 3 s, seed 1 / seed 2 | 34% / 45% | 6.6 / 3.6 s | 18.2 / 10.9 s |
| prefix + wait 6 s | 41% | 4.2 s | 12.5 s |

The order of the policies held in every pair, but the size of the gain moves
a lot with the arrival pattern: prefix routing alone gave 56% or 80% reuse on
the same setup. Differences under about 10 points of reuse or 1 s of mean
TTFT between single runs are inside that spread. With that in mind: the wait
is clearly worth it when the kinds fit the servers; the exact value between
1 and 6 s is not distinguishable here; with more kinds than slots the wait
adds little and does not reliably hurt.

### Closer to full (3 kinds, 0.45 req/s, 150 requests)

| | reuse | TTFT mean | TTFT p95 | queue mean |
|---|---|---|---|---|
| load only | 34% | 11.8 s | 21.5 s | 7.2 s |
| prefix | 40% | 14.6 s | 37.4 s | 10.2 s |
| prefix + wait 3 s | 86% | 2.4 s | 14.7 s | 1.4 s |

When servers are rarely free, "the free server that holds most" has almost
no choice left, so prefix routing without the wait gains nothing (and this
run was worse). With the wait the load stops being heavy at all, because a
reused prompt costs a fraction of the server time. Near saturation the wait
is the part that matters.

### Mixed ide / chat / batch prompts

On single-slot servers this load was far over capacity (mean queue 14 to
16 s) and no policy made a difference: reuse 8% load only, 16% prefix, 18%
with wait, TTFT mean 22.7 / 24.6 / 24.0 s. On servers with 8 cache slots
each (`slot_server.py`) the same load is comfortable, and load-only routing
already reuses as much as prefix routing:

| 8 slots per server | reuse | TTFT mean | TTFT p95 |
|---|---|---|---|
| load only | 61% | 5.4 s | 13.6 s |
| prefix | 59% | 7.0 s | 17.8 s |
| prefix + wait 3 s | 65% | 6.1 s | 17.0 s |

So when every server has room for every kind of prompt, where a request goes
matters little; cache room on the servers was worth far more than routing
(TTFT 22 s to 5 s). Routing matters in between: more kinds than one server
holds, fewer than all servers hold together.

(`llama_cpp.server --cache true` was meant for this row. It ran out of
memory twice on this load, one process at 13 GB, so the multi-slot server
stands in.)

### Through LiteLLM (each backend is a LiteLLM proxy in front of one server)

| 3 kinds | reuse | TTFT mean | TTFT p95 |
|---|---|---|---|
| load only | 28% | 5.6 s | 13.8 s |
| prefix | 70% | 1.9 s | 8.0 s |

Same picture as direct (28% and 56 to 80%). The gateway's estimate matched
the servers (69.8% against 69.5%). This holds when each LiteLLM instance
fronts one server; a LiteLLM that itself spreads over several servers hides
the cache from the gateway, and that is not measured.

### The five follow-up options

**Session admission (`slot_hold_ms`)**, 6 kinds on 3 single-slot servers:

| | reuse | TTFT mean | TTFT p95 |
|---|---|---|---|
| prefix (baseline), seed 1 / seed 2 | 21% / 44% | 6.2 / 3.3 s | 14.2 / 10.2 s |
| hold 5 s, max wait 10 s, seed 1 / seed 2 | 30% / 42% | 5.3 / 4.1 s | 14.2 / 11.8 s |
| hold 15 s, max wait 20 s, seed 1 | 35% | 20.4 s | 30.4 s |

Not shown to help. One seed is a little better, the other a little worse,
both inside the spread; a long hold raises reuse and wrecks waiting time
(queue 16 s). On the mixed load it changed nothing (16% reuse either way).
The prompts here are independent requests of a kind, not sessions with
think time between turns, which is the case the idea is for; that case is
not measured. Stays off. (Measured in the third round below, which
also found a fault that made these runs meaningless for the option.)

**Batch grouping (`batch_group_ms`)**, 60 batch requests of 6 kinds queued at
once in shuffled order:

| | reuse | all done after | TTFT mean |
|---|---|---|---|
| load only | 11% | 159 s | 83.6 s |
| prefix | 8% | 162 s | 82.8 s |
| prefix + batch grouping | 83% | 50 s | 25.2 s |

The clearest result of the round: the job finishes in a third of the time.
It is the favourable case (everything queued up front, one run); a trickle
of batch work gains less.

**Cost routing (`route_by = "cost"`)**, servers taking two requests at a time:

| | reuse (server) | TTFT mean | TTFT p95 |
|---|---|---|---|
| 6 kinds: load rule / cost rule | 30% / 35% | 4.4 / 3.9 s | 11.6 / 9.8 s |
| mixed: load rule / cost rule | 13% / 15% | 21.5 / 20.0 s | 38.8 / 38.2 s |

Cost was a little better in both pairs, by less than the spread between
seeds. One run each. Not enough to change the default. In these runs the
gateway's estimate was far above the servers' numbers (63 to 81% against 30
to 35%): the servers are single-slot but were given two requests at a time,
and the second evicts the first. Give a server no more concurrent requests
than it has slots, or the picture is wrong.

**Cache picture correction**, 6 kinds on 3 servers with 2 slots each that
report cached tokens:

| gateway told | reuse (server) | gateway's estimate | TTFT mean |
|---|---|---|---|
| load only | 33% | | 6.4 s |
| 2 slots (right) | 64% | 64% | 3.4 s |
| 8 slots (wrong), with correction | 53% | 65% | 4.3 s |
| 8 slots (wrong), before correction | 40% | 70% | 5.5 s |

With the wrong slot count the old build believed in caches that were gone
(estimate 70%, real 40%). The correction recovers about half of the loss
within a 5-minute run; the estimate still runs ahead of the servers while it
learns. Setting `prefix_slots` right remains better than relying on it.

**Refusal reasons and stale servers** are covered by tests only
(`a_refusal_says_whether_retrying_can_help`); there is nothing to measure.

### What this round does not show

One run per cell except the seed pairs; a 0.5B model on shared CPU cores
where reading the prompt dominates (a GPU server reads prompts much faster
relative to writing, so gains in seconds will be smaller there); synthetic
prompts; no stock `llama-server` or vLLM (installing one was not possible in
this environment, `slot_server.py` uses the same llama.cpp library); no
multi-turn sessions with think time; no LiteLLM that balances over several
servers itself.

## Third round (2026-11-14): conversations with think time

What the earlier rounds could not show: multi-turn conversations, where a
caller sends its next turn (the whole conversation so far plus a new
question) some time after the last answer. `prefix_drive.py --sessions N
--think 8`: N conversations over a 400-word context each, next turn about
8 s after the answer (closed loop, so a faster gateway gets more turns
done), 300 s, three single-slot llama.cpp servers restarted before every
run. All conversations come from one agent, which is the common case (one
key, many chats). One run per row.

| conversations | policy | turns done | reuse | TTFT mean | TTFT p50 | TTFT p95 |
|---|---|---|---|---|---|---|
| 3 (one per server) | load only (`sess3-off`) | 57 | 33% | 4.5 s | 3.9 s | 13.2 s |
| | prefix (`sess3-on`) | 84 | 93% | 0.7 s | 0.4 s | 2.7 s |
| | prefix + wait 3 s (`sess3-wait`) | 85 | 94% | 0.6 s | 0.4 s | 1.8 s |
| 6 (two per server) | load only (`sess6-off`) | 87 | 7% | 11.0 s | 10.4 s | 22.8 s |
| | prefix (`sess6-on-v2`) | 86 | 13% | 10.9 s | 10.6 s | 22.7 s |
| | prefix + wait 3 s (`sess6-wait-v2`) | 87 | 16% | 10.8 s | 8.9 s | 26.1 s |
| | prefix + session admission 15 s (`sess6-hold15-v2`) | 97 | 58% | 7.6 s | 0.8 s | 28.2 s |
| 9 (three per server) | load only (`sess9-off`) | 102 | 1% | 17.6 s | 17.8 s | 32.3 s |
| | prefix (`sess9-on-v2`) | 98 | 1% | 18.4 s | 18.4 s | 33.2 s |
| | prefix + session admission 15 s (`sess9-hold15-v2`) | 107 | 36% | 15.6 s | 22.4 s | 33.9 s |

Adding the 3 s wait to session admission changed nothing (58% and 34%
reuse, same times), so those rows are left out.

- **Conversations that fit the servers' slots:** prefix routing is the
  whole story. Reuse 33% to 93%, time to first token 4.5 s to 0.7 s, and
  47% more turns done in the same five minutes.
- **More conversations than slots:** routing alone does nothing, with or
  without the wait, because every turn finds its slot taken over by another
  conversation (reuse 13% with six, 1% with nine). This is where session
  admission (`slot_hold_ms`) earns its place: with six conversations reuse
  goes from 13% to 58%, half of all turns start within 0.8 s instead of
  10.6 s, and 13% more turns get done. The price is the tail: a
  conversation that is not on a server waits its turn (up to
  `slot_max_wait_ms`, here 20 s), so p95 goes from 22.7 s to 28.2 s.
- **Three conversations per slot:** still more work done (107 turns against
  98) and a lower mean, but now most turns are the waiting ones (p50 22 s
  against 18 s). Past about two conversations per slot the servers need
  more slots, not a better policy.
- **A fault found on the way.** In the first pass session admission made
  everything worse (reuse 5%, 66 turns). A held request at the head of a
  caller's queue also blocked that caller's later requests, the warm ones
  included, so one agent with several conversations stalled behind its own
  cold request. Fixed in this branch (a caller's warm request passes its
  own held request; `regrouped` in `/gateway/stats` counts it: 56 times in
  the six-conversation run). The earlier runs `adm-*` of the second round
  were taken with that fault and say nothing about session admission.

Suggested setting when conversations outnumber slots by up to about two to
one: `slot_hold_ms` a little above the usual pause between turns (15 s
here for 8 s pauses) and `slot_max_wait_ms` at what a caller can bear. Not
measured: other pause lengths, mixed pause lengths, several agents,
servers with several slots.
