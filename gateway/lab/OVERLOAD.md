# Overload: who gets served when there is more traffic than the servers can answer

Two policies that held up in the simulator (`gateway/sim/reference/prior-art`),
now options of the real gateway, measured on local models (2026-11-13). Three
llama.cpp servers with one request at a time each, together about 0.9
requests per second; every prompt different (caches play no part); open-loop
arrivals for 180 s; callers give up after their own timeout and do not
retry. One run per row. Logs: one folder per run under
`gateway-experiments/` with the names below.

## Refusing in stages (`shed_from`, `shed_ceiling`, `shed_floor`)

Three agents: ide (interactive, weight 4, gives up after 15 s), chat (normal,
weight 2, 30 s), batch (batch, weight 1, 60 s), sending 30 / 30 / 40% of the
traffic. `max_queue = 30`. "waited in vain" = requests that waited until
their timeout and got nothing.

| 1.5x capacity | ide answered | chat answered | batch answered | batch waited in vain | ide TTFT p50 / p95 |
|---|---|---|---|---|---|
| refuse when full (`over-refuse-x15`) | 91% | 91% | 61% | 25 of 85 | 3.7 / 7.6 s |
| evict when full (`over-evict-x15`) | 100% | 95% | 61% | 21 | 3.7 / 8.0 s |
| staged, no floor (`over-shed-x15`) | 100% | 89% | 52% | 0 | 3.6 / 7.9 s |
| staged, floor 0.1 (`over-shed-floor-x15`) | 100% | 95% | 52% | 0 | 3.9 / 9.3 s |

| 2x capacity | ide answered | chat answered | batch answered | batch waited in vain | chat waited in vain | ide TTFT p50 / p95 |
|---|---|---|---|---|---|---|
| refuse when full (`over-refuse-x20`) | 72% | 74% | 38% | 31 of 122 | 6 of 99 | 3.0 / 5.6 s |
| evict when full (`over-evict-x20`) | 100% | 35% | 34% | 6 | 64 | 6.7 / 11.8 s |
| staged, no floor (`over-shed-x20`) | 100% | 34% | 26% | 0 | 51 | 7.2 / 11.8 s |
| staged, floor 0.1 (`over-shed-floor-x20`) | 100% | 40% | 29% | 0 | 48 | 5.0 / 10.1 s |

What the runs show:

- Refusing whoever arrives at a full queue costs the ide callers 9% of
  their requests at 1.5x and 28% at 2x. Eviction and staged refusal both
  bring that to zero.
- Against eviction, staged refusal changes what happens to batch: it is told
  no at once (41 and 90 immediate refusals) instead of waiting a minute for
  nothing (21 and 6 timeouts, plus evictions after a wait). It answers
  somewhat fewer batch requests (52% against 61% at 1.5x). It did not make
  the ide callers faster here; the simulator's TTFT gain did not show.
- Batch was not starved at 2x (26% answered without a floor, 29% with).
  The floor's effect is within what one run can show; it is a guarantee for
  heavier overload, not a measured gain.
- Not solved by either: at 2x the middle class (chat) mostly waits 30 s and
  gives up (48 to 64 of 99), because the queue is longer than its patience.
  That is what `admit_by_wait` is for; it was off in these runs.

## The middle class at 2x: adding wait-based admission (2026-11-14)

In the runs above chat callers at 2x mostly waited out their 30 s and left.
`admit_by_wait` refuses at the door a request whose expected wait is longer
than it can wait. It needs to know how long that is: from the request
(`x-gateway-max-wait-ms`), the agent's `max_wait_ms`, or the upstream's
`queue_timeout_secs`. Same traffic as above, build of main after #10;
"stated" = every caller sends its timeout less 4 s (the time an answer
takes). "waited in vain" = gave up at its timeout, or was refused after
more than 2 s in the queue.

| 2x capacity | ide answered | chat answered | chat waited in vain | batch answered | batch waited in vain |
|---|---|---|---|---|---|
| staged + floor, seed 1 / 2 (`w-shed-x20`, `-s2`) | 100% / 100% | 63% / 64% | 26 / 17 of ~100 | 32% / 27% | 0 / 6 |
| + admit by wait, patience stated, seed 1 / 2 (`w-shed-wait-say-x20`, `-s2`) | 100% / 99% | 77% / 72% | 15 / 9 | 34% / 30% | 0 / 5 |
| + admit by wait, one 30 s limit, nothing stated (`w-shed-wait30-x20`) | 100% | 59% | 33 | 33% | 3 |
| evict + admit by wait, patience stated (`w-evict-wait-say-x20`) | 100% | 77% | 20 | 40% | 31 |

| 1.5x capacity | ide answered | chat answered | batch answered |
|---|---|---|---|
| staged + floor (`w-shed-x15`) | 100% | 100% | 59% |
| + admit by wait, patience stated (`w-shed-wait-say-x15`) | 100% | 100% | 66% |

- With patience stated, chat answers rose by about 10 points in both seeds
  (63 to 77%, 64 to 72%) and nobody waited out a client timeout any more.
  Batch did not lose (32 to 34%, 27 to 30%), ide stayed at 99 to 100%.
- It does not remove waiting for nothing: 9 to 15 chat requests were let
  in, waited to their limit and were then refused. The wait estimate at the
  door is too optimistic for the middle class under this much load.
- One limit for everyone does nothing here (59%): a 30 s limit is exactly
  what chat can wait, so the door lets in what will just miss. The limit has
  to be the caller's own, with room for the answer itself.
- Eviction with the same admission answers as much chat and more batch
  (40%), but 31 batch and 20 chat requests were pushed out after waiting.
- The baseline itself moved between days (chat 40% on 2026-11-13, 63% and
  64% today, same settings, different build and machine state), so compare
  rows within one table only.

Recommended for an upstream with mixed interactive and batch callers:

```toml
max_queue = 30            # about half a minute of work for the servers behind it
shed_from = 0.25
shed_ceiling = 0.75
shed_floor = 0.1
admit_by_wait = true

[[agents]]                # and per agent, unless callers send x-gateway-max-wait-ms
id = "chat"
max_wait_ms = 26000       # the caller's timeout less the time an answer takes
```

## Holding batch work through a busy time (`hold_batch`, 2026-11-16)

A busy time followed by a quiet one: 180 s at 1.5x capacity, then 180 s at
0.3x, same three agents and shares. Batch callers wait up to 300 s; ide and
chat state their patience as above. Both rows use the recommended settings
above; the second adds `hold_batch = true` and `hold_timeout_secs = 300`
and nothing else. Two seeds, shown as seed 1 / seed 2.

| | batch answered | batch refused | batch wait p50 / longest | chat answered | chat TTFT p50 / p95 | ide answered | ide TTFT p50 / p95 |
|---|---|---|---|---|---|---|---|
| not held (`over-nohold-peak`, `-s2`) | 57% / 54% | 46 of 107 / 54 of 117 | 16 / 30 s, 11 / 30 s | 93% / 93% | 14.7 / 24.1 s, 15.8 / 26.6 s | 99% / 100% | 4.0 / 7.7 s, 3.3 / 8.2 s |
| held (`over-hold-peak`, `-s2`) | 100% / 100% | 0 / 0 | 100 / 138 s, 111 / 149 s | 100% / 100% | 4.6 / 16.1 s, 5.7 / 18.4 s | 100% / 100% | 2.6 / 4.3 s, 2.5 / 4.8 s |

- Every batch request was answered instead of a little over half. The price
  is the wait: about 100 s at the median and up to 150 s, against at most
  30 s (the queue's limit) when not held. Nothing reached the 300 s limit.
- Chat and ide got faster as well, which was not the aim: chat's median
  time to first token fell from about 15 s to about 5 s and its 6 refusals
  per run went away. Without holding, batch takes its turn in the queue
  during the busy time; held, it stays out of the way until the servers
  have nothing else to do.
- The servers answered more in the same time (0.62 to 0.77 requests per
  second over the run in seed 1) because the quiet half was used.
- This shape is kind to holding: the quiet time is long enough to clear
  everything held. If the busy time outlasts `hold_timeout_secs`, or batch
  callers' own timeouts are shorter than the wait, held requests end in a
  503 after a long wait instead of an immediate one. Not measured here:
  `hold_max` being reached, thousands of held connections, a busy time
  with no quiet time after it.

```sh
python3 lab/experiment.py run over-hold-peak --kind overload --config lab/gateway.over-hold.toml \
  --drive "$PY lab/overload_drive.py --out {dir}/requests.jsonl --scenario classes --rate 1.35 --seconds 360 \
           --quiet-after 180 --quiet-rate 0.27 --batch-timeout 300 --say-patience" \
  --analyze "$PY lab/overload_drive.py --analyze {dir}"
```

## Least used first (`least_used_half_life_secs`)

Eight agents of equal weight at 1.5x capacity; two of them send ten times
what each of the other six sends. 45 s timeout, `max_queue = 200`.

| | light callers TTFT p50 / p95 | light answered | heavy answered | heavy TTFT p50 |
|---|---|---|---|---|
| taking turns (`users-turns`) | 3.9 / 6.3 s | 100% | 78% | 34 s |
| least used, half-life 60 s (`users-least60`) | 2.5 / 4.0 s | 100% | 71% | 36 s |
| least used, half-life 10 s (`users-least10`) | 2.6 / 4.0 s | 100% | 70% | 36 s |

The light callers wait about a third less. The gain is smaller than in the
simulator (13 s to 5 s there) because the real gateway's round robin already
gives every waiting agent a turn, so light callers were not badly off to
begin with. The heavy callers lose some answers (78% to 71%; total throughput
0.86 to 0.81 requests per second, which may be noise). The two half-lives
did not differ in this run; the simulator's "10 s is too short" did not
reproduce, with only 49 light requests to judge by.

## Limits

One run per row, 180 s, a 0.5B model on shared CPU cores, no retries, no
caches, three priority levels and one traffic mix. Both options are off by
default.

Reproduce: `lab/prefix_stack.sh up`, then

```sh
python3 lab/experiment.py run over-shed-floor-x15 --kind overload --config lab/gateway.over-shed-floor.toml \
  --drive "$PY lab/overload_drive.py --out {dir}/requests.jsonl --scenario classes --rate 1.35 --seconds 180" \
  --analyze "$PY lab/overload_drive.py --analyze {dir}"
```
