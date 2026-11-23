# The simulator against the overload and prefix-cache measurements

2026-11-14. The runs of PRs #8 to #10 on the lab's three local servers,
replayed in the simulator request for request: same arrival times, same
agents, same gateway settings. Server speed is the only fitted part. The
measured logs stay outside the repository; `reference.json` here and in
`../prefix/` hold what the comparison needs from them.

```
python3 reference/overload/replay.py --markdown        # needs the built gwsim
python3 reference/prefix/replay.py --skip wait --markdown
```

## What was wrong and what changed

The simulator had predicted more from "least used first" and from gradual
refusal than the lab then measured. The cause was not those policies but
the order the simulator compared them with: its queue still took one
request per agent in turn, the scheduler before the unification. The
gateway on main orders agents by a token counter (prompt tokens over
weight, an agent back from idle lifted to the least-served waiting one).
Against that order, both policies have less left to win.

`src/sched.rs` now follows `gateway/src/scheduler.rs` on main in three
places: the token-counter order, staged refusal (by rank, an exact share
instead of a coin, with the floor that keeps the lowest rank a foothold),
and the least-used key (use so far plus requests in flight times the
typical slot time).

| over the 11 overload runs | before | after |
|---|---|---|
| answered, gave up, refused: mean error | 3.0 points | 1.4 points |
| the same, worst | 26.7 points | 9.7 points |
| time to first token p50, p95: mean error | 12.4% | 5.3% |
| the same, worst | 68.8% | 25.1% |

Light users' time to first token p50, turn order to least used first:
measured 3.91 s to 2.49 s, simulator now 4.20 s to 2.58 s, before 4.88 s to
2.63 s.

## Overload runs

Measured / predicted (mean of 10 seeds). Mean error over all runs: shares 1.4 points (worst 9.7), times 5.3% (worst 25.1%). Over the nine runs the server speed was not fitted on: shares 1.5 points, times 5.6%.

| run | group | answered | first token p50 (s) | p95 (s) | gave up | refused |
|---|---|---|---|---|---|---|
| over-refuse-x15 | batch | 61% / 63% | 40.7 / 38.9 | 57.2 / 58.0 | 29% / 29% | 9% / 9% |
| over-refuse-x15 | chat | 91% / 95% | 11.3 / 11.5 | 27.3 / 26.4 | 4% / 2% | 5% / 2% |
| over-refuse-x15 | ide | 91% / 93% | 3.7 / 3.5 | 7.6 / 8.0 | 0% / 0% | 9% / 7% |
| over-evict-x15 (fitted) | batch | 61% / 60% | 37.3 / 40.7 | 57.9 / 57.5 | 25% / 23% | 14% / 17% |
| over-evict-x15 (fitted) | chat | 95% / 92% | 15.0 / 15.0 | 28.1 / 27.2 | 5% / 8% | 0% / 0% |
| over-evict-x15 (fitted) | ide | 100% / 100% | 3.7 / 3.8 | 8.0 / 8.3 | 0% / 0% | 0% / 0% |
| over-shed-x15 | batch | 52% / 52% | 33.7 / 35.7 | 50.8 / 49.5 | 0% / 0% | 48% / 48% |
| over-shed-x15 | chat | 89% / 93% | 17.5 / 16.8 | 26.8 / 27.1 | 9% / 6% | 1% / 1% |
| over-shed-x15 | ide | 100% / 100% | 3.6 / 3.9 | 7.9 / 8.3 | 0% / 0% | 0% / 0% |
| over-shed-floor-x15 (fitted) | batch | 52% / 52% | 33.9 / 35.6 | 45.6 / 49.5 | 0% / 0% | 48% / 48% |
| over-shed-floor-x15 (fitted) | chat | 95% / 93% | 16.4 / 16.8 | 26.9 / 27.1 | 5% / 6% | 0% / 1% |
| over-shed-floor-x15 (fitted) | ide | 100% / 100% | 3.9 / 3.9 | 9.3 / 8.3 | 0% / 0% | 0% / 0% |
| over-refuse-x20 | batch | 38% / 43% | 41.3 / 42.1 | 58.5 / 58.1 | 25% / 23% | 37% / 34% |
| over-refuse-x20 | chat | 74% / 70% | 18.0 / 14.1 | 27.0 / 26.6 | 6% / 8% | 20% / 22% |
| over-refuse-x20 | ide | 72% / 69% | 3.0 / 3.3 | 5.6 / 6.6 | 0% / 0% | 28% / 31% |
| over-evict-x20 | batch | 34% / 34% | 35.8 / 36.9 | 53.1 / 54.7 | 5% / 6% | 61% / 60% |
| over-evict-x20 | chat | 35% / 42% | 24.0 / 23.8 | 28.0 / 28.1 | 65% / 58% | 0% / 0% |
| over-evict-x20 | ide | 100% / 100% | 6.7 / 5.1 | 11.7 / 10.4 | 0% / 0% | 0% / 0% |
| over-shed-x20 | batch | 26% / 29% | 27.7 / 27.9 | 49.5 / 46.7 | 0% / 0% | 74% / 71% |
| over-shed-x20 | chat | 34% / 44% | 23.3 / 23.6 | 28.4 / 28.1 | 52% / 43% | 14% / 13% |
| over-shed-x20 | ide | 100% / 100% | 7.2 / 5.4 | 11.8 / 10.5 | 0% / 0% | 0% / 0% |
| over-shed-floor-x20 | batch | 29% / 29% | 25.6 / 27.8 | 43.7 / 46.7 | 0% / 0% | 71% / 71% |
| over-shed-floor-x20 | chat | 40% / 44% | 22.5 / 23.7 | 28.5 / 28.1 | 48% / 43% | 11% / 13% |
| over-shed-floor-x20 | ide | 100% / 100% | 5.0 / 5.4 | 10.1 / 10.5 | 0% / 0% | 0% / 0% |
| users-turns | heavy | 78% / 73% | 34.4 / 34.6 | 42.6 / 42.9 | 22% / 27% | 0% / 0% |
| users-turns | light | 100% / 100% | 3.9 / 4.1 | 6.3 / 6.7 | 0% / 0% | 0% / 0% |
| users-least60 | heavy | 71% / 70% | 35.8 / 36.3 | 42.7 / 43.1 | 29% / 30% | 0% / 0% |
| users-least60 | light | 100% / 100% | 2.5 / 2.6 | 4.0 / 4.5 | 0% / 0% | 0% / 0% |
| users-least10 | heavy | 70% / 70% | 36.0 / 36.5 | 42.8 / 43.0 | 30% / 30% | 0% / 0% |
| users-least10 | light | 100% / 100% | 2.6 / 2.7 | 3.9 / 4.5 | 0% / 0% | 0% / 0% |

What still misses:

- At 2x load with eviction or gradual refusal, chat answered is 7 to 10
  points too high (42% for 35%, 44% for 34%). The cause is not the queue
  model: in those two runs the servers took 3.46 and 3.51 s a request, 8
  to 10% longer than the 3.20 s fitted on the reference runs. With each
  run's own speed (`replay.py --own-speed`) the simulator says 31% for 35%
  and 33% for 34%. A chat request there is granted after 22 to 27 s of
  the 30 s its caller waits, so every percent of server speed moves about
  a point of chat answers. The shared speed stays in the comparison above,
  because a forecast does not know the day's speed either; at such a load
  read the answer as a range.
- `over-refuse-x20` chat p50 is 22% too low.

## Prefix-cache routing (PR #8)

Twelve runs: routing by load (`off`), by prompt prefix (`on`) and by prefix
with a wait of up to 3 s for the server that holds it (`wait`), 3 and 6
prefix groups, two arrival schedules. Fitted on the four `on` runs: a cold
read of the shared prefix, a warm one, the time per generated token, and how
much the three servers slow each other down on the 4-vCPU box (a cold read
took 3.6 s alone, 5.5 s beside one busy server, 7.6 s beside two; writing
tokens slowed far less, about 1.1x, which is `gen_contention = 0.2`).

| run | prompt tokens not read again | first token mean (s) | p50 (s) | p95 (s) |
|---|---|---|---|---|
| prefix-off-g3 | 28% / 33% | 7.5 / 5.3 | 6.8 / 5.0 | 17.5 / 13.2 |
| prefix-on-g3 | 56% / 52% | 3.5 / 3.9 | 0.4 / 2.5 | 11.5 / 11.4 |
| prefix-wait-g3 | 94% / 87% | 0.6 / 1.3 | 0.2 / 0.4 | 2.6 / 7.6 |
| prefix-off-g6 | 15% / 15% | 8.6 / 7.7 | 8.1 / 7.4 | 16.6 / 16.0 |
| prefix-on-g6 | 21% / 20% | 6.2 / 7.4 | 6.3 / 7.1 | 14.2 / 15.7 |
| prefix-wait-g6 | 34% / 28% | 6.6 / 6.5 | 5.9 / 6.5 | 18.2 / 15.1 |
| prefix-off-g3-s2 | 34% / 38% | 4.9 / 4.4 | 5.1 / 4.0 | 10.5 / 10.6 |
| prefix-on-g3-s2 | 80% / 71% | 1.0 / 2.2 | 0.2 / 0.4 | 5.7 / 8.5 |
| prefix-wait-g3-s2 | 92% / 92% | 0.5 / 0.8 | 0.2 / 0.3 | 3.8 / 3.1 |
| prefix-off-g6-s2 | 27% / 20% | 4.9 / 6.3 | 5.4 / 5.7 | 9.0 / 13.9 |
| prefix-on-g6-s2 | 44% / 31% | 3.3 / 5.4 | 3.1 / 5.3 | 10.2 / 12.7 |
| prefix-wait-g6-s2 | 45% / 45% | 3.6 / 4.1 | 3.3 / 3.5 | 10.9 / 10.8 |

Measured / predicted (mean of 10 seeds). `python3 reference/prefix/replay.py`.

What changed since the first replay (2026-11-14), when reuse with routing on
was predicted at 92% for a measured 56%:

- The simulator's router was not the gateway's. It scored every server,
  busy or not, with the server's true cache, and a request sent to a busy
  warm server queued there. The gateway only chooses among servers with a
  free seat and goes by its own record of what it sent where
  (`gateway/src/lead/prefix.rs`). `routing = "prefix"` is a port of that:
  the record per server (`prefix_slots` prompts, least recently used out),
  the 0.10 similarity rule for which slot a prompt lands in, `spill`, and
  `affinity_wait_s`.
- A server's cache held prefixes by name; it now holds whole prompts, the
  last `prefix_cache_entries` of them, as llama.cpp does per slot.

Reuse is now within 12 points in every run (5 on average). What still
misses:

- `prefix-on-g3`: first token p50 2.5 s for 0.4 s measured. Reuse is right
  on average (52% for 56%) but the measured run had more than half of its
  requests fully warm; the simulator spreads the misses more evenly.
- `prefix-wait-g3`: p95 7.6 s for 2.6 s. The same run, the same cause.
- Not modelled: the gateway correcting its record from the server's
  `cached_tokens`, and session admission (`slot_hold`).

## A day through the gateway, replayed from its log

`gwsim replay` builds the scenario from the gateway's own request log: the
arrivals, the kind of work, the token counts and the prompt's block hashes
(`prefix_chain`) are the logged ones, the server's speed is read off the same
log. A 20-minute day on the three lab servers with ide, chat and batch
callers of three teams (539 requests, 2026-11-15):
answered 86.8% logged, 87.6% simulated; gave up 9.5% / 9.0%; refused 3.7% /
3.4%; first token p50 3.38 s / 3.26 s, p95 24.3 s / 24.4 s; prompt tokens
not read again 34.5% / 31.2%. By kind in `reference/replay/lab-day-RESULTS.md`.
The speed is fitted on the same run, so this checks the queueing, the
routing and the cache, not the server model.
