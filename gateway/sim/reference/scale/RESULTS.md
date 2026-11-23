# How large a run gwsim carries

`python3 reference/scale/bench.py --sizes 500,3000,10000 --stride 16`, one
seed, on a 4-vCPU cloud machine (2026-11-16). People in three teams with
ide, chat and batch work, one day shortened to 24 minutes, model servers
for 1.3 times the busiest minute, one gateway sending straight to the
servers. Model speeds and request lengths are assumed; this measures the
simulator.

| people | model servers | days | requests | token stride | events (million) | time (s) | million events/s | peak memory (MB) | answered | first token p50 / p95 (s, slowest class) |
|---|---|---|---|---|---|---|---|---|---|---|
| 500 | 55 | 1 | 13,798 | 1 | 6.4 | 1.3 | 5.4 | 119 | 100.0% | 1.71 / 2.56 |
| 500 | 55 | 1 | 13,798 | 16 | 0.6 | 0.5 | 1.3 | 120 | 100.0% | 1.70 / 2.58 |
| 3,000 | 321 | 1 | 83,820 | 1 | 38.9 | 13.4 | 3.0 | 689 | 100.0% | 1.71 / 2.56 |
| 3,000 | 321 | 1 | 83,820 | 16 | 3.4 | 4.4 | 0.8 | 689 | 100.0% | 1.70 / 2.56 |
| 10,000 | 1068 | 1 | 276,947 | 1 | 128.6 | 109.2 | 1.2 | 2263 | 100.0% | 1.70 / 2.54 |
| 10,000 | 1068 | 1 | 276,947 | 16 | 11.2 | 35.7 | 0.3 | 2263 | 100.0% | 1.70 / 2.54 |

What limits a run:

- **Time grows faster than the number of requests.** With `token_stride =
  16` the simulator does 1.3 million events a second of CPU with 55
  servers and 0.3 million with 1,068: every dispatch looks at the servers,
  and the bookkeeping is spread over many small allocations and hash
  lookups (no single hot spot in a profile).
- **`token_stride`.** Following every output token is 11 times the events
  and about 3 times the time of `token_stride = 16`; answered shares and
  time to first token did not move (within 0.02 s) at these loads. Use 1
  when the question is about streaming itself.
- **Memory is about 8 KB a request** and is kept to the end of the run
  (every request's record is used by the report). 277,000 requests took
  2.3 GB. On a 16 GB machine that is about 1.5 million requests in one
  run: 10,000 people for 5 shortened days, or 3,000 for 18. In a browser
  (the monitor, 2 to 4 GB for a page) about 250,000.
- **Several seeds** run side by side (`gwsim run --seeds N --jobs J`), each
  with its own memory.

A real-length day (86,400 s instead of 1,440) has 60 times the requests of
a shortened one: 3,000 people are 5 million requests, which does not fit.
Not done: writing finished requests out and keeping only running totals,
which would take the memory limit away.
