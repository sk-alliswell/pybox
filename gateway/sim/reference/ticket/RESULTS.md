# Waiting tickets: the wait told against the wait had

A ticket is a request that waited 250 ms or more at the gateway on its
first try and was told a wait (`expected_ms`, the estimate made when it
joined the queue). `late` is the share that waited more than twice what it
was told and at least 2 s longer. Medians are over tickets that got a seat.

## Measured (real gateway, local models, 2026-11-13)

Client logs of the saturation runs, read by `measured.py`. The gateway was
the one on branch `gateway-unify` of that day; these runs have not been
repeated on a later gateway.

| Run | Class | Tickets | Told p50 | Waited p50 | Off p50 | Late |
|---|---|---|---|---|---|---|
| sat-smart-before | interactive | 16 | 2.0 s | 1.8 s | 0.1 s | 0.0% |
| sat-smart-before | batch | 304 | 115 s | 117 s | 5 s | 3.6% |
| sat-smart-after | interactive | 298 | 3.2 s | 2.9 s | 0.6 s | 1.0% |
| sat-smart-after | batch | 289 | 743 s | 241 s | 540 s | 9.0% |
| sat-unified | interactive | 236 | 5.0 s | 4.8 s | 0.5 s | 0.0% |
| sat-unified | batch | 311 | 549 s | 242 s | 304 s | 10.6% |
| sat-group | interactive | 241 | 3.8 s | 4.5 s | 3.0 s | 27.4% |
| sat-group | batch | 300 | 908 s | 240 s | 641 s | 4.3% |
| sat-smart-before-weights | interactive | 300 | 3.5 s | 3.4 s | 0.5 s | 0.0% |
| sat-smart-before-weights | batch | 296 | 606 s | 244 s | 379 s | 12.8% |

What it says: callers of interactive work were told a wait within about
half a second of the one they had, except with batch grouping on
(`sat-group`). Batch work was told two to four times the wait it had in
every run where interactive work goes first.

## Simulated (`scenarios/sat-smart.toml`, seed of the file)

| Class | Tickets | Told p50 | Waited p50 | Off p50 | Late |
|---|---|---|---|---|---|
| interactive | 254 | 2.8 s | 2.7 s | 0.6 s | 0.4% |
| batch | 269 | 287 s | 230 s | 126 s | 17.1% |

Beside `sat-smart-after`, the run that scenario describes: the interactive
ticket agrees (told 2.8 s for 3.2 s, off 0.6 s for 0.6 s) and so does the
wait batch work had (230 s for 241 s). What batch work was told does not:
287 s simulated for 743 s measured. So the simulator's batch ticket
reads better than the measured gateway's.

## Why batch work was told too long a wait

By when the request was sent (told / had, medians, seconds; the flood runs
from 20 s to 140 s and nothing is sent after 200 s):

| Run | 0-30 s | 30-60 s | 60-90 s | 90-120 s | 120-150 s |
|---|---|---|---|---|---|
| sat-smart-after, batch | 11 / 82 | 376 / 177 | 866 / 226 | 1512 / 266 | 1091 / 330 |
| sat-unified, batch | 8 / 67 | 211 / 192 | 1138 / 282 | 575 / 254 | 3428 / 435 |
| sat-smart-after, interactive | 1 / 1 | 4 / 3 | 3 / 2 | 4 / 4 | 4 / 3 |

The estimate divides what goes first by the pace at which work of that
importance has been getting seats lately. Before the flood that pace is
high, so the first batch requests are told too little (11 s for 82 s).
During the flood interactive work takes nearly every seat, so batch work is
told a wait that would hold if the flood went on; it ends, the queue drains
at full speed, and the wait had is a fraction of the wait told. The
estimate is right about the present and has no way to know when the load
changes. That is what the gateway's repeated notice (every 2 s) and
`expected_wait_max_ms` are for; one number given on joining cannot be right
for work that waits across a change of load. The simulator's estimate is
built the same way but moved less in this run; the two have not been
compared request by request.

