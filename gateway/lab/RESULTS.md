# Lab results (2026-11-12)

One 4-vCPU Linux VM ran everything: two llama-cpp-python servers (2 threads
each), LiteLLM Proxy 1.109 (one process), the gateway (release build), and the
load generator. 180-second runs, same schedule in both modes, nothing else
running. Numbers are what the clients measured, in seconds.

## Local models: the backend is the bottleneck

Two CPU model servers that each answer one request at a time, six interactive
agents, and a bulk agent holding 24 requests open for the middle 65% of the run.

| | direct to LiteLLM | through the gateway |
|---|---|---|
| interactive: requests finished | 57 | 92 |
| interactive: total time p50 / p95 / p99 | 14.3 / 47.3 / 51.2 | 12.0 / 19.2 / 20.8 |
| interactive: time to first token p95 | 44.5 | 13.9 |
| bulk: requests finished | 88 | 51 |
| bulk: total time p50 / p95 | 39.4 / 49.3 | 76.9 / 102.4 |
| all: output tokens per second | 30.9 | 29.2 |

Total work is the same; the gateway decides whose work goes first. Without it
the model servers serve whatever arrived first, so each interactive request
waits behind the bulk agent's backlog. With it every agent gets a turn, the
interactive agents finish 61% more requests with a p95 2.5 times shorter, and
the bulk agent absorbs the wait. A weight on the bulk agent's key moves that
balance either way.

## Hosted models: the backend has headroom

Twelve interactive agents plus a bulk agent holding 80 requests open, spread
over gemini-2.5-flash-lite, gpt-4o-mini and claude-haiku-5-5.

| | direct to LiteLLM | through the gateway |
|---|---|---|
| requests (errors) | 2,666 (1) | 2,759 (0) |
| time to first token p50 / p95 / p99 | 1.80 / 2.72 / 4.39 | 1.76 / 2.58 / 3.72 |
| total time p50 / p95 / p99 | 4.87 / 6.28 / 7.99 | 4.60 / 5.92 / 6.86 |
| output tokens per second | 1,155 | 1,199 |

No meaningful difference: the two columns are within what two runs against
live providers differ by anyway. The gateway's own cost was 0.02 ms p50 /
0.07 ms p99 of processing per request, 0.03 ms p50 in its queue, and 9 CPU
seconds over the run. Here it adds nothing to speed and takes nothing away;
what it adds is that every one of those requests is attributed, limited and
logged.

## What went wrong on the way

Three earlier gateway runs were worse than direct, each for a reason worth
keeping:

1. **One upstream for both local servers, cap 2.** Caps are per upstream, not
   per model, so both slots could land on one server while the other idled:
   local throughput fell 30%. Fix used here: one upstream entry per backend.
   A per-model (or per-backend) cap inside one upstream would be the real fix.
2. **Hosted cap of 40 under 92 open requests.** The cap became the bottleneck
   and throughput fell 17%. A cap below what the provider accepts only
   throttles; set it from the provider's limits, not from habit.
3. **Local cap of 1.** The server sat idle while the next request crossed a
   busy LiteLLM (0.3 s p50, 1.9 s p95 to response headers under load): local
   throughput fell 28%. A cap of 2 keeps one request waiting at the server.

Also: two of those runs overlapped with a compile on the same machine and were
thrown away.

## Limits of this test

- One run per configuration, direct and gateway run minutes apart; differences
  under about 10% are noise.
- The local models are slow because they share four cores with everything
  else; absolute latencies say nothing about these models elsewhere.
- No provider rate limit was hit, so the gateway's 429 cooldown and retries
  were never exercised here.
