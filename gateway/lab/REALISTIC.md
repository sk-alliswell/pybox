# A realistic day through the gateway (2026-11-12)

`realistic.py` compresses a working day into 8 minutes: 8 coding assistants
with long system prompts and growing multi-turn sessions, 6 help-desk bots
answering a skewed FAQ, 2 research orchestrators that fan out, a ticket
classifier on local models, a 60-worker eval batch started during the
afternoon peak, and one agent stuck in a retry loop. `analyze_day.py` reads
the client log and the gateway log. Same machine and stack as RESULTS.md.

Base run: 3,365 requests, 6.6M input and 0.24M output tokens, $2.30.

## Biggest bottleneck: interactive requests wait behind the batch

Time to first token for interactive agents (IDE, support, research):

| | quiet hours | batch running | batch running, tuned |
|---|---|---|---|
| client-side TTFT p50 / p95 (s) | 0.54 / 1.23 | 2.17 / 4.49 | 0.88 / 1.75 |
| gateway queue, avg (ms) | 0 | 957 | 0 |
| LiteLLM's own overhead, avg (ms) | 21 | 285 | 153 |
| rest of time to response headers, avg (ms) | 731 | 1,203 | 659 |

1. **One concurrency pool for everything.** The batch held most of the 60
   slots, so an interactive request waited about a second for one. Tuned run:
   interactive keys weight 4, batch key `max_concurrency = 24`. Queue for
   interactive traffic went to zero; the batch finished 12% fewer requests.
   Still missing: priority classes or reserved slots, so this does not depend
   on someone setting weights per key.
2. **LiteLLM is one Python process.** It used 227 CPU-seconds for the base run
   against the gateway's 15, and its per-request overhead grew 14-fold under
   the batch. Run several LiteLLM workers or instances and let the gateway
   spread load across them (the gateway cannot yet balance inside one
   upstream), or send hot paths straight to the provider.

## Where the money went

Input tokens are 96% of all tokens, so the levers are on the input side.

| Waste | Base run | What the gateway did or could do |
|---|---|---|
| Prompt-cache misalignment | $0.32 (14%) | Same work cost 1.9x on Anthropic without `cache_control` ($0.0129 vs $0.0068 per turn) and 1.7x on OpenAI with a timestamp at the top of the system prompt (0% vs 82% cached). Visible today as cache share per agent; not yet fixed by the gateway (inject `cache_control`, flag volatile prefixes). |
| Runaway loop | $0.42 (18%) | A 150k token budget cut it to $0.18 and turned 1,098 further calls into 429s. A budget only reacts after the money is spent; detecting the same request repeated N times would stop it within a few calls. |
| Exact duplicate requests | 22% of requests | With the cache on for all, 308 non-streaming repeats were served from memory. 56% of duplicates were streaming requests, which the cache does not cover. |
| Cancelled streams | 19 requests | Logged with 0 tokens and $0: usage only arrives in the last chunk, so the gateway under-reports spend on every cancelled stream. |
| Context growth | 5.4k -> 14.7k input tokens over 8 turns | The agent's job to compact, but the gateway can report tokens per turn per session so it is seen. |

Tuned run total: $2.15 (-6%) with more interactive work done. The largest
remaining item, cache misalignment, needs gateway code, not configuration.

## Improvement points, in order of measured effect

1. Priority classes / reserved interactive slots (latency: p95 4.5 s -> 1.75 s available today only through hand-set weights).
2. Prompt-cache alignment: inject `cache_control` for Anthropic-format models, report cache share per agent with the reason it broke (about 14% of spend here).
3. Loop detection per run, and budgets per run and per day rather than per key lifetime (18% of spend here).
4. Balance across several LiteLLM servers inside one upstream; per-model caps and per-model 429 cooldown.
5. Cache and coalesce streaming requests (replay as SSE).
6. Account for cancelled streams (estimate input from the request, output from chunks seen).

## Limits

One run per configuration on a 4-vCPU machine that also runs LiteLLM and the
load generator; the mix of personas and their bad habits is invented, so the
percentages show which levers exist, not what a given team would save.

## Third run: streamed responses cached, request loops stopped

`gateway.realistic-v2.toml` is the tuned profile plus `[loop_guard] max_repeats = 3`, on a
gateway that also caches streamed responses and records an input estimate for streams the
client cancelled. Same day, same seed.

| | base | v2 | change |
|---|---|---|---|
| fresh tokens (input - cache read + output, provider-billed) | 5.10 M | 4.58 M | -10% |
| total tokens billed | 6.88 M | 6.18 M | -10% |
| cost | $2.30 | $1.98 | -14% |
| interactive TTFT p95 (client side) | 3.60 s | 1.41 s | -61% |
| interactive TTFT p50 | 0.68 s | 0.55 s | -19% |
| interactive total p95 | 6.51 s | 3.64 s | -44% |
| IDE turns completed | 323 | 353 | +9% |
| requests answered from the gateway cache | 0 | 2,268 | |

- Every exact repeat inside the TTL (2,268 requests, streamed or not) was answered from the
  cache. The runaway agent's repeats were among them, so it cost $0.004 instead of $0.42 and
  nothing had to be refused; the loop guard only acts on repeats the cache cannot serve.
- 15 cancelled streams now carry an input estimate (124 k tokens) instead of zero.
- What is left is 2.25 M fresh tokens of batch documents and 2.01 M of IDE context. Neither
  repeats byte for byte, so the gateway cache cannot touch them; they need provider prompt
  caching to line up (cache points, stable prefixes) or less context to be sent.
