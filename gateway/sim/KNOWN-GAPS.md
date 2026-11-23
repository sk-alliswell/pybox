# Known gaps

What the simulator gets wrong or does not know, one entry per gap, with what
would close it. An entry leaves this file when a measured replay in
`reference/` covers it and the accuracy check passes with it. Numbers are from
the replays with measured arrivals, 20 seeds (see `VALIDATION.md`).

## Predictions that miss a measurement

| Gap | Size | Likely cause | What would close it |
|---|---|---|---|
| `sat-smart`: interactive time to first token during the flood | p50 20% too low | Interactive traffic alone uses about 97% of one server; at that load a 3% error in service time moves the wait by tens of percent | A second run of the same experiment to learn how much the measurement itself varies, then per-request service-time spread from server-side logs |
| `sat-plain`: interactive time to first token over the whole run | p50 31% too low | The real system recovered more slowly after the flood than the model | Server-side timestamps for the recovery period; the flood-phase numbers are within 5% |
| `sat-tight`: interactive requests answered | 5 to 7 points too few | Not known; the queue holds only 8, so single admissions matter | Gateway admission log for the run, compared event by event |
| Overload at 2x with eviction or gradual refusal (`reference/overload`) | chat answered 7 to 10 points too high with the shared server speed; 1 to 4 points with each run's own speed | The servers ran 8 to 10% slower in those two runs than in the runs the speed was fitted on (3.46 and 3.51 s a request for 3.20 s). At 2x a chat request is granted after 22 to 27 s of its 30, so 1% of server speed moves about 1 point of chat answers | Nothing in the model. For a forecast at such a load, run the speed 5 to 10% either way and read the answer as a range (`replay.py --own-speed` shows the size of it) |
| Prefix routing on, three groups (`reference/prefix`, `prefix-on-g3`) | first token p50 2.5 s for 0.4 s; reuse itself is within 12 points in all twelve runs | The measured run had more than half of its requests fully warm; the simulator spreads the misses evenly | The gateway's belief correction from the server's `cached_tokens`, then replay again |
| Prefix routing off (`prefix-off-g3`, `prefix-off-g6-s2`) | first token p95 13.2 s for 17.5 s, 13.9 s for 9.0 s | Host contention is two fitted numbers (reading slows about 2.2x beside two busy servers, writing 1.1x) | Per-request prefill times from the servers under 1, 2 and 3 busy neighbours |
| `sat-tight`, `sat-bounded-patient`: interactive first token during the flood | 6 to 9% lower than before the scheduler port | Those runs were measured on the scheduler before the unification; the simulator now follows main | Run them again on main |
| Waiting ticket of batch work (`reference/ticket`, `sat-smart`) | told 287 s for 743 s measured; the wait itself 230 s for 241 s | Both estimates assume the present pace lasts; the flood ends and batch work then drains quickly, so work that joined during the flood was told too long. The gateway's estimate swung further than the simulator's (up to 1,500 s told at the height of the flood) | Compare the two estimates request by request on one run with the told wait logged at every notice, not only on joining |
| Gateway memory under a retry storm | 390 MB measured, not reproduced | Linear model with rough constants | Memory samples against queue length and open connections |

## Scale

Checked against a real gateway: 16 stand-in model servers of four slots,
2,000 callers in three teams, three shortened days, 26,925 requests
(`reference/replay/fleet-RESULTS.md`, `gateway/lab/FLEET.md`). The servers
there only keep time, by the simulator's own server model, so that run
tests the gateway's queueing, refusal and prefix routing at that size and
not the model of a model server. Answered 84.6% logged, 83.9% simulated;
by kind of work within 1.1 points; prompt tokens not read again 32.4% and
32.8%.

Not checked: several LiteLLM proxies sharing load at that size, more than
one gateway, real model servers beyond three, and anything past 16 servers
(the monitor's presets go to about a thousand).

How large a run the simulator carries is in `reference/scale/RESULTS.md`.

## Parts with assumed numbers

Results that depend on these are illustrations until a measurement replaces
the assumption.

| Part | What is assumed | What would close it |
|---|---|---|
| LiteLLM proxy under CPU saturation | CPU cost per request and per chunk from one aggregate (227 CPU-s for 3,365 requests); the 14x growth of its overhead is reproduced only in shape | A load run against LiteLLM alone at several rates, with CPU samples |
| Hosted APIs | Latency, rate limits and retry-after as the scenario says | Client logs from a real hosted run (needs a decision to spend on one) |
| MCP servers | Service time and concurrency as the scenario says | A run with a real tool server in the path |
| Log store | 96,000 rows/s from TDB's own benchmark | A run with the gateway writing to TDB under load |
| Host CPU contention | Shared-core slowdown; off in the lab replays because fitted rates already include it | Runs with the servers pinned and unpinned |
| Setup page model catalogue | 20 open models (`reference/models`). Speed under concurrency is a public sweep on that GPU for 6 of them on an H100 (InferenceX), a single write-up for one, moved from another GPU for 3, and an estimate for 10 (time per layer plus weights read over memory bandwidth; against 22 published points the median error is 6% dense, 15% sparse, worst 61%). Memory is vLLM's stated minimum or weights x 1.19. Nothing was run by us on a GPU; the only numbers of our own are 4-bit files on 4 CPU cores. Sweeps differ in engine, precision and speculative decoding, so two models' numbers are not a like-for-like ranking | A concurrency sweep (`vllm bench serve` or the lab's `batch.py`) on the servers that will be used, then `batch_curve` and `tokens_per_s` from it |
| Batching on a replica | The setup page follows each model's sweep (`batch_curve`); a model without one borrows the shape of Llama 3.3 70B (dense) or gpt-oss 120B (sparse) on one H200. Past the last measured point (64 together for most) each doubling takes the median slope of the public single-turn sweeps that reach that count, so the planned count of a model whose sweep stopped early rests on other models' sweeps. Scenario files without a curve still use n^`batch_scaling` | A sweep on the real server that goes past the point where the total stops growing |
| Load-relative routing at the proxy | Every proxy sees every backend's true load | A run with two LiteLLM instances and per-deployment limits |
| Settings search | Best of the tries, in the simulator, one seed by default; untested against a real stack | Run the chosen settings on real servers and compare |
| Spare tokens in the idle report | Upper bound: every slot busy for the whole run | - |
| Gateway CPU | `cores` and `cpu_ms_per_request` are typed in; nothing was measured on the real gateway under CPU pressure | A load run with CPU samples of the gateway process |
| Gateway response cache | A fixed share of requests is answered from the cache (`hit_rate`), after `lookup_ms` | Count repeated requests in real gateway logs |
| Setup page teams | One simulated client per team and request type (`team~type`, class `team · type`); a person's three request streams are independent of each other | Session traces from the real agents |
| Prefix (KV) cache: how much of a prompt repeats | From the Qwen-Bailian traces (`reference/prior-art`): a cloud service's coder, chat and API traffic, two hours, sampled. Not this team's agents; every request of a type has the same length | Hash the first N tokens of each prompt in the gateway log and count repeats per caller and request type |
| Prefix (KV) cache: what a hit saves | 86 to 99% of the cached part's reading time, measured on a 0.5B model on 4 CPU threads (`reference/kv-cache`); the setup page uses 93%. Not measured on a GPU server or under concurrent load | The same script against the real servers |
| Prompt reading speed on the setup page | Per model, from the first-token times of the public sweeps with 1,024- and 8,192-token prompts (median 205 times the solo writing speed for dense models, 172 for sparse); models without both take the median. One caller's prompt at the lowest measured concurrency, not reading under load | Time to first token against prompt length on the real servers |
| Prefix (KV) cache: what a server keeps | llama.cpp's rule as read from its documentation (per slot, best match among idle slots, else least recently used), with a prompt reduced to two keys (own context, shared part) instead of token-by-token similarity; `--cache-reuse` is not modelled. Not compared with a server's own counters | Replay one scenario against real llama.cpp servers and compare cached-token counts |
| `affinity` and `cost` routing | The router knows exactly what each server has cached (not whether the slot is busy), its speeds, and for `cost` the length of the answer. PR #8 measured the gateway's belief within a point of the servers on three single-slot servers | Runs with multi-slot servers through the real gateway |
| `shed`, `least_attained_half_life_s`, `admit_by_deadline` | Not in the gateway; the simulator's version is the definition. Effects in `reference/prior-art/RESULTS.md` rest on that workload | Build, then replay the same scenario on local servers |
| Usage report (`gwsim usage`) | Run on one real log: 513 requests through the gateway with TDB and local models, a day shortened to 30 minutes (`lab/USAGE.md`); it agreed with the driver's record. Not run on a real 24-hour day or more than one model. Summary lines are fixed thresholds (30% used = quiet, queue p95 over 2 s or 1% refused = pressed, 25% cache reuse = low) | A day of production rows, read against what the teams say happened |
| Network | Fixed latency and jitter; no loss, no bandwidth limit | Only matters off one machine; a two-host run |

## Not modelled

Experiments that depend on these still have to be run for real.

- Adaptive concurrency, priority classes with a reserve, and the session
  admission of PR #9. The token-counter order, staged refusal and least used
  first are ported (`reference/overload/RESULTS.md`).
- The gateway correcting its prefix record from what the server reports,
  and session admission (`slot_hold`). `routing = "prefix"` and
  `affinity_wait_s` are ported.
- The LiteLLM proxy's own cache.
- Which requests repeat exactly (the gateway response cache is a fixed hit
  rate), and a conversation's prefix growing turn by turn for open-loop
  clients.
- Backend failures, restarts and mid-stream drops.
- More than one gateway instance.
