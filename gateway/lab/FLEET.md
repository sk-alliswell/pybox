# A larger system through the real gateway

The lab has three small model servers. To see whether the gateway and its
simulator (`gateway/sim`) still agree at a size the lab cannot reach with
real models, `examples/fleet.rs` stands in for everything around the
gateway: many model servers that only keep time, and thousands of callers.
The gateway under test is the real one (`pybox-gateway-tdb`, log in TDB).

```sh
cargo build --release --example fleet
F=target/release/examples/fleet
$F serve  --servers 16 --port 9300 &                 # 16 servers, 4 requests each
$F config --servers 16 --port 9300 > fleet.toml      # one upstream, prefix routing, staged refusal
tdb-backend/target/release/pybox-gateway-tdb --config fleet.toml &
$F drive  --users 2000 --days 3 --day-s 600 --out client.jsonl
curl -H 'authorization: Bearer fleet-admin' \
  'http://127.0.0.1:8788/gateway/obs/export?from=START_MS&to=END_MS&cols=agent_id,request_kind,priority,model,status,client_abort,attempts,queue_ms,ttft_ms,model_ttft_ms,gen_ms,total_ms,input_tokens,output_tokens,cache_read_tokens,gateway_cache,prefix_chain' > rows.jsonl
cd sim && cargo run --release -- replay rows.jsonl --base reference/replay/fleet.toml --seeds 10
```

A stand-in server runs four requests at once, keeps the last prompt of
every slot, takes 80 ms plus the unread prompt tokens at 3,000 a second
before the first token, and writes tokens at 40 a second shared by what
runs together. That is the simulator's own model of a server, on purpose:
this run tests the queueing, the refusal and the routing at size, not the
model of a model server (the local-model runs do that).

## Run of 2026-11-16

2,000 callers in three teams (ide, chat, batch), three days of 10 minutes
each, 26,925 requests, 16 servers. The busy hours ask for more than the
servers have, so the queue fills and requests are refused.

| | logged | simulated (10 seeds) |
|---|---|---|
| answered | 84.6% | 83.9% |
| refused | 15.4% | 16.1% |
| first token p50 | 0.63 s | 0.70 s |
| first token p95 | 17.3 s | 17.8 s |
| prompt tokens not read again | 32.4% | 32.8% |
| answered: ide / chat / batch | 89.0 / 76.1 / 80.9% | 88.0 / 76.3 / 80.0% |

By kind of work in `sim/reference/replay/fleet-RESULTS.md`.

Two things this run found:

- The simulator ordered waiting agents by prompt tokens; the gateway's
  default policy (`round_robin`) counts requests. With equal prompts, as in
  the earlier overload runs, the two are the same. Here they are not, and
  the first replay had every kind refused alike (15.8%) where the gateway
  refused chat 23.9% and ide 11.0%. The simulator now has both policies.
- `gwsim replay --fit` reads a server's speed off the log as callers saw
  it. With servers that run several requests at once that is the speed
  under load, and applying the server model on top counts the slowdown
  twice (answered 77% for 84.6%). For such servers give the speed in the
  base scenario and leave `--fit` out.

Not covered: LiteLLM in between, more than one upstream, real model servers
at this size.
