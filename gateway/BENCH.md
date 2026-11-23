# Serving benchmark

`examples/bench.rs` measures what the gateway itself costs. It starts a mock
model server in-process (SSE, TCP_NODELAY, no network), spawns the gateway
binary in front of it, and drives the same load directly and through the
gateway. CPU is the gateway process's user+system time from `/proc`.

```sh
cargo build --release --locked --example bench
target/release/examples/bench target/release/pybox-gateway --format openai
# --format openai|anthropic  --conc N  --requests N  --chunks N
# --gap-ms N (pause between chunks)  --prompt-kb N  --obs on|off
```

The OpenAI mock sends `"usage":null` on every content chunk and the real usage
in the last one, as the real API does with `stream_options.include_usage`.

## Before / after (2026-11-12)

"Before" is the head of the `gateway-observability` branch built with the same
flags. 4-core VM shared by the mock, the load generator and the gateway, mean
of two rounds, observability on (memory backend). Numbers are relative, not a
capacity claim.

| Scenario | | req/s | gateway CPU per request | TTFB p50 / p99 | total p50 |
|---|---|---|---|---|---|
| OpenAI stream, 203 chunks | before | 1,114 | 2,196 µs | 3.0 / 14.2 ms | 55.9 ms |
| | after | 1,537 | 1,384 µs | 2.6 / 13.5 ms | 39.3 ms |
| Anthropic stream, 203 chunks | before | 1,311 | 1,404 µs | 2.0 / 12.4 ms | 49.7 ms |
| | after | 1,599 | 1,313 µs | 2.2 / 13.2 ms | 37.7 ms |
| Short reply, 1 chunk, 2 KB prompt | before | 1,420 | 279 µs | 1.4 / 5.9 ms | 44.1 ms |
| | after | 11,391 | 208 µs | 3.9 / 7.7 ms | 5.4 ms |
| Paced stream, 300 concurrent, 10 ms between chunks | before | 247 | 2,408 µs | 45.7 / 54.0 ms | 1,165 ms |
| | after | 256 | 1,673 µs | 13.7 / 27.8 ms | 1,166 ms |

Direct to the mock, for reference: short reply total p50 1.2 ms, paced TTFB p50
11.6 ms.

What changed:

- **Nagle delay.** Accepted sockets did not set TCP_NODELAY, so a response
  written as headers and then a small body waited about 40 ms for the client's
  delayed ACK. That is the 44 ms total on short replies and the 46 ms first
  token on paced streams. Fixed in `cli.rs`. In the short scenario "before" is
  idle most of the time for this reason, which is why its TTFB looks lower
  while its throughput is 8x worse.
- **Usage parser.** The stream is scanned with `memchr` and only the lines that
  can carry usage or a finish reason are JSON-parsed. The OpenAI usage-chunk
  filter no longer copies chunks it passes through.
- **Body rewrite.** `stream_options.include_usage` is spliced into the request
  bytes instead of re-serialising the whole JSON body.
- **Build profile.** `lto = "thin"`, `codegen-units = 1`.

Fixed cost per request is about 115 µs of proxying plus about 75 µs of
observability (`--obs off` to compare).

Tried and dropped: mimalloc as the global allocator. No gain on streaming and
much higher resident memory.

Known limit: the in-memory observability backend holds whole rows and reached
about 720 MB resident after 40k requests with `memory_rows = 200000`. Use the
TDB backend, or lower `memory_rows`, for long runs.
