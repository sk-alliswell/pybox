# Replay of lab day

539 requests over 0.3 h, 10 seed(s), 0.01 s of computing per seed.

Read off the log: qwen-0.5b 496 ms before the first token plus 122 prompt tokens/s, 14.4 tokens/s (468 rows). Callers gave up after: platform-chat 30 s, platform-ide 15 s, research-batch 60 s.

| kind | metric | logged | simulated | ± 95% | range over seeds | gap |
|---|---|---|---|---|---|---|
| (everything) | requests | 539 | 539 |  | 539 to 539 |  |
| (everything) | answered | 86.8% | 87.8% | 0.3 pt | 87.2% to 88.5% | +1.0 pt |
| (everything) | gave_up | 9.5% | 8.8% | 0.3 pt | 8.2% to 9.3% | -0.7 pt |
| (everything) | refused | 3.7% | 3.4% | 0.2 pt | 3.2% to 3.7% | -0.3 pt |
| (everything) | ttft_p50 | 3.38 s | 3.28 s | 0.11 s | 3.09 s to 3.58 s | -3% |
| (everything) | ttft_p95 | 24.31 s | 23.94 s | 0.73 s | 22.15 s to 25.37 s | -2% |
| (everything) | wait_p50 | 0.49 s | 0.66 s | 0.08 s | 0.48 s to 0.89 s | +35% |
| (everything) | wait_p95 | 21.69 s | 21.36 s | 0.90 s | 19.25 s to 22.96 s | -2% |
| (everything) | reused | 34.5% | 31.6% | 0.5 pt | 30.5% to 32.4% | -2.9 pt |
| batch | requests | 112 | 112 |  | 112 to 112 |  |
| batch | answered | 80.4% | 82.9% | 1.6 pt | 77.7% to 85.7% | +2.6 pt |
| batch | gave_up | 3.6% | 2.8% | 1.0 pt | 1.8% to 6.2% | -0.8 pt |
| batch | refused | 16.1% | 14.3% | 0.7 pt | 12.5% to 16.1% | -1.8 pt |
| batch | ttft_p50 | 3.48 s | 3.41 s | 0.19 s | 3.05 s to 3.97 s | -2% |
| batch | ttft_p95 | 47.09 s | 48.40 s | 2.05 s | 44.43 s to 53.27 s | +3% |
| batch | wait_p50 | 0.07 s | 0.38 s | 0.16 s | 0.00 s to 0.69 s | +0.31 s |
| batch | wait_p95 | 44.25 s | 45.33 s | 2.15 s | 39.79 s to 49.34 s | +2% |
| batch | reused | 26.8% | 22.9% | 1.0 pt | 19.8% to 24.6% | -3.9 pt |
| chat | requests | 176 | 176 |  | 176 to 176 |  |
| chat | answered | 90.3% | 89.8% | 0.6 pt | 88.6% to 90.9% | -0.6 pt |
| chat | gave_up | 8.5% | 8.9% | 0.6 pt | 8.0% to 10.8% | +0.4 pt |
| chat | refused | 1.1% | 1.3% | 0.3 pt | 0.6% to 1.7% | +0.2 pt |
| chat | ttft_p50 | 4.04 s | 3.86 s | 0.08 s | 3.66 s to 3.97 s | -4% |
| chat | ttft_p95 | 24.44 s | 23.26 s | 1.03 s | 20.66 s to 25.31 s | -5% |
| chat | wait_p50 | 1.59 s | 1.35 s | 0.11 s | 1.09 s to 1.59 s | -15% |
| chat | wait_p95 | 22.15 s | 20.91 s | 0.99 s | 18.16 s to 22.93 s | -6% |
| chat | reused | 33.5% | 31.3% | 0.6 pt | 29.7% to 32.3% | -2.1 pt |
| ide | requests | 251 | 251 |  | 251 to 251 |  |
| ide | answered | 87.3% | 88.7% | 0.9 pt | 86.9% to 90.4% | +1.4 pt |
| ide | gave_up | 12.7% | 11.3% | 0.9 pt | 9.6% to 13.1% | -1.4 pt |
| ide | refused | 0.0% | 0.0% | 0.0 pt | 0.0% to 0.0% | +0.0 pt |
| ide | ttft_p50 | 2.55 s | 2.96 s | 0.09 s | 2.84 s to 3.23 s | +16% |
| ide | ttft_p95 | 12.54 s | 12.15 s | 0.20 s | 11.47 s to 12.52 s | -3% |
| ide | wait_p50 | 0.23 s | 0.49 s | 0.07 s | 0.39 s to 0.68 s | +111% |
| ide | wait_p95 | 10.17 s | 9.65 s | 0.38 s | 8.65 s to 10.17 s | -5% |
| ide | reused | 38.5% | 35.5% | 0.5 pt | 34.1% to 36.2% | -3.0 pt |
