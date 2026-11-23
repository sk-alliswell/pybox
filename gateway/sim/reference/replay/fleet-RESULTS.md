# Replay of stand-in fleet

26925 requests over 0.5 h, 10 seed(s), 0.30 s of computing per seed.

| kind | metric | logged | simulated | ± 95% | range over seeds | gap |
|---|---|---|---|---|---|---|
| (everything) | requests | 26925 | 26925 |  | 26925 to 26925 |  |
| (everything) | answered | 84.6% | 83.9% | 0.0 pt | 83.7% to 84.0% | -0.7 pt |
| (everything) | gave_up | 0.0% | 0.0% | 0.0 pt | 0.0% to 0.0% | +0.0 pt |
| (everything) | refused | 15.4% | 16.1% | 0.0 pt | 16.0% to 16.3% | +0.7 pt |
| (everything) | ttft_p50 | 0.63 s | 0.70 s | 0.00 s | 0.70 s to 0.71 s | +12% |
| (everything) | ttft_p95 | 17.25 s | 17.79 s | 0.28 s | 17.37 s to 18.53 s | +3% |
| (everything) | wait_p50 | 0.07 s | 0.07 s | 0.00 s | 0.07 s to 0.08 s | +0.01 s |
| (everything) | wait_p95 | 16.72 s | 17.16 s | 0.29 s | 16.70 s to 17.93 s | +3% |
| (everything) | reused | 32.4% | 32.8% | 0.1 pt | 32.6% to 33.0% | +0.3 pt |
| batch | requests | 3732 | 3732 |  | 3732 to 3732 |  |
| batch | answered | 80.9% | 80.0% | 0.1 pt | 79.8% to 80.4% | -0.9 pt |
| batch | gave_up | 0.0% | 0.0% | 0.0 pt | 0.0% to 0.0% | +0.0 pt |
| batch | refused | 19.1% | 20.0% | 0.1 pt | 19.6% to 20.2% | +0.9 pt |
| batch | ttft_p50 | 0.54 s | 0.64 s | 0.00 s | 0.63 s to 0.65 s | +19% |
| batch | ttft_p95 | 7.52 s | 7.56 s | 0.09 s | 7.29 s to 7.72 s | +1% |
| batch | wait_p50 | 0.12 s | 0.14 s | 0.00 s | 0.13 s to 0.15 s | +15% |
| batch | wait_p95 | 7.10 s | 7.07 s | 0.09 s | 6.81 s to 7.25 s | -1% |
| batch | reused | 24.0% | 25.3% | 0.2 pt | 24.9% to 25.8% | +1.3 pt |
| chat | requests | 6960 | 6960 |  | 6960 to 6960 |  |
| chat | answered | 76.1% | 76.3% | 0.4 pt | 75.6% to 77.4% | +0.1 pt |
| chat | gave_up | 0.0% | 0.0% | 0.0 pt | 0.0% to 0.0% | +0.0 pt |
| chat | refused | 23.9% | 23.7% | 0.4 pt | 22.6% to 24.4% | -0.1 pt |
| chat | ttft_p50 | 0.45 s | 0.51 s | 0.00 s | 0.51 s to 0.52 s | +14% |
| chat | ttft_p95 | 1.78 s | 1.91 s | 0.03 s | 1.83 s to 2.00 s | +7% |
| chat | wait_p50 | 0.02 s | 0.03 s | 0.00 s | 0.02 s to 0.03 s | +0.01 s |
| chat | wait_p95 | 1.37 s | 1.39 s | 0.04 s | 1.30 s to 1.49 s | +1% |
| chat | reused | 21.8% | 23.4% | 0.1 pt | 23.2% to 23.6% | +1.5 pt |
| ide | requests | 16233 | 16233 |  | 16233 to 16233 |  |
| ide | answered | 89.0% | 88.0% | 0.2 pt | 87.4% to 88.4% | -1.0 pt |
| ide | gave_up | 0.0% | 0.0% | 0.0 pt | 0.0% to 0.0% | +0.0 pt |
| ide | refused | 11.0% | 12.0% | 0.2 pt | 11.6% to 12.6% | +1.0 pt |
| ide | ttft_p50 | 0.75 s | 0.86 s | 0.00 s | 0.85 s to 0.86 s | +14% |
| ide | ttft_p95 | 17.93 s | 18.53 s | 0.27 s | 17.94 s to 19.09 s | +3% |
| ide | wait_p50 | 0.10 s | 0.10 s | 0.00 s | 0.10 s to 0.11 s | +0.00 s |
| ide | wait_p95 | 17.41 s | 17.86 s | 0.27 s | 17.27 s to 18.43 s | +3% |
| ide | reused | 35.6% | 35.6% | 0.1 pt | 35.4% to 35.7% | -0.0 pt |
