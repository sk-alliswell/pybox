# The model against rising load (2026-11-16)

Simulated, with the page's defaults (server lets in 4x its best batch,
128-token prompt chunks, context room 2x; all three assumed). The day
runs were made with the gateway sending a server 2x its best batch; the
page's default has since gone back to 1x.

## One server, callers in a closed loop (prompt 5,700, answer 470 tokens)

| callers | tokens/s one caller sees | first token p50 (s) | server total (tokens/s) | gave up |
|---|---|---|---|---|
| 1 | 90.2 | 2.2 | 48 | 0 |
| 2 | 67.0 | 2.3 | 77 | 0 |
| 4 | 43.1 | 2.5 | 114 | 0 |
| 8 | 26.1 | 4.4 | 149 | 0 |
| 16 | 14.4 | 7.0 | 172 | 0 |
| 32 | 13.4 | 48.1 | 174 | 16 |
| 64 | 13.4 | 40.3 | 17 | 484 |
| 128 | 14.1 | 40.4 | 17 | 995 |

Measured on the local model, decoded together: 1.00, 0.81, 0.73, 0.46,
0.29, 0.17, 0.09 of solo speed at 1, 2, 4, 8, 16, 32, 64. The simulated
server gives 1.00, 0.74, 0.48, 0.29, 0.16 at 1 to 16 callers, steeper
because its callers also read long prompts, and stops falling at 16
because context room lets no more in: past that the load shows as
waiting, not as slower writing.

## Default day, 500 people, load multiplied (hours 12 to 15)

| load | env | answered | ide tokens/s | ide first token (s) | total tokens/s | gave up |
|---|---|---|---|---|---|---|
| 0.25 | none | 100% | 57.7 | 1.6 | 3,389 | 0 |
| 0.25 | gateway | 100% | 79.3 | 1.5 | 3,397 | 0 |
| 0.5 | none | 100% | 30.0 | 2.9 | 6,734 | 0 |
| 0.5 | gateway | 100% | 44.2 | 1.6 | 6,917 | 0 |
| 1 | none | 93.7% | 16.6 | 24.4 | 10,363 | 1,027 |
| 1 | gateway | 93.2% | 16.6 | 21.0 | 10,582 | 1,033 |
| 1.5 | none | 51.0% | 16.5 | 63.5 | 9,810 | 12,046 |
| 1.5 | gateway | 73.9% | 16.3 | 52.9 | 10,145 | 3,040 |
| 2 | none | 38.1% | 16.2 | 80.7 | 6,880 | 20,113 |
| 2 | gateway | 61.9% | 15.7 | 58.5 | 9,823 | 4,019 |
| 3 | none | 27.8% | 16.2 | 86.8 | 448 | 35,079 |
| 3 | gateway | 49.4% | 15.5 | 49.0 | 9,684 | 5,190 |

One seed each.
