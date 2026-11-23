# Open models: memory and speed, from measurements

The setup page's model list (`web/monitor.html`, the block between
`CATALOG-BEGIN` and `CATALOG-END`) is built here. The aim is that a model
picked on the page takes the GPUs it really takes and writes tokens the way
it has been measured to, alone and with others beside it.

## Which models

The latest versions of the open-weight families with the most downloads on
Hugging Face in November 2026 (30-day downloads, listed in `hf.json`), from
9B to 1.6T parameters: Qwen 3.5 / 3.6 / 3.8, Gemma 4, gpt-oss, Llama 3.1 and
3.3 (no newer Llama is among the most downloaded), Ministral 3, Nemotron 3
and 3.5, MiniMax M3 and M3.1 Flash, GLM-5.3 Flash, DeepSeek V4.1 Flash and
V4 Pro. Ornith 1.5 9B and 35B-A3B, also near the top, have the structure
and size of Qwen3.5 9B and Qwen3.6 35B-A3B, so those rows stand for them.
Left out: Kimi K3 and K3.1 Pro (1.56 TB of weights, more than eight of any
GPU on the page hold), and GLM-5.3 (the Flash variant has four times its
downloads, and the public sweeps are of GLM-5.2).

## Where each number comes from

| Number | Source | Ours? |
|---|---|---|
| Parameters, weight size, layers, heads | The publisher's repo on Hugging Face (`fetch_hf.py` -> `hf.json`) | no |
| GPU memory a replica needs | `vram_minimum_gb` in the vLLM project's recipes (`fetch_recipes.py` -> `recipes.json`); without one, weights x 1.19 | no |
| One caller's speed with n served together, time to first token | InferenceX benchmark rows, SemiAnalysis, public API (`fetch_inferencex.py` -> `inferencex.json`): 400 sweeps on H100, H200, B200, B300 | no |
| The same for Qwen3.8 27B on an H100 | One write-up (Microsoft Tech Community, vLLM, FP8, 2,000-token prompts) | no |
| GPU against GPU on a small model | Runpod's GPU guide, Qwen2.5-7B on 21 cards | no |
| Speed of a model with no sweep | The estimate in `build_catalog.py`, checked against 22 published speeds | computed |
| Memory and speed of ten 4-bit files on 4 CPU cores | `measure_local.py` -> `local.json` | **yes** |

`build_catalog.py` puts them together into `catalog.json` and prints every
check; `RESULTS.md` is that output. Nothing was run by us on a GPU.

## Reading a row

- `need_gb`: memory for one replica. GPUs per replica = what holds it at 95%
  full, or the GPU count of the sweep the speed is taken from. More than
  eight: the model does not run on that kind of GPU on the page.
- `tps`: one caller alone, tokens a second. A sweep that starts at four
  callers is carried back to one along its first two points.
- `curve`: `[n, share of tps]`, straight from the sweep with dips removed.
  The engine follows it (`batch_curve`). `seen` is the last measured count;
  the points after it are carried on, a doubling at a time to 2,048, with
  the median slope of the single-turn sweeps that reach that count (taken
  from total output, so a server that queues inside counts as flat), never
  gentler than the sweep's own last segment, n^-0.5 at the gentlest and
  n^-1 at the steepest. `RESULTS.md` has the slopes.
- `knee`: requests together that still leave each one 30 tokens a second,
  no further than four times `seen` (not past `seen` for coding-agent
  sweeps, whose count is users). The page plans a server for that many and
  the gateway sends it no more.
- `read`: prompt tokens a second, from the first-token times with 1,024-
  and 8,192-token prompts.
- `src`: `public`, `agentic`, `blog`, `scaled` or `estimate`.

## What to keep in mind

- Sweeps are not like for like: vLLM or SGLang, FP4 or FP8, some with
  speculative decoding (every coding-agent run, and the Qwen3.8 write-up).
  They say how that deployment behaved, not which model is faster.
- In coding-agent sweeps n counts users, who also wait on their tools, so
  fewer than n requests run at once and the curve is gentler than a
  request-level one would be.
- The estimate is good for dense models (median 6% off, worst 11%) and
  rough for sparse ones (median 15%, worst 61%).
- Context memory per token computed from a model's structure matched what
  vLLM reported for gpt-oss 120B on H100 and H200 (within 8% in 7 setups)
  and did not for MiniMax M3 and DeepSeek V4 Pro, so the page does not size
  context room from it.
- The CPU measurements are of 4-bit files on a 4-core cloud box. They are
  the project's own numbers for small models on such a box and say nothing
  about a GPU.

## Refreshing

```
python3 fetch_hf.py > hf.json
python3 fetch_recipes.py > recipes.json
python3 fetch_inferencex.py > inferencex.json
python3 measure_local.py DIR_WITH_GGUF > local.json    # optional, slow
python3 build_catalog.py --page --markdown > RESULTS.md
```
