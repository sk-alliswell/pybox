#!/usr/bin/env python3
"""Build the model list of the setup page from what is published and what
we measured, and say for every number which of the two it is.

    python3 build_catalog.py              # writes catalog.json, prints the tables
    python3 build_catalog.py --page       # also rewrites the block in web/monitor.html
    python3 build_catalog.py --markdown   # the tables as markdown (RESULTS.md)

Inputs, all in this folder: hf.json (fetch_hf.py), recipes.json
(fetch_recipes.py), inferencex.json (fetch_inferencex.py), local.json
(measure_local.py) and the few numbers from write-ups in ANCHORS below.

Where a number comes from (`src` in catalog.json):
  public    a concurrency sweep on that GPU from InferenceX, single-turn
  agentic   the same source, replay of coding-agent traces (n = users)
  blog      one write-up's own measurement
  scaled    measured on another GPU, moved here with the estimate's ratio
  estimate  no measurement of this model: the bandwidth estimate below
Memory: `recipe` is the vLLM project's stated minimum, `computed` is the
published weight size times RECIPE_FACTOR.
"""
import json, math, os, re, statistics, sys

HERE = os.path.dirname(os.path.abspath(__file__))
load = lambda name: json.load(open(os.path.join(HERE, name)))

# GPU memory (GB) and memory bandwidth (TB/s): InferenceX chip specs for
# H100, H200 and B200, NVIDIA datasheets for L40S and A100. Prices are the
# page's own (October 2026 on-demand medians) and are not touched here.
GPUS = {
    "l40s": {"label": "L40S (48GB)", "mem": 48, "bw": 0.864, "price": 1.62},
    "a100": {"label": "A100 (80GB)", "mem": 80, "bw": 2.039, "price": 2.75},
    "h100": {"label": "H100 (80GB)", "mem": 80, "bw": 3.35, "price": 4.2},
    "h200": {"label": "H200 (141GB)", "mem": 141, "bw": 4.8, "price": 5.7},
    "b200": {"label": "B200 (180GB)", "mem": 180, "bw": 8.0, "price": 8.19},
}
# One caller's tokens a second on Qwen2.5-7B (BF16, vLLM, one request a
# second) per GPU: Runpod, "Best GPU for LLM inference", September 2026.
# The page's general speed factor of a GPU against an H100 is this ratio.
RUNPOD_7B = {"l40s": 48, "a100": 96, "h100": 165, "h200": 195, "b200": 272}

# The estimate for a model nobody has published a sweep of: one token takes
# a fixed time per layer plus the time to read the weights it touches once.
#   ms per token = MS_PER_LAYER * layers + touched GB / (GPUs * TB/s)
# The per-layer time is 0.08 ms for a dense model and 0.10 ms for a sparse
# one (routing to experts); the checks printed below show how far off that is.
MS_PER_LAYER = {"dense": 0.08, "moe": 0.10}
# A server holds at most this many GPUs; a model that needs more does not
# run on that kind of GPU here.
MAX_GPUS = 8
# vLLM's stated minimum memory over the published weight size, median of
# the recipes we could read (printed below).
RECIPE_FACTOR = 1.19
# A server is planned to run as many requests together as still leaves each
# of them this many tokens a second.
FLOOR_TPS = 30
# How full a GPU's memory may be planned.
FILL = 0.95

# The models on the page. `hf` is the checkpoint that is served (its size is
# the weight memory), `base` the original repo (downloads, structure),
# `active` the billions of parameters one token touches (the publisher's
# figure; equal to the total for a dense model).
MODELS = [
    dict(id="qwen3.5-9b", label="Qwen3.5 9B", hf="Qwen/Qwen3.5-9B", active=9.65, local="Qwen3.5-9B-Q4_K_M.gguf"),
    dict(id="gemma-4-12b", label="Gemma 4 12B", hf="google/gemma-4-12B-it", recipe="Google/gemma-4-12B-it", active=11.96, local="gemma-4-12b-it-qat-q4_0.gguf"),
    dict(id="ministral-3-14b", label="Ministral 3 14B", hf="mistralai/Ministral-3-14B-Instruct-2512", active=13.95),
    dict(id="llama-3.1-8b", label="Llama 3.1 8B", hf="meta-llama/Llama-3.1-8B-Instruct", active=8.03, layers=32, local="Meta-Llama-3.1-8B-Instruct-Q4_K_M.gguf"),
    dict(id="gpt-oss-20b", label="gpt-oss 20B", hf="openai/gpt-oss-20b", active=3.6, local="gpt-oss-20b-Q4_K_M.gguf"),
    dict(id="qwen3.8-27b", label="Qwen3.8 27B", hf="Qwen/Qwen3.8-27B-FP8", base="Qwen/Qwen3.8-27B", active=27.78),
    dict(id="qwen3.6-35b-a3b", label="Qwen3.6 35B-A3B", hf="Qwen/Qwen3.6-35B-A3B-FP8", base="Qwen/Qwen3.6-35B-A3B", active=3.0),
    dict(id="gemma-4-31b", label="Gemma 4 31B", hf="google/gemma-4-31B-it", recipe="Google/gemma-4-31B-it", active=31.27),
    dict(id="gemma-4-26b-a4b", label="Gemma 4 26B-A4B", hf="google/gemma-4-26B-A4B-it", recipe="Google/gemma-4-26B-A4B-it", active=3.8),
    dict(id="nemotron-3.5-30b-a3b", label="Nemotron 3.5 Lightning 30B-A3B", hf="nvidia/NVIDIA-Nemotron-3.5-Lightning-30B-A3B-BF16", active=3.0),
    dict(id="llama-3.3-70b", label="Llama 3.3 70B", hf="meta-llama/Llama-3.3-70B-Instruct", active=70.55, layers=80, ix="Llama-3.3-70B-Instruct-FP8", fp8=True),
    dict(id="gpt-oss-120b", label="gpt-oss 120B", hf="openai/gpt-oss-120b", active=5.13, ix="gpt-oss-120b"),
    dict(id="nemotron-3-120b-a12b", label="Nemotron 3 Super 120B-A12B", hf="nvidia/NVIDIA-Nemotron-3-Super-120B-A12B-BF16", active=12.0),
    dict(id="qwen3.8-flash-next", label="Qwen3.8 Flash Next 176B-A6B", hf="Qwen/Qwen3.8-Flash-Next-FP8", base="Qwen/Qwen3.8-Flash-Next", active=6.0, ix="Qwen3.8-Flash-Next"),
    dict(id="minimax-m3.1-flash", label="MiniMax M3.1 Flash", hf="MiniMaxAI/MiniMax-M3.1-Flash", active=None, ix="MiniMax-M3.1-Flash"),
    dict(id="glm-5.3-flash", label="GLM-5.3 Flash", hf="zai-org/GLM-5.3-Flash", active=None, ix="GLM-5.3-Flash"),
    dict(id="qwen3.5-397b-a17b", label="Qwen3.5 397B-A17B", hf="Qwen/Qwen3.5-397B-A17B-FP8", base="Qwen/Qwen3.5-397B-A17B", active=17.0, ix="Qwen-3.5-397B-A17B"),
    dict(id="minimax-m3", label="MiniMax M3 427B", hf="MiniMaxAI/MiniMax-M3-MXFP8", base="MiniMaxAI/MiniMax-M3", active=23.0, ix="MiniMax-M3"),
    dict(id="deepseek-v4.1-flash", label="DeepSeek V4.1 Flash", hf="deepseek-ai/DeepSeek-V4.1-Flash-1017", recipe="deepseek-ai/DeepSeek-V4.1-Flash", active=16.0, params=552.0, ix="DeepSeek-V4.1-Flash"),
    dict(id="deepseek-v4-pro", label="DeepSeek V4 Pro", hf="deepseek-ai/DeepSeek-V4-Pro-0813", recipe="deepseek-ai/DeepSeek-V4-Pro", active=49.0, params=1600.0, ix="DeepSeek-V4-Pro"),
]

# Numbers from single write-ups (each one's own measurement, not ours).
ANCHORS = {
    # Microsoft Tech Community, "Qwen3.8-27B on a $2 H100 Azure Spot VM",
    # one H100 NVL (94 GB), vLLM, FP8 weights and KV, max-num-seqs 32,
    # synthetic 2,000-token prompts: output 150 tok/s at 1 request, 821 at
    # 16, 1,187 at 32, 1,212 at 48.
    ("qwen3.8-27b", "h100"): dict(gpus=1, points=[(1, 150.0), (16, 821 / 16), (32, 1187 / 32), (48, 1212 / 48)],
                                  ref="Microsoft Tech Community, Qwen3.8-27B on one H100 NVL (vLLM, FP8, 2,000-token prompts)"),
}
# Single points used only to check the estimate: a checkpoint in hf.json,
# layers, billions of parameters one token touches (None = all), GPU or its
# bandwidth in TB/s, GPUs, published tokens a second for one caller, source.
CHECKS = [
    ("Qwen/Qwen2.5-7B-Instruct", 28, None, "l40s", 1, 48, "Runpod GPU guide"), ("Qwen/Qwen2.5-7B-Instruct", 28, None, "a100", 1, 96, "Runpod GPU guide"),
    ("Qwen/Qwen2.5-7B-Instruct", 28, None, "h100", 1, 165, "Runpod GPU guide"), ("Qwen/Qwen2.5-7B-Instruct", 28, None, "h200", 1, 195, "Runpod GPU guide"),
    ("Qwen/Qwen2.5-7B-Instruct", 28, None, "b200", 1, 272, "Runpod GPU guide"), ("Qwen/Qwen3.6-27B-FP8", 64, None, "b200", 1, 105, "Runpod GPU guide"),
    ("Qwen/Qwen3.6-35B-A3B-FP8", 40, 3.0, "l40s", 1, 99, "Runpod GPU guide (L40)"), ("Qwen/Qwen3.6-35B-A3B-FP8", 40, 3.0, 8.0, 1, 260, "Runpod GPU guide (B300)"),
    ("Qwen/Qwen3.6-35B-A3B-FP8", 40, 3.0, 1.79, 1, 209, "A. Kuo, Qwen3.6-35B-A3B on RTX PRO 6000 Blackwell (vLLM)"),
]

ENGINES = ["vllm", "sglang", "dynamo-vllm", "dynamo-sglang", "trt", "dynamo-trt"]


def monotone(points):
    """A sweep with its dips taken out: no point slower than a later one."""
    out, best = [], 0.0
    for n, each in reversed(points):
        best = max(best, each); out.append((n, best))
    return out[::-1]


def solo_of(points):
    """One caller alone: the first point if it is n = 1, else the time per
    token carried back to n = 1 along the line through the first two points
    (never more than 1.35 times the first point)."""
    (n0, e0), (n1, e1) = points[0], points[1]
    if n0 <= 1: return e0
    t0, t1 = 1 / e0, 1 / e1
    t = t0 - (t1 - t0) / (n1 - n0) * (n0 - 1)
    return min(1 / t if t > 0 else e0 * 1.35, e0 * 1.35)


def share_at(curve, n):
    """Mirror of `curve_share` in src/scenario.rs."""
    if n <= curve[0][0]:
        return curve[0][1] if curve[0][0] <= 1 else math.exp(math.log(n) / math.log(curve[0][0]) * math.log(curve[0][1]))
    if n >= curve[-1][0]:
        (n0, s0), (n1, s1) = curve[-2], curve[-1]
        return s1 * (n / n1) ** min(-0.5, max(-1.0, math.log(s1 / s0) / math.log(n1 / n0)))
    for a, b in zip(curve, curve[1:]):
        if n <= b[0]:
            t = (math.log(n) - math.log(a[0])) / (math.log(b[0]) - math.log(a[0]))
            return math.exp(math.log(a[1]) + t * (math.log(b[1]) - math.log(a[1])))


def knee_of(curve, solo, reach):
    """The most requests together that still leave each one FLOOR_TPS
    tokens a second (the lowest per-user target InferenceX reports). Where
    the sweep stops while each caller still gets more than that, the count
    comes from the carried-on part of the curve (`extend`), no further than
    `reach` (four times the last measured count; the last measured count
    itself for coding-agent sweeps, whose count is users, not requests)."""
    return max(2, max((n for n in range(1, int(reach) + 1) if solo * share_at(curve, n) >= FLOOR_TPS), default=2))


EXTEND_TO = [2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048]


def band_slopes(series):
    """How one caller's speed falls as the count doubles, from every
    single-turn sweep: for each count in EXTEND_TO, the median slope
    (d ln speed / d ln n) of the segments that end there. Taken from the
    server's total output divided by the count, not from the reported
    per-caller speed: once a server queues inside, the per-caller speed of
    those it is serving stops falling while the total has stopped growing.
    Past 512 the segments are few and are pooled."""
    seg = {}
    for s in series:
        if s["workload"] != "single_turn" or len(s["points"]) < 3: continue
        pts = sorted(s["points"], key=lambda q: q["n"])
        for a, b in zip(pts, pts[1:]):
            if a["total_tps"] > 0 and b["total_tps"] > 0 and b["n"] > a["n"]:
                seg.setdefault(b["n"] if b["n"] <= 512 else 2048, []).append(math.log(b["total_tps"] / a["total_tps"]) / math.log(b["n"] / a["n"]) - 1)
    out = {n: (statistics.median(seg[n]), len(seg[n])) for n in EXTEND_TO if seg.get(n)}
    out[2048] = (min(out[2048][0], out[512][0]), out[2048][1])  # no gentler than the doubling before it
    out[1024] = out[2048]
    return out


def extend(curve, bands, full):
    """Carry a curve on past its last measured point, to 2,048 together.
    Each doubling takes the median slope the public sweeps show at that
    count, never gentler than the sweep's own last segment and never
    gentler than n^-0.5 or steeper than n^-1 (the engine's limits). A sweep
    that ended with the server past its limit (`full`) gains nothing more:
    n^-1, the same total shared by more."""
    (n0, s0), (n1, s1) = curve[-2], curve[-1]
    own = max(-1.0, min(0.0, math.log(s1 / s0) / math.log(n1 / n0)))
    out = list(curve)
    for n in EXTEND_TO:
        if n <= out[-1][0]: continue
        slope = -1.0 if full else max(-1.0, min(-0.5, own, bands[n][0]))
        out.append((n, out[-1][1] * (n / out[-1][0]) ** slope))
    return out


def estimate_tps(kind, layers, touched_gb, gpu, gpus):
    bw = GPUS[gpu]["bw"] if gpu in GPUS else gpu
    return 1000 / (MS_PER_LAYER[kind] * layers + touched_gb / (gpus * bw))


def weight_gb(h):
    """Size of the served weights: from the tensor types when each has a
    plain width, else the weight files (packed 4-bit tensors are counted
    by element). Some repos carry the same weights twice."""
    width = {"BF16": 2, "F16": 2, "F32": 4, "F8_E4M3": 1, "F8_E5M2": 1, "I64": 8}
    d = h.get("dtypes") or {}
    if d and all(k in width for k in d): return min(sum(v * width[k] for k, v in d.items()), h["weight_bytes"] or 1e30) / 1e9
    return h["weight_bytes"] / 1e9


def main():
    hf, recipes, ix = load("hf.json"), load("recipes.json")["recipes"], load("inferencex.json")
    try: local = load("local.json")
    except (OSError, ValueError): local = {"models": {}}
    lines = []
    say = lines.append

    # --- memory --------------------------------------------------------
    factors = []
    for repo, r in recipes.items():
        for v in r.get("variants", []):
            h = hf.get(v["model_id"]) or hf.get(v["model_id"].replace("Google/", "google/")) or hf.get(repo.replace("Google/", "google/"))
            if h and h.get("weight_bytes") and v["precision"] in ("bf16", "fp8", "mxfp4") and v["model_id"].lower() == repo.lower():
                w = weight_gb(h); factors.append((repo, v["precision"], round(w, 1), v["vram_minimum_gb"], v["vram_minimum_gb"] / w))
    say("## Memory: vLLM's stated minimum against the published weights\n")
    say("| checkpoint | precision | weights GB | vLLM minimum GB | ratio |\n|---|---|---:|---:|---:|")
    for f in factors: say(f"| {f[0]} | {f[1]} | {f[2]} | {f[3]:.0f} | {f[4]:.2f} |")
    usable = [f[4] for f in factors if 1.0 < f[4] < 1.6]
    say(f"\nMedian ratio {statistics.median(usable):.2f} over {len(usable)} recipes (those between 1.0 and 1.6); models without a recipe get weights x {RECIPE_FACTOR}.\n")

    # KV cache per token of context from the config (two tensors per layer,
    # 16-bit), checked against the pool sizes vLLM reported in the sweeps.
    def kv_kb(cfg):
        L, kvh, hd = cfg.get("num_hidden_layers"), cfg.get("num_key_value_heads"), cfg.get("head_dim")
        if cfg.get("kv_lora_rank"): return L * (cfg["kv_lora_rank"] + 64) * 2 / 1024, "latent attention"
        if not (L and kvh): return None, None
        if not hd: hd = cfg["hidden_size"] // cfg["num_attention_heads"]
        note = "upper bound: part of the layers keep a short window" if cfg.get("sliding_window") else None
        if str(cfg.get("model_type", "")).startswith("qwen3_5"):
            # Measured here on four Qwen3.5 sizes: the memory a token of
            # context takes is a quarter of the all-layers figure.
            return 2 * L * kvh * hd * 2 / 1024 / 4, "a quarter of the layers keep keys and values (measured here on Qwen3.5 0.8B to 9B)"
        return 2 * L * kvh * hd * 2 / 1024, note

    # --- speed ---------------------------------------------------------
    series = ix["series"]

    def sweeps(name, gpu, workload):
        out = [s for s in series if s["model"] == name and s["gpu"] == gpu and s["workload"] == workload and len(s["points"]) >= 3
               and (workload == "agentic" or (s["prompt"] == 1024 and s["answer"] == 1024 and s["speculative"] == "none"))]
        return sorted(out, key=lambda s: (ENGINES.index(s["engine"]) if s["engine"] in ENGINES else 9, s["precision"] != "fp8"))

    def read_speed(s):
        """Prompt tokens read a second and the fixed time before the first
        token, from the same setup with 8,192- and 1,024-token prompts."""
        long = [t for t in series if all(t[k] == s[k] for k in ("model", "gpu", "engine", "precision", "gpus", "speculative")) and t["prompt"] == 8192 and t["answer"] == 1024]
        if not long or not long[0]["points"] or not s["points"]: return None, None
        a, b = s["points"][0]["ttft_s"], long[0]["points"][0]["ttft_s"]
        if not (b > a > 0): return None, None
        read = (8192 - 1024) / (b - a)
        return read, max(0.0, a - 1024 / read) * 1000

    catalog, check_rows, read_ratios = [], [], {"dense": [], "moe": []}
    for m in MODELS:
        h = hf[m["hf"]]; cfg = dict(h["config"]); base = hf.get(m.get("base", m["hf"]), h)
        layers = m.get("layers") or cfg.get("num_hidden_layers")
        weights = weight_gb(h)
        if m.get("fp8"): weights = weights / 2  # served as the FP8 build of the same weights
        params = m.get("params") or h["parameters"] / 1e9
        kind = "dense" if m["active"] is not None and m["active"] > 0.9 * params else "moe"
        dense = kind == "dense"
        touched = None if m["active"] is None else weights * (1.0 if dense else m["active"] / params)
        rec = recipes.get(m.get("recipe", m.get("base", m["hf"])), {}).get("variants", [])
        prec = "fp8" if m.get("fp8") or m["hf"].endswith(("FP8", "MXFP8")) else None
        hit = next((v for v in rec if v["model_id"].lower() == m["hf"].lower() or (prec and v["precision"] == prec)), None)
        if hit and not m.get("fp8"): need, need_src = hit["vram_minimum_gb"], "recipe"
        else: need, need_src = weights * RECIPE_FACTOR, "computed"
        # A recipe for the full-precision checkpoint does not size the FP8 one.
        if hit and hit["precision"] == "bf16" and prec == "fp8": need, need_src = weights * RECIPE_FACTOR, "computed"
        kv, kv_note = kv_kb(cfg) if cfg else (None, None)
        if m["id"].startswith("llama-3"):  # gated repo, config not readable: the published structure
            kv = 2 * layers * 8 * 128 * 2 / 1024
        entry = dict(id=m["id"], label=m["label"], hf=m["hf"], released=base.get("created"), downloads_30d=base.get("downloads_30d"),
                     license=base.get("license"), params_b=round(params, 2), active_b=m["active"], kind="dense" if dense else "moe", layers=layers,
                     context=cfg.get("max_position_embeddings"), weights_gb=round(weights, 1), need_gb=round(need, 1), need_src=need_src,
                     kv_kb_per_token=kv and round(kv, 1), kv_note=kv_note, on={})
        loc = local["models"].get(m.get("local") or "")
        if loc and "write_tps" in loc.get("ctx_2048", {}):
            c = loc["ctx_2048"]
            entry["local"] = dict(file=m["local"], file_gb=round(loc["file_bytes"] / 1e9, 2), ram_gb=round(c["rss_loaded"] / 1e9, 2),
                                  write_tps=c["write_tps"], read_tps=c["read_tps"], kv_kb_per_token=loc.get("bytes_per_context_token") and round(loc["bytes_per_context_token"] / 1024, 1))
        measured = {}
        for gpu in GPUS:
            fit = max(1, math.ceil(need / (GPUS[gpu]["mem"] * FILL)))
            got = None
            for workload in ("single_turn", "agentic"):
                cand = sweeps(m.get("ix"), gpu, workload) if m.get("ix") else []
                if cand:
                    # The fewest GPUs that hold it; a sweep on fewer than our
                    # sizing asks for still ran, so it counts.
                    enough = [s["gpus"] for s in cand if s["gpus"] >= fit]
                    least = min(enough) if enough else max(s["gpus"] for s in cand); s = next(s for s in cand if s["gpus"] == least)
                    # A point whose first token took over 10 s is a server past its limit, not a speed.
                    # Single-turn: where the count times the per-caller speed is well over the total, part of
                    # the count was waiting inside the server, so the speed of one of n is the total over n.
                    each = (lambda p: p["each_tps"]) if workload == "agentic" else (lambda p: p["total_tps"] / p["n"] if p["total_tps"] > 0 and p["n"] * p["each_tps"] > 1.25 * p["total_tps"] else p["each_tps"])
                    pts = monotone([(p["n"], each(p)) for p in s["points"] if p["ttft_s"] <= 10])
                    if len(pts) < 3: continue
                    full = any(p["ttft_s"] > 10 for p in s["points"])
                    read, first = read_speed(s) if workload == "single_turn" else (None, None)
                    got = dict(gpus=s["gpus"], points=pts, full=full, src="public" if workload == "single_turn" else "agentic", read=read, first=first,
                               ref=f"InferenceX {s['model']}, {s['gpus']}x {gpu.upper()}, {s['engine']}, {s['precision']}, {s['dates'][0]}" + (", coding-agent traces" if workload == "agentic" else ", 1k prompt / 1k answer"))
                    break
            if not got and (m["id"], gpu) in ANCHORS:
                a = ANCHORS[(m["id"], gpu)]; got = dict(gpus=a["gpus"], points=a["points"], src="blog", read=None, first=None, ref=a["ref"])
            if got: measured[gpu] = got
        for gpu in GPUS:
            fit = max(1, math.ceil(need / (GPUS[gpu]["mem"] * FILL)))
            g = measured.get(gpu)
            if g:
                solo = solo_of(g["points"]); curve = [(n, e / solo) for n, e in g["points"]]
                if curve[0][0] > 1: curve.insert(0, (1, 1.0))
                row = dict(gpus=g["gpus"], tps=solo, curve=curve, full=g.get("full", False), src=g["src"], ref=g["ref"], read=g["read"], first=g["first"])
                if touched and g["src"] == "public":
                    est = estimate_tps(kind, layers, touched, gpu, g["gpus"])
                    check_rows.append((m["label"], gpu, g["gpus"], solo, est, g["ref"].split(",")[0] + " (carried back to one caller)" if g["points"][0][0] > 1 else g["ref"].split(",")[0]))
                if g["read"]: read_ratios["dense" if dense else "moe"].append(g["read"] / solo)
            elif measured:
                # Same model measured elsewhere: nearest GPU by bandwidth.
                near = min(measured, key=lambda k: abs(math.log(GPUS[k]["bw"] / GPUS[gpu]["bw"])))
                d = measured[near]; solo0 = solo_of(d["points"])
                if touched: ratio = estimate_tps(kind, layers, touched, gpu, fit) / estimate_tps(kind, layers, touched, near, d["gpus"])
                else: ratio = (RUNPOD_7B[gpu] / RUNPOD_7B[near]) * (fit / d["gpus"]) ** 0.3
                curve = [(n, e / solo0) for n, e in d["points"]]
                if curve[0][0] > 1: curve.insert(0, (1, 1.0))
                row = dict(gpus=fit, tps=solo0 * ratio, curve=curve, full=d.get("full", False), src="scaled", ref=f"from {near.upper()} ({d['ref']})", read=d["read"] and d["read"] * ratio, first=d["first"])
            else:
                row = dict(gpus=fit, tps=estimate_tps(kind, layers, touched, gpu, fit), curve=None, src="estimate", ref="bandwidth estimate", read=None, first=None)
            if row["gpus"] > MAX_GPUS: row = dict(row, fits=False)
            entry["on"][gpu] = row
        catalog.append(entry)

    for repo, layers, active, gpu, gpus, tps, ref in CHECKS:
        w = weight_gb(hf[repo]); touched = w * active / (hf[repo]["parameters"] / 1e9) if active else w
        check_rows.append((repo.split("/")[1], gpu, gpus, tps, estimate_tps("moe" if active else "dense", layers, touched, gpu, gpus), ref))

    # Models with no sweep borrow the shape of one that has, on one GPU and
    # without speculative decoding: Llama 3.3 70B (dense), gpt-oss 120B (sparse).
    bands = band_slopes(series)
    by = {e["id"]: e for e in catalog}
    donor = {"dense": by["llama-3.3-70b"]["on"]["h200"], "moe": by["gpt-oss-120b"]["on"]["h200"]}
    ratio = {k: statistics.median(v) for k, v in read_ratios.items() if v}
    for e in catalog:
        for gpu, row in e["on"].items():
            if row["curve"] is None:
                d = donor[e["kind"]]; row["curve"] = d["curve"][:d.get("seen_points", len(d["curve"]))]; row["full"] = d.get("full", False); row["curve_src"] = "borrowed"
            # The sweep as measured ends at `seen`; the rest is carried on.
            row["seen"] = int(row["curve"][-1][0]); row["seen_points"] = len(row["curve"])
            row["curve"] = extend(row["curve"], bands, row.get("full", False))
            if not row["read"]:
                row["read"] = row["tps"] * ratio.get(e["kind"], ratio.get("moe")); row["read_src"] = "ratio"
            if row["first"] is None: row["first"] = 40.0; row["first_src"] = "assumed"
            row["knee"] = knee_of(row["curve"], row["tps"], row["seen"] * (1 if row["src"] == "agentic" or "coding-agent" in row["ref"] else 4))
            row["tps"] = round(row["tps"], 1); row["read"] = round(row["read"]); row["first"] = round(row["first"])
            row["curve"] = [[int(n), round(s, 4)] for n, s in row["curve"]]

    for e in catalog:
        for row in e["on"].values(): row.pop("seen_points", None)

    say("## Past the last measured point\n")
    say("A sweep stops at some count (64 together for most), often while each caller still gets well over 30 tokens a second. The page needs the curve further out, both to plan a server and to say what a server does when more arrive than planned. Each doubling past the last point takes the median slope of the single-turn sweeps that reach that count (slope of one caller's speed against the count; -1 is a total that no longer grows, -0.5 a total that grows by 41% a doubling). It is never gentler than the sweep's own last segment, and a sweep that ended with first tokens past 10 s gets -1.\n")
    say("| doubling ends at | segments | median slope, one caller | total grows by |\n|---:|---:|---:|---:|")
    for n in EXTEND_TO:
        if n == 1024: continue
        sl, cnt = bands[n]; say(f"| {n if n <= 512 else 'past 512'} | {cnt} | {sl:.2f} | {(2 ** (1 + sl) - 1) * 100:+.0f}% |")
    say("")
    say("## Speed estimate against published numbers\n")
    say("One caller alone, tokens a second. The estimate has two constants (time per layer, dense and sparse) and nothing fitted per model.\n")
    say("| model | GPU | published | estimate | error | source |\n|---|---|---:|---:|---:|---|")
    errs = {"dense": [], "moe": []}
    for label, gpu, gpus, tps, est, ref in check_rows:
        kind = next((e["kind"] for e in catalog if e["label"] == label), "moe" if "A3B" in label else "dense")
        errs[kind].append(abs(est / tps - 1))
        say(f"| {label} | {gpus}x {gpu.upper() if isinstance(gpu, str) else f'{gpu} TB/s'} | {tps:.0f} | {est:.0f} | {100 * (est / tps - 1):+.0f}% | {ref} |")
    for kind, v in errs.items():
        if v: say(f"\n{kind}: median error {100 * statistics.median(v):.0f}%, worst {100 * max(v):.0f}% over {len(v)} points.")
    say(f"\nPrompt reading speed over writing speed alone, median of the sweeps that have both prompt lengths: " + ", ".join(f"{k} {v:.0f}x ({len(read_ratios[k])} sweeps)" for k, v in ratio.items()) + ".\n")

    # The total a full server writes, where a write-up gives one.
    g = by["gemma-4-31b"]["on"]["h100"]
    say(f"Whole-server check: Gemma 4 31B on one H100 with 256 requests at once is published at 3,100 tokens a second in total (Spheron, cost-per-token benchmarks, FP16); the list's estimate with the borrowed curve gives {g['tps'] * share_at(g['curve'], 256) * 256:,.0f}. That is past the last measured point of the curve (64), so it also checks the slope assumed there.\n")

    # Context memory: what vLLM said it had room for against weights and structure.
    say("## Context (KV cache) room: reported by vLLM in the sweeps against the computed figure\n")
    say("Computed = (GPUs x memory x 0.9, vLLM's default share, - weights) / KV bytes per token from the model's structure.\n")
    say("| model | GPUs | reported tokens | computed tokens | error |\n|---|---|---:|---:|---:|")
    seen = set()
    for e in catalog:
        m = next(x for x in MODELS if x["id"] == e["id"])
        if not m.get("ix") or not e["kv_kb_per_token"]: continue
        for sr in series:
            pools = [p["kv_pool_tokens"] for p in sr["points"] if p.get("kv_pool_tokens")]
            key = (e["id"], sr["gpu"], sr["gpus"])
            if sr["model"] != m["ix"] or sr["gpu"] not in GPUS or sr["engine"] != "vllm" or sr["workload"] != "single_turn" or not pools or key in seen: continue
            seen.add(key)
            got = statistics.median(pools); calc = (sr["gpus"] * GPUS[sr["gpu"]]["mem"] * 0.9 - e["weights_gb"]) * 1e9 / (e["kv_kb_per_token"] * 1024)
            if calc > 0: say(f"| {e['label']} | {sr['gpus']}x {sr['gpu'].upper()} | {got:,.0f} | {calc:,.0f} | {100 * (calc / got - 1):+.0f}% |")
    say("")

    say("## The list\n")
    say("| model | params (active) B | weights GB | GPU memory GB | KV KB/token | GPUs, alone tok/s, planned together | source of the speed |\n|---|---:|---:|---:|---:|---|---|")
    for e in catalog:
        r = e["on"]["h100"]; g = "H100"
        if r.get("fits") is False: r = e["on"]["h200"]; g = "H200"
        say(f"| {e['label']} | {e['params_b']:.1f} ({e['active_b'] if e['active_b'] else '?'}) | {e['weights_gb']} | {e['need_gb']:.0f} ({e['need_src']}) | {e['kv_kb_per_token'] or '-'} | {r['gpus']}x {g}, {r['tps']:.0f}, {r['knee']} together{'' if r['knee'] <= r['seen'] else f" (measured to {r['seen']})"} | {r['src']}: {r['ref']} |")
    if any("local" in e for e in catalog):
        say("\n## Measured here (4-bit GGUF, llama.cpp, CPU)\n")
        say(f"Host: {local['host']['cpu']}, {local['host']['cores']} cores, {local['host']['ram_gb']} GB, {local['date']}.\n")
        say("| file | file GB | memory held GB (2,048 context) | writes tok/s | reads tok/s | context KB/token measured | all layers, from the file's structure |\n|---|---:|---:|---:|---:|---:|---:|")
        for name, loc in sorted(local["models"].items()):
            c = loc.get("ctx_2048", {})
            if "write_tps" not in c: say(f"| {name} | {loc['file_bytes'] / 1e9:.2f} | did not run: {str(c.get('error'))[-80:]} | | | | |"); continue
            mt = c.get("meta", {}); arch = mt.get("general.architecture", "")
            try: calc = 2 * int(mt[f"{arch}.block_count"]) * int(mt[f"{arch}.attention.head_count_kv"]) * int(mt.get(f"{arch}.attention.key_length", int(mt[f"{arch}.embedding_length"]) // int(mt[f"{arch}.attention.head_count"]))) * 2 / 1024
            except Exception: calc = None
            bpt = loc.get("bytes_per_context_token")
            say(f"| {name} | {loc['file_bytes'] / 1e9:.2f} | {c['rss_loaded'] / 1e9:.2f} | {c['write_tps']:.1f} | {c['read_tps']:.0f} | {bpt / 1024:.1f} | {calc and round(calc, 1)} |" if bpt else f"| {name} | {loc['file_bytes'] / 1e9:.2f} | {c['rss_loaded'] / 1e9:.2f} | {c['write_tps']:.1f} | {c['read_tps']:.0f} | - | {calc and round(calc, 1)} |")

    out = dict(built_from=["hf.json", "recipes.json", "inferencex.json", "local.json"], ms_per_layer=MS_PER_LAYER, recipe_factor=RECIPE_FACTOR,
               gpus={k: dict(v, k=round(RUNPOD_7B[k] / RUNPOD_7B["h100"], 2)) for k, v in GPUS.items()}, read_ratio={k: round(v) for k, v in ratio.items()},
               carry_on={str(n): round(bands[n][0], 3) for n in EXTEND_TO}, models=catalog)
    json.dump(out, open(os.path.join(HERE, "catalog.json"), "w"), indent=1)
    text = "\n".join(lines)
    if "--markdown" in sys.argv: print(text)
    else: print(re.sub(r"\n\|---[^\n]*", "", text))

    if "--page" in sys.argv:
        page = os.path.join(HERE, "..", "..", "web", "monitor.html")
        s = open(page, encoding="utf-8").read()
        slim = dict(gpus=out["gpus"], models=[{k: e[k] for k in ("id", "label", "hf", "params_b", "active_b", "kind", "weights_gb", "need_gb", "need_src", "kv_kb_per_token", "released")} |
                                              {"on": {g: {k: r[k] for k in ("gpus", "tps", "knee", "seen", "curve", "read", "first", "src", "ref")} | ({"curve_src": "borrowed"} if r.get("curve_src") else {}) | ({"fits": False} if r.get("fits") is False else {}) for g, r in e["on"].items()}} |
                                              ({"local": e["local"]} if "local" in e else {}) for e in catalog])
        block = "/*CATALOG-BEGIN (reference/models/build_catalog.py --page; do not edit by hand)*/\nconst CATALOG = " + json.dumps(slim, ensure_ascii=False, separators=(",", ":")) + ";\n/*CATALOG-END*/"
        s2, n = re.subn(r"/\*CATALOG-BEGIN.*?/\*CATALOG-END\*/", lambda _: block, s, flags=re.S)
        if n != 1: sys.exit("monitor.html has no CATALOG block")
        open(page, "w", encoding="utf-8").write(s2)


if __name__ == "__main__":
    main()
