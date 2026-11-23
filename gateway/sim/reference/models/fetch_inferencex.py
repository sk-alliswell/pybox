#!/usr/bin/env python3
"""Reduce the public InferenceX benchmark rows (SemiAnalysis, open data,
https://inferencex.semianalysis.com/api) to what the simulator needs: for a
model on a kind of GPU, how fast one caller gets tokens while n callers are
served together, how much the whole server writes, and the time to the first
token.

    python3 fetch_inferencex.py [--cache DIR] > inferencex.json

Runs on one node of at most eight GPUs without a separate prefill tier are
kept: single-turn runs (fixed prompt and answer lengths) and, marked
`agentic`, replays of coding-agent traces, where n counts users. For every
model, GPU, engine, precision, GPU count and prompt/answer length the newest
row of each concurrency is taken. Nothing here was measured by us.
"""
import collections, gzip, json, os, sys, time, urllib.request

BASE = "https://inferencex.semianalysis.com/api/v1/benchmarks?model="
MODELS = ["DeepSeek-R1-0528", "DeepSeek-V4-Pro", "DeepSeek-V4.1-Flash", "GLM-5", "GLM-5.2", "GLM-5.3-Flash", "Kimi-K2.5",
          "Kimi-K3", "Kimi-K3.1-Pro", "Llama-3.3-70B-Instruct-FP8", "MiniMax-M2.5", "MiniMax-M3", "MiniMax-M3.1-Flash",
          "Muse-Spark-Open", "Qwen-3.5-397B-A17B", "Qwen3.8-27B", "Qwen3.8-Flash-Next", "gpt-oss-120b"]
GPUS = {"h100", "h200", "b200", "b300", "rtx6000pro"}

def rows(model, cache):
    path = cache and os.path.join(cache, model + ".json")
    if path and os.path.exists(path): return json.load(open(path))
    req = urllib.request.Request(BASE + model, headers={"Accept-Encoding": "gzip", "User-Agent": "model-catalog"})
    with urllib.request.urlopen(req, timeout=120) as r:
        body = r.read()
        if r.headers.get("Content-Encoding") == "gzip": body = gzip.decompress(body)
    time.sleep(1)
    return json.loads(body)

def main():
    cache = sys.argv[sys.argv.index("--cache") + 1] if "--cache" in sys.argv else None
    series, agentic = collections.defaultdict(dict), collections.Counter()
    for name in MODELS:
        for r in rows(name, cache):
            if r["hardware"] not in GPUS: continue
            if r.get("disagg") or r.get("is_multinode") or r["num_decode_gpu"] > 8: continue
            # Agentic coding traces: long prompts that are mostly cached,
            # and n counts users, who also wait on their tools.
            turn = r["benchmark_type"] == "single_turn"
            if not turn: agentic[(name, r["hardware"])] += 1
            k = (name, r["hardware"], r["framework"], r["precision"], r["num_decode_gpu"], r["isl"] if turn else 0, r["osl"] if turn else 0, r.get("spec_method") or "none")
            old = series[k].get(r["conc"])
            if old is None or r["date"] > old["date"]: series[k][r["conc"]] = r
    out = []
    for k, by in sorted(series.items(), key=lambda kv: tuple(map(str, kv[0]))):
        if len(by) < 3: continue
        pts = []
        for c in sorted(by):
            m = by[c]["metrics"]
            if not m.get("median_intvty") or not m.get("output_tput_per_gpu"): continue
            pts.append({"n": c, "each_tps": round(m["median_intvty"], 1), "total_tps": round(m["output_tput_per_gpu"] * k[4], 0),
                        "ttft_s": round(m.get("median_ttft") or 0, 3), **({"kv_pool_tokens": m["kv_cache_pool_tokens"]} if m.get("kv_cache_pool_tokens") else {})})
        out.append({"model": k[0], "gpu": k[1], "engine": k[2], "precision": k[3], "gpus": k[4], "workload": "single_turn" if k[5] else "agentic",
                    "prompt": k[5] or round(next(iter(by.values()))["metrics"].get("mean_input_tokens") or 0), "answer": k[6] or round(next(iter(by.values()))["metrics"].get("mean_output_tokens_actual") or 0), "speculative": k[7],
                    "dates": sorted({r["date"] for r in by.values()})[::max(1, len(by) - 1)], "image": next(iter(by.values())).get("image"), "points": pts})
    json.dump({"source": "https://inferencex.semianalysis.com/api/v1/benchmarks", "fetched": time.strftime("%Y-%m-%d"),
               "series": out}, sys.stdout, indent=0)

if __name__ == "__main__":
    main()
