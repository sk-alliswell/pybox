#!/usr/bin/env python3
"""Read the GPU memory each model needs from the vLLM project's own serving
recipes (https://recipes.vllm.ai): `vram_minimum_gb` per published variant.

    python3 fetch_recipes.py > recipes.json

Nothing here was measured by us.
"""
import json, re, sys, time, urllib.request

REPOS = """Qwen/Qwen3.8-27B Qwen/Qwen3.6-35B-A3B Qwen/Qwen3.5-9B Qwen/Qwen3.5-4B Qwen/Qwen3.5-397B-A17B Qwen/Qwen3.8-Flash-Next
Google/gemma-4-31B-it Google/gemma-4-26B-A4B-it Google/gemma-4-12B-it Google/gemma-4-E4B-it
openai/gpt-oss-20b openai/gpt-oss-120b meta-llama/Llama-3.3-70B-Instruct meta-llama/Llama-3.1-8B-Instruct
mistralai/Ministral-3-14B-Instruct-2512 nvidia/NVIDIA-Nemotron-3.5-Lightning-30B-A3B-BF16 nvidia/NVIDIA-Nemotron-3-Super-120B-A12B-BF16
deepseek-ai/DeepSeek-V4.1-Flash deepseek-ai/DeepSeek-V4-Pro zai-org/GLM-5.3-Flash zai-org/GLM-5.3
MiniMaxAI/MiniMax-M3 MiniMaxAI/MiniMax-M3.1-Flash moonshotai/Kimi-K3 moonshotai/Kimi-K3.1-Pro tencent/Hy3""".split()

out = {}
for repo in (sys.argv[1:] or REPOS):
    url = "https://recipes.vllm.ai/" + repo
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 model-catalog"}), timeout=60) as r:
            page = r.read().decode("utf-8", "replace")
    except Exception as e:
        out[repo] = {"error": str(e)[:80]}; print(repo, out[repo], file=sys.stderr); continue
    page = page.replace('\\"', '"')
    seen, variants = set(), []
    for m in re.finditer(r'\{"(?:model_id":"([^"]+)","label":"([^"]*)",")?(?:label":"([^"]*)",")?precision":"([^"]+)","vram_minimum_gb":([\d.]+)', page):
        v = {"model_id": m.group(1) or repo, "label": m.group(2) or m.group(3), "precision": m.group(4), "vram_minimum_gb": float(m.group(5))}
        key = json.dumps(v, sort_keys=True)
        if key not in seen: seen.add(key); variants.append(v)
    out[repo] = {"url": url, "variants": variants}
    print(repo, variants, file=sys.stderr)
    time.sleep(0.5)
json.dump({"source": "https://recipes.vllm.ai", "fetched": time.strftime("%Y-%m-%d"), "recipes": out}, sys.stdout, indent=1)
