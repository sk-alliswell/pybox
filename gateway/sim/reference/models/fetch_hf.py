#!/usr/bin/env python3
"""Fetch model facts from the Hugging Face hub: parameter count, size of the
published weights, and the config fields that set the KV cache size.

    python3 fetch_hf.py > hf.json

Nothing here is measured by us; it is what the publisher uploaded.
"""
import json, sys, urllib.request

REPOS = """
Qwen/Qwen3.8-27B Qwen/Qwen3.8-27B-FP8 Qwen/Qwen3.6-35B-A3B Qwen/Qwen3.6-35B-A3B-FP8
Qwen/Qwen3.5-397B-A17B Qwen/Qwen3.5-397B-A17B-FP8 Qwen/Qwen3.8-Flash-Next Qwen/Qwen3.8-Flash-Next-FP8 Qwen/Qwen3.6-27B-FP8 Qwen/Qwen2.5-7B-Instruct
Qwen/Qwen3.5-9B Qwen/Qwen3.5-4B Qwen/Qwen3.5-2B Qwen/Qwen3.5-0.8B Qwen/Qwen3-Coder-30B-A3B-Instruct
deepseek-ai/DeepSeek-V4.1-Flash-1017 deepseek-ai/DeepSeek-V4.1-Flash deepseek-ai/DeepSeek-V4-Pro-0813 deepseek-ai/DeepSeek-V3.2
google/gemma-4-31B-it google/gemma-4-26B-A4B-it google/gemma-4-12B-it google/gemma-4-E4B-it google/gemma-4-E2B-it
openai/gpt-oss-20b openai/gpt-oss-120b
zai-org/GLM-5.3-Flash zai-org/GLM-5.3 zai-org/GLM-5.3-BF16
moonshotai/Kimi-K3 moonshotai/Kimi-K3.1-Pro
MiniMaxAI/MiniMax-M3 MiniMaxAI/MiniMax-M3-MXFP8 MiniMaxAI/MiniMax-M3.1-Flash
mistralai/Ministral-3-14B-Instruct-2512 mistralai/Ministral-3-14B-Instruct-2512-BF16 mistralai/Ministral-3-8B-Instruct-2512 mistralai/Mistral-Small-4-119B-2603
nvidia/NVIDIA-Nemotron-3.5-Lightning-30B-A3B-BF16 nvidia/NVIDIA-Nemotron-3.5-Lightning-30B-A3B-NVFP4 RedHatAI/Meta-Llama-3.3-70B-Instruct-FP8 RedHatAI/gemma-4-31B-it-FP8-dynamic nvidia/NVIDIA-Nemotron-3-Super-120B-A12B-BF16 nvidia/NVIDIA-Nemotron-3-Nano-4B-BF16
ornith-ai/Ornith-1.5-9B ornith-ai/Ornith-1.5-35B-A3B ornith-ai/Ornith-1.5-122B ornith-ai/Ornith-1.5-122B-FP8
tencent/Hy3 tencent/Hy3-FP8
meta-llama/Llama-3.3-70B-Instruct meta-llama/Llama-3.1-8B-Instruct meta-llama/Llama-4-Scout-17B-16E-Instruct
microsoft/phi-4 microsoft/Phi-4-mini-instruct
""".split()

def get(url):
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "model-catalog"}), timeout=40) as r:
            return json.load(r)
    except Exception as e:
        return {"_error": str(e)[:80]}

KEYS = ["num_hidden_layers", "hidden_size", "num_attention_heads", "num_key_value_heads", "head_dim",
        "max_position_embeddings", "vocab_size", "model_type", "num_experts", "num_experts_per_tok",
        "num_local_experts", "n_routed_experts", "sliding_window", "torch_dtype", "dtype", "kv_lora_rank", "architectures"]

out = {}
for repo in (sys.argv[1:] or REPOS):
    info = get(f"https://huggingface.co/api/models/{repo}?blobs=true")
    if "_error" in info:
        out[repo] = info; print(repo, info, file=sys.stderr); continue
    sib = info.get("siblings", [])
    # Weight files at the top of the repo: some repos carry a second copy
    # in a subfolder (gpt-oss has `original/` and `metal/`).
    weights = sum(s.get("size") or 0 for s in sib if "/" not in s["rfilename"] and s["rfilename"].endswith((".safetensors", ".bin")))
    cfg = get(f"https://huggingface.co/{repo}/resolve/main/config.json")
    flat = dict(cfg)
    for sub in ("text_config", "llm_config", "language_config"):
        if isinstance(cfg.get(sub), dict): flat = {**flat, **cfg[sub]}
    st = info.get("safetensors") or {}
    out[repo] = {
        "downloads_30d": info.get("downloads"), "created": (info.get("createdAt") or "")[:10],
        "license": (info.get("cardData") or {}).get("license"), "gated": info.get("gated"),
        "parameters": st.get("total"), "dtypes": st.get("parameters"),
        "weight_bytes": weights,
        "config": {k: flat[k] for k in KEYS if k in flat}, "config_error": cfg.get("_error"),
    }
    o = out[repo]; print(repo, o["parameters"], round(weights / 1e9, 1), o["config"], file=sys.stderr)
json.dump(out, sys.stdout, indent=1)
