import time, json, random, sys
from llama_cpp import Llama
llm = Llama(sys.argv[1], n_ctx=8192, n_threads=4, n_batch=512, verbose=False)
random.seed(1)
vocab = [llm.tokenize((" w%d" % i).encode(), add_bos=False)[0] for i in range(2000)]
def toks(n): return [random.choice(vocab) for _ in range(n)]
def read(tokens, keep=0):
    """Seconds to read `tokens` after the first `keep` tokens already in the KV cache."""
    if keep == 0: llm.reset()
    else: llm.n_tokens = keep
    t = time.perf_counter(); llm.eval(tokens); return time.perf_counter() - t
res = {"model": "Qwen2.5-0.5B-Instruct q4_k_m, llama.cpp, 4 CPU threads", "prefill": [], "cached": []}
read(toks(64))
for n in [250, 500, 1000, 2000, 4000, 6000]:
    s = read(toks(n))
    res["prefill"].append({"prompt_tokens": n, "seconds": round(s, 3), "tokens_per_s": round(n / s)})
    print(res["prefill"][-1], flush=True)
read(toks(200)); t = time.perf_counter()
for _ in range(64):
    tok = llm.sample(temp=0.0); llm.eval([tok])
res["gen_tokens_per_s"] = round(64 / (time.perf_counter() - t), 1)
print("gen", res["gen_tokens_per_s"], flush=True)
for n in [1000, 4000]:
    for share in [0.5, 0.7, 0.9, 0.99]:
        cold = read(toks(n)); k = int(n * share)
        warm = read(toks(n - k), keep=k)
        res["cached"].append({"prompt_tokens": n, "shared": share, "cold_s": round(cold, 3), "warm_s": round(warm, 3), "saved_of_shared": round((cold - warm) / (cold * share), 3)})
        print(res["cached"][-1], flush=True)
json.dump(res, open(sys.argv[2], "w"), indent=1)
