#!/usr/bin/env python3
"""Measure open models here, on CPU: memory held and tokens a second.

    pip install llama-cpp-python
    python3 measure_local.py DIR_WITH_GGUF > local.json

For every .gguf file in the folder, in a fresh process each:
  - memory held (resident set) with a 2,048- and a 16,384-token context, the
    weights loaded without mmap, so the difference is what the context costs;
  - prompt reading speed on a 1,024-token prompt;
  - writing speed over 128 tokens, one request alone.
These are the only numbers in the catalogue that we measured ourselves.
"""
import json, os, subprocess, sys, time

CHILD = r"""
import json, os, sys, time, resource
import llama_cpp
path, n_ctx, run = sys.argv[1], int(sys.argv[2]), sys.argv[3] == "run"
def rss():
    for line in open("/proc/self/status"):
        if line.startswith("VmRSS:"): return int(line.split()[1]) * 1024
t = time.time()
m = llama_cpp.Llama(path, n_ctx=n_ctx, n_threads=os.cpu_count(), n_batch=512, use_mmap=False, verbose=False)
out = {"n_ctx": n_ctx, "load_s": round(time.time() - t, 2), "rss_loaded": rss(), "meta": {k: v for k, v in m.metadata.items() if k.endswith((".block_count", ".embedding_length", ".head_count", ".head_count_kv", ".key_length", "general.architecture", "general.name", "general.size_label", "general.file_type"))}}
if run:
    words = "the gateway keeps a queue for every model and sends each request to the server with the most room while it watches how long callers wait ".split()
    text = " ".join(words[(i * 7 + i // 5) % len(words)] for i in range(4000))
    toks = m.tokenize(text.encode())[:1024]
    t = time.time(); m.eval(toks); read_s = time.time() - t
    out["read_tokens"] = len(toks); out["read_tps"] = round(len(toks) / read_s, 1)
    n = 0; t = time.time()
    for tok in m.generate([], reset=False, temp=0.8, top_k=40):
        n += 1
        if n >= 128: break
    out["write_tokens"] = n; out["write_tps"] = round(n / (time.time() - t), 2)
    out["rss_after"] = rss()
print("RESULT " + json.dumps(out))
"""

def child(path, n_ctx, run):
    p = subprocess.run([sys.executable, "-c", CHILD, path, str(n_ctx), "run" if run else "load"], capture_output=True, text=True)
    for line in p.stdout.splitlines():
        if line.startswith("RESULT "): return json.loads(line[7:])
    return {"error": (p.stderr or p.stdout)[-300:]}

def main():
    folder = sys.argv[1]
    cpu = next((l.split(":")[1].strip() for l in open("/proc/cpuinfo") if l.startswith("model name")), "")
    out = {"host": {"cpu": cpu, "cores": os.cpu_count(), "ram_gb": round(os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") / 1e9, 1)}, "date": time.strftime("%Y-%m-%d"), "models": {}}
    for name in sorted(os.listdir(folder)):
        if not name.endswith(".gguf"): continue
        path = os.path.join(folder, name); size = os.path.getsize(path)
        small = child(path, 2048, True)
        big = child(path, 16384, False) if "error" not in small else {}
        row = {"file_bytes": size, "ctx_2048": small, "ctx_16384": big}
        if "rss_loaded" in small and "rss_loaded" in big:
            row["bytes_per_context_token"] = round((big["rss_loaded"] - small["rss_loaded"]) / (16384 - 2048))
        out["models"][name] = row
        print(name, json.dumps(row)[:400], file=sys.stderr, flush=True)
    json.dump(out, sys.stdout, indent=1)

if __name__ == "__main__":
    main()
