"""Policies from the prior-art survey, tried in gwsim before any is ported.

    python3 reference/prior-art/experiments.py target/release/gwsim OUT.json

The workload takes prompt lengths, output lengths and how much of a prompt
repeats from the Qwen-Bailian traces (qwen-bailian-repeat.json): coder as
"ide", to-C chat as "chat", to-B API as "batch". Server speeds, the number of
people and their request rates are assumptions.
"""
import copy, json, subprocess, sys, tempfile, os, statistics as st

GWSIM, OUT = sys.argv[1], sys.argv[2]
SEEDS = [11, 12, 13]

def base(load=1.0, routing="least_loaded", traced=True, hot=False, entries=None, heavy=False, patient=True, queue=96, batch_timeout=300):
    def client(name, cls, n, per_min, inp, out, sigma, weight, timeout, prefix):
        return {"name": name, "class": cls, "count": n, "entry": "gateway", "models": ["m"], "weight": weight,
                "arrival": {"kind": "poisson", "rate_per_s": per_min / 60 * load}, "to_s": 600,
                "prompt_tokens": inp, "output_tokens": out, "output_sigma": sigma, "timeout_s": timeout, "prefix": prefix}
    if traced:
        ide = {"share": 0.69, "sigma": 0.3, "scope": "caller", "group_share": 0.42, "group": "ide", "follow_up": 0.39}
        chat = {"share": 0.67, "sigma": 0.3, "scope": "caller", "group_share": 0.52, "group": "chat", "follow_up": 0.46}
        tmpl = lambda k: {"share": 0.57, "sigma": 0.3, "scope": "group", "group": f"batch-{k}"}
    else:  # what the setup page assumed before the traces
        ide = {"share": 0.9, "sigma": 0.15, "scope": "caller"}
        chat = {"share": 0.7, "sigma": 0.15, "scope": "caller"}
        tmpl = lambda k: {"share": 0.4, "sigma": 0.15, "scope": "group", "group": "batch"}
    clients = [client("ide", "ide", 60, 1.2, 5700, 470, 0.8, 4, 120 if patient else 20, ide),
               client("chat", "chat", 40, 0.8, 2300, 380, 0.6, 4, 120 if patient else 40, chat)]
    if hot:  # one template takes most of the batch traffic
        clients.append(client("batch-hot", "batch", 30, 12, 900, 40, 0.8, 1, batch_timeout, tmpl(0)))
        clients += [client(f"batch-{k}", "batch", 2, 6, 900, 40, 0.8, 1, batch_timeout, tmpl(k)) for k in range(1, 6)]
    else:
        clients += [client(f"batch-{k}", "batch", 5, 6, 900, 40, 0.8, 1, batch_timeout, tmpl(k)) for k in range(8)]
    if heavy:  # three people who send ten times what the others do
        clients.append(client("ide-heavy", "ide-heavy", 3, 12, 5700, 470, 0.8, 4, 120, ide))
    srv = {"name": "srv", "count": 12, "models": ["m"], "slots": 4, "tokens_per_s": 60, "batch_scaling": 0.85,
           "prefill_ms": 100, "prefill_tokens_per_s": 1800, "prompt_cache_saving": 0.93}
    if entries: srv["prefix_cache_entries"] = entries
    up = {"name": "m", "models": ["m"], "targets": ["srv"], "routing": routing, "headroom": 1.0, "max_queue": queue}
    return {"name": "prior-art", "seed": 11, "max_s": 900, "gateway": {"upstream": [up]}, "model_server": [srv], "client": clients}

def run(sc):
    rows = []
    for seed in SEEDS:
        with tempfile.TemporaryDirectory() as d:
            p, o = os.path.join(d, "s.json"), os.path.join(d, "o.json")
            json.dump(sc, open(p, "w"))
            subprocess.run([GWSIM, "run", p, "--seed", str(seed), "--json", o], check=True, stdout=subprocess.DEVNULL)
            rows.append(json.load(open(o)))
    def cls(name, key):
        v = [c[key] for r in rows for c in r["classes"] if c["class"] == name and c["phase"] == "all" and c[key] is not None]
        return round(st.mean(v), 3) if v else None
    mean = lambda f: round(st.mean(f(r) for r in rows), 3)
    waits = [c["fail_wait_p50"] for r in rows for c in r["classes"] if c["class"] in ("ide", "chat") and c["phase"] == "all" and c["fail_wait_p50"] is not None]
    out = {"interactive_fail_wait_p50": round(st.mean(waits), 2) if waits else None, "reuse_pct": mean(lambda r: 100 * r["prefix_tokens"][1] / max(r["prefix_tokens"][0], 1)),
           "error_pct": mean(lambda r: r["scores"][0]), "tokens_per_s": mean(lambda r: r["scores"][1]),
           "wait_mean_s": mean(lambda r: r["scores"][3]), "shed_early": mean(lambda r: r["shed_early"])}
    for c in sorted({c["class"] for c in rows[0]["classes"]}):
        out[c] = {"ok_pct": round(100 * cls(c, "ok_share"), 1), "ttft_p50": cls(c, "ttft_p50"), "ttft_p95": cls(c, "ttft_p95"),
                  "gave_up": cls(c, "gave_up"), "refused": cls(c, "refused")}
    return out

def up(sc): return sc["gateway"]["upstream"][0]
res = {}
ONLY = sys.argv[3] if len(sys.argv) > 3 else ""
def exp(group, name, sc):
    if ONLY not in group: return
    res.setdefault(group, {})[name] = run(sc); print(group, name, json.dumps(res[group][name]), flush=True)

# 1. Where requests are sent, with the llama.cpp slot cache.
for load in [1.0, 1.5]:
    for hot in [False, True]:
        g = f"routing load={load}{' hot-template' if hot else ''}"
        for r in ["random", "least_loaded", "affinity", "cost"]:
            exp(g, r, base(load, r, hot=hot))
        for at in [0.25, 0.5, 1.0]:
            sc = base(load, "affinity", hot=hot); up(sc)["replicate_at"] = at; exp(g, f"affinity replicate_at={at}", sc)
        for w in [0.25, 4.0]:
            sc = base(load, "cost", hot=hot); up(sc)["cost_load_weight"] = w; exp(g, f"cost load_weight={w}", sc)
        sc = base(load, "cost", hot=hot); up(sc)["cost_in_tokens"] = True; exp(g, "cost counted in tokens", sc)
# 2. The same under a shared cache of 16 prefixes per server (vLLM-like).
for r in ["least_loaded", "affinity", "cost"]:
    exp("routing load=1.0 shared-cache", r, base(1.0, r, entries=16))
# 3. Assumed shares against the traces.
for traced in [False, True]:
    for r in ["least_loaded", "cost"]:
        exp("assumed vs traced shares", f"{'traced' if traced else 'assumed'} {r}", base(1.0, r, traced=traced))
# 4. Shedding under overload.
for load in [1.5, 2.0]:
    g = f"shedding load={load}"
    exp(g, "refuse when full", base(load, "cost"))
    sc = base(load, "cost"); up(sc)["overflow"] = "evict"; exp(g, "evict lower weight when full", sc)
    for frm, ceil in [(0.25, 0.75), (0.5, 0.9), (0.5, 1.0), (0.75, 1.0), (0.1, 0.5)]:
        sc = base(load, "cost"); up(sc)["shed"] = {"from": frm, "low_ceiling": ceil}; exp(g, f"shed from={frm} ceiling={ceil}", sc)
# 5. Early refusal of requests that cannot make their deadline: callers who
# give up after 20 s (ide), 40 s (chat) and 45 s (batch), and a queue long
# enough to wait longer than that.
for load in [1.5, 2.0]:
    g = f"deadline load={load}"
    kw = dict(patient=False, queue=384, batch_timeout=45)
    exp(g, "wait in queue", base(load, "cost", **kw))
    sc = base(load, "cost", **kw); up(sc)["admit_by_deadline"] = True; exp(g, "refuse if the wait is longer than the time left", sc)
    sc = base(load, "cost", **kw); up(sc)["shed"] = {"from": 0.25, "low_ceiling": 0.75}; exp(g, "gradual shedding only", sc)
    sc = base(load, "cost", **kw); up(sc)["admit_by_deadline"] = True; up(sc)["shed"] = {"from": 0.25, "low_ceiling": 0.75}; exp(g, "both", sc)
# 6. Least attained service with decay, with three heavy users.
for load in [1.0, 1.5]:
    g = f"attained service load={load}"
    exp(g, "round robin", base(load, "cost", heavy=True))
    for h in [10, 60, 600]:
        sc = base(load, "cost", heavy=True); up(sc)["least_attained_half_life_s"] = h; exp(g, f"least attained, half-life {h}s", sc)
json.dump(res, open(OUT, "w"), indent=1)
