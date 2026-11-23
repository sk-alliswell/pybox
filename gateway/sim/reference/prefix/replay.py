#!/usr/bin/env python3
"""Replay the measured prefix-routing runs (gateway/lab/PREFIX.md) in gwsim.

Twelve runs of 2026-11-13: 3 or 6 kinds of work, each with its own long
system prompt (about 620 of 635 prompt tokens), three llama.cpp servers that
each keep the last prompt they read, routing by load (`off`), by prompt
prefix (`on`) and by prefix with a wait of up to 3 s for the server that
holds it (`wait`); two arrival schedules ("" and "-s2", measured in two
sessions of different speed).

Fitted on the four `on` runs and used for all twelve: the time a server
working alone takes to read a whole prompt, to answer one it already holds,
and to write a token, and how much the three servers slow each other down
(they share one 4-vCPU machine: a read took 3.6 s alone, 5.5 s beside one
busy server, 7.6 s beside two). The arrivals are the recorded ones. What is compared is what routing makes of it: the share of prompt
tokens the servers did not have to read again, and the time to first token.

  replay.py DATA_DIR [--gwsim BIN] [--seeds N] [--write-reference]
  replay.py                        # from reference.json
"""
import argparse
import glob
import json
import statistics as st
import subprocess
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "overload"))
from replay import pct, write  # noqa: E402

PROMPT, PREFIX = 635, 620
ARMS = {
    "off": {"routing": "least_loaded"},
    "on": {"routing": "prefix"},
    "wait": {"routing": "prefix", "affinity_wait_s": 3.0},
}
RUNS = [f"prefix-{arm}-g{g}{s}" for s in ("", "-s2") for g in (3, 6) for arm in ARMS]


def busy_others(rows, r, start, end):
    """Mean number of other requests a server was working on during [start, end]."""
    total = 0.0
    for o in rows:
        if o is not r:
            s, e = o["at"] + o["queue_ms"] / 1000, o["at"] + o["total"]
            total += max(0.0, min(end, e) - max(start, s))
    return round(total / max(end - start, 1e-9))


def load(data, name):
    d = sorted(glob.glob(f"{data}/*-{name}"))[-1]
    rows = [json.loads(line) for line in open(f"{d}/requests.jsonl")]
    s = json.load(open(f"{d}/summary.json"))
    ttft = [r["ttft"] for r in rows if r.get("ttft")]
    # Reading a whole prompt and writing a token, by how many other servers
    # were busy meanwhile: the three servers share one 4-vCPU machine and
    # slow each other down.
    cold, token = {}, {}
    for r in rows:
        start = r["at"] + r["queue_ms"] / 1000
        if r.get("gw_warm") == 0:
            cold.setdefault(busy_others(rows, r, start, r["at"] + r["ttft"]), []).append(r["at"] + r["ttft"] - start)
        if r.get("tokens"):
            n = busy_others(rows, r, r["at"] + r["ttft"], r["at"] + r["total"])
            token.setdefault(n, []).append((r["total"] - r["ttft"]) / r["tokens"])
    own = [r["ttft"] - r["queue_ms"] / 1000 for r in rows if r.get("gw_warm", 0) > 500]
    return {
        "name": name,
        "arrivals": sorted([r["at"], r["group"]] for r in rows),
        "output_tokens": round(st.mean(r.get("tokens", 0) for r in rows), 1),
        "cold_by_busy": {k: round(st.median(v), 3) for k, v in sorted(cold.items())},
        "token_by_busy": {k: round(st.median(v), 4) for k, v in sorted(token.items())},
        "warm_s": round(st.median(own), 3) if own else None,
        "measured": {"reused": s["server_reused_share"], "ttft_mean": st.mean(ttft), "ttft_p50": pct(ttft, 0.5), "ttft_p95": pct(ttft, 0.95)},
    }


def arm_of(name):
    return name.split("-")[1]


def scenario(ref, seed):
    groups = {}
    for at, g in ref["arrivals"]:
        groups.setdefault(g, []).append(at)
    up = {"name": "local", "models": ["m"], "targets": ["s1", "s2", "s3"], "max_concurrency": 3, **ARMS[arm_of(ref["name"])]}
    warm_ms = ref["warm_s"] * 1000
    servers = [{"name": f"s{i}", "models": ["m"], "slots": 1, "host": "box", "threads": 1.0, "gen_contention": 0.2, "prefill_ms": warm_ms,
                "prefill_tokens_per_s": PROMPT / max(ref["cold_s"] - ref["warm_s"], 0.1), "prompt_cache_saving": 1.0,
                "tokens_per_s": 1 / ref["s_per_token"], "jitter": 0.1} for i in (1, 2, 3)]
    clients = [{"name": f"g{g}", "class": "all", "agent": "lab", "entry": "gateway", "models": ["m"],
                "arrival": {"kind": "trace", "rows": [{"t": t, "model": "m"} for t in ats]}, "to_s": 400,
                "prompt_tokens": PROMPT, "output_tokens": ref["output_tokens"], "output_sigma": 0.4,
                "prefix": {"share": PREFIX / PROMPT, "sigma": 0.0, "scope": "group", "group": f"g{g}"}}
               for g, ats in sorted(groups.items())]
    return {"name": ref["name"], "seed": seed, "max_s": 900, "net": {"default_ms": 0}, "host": [{"name": "box", "cores": ref["host_cores"]}],
            "gateway": {"upstream": [up]}, "model_server": servers, "client": clients}


def predict(ref, gwsim, seeds, tmp):
    runs = []
    for seed in range(1, seeds + 1):
        write(scenario(ref, seed), f"{tmp}/sc.toml")
        subprocess.run([gwsim, "run", f"{tmp}/sc.toml", "--records", f"{tmp}/rec.jsonl", "--json", f"{tmp}/rep.json"], check=True, capture_output=True)
        rep = json.load(open(f"{tmp}/rep.json"))
        total, saved = rep["prefix_tokens"]
        ttft = [json.loads(line).get("ttft") for line in open(f"{tmp}/rec.jsonl")]
        ttft = [t for t in ttft if t is not None]
        runs.append({"reused": saved / max(total, 1), "ttft_mean": st.mean(ttft), "ttft_p50": pct(ttft, 0.5), "ttft_p95": pct(ttft, 0.95)})
    return {k: {"mean": st.mean(r[k] for r in runs), "min": min(r[k] for r in runs), "max": max(r[k] for r in runs)} for k in runs[0]}


def main():
    p = argparse.ArgumentParser()
    p.add_argument("data", nargs="?")
    p.add_argument("--reference", default=str(Path(__file__).with_name("reference.json")))
    p.add_argument("--gwsim", default=str(Path(__file__).resolve().parents[2] / "target/release/gwsim"))
    p.add_argument("--seeds", type=int, default=10)
    p.add_argument("--out", default=str(Path(__file__).with_name("results.json")))
    p.add_argument("--markdown")
    p.add_argument("--write-reference", action="store_true")
    p.add_argument("--skip", default="", help="arms to leave out, comma separated")
    a = p.parse_args()
    if a.data:
        refs = [load(a.data, n) for n in RUNS]
        # One fit for all twelve runs, from the four `on` runs (their log
        # says which requests found nothing to reuse): the speeds of a
        # server working alone, and how many servers' worth of work the
        # machine does at full speed.
        on = [r for r in refs if "-on-" in r["name"]]
        alone = st.mean(r["cold_by_busy"][0] for r in on)
        cores = st.mean(n / (r["cold_by_busy"][n - 1] / r["cold_by_busy"][0]) for r in on for n in (2, 3))
        fit = {"cold_s": round(alone, 3), "host_cores": round(cores, 3), "warm_s": round(st.mean(r["warm_s"] for r in on), 3),
               "s_per_token": round(st.mean(r["token_by_busy"][0] for r in on), 4)}
        print("fit:", fit)
        for r in refs:
            r.update(fit)
        if a.write_reference:
            Path(a.reference).write_text(json.dumps(refs) + "\n")
    else:
        refs = json.loads(Path(a.reference).read_text())
    results = []
    lines = ["| run | prompt tokens not read again | first token mean (s) | p50 (s) | p95 (s) |", "|---|---|---|---|---|"]
    with tempfile.TemporaryDirectory() as tmp:
        for ref in refs:
            if arm_of(ref["name"]) in a.skip.split(","):
                continue
            q, m = predict(ref, a.gwsim, a.seeds, tmp), ref["measured"]
            results.append({"run": ref["name"], "measured": m, "predicted": q})
            print(f"{ref['name']:20s} reused {m['reused']:.2f} / {q['reused']['mean']:.2f} ({q['reused']['min']:.2f}..{q['reused']['max']:.2f})   "
                  f"ttft mean {m['ttft_mean']:5.2f} / {q['ttft_mean']['mean']:5.2f}   p50 {m['ttft_p50']:5.2f} / {q['ttft_p50']['mean']:5.2f}   p95 {m['ttft_p95']:5.2f} / {q['ttft_p95']['mean']:5.2f}")
            lines.append(f"| {ref['name']} | {m['reused']:.0%} / {q['reused']['mean']:.0%} | {m['ttft_mean']:.1f} / {q['ttft_mean']['mean']:.1f} | "
                         f"{m['ttft_p50']:.1f} / {q['ttft_p50']['mean']:.1f} | {m['ttft_p95']:.1f} / {q['ttft_p95']['mean']:.1f} |")
    Path(a.out).write_text(json.dumps({"seeds": a.seeds, "runs": results}, indent=1) + "\n")
    if a.markdown:
        Path(a.markdown).write_text("\n".join(lines) + "\n")


if __name__ == "__main__":
    sys.exit(main())
