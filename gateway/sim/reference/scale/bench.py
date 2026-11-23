#!/usr/bin/env python3
"""How large a run gwsim carries: time, memory and events by size.

Builds one system at several sizes (people in three teams doing ide, chat
and batch work once a shortened day; model servers for 1.3 times the demand
of the busiest minute; one gateway sending straight to the servers) and runs
each with `gwsim run --json`, timing it and reading the peak memory of the
process. With --stride it runs each size again with that `token_stride` and
reports how far the results move.

  bench.py [--gwsim BIN] [--sizes 500,3000,10000] [--days 1] [--stride 16] [--routing least_loaded] [--markdown OUT.md]

Prompt and answer lengths are the monitor's defaults (from the Qwen-Bailian
traces); model speeds are assumed. This measures the simulator, not a
system.
"""
import argparse
import json
import resource
import subprocess
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "overload"))
from replay import write  # noqa: E402

DAY_S = 1440.0
# name, model, prompt tokens, share of it that repeats, answer tokens, weight, timeout (s), requests a minute at the busiest
KINDS = [("ide", "small", 5700, 0.69, 470, 4, 120, 3.0), ("chat", "large", 2300, 0.67, 380, 4, 120, 1.0), ("batch", "large", 900, 0.57, 40, 1, 300, 0.3)]
MODELS = {"small": (90.0, 8, 120.0), "large": (45.0, 8, 250.0)}  # tokens/s, slots, ms before the first token
READ_PER_OUT, SCALING, ROOM = 30.0, 0.85, 1.3


def scenario(users, days, stride, routing):
    servers, ups = [], []
    for model, (tps, slots, prefill) in MODELS.items():
        per_slot = tps * slots ** SCALING / slots
        slot_s = sum(users * rate / 60 * (out / per_slot + inp * (1 - share * 0.5) / (tps * READ_PER_OUT) + prefill / 1000)
                     for _, m, inp, share, out, _, _, rate in KINDS if m == model)
        n = max(1, -(-int(slot_s * ROOM * 100) // (slots * 100)))
        servers.append({"name": f"gpu-{model}", "count": n, "models": [model], "slots": slots, "tokens_per_s": tps, "batch_scaling": SCALING,
                        "prefill_ms": prefill, "prefill_tokens_per_s": tps * READ_PER_OUT, "prompt_cache_saving": 0.93, "jitter": 0.1})
        cap = n * slots
        up = {"name": model, "models": [model], "targets": [f"gpu-{model}"], "routing": routing, "max_concurrency": cap, "max_queue": 2 * cap}
        if routing == "prefix":
            up["prefix_slots"] = slots
        ups.append(up)
    clients = []
    for team, share in (("platform", 0.4), ("research", 0.3), ("product", 0.3)):
        for kind, model, inp, rep, out, weight, timeout, rate in KINDS:
            clients.append({"name": f"{team}~{kind}", "class": f"{team} · {kind}", "count": int(users * share), "weight": weight, "entry": "gateway",
                            "models": [model], "arrival": {"kind": "poisson", "rate_per_s": rate / 60}, "to_s": DAY_S * days,
                            "prompt_tokens": inp, "output_tokens": out, "output_sigma": 0.4, "timeout_s": timeout,
                            "prefix": {"share": rep, "sigma": 0.3, "scope": "caller", "group": kind, "group_share": rep * 0.6},
                            "rhythm": {"period_s": DAY_S, "busy_at_s": DAY_S * 13 / 24, "busy_width_s": DAY_S * 9 / 24, "quiet": 0.1}})
    return {"name": f"scale-{users}", "seed": 1, "max_s": DAY_S * days + 300, "token_stride": stride, "sample_ms": 5000,
            "net": {"default_ms": 0}, "gateway": {"cores": 64, "upstream": ups}, "model_server": servers, "client": clients}


def run(gwsim, sc, tmp):
    write(sc, f"{tmp}/sc.toml")
    before = resource.getrusage(resource.RUSAGE_CHILDREN)
    t = time.time()
    subprocess.run([gwsim, "run", f"{tmp}/sc.toml", "--json", f"{tmp}/rep.json"], check=True, capture_output=True)
    wall = time.time() - t
    after = resource.getrusage(resource.RUSAGE_CHILDREN)
    rep = json.load(open(f"{tmp}/rep.json"))
    cls = [c for c in rep["classes"] if c["phase"] == "all"]
    sent = sum(c["sent"] for c in cls)
    return {"wall": wall, "cpu": after.ru_utime - before.ru_utime, "rss_mb": after.ru_maxrss / 1024, "events": rep["events"], "sent": sent,
            "answered": sum(c["ok"] for c in cls) / max(sent, 1),
            "ttft_p50": max(c.get("ttft_p50") or 0 for c in cls), "ttft_p95": max(c.get("ttft_p95") or 0 for c in cls),
            "servers": sum(s["count"] for s in sc["model_server"])}


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--gwsim", default=str(Path(__file__).resolve().parents[2] / "target/release/gwsim"))
    p.add_argument("--sizes", default="50,500,3000,10000")
    p.add_argument("--days", type=float, default=1)
    p.add_argument("--stride", type=int, default=16)
    p.add_argument("--routing", default="least_loaded")
    p.add_argument("--markdown")
    a = p.parse_args()
    lines = ["| people | model servers | days | requests | token stride | events (million) | time (s) | million events/s | peak memory (MB) | answered | first token p50 / p95 (s, slowest class) |",
             "|---|---|---|---|---|---|---|---|---|---|---|"]
    with tempfile.TemporaryDirectory() as tmp:
        for users in [int(x) for x in a.sizes.split(",")]:
            for stride in dict.fromkeys([1, a.stride]):
                r = run(a.gwsim, scenario(users, a.days, stride, a.routing), tmp)
                # ru_maxrss of the children is the largest child so far, so sizes go up.
                line = (f"| {users:,} | {r['servers']} | {a.days:g} | {r['sent']:,} | {stride} | {r['events'] / 1e6:.1f} | {r['wall']:.1f} | "
                        f"{r['events'] / 1e6 / max(r['cpu'], 1e-9):.1f} | {r['rss_mb']:.0f} | {r['answered']:.1%} | {r['ttft_p50']:.2f} / {r['ttft_p95']:.2f} |")
                print(line, flush=True)
                lines.append(line)
    if a.markdown:
        Path(a.markdown).write_text("\n".join(lines) + "\n")


if __name__ == "__main__":
    sys.exit(main())
