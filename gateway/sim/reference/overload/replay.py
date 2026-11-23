#!/usr/bin/env python3
"""Replay the measured overload runs (gateway/lab/OVERLOAD.md) in gwsim.

For each run: the arrivals the driver recorded (who, when), three
single-slot servers, and the gateway policy of the run. The servers' two
speeds (time to first token and total, less the gateway's queue wait) are
fitted on two of the runs and used for all eleven; nothing else is fitted.
Prints and writes measured against predicted (mean and range over seeds).

  replay.py DATA_DIR [--gwsim BIN] [--seeds N] [--out results.json] [--write-reference]

DATA_DIR holds the run folders (`*-over-shed-x20/requests.jsonl`, ...).
`reference.json` keeps the arrivals and measured numbers so the replay can
be repeated without the logs: `replay.py --reference reference.json`.
"""
import argparse
import glob
import json
import statistics as st
import subprocess
import sys
import tempfile
from pathlib import Path

CLASSES = {"ide": (4, 15), "chat": (2, 30), "batch": (1, 60)}
# run -> upstream settings in gwsim's words
RUNS = {
    "over-refuse-x15": {"max_queue": 30},
    "over-evict-x15": {"max_queue": 30, "overflow": "evict"},
    "over-shed-x15": {"max_queue": 30, "shed": {"from": 0.25, "low_ceiling": 0.75}},
    "over-shed-floor-x15": {"max_queue": 30, "shed": {"from": 0.25, "low_ceiling": 0.75, "floor": 0.1}},
    "over-refuse-x20": {"max_queue": 30},
    "over-evict-x20": {"max_queue": 30, "overflow": "evict"},
    "over-shed-x20": {"max_queue": 30, "shed": {"from": 0.25, "low_ceiling": 0.75}},
    "over-shed-floor-x20": {"max_queue": 30, "shed": {"from": 0.25, "low_ceiling": 0.75, "floor": 0.1}},
    "users-turns": {"max_queue": 200},
    "users-least60": {"max_queue": 200, "least_attained_half_life_s": 60},
    "users-least10": {"max_queue": 200, "least_attained_half_life_s": 10},
}


FITTED = ("over-evict-x15", "over-shed-floor-x15")


def pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(p * len(xs)))] if xs else None


def summary(rows):
    """rows: dicts with group, ok, ttft (s or None), refused, gave_up."""
    out = {}
    for g in sorted({r["group"] for r in rows}):
        rs = [r for r in rows if r["group"] == g]
        ttft = [r["ttft"] for r in rs if r["ok"] and r["ttft"] is not None]
        out[g] = {
            "answered": sum(r["ok"] for r in rs) / len(rs),
            "ttft_p50": pct(ttft, 0.5),
            "ttft_p95": pct(ttft, 0.95),
            "gave_up": sum(r["gave_up"] for r in rs) / len(rs),
            "refused": sum(r["refused"] for r in rs) / len(rs),
        }
    return out


def load(data, name):
    d = sorted(glob.glob(f"{data}/*-{name}"))[-1]
    rows = [json.loads(line) for line in open(f"{d}/requests.jsonl")]
    ok = [r for r in rows if r.get("done")]
    prefill = st.median(r["ttft"] - r["queue_ms"] / 1000 for r in ok if r["ttft"])
    total = st.median(r["total"] - r["queue_ms"] / 1000 for r in ok)
    return {
        "name": name,
        "prefill_s": round(prefill, 3),
        "service_s": round(total, 3),
        "arrivals": sorted([r["at"], r["agent"], r["group"]] for r in rows),
        "measured": summary([{"group": r["group"], "ok": bool(r.get("done")), "ttft": r.get("ttft"),
                              "refused": r.get("status") in (429, 503), "gave_up": bool(r.get("gave_up"))} for r in rows]),
    }


def scenario(ref, seed):
    agents = {}
    for at, agent, group in ref["arrivals"]:
        agents.setdefault(agent, (group, []))[1].append(at)
    up = {"name": "local", "models": ["m"], "targets": ["s1", "s2", "s3"], "max_concurrency": 3, **RUNS[ref["name"]]}
    servers = [{"name": f"s{i}", "models": ["m"], "slots": 1, "prefill_ms": ref["prefill_s"] * 1000,
                "tokens_per_s": 24 / (ref["service_s"] - ref["prefill_s"]), "jitter": 0.1, "on_disconnect": "first_token"} for i in (1, 2, 3)]
    clients = []
    for agent, (group, ats) in sorted(agents.items()):
        weight, timeout = CLASSES.get(agent, (1, 45))
        clients.append({"name": agent, "class": group, "agent": agent, "weight": weight, "entry": "gateway", "models": ["m"],
                        "arrival": {"kind": "trace", "rows": [{"t": t, "model": "m"} for t in ats]},
                        "to_s": 200, "output_tokens": 24, "output_sigma": 0.0, "timeout_s": timeout})
    return {"name": ref["name"], "seed": seed, "max_s": 400, "net": {"default_ms": 0},
            "gateway": {"upstream": [up]}, "model_server": servers, "client": clients}


def toml(v, key=""):
    """Enough TOML for the scenario above (inline tables everywhere)."""
    if isinstance(v, dict):
        return "{ " + ", ".join(f"{k} = {toml(x)}" for k, x in v.items()) + " }"
    if isinstance(v, list):
        return "[" + ", ".join(toml(x) for x in v) + "]"
    return json.dumps(v)


def write(sc, path):
    lines = [f"{k} = {toml(v)}" for k, v in sc.items() if not isinstance(v, (dict, list))]
    lines.append(f"net = {toml(sc['net'])}")
    if "host" in sc:
        lines.append(f"host = {toml(sc['host'])}")
    lines.append(f"gateway = {toml(sc['gateway'])}")
    lines.append(f"model_server = {toml(sc['model_server'])}")
    lines.append(f"client = {toml(sc['client'])}")
    Path(path).write_text("\n".join(lines) + "\n")


def predict(ref, gwsim, seeds, tmp):
    runs = []
    for seed in range(1, seeds + 1):
        write(scenario(ref, seed), f"{tmp}/sc.toml")
        subprocess.run([gwsim, "run", f"{tmp}/sc.toml", "--records", f"{tmp}/rec.jsonl"], check=True, capture_output=True)
        group = {a: g for _, a, g in ref["arrivals"]}
        rows = []
        for line in open(f"{tmp}/rec.jsonl"):
            r = json.loads(line)
            rows.append({"group": r["cls"], "ok": r["status"] == 200, "ttft": r.get("ttft"),
                         "refused": r["status"] in (429, 503), "gave_up": r["status"] == -1})
        runs.append(summary(rows))
        assert set(runs[-1]) == set(group.values())
    out = {}
    for g in runs[0]:
        out[g] = {}
        for k in runs[0][g]:
            vals = [r[g][k] for r in runs if r[g][k] is not None]
            out[g][k] = {"mean": st.mean(vals), "min": min(vals), "max": max(vals)} if vals else None
    return out


def errors(results, only=None):
    """Mean absolute error over runs and groups: shares in points, times in %."""
    share, time = [], []
    for r in results:
        if only is not None and (r["run"] in FITTED) != only:
            continue
        for g, m in r["measured"].items():
            q = r["predicted"][g]
            for k in ("answered", "gave_up", "refused"):
                share.append(abs(q[k]["mean"] - m[k]) * 100)
            for k in ("ttft_p50", "ttft_p95"):
                if m[k] and q[k]:
                    time.append(abs(q[k]["mean"] - m[k]) / m[k] * 100)
    return {"share_points_mean": round(st.mean(share), 1), "share_points_worst": round(max(share), 1),
            "time_percent_mean": round(st.mean(time), 1), "time_percent_worst": round(max(time), 1)}


def markdown(results, errs, seeds):
    f = lambda x: "–" if x is None else f"{x:.1f}"
    out = ["| run | group | answered | first token p50 (s) | p95 (s) | gave up | refused |", "|---|---|---|---|---|---|---|"]
    for r in results:
        for g, m in r["measured"].items():
            q = r["predicted"][g]
            mean = lambda k: q[k] and q[k]["mean"]
            out.append(f"| {r['run']}{' (fitted)' if r['run'] in FITTED else ''} | {g} | {m['answered']:.0%} / {mean('answered'):.0%} | "
                       f"{f(m['ttft_p50'])} / {f(mean('ttft_p50'))} | {f(m['ttft_p95'])} / {f(mean('ttft_p95'))} | "
                       f"{m['gave_up']:.0%} / {mean('gave_up'):.0%} | {m['refused']:.0%} / {mean('refused'):.0%} |")
    held = errors(results, only=False)
    head = (f"Measured / predicted (mean of {seeds} seeds). Mean error over all runs: shares {errs['share_points_mean']} points "
            f"(worst {errs['share_points_worst']}), times {errs['time_percent_mean']}% (worst {errs['time_percent_worst']}%). "
            f"Over the nine runs the server speed was not fitted on: shares {held['share_points_mean']} points, times {held['time_percent_mean']}%.\n\n")
    return head + "\n".join(out) + "\n"


def main():
    p = argparse.ArgumentParser()
    p.add_argument("data", nargs="?")
    p.add_argument("--reference", default=str(Path(__file__).with_name("reference.json")))
    p.add_argument("--gwsim", default=str(Path(__file__).resolve().parents[2] / "target/release/gwsim"))
    p.add_argument("--seeds", type=int, default=10)
    p.add_argument("--out", default=str(Path(__file__).with_name("results.json")))
    p.add_argument("--write-reference", action="store_true")
    p.add_argument("--no-floor", action="store_true", help="for a gwsim older than the floor setting")
    p.add_argument("--markdown", help="write the comparison as a table")
    p.add_argument("--own-speed", action="store_true",
                   help="each run with the server speed of its own log instead of the shared fit: how much of a gap is the servers' speed drifting between runs")
    a = p.parse_args()
    if a.data:
        refs = [load(a.data, n) for n in RUNS]
        # Server speed for every run comes from the two runs with the fewest
        # abandoned requests. In the others a request often reached a server
        # still busy with one whose caller had left, which the log shows as
        # a slower server; the simulator models that wait itself.
        fit = [r for r in refs if r["name"] in FITTED]
        prefill = round(st.mean(r["prefill_s"] for r in fit), 3)
        service = round(st.mean(r["service_s"] for r in fit), 3)
        for r in refs:
            r["own_prefill_s"], r["own_service_s"] = r["prefill_s"], r["service_s"]
            r["prefill_s"], r["service_s"] = prefill, service
        if a.write_reference:
            Path(a.reference).write_text(json.dumps(refs) + "\n")
    else:
        refs = json.loads(Path(a.reference).read_text())
    if a.own_speed:
        for r in refs:
            r["prefill_s"], r["service_s"] = r["own_prefill_s"], r["own_service_s"]
        if a.out == str(Path(__file__).with_name("results.json")):
            a.out = str(Path(__file__).with_name("results-own-speed.json"))
    if a.no_floor:
        for v in RUNS.values():
            v.get("shed", {}).pop("floor", None)
    results = []
    with tempfile.TemporaryDirectory() as tmp:
        for ref in refs:
            pred = predict(ref, a.gwsim, a.seeds, tmp)
            results.append({"run": ref["name"], "measured": ref["measured"], "predicted": pred})
            for g, m in ref["measured"].items():
                q = pred[g]
                f = lambda x: "  –  " if x is None else f"{x:5.2f}"
                print(f"{ref['name']:20s} {g:6s} answered {m['answered']:.2f} / {q['answered']['mean']:.2f}   "
                      f"ttft p50 {f(m['ttft_p50'])} / {f(q['ttft_p50'] and q['ttft_p50']['mean'])}   "
                      f"p95 {f(m['ttft_p95'])} / {f(q['ttft_p95'] and q['ttft_p95']['mean'])}   "
                      f"gave up {m['gave_up']:.2f} / {q['gave_up']['mean']:.2f}   refused {m['refused']:.2f} / {q['refused']['mean']:.2f}")
    errs = errors(results)
    print(json.dumps(errs))
    Path(a.out).write_text(json.dumps({"seeds": a.seeds, "errors": errs, "runs": results}, indent=1) + "\n")
    if a.markdown:
        Path(a.markdown).write_text(markdown(results, errs, a.seeds))


if __name__ == "__main__":
    sys.exit(main())
