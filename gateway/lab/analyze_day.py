#!/usr/bin/env python3
"""Where a realistic.py day spent its money and its time.

Reads the client-side JSONL and the gateway's own log (query API) and prints
one JSON document: spend by persona, where the time went, and how much each
kind of waste cost.

Token totals of two days only compare when the same work was done, and a
day in which a persona mostly failed is not a result at all. So the
scoreboard also says how much work was completed, what it cost per unit, what
this day's unit costs would add up to for the baseline's amount of work
(`--baseline`), and whether the day is `valid`: under 2% errors (the loop
guard's refusals of the runaway agent are intended and not counted) and
every persona answered at least 95% of the time.

Usage: analyze_day.py day.jsonl [--json out.json] [--baseline base-summary.json]
"""
import json
import sys
import urllib.request
from collections import defaultdict

GATEWAY = "http://127.0.0.1:8787"
ADMIN = "lab-admin"
# USD per million tokens: input, output, cached input.
PRICE = {"gpt-4o-mini": (0.15, 0.60, 0.075), "claude-haiku": (1.00, 5.00, 0.10), "gemini-flash-lite": (0.10, 0.40, 0.025),
         "qwen-0.5b": (0.01, 0.02, 0.01), "smollm2-360m": (0.01, 0.02, 0.01)}
CACHE_TTL = 600


def pct(xs, p):
    xs = sorted(x for x in xs if x is not None)
    if not xs:
        return None
    pos = p / 100 * (len(xs) - 1)
    lo = int(pos)
    hi = min(lo + 1, len(xs) - 1)
    return round(xs[lo] + (xs[hi] - xs[lo]) * (pos - lo), 3)


def served_from_cache(r):
    return r.get("gw_cache") in ("hit", "shared")


def billed(r):
    """(input, cached input, output) the provider charged for; a gateway cache replay costs nothing."""
    if served_from_cache(r):
        return 0, 0, 0
    return r.get("inp", 0), r.get("cached", 0), r.get("out", 0)


def cost(r):
    if served_from_cache(r):
        return 0.0
    i, o, c = PRICE[r["model"]]
    inp, cached = r.get("inp", 0), r.get("cached", 0)
    return ((inp - cached) * i + cached * c + r.get("out", 0) * o) / 1e6


def query(body):
    req = urllib.request.Request(f"{GATEWAY}/gateway/obs/query", json.dumps({"from": 0, "to": 9_999_999_999_999, **body}).encode(),
                                 {"authorization": f"Bearer {ADMIN}", "content-type": "application/json"})
    return json.load(urllib.request.urlopen(req))["rows"]


def work(rows, ok):
    """Completed work per persona, its unit cost, and whether the day counts."""
    fresh = lambda r: billed(r)[0] - billed(r)[1] + billed(r)[2]  # noqa: E731
    per, bad = {}, []
    for p in sorted({r["persona"] for r in rows if r["persona"] != "runaway"}):
        sent = [r for r in rows if r["persona"] == p]
        done = [r for r in ok if r["persona"] == p]
        f = sum(map(fresh, done))
        per[p] = {"sent": len(sent), "done": len(done), "answered": round(len(done) / len(sent), 3), "fresh_tokens": f,
                  "fresh_per_unit": round(f / len(done), 1) if done else None}
        if len(done) < 0.95 * len(sent):
            bad.append(f"{p} answered {len(done)}/{len(sent)}")
    real = [r for r in rows if r["persona"] != "runaway"]
    errors = sum(r["status"] != 200 for r in real)
    rate = errors / len(real) if real else 0
    if rate > 0.02:
        bad.append(f"error rate {rate:.1%}")
    done = sum(v["done"] for v in per.values())
    f = sum(v["fresh_tokens"] for v in per.values())
    return {"valid": not bad, "invalid_because": bad, "error_rate": round(rate, 4), "work_done": done,
            "fresh_tokens_per_unit_of_work": round(f / done, 1) if done else None,
            "runaway_fresh_tokens": sum(map(fresh, (r for r in ok if r["persona"] == "runaway"))), "by_persona": per}


def main():
    rows = [json.loads(line) for line in open(sys.argv[1]) if line.strip()]
    ok = [r for r in rows if r["status"] == 200]
    total = sum(cost(r) for r in ok)
    b = [billed(r) for r in ok]
    inp, cached, outp = (sum(x[k] for x in b) for k in range(3))
    inter = [r for r in ok if r["persona"] in ("ide", "support", "research")]
    out = {"requests": len(rows), "errors": len(rows) - len(ok), "error_status": {}, "cost_usd": round(total, 4),
           "input_tokens": inp, "output_tokens": outp,
           # The agreed scoreboard: fresh tokens are what the provider bills at full price.
           "scoreboard": {"fresh_tokens": inp - cached + outp, "total_tokens": inp + outp, "cost_usd": round(total, 4),
                          "interactive_ttft_p95": pct([r.get("ttft") for r in inter], 95),
                          "interactive_ttft_p50": pct([r.get("ttft") for r in inter], 50),
                          "interactive_total_p95": pct([r["total"] for r in inter], 95),
                          "ide_turns": sum(1 for r in ok if r["persona"] == "ide"),
                          "interactive_requests": len(inter)}}
    out["scoreboard"].update(work(rows, ok))
    if "--baseline" in sys.argv:
        base = json.load(open(sys.argv[sys.argv.index("--baseline") + 1]))["scoreboard"]
        mine = out["scoreboard"]["by_persona"]
        # This day's cost per unit of work, applied to the amount of work the baseline did.
        at = sum(mine[p]["fresh_per_unit"] * b["done"] for p, b in base["by_persona"].items() if p in mine and mine[p]["done"])
        at += out["scoreboard"]["runaway_fresh_tokens"]
        out["scoreboard"]["fresh_tokens_at_baseline_work"] = int(at)
        ref = base.get("fresh_tokens_at_baseline_work", base["fresh_tokens"])
        out["scoreboard"]["fresh_vs_baseline"] = round(at / ref, 3) if ref else None
        for k in ("interactive_ttft_p50", "interactive_ttft_p95"):
            if base.get(k) and out["scoreboard"].get(k) is not None:
                out["scoreboard"][k + "_vs_baseline"] = round(out["scoreboard"][k] / base[k], 3)
    for r in rows:
        if r["status"] != 200:
            out["error_status"][str(r["status"])] = out["error_status"].get(str(r["status"]), 0) + 1

    by = defaultdict(list)
    for r in ok:
        by[r["persona"]].append(r)
    out["by_persona"] = {p: {"requests": len(rs), "cost_usd": round(sum(map(cost, rs)), 4), "share": round(sum(map(cost, rs)) / total, 3),
                             "input_tokens": sum(billed(r)[0] for r in rs), "output_tokens": sum(billed(r)[2] for r in rs),
                             "fresh_tokens": sum(billed(r)[0] - billed(r)[1] + billed(r)[2] for r in rs),
                             "ttft_p50": pct([r.get("ttft") for r in rs], 50), "ttft_p95": pct([r.get("ttft") for r in rs], 95),
                             "total_p50": pct([r["total"] for r in rs], 50), "total_p95": pct([r["total"] for r in rs], 95)}
                         for p, rs in sorted(by.items())}

    # 1. Prompt (prefix) caching at the provider, per IDE variant.
    variants = defaultdict(list)
    for r in by["ide"]:
        variants[r["note"]].append(r)
    out["prompt_cache"] = {}
    for v, rs in sorted(variants.items()):
        inp, cached = sum(r.get("inp", 0) for r in rs), sum(r.get("cached", 0) for r in rs)
        out["prompt_cache"][v] = {"turns": len(rs), "input_tokens": inp, "cached_share": round(cached / inp, 3) if inp else None,
                                  "cost_usd": round(sum(map(cost, rs)), 4), "cost_per_turn": round(sum(map(cost, rs)) / len(rs), 5)}
    # What the uncached variants would have cost at their cached sibling's rate.
    pc = out["prompt_cache"]
    saving = 0.0
    for bad, good in (("anthropic-plain", "anthropic-cached"), ("openai-timestamp", "openai-stable")):
        if bad in pc and good in pc:
            s = pc[bad]["cost_usd"] - pc[good]["cost_per_turn"] * pc[bad]["turns"]
            pc[bad]["would_save_usd"] = round(s, 4)
            saving += max(s, 0)
    out["waste"] = {"prompt_cache_misalignment_usd": round(saving, 4)}

    # 2. Exact duplicates: same body seen again within the cache TTL (or while still in flight).
    seen, dup_cost, dup_n, inflight_n, dup_stream = {}, 0.0, 0, 0, 0
    for r in sorted(ok, key=lambda r: r["ts"]):
        first = seen.get(r["hash"])
        if first is not None and r["ts"] - first["ts"] < CACHE_TTL:
            dup_n += 1
            dup_cost += cost(r)
            dup_stream += bool(r["stream"])
            inflight_n += r["ts"] < first["ts"] + first["total"]
        else:
            seen[r["hash"]] = r
    dup_by = defaultdict(lambda: [0, 0.0])
    seen = {}
    for r in sorted(ok, key=lambda r: r["ts"]):
        if r["hash"] in seen and r["ts"] - seen[r["hash"]] < CACHE_TTL:
            dup_by[r["persona"]][0] += 1
            dup_by[r["persona"]][1] += cost(r)
        else:
            seen[r["hash"]] = r["ts"]
    out["duplicates"] = {"requests": dup_n, "share_of_requests": round(dup_n / len(ok), 3), "cost_usd": round(dup_cost, 4),
                         "of_which_streaming": dup_stream, "of_which_in_flight": inflight_n,
                         "by_persona": {p: {"requests": n, "cost_usd": round(c, 4)} for p, (n, c) in dup_by.items()},
                         "served_by_gateway_cache": sum(1 for r in ok if r.get("gw_cache") in ("hit", "shared"))}
    out["waste"]["exact_duplicates_usd"] = round(dup_cost, 4)

    # 3. The runaway loop, 4. cancelled streams, 5. client resends.
    out["waste"]["runaway_loop_usd"] = out["by_persona"].get("runaway", {}).get("cost_usd", 0)
    out["runaway_blocked"] = sum(1 for r in rows if r["persona"] == "runaway" and r["status"] == 429)
    aborted = [r for r in rows if r.get("aborted")]
    out["cancelled_streams"] = {"requests": len(aborted), "note": "usage is not reported to a client that hung up; see the gateway log"}

    # 6. Context growth: input tokens by turn of an IDE session.
    turns = defaultdict(list)
    for r in by["ide"]:
        turns[r["turn"]].append(r.get("inp", 0))
    out["ide_input_tokens_by_turn"] = {t: int(sum(v) / len(v)) for t, v in sorted(turns.items())}

    # 7. Fan-out: parallel sub-calls that share a prefix all miss the provider cache together.
    fan = [r for r in by.get("research", []) if r["note"] == "fan-out"]
    inp, cached = sum(r.get("inp", 0) for r in fan), sum(r.get("cached", 0) for r in fan)
    out["fan_out"] = {"calls": len(fan), "input_tokens": inp, "cached_share": round(cached / inp, 3) if inp else None}

    # 8. Interactive latency: quiet hours vs the hours the batch is running.
    inter = [r for r in ok if r["persona"] in ("ide", "support", "research") and "ttft" in r]
    quiet = [r for r in inter if r["frac"] < 0.55 or r["frac"] > 0.87]
    busy = [r for r in inter if 0.57 <= r["frac"] <= 0.85]
    out["interactive_ttft"] = {"quiet": {"n": len(quiet), "p50": pct([r["ttft"] for r in quiet], 50), "p95": pct([r["ttft"] for r in quiet], 95), "p99": pct([r["ttft"] for r in quiet], 99)},
                               "batch_running": {"n": len(busy), "p50": pct([r["ttft"] for r in busy], 50), "p95": pct([r["ttft"] for r in busy], 95), "p99": pct([r["ttft"] for r in busy], 99)}}

    # 9. The gateway's own view of where the time goes.
    try:
        seg = ["queue_ms", "gw_overhead_ms", "upstream_ttfb_ms", "litellm_overhead_ms", "model_ttft_ms", "gen_ms", "total_ms"]
        metrics = ["count", "sum:cost_usd", "sum:cache_saved_usd", "rate:client_abort", "sum:output_tokens"] + [f"{p}:{c}" for c in seg for p in ("avg", "p95")]
        out["gateway_time_ms"] = {"all": query({"metrics": metrics})[0],
                                  "by_upstream": query({"group_by": ["upstream"], "metrics": metrics}),
                                  "by_model": query({"group_by": ["model"], "metrics": ["count", "avg:upstream_ttfb_ms", "p95:upstream_ttfb_ms", "avg:litellm_overhead_ms", "p50:output_tps", "avg:gen_ms", "avg:queue_ms", "p95:queue_ms"]})}
        ab = query({"filter": {"client_abort": [True]}, "metrics": ["count", "sum:output_tokens", "sum:input_tokens", "sum:cost_usd"]})
        out["cancelled_streams"]["gateway"] = ab[0] if ab else {}
    except Exception as e:  # noqa: BLE001
        out["gateway_time_ms"] = {"error": str(e)}

    text = json.dumps(out, indent=1, ensure_ascii=False)
    if "--json" in sys.argv:
        open(sys.argv[sys.argv.index("--json") + 1], "w").write(text)
    print(text)


if __name__ == "__main__":
    main()
