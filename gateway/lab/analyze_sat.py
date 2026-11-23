#!/usr/bin/env python3
"""Compare saturate.py runs: analyze_sat.py name=file.jsonl [name=file.jsonl ...]"""
import json
import sys

WARM, FLOOD = 20, 120


def pct(xs, p):
    xs = sorted(xs)
    if not xs:
        return None
    pos = p / 100 * (len(xs) - 1)
    lo = int(pos)
    hi = min(lo + 1, len(xs) - 1)
    return round(xs[lo] + (xs[hi] - xs[lo]) * (pos - lo), 2)


def summary(rows):
    ok = [r for r in rows if r["status"] == 200 and "ttft" in r]
    return {"sent": len(rows), "ok": len(ok), "ok_share": round(len(ok) / len(rows), 3) if rows else None,
            "gave_up": sum(r["status"] == -1 for r in rows), "refused_503": sum(r["status"] == 503 for r in rows),
            "other_errors": sum(r["status"] not in (200, -1, 503) for r in rows),
            "ttft_p50": pct([r["ttft"] for r in ok], 50), "ttft_p95": pct([r["ttft"] for r in ok], 95), "ttft_p99": pct([r["ttft"] for r in ok], 99),
            "total_p50": pct([r["total"] for r in ok], 50), "total_p95": pct([r["total"] for r in ok], 95),
            # how long a caller who got nothing was kept waiting before finding out
            "fail_wait_p50": pct([r["total"] for r in rows if r["status"] != 200], 50),
            "retried": sum(r.get("tries", 1) > 1 for r in rows),
            # time in the gateway's queue, for the requests that were answered
            "wait_p50": pct([r.get("queue_ms", 0) / 1000 for r in ok], 50), "wait_p95": pct([r.get("queue_ms", 0) / 1000 for r in ok], 95)}


out = {}
for arg in sys.argv[1:]:
    name, path = arg.split("=", 1)
    rows = [json.loads(line) for line in open(path) if line.strip()]
    samples = [json.loads(line) for line in open(path.replace(".jsonl", ".samples.jsonl")) if line.strip()]
    flood = lambda r: WARM <= r["t"] < WARM + FLOOD  # noqa: E731
    o = {"interactive_all": summary([r for r in rows if r["cls"] == "interactive"]),
         "interactive_flood": summary([r for r in rows if r["cls"] == "interactive" and flood(r)]),
         "interactive_before": summary([r for r in rows if r["cls"] == "interactive" and r["t"] < WARM]),
         "batch": summary([r for r in rows if r["cls"] == "batch"]),
         "completed_per_min_flood": round(sum(1 for r in rows if r["status"] == 200 and "ttft" in r and flood(r)) / (FLOOD / 60), 1)}
    # Recovery: seconds after the flood ends until interactive TTFT stays under 3 s.
    after = sorted((r for r in rows if r["cls"] == "interactive" and r["t"] >= WARM + FLOOD), key=lambda r: r["t"])
    bad = [r["t"] for r in after if r["status"] != 200 or r.get("ttft", 99) > 3]
    o["recovery_s"] = round(max(bad) - (WARM + FLOOD), 1) if bad else 0.0
    # How good was the wait the gateway announced when the request joined the queue?
    told = [r for r in rows if r["status"] == 200 and r.get("tries", 1) == 1 and r.get("queue_ms", 0) > 500 and "expected_ms" in r]
    if told:
        err = [abs(r["expected_ms"] - r["queue_ms"]) / 1000 for r in told]
        rel = [abs(r["expected_ms"] - r["queue_ms"]) / r["queue_ms"] for r in told]
        o["wait_estimate"] = {"n": len(told), "abs_error_s_p50": pct(err, 50), "abs_error_s_p90": pct(err, 90), "rel_error_p50": pct(rel, 50),
                              "by_class": {c: {"n": len(x), "rel_error_p50": pct([abs(r["expected_ms"] - r["queue_ms"]) / r["queue_ms"] for r in x], 50),
                                               "wait_s_p50": pct([r["queue_ms"] / 1000 for r in x], 50)}
                                           for c in ("interactive", "batch") if (x := [r for r in told if r["cls"] == c])}}
    # When the last request was settled, and who did the work.
    o["all_done_s"] = round(max(r["t"] + r["total"] for r in rows), 1)
    done = [r for r in rows if r["status"] == 200 and "ttft" in r]
    o["answered_by_model"] = {m: sum(1 for r in done if r.get("served_model", r["model"]) == m) for m in sorted({r["model"] for r in rows})}
    o["moved_to_stand_in"] = sum(1 for r in done if r.get("served_model", r["model"]) != r["model"])
    last = {m: max((r["t"] + r["total"] for r in done if r.get("served_model", r["model"]) == m), default=0) for m in o["answered_by_model"]}
    # A server that finishes long before the other sat idle while work was still queued.
    o["server_idle_at_end_s"] = round(max(last.values()) - min(last.values()), 1) if last else None
    o["queue_max"] = max((s.get("queued", 0) for s in samples), default=0)
    o["gateway_rss_mb"] = {"start": samples[0]["gateway_rss_mb"], "max": max(s["gateway_rss_mb"] for s in samples)}
    o["litellm_rss_mb"] = {"start": samples[0]["litellm_rss_mb"], "max": max(s["litellm_rss_mb"] for s in samples)}
    out[name] = o
print(json.dumps(out, indent=1))
