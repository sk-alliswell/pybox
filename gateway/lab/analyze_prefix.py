#!/usr/bin/env python3
"""Summarise one prefix_drive.py run. Usage: analyze_prefix.py DIR"""
import json
import os
import sys


def pct(xs, q):
    xs = sorted(xs)
    return round(xs[min(len(xs) - 1, int(q * len(xs)))], 3) if xs else None


def mean(xs):
    return round(sum(xs) / len(xs), 3) if xs else None


def main():
    d = sys.argv[1]
    rows = [json.loads(x) for x in open(os.path.join(d, "requests.jsonl"))]
    sc = json.load(open(os.path.join(d, "server-cache.json")))
    ok = [r for r in rows if r.get("status") == 200 and r.get("ttft") is not None]
    ttft = [r["ttft"] for r in ok]
    total = [r["total"] for r in ok]
    queue = [r["queue_ms"] / 1000 for r in ok]
    # Time to first token once the request reached a server.
    server = [r["ttft"] - r["queue_ms"] / 1000 for r in ok]
    per = [x for reqs in sc["servers"].values() for x in reqs]
    reused, read = sum(x[0] for x in per), sum(x[1] for x in per)
    gw_warm, gw_prompt = sum(r.get("gw_warm", 0) for r in ok), sum(r.get("gw_prompt", 0) for r in ok)
    by_backend = {}
    for r in ok:
        by_backend[r["backend"]] = by_backend.get(r["backend"], 0) + 1
    out = {
        "requests": len(rows), "answered": len(ok), "groups": sc["args"]["groups"], "rate": sc["args"]["rate"],
        "sessions": sc["args"].get("sessions") or None, "think": sc["args"].get("think") if sc["args"].get("sessions") else None,
        "ttft_mean": mean(ttft), "ttft_p50": pct(ttft, 0.5), "ttft_p95": pct(ttft, 0.95),
        "total_mean": mean(total), "total_p50": pct(total, 0.5), "total_p95": pct(total, 0.95),
        "queue_mean": mean(queue), "queue_p95": pct(queue, 0.95),
        "server_ttft_mean": mean(server), "server_ttft_p50": pct(server, 0.5),
        "last_answer_s": round(max(r["at"] + r["total"] for r in ok), 1) if ok else None,
        # Reported by the model servers themselves.
        "server_requests": len(per),
        "server_prompt_tokens": reused + read,
        "server_reused_tokens": reused,
        "server_reused_share": round(reused / (reused + read), 3) if reused + read else None,
        "server_hit_share": round(sum(1 for x in per if x[0] >= 100) / len(per), 3) if per else None,
        # What the gateway believed when it routed (estimated tokens).
        "gateway_warm_share": round(gw_warm / gw_prompt, 3) if gw_prompt else None,
        "gateway_warm_requests": round(sum(1 for r in ok if r.get("gw_warm", 0) > 0) / len(ok), 3) if ok else None,
        "by_backend": by_backend,
    }
    kinds = sorted({r["kind"] for r in ok if r.get("kind")})
    if kinds:
        out["by_kind"] = {}
        for k in kinds:
            rs = [r for r in ok if r["kind"] == k]
            w, p = sum(r.get("gw_warm", 0) for r in rs), sum(r.get("gw_prompt", 0) for r in rs)
            out["by_kind"][k] = {"answered": len(rs), "ttft_mean": mean([r["ttft"] for r in rs]), "ttft_p50": pct([r["ttft"] for r in rs], 0.5),
                                 "ttft_p95": pct([r["ttft"] for r in rs], 0.95), "total_mean": mean([r["total"] for r in rs]),
                                 "gateway_warm_share": round(w / p, 3) if p else None}
    print(json.dumps(out, indent=1))


if __name__ == "__main__":
    main()
