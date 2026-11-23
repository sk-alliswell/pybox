#!/usr/bin/env python3
"""Compare what clients saw in two drive.py runs.

Usage: compare.py direct.jsonl gateway.jsonl [--json out.json]
"""
import json
import sys
from collections import defaultdict


def pct(xs, p):
    if not xs:
        return None
    xs = sorted(xs)
    pos = p / 100 * (len(xs) - 1)
    lo, hi = int(pos), min(int(pos) + 1, len(xs) - 1)
    return xs[lo] + (xs[hi] - xs[lo]) * (pos - lo)


def load(path):
    return [json.loads(line) for line in open(path) if line.strip()]


def summarize(rows):
    out = {}
    groups = defaultdict(list)
    for r in rows:
        kind = "bulk" if r["agent"].startswith("bulk") else "interactive"
        groups[(r["tier"], kind)].append(r)
        groups[(r["tier"], "all")].append(r)
    for (tier, kind), rs in sorted(groups.items()):
        ok = [r for r in rs if r["status"] == 200 and "ttft" in r]
        span = max(r["ts"] + r["total"] for r in rs) - min(r["ts"] for r in rs)
        codes = defaultdict(int)
        for r in rs:
            if r["status"] != 200:
                codes[str(r["status"])] += 1
        out[f"{tier}/{kind}"] = {
            "requests": len(rs),
            "ok": len(ok),
            "errors": len(rs) - len(ok),
            "error_codes": dict(codes),
            "ttft_p50": pct([r["ttft"] for r in ok], 50),
            "ttft_p95": pct([r["ttft"] for r in ok], 95),
            "ttft_p99": pct([r["ttft"] for r in ok], 99),
            "total_p50": pct([r["total"] for r in ok], 50),
            "total_p95": pct([r["total"] for r in ok], 95),
            "total_p99": pct([r["total"] for r in ok], 99),
            "output_tokens": sum(r.get("out", 0) for r in ok),
            "tokens_per_s": sum(r.get("out", 0) for r in ok) / span if span > 0 else None,
        }
    return out


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    result = {name: summarize(load(path)) for name, path in zip(("direct", "gateway"), args)}
    if "--json" in sys.argv:
        json.dump(result, open(sys.argv[sys.argv.index("--json") + 1], "w"), indent=1)
    f = lambda v: "     -" if v is None else f"{v:6.2f}"  # noqa: E731
    print(f"{'group':20} {'mode':8} {'req':>6} {'err':>5} {'ttft p50':>9} {'p95':>6} {'p99':>6} {'total p50':>9} {'p95':>6} {'p99':>6} {'tok/s':>7}")
    for group in sorted(result["direct"]):
        for mode in ("direct", "gateway"):
            s = result.get(mode, {}).get(group)
            if not s:
                continue
            print(f"{group:20} {mode:8} {s['requests']:6} {s['errors']:5} {f(s['ttft_p50']):>9} {f(s['ttft_p95'])} {f(s['ttft_p99'])} "
                  f"{f(s['total_p50']):>9} {f(s['total_p95'])} {f(s['total_p99'])} {f(s['tokens_per_s']):>7}")
            if s["error_codes"]:
                print(f"{'':29} errors by status: {s['error_codes']}")


if __name__ == "__main__":
    main()
