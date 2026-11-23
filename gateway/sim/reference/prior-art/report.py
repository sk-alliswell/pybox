"""Turn results.json into the tables of RESULTS.md (printed to stdout)."""
import json, sys
r = json.load(open(sys.argv[1]))
f = lambda v, d=1: "–" if v is None else f"{v:.{d}f}"
for g, rows in r.items():
    classes = [c for c in next(iter(rows.values())) if isinstance(next(iter(rows.values()))[c], dict)]
    print(f"\n### {g}\n")
    print("| setting | prompt tokens not read % | tokens/s | " + " | ".join(f"{c} answered % | {c} TTFT p50 / p95 s" for c in classes) + " |")
    print("|---|---|---|" + "---|---|" * len(classes))
    for n, x in rows.items():
        print(f"| {n} | {f(x['reuse_pct'])} | {f(x['tokens_per_s'], 0)} | " + " | ".join(f"{f(x[c]['ok_pct'])} | {f(x[c]['ttft_p50'], 2)} / {f(x[c]['ttft_p95'], 2)}" for c in classes) + " |")
