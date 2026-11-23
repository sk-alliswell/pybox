#!/usr/bin/env python3
"""Turn a loadtest/run.py result dir into summary numbers and a self-contained
interactive HTML timeline report.

  python3 loadtest/report.py report.html results-a/ [results-b/ ...]
"""
import json, math, os, statistics, sys

PROV = ["anthropic", "gemini", "openai"]
MODEL = {"anthropic": "claude-haiku-5-5", "gemini": "gemini-2.5-flash-lite", "openai": "gpt-4o-mini"}
# USD per 1M tokens (input, output) at time of test.
PRICE = {"anthropic": (0.10, 0.50), "gemini": (0.10, 0.40), "openai": (0.15, 0.60)}


def pct(xs, p):
    xs = sorted(x for x in xs if x is not None and not math.isnan(x))
    if not xs:
        return None
    k = (len(xs) - 1) * p / 100
    f = math.floor(k)
    c = min(f + 1, len(xs) - 1)
    return xs[f] + (xs[c] - xs[f]) * (k - f)


def load(d):
    reqs = [json.loads(l) for l in open(os.path.join(d, "requests.jsonl"))]
    samples = [json.loads(l) for l in open(os.path.join(d, "samples.jsonl"))]
    summary = json.load(open(os.path.join(d, "summary.json")))
    return reqs, samples, summary


def derive(reqs):
    for r in reqs:
        r["ttfb"] = (r["t_first_byte"] - r["t_start"]) * 1000 if r.get("t_first_byte") else None
        r["total"] = (r["t_end"] - r["t_start"]) * 1000
        r["hdr"] = (r["t_headers"] - r["t_start"]) * 1000 if r.get("t_headers") else None
        q = r.get("queue_ms")
        r["queue"] = q if q is not None and not (isinstance(q, float) and math.isnan(q)) else None
        dur = r["t_end"] - (r.get("t_first_byte") or r["t_end"])
        r["tok_s"] = r.get("out_tok", 0) / dur if r.get("stream") and dur > 0.2 and r.get("out_tok") else None


def per_provider(reqs, duration):
    rows = {}
    for p in PROV + ["all"]:
        rs = [r for r in reqs if p == "all" or r["provider"] == p]
        ok = [r for r in rs if r.get("status") == 200]
        st = [r for r in ok if r["stream"]]
        inp = sum(r.get("in_tok", 0) for r in ok)
        out = sum(r.get("out_tok", 0) for r in ok)
        cost = sum(PRICE[r["provider"]][0] * r.get("in_tok", 0) / 1e6 +
                   PRICE[r["provider"]][1] * r.get("out_tok", 0) / 1e6 for r in ok)
        statuses = {}
        for r in rs:
            statuses[str(r.get("status"))] = statuses.get(str(r.get("status")), 0) + 1
        rows[p] = dict(
            requests=len(rs), ok=len(ok), statuses=statuses,
            retried=sum(1 for r in rs if (r.get("attempts") or 1) > 1),
            queue_p50=pct([r["queue"] for r in ok], 50), queue_p95=pct([r["queue"] for r in ok], 95),
            queue_max=max([r["queue"] or 0 for r in ok], default=0),
            ttfb_p50=pct([r["ttfb"] for r in st], 50), ttfb_p95=pct([r["ttfb"] for r in st], 95),
            total_p50=pct([r["total"] for r in st], 50), total_p95=pct([r["total"] for r in st], 95),
            tok_s_p50=pct([r["tok_s"] for r in st], 50),
            in_tok=inp, out_tok=out, cost=cost,
            mb_up=sum(r["bytes_up"] for r in rs) / 1e6,
            mb_down=sum(r.get("bytes_down", 0) for r in rs) / 1e6,
            rps=len(ok) / duration,
        )
    return rows


def resources(samples, idle):
    s = [x for x in samples if "cpu_s" in x]
    cpu = []
    for a, b in zip(s, s[1:]):
        dt = b["t"] - a["t"]
        cpu.append(dict(t=b["t"], cpu=100 * (b["cpu_s"] - a["cpu_s"]) / dt if dt > 0 else 0,
                        up=(b["rchar"] - a["rchar"]) / dt / 1e6, down=(b["wchar"] - a["wchar"]) / dt / 1e6))
    return dict(
        idle_rss_mb=idle["rss_kb"] / 1024, idle_threads=idle["threads"],
        peak_rss_mb=max(x["rss_kb"] for x in s) / 1024,
        end_rss_mb=s[-1]["rss_kb"] / 1024,
        peak_cpu=max(c["cpu"] for c in cpu), avg_cpu=statistics.mean(c["cpu"] for c in cpu),
        cpu_total_s=s[-1]["cpu_s"] - s[0]["cpu_s"],
        peak_threads=max(x["threads"] for x in s), peak_fds=max(x["fds"] for x in s),
        peak_sock_in=max(x["sock_in"] for x in s), peak_sock_out=max(x["sock_out"] for x in s),
    ), cpu


def build(d):
    reqs, samples, summary = load(d)
    derive(reqs)
    duration = summary["config"]["duration"]
    rows = per_provider(reqs, duration)
    res, cpu = resources(samples, summary["idle"])
    peak_agents = max(sum(x["client_active"].values()) for x in samples)
    peak_q = {p: max(x.get("sched", {}).get(p, {}).get("queued", 0) for x in samples) for p in PROV}
    numbers = dict(per_provider=rows, resources=res, peak_active=peak_agents, peak_queued=peak_q,
                   agents=summary["agents"], duration=duration, config=summary["config"])
    json.dump(numbers, open(os.path.join(d, "numbers.json"), "w"), indent=1)

    # compact payloads for the page
    R = [[PROV.index(r["provider"]), r["agent"], round(r["t_start"], 2),
          round(r["t_headers"], 2) if r.get("t_headers") else None,
          round(r["t_first_byte"], 2) if r.get("t_first_byte") else None,
          round(r["t_end"], 2), r.get("status"), 1 if r["stream"] else 0,
          round(r["queue"], 1) if r["queue"] is not None else None,
          r.get("out_tok", 0), r["bytes_up"], r.get("bytes_down", 0)] for r in reqs]
    S = [[round(x["t"], 2), sum(x["client_active"].values())] +
         [x["client_active"].get(p, 0) for p in PROV] +
         [x.get("sched", {}).get(p, {}).get("inflight", 0) for p in PROV] +
         [x.get("sched", {}).get(p, {}).get("queued", 0) for p in PROV] +
         [round(x.get("rss_kb", 0) / 1024, 2), x.get("threads", 0), x.get("fds", 0),
          x.get("sock_in", 0), x.get("sock_out", 0)] for x in samples]
    C = [[round(c["t"], 2), round(c["cpu"], 2), round(c["up"], 3), round(c["down"], 3)] for c in cpu]
    page = dict(numbers)
    notes = os.path.join(d, "notes.json")   # optional: {label, lede, findings[], method}
    if os.path.exists(notes):
        page.update(json.load(open(notes)))
    page.setdefault("label", os.path.basename(os.path.normpath(d)))
    return dict(R=R, S=S, C=C, N=page), numbers


def main(out_html, dirs):
    runs = []
    for d in dirs:
        run, numbers = build(d)
        runs.append(run)
        print(d, json.dumps(numbers["resources"]))
    tpl = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "report_template.html")).read()
    data = json.dumps(runs, separators=(",", ":"), ensure_ascii=False).replace("</", "<\\/")
    open(out_html, "w").write(tpl.replace("/*DATA_RUNS*/[]", data))


if __name__ == "__main__":
    # report.py OUT.html RESULT_DIR [RESULT_DIR ...]
    main(sys.argv[1], sys.argv[2:])
