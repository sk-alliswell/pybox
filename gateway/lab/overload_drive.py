#!/usr/bin/env python3
"""More traffic than the local servers can answer, to see who gets served.

Open-loop arrivals (they keep coming whether or not earlier ones were
answered) against the gateway upstream `local` (three llama.cpp servers, see
prefix_stack.sh). Every prompt is different, so caches play no part. Callers
give up after their own timeout and do not retry. No hosted model is called.

  --scenario classes   three agents: ide (interactive, weight 4, gives up
                       after 15 s), chat (normal, weight 2, 30 s) and batch
                       (batch, weight 1, 60 s), sending 30/30/40% of the traffic
  --scenario users     eight agents of equal weight; u1 and u2 each send ten
                       times what each of the others sends (45 s timeout)

`--rate` is the total arrivals per second. `overload_drive.py --analyze DIR`
prints the summary of a finished run. With `--say-patience` every caller
tells the gateway how long it can wait (`x-gateway-max-wait-ms`), which
`admit_by_wait` needs to refuse at the door what would time out anyway.
`--quiet-after S --quiet-rate R` follows the busy time with a quiet one, and
`--batch-timeout T` makes batch callers patient: what `hold_batch` is for.

Usage: overload_drive.py --out requests.jsonl --scenario classes --rate 3 --seconds 180
"""
import argparse
import asyncio
import hashlib
import json
import random
import sys
import time

import httpx

GATEWAY = "http://127.0.0.1:8787"
SCENARIOS = {
    # agent -> (share of arrivals, client timeout s, group for the summary)
    "classes": {"ide": (0.3, 15, "ide"), "chat": (0.3, 30, "chat"), "batch": (0.4, 60, "batch")},
    "users": {**{f"u{i}": (10 / 26, 45, "heavy") for i in (1, 2)}, **{f"u{i}": (1 / 26, 45, "light") for i in range(3, 9)}},
}


def text(seed, words):
    r = random.Random(int(hashlib.sha256(seed.encode()).hexdigest()[:12], 16))
    return " ".join("".join(r.choice("abcdefghijklmnopqrstuvwxyz") for _ in range(r.randint(3, 8))) for _ in range(words))


async def one(client, a, n, at, agent, timeout, group, t0, out):
    await asyncio.sleep(max(0.0, t0 + at - time.time()))
    rec = {"n": n, "at": round(at, 3), "agent": agent, "group": group}
    body = {"model": "qwen-0.5b", "stream": True, "max_tokens": a.max_tokens,
            "messages": [{"role": "user", "content": f"Request {n} ({a.salt}): " + text(f"{a.salt}/{n}", a.words)}]}
    # What a caller that says how long it can wait would send: its own
    # timeout less the few seconds the answer itself takes.
    patience = {"x-gateway-max-wait-ms": str(int((timeout - a.answer_s) * 1000))} if a.say_patience else {}
    sent, first = time.time(), None
    try:
        async with asyncio.timeout(timeout):
            async with client.stream("POST", f"{GATEWAY}/local/v1/chat/completions", json=body,
                                     headers={"authorization": f"Bearer lab-agent-{agent}", **patience}) as r:
                rec["status"] = r.status_code
                rec["queue_ms"] = float(r.headers.get("x-gateway-queue-ms", 0))
                if r.status_code != 200:
                    rec["reason"] = r.headers.get("x-gateway-reject-reason")
                    rec["kind"] = (json.loads(await r.aread()).get("error") or {}).get("message", "")[:120]
                else:
                    async for line in r.aiter_lines():
                        if line.startswith("data: ") and line != "data: [DONE]":
                            if json.loads(line[6:])["choices"][0]["delta"].get("content"):
                                first = first or time.time()
                    rec["done"] = True
    except TimeoutError:
        rec["gave_up"] = True
    except Exception as e:  # noqa: BLE001
        rec["error"] = repr(e)[:200]
    rec["ttft"] = round(first - sent, 3) if first else None
    rec["total"] = round(time.time() - sent, 3)
    out.write(json.dumps(rec) + "\n")
    out.flush()


async def drive(a):
    rng = random.Random(a.seed)
    agents = SCENARIOS[a.scenario]
    names, shares = list(agents), [agents[n][0] for n in agents]
    jobs, at = [], 0.0
    while True:
        at += rng.expovariate(a.quiet_rate if a.quiet_after and at >= a.quiet_after else a.rate)
        if at >= a.seconds:
            break
        jobs.append((at, rng.choices(names, shares)[0]))
    if a.batch_timeout:
        agents = {n: ((s, a.batch_timeout, g) if g == "batch" else (s, t, g)) for n, (s, t, g) in agents.items()}
    limits = httpx.Limits(max_connections=None, max_keepalive_connections=0)
    with open(a.out, "w") as out:
        async with httpx.AsyncClient(timeout=None, limits=limits) as client:
            t0 = time.time()
            await asyncio.gather(*(one(client, a, n, at, ag, agents[ag][1], agents[ag][2], t0, out) for n, (at, ag) in enumerate(jobs)))


def pct(xs, p):
    xs = sorted(xs)
    return round(xs[min(len(xs) - 1, int(p * len(xs)))], 2) if xs else None


def analyze(d):
    rows = [json.loads(line) for line in open(f"{d}/requests.jsonl")]
    out = {"requests": len(rows), "groups": {}}
    for g in sorted({r["group"] for r in rows}):
        rs = [r for r in rows if r["group"] == g]
        ok = [r for r in rs if r.get("done")]
        ttft = [r["ttft"] for r in ok if r["ttft"] is not None]
        refused = [r for r in rs if r.get("status") == 503]
        out["groups"][g] = {
            "sent": len(rs), "answered": len(ok), "answered_share": round(len(ok) / len(rs), 3),
            "ttft_p50": pct(ttft, 0.5), "ttft_p95": pct(ttft, 0.95),
            "queue_s_p50": pct([r.get("queue_ms", 0) / 1000 for r in ok], 0.5),
            "queue_s_max": pct([r.get("queue_ms", 0) / 1000 for r in ok], 1.0),
            "refused": len(refused), "refused_early": sum("filling up" in (r.get("kind") or "") for r in refused),
            "refused_wait_p50": pct([r["total"] for r in refused], 0.5),
            "gave_up": sum(bool(r.get("gave_up")) for r in rs), "errors": sum("error" in r for r in rs),
        }
    out["answered"] = sum(g["answered"] for g in out["groups"].values())
    # Over the whole time anything was being served, the tail after the last arrival included.
    out["answered_per_s"] = round(out["answered"] / max(r["at"] + r["total"] for r in rows), 2)
    print(json.dumps(out, indent=1))


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "--analyze":
        analyze(sys.argv[2])
        sys.exit()
    p = argparse.ArgumentParser()
    p.add_argument("--out", required=True)
    p.add_argument("--scenario", choices=list(SCENARIOS), required=True)
    p.add_argument("--rate", type=float, required=True)
    p.add_argument("--seconds", type=float, default=180)
    p.add_argument("--words", type=int, default=60)
    p.add_argument("--max-tokens", type=int, default=24)
    p.add_argument("--seed", type=int, default=1)
    p.add_argument("--salt", default="")
    p.add_argument("--say-patience", action="store_true", help="callers send x-gateway-max-wait-ms (their timeout less --answer-s)")
    p.add_argument("--answer-s", type=float, default=4.0)
    p.add_argument("--quiet-after", type=float, default=0, help="from this second on, arrivals come at --quiet-rate (a busy time followed by a quiet one)")
    p.add_argument("--quiet-rate", type=float, default=0.0)
    p.add_argument("--batch-timeout", type=float, default=0, help="batch callers give up after this many seconds instead of 60")
    asyncio.run(drive(p.parse_args()))
