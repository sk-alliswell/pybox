#!/usr/bin/env python3
"""A shortened day of traffic against the gateway, to have a real log with a
daily shape for the usage report (`gwsim usage`). Local models only.

Three teams send three kinds of request (ide = interactive, chat = normal,
batch) through agents `<team>-<kind>` (see gateway.day.toml). Arrivals are
open-loop and follow one bump a day: `--quiet` of the peak rate outside it.
Each team has a few people (`x-gateway-user-id`); the first of each team
sends several times what the others send. A prompt is a preamble per kind, a
part per person and a new ending, so servers can reuse what they have read.
Callers give up after their own timeout and do not retry.

Usage: day_drive.py --out client.jsonl --day-s 1800 --peak 1.5
"""
import argparse
import asyncio
import hashlib
import json
import math
import random
import time

import httpx

GATEWAY = "http://127.0.0.1:8787"
# team -> (people, hour it is busiest, requests of ide/chat/batch per 10 sent)
TEAMS = {"platform": (10, 13.0, (6, 3, 1)), "research": (6, 13.5, (2, 3, 5)), "product": (8, 12.5, (5, 4, 1))}
# kind -> (client timeout s, max tokens)
KINDS = {"ide": (15, 24), "chat": (30, 32), "batch": (60, 16)}


def text(seed, words):
    r = random.Random(int(hashlib.sha256(seed.encode()).hexdigest()[:12], 16))
    return " ".join("".join(r.choice("abcdefghijklmnopqrstuvwxyz") for _ in range(r.randint(3, 8))) for _ in range(words))


def shape(hour, busy_at, width, quiet):
    d = min(abs(hour - busy_at), 24 - abs(hour - busy_at))
    return quiet + (1 - quiet) * math.exp(-0.5 * (d / (width / 2.355)) ** 2)


async def one(client, a, n, at, team, kind, user, t0, out):
    await asyncio.sleep(max(0.0, t0 + at - time.time()))
    timeout, max_tokens = KINDS[kind]
    rec = {"n": n, "at": round(at, 3), "team": team, "kind": kind, "user": user}
    # A system prompt per kind of work, the person's own context so far,
    # then the new request: three messages, as a chat client sends them, so
    # the gateway can see which requests share a start.
    messages = [{"role": "system", "content": text("kind/" + kind, 40)},
                {"role": "user", "content": text(f"person/{user}/{kind}", 30)},
                {"role": "assistant", "content": "ok"},
                {"role": "user", "content": f"Request {n}: {text(f'{a.seed}/{n}', 20)}"}]
    body = {"model": "qwen-0.5b", "stream": True, "max_tokens": max_tokens, "messages": messages}
    sent, first = time.time(), None
    try:
        async with asyncio.timeout(timeout):
            async with client.stream("POST", f"{GATEWAY}/local/v1/chat/completions", json=body,
                                     headers={"authorization": f"Bearer lab-agent-{team}-{kind}", "x-gateway-user-id": user}) as r:
                rec["status"] = r.status_code
                if r.status_code != 200:
                    rec["reason"] = r.headers.get("x-gateway-reject-reason")
                    await r.aread()
                else:
                    async for line in r.aiter_lines():
                        if line.startswith("data: ") and line != "data: [DONE]":
                            c = json.loads(line[6:])["choices"]
                            if c and c[0]["delta"].get("content"):
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
    people = sum(t[0] for t in TEAMS.values())
    jobs = []
    for team, (n, busy_at, mix) in TEAMS.items():
        users = [f"{team}-{i + 1}" for i in range(n)]
        weights = [5] + [1] * (n - 1)
        top = a.peak * n / people
        at = 0.0
        while True:  # thinning: draw at the peak rate, keep by the day's shape
            at += rng.expovariate(top)
            if at >= a.day_s * a.days:
                break
            if rng.random() < shape(at / a.day_s * 24 % 24, busy_at, a.width, a.quiet):
                jobs.append((at, team, rng.choices(list(KINDS), mix)[0], rng.choices(users, weights)[0]))
    jobs.sort()
    limits = httpx.Limits(max_connections=None, max_keepalive_connections=0)
    with open(a.out, "w") as out:
        async with httpx.AsyncClient(timeout=None, limits=limits) as client:
            t0 = time.time()
            print(json.dumps({"start_ms": int(t0 * 1000), "day_s": a.day_s, "requests": len(jobs)}), flush=True)
            await asyncio.gather(*(one(client, a, n, at, team, kind, user, t0, out) for n, (at, team, kind, user) in enumerate(jobs)))
            print(json.dumps({"end_ms": int(time.time() * 1000)}), flush=True)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--out", required=True)
    p.add_argument("--day-s", type=float, default=1800, help="length of the shortened day")
    p.add_argument("--days", type=float, default=1)
    p.add_argument("--peak", type=float, default=1.5, help="arrivals per second, all teams, at the busiest")
    p.add_argument("--width", type=float, default=6, help="hours the bump is at half its height or more")
    p.add_argument("--quiet", type=float, default=0.08)
    p.add_argument("--seed", type=int, default=1)
    asyncio.run(drive(p.parse_args()))
