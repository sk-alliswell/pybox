#!/usr/bin/env python3
"""Measure what routing by prompt prefix does on real model servers.

`--groups G` kinds of work each have their own long system prompt (about 600
tokens) and send short questions under it, the way sessions of one tool or
one team's batch job do. Arrivals are open loop and come from a seeded
schedule, so every routing policy sees exactly the same requests at the same
times. Everything goes through the gateway to three local llama.cpp servers
(see gateway.prefix-*.toml); no hosted model is called.

What is recorded:
  requests.jsonl      one line per request as the caller saw it (time to
                      first token, total time, which server, what the gateway
                      believed that server already held)
  server-cache.json   what the servers themselves report in their logs for
                      the same period: prompt tokens reused from the KV cache
                      and prompt tokens actually read, per request

`--mix` sends a mixed load instead, shaped like the three kinds of request
the simulator assumes (overlap is how much of a prompt repeats an earlier one):
  ide    6 sessions, each a growing conversation over its own 600-word
         context; a turn every 40 s on average               (overlap ~90%)
  chat   one shared 450-word system prompt, then a 150-word question of the
         caller's own                                        (overlap ~75%)
  batch  two jobs, each a 250-word instruction followed by a 350-word
         document that differs per request                   (overlap ~40%)

`--sessions N` runs N multi-turn conversations: each sends its next turn
(the whole conversation so far plus a new question) a think time after the
previous answer arrived, the way a person or an agent loop does.

`--burst N` puts N batch requests (`x-gateway-priority: batch`) of `--groups`
kinds in the queue at once, shuffled, the way a batch job submits its work,
and records how long the whole job takes.

Usage: prefix_drive.py --out DIR/requests.jsonl --groups 3 --rate 0.45 --seconds 240
                       [--logs ~/lab/logs/kv-9211.log,...] [--salt NAME]
"""
import argparse
import asyncio
import json
import os
import random
import re
import time

import httpx

GATEWAY = "http://127.0.0.1:8787"
WORDS = ("alpha beta gamma delta server cache token route queue model prefix batch stream latency "
         "gateway worker policy budget tenant ledger window credit lease backend").split()
QUESTIONS = ["Say hi.", "Name one risk.", "Give one tip.", "Summarise in a line.", "What comes next?", "Is this fine?"]


def system_prompt(salt, group, words):
    r = random.Random(f"{salt}/{group}")
    return f"Notes for task group {group} ({salt}). " + " ".join(r.choice(WORDS) for _ in range(words))


def schedule(groups, rate, seconds, seed):
    """(arrival time, group, question) for the whole run."""
    r = random.Random(seed)
    t, out = 0.0, []
    while True:
        t += r.expovariate(rate)
        if t >= seconds:
            return out
        out.append((t, r.randrange(groups), f"{r.choice(QUESTIONS)} (item {len(out)})"))


def text(key, words):
    r = random.Random(key)
    return " ".join(r.choice(WORDS) for _ in range(words))


def mixed(a):
    """The mixed load: (arrival time, kind, group, messages or None) per request.
    An ide turn's messages are built when it is sent, from the session so far."""
    r = random.Random(a.seed)
    out = []
    scale = a.rate / 0.3

    def poisson(rate):
        t = 0.0
        while True:
            t += r.expovariate(rate * scale)
            if t >= a.seconds:
                return
            yield t

    for s in range(6):
        for t in poisson(1 / 40):
            out.append((t, "ide", s, None))
    shared = f"House rules for the help desk ({a.salt}). " + text(f"{a.salt}/chat", 450)
    for i, t in enumerate(poisson(0.09)):
        out.append((t, "chat", 0, [{"role": "system", "content": shared},
                                   {"role": "user", "content": f"Case {i}. " + text(f"{a.salt}/chat/{i}", 150) + " What should I do?"}]))
    for i, t in enumerate(poisson(0.06)):
        job = r.randrange(2)
        out.append((t, "batch", job, [{"role": "system", "content": f"Batch job {job} ({a.salt}). " + text(f"{a.salt}/job/{job}", 250)},
                                      {"role": "user", "content": f"Document {i}. " + text(f"{a.salt}/doc/{i}", 350) + " Label it."}]))
    return sorted(out, key=lambda x: x[0])


class Session:
    """One ide conversation: turns go out in order, each with the answers so far."""

    def __init__(self, salt, s, words=600):
        self.lock = asyncio.Lock()
        self.turn = 0
        self.messages = [{"role": "system", "content": f"Workspace {s} ({salt}). " + text(f"{salt}/ide/{s}", words)}]


async def one(client, a, n, at, group, question, t0, out, kind=None, messages=None, session=None):
    await asyncio.sleep(max(0.0, t0 + at - time.time()))
    rec = {"n": n, "at": round(at, 3), "group": group}
    if kind:
        rec["kind"] = kind
    if session:
        await session.lock.acquire()
        session.turn += 1
        rec["turn"] = session.turn
        session.messages.append({"role": "user", "content": f"Step {session.turn}: " + text(f"{a.salt}/{group}/{session.turn}", 40)})
        messages = list(session.messages)
    elif messages is None:
        messages = [{"role": "system", "content": system_prompt(a.salt, group, a.words)},
                    {"role": "user", "content": question}]
    body = {"model": "qwen-0.5b", "stream": True, "max_tokens": a.max_tokens, "messages": messages}
    sent = time.time()
    first, tokens, answer = None, 0, []
    try:
        async with client.stream("POST", f"{GATEWAY}/local/v1/chat/completions", json=body,
                                 headers={"authorization": "Bearer lab-agent", **a.extra_headers}) as r:
            rec["status"] = r.status_code
            rec["backend"] = r.headers.get("x-gateway-backend")
            rec["queue_ms"] = float(r.headers.get("x-gateway-queue-ms", 0))
            warm = r.headers.get("x-gateway-prefix-warm")
            if warm:
                rec["gw_warm"], rec["gw_prompt"] = map(int, warm.split("/"))
            async for line in r.aiter_lines():
                if not line.startswith("data: ") or line == "data: [DONE]":
                    continue
                piece = json.loads(line[6:])["choices"][0]["delta"].get("content")
                if piece:
                    tokens += 1
                    answer.append(piece)
                    first = first or time.time()
    except Exception as e:  # noqa: BLE001
        rec["error"] = repr(e)[:200]
    if session:
        session.messages.append({"role": "assistant", "content": "".join(answer) or "ok"})
        session.lock.release()
    rec["ttft"] = round(first - sent, 3) if first else None
    rec["total"] = round(time.time() - sent, 3)
    rec["tokens"] = tokens
    out.write(json.dumps(rec) + "\n")
    out.flush()


HIT = re.compile(r"Llama\.generate: (\d+) prefix-match hit, remaining (\d+) prompt tokens")
EVAL = re.compile(r"prompt eval time =.*/\s*(\d+) tokens")


def server_cache(logs, offsets):
    """Per server: [reused, read] prompt tokens for each request since `offsets`."""
    res = {}
    for path in logs:
        with open(path, "rb") as f:
            f.seek(offsets[path])
            text = f.read().decode("utf-8", "replace")
        reqs, reused = [], 0
        for line in text.splitlines():
            m = HIT.search(line)
            if m:
                reused = int(m.group(1))
                continue
            m = EVAL.search(line)
            if m:
                reqs.append([reused, int(m.group(1))])
                reused = 0
        res[os.path.basename(path)] = reqs
    return res


async def main():
    p = argparse.ArgumentParser()
    p.add_argument("--out", required=True)
    p.add_argument("--groups", type=int, default=3)
    p.add_argument("--rate", type=float, default=0.45, help="arrivals per second, all groups together")
    p.add_argument("--seconds", type=float, default=240)
    p.add_argument("--words", type=int, default=600)
    p.add_argument("--max-tokens", type=int, default=24)
    p.add_argument("--seed", type=int, default=1)
    p.add_argument("--mix", action="store_true", help="ide/chat/batch mix instead of identical-prefix groups")
    p.add_argument("--burst", type=int, default=0, help="this many batch requests at once instead of arrivals over time")
    p.add_argument("--sessions", type=int, default=0, help="this many multi-turn conversations, each sending its next turn "
                   "a think time after the last answer (closed loop), instead of independent arrivals")
    p.add_argument("--think", type=float, default=8.0, help="mean seconds between an answer and the session's next turn")
    p.add_argument("--salt", default="", help="makes this run's prompts unlike any earlier run's, so it starts cold")
    lab = os.environ.get("LAB", os.path.expanduser("~/lab"))
    p.add_argument("--logs", default=",".join(f"{lab}/logs/kv-921{i}.log" for i in (1, 2, 3)))
    a = p.parse_args()
    a.salt = a.salt or f"run-{int(time.time())}"
    a.extra_headers = {"x-gateway-priority": "batch"} if a.burst else {}
    logs = [x for x in a.logs.split(",") if x]
    offsets = {x: os.path.getsize(x) for x in logs}
    t0 = time.time()
    with open(a.out, "w") as out:
        async with httpx.AsyncClient(timeout=httpx.Timeout(900.0), limits=httpx.Limits(max_connections=500)) as client:
            if a.sessions:
                plan = []

                async def converse(sid):
                    r = random.Random(f"{a.seed}/{sid}")
                    sess = Session(a.salt, sid, a.words)
                    at = r.uniform(0, min(30.0, a.seconds / 4))
                    while at < a.seconds:
                        plan.append(sid)
                        await one(client, a, len(plan), at, sid, None, t0, out, kind="session", session=sess)
                        at = time.time() - t0 + r.expovariate(1 / a.think)

                await asyncio.gather(*(converse(sid) for sid in range(a.sessions)))
            elif a.mix:
                plan = mixed(a)
                sessions = {s: Session(a.salt, s) for s in range(6)}
                await asyncio.gather(*(one(client, a, n, at, g, None, t0, out, kind=k, messages=m,
                                           session=sessions[g] if k == "ide" else None)
                                       for n, (at, k, g, m) in enumerate(plan)))
            elif a.burst:
                r = random.Random(a.seed)
                plan = [(0.02 * n, r.randrange(a.groups), f"{r.choice(QUESTIONS)} (item {n})") for n in range(a.burst)]
                await asyncio.gather(*(one(client, a, n, at, g, q, t0, out) for n, (at, g, q) in enumerate(plan)))
            else:
                plan = schedule(a.groups, a.rate, a.seconds, a.seed)
                await asyncio.gather(*(one(client, a, n, at, g, q, t0, out) for n, (at, g, q) in enumerate(plan)))
    await asyncio.sleep(1.0)
    json.dump({"servers": server_cache(logs, offsets), "args": {k: v for k, v in vars(a).items() if k != "extra_headers"}, "wall_s": round(time.time() - t0, 1)},
              open(os.path.join(os.path.dirname(a.out), "server-cache.json"), "w"), indent=1)
    print(f"{len(plan)} requests in {time.time() - t0:.0f} s")


if __name__ == "__main__":
    asyncio.run(main())
