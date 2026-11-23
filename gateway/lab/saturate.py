#!/usr/bin/env python3
"""Push the local models far past what they can serve and record what each
client saw, straight at LiteLLM (`--mode direct`) or through the gateway.

Arrivals are open loop (they keep coming whether or not earlier ones were
answered), which is what saturation looks like from outside:
  0-20 s     interactive only, under capacity
  20-140 s   interactive plus a batch flood, about 4x capacity in total
  140-200 s  interactive only again: how fast does it recover?

Interactive callers give up after 15 s, batch callers after 90 s (`--patient`:
batch waits up to 10 minutes, the way a job that only needs to finish does).
With `--retry` a caller that gets 503 comes back after `retry-after`, like an
SDK, until its own deadline. No hosted model is called.

`--topology group` sends both models to one gateway upstream named `local`
(two servers behind it, see gateway.sat-group.toml) instead of one upstream
per model; `--swap` lets the batch agent be answered by a stand-in model.

Usage: saturate.py --mode gateway --out sat.jsonl [--weights] [--priority] [--patient] [--retry]
                   [--topology split|group] [--swap]
"""
import argparse
import asyncio
import json
import os
import random
import time

import aiohttp

GATEWAY = "http://127.0.0.1:8787"
LITELLM = "http://127.0.0.1:4000"
ADMIN = "lab-admin"
MASTER = "sk-lab-master"
UPSTREAM = {"qwen-0.5b": "qwen", "smollm2-360m": "smol"}
# class -> (arrivals per second per model, max_tokens, client timeout s)
CLASSES = {"interactive": (0.8, 24, 15), "batch": (1.2, 64, 90)}
WARM, FLOOD, RECOVER = 20, 120, 60
TOPICS = ["an API gateway", "a token bucket", "p95 latency", "round-robin scheduling", "a request queue", "prompt caching"]


def rss_mb(pattern):
    total = 0
    for pid in filter(str.isdigit, os.listdir("/proc")):
        try:
            cmd = open(f"/proc/{pid}/cmdline").read().replace("\0", " ")
            if pattern in cmd and "saturate.py" not in cmd:
                total += int(open(f"/proc/{pid}/statm").read().split()[1]) * 4096
        except OSError:
            pass
    return round(total / 1e6, 1)


async def one(session, mode, cls, model, token, t0, out, n, retry):
    _, max_tokens, timeout = CLASSES[cls]
    base = f"{GATEWAY}/{UPSTREAM[model]}" if mode == "gateway" else LITELLM
    body = {"model": model, "max_tokens": max_tokens, "stream": True,
            "messages": [{"role": "user", "content": f"Explain {random.choice(TOPICS)} to a new engineer. (request {n})"}]}
    headers = {"authorization": f"Bearer {token}"}
    if mode == "gateway" and cls == "interactive":
        # Leave room for the answer itself inside the caller's 15 s.
        headers["x-gateway-max-wait-ms"] = str(int((timeout - 3) * 1000))
    start = time.monotonic()
    rec = {"t": round(start - t0, 3), "cls": cls, "model": model, "status": 0, "tries": 0}
    try:
        async with asyncio.timeout(timeout):
            while True:
                rec["tries"] += 1
                async with session.post(f"{base}/v1/chat/completions", json=body, headers=headers) as res:
                    rec["status"] = res.status
                    if res.status == 200:
                        rec["queue_ms"] = float(res.headers.get("x-gateway-queue-ms", 0))
                        for key, header in (("backend", "x-gateway-backend"), ("served_model", "x-gateway-model")):
                            if header in res.headers:
                                rec[key] = res.headers[header]
                        if "x-gateway-expected-wait-ms" in res.headers:
                            rec["expected_ms"] = float(res.headers["x-gateway-expected-wait-ms"])
                            rec["ahead"] = int(res.headers["x-gateway-queue-ahead"])
                        async for line in res.content:
                            if "ttft" not in rec and line.startswith(b"data: {") and b'"content"' in line:
                                rec["ttft"] = round(time.monotonic() - start, 3)
                        break
                    rec["error"] = (await res.text())[:120]
                    if not (retry and res.status == 503):
                        break
                    rec.setdefault("first_refusal_s", round(time.monotonic() - start, 3))
                    wait = float(res.headers.get("retry-after", 1))
                await asyncio.sleep(wait * random.uniform(0.8, 1.2))
    except TimeoutError:
        rec["status"] = -1  # the caller gave up
    except aiohttp.ClientError as e:
        rec["error"] = str(e)[:120]
    rec["total"] = round(time.monotonic() - start, 3)
    out.write(json.dumps(rec) + "\n")


async def arrivals(session, mode, cls, model, token, t0, out, start, end, tasks, retry):
    rate = CLASSES[cls][0]
    await asyncio.sleep(start)
    n = 0
    while time.monotonic() - t0 < end:
        n += 1
        tasks.append(asyncio.create_task(one(session, mode, cls, model, token, t0, out, f"{cls}-{model}-{n}", retry)))
        await asyncio.sleep(random.expovariate(rate))


async def sample(session, mode, t0, path, stop):
    admin = {"authorization": f"Bearer {ADMIN}"}
    with open(path, "w", buffering=1) as f:
        while not stop.is_set():
            s = {"t": round(time.monotonic() - t0, 1), "gateway_rss_mb": rss_mb("pybox-gateway"), "litellm_rss_mb": rss_mb("litellm")}
            if mode == "gateway":
                try:
                    async with session.get(f"{GATEWAY}/gateway/live", headers=admin) as res:
                        ups = (await res.json())["now"]["upstreams"]
                        s["queued"] = sum(u["queued"] for u in ups.values())
                        s["inflight"] = sum(u["inflight"] for u in ups.values())
                except aiohttp.ClientError:
                    pass
            f.write(json.dumps(s) + "\n")
            await asyncio.sleep(1)


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["direct", "gateway"], required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--weights", action="store_true", help="interactive agents get weight 4")
    ap.add_argument("--priority", action="store_true", help="agents declare interactive/batch (fair policy)")
    ap.add_argument("--patient", action="store_true", help="batch callers wait up to 10 minutes")
    ap.add_argument("--retry", action="store_true", help="come back after retry-after on 503")
    ap.add_argument("--topology", choices=["split", "group"], default="split")
    ap.add_argument("--swap", action="store_true", help="batch may be answered by a stand-in model")
    ap.add_argument("--seed", type=int, default=11)
    a = ap.parse_args()
    random.seed(a.seed)
    if a.topology == "group":
        UPSTREAM.update({m: "local" for m in UPSTREAM})
    if a.patient:
        CLASSES["batch"] = (*CLASSES["batch"][:2], 600)
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=0), timeout=aiohttp.ClientTimeout(total=None)) as session:
        tokens = {"interactive": MASTER, "batch": MASTER}
        if a.mode == "gateway":
            for cls in CLASSES:
                agent = f"sat-{cls}"
                admin = {"authorization": f"Bearer {ADMIN}"}
                await session.delete(f"{GATEWAY}/gateway/agents/{agent}", headers=admin)
                body = {"id": agent, "team_id": cls, "project_id": "saturation"}
                if a.weights and cls == "interactive":
                    body["weight"] = 4
                if a.priority:
                    body["priority"] = cls
                if a.swap and cls == "batch":
                    body["model_swap"] = True
                async with session.post(f"{GATEWAY}/gateway/agents", headers=admin, json=body) as res:
                    tokens[cls] = (await res.json())["token"]
        t0 = time.monotonic()
        stop, tasks = asyncio.Event(), []
        sampler = asyncio.create_task(sample(session, a.mode, t0, a.out.replace(".jsonl", ".samples.jsonl"), stop))
        with open(a.out, "w", buffering=1) as out:
            await asyncio.gather(*[arrivals(session, a.mode, "interactive", m, tokens["interactive"], t0, out, 0, WARM + FLOOD + RECOVER, tasks, a.retry) for m in UPSTREAM],
                                 *[arrivals(session, a.mode, "batch", m, tokens["batch"], t0, out, WARM, WARM + FLOOD, tasks, a.retry) for m in UPSTREAM])
            await asyncio.gather(*tasks)
        stop.set()
        await sampler
    print(f"done -> {a.out}")


if __name__ == "__main__":
    asyncio.run(main())
