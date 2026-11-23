#!/usr/bin/env python3
"""Drive the lab stack with many agents at once, either straight at LiteLLM
(`--mode direct`) or through the gateway (`--mode gateway`), and write what
each client saw to a JSONL file.

The schedule is the same in both modes:
  0-20%   only the interactive agents (one request at a time each)
  20-85%  a bulk agent joins and keeps many requests open
  85-100% interactive agents only again

Usage: drive.py --mode gateway --seconds 180 --out /tmp/lab/gateway.jsonl
"""
import argparse
import asyncio
import json
import random
import time

import aiohttp

GATEWAY = "http://127.0.0.1:8787"
LITELLM = "http://127.0.0.1:4000"
ADMIN = "lab-admin"
MASTER = "sk-lab-master"

LOCAL = ["qwen-0.5b", "smollm2-360m"]
CLOUD = ["gemini-flash-lite", "gpt-4o-mini", "claude-haiku"]
# Gateway upstream that fronts each model (see gateway.lab.toml).
UPSTREAM = {"qwen-0.5b": "qwen", "smollm2-360m": "smol"}
PROMPTS = [
    "Explain what an API gateway does in two sentences.",
    "Give three short tips for writing a good commit message.",
    "What is the difference between p95 and p99 latency?",
    "Summarize why caching reduces cost, in one short paragraph.",
    "Write a haiku about queues.",
    "List four causes of slow HTTP responses.",
    "What is a token bucket? Answer briefly.",
    "Describe round-robin scheduling to a beginner.",
]

# (agent id, team, project, tier, workers, think seconds, weight)
AGENTS = (
    [(f"chat-{i}", "interactive", "assistant", "local", 1, 0.3, 1) for i in range(6)]
    + [(f"copilot-{i}", "interactive", "ide", "cloud", 1, 0.2, 1) for i in range(12)]
    + [("bulk-indexer", "batch", "search-index", "local", 24, 0.0, 1)]
    + [("bulk-eval", "batch", "eval-suite", "cloud", 80, 0.0, 1)]
)


async def one(session, mode, tier, model, token, agent, run_id, out, max_tokens):
    base = f"{GATEWAY}/{UPSTREAM.get(model, 'cloud')}" if mode == "gateway" else LITELLM
    headers = {"authorization": f"Bearer {token}", "content-type": "application/json"}
    if mode == "gateway":
        headers["x-gateway-run-id"] = run_id
        headers["x-gateway-client-sent-at"] = str(int(time.time() * 1000))
    body = {
        "model": model,
        "max_tokens": max_tokens,
        "stream": True,
        "stream_options": {"include_usage": True},
        "messages": [{"role": "user", "content": random.choice(PROMPTS)}],
    }
    t0 = time.monotonic()
    rec = {"ts": time.time(), "mode": mode, "agent": agent, "tier": tier, "model": model, "status": 0}
    try:
        async with session.post(f"{base}/v1/chat/completions", headers=headers, json=body) as res:
            rec["status"] = res.status
            if res.status != 200:
                rec["error"] = (await res.text())[:200]
            else:
                rec["queue_ms"] = float(res.headers.get("x-gateway-queue-ms", 0))
                buf = b""
                async for chunk in res.content.iter_any():
                    buf += chunk
                    while b"\n\n" in buf:
                        event, buf = buf.split(b"\n\n", 1)
                        if not event.startswith(b"data: ") or event == b"data: [DONE]":
                            continue
                        try:
                            ev = json.loads(event[6:])
                        except ValueError:
                            continue
                        choices = ev.get("choices") or []
                        if choices and (choices[0].get("delta") or {}).get("content") and "ttft" not in rec:
                            rec["ttft"] = time.monotonic() - t0
                        if ev.get("usage"):
                            rec["out"] = ev["usage"].get("completion_tokens", 0)
                            rec["in"] = ev["usage"].get("prompt_tokens", 0)
    except Exception as e:  # noqa: BLE001 - every failure is a data point
        rec["error"] = f"{type(e).__name__}: {e}"[:200]
    rec["total"] = time.monotonic() - t0
    out.write(json.dumps(rec) + "\n")


async def worker(session, mode, spec, token, window, out, n):
    agent, _team, _project, tier, _workers, think, _weight = spec
    start, stop = window
    await asyncio.sleep(max(0.0, start - time.monotonic()) + random.random() * 0.5)
    i = 0
    while time.monotonic() < stop:
        model = random.choice(LOCAL if tier == "local" else CLOUD)
        run_id = f"{agent}-w{n}-{i // 5}"  # five calls make one "run"
        await one(session, mode, tier, model, token, agent, run_id, out, 48 if tier == "local" else 96)
        i += 1
        if think:
            await asyncio.sleep(think)


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["direct", "gateway"], required=True)
    ap.add_argument("--seconds", type=float, default=180)
    ap.add_argument("--out", required=True)
    ap.add_argument("--bulk-weight", type=int, default=1)
    a = ap.parse_args()

    timeout = aiohttp.ClientTimeout(total=300)
    conn = aiohttp.TCPConnector(limit=0)
    async with aiohttp.ClientSession(timeout=timeout, connector=conn) as session:
        tokens = {}
        for agent, team, project, *_ in AGENTS:
            if a.mode == "direct":
                tokens[agent] = MASTER
                continue
            await session.delete(f"{GATEWAY}/gateway/agents/{agent}", headers={"authorization": f"Bearer {ADMIN}"})
            async with session.post(
                f"{GATEWAY}/gateway/agents",
                headers={"authorization": f"Bearer {ADMIN}"},
                json={"id": agent, "team_id": team, "project_id": project, "user_id": agent.split("-")[0],
                      "session_id": f"{agent}-{int(time.time())}",
                      "weight": a.bulk_weight if agent.startswith("bulk") else 1},
            ) as res:
                tokens[agent] = (await res.json())["token"]

        t0 = time.monotonic()
        end = t0 + a.seconds
        with open(a.out, "w", buffering=1) as out:
            tasks = []
            for spec in AGENTS:
                bulk = spec[0].startswith("bulk")
                window = (t0 + a.seconds * 0.20, t0 + a.seconds * 0.85) if bulk else (t0, end)
                for n in range(spec[4]):
                    tasks.append(asyncio.create_task(worker(session, a.mode, spec, tokens[spec[0]], window, out, n)))
            await asyncio.gather(*tasks)
    print(f"{a.mode}: done in {time.monotonic() - t0:.0f}s -> {a.out}")


if __name__ == "__main__":
    asyncio.run(main())
