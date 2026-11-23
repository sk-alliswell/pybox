#!/usr/bin/env python3
"""A stand-in for hosted model APIs, so a day of traffic costs nothing.

It speaks the OpenAI chat-completions format and imitates the things about a
hosted provider that the gateway's behaviour depends on. Numbers measured
against it say how the gateway behaves, not what a real provider would bill.

  prompt cache   gpt-* and gemini-*: automatic, the longest prefix seen in the
                 last 5 minutes, from 1,024 tokens up, in 128-token steps.
                 claude-*: only up to a `cache_control` marker.
  parameters     gemini-* and claude-* answer 400 to `prompt_cache_key`
                 (unknown parameter), as they did behind LiteLLM.
  timing         first token after 0.25 s plus prompt reading time (cached
                 tokens read ten times faster), then a steady token rate.
  capacity       unlimited, like a hosted API at these volumes.

Tokens are counted as characters / 4. Faults for tests: `x-mock-fault: 500`,
`hang` or `cut` on a request makes that one request fail that way.

Usage: mock_cloud.py [--port 9300] [--speed 1.0]
"""
import argparse
import asyncio
import hashlib
import json
import random
import time

from aiohttp import web

BLOCK = 128 * 4  # characters per 128 tokens
MIN_BLOCKS = 8   # 1,024 tokens
TTL = 300
# model prefix -> output tokens per second
RATE = {"gpt": 90, "gemini": 180, "claude": 120}
WORDS = "the request queue holds work until a seat is free and then the server answers in order".split()

cache = {}  # (model, prefix hash) -> last seen
stats = {"requests": 0, "input_tokens": 0, "cached_tokens": 0, "output_tokens": 0, "rejected": 0}


def family(model):
    return next((f for f in RATE if model.startswith(f)), "gpt")


def prompt_text(messages):
    """(whole prompt, characters up to the last cache_control marker or None)."""
    parts, marker, n = [], None, 0
    for m in messages:
        content = m.get("content")
        blocks = content if isinstance(content, list) else [{"type": "text", "text": content or ""}]
        for b in blocks:
            text = f"{m.get('role')}:{b.get('text') or ''}\n"
            parts.append(text)
            n += len(text)
            if isinstance(b, dict) and b.get("cache_control"):
                marker = n
    return "".join(parts), marker


def cached_tokens(model, text, marker, now):
    """How much of the prompt the provider already holds, then remember it."""
    fam = family(model)
    limit = len(text) if fam != "claude" else (marker or 0)
    hashes, h = [], hashlib.blake2b(digest_size=8)
    for i in range(0, limit - limit % BLOCK, BLOCK):
        h.update(text[i:i + BLOCK].encode())
        hashes.append(h.hexdigest())
    hit = 0
    for k, digest in enumerate(hashes, 1):
        seen = cache.get((model, digest))
        if seen is not None and now - seen < TTL and k >= MIN_BLOCKS:
            hit = k
    for k, digest in enumerate(hashes, 1):
        if k >= MIN_BLOCKS:
            cache[(model, digest)] = now
    return hit * 128


async def chat(request):
    body = await request.json()
    model = body.get("model", "")
    fam = family(model)
    stats["requests"] += 1
    fault = request.headers.get("x-mock-fault", "")
    if fault.isdigit():
        return web.json_response({"error": {"message": "injected"}}, status=int(fault))
    if fault == "hang":
        await asyncio.sleep(3600)
    if fam != "gpt" and "prompt_cache_key" in body:
        stats["rejected"] += 1
        return web.json_response(
            {"error": {"message": f"{model} does not support parameters: ['prompt_cache_key']", "type": "invalid_request_error"}}, status=400)
    text, marker = prompt_text(body.get("messages") or [])
    now = time.time()
    inp = len(text) // 4
    cached = min(cached_tokens(model, text, marker, now), inp)
    rng = random.Random(hashlib.sha256(text.encode()).digest())
    out = max(1, int((body.get("max_tokens") or 128) * rng.uniform(0.4, 1.0)))
    speed = request.app["speed"]
    ttft = (0.25 + (inp - cached) / 20000 + cached / 200000) * rng.uniform(0.85, 1.25) / speed
    usage = {"prompt_tokens": inp, "completion_tokens": out, "total_tokens": inp + out,
             "prompt_tokens_details": {"cached_tokens": cached}}
    stats["input_tokens"] += inp
    stats["cached_tokens"] += cached
    stats["output_tokens"] += out
    words = [rng.choice(WORDS) for _ in range(out)]
    base = {"id": f"mock-{stats['requests']}", "object": "chat.completion.chunk", "created": int(now), "model": model}
    if not body.get("stream"):
        await asyncio.sleep(ttft + out / RATE[fam] / speed)
        return web.json_response({**base, "object": "chat.completion", "usage": usage,
                                  "choices": [{"index": 0, "finish_reason": "stop",
                                               "message": {"role": "assistant", "content": " ".join(words)}}]})
    res = web.StreamResponse(headers={"content-type": "text/event-stream", "cache-control": "no-cache"})
    await res.prepare(request)

    async def send(obj):
        await res.write(b"data: " + json.dumps(obj).encode() + b"\n\n")

    await asyncio.sleep(ttft)
    step = 4  # tokens per event: keeps the mock's own CPU out of the picture
    for i in range(0, out, step):
        piece = " ".join(words[i:i + step]) + " "
        await send({**base, "choices": [{"index": 0, "delta": {"role": "assistant", "content": piece}, "finish_reason": None}]})
        if fault == "cut" and i >= step:
            request.transport.close()
            return res
        await asyncio.sleep(step / RATE[fam] / speed)
    await send({**base, "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]})
    if (body.get("stream_options") or {}).get("include_usage"):
        await send({**base, "choices": [], "usage": usage})
    await res.write(b"data: [DONE]\n\n")
    return res


async def models(_):
    return web.json_response({"object": "list", "data": [{"id": m, "object": "model"} for m in ("gpt-4o-mini", "gemini-flash-lite", "claude-haiku")]})


async def show_stats(_):
    return web.json_response(stats)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=9300)
    ap.add_argument("--speed", type=float, default=1.0)
    a = ap.parse_args()
    app = web.Application(client_max_size=64 << 20)
    app["speed"] = a.speed
    app.add_routes([web.post("/v1/chat/completions", chat), web.get("/v1/models", models), web.get("/stats", show_stats)])
    web.run_app(app, host="127.0.0.1", port=a.port, print=None, access_log=None)


if __name__ == "__main__":
    main()
