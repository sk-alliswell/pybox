#!/usr/bin/env python3
"""A small model server with several cache slots that says how many prompt
tokens it reused, for the prefix-routing experiments.

llama-cpp-python's own server keeps one prompt and reports no cached-token
count. This one runs the same llama.cpp library and the same model, with:

  --slots N   N independent contexts, each keeping the KV state of the last
              prompt it read. A request continues in the slot whose prompt
              shares the longest start with it when that is at least
              --similarity of the new prompt (llama.cpp's slot rule,
              `--slot-prompt-similarity`), otherwise in the slot used longest
              ago. The rule is re-implemented here; the caching is llama.cpp's.
  usage.prompt_tokens_details.cached_tokens in every answer, and the same
  two log lines llama-cpp-python prints, so prefix_drive.py reads its log.

One request at a time, like llama-cpp-python's server. OpenAI-style
`/v1/chat/completions` (ChatML prompt, as the Qwen models expect), streaming
or not. Usage: slot_server.py --model x.gguf --port 9211 --slots 2
"""
import argparse
import asyncio
import json
import sys
import threading
import time

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse
from llama_cpp import Llama

p = argparse.ArgumentParser()
p.add_argument("--model", required=True)
p.add_argument("--model_alias", default="qwen-0.5b")
p.add_argument("--port", type=int, required=True)
p.add_argument("--slots", type=int, default=2)
p.add_argument("--similarity", type=float, default=0.10)
p.add_argument("--n_ctx", type=int, default=4096)
p.add_argument("--n_threads", type=int, default=1)
a = p.parse_args()

slots = [Llama(a.model, n_ctx=a.n_ctx, n_threads=a.n_threads, n_threads_batch=a.n_threads, verbose=False) for _ in range(a.slots)]
held = [[] for _ in slots]       # tokens each slot's context holds
used = [0.0] * len(slots)        # when each slot was last used
busy = threading.Lock()
app = FastAPI()


def chatml(messages):
    text = "".join(f"<|im_start|>{m['role']}\n{m['content']}<|im_end|>\n" for m in messages)
    return text + "<|im_start|>assistant\n"


def common(x, y):
    n = 0
    for u, v in zip(x, y):
        if u != v:
            break
        n += 1
    return n


def pick(tokens):
    best = max(range(len(slots)), key=lambda i: (common(held[i], tokens), -used[i]))
    lcp = common(held[best], tokens)
    if lcp / max(1, len(tokens)) >= a.similarity:
        return best, lcp
    oldest = min(range(len(slots)), key=lambda i: used[i])
    return oldest, common(held[oldest], tokens)


def log(msg):
    print(msg, file=sys.stderr, flush=True)


@app.get("/v1/models")
def models():
    return {"object": "list", "data": [{"id": a.model_alias, "object": "model"}]}


@app.post("/v1/chat/completions")
async def chat(req: Request):
    body = await req.json()
    tokens = slots[0].tokenize(chatml(body["messages"]).encode(), add_bos=False, special=True)
    max_tokens = int(body.get("max_tokens") or 64)
    rid = f"chatcmpl-{time.time_ns()}"

    def run():
        """Yields text pieces, then the usage dict."""
        i, lcp = pick(tokens)
        # Until the answer is complete nothing is known to be held: a request
        # cut short leaves the context part-way.
        held[i] = []
        # llama.cpp always evaluates at least the last prompt token again.
        cached = min(lcp, len(tokens) - 1)
        log(f"Llama.generate: {cached} prefix-match hit, remaining {len(tokens) - cached} prompt tokens to eval")
        t0 = time.time()
        out = []
        first = None
        for chunk in slots[i].create_completion(tokens, max_tokens=max_tokens, stream=True, stop=["<|im_end|>"],
                                                temperature=float(body.get("temperature", 0.7))):
            piece = chunk["choices"][0]["text"]
            if first is None:
                first = time.time()
                log(f"llama_perf_context_print: prompt eval time = {1000 * (first - t0):.2f} ms / {len(tokens) - cached} tokens (slot {i})")
            out.append(piece)
            yield piece
        text = "".join(out)
        held[i] = list(tokens) + slots[i].tokenize(text.encode(), add_bos=False, special=True)
        used[i] = time.time()
        n_out = max(1, len(out))
        yield {"prompt_tokens": len(tokens), "completion_tokens": n_out, "total_tokens": len(tokens) + n_out,
               "prompt_tokens_details": {"cached_tokens": cached}}

    async def pieces():
        # The whole generation runs in one worker thread that holds `busy`
        # from start to end, so a caller that hangs up (which cancels this
        # coroutine at once) can never let a second request into llama.cpp
        # while the first is still inside it: that crashes the process.
        loop = asyncio.get_running_loop()
        q = asyncio.Queue()
        gone = threading.Event()

        def work():
            with busy:
                gen = run()
                try:
                    for item in gen:
                        if gone.is_set():
                            break
                        loop.call_soon_threadsafe(q.put_nowait, item)
                finally:
                    gen.close()
                    loop.call_soon_threadsafe(q.put_nowait, None)

        threading.Thread(target=work, daemon=True).start()
        try:
            while True:
                item = await q.get()
                if item is None:
                    return
                yield item
        finally:
            gone.set()

    def frame(delta=None, finish=None, usage=None):
        d = {"id": rid, "object": "chat.completion.chunk", "model": a.model_alias,
             "choices": [] if usage else [{"index": 0, "delta": delta or {}, "finish_reason": finish}]}
        if usage:
            d["usage"] = usage
        return f"data: {json.dumps(d)}\n\n"

    if body.get("stream"):
        async def sse():
            async for item in pieces():
                if isinstance(item, dict):
                    yield frame(finish="stop")
                    yield frame(usage=item)
                else:
                    yield frame(delta={"content": item})
            yield "data: [DONE]\n\n"
        return StreamingResponse(sse(), media_type="text/event-stream")
    text, usage = [], {}
    async for item in pieces():
        if isinstance(item, dict):
            usage = item
        else:
            text.append(item)
    return JSONResponse({"id": rid, "object": "chat.completion", "model": a.model_alias, "usage": usage,
                         "choices": [{"index": 0, "message": {"role": "assistant", "content": "".join(text)}, "finish_reason": "stop"}]})


uvicorn.run(app, host="127.0.0.1", port=a.port, log_level="warning")
