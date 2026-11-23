import asyncio, aiohttp, time, sys, json
URL = "http://localhost:8911/v1/chat/completions"
async def run(n, secs, timeout):
    tok = done = gave = 0
    end = time.time() + secs
    async def one(s, i):
        nonlocal tok, done, gave
        k = 0
        while time.time() < end:
            k += 1
            body = {"model": "qwen", "max_tokens": 48, "messages": [{"role": "user", "content": f"Caller {i} turn {k}: write three sentences about the number {i * 7 + k}."}]}
            try:
                async with s.post(URL, json=body, timeout=aiohttp.ClientTimeout(total=timeout)) as r:
                    j = await r.json()
                    if time.time() < end + 0.01:
                        tok += j["usage"]["completion_tokens"]; done += 1
            except asyncio.TimeoutError:
                gave += 1
            except Exception as e:
                gave += 1
    async with aiohttp.ClientSession() as s:
        await asyncio.gather(*[one(s, i) for i in range(n)])
    return tok / secs, done, gave
async def main():
    await run(1, 6, None)
    for n, to in [(1, None), (4, None), (16, None), (48, None), (16, 8), (48, 8)]:
        t, d, g = await run(n, 40, to)
        print(json.dumps({"callers": n, "timeout_s": to, "tokens_per_s": round(t, 1), "answered": d, "gave_up": g}), flush=True)
        await asyncio.sleep(12)
asyncio.run(main())
