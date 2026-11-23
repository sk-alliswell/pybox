#!/usr/bin/env python3
"""Multi-provider streaming load test for pybox-gateway.

Starts the release gateway binary as a child process, drives many concurrent
agents (Anthropic / Gemini / OpenAI) through it on a ramp -> hold -> spike ->
ramp-down schedule, and samples the gateway process (CPU, RSS, threads, fds,
inbound/outbound TCP sockets, I/O bytes) plus /gateway/stats every 0.5 s.

Writes requests.jsonl, samples.jsonl and summary.json to the output dir.
Keys are read by the gateway from its own environment; this script never sees
or prints them.

  python3 loadtest/run.py --out results/ [--scale 1.0] [--duration 300]
"""
import argparse, asyncio, json, os, random, signal, subprocess, sys, time
import aiohttp

HERE = os.path.dirname(os.path.abspath(__file__))
GW = "http://127.0.0.1:8787"
ADMIN = {"Authorization": "Bearer loadtest-admin"}

PROVIDERS = {
    "anthropic": dict(model="claude-haiku-5-5", path="/anthropic/v1/messages"),
    "gemini": dict(model="gemini-2.5-flash-lite", path="/gemini/chat/completions"),
    "openai": dict(model="gpt-4o-mini", path="/openai/v1/chat/completions"),
}

WORDS = ("gateway sandbox agent stream token latency queue upstream socket buffer "
         "kernel memory scheduler retry budget limiter request response header body "
         "chunk event python rust tokio axum hyper reqwest proxy host guest vm").split()


def filler(n_bytes, rng):
    out, size = [], 0
    while size < n_bytes:
        w = rng.choice(WORDS)
        out.append(w)
        size += len(w) + 1
    return " ".join(out)


def build(provider, token, stream, rng, in_bytes, max_tokens):
    doc = filler(in_bytes, rng)
    ask = ("Below is a log of notes. First summarize it in 3 bullet points, then write "
           "a detailed ~450-word technical story about a gateway that uses as many of "
           "the words as possible.\n\n" + doc)
    p = PROVIDERS[provider]
    if provider == "anthropic":
        headers = {"x-api-key": token, "anthropic-version": "2023-06-01"}
        body = {"model": p["model"], "max_tokens": max_tokens, "stream": stream,
                "thinking": {"type": "disabled"},
                "messages": [{"role": "user", "content": ask}]}
    else:
        headers = {"Authorization": f"Bearer {token}"}
        body = {"model": p["model"], "max_tokens": max_tokens, "stream": stream,
                "messages": [{"role": "user", "content": ask}]}
        if stream:
            body["stream_options"] = {"include_usage": True}
    headers["content-type"] = "application/json"
    return GW + p["path"], headers, json.dumps(body).encode()


def usage_from(provider, text):
    """Pull input/output token counts out of a JSON body or an SSE stream."""
    inp = out = 0
    chunks = []
    if text.lstrip().startswith("{"):
        chunks = [text]
    else:
        for line in text.splitlines():
            if line.startswith("data:"):
                d = line[5:].strip()
                if d and d != "[DONE]":
                    chunks.append(d)
    for c in chunks:
        try:
            j = json.loads(c)
        except Exception:
            continue
        u = j.get("usage") or (j.get("message") or {}).get("usage")
        if not u:
            continue
        inp = max(inp, u.get("input_tokens") or u.get("prompt_tokens") or 0)
        out = max(out, u.get("output_tokens") or u.get("completion_tokens") or 0)
    return inp, out


async def agent(name, provider, token, start_at, stop_at, t0, session, log, cfg, live):
    rng = random.Random(name)
    await asyncio.sleep(max(0, t0 + start_at - time.time()))
    seq = 0
    while time.time() < t0 + stop_at:
        stream = rng.random() < cfg.stream_ratio
        url, headers, body = build(provider, token, stream, rng,
                                   rng.randint(cfg.in_min, cfg.in_max), cfg.max_tokens)
        rec = dict(agent=name, provider=provider, seq=seq, stream=stream,
                   bytes_up=len(body), t_start=time.time() - t0)
        live[provider] += 1
        try:
            async with session.post(url, data=body, headers=headers,
                                    timeout=aiohttp.ClientTimeout(total=180)) as r:
                rec["t_headers"] = time.time() - t0
                rec["status"] = r.status
                rec["queue_ms"] = float(r.headers.get("x-gateway-queue-ms", "nan"))
                rec["upstream_ms"] = float(r.headers.get("x-gateway-upstream-ms", "nan"))
                rec["attempts"] = int(r.headers.get("x-gateway-attempts", "0"))
                buf, down, first = [], 0, None
                async for chunk in r.content.iter_any():
                    if first is None:
                        first = time.time() - t0
                    down += len(chunk)
                    buf.append(chunk)
                rec["t_first_byte"] = first
                rec["bytes_down"] = down
                text = b"".join(buf).decode("utf-8", "replace")
                rec["in_tok"], rec["out_tok"] = usage_from(provider, text)
                if r.status != 200:
                    rec["error"] = text[:300]
        except Exception as e:
            rec["status"] = 0
            rec["error"] = f"{type(e).__name__}: {e}"[:300]
        rec["t_end"] = time.time() - t0
        live[provider] -= 1
        log.append(rec)
        seq += 1
        await asyncio.sleep(rng.uniform(0, cfg.think_max))


# ---------------------------------------------------------------- sampling ---

def tcp_table():
    """inode -> (local_port, remote_port, state) from /proc/net/tcp{,6}."""
    m = {}
    for f in ("/proc/net/tcp", "/proc/net/tcp6"):
        try:
            lines = open(f).read().splitlines()[1:]
        except OSError:
            continue
        for ln in lines:
            p = ln.split()
            lport = int(p[1].rsplit(":", 1)[1], 16)
            rport = int(p[2].rsplit(":", 1)[1], 16)
            m[p[9]] = (lport, rport, p[3])
    return m


def proc_sample(pid):
    clk = os.sysconf("SC_CLK_TCK")
    st = open(f"/proc/{pid}/stat").read().rsplit(")", 1)[1].split()
    cpu_s = (int(st[11]) + int(st[12])) / clk
    status = dict(l.split(":", 1) for l in open(f"/proc/{pid}/status").read().splitlines() if ":" in l)
    rss_kb = int(status["VmRSS"].split()[0])
    threads = int(status["Threads"])
    io = dict(l.split(": ") for l in open(f"/proc/{pid}/io").read().splitlines())
    fds = os.listdir(f"/proc/{pid}/fd")
    socks = []
    for fd in fds:
        try:
            t = os.readlink(f"/proc/{pid}/fd/{fd}")
        except OSError:
            continue
        if t.startswith("socket:["):
            socks.append(t[8:-1])
    tab = tcp_table()
    inbound = outbound = other = 0
    for ino in socks:
        e = tab.get(ino)
        if not e:
            other += 1
        elif e[0] == 8787 and e[2] == "01":
            inbound += 1
        elif e[1] == 443 or e[1] == 3128 or e[1] > 0 and e[0] != 8787:
            outbound += 1
        else:
            other += 1
    return dict(cpu_s=cpu_s, rss_kb=rss_kb, threads=threads, fds=len(fds),
                sock_in=inbound, sock_out=outbound, sock_other=other,
                rchar=int(io["rchar"]), wchar=int(io["wchar"]))


async def sampler(pid, t0, session, out, stop, live):
    while not stop.is_set():
        s = {"t": time.time() - t0}
        try:
            s.update(proc_sample(pid))
        except Exception as e:
            s["err"] = str(e)
        try:
            async with session.get(GW + "/gateway/stats", headers=ADMIN) as r:
                j = await r.json()
            s["sched"] = {k: {"inflight": v["scheduler"]["inflight"],
                              "queued": v["scheduler"]["queued"],
                              "cooldown_ms": v["scheduler"].get("cooldown_ms", 0)}
                          for k, v in j["upstreams"].items()}
        except Exception as e:
            s["stats_err"] = str(e)
        s["client_active"] = dict(live)
        out.append(s)
        try:
            await asyncio.wait_for(stop.wait(), 0.5)
        except asyncio.TimeoutError:
            pass


def schedule(cfg):
    """(provider, start, stop) per agent: base ramp, hold, spike, ramp-down."""
    D = cfg.duration
    plan = []
    for prov in PROVIDERS:
        nb, ns = round(cfg.base * cfg.scale), round(cfg.spike * cfg.scale)
        for k in range(nb):           # base: ramp up over first 20%, down over last 20%
            plan.append((prov, k * 0.2 * D / nb, 0.8 * D + k * 0.2 * D / nb))
        for k in range(ns):           # spike: burst in at 50%, leave by ~75%
            plan.append((prov, 0.5 * D + k * 0.033 * D / ns, 0.70 * D + k * 0.05 * D / ns))
    return plan


async def main(cfg):
    os.makedirs(cfg.out, exist_ok=True)
    binpath = os.path.join(HERE, "..", "target", "release", "pybox-gateway")
    env = dict(os.environ, RUST_LOG="warn")
    conf = open(os.path.join(HERE, "gateway.loadtest.toml")).read().replace(
        "max_concurrency = 20", f"max_concurrency = {cfg.max_concurrency}")
    conf_path = os.path.join(cfg.out, "gateway.toml")
    open(conf_path, "w").write(conf)
    gw = subprocess.Popen([binpath, "--config", conf_path],
                          env=env, stdout=open(os.path.join(cfg.out, "gateway.log"), "w"),
                          stderr=subprocess.STDOUT)
    conn = aiohttp.TCPConnector(limit=0, force_close=False)
    async with aiohttp.ClientSession(connector=conn) as session:
        for _ in range(50):
            try:
                async with session.get(GW + "/gateway/health") as r:
                    if r.status == 200:
                        break
            except Exception:
                await asyncio.sleep(0.1)
        plan = schedule(cfg)
        tokens = []
        for i, (prov, _, _) in enumerate(plan):
            async with session.post(GW + "/gateway/agents", headers=ADMIN,
                                    json={"id": f"{prov}-{i}"}) as r:
                tokens.append((await r.json())["token"])
        idle = proc_sample(gw.pid)
        log, samples, stop = [], [], asyncio.Event()
        live = {p: 0 for p in PROVIDERS}
        t0 = time.time() + 1.0
        samp = asyncio.create_task(sampler(gw.pid, t0, session, samples, stop, live))
        tasks = [asyncio.create_task(agent(f"{prov}-{i}", prov, tok, a, b, t0, session, log, cfg, live))
                 for i, ((prov, a, b), tok) in enumerate(zip(plan, tokens))]
        await asyncio.gather(*tasks)
        await asyncio.sleep(2)
        stop.set()
        await samp
        async with session.get(GW + "/gateway/stats", headers=ADMIN) as r:
            final_stats = await r.json()
    gw.send_signal(signal.SIGINT)
    gw.wait(timeout=10)
    with open(os.path.join(cfg.out, "requests.jsonl"), "w") as f:
        for r in log:
            f.write(json.dumps(r) + "\n")
    with open(os.path.join(cfg.out, "samples.jsonl"), "w") as f:
        for s in samples:
            f.write(json.dumps(s) + "\n")
    json.dump({"config": vars(cfg), "idle": idle, "agents": len(plan),
               "gateway_stats": final_stats}, open(os.path.join(cfg.out, "summary.json"), "w"), indent=1)
    ok = sum(1 for r in log if r.get("status") == 200)
    print(f"requests={len(log)} ok={ok} agents={len(plan)}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="results")
    ap.add_argument("--duration", type=float, default=300)
    ap.add_argument("--scale", type=float, default=1.0)
    ap.add_argument("--base", type=int, default=7, help="steady agents per provider")
    ap.add_argument("--spike", type=int, default=23, help="extra agents per provider in the spike")
    ap.add_argument("--stream-ratio", type=float, default=0.85)
    ap.add_argument("--in-min", type=int, default=6000, help="prompt bytes")
    ap.add_argument("--in-max", type=int, default=12000)
    ap.add_argument("--max-tokens", type=int, default=700)
    ap.add_argument("--max-concurrency", type=int, default=20, help="per-upstream slot cap")
    ap.add_argument("--think-max", type=float, default=0.5)
    asyncio.run(main(ap.parse_args()))
