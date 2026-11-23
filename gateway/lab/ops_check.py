#!/usr/bin/env python3
"""Day-to-day operation checks on local model servers.

Three single-slot llama.cpp servers behind one gateway upstream, two callers
sending short streaming chats all the time. The script kills and restarts
servers and the gateway and writes down what callers saw:

  1. one server dies and comes back, with health probes off and on;
  2. every server dies: how fast and how a caller is told, and the recovery;
  3. the gateway restarts: an agent made over the API still works, the
     outage record is still there;
  4. callers report their own timings; the gateway writes its report.

    LAB=~/lab python lab/ops_check.py --out ~/lab/ops
"""
import argparse, asyncio, json, os, signal, subprocess, sys, time
from pathlib import Path

import httpx

HERE = Path(__file__).resolve().parent
LAB = Path(os.environ.get("LAB", Path.home() / "lab"))
GW = "http://127.0.0.1:8787"
ADMIN = {"authorization": "Bearer lab-admin"}
PORTS = {"s1": 9211, "s2": 9212, "s3": 9213}
MODEL = "qwen-0.5b"


class Stack:
    def __init__(self, out: Path, binary: Path):
        self.out, self.binary = out, binary
        self.servers, self.gw = {}, None

    def server_up(self, name):
        log = open(self.out / f"server-{name}.log", "ab")
        self.servers[name] = subprocess.Popen(
            [str(LAB / "venv/bin/python"), "-m", "llama_cpp.server", "--model",
             str(LAB / "models/qwen2.5-0.5b-instruct-q4_k_m.gguf"), "--model_alias", MODEL,
             "--host", "127.0.0.1", "--port", str(PORTS[name]), "--n_ctx", "2048", "--n_threads", "1",
             "--interrupt_requests", "false"], stdout=log, stderr=log)

    def server_down(self, name):
        p = self.servers.pop(name, None)
        if p:
            p.kill()
            p.wait()

    async def server_ready(self, name, timeout=90):
        end = time.time() + timeout
        async with httpx.AsyncClient() as c:
            while time.time() < end:
                try:
                    if (await c.get(f"http://127.0.0.1:{PORTS[name]}/v1/models", timeout=2)).status_code == 200:
                        return
                except httpx.HTTPError:
                    pass
                await asyncio.sleep(0.3)
        raise SystemExit(f"server {name} did not come up")

    def gateway_up(self, probe: int, data: Path):
        cfg = (HERE / "gateway.ops.toml").read_text().replace("__PROBE__", str(probe)).replace("__DIR__", str(data))
        path = self.out / "gateway.toml"
        path.write_text(cfg)
        log = open(self.out / "gateway.log", "ab")
        self.gw = subprocess.Popen([str(self.binary), "--config", str(path)], stdout=log, stderr=log)

    def gateway_down(self):
        if self.gw:
            self.gw.send_signal(signal.SIGINT)
            try:
                self.gw.wait(15)
            except subprocess.TimeoutExpired:
                self.gw.kill()
            self.gw = None

    async def gateway_ready(self):
        async with httpx.AsyncClient() as c:
            for _ in range(100):
                try:
                    if (await c.get(f"{GW}/gateway/health", timeout=1)).status_code == 200:
                        return
                except httpx.HTTPError:
                    pass
                await asyncio.sleep(0.1)
        raise SystemExit("gateway did not come up")

    def all_down(self):
        self.gateway_down()
        for n in list(self.servers):
            self.server_down(n)


async def chat(c: httpx.AsyncClient, token: str, n: int, report=True):
    """One streaming chat. Returns what the caller saw."""
    body = {"model": MODEL, "stream": True, "max_tokens": 16,
            "messages": [{"role": "user", "content": f"Say a short sentence about the number {n}."}]}
    t0 = time.time()
    rec = {"t": t0, "status": 0, "attempts": 0, "backend": "", "ttft_ms": None, "total_ms": None, "reason": ""}
    try:
        async with c.stream("POST", f"{GW}/local/v1/chat/completions", json=body,
                            headers={"authorization": f"Bearer {token}",
                                     "x-gateway-client-sent-at": str(int(t0 * 1000))}, timeout=60) as r:
            rec["status"] = r.status_code
            rec["attempts"] = int(r.headers.get("x-gateway-attempts", 0))
            rec["backend"] = r.headers.get("x-gateway-backend", "")
            rec["reason"] = r.headers.get("x-gateway-reject-reason", "")
            trace = r.headers.get("x-gateway-trace-id", "")
            async for line in r.aiter_lines():
                if line.startswith("data: ") and rec["ttft_ms"] is None and '"content"' in line:
                    rec["ttft_ms"] = (time.time() - t0) * 1000
            rec["total_ms"] = (time.time() - t0) * 1000
            if report and r.status_code == 200 and trace:
                await c.post(f"{GW}/gateway/obs/client", headers={"authorization": f"Bearer {token}"},
                             json={"trace_id": trace, "ttft_ms": rec["ttft_ms"], "total_ms": rec["total_ms"]})
    except httpx.HTTPError as e:
        rec["reason"] = type(e).__name__
        rec["total_ms"] = (time.time() - t0) * 1000
    return rec


async def load(stop: asyncio.Event, token: str, records: list, gap=0.15):
    n = 0
    async with httpx.AsyncClient() as c:
        while not stop.is_set():
            n += 1
            records.append(await chat(c, token, n))
            await asyncio.sleep(gap)


async def servers():
    async with httpx.AsyncClient() as c:
        return (await c.get(f"{GW}/gateway/servers", headers=ADMIN, timeout=5)).json()


def health(v, name):
    return next(s for s in v["upstreams"]["local"]["servers"] if s["name"] == name)["health"]


async def wait_health(name, want, timeout):
    t0 = time.time()
    while time.time() - t0 < timeout:
        if health(await servers(), name) in want:
            return time.time() - t0
        await asyncio.sleep(0.1)
    return None


async def one_server_dies(st: Stack, probe: int, out: Path):
    """Kill s2 under load, bring it back; what did callers see?"""
    data = out / f"data-probe{probe}"
    st.gateway_up(probe, data)
    await st.gateway_ready()
    stop, recs = asyncio.Event(), []
    tasks = [asyncio.create_task(load(stop, t, recs)) for t in ("lab-ide-token", "lab-chat-token")]
    await asyncio.sleep(12)
    killed = time.time()
    st.server_down("s2")
    noticed = await wait_health("s2", ("down",), 40)
    await asyncio.sleep(max(0.0, 20 - (time.time() - killed)))
    st.server_up("s2")
    await st.server_ready("s2")
    ready = time.time()
    # Until s2 serves a request again.
    back = None
    while time.time() - ready < 150:
        if any(r["backend"] == "s2" and r["status"] == 200 and r["t"] > ready for r in recs):
            back = min(r["t"] + (r["total_ms"] or 0) / 1000 for r in recs
                       if r["backend"] == "s2" and r["status"] == 200 and r["t"] > ready) - ready
            break
        await asyncio.sleep(0.2)
    await asyncio.sleep(5)
    stop.set()
    await asyncio.gather(*tasks)
    events = (await servers())["events"]
    st.gateway_down()
    during = [r for r in recs if killed <= r["t"] < ready]
    after = [r for r in recs if r["t"] >= ready]
    ok = [r for r in recs if r["status"] == 200]
    res = {
        "probe_secs": probe,
        "requests": len(recs),
        "answered": len(ok),
        "failed": len(recs) - len(ok),
        "retried_while_down": sum(1 for r in during if r["attempts"] > 1),
        "retried_after_restart": sum(1 for r in after if r["attempts"] > 1),
        "noticed_after_s": noticed and round(noticed, 2),
        "back_in_use_after_s": back and round(back, 2),
        "ttft_p50_ms": round(sorted(r["ttft_ms"] for r in ok if r["ttft_ms"])[len(ok) // 2]),
        "ttft_max_while_down_ms": round(max([r["ttft_ms"] or 0 for r in during] or [0])),
        "events": [(e["server"], e["state"], e["reason"], e["down_ms"]) for e in reversed(events)],
    }
    (out / f"one-server-probe{probe}.json").write_text(json.dumps({"summary": res, "requests": recs}, indent=1))
    return res


async def all_servers_die(st: Stack, out: Path):
    data = out / "data-probe2"
    st.gateway_up(2, data)
    await st.gateway_ready()
    async with httpx.AsyncClient() as c:
        made = (await c.post(f"{GW}/gateway/agents", headers=ADMIN,
                             json={"id": "made-at-runtime", "team_id": "platform", "user_id": "cho"})).json()
        token = made["token"]
        first = await chat(c, token, 1)
        for n in PORTS:
            st.server_down(n)
        killed = time.time()
        t_all = None
        while time.time() - killed < 30:
            if (await servers())["servers_down"] == 3:
                t_all = time.time() - killed
                break
            await asyncio.sleep(0.1)
        refusals = [await chat(c, token, i) for i in range(5)]
        async with httpx.AsyncClient() as raw:
            r = await raw.post(f"{GW}/local/v1/chat/completions", headers={"authorization": f"Bearer {token}"},
                               json={"model": MODEL, "messages": [{"role": "user", "content": "hi"}]})
            refusal_body, retry_after = r.json(), r.headers.get("retry-after")
        # The gateway restarts while everything is down.
        st.gateway_down()
        st.gateway_up(2, data)
        await st.gateway_ready()
        after_restart = await chat(c, token, 2)
        kept_events = len((await servers())["events"])
        agents = (await c.get(f"{GW}/gateway/agents", headers=ADMIN)).json()["agents"]
        for n in PORTS:
            st.server_up(n)
        started = time.time()
        for n in PORTS:
            await st.server_ready(n)
        ready = time.time()
        t_back = None
        while time.time() - ready < 30:
            if (await servers())["servers_down"] == 0:
                t_back = time.time() - ready
                break
            await asyncio.sleep(0.1)
        recovered = [await chat(c, token, i) for i in range(6)]
        # A short spell of ordinary work, so the report has something in it.
        stop, recs = asyncio.Event(), []
        tasks = [asyncio.create_task(load(stop, t, recs)) for t in ("lab-ide-token", "lab-chat-token", token)]
        await asyncio.sleep(25)
        stop.set()
        await asyncio.gather(*tasks)
        await asyncio.sleep(1.5)
        day = time.strftime("%Y-%m-%d", time.gmtime(time.time() + 9 * 3600))
        report = (await c.post(f"{GW}/gateway/reports/run?day={day}", headers=ADMIN, timeout=60)).json()
        ops = (await c.get(f"{GW}/gateway/ops/data", headers=ADMIN, timeout=30)).json()
    res = {
        "first_request_status": first["status"],
        "all_marked_down_after_s": t_all and round(t_all, 2),
        "refusals": [(r["status"], r["reason"], round(r["total_ms"])) for r in refusals],
        "refusal_message": refusal_body.get("error", {}).get("message"),
        "retry_after": retry_after,
        "after_gateway_restart": {
            "runtime_agent_status": after_restart["status"],
            "runtime_agent_reason": after_restart["reason"],
            "runtime_agent_listed": any(a["id"] == "made-at-runtime" for a in agents),
            "outage_events_kept": kept_events,
        },
        "servers_took_s_to_start": round(ready - started, 1),
        "all_back_after_ready_s": t_back and round(t_back, 2),
        "recovered": [(r["status"], r["backend"], r["attempts"]) for r in recovered],
        "steady": {"requests": len(recs), "answered": sum(r["status"] == 200 for r in recs)},
        "report": report,
        "client": ops["client"]["all"],
        "client_agents": ops["client"]["agents"],
    }
    (out / "all-servers.json").write_text(json.dumps(res, indent=1))
    (out / "ops-data.json").write_text(json.dumps(ops))
    st.gateway_down()
    return res


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=str(LAB / "ops"))
    ap.add_argument("--binary", default=str(HERE.parent / "tdb-backend/target/release/pybox-gateway-tdb"))
    a = ap.parse_args()
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    st = Stack(out, Path(a.binary))
    try:
        for n in PORTS:
            st.server_up(n)
        for n in PORTS:
            await st.server_ready(n)
        results = {}
        for probe in (0, 2):
            results[f"one_server_probe{probe}"] = await one_server_dies(st, probe, out)
            print(json.dumps(results[f"one_server_probe{probe}"], indent=1), flush=True)
        results["all_servers"] = await all_servers_die(st, out)
        print(json.dumps(results["all_servers"], indent=1), flush=True)
        (out / "summary.json").write_text(json.dumps(results, indent=1))
    finally:
        st.all_down()


if __name__ == "__main__":
    asyncio.run(main())
