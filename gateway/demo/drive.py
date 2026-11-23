#!/usr/bin/env python3
"""Sends demo traffic through the gateway and registers deliverables.

Three projects, three teams, eight agents. Each agent works in runs; a run
either carries a deliverable id on every request or is linked to one afterwards.
"""
import json, random, sys, threading, time, urllib.request, uuid

GW = "http://127.0.0.1:8787"
ADMIN = "demo-admin"
SECONDS = int(sys.argv[1]) if len(sys.argv) > 1 else 60

AGENTS = [  # id, project, team, user, upstream, model
    ("writer-1", "apollo", "growth", "dana", "seoul", "gpt-smart"),
    ("writer-2", "apollo", "growth", "minho", "seoul", "gpt-fast"),
    ("coder-1", "apollo", "platform", "jisoo", "tokyo", "gpt-smart"),
    ("coder-2", "borealis", "platform", "jisoo", "seoul", "gpt-smart"),
    ("coder-3", "borealis", "platform", "taeyang", "tokyo", "gpt-fast"),
    ("analyst-1", "borealis", "research", "yuna", "seoul", "gpt-fast"),
    ("analyst-2", "cascade", "research", "yuna", "tokyo", "gpt-smart"),
    ("triage-1", "cascade", "growth", "minho", "tokyo", "gpt-fast"),
]
SYSTEM = "You are a careful assistant working inside a sandbox. " * 30
KINDS = ["pr", "report", "doc", "ticket"]


def call(method, path, body=None, token=ADMIN, headers=None):
    req = urllib.request.Request(GW + path, method=method, data=json.dumps(body).encode() if body is not None else None,
                                 headers={"authorization": "Bearer " + token, "content-type": "application/json", **(headers or {})})
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            data = r.read()
            return r.status, data
    except urllib.error.HTTPError as e:
        return e.code, e.read()
    except Exception as e:  # noqa: BLE001
        return 0, str(e).encode()


def agent_loop(spec, token, deadline, stats):
    aid, project, team, user, upstream, model = spec
    rnd = random.Random(aid)
    n = 0
    while time.time() < deadline:
        run_id = f"run-{aid}-{n}"
        n += 1
        deliverable = f"{project}-{KINDS[n % 4]}-{aid}-{n}"
        tag_requests = rnd.random() < 0.6          # otherwise linked afterwards, or left unattributed
        history = [{"role": "system", "content": SYSTEM + f" Agent {aid}."}]
        for step in range(rnd.randint(2, 6)):
            history.append({"role": "user", "content": f"step {step} of {run_id}: " + "detail " * rnd.randint(5, 80)})
            headers = {"x-gateway-run-id": run_id, "x-gateway-client-sent-at": str(int(time.time() * 1000) - rnd.randint(1, 12))}
            if tag_requests:
                headers["x-gateway-deliverable-id"] = deliverable
            body = {"model": model, "messages": history if rnd.random() > 0.08 else [{"role": "user", "content": "ping"}],
                    "stream": rnd.random() < 0.85}
            status, _ = call("POST", f"/{upstream}/v1/chat/completions", body, token, headers)
            stats[status] = stats.get(status, 0) + 1
            history.append({"role": "assistant", "content": "ok " * 20})
            time.sleep(rnd.uniform(0.05, 0.6))
        roll = rnd.random()
        if roll < 0.85:
            accepted = rnd.random() < 0.7
            call("POST", "/gateway/obs/meta/deliverable", {
                "id": deliverable, "project_id": project, "team_id": team, "kind": KINDS[n % 4],
                "title": f"{KINDS[n % 4].upper()} {n} by {aid}", "status": "accepted" if accepted else rnd.choice(["rejected", "in_progress"]),
                **({"hours_saved": round(rnd.uniform(0.02, 0.4), 2)} if rnd.random() < 0.7 else {"value_usd": round(rnd.uniform(0.5, 12), 2)}),
                "completed_at": int(time.time() * 1000)})
            if not tag_requests:
                call("POST", "/gateway/obs/meta/link", {"run_id": run_id, "deliverable_id": deliverable})


def main():
    for team, rate in [("growth", 55), ("platform", 80), ("*", 60)]:
        call("POST", "/gateway/obs/meta/rate", {"id": team, "hourly_usd": rate})
    deadline, stats, threads = time.time() + SECONDS, {}, []
    for spec in AGENTS:
        aid, project, team, user = spec[:4]
        status, data = call("POST", "/gateway/agents", {"id": aid, "project_id": project, "team_id": team, "user_id": user,
                                                        "session_id": f"sess-{aid}-{uuid.uuid4().hex[:6]}", "max_concurrency": 4})
        token = json.loads(data)["token"]
        for _ in range(2):
            t = threading.Thread(target=agent_loop, args=(spec, token, deadline, stats))
            t.start()
            threads.append(t)
    for t in threads:
        t.join()
    print("status counts:", stats)


if __name__ == "__main__":
    main()
