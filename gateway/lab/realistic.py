#!/usr/bin/env python3
"""A compressed working day of agent traffic through the gateway.

Unlike drive.py (identical short prompts), this imitates how agents really
use an LLM API, including the habits that waste money:

  ide-*       coding assistants: long shared system prompt, multi-turn sessions
              whose history grows with tool results, some turns cancelled by the
              user. Variants: Anthropic with / without cache_control, OpenAI with
              a stable prefix / with a timestamp at the top of the system prompt.
  support-*   RAG help-desk bots: one-shot questions drawn from a skewed FAQ
              list (many exact repeats), half non-streaming, and an impatient
              client that resends when an answer takes too long.
  research-*  an orchestrator that fans one task out to parallel sub-calls
              sharing a long prefix, then synthesizes.
  triage      a ticket classifier on a small local model, arriving in bursts.
  nightly-eval a batch someone starts in the middle of the afternoon peak.
  runaway     an agent stuck in a loop, sending the same request over and over.

Interactive load follows a day curve (morning ramp, lunch dip, afternoon peak).

Usage: realistic.py --seconds 480 --out day.jsonl [--profile base|tuned|fair]
  tuned: gives the runaway agent a token budget and the batch a concurrency
  limit (the gateway config decides the cache; see gateway.realistic-*.toml).
"""
import argparse
import asyncio
import hashlib
import json
import math
import random
import time

import aiohttp

GATEWAY = "http://127.0.0.1:8787"
ADMIN = "lab-admin"
UPSTREAM = {"qwen-0.5b": "qwen", "smollm2-360m": "smol"}

SENTENCES = [
    "Prefer small, reviewable changes over sweeping rewrites.",
    "Every public function needs a doc comment that states its contract.",
    "Never log secrets, tokens, or personal data, even at debug level.",
    "When a test fails, find the root cause before touching the assertion.",
    "Database migrations must be reversible and safe to run twice.",
    "Use the repository's existing helpers before adding a dependency.",
    "Error messages should say what happened and what the caller can do.",
    "Keep handlers thin: parse, call the service layer, format the response.",
    "Timeouts are mandatory on every outbound network call.",
    "Feature flags default to off and are removed within two releases.",
    "Measure before optimizing and record the numbers in the pull request.",
    "Retries need jitter, a cap, and an idempotency key.",
]
FILES = ["src/billing/invoice.py", "src/api/routes.ts", "internal/queue/worker.go", "app/models/user.rb",
         "services/search/indexer.rs", "web/components/Table.tsx", "infra/terraform/main.tf"]
ASKS = ["Why does this function return early?", "Add input validation here.", "Write a unit test for the edge case.",
        "Refactor this to remove the duplication.", "What is the time complexity?", "Find the bug in this handler.",
        "Rename the variables to match our conventions.", "Explain what this module does."]
FAQ = [f"How do I {verb} {thing}?" for verb in ["reset", "export", "cancel", "upgrade", "share", "restore", "merge", "archive"]
       for thing in ["my password", "an invoice", "a subscription", "a workspace", "my API key"]]


def filler(tokens, seed):
    """Deterministic prose of roughly `tokens` tokens."""
    rng = random.Random(seed)
    out, n = [], 0
    while n < tokens:
        s = rng.choice(SENTENCES)
        out.append(s)
        n += len(s) // 4 + 1
    return " ".join(out)


def code(lines, seed):
    rng = random.Random(seed)
    return "\n".join(f"{i + 1:4}  def step_{rng.randrange(9999)}(ctx, item): return ctx.apply(item, mode={rng.randrange(8)})"
                     for i in range(lines))


def day(frac):
    """Interactive intensity over the day, 0.25..1."""
    pts = [(0, 0.25), (0.2, 0.9), (0.4, 0.95), (0.5, 0.5), (0.6, 0.9), (0.75, 1.0), (0.9, 0.5), (1, 0.25)]
    for (x0, y0), (x1, y1) in zip(pts, pts[1:]):
        if frac <= x1:
            return y0 + (y1 - y0) * (frac - x0) / (x1 - x0)
    return 0.25


class Run:
    def __init__(self, session, out, t0, seconds):
        self.session, self.out, self.t0, self.seconds = session, out, t0, seconds
        self.tokens = {}

    def frac(self):
        return (time.monotonic() - self.t0) / self.seconds

    def live(self):
        return self.frac() < 1

    async def pause(self, base):
        """Think time, shorter when the day is busy."""
        await asyncio.sleep(random.uniform(0.6, 1.4) * base / day(min(self.frac(), 1)))

    async def call(self, agent, persona, model, messages, *, run_id, max_tokens=160, stream=True, abort_after=None,
                   turn=0, note=""):
        body = {"model": model, "max_tokens": max_tokens, "messages": messages}
        if stream:
            body["stream"] = True
            body["stream_options"] = {"include_usage": True}
        raw = json.dumps(body, sort_keys=True).encode()
        rec = {"ts": time.time(), "frac": round(self.frac(), 4), "agent": agent, "persona": persona, "model": model,
               "run_id": run_id, "turn": turn, "stream": stream, "hash": hashlib.sha256(raw).hexdigest()[:16],
               "status": 0, "note": note, "req_bytes": len(raw)}
        headers = {"authorization": f"Bearer {self.tokens[agent]}", "content-type": "application/json",
                   "x-gateway-run-id": run_id, "x-gateway-client-sent-at": str(int(time.time() * 1000))}
        t0, text = time.monotonic(), ""
        try:
            async with self.session.post(f"{GATEWAY}/{UPSTREAM.get(model, 'cloud')}/v1/chat/completions",
                                         headers=headers, data=raw) as res:
                rec["status"] = res.status
                rec["gw_cache"] = res.headers.get("x-gateway-cache", "")
                if res.status != 200:
                    rec["error"] = (await res.text())[:160]
                elif not stream:
                    data = await res.json()
                    text = (data["choices"][0]["message"].get("content") or "")
                    usage = data.get("usage") or {}
                    rec.update(inp=usage.get("prompt_tokens", 0), out=usage.get("completion_tokens", 0),
                               cached=(usage.get("prompt_tokens_details") or {}).get("cached_tokens") or 0)
                else:
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
                            piece = (choices[0].get("delta") or {}).get("content") if choices else None
                            if piece:
                                rec.setdefault("ttft", time.monotonic() - t0)
                                text += piece
                            usage = ev.get("usage")
                            if usage:
                                rec.update(inp=usage.get("prompt_tokens", 0), out=usage.get("completion_tokens", 0),
                                           cached=(usage.get("prompt_tokens_details") or {}).get("cached_tokens") or 0)
                        if abort_after and "ttft" in rec and time.monotonic() - t0 - rec["ttft"] > abort_after:
                            rec["aborted"] = True
                            break
        except Exception as e:  # noqa: BLE001
            rec["error"] = f"{type(e).__name__}: {e}"[:160]
        rec["total"] = time.monotonic() - t0
        self.out.write(json.dumps(rec) + "\n")
        return text


# ---- personas

async def ide(run, n):
    agent = f"ide-{n}"
    variant = ["anthropic-cached", "anthropic-cached", "anthropic-plain", "openai-stable", "openai-stable",
               "openai-stable", "openai-timestamp", "openai-timestamp"][n]
    model = "claude-haiku" if variant.startswith("anthropic") else "gpt-4o-mini"
    guide = "You are a coding assistant for the platform team. Follow the engineering handbook below.\n\n" + filler(4600, 1)
    await asyncio.sleep(random.uniform(0, 6))
    s = 0
    while run.live():
        s += 1
        run_id = f"{agent}-s{s}"
        history = []
        for turn in range(random.randint(5, 10)):
            if not run.live():
                return
            path = random.choice(FILES)
            history.append({"role": "user", "content": f"{random.choice(ASKS)}\n\nTool result for read_file({path}):\n"
                            + code(random.randint(20, 70), random.random())})
            system = guide
            if variant == "openai-timestamp":
                # A common habit that defeats provider prompt caching: volatile text first.
                system = f"Current time: {time.strftime('%Y-%m-%d %H:%M:%S')}  Request id: {random.random()}\n\n" + guide
            if variant == "anthropic-cached":
                sys_msg = {"role": "system", "content": [{"type": "text", "text": system, "cache_control": {"type": "ephemeral"}}]}
            else:
                sys_msg = {"role": "system", "content": system}
            reply = await run.call(agent, "ide", model, [sys_msg] + history, run_id=run_id, max_tokens=180, turn=turn,
                                   abort_after=1.0 if random.random() < 0.08 else None, note=variant)
            history.append({"role": "assistant", "content": reply or "(cancelled)"})
            await run.pause(5)
        await run.pause(8)


async def support(run, n):
    agent = f"support-{n}"
    system = "You answer customer questions for a SaaS product using only the documentation below.\n\n" + filler(900, 2)
    weights = [1 / (i + 1) for i in range(len(FAQ))]  # a few questions dominate
    i = 0
    await asyncio.sleep(random.uniform(0, 5))
    while run.live():
        i += 1
        question = random.choices(FAQ, weights)[0]
        docs = filler(700, question)  # the same question retrieves the same passages
        messages = [{"role": "system", "content": system},
                    {"role": "user", "content": f"Documentation excerpts:\n{docs}\n\nQuestion: {question}"}]
        stream = n % 2 == 0
        first = asyncio.create_task(run.call(agent, "support", "gemini-flash-lite", messages, run_id=f"{agent}-q{i}",
                                             max_tokens=140, stream=stream))
        done, _ = await asyncio.wait([first], timeout=2.5)
        if not done:
            # Impatient client: send it again and take whichever finishes.
            again = asyncio.create_task(run.call(agent, "support", "gemini-flash-lite", messages, run_id=f"{agent}-q{i}",
                                                 max_tokens=140, stream=stream, note="client-resend"))
            await asyncio.wait([first, again])
        await run.pause(3)


async def research(run, n):
    agent = f"research-{n}"
    i = 0
    await asyncio.sleep(random.uniform(2, 10))
    while run.live():
        i += 1
        run_id = f"{agent}-task{i}"
        corpus = "Background material shared by every sub-question.\n\n" + filler(2200, f"{agent}-{i}")
        subs = [f"{corpus}\n\nSub-question {k}: what does the material imply about {topic}?"
                for k, topic in enumerate(["cost", "latency", "reliability", "security", "team workflow", "migration risk"])]
        answers = await asyncio.gather(*[
            run.call(agent, "research", "gpt-4o-mini", [{"role": "user", "content": s}], run_id=run_id, max_tokens=120, turn=k,
                     note="fan-out") for k, s in enumerate(subs)])
        await run.call(agent, "research", "gpt-4o-mini",
                       [{"role": "user", "content": corpus + "\n\nFindings:\n" + "\n".join(a or "" for a in answers) + "\n\nWrite a five-line summary."}],
                       run_id=run_id, max_tokens=200, turn=9, note="synthesis")
        await run.pause(14)


async def triage(run):
    i = 0
    while run.live():
        burst = random.randint(1, 5)  # tickets arrive in clumps
        await asyncio.gather(*[
            run.call("triage", "triage", random.choice(["qwen-0.5b", "smollm2-360m"]),
                     [{"role": "user", "content": f"Classify as bug, billing, or how-to. Answer with one word.\nTicket: {random.choice(FAQ)} It stopped working after the update."}],
                     run_id=f"triage-b{i}", max_tokens=6) for _ in range(burst)])
        i += 1
        await run.pause(6)


async def nightly_eval(run, workers):
    while run.frac() < 0.55:
        await asyncio.sleep(1)
    system = "You grade answers for an evaluation suite. Apply the rubric strictly.\n\n" + filler(1300, 3)

    async def worker(w):
        i = 0
        while run.frac() < 0.85:
            i += 1
            await run.call("nightly-eval", "batch", "gpt-4o-mini",
                           [{"role": "system", "content": system},
                            {"role": "user", "content": f"Case {w}-{i}: grade this answer from 1 to 5 and justify in two sentences.\n" + filler(120, f"{w}-{i}")}],
                           run_id=f"eval-shard-{w % 8}", max_tokens=90)

    await asyncio.gather(*[worker(w) for w in range(workers)])


async def runaway(run):
    while run.frac() < 0.35:
        await asyncio.sleep(1)
    messages = [{"role": "system", "content": "You are an autonomous agent. Continue the task.\n\n" + filler(2600, 4)},
                {"role": "user", "content": "The previous step failed with: tool `deploy` returned exit code 1. Try again."}]
    while run.live():
        await run.call("runaway", "runaway", "claude-haiku", messages, run_id="runaway-loop", max_tokens=150)
        await asyncio.sleep(0.2)


AGENTS = (
    [(f"ide-{n}", "platform", "ide-assistant") for n in range(8)]
    + [(f"support-{n}", "cs", "helpdesk") for n in range(6)]
    + [(f"research-{n}", "strategy", "market-research") for n in range(2)]
    + [("triage", "cs", "helpdesk"), ("nightly-eval", "ml", "eval-suite"), ("runaway", "platform", "release-bot")]
)


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seconds", type=float, default=480)
    ap.add_argument("--out", required=True)
    ap.add_argument("--profile", choices=["base", "tuned", "fair"], default="base")
    ap.add_argument("--seed", type=int, default=7)
    a = ap.parse_args()
    random.seed(a.seed)
    admin = {"authorization": f"Bearer {ADMIN}"}
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=300), connector=aiohttp.TCPConnector(limit=0)) as session:
        with open(a.out, "w", buffering=1) as out:
            run = Run(session, out, time.monotonic(), a.seconds)
            for agent, team, project in AGENTS:
                await session.delete(f"{GATEWAY}/gateway/agents/{agent}", headers=admin)
                body = {"id": agent, "team_id": team, "project_id": project, "user_id": agent.split("-")[0],
                        "session_id": f"{agent}-{int(time.time())}"}
                if a.profile in ("tuned", "fair"):
                    if agent == "runaway":
                        body["token_budget"] = 150_000
                    if agent == "nightly-eval":
                        body["max_concurrency"] = 24
                    if agent.startswith(("ide", "support", "research")):
                        body["weight"] = 4
                if a.profile == "fair":
                    interactive = agent.startswith(("ide", "support", "research"))
                    body["priority"] = "interactive" if interactive else "batch"
                async with session.post(f"{GATEWAY}/gateway/agents", headers=admin, json=body) as res:
                    run.tokens[agent] = (await res.json())["token"]
            run.t0 = time.monotonic()
            await asyncio.gather(
                *[ide(run, n) for n in range(8)], *[support(run, n) for n in range(6)], *[research(run, n) for n in range(2)],
                triage(run), nightly_eval(run, 60), runaway(run))
    print(f"{a.profile}: done -> {a.out}")


if __name__ == "__main__":
    asyncio.run(main())
