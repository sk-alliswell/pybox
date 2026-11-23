#!/usr/bin/env python3
"""Long tool-heavy agent runs against an OpenAI-format endpoint.

Each run is one conversation that grows by a tool call and a large tool
result per turn, the way a coding agent's does. Prints what the model
server counted as prompt tokens and how long the turns took, so the same
runs can be compared with and without `[lead] tool_results = "on"`.

    long_runs.py --base http://127.0.0.1:8787/local --token <agent token> \
        --model qwen --runs 2 --turns 12 --interleave
"""
import argparse
import json
import random
import time
import urllib.request

WORDS = ("agent build cache deploy error file graph index json kernel latency merge node "
         "output parse queue route schema token update value worker yield zone").split()


def text(rng, n):
    return " ".join(rng.choice(WORDS) for _ in range(n))


def call(base, token, body, run):
    req = urllib.request.Request(
        base + "/v1/chat/completions",
        data=json.dumps(body).encode(),
        headers={"content-type": "application/json", "authorization": f"Bearer {token}",
                 "x-gateway-run-id": run},
    )
    t = time.time()
    with urllib.request.urlopen(req, timeout=600) as r:
        out = json.load(r)
    return time.time() - t, out.get("usage", {})


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", required=True)
    ap.add_argument("--token", default="none")
    ap.add_argument("--model", default="local")
    ap.add_argument("--runs", type=int, default=2)
    ap.add_argument("--turns", type=int, default=12)
    ap.add_argument("--result-words", type=int, default=220)
    ap.add_argument("--interleave", action="store_true", help="alternate between runs turn by turn")
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--tag", default="run")
    a = ap.parse_args()
    rng = random.Random(a.seed)
    system = "You are a coding agent. Answer in one short sentence. " + text(rng, 150)
    convs = [[{"role": "system", "content": system},
              {"role": "user", "content": f"task {i}: {text(rng, 40)}"}] for i in range(a.runs)]
    order = ([(r, t) for t in range(a.turns) for r in range(a.runs)] if a.interleave
             else [(r, t) for r in range(a.runs) for t in range(a.turns)])
    secs, prompt, completion = [], 0, 0
    started = time.time()
    for r, t in order:
        msgs = convs[r]
        dt, usage = call(a.base, a.token, {"model": a.model, "messages": msgs, "max_tokens": 16,
                                           "temperature": 0}, f"{a.tag}-{r}")
        secs.append(dt)
        prompt += usage.get("prompt_tokens", 0)
        completion += usage.get("completion_tokens", 0)
        call_id = f"call_{r}_{t}"
        msgs.append({"role": "assistant", "content": "", "tool_calls": [
            {"id": call_id, "type": "function", "function": {"name": "read_file", "arguments": "{}"}}]})
        msgs.append({"role": "tool", "tool_call_id": call_id,
                     "content": f"result {t}: " + text(rng, a.result_words)})
    secs.sort()
    print(json.dumps({
        "requests": len(secs), "prompt_tokens": prompt, "completion_tokens": completion,
        "wall_s": round(time.time() - started, 1),
        "turn_p50_s": round(secs[len(secs) // 2], 2), "turn_p95_s": round(secs[int(len(secs) * 0.95) - 1], 2),
        "turn_mean_s": round(sum(secs) / len(secs), 2),
    }))


if __name__ == "__main__":
    main()
