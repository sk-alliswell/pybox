"""Prefix repetition in the Qwen-Bailian traces: how much of each prompt was
already sent, by the same conversation or by anyone, and how long ago."""
import json, sys, statistics as st
def q(v, p):
    v = sorted(v); return v[min(len(v) - 1, int(p * len(v)))] if v else None
out = {}
for name, path in [a.split("=") for a in sys.argv[2:]]:
    rows = [json.loads(l) for l in open(path)]
    rows.sort(key=lambda r: r["timestamp"])
    by_id = {}; trie = {}; last_seen = {}
    own, anyone, gap, tok_in, tok_out, anyone_w, own_w = [], [], [], [], [], 0, 0
    first_turn_any = []
    for r in rows:
        h = r["hash_ids"]; n = max(len(h), 1)
        # with the conversation's previous request
        p = by_id.get(r["parent_chat_id"])
        if p is not None:
            k = 0
            for a, b in zip(h, p["hash_ids"]):
                if a != b: break
                k += 1
            own.append(k / n); gap.append(r["timestamp"] - p["timestamp"])
        else:
            own.append(0.0)
        # with any earlier request
        node = trie; k = 0; walking = True
        for b in h:
            nxt = node.get(b)
            if nxt is None:
                walking = False; nxt = node[b] = {}
            elif walking: k += 1
            node = nxt
        anyone.append(k / n)
        if p is None: first_turn_any.append(k / n)
        own_w += own[-1] * r["input_length"]; anyone_w += anyone[-1] * r["input_length"]
        tok_in.append(r["input_length"]); tok_out.append(r["output_length"]); by_id[r["chat_id"]] = r
    N = len(rows); span = rows[-1]["timestamp"] - rows[0]["timestamp"]
    hist = lambda v: [round(sum(1 for x in v if lo <= x < hi) / len(v), 4) for lo, hi in [(0, .01), (.01, .25), (.25, .5), (.5, .75), (.75, .9), (.9, 1.01)]]
    out[name] = {
        "requests": N, "span_s": round(span), "requests_per_s": round(N / span, 2),
        "input_tokens": {"p50": q(tok_in, .5), "mean": round(st.mean(tok_in)), "p90": q(tok_in, .9)},
        "output_tokens": {"p50": q(tok_out, .5), "mean": round(st.mean(tok_out)), "p90": q(tok_out, .9)},
        "has_previous_turn": round(sum(1 for r in rows if r["parent_chat_id"] in by_id and r["parent_chat_id"] != -1) / N, 4),
        "share_repeating_previous_turn": {"mean": round(st.mean(own), 4), "token_weighted": round(own_w / sum(tok_in), 4), "p50": round(q(own, .5), 3), "hist_0_25_50_75_90_100": hist(own)},
        "share_repeating_anything_earlier": {"mean": round(st.mean(anyone), 4), "token_weighted": round(anyone_w / sum(tok_in), 4), "p50": round(q(anyone, .5), 3), "hist_0_25_50_75_90_100": hist(anyone)},
        "first_turns_repeating_anything_earlier_mean": round(st.mean(first_turn_any), 4) if first_turn_any else None,
        "gap_to_previous_turn_s": {"p10": round(q(gap, .1), 1), "p50": round(q(gap, .5), 1), "p90": round(q(gap, .9), 1), "p99": round(q(gap, .99), 1)} if gap else None,
    }
    print(name, json.dumps(out[name]), flush=True)
json.dump(out, open(sys.argv[1], "w"), indent=1)
