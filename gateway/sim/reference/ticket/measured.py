#!/usr/bin/env python3
"""How right the gateway's told wait was in measured client logs.

Same rule as `report::summarize`: a ticket is a request tried once that
waited 250 ms or more at the gateway and was told a wait (`expected_ms`).
Usage: measured.py requests.jsonl [...]
"""
import json
import statistics
import sys

AFTER_MS, SLACK_S = 250.0, 2.0


def tickets(path):
    out = {}
    for line in open(path):
        r = json.loads(line)
        if r.get("expected_ms") is None or r.get("tries", 1) != 1:
            continue
        if r.get("queue_ms", 0.0) < AFTER_MS:
            continue
        out.setdefault(r["cls"], []).append(
            (r["expected_ms"] / 1000.0, r["queue_ms"] / 1000.0, r["status"] == 200)
        )
    return out


for path in sys.argv[1:]:
    for cls, rows in sorted(tickets(path).items()):
        seated = [t for t in rows if t[2]]
        late = sum(1 for told, waited, _ in rows if waited > 2 * told and waited - told >= SLACK_S)
        med = lambda v: statistics.median(v) if v else float("nan")
        print(
            f"{path}\t{cls}\ttickets {len(rows)}\ttold p50 {med([t[0] for t in seated]):.2f} s"
            f"\twaited p50 {med([t[1] for t in seated]):.2f} s"
            f"\toff p50 {med([abs(t[0] - t[1]) for t in seated]):.2f} s"
            f"\tlate {100 * late / len(rows):.1f}%"
        )
