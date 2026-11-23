#!/usr/bin/env python3
"""Run one experiment and keep everything about it in its own folder.

Every run gets `<root>/<UTC time>-<name>/`:

  manifest.json    what was run: commit, gateway config, command, machine, note
  gateway.toml     the gateway configuration used (a copy)
  gateway.log      the gateway's own output
  gateway-stats.json  /gateway/stats when the load ended
  driver.log       output of the load generator
  *.jsonl          raw per-request records written by the load generator
  summary.json     the analyzer's result

and one line in `<root>/index.jsonl` with the headline numbers, so runs can
be compared over time (`experiment.py list`, `experiment.py compare A B`).

A fresh gateway process is started for each run and stopped afterwards, so
no queue, cache, limit or log state leaks from one experiment into the next.
The model servers, LiteLLM and the mock provider are left running (stack.sh).

  experiment.py run NAME --config lab/gateway.X.toml [--note "..."] \\
      --drive  "lab/saturate.py --mode gateway --out {dir}/requests.jsonl ..." \\
      --analyze "lab/analyze_sat.py run={dir}/requests.jsonl"
  experiment.py run NAME --binary /path/to/older/pybox-gateway ...   (before/after on one machine)
  experiment.py run NAME --no-gateway --drive "... --mode direct ..." --analyze "..."
  experiment.py list [--kind sat|day|prefix|overload]
  experiment.py compare ID_A ID_B

`{dir}` in a command is the run's folder. Raw files are written to local
disk first and copied to the root at the end (the root may be a network
mount). Root: $GATEWAY_EXPERIMENTS, else /mnt/project-files/gateway-experiments
when /mnt/project-files exists, else ./experiments.
"""
import argparse
import datetime
import json
import os
import platform
import shutil
import signal
import subprocess
import sys
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
CRATE = os.path.dirname(HERE)
GATEWAY = "http://127.0.0.1:8787"
ADMIN = "lab-admin"
LAB = os.environ.get("LAB", os.path.expanduser("~/lab"))  # venv, models, local copies of runs


def root():
    if os.environ.get("GATEWAY_EXPERIMENTS"):
        return os.environ["GATEWAY_EXPERIMENTS"]
    if os.path.isdir("/mnt/project-files/"):
        return "/mnt/project-files/gateway-experiments"
    return os.path.join(CRATE, "experiments")


def sh(cmd):
    try:
        return subprocess.run(cmd, shell=True, capture_output=True, text=True, cwd=CRATE, timeout=20).stdout.strip()
    except Exception:  # noqa: BLE001
        return ""


def get(path):
    req = urllib.request.Request(GATEWAY + path, headers={"authorization": f"Bearer {ADMIN}"})
    return json.load(urllib.request.urlopen(req, timeout=10))


def headline(kind, s):
    """The few numbers worth a column in the index."""
    try:
        if kind == "sat":
            r = next(iter(s.values()))
            w = (r.get("wait_estimate") or {}).get("by_class", {})
            return {"interactive_answered": r["interactive_all"]["ok_share"], "interactive_ttft_p50_flood": r["interactive_flood"]["ttft_p50"],
                    "batch_answered": r["batch"]["ok_share"], "batch_wait_p50": r["batch"].get("wait_p50"), "batch_wait_p95": r["batch"].get("wait_p95"),
                    "all_done_s": r.get("all_done_s"), "retried": r["interactive_all"]["retried"] + r["batch"]["retried"],
                    "wait_estimate_rel_error_batch": (w.get("batch") or {}).get("rel_error_p50"),
                    "wait_estimate_rel_error_interactive": (w.get("interactive") or {}).get("rel_error_p50"),
                    "gateway_rss_max_mb": r["gateway_rss_mb"]["max"], "moved_to_stand_in": r.get("moved_to_stand_in")}
        if kind == "day":
            b = s["scoreboard"]
            return {k: b.get(k) for k in ("valid", "fresh_tokens", "fresh_tokens_per_unit_of_work", "fresh_tokens_at_baseline_work",
                                          "interactive_ttft_p95", "interactive_ttft_p50", "error_rate", "work_done", "cost_usd")}
        if kind == "overload":
            return {"answered_per_s": s.get("answered_per_s"),
                    **{f"{g}_{k}": v.get(k) for g, v in s["groups"].items() for k in ("answered_share", "ttft_p50", "ttft_p95", "refused", "gave_up")}}
        if kind == "prefix":
            return {k: s.get(k) for k in ("groups", "rate", "sessions", "think", "answered", "ttft_mean", "ttft_p50", "ttft_p95", "total_mean", "queue_mean",
                                          "server_reused_share", "server_hit_share", "gateway_warm_share", "last_answer_s")}
    except Exception as e:  # noqa: BLE001
        return {"headline_error": str(e)}
    return {}


def run(a):
    started = datetime.datetime.now(datetime.timezone.utc)
    run_id = f"{started:%Y%m%d-%H%M%S}-{a.name}"
    work = os.path.join(os.path.join(LAB, "experiments"), run_id)
    os.makedirs(work)
    manifest = {"id": run_id, "name": a.name, "kind": a.kind, "note": a.note, "started_at": started.isoformat(timespec="seconds"),
                "commit": sh("git rev-parse HEAD"), "branch": sh("git rev-parse --abbrev-ref HEAD"), "dirty": bool(sh("git status --porcelain -- .")),
                "config": a.config, "drive": a.drive, "analyze": a.analyze,
                "machine": {"cpus": os.cpu_count(), "platform": platform.platform(),
                            "mem_gb": round(os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") / 2**30, 1)},
                "stack": sh(f"cat {LAB}/stack.json 2>/dev/null") or None}
    gw = None
    try:
        if a.config:
            shutil.copy(os.path.join(CRATE, a.config), os.path.join(work, "gateway.toml"))
            binary = a.binary or os.path.join(CRATE, "target/release/pybox-gateway")
            manifest["gateway_binary"] = a.binary or "this commit"
            manifest["gateway_binary_built"] = datetime.datetime.fromtimestamp(os.path.getmtime(binary), datetime.timezone.utc).isoformat(timespec="seconds")
            gw = subprocess.Popen([binary, "--config", os.path.join(work, "gateway.toml")], cwd=work,
                                  stdout=open(os.path.join(work, "gateway.log"), "w"), stderr=subprocess.STDOUT)
            for _ in range(100):
                try:
                    urllib.request.urlopen(GATEWAY + "/gateway/health", timeout=1)
                    break
                except Exception:  # noqa: BLE001
                    time.sleep(0.1)
            else:
                raise SystemExit("the gateway did not come up; see " + os.path.join(work, "gateway.log"))
        json.dump(manifest, open(os.path.join(work, "manifest.json"), "w"), indent=1)
        t0 = time.time()
        with open(os.path.join(work, "driver.log"), "w") as log:
            code = subprocess.run(a.drive.replace("{dir}", work), shell=True, cwd=CRATE, stdout=log, stderr=subprocess.STDOUT).returncode
        manifest["drive_seconds"] = round(time.time() - t0, 1)
        manifest["drive_exit"] = code
        if gw:
            try:
                json.dump(get("/gateway/stats"), open(os.path.join(work, "gateway-stats.json"), "w"), indent=1)
            except Exception as e:  # noqa: BLE001
                manifest["stats_error"] = str(e)
        summary = {}
        if a.analyze:
            # The analyzer may query the gateway's log, so it runs before the gateway stops.
            out = subprocess.run(a.analyze.replace("{dir}", work), shell=True, cwd=CRATE, capture_output=True, text=True)
            try:
                summary = json.loads(out.stdout)
            except ValueError:
                manifest["analyze_error"] = (out.stderr or out.stdout)[-2000:]
            json.dump(summary, open(os.path.join(work, "summary.json"), "w"), indent=1, ensure_ascii=False)
    finally:
        if gw:
            gw.send_signal(signal.SIGTERM)
            try:
                gw.wait(10)
            except subprocess.TimeoutExpired:
                gw.kill()
    manifest["headline"] = headline(a.kind, summary)
    json.dump(manifest, open(os.path.join(work, "manifest.json"), "w"), indent=1)
    dest = os.path.join(root(), run_id)
    os.makedirs(root(), exist_ok=True)
    shutil.copytree(work, dest)
    line = {k: manifest.get(k) for k in ("id", "name", "kind", "started_at", "commit", "dirty", "config", "note", "drive_seconds", "headline")}
    with open(os.path.join(root(), "index.jsonl"), "a") as f:
        f.write(json.dumps(line, ensure_ascii=False) + "\n")
    print(json.dumps({"id": run_id, "folder": dest, "headline": manifest["headline"]}, indent=1))


def index():
    path = os.path.join(root(), "index.jsonl")
    return [json.loads(line) for line in open(path) if line.strip()] if os.path.exists(path) else []


def listing(a):
    rows = [r for r in index() if not a.kind or r["kind"] == a.kind]
    for r in rows:
        h = r.get("headline") or {}
        cells = "  ".join(f"{k}={v}" for k, v in h.items() if v is not None)
        if r.get("invalid"):
            print(f"{r['id']}  INVALID, do not compare")
            print(f"    {r.get('note', '')}")
            continue
        print(f"{r['id']}  [{r['kind']}] {(r.get('commit') or '')[:7]}{'*' if r.get('dirty') else ''}  {cells}")
        if r.get("note"):
            print(f"    {r['note']}")


def compare(a):
    by = {r["id"]: r for r in index()}
    x, y = by[a.a].get("headline") or {}, by[a.b].get("headline") or {}
    print(f"{'':40} {a.a:>32} {a.b:>32}  change")
    for k in dict.fromkeys([*x, *y]):
        u, v = x.get(k), y.get(k)
        change = ""
        if isinstance(u, (int, float)) and isinstance(v, (int, float)) and not isinstance(u, bool) and u:
            change = f"{(v - u) / u:+.0%}"
        print(f"{k:40} {str(u):>32} {str(v):>32}  {change}")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("name")
    r.add_argument("--kind", choices=["sat", "day", "prefix", "overload", "other"], default="other")
    r.add_argument("--config", help="gateway config, relative to the crate; omit with --no-gateway")
    r.add_argument("--no-gateway", action="store_true")
    r.add_argument("--binary", help="another gateway build, e.g. an older commit, to measure against")
    r.add_argument("--drive", required=True)
    r.add_argument("--analyze")
    r.add_argument("--note", default="")
    r.set_defaults(fn=run)
    ls = sub.add_parser("list")
    ls.add_argument("--kind")
    ls.set_defaults(fn=listing)
    c = sub.add_parser("compare")
    c.add_argument("a")
    c.add_argument("b")
    c.set_defaults(fn=compare)
    a = ap.parse_args()
    if a.cmd == "run" and not a.config and not a.no_gateway:
        ap.error("give --config or --no-gateway")
    a.fn(a)


if __name__ == "__main__":
    sys.exit(main())
