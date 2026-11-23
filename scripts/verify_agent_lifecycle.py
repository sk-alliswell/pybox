"""Verify the agent lifecycle through the packaged CLI and actual VMs."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import ctypes
import json
import os
from pathlib import Path
import shutil
import subprocess


def reaped(pid):
    if os.name != "nt":
        return not Path(f"/proc/{pid}").exists()
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.OpenProcess.argtypes = (ctypes.c_uint32, ctypes.c_int, ctypes.c_uint32)
    kernel.OpenProcess.restype = ctypes.c_void_p
    kernel.WaitForSingleObject.argtypes = (ctypes.c_void_p, ctypes.c_uint32)
    kernel.CloseHandle.argtypes = (ctypes.c_void_p,)
    handle = kernel.OpenProcess(0x00100000, False, pid)
    if not handle:
        return ctypes.get_last_error() == 87  # ERROR_INVALID_PARAMETER, PID absent
    try:
        return kernel.WaitForSingleObject(handle, 0) == 0
    finally:
        kernel.CloseHandle(handle)


def events(stderr):
    objects = (json.loads(line) for line in stderr.splitlines() if line.startswith("{"))
    return [event for event in objects if "event" in event]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    out = args.out.resolve()
    out.mkdir(parents=True)
    bundle = out / "bundle"
    shutil.copytree(args.bundle, bundle, ignore=shutil.ignore_patterns("cache", "state"))
    if os.name != "nt":
        (bundle / "hluk").chmod(0o755)
    source = out / "input.json"
    source.write_text(
        json.dumps({"items": ["  review  memory ", "Review memory", "verify 새 VM"]}),
        encoding="utf-8",
    )

    def command(*parts):
        return [str(args.binary.resolve()), "agent", *parts, "--root", str(bundle)]

    def call(*parts):
        return subprocess.run(command(*parts), capture_output=True, encoding="utf-8", timeout=40)

    def success(result):
        assert result.returncode == 0, result.stderr
        return json.loads(result.stdout)

    def refused_without_vm(result):
        assert result.returncode != 0, result.stdout
        assert not any(event["event"] == "native_spawned" for event in events(result.stderr))

    def checked(name):
        print(name + ": passed", flush=True)

    start = call("spawn", "checklist", "--input", str(source), "--events")
    paused = success(start)
    run_id, checkpoint_id = paused["run_id"], paused["checkpoint_id"]
    spawn_events = events(start.stderr)
    assert [e["event"] for e in spawn_events] == [
        "agent_run_created", "native_spawned", "memory_flushed", "native_reaped",
    ], spawn_events
    spawn_pid = paused["spawn_vm"]["pid"]
    assert spawn_events[1]["pid"] == spawn_events[3]["pid"] == spawn_pid
    assert spawn_events[2]["checkpoint_id"] == checkpoint_id
    assert paused["status"] == "paused" and paused["spawn_vm"]["resources_reclaimed"]
    assert reaped(spawn_pid)
    assert success(call("status", run_id)) == paused
    checked("memory_flushed_before_verified_vm_reap")

    run = bundle / "state" / "agents" / run_id
    memory = run / "memory.json"
    original_memory = memory.read_bytes()
    corrupt = json.loads(original_memory)
    corrupt["memory"]["raw_sha256"] = "0" * 64
    memory.write_text(json.dumps(corrupt), encoding="utf-8")
    bad = call("resume", run_id, checkpoint_id, "--events")
    refused_without_vm(bad)
    assert "checkpoint integrity mismatch" in bad.stderr
    assert not (run / "resume.json").exists()
    memory.write_bytes(original_memory)
    wrong_id = ("0" if checkpoint_id[0] != "0" else "1") + checkpoint_id[1:]
    refused_without_vm(call("resume", run_id, wrong_id, "--events"))
    assert not (run / "resume.json").exists()
    checked("corrupt_and_wrong_checkpoints_do_not_launch_or_consume")

    source.unlink()
    with ThreadPoolExecutor(max_workers=2) as helpers:
        futures = [helpers.submit(call, "resume", run_id, checkpoint_id, "--events") for _ in range(2)]
        racers = [future.result() for future in futures]
    all_events = [e for racer in racers for e in events(racer.stderr)]
    assert sum(e["event"] == "resume_claimed" for e in all_events) == 1, all_events
    assert sum(e["event"] == "native_spawned" for e in all_events) == 1, all_events
    completed = [success(r) for r in racers if r.returncode == 0]
    winners = [r for r in completed if r["replayed"] is False]
    assert len(winners) == 1, [(r.stdout, r.stderr) for r in racers]
    winner = winners[0]
    assert winner["status"] == "completed"
    assert winner["result"] == {"items": ["review memory", "verify 새 VM"], "count": 2}
    assert winner["spawn_vm"] == paused["spawn_vm"]
    assert winner["resume_vm"]["guest_nonce"] != paused["spawn_vm"]["guest_nonce"]
    assert winner["resume_vm"]["pid"] != spawn_pid
    assert winner["resume_vm"]["resources_reclaimed"] and reaped(winner["resume_vm"]["pid"])
    checked("concurrent_requests_restore_in_one_fresh_vm")

    again = call("resume", run_id, checkpoint_id, "--events")
    replay = success(again)
    expected = {**winner, "replayed": True}
    assert replay == expected and events(again.stderr) == []
    assert success(call("status", run_id)) == replay
    checked("duplicate_resume_replays_result_without_launch")

    # Model interruption immediately after durable admission. The stored claim
    # keeps subsequent callers from repeating an execution with unknown outcome.
    source.write_text('{"items":["second run"]}', encoding="utf-8")
    another = success(call("spawn", "checklist", "--input", str(source)))
    another_id, another_checkpoint = another["run_id"], another["checkpoint_id"]
    claim = bundle / "state" / "agents" / another_id / "resume.json"
    claim.write_text(json.dumps({"checkpoint_id": another_checkpoint}), encoding="utf-8")
    ambiguous = call("resume", another_id, another_checkpoint, "--events")
    refused_without_vm(ambiguous)
    assert "outcome uncertain" in ambiguous.stderr
    assert success(call("status", another_id))["status"] == "resume_outcome_uncertain"
    checked("interrupted_resume_claim_fences_relaunch")

    def obstruct_flush(stderr):
        line = stderr.readline()
        created = events(line)
        assert len(created) == 1 and created[0]["event"] == "agent_run_created", line
        interrupted_id = created[0]["run_id"]
        pending = bundle / "state" / "agents" / interrupted_id / "memory.pending"
        pending.write_bytes(b"{incomplete")
        return interrupted_id, line

    process = subprocess.Popen(
        command("spawn", "checklist", "--input", str(source), "--events"),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8",
    )
    with ThreadPoolExecutor(max_workers=1) as helpers:
        try:
            interrupted_id, first_line = helpers.submit(obstruct_flush, process.stderr).result(timeout=25)
            stdout, stderr = process.communicate(timeout=25)
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)
    failed_events = events(first_line + stderr)
    failed = bundle / "state" / "agents" / interrupted_id
    assert process.returncode != 0 and "durable agent memory flush failed" in stderr, (stdout, stderr)
    assert [e["event"] for e in failed_events] == ["agent_run_created", "native_spawned", "native_reaped"]
    assert failed_events[1]["pid"] == failed_events[2]["pid"] and reaped(failed_events[2]["pid"])
    assert not (failed / "memory.json").exists() and not (failed / "ready.json").exists()
    assert success(call("status", interrupted_id))["status"] == "spawn_interrupted"
    refused_without_vm(call("resume", interrupted_id, checkpoint_id, "--events"))
    checked("interrupted_memory_write_leaves_no_resumable_state")

    print(json.dumps({
        "passed": True,
        "checks": 6,
        "spawn_pid": spawn_pid,
        "resume_pid": winner["resume_vm"]["pid"],
        "resume_launches_across_racers": 1,
        "duplicate_launches": 0,
    }))


if __name__ == "__main__":
    main()
