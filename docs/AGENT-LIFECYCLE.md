# Agent spawn, memory flush and resume

The standalone CLI runs a registered LangGraph checklist agent through one
cooperative pause boundary. The agent builds a deduplicated checklist from
the input, pauses before producing the result, and resumes from that state in
a fresh VM. Use a current controller build with the verified Pybox runtime
bundle.

## Run the lifecycle

Create a UTF-8 JSON input file with 1 to 12 checklist entries. Each entry is
limited to 128 UTF-8 bytes, and the complete input file is limited to 2048 bytes.

```json
{"items":["  review memory  ","Review memory","verify resume"]}
```

```sh
pybox agent spawn checklist --input input.json --root /path/to/bundle --events
pybox agent status RUN_ID --root /path/to/bundle
pybox agent resume RUN_ID CHECKPOINT_ID --root /path/to/bundle --events
```

Use the run ID and checkpoint ID printed by `spawn`. A successful spawn reports
`paused` and shows verified VM reclamation. Resume takes its input from the
checkpoint and returns the two deduplicated items. Repeating the same resume
command after completion returns the recorded result with `"replayed": true`.
If another process currently owns the run lock, wait for it to release the lock
before requesting the result again.

## Publication and execution guarantees

- **Spawn.** The controller verifies the bundle, takes one of two bundle execution
  leases, and launches a new guarded VM without networking. The registered agent
  and the checkpoint adapter ship in the controller.
- **Memory flush.** The guest exports the bounded LangGraph saver state, writes
  a flush frame, and waits. The controller validates its binding, syncs and
  publishes the immutable memory record, and then stops the VM. It publishes
  the resumable marker only after it verifies process exit and pipe cleanup.
- **Resume.** The controller checks the requested checkpoint ID, stored memory
  digest, image identity and agent code. It serializes callers with an OS lock
  and publishes a single-use resume claim before starting the fresh VM. It
  commits a completed result only after the guest acknowledges the claimed
  checkpoint and the controller verifies VM cleanup.
- **Interruption.** An incomplete flush leaves no resumable marker. A committed
  claim without a completed result is reported as `resume_outcome_uncertain`.
  That claim blocks automatic retries even if the controller stopped before
  it could launch the VM.

Records are local to the verified bundle under `state/agents/RUN_ID`. Keep
them on an owner-controlled local filesystem. They contain the retained task
state. The guest receives that state as a bounded argument, so avoid putting
credentials in agent inputs. The guest adapter serializes its LangGraph saver
as bounded data, with pickle fallback disabled. The host does not import the
guest adapter or deserialize Python objects.

The supported path is a single cooperative LangGraph checkpoint for the
registered checklist agent. Full Python process memory, arbitrary agent
registration, external effects, manual recovery from an uncertain attempt and
power-loss behavior are outside the qualification.

## Verify with real VMs

```sh
python scripts/verify_agent_lifecycle.py \
  --binary /path/to/pybox --bundle /path/to/bundle --out verification/agent-run
```

The verifier checks flush-before-reap order, independently checks the old
process exited, and compares VM identities. It corrupts a checkpoint and
tests a wrong checkpoint ID, races two resume requests, checks replay in a
separate CLI process, models interruption after the durable claim, and forces a
memory staging failure while the agent runs. It also checks the resumed
result after deleting the original input file.

## Verification on 2026-11-23

| Platform | Agent lifecycle | Existing native runtime |
| --- | --- | --- |
| Windows x64 / WHP | 6 passed | 28 passed |
| Linux x64 under WSL2 / KVM | 6 passed | 28 passed |

Both lifecycle runs recorded one VM launch across the two concurrent resume
requests and zero launches for a repeat after completion. The saved checklist
produced the expected result after the original input file was removed.
