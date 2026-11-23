# Agent runtime roadmap

This inventory covers a self-hosted runtime that accepts agent work, runs it
in an isolated VM, and manages its state across executions. It covers the v0.2
baseline and the registered agent lifecycle in this checkout. A draft PR or a
standalone service can provide evidence before a capability ships in the runtime.

- **Implemented** means this checkout exposes the behavior through a normal
  entry point, within the limits in the table.
- **Partial** means the indicated subset works and the remaining acceptance
  criterion is open.
- **Draft** means there is work in the single-host runtime draft, outside this
  checkout. **Planned** means the baseline has no implementation to credit.
- **Optional** means a host-side add-on can provide the capability separately
  from the runtime contract.

## Capability inventory

| Capability | Status | Current coverage and acceptance criterion |
| --- | --- | --- |
| Verified runtime and image | Implemented | The controller checks the bundle inventory and hashes, pins image identity, and refuses unsupported bundles. Qualification is limited to the declared image, VMM and workload. |
| VM isolation | Implemented | WHP on Windows and KVM on Linux provide guest isolation for the qualified Python image. The CLI policy disables networking and refuses execution when virtualization is unavailable. |
| Resource admission and limits | Implemented | Two OS-held leases per bundle, bounded execution time and output, and platform-specific host process limits. The v0.2 guest memory and workload are fixed. |
| Startup snapshot | Implemented | Build an empty snapshot under a preparation lock, verify its identity and inventory, then restore it for an execution. |
| Cancellation and cleanup | Implemented | Cancellation and deadlines stop owned processes. The CLI and manager report cleanup evidence and block launches when cleanup is uncertain. |
| Job submission and queue | Implemented | The default local manager accepts run, prepare and verify jobs through its authenticated loopback API. It bounds the queue, snapshots settings at submission and assigns a job ID. |
| Job status, results and logs | Implemented | The local CLI/API and console expose job state, bounded logs, workload results and cleanup receipts. The manager retains bounded job history on disk. |
| Agent spawning | Partial | The standalone CLI accepts the registered checklist agent with bounded inputs and assigns a run ID. Expand registration to further eligible agent definitions. |
| Child agents and concurrency budgets | Draft | Establish parent/child ownership, enforce inherited resource and cancellation policy, and preserve lineage as agents start and finish. |
| Pause, checkpoint and resume | Implemented | The registered agent pauses at one cooperative boundary. A digest-bound checkpoint can resume in a fresh VM through a single-use claim. Corrupt and stale requests are refused. |
| Agent-memory flush | Implemented | The checklist agent's bounded LangGraph saver state is synced and published before the controller stops its VM. Successful publication and verified cleanup are required for resume. Additional state types and handoff remain open. |
| Human approval and external waits | Draft | Correlate a decision or result with the waiting run and checkpoint. Wait without an active VM and accept only an authorized resolution that matches the pending request. |
| Workspace files and output artifacts | Draft | Persist explicitly allowed files and artifacts across VMs with ownership, byte limits, and verified restore. Test path confinement and workspace separation. |
| Workflow orchestration | Draft | Define ordered or dependent runs, record transitions, and preserve cancellation and recovery semantics across steps. |
| Crash and retry handling | Partial | On manager restart the v0.2 implementation cancels queued jobs, marks active ones interrupted, and blocks launches until cleanup is resolved. Add idempotent request replay and recovery from committed checkpoints. |
| External effects and deduplication | Draft | Give external effects stable identifiers and define retry behavior. After an ambiguous result, require reconciliation before repeating an effect that could have succeeded. |
| Agent and image registry | Draft | Pin immutable agent definitions and qualified image revisions. A run record must identify exactly what executed and which policy applied. |
| Runtime API and SDK | Partial | The experimental v0.2 loopback API covers native jobs. Define versioned spawn, status, cancel, checkpoint and resume contracts with conformance checks. |
| Telemetry and audit | Partial | Local lifecycle events, PID observations and cleanup receipts exist. Add correlated run, step and effect traces with redaction, retention and export criteria. |
| Backup, restore and upgrades | Draft | Restore durable state on a supported version, verify storage compatibility, and exercise rollback and recovery without replaying external effects. |
| Tool, network and secret policy | Partial | The current runner disables guest networking and supplies no host tool callbacks. For any future brokered access, scope credentials and approved tools to a run, audit calls and keep secrets out of checkpoint and log data. |
| Tenant and remote access policy | Planned | Define authorization, tenant boundaries, audit and per-tenant budgets before offering remote or shared-host runtime access. The v0.2 manager is single-user and loopback-only. |
| LLM/MCP routing, budgets and cost reporting | Optional | The standalone Gateway owns upstream credentials, request routing and usage accounting. Exercise any runtime integration through an explicit host service interface. |
| Simulation and operator tools | Optional | The gateway simulator, experimental analysis tools and workroom UI are outside the runtime contract. The v0.2 manager console is the default UI for local job operations. |

The startup snapshot contains empty framework state. The checklist lifecycle
persists selected graph state in a separate, run-scoped record. See the
[agent lifecycle documentation and verification](AGENT-LIFECYCLE.md).

Baseline evidence is in [architecture](ARCHITECTURE.md), [runtime commands](RUN.md),
[local manager](MANAGER.md) and [manager validation](MANAGER-VALIDATION.md).
Draft coverage comes from the [single-host runtime PR](https://github.com/MichaelAlbertIhm/pybox/pull/22).
Gateway capabilities are documented in the [Gateway README](../gateway/README.md).
The [Gateway draft PR](https://github.com/MichaelAlbertIhm/pybox/pull/26)
covers context compaction, evaluation and Git/CI deliverable tracking.

## Delivery order

1. **Keep the native baseline green.** Build the standalone CLI and the default
   manager. Check that both honor the execution lock and that image verification,
   launch, cancellation, cleanup and local job history still work.
2. **Complete the first agent lifecycle.** The standalone checklist agent can
   spawn, flush state, release its VM and reach its result once in a fresh VM.
   The native verification covers invalid checkpoints, interrupted writes
   and duplicate resume requests. Extend this contract when adding agents
   and additional checkpoint boundaries.
3. **Make multi-step execution recoverable.** Add approval, workspace artifacts,
   child-agent controls, effect reconciliation, idempotent submissions and
   durable recovery. Prove isolation between runs and correctness after a
   manager restart.
4. **Stabilize operation and integrations.** Qualify supported platforms and
   versions, document API compatibility, exercise backup and rollback, and
   add correlated traces. Integrate optional host services at explicit
   interfaces. Treat remote and multi-tenant operation as separate qualification.

Update the affected row and link its verification evidence when behavior
changes. Record support and test results separately for Windows/WHP, Linux
under WSL2/KVM, and any other environment actually exercised.
