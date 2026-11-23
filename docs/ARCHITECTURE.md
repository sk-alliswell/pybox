# Architecture

`pybox` validates the allowlisted bundle and image identity, checks WHP/KVM,
acquires one of two OS-held leases, optionally prepares a clean snapshot, launches
the pinned `hluk` VMM under an OS guard, validates the workload result, and verifies
process/output-reader cleanup before reporting success.

- `src/lib.rs` exposes the `python_sandbox_controller` library API.
- `src/sandbox/` handles bundle and snapshot integrity, image execution,
  cancellation, execution leases, output and deadline bounds, and cleanup receipts.
- `src/main.rs` dispatches standalone CLI and optional manager commands.
- `src/host_gate.rs` coordinates standalone and managed execution with a per-user lock.
- `src/sandbox/platform/windows.rs` sets the Job Object at process birth and
  configures the explicit handle list, suspended launch, one CPU and kill-on-close.
- `src/sandbox/platform/unix.rs` provides an exec-only limiter, CPU and address-space
  limits, affinity, process-group cleanup and parent-death SIGKILL. The policy pipe
  stays CLOEXEC in the parent. The forked child clears it before entering the limiter.
- `guest/`: source and dependency locks for the actual guest workload.
- `scripts/`: development-only image conversion, packaging and notice collection.

Default guest RAM is 512 MiB. Execution limits are 15 seconds wall time, 8192 bytes
per output stream, Windows 384 MiB Job commit / one process, and Linux 5 CPU seconds
/ 1 GiB address space. Preparation has a 45-second wall bound and larger memory
limits (Windows 1536 MiB Job commit; Linux 3 GiB address space / 30 CPU seconds).
These host limits are not interchangeable with guest RAM or Docker cgroup limits.

Snapshots import frameworks and construct empty objects. They contain no model
requests, tool results or credentials. Snapshot identity covers image/VMM/ready
code and policy. Full file inventory and hashes are checked before reuse.

## Trust and limits

The hypervisor supplies the guest boundary. Hash manifests detect inconsistent
artifacts, but are not signed attestations and assume a trusted release source and
owner-controlled files. The low-level guard can launch arbitrary native programs;
calling it directly does not make a native program a guest.

The guest root is writable. Independent scratch quotas, Linux VMM namespaces and
seccomp, Windows AppContainer/LPAC, total host-memory accounting, general
process-memory checkpointing and multitenant service authorization are not implemented.
The CLI uses its declared Python image contract for both the fixed workload
and the registered agent. Networking is disabled. The prior workroom demo's network-enabled broker is not included.

## Management mode (v0.2)

The default Cargo feature `manager` enables `src/addons/manager.rs`, which
provides a loopback HTTP service, authenticated API, bounded queue and durable
job records. The embedded `console/` assets use that same API. With
`--no-default-features`, the standalone CLI still acquires the shared per-user
lock before accessing a bundle. Manager worker threads call the sandbox API
directly and use the same image, lease and process guard policy. Thread-local guard
monitors report native spawn, output and verified reap events. They do not grant
additional execution capabilities.

The manager owns at most two dispatched operations; snapshot preparation is
serialized. Current VMM count is based on native events, while active work also
includes integrity verification and waiting for preparation. Settings are copied
into each job at submission. A supervisor crash leaves interrupted, cleanup-unknown
history and blocks new launches. Legacy and other-user processes are out of scope.
See MANAGER.md for the API and exact persistence/recovery boundaries.

## Agent lifecycle

`src/agent_runtime/` owns spawning, memory flush and resume. The standalone CLI
takes the same per-user gate and bundle leases as the native runner. The guard
observes the guest's flush frame while the VM is alive and waits for durable
publication before stopping it. The runtime records verified cleanup separately
and claims a checkpoint before starting its continuation. See the
[agent lifecycle contract](AGENT-LIFECYCLE.md) for state and retry behavior.

## External services

`gateway/` is a standalone host-side Cargo workspace for LLM/MCP routing,
request limits and telemetry. Its simulator is a separate development tool.
The sandbox and local manager build independently of both. See the
[agent runtime roadmap](AGENT-RUNTIME-ROADMAP.md) for feature ownership and
verification targets.

## Ownership

New sandbox work belongs here. QC retains its historical experiment copies and
existing application behavior. This extraction does not silently switch QC's
runtime or rewrite its experiment evidence. A future QC dependency should pin a
Pybox release/version and preserve its own integration tests.
