# Architecture

`pybox` validates the allowlisted bundle and image identity, checks WHP/KVM,
acquires one of two OS-held leases, optionally prepares a clean snapshot, launches
the pinned `hluk` VMM under an OS guard, validates the workload result, and verifies
process/output-reader cleanup before reporting success.

- `src/lib.rs`: artifact validation, image identity, preparation and leases.
- `src/main.rs`: CLI, cancellation, fixed workload and final JSON.
- `src/guard.rs`: output/deadline bounds and cleanup receipts.
- `src/platform/windows.rs`: Job Object at process birth, explicit handle list,
  suspended launch, one CPU and kill-on-close.
- `src/platform/unix.rs`: exec-only limiter, CPU/address-space limits, affinity,
  process-group cleanup and parent-death SIGKILL. The policy pipe stays CLOEXEC in
  the parent; only the forked child clears it before entering the limiter.
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
seccomp, Windows AppContainer/LPAC, total host-memory accounting, persistent guest
checkpoints and a multitenant service authorization layer are not implemented.
The CLI only supports its declared fixed Python image contract. Networking is
disabled. The prior workroom demo's network-enabled broker is not included.

## Management mode (v0.2)

`src/manager.rs` adds a loopback HTTP service, a single-user OS lock, an authenticated
API, bounded queue and durable job records. The embedded `console/` assets use that
same API. Manager worker threads call the existing image/lease/guard functions;
there is no shell/Python host helper or weakened launch policy. Thread-local guard
monitors report native spawn, output and verified reap events. They do not grant
additional execution capabilities.

The manager owns at most two dispatched operations; snapshot preparation is
serialized. Current VMM count is based on native events, while active work also
includes integrity verification and waiting for preparation. Settings are copied
into each job at submission. A supervisor crash leaves interrupted, cleanup-unknown
history and blocks new launches. Legacy and other-user processes are out of scope.
See MANAGER.md for the API and exact persistence/recovery boundaries.

## Ownership

New sandbox work belongs here. QC retains its historical experiment copies and
existing application behavior. This extraction does not silently switch QC's
runtime or rewrite its experiment evidence. A future QC dependency should pin a
Pybox release/version and preserve its own integration tests.
