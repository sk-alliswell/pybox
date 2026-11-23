# Runtime

For the management console, start `pybox manage --open` or use the supplied
`run-console.cmd` / `run-console.sh` launcher. See MANAGER.md for lifecycle,
settings, history and API details. Standalone commands below refuse while a manager
is active for the current OS user; use `pybox submit`, `pybox jobs` and `pybox stop ID`.

Use a fresh directory owned by the current user. Do not share write access to
runtime files, manifests, state or snapshots with untrusted users.

| Command | Behavior |
|---|---|
| `pybox status` | Verify bundle, virtualization access, cleanup marker and any existing snapshot; does not execute a guest self-test. |
| `pybox` / `pybox run` | Prepare once if needed, restore a fresh VM, execute the fixed framework workload. |
| `pybox prepare` | Prepare an empty graph/agent snapshot without running a user request. |
| `pybox --cold` | Boot a fresh guest without a prepared snapshot. |
| `pybox --root DIR` | Select a bundle directory independently of the executable location. |
| `pybox --events` | Emit lifecycle events to stderr; final JSON remains on stdout. |
| `pybox --timeout-ms N` | Execution deadline, 1 to 15000 ms; preparation has its own 45-second bound. |
| `pybox --cancel-after-ms N` | Development cancellation trigger; Ctrl+C also cancels owned work. |

On Windows use `.\pybox.exe`; on Linux use `./pybox`. Exit 0 means success,
1 means execution/refusal failure, and 2 indicates argument or signal-handler setup failure.

Two jobs per bundle directory are admitted using OS locks. This is not a global
machine limit across independent bundle copies. Any unresolved cleanup receipt
blocks later jobs. Investigate the retained `state/unclean.json` and owned processes
before clearing it; deleting the marker does not prove cleanup.

Runtime requirements: Windows x64 with WHP enabled, or Linux x86-64 with accessible
KVM. No privilege elevation, feature enablement, reboot or host execution fallback
is performed automatically. Snapshot/cache disk space and configured guest RAM
are separate from the VMM process metrics reported in JSON.
