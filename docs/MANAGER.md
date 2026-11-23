# Local manager and console

Start the manager from an extracted v0.2 runtime bundle:

```powershell
.\pybox.exe manage --open
```

On Linux use `./pybox manage --open`. The `run-console.cmd` / `run-console.sh`
launchers do the same. Without a desktop session, omit `--open` and use the local
API or CLI. This is a loopback-only console, not a remote or multitenant service.

The manager prefers `127.0.0.1:8790` and selects a free loopback port if that default
is already in use. Use `--port N` to require a particular port
(0 selects an available port). `--root DIR` registers one runtime bundle;
`--data DIR` changes the history/settings directory. Default state is
`~/.pybox/manager`. `pybox console` opens an authenticated browser tab for an
already-running manager. Ctrl+C gracefully cancels queued/active jobs and waits
for cleanup before exit. Closing the browser does not stop the manager.

## Console

- Overview: actual VMM spawn/reap count, active work including verification and
  preparation, queued work, completed/failed/cancelled totals.
- Instances/history: project label, immutable requested settings, state, current
  and last PID, stdout/stderr, lifecycle events, workload result and cleanup receipt.
- Image: one registered bundle's identity/digest, latest bundle verification and
  snapshot presence; enqueue a full verify or prepare operation.
- Settings: default timeout (1..15000 ms), cold vs snapshot restore and maximum
  concurrent operations (1..2). Lowering concurrency lets existing work finish.
  Changes affect later submissions, not already queued/running work.

Memory (512 MiB), output cap (8192 bytes per stream), network-disabled policy and
the fixed qualified workload remain read-only. This does not add arbitrary-script
execution, image upload, guest filesystem browsing or configurable network access.

## CLI and API

```sh
pybox jobs
pybox submit --project research-agent
pybox submit --project smoke --cold --timeout-ms 5000
pybox submit --project maintenance --action verify
pybox stop JOB_ID
```

CLI calls use the same API and queue as the console. `~/.pybox/endpoint.json`
contains the loopback authority, current manager PID and access token. It is a
private local credential; never publish it. The browser receives the token in a
fragment, removes the fragment from its displayed URL and retains it in tab session
storage. The API requires `Authorization: Bearer TOKEN`. JSON mutations also require
`Content-Type: application/json`. Host/Origin checks reject nonmatching origins.
No CORS permission is granted. Use HTTP/1.1 with bounded Content-Length bodies.

| Method | Path | Behavior |
|---|---|---|
| GET | `/api/overview` or `/api/jobs` | Counts, job summaries, defaults and image metadata |
| GET | `/api/jobs/ID` | Job details, captured output and receipts |
| POST | `/api/jobs` | `{project, action?: "run"/"prepare"/"verify", timeout_ms?, cold?}`; returns 202 with job |
| POST | `/api/jobs/ID/stop` | Request cancellation; terminal receipt is the final result |
| POST | `/api/stop-all` | Cancel queued and active work |
| GET/POST | `/api/settings` | Read or replace `{max_concurrent, timeout_ms, cold}` |
| POST | `/api/shutdown` | Stop owned work and shut down the service |

Project names are labels, not authorization or separate filesystem tenants. The
manager API is an experimental v0.2 contract. Clients must not infer completion
from request acceptance or HTTP connection closure.

## Ownership and lifecycle

One manager per OS user holds an exclusive OS lock, independent of `--data` and
bundle path. v0.2 standalone `run`/`prepare`/`status` hold a shared lock and refuse
while the manager is active; use manager commands instead. An already-running
standalone command also prevents manager startup. The underlying two bundle
leases and all VMM guards remain in force.

Counts cover this manager's work only. Older v0.1 executables, other OS users,
unrelated Docker containers and manually launched VMMs are not inventoried or
controlled. This is not a system-wide broker across all users.

Job state is persisted before dispatch. A live PID comes from the native spawn
event and is removed after verified process/readers cleanup. The last PID is
historical evidence only, never used to kill an unrelated reused PID. On restart,
undispatched jobs become cancelled; previously active jobs become interrupted with
cleanup unverified, and new jobs are blocked. There is no automatic replay or
automatic assertion that an interrupted VM was reclaimed. Investigate actual
owned processes before archiving such records while the manager is stopped.
There is deliberately no UI button that blindly clears cleanup uncertainty.

Each job retains up to 16 KiB per manager log stream (preparation and execution
combined); the existing per-phase guard cap remains 8 KiB. The queue holds at most
32 pending jobs, history at most 500 records. At the history limit, new requests
are refused; archive records while stopped. History contains labels, outputs and
host paths and must remain owner-controlled. No power-loss durability guarantee
or disk encryption is claimed. Unix state directories are mode 0700; Windows
uses the current user's profile-directory access control.
