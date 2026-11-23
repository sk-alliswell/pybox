# Management console validation — 2026-11-06

The release binaries passed the existing 28-check sandbox lifecycle suite and the
new 26-check manager suite on both Windows/WHP and Ubuntu 22.04 under WSL2/KVM.
The Linux build is static musl. Physical Ubuntu is not qualified by these tests.
Sanitized results, binary and source hashes are in `validation-v0.2.0.json`.

| Area | Evidence |
|---|---|
| Native lifecycle | Existing core suite: 28 checks per platform, including integrity faults, cancellation, concurrency, controller crash and post-failure health |
| Manager API | 26 checks per platform: authentication/origin, default-port conflict, shared API/CLI queue, immutable settings, restart history and fail-closed interrupted work |
| Real concurrency | Two actual VMMs observed while a third request waited; stop-all reclaimed active work and cancelled queued work |
| Persistence | Defaults/history survived service restart; active records after a forced service crash became cleanup-unverified and blocked new jobs |
| Rust regression | Manager setting validation, queued cancellation, restart refusal and delayed split HTTP requests; existing Windows quoting test retained |
| Package regression | Three Python tests passed, including allowlists, digest failures, traversal refusal and retained Unicode/modes |
| Browser | Actual create/complete, stop/cancel with cleanup, result/log detail, settings save/reload, image verification and history navigation |
| Layout | Desktop 1440×960 and narrow 390×844 inspected; narrow document width remained 390px, with only the instance table scrolling horizontally |

## Corrections retained in local evidence

The first two Windows manager runs reproduced a connection-aborted failure during
rapid job polling. Windows accepted sockets inherited the listener's nonblocking
mode. Connection workers now explicitly switch to blocking I/O with read/write
deadlines. The delayed split-request regression protects this boundary, and the
fresh final Windows suite passed all 26 checks.

The initial browser launch found port 8790 occupied by another application. The
manager now picks a free loopback port when its default is occupied; explicit
`--port` requests still fail on conflicts. This behavior passed on both platforms.
No existing application was stopped or rebound.

The result detail tab now opens JSON at its beginning while output streams retain
their follow behavior. Browser authentication fragments are removed from the
displayed URL. No token, private state, raw host logs or initialized snapshot is
included in Git or runtime archives.

## Limits

The console manages one qualified bundle and a fixed Python workload. It does not
enumerate other OS users, legacy v0.1 controllers, arbitrary Docker containers or
unrelated VMMs. PID values are observational; the manager does not kill a historical
PID after restart. An interrupted cleanup record remains blocked for operator
investigation even if OS parent-death handling has already killed the VMM.

No remote access, arbitrary-script API, configurable network/mount policy,
production multitenant authorization or power-loss durability claim is made.
History/settings live under the current user's profile and require owner-controlled
storage. Hosted CI exercises unit/package/guard regressions, not WHP/KVM availability.
