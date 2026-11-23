# Changelog

## 0.2.0 — 2026-11-06

- Added a loopback management service and embedded Korean web console with live
  instance counts, per-job settings, logs, native cleanup receipts and retained history.
- Added authenticated API and `manage`, `console`, `submit`, `jobs`, `stop` commands.
- Added a bounded queue, one manager per OS user, configurable 1..2 dispatched
  operations, immutable submitted settings and fail-closed restart recovery.
- Kept memory/output/network policies and the fixed guest workload unchanged.
- Added Windows/Linux console launchers, manager API documentation and real-VM
  management verification, including concurrency, cancellation and service crashes.
- Fixed Windows accepted sockets inheriting nonblocking mode; regression checks
  exercise delayed split HTTP requests. An occupied default port now selects a
  free loopback port; explicit port requests still fail on conflicts.

## 0.1.0 — 2026-11-06

- Extracted the Rust single-job sandbox controller from the QC prototype into its
  own source tree, build, tests and Windows/Linux runtime releases.
- Preserved the fixed qualified guest image, snapshot contract, guard limits and
  existing `python_sandbox_controller` library name.
- Included the later Linux concurrent-spawn policy-pipe CLOEXEC fix and diagnostic
  improvement, with the 100-launch descriptor regression probe.
- Added allowlisted, checksummed packaging, pinned upstream retrieval, dependency
  notices, portable tests, CI, operating instructions and explicit scope boundaries.
- Fixed locale-dependent JSON reading in the end-to-end verifier.
- Excluded QC workroom UI, GenUI, cloud adapters, application state and raw logs.
