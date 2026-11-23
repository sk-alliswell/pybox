# Extraction validation — 2026-11-06

The standalone source was built in its own directory with no QC source dependency.
The initial release preserves the qualified guest image bytes and rebuilds the
native controller from the extracted source.

| Check | Result | Scope |
|---|---|---|
| Rust formatting and tests | Passed | Windows quote/argument regression and all targets' test builds |
| Python packaging tests | 3 passed | Archive traversal refusal, CPIO byte/mode/Unicode roundtrip, allowlist/digests and no state leakage |
| Native Windows guards | 5 passed | Policy, stdout/stderr bounds, timeout and cancellation |
| Linux guards and descriptor regression | Passed | Same five guards plus 100 launches at concurrency 10 |
| Windows WHP end-to-end | 28 passed | New extracted runtime copy, empty PATH, Unicode/space path |
| Ubuntu 22.04 under WSL2/KVM end-to-end | 28 passed | New runtime copy, empty PATH, Unicode/space path |
| Guest source correspondence | Passed | Committed harness/entrypoint match the bytes inside the packaged CPIO |

End-to-end checks cover first preparation, repeated ready clones, cold Docker
identity parity, complete status, guard receipts, actual VM timeout/cancellation,
snapshot mutation refusal, cleanup uncertainty refusal, same-size image corruption,
incomplete bundle rejection, lock interoperability, two-VM admission/third refusal,
controller-crash VMM cleanup, slot reclamation, console cancellation and final health.
Machine-readable sanitized results and source hashes are in `validation-v0.1.0.json`.

The first Windows verification attempt stopped after successful preparation because
the verifier used the host default text encoding for a JSON file containing a Korean
path. All verifier text reads now specify UTF-8. A fresh second run passed all 28
checks. The failed record remains local and is not rewritten as a pass.

## Packaging audit

Source publication includes only controller code, build tools, guest sources/locks,
tests, documentation and notices. Runtime archives use an explicit file allowlist;
they contain no host Python controller scripts, snapshots, credentials, workroom UI
or application state. Korean text is retained only in Unicode/path regression fixtures.
Rust notices cover dependencies resolved for the supported Windows and Linux targets.
Guest filesystem license/copyright files remain intact.

Tracked source and runtime file inventory were checked with manual secret/path
patterns and explicit allowlists. No blocking match was found in that scope.
No dedicated secret scanner was installed; this is not a comprehensive secret-scan
certification. There was no prior Git history in the extracted source. Historical
QC experiments, remote assets outside the new repository and arbitrary guest
dependency contents were not comprehensively scanned.

## Remaining boundaries

Physical Ubuntu, arbitrary guest scripts/images, production multitenant isolation,
live external models, persistent post-VM storage and total VM memory accounting
remain unqualified. The migrated Docker rebuild/conversion script is self-contained,
but a fresh networked Docker image rebuild was not repeated for this extraction.
This release packages the existing exact, verified image. Hosted CI checks do not
replace local WHP/KVM end-to-end evidence.
