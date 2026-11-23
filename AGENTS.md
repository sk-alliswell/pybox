# Pybox development

This is the independent owner of the lightweight sandbox. Read README.md and
docs/ARCHITECTURE.md before changing lifecycle or packaging behavior.

- Preserve unrelated working changes and retained verification evidence.
- Never fall back to executing guest code on the host.
- Preserve integrity checks, fail-closed cleanup, OS process guards, and empty
  preparation snapshots. Keep the CLI network disabled.
- Keep host build dependencies distinct from runtime requirements.
- Do not commit runtime images, credentials, snapshots, caches, or raw host logs.
- Keep QC workroom UI, GenUI, cloud adapters and application data outside this repo.
- Run cargo fmt --check, cargo test --locked --all-features, and the Python tests.
  Lifecycle changes also require verify.py on available WHP/KVM hosts.
- Report Windows/WHP and Linux/WSL2/KVM separately; do not claim physical Ubuntu
  qualification, generic OCI compatibility, or production multitenant security.
