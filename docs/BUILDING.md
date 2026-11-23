# Build and package

Rust and a native linker are build dependencies. Python 3.11+ is required only for
packaging and verification scripts. Docker and uv are image-build dependencies,
not runtime dependencies. Run commands from the repository root.

## Controller

```powershell
# Windows controller, probes, and cross-compiled static Linux musl binaries
.\build.ps1 -WithProbes -Linux
```

```sh
# Native Linux
cargo build --release --locked --all-features --bins
```

## Runtime inputs from a release

Authenticate GitHub CLI with access to this private repository, then download a
matching archive with `gh release download v0.2.0 --repo MichaelAlbertIhm/pybox`.
Verify its `.sha256` file before extracting. The existing extracted bundle can be
used directly with `target/release/pybox --root /path/to/bundle`.

For packaging, arrange reviewed inputs as follows (all ignored by Git):

```text
runtime/
  windows/hluk.exe
  windows/vcruntime140.dll
  windows/LICENSE-VC-Runtime.docx
  windows/NOTICE-VC-Runtime.txt
  linux/hluk
  image/rootfs.cpio
  image/image-manifest.json
  image/docker-baseline.json
```

These files can be copied from the matching runtime release. To retrieve upstream
VMM bytes independently, use `python scripts/fetch_upstream.py --platform both`;
downloads must match the committed archive and executable hashes. The Windows
DLL/license must come from the reviewed release or a valid Microsoft redistributable
installation, not an arbitrary DLL download site.

```powershell
python scripts/collect_licenses.py
python scripts/package.py --platform windows --binary target/release/pybox.exe --out dist/v0.2.0
python scripts/package.py --platform linux --binary target/x86_64-unknown-linux-musl/release/pybox --out dist/v0.2.0
```

Each platform directory must be new. Packaging verifies the image digest and
pinned VMM, includes only named runtime files/notices, and writes a SHA256 inventory
plus an archive checksum. It excludes developer probes, Python host scripts,
snapshots, logs and application state. Large runtime files belong in private
GitHub release assets, never Git history.

## Rebuild the guest image

The included restricted converter has no dependency on the QC source tree.
It requires the upstream Python driver base CPIO at `runtime/base.cpio` with SHA256
`e2098f351a26577c4c9e3ba771c265419881e72ef4fb370746a2acd241642816`.
Obtain that base from the upstream image
`ghcr.io/hyperlight-dev/hyperlight-unikraft/python:initrd-v0.19.1` using the upstream
rootfs export procedure. The converter refuses any other driver base hash.

```sh
python scripts/build_image.py docker --run-id local-build
python scripts/build_image.py convert --run-id local-build
```

This builds and smoke-tests the locked guest in Docker, exports the exact image,
and converts it to newc CPIO while preserving Python/package bytes. Only the pinned
driver and deterministic resolver files are added. Copy `runs/local-build/converted/rootfs.cpio`,
`converted/manifest.json` (as `image-manifest.json`) and `docker-smoke.json`
(as `docker-baseline.json`) into a new packaging input directory.

Supported image contract: Linux/amd64, root, no Docker volumes, `/app` workdir and
`/usr/local/bin/python3 /app/entrypoint.py` entrypoint. This is not a general OCI
converter. Guest dependency hashes are locked, but base image tags are not immutable;
a fresh Docker rebuild may change bytes and requires a new baseline and full VM
verification. The initial release reuses the previously verified exact image bytes.
The two guest Python source files retain their original line endings through Git
because their exact byte hashes are part of that image contract.

## Verification

```sh
cargo fmt --check
cargo test --locked --all-features
python -m unittest discover -s tests -v
python verify.py --payload dist/v0.2.0/pybox-windows-x64 --tools target/release --out verification/windows-new
python scripts/verify_manager.py --binary target/release/pybox.exe --bundle dist/v0.2.0/pybox-windows-x64 --out verification/manager-new
```

On Linux use the Linux payload and corresponding native tool directory. The full
suite requires WHP/KVM and retains failure records. CI runs portable checks and
native guard checks without claiming hosted-runner VM availability.
