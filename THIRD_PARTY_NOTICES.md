# Third-party components

Pybox wraps upstream virtualization and guest components; it does not claim to
implement the hypervisor, Unikraft kernel or Python interpreter.

- Hyperlight Unikraft `hluk` v0.19.1 and `hl_pydriver` originate from
  https://github.com/hyperlight-dev/hyperlight-unikraft/tree/v0.19.1.
  The upstream Apache-2.0 text is retained in `licenses/LICENSE-Hyperlight`.
  Pinned download and executable hashes are in `scripts/upstream.json`.
  The executable includes an upstream Unikraft kernel; upstream source and its
  component notices remain the authority for that distribution.
- The Docker-derived root filesystem retains its original Python, Debian and
  installed wheel license/copyright files. It is not relicensed by Pybox.
  The qualified source image is `python:3.12.15-slim-bookworm`; locked guest
  requirements and the Dockerfile are in `guest/`.
- Rust dependency notices are collated in `licenses/LICENSE-Rust-Dependencies.txt`
  with package metadata in `licenses/rust-dependencies.json`.
- Windows archives include the unmodified signed Microsoft Visual C++ Runtime
  14.40.33816.0, its original `LICENSE-VC-Runtime.docx`, and attribution in
  `NOTICE-VC-Runtime.txt`. The VMM imports this app-local DLL; the Rust controller
  uses a static CRT. See https://visualstudio.microsoft.com/license-terms/vs2022-cruntime/.

Original Pybox code is private and has no open-source license grant. Third-party
license terms continue to apply independently. No license text or required
attribution is removed to prepare the repository.
