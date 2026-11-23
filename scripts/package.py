"""Build allowlisted runtime archives. No snapshots, state, or host scripts."""
from pathlib import Path
import argparse
import hashlib
import json
import os
import shutil
import tarfile
import zipfile

ROOT = Path(__file__).resolve().parent.parent


def sha(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def package(system, runtime, binary, destination):
    runtime, binary, destination = runtime.resolve(), binary.resolve(), destination.resolve()
    native = 'hluk.exe' if system == 'windows' else 'hluk'
    executable = 'pybox.exe' if system == 'windows' else 'pybox'
    files = {
        executable: binary,
        native: runtime / system / native,
        'rootfs.cpio': runtime / 'image/rootfs.cpio',
        'image-manifest.json': runtime / 'image/image-manifest.json',
        'docker-baseline.json': runtime / 'image/docker-baseline.json',
        'LICENSE-Hyperlight': ROOT / 'licenses/LICENSE-Hyperlight',
        'LICENSE-Rust-Dependencies.txt': ROOT / 'licenses/LICENSE-Rust-Dependencies.txt',
        'THIRD_PARTY_NOTICES.md': ROOT / 'THIRD_PARTY_NOTICES.md',
        'RUN.md': ROOT / 'docs/RUN.md',
        'MANAGER.md': ROOT / 'docs/MANAGER.md',
    }
    if system == 'windows':
        for name in ('vcruntime140.dll', 'LICENSE-VC-Runtime.docx', 'NOTICE-VC-Runtime.txt'):
            files[name] = runtime / system / name
    launcher = 'run-console.cmd' if system == 'windows' else 'run-console.sh'
    files[launcher] = ROOT / 'console' / launcher
    for name, path in files.items():
        if not path.is_file() or path.is_symlink():
            raise ValueError(f'Missing or symlinked packaging input: {name}')
    image = json.loads(files['image-manifest.json'].read_text(encoding='utf-8'))
    if sha(files['rootfs.cpio']) != image['rootfs_sha256']:
        raise ValueError('Image digest mismatch')
    pins = json.loads((ROOT / 'scripts/upstream.json').read_text(encoding='utf-8'))
    expected = next(pin for pin in pins if pin['platform'] == system)
    if sha(files[native]) != expected['binary_sha256']:
        raise ValueError('Pinned VMM digest mismatch')
    stage = destination / f'pybox-{system}-x64'
    stage.mkdir(parents=True)  # Never overwrite an existing release.
    for name, path in files.items():
        shutil.copy2(path, stage / name)
        if system == 'linux':
            (stage / name).chmod(0o755 if name in ('pybox', 'hluk', 'run-console.sh') else 0o644)
    manifest = {name: {'bytes': (stage / name).stat().st_size, 'sha256': sha(stage / name)} for name in sorted(files)}
    (stage / 'BUNDLE_SHA256.json').write_text(json.dumps(manifest, indent=2) + '\n', encoding='utf-8')
    names = sorted([*files, 'BUNDLE_SHA256.json'])
    archive = destination / (stage.name + ('.zip' if system == 'windows' else '.tar.gz'))
    if system == 'windows':
        with zipfile.ZipFile(archive, 'x', zipfile.ZIP_DEFLATED, compresslevel=6) as bundle:
            for name in names:
                bundle.write(stage / name, name)
    else:
        with tarfile.open(archive, 'x:gz', compresslevel=6) as bundle:
            for name in names:
                info = bundle.gettarinfo(str(stage / name), arcname=name)
                info.uid = info.gid = 0
                info.uname = info.gname = ''
                info.mode = 0o755 if name in ('pybox', 'hluk', 'run-console.sh') else 0o644
                with (stage / name).open('rb') as stream:
                    bundle.addfile(info, stream)
    checksum = f'{sha(archive)}  {archive.name}\n'
    archive.with_name(archive.name + '.sha256').write_text(checksum, encoding='ascii')
    print(json.dumps({'archive': str(archive), 'bytes': archive.stat().st_size, 'sha256': sha(archive)}))
    return archive


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--platform', choices=['windows', 'linux'], required=True)
    parser.add_argument('--runtime', type=Path, default=ROOT / 'runtime')
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--out', type=Path, required=True)
    args = parser.parse_args()
    package(args.platform, args.runtime, args.binary, args.out)
