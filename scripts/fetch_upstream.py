"""Download the pinned VMM only; never overwrite different local bytes."""
from pathlib import Path
from urllib.request import Request, urlopen
import argparse
import hashlib
import io
import json
import tarfile
import zipfile

ROOT = Path(__file__).resolve().parent.parent


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--platform', choices=['windows', 'linux', 'both'], default='both')
    parser.add_argument('--out', type=Path, default=ROOT / 'runtime')
    args = parser.parse_args()
    for pin in json.loads((ROOT / 'scripts/upstream.json').read_text(encoding='utf-8')):
        if args.platform not in ('both', pin['platform']):
            continue
        native = 'hluk.exe' if pin['platform'] == 'windows' else 'hluk'
        target = args.out / pin['platform'] / native
        if target.exists():
            if hashlib.sha256(target.read_bytes()).hexdigest() != pin['binary_sha256']:
                raise ValueError(f'Existing {native} does not match pinned bytes')
            print(f'{pin["platform"]}: already verified')
            continue
        with urlopen(Request(pin['url'], headers={'User-Agent': 'pybox-builder'}), timeout=90) as response:
            raw = response.read()
        if hashlib.sha256(raw).hexdigest() != pin['archive_sha256']:
            raise ValueError('Upstream archive digest mismatch')
        if pin['platform'] == 'windows':
            with zipfile.ZipFile(io.BytesIO(raw)) as archive:
                names = [name for name in archive.namelist() if Path(name).name == native]
                if len(names) != 1:
                    raise ValueError('Unexpected archive inventory')
                body = archive.read(names[0])
        else:
            with tarfile.open(fileobj=io.BytesIO(raw), mode='r:gz') as archive:
                members = [member for member in archive if member.isfile() and Path(member.name).name == native]
                if len(members) != 1:
                    raise ValueError('Unexpected archive inventory')
                body = archive.extractfile(members[0]).read()
        if hashlib.sha256(body).hexdigest() != pin['binary_sha256']:
            raise ValueError('Upstream executable digest mismatch')
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open('xb') as stream:
            stream.write(body)
        target.chmod(0o755)
        print(f'{pin["platform"]}: verified')


if __name__ == '__main__':
    main()
