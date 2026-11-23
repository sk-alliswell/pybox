"""Collect Cargo dependency notices; fail if a package has no notice file."""
from pathlib import Path
import json
import subprocess

ROOT = Path(__file__).resolve().parent.parent


def main():
    resolved = {}
    for target in ('x86_64-pc-windows-msvc', 'x86_64-unknown-linux-musl'):
        metadata = json.loads(subprocess.check_output(
            ['cargo', 'metadata', '--locked', '--format-version', '1', '--filter-platform', target],
            cwd=ROOT, encoding='utf-8'))
        active = {node['id'] for node in metadata['resolve']['nodes']}
        resolved.update({package['id']: package for package in metadata['packages'] if package['id'] in active})
    texts = []
    packages = []
    for package in sorted(resolved.values(), key=lambda item: (item['name'], item['version'])):
        if package['source'] is None:
            continue
        folder = Path(package['manifest_path']).parent
        notices = sorted(path for path in folder.rglob('*')
                         if path.is_file() and path.name.lower().startswith(('license', 'copying', 'copyright', 'notice')))
        if not notices:
            raise ValueError(f'Missing notices: {package["name"]}@{package["version"]}')
        for path in notices:
            texts.append(f'{package["name"]} {package["version"]} / {path.relative_to(folder).as_posix()}\n'
                         + path.read_text(encoding='utf-8', errors='replace'))
        packages.append({'name': package['name'], 'version': package['version'], 'license': package['license'],
                         'repository': package['repository'], 'notices': [path.relative_to(folder).as_posix() for path in notices]})
    (ROOT / 'licenses/LICENSE-Rust-Dependencies.txt').write_text('\n\n---\n\n'.join(texts) + '\n', encoding='utf-8')
    (ROOT / 'licenses/rust-dependencies.json').write_text(json.dumps(packages, indent=2) + '\n', encoding='utf-8')
    print(f'Collected notices for {len(packages)} dependencies')


if __name__ == '__main__':
    main()
