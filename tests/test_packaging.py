import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch
import zipfile

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / 'scripts'))
import cpio
import package


class PackagingTests(unittest.TestCase):
    def test_archive_paths_reject_escape(self):
        for name in ('../secret', '/absolute', 'a/../../secret', 'a\\b', '\0file', ''):
            with self.subTest(name=name), self.assertRaises(ValueError):
                cpio.safe_name(name)
        self.assertEqual(cpio.safe_name('./app/main.py'), 'app/main.py')

    def test_cpio_roundtrip_preserves_unicode_and_modes(self):
        raw = cpio.record('app/한글.txt', b'payload', 0o100640, 1)
        raw += cpio.record('TRAILER!!!', b'', 0, 0)
        entries, _ = cpio.read_cpio(raw)
        self.assertEqual(entries['app/한글.txt'], {'data': b'payload', 'mode': 0o100640})
        with self.assertRaises(ValueError):
            cpio.read_cpio(raw[:115])

    def test_packages_exclude_state_and_check_digests(self):
        for system in ('windows', 'linux'):
            with self.subTest(system=system), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                for folder in ('runtime/image', 'runtime/windows', 'runtime/linux', 'licenses', 'docs', 'scripts', 'console'):
                    (root / folder).mkdir(parents=True)
                for name in ('licenses/LICENSE-Hyperlight', 'licenses/LICENSE-Rust-Dependencies.txt',
                             'THIRD_PARTY_NOTICES.md', 'docs/RUN.md', 'docs/MANAGER.md', 'console/run-console.cmd', 'console/run-console.sh'):
                    (root / name).write_text('test notice', encoding='utf-8')
                native = 'hluk.exe' if system == 'windows' else 'hluk'
                (root / 'runtime' / system / native).write_bytes(b'fixture-vmm')
                pins = [{'platform': system, 'binary_sha256': hashlib.sha256(b'fixture-vmm').hexdigest()}]
                (root / 'scripts/upstream.json').write_text(json.dumps(pins))
                for name in ('vcruntime140.dll', 'LICENSE-VC-Runtime.docx', 'NOTICE-VC-Runtime.txt'):
                    (root / 'runtime/windows' / name).write_bytes(b'fixture')
                (root / 'runtime/image/rootfs.cpio').write_bytes(b'fixture-image')
                image = {'rootfs_sha256': hashlib.sha256(b'fixture-image').hexdigest()}
                (root / 'runtime/image/image-manifest.json').write_text(json.dumps(image))
                (root / 'runtime/image/docker-baseline.json').write_text('{}')
                (root / 'runtime/.env').write_text('EXCLUDED=fixture')
                (root / 'runtime/state').mkdir()
                (root / 'runtime/state/browser-key').write_text('excluded fixture')
                (root / 'controller').write_bytes(b'fixture-controller')
                with patch.object(package, 'ROOT', root):
                    archive = package.package(system, root / 'runtime', root / 'controller', root / 'dist')
                    if system == 'windows':
                        with zipfile.ZipFile(archive) as bundle:
                            bodies = {name: bundle.read(name) for name in bundle.namelist()}
                    else:
                        with tarfile.open(archive) as bundle:
                            bodies = {member.name: bundle.extractfile(member).read() for member in bundle}
                            self.assertEqual(bundle.getmember('pybox').mode, 0o755)
                            self.assertEqual(bundle.getmember('hluk').mode, 0o755)
                    manifest = json.loads(bodies.pop('BUNDLE_SHA256.json'))
                    self.assertEqual(set(bodies), set(manifest))
                    self.assertNotIn('.env', bodies)
                    self.assertNotIn('state/browser-key', bodies)
                    for name, data in bodies.items():
                        self.assertEqual(hashlib.sha256(data).hexdigest(), manifest[name]['sha256'])
                        self.assertEqual(len(data), manifest[name]['bytes'])
                    with self.assertRaises(FileExistsError):
                        package.package(system, root / 'runtime', root / 'controller', root / 'dist')
                    (root / 'runtime/image/rootfs.cpio').write_bytes(b'corrupt-image')
                    with self.assertRaisesRegex(ValueError, 'Image digest mismatch'):
                        package.package(system, root / 'runtime', root / 'controller', root / 'dist-bad')
                    self.assertFalse((root / 'dist-bad').exists())


if __name__ == '__main__':
    unittest.main()
