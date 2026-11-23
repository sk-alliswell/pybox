"""Run the same harness and report Docker filesystem/config provenance."""
import contextlib
import hashlib
import io
import json
import os
from pathlib import Path
import runpy
import sys


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


buffer=io.StringIO()
with contextlib.redirect_stdout(buffer):
    if '/app' not in sys.path: sys.path.insert(0,'/app')
    import harness
    harness.run()
body=json.loads(buffer.getvalue())
body['image_contract']={
    'cwd':os.getcwd(), 'marker':os.environ.get('SANDBOX_IMAGE_MARKER'),
    'python_executable':sys.executable,
    'python_binary_sha256':digest('/usr/local/bin/python3'),
    'libpython_sha256':digest('/usr/local/lib/libpython3.12.so.1.0'),
    'harness_sha256':digest('/app/harness.py'),
    'entrypoint_sha256':digest('/app/entrypoint.py'),
}
print(json.dumps(body))
