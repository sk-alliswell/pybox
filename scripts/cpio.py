"""Minimal newc CPIO helpers for the pinned guest image."""
from pathlib import PurePosixPath
import hashlib
BASE_SHA="e2098f351a26577c4c9e3ba771c265419881e72ef4fb370746a2acd241642816"

def sha(data):
    return hashlib.sha256(data).hexdigest()

def file_sha(path):
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()

def safe_name(name):
    while name.startswith('./'):
        name = name[2:]
    path = PurePosixPath(name)
    if path.is_absolute() or '..' in path.parts or '\\' in name or '\x00' in name or not name:
        raise ValueError('Unsafe archive path: ' + repr(name))
    return path.as_posix()

def read_cpio(raw):
    offset = 0
    inventory = {}
    while True:
        start = offset
        header = raw[offset:offset + 110]
        if header[:6] != b'070701':
            raise ValueError('Expected newc CPIO')
        fields = [int(header[6 + i * 8:14 + i * 8], 16) for i in range(13)]
        size, namesize = fields[6], fields[11]
        name = raw[offset + 110:offset + 110 + namesize - 1].decode()
        offset = (offset + 110 + namesize + 3) & ~3
        data = raw[offset:offset + size]
        offset = (offset + size + 3) & ~3
        if len(data) != size:
            raise ValueError('Truncated CPIO')
        if name == 'TRAILER!!!':
            return inventory, start
        name = safe_name(name)
        if name in inventory:
            raise ValueError('Duplicate CPIO path')
        inventory[name] = {'mode': fields[1], 'data': data}

def record(name, data, mode, inode):
    name_bytes = name.encode() + b'\0'
    values = [inode, mode, 0, 0, 1, 0, len(data), 0, 0, 0, 0, len(name_bytes), 0]
    raw = b'070701' + b''.join(f'{v:08x}'.encode() for v in values) + name_bytes
    raw += b'\0' * (-len(raw) % 4)
    raw += data
    return raw + b'\0' * (-len(data) % 4)
