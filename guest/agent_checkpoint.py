"""Guest-only checkpoint transport for the pinned LangGraph image.

This module is staged as application source after empty snapshot preparation.
The host stores the envelope as data and never imports this module or unpickles it.
"""
import base64
import hashlib
import importlib.metadata
import inspect
import json
import zlib

from langgraph.checkpoint.memory import InMemorySaver
from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer

SCHEMA = 'pybox.langgraph-memory.v1'
MEMORY_SOURCE = '9e81ce01b41fc1bf591e930e350629282fb4ac86d667e211899bae9b5b573a84'
MAX_RAW = 128 * 1024
MAX_ENVELOPE = 4096


def _json_bytes(value):
    return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(',', ':')).encode('utf-8')


def _pack(value, depth=0):
    if depth > 32:
        raise ValueError('checkpoint nesting limit')
    if value is None or type(value) in (str, int, float, bool):
        return value
    if type(value) is bytes:
        return ['bytes', base64.b64encode(value).decode('ascii')]
    if isinstance(value, dict):
        return ['dict', [[_pack(k, depth+1), _pack(v, depth+1)] for k, v in value.items()]]
    if type(value) in (tuple, list):
        return ['tuple' if type(value) is tuple else 'list', [_pack(v, depth+1) for v in value]]
    raise TypeError('unsupported checkpoint container: ' + type(value).__name__)


def _unpack(value, depth=0):
    if depth > 32:
        raise ValueError('checkpoint nesting limit')
    if value is None or type(value) in (str, int, float, bool):
        return value
    if type(value) is not list or len(value) != 2:
        raise ValueError('invalid checkpoint container')
    tag, body = value
    if tag == 'bytes':
        data = base64.b64decode(body, validate=True)
        if base64.b64encode(data).decode('ascii') != body:
            raise ValueError('noncanonical checkpoint bytes')
        return data
    if tag in ('list', 'tuple') and type(body) is list:
        result = [_unpack(v, depth+1) for v in body]
        return tuple(result) if tag == 'tuple' else result
    if tag == 'dict' and type(body) is list:
        result = {}
        for item in body:
            if type(item) is not list or len(item) != 2:
                raise ValueError('invalid checkpoint map item')
            key, val = _unpack(item[0], depth+1), _unpack(item[1], depth+1)
            if key in result:
                raise ValueError('duplicate checkpoint key')
            result[key] = val
        return result
    raise ValueError('unsupported checkpoint encoding')


class CheckpointSession:
    """One invocation's memory saver, explicitly hydrated/exported across VMs."""
    def __init__(self, binding, envelope=None):
        if type(binding) is not str or not binding or len(binding) > 256:
            raise ValueError('invalid graph binding')
        with open(inspect.getsourcefile(InMemorySaver), 'rb') as source:
            observed = hashlib.sha256(source.read(128 * 1024)).hexdigest()
        if observed != MEMORY_SOURCE:
            raise ValueError('unsupported checkpoint implementation')
        self.binding = binding
        self.versions = {p: importlib.metadata.version(p) for p in ('langgraph', 'langgraph-checkpoint')}
        self.saver = InMemorySaver(serde=JsonPlusSerializer(pickle_fallback=False, allowed_json_modules=None, allowed_msgpack_modules=None))
        if envelope is not None:
            self._restore(envelope)

    def export(self):
        raw = _json_bytes(_pack({'storage': self.saver.storage, 'writes': self.saver.writes, 'blobs': self.saver.blobs}))
        if len(raw) > MAX_RAW:
            raise ValueError('checkpoint history exceeds raw transport limit')
        envelope = dict(schema=SCHEMA, binding=self.binding, versions=self.versions,
            implementation_sha256=MEMORY_SOURCE, raw_bytes=len(raw),
            raw_sha256=hashlib.sha256(raw).hexdigest(),
            payload=base64.b64encode(zlib.compress(raw, 6)).decode('ascii'))
        if len(_json_bytes(envelope)) > MAX_ENVELOPE:
            raise ValueError('checkpoint envelope exceeds transport limit')
        return envelope

    def _restore(self, envelope):
        fields = {'schema', 'binding', 'versions', 'implementation_sha256', 'raw_bytes', 'raw_sha256', 'payload'}
        if type(envelope) is not dict or set(envelope) != fields or len(_json_bytes(envelope)) > MAX_ENVELOPE:
            raise ValueError('invalid checkpoint envelope')
        if (envelope['schema'] != SCHEMA or envelope['binding'] != self.binding
                or envelope['versions'] != self.versions or envelope['implementation_sha256'] != MEMORY_SOURCE):
            raise ValueError('checkpoint identity mismatch')
        expected = envelope['raw_bytes']
        if type(expected) is not int or not 0 < expected <= MAX_RAW:
            raise ValueError('invalid checkpoint size')
        compressed = base64.b64decode(envelope['payload'], validate=True)
        if base64.b64encode(compressed).decode('ascii') != envelope['payload']:
            raise ValueError('noncanonical checkpoint envelope')
        decoder = zlib.decompressobj()
        raw = decoder.decompress(compressed, MAX_RAW+1)
        if (len(raw) != expected or not decoder.eof or decoder.unused_data or decoder.unconsumed_tail
                or hashlib.sha256(raw).hexdigest() != envelope['raw_sha256']):
            raise ValueError('checkpoint payload integrity/size mismatch')
        def unique(items):
            result = {}
            for key, value in items:
                if key in result:
                    raise ValueError('duplicate JSON key')
                result[key] = value
            return result
        parsed = json.loads(raw, object_pairs_hook=unique, parse_constant=lambda _: (_ for _ in ()).throw(ValueError('nonfinite checkpoint')))
        state = _unpack(parsed)
        if type(state) is not dict or set(state) != {'storage', 'writes', 'blobs'} or any(type(v) is not dict for v in state.values()):
            raise ValueError('invalid saver state')
        for thread, namespaces in state['storage'].items():
            for namespace, checkpoints in namespaces.items():
                self.saver.storage[thread][namespace].update(checkpoints)
        self.saver.writes.update(state['writes'])
        self.saver.blobs.update(state['blobs'])
