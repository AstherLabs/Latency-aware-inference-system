"""Versioned, bounded streaming bridge to the private Vast audio worker."""
from __future__ import annotations

import json
import math
from pathlib import Path
import re

import grpc

KIND = 'synthx_audio'
CHUNK_BYTES = 65536
MAX_INPUT_BYTES = 209715200
MAX_JSON_BYTES = 4 * 1024 * 1024
MAX_METADATA_BYTES = 8192
SERVICE = 'synthx.audio.v1.AudioWorker'
CHECKPOINTS = {
    'vital_v7_3': '3059361d046c2c04fd40ae3d29c97e1c7d56e167f93b4b945d96aee1d4e196de',
    'vital_v7_5': 'dd9efb895f2903941ead3e5e8761f0c37f07a522e742ef04819d75e059586aa3',
    'vital_v7_5_chord': 'dd9efb895f2903941ead3e5e8761f0c37f07a522e742ef04819d75e059586aa3',
}
METADATA_FIELDS = {'version', 'model', 'request_id', 'input_sha256', 'input_size', 'checkpoint_sha256'}


class AudioWorkerError(Exception):
    def __init__(self, code, message, retryable=False):
        super().__init__(message)
        self.code, self.message, self.retryable = code, message, bool(retryable)


def encode_json(value, limit=MAX_JSON_BYTES):
    try:
        data = json.dumps(value, allow_nan=False, separators=(',', ':')).encode('utf-8')
    except (ValueError, TypeError, RecursionError) as exc:
        raise AudioWorkerError('invalid_response', 'Invalid response JSON') from exc
    if len(data) > limit:
        raise AudioWorkerError('invalid_response', 'JSON exceeds protocol limit')
    return data


def decode_json(data, limit=MAX_JSON_BYTES):
    if len(data) > limit:
        raise AudioWorkerError('invalid_metadata', 'JSON exceeds protocol limit')
    try:
        def invalid(value):
            raise ValueError('nonfinite JSON')
        def finite_float(token):
            value = float(token)
            if not math.isfinite(value):
                raise ValueError('nonfinite JSON number')
            return value
        value = json.loads(data, parse_constant=invalid, parse_float=finite_float)
    except (ValueError, UnicodeError, RecursionError) as exc:
        raise AudioWorkerError('invalid_metadata', 'Invalid JSON') from exc
    if not isinstance(value, dict):
        raise AudioWorkerError('invalid_metadata', 'JSON must be an object')
    return value


def validate_identity(model, request_id):
    if not isinstance(model, str) or model not in CHECKPOINTS:
        raise AudioWorkerError('unknown_model', 'Unsupported audio model')
    if not isinstance(request_id, str) or not re.fullmatch('[a-f0-9]{32}', request_id):
        raise AudioWorkerError('invalid_metadata', 'Invalid request ID')


def validate_metadata(value):
    if set(value) != METADATA_FIELDS or type(value.get('version')) is not int or value['version'] != 1:
        raise AudioWorkerError('invalid_metadata', 'Unsupported metadata fields or version')
    validate_identity(value['model'], value['request_id'])
    size = value['input_size']
    if type(size) is not int or not 0 < size <= MAX_INPUT_BYTES:
        raise AudioWorkerError('invalid_size', 'Audio must be between 1 byte and 200 MiB')
    if not isinstance(value['input_sha256'], str) or not re.fullmatch('[a-f0-9]{64}', value['input_sha256']):
        raise AudioWorkerError('invalid_metadata', 'Invalid input SHA256')
    if value['checkpoint_sha256'] != CHECKPOINTS[value['model']]:
        raise AudioWorkerError('checkpoint_mismatch', 'Checkpoint differs from pinned model')
    return value


def _checked(value):
    error = value.get('error')
    if isinstance(error, dict):
        raise AudioWorkerError(error.get('code', 'worker_error'), error.get('message', 'Worker failed'), error.get('retryable', False))
    return value


def _transport(exc):
    for key, value in (exc.trailing_metadata() or ()):
        if key == 'audio-error-bin':
            try:
                _checked(decode_json(value, MAX_METADATA_BYTES))
            except AudioWorkerError as reported:
                return reported
    return AudioWorkerError('transport', f'Audio worker RPC failed: {exc.code().name}', True)


def execute_remote(channel, payload, timeout):
    metadata = {key: payload.get(key) for key in METADATA_FIELDS if key != 'version'}
    metadata['version'] = 1
    validate_metadata(metadata)
    path = Path(payload['input_path'])
    if path.stat().st_size != metadata['input_size']:
        raise AudioWorkerError('input_mismatch', 'Local input size changed')

    def frames():
        yield encode_json(metadata)
        # The remote validates the stream SHA/length and returns a structured
        # error. Raising after yielding the final chunk would mask that error
        # with gRPC UNKNOWN on the caller's request-iterator thread.
        remaining = metadata['input_size'] + 1
        with path.open('rb') as stream:
            while remaining:
                chunk = stream.read(min(CHUNK_BYTES, remaining))
                if not chunk:
                    break
                remaining -= len(chunk)
                yield chunk

    try:
        return _checked(decode_json(channel.stream_unary('/' + SERVICE + '/Execute')(frames(), timeout=timeout)))
    except grpc.RpcError as exc:
        raise _transport(exc) from exc


class _ArtifactStream:
    """An iterator whose blocked next() can be cancelled by a disconnect task."""
    def __init__(self, call):
        self._call = call

    def __iter__(self):
        return self

    def __next__(self):
        try:
            return next(self._call)
        except grpc.RpcError as exc:
            raise _transport(exc) from exc

    def cancel(self):
        return self._call.cancel()

    def close(self):
        self.cancel()


def remote_artifact(channel, model, request_id, kind, timeout=60):
    validate_identity(model, request_id)
    if not isinstance(kind, str) or kind not in {'vital', 'wav'}:
        raise AudioWorkerError('invalid_artifact', 'Unsupported artifact kind')
    call = channel.unary_stream('/' + SERVICE + '/Artifact')(
        encode_json(dict(model=model, request_id=request_id, kind=kind)), timeout=timeout)
    return _ArtifactStream(call)


def probe_remote(channel, timeout=5):
    try:
        return _checked(decode_json(channel.unary_unary('/' + SERVICE + '/Health')(b'{}', timeout=timeout)))
    except grpc.RpcError as exc:
        raise _transport(exc) from exc
