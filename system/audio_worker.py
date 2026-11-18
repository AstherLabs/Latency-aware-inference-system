"""Private streaming gRPC proxy; no model imports or checkpoint substitution.

Run: python -m system.audio_worker --root /workspace/synthx-audio-grpc
    --checkpoint-manifest /workspace/synthx-audio-grpc-checkpoints.json
An unresolved HTTP submission owns the execution slot until observed terminal,
even when its gRPC caller disappears or this process restarts.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import fcntl
import hashlib
import http.client
import os
from pathlib import Path
import re
import socket
import tempfile
import threading
import time
from urllib.parse import urlsplit

import grpc

from .audio_protocol import (AudioWorkerError, CHECKPOINTS, CHUNK_BYTES, MAX_INPUT_BYTES,
                             MAX_JSON_BYTES, MAX_METADATA_BYTES, SERVICE, decode_json,
                             encode_json, validate_identity, validate_metadata)

ORIGINS = {'vital_v7_3': 'http://127.0.0.1:18081', 'vital_v7_5': 'http://127.0.0.1:18085',
           'vital_v7_5_chord': 'http://127.0.0.1:18085'}
TERMINAL = {'completed', 'failed'}
# Durable state adds bounded request metadata to an already bounded RPC result.
MAX_DURABLE_BYTES = MAX_JSON_BYTES + 16384


def _error(exc):
    return {'error': {'code': exc.code, 'message': exc.message, 'retryable': exc.retryable}}


class AudioWorker:
    def __init__(self, root, origins=None, poll_interval=1, http_timeout=10, checkpoint_paths=None):
        if checkpoint_paths is not None:
            if not isinstance(checkpoint_paths, dict) or set(checkpoint_paths) != set(CHECKPOINTS):
                raise ValueError('Checkpoint manifest must contain exactly the pinned models')
            if any(not isinstance(path, (str, Path)) or not Path(path).is_absolute() for path in checkpoint_paths.values()):
                raise ValueError('Checkpoint manifest paths must be absolute')
            checkpoint_paths = {model: Path(path) for model, path in checkpoint_paths.items()}
        self.checkpoint_paths = checkpoint_paths
        self._checkpoint_cache = {}
        self._checkpoint_lock = threading.Lock()
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.origins = dict(ORIGINS if origins is None else origins)
        if set(self.origins) != set(CHECKPOINTS):
            raise ValueError('Origins must cover exactly the pinned models')
        for origin in self.origins.values():
            parsed = urlsplit(origin)
            if parsed.scheme != 'http' or parsed.hostname != '127.0.0.1' or not parsed.port or parsed.path or parsed.query or parsed.fragment or parsed.username:
                raise ValueError('Model origins must be loopback HTTP origins')
        self.poll_interval, self.http_timeout = poll_interval, http_timeout
        self._ownership = (self.root / 'worker.lock').open('a+b')
        try:
            fcntl.flock(self._ownership, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self._ownership.close()
            raise RuntimeError('Audio worker root already owned')
        self._condition = threading.Condition()
        self._stop = threading.Event()
        self.records = {}
        try:
            self._verified_checkpoint_files()
            for path in self.root.glob('*.json'):
                if not re.fullmatch('[a-f0-9]{32}', path.stem):
                    continue
                record = decode_json(path.read_bytes(), MAX_DURABLE_BYTES)
                validate_metadata(record['metadata'])
                if path.stem != record['metadata']['request_id']:
                    raise ValueError('Durable record identity mismatch')
                self.records[path.stem] = record
        except Exception:
            self._ownership.close()
            raise
        for partial in self.root.glob('upload-*'):
            partial.unlink()
        self._thread = threading.Thread(target=self._coordinate, daemon=True, name='audio-execution')
        self._thread.start()

    def add_to_server(self, server):
        server.add_generic_rpc_handlers((grpc.method_handlers_generic_handler(SERVICE, {
            'Execute': grpc.stream_unary_rpc_method_handler(self.Execute),
            'Artifact': grpc.unary_stream_rpc_method_handler(self.Artifact),
            'Health': grpc.unary_unary_rpc_method_handler(self.Health),
        }),))

    def close(self):
        self._stop.set()
        with self._condition:
            self._condition.notify_all()
        self._thread.join(timeout=self.http_timeout + 2)
        if self._thread.is_alive():
            raise RuntimeError('HTTP operation still running; execution ownership retained')
        self._ownership.close()

    def _save(self, record):
        key = record['metadata']['request_id']
        path = self.root / (key + '.json')
        temporary = path.with_suffix('.tmp')
        with temporary.open('wb') as stream:
            stream.write(encode_json(record, MAX_DURABLE_BYTES))
            stream.flush()
            os.fsync(stream.fileno())
        temporary.replace(path)
        descriptor = os.open(self.root, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def _publish(self, record, changes, remove=()):
        updated = {**record, **changes}
        for key in remove:
            updated.pop(key, None)
        self._save(updated)
        record.clear()
        record.update(updated)

    def Execute(self, frames, context):
        temporary = None
        try:
            try:
                metadata = validate_metadata(decode_json(next(frames), MAX_METADATA_BYTES))
            except StopIteration:
                raise AudioWorkerError('invalid_metadata', 'Missing metadata frame')
            digest, length = hashlib.sha256(), 0
            with tempfile.NamedTemporaryFile(dir=self.root, prefix='upload-', delete=False) as stream:
                temporary = Path(stream.name)
                for chunk in frames:
                    if len(chunk) > CHUNK_BYTES:
                        raise AudioWorkerError('invalid_chunk', 'Audio chunk exceeds 64 KiB')
                    length += len(chunk)
                    if length > metadata['input_size'] or length > MAX_INPUT_BYTES:
                        raise AudioWorkerError('invalid_size', 'Audio exceeds declared size')
                    digest.update(chunk)
                    stream.write(chunk)
                stream.flush()
                os.fsync(stream.fileno())
            if length != metadata['input_size'] or digest.hexdigest() != metadata['input_sha256']:
                raise AudioWorkerError('input_mismatch', 'Audio SHA256 or length mismatch')
            key = metadata['request_id']
            with self._condition:
                record = self.records.get(key)
                if record is not None and record['metadata'] != metadata:
                    raise AudioWorkerError('identity_conflict', 'Request ID belongs to another model or input')
                if record is None:
                    # Bounded durable admission: one active execution plus three waiting.
                    if sum(r['state'] not in TERMINAL for r in self.records.values()) >= 4:
                        raise AudioWorkerError('busy', 'Audio worker queue is full', True)
                    destination = self.root / (key + '.audio')
                    temporary.replace(destination)
                    temporary = None
                    record = {'metadata': metadata, 'state': 'queued', 'created_at': time.time()}
                    self._save(record)
                    self.records[key] = record
                    self._condition.notify_all()
                while record['state'] not in TERMINAL:
                    if 'quarantine_error' in record:
                        return encode_json({'error': record['quarantine_error']})
                    if not context.is_active() or self._stop.is_set():
                        raise AudioWorkerError('transport', 'Caller disconnected; durable inference remains active', True)
                    self._condition.wait(.1)
                if record['state'] == 'failed':
                    return encode_json({'error': record['error']})
                return encode_json(record['response'])
        except AudioWorkerError as exc:
            return encode_json(_error(exc))
        except (OSError, grpc.RpcError) as exc:
            return encode_json(_error(AudioWorkerError('transport', 'Upload interrupted', True)))
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)

    def _connection(self, model):
        parsed = urlsplit(self.origins[model])
        return http.client.HTTPConnection(parsed.hostname, parsed.port, timeout=self.http_timeout)

    def _json(self, model, path, input_path=None):
        connection = self._connection(model)
        try:
            if input_path is None:
                connection.request('GET', path)
            else:
                with input_path.open('rb') as stream:
                    try:
                        connection.request('POST', path, body=stream,
                                           headers={'Content-Length': str(input_path.stat().st_size),
                                                    'Content-Type': 'application/octet-stream'})
                    except BrokenPipeError:
                        # A rejecting service may send its HTTP status before
                        # consuming the upload. Read that status if available.
                        pass
            response = connection.getresponse()
            data = response.read(MAX_JSON_BYTES + 1)
            if response.status not in {200, 202}:
                if input_path is not None and response.status in {400, 401, 403, 404, 405, 413, 415, 422}:
                    raise AudioWorkerError('submission_rejected', f'Model rejected stable request with HTTP {response.status}')
                if response.status == 409:
                    raise AudioWorkerError('identity_conflict', 'Model reported conflicting stable request identity')
                if 400 <= response.status < 500 and response.status not in {408, 429}:
                    raise AudioWorkerError('http_invalid', f'Model HTTP returned {response.status}')
                raise AudioWorkerError('http_unavailable', f'Model HTTP returned {response.status}', True)
            return decode_json(data)
        finally:
            connection.close()

    def _validate_result(self, record, result):
        metadata = record['metadata']
        if any(result.get(key) != metadata[key] for key in ('model', 'input_sha256', 'checkpoint_sha256')):
            raise AudioWorkerError('result_mismatch', 'Result model, checkpoint or input identity mismatch')
        document = result.get('document')
        if (not isinstance(document, dict) or document.get('_schema') != 'unified-synth-preset-v3'
                or not isinstance(document.get('origin'), dict) or document['origin'].get('synth') != 'vital'):
            raise AudioWorkerError('invalid_result', 'Result must contain a Unified Vital document')
        for kind in ('vital', 'wav'):
            if not isinstance(result.get(kind + '_sha256'), str) or not re.fullmatch('[a-f0-9]{64}', result[kind + '_sha256']):
                raise AudioWorkerError('invalid_result', 'Result artifact SHA256 missing')

    def _coordinate(self):
        while not self._stop.is_set():
            with self._condition:
                pending = [r for r in self.records.values() if r['state'] not in TERMINAL]
                if not pending:
                    self._condition.wait(.2)
                    continue
                record = min(pending, key=lambda r: r['created_at'])
            try:
                self._advance(record)
            except AudioWorkerError as exc:
                if not exc.retryable:
                    with self._condition:
                        try:
                            if exc.code == 'submission_rejected' and 'job_id' not in record:
                                self._publish(record, {'state': 'failed', 'error': _error(exc)['error']})
                                (self.root / (record['metadata']['request_id'] + '.audio')).unlink(missing_ok=True)
                            else:
                                # A malformed acceptance may still own live GPU work.
                                self._publish(record, {'quarantine_error': _error(exc)['error']})
                            self._condition.notify_all()
                        except OSError:
                            pass  # Keep the last durable state until storage recovers.
            except (OSError, http.client.HTTPException):
                # Ambiguous transport failures never free the execution slot.
                pass
            self._stop.wait(self.poll_interval)

    def _advance(self, record):
        metadata = record['metadata']
        key, model = metadata['request_id'], metadata['model']
        if 'job_id' not in record:
            with self._condition:
                # Must precede the potentially ambiguous HTTP submission.
                self._publish(record, {'state': 'submitting'})
            submitted = self._json(model, f'/requests/{key}/{model}', self.root / (key + '.audio'))
            job_id = submitted.get('job_id')
            validate_identity(model, job_id)
            if job_id != key or submitted.get('model') != model:
                raise AudioWorkerError('result_mismatch', 'Submission model identity mismatch')
            with self._condition:
                self._publish(record, {'job_id': job_id, 'state': 'running'})
        job_id = record['job_id']
        status = self._json(model, f'/jobs/{job_id}')
        if status.get('job_id') != job_id or status.get('model') != model:
            raise AudioWorkerError('result_mismatch', 'Status identity mismatch')
        state = status.get('state')
        if not isinstance(state, str) or state not in {'queued', 'running', 'completed', 'failed'}:
            raise AudioWorkerError('invalid_status', 'Model returned an unknown job state')
        if 'quarantine_error' in record:
            with self._condition:
                self._publish(record, {}, remove=('quarantine_error',))
                self._condition.notify_all()
        if state not in TERMINAL:
            return
        with self._condition:
            if state == 'failed':
                updates = {'state': 'failed', 'error': _error(AudioWorkerError('model_failed', str(status.get('error', 'Model failed'))[:1800]))['error']}
            else:
                try:
                    result = self._json(model, f'/jobs/{job_id}/result')
                    self._validate_result(record, result)
                    response = {'job_id': job_id, 'model': model, 'state': 'completed', 'result': result}
                    # Reject only a known-terminal result that cannot fit the RPC.
                    # The durable envelope has its own slightly larger bound.
                    encode_json(response)
                except AudioWorkerError as exc:
                    if exc.retryable:
                        raise
                    updates = {'state': 'failed', 'error': _error(exc)['error']}
                else:
                    updates = {'state': 'completed', 'response': response}
            self._publish(record, updates)
            (self.root / (key + '.audio')).unlink(missing_ok=True)
            self._condition.notify_all()

    def Artifact(self, request, context):
        try:
            value = decode_json(request, MAX_METADATA_BYTES)
            if set(value) != {'model', 'request_id', 'kind'} or not isinstance(value['kind'], str) or value['kind'] not in {'vital', 'wav'}:
                raise AudioWorkerError('invalid_artifact', 'Invalid artifact request')
            validate_identity(value['model'], value['request_id'])
            with self._condition:
                record = self.records.get(value['request_id'])
                if record is None or record['state'] != 'completed' or record['metadata']['model'] != value['model']:
                    raise AudioWorkerError('not_found', 'Completed audio request not found')
                self._validate_result(record, record['response']['result'])
                expected = record['response']['result'][value['kind'] + '_sha256']
                job_id = record['job_id']
            connection = self._connection(value['model'])
            cancelled = threading.Event()
            transport_lock = threading.Lock()
            upstream_socket = None
            response = None

            def interrupt():
                cancelled.set()
                with transport_lock:
                    transport = upstream_socket or connection.sock
                if transport is not None:
                    try:
                        # close() alone may leave a response.makefile() read
                        # blocked. shutdown() wakes that read across threads.
                        transport.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass

            def check_active():
                if cancelled.is_set() or not context.is_active():
                    interrupt()
                    raise AudioWorkerError('transport', 'Artifact caller disconnected', True)

            if not context.add_callback(interrupt):
                interrupt()
            try:
                check_active()
                connection.connect()
                with transport_lock:
                    # HTTP/1.0 getresponse() detaches connection.sock. Keep its
                    # socket reference so cancellation can still stop prefetch.
                    upstream_socket = connection.sock
                check_active()
                connection.request('GET', f'/jobs/{job_id}/artifacts/{value["kind"]}')
                check_active()
                response = connection.getresponse()
                check_active()
                if response.status != 200:
                    raise AudioWorkerError('artifact_unavailable', 'Model artifact unavailable', True)
                digest, length = hashlib.sha256(), 0
                with tempfile.TemporaryFile(dir=self.root) as stream:
                    while True:
                        check_active()
                        chunk = response.read(CHUNK_BYTES)
                        check_active()
                        if not chunk:
                            break
                        length += len(chunk)
                        if length > MAX_INPUT_BYTES:
                            raise AudioWorkerError('invalid_artifact', 'Artifact exceeds size limit')
                        digest.update(chunk)
                        stream.write(chunk)
                    if not length or digest.hexdigest() != expected:
                        raise AudioWorkerError('artifact_mismatch', 'Artifact SHA256 mismatch')
                    stream.seek(0)
                    while True:
                        check_active()
                        chunk = stream.read(CHUNK_BYTES)
                        if not chunk:
                            break
                        check_active()
                        yield chunk
            finally:
                if response is not None:
                    response.close()
                connection.close()
                with transport_lock:
                    upstream_socket = None
        except (AudioWorkerError, OSError, http.client.HTTPException) as exc:
            reported = exc if isinstance(exc, AudioWorkerError) else AudioWorkerError('transport', 'Artifact transfer interrupted', True)
            context.set_trailing_metadata((('audio-error-bin', encode_json(_error(reported))),))
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, reported.message)

    def _verified_checkpoint_files(self):
        if self.checkpoint_paths is None:
            return {}
        verified = {}
        with self._checkpoint_lock:
            for model, path in self.checkpoint_paths.items():
                try:
                    def identity(stat):
                        return (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns)
                    before = identity(path.stat())
                    cached = self._checkpoint_cache.get(path)
                    if cached is None or cached[0] != before:
                        digest = hashlib.sha256()
                        with path.open('rb') as stream:
                            if identity(os.fstat(stream.fileno())) != before:
                                continue
                            while chunk := stream.read(1024 * 1024):
                                digest.update(chunk)
                            if identity(os.fstat(stream.fileno())) != before:
                                continue
                        if identity(path.stat()) != before:
                            continue
                        cached = (before, digest.hexdigest())
                        self._checkpoint_cache[path] = cached
                    if cached[1] == CHECKPOINTS[model]:
                        verified[model] = cached[1]
                except OSError:
                    self._checkpoint_cache.pop(path, None)
        return verified

    def Health(self, request, context):
        with self._condition:
            active = [r for r in self.records.values() if r['state'] not in TERMINAL]
            execution = {'inflight': len(active), 'quarantined': [
                {'request_id': r['metadata']['request_id'], 'model': r['metadata']['model'],
                 'error': r['quarantine_error']} for r in active if 'quarantine_error' in r]}
        verified = self._verified_checkpoint_files()
        available = []
        try:
            if decode_json(request, MAX_METADATA_BYTES) != {}:
                raise AudioWorkerError('invalid_metadata', 'Health expects empty metadata')
            for model in CHECKPOINTS:
                health = self._json(model, '/health')
                if self.checkpoint_paths is None and health.get('checkpoints') == CHECKPOINTS:
                    verified[model] = CHECKPOINTS[model]
                if (model in health.get('models', []) and health.get('ready', True) is not False
                        and model in verified):
                    available.append(model)
        except (AudioWorkerError, OSError, http.client.HTTPException):
            available = []
        return encode_json({'ready': len(available) == len(CHECKPOINTS) and not execution['quarantined'],
                            'models': available, 'checkpoints': verified,
                            'expected_checkpoints': CHECKPOINTS, **execution})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--checkpoint-manifest', type=Path, required=True,
                        help='JSON object mapping all three model names to absolute checkpoint paths')
    parser.add_argument('--bind', choices=['127.0.0.1'], default='127.0.0.1')
    parser.add_argument('--port', type=int, default=50061)
    args = parser.parse_args()
    checkpoints = decode_json(args.checkpoint_manifest.read_bytes(), MAX_METADATA_BYTES)
    worker = AudioWorker(args.root, checkpoint_paths=checkpoints)
    server = grpc.server(ThreadPoolExecutor(max_workers=8), options=[('grpc.max_receive_message_length', MAX_JSON_BYTES)])
    worker.add_to_server(server)
    if not server.add_insecure_port(f'{args.bind}:{args.port}'):
        worker.close()
        raise RuntimeError('Cannot bind private audio worker')
    server.start()
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(0).wait()
    finally:
        worker.close()


if __name__ == '__main__':
    main()
