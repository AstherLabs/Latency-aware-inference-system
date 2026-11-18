"""Private, crash durable audio uploads dispatched only by the existing scheduler."""
import fcntl
import asyncio
from contextlib import nullcontext
import hashlib
import json
import logging
import os
from pathlib import Path
from queue import Full
import re
import shutil
import threading
import time
import uuid

import grpc

from . import audio_protocol
from .task import Task, TaskStatus

logger = logging.getLogger(__name__)
TERMINAL = {'completed', 'failed'}
RETRYABLE = {'transport', 'transport_error', 'worker_busy', 'already_running', 'in_progress',
             'worker_unavailable', 'http_unavailable', 'unavailable', 'overloaded', 'busy', 'socket_unavailable'}


class AudioJobError(Exception):
    def __init__(self, status_code, code, message, headers=None):
        super().__init__(message)
        self.status_code, self.code, self.headers = status_code, code, headers or {}


class AudioJobManager:
    def __init__(self, root, queue, worker_address, deployment_gate=None, max_pending=4, job_timeout=1700):
        if max_pending < 1 or job_timeout <= 0:
            raise ValueError('invalid audio admission settings')
        self.root = Path(root)
        self.queue, self.worker_address, self.gate = queue, worker_address, deployment_gate
        self.max_pending, self.job_timeout = max_pending, job_timeout
        # Reserve one input and both old/new JSON versions during atomic replacement.
        self._per_job_bytes = audio_protocol.MAX_INPUT_BYTES + 2 * audio_protocol.MAX_JSON_BYTES + 65536
        self._disk_limit = max_pending * self._per_job_bytes
        self._condition = threading.Condition(threading.RLock())
        self._records, self._attempts, self._held = {}, {}, set()
        self._uploading = set()
        self._upload_paths = {}
        self._stop = threading.Event()
        self._thread = self._ownership = None
        self._channel = None
        self._running = False

    def _write(self, record):
        directory = self.root / record['job_id']
        if directory.is_symlink():
            raise ValueError('Audio job directory must not be a symlink')
        directory.mkdir(mode=0o700, exist_ok=True)
        self._sync_directory(self.root)
        temporary = directory / ('state.' + uuid.uuid4().hex + '.tmp')
        try:
            with temporary.open('x', encoding='utf8') as output:
                os.chmod(temporary, 0o600)
                json.dump(record, output, allow_nan=False, separators=(',', ':'))
                output.flush()
                os.fsync(output.fileno())
            os.replace(temporary, directory / 'state.json')
            self._sync_directory(directory)
        finally:
            temporary.unlink(missing_ok=True)
        self._records[record['job_id']] = record
        self._condition.notify_all()

    @staticmethod
    def _sync_directory(directory):
        descriptor = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def _acquire_gate(self, request_id, existing=False):
        if request_id in self._held:
            return True
        if self.gate:
            enter = getattr(self.gate, 'enter_existing', self.gate.enter) if existing else self.gate.enter
            if enter() is False:
                return False
        self._held.add(request_id)
        return True

    def _release_gate(self, request_id):
        if request_id in self._held:
            self._held.remove(request_id)
            if self.gate:
                self.gate.leave()

    def start(self):
        with self._condition:
            if self._running:
                return
            if self._ownership is not None:
                raise RuntimeError('Audio uploads are still shutting down')
            if self.root.is_symlink():
                raise ValueError('Audio storage root must not be a symlink')
            self.root.mkdir(parents=True, mode=0o700, exist_ok=True)
            os.chmod(self.root, 0o700)
            descriptor = os.open(self.root / '.owner.lock', os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
            self._ownership = os.fdopen(descriptor, 'a+b')
            try:
                fcntl.flock(self._ownership, fcntl.LOCK_EX | fcntl.LOCK_NB)
                self._records.clear()
                for path in self.root.glob('*/state.json'):
                    if path.is_symlink() or path.parent.is_symlink():
                        raise ValueError('Audio durable records must not be symlinks')
                    record = json.loads(path.read_text())
                    self._validate_id(record['job_id'])
                    if path.parent.name != record['job_id'] or record['model'] not in audio_protocol.CHECKPOINTS:
                        raise ValueError('invalid durable audio record')
                    if record['checkpoint_sha256'] != audio_protocol.CHECKPOINTS[record['model']]:
                        raise ValueError('durable audio checkpoint differs')
                    if record['state'] == 'uploading':
                        record.update(state='failed', error={'code': 'upload_interrupted', 'message': 'Upload interrupted before admission'})
                        self._write(record)
                    else:
                        self._records[record['job_id']] = record
                    if record['state'] not in TERMINAL:
                        self._acquire_gate(record['job_id'], existing=True)
                self._channel = grpc.insecure_channel(self.worker_address)
                self._stop.clear()
                self._running = True
                self._thread = threading.Thread(target=self._loop, daemon=True, name='AudioJobs')
                self._thread.start()
            except BaseException:
                for request_id in list(self._held):
                    self._release_gate(request_id)
                self._ownership.close()
                self._ownership = None
                raise

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)
        with self._condition:
            self._running = False
            # Attempts are not cancelled: the remote request ID remains authoritative.
            self._attempts.clear()
            if not self._uploading:
                self._close_resources()

    def _close_resources(self):
        for request_id in list(self._held):
            self._release_gate(request_id)
        if self._channel:
            self._channel.close()
            self._channel = None
        if self._ownership:
            self._ownership.close()
            self._ownership = None

    @staticmethod
    def _validate_id(request_id):
        if not isinstance(request_id, str) or not re.fullmatch('[0-9a-f]{32}', request_id):
            raise AudioJobError(400, 'invalid_request', 'request ID must be lowercase hex32')

    def _get(self, request_id):
        self._validate_id(request_id)
        try:
            return self._records[request_id]
        except KeyError:
            raise AudioJobError(404, 'not_found', 'Audio job not found') from None

    def _disk_bytes(self):
        return sum(path.stat().st_size for path in self.root.glob('*/*')
                   if path not in self._upload_paths.values() and path.is_file())

    async def submit(self, model, request_id, async_chunks):
        self._validate_id(request_id)
        if model not in audio_protocol.CHECKPOINTS:
            raise AudioJobError(400, 'invalid_model', 'Unsupported audio model')
        with self._condition:
            if not self._running or self._stop.is_set():
                raise AudioJobError(503, 'unavailable', 'Audio gateway is stopping')
            old = self._records.get(request_id)
            if old and old['model'] != model:
                raise AudioJobError(409, 'identity_conflict', 'Request already has a different model')
            if request_id in self._uploading:
                raise AudioJobError(503, 'upload_in_progress', 'Request upload is already in progress', {'Retry-After': '1'})
            if len(self._uploading) >= self.max_pending:
                raise AudioJobError(503, 'overloaded', 'Audio upload capacity reached', {'Retry-After': '2'})
            new = old is None or old.get('error', {}).get('code') == 'upload_interrupted'
            if new and sum(r['state'] not in TERMINAL for r in self._records.values()) >= self.max_pending:
                raise AudioJobError(503, 'overloaded', 'Audio job capacity reached', {'Retry-After': '2'})
            maximum = audio_protocol.MAX_INPUT_BYTES
            if new and self._disk_bytes() + (len(self._upload_paths) + 1) * maximum + 2 * audio_protocol.MAX_JSON_BYTES + 65536 > self._disk_limit:
                raise AudioJobError(503, 'storage_full', 'Audio input storage capacity reached', {'Retry-After': '2'})
            if new and shutil.disk_usage(self.root).free < (len(self._upload_paths) + 1) * maximum:
                raise AudioJobError(503, 'storage_full', 'Audio input storage unavailable')
            if new and not self._acquire_gate(request_id):
                raise AudioJobError(503, 'DEPLOYMENT_DRAINING', 'Inference service is updating. Retry shortly.', {'Retry-After': '30'})
            record = old
            if new:
                created = old['created_at'] if old else time.time()
                record = {'job_id': request_id, 'model': model, 'state': 'uploading',
                          'checkpoint_sha256': audio_protocol.CHECKPOINTS[model], 'created_at': created,
                          'deadline_at': old['deadline_at'] if old else created + self.job_timeout, 'attempts': 0}
                if old and 'input_sha256' in old:
                    record.update(input_sha256=old['input_sha256'], input_size=old['input_size'])
                try:
                    self._write(record)
                except BaseException:
                    self._release_gate(request_id)
                    raise
            self._uploading.add(request_id)
            directory = self.root / request_id
            upload = directory / (uuid.uuid4().hex + '.upload') if new else None
            if new:
                self._upload_paths[request_id] = upload
        digest, size = hashlib.sha256(), 0
        updated = None
        try:
            with (upload.open('xb') if upload else nullcontext(None)) as output:
                if upload:
                    os.chmod(upload, 0o600)
                upload_timeout = max(0, record['deadline_at'] - time.time()) if new else self.job_timeout
                try:
                    async with asyncio.timeout(upload_timeout):
                        async for chunk in async_chunks:
                            if not isinstance(chunk, bytes):
                                raise AudioJobError(400, 'invalid_input', 'Audio upload chunks must be bytes')
                            size += len(chunk)
                            if size > maximum:
                                raise AudioJobError(413, 'input_too_large', 'Audio input exceeds 200 MiB')
                            # Bound disk writes even when the ASGI server supplies a large chunk.
                            for offset in range(0, len(chunk), audio_protocol.CHUNK_BYTES):
                                piece = memoryview(chunk)[offset:offset + audio_protocol.CHUNK_BYTES]
                                digest.update(piece)
                                if output is not None:
                                    output.write(piece)
                except TimeoutError as exc:
                    raise AudioJobError(408, 'upload_timeout', 'Audio upload deadline exceeded') from exc
                if not size:
                    raise AudioJobError(400, 'invalid_input', 'Audio input is empty')
                if output is not None:
                    output.flush()
                    os.fsync(output.fileno())
            with self._condition:
                if not new:
                    if record['input_sha256'] != digest.hexdigest() or record['input_size'] != size:
                        raise AudioJobError(409, 'identity_conflict', 'Request already has a different audio source')
                    return self.status(request_id)
                if 'input_sha256' in record and (record['input_sha256'] != digest.hexdigest() or record['input_size'] != size):
                    raise AudioJobError(409, 'identity_conflict', 'Request already has a different audio source')
                os.replace(upload, directory / 'input')
                updated = dict(record, state='queued', input_sha256=digest.hexdigest(), input_size=size)
                self._sync_directory(directory)
                self._write(updated)
                return self.status(request_id)
        except BaseException:
            if new:
                with self._condition:
                    failed = dict(updated or record, state='failed', error={'code': 'upload_interrupted', 'message': 'Upload was not admitted'})
                    try:
                        self._write(failed)
                    except OSError:
                        # No success was acknowledged. Keep repairable local state
                        # without claiming that the disk failure was persisted.
                        self._records[request_id] = dict(failed, durable=False)
                    finally:
                        self._release_gate(request_id)
                        self._condition.notify_all()
            raise
        finally:
            if upload:
                try:
                    upload.unlink(missing_ok=True)
                except OSError:
                    logger.warning('Retaining interrupted upload after filesystem cleanup failure')
            with self._condition:
                self._uploading.discard(request_id)
                self._upload_paths.pop(request_id, None)
                if not self._running and not self._uploading:
                    self._close_resources()
                self._condition.notify_all()

    def status(self, request_id):
        with self._condition:
            record = self._get(request_id)
            keys = ('job_id', 'model', 'state', 'checkpoint_sha256', 'input_sha256', 'input_size', 'created_at', 'deadline_at', 'error', 'durable')
            return {key: record[key] for key in keys if key in record}

    def result(self, request_id):
        with self._condition:
            record = self._get(request_id)
            if record['state'] != 'completed':
                raise AudioJobError(409, 'not_completed', 'Audio result is not completed')
            # Return an independent value, preventing external mutation of persisted identity.
            return json.loads(json.dumps(record['result']))

    def _finish(self, record, *, result=None, code=None, message=None):
        updated = dict(record)
        if code:
            updated.update(state='failed', error={'code': code, 'message': message or code})
        else:
            updated.update(state='completed', result=result)
            updated.pop('error', None)
        self._write(updated)
        self._attempts.pop(record['job_id'], None)
        self._release_gate(record['job_id'])
        if result is not None:
            (self.root / record['job_id'] / 'input').unlink(missing_ok=True)
            self._sync_directory(self.root / record['job_id'])

    def _validated_result(self, record, response):
        try:
            audio_protocol.encode_json(response)
        except (ValueError, TypeError, audio_protocol.AudioWorkerError) as exc:
            raise ValueError('Audio result must be bounded finite JSON') from exc
        if not isinstance(response, dict) or any(response.get(k) != v for k, v in
            {'job_id': record['job_id'], 'model': record['model'], 'state': 'completed'}.items()):
            raise ValueError('Audio worker returned a different job identity')
        result = response.get('result')
        if not isinstance(result, dict) or any(result.get(k) != record[k] for k in
            ('model', 'checkpoint_sha256', 'input_sha256')):
            raise ValueError('Audio result model, checkpoint or input fingerprint differs')
        return result

    def _poll(self):
        with self._condition:
            for request_id, record in list(self._records.items()):
                if record['state'] in TERMINAL or record['state'] == 'uploading':
                    continue
                task = self._attempts.get(request_id)
                if time.time() >= record['deadline_at']:
                    if task:
                        task.mark_failed('Audio job deadline exceeded', 'deadline_exceeded')
                    self._finish(record, code='deadline_exceeded')
                    continue
                if task and task.status == TaskStatus.DONE:
                    try:
                        result = self._validated_result(record, task.result)
                    except ValueError as exc:
                        self._finish(record, code='identity_mismatch', message=str(exc))
                    else:
                        self._finish(record, result=result)
                    continue
                if task and task.status == TaskStatus.FAILED:
                    code = task.error_code or 'worker_error'
                    if code not in RETRYABLE:
                        self._finish(record, code=code, message=task.error)
                        continue
                    updated = dict(record, state='queued', retry_at=time.time() + min(5, .1 * 2 ** min(record['attempts'], 6)))
                    self._write(updated)
                    record = updated
                    self._attempts.pop(request_id)
                    task = None
                if task:
                    if task.status == TaskStatus.PROCESSING and record['state'] != 'running':
                        self._write(dict(record, state='running'))
                    continue
                if time.time() < record.get('retry_at', 0) or not self._acquire_gate(request_id, existing=True):
                    continue
                path = self.root / request_id / 'input'
                if path.is_symlink() or not path.is_file():
                    self._finish(record, code='input_missing', message='Durable audio source is unavailable')
                    continue
                payload = {key: record[key] for key in ('model', 'input_sha256', 'input_size', 'checkpoint_sha256')}
                payload.update(kind=audio_protocol.KIND, request_id=request_id, input_path=str(path))
                task = Task(input_data=payload, deadline=time.monotonic() + max(0, record['deadline_at'] - time.time()))
                try:
                    self.queue.put(task, timeout=0)
                except Full:
                    continue
                self._attempts[request_id] = task
                self._write(dict(record, attempts=record['attempts'] + 1, state='queued'))

    def _loop(self):
        while not self._stop.is_set():
            try:
                self._poll()
            except Exception:
                logger.exception('Audio gateway monitor failed; durable records retained')
            self._stop.wait(.05)

    def artifact(self, request_id, kind):
        if kind not in {'vital', 'wav'}:
            raise AudioJobError(400, 'invalid_artifact', 'Unsupported audio artifact')
        self.result(request_id)
        with self._condition:
            record = self._get(request_id)
            if not self._channel:
                raise AudioJobError(503, 'unavailable', 'Audio gateway is stopping')
            return audio_protocol.remote_artifact(self._channel, record['model'], request_id, kind)

    def health(self):
        with self._condition:
            local = {'ready': self._running and not self._stop.is_set(),
                     'pending': sum(r['state'] not in TERMINAL for r in self._records.values()),
                     'max_pending': self.max_pending}
            channel = self._channel
        if self.gate:
            local['deployment'] = self.gate.snapshot()
        try:
            worker = audio_protocol.probe_remote(channel, timeout=5) if channel else {'ready': False}
            local['worker'] = worker
            identities_match = (isinstance(worker.get('models'), list)
                                and set(audio_protocol.CHECKPOINTS).issubset(worker['models'])
                                and worker.get('checkpoints') == audio_protocol.CHECKPOINTS)
            local['ready'] = bool(local['ready'] and worker.get('ready') and identities_match)
        except Exception:
            local.update(ready=False, worker={'ready': False})
        local['accepting'] = bool(local['ready'] and not local.get('deployment', {}).get('draining', False))
        return local
