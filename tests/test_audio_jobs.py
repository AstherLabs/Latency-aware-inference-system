import asyncio
import hashlib
import json
import threading
import time
from pathlib import Path

import pytest

from system.queue import TaskQueue
from system.task import Task

ID = 'a' * 32
MODELS = ('vital_v7_3', 'vital_v7_5', 'vital_v7_5_chord')

async def chunks(data=b'audio'):
    for offset in range(0, len(data), 65536):
        yield data[offset:offset + 65536]


def load_module():
    import system.audio_jobs as module
    return module


def submit(manager, model=MODELS[0], request_id=ID, data=b'audio'):
    return asyncio.run(manager.submit(model, request_id, chunks(data)))


def completion(task):
    payload = task.input_data
    return {'job_id': payload['request_id'], 'model': payload['model'], 'state': 'completed',
            'result': {'model': payload['model'], 'checkpoint_sha256': payload['checkpoint_sha256'],
                       'input_sha256': payload['input_sha256'], 'artifacts': {'vital': {'sha256': '1'*64}}}}


def wait_state(manager, request_id, state):
    with manager._condition:
        assert manager._condition.wait_for(lambda: manager.status(request_id)['state'] == state, timeout=3)
    return manager.status(request_id)


@pytest.mark.parametrize('model', MODELS)
def test_scheduler_task_and_durable_validated_result(tmp_path, model):
    module = load_module()
    queue = TaskQueue()
    manager = module.AudioJobManager(tmp_path, queue, 'worker:50061')
    manager.start()
    try:
        accepted = submit(manager, model)
        assert accepted['job_id'] == ID
        task = queue.get(timeout=2)
        assert task is not None
        assert task.input_data['kind'] == 'synthx_audio'
        assert Path(task.input_data['input_path']).read_bytes() == b'audio'
        assert task.deadline <= time.monotonic() + 1700
        result = completion(task)
        task.mark_done(result)
        wait_state(manager, ID, 'completed')
        assert manager.result(ID) == result['result']
        assert not Path(task.input_data['input_path']).exists()
        assert submit(manager, model)['job_id'] == ID
        with pytest.raises(module.AudioJobError, match='different'):
            submit(manager, model, data=b'changed')
        other = next(x for x in MODELS if x != model)
        with pytest.raises(module.AudioJobError, match='different'):
            submit(manager, other)
    finally:
        manager.stop()
    recovered = module.AudioJobManager(tmp_path, TaskQueue(), 'worker:50061')
    recovered.start()
    try:
        assert recovered.result(ID) == result['result']
    finally:
        recovered.stop()


def test_module_exists_for_durable_scheduler():
    assert load_module().AudioJobManager is not None


def test_ownership_and_restart_replay_same_identity(tmp_path):
    module = load_module()
    queue = TaskQueue()
    first = module.AudioJobManager(tmp_path, queue, 'worker:50061')
    first.start()
    submit(first)
    task = queue.get(timeout=2)
    second = module.AudioJobManager(tmp_path, TaskQueue(), 'worker:50061')
    with pytest.raises((BlockingIOError, RuntimeError)):
        second.start()
    deadline = first.status(ID)['deadline_at']
    first.stop()
    second.start()
    try:
        replay = second.queue.get(timeout=2)
        assert replay.input_data == task.input_data
        assert second.status(ID)['deadline_at'] == deadline
        replay.mark_done(completion(replay))
        wait_state(second, ID, 'completed')
    finally:
        second.stop()


def test_pending_admission_queue_saturation_retry_and_deadline(tmp_path):
    module = load_module()
    queue = TaskQueue(maxsize=1)
    blocker = Task({})
    queue.put(blocker)
    manager = module.AudioJobManager(tmp_path, queue, 'worker:50061', max_pending=1, job_timeout=2)
    manager.start()
    try:
        submit(manager)
        with pytest.raises(module.AudioJobError) as error:
            submit(manager, request_id='b'*32)
        assert error.value.status_code == 503
        assert queue.get(timeout=1) is blocker
        task = queue.get(timeout=2)
        original_deadline = task.deadline
        task.mark_failed('disconnected after submit', 'transport_error')
        replay = queue.get(timeout=2)
        assert replay.input_data == task.input_data
        assert abs(replay.deadline - original_deadline) < .02
        replay.mark_failed('model crashed', 'model_error')
        wait_state(manager, ID, 'failed')
        assert manager.status(ID)['error']['code'] == 'model_error'
    finally:
        manager.stop()
    expired = module.AudioJobManager(tmp_path / 'deadline', TaskQueue(), 'worker:50061', job_timeout=.05)
    expired.start()
    try:
        submit(expired)
        wait_state(expired, ID, 'failed')
        assert expired.status(ID)['error']['code'] == 'deadline_exceeded'
    finally:
        expired.stop()


@pytest.mark.parametrize('field,value', [('model', 'vital_v7_5'), ('checkpoint_sha256', '0'*64), ('input_sha256', '0'*64)])
def test_malformed_result_never_becomes_completed(tmp_path, field, value):
    module = load_module()
    manager = module.AudioJobManager(tmp_path, TaskQueue(), 'worker:50061')
    manager.start()
    try:
        submit(manager)
        task = manager.queue.get(timeout=2)
        response = completion(task)
        response['result'][field] = value
        task.mark_done(response)
        wait_state(manager, ID, 'failed')
        assert manager.status(ID)['error']['code'] == 'identity_mismatch'
        with pytest.raises(module.AudioJobError):
            manager.result(ID)
    finally:
        manager.stop()


def test_streamed_large_upload_limit_and_interruption(tmp_path, monkeypatch):
    module = load_module()
    monkeypatch.setattr(module.audio_protocol, 'MAX_INPUT_BYTES', 2*1024*1024)
    manager = module.AudioJobManager(tmp_path, TaskQueue(), 'worker:50061', max_pending=3)
    manager.start()
    chunk = b'0' * 65536
    async def large():
        for _ in range(25):
            yield chunk
    try:
        accepted = asyncio.run(manager.submit(MODELS[0], ID, large()))
        assert accepted['input_size'] == 25*65536
        task = manager.queue.get(timeout=2)
        assert Path(task.input_data['input_path']).stat().st_size == 25*65536
        async def oversized():
            for _ in range(33):
                yield chunk
        with pytest.raises(module.AudioJobError) as error:
            asyncio.run(manager.submit(MODELS[0], 'b'*32, oversized()))
        assert error.value.status_code == 413
        assert manager.status('b'*32)['state'] == 'failed'
        assert not list((tmp_path / ('b'*32)).glob('*.upload'))
        assert submit(manager, request_id='b'*32)['state'] == 'queued'
    finally:
        manager.stop()


def test_drain_holds_until_terminal_and_recovers_accepted_work(tmp_path):
    module = load_module()
    from system.deployment import DeploymentGate
    gate = DeploymentGate(tmp_path / 'drain')
    manager = module.AudioJobManager(tmp_path / 'jobs', TaskQueue(), 'worker:50061', deployment_gate=gate)
    manager.start()
    try:
        submit(manager)
        task = manager.queue.get(timeout=2)
        assert gate.snapshot()['active'] == 1
        gate.marker.write_text('deployment')
        assert submit(manager)['job_id'] == ID
        with pytest.raises(module.AudioJobError) as error:
            submit(manager, request_id='b'*32)
        assert error.value.code == 'DEPLOYMENT_DRAINING'
        assert manager.status(ID)['state'] == 'queued'
        task.mark_done(completion(task))
        wait_state(manager, ID, 'completed')
        assert gate.snapshot()['active'] == 0
    finally:
        manager.stop()


def test_rejects_symlinked_private_state(tmp_path):
    module = load_module()
    other = tmp_path / 'elsewhere'
    other.mkdir()
    linked = tmp_path / 'jobs'
    linked.symlink_to(other, target_is_directory=True)
    manager = module.AudioJobManager(linked, TaskQueue(), 'worker:50061')
    with pytest.raises((ValueError, OSError)):
        manager.start()
    assert list(other.iterdir()) == []


def test_identical_replay_works_at_full_pending_and_disk_admission(tmp_path):
    module = load_module()
    manager = module.AudioJobManager(tmp_path, TaskQueue(), 'worker:50061', max_pending=1)
    manager.start()
    try:
        submit(manager)
        first = manager.queue.get(timeout=2)
        assert submit(manager)['job_id'] == ID
        assert manager.queue.empty()
        first.mark_done(completion(first))
        wait_state(manager, ID, 'completed')
    finally:
        manager.stop()


def test_stop_during_upload_retains_process_lock_until_stream_finishes(tmp_path):
    module = load_module()
    manager = module.AudioJobManager(tmp_path, TaskQueue(), 'worker:50061')
    manager.start()
    async def scenario():
        started, release = asyncio.Event(), asyncio.Event()
        async def stream():
            yield b'first'
            started.set()
            await release.wait()
            yield b'second'
        upload = asyncio.create_task(manager.submit(MODELS[0], ID, stream()))
        await started.wait()
        manager.stop()
        second = module.AudioJobManager(tmp_path, TaskQueue(), 'worker:50061')
        try:
            with pytest.raises((BlockingIOError, RuntimeError)):
                second.start()
        finally:
            second.stop()
            release.set()
            await upload
        second.start()
        try:
            replay = second.queue.get(timeout=2)
            assert replay.input_data['input_sha256'] == hashlib.sha256(b'firstsecond').hexdigest()
        finally:
            second.stop()
    asyncio.run(scenario())


def test_real_process_crash_recovers_accepted_job_and_partial_upload(tmp_path):
    import subprocess
    import sys
    module = load_module()
    script = '''
import asyncio, os, sys
from system.audio_jobs import AudioJobManager
from system.queue import TaskQueue
manager=AudioJobManager(sys.argv[1], TaskQueue(), 'worker:50061')
manager.start()
async def source():
    yield b'audio'
asyncio.run(manager.submit('vital_v7_3', 'a'*32, source()))
manager.queue.get(timeout=2)
async def broken():
    yield b'preserve-this-partial-upload'
    os._exit(0)
asyncio.run(manager.submit('vital_v7_3', 'b'*32, broken()))
'''
    subprocess.run([sys.executable, '-c', script, str(tmp_path)], check=True, timeout=10)
    accepted = json.loads((tmp_path / ID / 'state.json').read_text())
    partials = list((tmp_path / ('b'*32)).glob('*.upload'))
    assert len(partials) == 1
    manager = module.AudioJobManager(tmp_path, TaskQueue(), 'worker:50061')
    manager.start()
    try:
        task = manager.queue.get(timeout=2)
        assert task.input_data['request_id'] == ID
        assert manager.status(ID)['deadline_at'] == accepted['deadline_at']
        assert manager.status('b'*32)['error']['code'] == 'upload_interrupted'
        assert partials[0].exists()
        assert submit(manager, request_id='b'*32)['state'] == 'queued'
        assert partials[0].exists()
    finally:
        manager.stop()


def test_recovery_restores_gate_hold_even_during_deployment_drain(tmp_path):
    module = load_module()
    from system.deployment import DeploymentGate
    gate = DeploymentGate(tmp_path / 'drain')
    first = module.AudioJobManager(tmp_path / 'jobs', TaskQueue(), 'worker:50061', deployment_gate=gate)
    first.start()
    submit(first)
    first.queue.get(timeout=2)
    first.stop()
    assert gate.snapshot()['active'] == 0
    gate.marker.write_text('draining')
    second = module.AudioJobManager(tmp_path / 'jobs', TaskQueue(), 'worker:50061', deployment_gate=gate)
    second.start()
    try:
        assert gate.snapshot()['active'] == 1
        replay = second.queue.get(timeout=2)
        replay.mark_done(completion(replay))
        wait_state(second, ID, 'completed')
        assert gate.snapshot()['active'] == 0
    finally:
        second.stop()


def test_filesystem_admission_failure_does_not_leak_gate_hold(tmp_path):
    module = load_module()
    from system.deployment import DeploymentGate
    gate = DeploymentGate(tmp_path / 'drain')
    manager = module.AudioJobManager(tmp_path / 'jobs', TaskQueue(), 'worker:50061', deployment_gate=gate)
    manager.start()
    try:
        (manager.root / ID).write_bytes(b'preserve-user-data')
        with pytest.raises(OSError):
            submit(manager)
        assert gate.snapshot()['active'] == 0
        assert (manager.root / ID).read_bytes() == b'preserve-user-data'
    finally:
        manager.stop()


def test_health_requires_pinned_worker_identity(tmp_path, monkeypatch):
    module = load_module()
    manager = module.AudioJobManager(tmp_path, TaskQueue(), 'worker:50061')
    manager.start()
    try:
        healthy = {'ready': True, 'models': list(MODELS), 'checkpoints': dict(module.audio_protocol.CHECKPOINTS)}
        monkeypatch.setattr(module.audio_protocol, 'probe_remote', lambda *a, **k: healthy)
        assert manager.health()['ready']
        healthy['checkpoints'][MODELS[0]] = '0'*64
        assert not manager.health()['ready']
    finally:
        manager.stop()


def test_upload_deadline_bounds_a_stalled_stream_and_releases_gate(tmp_path):
    module = load_module()
    from system.deployment import DeploymentGate
    gate = DeploymentGate(tmp_path / 'drain')
    manager = module.AudioJobManager(tmp_path / 'jobs', TaskQueue(), 'worker:50061', deployment_gate=gate, job_timeout=.05)
    manager.start()
    async def stalled():
        yield b'partial'
        await asyncio.Event().wait()
    async def run():
        with pytest.raises(module.AudioJobError) as error:
            await asyncio.wait_for(manager.submit(MODELS[0], ID, stalled()), timeout=.5)
        assert error.value.status_code == 408
    try:
        asyncio.run(run())
        assert gate.snapshot()['active'] == 0
        assert manager.queue.empty()
    finally:
        manager.stop()


def test_result_with_nonfinite_json_is_terminal_invalid_response(tmp_path):
    module = load_module()
    manager = module.AudioJobManager(tmp_path, TaskQueue(), 'worker:50061')
    manager.start()
    try:
        submit(manager)
        task = manager.queue.get(timeout=2)
        response = completion(task)
        response['result']['document'] = {'cutoff': float('inf')}
        task.mark_done(response)
        wait_state(manager, ID, 'failed')
        assert manager.status(ID)['error']['code'] == 'identity_mismatch'
    finally:
        manager.stop()


def test_retained_private_metadata_counts_toward_disk_budget(tmp_path, monkeypatch):
    module = load_module()
    monkeypatch.setattr(module.audio_protocol, 'MAX_INPUT_BYTES', 1024)
    monkeypatch.setattr(module.audio_protocol, 'MAX_JSON_BYTES', 1024)
    manager = module.AudioJobManager(tmp_path, TaskQueue(), 'worker:50061', max_pending=1)
    manager.start()
    try:
        directory = tmp_path / 'retained'
        directory.mkdir()
        retained = directory / 'audit.json'
        retained.write_bytes(b'x' * (128 * 1024))
        with pytest.raises(module.AudioJobError) as error:
            submit(manager)
        assert error.value.code == 'storage_full'
        assert retained.stat().st_size == 128*1024
    finally:
        manager.stop()


def test_pending_upload_replay_is_retryable_until_fingerprint_known(tmp_path):
    module = load_module()
    manager = module.AudioJobManager(tmp_path, TaskQueue(), 'worker:50061')
    manager.start()
    async def run():
        started, release = asyncio.Event(), asyncio.Event()
        async def pending():
            yield b'au'
            started.set()
            await release.wait()
            yield b'dio'
        first = asyncio.create_task(manager.submit(MODELS[0], ID, pending()))
        await started.wait()
        try:
            with pytest.raises(module.AudioJobError) as error:
                await manager.submit(MODELS[0], ID, chunks())
            assert error.value.status_code in {429, 503}
            assert error.value.headers['Retry-After'] == '1'
        finally:
            release.set()
            await first
        assert submit_status(await manager.submit(MODELS[0], ID, chunks())) == ID
    def submit_status(status):
        return status['job_id']
    try:
        asyncio.run(run())
    finally:
        manager.stop()


def test_both_admission_state_writes_fail_releases_gate_and_repairs_same_id(tmp_path):
    module = load_module()
    from system.deployment import DeploymentGate
    gate = DeploymentGate(tmp_path / 'drain')
    manager = module.AudioJobManager(tmp_path / 'jobs', TaskQueue(), 'worker:50061', deployment_gate=gate)
    manager.start()
    state = manager.root / ID / 'state.json'
    receipt = manager.root / ID / 'original-upload-state.json'
    async def disk_fault():
        yield b'audio'
        state.rename(receipt)
        state.mkdir()  # Real filesystem failure: both atomic state replacements fail.
    try:
        with pytest.raises(OSError):
            asyncio.run(manager.submit(MODELS[0], ID, disk_fault()))
        assert gate.snapshot()['active'] == 0
        assert manager.status(ID)['state'] == 'failed'
        assert manager.status(ID)['error']['code'] == 'upload_interrupted'
        assert manager.status(ID)['durable'] is False
        assert manager.queue.empty()
        source = manager.root / ID / 'input'
        assert source.read_bytes() == b'audio'
        assert json.loads(receipt.read_text())['state'] == 'uploading'
        state.rmdir()
        receipt.rename(state)
        with pytest.raises(module.AudioJobError, match='different'):
            submit(manager, data=b'changed')
        assert source.read_bytes() == b'audio'
        repaired = submit(manager)
        assert repaired['input_sha256'] == hashlib.sha256(b'audio').hexdigest()
        assert repaired.get('durable', True)
        task = manager.queue.get(timeout=2)
        assert task.input_data['request_id'] == ID
        task.mark_done(completion(task))
        wait_state(manager, ID, 'completed')
        assert gate.snapshot()['active'] == 0
    finally:
        if state.is_dir():
            state.rmdir()
            receipt.rename(state)
        manager.stop()


def test_draining_preserves_real_worker_health_but_stops_admission(tmp_path):
    from tests.test_audio_worker import ModelHTTP, start_worker, finish
    from system.deployment import DeploymentGate
    module = load_module()
    http_model = ModelHTTP()
    running = start_worker(tmp_path / 'worker', http_model)
    address = running[2]._channel.target().decode()
    gate = DeploymentGate(tmp_path / 'drain')
    manager = module.AudioJobManager(tmp_path / 'gateway', TaskQueue(), address, deployment_gate=gate)
    manager.start()
    try:
        healthy = manager.health()
        assert healthy['ready'] is True
        assert healthy['accepting'] is True
        gate.marker.write_text('rolling')
        draining = manager.health()
        assert draining['ready'] is True
        assert draining['accepting'] is False
        assert draining['deployment']['draining'] is True
        assert draining['worker']['checkpoints'] == module.audio_protocol.CHECKPOINTS
        with pytest.raises(module.AudioJobError) as error:
            submit(manager)
        assert error.value.status_code == 503
        assert error.value.code == 'DEPLOYMENT_DRAINING'
    finally:
        manager.stop()
        finish(running)
        http_model.close()
