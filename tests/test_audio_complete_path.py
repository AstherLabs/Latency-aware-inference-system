"""Real gateway lifecycle, scheduler, gRPC and HTTP model boundary together."""
import os
import time

from fastapi.testclient import TestClient

from system import api_server
from system.config import config
from system.grpc_worker_pool import GrpcWorkerPool
from tests.test_audio_worker import ModelHTTP, start_worker, finish, sha


def test_all_models_complete_through_gateway_scheduler_and_streamed_worker(tmp_path, monkeypatch):
    model = ModelHTTP()
    running = start_worker(tmp_path / 'worker', model)
    address = running[2]._channel.target().decode()
    old_batch = config.batch_size
    config.update(batch_size=1)
    token = 'integration-service-token'
    monkeypatch.setattr(api_server, 'JOBS_ENABLED', False)
    monkeypatch.setattr(api_server, 'AUDIO_ENABLED', True)
    monkeypatch.setattr(api_server, 'USE_GRPC', True)
    monkeypatch.setattr(api_server, 'GrpcWorkerPool', GrpcWorkerPool, raising=False)
    for key, value in {
        'INFERENCE_SERVICE_TOKEN': token,
        'SYNTHX_AUDIO_ROOT': str(tmp_path / 'gateway'),
        'SYNTHX_AUDIO_WORKER_ADDRESS': address,
        'STATIC_WORKERS': '{"73":"' + address + '"}',
        'WORKER_CAPABILITIES': '{"73":["synthx_audio"]}',
    }.items():
        monkeypatch.setenv(key, value)
    try:
        with TestClient(api_server.app) as client:
            assert client.get('/audio/health').status_code == 401
            headers = {'Authorization': 'Bearer ' + token}
            assert client.get('/audio/health', headers=headers).json()['ready'] is True
            assert client.get('/readyz').status_code == 200
            for number, variant in enumerate(['vital_v7_3', 'vital_v7_5', 'vital_v7_5_chord'], 1):
                identity = f'{number:032x}'
                data = b'RIFF' + variant.encode() * 12000
                route = f'/audio/requests/{identity}/{variant}'
                accepted = client.post(route, content=data, headers=headers)
                assert accepted.status_code == 202
                assert accepted.json()['job_id'] == identity
                deadline = time.monotonic() + 5
                while time.monotonic() < deadline:
                    state = client.get('/audio/jobs/' + identity, headers=headers).json()
                    if state['state'] in {'completed', 'failed'}:
                        break
                    time.sleep(.01)
                assert state['state'] == 'completed', state
                result = client.get(f'/audio/jobs/{identity}/result', headers=headers).json()
                assert result['model'] == variant
                assert result['input_sha256'] == sha(data)
                for kind in ['vital', 'wav']:
                    artifact = client.get(f'/audio/jobs/{identity}/artifacts/{kind}', headers=headers)
                    assert artifact.status_code == 200
                    assert sha(artifact.content) == result[kind + '_sha256']
                assert client.post(route, content=data, headers=headers).status_code == 202
            assert model.calls == [('00000000000000000000000000000001', 'vital_v7_3'),
                                   ('00000000000000000000000000000002', 'vital_v7_5'),
                                   ('00000000000000000000000000000003', 'vital_v7_5_chord')]
            assert api_server._deployment_gate.snapshot()['active'] == 0
    finally:
        config.update(batch_size=old_batch)
        finish(running)
        model.close()
