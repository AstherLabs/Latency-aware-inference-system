import hashlib
import json
import os
from pathlib import Path
import tempfile
import time
import unittest
from concurrent import futures
from unittest.mock import patch

import grpc

from system.grpc_worker_pool import GrpcWorkerPool
from system.task import Task, TaskStatus
from system.deployment import DeploymentGate


class AudioPoolRoutingTests(unittest.TestCase):
    def test_current_models_stream_through_audio_capability_without_local_paths(self):
        received = []
        pins = {
            'vital_v7_3': '3059361d046c2c04fd40ae3d29c97e1c7d56e167f93b4b945d96aee1d4e196de',
            'vital_v7_5': 'dd9efb895f2903941ead3e5e8761f0c37f07a522e742ef04819d75e059586aa3',
            'vital_v7_5_chord': 'dd9efb895f2903941ead3e5e8761f0c37f07a522e742ef04819d75e059586aa3',
        }
        source = b'RIFF' + b'audio' * 40000

        def execute(frames, context):
            frames = iter(frames)
            header = json.loads(next(frames))
            chunks = list(frames)
            received.append((header, chunks))
            result = dict(model=header['model'], checkpoint_sha256=header['checkpoint_sha256'],
                          input_sha256=header['input_sha256'], document={'_schema': 'unified-synth-preset-v3'},
                          vital_sha256='1' * 64, wav_sha256='2' * 64)
            return json.dumps(dict(job_id=header['request_id'], model=header['model'],
                                   state='completed', result=result)).encode()

        server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
        server.add_generic_rpc_handlers([grpc.method_handlers_generic_handler(
            'synthx.audio.v1.AudioWorker', {'Execute': grpc.stream_unary_rpc_method_handler(
                execute, request_deserializer=lambda b: b, response_serializer=lambda b: b)})])
        port = server.add_insecure_port('127.0.0.1:0')
        server.start()
        try:
            with tempfile.TemporaryDirectory() as root, patch.dict(os.environ, {
                'WORKER_CAPABILITIES': '{"audio-test":["synthx_audio"]}',
            }):
                path = Path(root) / 'input.audio'
                path.write_bytes(source)
                pool = GrpcWorkerPool(static_workers={'audio-test': f'127.0.0.1:{port}'})
                pool.start()
                try:
                    for i, (model, pin) in enumerate(pins.items()):
                        task = Task(input_data={'kind': 'synthx_audio', 'model': model,
                            'request_id': f'{i + 1:032x}', 'input_path': str(path),
                            'input_sha256': hashlib.sha256(source).hexdigest(),
                            'input_size': len(source), 'checkpoint_sha256': pin},
                            deadline=time.monotonic() + 5)
                        pool.submit([task])
                        deadline = time.monotonic() + 5
                        while task.status not in (TaskStatus.DONE, TaskStatus.FAILED) and time.monotonic() < deadline:
                            time.sleep(.01)
                        self.assertEqual(task.status, TaskStatus.DONE, task.error)
                        self.assertEqual(task.result['model'], model)
                        self.assertEqual(task.result['result']['checkpoint_sha256'], pin)
                finally:
                    pool.stop()
            self.assertEqual(len(received), 3)
            for header, chunks in received:
                self.assertEqual(header['version'], 1)
                self.assertNotIn('input_path', header)
                self.assertEqual(b''.join(chunks), source)
                self.assertTrue(all(len(chunk) <= 65536 for chunk in chunks))
        finally:
            server.stop(0).wait()

    def test_recovered_admission_remains_counted_while_drain_blocks_new_jobs(self):
        with tempfile.TemporaryDirectory() as root:
            marker = Path(root) / 'drain'
            gate = DeploymentGate(marker)
            marker.write_text('deployment')
            self.assertFalse(gate.enter())
            gate.enter_existing()
            self.assertEqual(gate.snapshot()['active'], 1)
            self.assertTrue(gate.snapshot()['draining'])
            gate.leave()
            self.assertEqual(gate.snapshot()['active'], 0)


if __name__ == '__main__':
    unittest.main()
