"""Exercise worker removal using real local gRPC calls and deterministic gates."""

from concurrent import futures
import json
import threading
import time
import unittest

from system.zookeeper_registry import WorkerInfo

import grpc

from system import inference_pb2, inference_pb2_grpc
from system.grpc_worker_pool import GrpcWorkerPool
from system.task import Task, TaskStatus


class Backend(inference_pb2_grpc.WorkerServiceServicer):
    def __init__(self, blocked=False):
        self.blocked = blocked
        self.started = threading.Event()
        self.received = []
        self.abort_code = None

    def ProcessBatch(self, request, context):
        self.received.extend(t.task_id for t in request.tasks)
        self.started.set()
        if self.abort_code is not None:
            context.abort(self.abort_code, "test failure")
        while self.blocked and context.is_active():
            time.sleep(.005)
        return inference_pb2.BatchResponse(results=[
            inference_pb2.TaskResult(
                task_id=t.task_id, success=True,
                result_json=json.dumps({"prediction": 9}),
            ) for t in request.tasks
        ])


class HandoffTests(unittest.TestCase):
    def setUp(self):
        self.servers = []
        self.pool = GrpcWorkerPool()
        self.dead = Backend(blocked=True)
        self.live = Backend()
        self.dead_addr = self.start_backend(self.dead)
        self.live_addr = self.start_backend(self.live)
        self.pool._update_workers({"dead": WorkerInfo(self.dead_addr, "READY"), "live": WorkerInfo(self.live_addr, "READY")})
        # Legacy handoff checks explicitly exercise the original two-slot profile.
        for worker in self.pool._workers.values():
            worker.max_outstanding_batches = 2
        self.addCleanup(self.cleanup)

    def start_backend(self, backend):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
        inference_pb2_grpc.add_WorkerServiceServicer_to_server(backend, server)
        port = server.add_insecure_port("127.0.0.1:0")
        server.start()
        self.servers.append(server)
        return f"127.0.0.1:{port}"

    def cleanup(self):
        self.pool.stop()
        for server in self.servers:
            server.stop(0).wait()

    def task(self, remaining=10):
        return Task(input_data={"x": 3}, deadline=time.monotonic()+remaining)

    def wait_done(self, tasks):
        end = time.monotonic()+2
        while time.monotonic()<end:
            if all(t.status in (TaskStatus.DONE, TaskStatus.FAILED) for t in tasks):
                return
            time.sleep(.01)
        self.fail("tasks never reached a final state")

    def test_removal_transfers_active_and_queued_tasks_without_pool_lock(self):
        active, queued = self.task(), self.task()
        worker = self.pool._workers["dead"]
        self.assertTrue(worker.try_submit([active]))
        self.assertTrue(self.dead.started.wait(2))
        self.assertTrue(worker.try_submit([queued]))
        remove = threading.Thread(
            target=self.pool._update_workers, args=({"live": WorkerInfo(self.live_addr, "READY")},), daemon=True
        )
        remove.start()
        remove.join(2)
        self.assertFalse(remove.is_alive(), "removal waits while holding Pool lock")
        self.wait_done([active, queued])
        self.assertEqual([active.status, queued.status], [TaskStatus.DONE]*2)
        self.assertEqual([active.retry_count, queued.retry_count], [1, 1])
        self.assertCountEqual(self.live.received, [active.task_id, queued.task_id])
        self.assertFalse(worker.try_submit([self.task()]))

    def test_removal_skips_expired_completed_and_retry_exhausted_tasks(self):
        active = self.task()
        worker = self.pool._workers["dead"]
        worker.try_submit([active])
        self.assertTrue(self.dead.started.wait(2))
        expired, completed, exhausted = self.task(-1), self.task(), self.task()
        completed.mark_done({"prediction": 9})
        exhausted.retry_count = 1
        worker.try_submit([expired, completed, exhausted])
        self.pool._update_workers({"live": WorkerInfo(self.live_addr, "READY")})
        self.wait_done([active, expired, completed, exhausted])
        self.assertEqual(expired.error_code, "deadline_exceeded")
        self.assertEqual(completed.status, TaskStatus.DONE)
        self.assertEqual(exhausted.status, TaskStatus.FAILED)
        self.assertEqual(self.live.received, [active.task_id])

    def test_platform_shutdown_does_not_retry(self):
        active, queued = self.task(), self.task()
        worker = self.pool._workers["dead"]
        worker.try_submit([active])
        self.assertTrue(self.dead.started.wait(2))
        worker.try_submit([queued])
        self.pool.stop()
        self.wait_done([active, queued])
        self.assertEqual([active.retry_count, queued.retry_count], [0, 0])
        self.assertEqual(self.live.received, [])

    def test_unavailable_retries_once_on_other_worker(self):
        self.dead.abort_code = grpc.StatusCode.UNAVAILABLE
        task = self.task()
        self.pool._workers["dead"].try_submit([task])
        self.wait_done([task])
        self.assertEqual(task.status, TaskStatus.DONE)
        self.assertEqual(task.retry_count, 1)
        self.assertEqual(self.live.received, [task.task_id])

    def test_unrelated_rpc_cancellation_does_not_retry(self):
        self.dead.abort_code = grpc.StatusCode.CANCELLED
        task = self.task()
        self.pool._workers["dead"].try_submit([task])
        self.wait_done([task])
        self.assertEqual(task.status, TaskStatus.FAILED)
        self.assertEqual(task.retry_count, 0)
        self.assertEqual(self.live.received, [])

    def test_removal_without_survivors_finishes_tasks(self):
        active, queued = self.task(), self.task()
        worker = self.pool._workers["dead"]
        worker.try_submit([active])
        self.assertTrue(self.dead.started.wait(2))
        worker.try_submit([queued])
        self.pool._update_workers({})
        self.wait_done([active, queued])
        self.assertEqual([active.status, queued.status], [TaskStatus.FAILED]*2)
        self.assertEqual(self.live.received, [])


if __name__ == "__main__":
    unittest.main()
