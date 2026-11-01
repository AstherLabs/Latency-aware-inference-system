"""Runtime progress and durable ownership regression checks."""
import asyncio
import threading
import time
import unittest
from unittest.mock import patch
import httpx
from system.queue import TaskQueue
from system.scheduler import Scheduler
from system.task import Task, TaskStatus
from test_job_gateway_unit import MemoryStore

class RuntimeHealthTests(unittest.TestCase):
    def test_idle_scheduler_progresses_but_blocked_dispatch_goes_stale(self):
        from system.health import LoopProgress
        progress = LoopProgress(.1)
        scheduler = Scheduler(TaskQueue(2), lambda batch: None)
        scheduler.progress = progress
        scheduler.start()
        try:
            time.sleep(.22)
            self.assertTrue(scheduler.is_healthy())
        finally:
            scheduler.stop()
        self.assertFalse(scheduler.is_healthy())
        entered, release = threading.Event(), threading.Event()
        def block(batch):
            entered.set()
            release.wait(2)
        queue = TaskQueue(2)
        queue.put(Task({}), timeout=0)
        scheduler = Scheduler(queue, block)
        scheduler.progress = LoopProgress(.1)
        scheduler.start()
        try:
            self.assertTrue(entered.wait(1))
            time.sleep(.15)
            self.assertTrue(scheduler._thread.is_alive())
            self.assertFalse(scheduler.is_healthy())
        finally:
            release.set()
            scheduler.stop()

    def test_repeated_dispatch_errors_are_not_successful_loop_progress(self):
        from system.health import LoopProgress
        def fail(batch):
            raise RuntimeError("dispatch is broken")
        queue = TaskQueue(1)
        queue.put(Task({}), timeout=0)
        scheduler = Scheduler(queue, fail)
        scheduler.progress = LoopProgress(.08)
        scheduler.start()
        try:
            time.sleep(.2)
            self.assertFalse(scheduler.is_healthy())
        finally:
            scheduler.stop()

    def test_normal_capacity_wait_is_healthy_progress(self):
        from system.health import LoopProgress
        queue = TaskQueue(1)
        queue.put(Task({}), timeout=0)
        scheduler = Scheduler(queue, lambda batch: batch)
        scheduler.progress = LoopProgress(.1)
        scheduler.start()
        try:
            time.sleep(.22)
            self.assertTrue(scheduler.is_healthy())
        finally:
            scheduler.stop()

    def test_scheduler_stalling_during_sql_claim_does_not_enqueue_attempt(self):
        from system.job_gateway import JobGateway
        healthy = [True]
        class SlowClaimStore(MemoryStore):
            def claim(self, **kwargs):
                row = super().claim(**kwargs)
                healthy[0] = False
                return row
        store, queue = SlowClaimStore(), TaskQueue(1)
        gateway = JobGateway(store, queue, scheduler_healthy=lambda: healthy[0])
        gateway.submit('job')
        self.assertTrue(queue.empty())
        self.assertEqual(store.row['status'], 'running')
        self.assertFalse(gateway._active)

    def test_unhealthy_scheduler_leaves_sql_lease_and_invalidates_local_task(self):
        from system.job_gateway import JobGateway
        store, queue = MemoryStore(), TaskQueue(2)
        healthy = [True]
        gateway = JobGateway(store, queue, renew_interval=0, scheduler_healthy=lambda: healthy[0])
        gateway.submit('job')
        task = queue.get(timeout=0)
        healthy[0] = False
        with patch.object(store, 'renew', wraps=store.renew) as renew, patch.object(store, 'defer', wraps=store.defer) as defer:
            gateway.poll()
            self.assertEqual(task.status, TaskStatus.FAILED)
            self.assertEqual(task.error_code, 'lease_lost')
            renew.assert_not_called()
            defer.assert_not_called()
        self.assertEqual(store.row['status'], 'running')
        task.mark_done({'late': True})
        self.assertIsNone(task.result)
        self.assertFalse(gateway._active)

    def test_unhealthy_scheduler_does_not_claim_jobs(self):
        from system.job_gateway import JobGateway
        store, queue = MemoryStore(), TaskQueue(2)
        gateway = JobGateway(store, queue, scheduler_healthy=lambda: False)
        gateway.submit('job')
        gateway.poll()
        self.assertEqual(store.row['status'], 'queued')
        self.assertTrue(queue.empty())

    def test_rpc_timeout_defers_durable_job_without_extending_deadline(self):
        from system.job_gateway import JobGateway
        class WaitingStore(MemoryStore):
            due = True
            def claim(self, **kwargs):
                return super().claim(**kwargs) if self.due else None
            def defer(self, *args):
                self.due = False
                return super().defer(*args)
        store, queue = WaitingStore(), TaskQueue(2)
        gateway = JobGateway(store, queue, rpc_timeout_seconds=.1)
        gateway.submit('job')
        task = queue.get(timeout=0)
        deadline = store.row['deadline_at']
        self.assertEqual(task.rpc_timeout_seconds, .1)
        task.mark_failed('attempt timed out', 'rpc_timeout')
        gateway.poll()
        self.assertEqual(store.row['status'], 'queued')
        self.assertEqual(store.row['deadline_at'], deadline)

    def test_runtime_probe_checks_progress_without_waiting_for_gateway_lock(self):
        from system import api_server
        from system.job_gateway import JobGateway
        gateway = JobGateway(MemoryStore(), TaskQueue(1))
        scheduler = Scheduler(TaskQueue(1), lambda batch: None)
        async def probe():
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=api_server.app), base_url='http://test') as client:
                result = await client.get('/health/runtime')
                self.assertEqual(result.status_code, 503)
                self.assertFalse(result.json()['loops']['gateway']['healthy'])
        entered, release = threading.Event(), threading.Event()
        def hold():
            with gateway._lock:
                entered.set()
                release.wait(2)
        holder = threading.Thread(target=hold)
        holder.start()
        try:
            self.assertTrue(entered.wait(1))
            with patch.object(api_server, '_job_gateway', gateway), patch.object(api_server, '_scheduler', scheduler, create=True):
                started = time.monotonic()
                asyncio.run(probe())
                self.assertLess(time.monotonic()-started, .5)
        finally:
            release.set()
            holder.join(2)


class RpcBudgetTests(unittest.TestCase):
    def test_shared_rpc_obeys_strictest_attempt_cap(self):
        from system import inference_pb2
        from system.grpc_worker_pool import GrpcWorker
        class Stub:
            timeout = None
            def ProcessBatch(self, request, timeout):
                self.timeout = timeout
                return inference_pb2.BatchResponse(results=[inference_pb2.TaskResult(
                    task_id=t.task_id, success=True, result_json='{}') for t in request.tasks])
        worker = GrpcWorker('mixed-budget', '127.0.0.1:1', lambda *args: False)
        self.addCleanup(worker.stop)
        worker._stub = stub = Stub()
        worker._process([Task({}, deadline=time.monotonic()+10, rpc_timeout_seconds=1),
                         Task({}, deadline=time.monotonic()+10, rpc_timeout_seconds=5)])
        self.assertEqual(stub.timeout, 1)


    def test_real_grpc_attempt_cap_is_retryable_but_total_expiry_is_terminal(self):
        import grpc
        from concurrent import futures
        from system import inference_pb2, inference_pb2_grpc
        from system.grpc_worker_pool import GrpcWorker
        class Slow(inference_pb2_grpc.WorkerServiceServicer):
            def ProcessBatch(self, request, context):
                while context.is_active():
                    time.sleep(.005)
                return inference_pb2.BatchResponse()
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
        inference_pb2_grpc.add_WorkerServiceServicer_to_server(Slow(), server)
        port = server.add_insecure_port('127.0.0.1:0')
        server.start()
        self.addCleanup(lambda: server.stop(0).wait())
        worker = GrpcWorker('budget-test', f'127.0.0.1:{port}', retry_handler=lambda *args: False)
        self.addCleanup(worker.stop)
        grpc.channel_ready_future(worker._channel).result(timeout=2)
        task = Task({'kind': 'synthx_v73'}, deadline=time.monotonic()+2, rpc_timeout_seconds=.05)
        started = time.monotonic()
        worker._process([task])
        self.assertLess(time.monotonic()-started, 1)
        self.assertEqual(task.error_code, 'rpc_timeout')
        self.assertGreater(task.remaining_seconds(), 0)
        expired = Task({}, deadline=time.monotonic()+.05)
        worker._process([expired])
        self.assertEqual(expired.error_code, 'deadline_exceeded')


class GatewayHealthTests(unittest.TestCase):
    def test_blocked_gateway_is_unhealthy_even_while_its_thread_is_alive(self):
        from system.job_gateway import JobGateway
        entered, release = threading.Event(), threading.Event()
        class BlockedStore(MemoryStore):
            def expire_deadlines(self):
                entered.set()
                release.wait(2)
        gateway = JobGateway(BlockedStore(), TaskQueue(2), health_timeout_seconds=.05)
        gateway.start()
        try:
            self.assertTrue(entered.wait(1))
            time.sleep(.08)
            self.assertTrue(gateway._thread.is_alive())
            self.assertFalse(gateway.is_healthy())
        finally:
            release.set()
            gateway.stop()

    def test_invalid_timeout_values_are_rejected_before_startup(self):
        from system.job_gateway import JobGateway
        from system.health import LoopProgress
        for value in (0, -1, float('nan'), float('inf')):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    JobGateway(MemoryStore(), TaskQueue(1), rpc_timeout_seconds=value)
                with self.assertRaises(ValueError):
                    LoopProgress(value)


class MetricsIsolationTests(unittest.TestCase):
    def test_stalled_scrape_does_not_block_health_or_spawn_more_collectors(self):
        from system import api_server
        entered, release = threading.Event(), threading.Event()
        def collect():
            entered.set()
            release.wait(2)
            return b'test_metric 1\n'
        async def probe():
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=api_server.app), base_url='http://test') as client:
                started_loop = time.monotonic()
                first = asyncio.create_task(client.get('/metrics'))
                await asyncio.sleep(.02)
                self.assertLess(time.monotonic()-started_loop, .2)
                for _ in range(50):
                    if entered.is_set():
                        break
                    await asyncio.sleep(.005)
                self.assertTrue(entered.is_set())
                started = time.monotonic()
                response = await client.get('/health/runtime')
                self.assertIn(response.status_code, (200, 503))
                self.assertLess(time.monotonic()-started, .2)
                second = await client.get('/metrics')
                self.assertEqual(second.status_code, 503)
                result = await first
                self.assertEqual(result.status_code, 503)
                release.set()
                await asyncio.sleep(.03)
        with patch.object(api_server, 'generate_latest', collect):
            try:
                asyncio.run(probe())
            finally:
                release.set()

if __name__ == '__main__':
    unittest.main()
