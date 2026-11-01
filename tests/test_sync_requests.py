"""Completion races, bounded ASGI admission, and abandoned ephemeral requests."""
import asyncio
import importlib.util
import json
import threading
import time
import unittest
from unittest.mock import patch

from system.task import Task, TaskStatus
from system.queue import TaskQueue
from system.config import config


class CompletionTests(unittest.IsolatedAsyncioTestCase):
    async def test_background_completion_wakes_all_waiters_and_preserves_first_result(self):
        task = Task({'x': 3})
        self.assertTrue(hasattr(task, 'wait_finished'), 'Task needs asynchronous completion notification')
        waiters = [asyncio.create_task(task.wait_finished()) for _ in range(20)]
        await asyncio.sleep(0)
        worker = threading.Thread(target=lambda: task.mark_done({'prediction': 9}))
        worker.start()
        await asyncio.wait_for(asyncio.gather(*waiters), 1)
        worker.join()
        task.mark_failed('late failure')
        self.assertEqual(task.result, {'prediction': 9})
        self.assertEqual(task.status, TaskStatus.DONE)
        self.assertEqual(task._waiters, [])

    async def test_completion_before_registration_is_not_lost(self):
        task = Task({})
        self.assertTrue(hasattr(task, 'wait_finished'))
        task.mark_failed('failed', 'worker_error')
        await asyncio.wait_for(task.wait_finished(), .1)
        self.assertEqual(task.error_code, 'worker_error')

    async def test_timeout_and_cancel_remove_waiters_without_cancelling_shared_task(self):
        task = Task({})
        self.assertTrue(hasattr(task, 'wait_finished'))
        with self.assertRaises(TimeoutError):
            await task.wait_finished(timeout=.01)
        waiter = asyncio.create_task(task.wait_finished())
        await asyncio.sleep(0)
        waiter.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await waiter
        self.assertEqual(task._waiters, [])
        self.assertEqual(task.status, TaskStatus.PENDING)
        task.mark_done({})


class AdmissionTests(unittest.IsolatedAsyncioTestCase):
    async def test_capacity_rejects_before_body_read_and_health_stays_available(self):
        self.assertIsNotNone(importlib.util.find_spec('system.sync_requests'), 'Need bounded synchronous admission')
        from system.sync_requests import SyncAdmissionMiddleware
        entered, release = asyncio.Event(), asyncio.Event()
        calls = []
        async def app(scope, receive, send):
            calls.append(scope['path'])
            if scope['method'] == 'POST':
                entered.set()
                await release.wait()
            await send({'type': 'http.response.start', 'status': 200, 'headers': []})
            await send({'type': 'http.response.body', 'body': b'ok'})
        middleware = SyncAdmissionMiddleware(app)
        async def receive():
            self.fail('An overloaded request must not read its body')
        async def invoke(path, method='POST'):
            messages = []
            async def send(message): messages.append(message)
            await middleware({'type': 'http', 'method': method, 'path': path}, receive, send)
            return messages
        with patch.object(config, 'max_sync_requests', 1):
            first = asyncio.create_task(invoke('/predict'))
            await entered.wait()
            try:
                refused = await invoke('/text/tasks')
                self.assertEqual(refused[0]['status'], 503)
                self.assertIn((b'retry-after', b'1'), refused[0]['headers'])
                self.assertEqual((await invoke('/metrics', 'GET'))[0]['status'], 200)
                self.assertNotIn('/text/tasks', calls)
            finally:
                release.set()
                await first
            self.assertEqual((await invoke('/predict'))[0]['status'], 200)

    async def test_cancelled_handler_releases_capacity(self):
        self.assertIsNotNone(importlib.util.find_spec('system.sync_requests'))
        from system.sync_requests import SyncAdmissionMiddleware
        entered = asyncio.Event()
        async def app(scope, receive, send):
            entered.set()
            await asyncio.Event().wait()
        middleware = SyncAdmissionMiddleware(app)
        async def noop(*args): pass
        with patch.object(config, 'max_sync_requests', 1):
            running = asyncio.create_task(middleware({'type':'http','method':'POST','path':'/predict'}, noop, noop))
            await entered.wait()
            running.cancel()
            with self.assertRaises(asyncio.CancelledError): await running
            self.assertEqual(middleware.active, 0)


class QueueCleanupTests(unittest.TestCase):
    def test_discard_preserves_other_tasks_and_releases_capacity(self):
        queue = TaskQueue(maxsize=2)
        abandoned, kept = Task({}), Task({})
        queue.put(abandoned, timeout=0)
        queue.put(kept, timeout=0)
        self.assertTrue(hasattr(queue, 'discard'))
        queue.discard(abandoned)
        replacement = Task({})
        queue.put(replacement, timeout=0)
        self.assertEqual(queue.drain(), [kept, replacement])


class EndpointCancellationTests(unittest.IsolatedAsyncioTestCase):
    async def test_cancelled_http_request_does_not_leave_queued_work(self):
        from system import api_server
        import httpx
        queue = TaskQueue(maxsize=2)
        with patch.multiple(api_server, _task_queue=queue, create=True):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=api_server.app), base_url='http://test') as client:
                call = asyncio.create_task(client.post('/predict', json={'x':3}))
                for _ in range(100):
                    if queue.qsize(): break
                    await asyncio.sleep(.001)
                task = queue.get(timeout=0)
                self.assertIsNotNone(task)
                queue.put(task, timeout=0)
                call.cancel()
                with self.assertRaises(asyncio.CancelledError): await call
                self.assertEqual(task.status, TaskStatus.FAILED)
                self.assertEqual(task.error_code, 'cancelled')
                self.assertEqual(queue.qsize(), 0)


class ProxyCleanupTests(unittest.TestCase):
    def test_discard_queued_task_preserves_active_rpc_and_other_batch_members(self):
        import test_worker_handoff as fixtures
        fixture = fixtures.HandoffTests()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        fixture.live.blocked = True
        proxy = fixture.pool._workers['live']
        active, abandoned, kept = fixture.task(), fixture.task(), fixture.task()
        proxy.try_submit([active])
        self.assertTrue(fixture.live.started.wait(1))
        proxy.try_submit([abandoned, kept])
        self.assertTrue(hasattr(fixture.pool, 'discard_pending'))
        abandoned.mark_failed('caller cancelled', 'cancelled')
        fixture.pool.discard_pending(abandoned)
        self.assertEqual(proxy._batch_queue, [[kept]])
        self.assertTrue(proxy._processing)
        fixture.live.blocked = False
        fixture.wait_done([active, kept])
        self.assertNotIn(abandoned.task_id, fixture.live.received)


class WaitCleanupTests(unittest.IsolatedAsyncioTestCase):
    async def test_disconnect_immediately_cancels_and_purges_waiting_task(self):
        self.assertIsNotNone(importlib.util.find_spec('system.sync_requests'))
        from system.sync_requests import wait_for_task
        from fastapi import HTTPException
        queue = TaskQueue()
        task = Task({}, deadline=time.monotonic()+10)
        queue.put(task, timeout=0)
        class Request:
            async def receive(self): return {'type':'http.disconnect'}
        with self.assertRaises(HTTPException) as error:
            await wait_for_task(task, Request(), queue.discard)
        self.assertEqual(error.exception.status_code, 499)
        self.assertEqual(queue.qsize(), 0)
        self.assertEqual(task.error_code, 'cancelled')
        self.assertEqual(task._waiters, [])

    async def test_original_deadline_expires_without_a_worker_notification(self):
        self.assertIsNotNone(importlib.util.find_spec('system.sync_requests'))
        from system.sync_requests import wait_for_task
        task = Task({}, deadline=time.monotonic()+.01)
        queue = TaskQueue()
        queue.put(task, timeout=0)
        class Request:
            async def receive(self): await asyncio.Event().wait()
        await asyncio.wait_for(wait_for_task(task, Request(), queue.discard), 1)
        self.assertEqual(task.error_code, 'deadline_exceeded')
        self.assertEqual(task._waiters, [])
        self.assertEqual(queue.qsize(), 0)


class AlternateQueueCleanupTests(unittest.TestCase):
    def test_discard_cleans_latency_buffer_even_after_strategy_change(self):
        from system.scheduler import Scheduler
        queue = TaskQueue()
        tasks = [Task({'x': index}) for index in range(4)]
        for task in tasks: queue.put(task, timeout=0)
        scheduler = Scheduler(queue, lambda batch: None)
        strategy = scheduler._strategies['latency_aware']
        with patch.object(config, 'batch_size', 1):
            strategy.next_batch(queue)
        self.assertEqual(scheduler.pending_count(), 3)
        tasks[1].mark_failed('caller cancelled', 'cancelled')
        with patch.object(config, 'scheduler_strategy', 'fifo'):
            scheduler.discard_pending(tasks[1])
        self.assertEqual(scheduler.pending_count(), 2)
        self.assertNotIn(tasks[1], strategy._buffer)

    def test_local_worker_pool_releases_abandoned_batch(self):
        from system.worker import Worker, WorkerPool
        pool = WorkerPool()
        worker = Worker(1)
        pool._workers = [worker]
        abandoned, kept = Task({}), Task({})
        worker.submit([abandoned, kept])
        self.assertTrue(hasattr(pool, 'discard_pending'))
        abandoned.mark_failed('caller cancelled', 'cancelled')
        pool.discard_pending(abandoned)
        self.assertEqual(worker._batch_queue, [[kept]])

    def test_local_worker_skips_terminal_batch_without_spending_inference_time(self):
        from system.worker import Worker
        task = Task({})
        task.mark_failed('caller cancelled', 'cancelled')
        with patch.object(config, 'inference_latency_ms', 2000):
            started = time.monotonic()
            Worker(1)._process([task])
        self.assertLess(time.monotonic()-started, .2)
