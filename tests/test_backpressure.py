"""Real gRPC checks for central waiting, retry ownership, and configurable slots."""
import time
import unittest
from unittest.mock import patch
import grpc

from system.config import config
from system.queue import TaskQueue
from system.scheduler import Scheduler
from system.task import TaskStatus
from system.zookeeper_registry import WorkerInfo
from queue import Full
import test_worker_handoff as fixtures


class BackpressureTests(unittest.TestCase):
    def setUp(self):
        batching = patch.object(config, 'batch_size', 1)
        batching.start()
        self.addCleanup(batching.stop)
        self.fixture = fixtures.HandoffTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.pool = self.fixture.pool
        self.queue = TaskQueue(maxsize=8)
        self.scheduler = Scheduler(self.queue, self.pool.submit)
        self.scheduler.start()
        self.addCleanup(self.scheduler.stop)

    def wait(self, predicate, timeout=2):
        until = time.monotonic() + timeout
        while time.monotonic() < until:
            if predicate():
                return
            time.sleep(.005)
        self.fail('condition did not become true')

    def test_refinement_uses_the_shared_backpressure_dispatch_path(self):
        self.pool._capabilities = {'dead': {'simulation'}, 'live': {'synthx_text_refinement'}}
        self.pool._workers['dead'].set_routing_state('DRAINING')
        task = self.fixture.task()
        task.input_data = {'kind': 'synthx_text_refinement'}
        self.queue.put(task, timeout=0)
        self.fixture.wait_done([task])
        self.assertEqual(task.status, TaskStatus.DONE)
        self.assertIn(task.task_id, self.fixture.live.received)

    def test_retired_kind_is_rejected_by_shared_dispatch(self):
        task = self.fixture.task()
        task.input_data = {'kind': 'synthx_text_v122'}
        self.pool.try_submit([task])
        self.assertEqual(task.status, TaskStatus.FAILED)
        self.assertEqual(task.error_code, 'invalid_spec')

    def test_full_worker_waits_and_preserves_fifo_instead_of_failing(self):
        self.pool._workers['dead'].set_routing_state('DRAINING')
        self.pool._workers['live'].max_outstanding_batches = 1
        self.fixture.live.blocked = True
        tasks = [self.fixture.task() for _ in range(5)]
        for task in tasks:
            self.queue.put(task, timeout=0)
        self.assertTrue(self.fixture.live.started.wait(2))
        time.sleep(.08)
        self.assertTrue(all(t.status != TaskStatus.FAILED for t in tasks))
        self.assertGreater(self.scheduler.pending_count(), 0)
        self.fixture.live.blocked = False
        self.fixture.wait_done(tasks)
        self.assertTrue(all(t.status == TaskStatus.DONE for t in tasks))
        self.assertEqual(self.fixture.live.received, [t.task_id for t in tasks])

    def test_retry_waits_for_capacity_without_consuming_an_attempt(self):
        self.fixture.live.blocked = True
        self.pool._workers['live'].max_outstanding_batches = 1
        occupying = self.fixture.task()
        self.pool._workers['live'].try_submit([occupying])
        self.assertTrue(self.fixture.live.started.wait(2))
        self.fixture.dead.abort_code = grpc.StatusCode.UNAVAILABLE
        retry = self.fixture.task()
        original = (retry.task_id, retry.deadline)
        self.pool._workers['dead'].try_submit([retry])
        self.wait(lambda: retry.retry_pending)
        time.sleep(.05)
        self.assertEqual(retry.retry_count, 0)
        self.assertNotEqual(retry.status, TaskStatus.FAILED)
        self.assertFalse(self.pool._workers['dead'].is_ready())
        self.fixture.live.blocked = False
        self.fixture.wait_done([occupying, retry])
        self.assertEqual(retry.status, TaskStatus.DONE)
        self.assertEqual(retry.retry_count, 1)
        self.assertEqual((retry.task_id, retry.deadline), original)
        self.assertEqual(self.fixture.live.received.count(retry.task_id), 1)

    def test_missing_workers_wait_until_original_deadline(self):
        self.pool._update_workers({})
        task = self.fixture.task(.12)
        self.queue.put(task, timeout=0)
        self.wait(lambda: task.status == TaskStatus.FAILED)
        self.assertEqual(task.error_code, 'deadline_exceeded')
        self.assertEqual(task.retry_count, 0)

    def test_expired_waiting_task_never_reaches_a_worker(self):
        self.pool._workers['dead'].set_routing_state('DRAINING')
        self.pool._workers['live'].max_outstanding_batches = 1
        self.fixture.live.blocked = True
        occupying, expired = self.fixture.task(), self.fixture.task(.1)
        self.queue.put(occupying, timeout=0)
        self.queue.put(expired, timeout=0)
        self.wait(lambda: expired.status == TaskStatus.FAILED)
        self.assertEqual(expired.error_code, 'deadline_exceeded')
        self.assertNotIn(expired.task_id, self.fixture.live.received)

    def test_missing_model_does_not_block_another_compatible_kind(self):
        self.pool._capabilities = {'dead': {'synthx_v73'}, 'live': {'simulation'}}
        self.pool._workers['dead'].set_routing_state('DRAINING')
        blocked, healthy = self.fixture.task(), self.fixture.task()
        blocked.input_data = {'kind': 'synthx_v73'}
        self.queue.put(blocked, timeout=0)
        self.queue.put(healthy, timeout=0)
        self.wait(lambda: healthy.status == TaskStatus.DONE)
        self.assertNotEqual(blocked.status, TaskStatus.FAILED)
        self.assertNotIn(blocked.task_id, self.fixture.live.received)

    def test_unknown_kinds_fail_validation_instead_of_growing_backlog(self):
        self.pool._update_workers({})
        tasks = [self.fixture.task() for _ in range(8)]
        for index, task in enumerate(tasks):
            task.input_data = {'kind': f'unknown-{index}'}
            self.queue.put(task, timeout=0)
        self.wait(lambda: all(task.status == TaskStatus.FAILED for task in tasks))
        self.assertTrue(all(task.error_code == 'invalid_spec' for task in tasks))
        self.assertEqual(self.scheduler.pending_count(), 0)

    def test_transport_quarantine_recovers_without_membership_change(self):
        self.pool._workers['dead'].set_routing_state('DRAINING')
        proxy = self.pool._workers['live']
        self.fixture.live.abort_code = grpc.StatusCode.UNAVAILABLE
        task = self.fixture.task()
        with patch.object(config, 'worker_recovery_backoff_seconds', .05):
            self.queue.put(task, timeout=0)
            self.wait(lambda: not proxy.is_ready())
            self.fixture.live.abort_code = None
            self.fixture.wait_done([task])
        self.assertEqual(task.status, TaskStatus.DONE)
        self.assertTrue(proxy.is_ready())
        self.assertGreater(task.retry_count, 0)

    def test_reduced_retry_budget_finishes_instead_of_dropping_pending_task(self):
        task = self.fixture.task()
        task.retry_count = 2
        self.assertTrue(task.prepare_retry(3))
        with patch.object(config, 'max_task_retries', 1):
            self.assertTrue(self.pool._workers['live'].try_submit([task]))
        self.assertEqual(task.status, TaskStatus.FAILED)
        self.assertEqual(task.error_code, 'retry_exhausted')

    def test_default_capacity_is_three_and_configurable(self):
        self.assertEqual(config.worker_max_outstanding_batches, 3)
        with self.assertRaises(ValueError):
            config.update(worker_max_outstanding_batches=0)

    def test_no_survivor_keeps_accepted_work_until_worker_rejoins(self):
        self.fixture.live.blocked = True
        task = self.fixture.task()
        self.pool._workers['live'].try_submit([task])
        self.assertTrue(self.fixture.live.started.wait(2))
        self.pool._update_workers({})
        self.wait(lambda: task.retry_pending)
        self.assertNotEqual(task.status, TaskStatus.FAILED)
        self.assertEqual(task.retry_count, 0)
        self.fixture.live.blocked = False
        self.pool._update_workers({'live': WorkerInfo(self.fixture.live_addr, 'READY')})
        self.fixture.wait_done([task])
        self.assertEqual(task.status, TaskStatus.DONE)
        self.assertEqual(task.retry_count, 1)

    def test_retry_is_retained_even_when_new_request_queue_is_full(self):
        self.fixture.live.blocked = True
        self.pool._workers['live'].max_outstanding_batches = 1
        occupying = self.fixture.task()
        self.pool._workers['live'].try_submit([occupying])
        self.assertTrue(self.fixture.live.started.wait(2))
        self.fixture.dead.abort_code = grpc.StatusCode.UNAVAILABLE
        retry = self.fixture.task()
        self.pool._workers['dead'].try_submit([retry])
        self.wait(lambda: retry.retry_pending)
        queued = []
        for _ in range(10):
            task = self.fixture.task()
            try:
                self.queue.put(task, timeout=0)
                queued.append(task)
            except Full:
                break
        else:
            self.fail('new arrivals must remain bounded')
        self.assertNotEqual(retry.status, TaskStatus.FAILED)
        self.fixture.live.blocked = False
        self.fixture.wait_done([occupying, retry, *queued])
        self.assertEqual(retry.status, TaskStatus.DONE)
        self.assertEqual(self.fixture.live.received.count(retry.task_id), 1)


class RetryOwnershipTests(unittest.TestCase):
    def test_immediate_second_failure_keeps_retry_owned(self):
        task = fixtures.HandoffTests().task()
        callbacks = []
        scheduler = None

        def dispatch(batch):
            self.assertTrue(task.accept_retry(config.max_task_retries))
            callbacks.append(scheduler.enqueue_retry(task, 'second-worker'))
            scheduler._stop.set()
            return []

        scheduler = Scheduler(TaskQueue(), dispatch)
        self.assertTrue(scheduler.enqueue_retry(task, 'first-worker'))
        scheduler._loop()
        self.assertEqual(callbacks, [True])
        self.assertEqual(scheduler.pending_count(), 1)
        self.assertTrue(task.retry_pending)
        self.assertEqual(task.retry_count, 1)


if __name__ == '__main__':
    unittest.main()
