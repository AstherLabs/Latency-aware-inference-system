"""Exercise real PostgreSQL claims, recovery, fencing, and HTTP admission.

Requires the disposable test database, explicitly supplied as
SYNTHX_TEST_DATABASE_URL, with SynthX migrations 001 and 002 applied.
"""
import asyncio
import hashlib
import json
import os
import time
import unittest
import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import patch
from concurrent import futures

import httpx
import psycopg
import grpc
from psycopg.types.json import Jsonb

from system.queue import TaskQueue


TEST_DSN = os.environ.get("SYNTHX_TEST_DATABASE_URL")


@unittest.skipUnless(TEST_DSN, "set SYNTHX_TEST_DATABASE_URL for disposable PostgreSQL")
class DurableGatewayTests(unittest.TestCase):
    def setUp(self):
        from system.job_store import JobStore
        self.project = str(uuid.uuid4())
        self.store = JobStore(TEST_DSN)
        with psycopg.connect(TEST_DSN) as conn:
            conn.execute("INSERT INTO synthx.projects(id,user_id,title,created_at) VALUES (%s,'gateway-test','test',now())", (self.project,))
        self.addCleanup(self.cleanup_rows)

    def cleanup_rows(self):
        with psycopg.connect(TEST_DSN) as conn:
            conn.execute("DELETE FROM synthx.inference_jobs WHERE project_id=%s", (self.project,))
            conn.execute("DELETE FROM synthx.projects WHERE id=%s", (self.project,))

    def job(self, deadline_seconds=3600):
        job = str(uuid.uuid4())
        spec = {"job_id": job, "project_id": self.project, "user_id": "gateway-test", "model": "v7_3_full"}
        digest = hashlib.sha256(json.dumps(spec, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()
        with psycopg.connect(TEST_DSN) as conn:
            conn.execute("""INSERT INTO synthx.inference_jobs
                (job_id,project_id,user_id,model,spec,spec_sha256,status,deadline_at)
                VALUES (%s,%s,'gateway-test','v7_3_full',%s,%s,'queued',%s)""",
                (job, self.project, Jsonb(spec), digest, datetime.now(timezone.utc) + timedelta(seconds=deadline_seconds)))
        return job

    def gateway(self, queue=None, **kwargs):
        from system.job_gateway import JobGateway
        return JobGateway(self.store, queue or TaskQueue(maxsize=2), **kwargs)

    def test_live_lease_is_not_reclaimed_and_expired_lease_gets_new_attempt(self):
        job = self.job()
        first = self.store.claim(job_id=job, lease_seconds=60)
        self.assertEqual(first["attempts"], 1)
        self.assertIsNone(self.store.claim(job_id=job, lease_seconds=60))
        with psycopg.connect(TEST_DSN) as conn:
            conn.execute("UPDATE synthx.inference_jobs SET lease_until=now()-interval '1 second' WHERE job_id=%s", (job,))
        second = self.store.claim(job_id=job, lease_seconds=60)
        self.assertNotEqual(first["attempt_id"], second["attempt_id"])
        self.assertEqual(second["attempts"], 2)
        self.assertFalse(self.store.complete(job, first["attempt_id"], result={"stale": True}))
        self.assertTrue(self.store.complete(job, second["attempt_id"], result={"valid": True}))
        self.assertEqual(self.store.get(job)["result"], {"valid": True})

    def test_locked_candidate_does_not_block_other_claims(self):
        locked, available = self.job(), self.job()
        with psycopg.connect(TEST_DSN) as conn:
            conn.execute("SELECT job_id FROM synthx.inference_jobs WHERE job_id=%s FOR UPDATE", (locked,))
            self.assertIsNone(self.store.claim(job_id=locked))
            self.assertEqual(self.store.claim(job_id=available)["job_id"], available)

    def test_deadline_expiry_fences_late_results_and_is_terminal(self):
        job = self.job()
        row = self.store.claim(job_id=job)
        with psycopg.connect(TEST_DSN) as conn:
            conn.execute("UPDATE synthx.inference_jobs SET deadline_at=now()-interval '1 second' WHERE job_id=%s", (job,))
        self.assertFalse(self.store.complete(job, row["attempt_id"], result={"late": True}))
        self.store.expire_deadlines()
        self.assertEqual(self.store.get(job)["error"]["code"], "deadline_exceeded")
        self.assertIsNone(self.store.claim(job_id=job))

    def test_gateway_uses_authoritative_spec_and_persists_completion(self):
        job = self.job()
        queue = TaskQueue(maxsize=2)
        gateway = self.gateway(queue)
        gateway.submit(job)
        task = queue.get(timeout=0)
        self.assertEqual(task.input_data, {"kind": "synthx_v73", "spec": self.store.get(job)["spec"], "attempt_id": self.store.get(job)["attempt_id"]})
        gateway.submit(job)
        self.assertTrue(queue.empty())
        task.mark_done({"job_id": job, "artifacts": {}})
        gateway.poll()
        self.assertEqual(self.store.get(job)["status"], "succeeded")

    def test_full_queue_defers_job_without_losing_original_deadline(self):
        from system.task import Task
        job = self.job()
        deadline = self.store.get(job)["deadline_at"]
        queue = TaskQueue(maxsize=1)
        queue.put(Task(input_data={}), timeout=0)
        self.gateway(queue).submit(job)
        row = self.store.get(job)
        self.assertEqual(row["status"], "queued")
        self.assertEqual(row["error"]["code"], "overloaded")
        self.assertEqual(row["deadline_at"], deadline)

    def test_recovery_discovers_jobs_without_http_poke_and_renews_live_lease(self):
        job = self.job()
        queue = TaskQueue(maxsize=2)
        gateway = self.gateway(queue, lease_seconds=60, renew_interval=0)
        gateway.poll()
        task = queue.get(timeout=0)
        self.assertEqual(task.input_data["spec"]["job_id"], job)
        before = self.store.get(job)["lease_until"]
        gateway.poll()
        self.assertGreaterEqual(self.store.get(job)["lease_until"], before)
        self.assertTrue(queue.empty())

    def test_worker_busy_retries_but_permanent_input_error_is_terminal(self):
        for code, want in (("worker_busy", "queued"), ("already_running", "queued"), ("storage_error", "queued"), ("worker_unavailable", "queued"), ("worker_error", "queued"), ("checkpoint_mismatch", "failed"), ("invalid_input", "failed"), ("no_usable_note", "failed")):
            with self.subTest(code=code):
                job = self.job()
                queue = TaskQueue(maxsize=2)
                gateway = self.gateway(queue)
                gateway.submit(job)
                queue.get(timeout=0).mark_failed("worker error", code)
                gateway.poll()
                self.assertEqual(self.store.get(job)["status"], want)
                self.assertEqual(self.store.get(job)["error"]["code"], code)

    def test_restart_does_not_resubmit_live_lease_and_fences_old_attempt(self):
        job = self.job()
        first_queue, recovery_queue = TaskQueue(maxsize=2), TaskQueue(maxsize=2)
        first, recovery = self.gateway(first_queue), self.gateway(recovery_queue)
        first.submit(job)
        old_task = first_queue.get(timeout=0)
        recovery.poll()
        self.assertTrue(recovery_queue.empty())
        with psycopg.connect(TEST_DSN) as conn:
            conn.execute("UPDATE synthx.inference_jobs SET lease_until=now()-interval '1 second' WHERE job_id=%s", (job,))
        recovery.poll()
        new_task = recovery_queue.get(timeout=0)
        self.assertNotEqual(old_task.input_data["attempt_id"], new_task.input_data["attempt_id"])
        new_task.mark_done({"recovered": True})
        recovery.poll()
        old_task.mark_done({"stale": True})
        first.poll()
        self.assertEqual(self.store.get(job)["result"], {"recovered": True})

    def test_lost_lease_prevents_waiting_old_task_execution(self):
        job = self.job()
        queue = TaskQueue(maxsize=2)
        gateway = self.gateway(queue, renew_interval=0)
        gateway.submit(job)
        with psycopg.connect(TEST_DSN) as conn:
            conn.execute("UPDATE synthx.inference_jobs SET lease_until=now()-interval '1 second' WHERE job_id=%s", (job,))
        gateway.poll()
        old_task = queue.get(timeout=0)
        self.assertEqual(old_task.error_code, "lease_lost")
        new_task = queue.get(timeout=0)
        self.assertNotEqual(old_task.input_data["attempt_id"], new_task.input_data["attempt_id"])

    def test_expired_attempt_cannot_renew_or_complete_before_recovery(self):
        job = self.job()
        row = self.store.claim(job_id=job)
        with psycopg.connect(TEST_DSN) as conn:
            conn.execute("UPDATE synthx.inference_jobs SET lease_until=now()-interval '1 second' WHERE job_id=%s", (job,))
        self.assertFalse(self.store.renew(job, row["attempt_id"], 120))
        self.assertFalse(self.store.complete(job, row["attempt_id"], result={"stale": True}))
        self.assertFalse(self.store.defer(job, row["attempt_id"], {"code": "worker_busy"}))

    def test_tampered_spec_is_failed_before_dispatch(self):
        job = self.job()
        with psycopg.connect(TEST_DSN) as conn:
            conn.execute("UPDATE synthx.inference_jobs SET spec=spec || '{\"mode\":\"mono\"}'::jsonb WHERE job_id=%s", (job,))
        queue = TaskQueue(maxsize=2)
        self.gateway(queue).submit(job)
        self.assertTrue(queue.empty())
        self.assertEqual(self.store.get(job)["error"]["code"], "invalid_spec")

    def test_http_requires_bearer_rejects_extra_fields_and_returns_durable_state(self):
        from system import api_server
        job = self.job()
        gateway = self.gateway()
        async def request():
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=api_server.app), base_url="http://test") as client:
                self.assertEqual((await client.post("/jobs", json={"job_id": job})).status_code, 401)
                headers = {"Authorization": "Bearer service-test"}
                self.assertEqual((await client.get("/jobs/" + job, headers={"Authorization": "Bearer wrong"})).status_code, 401)
                self.assertEqual((await client.post("/jobs", headers=headers, json={"job_id": "invalid"})).status_code, 422)
                self.assertEqual((await client.get("/jobs/invalid", headers=headers)).status_code, 422)
                self.assertEqual((await client.post("/jobs", headers=headers, json={"job_id": job, "spec": {}})).status_code, 422)
                response = await client.post("/jobs", headers=headers, json={"job_id": job})
                self.assertEqual(response.status_code, 202)
                self.assertEqual(response.json()["job_id"], job)
                response = await client.get("/jobs/" + job, headers=headers)
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json()["status"], "running")
                self.assertEqual((await client.get("/jobs/" + str(uuid.uuid4()), headers=headers)).status_code, 404)
        with patch.object(api_server, "_job_gateway", gateway, create=True), patch.object(api_server, "_jobs_token", "service-test", create=True):
            asyncio.run(request())

    def test_database_statement_wait_is_bounded(self):
        from system.job_store import JobStore
        store = JobStore(TEST_DSN, statement_timeout_seconds=.05)
        started = time.monotonic()
        with self.assertRaises(psycopg.errors.QueryCanceled):
            with store._connection() as conn:
                conn.execute("SELECT pg_sleep(.5)")
        self.assertLess(time.monotonic() - started, 1)

    def test_stalled_scheduler_waits_for_lease_expiry_before_recovery(self):
        job = self.job()
        queue, recovered = TaskQueue(2), TaskQueue(2)
        healthy = [True]
        gateway = self.gateway(queue, scheduler_healthy=lambda: healthy[0])
        gateway.submit(job)
        old_task = queue.get(timeout=0)
        healthy[0] = False
        gateway.poll()
        self.assertEqual(old_task.error_code, "lease_lost")
        recovery = self.gateway(recovered)
        recovery.poll()
        self.assertTrue(recovered.empty())
        with psycopg.connect(TEST_DSN) as conn:
            conn.execute("UPDATE synthx.inference_jobs SET lease_until=now()-interval '1 second' WHERE job_id=%s", (job,))
        recovery.poll()
        new_task = recovered.get(timeout=0)
        self.assertNotEqual(old_task.input_data["attempt_id"], new_task.input_data["attempt_id"])
        new_task.mark_done({"recovered": True})
        recovery.poll()
        self.assertEqual(self.store.get(job)["result"], {"recovered": True})
        old_task.mark_done({"stale": True})
        gateway.poll()
        self.assertEqual(self.store.get(job)["result"], {"recovered": True})

    def test_attempt_rpc_timeout_retries_through_sql_on_another_worker(self):
        import threading
        from system import inference_pb2, inference_pb2_grpc
        from system.grpc_worker_pool import GrpcWorkerPool
        from system.scheduler import Scheduler
        from system.config import config
        job = self.job()
        seen = []
        class Backend(inference_pb2_grpc.WorkerServiceServicer):
            def __init__(self, slow):
                self.slow = slow
            def ProcessBatch(self, request, context):
                seen.append((self.slow, json.loads(request.tasks[0].input_json)["attempt_id"]))
                if self.slow:
                    while context.is_active():
                        time.sleep(.005)
                    return inference_pb2.BatchResponse()
                return inference_pb2.BatchResponse(results=[inference_pb2.TaskResult(
                    task_id=request.tasks[0].task_id, success=True,
                    result_json=json.dumps({"recovered": True}))])
        addresses = {}
        for name, slow in (("first", True), ("second", False)):
            server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
            inference_pb2_grpc.add_WorkerServiceServicer_to_server(Backend(slow), server)
            port = server.add_insecure_port("127.0.0.1:0")
            server.start()
            self.addCleanup(lambda server=server: server.stop(0).wait())
            addresses[name] = f"127.0.0.1:{port}"
        original = config.batch_size
        config.update(batch_size=1)
        self.addCleanup(lambda: config.update(batch_size=original))
        with patch.dict(os.environ, {"STATIC_WORKERS": json.dumps(addresses)}):
            pool = GrpcWorkerPool()
            pool.start()
        self.addCleanup(pool.stop)
        queue = TaskQueue(2)
        scheduler = Scheduler(queue, pool.submit)
        scheduler.start()
        self.addCleanup(scheduler.stop)
        gateway = self.gateway(queue, rpc_timeout_seconds=.1, retry_seconds=.05,
                               poll_interval=.01, scheduler_healthy=scheduler.is_healthy)
        deadline = self.store.get(job)["deadline_at"]
        gateway.start()
        self.addCleanup(gateway.stop)
        until = time.monotonic() + 5
        while time.monotonic() < until:
            row = self.store.get(job)
            if row["status"] == "succeeded":
                break
            time.sleep(.02)
        self.assertEqual(row["result"], {"recovered": True})
        self.assertEqual(row["attempts"], 2)
        self.assertEqual(row["deadline_at"], deadline)
        self.assertEqual([item[0] for item in seen], [True, False])
        self.assertNotEqual(seen[0][1], seen[1][1])
        self.assertFalse(self.store.complete(job, seen[0][1], result={"stale": True}))

    def test_enabled_lifespan_runs_sql_queue_scheduler_grpc_and_completion(self):
        from system import api_server, inference_pb2, inference_pb2_grpc
        from system.grpc_worker_pool import GrpcWorkerPool
        from system.config import config
        job = self.job()
        class Echo(inference_pb2_grpc.WorkerServiceServicer):
            def ProcessBatch(inner_self, request, context):
                results = []
                for task in request.tasks:
                    payload = json.loads(task.input_json)
                    if payload["kind"] != "synthx_v73" or not uuid.UUID(payload["attempt_id"]):
                        context.abort(grpc.StatusCode.INVALID_ARGUMENT, "invalid wire envelope")
                    results.append(inference_pb2.TaskResult(task_id=task.task_id, success=True,
                        result_json=json.dumps({"job_id": payload["spec"]["job_id"], "from_rpc": True})))
                return inference_pb2.BatchResponse(results=results)
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
        inference_pb2_grpc.add_WorkerServiceServicer_to_server(Echo(), server)
        port = server.add_insecure_port("127.0.0.1:0")
        server.start()
        self.addCleanup(lambda: server.stop(0).wait())
        original_batch = config.batch_size
        self.addCleanup(lambda: config.update(batch_size=original_batch))
        async def request():
            async with api_server.lifespan(api_server.app):
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app=api_server.app), base_url="http://test") as client:
                    headers = {"Authorization": "Bearer service-test"}
                    self.assertEqual((await client.post("/jobs", headers=headers, json={"job_id": job})).status_code, 202)
                    until = time.monotonic() + 4
                    while time.monotonic() < until:
                        row = (await client.get("/jobs/" + job, headers=headers)).json()
                        if row["status"] == "succeeded":
                            break
                        await asyncio.sleep(.05)
                    self.assertEqual(row["result"], {"job_id": job, "from_rpc": True})
                    self.assertEqual(config.batch_size, 1)
        with patch.object(api_server, "JOBS_ENABLED", True), patch.object(api_server, "USE_GRPC", True), \
             patch.object(api_server, "GrpcWorkerPool", GrpcWorkerPool, create=True), \
             patch.dict(os.environ, {"DATABASE_URL": TEST_DSN, "INFERENCE_SERVICE_TOKEN": "service-test", "STATIC_WORKERS": json.dumps({"test": f"127.0.0.1:{port}"})}):
            asyncio.run(request())
        self.assertIsNone(api_server._job_gateway)
        self.assertIsNone(api_server._jobs_token)


if __name__ == "__main__":
    unittest.main()
