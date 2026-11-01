"""Verify readiness HTTP responses without starting discovery or worker threads."""

import asyncio
import json
import unittest
from unittest.mock import patch

from fastapi import FastAPI

from system import api_server
from system.scheduler import Scheduler
from system.queue import TaskQueue
from system.grpc_worker_pool import GrpcWorkerPool


class BusyWorker:
    def __init__(self, ready=True):
        self.ready = ready

    def is_ready(self):
        return self.ready

    def busy(self):
        return True


class ReadinessTests(unittest.TestCase):
    def response(self, pool, grpc_mode=True):
        self.assertTrue(any(getattr(route, "path", None) == "/readyz"
                            for route in api_server.app.routes))
        app = FastAPI()
        app.add_api_route("/readyz", api_server.readiness, methods=["GET"])
        messages = []

        async def receive():
            return {"type": "http.request", "body": b"", "more_body": False}

        async def send(message):
            messages.append(message)

        async def request():
            await app({
                "type": "http", "asgi": {"version": "3.0"},
                "http_version": "1.1", "method": "GET", "scheme": "http",
                "path": "/readyz", "raw_path": b"/readyz", "query_string": b"",
                "root_path": "", "headers": [],
                "server": ("test", 80), "client": ("test", 1234),
            }, receive, send)

        scheduler = Scheduler(TaskQueue(1), lambda batch: None)
        scheduler.start()
        self.addCleanup(scheduler.stop)
        with patch.object(api_server, "_scheduler", scheduler, create=True), \
                patch.object(api_server, "_job_gateway", None), \
                patch.object(api_server, "JOBS_ENABLED", False), \
                patch.object(api_server, "_worker_pool", pool), \
                patch.object(api_server, "USE_GRPC", grpc_mode):
            asyncio.run(request())
        status = next(message["status"] for message in messages
                      if message["type"] == "http.response.start")
        body = b"".join(message.get("body", b"") for message in messages
                        if message["type"] == "http.response.body")
        return status, json.loads(body)

    def pool(self, has_worker=False, stopping=False):
        pool = GrpcWorkerPool()
        if has_worker:
            pool._workers["test"] = BusyWorker()
        pool._stopping = stopping
        return pool

    def test_uninitialized_pool_returns_http_503(self):
        status, body = self.response(None)
        self.assertEqual(status, 503)
        self.assertIn("not initialized", body["detail"])

    def test_empty_membership_returns_http_503(self):
        self.assertEqual(self.response(self.pool())[0], 503)

    def test_registered_worker_returns_ready(self):
        self.assertEqual(self.response(self.pool(has_worker=True)),
                         (200, {"status": "ready"}))

    def test_stopping_pool_returns_http_503_even_with_members(self):
        self.assertEqual(self.response(self.pool(has_worker=True, stopping=True))[0], 503)

    def test_busy_worker_does_not_make_the_api_unready(self):
        pool = self.pool(has_worker=True)
        self.assertTrue(pool._workers["test"].busy())
        self.assertEqual(self.response(pool)[0], 200)

    def test_all_draining_returns_http_503(self):
        pool = self.pool()
        pool._workers["a"] = BusyWorker(ready=False)
        pool._workers["b"] = BusyWorker(ready=False)
        self.assertEqual(self.response(pool)[0], 503)

    def test_mixed_ready_and_draining_returns_http_200(self):
        pool = self.pool(has_worker=True)
        pool._workers["draining"] = BusyWorker(ready=False)
        self.assertEqual(self.response(pool)[0], 200)

    def test_local_mode_returns_http_503_without_calling_grpc_methods(self):
        self.assertEqual(self.response(object(), grpc_mode=False)[0], 503)


if __name__ == "__main__":
    unittest.main()
