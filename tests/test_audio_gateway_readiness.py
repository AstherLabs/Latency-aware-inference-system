import asyncio
import unittest
from unittest.mock import patch

from fastapi import HTTPException
from system import api_server


class AudioReadinessTests(unittest.TestCase):
    def test_local_text_worker_cannot_hide_unavailable_audio_models(self):
        class Pool:
            def has_workers(self):
                return True
        class Audio:
            def health(self):
                return {'ready': False, 'worker': {'models': []}}
        with patch.object(api_server, 'USE_GRPC', True), \
             patch.object(api_server, '_worker_pool', Pool()), \
             patch.object(api_server, 'AUDIO_ENABLED', True, create=True), \
             patch.object(api_server, '_audio_manager', Audio(), create=True):
            with self.assertRaises(HTTPException) as failure:
                asyncio.run(api_server.readiness())
            self.assertEqual(failure.exception.status_code, 503)

    def test_audio_readiness_requires_configured_manager(self):
        class Pool:
            def has_workers(self):
                return True
        with patch.object(api_server, 'USE_GRPC', True), \
             patch.object(api_server, '_worker_pool', Pool()), \
             patch.object(api_server, 'AUDIO_ENABLED', True, create=True), \
             patch.object(api_server, '_audio_manager', None, create=True):
            with self.assertRaises(HTTPException) as failure:
                asyncio.run(api_server.readiness())
            self.assertEqual(failure.exception.status_code, 503)


if __name__ == '__main__':
    unittest.main()
