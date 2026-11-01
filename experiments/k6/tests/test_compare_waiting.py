"""Verify benchmark snapshots capture source changes without copying unrelated files."""
import importlib.util
from pathlib import Path
import sys
import tempfile
import unittest

HERE = Path(__file__).parents[1]
sys.path.insert(0, str(HERE))
try:
    import compare_waiting
finally:
    sys.path.pop(0)


class SnapshotTests(unittest.TestCase):
    def test_snapshot_captures_changed_source_and_excludes_secrets_and_outputs(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            source=root/'source';(source/'system').mkdir(parents=True)
            (source/'system/task.py').write_text('version = 1\n')
            (source/'requirements-production.txt').write_text('fastapi\n')
            (source/'.env').write_text('not-a-real-secret')
            (source/'unrelated.wav').write_bytes(b'not runtime source')
            (source/'system/__pycache__').mkdir()
            (source/'system/__pycache__/task.pyc').write_bytes(b'cache')
            first=compare_waiting.snapshot(source,root/'first')
            (source/'system/task.py').write_text('version = 2\n')
            second=compare_waiting.snapshot(source,root/'second')
            self.assertNotEqual(first['system/task.py'],second['system/task.py'])
            self.assertEqual((root/'first/system/task.py').read_text(),'version = 1\n')
            self.assertNotIn('.env',first)
            self.assertNotIn('unrelated.wav',first)
            self.assertFalse(any('__pycache__' in path for path in first))
