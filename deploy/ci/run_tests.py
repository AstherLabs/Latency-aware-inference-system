#!/usr/bin/env python3
"""Run asset-free platform CI against an explicitly disposable loopback database."""
from pathlib import Path
import os
import sys
import pytest
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[2]


def validate_test_database(url):
    parsed = urlsplit(url)
    if parsed.scheme not in {'postgres','postgresql'} or parsed.hostname not in {'127.0.0.1','localhost','::1'} or not parsed.path.startswith('/synthx_platform_ci'):
        raise ValueError('CI requires a loopback database named synthx_platform_ci*')
    return url


def initialize_database(url):
    import psycopg
    validate_test_database(url)
    with psycopg.connect(url) as connection:
        if connection.execute("SELECT to_regclass('synthx.inference_jobs')").fetchone()[0] is None:
            connection.execute((Path(__file__).with_name('schema.sql')).read_text())


class RequiredCoverage:
    def __init__(self):
        self.skipped = []

    def pytest_runtest_logreport(self, report):
        if report.skipped or hasattr(report, 'wasxfail'):
            self.skipped.append(report.nodeid)

    def pytest_collectreport(self, report):
        if report.skipped:
            self.skipped.append(report.nodeid)

    def pytest_sessionfinish(self, session, exitstatus):
        if self.skipped:
            print('Required CI checks were skipped:', self.skipped, file=sys.stderr)
            session.exitstatus = 1


def main():
    url = validate_test_database(os.environ.get('SYNTHX_TEST_DATABASE_URL',''))
    initialize_database(url)
    os.chdir(ROOT)
    sys.path[:0] = [str(ROOT),str(ROOT/'tests')]
    # Execute unittest classes AND pytest functions/fixtures, including audio recovery.
    return int(pytest.main([str(ROOT/'tests'), str(ROOT/'deploy/ci/tests'),
                            '-q', '-p', 'no:cacheprovider'], plugins=[RequiredCoverage()]))

if __name__ == '__main__': raise SystemExit(main())
