from types import SimpleNamespace

from deploy.ci import run_tests


def test_main_initializes_required_database_and_executes_both_pytest_suites(monkeypatch):
    events = []
    monkeypatch.setenv('SYNTHX_TEST_DATABASE_URL', 'postgresql://u:p@127.0.0.1/synthx_platform_ci')
    monkeypatch.setattr(run_tests, 'initialize_database', lambda url: events.append('database'))
    def execute(args, plugins):
        assert events == ['database']
        assert str(run_tests.ROOT/'tests') in args
        assert str(run_tests.ROOT/'deploy/ci/tests') in args
        assert isinstance(plugins[0], run_tests.RequiredCoverage)
        return 0
    monkeypatch.setattr(run_tests.pytest, 'main', execute)
    assert run_tests.main() == 0


def test_required_skips_and_collection_skips_fail_ci():
    coverage = run_tests.RequiredCoverage()
    coverage.pytest_runtest_logreport(SimpleNamespace(skipped=True, nodeid='audio::recovery'))
    coverage.pytest_collectreport(SimpleNamespace(skipped=True, nodeid='audio_module'))
    session = SimpleNamespace(exitstatus=0)
    coverage.pytest_sessionfinish(session, 0)
    assert session.exitstatus == 1
    assert coverage.skipped == ['audio::recovery', 'audio_module']


def test_unexpected_xfail_does_not_authorize_release():
    coverage = run_tests.RequiredCoverage()
    coverage.pytest_runtest_logreport(SimpleNamespace(skipped=False, wasxfail='known', nodeid='audio'))
    session = SimpleNamespace(exitstatus=0)
    coverage.pytest_sessionfinish(session, 0)
    assert session.exitstatus == 1
