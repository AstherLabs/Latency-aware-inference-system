"""Audio persistence is a narrow staging-only exception to the mount policy."""
import copy
import json
from pathlib import Path

import pytest

from deploy.ci import deploy_platform as platform

CONTROL = '/home/ubuntu/services/.synthx/staging-platform-cicd/control'
AUDIO = '/home/ubuntu/services/.synthx/staging-platform-cicd/audio-jobs'
CA = '/opt/synthx-staging/deploy/certs/supabase-prod-ca-2021.crt'


def layout(audio=True):
    marker = {'type': 'bind', 'source': CONTROL, 'target': '/run/synthx-deploy', 'read_only': True}
    services = {name: {'networks': {'default': None}, 'security_opt': ['no-new-privileges:true'],
                       'cap_drop': ['ALL'], 'cpus': cpu, 'mem_limit': memory, 'volumes': [copy.deepcopy(marker)]}
                for name, cpu, memory in [('gateway', 1, 1073741824), ('text-worker', .5, 536870912)]}
    gateway = services['gateway']
    gateway['environment'] = {'PGSSLMODE': 'verify-full', 'PGSSLROOTCERT': '/etc/synthx-staging/supabase-prod-ca-2021.crt'}
    gateway['ports'] = [{'host_ip': '127.0.0.1', 'published': '18100', 'target': 8000}]
    gateway['volumes'].append({'type': 'bind', 'source': CA, 'target': '/etc/synthx-staging/supabase-prod-ca-2021.crt', 'read_only': True})
    if audio:
        gateway['environment'].update({'SYNTHX_AUDIO_ENABLED': '1', 'SYNTHX_AUDIO_ROOT': '/var/lib/synthx-audio',
            'SYNTHX_AUDIO_WORKER_ADDRESS': 'model-tunnel:50061',
            'STATIC_WORKERS': '{"73":"model-tunnel:50061","122":"text-worker:50062"}',
            'WORKER_CAPABILITIES': '{"73":["synthx_audio"],"122":["synthx_text_v123","synthx_text_refinement"]}'})
        gateway['volumes'].append({'type': 'bind', 'source': AUDIO, 'target': '/var/lib/synthx-audio', 'read_only': False})
    return {'name': 'synthx-staging-platform', 'services': services,
            'networks': {'default': {'external': True, 'name': 'synthx-staging_default'}}}


def test_audio_mount_is_allowed_only_with_exact_staging_opt_in():
    platform.validate_layout(layout(), Path(CONTROL), 'staging')
    platform.validate_layout(layout(False), Path(CONTROL), 'staging')


@pytest.mark.parametrize('mutation', [
    lambda c: c['services']['gateway']['environment'].pop('SYNTHX_AUDIO_ENABLED'),
    lambda c: c['services']['gateway']['environment'].update(SYNTHX_AUDIO_ENABLED='true'),
    lambda c: c['services']['gateway']['environment'].update(SYNTHX_AUDIO_ROOT='/tmp/audio'),
    lambda c: c['services']['gateway']['environment'].update(SYNTHX_AUDIO_WORKER_ADDRESS='vast-tunnel:50061'),
    lambda c: c['services']['gateway']['environment'].update(WORKER_CAPABILITIES='{"73":["synthx_v73"],"122":["synthx_text_v123","synthx_text_refinement"]}'),
    lambda c: c['services']['gateway']['environment'].update(WORKER_CAPABILITIES='{"73":["synthx_audio"],"122":["synthx_text_v123"]}'),
    lambda c: c['services']['gateway']['environment'].update(STATIC_WORKERS='{"73":"arbitrary:50061","122":"text-worker:50062"}'),
    lambda c: c['services']['gateway']['volumes'][-1].update(source='/tmp/other'),
    lambda c: c['services']['gateway']['volumes'][-1].update(target='/var/lib/other'),
    lambda c: c['services']['gateway']['volumes'][-1].update(type='volume'),
    lambda c: c['services']['gateway']['volumes'][-1].update(read_only=True),
    lambda c: c['services']['gateway']['volumes'][0].update(read_only=False),
    lambda c: c['services']['gateway']['volumes'][1].update(read_only=False),
    lambda c: c['services']['text-worker']['volumes'].append(c['services']['gateway']['volumes'].pop()),
    lambda c: c['services']['text-worker'].update(environment={'SYNTHX_AUDIO_ENABLED': '1'}),
    lambda c: c['services']['gateway'].update(cpus=2),
    lambda c: c['services']['gateway'].update(mem_limit=2147483648),
    lambda c: c['services']['gateway'].update(cap_add=['SYS_ADMIN']),
    lambda c: c['services']['gateway']['ports'][0].update(host_ip='0.0.0.0'),
])
def test_audio_does_not_broaden_mount_environment_or_resource_guards(mutation):
    value = layout()
    mutation(value)
    with pytest.raises(platform.DeployError):
        platform.validate_layout(value, Path(CONTROL), 'staging')


def runtime(tmp_path, environment='staging'):
    instance = object.__new__(platform.Runtime)
    instance.target = platform.Environment(environment)
    instance.root = instance.target.state_dir
    instance.env_file = Path('/opt/synthx-staging/.env.staging.platform')
    instance.config = {'audio_enabled': True}
    calls = []
    instance.command = lambda args, **kwargs: calls.append((args, kwargs)) or ''
    source = tmp_path / 'compose.platform.yml'
    source.write_text('services: {}')
    return instance, calls, {'compose': str(source), 'audio_enabled': True}


def test_compose_emits_fixed_overlay_and_rollback_omits_it(tmp_path):
    instance, calls, release = runtime(tmp_path)
    instance.compose(release, 'config')
    overlay = json.loads((tmp_path / 'audio.json').read_text())
    assert set(overlay['services']) == {'gateway'}
    gateway = overlay['services']['gateway']
    assert gateway['environment']['SYNTHX_AUDIO_ENABLED'] == '1'
    assert gateway['environment']['SYNTHX_AUDIO_ROOT'] == '/var/lib/synthx-audio'
    assert gateway['environment']['SYNTHX_AUDIO_WORKER_ADDRESS'] == 'model-tunnel:50061'
    assert json.loads(gateway['environment']['WORKER_CAPABILITIES']) == {'73': ['synthx_audio'], '122': ['synthx_text_v123', 'synthx_text_refinement']}
    assert gateway['volumes'] == [{'type': 'bind', 'source': AUDIO, 'target': '/var/lib/synthx-audio', 'read_only': False}]
    assert str(tmp_path / 'audio.json') in calls[-1][0]
    # Host opt-in must not alter the recorded disabled previous release.
    instance.compose({'compose': release['compose']}, 'up')
    assert str(tmp_path / 'audio.json') not in calls[-1][0]


def test_audio_cannot_be_enabled_for_dev(tmp_path):
    instance, calls, release = runtime(tmp_path, 'dev')
    with pytest.raises(platform.DeployError, match='staging'):
        instance.compose(release, 'config')
    assert calls == []


def test_real_compose_merge_preserves_ca_control_text_and_networks(tmp_path):
    import shutil
    import subprocess
    instance, calls, release = runtime(tmp_path)
    source_dir = Path(platform.__file__).parent
    for name in ('compose.platform.yml', 'compose.platform.staging.yml'):
        shutil.copyfile(source_dir / name, tmp_path / name)
    (tmp_path / 'images.json').write_text(json.dumps({'services': {name: {'image': 'synthx-platform:test'} for name in ('gateway', 'text-worker')}}))
    instance.env_file = tmp_path / '.env.platform'
    instance.env_file.write_text('SYNTHX_ENVIRONMENT=staging\n')
    # Docker CLI is the only external boundary; Compose merges the real manifests.
    def docker(*args, **kwargs):
        result = subprocess.run(['docker', *args], capture_output=True, text=True, **kwargs)
        assert result.returncode == 0, result.stderr
        return result.stdout
    instance.docker = docker
    value = json.loads(instance.compose(release, 'config', '--format', 'json'))
    platform.validate_layout(value, Path(CONTROL), 'staging')
    assert value['services']['text-worker']['environment']['INFERENCE_BACKEND'] == 'text_v1.2.3'
    assert value['services']['text-worker']['environment']['TEXT_SERVICE_URL'] == 'http://model-tunnel:18083'
    assert value['networks']['default']['name'] == 'synthx-staging_default'
    disabled = json.loads(instance.compose({'compose': release['compose']}, 'config', '--format', 'json'))
    platform.validate_layout(disabled, Path(CONTROL), 'staging')
    assert 'SYNTHX_AUDIO_ENABLED' not in disabled['services']['gateway']['environment']
    assert json.loads(disabled['services']['gateway']['environment']['WORKER_CAPABILITIES'])['73'] == ['synthx_v73']


@pytest.mark.parametrize('enabled', [False, True])
def test_prepare_records_the_opt_in_with_the_reviewed_release(tmp_path, monkeypatch, enabled):
    import shutil
    import subprocess
    from types import SimpleNamespace
    instance, calls, _ = runtime(tmp_path)
    instance.config = {'audio_enabled': enabled}
    instance.root = tmp_path / 'state'
    instance.repo = tmp_path / 'repo'
    instance.assert_platform_environment = lambda: None  # host credential boundary, covered by existing tests
    instance.assert_product_compatible = lambda: None
    instance.backend_call = lambda name: None
    sha = 'a' * 40
    monkeypatch.setattr(platform.uuid, 'uuid4', lambda: SimpleNamespace(hex='b' * 32))
    source = instance.root / 'releases' / (sha + '-' + 'b' * 12) / 'source'
    (source / 'deploy/ci').mkdir(parents=True)
    (source / '.platform-revision').write_text(sha)
    for name in ('compose.platform.yml', 'compose.platform.staging.yml'):
        shutil.copyfile(Path(platform.__file__).with_name(name), source / 'deploy/ci' / name)
    instance.env_file = tmp_path / '.env.platform'
    instance.env_file.write_text('SYNTHX_ENVIRONMENT=staging\n')
    instance.command = lambda args, **kwargs: sha if 'rev-parse' in args else ''
    def docker(*args, **kwargs):
        result = subprocess.run(['docker', *args], capture_output=True, text=True, **kwargs)
        assert result.returncode == 0, result.stderr
        return result.stdout
    instance.docker = docker
    release = instance.prepare(sha)
    assert release.get('audio_enabled', False) is enabled
    value = json.loads(instance.compose(release, 'config', '--format', 'json'))
    assert (value['services']['gateway']['environment'].get('SYNTHX_AUDIO_ENABLED') == '1') is enabled
