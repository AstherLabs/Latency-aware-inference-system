#!/usr/bin/env python3
"""Restricted, serialized rollout of the EC2 gateway and text adapter only."""
from __future__ import annotations
import copy
import fcntl
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.request
import uuid

STATE_BASE = Path('/home/ubuntu/services/.synthx')
SERVICES = ('gateway', 'text-worker')
MAX_ARCHIVE = 2 * 1024**3
AUDIO_SOURCE = '/home/ubuntu/services/.synthx/staging-platform-cicd/audio-jobs'
AUDIO_TARGET = '/var/lib/synthx-audio'
AUDIO_ENVIRONMENT = {
    'SYNTHX_AUDIO_ENABLED': '1', 'SYNTHX_AUDIO_ROOT': AUDIO_TARGET,
    'SYNTHX_AUDIO_WORKER_ADDRESS': 'model-tunnel:50061',
    'STATIC_WORKERS': '{"73":"model-tunnel:50061","122":"text-worker:50062"}',
    'WORKER_CAPABILITIES': '{"73":["synthx_audio"],"122":["synthx_text_v123","synthx_text_refinement"]}',
}


class DeployError(RuntimeError):
    def __init__(self, message, exit_code=1):
        super().__init__(message)
        self.exit_code = exit_code


class Environment:
    """Fixed deployment identities; SSH callers cannot supply paths or Docker targets."""
    def __init__(self, name):
        if name not in ('dev', 'staging'):
            raise DeployError('Environment must be staging or dev')
        self.name = name
        staging = name == 'staging'
        self.state_dir = STATE_BASE / ('staging-platform-cicd' if staging else 'platform-cicd')
        self.backend_state = STATE_BASE / ('staging-cicd' if staging else 'cicd')
        self.config = self.state_dir / 'config.json'
        self.backend_config = self.backend_state / 'config.json'
        self.project = 'synthx-staging-platform' if staging else 'deploy'
        self.containers = {service: self.project + '-' + service + '-1' for service in SERVICES}
        self.backend_container = 'synthx-staging-backend-1' if staging else 'deploy-backend-1'
        self.tunnel_container = 'synthx-staging-models-model-tunnel-1' if staging else 'deploy-vast-tunnel-1'
        self.network = 'synthx-staging_default' if staging else 'deploy_default'
        self.gateway_port = 18100 if staging else 8100
        self.model_host = 'model-tunnel' if staging else 'vast-tunnel'


def validate_environment(config, shared, requested=None):
    target = Environment(config.get('environment', 'dev'))
    if requested is not None and target.name != requested:
        raise DeployError('SSH command and configured environment differ')
    if shared.get('environment', 'dev') != target.name:
        raise DeployError('Platform and backend environments differ')
    expected = ((config['state_dir'], target.state_dir),
                (config['backend_config'], target.backend_config),
                (shared['state_dir'], target.backend_state))
    if any(Path(actual) != wanted for actual, wanted in expected):
        raise DeployError('Deployment paths do not belong to the selected environment')
    env_file = Path('/opt/synthx-staging/.env.staging') if target.name == 'staging' else Path(shared['repo']) / '.env.production'
    if Path(shared.get('env_file', str(Path(shared['repo']) / '.env.production'))) != env_file:
        raise DeployError('Backend environment file does not belong to the selected environment')
    return target


def parse_command(command):
    match = re.fullmatch(r'deploy-platform(-staging)? ([0-9a-f]{40}) ([0-9a-f]{64})', command)
    if not match:
        raise DeployError('Only deploy-platform[-staging] SHA ARCHIVE_SHA256 is permitted')
    suffix, sha, checksum = match.groups()
    return ('staging' if suffix else 'dev'), sha, checksum


def digest(path):
    result = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024**2), b''):
            result.update(block)
    return result.hexdigest()


def atomic_json(path, value):
    path = Path(path)
    temporary = path.with_suffix('.new')
    with temporary.open('w') as stream:
        json.dump(value, stream, indent=2)
        stream.write('\n'); stream.flush(); os.fsync(stream.fileno())
    os.chmod(temporary, 0o600)
    os.replace(temporary, path)


def validate_archive(path, sha, expected):
    if digest(path) != expected:
        raise DeployError('Image archive checksum mismatch')
    try:
        with tarfile.open(path, 'r:gz') as archive:
            item = archive.getmember('manifest.json')
            if not item.isfile() or item.size > 1024**2:
                raise ValueError('invalid manifest')
            manifest = json.load(archive.extractfile(item))
            if len(manifest) != 1 or manifest[0]['RepoTags'] != ['synthx-platform:' + sha]:
                raise ValueError('unexpected tags')
    except (ValueError, KeyError, TypeError, AttributeError, tarfile.TarError, OSError) as error:
        raise DeployError('Archive must contain exactly the expected platform image') from error


def metrics_idle(text):
    """Require all expected live metrics; absent/stale routing is never idle."""
    def key(value):
        if '{' not in value: return value
        name, labels = value[:-1].split('{', 1)
        return name + '{' + ','.join(sorted(labels.split(','))) + '}'
    values = {}
    for line in text.splitlines():
        if not line or line.startswith('#'): continue
        parts = line.split()
        if len(parts) >= 2:
            try: values[key(parts[0])] = float(parts[1])
            except ValueError: return False
    required = {'inference_api_queue_depth': 0, 'inference_registered_workers': 2}
    for worker in ('73', '122'):
        required.update({f'inference_worker_queued_batches{{worker_id="{worker}"}}': 0,
            f'inference_worker_active_rpcs{{worker_id="{worker}"}}': 0,
            f'inference_worker_routing_state{{worker_id="{worker}",state="READY"}}': 1})
    return all(key(name) in values and math.isfinite(values[key(name)]) and values[key(name)] == want
               for name, want in required.items())


def validate_layout(layout, control_dir=None, environment='dev'):
    target = Environment(environment)
    services = layout.get('services', {})
    network = layout.get('networks', {}).get('default', {})
    if set(services) != set(SERVICES) or network.get('name') != target.network or network.get('external') is not True:
        raise DeployError('Only gateway/text-worker on the selected external network are allowed')
    ports = services['gateway'].get('ports', [])
    if len(ports) != 1 or ports[0].get('host_ip') != '127.0.0.1' or str(ports[0].get('published')) != str(target.gateway_port) or ports[0].get('target') != 8000:
        raise DeployError('Gateway must retain its selected loopback port')
    if services['text-worker'].get('ports'):
        raise DeployError('Text adapter must not publish a host port')
    if target.name == 'staging' and layout.get('name') != target.project:
        raise DeployError('Staging Compose project identity changed')
    gateway_env = services['gateway'].get('environment', {})
    audio_enabled = gateway_env.get('SYNTHX_AUDIO_ENABLED') == '1'
    for name, service in services.items():
        env = service.get('environment', {})
        audio_keys = {key for key in env if key.startswith('SYNTHX_AUDIO_')}
        if audio_keys:
            if name != 'gateway' or target.name != 'staging' or not audio_enabled:
                raise DeployError('Audio configuration requires explicit staging gateway opt-in')
            if audio_keys != {'SYNTHX_AUDIO_ENABLED', 'SYNTHX_AUDIO_ROOT', 'SYNTHX_AUDIO_WORKER_ADDRESS'}:
                raise DeployError('Unexpected audio deployment configuration')
        if audio_enabled and name == 'gateway':
            for key, expected in AUDIO_ENVIRONMENT.items():
                actual = env.get(key)
                if key in {'STATIC_WORKERS', 'WORKER_CAPABILITIES'}:
                    try:
                        valid = json.loads(actual) == json.loads(expected)
                    except (TypeError, ValueError):
                        valid = False
                else:
                    valid = actual == expected
                if not valid:
                    raise DeployError('Audio gateway identity, persistence or worker capabilities changed')
        if target.name == 'staging':
            if name == 'gateway':
                env = service.get('environment', {})
                if env.get('PGSSLMODE') != 'verify-full' or env.get('PGSSLROOTCERT') != '/etc/synthx-staging/supabase-prod-ca-2021.crt':
                    raise DeployError('Staging gateway must use the approved CA and verify-full database TLS')
            if any(service.get(key) for key in ('container_name', 'cap_add', 'devices', 'pid', 'ipc', 'extra_hosts')):
                raise DeployError('Staging container identity and host isolation cannot be overridden')
            try:
                cpus, memory = float(service.get('cpus', 0)), int(service.get('mem_limit', 0))
            except (TypeError, ValueError):
                raise DeployError('Staging resource limits must be explicit')
            if not 0 < cpus <= (1 if name == 'gateway' else .5) or not 0 < memory <= (1073741824 if name == 'gateway' else 536870912):
                raise DeployError('Staging resource limits exceed the approved budget')
            if service.get('cap_drop') != ['ALL'] or 'no-new-privileges:true' not in service.get('security_opt', []):
                raise DeployError('Staging requires dropped capabilities and no privilege escalation')
        mounts = service.get('volumes', [])
        if control_dir is not None:
            expected_mounts = {(str(control_dir), '/run/synthx-deploy'): True}
            if target.name == 'staging' and name == 'gateway':
                expected_mounts[('/opt/synthx-staging/deploy/certs/supabase-prod-ca-2021.crt',
                                 '/etc/synthx-staging/supabase-prod-ca-2021.crt')] = True
                if audio_enabled:
                    expected_mounts[(AUDIO_SOURCE, AUDIO_TARGET)] = False
            if (len(mounts) != len(expected_mounts)
                    or {(m.get('source'), m.get('target')) for m in mounts} != set(expected_mounts)
                    or any(m.get('type') != 'bind' or m.get('read_only', False) is not expected_mounts.get((m.get('source'), m.get('target'))) for m in mounts)):
                raise DeployError('Only approved maintenance, CA and opt-in audio persistence mounts are allowed')
        elif mounts:
            raise DeployError('Unexpected deployment mounts')
        if set(service.get('networks', {})) != {'default'} or service.get('build') or service.get('privileged') or service.get('network_mode'):
            raise DeployError('Platform mounts, builds and network changes require separate review')


class Deployment:
    def __init__(self, runtime): self.runtime = runtime

    def run(self, sha, archive, expected):
        validate_archive(archive, sha, expected)
        r = self.runtime
        release = r.prepare(sha)
        image = r.load(archive, sha)
        images = dict.fromkeys(SERVICES, image)
        previous = r.previous()
        if release.get('audio_gateway_only') is True:
            r.configure_audio_rollout(previous)
            images['text-worker'] = previous['images']['text-worker']
        if r.busy(): raise DeployError('Active jobs; retry platform deployment later', 75)
        resume_safe = True
        transaction = False
        try:
            r.begin_transaction(previous); transaction = True
            r.begin_drain()
            r.wait_idle()
            resume_safe = False
            try:
                r.stop()
                r.start(release, images)
                if not r.healthy(images): raise DeployError('Platform health or Vast connectivity check failed')
                r.record(release, images, previous)
                resume_safe = True
            except BaseException as error:
                try:
                    r.stop(); r.start(previous, previous['images'])
                    if not r.healthy(previous['images']): raise DeployError('Previous platform is unhealthy')
                    r.restore(previous)
                    resume_safe = True
                except BaseException as rollback:
                    raise DeployError('Platform rollback failed; admission remains closed for operator recovery') from rollback
                raise DeployError('Platform deployment failed; previous images and pins restored') from error
        finally:
            if resume_safe:
                r.end_drain()
                if transaction: r.finish()
        return images


class Runtime:
    def __init__(self, config):
        self.config = config
        self.root = Path(config['state_dir'])
        self.repo = Path(config['repo'])
        self.shared_path = Path(config['backend_config'])
        self.shared = json.loads(self.shared_path.read_text())
        self.target = validate_environment(config, self.shared)
        receiver = Path(config['backend_receiver'])
        if digest(receiver) != config['backend_receiver_sha256']:
            raise DeployError('Approved backend deployment protocol changed; review compatibility')
        spec = importlib.util.spec_from_file_location('approved_backend_receiver', receiver)
        module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
        class BoundedBackend(module.Runtime):
            def command(self, args, **kwargs):
                kwargs.setdefault('timeout', 120)
                return super().command(args, **kwargs)
        self.backend = BoundedBackend(self.shared)
        self.backend_repo = Path(self.shared['repo'])
        self.env_file = Path('/opt/synthx-staging/.env.staging.platform') if self.target.name == 'staging' else self.backend_repo / '.env.production'
        self.backend_env_file = Path(self.shared.get('env_file', self.backend_repo / '.env.production'))
        self.environment_hash = self.environment_digest()
        self.backend_environment_hash = self.environment_digest(self.backend_env_file)
        self.protected = None
        self.platform_token = None
        self.marker_names = set()
        if (self.root / 'transaction.json').exists():
            raise DeployError('Unfinished platform transaction; inspect recovery record before another rollout')

    def command(self, args, **kwargs):
        kwargs.setdefault('timeout', 180)
        result = subprocess.run(args, capture_output=True, text=True, **kwargs)
        if result.returncode:
            raise DeployError(f'Command failed: {Path(args[0]).name} (exit {result.returncode})')
        return result.stdout.strip()

    def environment_digest(self, path=None):
        path = path or self.env_file
        if self.target.name == 'dev':
            return digest(path)
        # Read only the digest of the root-owned environment file, never its secrets.
        result = self.command(['sudo', '-n', 'sha256sum', str(path)])
        value = result.split()[0] if result else ''
        if not re.fullmatch('[0-9a-f]{64}', value):
            raise DeployError('Cannot verify protected staging environment file')
        return value

    def docker(self, *args, **kwargs):
        prefix = ['sudo', '-n']
        if 'env' in kwargs:
            # sudo otherwise drops these non-secret interpolation paths.
            prefix.append('--preserve-env=SYNTHX_ENV_FILE,PLATFORM_CONTROL_DIR,PLATFORM_MODEL_HOST,PLATFORM_GATEWAY_PORT,PLATFORM_NETWORK')
        return self.command([*prefix, 'docker', *args], **kwargs)
    def inspect(self, target, expression): return self.docker('inspect', '--format', expression, target)
    def backend_call(self, name):
        try: return getattr(self.backend, name)()
        except Exception as error:
            raise DeployError('Backend maintenance protocol failed: ' + str(error), getattr(error, 'exit_code', 1)) from error

    def compose(self, release, *args):
        source = Path(release['compose'])
        overrides = ['-f', str(source.with_name('compose.platform.staging.yml'))] if self.target.name == 'staging' else []
        enabled = release.get('audio_enabled', False)
        gateway_only = release.get('audio_gateway_only', False)
        if (type(enabled) is not bool or type(gateway_only) is not bool
                or ((enabled or gateway_only) and self.target.name != 'staging')
                or (gateway_only and not enabled)):
            raise DeployError('Audio deployment is an explicit staging-only boolean opt-in')
        if enabled:
            # Derive the complete reviewed overlay rather than accepting host paths.
            overlay = source.with_name('audio.json')
            atomic_json(overlay, {'services': {'gateway': {
                'environment': AUDIO_ENVIRONMENT,
                'volumes': [{'type': 'bind', 'source': AUDIO_SOURCE, 'target': AUDIO_TARGET, 'read_only': False}],
            }}})
            overrides.extend(['-f', str(overlay)])
        return self.docker('compose', '-p', self.target.project, '--project-directory', str(source.parent),
            '--env-file', '/dev/null' if self.target.name == 'staging' else str(self.env_file), '-f', str(source),
            *overrides, '-f', str(source.with_name('images.json')), *args,
            env={**os.environ, 'SYNTHX_ENV_FILE': str(self.env_file), 'PLATFORM_CONTROL_DIR': str(self.root / 'control'),
                 'PLATFORM_MODEL_HOST': self.target.model_host, 'PLATFORM_GATEWAY_PORT': str(self.target.gateway_port),
                 'PLATFORM_NETWORK': self.target.network})

    def audio_rollout_settings(self):
        enabled = self.config.get('audio_enabled', False)
        gateway_only = self.config.get('audio_gateway_only', False)
        if (type(enabled) is not bool or type(gateway_only) is not bool
                or ((enabled or gateway_only) and self.target.name != 'staging')
                or (gateway_only and not enabled)):
            raise DeployError('Audio gateway-only rollout requires staging and boolean audio opt-in')
        return enabled, gateway_only

    def audio_container_snapshot(self, name, include_process=False):
        value = json.loads(self.inspect(name, '{{json .}}'))
        if value.get('State', {}).get('Running') is not True:
            raise DeployError('Protected audio rollout container is not running')
        config = copy.deepcopy(value.get('Config', {}))
        # These identify the newly created container or Compose invocation, not its runtime contract.
        config.pop('Hostname', None)
        config.pop('Image', None)
        if 'Labels' in config:
            config['Labels'] = {k: v for k, v in (config['Labels'] or {}).items()
                                if not k.startswith('com.docker.compose.')}
        layout = {'config': config, 'host': value.get('HostConfig', {}),
                  'networks': sorted(value.get('NetworkSettings', {}).get('Networks', {})),
                  'mounts': sorted(value.get('Mounts', []), key=lambda m: m.get('Destination', ''))}
        snapshot = {'image': value['Image'], 'layout_sha256': hashlib.sha256(
            json.dumps(layout, sort_keys=True, separators=(',', ':')).encode()).hexdigest()}
        if include_process:
            snapshot.update(container_id=value['Id'], pid=value['State']['Pid'], started_at=value['State']['StartedAt'])
        return snapshot

    def gateway_readiness(self):
        with urllib.request.urlopen(f'http://127.0.0.1:{self.target.gateway_port}/readyz', timeout=5) as response:
            value = json.load(response)
        if value.get('status') != 'ready':
            raise DeployError('Gateway is not ready')
        return value

    def configure_audio_rollout(self, previous):
        if self.target.name != 'staging':
            raise DeployError('Audio gateway-only rollout is staging-only')
        self._audio_text_baseline = self.audio_container_snapshot(self.target.containers['text-worker'], include_process=True)
        if self._audio_text_baseline['image'] != previous['images']['text-worker']:
            raise DeployError('Unchanged text worker differs from its recorded image')
        self._audio_previous_layout = self.audio_container_snapshot(self.target.containers['gateway'])
        if self._audio_previous_layout['image'] != previous['images']['gateway']:
            raise DeployError('Previous gateway differs from its recorded image')
        self._audio_previous_readiness = self.gateway_readiness()
        self._audio_previous = copy.deepcopy(previous)
        self._audio_rollback = False
        self.protected[self.target.containers['text-worker']] = previous['images']['text-worker']

    def assert_audio_text_unchanged(self):
        current = self.audio_container_snapshot(self.target.containers['text-worker'], include_process=True)
        if current != self._audio_text_baseline:
            raise DeployError('Unchanged text worker image, configuration or process changed')

    def assert_platform_environment(self):
        if self.target.name != 'staging':
            return
        # Validate using the reviewed, currently running backend revision, not a
        # mutable checkout or a caller-selected Python module directory.
        current = json.loads((self.target.backend_state / 'current.json').read_text())
        sha = current.get('sha', '')
        if not isinstance(sha, str) or not re.fullmatch('[0-9a-f]{40}', sha):
            raise DeployError('Staging backend release must record an immutable revision')
        source = self.target.backend_state / 'releases' / sha / 'source'
        expected = source / 'deploy/ci/compose.backend-staging.yml'
        if (current.get('compose') != str(expected) or source.resolve() != source
                or not (source / '.synthx-revision').is_file()
                or (source / '.synthx-revision').read_text().strip() != sha):
            raise DeployError('Staging backend validator source is not its approved release')
        if self.inspect(self.target.backend_container, '{{.Image}}') != current.get('image'):
            raise DeployError('Staging backend image differs from its recorded validator release')
        output = self.command(['sudo', '-n', '/opt/synthx-staging/.venv/bin/python',
            '-m', 'backend.scripts.staging_config', '--manifest',
            '/opt/synthx-staging/.staging-secrets/target.json', '--runtime-env', str(self.backend_env_file),
            '--platform-env', str(self.env_file)], cwd=source)
        try:
            report = json.loads(output)
            if report.get('status') != 'passed' or report.get('platform_checked') is not True:
                raise ValueError
        except (ValueError, AttributeError):
            raise DeployError('Staging platform credentials did not pass backend target validation') from None

    def assert_product_compatible(self):
        environment = dict(item.split('=', 1) for item in json.loads(
            self.inspect(self.target.backend_container, '{{json .Config.Env}}')))
        if environment.get('SYNTHFLOW_TEXT_PIPELINE') != 'text_v1.2.3':
            raise DeployError('Strict text platform cannot resume a backend sending retired requests; coordinated v1.2.3 cutover is required')

    def prepare(self, sha):
        self.assert_platform_environment()
        self.assert_product_compatible()
        enabled, gateway_only = self.audio_rollout_settings()
        self.command(['git', '-C', str(self.repo), 'fetch', '--no-tags', 'origin', 'main'])
        if self.command(['git', '-C', str(self.repo), 'rev-parse', 'origin/main']) != sha:
            raise DeployError('Deployment SHA is not latest origin/main')
        self.backend_call('assert_dependencies')
        release = self.root / 'releases' / (sha + '-' + uuid.uuid4().hex[:12])
        source = release / 'source'
        if not source.exists():
            release.mkdir(parents=True, exist_ok=True)
            with tempfile.TemporaryDirectory(dir=release) as temporary:
                archive = Path(temporary) / 'source.tar'
                with archive.open('wb') as stream:
                    subprocess.run(['git', '-C', str(self.repo), 'archive', sha, 'deploy'], stdout=stream, check=True, timeout=60)
                tree = Path(temporary) / 'tree'; tree.mkdir()
                with tarfile.open(archive) as contents: contents.extractall(tree, filter='data')
                (tree / '.platform-revision').write_text(sha)
                os.replace(tree, source)
        if (source / '.platform-revision').read_text().strip() != sha:
            raise DeployError('Incomplete release source export')
        result = {'sha': sha, 'compose': str(source / 'deploy/ci/compose.platform.yml')}
        if enabled:
            result['audio_enabled'] = True
        if gateway_only:
            result['audio_gateway_only'] = True
        atomic_json(Path(result['compose']).with_name('images.json'), {'services': {s: {'image': 'synthx-platform:' + sha} for s in SERVICES}})
        validate_layout(json.loads(self.compose(result, 'config', '--format', 'json')), self.root / 'control', self.target.name)
        return result

    def load(self, archive, sha):
        with Path(archive).open('rb') as stream:
            result = subprocess.run(['sudo', '-n', 'docker', 'load'], stdin=stream, capture_output=True, timeout=300)
        if result.returncode: raise DeployError('Image load failed')
        tag = 'synthx-platform:' + sha
        labels = json.loads(self.inspect(tag, '{{json .Config.Labels}}')) or {}
        if labels.get('org.opencontainers.image.revision') != sha or self.inspect(tag, '{{.Architecture}}') != 'amd64':
            raise DeployError('Image revision/architecture differs from tested release')
        return self.inspect(tag, '{{.Id}}')

    def previous(self):
        previous = json.loads((self.root / 'current.json').read_text())
        if previous.get('bootstrap_required'):
            raise DeployError('First maintenance-protocol bootstrap requires separate operator approval')
        if any(self.inspect(self.target.containers[s], '{{.Image}}') != previous['images'][s] for s in SERVICES):
            raise DeployError('Platform changed outside CI; explicitly adopt it before deployment')
        backend_state = json.loads((Path(self.shared['state_dir']) / 'current.json').read_text())
        self.protected = {self.target.backend_container: backend_state['image'],
                          self.target.tunnel_container: self.shared['dependency_images'][self.target.tunnel_container]}
        self.assert_protected()
        return previous

    def assert_protected(self):
        if self.environment_digest() != self.environment_hash or self.environment_digest(self.backend_env_file) != self.backend_environment_hash:
            raise DeployError('Selected environment file changed during deployment')
        for container, image in self.protected.items():
            if self.inspect(container, '{{.Image}}') != image or self.inspect(container, '{{.State.Running}}') != 'true':
                raise DeployError('Protected dependency changed or stopped: ' + container)
        if getattr(self, '_audio_previous', None):
            self.assert_audio_text_unchanged()

    def busy(self): return self.backend_call('busy')
    def begin_drain(self):
        self.platform_token = uuid.uuid4().hex
        self.backend_call('begin_drain')

    def fence(self, name):
        """Publish a complete token exclusively; retain ownership even on interruption."""
        marker = self.root / 'control' / name
        self.marker_names.add(name)
        fd, temporary = tempfile.mkstemp(dir=marker.parent, prefix='.drain-')
        temporary = Path(temporary)
        try:
            with os.fdopen(fd, 'w') as stream:
                stream.write(self.platform_token)
                stream.flush(); os.fsync(stream.fileno())
            os.chmod(temporary, 0o644)  # container processes only need to read
            os.link(temporary, marker)  # exclusive, atomic publication; never replace another owner
        finally:
            temporary.unlink(missing_ok=True)

    def end_drain(self):
        # Validate every owner before opening any boundary. A foreign marker needs inspection.
        for name in self.marker_names:
            marker = self.root / 'control' / name
            if marker.exists() and marker.read_text().strip() != self.platform_token:
                raise DeployError('Platform drain marker ownership changed; operator recovery required')
        for name in ('worker-drain', 'gateway-drain'):
            if name in self.marker_names:
                (self.root / 'control' / name).unlink(missing_ok=True)
        self.marker_names.clear(); self.platform_token = None
        self.backend_call('end_drain')

    def gateway_status(self):
        with urllib.request.urlopen(f'http://127.0.0.1:{self.target.gateway_port}/health/deployment', timeout=5) as response:
            return json.load(response)

    def worker_status(self):
        program = "import json,urllib.request; print(json.dumps(json.load(urllib.request.urlopen('http://127.0.0.1:16062/health/deployment',timeout=5))))"
        return json.loads(self.docker('exec',self.target.containers['text-worker'],'python','-c',program))

    def acknowledged(self, state):
        if state.get('protocol') != 1 or state.get('draining') is not True or state.get('drain_id') != self.platform_token:
            raise DeployError('Platform process did not acknowledge this maintenance window')
        return state.get('active') == 0

    def platform_status(self):
        return self.acknowledged(self.gateway_status()) and self.acknowledged(self.worker_status())

    def audio_remote_quiet(self):
        """A terminal gateway deadline does not prove remote GPU ownership ended."""
        try:
            environment = dict(item.split('=', 1) for item in json.loads(
                self.inspect(self.target.containers['gateway'], '{{json .Config.Env}}')))
            enabled = environment.get('SYNTHX_AUDIO_ENABLED', '0')
            if enabled == '0':
                return True  # The currently running legacy/rollback profile has no audio bridge.
            if enabled != '1':
                return False
            program = """import json,os,urllib.request
r=urllib.request.Request('http://127.0.0.1:8000/audio/health',headers={'Authorization':'Bearer '+os.environ['INFERENCE_SERVICE_TOKEN']})
h=json.load(urllib.request.urlopen(r,timeout=15))
w=h.get('worker',{})
assert h.get('ready') is True and w.get('ready') is True
assert type(w.get('inflight')) is int and w['inflight']==0
assert isinstance(w.get('quarantined'),list) and not w['quarantined']
print('audio-remote-idle')
"""
            return self.docker('exec', self.target.containers['gateway'], 'python', '-c', program) == 'audio-remote-idle'
        except (DeployError, OSError, ValueError, TypeError, subprocess.TimeoutExpired):
            return False

    def gateway_quiet(self):
        with urllib.request.urlopen(f'http://127.0.0.1:{self.target.gateway_port}/metrics', timeout=5) as response:
            locally_idle = metrics_idle(response.read(2 * 1024**2).decode())
        return locally_idle and self.audio_remote_quiet()

    def wait_idle(self):
        # Preserve dependencies for already accepted work: backend -> gateway -> adapter.
        self.backend_call('wait_idle')
        self.fence('gateway-drain')
        deadline = time.monotonic() + self.config.get('drain_timeout_seconds', 300)
        while True:
            state = self.backend_call('deployment_status')
            if state.get('draining') is not True or state.get('drain_id') != self.backend.drain_token:
                raise DeployError('Backend did not acknowledge this platform maintenance window')
            quiet = self.gateway_quiet()
            gateway_idle = self.acknowledged(self.gateway_status())
            if state.get('active_requests') == 0 and state.get('active_operations') == 0 and not self.busy() and quiet and gateway_idle:
                if 'worker-drain' not in self.marker_names:
                    self.fence('worker-drain')
                if self.acknowledged(self.worker_status()):
                    self.assert_protected(); return
            if time.monotonic() >= deadline: raise DeployError('Platform did not become idle; old services remain running', 75)
            time.sleep(2)

    def audio_rollout_receipt(self):
        if not getattr(self, '_audio_previous', None):
            return {}
        self.assert_audio_text_unchanged()
        return {'audio_gateway_only': True,
                'text_model_health': 'preexisting_unavailable_not_changed',
                'protected_text_worker': self._audio_text_baseline,
                'previous_gateway_readiness': self._audio_previous_readiness,
                'previous_gateway_layout': self._audio_previous_layout}

    def begin_transaction(self, previous):
        atomic_json(self.root / 'transaction.json', {'previous': previous, 'backend_config': self.shared,
            **self.audio_rollout_receipt(), 'phase': 'before-maintenance', 'started_at': time.time()})

    def stop(self):
        if getattr(self, '_audio_previous', None):
            self.assert_audio_text_unchanged()
        for service in (('gateway',) if getattr(self, '_audio_previous', None) else SERVICES):
            found = self.docker('container', 'ls', '--all', '--filter', 'name=^/' + self.target.containers[service] + '$', '--format', '{{.ID}}')
            if found: self.docker('stop', '--time', '120', found)

    def start(self, release, images):
        self.assert_platform_environment()
        self.assert_protected()
        if getattr(self, '_audio_previous', None):
            self.assert_audio_text_unchanged()
            if images['text-worker'] != self._audio_previous['images']['text-worker']:
                raise DeployError('Gateway-only rollout cannot change the text worker image')
            self._audio_rollback = release['compose'] == self._audio_previous['compose']
        atomic_json(Path(release['compose']).with_name('images.json'), {'services': {s: {'image': images[s]} for s in SERVICES}})
        validate_layout(json.loads(self.compose(release, 'config', '--format', 'json')), self.root / 'control', self.target.name)
        services = ('gateway',) if getattr(self, '_audio_previous', None) else ('text-worker', 'gateway')
        self.compose(release, 'up', '-d', '--no-deps', '--no-build', *services)
        if getattr(self, '_audio_previous', None):
            self.assert_audio_text_unchanged()

    def healthy(self, images):
        if getattr(self, '_audio_previous', None):
            return self.audio_gateway_healthy(images)
        deadline = time.monotonic() + 90
        # Probe the running release, including an older image restored on failure.
        # This is rollback verification, never API compatibility or model fallback.
        worker_env = dict(item.split('=', 1) for item in json.loads(
            self.inspect(self.target.containers['text-worker'], '{{json .Config.Env}}')))
        version = worker_env.get('INFERENCE_BACKEND')
        origin = worker_env.get('TEXT_SERVICE_URL')
        expected = {'text_v1.2.3':f'http://{self.target.model_host}:18083',
                    'text_v1.2.2':f'http://{self.target.model_host}:18080'}
        if version not in expected or origin != expected[version]:
            raise DeployError('Text worker model/origin does not match its release')
        refinement_origin = worker_env.get('TEXT_REFINEMENT_SERVICE_URL', '') if version == 'text_v1.2.3' else ''
        if version == 'text_v1.2.3' and refinement_origin != f'http://{self.target.model_host}:18080':
            raise DeployError('Independent MFCC origin is missing')
        # Read-only transport checks: these do not submit model generation work.
        program = """import grpc,json,sys,urllib.request
for address in (sys.argv[4]+':50061','text-worker:50062'):
 c=grpc.insecure_channel(address)
 try: grpc.channel_ready_future(c).result(timeout=10)
 finally: c.close()
h=json.load(urllib.request.urlopen(sys.argv[1]+'/health',timeout=10))
assert h.get('bank_available') and sys.argv[2] in h.get('supported_pipelines',[])
if sys.argv[3]:
 h=json.load(urllib.request.urlopen(sys.argv[3]+'/health',timeout=10))
 assert h.get('bank_available') and h.get('mfcc_assets_available') and 'text_v1.2_refinement' in h.get('supported_pipelines',[])
print('transport-ready')
"""
        while time.monotonic() < deadline:
            try:
                self.assert_protected()
                if not self.platform_status(): raise DeployError('Platform handlers are still active')
                if any(self.inspect(self.target.containers[s], '{{.Image}}') != images[s] or self.inspect(self.target.containers[s], '{{.State.Running}}') != 'true' for s in SERVICES):
                    raise DeployError('Platform container image/state mismatch')
                with urllib.request.urlopen(f'http://127.0.0.1:{self.target.gateway_port}/readyz', timeout=5) as response:
                    if json.load(response).get('status') != 'ready': raise DeployError('Gateway not ready')
                if self.docker('exec', self.target.containers['gateway'], 'python', '-c', program, origin, version, refinement_origin, self.target.model_host) != 'transport-ready':
                    raise DeployError('Model transport not ready')
                return True
            except (DeployError, OSError, ValueError, subprocess.TimeoutExpired): time.sleep(2)
        return False

    def audio_gateway_healthy(self, images):
        deadline = time.monotonic() + 90
        # No generation and no token output: execute authenticated health inside the gateway.
        program = """import grpc,json,os,urllib.request
c=grpc.insecure_channel('text-worker:50062')
try: grpc.channel_ready_future(c).result(timeout=10)
finally: c.close()
r=urllib.request.Request('http://127.0.0.1:8000/audio/health',headers={'Authorization':'Bearer '+os.environ['INFERENCE_SERVICE_TOKEN']})
h=json.load(urllib.request.urlopen(r,timeout=15))
expected={'vital_v7_3':'3059361d046c2c04fd40ae3d29c97e1c7d56e167f93b4b945d96aee1d4e196de','vital_v7_5':'dd9efb895f2903941ead3e5e8761f0c37f07a522e742ef04819d75e059586aa3','vital_v7_5_chord':'dd9efb895f2903941ead3e5e8761f0c37f07a522e742ef04819d75e059586aa3'}
w=h.get('worker',{})
assert h.get('ready') is True and w.get('ready') is True
assert set(w.get('models',[]))==set(expected) and w.get('checkpoints')==expected
print('audio-transport-ready')
"""
        rollback_program = """import grpc
c=grpc.insecure_channel('text-worker:50062')
try: grpc.channel_ready_future(c).result(timeout=10)
finally: c.close()
print('text-adapter-unchanged')
"""
        while time.monotonic() < deadline:
            try:
                self.assert_protected()
                self.assert_audio_text_unchanged()
                if not self.platform_status():
                    raise DeployError('Platform handlers are still active')
                if self.inspect(self.target.containers['gateway'], '{{.Image}}') != images['gateway'] or self.inspect(self.target.containers['gateway'], '{{.State.Running}}') != 'true':
                    raise DeployError('Gateway image/state mismatch')
                ready = self.gateway_readiness()
                if self._audio_rollback:
                    if ready != self._audio_previous_readiness or self.audio_container_snapshot(self.target.containers['gateway']) != self._audio_previous_layout:
                        raise DeployError('Previous gateway readiness or layout differs from its baseline')
                    expected, probe = 'text-adapter-unchanged', rollback_program
                else:
                    expected, probe = 'audio-transport-ready', program
                if self.docker('exec', self.target.containers['gateway'], 'python', '-c', probe) != expected:
                    raise DeployError('Gateway-only transport validation failed')
                self.assert_audio_text_unchanged()
                return True
            except (DeployError, OSError, ValueError, subprocess.TimeoutExpired):
                time.sleep(2)
        return False

    def record(self, release, images, previous):
        updated = copy.deepcopy(self.shared)
        updated['dependency_images'].update({self.target.containers[s]: images[s] for s in SERVICES})
        atomic_json(self.shared_path, updated)
        receipt = self.audio_rollout_receipt()
        atomic_json(self.root / 'current.json', {**release, **receipt, 'images': images,
            'previous': {k: v for k, v in previous.items() if k != 'previous'}, 'deployed_at': time.time()})

    def restore(self, previous):
        atomic_json(self.shared_path, self.shared)
        atomic_json(self.root / 'current.json', previous)

    def finish(self): (self.root / 'transaction.json').unlink(missing_ok=True)


def main():
    environment, sha, expected = parse_command(os.environ.get('SSH_ORIGINAL_COMMAND', ''))
    config = json.loads(Environment(environment).config.read_text())
    shared = json.loads(Path(config['backend_config']).read_text())
    validate_environment(config, shared, environment)
    root = Path(config['state_dir']); root.mkdir(parents=True, exist_ok=True)
    # Same host lock as Synth_X backend CI/CD, including configuration pin updates.
    with (Path(shared['state_dir']) / 'deploy.lock').open('a') as lock:
        try: fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError: raise DeployError('Another backend/platform deployment is running', 75)
        if shutil.disk_usage(root).free < 6 * 1024**3: raise DeployError('At least 6 GiB free deployment space is required')
        incoming = root / 'incoming'; incoming.mkdir(exist_ok=True)
        archive = incoming / (sha + '.tar.gz')
        partial = archive.with_suffix('.partial')
        try:
            with partial.open('wb') as stream:
                count = 0
                for block in iter(lambda: sys.stdin.buffer.read(1024**2), b''):
                    count += len(block)
                    if count > MAX_ARCHIVE: raise DeployError('Image archive exceeds 2 GiB')
                    stream.write(block)
            os.replace(partial, archive)
            Deployment(Runtime(config)).run(sha, archive, expected)
            archive.unlink()
            print(json.dumps({'deployed': True, 'sha': sha}), flush=True)
        finally: partial.unlink(missing_ok=True)


if __name__ == '__main__':
    def interrupted(signum, frame): raise DeployError('Deployment interrupted')
    signal.signal(signal.SIGTERM, interrupted); signal.signal(signal.SIGHUP, interrupted)
    try: main()
    except Exception as error:
        # Never include expanded Compose, subprocess output, or database credentials.
        message = str(error) if isinstance(error, DeployError) else type(error).__name__
        print(json.dumps({'deployed': False, 'error': message}), file=sys.stderr, flush=True)
        raise SystemExit(getattr(error, 'exit_code', 1))
