"""
Run QuestDB from a published Docker image (e.g. ``questdb/questdb:nightly``)
instead of a locally-built jar.

The ``vs master`` CI leg used to ``git clone`` QuestDB and compile it from
source just to test the Python client against the tip of master. That build is
heavy (it now also compiles QuestDB's native Rust ``qdbr`` crate, which needs a
pinned nightly toolchain) and pins a JDK. The nightly Docker image is already
built from master by QuestDB's own release pipeline, so pulling it sidesteps the
JDK, Rust and Maven build entirely.

The image is a self-contained jlink runtime (no standalone ``questdb.jar``), so
the local-process ``QuestDbFixture`` cannot launch it directly. This fixture
instead runs the container and reuses ``QuestDbFixtureBase``'s HTTP query helpers
and the ``TlsProxyFixture`` so the existing test suite is unchanged.
"""

import sys
import os
import pathlib
import socket
import subprocess
import tempfile
import textwrap
import atexit
import http.client
import urllib.request
import urllib.error

PROJ_ROOT = pathlib.Path(__file__).absolute().parent.parent
sys.path.append(str(PROJ_ROOT / 'c-questdb-client' / 'system_test'))
from fixture import QuestDbFixtureBase, TlsProxyFixture, AUTH_TXT, retry


# QuestDB resolves a relative `line.tcp.auth.db.path` against its root directory,
# which is `/var/lib/questdb` inside the official image. Mount the auth db there.
_CONTAINER_ROOT = '/var/lib/questdb'
_AUTH_DB_NAME = 'auth.txt'

# Bound every docker CLI call so a wedged daemon or registry can't hang the
# run. The polling waits are bounded separately (retry timeout_sec below, plus
# a urlopen timeout on every HTTP probe).
_DOCKER_CLI_TIMEOUT = 30      # quick inspect/port/logs/rm and detached run
_DOCKER_PULL_TIMEOUT = 600    # image pull can be hundreds of MB

# Server config injected via env vars. QuestDB maps `QDB_FOO_BAR` to the config
# key `foo.bar`. These mirror the tuning the local `QuestDbFixture` writes into
# server.conf so committed rows become visible to the tests quickly.
_BASE_ENV = {
    'QDB_TELEMETRY_ENABLED': 'false',
    'QDB_HTTP_MIN_ENABLED': 'false',
    'QDB_LINE_UDP_ENABLED': 'false',
    'QDB_LINE_TCP_MAINTENANCE_JOB_INTERVAL': '100',
    'QDB_LINE_TCP_MIN_IDLE_MS_BEFORE_WRITER_RELEASE': '300',
    'QDB_CAIRO_COMMIT_LAG': '100',
    'QDB_LINE_TCP_COMMIT_INTERVAL_FRACTION': '0.1',
}


class DockerQuestDbFixture(QuestDbFixtureBase):
    def __init__(self, image, auth=False, wrap_tls=False, http=True,
                 protocol_version=None):
        self._image = image
        self.host = 'localhost'
        self.http_server_port = None
        self.line_tcp_port = None
        self.pg_port = None
        self.wrap_tls = wrap_tls
        self._tls_proxy = None
        self.tls_line_tcp_port = None
        self.auth = auth
        self.http = http
        self.protocol_version = protocol_version
        self.version = None
        self._container_id = None
        self._auth_file = None

    def print_log(self):
        if not self._container_id:
            sys.stderr.write('No QuestDB container to read logs from.\n')
            return
        try:
            logs = subprocess.run(
                ['docker', 'logs', self._container_id],
                capture_output=True, text=True, timeout=_DOCKER_CLI_TIMEOUT)
        except subprocess.TimeoutExpired:
            sys.stderr.write('Timed out reading container logs.\n')
            return
        sys.stderr.write(textwrap.indent(logs.stdout + logs.stderr, '    '))
        sys.stderr.write('\n\n')

    def start(self):
        self._ensure_image()
        run_args = ['docker', 'run', '-d',
                    '-p', '127.0.0.1::9000',
                    '-p', '127.0.0.1::9009']
        env = dict(_BASE_ENV)
        if self.http:
            env['QDB_LINE_HTTP_ENABLED'] = 'true'
        if self.auth:
            self._auth_file = self._write_auth_db()
            run_args += ['-v',
                         f'{self._auth_file}:{_CONTAINER_ROOT}/{_AUTH_DB_NAME}:ro']
            env['QDB_LINE_TCP_AUTH_DB_PATH'] = _AUTH_DB_NAME
        for key, value in env.items():
            run_args += ['-e', f'{key}={value}']
        run_args.append(self._image)

        sys.stderr.write(
            f'Starting QuestDB container from {self._image!r} '
            f'(auth: {self.auth}, http: {self.http})\n')
        self._container_id = subprocess.check_output(
            run_args, text=True, timeout=_DOCKER_CLI_TIMEOUT).strip()
        atexit.register(self.stop)

        # Tear the container (and TLS proxy) down immediately if any post-launch
        # step fails, rather than leaking it until the atexit handler runs.
        try:
            self.http_server_port = self._host_port(9000)
            self.line_tcp_port = self._host_port(9009)
            self._await_http_up()

            # Read the actual version from the running container, e.g. a
            # `9.4.3-SNAPSHOT` nightly. Drives the test suite's feature-gating.
            self.version = self.query_version()

            if self.wrap_tls:
                self._tls_proxy = TlsProxyFixture(self.line_tcp_port)
                self._tls_proxy.start()
                self.tls_line_tcp_port = self._tls_proxy.listen_port
        except BaseException:
            self.stop()
            raise

    def stop(self):
        if self._tls_proxy:
            self._tls_proxy.stop()
            self._tls_proxy = None
        if self._container_id:
            try:
                subprocess.run(
                    ['docker', 'rm', '-f', self._container_id],
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                    timeout=_DOCKER_CLI_TIMEOUT)
            except subprocess.TimeoutExpired:
                pass
            self._container_id = None
        if self._auth_file:
            try:
                os.unlink(self._auth_file)
            except OSError:
                pass
            self._auth_file = None

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, _ty, _value, _tb):
        self.stop()

    def _ensure_image(self):
        present = subprocess.run(
            ['docker', 'image', 'inspect', self._image],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            timeout=_DOCKER_CLI_TIMEOUT)
        if present.returncode != 0:
            sys.stderr.write(f'Pulling QuestDB image {self._image!r}...\n')
            subprocess.check_call(['docker', 'pull', self._image],
                                  timeout=_DOCKER_PULL_TIMEOUT)

    def _write_auth_db(self):
        fd, path = tempfile.mkstemp(prefix='qdb_auth_', suffix='.txt')
        with os.fdopen(fd, 'w', encoding='utf-8') as auth_file:
            auth_file.write(AUTH_TXT)
        # The container runs QuestDB as a non-root user; make the bind-mounted
        # db world-readable so that user can load it.
        os.chmod(path, 0o644)
        return path

    def _host_port(self, container_port):
        out = subprocess.check_output(
            ['docker', 'port', self._container_id, f'{container_port}/tcp'],
            text=True, timeout=_DOCKER_CLI_TIMEOUT).strip().splitlines()
        # e.g. '127.0.0.1:54293' -> 54293
        return int(out[0].rsplit(':', 1)[1])

    def _container_running(self):
        res = subprocess.run(
            ['docker', 'inspect', '-f', '{{.State.Running}}',
             self._container_id],
            capture_output=True, text=True, timeout=_DOCKER_CLI_TIMEOUT)
        return res.returncode == 0 and res.stdout.strip() == 'true'

    def _await_http_up(self):
        def check_http_up():
            if not self._container_running():
                raise RuntimeError('QuestDB container exited during startup.')
            req = urllib.request.Request(
                f'http://{self.host}:{self.http_server_port}/ping',
                method='GET')
            try:
                resp = urllib.request.urlopen(req, timeout=1)
                return resp.status == 204
            except (OSError, http.client.HTTPException):
                # Docker's port proxy accepts the connection before QuestDB
                # binds its HTTP listener and then resets it, so the early
                # polls see RemoteDisconnected/ConnectionReset (OSError
                # subclasses) and occasionally a partial HTTP response, as
                # well as plain connection-refused/timeout. urllib.error.URLError
                # and socket.timeout are OSError subclasses too. Keep retrying
                # until the container is serving or has exited (checked above).
                return False

        try:
            retry(check_http_up, timeout_sec=300,
                  msg='Timed out waiting for QuestDB HTTP service to come up.')
        except Exception:
            sys.stderr.write('QuestDB container log:\n')
            self.print_log()
            raise
