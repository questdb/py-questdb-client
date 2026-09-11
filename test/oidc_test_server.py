"""Deterministic loopback QuestDB discovery and OIDC device-flow fixture."""

from __future__ import annotations

import http.server
import json
import threading
import urllib.parse


class _ThreadingHTTPServer(http.server.ThreadingHTTPServer):
    daemon_threads = True


class OidcTestServer:
    """Serve QuestDB settings, an IdP device flow, and ILP/HTTP locally."""

    def __init__(
            self,
            *,
            initial_access_token='AT-initial',
            initial_expires_in=300,
            device_expires_in=600,
            refresh_token='RT-1',
            refreshed_access_token='AT-refreshed',
            refreshed_expires_in=300,
            write_statuses=(),
            device_token_response=None,
            device_token_responses=(),
            refresh_token_response=None,
            settings_config_overrides=None):
        self.initial_access_token = initial_access_token
        self.initial_expires_in = initial_expires_in
        # Device-code lifetime. A cancellation test should pass a short value:
        # if the cancellation it asserts ever regresses, sign_in() blocks for
        # this long before the assertion after it can even run, so the default
        # turns a failure into a ten-minute stall that the CI watchdog re-arms
        # past rather than a visible test failure.
        self.device_expires_in = device_expires_in
        self.refresh_token = refresh_token
        self.refreshed_access_token = refreshed_access_token
        self.refreshed_expires_in = refreshed_expires_in
        # Optional error injection for the token endpoint. Each override is a
        # ``(status, body, headers)`` tuple where ``body`` is a dict (serialized
        # as JSON), raw ``bytes`` (e.g. a non-JSON error page), or ``None``, and
        # ``headers`` is an optional dict (e.g. ``{'Retry-After': '7'}``).
        # ``device_token_response`` overrides the device-grant poll response;
        # ``refresh_token_response`` overrides the refresh-grant response.
        # ``device_token_responses`` is a sequence consumed one entry per
        # successive device-grant poll (e.g. an ``authorization_pending`` reply
        # followed by the default success), for driving the multi-poll path;
        # once it is exhausted the server falls back to ``device_token_response``
        # (or the default success).
        self.device_token_response = device_token_response
        self._device_token_responses = list(device_token_responses)
        self.refresh_token_response = refresh_token_response
        # Merged into the ``config`` object of the ``/settings`` response, so a
        # test can serve hostile / MITM'd discovery values (e.g. a client id
        # carrying control / bidi / zero-width characters).
        self.settings_config_overrides = dict(settings_config_overrides or {})
        self._write_statuses = list(write_statuses)
        self._lock = threading.Lock()
        self._requests = []
        self._server = None
        self._thread = None

    def __enter__(self):
        fixture = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, _format, *_args):
                pass

            def do_GET(self):
                fixture._handle(self)

            def do_POST(self):
                fixture._handle(self)

        self._server = _ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self._thread = threading.Thread(
            target=self._server.serve_forever, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, _exc_type, _exc, _tb):
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=5)

    @property
    def url(self):
        return f'http://127.0.0.1:{self._server.server_port}'

    @property
    def port(self):
        return self._server.server_port

    def requests(self, path=None, method=None):
        with self._lock:
            requests = list(self._requests)
        if path is not None:
            requests = [request for request in requests
                        if request['path'] == path]
        if method is not None:
            requests = [request for request in requests
                        if request['method'] == method]
        return requests

    def _record(self, handler, body):
        parsed = urllib.parse.urlsplit(handler.path)
        request = {
            'method': handler.command,
            'path': parsed.path,
            'query': urllib.parse.parse_qs(parsed.query),
            'headers': {key.lower(): value
                        for key, value in handler.headers.items()},
            'body': body,
            # Only the OAuth endpoints send form-encoded text. This fixture
            # also serves `POST /write`, and it advertises protocol versions
            # 1-3, so an ILP body arrives binary-encoded -- a float column is
            # enough. Decoding it strictly killed the handler thread with
            # UnicodeDecodeError, which the client saw as a bare
            # "Peer disconnected" with nothing recorded, and the one existing
            # /write test escaped only by sending an integer.
            'form': urllib.parse.parse_qs(
                body.decode('utf-8', 'replace'), keep_blank_values=True),
        }
        with self._lock:
            self._requests.append(request)
        return request

    def _handle(self, handler):
        length = int(handler.headers.get('Content-Length', '0'))
        body = handler.rfile.read(length) if length else b''
        request = self._record(handler, body)
        path = request['path']

        if handler.command == 'GET' and path == '/settings':
            config = {
                'acl.oidc.enabled': True,
                'acl.oidc.client.id': 'discovered-client',
                'acl.oidc.scope': 'openid offline_access',
                'acl.oidc.groups.encoded.in.token': False,
                'acl.oidc.token.endpoint': self.url + '/token',
                'acl.oidc.device.authorization.endpoint': (
                    self.url + '/device'),
                'line.proto.support.versions': [1, 2, 3],
                'ilp.proto.transports': ['tcp', 'http'],
            }
            config.update(self.settings_config_overrides)
            self._json(handler, 200, {
                'config': config,
                'preferences': {},
            })
            return

        if handler.command == 'POST' and path == '/device':
            self._json(handler, 200, {
                'device_code': 'DEV-CODE-123',
                'user_code': 'WXYZ-1234',
                'verification_uri': self.url + '/verify',
                'verification_uri_complete': (
                    self.url + '/verify?user_code=WXYZ-1234'),
                'expires_in': self.device_expires_in,
                'interval': 5,
            })
            return

        if handler.command == 'POST' and path == '/token':
            grant_type = request['form'].get('grant_type', [None])[0]
            if grant_type == 'refresh_token':
                if self.refresh_token_response is not None:
                    self._emit(handler, self.refresh_token_response)
                    return
                self._json(handler, 200, {
                    'access_token': self.refreshed_access_token,
                    'token_type': 'Bearer',
                    'expires_in': self.refreshed_expires_in,
                })
            else:
                with self._lock:
                    override = (
                        self._device_token_responses.pop(0)
                        if self._device_token_responses
                        else self.device_token_response)
                if override is not None:
                    self._emit(handler, override)
                    return
                self._json(handler, 200, {
                    'access_token': self.initial_access_token,
                    'refresh_token': self.refresh_token,
                    'token_type': 'Bearer',
                    'expires_in': self.initial_expires_in,
                    'scope': 'openid offline_access',
                })
            return

        if handler.command == 'POST' and path == '/write':
            with self._lock:
                status = (
                    self._write_statuses.pop(0)
                    if self._write_statuses else 204)
            response = b'retriable error' if status >= 400 else b''
            self._bytes(handler, status, response, 'text/plain')
            return

        self._json(handler, 404, {'error': 'not found'})

    @staticmethod
    def _emit(handler, override):
        """Send an injected ``(status, body, headers)`` token-endpoint response.

        ``body`` may be a dict (JSON), raw ``bytes`` (non-JSON), or ``None``.
        """
        status, body, headers = override
        if isinstance(body, (bytes, bytearray)):
            payload, content_type = bytes(body), 'text/plain'
        else:
            payload = json.dumps(
                body or {}, separators=(',', ':')).encode('utf-8')
            content_type = 'application/json'
        handler.send_response(status)
        handler.send_header('Content-Type', content_type)
        handler.send_header('Content-Length', str(len(payload)))
        for key, value in (headers or {}).items():
            handler.send_header(key, value)
        handler.send_header('Connection', 'close')
        handler.end_headers()
        if payload:
            handler.wfile.write(payload)

    @staticmethod
    def _json(handler, status, value):
        OidcTestServer._bytes(
            handler,
            status,
            json.dumps(value, separators=(',', ':')).encode('utf-8'),
            'application/json')

    @staticmethod
    def _bytes(handler, status, body, content_type):
        handler.send_response(status)
        handler.send_header('Content-Type', content_type)
        handler.send_header('Content-Length', str(len(body)))
        handler.send_header('Connection', 'close')
        handler.end_headers()
        if body:
            handler.wfile.write(body)
