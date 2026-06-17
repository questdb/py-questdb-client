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
A tiny HTTP helper built on the standard library.

OIDC device flow implementation deliberately avoids a hard dependency on ``requests``/``httpx``
so that ``OidcDeviceAuth.token()`` / ``headers()`` work out of the box with no
extra installs. Only the device flow, discovery and the REST adapter use this
module; the heavier adapters (SQLAlchemy / psycopg / ingestion ``Sender``) bring
their own transports.

Standard proxy environment variables (``HTTPS_PROXY`` / ``HTTP_PROXY`` /
``NO_PROXY``) are honoured automatically by ``urllib``. A custom CA bundle can be
supplied explicitly or via ``REQUESTS_CA_BUNDLE`` / ``SSL_CERT_FILE``.
"""

from __future__ import annotations

import ipaddress
import json
import os
import ssl
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, Mapping, Optional

from ._errors import OidcConfigError, OidcNetworkError, OidcError

_DEFAULT_TIMEOUT = 30
_USER_AGENT = 'questdb-python-client (oidc-auth)'


def build_ssl_context(ca_bundle: Optional[str] = None) -> ssl.SSLContext:
    """
    Build an SSL context, honouring an explicit CA bundle or the
    ``REQUESTS_CA_BUNDLE`` / ``SSL_CERT_FILE`` environment variables
    (useful behind a corporate TLS-intercepting proxy).
    """
    ca = (
        ca_bundle
        or os.environ.get('REQUESTS_CA_BUNDLE')
        or os.environ.get('SSL_CERT_FILE'))
    if ca:
        if os.path.isdir(ca):
            return ssl.create_default_context(capath=ca)
        return ssl.create_default_context(cafile=ca)
    return ssl.create_default_context()


class HttpResponse:
    """A minimal response wrapper (status + raw body + headers)."""

    __slots__ = ('status', 'body', 'headers')

    def __init__(self, status: int, body: bytes, headers: Mapping[str, str]):
        self.status = status
        self.body = body
        self.headers = dict(headers)

    def text(self) -> str:
        return self.body.decode('utf-8', errors='replace')

    def json(self) -> Any:
        return json.loads(self.body.decode('utf-8'))

    @property
    def ok(self) -> bool:
        return 200 <= self.status < 300


def safe_urlparse(url: str) -> tuple:
    """
    ``urllib.parse.urlparse(url)`` paired with its port, but with a typed error.

    ``ParseResult.port`` raises a bare ``ValueError`` for a non-integer port
    (e.g. ``https://idp:notaport``); re-raise it as :class:`OidcConfigError` so
    a malformed endpoint URL stays within the package's error contract instead
    of escaping as a raw ``ValueError``. Returns ``(parts, port)``.
    """
    parts = urllib.parse.urlparse(url)
    try:
        return parts, parts.port
    except ValueError as e:
        raise OidcConfigError(
            f'Malformed endpoint URL {url!r}: invalid port.') from e


def _is_loopback(host: Optional[str]) -> bool:
    # Traffic to a loopback address never leaves the host, so plaintext http
    # carries no network interception risk and is always permitted.
    if not host:
        return False
    if host.lower() == 'localhost':
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _require_secure(url: str, insecure: bool) -> None:
    parts = urllib.parse.urlparse(url)
    scheme = parts.scheme.lower()
    if scheme == 'https':
        return
    if scheme == 'http':
        if _is_loopback(parts.hostname):
            return
        if insecure:
            return
    raise OidcConfigError(
        f'Refusing to use insecure URL {url!r} (scheme {scheme!r}). Use https '
        '(loopback http is always allowed for local development); pass '
        'insecure=True only to permit plaintext to a non-loopback host.')


def _opener(ctx: Optional[ssl.SSLContext]) -> urllib.request.OpenerDirector:
    # build_opener keeps the default ProxyHandler (which reads *_PROXY env
    # vars), while letting us pin our own TLS context.
    if ctx is None:
        return urllib.request.build_opener()
    return urllib.request.build_opener(urllib.request.HTTPSHandler(context=ctx))


def request(
        method: str,
        url: str,
        *,
        form: Optional[Mapping[str, Any]] = None,
        data: Optional[bytes] = None,
        headers: Optional[Mapping[str, str]] = None,
        timeout: float = _DEFAULT_TIMEOUT,
        ctx: Optional[ssl.SSLContext] = None,
        insecure: bool = False) -> HttpResponse:
    """
    Perform a single HTTP request.

    ``form`` is form-url-encoded into the body (``application/x-www-form-
    urlencoded``). HTTP error statuses (``4xx``/``5xx``) are returned as an
    :class:`HttpResponse` rather than raised, so callers can inspect OAuth
    error bodies (e.g. ``authorization_pending``). Only genuine network
    failures raise (:class:`OidcNetworkError`).
    """
    _require_secure(url, insecure)
    body: Optional[bytes] = data
    req_headers = {'User-Agent': _USER_AGENT, 'Accept': 'application/json'}
    if form is not None:
        body = urllib.parse.urlencode(
            {k: v for k, v in form.items() if v is not None}).encode('utf-8')
        req_headers['Content-Type'] = 'application/x-www-form-urlencoded'
    if headers:
        req_headers.update(headers)

    req = urllib.request.Request(
        url, data=body, headers=req_headers, method=method.upper())
    try:
        with _opener(ctx).open(req, timeout=timeout) as resp:
            return HttpResponse(
                getattr(resp, 'status', resp.getcode()),
                resp.read(),
                resp.headers)
    except urllib.error.HTTPError as e:
        # 4xx/5xx still carry a (possibly JSON) body we want to inspect.
        # Map a mid-body read failure to a network error (rather than letting a
        # bare OSError escape) and close the error response so its socket isn't
        # leaked (the poll loop drives many 400s during a long sign-in).
        try:
            body = e.read()
        except (TimeoutError, OSError) as read_err:
            raise OidcNetworkError(
                f'Failed to read response from {url}: {read_err}') from read_err
        finally:
            e.close()
        return HttpResponse(e.code, body, e.headers or {})
    except urllib.error.URLError as e:
        raise OidcNetworkError(f'Failed to reach {url}: {e.reason}') from e
    except (TimeoutError, OSError) as e:
        raise OidcNetworkError(f'Failed to reach {url}: {e}') from e


def get_json(
        url: str,
        *,
        headers: Optional[Mapping[str, str]] = None,
        timeout: float = _DEFAULT_TIMEOUT,
        ctx: Optional[ssl.SSLContext] = None,
        insecure: bool = False) -> Any:
    """GET a URL and parse a JSON response, raising on non-2xx."""
    resp = request(
        'GET', url, headers=headers, timeout=timeout, ctx=ctx,
        insecure=insecure)
    if not resp.ok:
        raise OidcError(
            f'HTTP {resp.status} from {url}: {resp.text()[:200]}')
    try:
        return resp.json()
    except (ValueError, UnicodeDecodeError) as e:
        raise OidcError(f'Invalid JSON from {url}: {e}') from e


def post_form(
        url: str,
        form: Mapping[str, Any],
        *,
        headers: Optional[Mapping[str, str]] = None,
        timeout: float = _DEFAULT_TIMEOUT,
        ctx: Optional[ssl.SSLContext] = None,
        insecure: bool = False) -> tuple[int, Dict[str, Any]]:
    """
    POST a form-url-encoded body and parse the JSON response.

    Returns ``(status, parsed_json)``. Used for the device-authorization and
    token endpoints, which return JSON bodies on both success and error.
    """
    resp = request(
        'POST', url, form=form, headers=headers, timeout=timeout, ctx=ctx,
        insecure=insecure)
    try:
        parsed = resp.json()
    except (ValueError, UnicodeDecodeError):
        if resp.ok:
            raise OidcError(
                f'Expected JSON from {url}, got: {resp.text()[:200]}')
        # Non-JSON error body: surface the status + text.
        raise OidcError(f'HTTP {resp.status} from {url}: {resp.text()[:200]}')
    if not isinstance(parsed, dict):
        raise OidcError(f'Unexpected JSON shape from {url}: {parsed!r}')
    return resp.status, parsed
