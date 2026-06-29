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
A tiny stdlib-only HTTP helper.

Avoids a hard dependency on ``requests``/``httpx`` so ``OidcDeviceAuth.token()``
/ ``headers()`` work with no extra installs. Only the device flow, discovery and
the REST adapter use this; heavier adapters (SQLAlchemy / psycopg / ingestion
``Sender``) bring their own transports.

``urllib`` honours the standard proxy env vars (``HTTPS_PROXY`` / ``HTTP_PROXY``
/ ``NO_PROXY``); a custom CA bundle can come from ``REQUESTS_CA_BUNDLE`` /
``SSL_CERT_FILE``.
"""

from __future__ import annotations

import http.client
import ipaddress
import json
import os
import socket
import ssl
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, Mapping, Optional

from ._errors import OidcConfigError, OidcNetworkError, OidcError

_DEFAULT_TIMEOUT = 30
_USER_AGENT = 'questdb-python-client (oidc-auth)'

# Bound a response body by total size and wall-clock time. urllib's timeout is
# per-socket-read, so a server dribbling the body (a byte just inside each
# timeout window) could keep a bare read() running indefinitely, and a huge
# body would buffer unbounded into memory. OIDC / JSON responses are KBs, so
# 4 MiB is ample headroom.
_MAX_RESPONSE_BYTES = 4 * 1024 * 1024
_READ_CHUNK = 65536
_monotonic = time.monotonic


def build_ssl_context(ca_bundle: Optional[str] = None) -> ssl.SSLContext:
    """
    Build an SSL context from an explicit CA bundle or the ``REQUESTS_CA_BUNDLE``
    / ``SSL_CERT_FILE`` env vars (useful behind a TLS-intercepting proxy).
    """
    ca = (
        ca_bundle
        or os.environ.get('REQUESTS_CA_BUNDLE')
        or os.environ.get('SSL_CERT_FILE'))
    if not ca:
        return ssl.create_default_context()
    # Map the raw FileNotFoundError / ssl.SSLError from a missing/invalid bundle
    # to a typed error so a mistyped path fails clearly.
    try:
        if os.path.isdir(ca):
            return ssl.create_default_context(capath=ca)
        return ssl.create_default_context(cafile=ca)
    except (OSError, ssl.SSLError) as e:
        raise OidcConfigError(
            f'Could not load the CA bundle {ca!r}: {e}. Check the path points '
            'to a readable PEM/DER certificate file (or a directory of them).'
        ) from e


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
    ``urlparse(url)`` paired with its port, but with a typed error.

    Both ``urlparse`` (malformed IPv6 literal) and ``ParseResult.port``
    (non-integer port) raise a bare ``ValueError``; re-raise as
    :class:`OidcConfigError` to keep a malformed URL within the error contract.
    Returns ``(parts, port)``.
    """
    try:
        parts = urllib.parse.urlparse(url)
        return parts, parts.port
    except ValueError as e:
        raise OidcConfigError(
            f'Malformed endpoint URL {url!r}: {e}.') from e


def _is_loopback(host: Optional[str]) -> bool:
    # Loopback traffic never leaves the host, so plaintext http is safe here.
    if not host:
        return False
    if host.lower() == 'localhost':
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _require_secure(url: str, insecure: bool) -> None:
    # safe_urlparse maps a malformed URL to OidcConfigError, not a bare ValueError.
    parts, _ = safe_urlparse(url)
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


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """Refuse to follow HTTP redirects.

    These endpoints never legitimately redirect, and auto-following is unsafe:
    only the *original* URL is vetted (``_require_secure`` /
    ``validate_endpoint_origins`` never see the target), and urllib does not
    strip ``Authorization`` on a cross-origin redirect — so one ``302`` from
    ``/exec`` could re-send the bearer token to an attacker host, even
    downgrading to plaintext ``http``.

    Returning ``None`` surfaces the ``30x`` as an ``HTTPError`` (which
    :func:`request` turns into a non-2xx :class:`HttpResponse`), giving callers a
    clean failure.
    """

    def redirect_request(self, *args, **kwargs):
        return None


def _opener(ctx: Optional[ssl.SSLContext]) -> urllib.request.OpenerDirector:
    # build_opener keeps the default ProxyHandler (reads *_PROXY env vars) while
    # letting us pin our own TLS context and forbid redirects.
    handlers: list = [_NoRedirect()]
    if ctx is not None:
        handlers.append(urllib.request.HTTPSHandler(context=ctx))
    return urllib.request.build_opener(*handlers)


def _underlying_socket(resp: Any):
    """
    Best-effort: the raw socket behind an http.client response / ``HTTPError``.

    The deadline watchdog in :func:`_read_body` uses it to break a read that is
    blocked past the deadline. Returns ``None`` if the socket can't be located
    (a non-socket stream, or an unexpected stdlib layout) — the caller then
    relies on the between-reads deadline check alone (the pre-watchdog
    behavior), so a layout change degrades safely rather than crashing.
    """
    obj = resp
    # HTTPError -> HTTPResponse -> BufferedReader (.raw is a SocketIO) -> socket.
    for _ in range(5):
        if obj is None:
            break
        sock = (getattr(getattr(obj, 'raw', None), '_sock', None)
                or getattr(obj, '_sock', None))
        if sock is not None:
            return sock
        obj = getattr(obj, 'fp', None)
    return None


def _shutdown_socket(sock: Any) -> None:
    # Force a read blocked past the deadline to return/raise. shutdown() (not
    # close()) is what actually unblocks a thread parked in recv(); an error here
    # (socket already closed, or not connected) is irrelevant to that goal.
    try:
        sock.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass


def _read_body(resp: Any, *, max_bytes: int, deadline: float) -> bytes:
    """
    Read a response body bounded by a total byte cap and a wall-clock deadline.

    Reads in chunks so a hostile or stalled server can neither dribble the body
    past the caller's timeout (urllib's timeout is per-socket-read, not a
    whole-read bound) nor exhaust memory with an unbounded body.
    """
    # read1() returns after a SINGLE underlying socket read on a Content-Length
    # body, so the deadline check below runs between reads. But read1() alone is
    # NOT sufficient: for a *chunked* body it calls http.client's readline() to
    # parse each chunk-size line, and readline() loops over socket reads until it
    # sees a newline — so a server that dribbles the size line one byte per
    # socket-timeout window (never terminating it) keeps a single read1() blocked
    # for up to _MAXLINE (~hours), and this loop's deadline check never runs. The
    # per-leg socket timeout doesn't fire either (each dribbled byte resets it).
    # Guard that with a watchdog that shuts the socket down at the deadline: the
    # blocked read then returns/raises and is mapped to a typed OidcNetworkError
    # below, instead of hanging the calling thread (which holds the acquisition
    # lock). read1 is provided by http.client.HTTPResponse and (by delegation)
    # urllib's HTTPError; fall back to read() for any stream that lacks it.
    read = getattr(resp, 'read1', None) or resp.read
    sock = _underlying_socket(resp)
    timer = None
    if sock is not None:
        timer = threading.Timer(
            max(0.0, deadline - _monotonic()), _shutdown_socket, (sock,))
        timer.daemon = True
        timer.start()
    chunks = []
    total = 0
    try:
        while True:
            if _monotonic() > deadline:
                raise OidcNetworkError(
                    'Timed out reading the response body; the server is too '
                    'slow or is dribbling data.')
            try:
                chunk = read(_READ_CHUNK)
            except OidcNetworkError:
                raise
            except (OSError, http.client.HTTPException, ValueError) as e:
                # The watchdog shut the socket down at the deadline to break a
                # stalled read (a chunked size-line dribble surfaces here as an
                # IncompleteRead / a bad chunk size / a socket error); or a
                # genuine transport failure occurred mid-body. Either way it is a
                # network problem, not a usable response — keep the typed-error
                # contract rather than leak a raw socket/decode exception.
                if _monotonic() > deadline:
                    raise OidcNetworkError(
                        'Timed out reading the response body; the server is too '
                        'slow or is dribbling data.') from e
                raise OidcNetworkError(
                    f'Failed while reading the response body: {e}') from e
            if not chunk:
                # An empty read is a clean end-of-body — UNLESS the watchdog
                # tore the socket down at the deadline, which on a Content-Length
                # body surfaces as EOF (not an exception). Treat a post-deadline
                # EOF as the timeout it is, so a dribbled body isn't mistaken for
                # a complete one and returned silently truncated.
                if _monotonic() > deadline:
                    raise OidcNetworkError(
                        'Timed out reading the response body; the server is too '
                        'slow or is dribbling data.')
                return b''.join(chunks)
            total += len(chunk)
            if total > max_bytes:
                raise OidcNetworkError(
                    f'Response body exceeded the {max_bytes}-byte limit; '
                    'refusing to buffer an unbounded response.')
            chunks.append(chunk)
    finally:
        if timer is not None:
            timer.cancel()


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

    ``form`` is encoded into the body as ``application/x-www-form-urlencoded``.
    HTTP error statuses (``4xx``/``5xx``) are returned as an
    :class:`HttpResponse`, not raised, so callers can inspect OAuth error bodies
    (e.g. ``authorization_pending``); only genuine network failures raise
    (:class:`OidcNetworkError`).
    """
    _require_secure(url, insecure)
    body: Optional[bytes] = data
    req_headers = {'User-Agent': _USER_AGENT, 'Accept': 'application/json'}
    try:
        # Build the request INSIDE the try. urlencode(...).encode('utf-8') on a
        # form value carrying a lone surrogate — a JSON string a hostile IdP can
        # return as a device_code / refresh_token / scope, which passes the
        # isinstance(str) coercion guards — and http.client's encode of a
        # non-ASCII URL host both raise a raw UnicodeEncodeError. Previously the
        # encode and Request() ran before this try, so that escaped the
        # typed-error contract. A non-ASCII credential-endpoint authority is also
        # rejected up-front by _reject_confusable_authority; catching here is the
        # backstop covering every other path (the /settings and IdP-discovery
        # URLs, whose hosts that check never sees).
        if form is not None:
            body = urllib.parse.urlencode(
                {k: v for k, v in form.items() if v is not None}).encode('utf-8')
            req_headers['Content-Type'] = 'application/x-www-form-urlencoded'
        if headers:
            req_headers.update(headers)
        req = urllib.request.Request(
            url, data=body, headers=req_headers, method=method.upper())
        with _opener(ctx).open(req, timeout=timeout) as resp:
            return HttpResponse(
                getattr(resp, 'status', resp.getcode()),
                _read_body(resp, max_bytes=_MAX_RESPONSE_BYTES,
                           deadline=_monotonic() + timeout),
                resp.headers)
    except UnicodeError as e:
        # A non-ASCII URL host or an unencodable request field (e.g. a lone
        # surrogate) — keep it within the typed-error contract instead of leaking
        # a raw UnicodeEncodeError from urlencode().encode() / http.client.
        raise OidcConfigError(
            f'Could not encode the request to {url!r}: {e}. The URL host or a '
            'request field contains a non-ASCII or unencodable character (e.g. a '
            'lone surrogate), indicating a malformed or tampered configuration '
            'or server response.') from e
    except urllib.error.HTTPError as e:
        # 4xx/5xx still carry a (possibly JSON) body to inspect. Bound the read
        # (same cap/deadline), map a mid-body read failure to a network error,
        # and close the response so its socket isn't leaked (the poll loop drives
        # many 400s during a long sign-in).
        try:
            body = _read_body(e, max_bytes=_MAX_RESPONSE_BYTES,
                              deadline=_monotonic() + timeout)
        except (TimeoutError, OSError, http.client.HTTPException) as read_err:
            # Mirror the success path's handler: a mid-body read failure such as
            # http.client.IncompleteRead (an HTTPException, NOT an OSError) when
            # the server resets the connection mid-error-body must map to a typed
            # network error, not escape raw. (_read_body's own OidcNetworkError
            # for the size/deadline cap is already typed and propagates here.)
            raise OidcNetworkError(
                f'Failed to read response from {url}: {read_err}') from read_err
        finally:
            e.close()
        return HttpResponse(e.code, body, e.headers or {})
    except urllib.error.URLError as e:
        raise OidcNetworkError(f'Failed to reach {url}: {e.reason}') from e
    except http.client.InvalidURL as e:
        # A malformed URL (e.g. non-integer port) can't become a request;
        # surface it as a config error, not a raw http.client exception.
        raise OidcConfigError(f'Malformed URL {url!r}: {e}') from e
    except (TimeoutError, OSError, http.client.HTTPException) as e:
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
        # Map to the OidcError SUBCLASS that matches the cause, so a caller can
        # `except OidcConfigError` / `except OidcNetworkError` around
        # from_questdb() the same way _refresh / _poll_for_token classify
        # post_form's errors: a 5xx/429 is a transient server / rate-limit issue,
        # anything else (a 4xx/3xx from /settings or IdP discovery — a wrong URL,
        # OIDC not advertised, an auth gate) is a configuration one. The HTTP
        # status is attached either way (mirroring post_form) so a future retry
        # caller can still classify terminal-vs-transient uniformly.
        msg = f'HTTP {resp.status} from {url}: {resp.text()[:200]}'
        if resp.status >= 500 or resp.status == 429:
            raise OidcNetworkError(msg, status=resp.status)
        raise OidcConfigError(msg, status=resp.status)
    try:
        return resp.json()
    except (ValueError, UnicodeDecodeError, RecursionError) as e:
        # A non-JSON body where OIDC JSON was expected (an HTML login/error page
        # from a proxy, or the wrong URL) is a configuration problem, not a
        # transport one. RecursionError (deeply-nested JSON) isn't a ValueError,
        # so catch it explicitly to keep the typed contract.
        raise OidcConfigError(
            f'Invalid JSON from {url}: {e}', status=resp.status) from e


def _parse_retry_after(headers: Optional[Mapping[str, str]]) -> Optional[int]:
    """
    A ``Retry-After`` header as a non-negative ``int`` of seconds, else ``None``.

    Honors the delta-seconds form (RFC 7231 §7.1.3); the HTTP-date form is
    ignored (the caller's fixed back-off covers that rarer case, and parsing a
    date pulls in tz handling for little gain). Case-insensitive, so an HTTP/2 /
    proxy-lowercased header name is still matched.
    """
    if not headers:
        return None
    value = None
    for key, val in headers.items():
        if key.lower() == 'retry-after':
            value = val
            break
    try:
        secs = int(str(value).strip())
    except (TypeError, ValueError):
        return None
    return secs if secs >= 0 else None


class _PostResult(tuple):
    """``(status, body)`` carrying an extra ``.retry_after`` (seconds or None).

    A 2-tuple subclass, so existing ``status, body = post_form(...)`` callers are
    unchanged; the device-flow poll additionally reads ``.retry_after`` to honor
    a 429 / 503 ``Retry-After`` header instead of its fixed +5s back-off.
    """

    def __new__(cls, status: int, body: Dict[str, Any],
                retry_after: Optional[int]):
        self = super().__new__(cls, (status, body))
        self.retry_after = retry_after
        return self


def post_form(
        url: str,
        form: Mapping[str, Any],
        *,
        headers: Optional[Mapping[str, str]] = None,
        timeout: float = _DEFAULT_TIMEOUT,
        ctx: Optional[ssl.SSLContext] = None,
        insecure: bool = False) -> '_PostResult':
    """
    POST a form-url-encoded body and parse the JSON response.

    Returns ``(status, parsed_json)`` — a :class:`_PostResult`, a 2-tuple that
    also carries ``.retry_after`` (the parsed ``Retry-After`` seconds, or
    ``None``). Used for the device-authorization and token endpoints, which
    return JSON on both success and error.
    """
    resp = request(
        'POST', url, form=form, headers=headers, timeout=timeout, ctx=ctx,
        insecure=insecure)
    retry_after = _parse_retry_after(resp.headers)
    try:
        parsed = resp.json()
    except (ValueError, UnicodeDecodeError, RecursionError):
        # RecursionError (deeply-nested JSON) isn't a ValueError, so catch it
        # explicitly to keep the typed contract.
        if resp.ok:
            raise OidcError(
                f'Expected JSON from {url}, got: {resp.text()[:200]}',
                status=resp.status, retry_after=retry_after)
        # Non-JSON error body: attach the HTTP status so callers (poll loop /
        # silent refresh) can tell a terminal 4xx from a transient 5xx/429, and
        # the parsed Retry-After so a non-JSON 429/503 backs off by the server's
        # value rather than the fixed +5s step.
        raise OidcError(
            f'HTTP {resp.status} from {url}: {resp.text()[:200]}',
            status=resp.status, retry_after=retry_after)
    if not isinstance(parsed, dict):
        # Attach the status (mirroring the non-JSON branches) so a non-object
        # body on a terminal 4xx — e.g. a JSON array from a non-conformant IdP
        # — fails the poll loop fast instead of polling on to "code expired".
        raise OidcError(
            f'Unexpected JSON shape from {url}: {parsed!r}',
            status=resp.status, retry_after=retry_after)
    return _PostResult(resp.status, parsed, retry_after)
