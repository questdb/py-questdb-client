################################################################################
##     ___                  _   ____  ____
##    / _ \ _   _  ___  ___| |_|  _ \| __ )
##   | | | | | | |/ _ \/ __| __| | | |  _ \
##   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
##    \__\_\\__,_|\___||___/\__|____/|____/
##
##  Copyright (c) 2014-2019 Appsicle
##  Copyright (c) 2019-2026 QuestDB
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

"""Exceptions raised by :mod:`questdb.auth`."""

from __future__ import annotations

from typing import Optional

# _render is stdlib-only (no internal imports), so this introduces no cycle.
from ._render import _strip_control
# OIDC failures are QuestDB auth failures, so OidcError subclasses QuestDBError:
# an existing ``except QuestDBError`` ingestion / retry / dead-letter handler
# keeps catching them when a transport is attached with ``oidc_auth=``, while
# ``except OidcError`` (and the typed subclasses) still allow auth-specific
# handling. ``questdb._client`` is always fully imported before this module
# (the extension imports it lazily on the error path; ``questdb.auth`` imports
# it up front), so this is not a circular import.
from questdb._client import QuestDBError, QuestDBErrorCode


class OidcError(QuestDBError):
    """Base class for QuestDB OIDC authentication lifecycle errors.

    A subclass of :class:`~questdb.QuestDBError`, so a transport attached with
    ``oidc_auth=`` whose token acquisition fails is caught by an existing
    ``except QuestDBError`` handler; catch :class:`OidcError` — or a typed
    subclass such as :class:`OidcInteractionRequired` — to handle auth failures
    specifically.

    ``code`` mirrors the native classification: ``QuestDBErrorCode.AuthError``
    for a terminal auth failure, ``SocketError`` for one the client considers
    retryable (a transient token-provider failure on a reconnect), and
    ``ConfigError`` for a misconfiguration. Retry logic that keys on ``code``
    therefore treats an OIDC failure exactly as it treats any other.

    ``status`` is the HTTP status of the failing IdP response when known,
    otherwise ``None`` (a failure with no HTTP exchange behind it — a transport
    error, a cancellation, a misconfiguration). ``retry_after`` is the parsed
    ``Retry-After`` delay in seconds when the response carried one, otherwise
    ``None``. Both are populated whether or not the IdP answered with a JSON
    OAuth error body: a JSON ``429 slow_down`` with ``Retry-After: 7`` reports
    ``status == 429`` and ``retry_after == 7``, just as an HTML error page does.
    """

    #: Code reported when a raise site does not supply one. Native-built
    #: errors always pass `code=` explicitly (see `_oidc_err_to_py_unowned`);
    #: this is what a directly constructed error gets, and subclasses override
    #: it so both routes agree.
    _DEFAULT_CODE = QuestDBErrorCode.AuthError

    def __init__(self, *args, status: Optional[int] = None,
                 retry_after: Optional[int] = None,
                 in_doubt: bool = False,
                 code=None,
                 sender_error=None):
        # Strip terminal/bidi/zero-width control characters from every string
        # message argument before it can reach a display sink. Error messages
        # routinely interpolate untrusted IdP fields (error_description, response
        # bodies, verification URIs), and an uncaught exception's traceback —
        # printed to a terminal or rendered by Jupyter, both of which interpret
        # ANSI — is a sink the renderer's own sanitization never sees. Without
        # this, a hostile or MITM'd IdP could inject ANSI escapes or a bidi
        # override into that traceback to spoof the prompt. Doing it here (not at
        # each raise site) means no raise site can forget. A non-string arg is
        # coerced through str() so its text representation is sanitized too (no
        # raise site passes one today — this is defense-in-depth).
        args = tuple(
            _strip_control(a if isinstance(a, str) else str(a)) for a in args)
        # Seed the QuestDBError base with an auth code + the (sanitized) first
        # message, then restore the full args tuple so str()/repr() match the
        # historical Exception-based behavior (raise sites pass a single message
        # today; the tuple keeps the defense-in-depth multi-arg case intact).
        # in_doubt threads through to the base so the OIDC error path reports
        # delivery uncertainty consistently with the non-OIDC QuestDBError path;
        # an ``except QuestDBError`` retry/dead-letter handler reads it.
        # `code` mirrors the native classification when the binding builds this
        # from a native error. Hardcoding AuthError discarded the code the
        # native side had deliberately chosen -- notably the retryable
        # SocketError that `classify_provider_error` assigns to a recoverable
        # token-provider failure -- so callers keying on `.code` mis-classified
        # it. For a directly constructed error the class supplies the default
        # via `_DEFAULT_CODE`, so a Python-raised error reports the same code
        # as the native-built error of the same type: every raise site in this
        # package builds an `OidcConfigError` without passing `code`, and those
        # used to report AuthError while `oidc.pxi` gave the native ones
        # ConfigError, contradicting this class's own documented contract.
        # `sender_error` carries the structured QWP/WebSocket diagnostic when
        # native attached one. The native predicate behind this class is
        # "caused by an OIDC failure", not "is one", so a QWP/WebSocket
        # rejection whose root cause was a token refresh arrives here with the
        # transport's own payload still on it -- and dropping that payload is
        # the same loss `.sender_error` exists to prevent on the non-OIDC path.
        QuestDBError.__init__(
            self,
            self._DEFAULT_CODE if code is None else code,
            args[0] if args else '',
            sender_error,
            in_doubt=in_doubt)
        self.args = args
        #: HTTP status of the failing IdP response, otherwise ``None``. Set for
        #: a JSON OAuth error body as well as for a non-JSON one (an HTML WAF
        #: page), so a poll or silent-refresh caller can distinguish a terminal
        #: 4xx from a transient 5xx/429 in both shapes.
        self.status = status
        #: Parsed ``Retry-After`` delta-seconds when the failing response
        #: carried the header (typically 429/503), otherwise ``None``.
        self.retry_after = retry_after


class OidcConfigError(OidcError):
    """
    The OIDC configuration could not be resolved or is inconsistent (e.g.
    QuestDB does not advertise OIDC, the IdP device-authorization endpoint
    cannot be discovered, or a required argument is missing).

    ``code`` is :attr:`~questdb.QuestDBErrorCode.ConfigError`, matching what
    native reports for the same failure and what this package documents: a
    misconfiguration is terminal, and retry logic keying on ``code`` must not
    see it as an auth failure that signing in again could clear.
    """

    _DEFAULT_CODE = QuestDBErrorCode.ConfigError


class OidcNetworkError(OidcError):
    """A network-level failure while talking to QuestDB or the IdP."""

    #: Native maps ``OidcErrorKind::Network`` to ``SocketError`` (retryable),
    #: so a directly constructed one must too -- it is the class the base
    #: docstring names as *the* retryable kind, and it was the only subclass of
    #: six whose two construction routes disagreed.
    _DEFAULT_CODE = QuestDBErrorCode.SocketError


class OidcInteractionRequired(OidcError):
    """
    Interactive sign-in is required, but raised instead of hanging in a
    non-interactive context (``papermill``, cron, CI). Use a QuestDB
    service-account REST token or the OAuth2 client-credentials grant there.
    """


class OidcCancelledError(OidcError):
    """A sign-in attempt was cancelled, or its provider was closed permanently."""


class OidcDeviceFlowError(OidcError):
    """
    The OAuth 2.0 device authorization grant failed. The IdP fields are exposed
    as ``error`` and ``error_description`` when available, after display-control
    sanitisation. If a token endpoint reflects the submitted device code or
    refresh token in either field, that credential is replaced with the literal
    ``[redacted credential]`` before the exception is constructed. Issued token
    fields are not altered.
    """

    def __init__(
            self,
            message: str,
            *,
            error: Optional[str] = None,
            error_description: Optional[str] = None,
            status: Optional[int] = None,
            retry_after: Optional[int] = None,
            in_doubt: bool = False,
            code=None,
            sender_error=None):
        # Forward status to OidcError so a device-flow error raised in response
        # to a known HTTP status carries it (e.g. for a caller inspecting
        # err.status), rather than always reporting None. in_doubt likewise
        # forwards so a device-flow error never under-reports delivery
        # uncertainty relative to the non-OIDC path, and sender_error so a
        # transport diagnostic behind the failure is not lost.
        super().__init__(
            message, status=status, retry_after=retry_after,
            in_doubt=in_doubt, code=code, sender_error=sender_error)
        # error / error_description come straight from the untrusted IdP
        # response and are exposed as attributes (a caller may re-display them),
        # so strip them too — same rationale as the message in OidcError. Coerce
        # a non-string (a JSON object/number/array from a buggy or hostile IdP)
        # through str() first, exactly as OidcError does for its message args, so
        # a non-string field can't crash the strip with a TypeError and escape
        # the typed-error contract. None is kept as None (not coerced to '') so
        # "absent" stays distinguishable.
        #: OAuth ``error`` returned by the IdP, or ``None``. A reflected
        #: submitted credential appears as ``[redacted credential]``.
        self.error = (
            _strip_control(error if isinstance(error, str) else str(error))
            if error is not None else None)
        #: OAuth ``error_description`` returned by the IdP, or ``None``, with
        #: the same reflected-credential redaction as :attr:`error`.
        self.error_description = (
            _strip_control(
                error_description if isinstance(error_description, str)
                else str(error_description))
            if error_description is not None else None)


class OidcTimeoutError(OidcDeviceFlowError):
    """The user did not authorize the device in time (the code expired)."""
