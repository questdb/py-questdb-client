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
    """Base class for every error raised by :mod:`questdb.auth`.

    A subclass of :class:`~questdb.QuestDBError` (with ``code``
    ``QuestDBErrorCode.AuthError``), so a transport attached with ``oidc_auth=``
    whose token acquisition fails is caught by an existing ``except
    QuestDBError`` handler; catch :class:`OidcError` — or a typed subclass such
    as :class:`OidcInteractionRequired` — to handle auth failures specifically.
    """

    def __init__(self, *args, status: Optional[int] = None,
                 retry_after: Optional[int] = None,
                 in_doubt: bool = False):
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
        QuestDBError.__init__(
            self, QuestDBErrorCode.AuthError, args[0] if args else '',
            in_doubt=in_doubt)
        self.args = args
        # HTTP status behind a non-JSON HTTP response (else None), so the poll
        # loop and silent refresh can tell a terminal 4xx (e.g. a WAF error
        # page) from a transient 5xx/429/network blip.
        self.status = status
        # Parsed Retry-After (delta-seconds) off a non-JSON 429/503 error body,
        # so the poll loop can honor it the same way the JSON path does (via
        # _PostResult.retry_after). None when absent / not applicable.
        self.retry_after = retry_after


class OidcConfigError(OidcError):
    """
    The OIDC configuration could not be resolved or is inconsistent (e.g.
    QuestDB does not advertise OIDC, the IdP device-authorization endpoint
    cannot be discovered, or a required argument is missing).
    """


class OidcNetworkError(OidcError):
    """A network-level failure while talking to QuestDB or the IdP."""


class OidcInteractionRequired(OidcError):
    """
    Interactive sign-in is required, but raised instead of hanging in a
    non-interactive context (``papermill``, cron, CI). Use a QuestDB
    service-account REST token or the OAuth2 client-credentials grant there.
    """


class OidcCancelledError(OidcError):
    """An OIDC operation was cancelled because its provider was closed."""


class OidcDeviceFlowError(OidcError):
    """
    The OAuth 2.0 device authorization grant failed; the IdP
    ``error``/``error_description`` are preserved when available.
    """

    def __init__(
            self,
            message: str,
            *,
            error: Optional[str] = None,
            error_description: Optional[str] = None,
            status: Optional[int] = None,
            retry_after: Optional[int] = None,
            in_doubt: bool = False):
        # Forward status to OidcError so a device-flow error raised in response
        # to a known HTTP status carries it (e.g. for a caller inspecting
        # err.status), rather than always reporting None. in_doubt likewise
        # forwards so a device-flow error never under-reports delivery
        # uncertainty relative to the non-OIDC path.
        super().__init__(
            message, status=status, retry_after=retry_after,
            in_doubt=in_doubt)
        # error / error_description come straight from the untrusted IdP
        # response and are exposed as attributes (a caller may re-display them),
        # so strip them too — same rationale as the message in OidcError. Coerce
        # a non-string (a JSON object/number/array from a buggy or hostile IdP)
        # through str() first, exactly as OidcError does for its message args, so
        # a non-string field can't crash the strip with a TypeError and escape
        # the typed-error contract. None is kept as None (not coerced to '') so
        # "absent" stays distinguishable.
        self.error = (
            _strip_control(error if isinstance(error, str) else str(error))
            if error is not None else None)
        self.error_description = (
            _strip_control(
                error_description if isinstance(error_description, str)
                else str(error_description))
            if error_description is not None else None)


class OidcTimeoutError(OidcDeviceFlowError):
    """The user did not authorize the device in time (the code expired)."""
