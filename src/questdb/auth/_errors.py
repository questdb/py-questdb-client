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

"""Exceptions raised by :mod:`questdb.auth`."""

from __future__ import annotations

from typing import Optional

# _render is stdlib-only (no internal imports), so this introduces no cycle.
from ._render import _strip_control


class OidcError(Exception):
    """Base class for every error raised by :mod:`questdb.auth`."""

    def __init__(self, *args, status: Optional[int] = None):
        # Strip terminal/bidi/zero-width control characters from every string
        # message argument before it can reach a display sink. Error messages
        # routinely interpolate untrusted IdP fields (error_description, response
        # bodies, verification URIs), and an uncaught exception's traceback —
        # printed to a terminal or rendered by Jupyter, both of which interpret
        # ANSI — is a sink the renderer's own sanitization never sees. Without
        # this, a hostile or MITM'd IdP could inject ANSI escapes or a bidi
        # override into that traceback to spoof the prompt. Doing it here (not at
        # each raise site) means no raise site can forget; non-string args (rare)
        # pass through unchanged.
        args = tuple(
            _strip_control(a) if isinstance(a, str) else a for a in args)
        super().__init__(*args)
        # HTTP status behind a non-JSON HTTP response (else None), so the poll
        # loop and silent refresh can tell a terminal 4xx (e.g. a WAF error
        # page) from a transient 5xx/429/network blip.
        self.status = status


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
            error_description: Optional[str] = None):
        super().__init__(message)
        # error / error_description come straight from the untrusted IdP
        # response and are exposed as attributes (a caller may re-display them),
        # so strip them too — same rationale as the message in OidcError. None is
        # kept as None (not coerced to '') so "absent" stays distinguishable.
        self.error = _strip_control(error) if error is not None else None
        self.error_description = (
            _strip_control(error_description)
            if error_description is not None else None)


class OidcTimeoutError(OidcDeviceFlowError):
    """The user did not authorize the device in time (the code expired)."""
