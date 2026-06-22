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


class OidcError(Exception):
    """Base class for every error raised by :mod:`questdb.auth`."""

    def __init__(self, *args, status: Optional[int] = None):
        super().__init__(*args)
        # HTTP status that produced this error, when it originated from a
        # non-JSON HTTP response (else None). Lets the device-flow poll loop and
        # the silent refresh tell a terminal 4xx rejection (e.g. a WAF/proxy
        # error page) from a transient 5xx/429/network blip even when the body
        # was not a conformant JSON OAuth error.
        self.status = status


class OidcConfigError(OidcError):
    """
    The OIDC configuration could not be resolved or is inconsistent.

    Raised, for example, when QuestDB does not advertise OIDC, when the
    IdP device-authorization endpoint cannot be discovered, or when a
    required argument is missing.
    """


class OidcNetworkError(OidcError):
    """A network-level failure while talking to QuestDB or the IdP."""


class OidcInteractionRequired(OidcError):
    """
    Interactive sign-in is required but the process is not interactive.

    This is raised instead of hanging forever when the device flow is
    started from a context with no human to authorize it (e.g. a
    ``papermill`` run, a cron job or CI). Use a QuestDB service-account
    REST token or the OAuth2 client-credentials grant in those contexts.
    """


class OidcDeviceFlowError(OidcError):
    """
    The OAuth 2.0 device authorization grant failed.

    The original IdP ``error``/``error_description`` are preserved on the
    exception when available.
    """

    def __init__(
            self,
            message: str,
            *,
            error: Optional[str] = None,
            error_description: Optional[str] = None):
        super().__init__(message)
        self.error = error
        self.error_description = error_description


class OidcTimeoutError(OidcDeviceFlowError):
    """The user did not authorize the device in time (the code expired)."""


class OidcAuthError(OidcError):
    """
    QuestDB rejected the token we presented.

    Typically a ``401``/``403`` from the server. The message includes hints
    about the most common causes (scope / ``groups.encoded.in.token`` /
    ``audience`` mismatches).
    """
