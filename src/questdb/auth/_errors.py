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
        self.error = error
        self.error_description = error_description


class OidcTimeoutError(OidcDeviceFlowError):
    """The user did not authorize the device in time (the code expired)."""
