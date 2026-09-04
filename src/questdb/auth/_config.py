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

"""Resolved configuration exposed by the native OIDC provider."""

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class OidcConfig:
    """The OIDC configuration a provider actually resolved.

    Returned by :attr:`questdb.auth.OidcDeviceAuth.config`, and read-only:
    it reports what the provider settled on after discovery and any explicit
    overrides, rather than what was requested. Useful for confirming which
    identity provider and token kind a provider built by
    :meth:`~questdb.auth.OidcDeviceAuth.from_questdb` picked up from a
    server's ``/settings``.

    Every field is display-sanitized — control, bidi and zero-width characters
    are stripped — because a ``/settings`` response is untrusted and this
    object's ``repr`` can reach a terminal, a notebook or a logged traceback.

    :param client_id: The OAuth client id used with the identity provider.
    :param token_endpoint: The IdP endpoint tokens are requested from.
    :param device_authorization_endpoint: The IdP endpoint the device flow
        starts at.
    :param scope: The scope string sent verbatim on the initial request and on
        every refresh.
    :param groups_in_token: Which token
        :meth:`~questdb.auth.OidcDeviceAuth.token` returns — ``True`` selects
        the ID token, ``False`` the access token.
    :param audience: The configured audience, or ``None`` when unset.
    :param issuer: The configured issuer, or ``None`` when unset. When set it
        also pins which credential endpoints are accepted.
    """

    client_id: str
    token_endpoint: str
    device_authorization_endpoint: str
    scope: str = 'openid'
    groups_in_token: bool = False
    audience: Optional[str] = None
    issuer: Optional[str] = None
