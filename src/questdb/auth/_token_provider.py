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

"""
The two extension points of rotating Bearer credentials, as typing protocols.

A :class:`TokenSource` fetches a new token for a
:class:`~questdb.auth.RefreshingTokenProvider`; a :class:`BearerTokenProvider`
supplies the current token to a client directly. Both are structural: any
callable, or any object with the right method, qualifies.
"""

from typing import Protocol, runtime_checkable

__all__ = ['BearerTokenProvider', 'TokenSource']


@runtime_checkable
class TokenSource(Protocol):
    """
    Fetches a new token from an identity platform for a
    :class:`~questdb.auth.RefreshingTokenProvider`: Azure Identity, an OAuth2
    client-credentials or PKCE flow, a device flow, a secrets vault.

    A plain callable taking no arguments works too. Return an
    :class:`~questdb.auth.ExpiringToken`, a ``(token, expires_at)`` tuple, or
    any object with ``token`` and ``expires_on`` (epoch seconds) attributes,
    such as Azure Core's ``AccessToken``. Raise
    :class:`~questdb.auth.TokenUnavailableError` to classify a failure.

    The provider calls it only on its own thread, never concurrently with
    itself. It may block, but should bound its own network calls.
    """

    def fetch_token(self):
        ...


@runtime_checkable
class BearerTokenProvider(Protocol):
    """
    Supplies the current Bearer token to a client: the counterpart of the Java
    client's ``HttpTokenProvider``. Pass one as ``bearer_token_provider=``.

    ``token()`` runs on every connect round, possibly on a native thread and
    concurrently, so it should return quickly: prefer a
    :class:`~questdb.auth.RefreshingTokenProvider` unless you already cache
    tokens. The optional ``on_rejected(token, http_status)`` hears that a
    server rejected ``token`` (HTTP 401); the client then calls ``token()``
    again and retries once if the token changed.
    """

    def token(self) -> str:
        ...
