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
PG-wire connection adapters.

Feed an :class:`OidcDeviceAuth` token into SQLAlchemy / psycopg as the QuestDB
``_sso`` password. These are thin conveniences over the token: for REST or the
ingestion ``Sender``, take :meth:`OidcDeviceAuth.headers` / :meth:`token` and
wire it up yourself.
"""

from __future__ import annotations

import re
from typing import Any, Optional

from ._device import OidcDeviceAuth
from ._errors import OidcConfigError
from ._http import safe_urlparse

_DEFAULT_PG_PORT = 8812
_DEFAULT_DATABASE = 'qdb'

# Reject connection-string delimiters (';', '='), whitespace/control chars, and
# '%' in the host: a real hostname / IPv4 / IPv6-literal never has them, so their
# presence means a tampered URL trying to inject PG connection parameters
# (psycopg turns its kwargs into a libpq conninfo string). ':' is allowed — IPv6
# literals contain it, and the PG drivers take host and port separately. '%'
# would only appear as an IPv6 zone-id (e.g. 'fe80::1%eth0'), meaningful only for
# a link-local address on the local machine and never for reaching a remote
# QuestDB; rejecting it keeps the guard a strict plain-host allowlist
# (defense-in-depth — '%' is not itself a conninfo delimiter).
_ILLEGAL_HOST_CHARS = re.compile(r'[\x00-\x20\x7f;=%]')


def _pg_module():
    try:
        import psycopg  # type: ignore  # psycopg v3
        return psycopg
    except ImportError:
        pass
    try:
        import psycopg2  # type: ignore
        return psycopg2
    except ImportError as e:
        raise ImportError(
            'A PostgreSQL driver is required: install `psycopg` (v3) or '
            '`psycopg2-binary`.') from e


def _require_host(url: str, host: Optional[str] = None) -> str:
    """
    Resolve the PG-wire host: an explicit ``host`` override, else the host from
    the QuestDB ``url``. Raises (rather than passing a bare ``None`` to the
    driver) when neither yields one, e.g. a URL with no authority such as
    ``"localhost"`` or ``"questdb:9000"``.

    The returned host is *unbracketed* — psycopg and SQLAlchemy take address and
    port separately. ``safe_urlparse`` validates the port up-front, raising
    ``OidcConfigError`` (not a bare ``ValueError``) for a malformed one.
    """
    parts, _ = safe_urlparse(url)
    resolved = host or parts.hostname
    if not resolved:
        raise OidcConfigError(
            f'The QuestDB URL {url!r} has no host. Use a URL with an explicit '
            'host (e.g. "https://questdb.example.com:9000"), or pass host=... '
            'to the adapter.')
    # An explicit host="[::1]" override arrives bracketed; the URL-derived path is
    # already unbracketed (urlparse strips the brackets off an IPv6 literal). The
    # drivers take a BARE address, so strip a single surrounding [...] here too,
    # keeping the "returned host is unbracketed" contract for both paths. Done
    # before the illegal-char check so it validates the bare host handed to the
    # driver (and any junk inside the brackets is still caught).
    if resolved.startswith('[') and resolved.endswith(']') and len(resolved) > 2:
        resolved = resolved[1:-1]
    if _ILLEGAL_HOST_CHARS.search(resolved):
        raise OidcConfigError(
            f'The QuestDB host {resolved!r} contains an illegal character '
            "(';', '=', '%', whitespace or a control character). A hostname or "
            'IP address never does; this indicates a malformed or tampered URL. '
            '(Such a host could otherwise inject PG connection parameters.)')
    return resolved


def _coerce_port(pg_port: Any) -> int:
    """
    Coerce ``pg_port`` to an ``int`` within the module's typed-error contract.

    A non-integer ``pg_port`` (e.g. a port read from an env var without an
    ``int()``) would otherwise reach ``URL.create(port=...)`` /
    ``driver.connect(port=...)`` and surface as a bare ``ValueError`` / driver
    error, escaping ``OidcConfigError``. ``bool`` is an ``int`` subclass but
    ``True``/``False`` is never a meaningful port, so reject it explicitly —
    mirroring the constructor's other up-front type checks.
    """
    if isinstance(pg_port, bool):
        raise OidcConfigError(
            f'pg_port must be an integer port number, got {pg_port!r}.')
    try:
        port = int(pg_port)
    except (TypeError, ValueError, OverflowError) as e:
        # int(float('inf')) / int(1e400) raise OverflowError (not ValueError),
        # so catch it too — else a non-finite pg_port escapes the typed-error
        # contract as a bare OverflowError (mirrors _validate_positive_number).
        raise OidcConfigError(
            f'pg_port must be an integer port number, got {pg_port!r}.') from e
    if not 1 <= port <= 65535:
        raise OidcConfigError(
            f'pg_port must be a valid TCP port (1-65535), got {port}.')
    return port


def sqlalchemy_engine(
        auth: OidcDeviceAuth,
        url: str,
        *,
        host: Optional[str] = None,
        pg_port: int = _DEFAULT_PG_PORT,
        database: str = _DEFAULT_DATABASE,
        drivername: Optional[str] = None,
        **engine_kwargs) -> 'sqlalchemy.engine.Engine':
    """
    Build a SQLAlchemy ``Engine`` for QuestDB's PG-wire endpoint, authenticated
    with ``auth``.

    Connects as user ``_sso``, injecting a **fresh** token as the password on
    every new connection (via a ``do_connect`` listener) so pooled connections
    always authenticate with a valid, auto-refreshed token. Requires
    ``acl.oidc.pg.token.as.password.enabled=true`` on the server.

    Sign in once up front (``auth.token()``) before the pool opens connections.
    The per-connection injection is **non-interactive**: it reuses and silently
    refreshes the cached token, but never launches a browser prompt from a pool
    thread. If no token has been acquired yet it raises
    :class:`OidcInteractionRequired` rather than blocking the pool on an
    interactive sign-in.

    :param auth: An :class:`OidcDeviceAuth`, e.g. from
        :meth:`OidcDeviceAuth.from_questdb`.
    :param url: The QuestDB base URL; the PG host is derived from it unless
        ``host=`` is given.
    :param pg_port: PG-wire port (default ``8812``).
    :param database: Database name (default ``"qdb"``).
    :param drivername: SQLAlchemy driver; defaults to ``postgresql+psycopg``
        (v3) or ``postgresql+psycopg2`` depending on what is installed.
    :param engine_kwargs: Forwarded to ``create_engine``.
    """
    pg_port = _coerce_port(pg_port)
    try:
        from sqlalchemy import create_engine, event
        from sqlalchemy.engine import URL
    except ImportError as e:
        raise ImportError(
            'SQLAlchemy is required for questdb.auth.sqlalchemy_engine(); '
            'install it with `pip install sqlalchemy`.') from e

    if drivername is None:
        mod = _pg_module()
        drivername = (
            'postgresql+psycopg'
            if mod.__name__ == 'psycopg'
            else 'postgresql+psycopg2')

    engine = create_engine(
        URL.create(
            drivername=drivername,
            username='_sso',
            host=_require_host(url, host),
            port=pg_port,
            database=database),
        **engine_kwargs)

    @event.listens_for(engine, 'do_connect')
    def _provide_token(dialect, conn_rec, cargs, cparams):  # noqa: ANN001
        # Non-interactive: reuse / silently refresh the up-front token, but never
        # run an interactive device flow from a pool thread (it would block the
        # pool). Raises OidcInteractionRequired if no token was acquired first.
        cparams['password'] = auth._token(allow_interactive=False)

    return engine


def psycopg_connect(
        auth: OidcDeviceAuth,
        url: str,
        *,
        host: Optional[str] = None,
        pg_port: int = _DEFAULT_PG_PORT,
        database: str = _DEFAULT_DATABASE,
        **connect_kwargs) -> Any:
    """
    Open a raw psycopg (v3) or psycopg2 connection to QuestDB's PG-wire
    endpoint, authenticating as ``_sso`` with the current token.

    The token is captured at connect time; reconnect to pick up a refreshed
    token. Requires ``acl.oidc.pg.token.as.password.enabled=true`` on the
    server.

    :param auth: An :class:`OidcDeviceAuth`, e.g. from
        :meth:`OidcDeviceAuth.from_questdb`.
    :param url: The QuestDB base URL; the PG host is derived from it unless
        ``host=`` is given.
    :param connect_kwargs: Forwarded to the driver's ``connect()``.
    """
    pg_port = _coerce_port(pg_port)
    mod = _pg_module()
    return mod.connect(
        host=_require_host(url, host),
        port=pg_port,
        dbname=database,
        user='_sso',
        password=auth.token(),
        **connect_kwargs)
