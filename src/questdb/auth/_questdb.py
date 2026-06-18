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

"""High-level QuestDB session: token, REST queries, and connection adapters."""

from __future__ import annotations

import urllib.parse
from typing import Any, Dict, Optional

from ._device import OidcDeviceAuth
from ._errors import OidcAuthError, OidcConfigError, OidcError
from ._http import request, safe_urlparse

_DEFAULT_PG_PORT = 8812
_DEFAULT_DATABASE = 'qdb'

_AUTH_HINT = (
    'QuestDB rejected the token (HTTP {status}). Common causes:\n'
    "  * scope / 'acl.oidc.groups.encoded.in.token' mismatch — the server may "
    'expect the id_token (groups in token) while an access_token was sent, or '
    'vice-versa;\n'
    "  * the 'groups'/'sub' claim is missing — check the requested scope;\n"
    "  * 'aud' mismatch — the token's audience does not match "
    "'acl.oidc.audience' (try passing audience=...).")


def _import_pandas():
    try:
        import pandas  # type: ignore
        return pandas
    except ImportError as e:
        raise ImportError(
            'Missing optional dependency `pandas`, required for '
            'QuestDB.sql(). Install it with `pip install questdb[dataframe]`. '
            'See https://py-questdb-client.readthedocs.io/en/latest/'
            'installation.html') from e


def _exec_json_to_df(data: Dict[str, Any], pandas):
    columns = data.get('columns') or []
    # /exec returns a list of {"name", "type"} column descriptors. A malformed
    # response (a non-list, or entries that aren't objects) must surface as a
    # clean OidcError, not an AttributeError from .get() escaping the package's
    # typed-error contract.
    if not isinstance(columns, list) or not all(
            isinstance(c, dict) for c in columns):
        raise OidcError(
            'QuestDB /exec returned a malformed "columns" field; '
            'cannot build a DataFrame.')
    names = [c.get('name') for c in columns]
    dataset = data.get('dataset')
    if dataset is None:
        dataset = data.get('data') or []
    try:
        df = pandas.DataFrame(dataset, columns=names or None)
    except ValueError as e:
        raise OidcError(
            f'Unexpected shape in QuestDB /exec response: {e}') from e
    for col in columns:
        name = col.get('name')
        if col.get('type') in ('TIMESTAMP', 'DATE') and name in df.columns:
            try:
                df[name] = pandas.to_datetime(df[name], errors='coerce')
            except Exception:
                pass
    return df


def _pg_module():
    try:
        import psycopg  # type: ignore  # psycopg v3
        return psycopg
    except ImportError:
        pass
    try:
        import psycopg2  # type: ignore
        return psycopg2
    except ImportError:
        raise ImportError(
            'A PostgreSQL driver is required: install `psycopg` (v3) or '
            '`psycopg2-binary`.')


class QuestDB:
    """
    A thin, authenticated QuestDB session built on an :class:`OidcDeviceAuth`.

    Provides a one-call DataFrame query over REST plus adapters that feed the
    same auto-refreshed token into your existing tools (SQLAlchemy / psycopg /
    the ingestion ``Sender``). You can also just take :meth:`token` /
    :meth:`headers` and wire them up yourself.
    """

    def __init__(
            self,
            url: str,
            auth: OidcDeviceAuth,
            *,
            insecure: bool = False):
        self.url = url.rstrip('/')
        self.auth = auth
        self._insecure = insecure
        self._ctx = auth._ctx
        # safe_urlparse validates the port up-front, raising OidcConfigError
        # (not a bare ValueError) for a malformed one, so the adapters that read
        # the port stay within the package's typed-error contract.
        self._parts, self._port = safe_urlparse(self.url)

    # -- token access -------------------------------------------------------

    def token(self) -> str:
        """Return a valid, auto-refreshed token (see :meth:`OidcDeviceAuth.token`)."""
        return self.auth.token()

    def headers(self) -> Dict[str, str]:
        """Return ``{"Authorization": "Bearer <token>"}``."""
        return self.auth.headers()

    # -- REST query ---------------------------------------------------------

    def sql(self, query: str, *, limit: Optional[str] = None,
            timeout: float = 60) -> 'pandas.DataFrame':
        """
        Run a SQL query over QuestDB's REST ``/exec`` endpoint and return a
        :class:`pandas.DataFrame`.

        Uses ``Authorization: Bearer`` (no token-length limit, unlike PG-wire),
        which makes it the recommended path for large groups-encoded JWTs.

        :param query: The SQL query to run.
        :param limit: Optional QuestDB ``limit`` (e.g. ``"1,1000"``).
        :param timeout: Request timeout in seconds.
        """
        pandas = _import_pandas()
        params = {'query': query}
        if limit is not None:
            params['limit'] = limit
        url = f'{self.url}/exec?' + urllib.parse.urlencode(params)
        resp = request(
            'GET', url, headers=self.headers(), ctx=self._ctx,
            insecure=self._insecure, timeout=timeout)
        if resp.status in (401, 403):
            raise OidcAuthError(_AUTH_HINT.format(status=resp.status))
        if not resp.ok:
            detail = resp.text()[:300]
            try:
                detail = resp.json().get('error', detail)
            except Exception:
                pass
            raise OidcError(
                f'QuestDB query failed (HTTP {resp.status}): {detail}')
        try:
            data = resp.json()
        except (ValueError, UnicodeDecodeError):
            # A 2xx body that isn't JSON (e.g. an HTML error/login page from a
            # reverse proxy or captive portal) must surface as a clean
            # OidcError, not a raw JSONDecodeError. Mirrors the error path and
            # post_form().
            raise OidcError(
                'QuestDB returned a non-JSON success response from /exec: '
                f'{resp.text()[:300]}')
        if not isinstance(data, dict):
            # Valid JSON but not an object (e.g. a bare list) would make
            # _exec_json_to_df fail with AttributeError on .get(); reject it.
            raise OidcError(
                'QuestDB /exec returned JSON that is not an object '
                f'(got {type(data).__name__}); cannot build a DataFrame.')
        return _exec_json_to_df(data, pandas)

    # -- connection adapters ------------------------------------------------

    def _require_host(self, host: Optional[str] = None) -> str:
        """
        Resolve the PG-wire / ILP host: an explicit ``host`` override, else the
        host from the QuestDB URL. Raises when neither yields one (e.g. a URL
        with no authority such as ``"localhost"`` or ``"questdb:9000"``) instead
        of passing a bare ``None`` down to the driver.

        The returned host is *unbracketed* — psycopg and SQLAlchemy take the
        address and port as separate arguments. :meth:`_ilp_addr` adds the
        brackets an IPv6 literal needs in the ILP ``addr=host:port`` form.
        """
        resolved = host or self._parts.hostname
        if not resolved:
            raise OidcConfigError(
                f'The QuestDB URL {self.url!r} has no host. Use a URL with an '
                'explicit host (e.g. "https://questdb.example.com:9000"), or '
                'pass host=... to the adapter.')
        return resolved

    @staticmethod
    def _ilp_addr(host: str, port: int) -> str:
        # Bracket an IPv6 literal so the ILP conf parser reads host:port
        # unambiguously; hostnames and IPv4 addresses never contain ':'.
        bracketed = f'[{host}]' if ':' in host else host
        return f'{bracketed}:{port}'

    def sqlalchemy_engine(
            self,
            *,
            host: Optional[str] = None,
            pg_port: int = _DEFAULT_PG_PORT,
            database: str = _DEFAULT_DATABASE,
            drivername: Optional[str] = None,
            **engine_kwargs) -> 'sqlalchemy.engine.Engine':
        """
        Build a SQLAlchemy ``Engine`` for QuestDB's PG-wire endpoint.

        Connects as user ``_sso`` and injects a **fresh** token as the password
        for every new connection (via a ``do_connect`` listener), so pooled
        connections always authenticate with a valid token. Requires
        ``acl.oidc.pg.token.as.password.enabled=true`` on the server.
        """
        try:
            from sqlalchemy import create_engine, event
            from sqlalchemy.engine import URL
        except ImportError as e:
            raise ImportError(
                'SQLAlchemy is required for QuestDB.sqlalchemy_engine(); '
                'install it with `pip install sqlalchemy`.') from e

        if drivername is None:
            mod = _pg_module()
            drivername = (
                'postgresql+psycopg'
                if mod.__name__ == 'psycopg'
                else 'postgresql+psycopg2')

        url = URL.create(
            drivername=drivername,
            username='_sso',
            host=self._require_host(host),
            port=pg_port,
            database=database)
        engine = create_engine(url, **engine_kwargs)

        auth = self.auth

        @event.listens_for(engine, 'do_connect')
        def _provide_token(dialect, conn_rec, cargs, cparams):  # noqa: ANN001
            cparams['password'] = auth.token()

        return engine

    def psycopg(
            self,
            *,
            host: Optional[str] = None,
            pg_port: int = _DEFAULT_PG_PORT,
            database: str = _DEFAULT_DATABASE,
            **connect_kwargs) -> 'Any':
        """
        Open a raw psycopg (v3) or psycopg2 connection to QuestDB's PG-wire
        endpoint, authenticating as ``_sso`` with the current token.

        The token is captured at connect time; open a new connection to pick up
        a refreshed token.
        """
        mod = _pg_module()
        return mod.connect(
            host=self._require_host(host),
            port=pg_port,
            dbname=database,
            user='_sso',
            password=self.auth.token(),
            **connect_kwargs)

    def sender(self, *, port: Optional[int] = None,
               **sender_kwargs) -> 'questdb.ingress.Sender':
        """
        Build a :class:`questdb.ingress.Sender` (ILP-over-HTTP) configured with
        the current bearer token, for ingestion.

        The token is captured at creation time; create a new sender to pick up
        a refreshed token.
        """
        try:
            from questdb.ingress import Sender
        except ImportError as e:
            raise ImportError(
                'The compiled `questdb.ingress` module is required for '
                'QuestDB.sender(). Install the full client wheel '
                '(`pip install questdb`).') from e

        scheme = 'https' if self._parts.scheme == 'https' else 'http'
        resolved_port = port or self._port or (
            443 if scheme == 'https' else 9000)
        conf = (f'{scheme}::addr='
                f'{self._ilp_addr(self._require_host(), resolved_port)};')
        return Sender.from_conf(conf, token=self.auth.token(), **sender_kwargs)


def connect(
        url: str,
        *,
        flow: str = 'auto',
        cache: Any = 'memory',
        insecure: bool = False,
        eager: bool = True,
        **opts) -> QuestDB:
    """
    High-level entry point: authenticate to QuestDB and return a
    :class:`QuestDB` session.

    .. code-block:: python

        from questdb.auth import connect

        qdb = connect("https://questdb.example.com:9000")   # signs in
        df = qdb.sql("SELECT * FROM trades LIMIT 10")

    Configuration (OIDC client id, scope, endpoints, groups mode) is discovered
    from ``{url}/settings`` and, as needed, the IdP ``.well-known`` document.
    Re-running the same call reuses the cached token (no re-prompt).

    :param url: The QuestDB HTTP(S) base URL, e.g.
        ``"https://questdb.example.com:9000"``.
    :param flow: ``"auto"`` (default), ``"device"`` or ``"loopback"``. Today
        ``"auto"`` always resolves to the device flow (works on local and
        remote kernels); ``"loopback"`` is reserved for a future release.
    :param cache: Token cache backend: ``"memory"`` (default) or ``None``.
    :param insecure: Allow plaintext ``http://`` URLs (development only).
    :param eager: If ``True`` (default), sign in immediately; otherwise defer
        until the first call that needs a token.
    :param opts: Forwarded to :meth:`OidcDeviceAuth.from_questdb` (e.g.
        ``client_id``, ``scope``, ``audience``, ``issuer``, ``open_browser``,
        ``qr``, ``ca_bundle``).
    """
    auth = OidcDeviceAuth.from_questdb(
        url, flow=flow, cache=cache, insecure=insecure, **opts)
    qdb = QuestDB(url, auth, insecure=insecure)
    if eager:
        auth.token()
    return qdb
