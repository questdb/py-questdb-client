__version__ = '6.0.1'

import sys as _sys
from types import ModuleType as _ModuleType
from typing import Callable as _Callable, Optional as _Optional, Union as _Union

from questdb import _client
from questdb._client import (
    Char,
    ConnectionEvent,
    ConnectionEventKind,
    DateMillis,
    ExecDone,
    Geohash,
    Long256,
    OidcDeviceAuth as _OidcDeviceAuth,
    PooledReader,
    PooledSender,
    Protocol,
    QueryResult,
    QuestDB,
    QuestDBError,
    QuestDBErrorCode,
    QuestDBServerRejectionError,
    QwpWsProgress,
    Sender,
    SenderError,
    SenderErrorCategory,
    SenderErrorPolicy,
    SenderTransaction,
    ServerInfo,
    ServerRole,
    ServerTimestamp,
    ServerTimestampType,
    TimestampMicros,
    TimestampNanos,
    TlsCa,
    UnsupportedDataFrameShapeError,
)

__all__ = [
    'Char',
    'ConnectionEvent',
    'ConnectionEventKind',
    'DateMillis',
    'ExecDone',
    'Geohash',
    'Long256',
    'PooledReader',
    'PooledSender',
    'Protocol',
    'QueryResult',
    'QuestDB',
    'QuestDBError',
    'QuestDBErrorCode',
    'QuestDBServerRejectionError',
    'QwpWsProgress',
    'Sender',
    'SenderError',
    'SenderErrorCategory',
    'SenderErrorPolicy',
    'SenderTransaction',
    'ServerInfo',
    'ServerRole',
    'ServerTimestamp',
    'ServerTimestampType',
    'TimestampMicros',
    'TimestampNanos',
    'TlsCa',
    'UnsupportedDataFrameShapeError',
    'WARN_HIGH_RECONNECTS',
    '__version__',
    'connect',
]


def _conf_value(key: str, value) -> str:
    if isinstance(value, bool):
        if key == 'tls_verify':
            value = 'on' if value else 'unsafe_off'
        else:
            value = 'on' if value else 'off'
    return str(value).replace(';', ';;')


def connect(
        conf_str: _Optional[str] = None,
        *,
        host: _Optional[str] = None,
        port: _Union[int, str, None] = None,
        tls: _Optional[bool] = None,
        oidc_auth: _Optional[_OidcDeviceAuth] = None,
        bearer_token_provider=None,
        connection_listener: _Optional[
            _Callable[[ConnectionEvent], None]] = None,
        connection_event_inbox_capacity: int = 0,
        error_handler: _Optional[
            _Callable[[SenderError], None]] = None,
        error_event_inbox_capacity: int = 0,
        **params) -> QuestDB:
    """
    Connect to a QuestDB deployment and return a :class:`QuestDB` handle.

    This is the deployment-level entry point and the default for new
    code. The handle owns connection pools for both ingestion and
    queries: borrow row-building senders with :meth:`QuestDB.sender`,
    bulk-load DataFrames with :meth:`QuestDB.dataframe`, run SQL with
    :meth:`QuestDB.query`, and borrow reader leases with
    :meth:`QuestDB.reader`. The standalone :class:`Sender` is the
    complementary connection-level API (one sender = one connection)
    for point-to-point needs: ILP/HTTP transactions, ILP/TCP, QWP/UDP
    datagrams, and manually driven single ws connections.

    Pass either a QWP/WebSocket configuration string
    (``ws::addr=host:port;`` or ``wss::...``) or the equivalent
    keywords: ``host``, ``port`` (default 9000) and ``tls`` (default
    ``False``) select the endpoint. Either way, any further keyword
    arguments are added as configuration-string settings
    (``username='u'``, ``sender_pool_max=4``, ...; booleans normally map
    to ``on``/``off``, while ``tls_verify=False`` maps to
    ``unsafe_off``); a setting present both in the string and as a
    keyword raises :class:`ValueError`, matching
    :meth:`Sender.from_conf`. One configuration addresses the whole
    deployment; list every cluster node in a single ``addr`` server
    list.

    ``oidc_auth`` accepts :class:`questdb.auth.OidcDeviceAuth`. Call
    ``oidc_auth.sign_in()`` before ``connect``; pooled connections and
    reconnects only load or silently refresh tokens and never prompt.
    Use ``wss::``: over plain ``ws::`` to a non-loopback host the token is
    sent in cleartext.

    ``bearer_token_provider`` attaches a rotating Bearer-token provider that
    every pooled connection shares: a
    :class:`questdb.auth.RefreshingTokenProvider` around a token source of
    your own (Azure Identity, an OAuth2 flow, ...), a callable returning a
    token, or an object with ``token()`` and an optional ``on_rejected()``.
    It is mutually exclusive with ``oidc_auth`` and with ``username``,
    ``password`` and ``token``. See :ref:`refreshing_token_provider`. A
    provider that runs on the calling thread runs inside the call that needs
    the token; see :ref:`token_provider_reentrancy` for what it, and a Python
    signal handler that runs there, must not do.

    By default ``connect()`` opens the warm minimums up front and fails
    fast when the server is unreachable or rejects the credentials. Set
    ``lazy_connect=True`` (or ``lazy_connect=true`` in the string) to
    tolerate a down server at startup: nothing connects here, sender
    leases buffer locally and connect in the background, and readers
    connect on the first query.

    Pooled row senders auto-flush by default at 1,000 rows, 100 milliseconds,
    or a cap-derived byte threshold. Set ``auto_flush_bytes='off'`` to disable
    only the byte trigger, or ``auto_flush='off'`` to disable all automatic
    publishing. Auto-triggered publishes do not wait for server
    acknowledgement; use an explicit ``flush(wait=True)`` or ``wait()`` as an
    acknowledgement barrier.

    ``connection_listener`` receives one :class:`ConnectionEvent` per
    connection-state transition; ``error_handler`` receives one
    :class:`SenderError` per server rejection (without it rejections are
    logged through the ``questdb`` logger). Each runs on its own
    dispatcher thread; see :meth:`QuestDB.from_conf` for details. When
    a handler or listener closes over the returned handle, call
    :meth:`QuestDB.close` explicitly (or use ``with``) rather than
    leaving the handle to the garbage collector.

    .. code-block:: python

        import questdb

        with questdb.connect('ws::addr=localhost:9000;') as db:
            with db.sender() as sender:
                sender.row('trades', symbols={'sym': 'BTC-USD'},
                           columns={'price': 62000.0}, at=ts)
            db.dataframe(df, table_name='trades', at='ts')
            result = db.query('SELECT * FROM trades LIMIT 10')

        with questdb.connect(host='localhost', port=9000) as db:
            ...
    """
    if (conf_str is None) == (host is None):
        raise TypeError(
            'connect() takes either a configuration string or a host= '
            'keyword (with optional port=, tls= and further settings), '
            'but not both.')
    for key in params:
        if not key.isidentifier():
            raise TypeError(f'invalid settings keyword {key!r}')
    if conf_str is not None:
        given = [name for name, value
                 in (('port', port), ('tls', tls)) if value is not None]
        if given:
            raise TypeError(
                'connect() only accepts the endpoint keywords '
                f'({", ".join(given)}) together with host=, not with a '
                'configuration string; use ws::/wss:: and addr= in the '
                'string instead.')
        if params:
            # Insert right after 'ws::'/'wss::': appending at the tail
            # would mis-merge when the string ends inside an escaped
            # ';;' value.
            schema, sep, rest = conf_str.partition('::')
            conf_str = schema + sep + ''.join(
                f'{key}={_conf_value(key, value)};'
                for key, value in params.items()) + rest
    else:
        if not isinstance(host, str):
            raise TypeError(
                f'"host" must be a str, not {type(host).__name__}')
        if host.startswith('['):
            if not host.endswith(']'):
                raise ValueError(
                    f'"host" must not include a port ({host!r}); pass the '
                    'port via port= instead.')
        elif host.count(':') >= 2:
            host = f'[{host}]'
        elif ':' in host:
            raise ValueError(
                f'"host" must not include a port ({host!r}); pass the port '
                'via port= instead.')
        settings = [('addr', f'{host}:{port if port is not None else 9000}')]
        settings.extend(params.items())
        conf_str = ('wss::' if tls else 'ws::') + ''.join(
            f'{key}={_conf_value(key, value)};' for key, value in settings)
    try:
        return QuestDB.from_conf(
            conf_str,
            oidc_auth=oidc_auth,
            bearer_token_provider=bearer_token_provider,
            connection_listener=connection_listener,
            connection_event_inbox_capacity=connection_event_inbox_capacity,
            error_handler=error_handler,
            error_event_inbox_capacity=error_event_inbox_capacity)
    except QuestDBError as e:
        # Preserve structured OIDC failures even if untrusted IdP text happens
        # to resemble the native config parser's duplicate-key diagnostic.
        #
        # An OidcError cannot exist unless `questdb.auth._errors` is already
        # imported, so resolve the class from `sys.modules` rather than
        # importing it here. This runs while a failure is already being
        # reported, and an import that raises -- during interpreter
        # finalization, through a blocked or poisoned `sys.modules` entry, or
        # via a shadowed stdlib name on the `questdb.auth` import chain --
        # would replace the connect error the caller actually needs. It also
        # keeps `questdb.auth`, and the `unicodedata` / `re` / `ipaddress` /
        # `urllib.parse` behind it, out of every failed connect.
        _errs = _sys.modules.get('questdb.auth._errors')
        if _errs is not None:
            _oidc_error_cls = getattr(_errs, 'OidcError', None)
            if _oidc_error_cls is not None and isinstance(e, _oidc_error_cls):
                raise
        # The native parser reports a keyword/string conflict as a
        # duplicate key at a position in the merged string the caller
        # never wrote; rephrase it like Sender.from_conf does.
        msg = str(e)
        for key in params:
            if f'duplicate key "{key}"' in msg:
                raise ValueError(
                    f'"{key}" is already present in the conf_str '
                    'and cannot be overridden.') from None
        raise


class _QuestdbModule(_ModuleType):
    @property
    def WARN_HIGH_RECONNECTS(self):
        return _client.WARN_HIGH_RECONNECTS

    @WARN_HIGH_RECONNECTS.setter
    def WARN_HIGH_RECONNECTS(self, value):
        _client.WARN_HIGH_RECONNECTS = value


_sys.modules[__name__].__class__ = _QuestdbModule
