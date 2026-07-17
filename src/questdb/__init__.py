__version__ = '5.0.0'

import sys as _sys
from types import ModuleType as _ModuleType
from typing import Callable as _Callable, Optional as _Optional, Union as _Union

from questdb import _client
from questdb._client import (
    ConnectionEvent,
    ConnectionEventKind,
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
    'ConnectionEvent',
    'ConnectionEventKind',
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
            connection_listener=connection_listener,
            connection_event_inbox_capacity=connection_event_inbox_capacity,
            error_handler=error_handler,
            error_event_inbox_capacity=error_event_inbox_capacity)
    except QuestDBError as e:
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
