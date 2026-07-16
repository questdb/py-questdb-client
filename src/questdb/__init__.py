__version__ = '5.0.0'

import sys as _sys
from types import ModuleType as _ModuleType
from typing import Optional as _Optional, Union as _Union

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
    QwpWsError,
    QwpWsErrorCategory,
    QwpWsErrorPolicy,
    QwpWsProgress,
    Sender,
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
    'QwpWsError',
    'QwpWsErrorCategory',
    'QwpWsErrorPolicy',
    'QwpWsProgress',
    'Sender',
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


def _conf_value(value) -> str:
    if isinstance(value, bool):
        value = 'on' if value else 'off'
    return str(value).replace(';', ';;')


def connect(
        conf_str: _Optional[str] = None,
        *,
        host: _Optional[str] = None,
        port: _Union[int, str] = 9000,
        tls: bool = False,
        connection_listener=None,
        connection_event_inbox_capacity: int = 0,
        **params) -> QuestDB:
    """
    Connect to a QuestDB deployment and return a :class:`QuestDB` handle.

    This is the entry point for new code. The handle owns connection
    pools for both ingestion and queries: borrow row-building senders
    with :meth:`QuestDB.sender`, bulk-load DataFrames with
    :meth:`QuestDB.dataframe`, run SQL with :meth:`QuestDB.query`, and
    borrow reader leases with :meth:`QuestDB.reader`. The
    :class:`Sender` class remains the channel for the legacy ILP
    protocols (HTTP, TCP, UDP).

    Pass either a QWP/WebSocket configuration string
    (``ws::addr=host:port;`` or ``wss::...``) or the equivalent
    keywords: ``host``, ``port`` (default 9000) and ``tls`` (default
    ``False``) select the endpoint, and any further keyword arguments
    are appended as configuration-string settings verbatim
    (``username='u'``, ``sender_pool_max=4``, ...; booleans map to
    ``on``/``off``). One configuration addresses the whole deployment;
    list every cluster node in a single ``addr`` server list.

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
    if conf_str is not None and params:
        unexpected = ', '.join(sorted(params))
        raise TypeError(
            'connect() only accepts additional settings keywords '
            f'({unexpected}) together with host=, not with a '
            'configuration string; add them to the string instead.')
    if conf_str is None:
        settings = [('addr', f'{host}:{port}')]
        settings.extend(params.items())
        conf_str = ('wss::' if tls else 'ws::') + ''.join(
            f'{key}={_conf_value(value)};' for key, value in settings)
    return QuestDB.from_conf(
        conf_str,
        connection_listener=connection_listener,
        connection_event_inbox_capacity=connection_event_inbox_capacity)


class _QuestdbModule(_ModuleType):
    @property
    def WARN_HIGH_RECONNECTS(self):
        return _client.WARN_HIGH_RECONNECTS

    @WARN_HIGH_RECONNECTS.setter
    def WARN_HIGH_RECONNECTS(self, value):
        _client.WARN_HIGH_RECONNECTS = value


_sys.modules[__name__].__class__ = _QuestdbModule
