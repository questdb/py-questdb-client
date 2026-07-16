__version__ = '5.0.0'

import sys as _sys
from types import ModuleType as _ModuleType

from questdb import _client
from questdb._client import (
    ConnectionEvent,
    ConnectionEventKind,
    PooledQuery,
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
    'PooledQuery',
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


def connect(
        conf_str: str,
        *,
        connection_listener=None,
        connection_event_inbox_capacity: int = 0) -> QuestDB:
    """
    Connect to a QuestDB deployment and return a :class:`QuestDB` handle.

    The handle owns connection pools for both ingestion and queries:
    borrow row-building senders with :meth:`QuestDB.sender`, bulk-load
    DataFrames with :meth:`QuestDB.dataframe`, and run SQL with
    :meth:`QuestDB.query`.

    ``conf_str`` must be a QWP/WebSocket configuration string
    (``ws::addr=host:port;`` or ``wss::...``). One string configures the
    whole deployment; list every cluster node in a single ``addr`` server
    list.

    .. code-block:: python

        import questdb

        with questdb.connect('ws::addr=localhost:9000;') as db:
            with db.sender() as sender:
                sender.row('trades', symbols={'sym': 'BTC-USD'},
                           columns={'price': 62000.0}, at=ts)
            db.dataframe(df, table_name='trades', at='ts')
            result = db.query('SELECT * FROM trades LIMIT 10')
    """
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
