__version__ = '5.0.0'

import sys as _sys
from types import ModuleType as _ModuleType

from questdb import _client
from questdb._client import (
    Buffer,
    Client,
    ClientSender,
    Protocol,
    QueryResult,
    QuestDBError,
    QuestDBErrorCode,
    QuestDBServerRejectionError,
    QwpWsError,
    QwpWsErrorCategory,
    QwpWsErrorPolicy,
    QwpWsProgress,
    Sender,
    SenderTransaction,
    ServerTimestamp,
    ServerTimestampType,
    TimestampMicros,
    TimestampNanos,
    TlsCa,
    UnsupportedDataFrameShapeError,
)

__all__ = [
    'Buffer',
    'Client',
    'ClientSender',
    'Protocol',
    'QueryResult',
    'QuestDBError',
    'QuestDBErrorCode',
    'QuestDBServerRejectionError',
    'QwpWsError',
    'QwpWsErrorCategory',
    'QwpWsErrorPolicy',
    'QwpWsProgress',
    'Sender',
    'SenderTransaction',
    'ServerTimestamp',
    'ServerTimestampType',
    'TimestampMicros',
    'TimestampNanos',
    'TlsCa',
    'UnsupportedDataFrameShapeError',
    'WARN_HIGH_RECONNECTS',
    '__version__',
]


class _QuestdbModule(_ModuleType):
    @property
    def WARN_HIGH_RECONNECTS(self):
        return _client.WARN_HIGH_RECONNECTS

    @WARN_HIGH_RECONNECTS.setter
    def WARN_HIGH_RECONNECTS(self, value):
        _client.WARN_HIGH_RECONNECTS = value


_sys.modules[__name__].__class__ = _QuestdbModule
