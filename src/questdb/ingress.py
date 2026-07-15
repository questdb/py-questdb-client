"""
Deprecated import location for the 4.x ILP/HTTP and ILP/TCP ingestion API.

Existing code such as ``from questdb.ingress import Sender`` keeps working;
new code should use ``from questdb import Sender`` for row streaming over
the legacy protocols, or ``questdb.connect()`` for QWP/WebSocket.
"""

import sys as _sys
import warnings as _warnings
from types import ModuleType as _ModuleType

from questdb import _client
from questdb._client import (
    Buffer,
    Protocol,
    QuestDBError,
    QuestDBErrorCode,
    Sender,
    SenderTransaction,
    ServerTimestamp,
    ServerTimestampType,
    TaggedEnum,
    TimestampMicros,
    TimestampNanos,
    TlsCa,
)

IngressError = QuestDBError
IngressErrorCode = QuestDBErrorCode

__all__ = [
    'Buffer',
    'IngressError',
    'IngressErrorCode',
    'Protocol',
    'Sender',
    'SenderTransaction',
    'ServerTimestamp',
    'ServerTimestampType',
    'TimestampMicros',
    'TimestampNanos',
    'TlsCa',
    'WARN_HIGH_RECONNECTS',
]

_warnings.warn(
    'questdb.ingress is deprecated; import from questdb instead '
    '(or use questdb.connect() for QWP/WebSocket).',
    DeprecationWarning,
    stacklevel=2)


class _IngressModule(_ModuleType):
    @property
    def WARN_HIGH_RECONNECTS(self):
        return _client.WARN_HIGH_RECONNECTS

    @WARN_HIGH_RECONNECTS.setter
    def WARN_HIGH_RECONNECTS(self, value):
        _client.WARN_HIGH_RECONNECTS = value


_sys.modules[__name__].__class__ = _IngressModule
