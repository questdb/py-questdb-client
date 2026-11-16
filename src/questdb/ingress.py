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
    VERSION,
    Buffer,
    Char,
    DateMillis,
    Geohash,
    Long256,
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

# ``VERSION``, ``TaggedEnum``, ``QuestDBError`` and ``QuestDBErrorCode`` are
# importable module attributes (as in 4.x) but are deliberately kept out of
# ``__all__``. The four QWP row-value wrappers are intentionally added to the
# otherwise legacy star-import surface.
__all__ = [
    'Buffer',
    'Char',
    'DateMillis',
    'Geohash',
    'IngressError',
    'IngressErrorCode',
    'Long256',
    'Protocol',
    'Sender',
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
