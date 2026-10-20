=============
API Reference
=============

questdb
===============

.. testsetup::

    from questdb import *

.. autofunction:: questdb.connect

.. autoclass:: questdb.QuestDB
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: questdb.PooledSender
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: questdb.PooledReader
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: questdb.Sender
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: questdb.SenderTransaction
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: questdb.QueryResult
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: questdb.ConnectionEvent
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: questdb.ConnectionEventKind
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: questdb.ServerInfo
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: questdb.ServerRole
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: questdb.QuestDBError
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: questdb.QuestDBServerRejectionError
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: questdb.UnsupportedDataFrameShapeError
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: questdb.QuestDBErrorCode
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: questdb.SenderError
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: questdb.SenderErrorCategory
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: questdb.SenderErrorPolicy
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: questdb.QwpWsProgress
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: questdb.Protocol
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: questdb.TimestampMicros
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: questdb.TimestampNanos
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: questdb.Char
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: questdb.DateMillis
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: questdb.Long256
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: questdb.Geohash
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: questdb.TlsCa
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: questdb.ServerTimestampType
   :members:
   :undoc-members:
   :show-inheritance:

.. autodata:: questdb.ServerTimestamp
    :annotation:
    :no-value:

.. autoclass:: questdb._client.TaggedEnum
   :members:
   :show-inheritance:

.. autodata:: questdb.WARN_HIGH_RECONNECTS
    :annotation:

questdb.ingress (legacy)
========================

The deprecated 4.x compatibility shim. New code imports from ``questdb``.

.. autoclass:: questdb.ingress.Buffer
   :members:
   :undoc-members:
