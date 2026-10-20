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
   :special-members: __arrow_c_schema__, __arrow_c_stream__
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

.. autodata:: questdb.SchemaOverride
    :annotation:

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
   :show-inheritance:

questdb.auth
============

.. py:module:: questdb.auth

See the :ref:`oidc_auth` and :ref:`refreshing_token_provider` guides for an
overview.

.. autoclass:: questdb.auth.RefreshingTokenProvider
   :members:
   :show-inheritance:

.. autoclass:: questdb.auth.ExpiringToken
   :members:
   :show-inheritance:

.. autoexception:: questdb.auth.TokenUnavailableError
   :members:
   :show-inheritance:

.. autoclass:: questdb.auth.TokenProviderStatus
   :members:

.. autoclass:: questdb.auth.FetchFailure
   :members:

.. autoclass:: questdb.auth.TokenSource
   :members:

.. autoclass:: questdb.auth.BearerTokenProvider
   :members:

.. autoclass:: questdb.auth.OidcDeviceAuth
   :members:
   :undoc-members:
   :show-inheritance:

.. autofunction:: questdb.auth.sqlalchemy_engine

.. autofunction:: questdb.auth.psycopg_connect

.. autoclass:: questdb.auth.OidcConfig
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: questdb.auth.FileTokenStore
   :members:
   :undoc-members:
   :show-inheritance:

.. autodata:: questdb.auth.TOKEN_STORE_DIR_ENV

.. autoclass:: questdb.auth.Renderer
   :members:
   :show-inheritance:

.. autofunction:: questdb.auth.sanitize_display_text

.. autoexception:: questdb.auth.OidcError
   :members:
   :show-inheritance:

.. autoexception:: questdb.auth.OidcConfigError
   :members:
   :show-inheritance:

.. autoexception:: questdb.auth.OidcCancelledError
   :members:
   :show-inheritance:

.. autoexception:: questdb.auth.OidcNetworkError
   :members:
   :show-inheritance:

.. autoexception:: questdb.auth.OidcInteractionRequired
   :members:
   :show-inheritance:

.. autoexception:: questdb.auth.OidcDeviceFlowError
   :members:
   :show-inheritance:

.. autoexception:: questdb.auth.OidcTimeoutError
   :members:
   :show-inheritance:
