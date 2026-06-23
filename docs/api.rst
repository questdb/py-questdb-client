=============
API Reference
=============

questdb.ingress
===============

.. testsetup::

    from questdb.ingress import *

.. autoclass:: questdb.ingress.Sender
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: questdb.ingress.Buffer
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: questdb.ingress.SenderTransaction
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: questdb.ingress.IngressError
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: questdb.ingress.IngressErrorCode
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: questdb.ingress.Protocol
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: questdb.ingress.TimestampMicros
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: questdb.ingress.TimestampNanos
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: questdb.ingress.TlsCa
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: questdb.ingress.ServerTimestampType
   :members:
   :undoc-members:
   :show-inheritance:

.. autodata:: questdb.ingress.ServerTimestamp
    :annotation:
    :no-value:

.. autoclass:: questdb.ingress.TaggedEnum
   :members:
   :undoc-members:
   :show-inheritance:

questdb.auth
============

See the :ref:`oidc_auth` guide for an overview.

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

.. autoclass:: questdb.auth.TokenSet
   :members:
   :undoc-members:
   :show-inheritance:

.. autoexception:: questdb.auth.OidcError
   :show-inheritance:

.. autoexception:: questdb.auth.OidcConfigError
   :show-inheritance:

.. autoexception:: questdb.auth.OidcInteractionRequired
   :show-inheritance:

.. autoexception:: questdb.auth.OidcDeviceFlowError
   :show-inheritance:

.. autoexception:: questdb.auth.OidcTimeoutError
   :show-inheritance:

.. autoexception:: questdb.auth.OidcNetworkError
   :show-inheritance:
