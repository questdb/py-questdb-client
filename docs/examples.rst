========
Examples
========

Getting started
===============

.. _connect_example:

connect() quick start
---------------------

The following example leases a pooled sender from a
:class:`QuestDB <questdb.QuestDB>` handle, writes a row over QWP/WebSocket,
waits for the server acknowledgement, then runs a bind-parameter query into
pandas.

.. literalinclude:: ../examples/connect_basic.py
   :language: python

.. _connect_query_example:

Queries
-------

The following example runs DDL through ``query()``, binds positional
parameters, streams a large result batch by batch, and leases one reader
connection for a run of queries.

.. literalinclude:: ../examples/connect_query.py
   :language: python

.. _connect_dataframe_example:

DataFrame bulk load and reader leases
-------------------------------------

The following example bulk-loads a pandas ``DataFrame`` through
:meth:`QuestDB.dataframe <questdb.QuestDB.dataframe>`, then leases one reader
connection with ``db.reader()`` and runs two queries on it.

.. literalinclude:: ../examples/connect_dataframe.py
   :language: python

.. _connect_random_data_example:

Ticking data and flush cadence
------------------------------

The following example mimics an application loop producing rows at random
intervals. The pooled sender has no auto-flush, so it publishes on an
explicit row-count / elapsed-time cadence.

.. literalinclude:: ../examples/connect_random_data.py
   :language: python

Data Frames
===========

Pandas Basics
-------------

The following example shows how to insert data from a Pandas DataFrame to the
``'trades'`` table and run a generated query into Pandas.

.. literalinclude:: ../examples/pandas_basic.py
   :language: python

For details on all options, see the
:meth:`QuestDB.dataframe <questdb.QuestDB.dataframe>` method.


``pd.Categorical`` and multiple tables
--------------------------------------

The next example shows some more advanced features inserting data from Pandas
and running a generated query into Pandas.

* The data is sent to multiple tables: the columnar path loads one table
  per call, so the frame is split by its table-naming ``pd.Categorical``
  column and each group is bulk-loaded with its own
  :meth:`QuestDB.dataframe <questdb.QuestDB.dataframe>` call.

* Columns of type ``pd.Categorical`` are sent as ``SYMBOL`` types.

.. literalinclude:: ../examples/pandas_advanced.py
   :language: python

After running this example, the rows will be split across the ``'humidity'``,
``'temp_c'`` and ``'voc_index'`` tables.

For details on all options, see the
:meth:`QuestDB.dataframe <questdb.QuestDB.dataframe>` method.

Loading Pandas from a Parquet File
----------------------------------

The following example shows how to load a Pandas DataFrame from a Parquet file
and run a generated query into Pandas.

The example also relies on the dataframe's index name to determine the table
name.

.. literalinclude:: ../examples/pandas_parquet.py
   :language: python

For details on all options, see the
:meth:`QuestDB.dataframe <questdb.QuestDB.dataframe>` method.

.. _ws_polars_example:

Polars
------

The following example ingests a Polars ``DataFrame`` over QWP/WebSocket via
:meth:`QuestDB.dataframe <questdb.QuestDB.dataframe>`, runs a query into
Polars, and includes a ``schema_overrides`` variant.

.. literalinclude:: ../examples/polars_basic.py
   :language: python

.. _ws_pyarrow_example:

PyArrow
-------

The following example ingests a PyArrow table over QWP/WebSocket via
:meth:`QuestDB.dataframe <questdb.QuestDB.dataframe>` and runs a query into
PyArrow.

.. literalinclude:: ../examples/pyarrow_basic.py
   :language: python

Decimal Types (QuestDB 9.2.0+)
------------------------------

The following example shows how to insert data with decimal precision using
Python's :class:`decimal.Decimal` type, both row by row and as an
object-dtype DataFrame column.

Requires QuestDB server 9.2.0+. ``DECIMAL`` columns must be
:ref:`pre-created <sender_auto_creation>` (auto-creation not supported); the
examples create them through ``query()``.

.. literalinclude:: ../examples/decimal_basic.py
   :language: python

For better performance with DataFrames, use PyArrow decimal types, which
carry width and scale in the column type:

.. literalinclude:: ../examples/decimal_arrow.py
   :language: python

.. note::

    Over ``ws::`` / ``wss::`` decimal support is negotiated automatically.
    On the legacy ILP transports, HTTP/HTTPS auto-negotiates
    :ref:`protocol version 3 <sender_conf_protocol_version>` while TCP/TCPS
    must configure it explicitly:
    ``tcp::addr=localhost:9009;protocol_version=3;``.

For more details, see the
`QuestDB DECIMAL documentation <https://questdb.com/docs/reference/sql/datatypes/#decimal>`_.

Production
==========

.. _connect_auth_tls_example:

TLS, token auth, multi-host
---------------------------

The following production-shaped example combines TLS with OS trust roots, a
bearer token, a multi-node ``addr`` list, a disk-backed store-and-forward
spool, and a connection-event listener.

.. literalinclude:: ../examples/connect_auth_tls.py
   :language: python

Connection-level transports
===========================

The examples below use the standalone :class:`Sender <questdb.Sender>` —
the connection-level API, where one sender drives exactly one connection.
This layer carries the point-to-point capabilities the deployment-level
handle does not: QWP/UDP datagrams, the legacy ILP transports (HTTP/TCP),
and HTTP transactions. See :ref:`sender_api_layers`.

.. _qwp_udp_example:

QWP over UDP
------------

The following example sends a row using QuestWire Protocol over UDP.

Requires a QuestDB instance with QWP/UDP receiver support enabled. The
default listener port is ``9007``.

.. literalinclude:: ../examples/qwp_udp.py
   :language: python

Legacy ILP (HTTP/TCP)
---------------------

InfluxDB Line Protocol ingestion is fully supported through the standalone
:class:`Sender <questdb.Sender>` — including HTTP transactions, which are
unique to this layer.

HTTP with Token Auth
~~~~~~~~~~~~~~~~~~~~

The following example connects to the database and sends two rows (lines).

The connection is made via HTTPS and uses token based authentication.

The data is sent at the end of the ``with`` block.

.. literalinclude:: ../examples/http_basic.py
   :language: python

HTTP Transactions
~~~~~~~~~~~~~~~~~

The following example sends rows atomically inside an ILP/HTTP transaction,
a capability unique to the HTTP transport.

.. literalinclude:: ../examples/http_transaction.py
   :language: python

TCP Basics
~~~~~~~~~~

The following example writes rows over ILP/TCP with auto-flush.

.. literalinclude:: ../examples/tcp_basic.py
   :language: python

.. _auth_and_tls_example:

TCP Authentication and TLS
~~~~~~~~~~~~~~~~~~~~~~~~~~

Continuing from the previous example, the connection is authenticated
and also uses TLS.

.. literalinclude:: ../examples/tcp_auth_tls.py
   :language: python

Further legacy ILP examples — ECDSA auth (``tcp_auth.py``) and
configuration loading (``http_from_conf.py``) — live in the repository's
`examples directory <https://github.com/questdb/py-questdb-client/tree/main/examples>`_.
