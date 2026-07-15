.. _sender:

============
Sending Data
============

Overview
========

For QWP/WebSocket, use one :class:`QuestDB <questdb.QuestDB>` for row ingestion,
whole-dataframe ingestion, and queries. The handle owns the connection pools;
``db.sender()`` lends a row-building sender and ``db.dataframe()``
keeps whole sources on the direct columnar path.

.. code-block:: python

    import questdb
    from questdb import TimestampNanos
    import pandas as pd

    conf = 'ws::addr=localhost:9000;'
    with questdb.connect(conf) as db:
        with db.sender() as sender:
            sender.row(
                'trades',
                symbols={'symbol': 'ETH-USD', 'side': 'sell'},
                columns={'price': 2615.54, 'amount': 0.00044},
                at=TimestampNanos.now())
            sender.flush(wait=True)

        # Whole dataframes at once
        df = pd.DataFrame({
            'symbol': pd.Categorical(['ETH-USD', 'BTC-USD']),
            'side': pd.Categorical(['sell', 'sell']),
            'price': [2615.54, 39269.98],
            'amount': [0.00044, 0.001],
            'timestamp': pd.to_datetime(['2021-01-01', '2021-01-02'])})

        db.dataframe(df, table_name='trades', at='timestamp')

The pooled sender holds an internal QWP buffer. A successful ``with`` block
publishes pending rows when it returns the lease to the pool.

The standalone :class:`Sender <questdb.Sender>` remains available as the
lower-level multi-transport API for ILP over HTTP/TCP, QWP/UDP, and
QWP/WebSocket. Its ``dataframe()`` method is deprecated; see the :doc:`5.0
migration guide <migration>`.

You can read more on :ref:`sender_preparing_data` and :ref:`sender_flushing`.

Constructing the Sender
=======================

From Configuration
------------------

The ``Sender`` class is generally initialized from a
:ref:`configuration string <sender_conf>`.

.. code-block:: python

    from questdb import Sender

    conf = 'http::addr=localhost:9000;'
    with Sender.from_conf(conf) as sender:
        ...

See the :ref:`sender_conf` guide for more details.

From Env Variable
-----------------

You can also initialize the sender from an environment variable::

    export QDB_CLIENT_CONF='http::addr=localhost:9000;'

The content of the environment variable is the same
:ref:`configuration string <sender_conf>` as taken by the
:func:`Sender.from_conf <questdb.Sender.from_conf>` method,
but moving it to an environment variable is more secure and allows you to avoid
hardcoding sensitive information such as passwords and tokens in your code.

.. code-block:: python

    from questdb import Sender

    with Sender.from_env() as sender:
        ...

Programmatic Construction
-------------------------

If you prefer, you can also construct the sender programmatically.
See :ref:`sender_programmatic_construction`.

.. _sender_preparing_data:

Preparing Data
==============

Appending Rows
--------------

You can append as many rows as you like through
:meth:`QuestDB.sender <questdb.QuestDB.sender>`. The row arguments match
:meth:`Sender.row <questdb.Sender.row>`.

Appending Pandas Dataframes
---------------------------

Use :meth:`QuestDB.dataframe <questdb.QuestDB.dataframe>` to ingest a Pandas
dataframe directly.

This is `orders of magnitude <https://github.com/questdb/py-tsbs-benchmark/blob/main/README.md>`_
faster than appending rows one by one.

.. literalinclude:: ../examples/pandas_basic.py
   :language: python

For the old row-buffer dataframe methods and their 6.0.0 removal plan, see the
:doc:`5.0 migration guide <migration>`.

String vs Symbol Columns
------------------------
QuestDB has a concept of symbols which are a more efficient way of storing
categorical data (identifiers). Internally, symbols are deduplicated and
stored as integers.

When sending data, you can specify a column as a symbol by using the
``symbols`` parameter of the ``row`` or ``dataframe`` methods.

Alternatively, if a column is expected to hold a collection of one-off strings,
you can use the ``strings`` parameter.

Here is an example of sending a row with a symbol and a string:

.. code-block:: python

    from questdb import Sender, TimestampNanos
    import datetime

    conf = 'http::addr=localhost:9000;'
    with Sender.from_conf(conf) as sender:
        sender.row(
            'trades',
            symbols={
                'symbol': 'ETH-USD', 'side': 'sell'},
            columns={
                'price': 2615.54,
                'amount': 0.00044}
            at=datetime.datetime(2021, 1, 1, 12, 0, 0))

Decimal Columns
---------------

Starting with QuestDB server version 9.2.0, you can ingest data into the
database's native ``DECIMAL(precision, scale)`` column type. This is useful when
you need exact precision for financial calculations or other scenarios where
floating-point rounding errors are unacceptable.

Decimal ingestion requires :ref:`protocol version 3 <sender_conf_protocol_version>`
(must be :ref:`configured explicitly for TCP/TCPS <sender_conf_protocol_version>`).
Unlike other column types, ``DECIMAL`` columns cannot be auto-created and must be
:ref:`pre-created <troubleshooting-decimal>` with the appropriate
``DECIMAL(precision, scale)`` definition. See the
`QuestDB DECIMAL documentation <https://questdb.com/docs/reference/sql/datatypes/#decimal>`_
and :ref:`troubleshooting guide <troubleshooting-flushing>` for more details.

To send decimal values, use Python's :class:`decimal.Decimal` type in the
``row`` method or pandas DataFrames:

.. code-block:: python

    from decimal import Decimal
    from questdb import Sender, TimestampNanos
    import pandas as pd

    # CREATE TABLE prices (
    #     symbol SYMBOL,
    #     price DECIMAL(18, 6),
    #     timestamp TIMESTAMP_NS
    # ) TIMESTAMP(timestamp) PARTITION BY DAY;

    conf = 'http::addr=localhost:9000;'
    with Sender.from_conf(conf) as sender:
        sender.row(
            'prices',
            symbols={'symbol': 'BTC-USD'},
            columns={'price': Decimal('50123.456789')},
            at=TimestampNanos.now())
        
        df = pd.DataFrame({
            'symbol': ['BTC-USD', 'ETH-USD'],
            'price': [Decimal('50123.456789'), Decimal('2615.123456')]
        })
        sender.dataframe(df, table_name='prices', symbols=['symbol'],
                        at=TimestampNanos.now())

When using pandas DataFrames, you can also use PyArrow decimal types for better
performance:

.. code-block:: python

    import pyarrow as pa

    df = pd.DataFrame({
        'symbol': ['BTC-USD', 'ETH-USD'],
        'price': pd.Series([50123.456789, 2615.123456],
                          dtype=pd.ArrowDtype(pa.decimal128(12, 6)))
    })

Populating Designated Timestamps
--------------------------------

The ``at`` parameter of the ``row`` and ``dataframe`` methods is used to specify
the `designated timestamp <https://questdb.com/docs/concept/designated-timestamp/>`_
of the rows. The designated timestamp column determines the order in which data
is stored as rows and is used for
`partitioning <https://questdb.com/docs/concept/partitions/>`.

Set by client
~~~~~~~~~~~~~

It can be either a :class:`TimestampNanos <questdb.TimestampNanos>`
object, a :class:`TimestampMicros <questdb.TimestampMicros>` object or a
`datetime.datetime <https://docs.python.org/3/library/datetime.html>`_ object.

In case of dataframes you can also specify the timestamp column name or index.
If so, the column type should be a Pandas ``datetime64``, with or without
timezone information.

QuestDB stores timestamps as either microseconds (``TIMESTAMP`` QuestDB column
type) or nanoseconds (``TIMESTAMP_NS`` QuestDB column type) as a numeric value
from unix epoch in UTC. Any timezone information is dropped when sent to
the database.

.. note::

    Nanosecond timestamp support is only available from QuestDB 9.1.0 onwards.

.. _sender_server_timestamp:

Set by server
~~~~~~~~~~~~~

If you prefer, you can specify ``at=ServerTimestamp`` which will instruct
QuestDB to set the timestamp on your behalf for each row as soon as it's
received by the server.

.. code-block:: python

    from questdb import Sender, ServerTimestamp

    conf = 'http::addr=localhost:9000;'
    with Sender.from_conf(conf) as sender:
        sender.row(
            'trades',
            symbols={'symbol': 'ETH-USD', 'side': 'sell'},
            columns={'price': 2615.54, 'amount': 0.00044},
            at=ServerTimestamp)  # Legacy feature, not recommended.

.. warning::

    Using ``ServerTimestamp`` is not recommended as it removes the ability
    for QuestDB to deduplicate rows and is considered a *legacy feature*.


.. _sender_flushing:

Flushing
========

The sender accumulates data into an internal buffer. Calling
:func:`Sender.flush <questdb.Sender.flush>` will send the buffered data
to QuestDB, and clear the buffer.

Flushing can be done explicitly or automatically.

Explicit Flushing
-----------------

An explicit call to :func:`Sender.flush <questdb.Sender.flush>` will
send any pending data immediately.

.. code-block:: python

    conf = 'http::addr=localhost:9000;'
    with Sender.from_conf(conf) as sender:
        sender.row(
            'trades',
            symbols={'symbol': 'ETH-USD', 'side': 'sell'},
            columns={'price': 2615.54, 'amount': 0.00044},
            at=TimestampNanos.now())
        sender.flush()
        sender.row(
            'trades',
            symbols={'symbol': 'BTC-USD', 'side': 'sell'},
            columns={'price': 39269.98, 'amount': 0.001},
            at=TimestampNanos.now())
        sender.flush()

Note that the last `sender.flush()` is entirely optional as flushing
also happens at the end of the ``with`` block.

.. _sender_auto_flush:

Auto-flushing
-------------

To avoid accumulating very large buffers, the sender will - by default -
occasionally flush the buffer automatically.

Auto-flushing is triggered when:

* appending a row to the internal sender buffer

* and the buffer either:

    * Reaches 75'000 rows (for HTTP) or 600 rows (for TCP).

    * Hasn't been flushed for 1 second (there are no timers).

Here is an example :ref:`configuration string <sender_conf>` that auto-flushes
sets up a sender to flush every 10 rows and disables
the interval-based auto-flushing logic.

``http::addr=localhost:9000;auto_flush_rows=10;auto_flush_interval=off;``

Here is a configuration string with auto-flushing
completely disabled:

``http::addr=localhost:9000;auto_flush=off;``

See the :ref:`sender_conf_auto_flush` section for more details. and note that
``auto_flush_interval`` :ref:`does NOT start a timer <sender_conf_auto_flush_interval>`.

Error Reporting
===============

**TL;DR: Use HTTP for better error reporting**

The sender will do its best to check for errors before sending data to the
server.

When using the HTTP protocol, the server will send back an error message if
the data is invalid or if there is a problem with the server. This will be
raised as an :class:`QuestDBError <questdb.QuestDBError>` exception.

The HTTP layer will also attempt retries, configurable via the
:ref:`retry_timeout <sender_conf_request>` parameter.`

When using the TCP protocol errors are *not* sent back from the server and
must be searched for in the logs. See the :ref:`troubleshooting-flushing`
section for more details.

.. _sender_transaction:

HTTP Transactions
=================

When using the HTTP protocol, the sender can be configured to send a batch of
rows as a single transaction.

**Transactions are limited to a single table.**

.. code-block:: python

    conf = 'http::addr=localhost:9000;'
    with Sender.from_conf(conf) as sender:
        with sender.transaction('weather_sensor') as txn:
            txn.row(
                'trades',
                symbols={'symbol': 'ETH-USD', 'side': 'sell'},
                columns={'price': 2615.54, 'amount': 0.00044},
                at=TimestampNanos.now())
            txn.row(
                'trades',
                symbols={'symbol': 'BTC-USD', 'side': 'sell'},
                columns={'price': 39269.98, 'amount': 0.001},
                at=TimestampNanos.now())

If auto-flushing is enabled, any pending data will be flushed before the
transaction is started.

Auto-flushing is disabled during the scope of the transaction.

The transaction is automatically completed a the end
of the ``with`` block.

* If the there are no errors, the transaction is committed and sent to the
  server without delays.

* If an exception is raised with the block, the transaction is rolled back and
  the exception is propagated.

You can also terminate a transaction explicity by calling the
:func:`commit <questdb.SenderTransaction.commit>` or the
:func:`rollback <questdb.SenderTransaction.rollback>` methods.

While transactions that span multiple tables are not supported by QuestDB, you
can reuse the same sender for mutliple tables.

You can also create parallel transactions by creating multiple sender objects
across multiple threads.

.. _sender_auto_creation:

Table and Column Auto-creation
==============================

When sending data to a table that does not exist, the server will
create the table automatically.

This also applies to columns that do not exist.

The server will use the first row of data to determine the column types.

If the table already exists, the server will validate that the columns match
the existing table.

If you're using QuestDB enterprise you might need to grant further permissions
to the authenticated user.

.. code-block:: sql

    CREATE SERVICE ACCOUNT ingest;
    GRANT ilp, create table TO ingest;
    GRANT add column, insert ON all tables TO ingest;
    --  OR
    GRANT add column, insert ON table1, table2 TO ingest;

Read more setup details in the
`Enterprise quickstart <https://questdb.com/docs/guides/enterprise-quick-start/#4-ingest-data-influxdb-line-protocol>`_
and the `role-based access control <https://questdb.com/docs/operations/rbac/>`_ guides.

.. _sender_good_practices:

Good Practices
==============

Create tables in advance
------------------------

If you're not happy with the default :ref:`table auto creation <sender_auto_creation>`
logic, create the tables in advance. This will allow you to:

* Specify the column types explicitly.

* Configure de-duplication rules for the table.

Specify your own timestamps
---------------------------

Always specify your own timestamps using the ``at`` parameter.

If you use the ``ServerTimestamp`` option, QuestDB will not be able to
deduplicate rows, should you ever need to send them again.

Instead, if you don't have an a timestamp immediately available, use
``TimestampNanos.now()`` to set the timestamp to the current time.

This is lighter-weight than using a fully-fledged ``datetime.datetime`` object.

Prefer ILP/HTTP
---------------

Use the ILP/HTTP protocol instead of ILP/TCP for better error reporting and
transaction control. Use QWP/UDP only when you need fire-and-forget,
lowest-latency ingestion and can tolerate potential data loss.

.. _sender_tips_connection_reuse:

Reuse Sender Objects
--------------------

Create longer-lived sender objects, as these are not automatically pooled.

Instead of creating a new sender object for every request, create a single
sender object and reuse it across multiple requests.

.. code-block:: python

    from questdb import Sender

    conf = 'http::addr=localhost:9000;'
    with Sender.from_conf(conf) as sender:
        # Use the sender object for multiple requests
        sender.row(...)
        sender.row(...) # remember auto-flush may trigger after any row
        sender.row(...)
        sender.flush() # you can flush explicitly at any point too
        # ...
        sender.row(...)
        sender.dataframe(...) # auto-flush may trigger within a dataframe too
        sender.flush()

Use transactions
----------------

Use :ref:`transactions <sender_transaction>` if you want to ensure that a group
of rows is sent as a single transaction.

This feature will guarantee that the rows are sent to the server as one,
even if you're using auto-flushing.

Tune for Performance
--------------------

If you need better performance:

* Tune for larger batches of rows. Tweak the auto-flush settings, or
  call :func:`Sender.flush <questdb.Sender.flush>` less frequently.

* Use the :func:`Sender.dataframe <questdb.Sender.dataframe>` method To
  send dataframes instead of appending rows one by one.

* Try multi-threading: The ``Sender`` logic is designed to release the Python
  GIL whenever possible, so you should notice an uplift in performance if you
  were bottlenecked by network I/O.

* Avoid sending data which is very much out of order: The server will re-order
  data by timestamp as it arrives. This is generally cheap for data that only
  affects the recent past, but if you are sending data that is very much out of
  order (for example, from different days), you may want to consider
  re-ordering it before sending. For bulk data uploads of historical data,
  consider using the `CSV import <https://questdb.com/docs/guides/import-csv>`_
  feature for best performance.

.. _sender_advanced:

Advanced Usage
==============

Independent Buffers (legacy)
----------------------------

Buffers are managed internally by senders and are not part of the top-level
5.0 API. Legacy ILP/HTTP and ILP/TCP code that builds buffers explicitly and
flushes them with ``sender.flush(buffer)`` — including the multi-database
``flush(buf, clear=False)`` fan-out pattern — keeps working through the
deprecated ``questdb.ingress`` compatibility shim:

.. code-block:: python

    from questdb.ingress import Buffer, Sender, TimestampNanos

    buf = Buffer(protocol_version=2)
    buf.row(
        'trades',
        symbols={'symbol': 'ETH-USD', 'side': 'sell'},
        columns={'price': 2615.54, 'amount': 0.00044},
        at=TimestampNanos.now())

    conf = 'http::addr=localhost:9000;'
    with Sender.from_conf(conf) as sender:
        sender.flush(buf, transactional=True)

The ``transactional`` parameter is optional and defaults to ``False``.
When set to ``True``, the buffer is guaranteed to be committed as a single
transaction, but must only contain rows for a single table.

For new code, decouple serialization from sending by borrowing one sender per
thread from a :func:`questdb.connect` pool instead of sharing buffers.

Threading Considerations
------------------------

A sender object is not thread-safe, but can be shared between threads if you
take care of exclusive access (such as using a lock) yourself.

The simplest concurrency rule: borrow (or create) one sender per thread. With
QWP/WebSocket, :meth:`QuestDB.sender <questdb.QuestDB.sender>` makes this
cheap — each borrow leases a pooled connection.

Notice that the ``questdb`` python module is mostly implemented in native code
and is designed to release the Python GIL whenever possible, so you can expect
good performance in multi-threaded scenarios.

As an example, appending a dataframe to a buffer releases the GIL (unless any
of the columns reference python objects).

All network activity also fully releases the GIL.

.. _sender_http_performance:

Optimising HTTP Performance
---------------------------

The sender's network communication is implemented in native code and thus does
not require access to the GIL, allowing for true parallelism when used using
multiple threads.

For simplicity of design and best error feedback, the `.flush()` method blocks
until the server has acknowledged the data.

If you need to send a large number of smaller requests (in other words, if you
need to flush very frequently) or are in a high-latency network, you
can significantly improve performance by creating and sending using multiple
sender objects in parallel.

.. code-block:: python

    from questdb import Sender, TimestampNanos
    import pandas as pd
    from concurrent.futures import ThreadPoolExecutor
    import datetime

    def send_data(df):
        conf_string = 'http::addr=localhost:9000;'
        with Sender.from_conf(conf_string) as sender:
            sender.dataframe(
                df,
                table_name='trades',
                symbols=['symbol', 'side'],
                at='timestamp')

    dfs = [
            pd.DataFrame({
            'symbol': pd.Categorical(['ETH-USD', 'BTC-USD']),
            'side': pd.Categorical(['sell', 'sell']),
            'price': [2615.54, 39269.98],
            'amount': [0.00044, 0.001],
            'timestamp': pd.to_datetime(['2021-01-01', '2021-01-02'])}
            ),
            pd.DataFrame({
            'symbol': pd.Categorical(['BTC-USD', 'BTC-USD']),
            'side': pd.Categorical(['buy', 'sell']),
            'price': [39268.76, 39270.02],
            'amount': [0.003, 0.010],
            'timestamp': pd.to_datetime(['2021-01-03', '2021-01-03'])}
            ),
    ]

    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(send_data, df)
            for df in dfs]
        for future in futures:
            future.result()

For maxium performance you should also cache the sender objects and reuse them
across multiple requests, since internally they maintain a connection pool.

Sender Lifetime Control
-----------------------

Instead of using a ``with Sender .. as sender:`` block you can also manually
control the lifetime of the sender object.

.. code-block:: python

    from questdb import Sender

    conf = 'http::addr=localhost:9000;'
    sender = Sender.from_conf(conf)
    sender.establish()
    # ...
    sender.close()

The :func:`establish <questdb.Sender.establish>` method is needs to be
called exactly once, but the :func:`close <questdb.Sender.close>` method
is idempotent and can be called multiple times.


Table and Column Names
======================

The client will validate table and column names while constructing the buffer.

Table names and column names must not be empty and must adhere to the following:

Table Names
-----------

Cannot contain the following characters: ``?``, ``,``, ``'``, ``"``, ``\``,
``/``, ``:``, ``)``, ``(``, ``+``, ``*``, ``%``, ``~``, carriage return
(``\r``), newline (``\n``), null character (``\0``), and Unicode characters from
``\u{0001}`` to ``\u{000F}`` and ``\u{007F}``.
Additionally, the Unicode character for zero-width no-break space (UTF-8 BOM,
``\u{FEFF}``) is not allowed.

A dot (``.``) is allowed except at the start or end of the name,
and cannot be consecutive (e.g., ``valid.name`` is valid, but ``.invalid``,
``invalid.``, and ``in..valid`` are not).

Column Names
------------

Cannot contain the following characters: ``?``, ``.``, ``,``, ``'``, ``"``,
``\``, ``/``, ``:``, ``)``, ``(``, ``+``, ``-``, ``*``, ``%``, ``~``,
carriage return (``\r``), newline (``\n``), null character (``\0``),
and Unicode characters from ``\u{0001}`` to ``\u{000F}`` and ``\u{007F}``.
Like table names, the Unicode character for zero-width no-break space
(UTF-8 BOM, ``\u{FEFF}``) is not allowed.

Unlike table names, a dot (``.``) is not allowed in column names at all.

.. _sender_programmatic_construction:

Programmatic Construction
=========================

Sender Constructor
------------------

You can also specify the configuration parameters programmatically:

.. code-block:: python

    from questdb import Sender, Protocol
    from datetime import timedelta

    with Sender(Protocol.Tcp, 'localhost', 9009,
            auto_flush=True,
            auto_flush_interval=timedelta(seconds=10)) as sender:
        ...


See the :ref:`sender_conf` section for a full list of configuration parameters:
each configuration parameter can be passed as named arguments to the constructor.

Python type mappings:

* Parameters that require strings take a ``str``.

* Parameters that require numbers can also take an ``int``.

* Millisecond durations can take an ``int`` or a ``datetime.timedelta``.

* Any ``'on'`` / ``'off'`` / ``'unsafe_off'`` parameters can also be specified
  as a ``bool``.

* Paths can also be specified as a ``pathlib.Path``.

.. note::

    The constructor arguments have changed between 1.x and 2.x.
    If you are upgrading, take a look at the :ref:`changelog <changelog>`.

Customising ``.from_conf()`` and ``.from_env()``
------------------------------------------------

If you want to further customise the behaviour of the ``.from_conf()`` or
``.from_env()`` methods, you can pass additional parameters to these methods.
The parameters are the same as the ones for the ``Sender`` constructor, as
documented above.

For example, here is a :ref:`configuration string <sender_conf>` that is loaded
from an environment variable and then customised to specify a 10 second
auto-flush interval::

    export QDB_CLIENT_CONF='http::addr=localhost:9000;'

.. code-block:: python

    from questdb import Sender, Protocol
    from datetime import timedelta

    with Sender.from_env(auto_flush_interval=timedelta(seconds=10)) as sender:
        ...


.. _sender_protocol_version:

Protocol Version
================

Explicitly specifies the version of InfluxDB Line Protocol to use for sender.

Valid options are:

* ``protocol_version=1``
* ``protocol_version=2``
* ``protocol_version=3``
* ``protocol_version=auto`` (default, if unspecified)

Behavior details:

+----------------+--------------------------------------------------------------+
| Value          | Behavior                                                     |
+================+==============================================================+
|                | - Plain text serialization                                   |
|     ``1``      | - Compatible with InfluxDB servers                           |
|                | - No array type support                                      |
+----------------+--------------------------------------------------------------+
|     ``2``      | - Binary encoding for f64                                    |
|                | - Full support for array                                     |
|                | - requires QuestDB server version 9.0.0 or higher            |
+----------------+--------------------------------------------------------------+
|     ``3``      | - Decimal support                                            |
|                | - requires QuestDB server version 9.2.0 or higher            |
+----------------+--------------------------------------------------------------+
|                | - **HTTP/HTTPS**: Auto-detects server capability during      |
|     ``auto``   |   handshake (supports version negotiation)                   |
|                | - **TCP/TCPS**: Defaults to version 1 for compatibility      |
+----------------+--------------------------------------------------------------+

Here is a configuration string with ``protocol_version=3`` for ``TCP``::

    tcp::addr=localhost:9000;protocol_version=3;

See the :ref:`sender_conf_protocol_version` section for more details.

.. _sender_which_protocol:

Which protocol?
===============

The sender supports ``tcp``, ``tcps``, ``http``, ``https``, ``udp``,
``ws``, and ``wss`` protocols.

**You should prefer to use ILP/HTTP in most cases as it provides better
feedback on errors and transaction control.**

.. _sender_qwp_udp:

QWP/UDP
-------

QWP/UDP (``udp``) uses fire-and-forget UDP datagrams for lowest-latency
ingestion. It does not support authentication, TLS, or transactions. The
default port is 9007. See the :ref:`qwp_udp_example` example.

Key differences from ILP:

* **No delivery guarantee.** UDP datagrams may be dropped under load or network
  congestion. There is no retry mechanism and the server sends no
  acknowledgement. Use ILP/HTTP if you need reliable delivery.

* **No error feedback.** If a row contains invalid data (e.g. wrong column type
  for an existing table), the server silently drops it. With ILP/HTTP you would
  get an error response.

* **Buffer inspection.** ``bytes(sender)`` returns ``b''`` because QWP encoding
  is deferred to flush. ``len(sender)`` returns an estimated size hint, not the
  exact serialized byte count.

* **Auto-flush.** ``auto_flush_bytes`` defaults to ``max_datagram_size`` (1400
  by default) so that rows are flushed when the buffer approaches a single
  datagram's worth of data. Rows and interval thresholds work the same as ILP.

* **Datagram size limit.** A single row that exceeds ``max_datagram_size`` will
  raise :class:`QuestDBError` at flush time. Configure ``max_datagram_size`` via
  the constructor or :ref:`configuration string <sender_conf>`.

* **No protocol version.** QWP has its own versioning. The ``protocol_version``
  parameter and property are not applicable and will raise an error.

.. _sender_qwp_ws:

QWP/WebSocket
-------------

QWP/WebSocket (``ws``, or ``wss`` for TLS) is an acknowledged streaming
transport. Each flush publishes a frame identified by a monotonically
increasing **frame sequence number (FSN)**; the server acknowledges frames as
it durably applies them, so the client can confirm delivery.

* **Confirming delivery.** :func:`Sender.flush_and_get_fsn` flushes and returns
  the FSN of the published frame; :func:`Sender.flush_and_keep_and_get_fsn`
  does the same without clearing the buffer. :func:`Sender.await_acked_fsn`
  blocks until a given FSN is acknowledged (or a timeout elapses), and
  :func:`Sender.acked_fsn` / :func:`Sender.published_fsn` report progress
  without blocking.

* **Progress modes.** With the default ``qwp_ws_progress=background``,
  acknowledgements are progressed on a background thread. With
  ``qwp_ws_progress=manual``, the application must call
  :func:`Sender.drive_once` (or one of the flush/await methods) to pump the
  connection.

* **Server diagnostics.** Per-frame server feedback is delivered to the
  ``qwp_ws_error_handler`` callback, or polled via
  :func:`Sender.poll_qwp_ws_error` as :class:`QwpWsError` values
  (:func:`Sender.qwp_ws_errors_dropped` reports how many were dropped when no
  handler kept up). A diagnostic with a ``halt`` policy is terminal: the next
  sender call raises :class:`QuestDBServerRejectionError`. The handler must
  not call back into the same sender, must be cheap and non-blocking, and —
  under ``qwp_ws_progress=background`` — may run on a background thread.

* **Draining on close.** :func:`Sender.close_drain` waits for outstanding
  frames to be acknowledged before closing.

.. _query_egress:

Querying data
=============

:class:`QuestDB` reads query results back over the QWP/WebSocket read endpoint.
:meth:`QuestDB.query` returns a single-use :class:`QueryResult` that streams rows
as Arrow record batches::

    with questdb.connect('ws::addr=localhost:9000;') as db:
        with db.query('SELECT * FROM trades WHERE ts > $1') as result:
            df = result.to_pandas()

A :class:`QueryResult` can be materialised with ``to_arrow`` / ``to_pandas`` or
streamed batch-by-batch with ``iter_arrow`` / ``iter_pandas``. ``to_arrow`` /
``iter_arrow`` (and ``to_pandas`` / ``iter_pandas`` with ``dtype_backend`` or
``types_mapper``) require pyarrow; the default ``to_pandas`` / ``iter_pandas``
are pyarrow-free. It also implements the Arrow C stream PyCapsule protocol
(``__arrow_c_stream__``), so ``polars.from_arrow(result)`` or
``duckdb.from_arrow(result)`` consume it directly without pyarrow installed.
Each result is consumed once; call :func:`QueryResult.cancel` to ask the server
to stop streaming and :func:`QueryResult.close` to release resources.

``SYMBOL`` columns: ``to_polars`` / ``to_pandas`` build the categorical directly
(connection dictionary interned once, no per-row remap). ``to_arrow`` /
``iter_arrow`` / ``__arrow_c_stream__`` emit a generic Arrow form whose
per-batch ``SYMBOL`` dictionary is compacted to the values each batch uses,
which a generic consumer reconciles. So when the target is a polars / pandas
frame, the dedicated methods avoid the re-reconciliation that
``polars.from_arrow(result)`` / ``to_arrow().to_pandas()`` pay on
``SYMBOL``-heavy results.

The same :class:`QuestDB` can ingest dataframes through the pooled columnar QWP
path with :meth:`QuestDB.dataframe`. Dataframe ingestion always uses the direct
(non-store-and-forward) column sender, independent of ``sf_dir``, and returns
once the whole frame is committed (``AckLevel::Ok``). On a transient connection
failure the frame is re-sent from the caller's DataFrame only when the failed
operation is provably not delivered. If the native client reports delivery as
in doubt, or an intermediate commit checkpoint on a large frame has already
landed, the error surfaces immediately and a committed prefix may remain in the
table. Retrying an in-doubt operation can duplicate rows unless the table has
appropriate ``DEDUP UPSERT KEYS``.

ILP/HTTP is available from:

* QuestDB 7.3.10 and later.
* QuestDB Enterprise 1.2.7 and later.

ILP/HTTP Also supports :ref:`protocol version <sender_protocol_version>`
auto-detection.

+----------------+--------------------------------------------------------------+
| Protocol       | Protocol version auto-detection                              |
+================+==============================================================+
| ILP/HTTP       | **Yes**: The client will communcate to the server using the  |
|                | latest version supported by both client and the server.      |
+----------------+--------------------------------------------------------------+
| ILP/TCP        | **No**: You need to                                          |
|                | :ref:`configure <sender_conf_protocol_version>`              |
|                | ``protocol_version=N`` to to match a version supported by    |
|                | the server.                                                  |
+----------------+--------------------------------------------------------------+
| QWP/UDP        | **N/A**: QWP uses its own wire format. The                   |
|                | ``protocol_version`` setting is not applicable.              |
+----------------+--------------------------------------------------------------+

.. note::

    The client will disable features that require a newer
    protocol versions than the one used to communicate with the server.


Since TCP does not block for a response it is useful for high-throughput
scenarios in higher latency networks or on older versions of QuestDB which do
not support ILP/HTTP quite yet.

It should be noted that you can achieve equivalent or better performance to TCP
with HTTP by :ref:`using multiple sender objects in parallel <sender_http_performance>`.

Either way, you can easily switch between the two protocols by changing:

* The ``<protocol>`` part of the :ref:`configuration string <sender_conf>`.

* The port number (ILP/TCP default is 9009, ILP/HTTP default is 9000,
  QWP/UDP default is 9007).

* Any :ref:`authentication parameters <sender_conf_auth>` such as ``username``, ``token``, et cetera.
