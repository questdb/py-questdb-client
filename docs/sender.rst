.. _sender:

=====================
Sending Data over ILP
=====================

Overview
========

The :class:`Sender <questdb.ingress.Sender>` class is a client that inserts
rows into QuestDB via the
`ILP protocol <https://questdb.io/docs/reference/api/ilp/overview/>`_, with
support for both ILP over TCP and the newer and recommended ILP over HTTP.
The sender also supports TLS and authentication.

.. code-block:: python

    from questdb.ingress import Sender, TimestampNanos
    import pandas as pd

    conf = 'http::addr=localhost:9000;'
    with Sender.from_conf(conf) as sender:
        # Adding by rows
        sender.row(
            'trades',
            symbols={'symbol': 'ETH-USD', 'side': 'sell'},
            columns={'price': 2615.54, 'amount': 0.00044},
            at=TimestampNanos.now())
        # It is highly recommended to auto-flush or to flush in batches,
        # rather than for every row
        sender.flush()

        # Whole dataframes at once
        df = pd.DataFrame({
            'symbol': pd.Categorical(['ETH-USD', 'BTC-USD']),
            'side': pd.Categorical(['sell', 'sell']),
            'price': [2615.54, 39269.98],
            'amount': [0.00044, 0.001],
            'timestamp': pd.to_datetime(['2021-01-01', '2021-01-02'])})

        sender.dataframe(df, table_name='trades', at='timestamp')

The ``Sender`` object holds an internal buffer which will be flushed and sent
at when the ``with`` block ends.

You can read more on :ref:`sender_preparing_data` and :ref:`sender_flushing`.

Constructing the Sender
=======================

From Configuration
------------------

The ``Sender`` class is generally initialized from a
:ref:`configuration string <sender_conf>`.

.. code-block:: python

    from questdb.ingress import Sender

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
:func:`Sender.from_conf <questdb.ingress.Sender.from_conf>` method,
but moving it to an environment variable is more secure and allows you to avoid
hardcoding sensitive information such as passwords and tokens in your code.

.. code-block:: python

    from questdb.ingress import Sender

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

You can append as many rows as you like by calling the
:func:`Sender.row <questdb.ingress.Sender.row>` method. The full method arguments are
documented in the :func:`Buffer.row <questdb.ingress.Buffer.row>` method.

Appending Pandas Dataframes
---------------------------

The sender can also append data from a Pandas dataframe.

This is `orders of magnitude <https://github.com/questdb/py-tsbs-benchmark/blob/main/README.md>`_
faster than appending rows one by one.

.. literalinclude:: ../examples/pandas_basic.py
   :language: python

For more details see :func:`Sender.dataframe <questdb.ingress.Sender.dataframe>`
and for full argument options see
:func:`Buffer.dataframe <questdb.ingress.Buffer.dataframe>`.

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

    from questdb.ingress import Sender, TimestampNanos
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

Populating Timestamps
---------------------

The ``at`` parameter of the ``row`` and ``dataframe`` methods is used to specify
the timestamp of the rows.

Set by client
~~~~~~~~~~~~~

It can be either a :class:`TimestampNanos <questdb.ingress.TimestampNanos>`
object or a
`datetime.datetime <https://docs.python.org/3/library/datetime.html>`_ object.

In case of dataframes you can also specify the timestamp column name or index.
If so, the column type should be a Pandas ``datetime64``, with or without
timezone information.

Note that all timestamps in QuestDB are stored as microseconds since the epoch,
without timezone information. Any timezone information is dropped when the data
is appended to the ILP buffer.

.. _sender_server_timestamp:

Set by server
~~~~~~~~~~~~~

If you prefer, you can specify ``at=ServerTimestamp`` which will instruct
QuestDB to set the timestamp on your behalf for each row as soon as it's
received by the server.

.. code-block:: python

    from questdb.ingress import Sender, ServerTimestamp

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
:func:`Sender.flush <questdb.ingress.Sender.flush>` will send the buffered data
to QuestDB, and clear the buffer.

Flushing can be done explicitly or automatically.

Explicit Flushing
-----------------

An explicit call to :func:`Sender.flush <questdb.ingress.Sender.flush>` will
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

.. _sender_protocol_version:

Protocol Version
================

Specifies the version of InfluxDB Line Protocol to use for sender.

Valid options are:

* ``1`` - Text-based format compatible with InfluxDB database when used over HTTP.
* ``2`` - Array support and binary format serialization for 64-bit floats (version specific to QuestDB).
* ``auto`` (default) - Automatic version selection based on connection type.

Behavior details:

^^^^^^^^^^^^^^^^^

+----------------+--------------------------------------------------------------+
| Value          | Behavior                                                     |
+================+==============================================================+
|                | - Plain text serialization                                   |
|     ``1``      | - Compatible with InfluxDB servers                           |
|                | - No array type support                                      |
+----------------+--------------------------------------------------------------+
|     ``2``      | - Binary encoding for f64                                    |
|                | - Full support for array                                     |
+----------------+--------------------------------------------------------------+
|                | - **HTTP/HTTPS**: Auto-detects server capability during      |
|     ``auto``   |   handshake (supports version negotiation)                   |
|                | - **TCP/TCPS**: Defaults to version 1 for compatibility      |
+----------------+--------------------------------------------------------------+

Here is a configuration string with ``protocol_version=2`` for ``TCP``:

``tcp::addr=localhost:9000;protocol_version=2;``

See the :ref:`sender_conf_protocol_version` section for more details.

.. note::
    Protocol version ``2`` requires QuestDB server version 8.4.0 or higher.

Error Reporting
===============

**TL;DR: Use HTTP for better error reporting**

The sender will do its best to check for errors before sending data to the
server.

When using the HTTP protocol, the server will send back an error message if
the data is invalid or if there is a problem with the server. This will be
raised as an :class:`IngressError <questdb.ingress.IngressError>` exception.

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
:func:`commit <questdb.ingress.SenderTransaction.commit>` or the
:func:`rollback <questdb.ingress.SenderTransaction.rollback>` methods.

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
`Enterprise quickstart <https://questdb.io/docs/guides/enterprise-quick-start/#4-ingest-data-influxdb-line-protocol>`_
and the `role-based access control <https://questdb.io/docs/operations/rbac/>`_ guides.

.. _sender_advanced:

Advanced Usage
==============

Independent Buffers
-------------------

All examples so far have shown appending data to the sender's internal buffer.

You can also create independent buffers and send them independently.

This is useful for more complex applications whishing to decouple the
serialisation logic from the sending logic.

Note that the sender's auto-flushing logic will not apply to independent
buffers.

.. code-block:: python

    from questdb.ingress import Buffer, Sender, TimestampNanos

    buf = Buffer()
    buf.row(
        'trades',
        symbols={'symbol': 'ETH-USD', 'side': 'sell'},
        columns={'price': 2615.54, 'amount': 0.00044},
        at=TimestampNanos.now())
    buf.row(
        'trades',
        symbols={'symbol': 'BTC-USD', 'side': 'sell'},
        columns={'price': 39269.98, 'amount': 0.001},
        at=TimestampNanos.now())

    conf = 'http::addr=localhost:9000;'
    with Sender.from_conf(conf) as sender:
        sender.flush(buf, transactional=True)

The ``transactional`` parameter is optional and defaults to ``False``.
When set to ``True``, the buffer is guaranteed to be committed as a single
transaction, but must only contain rows for a single table.

You should not mix using a transaction block with flushing an independent buffer transactionally.

Multiple Databases
------------------

Handling buffers explicitly is also useful when sending data to multiple
databases via the ``.flush(buf, clear=False)`` option.

.. code-block:: python

    from questdb.ingress import Buffer, Sender, TimestampNanos

    buf = Buffer()
    buf.row(
        'trades',
        symbols={'symbol': 'ETH-USD', 'side': 'sell'},
        columns={'price': 2615.54, 'amount': 0.00044},
        at=TimestampNanos.now())

    conf1 = 'http::addr=db1.host.com:9000;'
    conf2 = 'http::addr=db2.host.com:9000;'
    with Sender.from_conf(conf1) as sender1, Sender.from_conf(conf2) as sender2:
        sender1.flush(buf1, clear=False)
        sender2.flush(buf2, clear=False)

    buf.clear()

This uses the ``clear=False`` parameter which otherwise defaults to ``True``.

Threading Considerations
------------------------

Neither buffer API nor the sender object are thread-safe, but can be shared
between threads if you take care of exclusive access (such as using a lock)
yourself.

Independent buffers also allows you to prepare separate buffers in different
threads and then send them later through a single exclusively locked sender.

Alternatively you can also create multiple senders, one per thread.

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

    from questdb.ingress import Sender, TimestampNanos
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

    from questdb.ingress import Sender

    conf = 'http::addr=localhost:9000;'
    sender = Sender.from_conf(conf)
    sender.establish()
    # ...
    sender.close()

The :func:`establish <questdb.ingress.Sender.establish>` method is needs to be
called exactly once, but the :func:`close <questdb.ingress.Sender.close>` method
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

    from questdb.ingress import Sender, Protocol
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

    from questdb.ingress import Sender, Protocol
    from datetime import timedelta

    with Sender.from_env(auto_flush_interval=timedelta(seconds=10)) as sender:
        ...


.. _sender_which_protocol:

ILP/TCP or ILP/HTTP
===================

The sender supports ``tcp``, ``tcps``, ``http``, and ``https`` protocols.

You should prefer to use the new ILP/HTTP protocol instead of ILP/TCP in most
cases as it provides better feedback on errors and transaction control.

ILP/HTTP is available from:

* QuestDB 7.3.10 and later.
* QuestDB Enterprise 1.2.7 and later.

Since TCP does not block for a response it is useful for high-throughput
scenarios in higher latency networks or on older versions of QuestDB which do
not support ILP/HTTP quite yet.

It should be noted that you can achieve equivalent or better performance to TCP
with HTTP by :ref:`using multiple sender objects in parallel <sender_http_performance>`.

Either way, you can easily switch between the two protocols by changing:

* The ``<protocol>`` part of the :ref:`configuration string <sender_conf>`.

* The port number (ILP/TCP default is 9009, ILP/HTTP default is 9000).

* Any :ref:`authentication parameters <sender_conf_auth>` such as ``username``, ``token``, et cetera.
