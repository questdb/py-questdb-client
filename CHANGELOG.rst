.. _changelog:


Changelog
=========

5.0.1 (unreleased)
------------------

- ``row()`` on QWP senders now supports UUID, IPV4, BINARY, CHAR, DATE,
  LONG256, and GEOHASH columns. Pass ``uuid.UUID``,
  ``ipaddress.IPv4Address``, or bytes-like values directly, and use the new
  :class:`Char <questdb.Char>`, :class:`DateMillis <questdb.DateMillis>`,
  :class:`Long256 <questdb.Long256>`, and :class:`Geohash <questdb.Geohash>`
  wrappers for the rest. These types require QuestDB 10 or newer. On ILP
  senders these values now raise
  :class:`QuestDBError <questdb.QuestDBError>` (``InvalidApiCall``) instead
  of ``TypeError``.
- DataFrame BINARY columns now accept ``bytearray`` and ``memoryview`` cells
  in addition to ``bytes``.
- Applications may now create a ``QueryResult`` on one thread and process it on
  another, including through its Arrow stream. Hand it off with normal thread
  synchronization and never use it from two threads at once. If it came from a
  ``PooledReader``, keep that reader on its original thread until processing
  finishes.
- WebSocket connections keep a dictionary of ``SYMBOL`` values to avoid sending
  repeated text in full. Repeated values do not grow it, but a long-lived
  connection fills it after 2,000,000 distinct values or 256 MiB of symbol
  text. The client then raises
  :class:`QuestDBError <questdb.QuestDBError>` with ``code`` set to
  ``QuestDBErrorCode.SymbolDictFull``. Pooled clients replace the connection;
  standalone senders should call ``close_drain()`` and reconnect. Before
  retrying, check ``err.in_doubt``; when it is true, some rows may already be
  stored.
- If a DataFrame load fails, batches waiting in memory are now discarded. Most
  loads commit once at the end, but very large Arrow loads may commit an earlier
  checkpoint. If a later batch fails, that prefix from the same DataFrame call
  remains, so retrying the whole DataFrame can duplicate rows.
- When ``sf_dir`` enables disk-backed delivery, shutdown now obeys its configured
  timeout and recovery is safer after a crash or an incomplete final write.
- If a ``ws::`` or ``wss::`` configuration contains ``max_in_flight`` or
  ``in_flight_window``, remove it. These options are no longer supported and now
  raise :class:`QuestDBError <questdb.QuestDBError>` with ``code`` set to
  ``QuestDBErrorCode.ConfigError`` during startup.

5.0.0 (2026-07-27)
------------------

Highlights
~~~~~~~~~~

- One QWP/WebSocket handle for everything: :func:`questdb.connect` returns
  a pooled, thread-safe :class:`QuestDB <questdb.QuestDB>` that streams
  rows, bulk-loads DataFrames, and runs SQL queries.
- Acknowledged, durable ingestion: rows publish into a store-and-forward
  queue with automatic reconnect and replay; opt into disk persistence
  with ``sf_dir`` to survive client restarts.
- Server rejections are never silent: every rejection is pushed to a
  handler (or the ``questdb`` logger), with terminal failures also raised
  from the affected sender.
- Query egress: SQL with bind parameters streamed back as Arrow record
  batches into pandas, polars, or pyarrow — pyarrow-free by default.
- Columnar DataFrame ingestion for pandas / polars / pyarrow and any
  Arrow C Data Interface source.
- Requires QuestDB server version 10 or later


Breaking changes
~~~~~~~~~~~~~~~~~

- The public API now lives at the top level of the ``questdb`` package:
  ``from questdb import Sender, QuestDB, QueryResult, ...`` (the compiled
  module is now the private ``questdb._client``). ``questdb.ingress`` remains
  as a deprecated compatibility shim re-exporting the 4.x ILP/HTTP and
  ILP/TCP surface (``Sender``, ``Buffer``, ``Protocol``, ``IngressError``,
  ``VERSION``, ...) with a single import-time ``DeprecationWarning``; new
  code imports from ``questdb``.
- The QWP/WebSocket entry point is :func:`questdb.connect`, returning a
  :class:`QuestDB <questdb.QuestDB>` handle. The handle lends
  row-building senders (:meth:`QuestDB.sender <questdb.QuestDB.sender>`), bulk-loads DataFrames
  (:meth:`QuestDB.dataframe <questdb.QuestDB.dataframe>`), runs queries (:meth:`QuestDB.query <questdb.QuestDB.query>`) and
  lends reader leases (:meth:`QuestDB.reader <questdb.QuestDB.reader>`). Besides the configuration
  string, :func:`questdb.connect` also accepts the equivalent keywords
  (``host=``, ``port=``, ``tls=`` plus any further settings).
- The sender borrowed from the pool is a lean pooled row sender: it
  exposes ``row()``, ``dataframe()``, ``flush()``, ``wait()``, the FSN
  delivery receipts (``flush_and_get_fsn``,
  ``flush_and_keep_and_get_fsn``, ``published_fsn`` / ``acked_fsn``,
  ``await_acked_fsn``), lease-scoped rejection polling (``poll_error``,
  ``error_events_dropped``) and ``close()``, and is not a ``Sender``
  subclass — the standalone-only API such as ``establish()``, manual
  progress or explicit buffers simply does not exist on it. One
  concurrency rule applies everywhere: borrow one sender per thread.
  Pooled row senders auto-flush by default at 1,000 rows, 100 milliseconds,
  or a byte threshold derived from the QWP frame cap; ``auto_flush=off``
  restores fully explicit publishing. Auto-flush publishes without waiting
  for an acknowledgement; use ``flush(wait=True)`` or ``wait()`` as the
  acknowledgement barrier.
  The lease types are exported as :class:`questdb.PooledSender` and
  :class:`questdb.PooledReader` for ``isinstance`` checks and type
  annotations; instances are only ever constructed by
  :meth:`QuestDB.sender <questdb.QuestDB.sender>` and :meth:`QuestDB.reader <questdb.QuestDB.reader>`.
- ``Buffer`` is no longer part of the top-level API. Buffers are managed
  internally by senders; for concurrent serialization borrow more senders.
  Legacy explicit-buffer code keeps working through the ``questdb.ingress``
  shim.
- Over ``ws::`` / ``wss::``, DataFrame bulk loads use the direct columnar
  path — a database operation, independent of ``sf_dir``, committed on
  return, with precise ``in_doubt`` failure semantics. Available as
  :meth:`QuestDB.dataframe <questdb.QuestDB.dataframe>` on the handle, as ``dataframe()`` on a pooled
  sender from :meth:`QuestDB.sender <questdb.QuestDB.sender>` (which borrows a direct connection from
  the pool for that call), and as ``dataframe()`` on any standalone
  :class:`Sender <questdb.Sender>` (which opens a poolless direct connection from the sender's
  own configuration for that call, carrying its auth/TLS regardless of how
  the sender was built). In all cases the direct load has no ordering
  relationship with rows buffered via ``row()`` and does not flush them.
  Over ILP/HTTP, ILP/TCP and QWP/UDP, ``Sender.dataframe()`` remains fully
  supported and is no longer deprecated (over UDP it serializes row by row
  into fire-and-forget datagrams, with the same delivery caveats as
  ``row()``).
- ``IngressError`` is renamed to :class:`QuestDBError <questdb.QuestDBError>` and
  ``IngressErrorCode`` to :class:`QuestDBErrorCode <questdb.QuestDBErrorCode>` (the names now also cover
  the query egress path, which raises the same types).
- The error model is unified across ingestion and queries. Query/reader
  failures now surface their own :class:`QuestDBErrorCode <questdb.QuestDBErrorCode>` members
  (``HandshakeError``, ``UnsupportedServer``, ``ProtocolError``,
  ``InvalidBind``, ``ServerSchemaMismatch``, ``ServerParseError``,
  ``ServerInternalError``, ``ServerSecurityError``, ``LimitExceeded``,
  ``ServerLimitExceeded``, ``SchemaDrift``, ``NoSchema``, ``ArrowExport``)
  rather than being bucketed under ``ServerFlushError``. Existing member
  names are unchanged and the numeric codes 0–13 are identical to 4.x. The
  only shifted 4.x member is ``BadDataFrame``, whose ``.value`` moved from
  14 to ``0x10000`` (it is a Python-only sentinel with no backing FFI
  code); raw value 14 now belongs to the new ``ServerRejection``. Compare
  by member identity, e.g. ``err.code is QuestDBErrorCode.BadDataFrame`` —
  that is the stable contract, not the integer value.
- Catching errors from the ``questdb.ingress`` shim is broader than in 4.x:
  ``IngressError`` is an alias of the unified :class:`QuestDBError <questdb.QuestDBError>`, so
  ``except IngressError`` now also catches query, pool and egress failures
  raised through the same handle.
- Pre-epoch ``datetime`` values passed as scalars (``at=`` or ``row()``
  column values) now floor toward negative infinity when converted to an
  epoch timestamp. 4.1.0 truncated toward zero, encoding pre-epoch instants
  with a fractional second one second too high; such values now serialize
  lower than before, at the correct instant.
- :meth:`TimestampNanos.from_datetime <questdb.TimestampNanos.from_datetime>` now raises ``ValueError`` for
  datetimes outside the signed 64-bit nanosecond range (1677-09-21 to
  2262-04-11 UTC); 4.x silently overflowed.
- Millisecond-duration parameters given as a negative sub-second
  ``datetime.timedelta`` (e.g. ``timedelta(milliseconds=-500)``) now raise
  ``ValueError``; 4.x silently mis-accepted them as positive durations.
- Naive (timezone-unaware) timestamps are interpreted as UTC everywhere:
  DataFrame column cells, scalar ``at=`` values, ``datetime`` values passed
  to ``row()``, ``TimestampNanos.from_datetime`` /
  ``TimestampMicros.from_datetime``, and query bind parameters. 4.x
  interpreted naive *scalars* in the process-local timezone
  (``datetime.timestamp()`` semantics) — the first naive conversion on such
  a path now emits a one-per-process ``UserWarning``. Note that
  ``datetime.now()`` is your local wall clock: for "now", use
  ``TimestampNanos.now()`` or ``datetime.now(timezone.utc)``.

Features
~~~~~~~~

QWP Ingestion Protocol
**********************

Adds support for the QuestDB Wire Protocol (QWP) alongside the existing
ILP transports.

- **QWP/UDP** (``udp::``): fire-and-forget datagram ingestion,
  defaulting to port 9007. New configuration keys ``max_datagram_size``
  and ``multicast_ttl``; ``protocol_version`` does not apply.
- **QWP/WebSocket** (``ws::`` / ``wss::``): acknowledged streaming
  ingestion with frame-sequence-number (FSN) tracking. New ``Sender``
  methods ``flush_and_get_fsn``, ``flush_and_keep_and_get_fsn``,
  ``published_fsn``, ``acked_fsn``, ``await_acked_fsn``, ``drive_once``,
  ``poll_error``, ``error_events_dropped`` and ``close_drain``.
  Server diagnostics are reported through an ``error_handler``
  callback or polled as :class:`SenderError <questdb.SenderError>` values (classified by the
  :class:`SenderErrorCategory <questdb.SenderErrorCategory>`, :class:`SenderErrorPolicy <questdb.SenderErrorPolicy>` and
  :class:`QwpWsProgress <questdb.QwpWsProgress>` enums); terminal server rejections raise
  :class:`QuestDBServerRejectionError <questdb.QuestDBServerRejectionError>`.

Additional configuration keys ``tls_roots_password``,
``retry_max_backoff_millis`` and ``qwp_ws_progress`` are also accepted.
Every new key is equally available as a ``Sender`` / ``Sender.from_conf`` /
``Sender.from_env`` keyword argument (``max_datagram_size``,
``multicast_ttl``, ``tls_roots_password``, ``retry_max_backoff``,
``qwp_ws_progress`` and ``error_handler``).

The pooled :class:`QuestDB <questdb.QuestDB>` handle exposes :meth:`QuestDB.sender <questdb.QuestDB.sender>`, returning
a context-managed row sender for row-at-a-time QWP/WebSocket ingestion.
Row builders, whole-dataframe ingestion, and queries share one
``questdb.connect("ws::...")`` configuration without exposing separate row
and column sender pools. The pooled sender's ``dataframe()`` routes to the
same direct columnar path as :meth:`QuestDB.dataframe <questdb.QuestDB.dataframe>`, borrowing a direct
connection from the pool for that call only (prefer calling
:meth:`QuestDB.dataframe <questdb.QuestDB.dataframe>` directly).

Store-and-forward delivery
**************************

Pooled row senders publish into a local store-and-forward queue: ``flush()``
returns on local acceptance and a background runner delivers the frames,
reconnecting and replaying across transient failures within the
``reconnect_*`` budget. With the opt-in ``sf_dir`` directory the queue is
disk-backed per sender slot and unacknowledged frames are replayed after a
client restart (``sender_id`` names the slots; ``sf_max_segment_bytes`` /
``sf_max_total_bytes`` / ``sf_append_deadline_millis`` /
``close_flush_timeout_millis`` bound it — see the configuration reference).
``flush(wait=True)`` / ``wait()`` observe the server acknowledgement;
``request_durable_ack=on`` upgrades acknowledgements to durable ones on
servers that support them.

Server rejections are pushed to the ``error_handler`` callable
passed to :func:`questdb.connect` (one :class:`questdb.SenderError` per
rejection, on a dedicated dispatcher thread; totals via
:attr:`QuestDB.error_events_delivered <questdb.QuestDB.error_events_delivered>` /
:attr:`QuestDB.error_events_dropped <questdb.QuestDB.error_events_dropped>`). Without a handler every
rejection is logged through the ``questdb`` logger, so rejections are
never silent. ``PooledSender.flush(wait=True)`` / ``wait()`` are pure ack
barriers scoped to the lease's own publications: only a terminal
connection failure raises there.

Query connections take their own keys — result-set ``compression``
(Zstandard) and the mid-query ``failover_*`` / ``target`` policy — all
documented in the configuration reference.

Query Egress
************

Adds :meth:`QuestDB.query <questdb.QuestDB.query>`, returning a
:class:`QueryResult <questdb.QueryResult>` that streams rows as Arrow record batches over the
QWP/WebSocket read endpoint. Queries take positional bind parameters
matching ``$1``..``$N`` placeholders —
``db.query('SELECT * FROM trades WHERE ts > $1 AND sym = $2', [ts, 'BTC-USD'])``
— covering ``None`` (SQL NULL), ``bool``, ``int``, ``float``, ``str``,
``datetime.datetime``, :class:`TimestampMicros <questdb.TimestampMicros>`, :class:`TimestampNanos <questdb.TimestampNanos>`
and ``uuid.UUID``; always prefer binds over interpolating values into the
SQL text. Results can be consumed via ``to_arrow``,
``to_pandas``, ``to_polars``, ``iter_arrow``, ``iter_pandas``,
``iter_polars`` or the Arrow C stream PyCapsule protocol
(``__arrow_c_stream__``). ``to_polars`` / ``iter_polars`` use pyarrow to
buffer failover-safe batches; ``__arrow_c_stream__`` (consumed as
``polars.from_arrow(result)``) is the pyarrow-free polars path. SYMBOL
columns are dictionary-encoded on the wire and map to a pandas
``Categorical`` (``to_pandas`` / ``iter_pandas``) or a polars
``Categorical`` (``to_polars`` / ``iter_polars``), the latter sharing one
persistent ``Categories`` identity across streamed batches so
``polars.concat`` stitches them without a categories-mismatch error.

``to_pandas`` / ``iter_pandas`` default to a native (no-pyarrow) build
straight from the QWP column buffers: a nullable integer column becomes a
pandas nullable ``Int*`` when it contains nulls and plain numpy otherwise,
``double`` stays numpy with ``NaN``, ``TIMESTAMP`` → ``datetime64``, and
the QuestDB column kinds are recorded in ``df.attrs['questdb']`` for a type
round-trip through :meth:`QuestDB.dataframe <questdb.QuestDB.dataframe>`. Pass
``dtype_backend="pyarrow"`` / ``"numpy_nullable"`` (or ``types_mapper=``)
to select the pyarrow-backed conversion instead.

:class:`QuestDB <questdb.QuestDB>` is a context manager and exposes :meth:`QuestDB.close <questdb.QuestDB.close>` and
:meth:`QuestDB.reap_idle <questdb.QuestDB.reap_idle>` for pooled-connection lifecycle management.
Pool sizing follows the Java client's configuration keys:
``sender_pool_min`` / ``sender_pool_max`` (ingestion, defaults 1/4),
``query_pool_min`` / ``query_pool_max`` (readers, defaults 1/4),
``acquire_timeout_ms`` (default 5000 — how long a borrow waits at the cap
before failing; ``0`` fails fast), ``idle_timeout_ms`` (default 60000 —
after which above-minimum idle connections are reaped) and ``pool_reap``
(``auto`` | ``manual``, default ``auto`` — whether a background thread
reaps idle connections, or the application calls :meth:`QuestDB.reap_idle <questdb.QuestDB.reap_idle>`
on its own cadence).

- Closing or abandoning a :class:`QueryResult <questdb.QueryResult>` now releases its pooled
  connection immediately instead of at garbage collection; abandoning a
  result without closing it emits a ``ResourceWarning``.

- :meth:`QuestDB.reader <questdb.QuestDB.reader>` returns a reader lease
  (the read-side twin of :meth:`QuestDB.sender <questdb.QuestDB.sender>`) that borrows one pooled
  reader connection for its lifetime and runs queries on it sequentially —
  ``with db.reader() as r: r.query(sql)`` — optionally keeping the
  connection's SYMBOL dictionary warm across queries via
  ``reset_symbol_dict=False``.

- :meth:`QuestDB.execute <questdb.QuestDB.execute>` (also on the reader
  lease) runs a statement, drains whatever it returns, and hands the
  pooled connection back in one call —
  ``db.execute('CREATE TABLE ...')`` — replacing the
  ``with db.query(ddl) as r: r.to_pandas()`` drain ceremony for DDL and
  DML. Statement output is discarded and ``None`` is returned; use
  :meth:`QuestDB.query <questdb.QuestDB.query>` when you want the result.

Columnar DataFrame Ingestion
****************************

Adds :meth:`QuestDB.dataframe <questdb.QuestDB.dataframe>`, ingesting pandas / polars / pyarrow and
any Arrow C Data Interface object over QWP/WebSocket. A
``schema_overrides`` keyword reclassifies columns as ``symbol``,
``ipv4``, ``char`` or ``geohash`` (e.g. ``{'addr': 'ipv4', 'loc':
('geohash', 20)}``); it requires fully Arrow-backed input — on input that
falls back to the NumPy planner it raises
:class:`UnsupportedDataFrameShapeError <questdb.UnsupportedDataFrameShapeError>` rather than being silently
ignored. A ``max_rows_per_batch`` keyword (default 16384) bounds the
rows sent per columnar batch.

The designated-timestamp argument ``at`` is the timestamp column itself,
given by name (``str``) or position (``int``), a fixed
``TimestampNanos`` / ``datetime`` shared by every row, or the explicit
``ServerTimestamp`` sentinel. A frame
produced by :meth:`QueryResult.to_pandas <questdb.QueryResult.to_pandas>` round-trips back to the same
QuestDB column types automatically: the kinds recorded in
``df.attrs['questdb']`` and pandas nullable extension dtypes are honoured.

Connection Observability
************************

A ``connection_listener`` callable (accepted by :func:`questdb.connect`,
:meth:`QuestDB.from_conf <questdb.QuestDB.from_conf>` and the ``Sender`` constructors, alongside a
``connection_event_inbox_capacity`` bound) receives one
:class:`ConnectionEvent <questdb.ConnectionEvent>` per connection-state transition — initial
connect, per-endpoint attempt failures, failover, terminal auth
rejection — classified by :class:`ConnectionEventKind <questdb.ConnectionEventKind>`. The listener
runs on a dedicated dispatcher thread fed by a bounded drop-oldest
inbox; delivery totals are exposed as ``connection_events_delivered`` /
``connection_events_dropped`` on both :class:`QuestDB <questdb.QuestDB>` and
:class:`Sender <questdb.Sender>`. :meth:`QuestDB.server_info <questdb.QuestDB.server_info>` snapshots the server's
``SERVER_INFO`` handshake as a :class:`ServerInfo <questdb.ServerInfo>` (role as
:class:`ServerRole <questdb.ServerRole>`, failover epoch, capabilities and cluster / node /
zone ids).

Errors
******

Adds :class:`UnsupportedDataFrameShapeError <questdb.UnsupportedDataFrameShapeError>` (raised when a DataFrame
cannot be expressed on the QWP columnar path) and the
:class:`QuestDBErrorCode <questdb.QuestDBErrorCode>` members ``ServerRejection``, ``RoleMismatch``,
``ArrowUnsupportedColumnKind``, ``ArrowIngest``, ``FailoverRetry``,
``ConnectTimeout``, ``FailoverWouldDuplicate``, ``Cancelled``,
``BatchTooLarge`` and ``StoreResendRequired``.
:class:`QuestDBError <questdb.QuestDBError>` gains a ``sender_error`` property exposing the
structured :class:`SenderError <questdb.SenderError>` view on a server-side QWP/WebSocket
rejection, plus an ``in_doubt`` property indicating that failed ingress input
may already have been delivered and must not be blindly replayed without an
appropriate table-level deduplication guarantee.

Build & dependencies
~~~~~~~~~~~~~~~~~~~~~~

- The bundled ``c-questdb-client`` native library is upgraded to the 7.0.0
  development series (pinned at ``6.1.0-441-g2158029``), providing the QWP
  transports, the columnar ``column_sender`` API and the ``line_reader``
  query API that back the new features above.
- ``numpy>=1.21.0`` is now a hard runtime dependency (previously it was
  pulled in only via the ``dataframe`` extra).
- **pyarrow is now optional.** It is imported lazily, only when actually
  needed (``pd.ArrowDtype`` columns, pyarrow sources, ``schema_overrides``,
  ``to_polars`` / ``iter_polars``, and the ``to_arrow`` / ``iter_arrow`` /
  ``dtype_backend`` helpers). The ``__arrow_c_stream__`` egress path
  (consumed as ``polars.from_arrow(result)``) and the default
  ``to_pandas`` / ``iter_pandas`` work without pyarrow.
- The ``dataframe`` extra now pins ``pandas>=1.3.5`` and
  ``pyarrow>=10.0.1``.

Deprecations
~~~~~~~~~~~~

- The ``questdb.ingress`` module is deprecated (kept as a compatibility shim
  for 4.x ILP/HTTP and ILP/TCP code); it emits one ``DeprecationWarning`` at
  import. New code imports from ``questdb``.

4.1.0 (2025-11-28)
------------------

Features
~~~~~~~~

Decimal Data Type Support
*************************

This release adds support for ingesting data into QuestDB's native
``DECIMAL(precision, scale)`` column type introduced in QuestDB 9.2.0.
Use decimals when you need exact-precision values (e.g. prices, balances)
without floating-point rounding issues.

Decimal values can be sent using Python ``decimal.Decimal`` objects in both
``row()`` and ``dataframe()`` methods. When sending dataframes, you can also use
PyArrow decimal types for better performance.

Sending decimals requires protocol version 3. When using HTTP, this protocol
version is auto-negotiated. For TCP connections, you must explicitly specify
``protocol_version=3`` in the configuration string:

.. code-block:: python

    # HTTP - protocol version 3 is auto-negotiated
    conf = 'http::addr=localhost:9000;'

    # TCP - must specify protocol_version=3 explicitly
    conf = 'tcp::addr=localhost:9009;protocol_version=3;'

.. important::
    **Server Requirement**: This feature requires QuestDB server version 9.2.0 or higher.

    Unlike other column types, DECIMAL columns **must be created in advance** via SQL
    before ingesting data. Auto-creation is not supported for DECIMAL columns.

    For details on creating DECIMAL columns and working with this data type, see the
    `QuestDB Decimal documentation <https://questdb.com/docs/concept/decimal/>`_.

**Usage with Python Decimal objects:**

.. code-block:: python

    from decimal import Decimal
    from questdb import Sender, TimestampNanos

    # First, create the table with DECIMAL column via SQL:
    # CREATE TABLE trades (
    #     symbol SYMBOL,
    #     price DECIMAL(18,4),
    #     amount DECIMAL(18,8),
    #     timestamp TIMESTAMP
    # ) TIMESTAMP(timestamp);

    conf = 'http::addr=localhost:9000;'
    with Sender.from_conf(conf) as sender:
        sender.row(
            'trades',
            symbols={'symbol': 'ETH-USD'},
            columns={
                'price': Decimal('2615.5400'),
                'amount': Decimal('0.00044000')},
            at=TimestampNanos.now())

**Usage with Python Decimal objects in Pandas dataframes:**

.. code-block:: python

    import pandas as pd
    from decimal import Decimal

    # Create DataFrame with Python Decimal objects
    df = pd.DataFrame({
        'symbol': ['ETH-USD', 'BTC-USD'],
        'price': [Decimal('2615.5400'), Decimal('43210.1234')],
        'volume': [Decimal('1234.56789012'), Decimal('98.76543210')]
    })

    with Sender.from_conf(conf) as sender:
        sender.dataframe(
            df,
            table_name='trades',
            symbols='symbol',
            at=TimestampNanos.now())

**Usage with PyArrow Decimal types in Pandas dataframes:**

.. code-block:: python

    import pandas as pd
    import pyarrow as pa
    from decimal import Decimal

    # Create DataFrame with Arrow decimal types
    df = pd.DataFrame({
        'prices': pd.array(
            [Decimal('-99999.99'), Decimal('-678.00')],
            dtype=pd.ArrowDtype(pa.decimal128(18, 2))
        )
    })

    with Sender.from_conf(conf) as sender:
        sender.dataframe(df, table_name='prices', at=TimestampNanos.now())

Additional Arrow Data Type Support
**********************************

Added support for additional PyArrow column types commonly encountered when
deserializing from Parquet files or converting Polars dataframes to Pandas:

* ``int16``, ``int32``, ``float32`` (float), ``bool``
* ``string``, ``large_string`` (including as symbol types)
* ``timestamp[us]`` with timezone support for microsecond-precision timestamps

Microsecond Timestamp Precision
*******************************

Microsecond-precision timestamp columns (``datetime64[us]`` in NumPy and
``timestamp[us]`` in PyArrow) are now fully supported. When using
``protocol_version`` 2 or higher, microsecond timestamps are sent with full
precision, including for the designated timestamp column.

Bug fixes
~~~~~~~~~

* Updated type hints to allow ``None`` as a valid column value in
  ``Sender.row()``. This brings the type annotations in line with the actual
  behavior, where ``None`` values have always been supported to represent NULL
  values.

Python Version Support
~~~~~~~~~~~~~~~~~~~~~~

* Added support for Python 3.14 on all platforms and 3.14t (free threaded) on
  all platforms except Windows.
* Dropped support for Python 3.9 (end of life).

4.0.0 (2025-10-17)
------------------

New Breaking Change Feature
~~~~~~~~~~~~~~~~~~~~~~~~~~~

From QuestDB 9.1.0 onwards you can use ``CREATE TABLE`` SQL statements with
``TIMESTAMP_NS`` column types, or rely on column auto-creation.

This client release adds support for sending nanoseconds timestamps to the
server without loss of precision.

This release does not introduce new APIs, instead enhancing the sender/buffer's
``.row()`` API to additionally accept nanosecond precision.

.. code-block:: python

    conf = 'http::addr=localhost:9000;'
    # or `conf = 'tcp::addr=localhost:9009;protocol_version=2;'`
    with Sender.from_conf(conf) as sender:
        sender.row(
            'trade_executions',
            symbols={
                'product': 'VOD.L',
                'parent_order': '65d1ba36-390e-49a2-93e3-a05ef004b5ff',
                'side': 'buy'},
            columns={
                'order_sent': TimestampNanos(1759246702031355012)},
            at=TimestampNanos(1759246702909423071))

If you're using dataframes, nanosecond timestamps are now also transferred with
full precision.

The change is backwards compatible with older QuestDB releases which will simply
continue using the ``TIMESTAMP`` column, even when nanoseconds are specified in
the client.

This is a breaking change because it introduces new breaking timestamp
`column auto-creation <https://questdb.com/docs/reference/api/ilp/overview/#table-and-column-auto-creation>`_
behaviour. For full details and upgrade advice, see the
`nanosecond PR on GitHub <https://github.com/questdb/py-questdb-client/pull/113>`_.

3.0.0 (2025-07-07)
------------------

Features
~~~~~~~~

This is the first major release of the QuestDB Python client library
which supports n-dimensional arrays of doubles for QuestDB servers 9.0.0 and up.

.. code-block:: python

        import numpy as np

        # Create 2D numpy array
        array_2d = np.array([
            [1.1, 2.2, 3.3],
            [4.4, 5.5, 6.6]], dtype=np.float64)

        sender.row(
            'table',
            columns={'array_2d': array_2d},
            at=timestamp)

The array data is sent over a new protocol version (2) that is auto-negotiated
when using HTTP(s), or can be specified explicitly via the ``protocol_version=2``
parameter when using TCP(s).

We recommend using HTTP(s), but here is an TCP example, should you need it::

  tcp::addr=localhost:9009;protocol_version=2;

When using ``protocol_version=2`` (with either TCP(s) or HTTP(s)), the sender
will now also serialize ``float`` (double-precision) columns as binary.
You might see a performance uplift if this is a dominant data type in your
ingestion workload.

When compared to 2.0.4, this release includes all the changes from 3.0.0rc1 and
additionally:

* Has optimised ingestion performance from C-style contiguous NumPy arrays.

* Warns at most every 10 minutes when burst of reconnections are detected.
  This is to warn about code patterns that may lead to performance issues, such as

  .. code-block:: python

    # Don't do this! Sender objects should be reused.
    for row_fields in data:
        with Sender.from_conf(conf) as sender:
            sender.row(**row_fields)

  This feature can be disabled in code by setting:

  .. code-block:: python

    import questdb as qi
    qi.WARN_HIGH_RECONNECTS = False

* Fixed ILP/TCP connection shutdown on Windows where some rows could be
  lost when closing the ``Sender``, even if explicitly flushed.

* Added a "Good Practices" section to the "Sending Data over ILP" section of
  the documentation.

Breaking Changes
~~~~~~~~~~~~~~~~
Refer to the release notes for 3.0.0rc1 for the breaking changes introduced
in this release compared to 2.x.x.


3.0.0rc1 (2025-06-02)
---------------------

This is the pre-release of a major release introducing array ingestion and some
minor breaking changes.

Features
~~~~~~~~
* Array Data Type Support. Adds native support for NumPy arrays
  (currently only for ``np.float64`` element type and up to 32 dimensions).

.. note::
    **Server Requirement**: This feature requires QuestDB server version 9.0.0 or higher.
    Ensure your server is upgraded before ingesting array types, otherwise data ingestion will fail.

.. code-block:: python

        import numpy as np

        # Create 2D numpy array
        array_2d = np.array([
            [1.1, 2.2, 3.3],
            [4.4, 5.5, 6.6]], dtype=np.float64)

        sender.row(
            'table',
            columns={'array_2d': array_2d},
            at=timestamp)

* Implements binary protocol for columns of ``float`` (double-precision) and
  ``numpy.ndarray[np.float64]``, with performance improvements for these
  two datatypes.

Breaking Changes
~~~~~~~~~~~~~~~~
* Buffer Constructor Changes. The ``Buffer`` constructor now requires the ``protocol_version`` parameter.
  You can create buffer through the sender for automatic ``protocol_version`` management:

.. code-block:: python

    buf = sender.new_buffer()  # protocol_version determined automatically
    buf.row(
      'table',
      columns={'arr': np.array([1.5, 3.0], dtype=np.float64)},
      at=timestamp)

* To access the raw payload, call ``bytes(sender)`` or ``bytes(buffer)`` (
  rather than calling the ``str`` function on the same objects as in version
  2.x.x of the questdb library) method.

* **NumPy Dependency**

  Array functionality mandates NumPy installation.

* **Sender/Buffer String Conversion Removal**

  The legacy string conversion via `str(sender)` is removed.
  Access raw binary payloads through the `bytes(sender)` method:

  .. code-block:: python

      # for debugging
      payload = bytes(sender)

* Python 3.8 support is dropped.

  The minimum supported Python version is now 3.9.

2.0.4 (2025-04-02)
------------------

Building for Python 3.13.

2.0.3 (2024-06-06)
------------------

Patch release with bug fixes. No breaking changes.

Bug fixes
~~~~~~~~~
* HTTP timeout wasn't always being correctly applied in the downstream ``c-questdb-client`` dependency.
* ``request_timeout > 0`` will now be enforced. This was always required, but would not error.
* Fixed the source distribution "sdist" package: This allows the package to be installed from source
  via "pip install" on previously unsupported platforms (YMMV).

2.0.2 (2024-04-11)
------------------

Patch release with a performance bug fix. No breaking changes.

Bug fixes
~~~~~~~~~
* Fixed the defaulting logic for ``auto_flush_rows`` parameter for HTTPS.
  It is now correctly set to 75000 rows by default. The old incorrect default
  of 600 rows was causing the sender to flush too often, impacting performance.
  Note that TCP, TCPS and HTTP were not affected.

Features
~~~~~~~~
* The sender now exposes the ``auto_flush`` settings as read-only properties.
  You can inspect the values in use with ``.auto_flush``, ``.auto_flush_rows``,
  ``.auto_flush_interval`` and ``.auto_flush_bytes``.

2.0.1 (2024-04-03)
------------------

Patch release with bug fixes, no API changes and some documentation tweaks.

Bug fixes
~~~~~~~~~
* Fixed a bug where an internal "last flushed" timestamp used
  by ``auto_flush_interval`` wasn't updated correctly causing the auto-flush
  logic to trigger after each row.

* Removed two unnecessary debugging ``print()`` statements that were
  accidentally left in the code in ``Sender.from_conf()`` and
  ``Sender.from_env()``.

Documentation
~~~~~~~~~~~~~
* Introduced the ability to optionally install ``pandas`` and ``pyarrow`` via
  ``python3 -m pip install -U questdb[dataframe]`` and updated the documentation
  to reflect this.


2.0.0 (2024-03-19)
------------------

This is a major release with new features and breaking changes.

Features
~~~~~~~~

* Support for ILP over HTTP. The sender can now send data to QuestDB via HTTP
  instead of TCP. This provides error feedback from the server and new features.

  .. code-block:: python

    conf = 'http::addr=localhost:9000;'
    with Sender.from_conf(conf) as sender:
        sender.row(...)
        sender.dataframe(...)

        # Will raise `QuestDBError` if there is an error from the server.
        sender.flush()

* New configuration string construction. The sender can now be also constructed
  from a :ref:`configuration string <sender_conf>` in addition to the
  constructor arguments.
  This allows for more flexible configuration and is the recommended way to
  construct a sender.
  The same string can also be loaded from the ``QDB_CLIENT_CONF`` environment
  variable.
  The constructor arguments have been updated and some options have changed.

* Explicit transaction support over HTTP. A set of rows for a single table can
  now be committed via the sender transactionally. You can do this using a
  ``with sender.transaction('table_name') as txn:`` block.

  .. code-block:: python

    conf = 'http::addr=localhost:9000;'
    with Sender.from_conf(conf) as sender:
        with sender.transaction('test_table') as txn:
            # Same arguments as the sender methods, minus the table name.
            txn.row(...)
            txn.dataframe(...)

* A number of documentation improvements.


Breaking Changes
~~~~~~~~~~~~~~~~

* New ``protocol`` parameter in the
  :ref:`Sender <sender_programmatic_construction>` constructor.

  In previous version the protocol was always TCP.
  In this new version you must specify the protocol explicitly.

* New auto-flush defaults. In previous versions
  :ref:`auto-flushing <sender_auto_flush>` was enabled by
  default and triggered by a maximum buffer size. In this new version
  auto-flushing is enabled by row count (600 rows by default) and interval
  (1 second by default), while auto-flushing by buffer size is disabled by
  default.

  The old behaviour can be still be achieved by tweaking the auto-flush
  settings.

  .. list-table::
    :header-rows: 1

    * - Setting
      - Old default
      - New default
    * - **auto_flush_rows**
      - off
      - 600
    * - **auto_flush_interval**
      - off
      - 1000
    * - **auto_flush_bytes**
      - 64512
      - off

* The ``at=..`` argument of :func:`row <questdb.Sender.row>` and
  :func:`dataframe <questdb.Sender.dataframe>` methods is now mandatory.
  Omitting it would previously use a server-generated timestamp for the row.
  Now if you want a server generated timestamp, you can pass the :ref:`ServerTimestamp <sender_server_timestamp>`
  singleton to this parameter. _The ``ServerTimestamp`` behaviour is considered legacy._

* The ``auth=(u, t, x, y)`` argument of the ``Sender`` constructor has now been
  broken up into multiple arguments: ``username``, ``token``, ``token_x``, ``token_y``.

* The ``tls`` argument of the ``Sender`` constructor has been removed and
  replaced with the ``protocol`` argument. Use ``Protocol.Tcps``
  (or ``Protocol.Https``) to enable TLS.
  The ``tls`` values have been moved to new ``tls_ca`` and ``tls_roots``
  :ref:`configuration settings <sender_conf_tls>`.

* The ``net_interface`` argument of the ``Sender`` constructor has been renamed
  to ``bind_interface`` and is now only available for TCP connections.

The following example shows how to migrate to the new API.

**Old questdb 1.x code**

.. code-block:: python

    from questdb import Sender

    auth = (
        'testUser1',
        '5UjEMuA0Pj5pjK8a-fa24dyIf-Es5mYny3oE_Wmus48',
        'token_x=fLKYEaoEb9lrn3nkwLDA-M_xnuFOdSt9y0Z7_vWSHLU',
        'token_y=Dt5tbS1dEDMSYfym3fgMv0B99szno-dFc1rYF9t0aac')
    with Sender('localhost', 9009, auth=auth, tls=True) as sender:
        sender.row(
            'test_table',
            symbols={'sym': 'AAPL'},
            columns={'price': 100.0})  # `at=None` was defaulted for server time

**Equivalent questdb 2.x code**

.. code-block:: python

    from questdb import Sender, Protocol, ServerTimestamp

    sender = Sender(
        Protocol.Tcps,
        'localhost',
        9009,
        username='testUser1',
        token='5UjEMuA0Pj5pjK8a-fa24dyIf-Es5mYny3oE_Wmus48',
        token_x='token_x=fLKYEaoEb9lrn3nkwLDA-M_xnuFOdSt9y0Z7_vWSHLU',
        token_y='token_y=Dt5tbS1dEDMSYfym3fgMv0B99szno-dFc1rYF9t0aac',
        auto_flush_rows='off',
        auto_flush_interval='off',
        auto_flush_bytes=64512)
    with sender:
        sender.row(
            'test_table',
            symbols={'sym': 'AAPL'},
            columns={'price': 100.0},
            at=ServerTimestamp)

**Equivalent questdb 2.x code with configuration string**

.. code-block:: python

    from questdb import Sender

    conf = (
        'tcp::addr=localhost:9009;' +
        'username=testUser1;' +
        'token=5UjEMuA0Pj5pjK8a-fa24dyIf-Es5mYny3oE_Wmus48;' +
        'token_x=token_x=fLKYEaoEb9lrn3nkwLDA-M_xnuFOdSt9y0Z7_vWSHLU;' +
        'token_y=token_y=Dt5tbS1dEDMSYfym3fgMv0B99szno-dFc1rYF9t0aac;' +
        'auto_flush_rows=off;' +
        'auto_flush_interval=off;' +
        'auto_flush_bytes=64512;')
    with Sender.from_conf(conf) as sender:
        sender.row(
            'test_table',
            symbols={'sym': 'AAPL'},
            columns={'price': 100.0},
            at=ServerTimestamp)


1.2.0 (2023-11-23)
------------------

This is a minor release bringing in minor new features and a few bug fixes,
without any breaking changes.

Most changes are inherited by internally upgrading to version ``3.1.0`` of
the ``c-questdb-client``.

Features
~~~~~~~~

* ``Sender(..., tls=True)`` now also uses the OS-provided certificate store.
  The `tls` argument can now also be set to ``tls='os_roots'`` (to *only* use
  the OS-provided certs) or ``tls='webpki_roots'`` (to *only* use the certs
  provided by the ``webpki-roots``, i.e. the old behaviour prior to this
  release). The new default behaviour for ``tls=True`` is equivalent to setting
  ``tls='webpki_and_os_roots'``.

* Upgraded dependencies to newer library versions. This also includes the latest
  `webpki-roots <https://github.com/rustls/webpki-roots>`_ crate providing
  updated TLS CA certificate roots.

* Various example code and documentation improvements.

Bug fixes
~~~~~~~~~

* Fixed a bug where timestamp columns could not accept values before Jan 1st
  1970 UTC.

* TCP connections now enable ``SO_KEEPALIVE``: This should ensure that
  connections don't drop after a period of inactivity.

1.1.0 (2023-01-04)
------------------

Features
~~~~~~~~

* High-performance ingestion of `Pandas <https://pandas.pydata.org/>`_
  dataframes into QuestDB via ILP.
  We now support most Pandas column types. The logic is implemented in native
  code and is orders of magnitude faster than iterating the dataframe
  in Python and calling the ``Buffer.row()`` or ``Sender.row()`` methods: The
  ``Buffer`` can be written from Pandas at hundreds of MiB/s per CPU core.
  The new ``dataframe()`` method continues working with the ``auto_flush``
  feature.
  See API documentation and examples for the new ``dataframe()`` method
  available on both the ``Sender`` and ``Buffer`` classes.

* New ``TimestampNanos.now()`` and ``TimestampMicros.now()`` methods.
  *These are the new recommended way of getting the current timestamp.*

* The Python GIL is now released during calls to ``Sender.flush()`` and when
  ``auto_flush`` is triggered. This should improve throughput when using the
  ``Sender`` from multiple threads.

Errata
~~~~~~

* In previous releases the documentation for the ``from_datetime()`` methods of
  the ``TimestampNanos`` and ``TimestampMicros`` types recommended calling
  ``datetime.datetime.utcnow()`` to get the current timestamp. This is incorrect
  as it will (confusingly) return an object with the local timezone instead of UTC.
  This documentation has been corrected and now recommends calling
  ``datetime.datetime.now(tz=datetime.timezone.utc)`` or (more efficiently) the
  new ``TimestampNanos.now()`` and ``TimestampMicros.now()`` methods.

1.0.2 (2022-10-31)
------------------

Features
~~~~~~~~

* Support for Python 3.11.
* Updated to version 2.1.1 of the ``c-questdb-client`` library:

  * Setting ``SO_REUSEADDR`` on outbound socket. This is helpful to users with large number of connections who previously ran out of outbound network ports.


1.0.1 (2022-08-16)
------------------

Features
~~~~~~~~

* As a matter of convenience, the ``Buffer.row`` method can now take ``None`` column
  values. This has the same semantics as skipping the column altogether.
  Closes `#3 <https://github.com/questdb/py-questdb-client/issues/3>`_.

Bug fixes
~~~~~~~~~

* Fixed a major bug where Python ``int`` and ``float`` types were handled with
  32-bit instead of 64-bit precision. This caused certain ``int`` values to be
  rejected and other ``float`` values to be rounded incorrectly.
  Closes `#13 <https://github.com/questdb/py-questdb-client/issues/13>`_.
* Fixed a minor bug where an error auto-flush caused a second clean-up error.
  Closes `#4 <https://github.com/questdb/py-questdb-client/issues/4>`_.


1.0.0 (2022-07-15)
------------------

Features
~~~~~~~~

* First stable release.
* Insert data into QuestDB via ILP.
* Sender and Buffer APIs.
* Authentication and TLS support.
* Auto-flushing of buffers.


0.0.3 (2022-07-14)
------------------

Features
~~~~~~~~

* Initial set of features to connect to the database.
* ``Buffer`` and ``Sender`` classes.
* First release where ``pip install questdb`` should work.


0.0.1 (2022-07-08)
------------------

Features
~~~~~~~~

* First release on PyPI.
