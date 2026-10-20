================
Migration Guide
================

5.0 to 6.0
==========

These changes need action: the two UUID / fixed-size-binary items below, the
stricter ``schema_overrides`` tuple validation, caps on callback inbox
capacities and config-string length, the error a query failover that runs out
of time raises, ``QuestDB.close()`` no longer waiting without a limit for
leases, a sender's own callbacks no longer being able to append to it, the
exception a commit raises once its sender is closed, a warning an abandoned
query result now surfaces instead of swallowing, and a connected sender riding
out a rejected credential instead of stopping. The :ref:`changelog
<changelog>` lists the rest of 6.0's breaking changes, most of them about the
new QWP-only column types. The new
``ConnectionEventKind.CredentialUnavailable`` event kind needs none — it is
additive, and an existing listener keeps working.

* **UUID bytes are canonical RFC 4122.** UUID values are read and written in
  canonical big-endian order at every API boundary; the client byte-swaps to
  QWP wire order internally. The wire format is unchanged and still matches the
  Java client, so stored data and round-trips are unaffected — only the bytes
  your application supplies or receives change. A ``uuid.UUID`` object column
  needs no byte-order change. If you pre-reversed bytes to work around the old layout, remove that workaround
  — on the **read** path too. The bytes a UUID column
  yields changed in the same way and just as silently, on every Arrow-backed
  reader: :meth:`~questdb.QueryResult.to_arrow`,
  :meth:`~questdb.QueryResult.to_polars`,
  :meth:`~questdb.QueryResult.iter_arrow`,
  :meth:`~questdb.QueryResult.iter_polars`, ``__arrow_c_stream__`` and
  :meth:`~questdb.QueryResult.to_pandas` /
  :meth:`~questdb.QueryResult.iter_pandas` with either ``dtype_backend`` (both
  ``"pyarrow"`` and ``"numpy_nullable"``) or ``types_mapper``. Only the
  argument-free :meth:`~questdb.QueryResult.to_pandas` /
  :meth:`~questdb.QueryResult.iter_pandas` build ``uuid.UUID`` objects and are
  unaffected.

* **Fixed-size-binary width alone no longer claims UUID or LONG256.** An
  unclaimed 16- or 32-byte column in native Arrow input -- a ``pa.Table``, a
  ``RecordBatchReader`` or any other ``__arrow_c_stream__`` producer -- now
  lands as ``BINARY``, with a warning on the ``questdb`` logger, which the
  server rejects against an existing UUID or LONG256 column rather than storing
  the wrong type silently. A pandas frame refuses such a column with
  ``BadDataFrame`` before sending anything: its ``ArrowDtype`` cannot carry a
  label, so the column may be a UUID or LONG256 column that lost one. To write
  either width as plain bytes, label it with
  ``schema_overrides={'col': 'binary'}``. The ``arrow.uuid`` extension label claims a 16-byte
  column as UUID. Whole-table or stream Arrow inputs can also retain
  column-field metadata ``questdb.column_type=uuid`` or
  ``questdb.column_type=long256`` to claim either type without overrides.
  This must be field metadata, not top-level schema metadata. For example:

  .. code-block:: python

      pa.field('id', pa.binary(16), metadata={'questdb.column_type': 'uuid'})
      pa.field('hash', pa.binary(32), metadata={'questdb.column_type': 'long256'})

  Pyarrow drops field metadata when exporting a single pandas column, so the
  pandas per-column planner cannot use these metadata claims. Alternatively,
  claim the columns explicitly:

  .. code-block:: python

      sender.dataframe(df, table_name='trades', at='ts',
                       schema_overrides={'id': 'uuid', 'hash': 'long256'})

  ``schema_overrides`` accepts the new ``'uuid'`` and ``'long256'`` kinds for
  this purpose, but it **requires fully Arrow-backed input** — pyarrow,
  polars, or a pandas frame where *every* column uses ``ArrowDtype`` — and no
  ``table_name_col``. One NumPy-dtype column is enough to take the NumPy
  planner instead, which does not apply overrides and raises
  :class:`~questdb.UnsupportedDataFrameShapeError`; a plain ``datetime64``
  ``at=`` column does it. That is exactly the shape that used to write
  LONG256 with no override, so convert the frame first::

      df = df.convert_dtypes(dtype_backend='pyarrow')

  Wrapping the column in pyarrow's ``arrow.uuid`` extension type also works
  for the 16-byte case, on either path and with no conversion. LONG256 has no
  equivalent extension label for the pandas per-column planner: use a fully
  Arrow-backed frame plus ``schema_overrides``, or a whole Arrow table/stream
  retaining ``questdb.column_type=long256`` field metadata.

* **Only a geohash schema override accepts an argument.** A tuple such as
  ``{'x': ('symbol', 16)}`` previously worked by silently discarding ``16``;
  write ``{'x': 'symbol'}`` instead. The two-item form is reserved for
  ``('geohash', bits)``. :data:`questdb.SchemaOverride` is now importable for
  annotating these values.

* **Callback inbox capacities are capped at 65536.**
  ``connection_event_inbox_capacity`` and ``error_event_inbox_capacity``, and
  the ``error_inbox_capacity`` config-string key, now reject a larger value
  where one was previously accepted. Lower any value above the cap; these
  inboxes bound memory when a listener cannot keep up, and a capacity that
  large is an allocation failure waiting to happen rather than useful
  buffering.

* **Config strings are capped at 1 MiB.** This applies to
  :meth:`Sender.from_conf <questdb.Sender.from_conf>`,
  :meth:`Sender.from_env <questdb.Sender.from_env>` (``QDB_CLIENT_CONF``),
  :func:`questdb.connect` and :meth:`QuestDB.from_conf
  <questdb.QuestDB.from_conf>`, which raise ``QuestDBError(InvalidApiCall)``
  for a longer string. Remove accidentally duplicated or attacker-controlled
  content from any string above that bound.

* **A query failover that runs out of time raises what it found.** When
  ``failover_max_duration_ms`` expires while reconnect attempts remain and the
  last round was rejected on role, at the WebSocket upgrade or at TLS, the
  query raises that ``RoleMismatch`` / ``HandshakeError`` / ``TlsError``
  instead of the connection failure that started the failover, as a failover
  that ran out of attempts already did. Code that branches on
  ``QuestDBError.code`` after a failed query should treat these codes as a
  possible outcome of a timed-out failover. These codes describe *why* the
  query failed, not whether an immediate retry is safe or useful: wait for a
  matching-role endpoint after ``RoleMismatch``. A late HTTP 401/403 is a
  credential rejection (``AuthError``), not a transient socket failure; fix
  the credentials before retrying. See :doc:`auth` for OIDC-specific handling.

* :meth:`QuestDB.close <questdb.QuestDB.close>` **no longer waits without a
  limit for leases.** It waits up to a minute by default for outstanding
  ``sender()`` / ``reader()`` leases, then raises
  ``QuestDBError(InvalidApiCall)`` and leaves the handle closing; 5.0 waited
  for ever, which never ended for a lease held by the calling thread. Called
  from inside a call on the handle or on one of its leases, it is refused at
  once; with greenlets, every greenlet on an OS thread counts as the same
  caller. Close every lease before closing the handle, or pass ``timeout``
  (``None`` waits without a limit). A lease handed to a worker is waited for
  until the worker returns it::

      sender = db.sender()

      def work():
          with sender:
              sender.row('trades', columns={'price': 1.0},
                         at=questdb.ServerTimestamp)

      threading.Thread(target=work).start()
      db.close()  # waits, up to its timeout, for the worker to return it

* **A sender's own callbacks can no longer append to it.** Called from the
  sender's own ``connection_listener`` or ``error_handler``, ``row()``,
  ``dataframe()``, ``new_buffer()``, ``establish()``, ``transaction()`` and the
  :class:`~questdb.SenderTransaction` operations raise
  ``QuestDBError(InvalidApiCall)``, as ``flush()`` and ``close()`` already did,
  even while the sender is idle. 5.0 accepted ``row()`` there. The read-only
  ``max_name_len``, ``protocol_version``, ``connection_events_dropped`` and
  ``connection_events_delivered`` also raise, from any thread, while the sender
  is inside a native call such as ``flush()``. So do ``len()``, ``bytes()``,
  truthiness, ``capacity()``, ``reserve()``, ``clear()`` and ``row()`` on a
  :class:`~questdb.ingress.Buffer` that a native call is modifying (the internal buffer
  during ``flush()``, an explicit one during ``flush(buffer)``, or any one
  during a row-serializing ``dataframe()``, ``SenderTransaction.dataframe()``
  included), from any other thread; code a ``dataframe()`` runs on its own
  thread follows the rules for re-entering ``row()`` instead. A
  ``flush(buffer, clear=False)`` only reads the buffer, so
  concurrent reads and other ``clear=False`` flushes of it still work. Hand
  listener events to another sender or to the owning thread instead::

      events = queue.SimpleQueue()
      sender = Sender.from_conf(conf, connection_listener=events.put)
      ...
      while not events.empty():  # on the sender's own thread
          event = events.get()
          sender.row('conn_events', symbols={'kind': event.kind.tag},
                     at=questdb.ServerTimestamp)

* :meth:`SenderTransaction.commit <questdb.SenderTransaction.commit>` **after
  the owning sender was closed now raises** ``QuestDBError(InvalidApiCall)``
  instead of an internal ``TypeError``. A commit whose flush fails at the
  server leaves the sender's buffer empty and the transaction completed, as in
  5.0; one refused before it sends anything now leaves the transaction open,
  to commit again or roll back.

* **An abandoned** :class:`~questdb.QueryResult` **now reports its**
  ``ResourceWarning`` **when promoted to an exception**. The finalizer previously
  swallowed that exception; after releasing the cursor it now lets it reach
  :func:`sys.unraisablehook`. Otherwise the warning follows the normal warning
  filters, as before. Under warnings-as-errors this becomes a failure — and
  because it fires whenever garbage collection happens to run, pytest attributes
  it to whatever test was executing rather than to the one that abandoned the
  result. Close results deterministically::

      with db.query('select * from trades') as result:
          ...

  or filter :class:`ResourceWarning` if a suite runs with ``-W error``.

* **A connected sender rides out a rejected credential.** Once a QWP/WebSocket
  sender has connected, a server that answers its reconnect with HTTP 401 or
  403 no longer makes it terminal: the sender keeps reconnecting with capped
  backoff, keeps its unacknowledged rows, and reports each failed attempt as a
  :attr:`ConnectionEventKind.AuthFailed <questdb.ConnectionEventKind.AuthFailed>`
  event. 5.0 stopped the sender at the first rejection and raised
  ``AuthError`` from its next call. Fixed ``token=`` and ``username=`` /
  ``password=`` credentials are affected too. A connection that is waited for
  -- a sender's first connection, an eager :func:`questdb.connect` -- still
  fails at once. Code that relied on the sender stopping must now close it
  itself, for example when its listener reports ``AuthFailed``, and a listener
  that tore the pool down on ``AuthFailed`` should expect one event per
  attempt::

      rejected = threading.Event()

      def on_event(event):
          if event.kind is questdb.ConnectionEventKind.AuthFailed:
              rejected.set()   # a listener cannot close its own sender

      sender = Sender.from_conf(conf, connection_listener=on_event)
      ...
      if rejected.is_set():   # on the sender's own thread
          sender.close()

  See :ref:`token_rejections`.

4.x to 5.0
==========

Connect once, then stream, load, or query
-----------------------------------------

The QWP/WebSocket API has one connection-owning root, the :class:`QuestDB
<questdb.QuestDB>` handle returned by :func:`questdb.connect`:

* :meth:`QuestDB.sender <questdb.QuestDB.sender>` lends a pooled
  row-building sender — a lean lease exposing ``row()``, ``dataframe()``,
  ``flush()``, ``wait()``, the FSN delivery receipts
  (``flush_and_get_fsn`` / ``await_acked_fsn``), rejection polling
  (``poll_error``) and ``close()``, not a
  :class:`Sender <questdb.Sender>` instance (``isinstance`` checks and type annotations
  must not assume the ``Sender`` class — use the exported
  :class:`questdb.PooledSender` instead);
* :meth:`QuestDB.dataframe <questdb.QuestDB.dataframe>` bulk-loads a whole
  DataFrame through the direct columnar path;
* :meth:`QuestDB.query <questdb.QuestDB.query>` executes queries; and
* :meth:`QuestDB.reader <questdb.QuestDB.reader>` lends a pooled
  :class:`questdb.PooledReader` for running several queries on one
  connection.

Existing row code can move from a standalone ``Sender`` to a pooled one:

.. code-block:: python

    import questdb
    from questdb import Sender, ServerTimestamp

    # Before: a standalone multi-transport sender.
    with Sender.from_conf('ws::addr=localhost:9000;') as sender:
        sender.row('weather', columns={'temperature': 21.5},
                   at=ServerTimestamp)
        sender.flush()

    # After: the handle owns the pool and lends a row builder.
    with questdb.connect('ws::addr=localhost:9000;') as db:
        with db.sender() as sender:
            sender.row('weather', columns={'temperature': 21.5},
                       at=ServerTimestamp)
            sender.flush(wait=True)

The pooled sender publishes into the store-and-forward QWP path. A plain
``flush()`` returns after local acceptance; ``flush(wait=True)`` also waits
for the server's OK acknowledgement of everything published through the
lease. The wait is a pure ack barrier — only a terminal connection failure
raises. Server rejections are pushed to the pool's ``error_handler``
(a :func:`questdb.connect` keyword; one :class:`questdb.SenderError` per
rejection, on a dedicated dispatcher thread). Without a handler every
rejection is logged through the ``questdb`` logger — ``ERROR`` for terminal
rejections, ``WARNING`` for retriable ones, which the queue replays — so
rejections are never silent. To ingest concurrently, borrow one sender per
thread.

DataFrame bulk loads over QWP/WebSocket
---------------------------------------

Over ``ws::`` / ``wss::``, DataFrame bulk loads use the direct columnar
path — a database operation, not stream serialization. The recommended
entry point is :meth:`QuestDB.dataframe <questdb.QuestDB.dataframe>` on the
handle; the sender variants exist as conveniences and share the same path:

.. code-block:: python

    import pandas as pd
    import questdb
    from questdb import Sender, ServerTimestamp

    frame = pd.DataFrame({
        't': [21.5, 20.9],
        'ts': pd.to_datetime(['2026-01-01T00:00:00Z', '2026-01-01T00:00:01Z'])})

    with questdb.connect('ws::addr=localhost:9000;') as db:
        # Recommended — on the handle:
        db.dataframe(frame, table_name='weather', at='ts')

        # Convenience: on a pooled sender — same path, direct connection
        # from the pool; NOT part of the sender's row stream:
        with db.sender() as sender:
            sender.row('weather', columns={'t': 21.5}, at=ServerTimestamp)
            sender.dataframe(frame, table_name='weather', at='ts')
            sender.flush()

    # Convenience: on a standalone sender — same path, poolless direct
    # connection:
    with Sender.from_conf('ws::addr=localhost:9000;') as sender:
        sender.dataframe(frame, table_name='weather', at='ts')

On success, each call returns only after every row in the source has been
committed. The first successful batch on a fresh direct connection is already
a commit boundary; subsequent batches are pipelined until the final commit.
To keep memory bounded, a very large Arrow load adds a checkpoint about every
100 batches, and the client may checkpoint earlier if deferred capacity fills
or a batch must be split to fit. This interval caps the uncommitted tail; it is
not a safe whole-source replay window.

After a transient failure, the client replays the original source only if no
batch was successfully published and the failed operation is not ``in_doubt``.
Otherwise it raises. When any batch from the call may have committed, the
public exception has ``in_doubt=True`` even if the final native write itself
was provably not delivered. This includes local validation and Arrow stream
errors after publication. Internal checkpoints do not reset this call-level
status. The load may then have an already committed prefix, and an
application-level retry of the entire DataFrame can duplicate
it unless the table uses suitable ``DEDUP UPSERT KEYS``. A consumed one-shot
stream can be non-replayable with ``in_doubt=False`` when no rows could have
landed; its error asks the caller to supply a fresh reader.

These calls are independent of ``sf_dir``, and none converts the DataFrame
into row calls. ``dataframe()`` opens a direct connection just for that call
(borrowed from the pool for a pooled sender, opened from the sender's own
configuration — carrying its auth/TLS — for a standalone one). It has **no
ordering relationship** with rows buffered on a sender via ``row()`` and does
not flush them; publish those with ``flush()``. This works for any standalone
ws sender, whether built via ``Sender.from_conf`` / ``Sender.from_env`` or the
``Sender(...)`` constructor.

Over ILP/HTTP, ILP/TCP and QWP/UDP, ``sender.dataframe()`` is unchanged and
fully supported (over UDP it serializes row by row into fire-and-forget
datagrams, with the same delivery caveats as ``row()``).

Update imports from questdb.ingress
-----------------------------------

The 4.x ``questdb.ingress`` module is now a deprecated compatibility shim.
It keeps ILP/HTTP and ILP/TCP code running — including ``IngressError`` /
``IngressErrorCode`` aliases and explicit ``Buffer`` construction with
``sender.flush(buffer)`` — and emits one ``DeprecationWarning`` at import.
New code imports from the top-level package:

.. code-block:: python

    # Before
    from questdb.ingress import Sender, IngressError

    # After
    from questdb import Sender, QuestDBError

``Buffer`` is not part of the top-level API: buffers are managed internally
by senders. Where 4.x code built buffers on worker threads and flushed them
through one sender, borrow one pooled sender per thread instead.

Behavioural changes to watch for
--------------------------------

* **Removed QWP flight-window keys.** ``max_in_flight`` and
  ``in_flight_window`` are no longer accepted. Remove them from ws/wss
  configuration strings; the client raises ``QuestDBError(ConfigError)``
  instead of silently ignoring an obsolete setting.

* **Broader** ``except IngressError``. ``IngressError`` is an alias of the
  unified :class:`QuestDBError <questdb.QuestDBError>`, so handlers written
  for 4.x ingestion errors now also catch query, pool and egress failures
  raised through the same handle. Narrow by ``err.code`` where that matters.

* ``BadDataFrame.value`` **changed**. The ``BadDataFrame`` error code's
  numeric ``.value`` moved from 14 to ``0x10000``; raw value 14 now belongs
  to ``ServerRejection``. Codes 0–13 are unchanged. Compare by member
  identity (``err.code is QuestDBErrorCode.BadDataFrame``), never by
  integer.

* **Stricter timestamp handling.** Pre-epoch ``datetime`` scalars now floor
  toward negative infinity instead of truncating toward zero (4.x encoded
  fractional-second pre-epoch instants one second too high);
  :meth:`TimestampNanos.from_datetime <questdb.TimestampNanos.from_datetime>`
  raises ``ValueError`` for datetimes outside the 64-bit nanosecond range
  instead of silently overflowing; and negative sub-second
  ``datetime.timedelta`` durations (e.g. ``timedelta(milliseconds=-500)``)
  raise ``ValueError`` instead of being mis-read as positive.

* **Naive datetimes are UTC.** Every path — DataFrame column cells, scalar
  ``at=``, ``row()`` values, ``from_datetime``, query binds — interprets
  naive (timezone-unaware) values as UTC. 4.x interpreted naive *scalars*
  in the process-local timezone; if your values were local wall-clock
  times, convert with ``dt.astimezone()``; if they were already UTC, use
  ``dt.replace(tzinfo=timezone.utc)`` (or just silence the one-time
  ``UserWarning``). ``datetime.now()`` callers should switch to
  ``TimestampNanos.now()`` or ``datetime.now(timezone.utc)``.
