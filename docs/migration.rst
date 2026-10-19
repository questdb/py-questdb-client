================
Migration Guide
================

5.0 to 6.0
==========

Four changes need action: ``QuestDB.close()`` raising instead of waiting for
its own thread's lease, a sender's own callbacks no longer being able to
append to it, the exception a commit raises once its sender is closed, and a
warning an abandoned query result now surfaces instead of swallowing.

* :meth:`QuestDB.close <questdb.QuestDB.close>` **raises instead of waiting
  for a lease held by the calling thread.** It raises
  ``QuestDBError(InvalidApiCall)`` while the calling thread owns an active pool
  operation or a lease attributed to it; 5.0 waited, which never ended for a
  same-thread lease. A sender lease handed to a worker stays attributed to the
  borrowing thread until the worker first uses it, so a ``close()`` there that
  5.0 would have waited through now raises. A reader lease (``db.reader()``)
  stays attributed to the borrowing thread however it is used, so close the
  pool from another thread while one is open. For a sender lease, have the
  worker use the lease and signal that before closing, or close from a thread
  that never held it::

      sender = db.sender()
      used = threading.Event()

      def work():
          with sender:
              used.set()
              sender.row('trades', columns={'price': 1.0},
                         at=questdb.ServerTimestamp)

      threading.Thread(target=work).start()
      used.wait()
      db.close()  # waits for the worker to return the lease

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
  during ``dataframe()``, ``SenderTransaction.dataframe()`` included); a
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
  instead of an internal ``TypeError``. A commit whose flush fails leaves the
  sender's buffer empty and the transaction completed, exactly as in 5.0.

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
committed. Most loads queue their batches and commit once at the end. To keep
memory bounded, a very large Arrow load checkpoints about every 100 batches.
The client may checkpoint earlier if the connection cannot queue another batch
or if a batch must be split to fit. If a later batch fails, the exception means
that the load did not finish, not necessarily that no rows landed. Any already
committed prefix from that call remains in the table, and retrying the entire
DataFrame can duplicate it unless the table uses suitable
``DEDUP UPSERT KEYS``.

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
