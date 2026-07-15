===================
5.0 Migration Guide
===================

Connect once, then stream, load, or query
=========================================

The QWP/WebSocket API has one connection-owning root, the :class:`QuestDB
<questdb.QuestDB>` handle returned by :func:`questdb.connect`:

* :meth:`QuestDB.sender <questdb.QuestDB.sender>` lends a row-building
  :class:`Sender <questdb.Sender>`;
* :meth:`QuestDB.dataframe <questdb.QuestDB.dataframe>` bulk-loads a whole
  DataFrame through the direct columnar path; and
* :meth:`QuestDB.query <questdb.QuestDB.query>` executes queries.

Existing row code can move from a standalone ``Sender`` to a pooled one:

.. code-block:: python

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
for the server's OK acknowledgement. To ingest concurrently, borrow one
sender per thread.

Move ws dataframe calls to QuestDB.dataframe
============================================

Over ``ws::`` / ``wss::``, ``sender.dataframe()`` raises: DataFrame bulk
loads over QWP are a database operation, not stream usage. Call
:meth:`QuestDB.dataframe <questdb.QuestDB.dataframe>` on the handle:

.. code-block:: python

    # Before: dataframe -> row Buffer serialization on the sender.
    with Sender.from_conf('ws::addr=localhost:9000;') as sender:
        sender.dataframe(frame, table_name='weather', at='ts')

    # After: direct columnar ingestion on the handle.
    with questdb.connect('ws::addr=localhost:9000;') as db:
        db.dataframe(frame, table_name='weather', at='ts')

``QuestDB.dataframe()`` commits the whole source before returning, is
independent of ``sf_dir``, and re-sends from your DataFrame on transient
failures (raising with ``in_doubt`` set when a blind re-send could duplicate
rows). It does not convert the dataframe into row calls.

Over ILP/HTTP, ILP/TCP and QWP/UDP, ``sender.dataframe()`` is unchanged and
fully supported (over UDP it serializes row by row into fire-and-forget
datagrams, with the same delivery caveats as ``row()``).

Update imports from questdb.ingress
===================================

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
