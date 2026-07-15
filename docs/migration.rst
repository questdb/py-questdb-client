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

DataFrame bulk loads over QWP/WebSocket
=======================================

Over ``ws::`` / ``wss::``, DataFrame bulk loads use the direct columnar
path — a database operation, not stream serialization. Three equivalent
entry points:

.. code-block:: python

    with questdb.connect('ws::addr=localhost:9000;') as db:
        # On the handle:
        db.dataframe(frame, table_name='weather', at='ts')

        # Or on a pooled sender — same path, direct connection from the pool:
        with db.sender() as sender:
            sender.row('weather', columns={'t': 21.5}, at=ServerTimestamp)
            sender.dataframe(frame, table_name='weather', at='ts')
            sender.flush()

    # Or on a standalone sender — same path, poolless direct connection:
    with Sender.from_conf('ws::addr=localhost:9000;') as sender:
        sender.dataframe(frame, table_name='weather', at='ts')

All commit the whole source before returning, are independent of ``sf_dir``,
and re-send from your DataFrame on transient failures (raising with
``in_doubt`` set when a blind re-send could duplicate rows); none converts
the dataframe into row calls. ``dataframe()`` opens a direct connection just
for that call (borrowed from the pool for a pooled sender, opened from the
sender's own configuration — carrying its auth/TLS — for a standalone one)
and commits immediately — it has **no ordering relationship** with rows
buffered on a sender via ``row()`` and does not flush them; publish those
with ``flush()``. This works for any standalone ws sender, whether built
via ``Sender.from_conf`` / ``Sender.from_env`` or the ``Sender(...)``
constructor.

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
