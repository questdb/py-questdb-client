===================
5.0 Migration Guide
===================

Use one Client for QWP
======================

The recommended QWP/WebSocket API now has one connection-owning root:

* :meth:`Client.sender <questdb.Client.sender>` lends a row-building sender;
* :meth:`Client.dataframe <questdb.Client.dataframe>` ingests a whole source
  through the direct columnar path; and
* :meth:`Client.query <questdb.Client.query>` executes queries.

Existing row code can move from a standalone ``Sender`` to a Client lease:

.. code-block:: python

    # Before: a standalone multi-transport sender.
    with Sender.from_conf('ws::addr=localhost:9000;') as sender:
        sender.row('weather', columns={'temperature': 21.5},
                   at=ServerTimestamp)
        sender.flush()

    # After: the Client owns the pool and lends a row builder.
    with Client.from_conf('ws::addr=localhost:9000;') as client:
        with client.sender() as sender:
            sender.row('weather', columns={'temperature': 21.5},
                       at=ServerTimestamp)
            sender.flush(wait=True)

The Client sender publishes into the store-and-forward QWP path. A plain
``flush()`` returns after local acceptance; ``flush(wait=True)`` also waits for
the server's OK acknowledgement. The lease has no dataframe method.

Move dataframe calls to Client.dataframe
========================================

``Sender.dataframe()`` and ``Buffer.dataframe()`` are deprecated in 5.0.0 and
planned for removal in 6.0.0. Both emit ``DeprecationWarning``. Move whole
dataframes directly to ``Client.dataframe()``:

.. code-block:: python

    # Before: dataframe -> row Buffer serialization.
    with Sender.from_conf('ws::addr=localhost:9000;') as sender:
        sender.dataframe(frame, table_name='weather', at='ts')

    # After: direct Chunk/Arrow ingestion hidden behind Client.dataframe().
    with Client.from_conf('ws::addr=localhost:9000;') as client:
        client.dataframe(frame, table_name='weather', at='ts')

``Client.dataframe()`` keeps its whole-source commit and retry contract; it
does not convert the dataframe into row calls. ``Sender`` itself remains
available for ILP, QWP/UDP, and standalone multi-transport use.
