===============
Troubleshooting
===============

Common issues
=============

You may be experiencing one of the issues below.

Production-optimized QuestDB configuration
------------------------------------------

If you can't initially see your data through a ``select`` SQL query straight
away, this is normal: by default the database will only commit data it receives
though the line protocol periodically to maximize throughput.

For dev/testing you may want to tune the following database configuration
parameters as so::

    # server.conf
    cairo.max.uncommitted.rows=1
    line.tcp.maintenance.job.interval=100


The default QuestDB configuration is more applicable for a production
environment.

For these and more configuration parameters refer to `database configuration
<https://questdb.io/docs/reference/configuration/>`_ documentation.


Infrequent Flushing
-------------------

You may not see data appear in a timely manner because you're not calling
:func:`flush <questdb.ingress.Sender.flush>` often enough.

You might be having issues with the :class:`Sender <questdb.ingress.Sender>`'s
:ref:`auto-flush <sender_auto_flush>` feature.

.. _troubleshooting-flushing:

Errors during flushing
----------------------

Decimal Column Errors
~~~~~~~~~~~~~~~~~~~~~

If you're trying to ingest decimal data and encountering errors, check the
following:

**Table not pre-created**: Unlike other column types, ``DECIMAL`` columns cannot
be auto-created. You must create the table with ``DECIMAL(precision, scale)``
columns before sending data:

.. code-block:: sql

    CREATE TABLE my_table (
        symbol SYMBOL,
        price DECIMAL(18, 6),
        timestamp TIMESTAMP
    ) TIMESTAMP(timestamp) PARTITION BY DAY;

**Protocol version mismatch**: Decimal support requires protocol version 3,
which is only available on QuestDB server 9.2.0 or later.

* For HTTP/HTTPS: Protocol version 3 is auto-negotiated. Ensure your server is
  version 9.2.0 or later.

* For TCP/TCPS: You must explicitly configure ``protocol_version=3`` in your
  configuration string::

      tcp::addr=localhost:9009;protocol_version=3;

**Precision/scale mismatch**: Ensure the precision and scale of your Python
:class:`decimal.Decimal` or PyArrow decimal values match the table definition.
For example, if the table has ``DECIMAL(12, 6)``, values with more than 6
decimal places or more than 12 total digits will cause errors.

For more details on decimal types, see the
`QuestDB DECIMAL documentation <https://questdb.io/docs/reference/sql/datatypes/#decimal>`_.

ILP/TCP Server disconnects
~~~~~~~~~~~~~~~~~~~~~~~~~~

If you're using TCP instead of HTTP, you may see a server disconnect after
flushing.

If the server receives invalid data over ILP/TCP it will drop the connection.

The ILP/TCP protocol does not send errors back to the client. Instead,
by design, it will disconnect a client if it encounters any insertion errors.
This is to avoid errors going unnoticed.

As an example, if a client were to insert a ``STRING`` value into a ``BOOLEAN``
column, the QuestDB server would disconnect the client.

To determine the root cause of a disconnect, inspect the `server logs
<https://questdb.io/docs/concept/root-directory-structure#log-directory>`_.

.. note::

    For a better developer experience consider using
    :ref:`HTTP instead of TCP <sender_which_protocol>`.


Logging outgoing messages
~~~~~~~~~~~~~~~~~~~~~~~~~

To understand what data was sent to the server, you may log outgoing messages
from Python.

Here's an example if you append rows to the ``Sender`` object:

.. code-block:: python

    import textwrap

    with Sender.from_conf(...) as sender:
        # sender.row(...)
        # sender.row(...)
        # ...
        pending = str(sender)
        logging.info('About to flush:\n%s', textwrap.indent(pending, '    '))
        sender.flush()

Alternatively, if you're constructing buffers explicitly:

.. code-block:: python

    import textwrap

    buffer = sender.new_buffer()
    # buffer.row(...)
    # buffer.row(...)
    # ...
    pending = str(buffer)
    logging.info('About to flush:\n%s', textwrap.indent(pending, '    '))
    sender.flush(buffer)


Note that to handle out-of-order messages efficiently, the QuestDB server will
delay appling changes it receives over ILP after a configurable
`commit lag <https://questdb.io/docs/guides/out-of-order-commit-lag>`_.

Due to this commit lag, the line that caused the error may not be the last line.


Asking for help
===============

The best way to get help is through our `Community Forum <https://community.questdb.io>`_.
