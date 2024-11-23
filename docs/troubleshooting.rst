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
