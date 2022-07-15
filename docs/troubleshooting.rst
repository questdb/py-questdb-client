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
:func:`questdb.ingress.Sender.flush` often enough.

The :class:`questdb.ingress.Sender` class only  provides auto-flushing based on
a buffer size and *not on a timer*.


Inspecting and debugging errors
===============================

Both the :class:`questdb.ingress.Sender` and :class:`questdb.ingress.Buffer`
types support ``__len__`` and ``__str__`` methods to inspect the buffer that is
about to be flushed.

Note that the ILP protocol does not send errors back to the client.

On error, the QuestDB server will disconnect and any error messages will be
present in the `server logs
<https://questdb.io/docs/concept/root-directory-structure#log-directory>`_.


Asking for help
===============

The best way to get help is through `Slack <https://slack.questdb.io>`_.
