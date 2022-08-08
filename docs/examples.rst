========
Examples
========

Basics
======

The following example connects to the database and sends two rows (lines).

The connection is unauthenticated and the data is sent at the end of the
``with`` block.

Here the :class:`questdb.ingress.Sender` is constructed with just ``host`` and
``port``.

.. literalinclude:: ../examples/basic.py
   :language: python


Authentication and TLS
======================

Continuing from the previous example, the connection is authenticated
and also uses TLS.

Here the :class:`questdb.ingress.Sender` is also constructed with the ``auth``
and ``tls`` arguments.

.. literalinclude:: ../examples/auth_and_tls.py
   :language: python


Explicit Buffers
================

For more advanced use cases where the same messages need to be sent to multiple
questdb instances or you want to decouple serialization and sending (as may be
in a multi-threaded application) construct :class:`questdb.ingress.Buffer`
objects explicitly, then pass them to the :func:`questdb.ingress.Sender.flush`
method.

Note that this bypasses ``auto-flush`` logic
(see :class:`questdb.ingress.Sender`) and you are fully responsible for ensuring
all data is sent.

.. literalinclude:: ../examples/buffer.py
   :language: python


Ticking Random Data and Timer-based Flush
=========================================

The following example somewhat mimics the behavior of a loop in an application.

It creates random ticking data at a random interval and flushes it explicitly
based on a timer if the auto-flushing logic was not triggered recently.

.. literalinclude:: ../examples/random_data.py
   :language: python
