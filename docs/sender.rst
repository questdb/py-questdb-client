.. _sender:

=====================
Sending Data over ILP
=====================

The :class:`questdb.ingress.Sender` class is a client that inserts rows into
QuestDB via the ILP protocol.

It supports both TCP and HTTP protocols, authentication and TLS.

You should prefer to use HTTP over TCP in most cases as it provides better
feedback on errors and transaction control.

TCP is useful for high-throughput scenarios in higher latency networks.

Basic Usage
===========

In this example, data will be flushed and sent at the end of the ``with``
block.

.. code-block:: python

    from questdb.ingress import Sender, TimestampNanos

    conf = 'http::addr=localhost:9000;'
    with Sender.from_conf(conf) as sender:
        sender.row(
            'weather_sensor',
            symbols={'id': 'toronto1'},
            columns={'temperature': 23.5, 'humidity': 0.49},
            at=TimestampNanos.now())
        sensor.row(
            'weather_sensor',
            symbols={'id': 'dubai2'},
            columns={'temperature': 41.2, 'humidity': 0.34},
            at=TimestampNanos.now())

The ``Sender`` object holds an internal buffer. The call to ``.row()``
simply forwards all arguments to the :func:`Buffer.row` method.

The ``Sender`` class is generally initialised with a configuration string.

The format of the configuration string is::

    <protocol>::<key>=<value>;<key>=<value>;...;

The valid protocols are:

* ``tcp``: ILP/TCP
* ``tcps``: ILP/TCP with TLS
* ``http``: ILP/HTTP
* ``https``: ILP/HTTP with TLS

Only the ``addr=host:port`` key is mandatory. It specifies the hostname and port
of the QuestDB server. If omitted, the port will be defaulted to 9009 for TCP(s)
and 9000 for HTTP(s).

The full set of possible ``key=value;`` settings is documented in the
:ref:`configuring-sender` section.

Preparing Data
==============

Sending Rows
------------

You can send as many rows as you like by calling the
:func:`questdb.ingress.Sender.row` method. The full method arguments are
documented in the :func:`questdb.ingress.Buffer.row` method.

Sending Pandas Dataframes
-------------------------

The sender can also send data from a Pandas dataframe.

.. literalinclude:: ../examples/pandas_basic.py
   :language: python

For more details see :func:`questdb.ingress.Sender.dataframe`
and for full argument options see :func:`questdb.ingress.Buffer.dataframe`.

Populating Timestamps
---------------------

The ``at`` parameter is used to specify the timestamp of the row. It can be
either a ``TimestampNanos`` object or a ``datetime.datetime`` object.

In case of dataframes you can also specify the timestamp column name.

If you prefer the server to set the timestamp for you (not recommended),
you can use the ``at=ServerTimestamp`` singleton.

.. code-block:: python

    from questdb.ingress import Sender, ServerTimestamp

    conf = 'http::addr=localhost:9000;'
    with Sender.from_conf(conf) as sender:
        sender.row(
            'weather_sensor',
            symbols={'id': 'toronto1'},
            columns={'temperature': 23.5, 'humidity': 0.49},
            at=ServerTimestamp)

This removes the ability for QuestDB to deduplicate rows and is considered a
legacy feature.

Flushing
========

The sender accumulates data into an internal buffer. Flushing the buffer
sends the data to the server over the network and clears the buffer.

Flushing can be done explicitly or automatically.

Explicit flushing
-----------------

An explicit call to :func:`questdb.ingress.Sender.flush` will send any pending
data immediately.

.. code-block:: python

    conf = 'http::addr=localhost:9000;'
    with Sender.from_conf(conf) as sender:
        sender.row(
            'weather_sensor',
            symbols={'id': 'toronto1'},
            columns={'temperature': 23.5, 'humidity': 0.49},
            at=TimestampNanos.now())
        sender.flush()
        sender.row(
            'weather_sensor',
            symbols={'id': 'dubai2'},
            columns={'temperature': 41.2, 'humidity': 0.34},
            at=TimestampNanos.now())
        sender.flush()

Note that the last `sender.flush()` is entirely optional as flushing
also happens at the end of the ``with`` block.

.. _sender-auto-flushing:

Auto-flushing
-------------

To avoid accumulating very large buffers, the sender will - by default -
flush the buffer automatically.

Auto-flushing is triggered when appending a row to the internal sender
buffer and the buffer either:

* Reaches 75'000 rows (for HTTP) or 600 rows (for TCP).
* Hasn't been flushed for 1 second.

Here is an example that auto-flushes every 10 rows and disables
the interval-based auto-flushing logic.

``http::addr=localhost:9000;auto_flush_rows=10;auto_flush_interval=off;``

Here is auto-flushing disabled:

``http::addr=localhost:9000;auto_flush=off;``

See the :ref:`configuring-sender-auto-flushing` section for more details.

Error Reporting
===============

**TL;DR: Use HTTP for better error reporting**

The sender will do its best to check for errors before sending data to the
server.

When using the HTTP protocol, the server will send back an error message if
the data is invalid or if there is a problem with the server. This will be
raised as an class:`questdb.ingress.IngressError` exception.

The HTTP layer will also attempt retries, configurable via the 
:ref:`retry_timeout <configuring-sender-request>` parameter.`

When using the TCP protocol errors are *not* sent back from the server and
must be searched for in the logs. See the :ref:`troubleshooting-flushing`
section for more details.

HTTP transactions
=================

When using the HTTP protocol, the sender can be configured to send a batch of
rows as a single transaction.

**Transactions are limited to a single table.**

If auto-flusing is enabled, any pending data will be flushed before the
transaction is started.

Auto-flushing is disabled during the scope of the transaction.

The transaction is automatically committed when the ``with`` block is exited.

.. code-block:: python

    conf = 'http::addr=localhost:9000;'
    with Sender.from_conf(conf) as sender:
        with sender.transaction('weather_sensor') as txn:
            txn.row(
                symbols={'id': 'toronto1'},
                columns={'temperature': 23.5, 'humidity': 0.49},
                at=TimestampNanos.now())
            txn.row(
                symbols={'id': 'dubai2'},
                columns={'temperature': 41.2, 'humidity': 0.34},
                at=TimestampNanos.now())

You can complete a transaction explicity by calling the
:func:`questdb.ingress.SenderTransaction.commit` or the
:func:`questdb.ingress.SenderTransaction.rollback` methods.

Raising an exception from within the transaction ``with`` block will also cause
the transaction to be rolled back.


.. _configuring-sender:

Full configuration options
==========================

The configuration options are common between QuestDB clients and can also be
found in the core QuestDB `client library documentation <https://questdb.io/docs/reference/clients/overview/>`_.

Authentication
--------------

TCP Auth
~~~~~~~~

* ``username`` - ``str``: Username for TCP authentication (A.K.A. *kid*).
* ``token`` - ``str``: Token for TCP authentication (A.K.A. *d*).
* ``token_x`` - ``str``: Token X for TCP authentication (A.K.A. *x*).
* ``token_y`` - ``str``: Token Y for TCP authentication (A.K.A. *y*).

You can additionally set the ``auth_timeout`` parameter (milliseconds) to
control how long the client will wait for a response from the server during
the authentication process. The default is 15 seconds.

See the :ref:`auth_and_tls_example` example for more details.

HTTP Basic Auth
~~~~~~~~~~~~~~~

* ``username`` - ``str``: Username for HTTP basic authentication.
* ``password`` - ``str``: Password for HTTP basic authentication.

HTTP Bearer Token
~~~~~~~~~~~~~~~~~
* ``token`` - ``str``: Bearer token for HTTP authentication.

TLS
---

TLS in enabled by selecting the ``tcps`` or ``https`` protocol.

* ``tls_ca`` - The remote server's certificate authority verification mechamism.

  * ``'webpki_roots'``: Use the
    `webpki-roots <https://crates.io/crates/webpki-roots>`_ Rust crate to
    recognize certificates.

  * ``'os_roots'``: Use the OS-provided certificate store.

  * ``'webpki_and_os_roots'``: Use both the
    `webpki-roots <https://crates.io/crates/webpki-roots>`_ Rust crate and
    the OS-provided certificate store to recognize certificates.

  * ``pem_file``: Path to a PEM-encoded certificate authority file.
    This is useful for testing with self-signed certificates.

  The default is: ``'webpki_roots'``.

* ``tls_roots`` - ``str``: Path to a PEM-encoded certificate authority file.
  When used it defaults the ``tls_ca`` to ``'pem_file'``.

* ``tls_verify`` - ``'on'`` | ``'unsafe_off'``: Whether to verify the server's
  certificate. This should only be used for testing as a last resort and never
  used in production as it makes the connection vulnerable to man-in-the-middle
  attacks.
  
  The default is: ``'on'``.

.. _configuring-sender-auto-flushing:

Auto-flushing
-------------

The following paramers control the :ref:`sender-auto-flushing` behavior.

* ``auto_flush`` - ``'on'`` | ``'off'``: Global switch for the auto-flushing
  behavior.

  *Default: ``'on'``.*

* ``auto_flush_rows`` - ``int > 0`` | ``'off'``: The number of rows that will
  trigger a flush. Set to ``'off'`` to disable.
    
  *Default: 75000 (HTTP) | 600 (TCP).*

* ``auto_flush_bytes`` - ``int > 0`` | ``'off'``: The number of bytes that will
  trigger a flush. Set to ``'off'`` to disable.
        
  *Default: ``'off'``.*

* ``auto_flush_interval`` - ``int > 0`` | ``'off'``: The time in milliseconds
  that will trigger a flush. Set to ``'off'`` to disable.
    
  *Default: 1000 (millis).*

.. _configuring-sender-buffer:

Buffer
------

* ``init_buf_size`` - ``int > 0``: Initial buffer capacity.
    
  *Default: 65536 (64KiB).*

* ``max_buf_size`` - ``int > 0``: Maximum flushable buffer capacity.
    
  *Default: 104857600 (100MiB).*

* ``max_name_len`` - ``int > 0``: Maximum length of a table or column name.

  *Default: 127.*

.. _configuring-sender-request:

HTTP Request
------------

The following parameters control the HTTP request behavior.

* ``retry_timeout`` - ``int > 0``: The time in milliseconds to continue retrying
  after a failed HTTP request. The interval between retries is an exponential
  backoff starting at 10ms and doubling after each failed attempt up to a
  maximum of 1 second.
    
  *Default: 10000 (10 seconds).*

* ``request_timeout`` - ``int > 0``: The time in milliseconds to wait for a
  response from the server. This is in addition to the calculation derived from
  the ``request_min_throughput`` parameter.
    
  *Default: 10000 (10 seconds).*

* ``request_min_throughput`` - ``int > 0``: Minimum expected throughput in
  bytes per second for HTTP requests. If the throughput is lower than this
  value, the connection will time out.
  This is used to calculate an additional timeout on top of ``request_timeout``.
  This is useful for large requests.
  You can set this value to ``0`` to disable this logic.
    
  *Default: 102400 (100 KiB/s).*


The final request timeout calculation is::

    request_timeout + (buffer_size / request_min_throughput)


Connection
----------

* ``bind_interface`` - TCP-only, ``str``: Network interface to bind from.
  Useful if you have an accelerated network interface (e.g. Solarflare) and
  want to use it.
  
  The default is ``0.0.0.0``.


Programmatic configuration
==========================

You can also specify the configuration parameters programmatically:

.. code-block:: python

    from questdb.ingress import Sender, Protocol
    from datetime import timedelta

    with Sender(Protocol.Tcp, 'localhost', 9009,
            auto_flush=True,
            auto_flush_interval=timedelta(seconds=10)) as sender:
        ...


Python type mappings:

* Parameters that require strings take a ``str``.

* Parameters that require numbers can also take an ``int``.

* Millisecond durations can take an ``int`` or a ```datetime.timedelta``.

* Any ``'on'`` / ``'off'`` / ``'unsafe_off'`` parameters can also be specified
  as a ``bool``.

* Paths can also be specified as a ``pathlib.Path``.

