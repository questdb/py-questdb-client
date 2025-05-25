.. _sender_conf:

=============
Configuration
=============

When constructing a :ref:`sender <sender>` you can pass a configuration string
to the :func:`Sender.from_conf <questdb.ingress.Sender.from_conf>` method.

.. code-block:: python

    from questdb.ingress import Sender

    conf = "http::addr=localhost:9009;username=admin;password=quest;"
    with Sender.from_conf(conf) as sender:
        ...

The format of the configuration string is::

    <protocol>::<key>=<value>;<key>=<value>;...;

.. note::

    * The keys are case-sensitive.
    * The trailing semicolon is mandatory.

The valid protocols are:

* ``tcp``: ILP/TCP
* ``tcps``: ILP/TCP with TLS
* ``http``: ILP/HTTP
* ``https``: ILP/HTTP with TLS

If you're unsure which protocol to use, see :ref:`sender_which_protocol`.

Only the ``addr=host:port`` key is mandatory. It specifies the hostname and port
of the QuestDB server.

The same configuration string can also be loaded from the ``QDB_CLIENT_CONF``
environment variable. This is useful for keeping sensitive information out of
your code.

.. code-block:: bash

    export QDB_CLIENT_CONF="http::addr=localhost:9009;username=admin;password=quest;"

.. code-block:: python

    from questdb.ingress import Sender

    with Sender.from_env() as sender:
        ...

Connection
==========

* ``addr`` - ``str``: The address of the server in the form of
  ``host:port``.

  This key-value pair is mandatory, but the port can be defaulted.
  If omitted, the port will be defaulted to 9009 for TCP(s)
  and 9000 for HTTP(s).

* ``bind_interface`` - TCP-only, ``str``: Network interface to bind from.
  Useful if you have an accelerated network interface (e.g. Solarflare) and
  want to use it.
  
  The default is ``0.0.0.0``.

.. _sender_conf_auth:

Authentication
==============

If you're using QuestDB enterprise you can read up on creating and permissioning
users in the `Enterprise quickstart <https://questdb.io/docs/guides/enterprise-quick-start/#4-ingest-data-influxdb-line-protocol>`_
and the `role-based access control <https://questdb.io/docs/operations/rbac/>`_ guides.

HTTP Bearer Token
-----------------
* ``token`` - ``str``: Bearer token for HTTP authentication.

HTTP Basic Auth
---------------

* ``username`` - ``str``: Username for HTTP basic authentication.
* ``password`` - ``str``: Password for HTTP basic authentication.

TCP Auth
--------

* ``username`` - ``str``: Username for TCP authentication (A.K.A. *kid*).
* ``token`` - ``str``: Token for TCP authentication (A.K.A. *d*).
* ``token_x`` - ``str``: Token X for TCP authentication (A.K.A. *x*).
* ``token_y`` - ``str``: Token Y for TCP authentication (A.K.A. *y*).

You can additionally set the ``auth_timeout`` parameter (milliseconds) to
control how long the client will wait for a response from the server during
the authentication process. The default is 15 seconds.

See the :ref:`auth_and_tls_example` example for more details.

.. _sender_conf_tls:

TLS
===

TLS in enabled by selecting the ``tcps`` or ``https`` protocol.

See the `QuestDB enterprise TLS documentation <https://questdb.io/docs/operations/tls/>`_
on how to enable this feature in the server.

Open source QuestDB does not offer TLS support out of the box, but you can
still use TLS by setting up a proxy in front of QuestDB, such as
`HAProxy <https://www.haproxy.org/>`.

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

  The default is: ``'webpki_and_os_roots'``.

* ``tls_roots`` - ``str``: Path to a PEM-encoded certificate authority file.
  When used it defaults the ``tls_ca`` to ``'pem_file'``.

* ``tls_verify`` - ``'on'`` | ``'unsafe_off'``: Whether to verify the server's
  certificate. This should only be used for testing as a last resort and never
  used in production as it makes the connection vulnerable to man-in-the-middle
  attacks.
  
  The default is: ``'on'``.

As an example, if you are in a corporate environment and need to use the OS
certificate store, you can use the following configuration string::

    https::addr=localhost:9009;tls_ca=os_roots;

Alternatively, if you are testing with a self-signed certificate, you can use
the following configuration string::

    https::addr=localhost:9009;tls_roots=/path/to/cert.pem;

For more details on using self-signed test certificates, see:

* For Open Source QuestDB: https://github.com/questdb/c-questdb-client/blob/main/tls_certs/README.md#self-signed-certificates

* For QuestDB Enterprise: https://questdb.io/docs/operations/tls/#demo-certificates

.. _sender_conf_auto_flush:

Auto-flushing
=============

The following parameters control the :ref:`sender_auto_flush` behavior.

* ``auto_flush`` - ``'on'`` | ``'off'``: Global switch for the auto-flushing
  behavior.

  Default: ``'on'``.

* ``auto_flush_rows`` - ``int > 0`` | ``'off'``: The number of rows that will
  trigger a flush. Set to ``'off'`` to disable.
    
  *Default: 75000 (HTTP) | 600 (TCP).*

* ``auto_flush_bytes`` - ``int > 0`` | ``'off'``: The number of bytes that will
  trigger a flush. Set to ``'off'`` to disable.
        
  Default: ``'off'``.

* ``auto_flush_interval`` - ``int > 0`` | ``'off'``: The time in milliseconds
  that will trigger a flush. Set to ``'off'`` to disable.
    
  Default: 1000 (millis).

.. _sender_conf_auto_flush_interval:

``auto_flush_interval``
-----------------------

The `auto_flush_interval` parameter controls how long the sender's buffer can be
left unflushed for after appending a new row via the
:func:`Sender.row <questdb.ingress.Sender.row>` or the
:func:`Sender.dataframe <questdb.ingress.Sender.dataframe>` methods.
It is defined in milliseconds.

Note that this parameter does *not* create a timer that counts down
each time data is added. Instead, the client checks the time elapsed since the
last flush each time new data is added. If the elapsed time exceeds the
specified ``auto_flush_interval``, the client automatically flushes the current
buffer to the database.

Consider the following example:

.. code-block:: python

    from questdb.ingress import Sender, TimestampNanos
    import time
    conf = "http::addr=localhost:9009;auto_flush_interval=1000;"
    with Sender.from_conf(conf) as sender:
        # row 1
        sender.row('table1', columns={'val': 1}, at=TimestampNanos.now())
        time.sleep(60)  # sleep for 1 minute
        # row 2
        sender.row('table2', columns={'val': 2}, at=TimestampNanos.now())

In this example above, "row 1" will not be flushed for a whole minute, until
"row 2" is added and the ``auto_flush_interval`` limit of 1 second is exceeded,
causing both "row 1" and "row 2" to be flushed together.

If you need consistent flushing at specific intervals, you should set
``auto_flush_interval=off`` and implement your own timer-based logic.
The :ref:`sender_advanced` documentation should help you.

.. _sender_conf_protocol_version:

Protocol Version
======

Specifies the version of Ingestion Line Protocol to use. Valid options are:

* ``1`` - Text-based format compatible with InfluxDB line protocol.

* ``2`` - Array support and binary format serialization for f64.

* ``auto`` (default) - Automatic version selection based on protocol type.

  HTTP/HTTPS: Auto-detects server capability during handshake (supports version negotiation)

  TCP/TCPS: Defaults to version 1 for compatibility

.. _sender_conf_buffer:

Buffer
======

* ``protocol_version`` - ``int (1, 2)``: Buffer protocol version.

* ``init_buf_size`` - ``int > 0``: Initial buffer capacity.
    
  Default: 65536 (64KiB).

* ``max_buf_size`` - ``int > 0``: Maximum flushable buffer capacity.
    
  Default: 104857600 (100MiB).

* ``max_name_len`` - ``int > 0``: Maximum length of a table or column name.

  Default: 127.

.. _sender_conf_request:

HTTP Request
============

The following parameters control the HTTP request behavior.

* ``retry_timeout`` - ``int > 0``: The time in milliseconds to continue retrying
  after a failed HTTP request. The interval between retries is an exponential
  backoff starting at 10ms and doubling after each failed attempt up to a
  maximum of 1 second.
    
  Default: 10000 (10 seconds).

* ``request_timeout`` - ``int > 0``: The time in milliseconds to wait for a
  response from the server. This is in addition to the calculation derived from
  the ``request_min_throughput`` parameter.
    
  Default: 10000 (10 seconds).

* ``request_min_throughput`` - ``int > 0``: Minimum expected throughput in
  bytes per second for HTTP requests. If the throughput is lower than this
  value, the connection will time out.
  This is used to calculate an additional timeout on top of ``request_timeout``.
  This is useful for large requests.
  You can set this value to ``0`` to disable this logic.
    
  Default: 102400 (100 KiB/s).


The final request timeout calculation is::

    request_timeout + (buffer_size / request_min_throughput)
