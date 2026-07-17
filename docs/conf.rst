.. _sender_conf:

=============
Configuration
=============

Configuration strings configure both the QWP/WebSocket
:class:`QuestDB <questdb.QuestDB>` handle and the legacy standalone
:class:`Sender <questdb.Sender>`:

.. code-block:: python

    import questdb

    conf = "ws::addr=localhost:9000;username=admin;password=quest;"
    with questdb.connect(conf) as db:
        ...

.. code-block:: python

    from questdb import Sender

    conf = "http::addr=localhost:9009;username=admin;password=quest;"
    with Sender.from_conf(conf) as sender:
        ...

The format of the configuration string is::

    <protocol>::<key>=<value>;<key>=<value>;...;

.. note::

    The keys are case-sensitive.

The valid protocols are:

* ``ws``: QWP/WebSocket (QuestWire Protocol) — **recommended**
* ``wss``: QWP/WebSocket with TLS — **recommended**
* ``http``: ILP/HTTP
* ``https``: ILP/HTTP with TLS
* ``tcp``: ILP/TCP
* ``tcps``: ILP/TCP with TLS
* ``udp``: QWP/UDP

``ws`` / ``wss`` configure the :class:`QuestDB <questdb.QuestDB>` handle via
:func:`questdb.connect` and the standalone
:class:`Sender <questdb.Sender>` alike; the other protocols are
:class:`Sender <questdb.Sender>`-only. If you're unsure which protocol to
use, see :ref:`sender_which_protocol`.

Only the ``addr=host:port`` key is mandatory. It specifies the hostname and port
of the QuestDB server.

The same configuration string can also be loaded from the ``QDB_CLIENT_CONF``
environment variable. This is useful for keeping sensitive information out of
your code.

.. code-block:: bash

    export QDB_CLIENT_CONF="http::addr=localhost:9009;username=admin;password=quest;"

.. code-block:: python

    from questdb import Sender

    with Sender.from_env() as sender:
        ...

Connection
==========

* ``addr`` - ``str``: The address of the server in the form of
  ``host:port``.

  This key-value pair is mandatory, but the port can be defaulted.
  If omitted, the port will be defaulted to 9009 for TCP(s),
  9000 for HTTP(s) and QWP/WebSocket, and 9007 for QWP/UDP.

  For ``ws`` / ``wss`` the value may be a comma-separated server list naming
  every node of the deployment, e.g.
  ``ws::addr=node1:9000,node2:9000,node3:9000;``. One configuration string
  configures the whole deployment; the client walks the list to find a
  suitable node and fails over within it.

* ``bind_interface`` - TCP/QWP-UDP only, ``str``: Network interface to bind
  from. Useful if you have an accelerated network interface (e.g. Solarflare)
  and want to use it.

  The default is ``0.0.0.0``.

* ``max_datagram_size`` - QWP/UDP-only, ``int > 0``: Maximum UDP datagram
  payload size in bytes.

  Default: 1400.

* ``multicast_ttl`` - QWP/UDP-only, ``int (0-255)``: Multicast TTL
  (time-to-live) for UDP datagrams.

  Default: 1.

* ``qwp_ws_progress`` - QWP/WebSocket-only, ``background`` | ``manual``:
  Whether frame acknowledgements are progressed on a background thread
  (``background``) or only when the sender's drive/await/flush methods are
  called (``manual``).

  Default: ``background``.

.. _sender_conf_pooling:

Connection pooling (QWP/WebSocket)
==================================

The following keys apply to the :class:`QuestDB <questdb.QuestDB>` handle
returned by :func:`questdb.connect` (or
:meth:`QuestDB.from_conf <questdb.QuestDB.from_conf>`), which owns
connection pools for both ingestion and queries. The key names align with
the Java client's configuration.

* ``sender_pool_min`` - ``int``: Warm/minimum sender (ingestion)
  connections the reaper keeps once connections have been opened.

  Default: 1.

* ``sender_pool_max`` - ``int``: Cap on the ingestion and direct
  (DataFrame) pools, each capped independently.

  Default: 4.

* ``query_pool_min`` - ``int``: Warm/minimum reader (query) connections.

  Default: 1.

* ``query_pool_max`` - ``int``: Cap on the reader pool.

  Default: 4.

* ``acquire_timeout_ms`` - ``int``: How long a borrow at the cap waits for
  a connection to be returned before failing. ``0`` fails fast.

  Default: 5000.

* ``idle_timeout_ms`` - ``int``: Idle time after which above-minimum idle
  connections are reaped.

  Default: 60000.

* ``pool_reap`` - ``'auto'`` | ``'manual'``: With ``auto``, a background
  thread periodically reaps idle above-minimum connections per
  ``idle_timeout_ms``. With ``manual``, no background thread runs and the
  application calls :meth:`QuestDB.reap_idle <questdb.QuestDB.reap_idle>`
  on its own cadence.

  Default: ``'auto'``.

* ``sf_dir`` - ``str``: Opt-in directory for disk-backed store-and-forward
  queues used by the pooled ingestion senders. DataFrame ingestion
  (:meth:`QuestDB.dataframe <questdb.QuestDB.dataframe>` and the
  ``dataframe()`` method on pooled and standalone ws senders) always uses
  the direct column path and is independent of ``sf_dir``. See
  :ref:`sender_conf_sf` for the queue's own settings.

.. _sender_conf_reconnect:

Reconnect and delivery (QWP/WebSocket)
======================================

Row senders publish frames into a local store-and-forward queue; a
background runner delivers them and reconnects on transient failures,
walking the ``addr`` server list.

* ``reconnect_max_duration_millis`` - ``int``: Total reconnect/failover
  budget after a connection loss. Authentication and protocol-version
  failures are terminal and are not retried.

  Default: 300000 (5 minutes).

* ``reconnect_initial_backoff_millis`` - ``int``: First reconnect delay;
  subsequent attempts back off exponentially with jitter.

  Default: 100.

* ``reconnect_max_backoff_millis`` - ``int``: Reconnect delay ceiling.

  Default: 5000.

* ``connect_timeout`` - ``int``: Per-attempt TCP/TLS/upgrade connect
  deadline in milliseconds. Unset, the operating system's TCP deadline
  applies (background drainers use a finite internal fallback).

* ``close_flush_timeout_millis`` - ``int``: Best-effort drain window when a
  connection is retired or the handle closes. An in-memory queue that
  cannot drain within it loses its remaining tail; a disk-backed queue
  (``sf_dir``) keeps it for restart replay.

  Default: 5000.

* ``request_durable_ack`` - ``'on'`` | ``'off'``: Negotiate durable
  acknowledgements: the server acknowledges frames only once they are
  durably stored (e.g. uploaded to object storage on Enterprise
  deployments). A server without the capability rejects the first
  operation.

  Default: ``'off'``.

* ``durable_ack_keepalive_interval_millis`` - ``int``: Keepalive ping
  cadence while waiting for durable acknowledgements.

  Default: 200.

* ``error_inbox_capacity`` - ``int >= 16``: Per-connection capacity of the
  server-rejection diagnostic ring (oldest entries are dropped on
  overflow, counted by
  :func:`Sender.error_events_dropped <questdb.Sender.error_events_dropped>`).

  Default: 256.

.. _sender_conf_sf:

Store-and-forward queue (QWP/WebSocket)
=======================================

* ``sender_id`` - ``str``: Base identity for disk store-and-forward slots
  (``<sf_dir>/<sender_id>-ingest-<index>``). Give each handle sharing an
  ``sf_dir`` a distinct ``sender_id`` so restart replay reopens the right
  slots.

  Default: ``'default'``.

* ``sf_max_bytes`` - ``int``: Segment and single-payload size cap for the
  store-and-forward queue.

  Default: 4194304 (4 MiB).

* ``sf_max_total_bytes`` - ``int``: Total queue budget per sender
  connection. When producers outrun the server past this budget,
  publication waits up to ``sf_append_deadline_millis`` for ack-driven
  space, then raises.

  Default: 134217728 (128 MiB) in memory; 10737418240 (10 GiB) with
  ``sf_dir``.

* ``sf_append_deadline_millis`` - ``int``: Maximum no-progress wait for
  queue space before publication fails.

  Default: 30000.

* ``drain_orphans`` - ``'on'`` | ``'off'``: Also adopt and replay unowned
  disk slots left in ``sf_dir`` by senders with *other* ``sender_id``
  bases. The pool always recovers its own managed slots on restart.

  Default: ``'off'``.

* ``max_background_drainers`` - ``int``: Concurrency cap for background
  slot recovery drains.

  Default: 4.

.. _sender_conf_egress:

Query egress (QWP/WebSocket)
============================

These keys shape :meth:`QuestDB.query <questdb.QuestDB.query>` /
:meth:`QuestDB.reader <questdb.QuestDB.reader>` connections.

* ``compression`` - ``'raw'`` | ``'zstd'`` | ``'auto'``: Result-set
  compression. ``auto`` accepts Zstandard when the server supports it;
  decompression is transparent.

  Default: ``'raw'``.

* ``compression_level`` - ``int (1-22)``: Zstandard level advertised to the
  server (which clamps to its supported range). Ignored with
  ``compression=raw``.

  Default: 1.

* ``target`` - ``'any'`` | ``'primary'`` | ``'replica'``: Cluster role the
  reader connections must land on.

  Default: ``'any'``.

* ``failover`` - ``'on'`` | ``'off'``: Permit mid-query failover to another
  endpoint of the ``addr`` list.

  Default: ``'on'``.

* ``failover_max_attempts`` - ``int``: Total execute attempts per query,
  including the first.

  Default: 8.

* ``failover_max_duration_ms`` - ``int``: Overall failover budget per
  query; ``0`` means unbounded.

  Default: 30000.

* ``failover_backoff_initial_ms`` - ``int``: First retry delay; ``0``
  disables sleeping between attempts.

  Default: 50.

* ``failover_backoff_max_ms`` - ``int``: Retry-delay ceiling.

  Default: 1000.

Advanced QWP tuning
===================

Rarely needed; the defaults suit most deployments.

* ``max_in_flight`` - ``int``: Published-but-unacknowledged frame window
  per connection. Default: 128.

* ``max_frame_rejections`` - ``int``: Consecutive no-progress rejections of
  the same frame before the client stops retrying it and turns terminal.
  Default: 4.

* ``poison_min_escalation_window_millis`` - ``int``: Minimum wall-clock
  window over which those rejections must spread before escalation.
  Default: 5000.

.. _sender_conf_auth:

Authentication
==============

If you're using QuestDB enterprise you can read up on creating and permissioning
users in the `Enterprise quickstart <https://questdb.com/docs/guides/enterprise-quick-start/#4-ingest-data-influxdb-line-protocol>`_
and the `role-based access control <https://questdb.com/docs/operations/rbac/>`_ guides.

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

TLS is enabled by selecting the ``tcps``, ``https``, or ``wss`` protocol.

See the `QuestDB enterprise TLS documentation <https://questdb.com/docs/operations/tls/>`_
on how to enable this feature in the server.

Open source QuestDB does not offer TLS support out of the box, but you can
still use TLS by setting up a proxy in front of QuestDB, such as
`HAProxy <https://www.haproxy.org/>`_.

* ``tls_ca`` - The remote server's certificate authority verification mechanism.

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

  For ``wss``, this can also point at a JKS or PKCS#12 keystore when
  paired with ``tls_roots_password``.

* ``tls_roots_password`` - ``str``: Password for the JKS or PKCS#12 keystore
  configured by ``tls_roots``. This is supported only for ``wss``.

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

* For QuestDB Enterprise: https://questdb.com/docs/operations/tls/#demo-certificates

.. _sender_conf_auto_flush:

Auto-flushing
=============

The following parameters control the :ref:`sender_auto_flush` behavior.

* ``auto_flush`` - ``'on'`` | ``'off'``: Global switch for the auto-flushing
  behavior.

  Default: ``'on'``.

* ``auto_flush_rows`` - ``int > 0`` | ``'off'``: The number of rows that will
  trigger a flush. Set to ``'off'`` to disable.

  *Default: 75000 (HTTP) | 600 (TCP, QWP/UDP).*

* ``auto_flush_bytes`` - ``int > 0`` | ``'off'``: The number of bytes that will
  trigger a flush. Set to ``'off'`` to disable.

  *Default: off (TCP, HTTP) | max_datagram_size (QWP/UDP, 1400 by default).*

* ``auto_flush_interval`` - ``int > 0`` | ``'off'``: The time in milliseconds
  that will trigger a flush. Set to ``'off'`` to disable.
    
  Default: 1000 (millis).

.. _sender_conf_auto_flush_interval:

``auto_flush_interval``
-----------------------

The `auto_flush_interval` parameter controls how long the sender's buffer can be
left unflushed for after appending a new row via the
:func:`Sender.row <questdb.Sender.row>` or the
:func:`Sender.dataframe <questdb.Sender.dataframe>` methods.
It is defined in milliseconds.

Note that this parameter does *not* create a timer that counts down
each time data is added. Instead, the client checks the time elapsed since the
last flush each time new data is added. If the elapsed time exceeds the
specified ``auto_flush_interval``, the client automatically flushes the current
buffer to the database.

Consider the following example:

.. code-block:: python

    from questdb import Sender, TimestampNanos
    import time
    conf = "http::addr=localhost:9000;auto_flush_interval=1000;"
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
================

Specifies the version of InfluxDB Line Protocol to use.
Not applicable for QWP/UDP senders.

Here is a configuration string with ``protocol_version=2`` for ``TCP``::

    tcp::addr=localhost:9000;protocol_version=2;

Valid options are:

* ``1`` - Text-based format compatible with InfluxDB database when used over HTTP.

* ``2`` - Array support and binary format serialization for 64-bit floats (version specific to QuestDB).

* ``3`` - Decimal type support (requires QuestDB 9.2.0+). Also includes all features from version 2.

* ``auto`` (default) - Automatic version selection based on protocol type.

  HTTP/HTTPS: Auto-detects server capability during handshake (supports version negotiation)

  TCP/TCPS: Defaults to version 1 for compatibility

.. note::
    Protocol version ``2`` requires QuestDB server version 9.0.0 or higher.
    
    Protocol version ``3`` requires QuestDB server version 9.2.0 or higher and
    is needed for ingesting data into ``DECIMAL`` columns.

.. _sender_conf_buffer:

Buffer
======

Settings for the sender's internal serialization buffer.

* ``protocol_version`` - ``int (1, 2, 3)``: Buffer protocol version.
  Version 3 is required for ``DECIMAL`` columns.

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
  backoff starting at 10ms and doubling after each failed attempt up to
  ``retry_max_backoff_millis``.
    
  Default: 10000 (10 seconds).

* ``retry_max_backoff_millis`` - ``int >= 10``: Maximum per-attempt backoff in
  milliseconds for the HTTP retry loop. As a ``Sender`` / ``from_conf`` /
  ``from_env`` keyword argument this is named ``retry_max_backoff``.

  Default: 1000 (1 second).

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
