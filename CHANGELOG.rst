.. _changelog:


Changelog

=========

3.0.0 (2025-07-07)
------------------

Features
~~~~~~~~

This is the first major release of the QuestDB Python client library
which supports n-dimensional arrays of doubles for QuestDB servers 9.0.0 and up.

.. code-block:: python

        import numpy as np

        # Create 2D numpy array
        array_2d = np.array([
            [1.1, 2.2, 3.3],
            [4.4, 5.5, 6.6]], dtype=np.float64)

        sender.row(
            'table',
            columns={'array_2d': array_2d},
            at=timestamp)

The array data is sent over a new protocol version (2) that is auto-negotiated
when using HTTP(s), or can be specified explicitly via the ``protocol_version=2``
parameter when using TCP(s).

We recommend using HTTP(s), but here is an TCP example, should you need it::

  tcp::addr=localhost:9009;protocol_version=2;

When using ``protocol_version=2`` (with either TCP(s) or HTTP(s)), the sender
will now also serialize ``float`` (double-precision) columns as binary.
You might see a performance uplift if this is a dominant data type in your
ingestion workload.

When compared to 2.0.4, this release includes all the changes from 3.0.0rc1 and
additionally:

* Has optimised ingestion performance from C-style contiguous NumPy arrays.

* Warns at most every 10 minutes when burst of reconnections are detected.
  This is to warn about code patterns that may lead to performance issues, such as

  .. code-block:: python

    # Don't do this! Sender objects should be reused.
    for row_fields in data:
        with Sender.from_conf(conf) as sender:
            sender.row(**row_fields)

  This feature can be disabled in code by setting:

  .. code-block:: python

    import questdb.ingress as qi
    qi.WARN_HIGH_RECONNECTS = False

* Fixed ILP/TCP connection shutdown on Windows where some rows could be
  lost when closing the ``Sender``, even if explicitly flushed.

* Added a "Good Practices" section to the "Sending Data over ILP" section of
  the documentation.

Breaking Changes
~~~~~~~~~~~~~~~~
Refer to the release notes for 3.0.0rc1 for the breaking changes introduced
in this release compared to 2.x.x.


3.0.0rc1 (2025-06-02)
---------------------

This is the pre-release of a major release introducing array ingestion and some
minor breaking changes.

Features
~~~~~~~~
* Array Data Type Support. Adds native support for NumPy arrays
  (currently only for ``np.float64`` element type and up to 32 dimensions).

.. note::
    **Server Requirement**: This feature requires QuestDB server version 9.0.0 or higher.
    Ensure your server is upgraded before ingesting array types, otherwise data ingestion will fail.

.. code-block:: python

        import numpy as np

        # Create 2D numpy array
        array_2d = np.array([
            [1.1, 2.2, 3.3],
            [4.4, 5.5, 6.6]], dtype=np.float64)

        sender.row(
            'table',
            columns={'array_2d': array_2d},
            at=timestamp)

* Implements binary protocol for columns of ``float`` (double-precision) and
  ``numpy.ndarray[np.float64]``, with performance improvements for these
  two datatypes.

Breaking Changes
~~~~~~~~~~~~~~~~
* Buffer Constructor Changes. The ``Buffer`` constructor now requires the ``protocol_version`` parameter.
  You can create buffer through the sender for automatic ``protocol_version`` management:

.. code-block:: python

    buf = sender.new_buffer()  # protocol_version determined automatically
    buf.row(
      'table',
      columns={'arr': np.array([1.5, 3.0], dtype=np.float64)},
      at=timestamp)

* To access the raw payload, call ``bytes(sender)`` or ``bytes(buffer)`` (
  rather than calling the ``str`` function on the same objects as in version
  2.x.x of the questdb library) method.

* **NumPy Dependency**

  Array functionality mandates NumPy installation.

* **Sender/Buffer String Conversion Removal**

  The legacy string conversion via `str(sender)` is removed.
  Access raw binary payloads through the `bytes(sender)` method:

  .. code-block:: python

      # for debugging
      payload = bytes(sender)

* Python 3.8 support is dropped.

  The minimum supported Python version is now 3.9.

2.0.4 (2025-04-02)
------------------

Building for Python 3.13.

2.0.3 (2024-06-06)
------------------

Patch release with bug fixes. No breaking changes.

Bug fixes
~~~~~~~~~
* HTTP timeout wasn't always being correctly applied in the downstream ``c-questdb-client`` dependency.
* ``request_timeout > 0`` will now be enforced. This was always required, but would not error.
* Fixed the source distribution "sdist" package: This allows the package to be installed from source
  via "pip install" on previously unsupported platforms (YMMV).

2.0.2 (2024-04-11)
------------------

Patch release with a performance bug fix. No breaking changes.

Bug fixes
~~~~~~~~~
* Fixed the defaulting logic for ``auto_flush_rows`` parameter for HTTPS.
  It is now correctly set to 75000 rows by default. The old incorrect default
  of 600 rows was causing the sender to flush too often, impacting performance.
  Note that TCP, TCPS and HTTP were not affected.

Features
~~~~~~~~
* The sender now exposes the ``auto_flush`` settings as read-only properties.
  You can inspect the values in use with ``.auto_flush``, ``.auto_flush_rows``,
  ``.auto_flush_interval`` and ``.auto_flush_bytes``.

2.0.1 (2024-04-03)
------------------

Patch release with bug fixes, no API changes and some documentation tweaks.

Bug fixes
~~~~~~~~~
* Fixed a bug where an internal "last flushed" timestamp used
  by ``auto_flush_interval`` wasn't updated correctly causing the auto-flush
  logic to trigger after each row.

* Removed two unnecessary debugging ``print()`` statements that were
  accidentally left in the code in ``Sender.from_conf()`` and
  ``Sender.from_env()``.

Documentation
~~~~~~~~~~~~~
* Introduced the ability to optionally install ``pandas`` and ``pyarrow`` via
  ``python3 -m pip install -U questdb[dataframe]`` and updated the documentation
  to reflect this.


2.0.0 (2024-03-19)
------------------

This is a major release with new features and breaking changes.

Features
~~~~~~~~

* Support for ILP over HTTP. The sender can now send data to QuestDB via HTTP
  instead of TCP. This provides error feedback from the server and new features.

  .. code-block:: python

    conf = 'http::addr=localhost:9000;'
    with Sender.from_conf(conf) as sender:
        sender.row(...)
        sender.dataframe(...)

        # Will raise `IngressError` if there is an error from the server.
        sender.flush()

* New configuration string construction. The sender can now be also constructed
  from a :ref:`configuration string <sender_conf>` in addition to the
  constructor arguments.
  This allows for more flexible configuration and is the recommended way to
  construct a sender.
  The same string can also be loaded from the ``QDB_CLIENT_CONF`` environment
  variable.
  The constructor arguments have been updated and some options have changed.

* Explicit transaction support over HTTP. A set of rows for a single table can
  now be committed via the sender transactionally. You can do this using a
  ``with sender.transaction('table_name') as txn:`` block.

  .. code-block:: python

    conf = 'http::addr=localhost:9000;'
    with Sender.from_conf(conf) as sender:
        with sender.transaction('test_table') as txn:
            # Same arguments as the sender methods, minus the table name.
            txn.row(...)
            txn.dataframe(...)

* A number of documentation improvements.


Breaking Changes
~~~~~~~~~~~~~~~~

* New ``protocol`` parameter in the
  :ref:`Sender <sender_programmatic_construction>` constructor.

  In previous version the protocol was always TCP.
  In this new version you must specify the protocol explicitly.

* New auto-flush defaults. In previous versions
  :ref:`auto-flushing <sender_auto_flush>` was enabled by
  default and triggered by a maximum buffer size. In this new version
  auto-flushing is enabled by row count (600 rows by default) and interval
  (1 second by default), while auto-flushing by buffer size is disabled by
  default.

  The old behaviour can be still be achieved by tweaking the auto-flush
  settings.
  
  .. list-table::
    :header-rows: 1

    * - Setting
      - Old default
      - New default
    * - **auto_flush_rows**
      - off
      - 600
    * - **auto_flush_interval**
      - off
      - 1000
    * - **auto_flush_bytes**
      - 64512
      - off

* The ``at=..`` argument of :func:`row <questdb.ingress.Sender.row>` and
  :func:`dataframe <questdb.ingress.Sender.dataframe>` methods is now mandatory.
  Omitting it would previously use a server-generated timestamp for the row.
  Now if you want a server generated timestamp, you can pass the :ref:`ServerTimestamp <sender_server_timestamp>`
  singleton to this parameter. _The ``ServerTimestamp`` behaviour is considered legacy._

* The ``auth=(u, t, x, y)`` argument of the ``Sender`` constructor has now been
  broken up into multiple arguments: ``username``, ``token``, ``token_x``, ``token_y``.

* The ``tls`` argument of the ``Sender`` constructor has been removed and
  replaced with the ``protocol`` argument. Use ``Protocol.Tcps``
  (or ``Protocol.Https``) to enable TLS.
  The ``tls`` values have been moved to new ``tls_ca`` and ``tls_roots``
  :ref:`configuration settings <sender_conf_tls>`.

* The ``net_interface`` argument of the ``Sender`` constructor has been renamed
  to ``bind_interface`` and is now only available for TCP connections.

The following example shows how to migrate to the new API.

**Old questdb 1.x code**

.. code-block:: python

    from questdb.ingress import Sender

    auth = (
        'testUser1', 
        '5UjEMuA0Pj5pjK8a-fa24dyIf-Es5mYny3oE_Wmus48',
        'token_x=fLKYEaoEb9lrn3nkwLDA-M_xnuFOdSt9y0Z7_vWSHLU',
        'token_y=Dt5tbS1dEDMSYfym3fgMv0B99szno-dFc1rYF9t0aac')
    with Sender('localhost', 9009, auth=auth, tls=True) as sender:
        sender.row(
            'test_table',
            symbols={'sym': 'AAPL'},
            columns={'price': 100.0})  # `at=None` was defaulted for server time

**Equivalent questdb 2.x code**

.. code-block:: python

    from questdb.ingress import Sender, Protocol, ServerTimestamp

    sender = Sender(
        Protocol.Tcps,
        'localhost',
        9009,
        username='testUser1',
        token='5UjEMuA0Pj5pjK8a-fa24dyIf-Es5mYny3oE_Wmus48',
        token_x='token_x=fLKYEaoEb9lrn3nkwLDA-M_xnuFOdSt9y0Z7_vWSHLU',
        token_y='token_y=Dt5tbS1dEDMSYfym3fgMv0B99szno-dFc1rYF9t0aac',
        auto_flush_rows='off',
        auto_flush_interval='off',
        auto_flush_bytes=64512)
    with sender:
        sender.row(
            'test_table',
            symbols={'sym': 'AAPL'},
            columns={'price': 100.0},
            at=ServerTimestamp)  

**Equivalent questdb 2.x code with configuration string**

.. code-block:: python

    from questdb.ingress import Sender

    conf = (
        'tcp::addr=localhost:9009;' +
        'username=testUser1;' +
        'token=5UjEMuA0Pj5pjK8a-fa24dyIf-Es5mYny3oE_Wmus48;' +
        'token_x=token_x=fLKYEaoEb9lrn3nkwLDA-M_xnuFOdSt9y0Z7_vWSHLU;' +
        'token_y=token_y=Dt5tbS1dEDMSYfym3fgMv0B99szno-dFc1rYF9t0aac;' +
        'auto_flush_rows=off;' +
        'auto_flush_interval=off;' +
        'auto_flush_bytes=64512;')
    with Sender.from_conf(conf) as sender:
        sender.row(
            'test_table',
            symbols={'sym': 'AAPL'},
            columns={'price': 100.0},
            at=ServerTimestamp)


1.2.0 (2023-11-23)
------------------

This is a minor release bringing in minor new features and a few bug fixes,
without any breaking changes.

Most changes are inherited by internally upgrading to version ``3.1.0`` of
the ``c-questdb-client``.

Features
~~~~~~~~

* ``Sender(..., tls=True)`` now also uses the OS-provided certificate store.
  The `tls` argument can now also be set to ``tls='os_roots'`` (to *only* use
  the OS-provided certs) or ``tls='webpki_roots'`` (to *only* use the certs
  provided by the ``webpki-roots``, i.e. the old behaviour prior to this
  release). The new default behaviour for ``tls=True`` is equivalent to setting
  ``tls='webpki_and_os_roots'``.

* Upgraded dependencies to newer library versions. This also includes the latest
  `webpki-roots <https://github.com/rustls/webpki-roots>`_ crate providing
  updated TLS CA certificate roots.

* Various example code and documentation improvements.

Bug fixes
~~~~~~~~~

* Fixed a bug where timestamp columns could not accept values before Jan 1st
  1970 UTC.

* TCP connections now enable ``SO_KEEPALIVE``: This should ensure that
  connections don't drop after a period of inactivity.

1.1.0 (2023-01-04)
------------------

Features
~~~~~~~~

* High-performance ingestion of `Pandas <https://pandas.pydata.org/>`_
  dataframes into QuestDB via ILP.
  We now support most Pandas column types. The logic is implemented in native
  code and is orders of magnitude faster than iterating the dataframe
  in Python and calling the ``Buffer.row()`` or ``Sender.row()`` methods: The
  ``Buffer`` can be written from Pandas at hundreds of MiB/s per CPU core.
  The new ``dataframe()`` method continues working with the ``auto_flush``
  feature.
  See API documentation and examples for the new ``dataframe()`` method
  available on both the ``Sender`` and ``Buffer`` classes.

* New ``TimestampNanos.now()`` and ``TimestampMicros.now()`` methods.
  *These are the new recommended way of getting the current timestamp.*

* The Python GIL is now released during calls to ``Sender.flush()`` and when
  ``auto_flush`` is triggered. This should improve throughput when using the
  ``Sender`` from multiple threads.

Errata
~~~~~~

* In previous releases the documentation for the ``from_datetime()`` methods of
  the ``TimestampNanos`` and ``TimestampMicros`` types recommended calling
  ``datetime.datetime.utcnow()`` to get the current timestamp. This is incorrect
  as it will (confusinly) return object with the local timezone instead of UTC.
  This documentation has been corrected and now recommends calling
  ``datetime.datetime.now(tz=datetime.timezone.utc)`` or (more efficiently) the
  new ``TimestampNanos.now()`` and ``TimestampMicros.now()`` methods.

1.0.2 (2022-10-31)
------------------

Features
~~~~~~~~

* Support for Python 3.11.
* Updated to version 2.1.1 of the ``c-questdb-client`` library:

  * Setting ``SO_REUSEADDR`` on outbound socket. This is helpful to users with large number of connections who previously ran out of outbound network ports.


1.0.1 (2022-08-16)
------------------

Features
~~~~~~~~

* As a matter of convenience, the ``Buffer.row`` method can now take ``None`` column
  values. This has the same semantics as skipping the column altogether.
  Closes `#3 <https://github.com/questdb/py-questdb-client/issues/3>`_.

Bug fixes
~~~~~~~~~

* Fixed a major bug where Python ``int`` and ``float`` types were handled with
  32-bit instead of 64-bit precision. This caused certain ``int`` values to be
  rejected and other ``float`` values to be rounded incorrectly.
  Closes `#13 <https://github.com/questdb/py-questdb-client/issues/13>`_.
* Fixed a minor bug where an error auto-flush caused a second clean-up error.
  Closes `#4 <https://github.com/questdb/py-questdb-client/issues/4>`_.


1.0.0 (2022-07-15)
------------------

Features
~~~~~~~~

* First stable release.
* Insert data into QuestDB via ILP.
* Sender and Buffer APIs.
* Authentication and TLS support.
* Auto-flushing of buffers.


0.0.3 (2022-07-14)
------------------

Features
~~~~~~~~

* Initial set of features to connect to the database.
* ``Buffer`` and ``Sender`` classes.
* First release where ``pip install questdb`` should work.


0.0.1 (2022-07-08)
------------------

Features
~~~~~~~~

* First release on PyPI.
