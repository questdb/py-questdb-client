
Changelog
=========

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

Bugfixes
~~~~~~~~

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
