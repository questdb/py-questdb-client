============
Installation
============

Dependency
==========

The Python QuestDB client does not have any additional run-time dependencies and
will run on any version of Python >= 3.10 on most platforms and architectures.

From version 3.0.0, this library depends on ``numpy>=1.21.0``.

Optional Dependencies
---------------------

The ``dataframe`` extra bundles ``pandas`` and ``pyarrow``:

* ``dataframe`` → ``pandas`` and ``pyarrow``

Install it to ingest a **pandas** DataFrame, or to use the
``to_pandas`` / ``to_arrow`` / ``iter_*`` helpers on ``QuestDB.query()``
results. polars, pyarrow, duckdb and any other Arrow-native source need
no extra — they go through the Arrow PyCapsule Interface; just install
the source library as usual.

Without it, you may still ingest data row-by-row through
``Sender.row()``, and read query results through the
``__arrow_c_stream__`` PyCapsule protocol.

PIP
---

DataFrame ingest (pandas + pyarrow)::

    python3 -m pip install -U questdb[dataframe]

Row-only::

    python3 -m pip install -U questdb

Poetry
------

Equivalents for poetry::

    poetry add questdb[dataframe]
    poetry add questdb


Verifying the Installation
==========================

If you want to check that you've installed the wheel correctly, you can run
the following statements from a ``python3`` interactive shell:

.. code-block:: python

    >>> import questdb
    >>> questdb.__version__
    '5.0.0'
    >>> questdb.connect
    <function connect at 0x104b68240>

With a QuestDB server running locally, you can verify a full round trip
(ingestion and query) in a few lines:

.. code-block:: python

    >>> import questdb
    >>> db = questdb.connect('ws::addr=localhost:9000;')
    >>> with db.sender() as sender:
    ...     sender.row('install_check', columns={'x': 1},
    ...                at=questdb.ServerTimestamp)
    ...     sender.flush(wait=True)
    >>> db.query('SELECT count() FROM install_check').to_pandas()
       count
    0      1
    >>> db.close()
