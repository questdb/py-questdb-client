============
Installation
============

Dependency
==========

The Python QuestDB client runs on any version of Python >= 3.10 on most
platforms and architectures. Its only required run-time dependency is
``numpy>=1.21.0``.

Optional Dependencies
---------------------

Each extra installs the oldest versions that the client is tested with, or
newer:

* ``dataframe`` → ``pandas>=2.2.0`` and ``pyarrow>=14.0.1``. Install it to
  ingest a **pandas** DataFrame, or to use the ``to_pandas`` / ``to_arrow`` /
  ``iter_*`` helpers on ``QuestDB.query()`` results.
* ``polars`` → ``polars>=1.32.0`` and ``pyarrow>=14.0.1``. Install it to
  ingest polars frames and to use ``to_polars`` / ``iter_polars``.
* ``duckdb`` → ``duckdb>=1.2.0``. Install it to read query results with
  duckdb.

The extras are optional. polars, pyarrow, duckdb and any other Arrow-native
library go through the Arrow PyCapsule Interface, so you can also install the
library as usual. For example, ``polars.DataFrame(result)`` and polars ingest
need no pyarrow.

The client is tested with these minimum versions:

* pandas 2.2 and pyarrow 14.0.1. pyarrow tables, record batches and
  readers also need pyarrow 14.0.1 when you ingest them.
* polars 1.32, to ingest polars frames and to read query results with
  ``polars.DataFrame(result)``. For that read, polars 1.37.1 or later is
  recommended: older polars raises ``PanicException`` on a failover.
* duckdb 1.2, to read query results with ``duckdb.from_arrow(result)``.

To read query results that have ``DECIMAL`` columns with a precision of 18
or less through pyarrow (``to_arrow``, ``iter_arrow``, ``to_polars``,
``iter_polars``, ``pa.table(result)``, or ``to_pandas`` / ``iter_pandas``
with ``dtype_backend`` or ``types_mapper``), you need pyarrow 20 or later.
pyarrow before 19 cannot read the ``decimal32`` and ``decimal64`` Arrow
types that QuestDB sends for them, and pyarrow 19 cannot convert them to
pandas.

pyarrow 26 and later needs NumPy 2 but does not declare it. If you keep
NumPy 1.x, also install ``pyarrow<26``.

Without the extra, you may still ingest data row-by-row through
``Sender.row()``, and read query results through the
``__arrow_c_stream__`` PyCapsule protocol.

The :ref:`OIDC authentication helper <oidc_auth>` (:mod:`questdb.auth`) needs
no extra dependencies for ``sign_in()`` / ``token()``. Some conveniences
import the following lazily, only when used: ``sqlalchemy`` and ``psycopg`` /
``psycopg2`` (PG-wire adapters), ``qrcode`` (terminal QR prompt;
``qrcode[pil]`` or Pillow for the notebook PNG QR) and ``IPython``
(rich Jupyter prompt).

PIP
---

DataFrame ingest (pandas + pyarrow)::

    python3 -m pip install -U questdb[dataframe]

polars or duckdb::

    python3 -m pip install -U questdb[polars]
    python3 -m pip install -U questdb[duckdb]

Row-only::

    python3 -m pip install -U questdb

Poetry
------

Equivalents for poetry::

    poetry add questdb[dataframe]
    poetry add questdb[polars]
    poetry add questdb[duckdb]
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

``flush(wait=True)`` confirms the server accepted the row, but query
visibility follows asynchronously once the WAL is applied — if the count
reads ``0``, re-run the query.
