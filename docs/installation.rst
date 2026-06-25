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
``to_pandas`` / ``to_arrow`` / ``iter_*`` helpers on ``Client.query()``
results. polars, pyarrow, duckdb and any other Arrow-native source need
no extra — they go through the Arrow PyCapsule Interface; just install
the source library as usual.

Without it, you may still ingest data row-by-row through
``Sender.row()`` and ``Buffer.row()``, and read query results through
the ``__arrow_c_stream__`` PyCapsule protocol.

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

If you want to check that you've installed the wheel correctly, you can run the
following statements from a ``python3`` interactive shell:

.. code-block:: python

    >>> import questdb
    >>> buf = questdb.Buffer.ilp()
    >>> buf.row('test', symbols={'a': 'b'}, columns={'x': 1}, at=questdb.ServerTimestamp)
    <questdb.Buffer object at 0x104b68240>
    >>> bytes(buf)
    b'test,a=b x=1i\n'

If you also want to check you can serialize from Pandas
(which requires additional dependencies):

.. code-block:: python

    >>> import questdb
    >>> import pandas as pd
    >>> df = pd.DataFrame({'a': [1, 2]})
    >>> buf = questdb.Buffer.ilp()
    >>> buf.dataframe(df, table_name='test', at=questdb.ServerTimestamp)
    <questdb.Buffer object at 0x104b68240>
    >>> bytes(buf)
    b'test a=1i\ntest a=2i\n'
