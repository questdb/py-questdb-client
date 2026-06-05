============
Installation
============

Dependency
==========

The Python QuestDB client does not have any additional run-time dependencies and
will run on any version of Python >= 3.9 on most platforms and architectures.

From version 3.0.0, this library depends on ``numpy>=1.21.0``.

Optional Dependencies
---------------------

DataFrame ingest requires ``pandas`` (bundled in the ``dataframe`` extra).

The following extras pull in optional libraries on demand:

* ``dataframe`` → ``pandas``
* ``pyarrow`` → ``pyarrow`` (only needed when you ingest
  ``pd.ArrowDtype`` / ``pd.Categorical`` / ``string`` dtype columns,
  ``pa.Table`` / ``pa.RecordBatch`` sources, an ``__arrow_c_array__``
  single-batch object, or pass ``schema_overrides=`` to a path that
  needs metadata patching from Python). It is also required for
  ``Client.query()`` egress.
* ``polars`` → ``polars`` (Polars frames go through the Arrow
  PyCapsule Interface end-to-end and **do not** need pyarrow,
  including with ``schema_overrides=``).

Without these extras, you may still ingest data row-by-row through
``Sender.row()`` and ``Buffer.row()``.

PIP
---

DataFrame ingest (pandas only)::

    python3 -m pip install -U questdb[dataframe]

DataFrame ingest with pyarrow features::

    python3 -m pip install -U questdb[dataframe,pyarrow]

Polars ingest::

    python3 -m pip install -U questdb[polars]

Row-only::

    python3 -m pip install -U questdb

Poetry
------

Equivalents for poetry::

    poetry add questdb[dataframe]
    poetry add questdb[dataframe,pyarrow]
    poetry add questdb[polars]
    poetry add questdb


Verifying the Installation
==========================

If you want to check that you've installed the wheel correctly, you can run the
following statements from a ``python3`` interactive shell:

.. code-block:: python

    >>> import questdb.ingress
    >>> buf = questdb.ingress.Buffer.ilp()
    >>> buf.row('test', symbols={'a': 'b'}, columns={'x': 1}, at=questdb.ingress.ServerTimestamp)
    <questdb.ingress.Buffer object at 0x104b68240>
    >>> bytes(buf)
    b'test,a=b x=1i\n'

If you also want to check you can serialize from Pandas
(which requires additional dependencies):

.. code-block:: python

    >>> import questdb.ingress
    >>> import pandas as pd
    >>> df = pd.DataFrame({'a': [1, 2]})
    >>> buf = questdb.ingress.Buffer.ilp()
    >>> buf.dataframe(df, table_name='test', at=questdb.ingress.ServerTimestamp)
    <questdb.ingress.Buffer object at 0x104b68240>
    >>> bytes(buf)
    b'test a=1i\ntest a=2i\n'
