============
Installation
============

Dependency
==========

The Python QuestDB client does not have any additional run-time dependencies and
will run on any version of Python >= 3.8 on most platforms and architectures.

Optional Dependencies
---------------------

Ingesting dataframes also require the following
dependencies to be installed:

* ``pandas``
* ``pyarrow``
* ``numpy``

These are bundled as the ``dataframe`` extra.

Without this option, the ``questdb`` package has no dependencies other than
to the Python standard library.

PIP
---

You can install it (or update it) globally by running::

    python3 -m pip install -U questdb[dataframe]


Or, from within a virtual environment::

    pip install -U questdb[dataframe]


If you don't need to work with dataframes::
    
    python3 -m pip install -U questdb

Poetry
------

If you're using poetry, you can add ``questdb`` as a dependency::

    poetry add questdb[dataframe]

Similarly, if you don't need to work with dataframes::

    poetry add questdb

or to update the dependency::

    poetry update questdb


Verifying the Installation
==========================

If you want to check that you've installed the wheel correctly, you can run the
following statements from a ``python3`` interactive shell:

.. code-block:: python

    >>> import questdb.ingress
    >>> buf = questdb.ingress.Buffer()
    >>> buf.row('test', symbols={'a': 'b'})
    <questdb.ingress.Buffer object at 0x104b68240>
    >>> str(buf)
    'test,a=b\n'

If you also want to if check you can serialize from Pandas
(which requires additional dependencies):

.. code-block:: python

    >>> import questdb.ingress
    >>> import pandas as pd
    >>> df = pd.DataFrame({'a': [1, 2]})
    >>> buf = questdb.ingress.Buffer()
    >>> buf.dataframe(df, table_name='test')
    >>> str(buf)
    'test a=1i\ntest a=2i\n'
