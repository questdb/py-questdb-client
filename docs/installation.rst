============
Installation
============

The Python QuestDB client does not have any additional run-time dependencies and
will run on any version of Python >= 3.7 on most platforms and architectures.

You can install it globally by running::

    python3 -m pip install questdb


Or, from within a virtual environment::

    pip install questdb


If you're using poetry, you can add ``questdb`` as a dependency::

    poetry add questdb


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
