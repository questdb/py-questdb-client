========
Examples
========

Basics
======

HTTP with Token Auth
--------------------

The following example connects to the database and sends two rows (lines).

The connection is made via HTTPS and uses token based authentication.

The data is sent at the end of the ``with`` block.

.. literalinclude:: ../examples/http.py
   :language: python


.. _auth_and_tls_example:

TCP Authentication and TLS
--------------------------

Continuing from the previous example, the connection is authenticated
and also uses TLS.

.. literalinclude:: ../examples/auth_and_tls.py
   :language: python


Explicit Buffers
----------------

For more :ref:`advanced use cases <sender_advanced>` where the same messages
need to be sent to multiple questdb instances or you want to decouple
serialization and sending (as may be in a multi-threaded application) construct
:class:`Buffer <questdb.ingress.Buffer>` objects explicitly, then pass them to
the :func:`Sender.flush <questdb.ingress.Sender.flush>` method.

Note that this bypasses :ref:`auto-flushing <sender_auto_flush>`.

.. literalinclude:: ../examples/buffer.py
   :language: python


Ticking Data and Auto-Flush
---------------------------

The following example somewhat mimics the behavior of a loop in an application.

It creates random ticking data at a random interval and uses non-default
auto-flush settings.

.. literalinclude:: ../examples/random_data.py
   :language: python


Data Frames
===========

Pandas Basics
-------------

The following example shows how to insert data from a Pandas DataFrame to the
``'trades_python'`` table.

.. literalinclude:: ../examples/pandas_basic.py
   :language: python

For details on all options, see the
:func:`Buffer.dataframe <questdb.ingress.Buffer.dataframe>` method.


``pd.Categorical`` and multiple tables
--------------------------------------

The next example shows some more advanced features inserting data from Pandas.

* The data is sent to multiple tables.

* It uses the ``pd.Categorical`` type to determine the table to insert and also
  uses it for the sensor name.

* Columns of type ``pd.Categorical`` are sent as ``SYMBOL`` types.

* The ``at`` parameter is specified using a column index: -1 is the last column.

.. literalinclude:: ../examples/pandas_advanced.py
   :language: python

After running this example, the rows will be split across the ``'humidity'``,
``'temp_c'`` and ``'voc_index'`` tables.

For details on all options, see the
:func:`Buffer.dataframe <questdb.ingress.Buffer.dataframe>` method.

Loading Pandas from a Parquet File
----------------------------------

The following example shows how to load a Pandas DataFrame from a Parquet file.

The example also relies on the dataframe's index name to determine the table
name.

.. literalinclude:: ../examples/pandas_parquet.py
   :language: python

For details on all options, see the
:func:`Buffer.dataframe <questdb.ingress.Buffer.dataframe>` method.
