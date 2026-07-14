=================================
QuestDB Client Library for Python
=================================

This is the official Python client library for `QuestDB <https://questdb.com>`_.

This client library implements the QuestDB Wire Protocol (QWP) over WebSocket
for pooled ingestion and queries. It also supports QuestDB's variant of the
`InfluxDB Line Protocol <https://questdb.com/docs/reference/api/ilp/overview/>`_
(ILP) over HTTP and TCP, plus QWP over UDP.

This implementation supports `authentication
<https://py-questdb-client.readthedocs.io/en/latest/conf.html#authentication>`_
and full-connection encryption with
`TLS <https://py-questdb-client.readthedocs.io/en/latest/conf.html#tls>`_.

Install
=======

The latest version of the library is 5.0.0 (`changelog <https://py-questdb-client.readthedocs.io/en/latest/changelog.html>`_).

::

    python3 -m pip install -U questdb[dataframe]

Quickstart
==========

Start by `setting up QuestDB <https://questdb.com/docs/quick-start/>`_ .
Once set up, you can use this library to insert data.

The most common way to insert data is from a Pandas dataframe.

.. code-block:: python

    import numpy as np
    import pandas as pd
    from questdb import Client

    df = pd.DataFrame({
        'symbol': pd.Categorical(['ETH-USD', 'BTC-USD']),
        'side': pd.Categorical(['sell', 'sell']),
        'price': [2615.54, 39269.98],
        'amount': [0.00044, 0.001],

        # NumPy float64 arrays are supported from v3.0.0rc1 onwards.
        # Note that requires QuestDB server >= 9.0.0 for array support
        'ord_book_bids': [
            np.array([2615.54, 2618.63]),
            np.array([39269.98, 39270.00])
        ],

        'timestamp': pd.to_datetime(['2021-01-01', '2021-01-02'])})

    conf = 'ws::addr=localhost:9000;'
    with Client.from_conf(conf) as client:
        client.dataframe(df, table_name='trades', at='timestamp')

You can also send individual rows. This only requires a more minimal installation::

    python3 -m pip install -U questdb

.. code-block:: python

    import numpy as np
    from questdb import Client, TimestampNanos

    conf = 'ws::addr=localhost:9000;'
    with Client.from_conf(conf) as client:
        with client.sender() as sender:
            sender.row(
                'trades',
                symbols={'symbol': 'ETH-USD', 'side': 'sell'},
                columns={
                    'price': 2615.54,
                    'amount': 0.00044,

                    # NumPy float64 arrays require QuestDB server >= 9.0.0.
                    'ord_book_bids': np.array([2615.54, 2618.63]),
                },
                at=TimestampNanos.now())
            sender.flush(wait=True)


To connect via the `older TCP protocol <https://py-questdb-client.readthedocs.io/en/latest/sender.html#ilp-tcp-or-ilp-http>`_, set the
`configuration string <https://py-questdb-client.readthedocs.io/en/latest/conf.html>`_ to:

.. code-block:: python

    conf = f'tcp::addr=localhost:9009;'
    with Sender.from_conf(conf) as sender:
        ...

See the `5.0 migration guide
<https://py-questdb-client.readthedocs.io/en/latest/migration.html>`_ when
moving existing ``Sender.dataframe()`` or ``Buffer.dataframe()`` code to
``Client.dataframe()``. Those two methods are deprecated in 5.0 and planned
for removal in 6.0.0.


You can continue by reading the
`Sending Data Over ILP <https://py-questdb-client.readthedocs.io/en/latest/sender.html>`_
guide.

Links
=====

* `Core database documentation <https://questdb.com/docs/>`_

* `Python library documentation <https://py-questdb-client.readthedocs.io/>`_

* `GitHub repository <https://github.com/questdb/py-questdb-client>`_

* `Package on PyPI <https://pypi.org/project/questdb/>`_

Community
=========

Stop by our `Community Forum <https://community.questdb.com>`_ to 
chat with the QuestDB team.

You can also `sign up to our mailing list <https://questdb.com/contributors/>`_
to get notified of new releases.


License
=======

The code is released under the `Apache License 2.0
<https://github.com/questdb/py-questdb-client/blob/main/LICENSE.txt>`_. 
