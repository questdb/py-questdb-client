=================================
QuestDB Client Library for Python
=================================

This is the official Python client library for `QuestDB <https://questdb.io>`_.

This client library implements QuestDB's variant of the
`InfluxDB Line Protocol <https://questdb.io/docs/reference/api/ilp/overview/>`_
(ILP) over HTTP and TCP.

ILP provides the fastest way to insert data into QuestDB.

This implementation supports `authentication
<https://py-questdb-client.readthedocs.io/en/latest/conf.html#authentication>`_
and full-connection encryption with
`TLS <https://py-questdb-client.readthedocs.io/en/latest/conf.html#tls>`_.

Quickstart
==========

The latest version of the library is 2.0.0.

::

    python3 -m pip install -U questdb

Please start by `setting up QuestDB <https://questdb.io/docs/quick-start/>`_ . Once set up, you can use this library to insert data.

.. code-block:: python

    from questdb.ingress import Sender, TimestampNanos

    conf = f'http::addr=localhost:9000;'
    with Sender.from_conf(conf) as sender:
        sender.row(
            'sensors',
            symbols={'id': 'toronto1'},
            columns={'temperature': 20.0, 'humidity': 0.5},
            at=TimestampNanos.now())
        sender.flush()

You can also send Pandas dataframes:

.. code-block:: python

    import pandas as pd
    from questdb.ingress import Sender

    df = pd.DataFrame({
        'id': pd.Categorical(['toronto1', 'paris3']),
        'temperature': [20.0, 21.0],
        'humidity': [0.5, 0.6],
        'timestamp': pd.to_datetime(['2021-01-01', '2021-01-02'])})

    conf = f'http::addr=localhost:9000;'
    with Sender.from_conf(conf) as sender:
        sender.dataframe(df, table_name='sensors', at='timestamp')


To connect via TCP, set the
`configuration string <https://py-questdb-client.readthedocs.io/en/latest/conf.html>`_ to:

.. code-block:: python

    conf = f'tcp::addr=localhost:9009;'
    with Sender.from_conf(conf) as sender:
        ...


You can continue by reading the
`Sending Data Over ILP <https://py-questdb-client.readthedocs.io/en/latest/sender.html>`_
guide.

Links
=====

* `Core database documentation <https://questdb.io/docs/>`_

* `Python library documentation <https://py-questdb-client.readthedocs.io/>`_

* `GitHub repository <https://github.com/questdb/py-questdb-client>`_

* `Package on PyPI <https://pypi.org/project/questdb/>`_

Community
=========

If you need help, you can ask on `Stack Overflow
<https://stackoverflow.com/questions/ask?tags=questdb&tags=py-questdb-client>`_:
We monitor the ``#questdb`` and ``#py-questdb-client`` tags.

Alternatively, you may find us on `Slack <https://slack.questdb.io>`_.

You can also `sign up to our mailing list <https://questdb.io/community/>`_
to get notified of new releases.


License
=======

The code is released under the `Apache License 2.0
<https://github.com/questdb/py-questdb-client/blob/main/LICENSE.txt>`_.
