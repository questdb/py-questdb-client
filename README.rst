=================================
QuestDB Client Library for Python
=================================

This library makes it easy to insert data into `QuestDB <https://questdb.io>`_.

This client library implements QuestDB's variant of the
`InfluxDB Line Protocol <https://questdb.io/docs/reference/api/ilp/overview/>`_
(ILP) over TCP.

ILP provides the fastest way to insert data into QuestDB.

This implementation supports `authentication
<https://questdb.io/docs/reference/api/ilp/authenticate/>`_ and full-connection
encryption with TLS.

Quickstart
==========

The latest version of the library is 1.0.2.

::

    python3 -m pip install questdb

.. code-block:: python

    from questdb.ingress import Sender

    with Sender('localhost', 9009) as sender:
        sender.row(
            'sensors',
            symbols={'id': 'toronto1'},
            columns={'temperature': 20.0, 'humidity': 0.5})
        sender.flush()


Docs
====

https://py-questdb-client.readthedocs.io/


Code
====

https://github.com/questdb/py-questdb-client


Package on PyPI
===============

https://pypi.org/project/questdb/


Community
=========

If you need help, have additional questions or want to provide feedback, you
may find us on `Slack <https://slack.questdb.io>`_.

You can also `sign up to our mailing list <https://questdb.io/community/>`_
to get notified of new releases.


License
=======

The code is released under the `Apache License 2.0
<https://github.com/questdb/py-questdb-client/blob/main/LICENSE.txt>`_.
