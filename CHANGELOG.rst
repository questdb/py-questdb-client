
Changelog
=========

1.0.1 (2022-08-03)
------------------

* As a convenience, the ``Buffer.row`` method can now take ``None`` columnn
  values. This has the same semantics as skipping the column altogether.
  Closes (`#3 <https://github.com/questdb/py-questdb-client/issues/3>`_).
* Fixed a minor bug where an error auto-flush caused a second clean-up error.
  Closes (`#4 <https://github.com/questdb/py-questdb-client/issues/4>`_).


1.0.0 (2022-07-15)
------------------

* First stable release.
* Insert data into QuestDB via ILP.
* Sender and Buffer APIs.
* Authentication and TLS support.
* Auto-flushing of buffers.


0.0.3 (2022-07-14)
------------------

* Initial set of features to connect to the database.
* ``Buffer`` and ``Sender`` classes.
* First release where ``pip install questdb`` should work.


0.0.1 (2022-07-08)
------------------

* First release on PyPI.
