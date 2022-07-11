====
TODO
====

Stuff to get done before 1.0.0


Build Tooling
=============
* **[HIGH]** Integrate tooling to build binaries for a matrix of operating systems,
  architectures and Python versions. This will also be our CI.
  This should help: https://github.com/pypa/cibuildwheel

* **[MEDIUM]** Integrate TOX https://tox.wiki/en/latest/ to trigger our testcases.
  This makes it easier to eventually run tests vs. different dependency
  versions.

* **[MEDIUM]** Figure out how ``bumpversion`` works, how it cuts tags, etc.
  Does ``bumpversion`` help us with ``CHANGELOG.rst``?

* **[LOW]** Consider converting `setup.py` to `built.py` and transitioning to `poetry`.
  Whilst we don't have other python dependencies for now (though we will have
  numpy and pandas eventually), it would standardise the way we build other
  python packages.
  *This can probably wait for a future release.*

Docs
====
* **[MEDIUM]** Document on a per-version basis.

* **[HIGH]** Author a few examples of how to use the client.
  This will help people get started. The examples should be presented in Sphinx
  using ``.. literalinclude::``.
  See: https://www.sphinx-doc.org/en/master/usage/restructuredtext/directives.html#directive-literalinclude
  The examples should be in the ``examples/`` directory in the repo.

* **[MEDIUM]** These examples should be tested as part of the unit tests (as they
  are in the C client). This is to ensure they don't "bit rot" as the code
  changes.


Development
===========
* **[HIGH]** Review API naming!

* **[HIGH]** Implement ``tabular()`` API in the buffer.

* **[HIGH]** Test the flush API carefully with exceptions before / after flushing.

* **[HIGH]** Implement the auto-commit logic based on a watermark.

* **[MEDIUM]** Once we're done with them, merge in changes in the ``py_client_tweaks`` branch
  of the C client.

* **[LOW]** Implement ``pandas()`` API in the buffer.
  *This can probably wait for a future release.*