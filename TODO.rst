====
TODO
====


Build Tooling
=============
* **[HIGH]** Integrate tooling to build binaries for a matrix of operating
  systems, architectures and Python versions. This will also be our CI.
  This should help: https://github.com/pypa/cibuildwheel


Docs
====
* **[HIGH]** Author a few examples of how to use the client.
  This will help people get started. The examples should be presented in Sphinx
  using ``.. literalinclude::``.
  See: https://www.sphinx-doc.org/en/master/usage/restructuredtext/directives.html#directive-literalinclude
  The examples should be in the ``examples/`` directory in the repo.

* **[MEDIUM]** Document on a per-version basis.

* **[MEDIUM]** These examples should be tested as part of the unit tests (as they
  are in the C client). This is to ensure they don't "bit rot" as the code
  changes.


Development
===========
* **[HIGH]** Review API naming!

* **[HIGH]** Implement the auto-commit logic based on a watermark.

* **[MEDIUM]** Once we're done with them, merge in changes in the ``py_client_tweaks`` branch
  of the C client.

* **[LOW]** Implement ``tabular()`` API in the buffer.

* **[LOW]** Implement ``pandas()`` API in the buffer.
  *This can probably wait for a future release.*