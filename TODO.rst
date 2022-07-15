====
TODO
====


Build Tooling
=============

* **[HIGH]** Transition to Azure, move Linux arm to ARM pipeline without QEMU.

* **[MEDIUM]** Automate Apple Silicon as part of CI.

* **[LOW]** Release to PyPI from CI.


Docs
====

* **[MEDIUM]** Examples should be tested as part of the unit tests (as they
  are in the C client). This is to ensure they don't "bit rot" as the code
  changes.

* **[MEDIUM]** Document on a per-version basis.

Development
===========

* **[HIGH]** Implement ``tabular()`` API in the buffer.

* **[MEDIUM]** Implement ``pandas()`` API in the buffer.
  *This can probably wait for a future release.*