=================
Development Notes
=================

Source code
===========

Clone from Github: https://github.com/questdb/py-questdb-client

Pre-requisites
==============

Compiling
---------

Install Rust as per https://rustup.rs/.

.. code-block:: bash

    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

Install Cython:

.. code-block:: bash

    python3 -m pip install cython

Testing
-------

.. code-block:: bash

    python3 -m pip install tox

Documentation
-------------

.. code-block:: bash

    python3 -m pip install sphinx
    python3 -m pip install sphinx_rtd_theme


Building and packaging
======================

For a crash-course in `setuptools` you may want to watch this `video
<https://www.youtube.com/watch?v=GIF3LaRqgXo&ab_channel=CodingTech>`_.

Sync submodule before building
------------------------------

.. code-block:: bash

    git submodule update --init --recursive

Building
--------

.. code-block:: bash

    python3 setup.py build_ext --inplace

Creating a wheel
----------------

.. code-block:: bash

    python3 setup.py bdist_wheel

Checking `pip install` works
----------------------------

.. code-block:: bash

    python3 -m pip install -e .


The `-e` flag links to the current directory rather than copying.

Creating a source distribution
------------------------------

.. code-block:: bash

    $ python3 setup.py sdist

Uploading to PyPI
-----------------

Build wheel and source:

.. code-block:: bash

    python3 setup.py bdist_wheel sdist


Install dependencies for uploading to PyPI:

.. code-block:: bash

    python3 -m pip install -e ".[publish]"

Upload to PyPI:

.. code-block:: bash

    python3 -m twine upload dist/*

