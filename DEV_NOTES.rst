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


Building, packaging and testing with ``cibuildwheel``
=====================================================

Builds, tests and packages across a number of operating systems, architectures
and configurations.

Testing locally
---------------

Install ``cibuildwheel``:

.. code-block:: bash

    python3 -m pip install cibuildwheel


Package locally:

.. code-block:: bash

    # Or `--platform` set to `macos` or `windows`
    cibuildwheel --platform linux --output-dir dist

The wheels will end up in the ``dist/`` directory when packaging locally.


Debugging with on Linux
=======================

GDB
---

Debugging with GDB is best done with the debug build of the Python interpreter.
This automatically loads the debug helper scripts for GDB.

On Ubuntu, you can install the debug build of Python with:

.. code-block:: bash

    sudo apt-get install python3-dbg

When in a GDB session, you can now also use additional commands like ``py-bt``.

Read more on the `Python GDB 
<https://devguide.python.org/advanced-tools/gdb/index.html>`_ documentation.


Valgrind
--------

We can set ``PYTHONMALLOC`` to disable python custom memory pools.

.. code-block:: bash

    export PYTHONMALLOC=malloc
    valgrind \
        --leak-check=full \
        --show-leak-kinds=all \
        --track-origins=yes \
        --verbose \
        python3 test/test.py -v


Debugging in side a ``cibuildwheel`` container
==============================================

In ``pyproject.toml``, add the following to the ``[tool.cibuildwheel]`` section:

.. code-block:: toml

    [tool.cibuildwheel]

    # .. other existing config

    # With GDB
    test-command = """
    echo set auto-load python-scripts on >> ~/.gdbinit
    echo add-auto-load-safe-path {project}/gdb >> ~/.gdbinit
    cat ~/.gdbinit

    ulimit -u unlimited
    export PYTHONMALLOC=malloc
    gdb -x {project}/commands.txt --batch --return-child-result --args \
        python {project}/test/test.py -v
    """

    # With Valgrind
    test-command = """
    export PYTHONMALLOC=malloc
    valgrind \
        --leak-check=full \
        --show-leak-kinds=all \
        --track-origins=yes \
        --verbose \
        python {project}/test/test.py -v
    """

    [tool.cibuildwheel.linux]
    before-all = """
    yum -y install gdb
    yum -y install valgrind
    """

Note the ``gdb/commands.txt`` file. Review it and change it to fit your needs.