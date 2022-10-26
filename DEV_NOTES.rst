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

Install your local Python3 environment **venv**

.. code-block:: bash

    python3 -m venv venv
    venv/bin/python install -U pip
    venv/bin/python install -r dev_requirements.txt

    # or simply:
    ./proj venv

    # either of the above should be followed by:
    source venv/bin/activate

The development requirements are these if you prefer to install them one by one:

- Install Cython:

.. code-block:: bash

    python3 -m pip install cython

- Documentation

.. code-block:: bash

    python3 -m pip install sphinx
    python3 -m pip install sphinx_rtd_theme

- Packaging and releasing

.. code-block:: bash

    python3 -m pip install wheel
    python3 -m pip install twine
    python3 -m pip install cibuildwheel
    python3 -m pip install bump2version


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

This will build in-place and is good enough for quick testing.

No wheels are made.

.. code-block:: bash

    ./proj build


Cleaning
--------

.. code-block:: bash

    ./proj clean


Packaging Locally
-----------------

For each of Linux 64-bit / Linux ARM 64 / Windows 64-bit / MacOS Intel /
MacOS Apple Silicon, run:

.. code-block:: bash

    ./proj sdist   # source distribution
    ./proj cibuildwheel # the order of these two lines does not matter

This will end up putting everything in the ``dist/`` directory.

As this is very time-consuming, instead download all targets
(except for Apple Silicon) from the CI.


Checking `pip install` works
----------------------------

You can generally skip this if you used ``cibuildwheel`` as the tool did this
already and ran tests against each built wheel.

.. code-block:: bash

    python3 -m pip install -e .

The `-e` flag links to the current directory rather than copying.

.. code-block:: bash

    $ python3  sdist


Uploading to PyPI
-----------------

Update the change log file that describes the latest changes in aggregate.

.. code-block:: bash

    vim CHANGELOG.rst


Create a tag, update versions etc using ``bump2version``.

For more command line options, see: https://pypi.org/project/bump2version/

Note that here ``patch`` would bump from (for example) ``0.1.0`` to
``0.1.1``. ``minor`` would bump from ``0.1.0`` to ``0.2.0`` and ``major``
would bump from ``0.1.0`` to ``1.0.0``.

.. code-block:: bash

    bump2version --config-file .bumpversion.cfg patch


Now make sure you push the tag or it will not show up in GitHub:

.. code-block:: bash

    git push --tags


Once you've triple-checked everything is in ``dist/``, you can upload to PyPI.

.. code-block:: bash

    python3 -m twine upload dist/*


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


Debugging inside a ``cibuildwheel`` container
=============================================

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
