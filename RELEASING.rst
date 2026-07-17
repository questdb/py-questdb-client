Cutting a new release
=====================

Overview
--------

Most release binaries are built by CI (``ci/cibuildwheel.yaml``):

* Linux x86_64 and aarch64: CPython 3.10 through 3.14 (including the
  free-threaded 3.14t), for both manylinux and musllinux.
* PyPy on Linux x86_64.
* Windows win32 and win_amd64: CPython 3.10 through 3.14 (the
  free-threaded 3.14t is skipped on Windows — pandas dependencies are
  missing there; see ``skip`` in ``pyproject.toml``).
* macOS x86_64: the Azure Pipelines hosted macOS agents run on Intel
  hardware, so CI produces the Intel wheels.

The macOS ARM (arm64) wheels are the one set CI cannot produce. We build
them manually inside a macOS VM, which is also where we bundle the source
distribution and perform the final upload to PyPI.

We cut from a VM to use as old of a MacOS version as possible to ensure
backwards compatibility with older MacOS versions.

Bumping Version and updating Changelog
--------------------------------------

Create a new PR with the new changes in ``CHANGELOG.rst``.

Make a commit and push the changes to a new branch.

You also want to bump the version. This process is semi-automated.

Ensure you have ``uv`` and ``bump-my-version`` installed:

* ``curl -LsSf https://astral.sh/uv/install.sh | sh`` : see
  https://docs.astral.sh/uv/getting-started/installation/
* ``uv tool install bump-my-version``: see
  https://github.com/callowayproject/bump-my-version.

::

    bump-my-version replace --new-version NEW_VERSION

If you're unsure, append ``--dry-run`` to preview changes.

Now merge the PR with the title "Bump version: V.V.V → W.W.W".

Note that CI will run all the ``cibuildwheel`` jobs which will in turn
generate the binaries for all the platforms, except for MacOS ARM.

Double-check the date in the CHANGELOG
--------------------------------------

Open ``CHANGELOG.rst`` and ensure that the version you are releasing no
longer says "(unreleased)" and that the date next to it matches today's
date. If the CHANGELOG was created earlier, it might have an older date.
If so, update it.

Preparing the MacOS VM
----------------------

Skip if you already have the MacOS VM set up in UTM.

.. warning::

    Releasing from an up to date MacOS install will not work as the binaries
    may be incompatible with older MacOS versions.

From a MacOS ARM computer install UTM.

* Download from https://mac.getutm.app/
* Install MacOS X 12.4 (Monterey). See https://docs.getutm.app/guest-support/macos/
* Install Rust from https://rustup.rs/
* Install Firefox
* Install a recent OFFICIAL Python release (3.10 or newer) to drive the
  build and run the smoke tests.

  * https://www.python.org/downloads/macos/
  * Do NOT use Homebrew to install Python.
  * ``cibuildwheel`` downloads and installs the official CPython
    release for each wheel it builds, so the remaining versions do not
    need to be pre-installed.

* Optionally install VS Code

Now clone the repository. The rest of the steps will assume this is done as so::

    cd ~
    mkdir -p questdb
    cd questdb
    git clone https://github.com/questdb/py-questdb-client.git
    cd py-questdb-client
    git submodule update --init --recursive

Updating the MacOS VM
---------------------

Do this before every release.

Inside the VM, open a terminal (or use the terminal Window in VSCode) and run the following commands::

    cd ~/questdb/py-questdb-client
    git checkout main
    git pull
    git submodule update --init --recursive

    rustup update stable

    python3 -m pip install -U pip
    python3 -m pip install -U \
        setuptools wheel twine Cython cibuildwheel \
        pandas numpy pyarrow polars

Smoke-testing the build
-----------------------

From ``~/questdb/py-questdb-client`` run the following commands::

    ./proj clean
    ./proj build
    ./proj test


Building the MacOS ARM binaries
-------------------------------

Clean and build the final binaries for each Python version::

    ./proj clean
    ./proj cibuildwheel

This runs ``cibuildwheel`` for the host architecture (arm64), building
and testing a wheel for each supported CPython version (3.10 through
3.14, including the free-threaded 3.14t). The new binaries land in the
``dist/`` directory.

Note that the wheels are built without the ``insecure-skip-verify``
native feature: released binaries must reject ``tls_verify=unsafe_off``.
Don't be tempted to set ``QUESTDB_INSECURE_SKIP_VERIFY=1`` here — it
exists for test harnesses only.

Prepare the source distribution
-------------------------------

The source code distribution is for any other platforms that we don't have
binaries for. I don't think it's _actually_ used by anyone, but it might get
used by IDEs.

.. code-block:: bash

    python3 setup.py sdist

Download the other binaries from CI
-----------------------------------

From the MacOS VM, from a terminal, run::

    cd ~/Downloads
    rm drop.zip
    rm -rf drop

Launch Firefox and log into GitHub and open the last (closed and merged) PR.

Click on the "Checks" tab and open up the last "questdb.py-questdb-client (1)"
check. There will be a link to the Azure DevOps page.

The following link might also work: https://dev.azure.com/questdb/questdb/_build?definitionId=21&_a=summary

If you open up the last run, you'll find a link called "1 published".
This will redirect you to the "Published artifacts" page.

There will be a "drop" directory.
* Don't open it.
* Instead use click on the three vertical dots on the right-hand
side and select download artifacts.

This will download a file called "drop.zip".

double-check it in Finder: It will extract to a directory called "drop".

Now from the terminal, run::

    cd ~/questdb/py-questdb-client
    cp -vr ~/Downloads/drop/* dist/

Sanity-check the contents of ``dist/`` before uploading. You should see:

* ``manylinux`` and ``musllinux`` wheels for both ``x86_64`` and
  ``aarch64``, CPython 3.10 through 3.14 plus ``cp314t``;
* PyPy (``pp3*``) wheels for Linux x86_64;
* ``win32`` and ``win_amd64`` wheels, CPython 3.10 through 3.14
  (no ``cp314t``);
* ``macosx`` wheels for both ``x86_64`` (from CI) and ``arm64``
  (built in this VM), CPython 3.10 through 3.14 plus ``cp314t``;
* exactly one ``.tar.gz`` source distribution.

Tagging the release
-------------------

In GitHub with a web browser create a new release with the tag "vX.Y.Z"
(where X.Y.Z is the new version number).

The release notes should be copied from the ``CHANGELOG.rst`` file,
but reformatted as Markdown.


Uploading to PyPI
-----------------

Now the MacOS VM has all the binaries and the source distribution, ready to be
uploaded to PyPI.

This is a good time to double-check you can log into PyPI and have set up an
API token. If you don't have one (or lost it), you can create a new one here:
https://pypi.org/manage/account/ (scroll down to "API tokens").

Once you've triple-checked everything is in ``dist/``, you can upload to PyPI.

.. code-block:: bash

    python3 -m twine upload dist/*

This will prompt you for your PyPI username and token.

Once the upload is complete, you can check the PyPI page to see if the new
release is there: https://pypi.org/project/questdb/


Updating the docs
-----------------

Log into ReadTheDocs and trigger a new build for the project.

https://readthedocs.org/dashboard/py-questdb-client/users/

Watch it to ensure there are no errors.

Once the build is complete, COMMAND-SHIFT-R to refresh the page (without cache)
and check the new version is there.
