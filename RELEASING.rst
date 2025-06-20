Cutting a new release
=====================

Overview
--------

We will collate the generated binaries from CI, together with a final
binary we need to build manually from for MacOS ARM within a MacOS VM.

We cut from a VM to use as old of a MacOS version as possible to ensure
backwards compatibility with older MacOS versions.

From the VM we will also bundle a source distribution and perform the final
upload to PyPI.

Bumping Version and updating Changelog
--------------------------------------

Create a new PR with the new changes in ``CHANGELOG.rst``.

Make a commit and push the changes to a new branch.

You also want to bump the version. This process is semi-automated.

* Ensure you have `uv` and `bump-my-version` installed:
  * `curl -LsSf https://astral.sh/uv/install.sh | sh` : see https://docs.astral.sh/uv/getting-started/installation/
  * `uv tool install bump-my-version`: see https://github.com/callowayproject/bump-my-version.

```console
bump-my-version replace --new-version NEW_VERSION
```

If you're unsure, append `--dry-run` to preview changes.

Now merge the PR with the title "Bump version: V.V.V → W.W.W".

Note that CI will run all the ``cibuildwheel`` jobs which will in turn 
generate the binaries for all the platforms, except for MacOS ARM.

Double-check the date in the CHANGELOG
--------------------------------------

Open ``CHANGELOG.rst`` and ensure that the date next to the version you are releasing matches today's date. 
If the CHANGELOG was created earlier, it might have an older date. If so, update it.

Preparing the MacOS VM
----------------------

Skip if you already have the MacOS VM set up in UTM.

.. warn::
    
    Releasing from an up to date MacOS install will not work as the binaries
    may be incompatible with older MacOS versions.

From a MacOS ARM computer install UTM.

* Download from https://mac.getutm.app/
* Install MacOS X 12.4 (Monterey). See https://docs.getutm.app/guest-support/macos/
* Install Rust from https://rustup.rs/
* Install Firefox
* Install *all* OFFICIAL Python Releases from Python 3.8 onwards. Use the latest patch version for each minor release. 
    * https://www.python.org/downloads/macos/
    * Do NOT use Homebrew to install Python.
    * Python 3.8.10 requires Rosetta, install it when prompted to do so. 

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
        setuptools wheel twine Cython cibuildwheel pandas numpy pyarrow

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

This should have created new binaries in the ``dist/`` directory.

Prepare the source distribution
-------------------------------

The source code distribution is for any other platforms that we don't have
binaries for. I don't think it's _actually_ used by anyone, but it might get
used by IDEs.

.. code-block:: bash

    python3 setup.py sdist

Download the other binaries from CI
-----------------------------------

From the MacOS VM, From a terminal, run::

    cd ~/Downloads
    rm drop.zip
    rm -rf drop

Launch Firefox and log into GitHub and open the last (closed and merged) PR.

Click on the "Checks" tab and open up the last "questdb.py-questdb-client (1)"
check. There will be a link to the Azure DevOps page.

The following link might also work: https://dev.azure.com/questdb/questdb/_build?definitionId=21&_a=summary

If you open up the last run, you'll find a link called "1 published".
This will redirect you to the "Publushed artifacts" page.

There will be a "drop" directory.
* Don't open it.
* Instead use click on the three vertical dots on the right-hand
side and select download artifacts.

This will download a file called "drop.zip".

double-check it in Finder: It will extract to a directory called "drop".

Now from the terminal, run::

    cd ~/questdb/py-questdb-client
    cp -vr ~/Downloads/drop/* dist/


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
