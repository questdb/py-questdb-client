Cutting a new release
=====================

Overview
--------

All release binaries are built by CI, across two systems:

Azure Pipelines (``ci/cibuildwheel.yaml``):

* Linux x86_64 and aarch64: CPython 3.10 through 3.14 (including the
  free-threaded 3.14t), for both manylinux and musllinux.
* PyPy on Linux x86_64.
* Windows win32 and win_amd64: CPython 3.10 through 3.14 (the
  free-threaded 3.14t is skipped on Windows — pandas dependencies are
  missing there; see ``skip`` in ``pyproject.toml``).
* macOS x86_64: the Azure Pipelines hosted macOS agents run on Intel
  hardware, so this is where the Intel wheels come from.

GitHub Actions (``.github/workflows/macos-arm64-wheels.yml``):

* macOS arm64: built on the Apple Silicon ``macos-15`` runners, CPython
  3.10 through 3.14 plus 3.14t. Runs on every non-fork pull request,
  like the Azure jobs. ``MACOSX_DEPLOYMENT_TARGET=11.0`` keeps the
  ``macosx_11_0_arm64`` floor shipped since 4.x.

Every wheel is built without the ``insecure-skip-verify`` native
feature: released binaries must reject ``tls_verify=unsafe_off``. Never
set ``QUESTDB_INSECURE_SKIP_VERIFY=1`` for a release build — it exists
for test harnesses only.

The source distribution is built on your workstation, which is also
where you collate the CI artifacts and perform the final upload to PyPI.

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

Note that CI builds the release binaries on the bump PR itself — both
systems run as PR checks:

* Azure Pipelines runs the ``cibuildwheel`` jobs for Linux, Windows and
  macOS Intel.
* The GitHub Actions "macOS arm64 wheels" workflow builds the macOS ARM
  wheels. (It can also be triggered manually: Actions tab → "macOS arm64
  wheels" → "Run workflow", or ``gh workflow run "macOS arm64 wheels"``.)

The binaries you release are collected from the merged bump PR's runs.

Double-check the date in the CHANGELOG
--------------------------------------

Open ``CHANGELOG.rst`` and ensure that the version you are releasing no
longer says "(unreleased)" and that the date next to it matches today's
date. If the CHANGELOG was created earlier, it might have an older date.
If so, update it.

Prepare the source distribution
-------------------------------

The source code distribution is for any other platforms that we don't have
binaries for. I don't think it's _actually_ used by anyone, but it might get
used by IDEs.

From an up-to-date clone (with submodules initialized) on the release
commit::

    cd ~/questdb/py-questdb-client
    git checkout main
    git pull
    git submodule update --init --recursive
    python3 setup.py sdist

Download the macOS ARM binaries from GitHub Actions
---------------------------------------------------

Find the workflow run for the bump PR and download its artifact into
``dist/``::

    gh run list --workflow "macOS arm64 wheels" --limit 5
    gh run download <run-id> --name wheels-macos-arm64 --dir dist

(Or via the browser: the bump PR's Checks tab → "macOS arm64 wheels" —
or Actions tab → "macOS arm64 wheels" → the run for the bump PR — then
download the ``wheels-macos-arm64`` artifact.)

Download the other binaries from Azure CI
-----------------------------------------

From a terminal, run::

    cd ~/Downloads
    rm drop.zip
    rm -rf drop

Launch a browser, log into GitHub and open the last (closed and merged) PR.

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
* ``macosx`` wheels for both ``x86_64`` (from Azure) and ``arm64``
  (from GitHub Actions), CPython 3.10 through 3.14 plus ``cp314t``;
* exactly one ``.tar.gz`` source distribution.

Tagging the release
-------------------

In GitHub with a web browser create a new release with the tag "vX.Y.Z"
(where X.Y.Z is the new version number).

The release notes should be copied from the ``CHANGELOG.rst`` file,
but reformatted as Markdown.


Uploading to PyPI
-----------------

``dist/`` now holds all the binaries and the source distribution, ready
to be uploaded to PyPI.

This is a good time to double-check you can log into PyPI and have set up an
API token. If you don't have one (or lost it), you can create a new one here:
https://pypi.org/manage/account/ (scroll down to "API tokens").

Once you've triple-checked everything is in ``dist/``, you can upload to PyPI.

.. code-block:: bash

    python3 -m pip install -U twine
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
