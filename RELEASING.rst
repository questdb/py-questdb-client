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

Every wheel is built with the ``insecure-skip-verify`` native feature,
matching the 2.x releases: ``tls_verify=unsafe_off`` works out of the
box, and TLS verification stays on unless the user explicitly disables
it in the conf string.

The release workflow collects the CI wheels, builds the source
distribution, validates the complete bundle, and publishes it.

One-time administrator setup
----------------------------

This setup is required before the first live release workflow run. It is
not required to review or merge the workflow code.

1. In the GitHub repository, create an environment named ``pypi`` and
   configure required reviewers.
2. On PyPI, add a Trusted Publisher for:

   * repository: ``questdb/py-questdb-client``
   * workflow: ``release.yml``
   * environment: ``pypi``

3. In the GitHub repository, add repository secret
   ``AZURE_DEVOPS_TOKEN`` with its Azure DevOps token scoped to Build Read.
4. In the GitHub ``pypi`` environment, add environment secret
   ``READTHEDOCS_TOKEN``.

Prepare the release PR
----------------------

1. Update ``CHANGELOG.rst`` with the release notes.
2. Set the version with ``bump-my-version``::

       bump-my-version replace --new-version X.Y.Z

   Add ``--dry-run`` to preview the changes.
3. Set the changelog date to the intended UTC release date. The workflow
   requires that date to match the UTC date on which it runs.
4. Open the version/changelog PR. Merge it only after both the Azure
   wheel checks and the **macOS arm64 wheels** check pass.
5. Keep the merged PR number for the ``release_pr`` workflow input.

Run the release
---------------

1. In GitHub, open **Actions** → **Release** → **Run workflow**.
2. Select branch ``main``.
3. Enter ``version`` without the ``v`` prefix, for example ``X.Y.Z``.
4. Enter the merged PR number in ``release_pr``, then select
   **Run workflow**.
5. Open the ``prepare`` job and inspect its **Validated release bundle**
   summary, including the release commit, source runs, package count,
   and package list.
6. At the protected ``pypi`` environment gate, approve the ``publish``
   job. This publishes to PyPI using Trusted Publishing, creates tag
   ``vX.Y.Z`` and the GitHub release, and triggers Read the Docs.

Verify
------

1. Open the ``publish`` job's **Release publication** summary.
2. Follow and check all three links:

   * **PyPI**: the new version and files are present.
   * **GitHub release**: the release notes and distribution assets are
     present under ``vX.Y.Z``.
   * **Read the Docs build**: the build finished successfully.

Recover
-------

1. Fix a transient dependency or configuration failure, then rerun the
   failed job with the same ``version`` and ``release_pr`` inputs. The
   publish steps recover completed work and do not replace matching
   artifacts.
2. Never move an existing release tag. If ``vX.Y.Z`` points to a commit
   other than the validated release commit, stop and investigate the
   tag-to-commit mismatch before retrying.
