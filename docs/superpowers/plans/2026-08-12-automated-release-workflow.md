# Automated Release Workflow Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add one manually dispatched GitHub Actions workflow that assembles the release-PR artifacts, validates them, pauses for approval, publishes to PyPI, creates the GitHub release, and verifies ReadTheDocs.

**Architecture:** `.github/workflows/release.yml` owns orchestration and keeps all release-specific shell/Python inline. A read-only `prepare` job validates the merged PR and packages and uploads one internal artifact; a protected `publish` job consumes exactly that artifact and performs external changes. `RELEASING.rst` becomes the short operator runbook.

**Tech Stack:** GitHub Actions, GitHub CLI/API, Azure DevOps REST API 7.1, inline Python 3, `build`, `packaging`, `twine`, PyPI Trusted Publishing, ReadTheDocs API v3.

## Global Constraints

- Keep Azure and GitHub wheel builders unchanged.
- Add no repository-owned release program or third-party release service.
- Start only through `workflow_dispatch` on `main`, with required `version` and `release_pr` inputs.
- Reuse wheel artifacts from the merged release PR; do not rebuild wheels.
- Require protected GitHub environment `pypi` before any publishing side effect.
- Use PyPI Trusted Publishing and a read-only Azure build-artifact token.
- Trigger and monitor the ReadTheDocs `latest` build.
- Make reruns safe without deleting or moving an existing tag.

---

### Task 1: Build and validate the release bundle

**Files:**
- Create: `.github/workflows/release.yml`

**Interfaces:**
- Consumes: dispatch inputs `version: string`, `release_pr: string`; secret `AZURE_DEVOPS_TOKEN`.
- Produces: job outputs `version: string`, `tag: string`, `release_sha: 40-character commit SHA`; Actions artifact `release-bundle` containing `dist/*` and `release-notes.md`.

- [ ] **Step 1: Write a failing structural check**

Run this before the workflow exists:

```bash
python - <<'PY'
from pathlib import Path
p = Path('.github/workflows/release.yml')
assert p.exists()
s = p.read_text()
for required in ('workflow_dispatch:', 'version:', 'release_pr:', 'prepare:',
                 'AZURE_DEVOPS_TOKEN', 'release-bundle'):
    assert required in s, required
PY
```

Expected: FAIL because `.github/workflows/release.yml` does not exist.

- [ ] **Step 2: Add the trigger and read-only prepare-job shell**

Create the workflow with:

```yaml
name: Release

on:
  workflow_dispatch:
    inputs:
      version:
        description: Version without the v prefix (for example, 5.0.1)
        required: true
        type: string
      release_pr:
        description: Merged version/changelog PR number
        required: true
        type: string

concurrency:
  group: release
  cancel-in-progress: false

permissions:
  contents: read
  actions: read
  pull-requests: read

jobs:
  prepare:
    runs-on: ubuntu-latest
    outputs:
      version: ${{ steps.release.outputs.version }}
      tag: ${{ steps.release.outputs.tag }}
      release_sha: ${{ steps.release.outputs.release_sha }}
```

The prepare steps must do the following, with `set -Eeuo pipefail` on every shell block:

1. Reject a dispatch whose `github.ref` is not `refs/heads/main`.
2. Validate `version` against `^[0-9]+\.[0-9]+\.[0-9]+([a-zA-Z0-9.-]+)?$` and `release_pr` against `^[0-9]+$`.
3. Use `gh api repos/$GITHUB_REPOSITORY/pulls/<PR>` to require `merged == true`, `base.ref == main`, and `head.repo.full_name == $GITHUB_REPOSITORY`; emit the merge commit SHA, version, and `v<version>` through `$GITHUB_OUTPUT`.
4. Check out that merge SHA using `actions/checkout@v4`, `fetch-depth: 0`, and recursive submodules, then require `git merge-base --is-ancestor HEAD origin/main`.
5. In inline Python, load `.bumpversion.toml` with `tomllib`, confirm `current_version`, evaluate each configured literal `search` after replacing `{current_version}`, and require it in the target file. Parse the first matching `CHANGELOG.rst` heading and require `<version> (<UTC YYYY-MM-DD>)`, rejecting `unreleased`.
6. Query `.github/workflows/macos-arm64-wheels.yml` runs with `gh api`, selecting exactly the newest successful `pull_request` run whose `pull_requests[].number` matches the input. Download only its non-expired `wheels-macos-arm64` artifact.
7. Query Azure build API 7.1 with definition `21`, branch `refs/pull/<PR>/merge`, reason `pullRequest`, completed/succeeded filters, newest-first ordering, and top 10. Select the newest build whose `sourceBranch` exactly matches, download artifact `drop` with `curl --fail --location --user ":$AZURE_DEVOPS_TOKEN"`, and unzip it under a temporary source directory.
8. Install `build`, `packaging`, and `twine`; run `python -m build --sdist --outdir dist` from the checked-out commit.
9. In inline Python, copy all discovered wheels into `dist` while rejecting duplicate basenames before copying. Parse wheel and sdist filenames with `packaging.utils`, require project `questdb` and the requested version, and compare CPython wheels against the matrix represented by the current CI files:
   - CPython 3.10–3.13 plus CPython 3.14 and 3.14t on manylinux and musllinux, each for x86_64 and aarch64;
   - the same CPython set on macOS x86_64 and arm64;
   - CPython 3.10–3.14, without free-threaded builds, on win32 and win_amd64;
   - one or more PyPy 3 wheels on Linux x86_64;
   - no other wheel targets and exactly one `.tar.gz`.
10. Run `python -m twine check dist/*`.
11. Extract the matching changelog section to `release-notes.md`. Convert its RST underline headings to Markdown headings, double-backtick literals to Markdown code spans, and Sphinx `:role:` links to readable labels without trying to implement a general RST converter.
12. Write source run URLs, release SHA, and a sorted package list to `$GITHUB_STEP_SUMMARY`.
13. Upload `dist/` and `release-notes.md` as `release-bundle` with `actions/upload-artifact@v4`, `retention-days: 7`, and `if-no-files-found: error`.

- [ ] **Step 3: Run static and semantic checks**

```bash
ruby -e "require 'yaml'; YAML.load_file('.github/workflows/release.yml')"
python - <<'PY'
from pathlib import Path
s = Path('.github/workflows/release.yml').read_text()
for required in (
    'workflow_dispatch:', 'github.ref', 'merge-base --is-ancestor',
    '.bumpversion.toml', 'wheels-macos-arm64', 'definitions=21',
    'python -m build --sdist', 'python -m twine check',
    'actions/upload-artifact@v4', 'retention-days: 7',
):
    assert required in s, required
assert 'twine upload' not in s
PY
```

Expected: both commands exit 0.

- [ ] **Step 4: Exercise the wheel validator with representative names**

Run the workflow's inline validator logic locally against a temporary fixture containing representative `questdb-5.0.1` filenames for every required target, then repeat with one required wheel removed, one wrong-version wheel, and a duplicate basename from two source directories.

Expected: the complete fixture passes; each mutated fixture fails with the relevant filename or missing target in its error.

- [ ] **Step 5: Commit the preparation job**

```bash
git add .github/workflows/release.yml
git commit -m "ci: prepare validated release bundles"
```

---

### Task 2: Add protected publishing and recovery behavior

**Files:**
- Modify: `.github/workflows/release.yml`

**Interfaces:**
- Consumes: Task 1 outputs and `release-bundle`; environment `pypi`; environment secret `READTHEDOCS_TOKEN`; GitHub OIDC.
- Produces: PyPI version, immutable `v<version>` tag, GitHub release with attached packages, and a successful ReadTheDocs `latest` build.

- [ ] **Step 1: Write a failing protected-publish structural check**

```bash
python - <<'PY'
from pathlib import Path
s = Path('.github/workflows/release.yml').read_text()
for required in ('publish:', 'environment: pypi', 'id-token: write',
                 'pypa/gh-action-pypi-publish@release/v1',
                 'READTHEDOCS_TOKEN', '/versions/latest/builds/'):
    assert required in s, required
PY
```

Expected: FAIL at `publish:`.

- [ ] **Step 2: Add the protected publish job**

Add a `publish` job that:

```yaml
  publish:
    needs: prepare
    runs-on: ubuntu-latest
    environment: pypi
    permissions:
      actions: read
      contents: write
      id-token: write
    steps:
      - uses: actions/checkout@v4
        with:
          ref: ${{ needs.prepare.outputs.release_sha }}
          fetch-depth: 0
      - uses: actions/download-artifact@v4
        with:
          name: release-bundle
      - name: Publish to PyPI
        uses: pypa/gh-action-pypi-publish@release/v1
        with:
          packages-dir: dist/
          skip-existing: true
```

Before publishing, fetch tags and require any existing `v<version>` tag to resolve to `release_sha`; fail rather than deleting or moving it. After PyPI succeeds:

1. Create and push a lightweight tag only when absent.
2. Use `gh release create` when the release is absent; otherwise use `gh release edit` and `gh release upload --clobber`. In both paths use `release-notes.md`, title `QuestDB Python client <version>`, and attach `dist/*`.
3. Trigger `POST https://app.readthedocs.org/api/v3/projects/py-questdb-client/versions/latest/builds/` with `Authorization: Token $READTHEDOCS_TOKEN`.
4. Parse the returned build ID and poll `GET /api/v3/projects/py-questdb-client/builds/<ID>/` every 15 seconds for at most 30 minutes. Succeed only when `state.code == finished` and `success == true`; fail immediately on `cancelled` or a finished unsuccessful build.
5. Always put the PyPI, GitHub release, and ReadTheDocs build URLs in `$GITHUB_STEP_SUMMARY`.

Keep the PyPI action before tag/release creation: if GitHub or ReadTheDocs fails afterward, `skip-existing`, the immutable-tag check, release update behavior, and safe repeated docs trigger allow a rerun.

- [ ] **Step 3: Run static safety checks**

```bash
ruby -e "require 'yaml'; YAML.load_file('.github/workflows/release.yml')"
python - <<'PY'
from pathlib import Path
s = Path('.github/workflows/release.yml').read_text()
for required in (
    'environment: pypi', 'contents: write', 'id-token: write',
    'pypa/gh-action-pypi-publish@release/v1', 'skip-existing: true',
    'gh release create', 'gh release edit', 'gh release upload',
    'READTHEDOCS_TOKEN', 'state.code', 'success',
):
    assert required in s, required
for forbidden in ('twine upload', 'git tag -f', 'git push --force'):
    assert forbidden not in s, forbidden
assert s.index('pypa/gh-action-pypi-publish@release/v1') < s.index('gh release create')
PY
```

Expected: both commands exit 0.

- [ ] **Step 4: Review permission and secret boundaries**

Confirm from the YAML that `prepare` has no `contents: write` or `id-token: write`, `publish` alone names `environment: pypi`, `AZURE_DEVOPS_TOKEN` appears only in prepare, and `READTHEDOCS_TOKEN` appears only in publish.

Expected: no publishing credential is available before approval.

- [ ] **Step 5: Commit protected publishing**

```bash
git add .github/workflows/release.yml
git commit -m "ci: publish approved releases"
```

---

### Task 3: Replace the manual operator runbook and verify the complete change

**Files:**
- Modify: `RELEASING.rst`

**Interfaces:**
- Consumes: final workflow input names, secret names, and job behavior from Tasks 1–2.
- Produces: the maintainer and administrator setup/runbook.

- [ ] **Step 1: Write a failing documentation check**

```bash
python - <<'PY'
from pathlib import Path
s = Path('RELEASING.rst').read_text()
for required in ('Run workflow', 'release_pr', 'Trusted Publishing',
                 'AZURE_DEVOPS_TOKEN', 'READTHEDOCS_TOKEN'):
    assert required in s, required
PY
```

Expected: FAIL because the current runbook documents manual artifact collection.

- [ ] **Step 2: Rewrite `RELEASING.rst` as the automated runbook**

Retain the existing platform/wheel overview, then replace workstation artifact collection, browser tagging, token-based Twine upload, and manual ReadTheDocs sections with:

1. **One-time administrator setup:** create protected `pypi` environment with required reviewers; register PyPI Trusted Publisher for repository `questdb/py-questdb-client`, workflow `release.yml`, environment `pypi`; add repository secret `AZURE_DEVOPS_TOKEN` with Azure build-read scope; add `READTHEDOCS_TOKEN` to the `pypi` environment.
2. **Prepare release PR:** update changelog/version using `bump-my-version`, make the changelog date the intended UTC release date, and merge only after Azure and macOS ARM wheel checks pass.
3. **Run release:** Actions → Release → Run workflow on `main`; enter version without `v` and merged PR number; inspect the prepare summary; approve the `pypi` job.
4. **Verify:** follow workflow links to PyPI, GitHub release, and ReadTheDocs.
5. **Recover:** rerun failed jobs with the same inputs; never move an existing tag; investigate any tag-to-commit mismatch.

- [ ] **Step 3: Run documentation and workflow checks**

```bash
python - <<'PY'
from pathlib import Path
s = Path('RELEASING.rst').read_text()
for required in ('Run workflow', 'release_pr', 'Trusted Publishing',
                 'AZURE_DEVOPS_TOKEN', 'READTHEDOCS_TOKEN',
                 'rerun', 'vX.Y.Z'):
    assert required in s, required
for obsolete in ('~/Downloads/drop', 'python3 -m twine upload dist/*',
                 'Log into ReadTheDocs and trigger'):
    assert obsolete not in s, obsolete
PY
ruby -e "require 'yaml'; YAML.load_file('.github/workflows/release.yml')"
git diff --check
```

Expected: all commands exit 0.

- [ ] **Step 4: Verify the final diff and known non-live limitation**

```bash
git diff --stat HEAD~2
git status --short
git log -3 --oneline
```

Confirm the implementation changes only `.github/workflows/release.yml` and `RELEASING.rst` beyond the already-approved spec/plan. Record that real Azure download, Trusted Publishing, GitHub release creation, and ReadTheDocs triggering require administrator configuration and therefore were not executed locally.

- [ ] **Step 5: Commit the runbook**

```bash
git add RELEASING.rst
git commit -m "docs: automate the release runbook"
```
