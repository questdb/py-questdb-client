# Automated release workflow design

## Goal

Replace the workstation-based release steps with one manually dispatched GitHub Actions workflow. A maintainer enters a version and merged release PR number, reviews the prepared artifacts, and approves one protected job to publish the release.

The version and changelog PR remains a human-reviewed process. Existing Azure and GitHub wheel builders remain unchanged.

## Scope

The change will add one self-contained `.github/workflows/release.yml` and update `RELEASING.rst`. It will not add a release service, repository-owned release program, or wheel-builder migration.

The workflow may use GitHub-maintained actions, GitHub's preinstalled `gh` CLI, standard runner shell/Python, packaging utilities, and PyPA's recommended PyPI publishing action. Release-specific validation stays inline in the workflow.

## Trigger and inputs

The workflow uses `workflow_dispatch` with two required inputs:

- `version`, without a `v` prefix, such as `5.0.1`
- `release_pr`, the merged version/changelog PR number

Only a dispatch from the default `main` branch is accepted. A release concurrency group prevents two release runs from publishing concurrently.

## Prepare job: no approval and no external changes

The first job performs all read-only checks and artifact preparation:

1. Fetch the PR through the GitHub API and require that it:
   - is merged;
   - targets `main`;
   - comes from this repository rather than a fork;
   - has a merge commit reachable from the current `origin/main`.
2. Check out the PR merge commit with submodules.
3. Validate the version input and require all files listed in `.bumpversion.toml` to contain that version.
4. Require the matching top-level `CHANGELOG.rst` section to have today's UTC date and not say `unreleased`.
5. Locate the successful `macOS arm64 wheels` pull-request run associated with the supplied PR and download `wheels-macos-arm64` with the GitHub API/CLI.
6. Query Azure DevOps pipeline definition 21 for the newest successful pull-request build of `refs/pull/<PR>/merge`, then download its `drop` artifact using a read-only Azure token.
7. Collect wheels without silently overwriting duplicate filenames.
8. Build exactly one source distribution from the checked-out release commit.
9. Validate that:
   - every distribution is for project `questdb` and the requested version;
   - each required CPython/platform combination in the current CI configuration is present exactly once;
   - Linux PyPy x86_64 wheels are present;
   - no unexpected wheel, duplicate filename, or extra source distribution is present;
   - `twine check` succeeds for every distribution.
10. Extract the requested changelog section and perform minimal RST-to-Markdown cleanup for the GitHub release body.
11. Write a GitHub job summary containing the release commit, source CI runs, file count, and sorted distribution list.
12. Upload the validated `dist/` directory and generated release notes as one short-retention GitHub Actions artifact for the publish job.

Any ambiguity—multiple candidate runs, missing artifacts, wrong versions, or a changed wheel matrix—fails before approval.

## Publish job: protected environment

A second job depends on the prepare job and uses the protected GitHub environment `pypi`. Required reviewers provide the final release approval. The job receives only the already-validated artifact; it does not rebuild packages.

After approval it:

1. Downloads the prepared artifact.
2. Rechecks tag state. `v<version>` may be absent or already point to the selected release commit; a tag pointing elsewhere fails.
3. Publishes `dist/*` through PyPI Trusted Publishing using PyPA's recommended action. Existing files are skipped to make recovery from a partial prior upload possible.
4. Creates the tag and GitHub release at the selected release commit, using the generated notes, and attaches the distributions. If the same release already exists at the correct commit, it is updated rather than duplicated.
5. Triggers a ReadTheDocs build for the `latest` version through API v3 and polls until it succeeds or reaches a bounded timeout. A docs failure is reported clearly but does not pretend that the already-published PyPI release was rolled back.
6. Writes direct links to PyPI, the GitHub release, and the ReadTheDocs build in the job summary.

## Authentication and repository setup

An administrator must configure:

- GitHub environment `pypi`, with required reviewers;
- a PyPI Trusted Publisher for this repository, workflow, and `pypi` environment;
- repository secret `AZURE_DEVOPS_TOKEN`, scoped read-only to Azure build artifacts;
- environment secret `READTHEDOCS_TOKEN`, authorized for `py-questdb-client`.

The workflow requests only the GitHub permissions needed for Actions artifact reads, release/tag writes, PR reads, and PyPI OIDC. Secrets are never passed to PR workflows.

## Failure and rerun behavior

Preparation is side-effect free and can be rerun freely. Nothing reaches PyPI before protected-environment approval.

Publishing spans multiple services and cannot be atomic. Steps are ordered and made idempotent so a maintainer can rerun after a partial failure:

- PyPI ignores filenames already uploaded for the version;
- the tag must point to the same release commit;
- the GitHub release is created or updated, never duplicated;
- triggering another ReadTheDocs build is safe.

The workflow never deletes or moves an existing tag automatically.

## Documentation and verification

`RELEASING.rst` will describe:

1. preparing and merging the version/changelog PR;
2. opening **Actions → Release → Run workflow**;
3. entering the version and PR number;
4. reviewing the prepare summary;
5. approving the `pypi` environment;
6. following the result links.

Implementation verification will include YAML parsing, shell syntax checks where extractable, checks of the inline validator against representative wheel filenames, and a review of the final workflow permissions and failure paths. Live publishing is intentionally not exercised during development.
