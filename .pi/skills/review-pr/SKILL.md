---
name: review-pr
description: Review a GitHub pull request or local Git range against py-questdb-client (Cython + C-ABI) coding standards. Use when asked to review a PR, a PR number or URL, or a BASE..HEAD range, optionally at review level 0..3. Performs an adversarial, blocking, evidence-gated code review.
allowed-tools: bash read subagent
metadata:
  argument-hint: "[PR number or URL | --range=<base>..<head>] [--level=0..3]"
---

# Review a QuestDB Python client pull request

**Usage:** `/skill:review-pr [PR number or URL | --range=<base>..<head>] [--level=0..3]`

Review the PR or local range identified by the invocation arguments. When this skill
is run as `/skill:review-pr <args>`, the `<args>` are appended as a `User:` message;
treat that text as `$ARGUMENTS`. Parse exactly one review target: a PR number/URL,
or `--range=<base>..<head>`. The range head may be omitted (`--range=<base>..`) to
review the working tree, including uncommitted changes. If both targets are supplied,
stop and ask which was intended. If neither is supplied, ask for one.

> Harness note: this skill runs inside **pi**. Use `bash` for read-only `gh`, `git`,
> `rg`, `grep`, and `find` commands and for executing tests or probes that produce
> evidence, `read` for files, and fresh-context `reviewer` agents through
> `subagent(...)`. There is no Claude `Agent` tool and no separate `Grep`/`Glob` tool.
> Do not edit files, commit, or push. Any build or test run that needs a checkout
> other than the current one uses a scratch `git worktree`, never the primary
> working tree.

## What this repository is

`py-questdb-client` is the QuestDB Python client (`pip install questdb`). It is a
**Cython** extension that wraps the **`c-questdb-client`** Rust library through its
**C ABI**, plus a small Rust helper crate. A defect in any layer lands inside a
customer's Python process:

| Path | What lives here |
| ---- | --------------- |
| `src/questdb/_client.pyx` | The extension module `questdb._client`. Every Python-visible class (`QuestDB`, `Sender`, `PooledSender`, `PooledReader`, `Buffer`, `SenderTransaction`, timestamps, enums, errors). It textually `include`s `dataframe.pxi` (pandas / numpy / pyarrow / polars ingestion, Arrow C Data Interface, PyCapsule) and `egress.pxi` (QWP query egress: `QueryResult`, cursors, pooled readers, the Arrow stream export). Symbols are shared across all three files. |
| `src/questdb/*.pxd` | **Hand-written** Cython declarations of the C ABI. `line_sender.pxd` mirrors `questdb/ingress/line_sender.h`, `questdb/ingress/qwp_sender.h` and `questdb/egress/qwp_reader.h`; `rpyutils.pxd` mirrors `rpyutils/include/rpyutils.h`; `conf_str.pxd`, `arrow_c_data_interface.pxd`, `mpdecimal_compat.pxd` mirror local headers; `extra_cpython.pxd` / `extra_numpy.pxd` declare CPython and numpy C-API symbols Cython does not ship; `_client_helper.pxd` wraps the C snippets in `_client_helper.inc`. **Nothing checks a `.pxd` against its header** — drift is silent memory corruption, not a compile error. |
| `src/questdb/_client.pyi`, `__init__.py`, `ingress.py` | The type stub, the package re-exports and `__all__`, and the deprecated 4.x `questdb.ingress` compatibility shim. All three must agree with the implementation. |
| `c-questdb-client/` | Git submodule (default branch `main`): the Rust core and the `questdb-rs-ffi` C ABI crate. `setup.py` builds it with `cargo build --release --features confstr-ffi,arrow,insecure-skip-verify` and links the static library with `-flto`. Its headers under `include/questdb/` are the source of truth for every `.pxd`. Its `system_test/` directory is on `sys.path` during tests (`fixture.py`, the QuestDB fixture). |
| `rpyutils/` | Rust `staticlib`: `pystr_to_utf8.rs` (the `qdb_pystr_buf` UTF-8 arena and UCS1/2/4 converters), `active_senders.rs` (process-wide sender registry), `mpd_to_bigendian.rs` (decimal conversion). `rpyutils/include/rpyutils.h` is **cbindgen-generated** and must be regenerated whenever `rpyutils/src` changes; `rpyutils.pxd` must then match it. |
| `test/` | `test.py` is the aggregator every CI leg runs; it imports the mock-server suites (`mock_server.py` for ILP TCP/HTTP, `qwp_ws_ack_server.py` for QWP/WebSocket), `test_client_capsule_path.py`, `test_client_dataframe_failures.py`, `test_client_dataframe_fuzz.py`, `test_client_polars_fuzz.py`, `test_dataframe_leaks.py`, `test_dataframe.py` (when pandas is present) and, under `TEST_QUESTDB_INTEGRATION=1`, the live suites in `system_test.py` and `test_client_row_egress.py`. `test_dataframe_fuzz.py` is the atheris fuzzer (`proj.py test_fuzzing`). `benchmark*.py` are the throughput harnesses. |
| `docs/`, `examples/`, `examples.manifest.yaml`, `CHANGELOG.rst` | Sphinx docs built with `-nW` by `proj.py doc`; runnable examples whose paths `TestManifest` checks; the user-facing changelog. |
| `src/questdb/_client.c`, `*.html`, `*.so`, `build/` | **Generated.** Never review them. The source of truth is `*.pyx`, `*.pxi`, `*.pxd`, `*.pyi`. If a diff contains a regenerated `_client.c`, review the Cython change that produced it. |

**CI coverage is layered, and that matters for evidence.** `ci/run_tests_pipeline.yaml`
runs `python3 proj.py test 1` on every PR, which first runs `cargo test` in `rpyutils/`
and then `test/test.py` with `TEST_QUESTDB_INTEGRATION=1`: the mock-server suites **and**
the live suites against a **released** QuestDB that `system_test.py` downloads into
`build/questdb/`. The matrix is Linux (Python 3.12 + pandas 3, 3.12 + pandas 2.3.3,
3.10 + pandas 2), macOS and Windows. Only the `linux-qdb-master` leg builds QuestDB
`master` (`QDB_REPO_PATH=./questdb`) and re-runs the suite against it; features gated on
`QDB_REPO_PATH` or on a server capability the released build lacks run **only there**.
Two numpy-matrix jobs run just `TestBufferProtocolVersionV2`. The wheel builds
(`ci/cibuildwheel.yaml`, `.github/workflows/macos-arm64-wheels.yml`) run `test/test.py`
**without** the integration flag — mock-only — but on every shipped target: CPython
3.10–3.14, the free-threaded `cp313t`/`cp314t` builds, PyPy 3.10/3.11, manylinux and
musllinux. **Many live tests `skipTest` when the server lacks a capability**; a test that
exists is not coverage if the leg that would exercise the change skips it. Do not cite
"CI is green" as evidence that a capability-gated path was exercised, and do not cite a
CPython-only run as evidence that a C-API change builds on PyPy.

## Review mindset

You are a senior QuestDB engineer performing a blocking code review. This library is mission-critical software: bugs cause data loss, silent data corruption, segfaults or aborts that take down the host Python interpreter with no traceback, reference-count leaks, native memory leaks, GIL deadlocks, and hangs at interpreter shutdown — in customer applications. Be critical, thorough, and opinionated. Your job is to catch problems that would hurt a user before they ship — not to be nice, and not to demonstrate thoroughness by volume.

**A review that blocks on everything blocks on nothing.** Every finding costs the author a CI round-trip, and an inflated one costs the whole report its credibility. Reserve blocking severity for defects with a real user consequence, report everything else honestly at the severity it deserves, and approve when the gates pass. "Approve" is a normal, expected outcome of reviewing competent work — not a failure of rigour.

- **Assume nothing is correct until you've verified it.** Read surrounding code to understand context — don't just look at the diff in isolation.
- **The diff is a hint, not the boundary of the review.** The highest-value bugs almost always live at callsites outside the diff that depend on contracts the diff quietly changed: a `cdef` helper's `except` convention, who frees a `line_sender_error*`, the lifetime of a `qdb_pystr_buf` pointer, which lock a method expects to hold, whether a handle may be freed while a batch is still decoding. Treat the diff as the entry point, not the scope.
- **Discovery is not a finding.** Treat every concern — including one produced by several agents — as an untrusted hypothesis until it passes the Step 3b admission gate. Report every *admitted* issue at the severity its evidence earns; omit everything else. A review with zero findings is a successful outcome.
- **Falsify before you explain.** Search for the missing caller, unsupported configuration, absent producer, guard, retry, validation, and merge-base behavior before building a narrative. Failure to disprove a hypothesis is not evidence for it, and uncertainty is never promoted to severity.
- **Keep the blast radius of the PR small.** This PR should fix what it set out to fix, plus anything this change demonstrably breaks. Pre-existing bugs, residual hardening opportunities whose behavior is unchanged from base, and propositions that only support another candidate are never findings against this PR and never affect its verdict. The one exception is a pre-existing bug that this PR demonstrably moves onto a live path. Small blast radius governs what this PR must *fix*, not what the review is allowed to *know*: a pre-existing bug proved to the same evidence bar leaves as a Step 4 adjacent issue draft rather than being thrown away.
- **Do not praise the code.** Skip "looks good", "nice work", "clever approach". Focus entirely on problems and risks.
- **Think adversarially.** For each change, work through:
  - Inputs: which values break this? `None`, empty buffers, zero-length strings, max-length symbols, `INT64_MIN`/`INT64_MAX`, a Python `int` wider than 64 bits, NaN/Inf, `datetime` without tzinfo, `pandas.Timestamp` at the `NaT` sentinel, zero-row and zero-column DataFrames, zero-length and zero-dimension arrays, decimals at the precision/scale limits, UUIDs, 16- and 32-byte binary columns.
  - Encoding: a `str` with lone surrogates or astral code points, UCS1/UCS2/UCS4 kinds, `bytes` with embedded NULs, a `bytes`/`str` mix in one column, non-contiguous or negatively-strided numpy arrays, chunked or sliced Arrow arrays with a non-zero offset, dictionary-encoded and large-string Arrow types.
  - Memory: every `malloc`/`calloc`/`realloc` — freed on the success, early-return and exception paths? Every `Py_INCREF` — matched? Every `PyObject_GetBuffer` — a matching `PyBuffer_Release`? Every Arrow `release` callback — called exactly once?
  - GIL and threads: does a `with nogil` block or `noexcept nogil` function touch a Python object? Does a `noexcept with gil` callback arriving on a Rust thread check `qdb_py_is_finalizing()`? Can the GC run `__dealloc__` of a sibling object on *this* thread in the middle of a native decode, and does a re-entrant `RLock` then let it free a handle the outer frame still uses? Can a `KeyboardInterrupt` land inside the lock body?
  - Failure modes: connection dropped mid-flush, partial write, TLS handshake failure, auth rejection, server rejection, QWP role reject, server lacking a capability, query timeout, cursor cancelled mid-stream, pooled connection returned dirty — does the `Buffer`/`Sender`/`QueryResult` end in a usable state, and does native memory get released?
  - C-ABI callers: what happens when a C function returns NULL, returns an error through its out-param, hands back a pointer the Cython side must free exactly once, or is handed a pointer the `.pxd` declares with the wrong type?
- **Check what's missing**, not just what's there. Missing tests, missing error handling, missing edge cases, a public change with no `_client.pyi` / `__init__.py` / `docs/` / `CHANGELOG.rst` update, a `.pxd` declaration that no longer matches the pinned header, a `rpyutils.h` not regenerated after a Rust change.
- **Untested changed behavior is a coverage risk, not proof of a defect.** Missing tests alone cannot make a finding Critical. A Critical coverage gap must identify a supported, reachable user/operator population and a credible regression mode with material impact. A named test with a real failure link remains the strongest evidence; when none exists, assess change risk and the least fragile meaningful test rather than blocking by category. Test difficulty never reduces the severity of an actual functional, memory-safety, availability, corruption, or data-loss defect.
- **Urgency is neither evidence nor an exemption.** It may inform delivery sequencing only after user impact, regression risk, and stable-test feasibility are established. "Urgent", "simple", and "hard to test" are conclusions to prove, not reasons to skip analysis.
- **Treat public API ergonomics and cross-surface consistency as correctness, not polish.** This library has several entry points for the same concepts — `Sender`/`Buffer` row ingestion, `QuestDB.dataframe` / `Sender.dataframe`, the Arrow/PyCapsule path, `QueryResult.to_pandas` / `to_arrow` / `to_polars` / `iter_rows` and their `iter_*` siblings, keyword arguments versus configuration-string keys, `questdb.connect()` versus `Sender.from_conf()`. Every public symbol the PR adds or changes must (a) express each shared concept — table/column names, designated timestamp, batch/row limits, column-type overrides, timeouts, error reporting — the *same way* every sibling surface already does, and (b) make the easy path the safe path. An ergonomic inconsistency that can cause data loss or silent misuse is a blocking finding, not a Minor nit.
- **The pooled transports must be outage-tolerant.** Any change touching `PooledSender`, `PooledReader`, `QuestDB` connection state, `SenderErrorPolicy`, the rejection or connection-event handlers, failover reset, lease recycling, or the classification of a native error as terminal must preserve this invariant: a transient server failure, a role switch, a timeout, or a cancelled query must not permanently poison a healthy pooled connection, abandon acknowledged work, or report a failed batch as delivered. See the "Pooled transports, failover, and error classification" checklist — a violation there is Critical by definition.
- **Graceful role switch is not a protocol violation.** Enterprise primary↔replica switch happens without a process restart and with existing client connections still open. A write to an old primary after it becomes read-only is an expected server-side rejection under the surface's documented contract, not a reason to raise an unrecoverable error, drop the pool, or abort.
- **Demand appropriate algorithms where they matter.** On the per-row buffer build, the per-cell DataFrame loops, the flush path and the per-cell decode path, "works correctly" is not sufficient — a Python-object operation per cell, an attribute or `dict` lookup per row, a boxed `int` created by an untyped literal in typed arithmetic, an extra copy of a column, or an O(n²) pattern is a blocking defect. Off those paths (sender construction, configuration parsing, schema planning done once per DataFrame, pool setup) apply judgement: a bounded, non-scaling cost is Moderate, not blocking. Ask "is there a faster way?" for every loop and data-structure choice — then ask "does the user feel the difference?" before choosing the severity.
- **Verify every claim.** If the PR title says "fix", verify the bug actually existed and the fix is correct. If it says "improve performance", look for benchmark output or reason about the change against the hot path. If it says "simplify", verify the new code is actually simpler and doesn't drop behavior (a dropped `free` on an error branch, a dropped `PyBuffer_Release`). If it says "no behavior change", diff the generated Cython annotations in your head: a `cdef` that lost its `except` clause changed behavior. Treat the PR description as an unverified hypothesis, not a statement of fact.
- **Read the full context of changed files** when the diff alone is ambiguous. Use `read` and `bash` (`rg`/`grep`/`find`) to inspect surrounding code, callers, and related tests. Remember the `include` model: a symbol defined in `dataframe.pxi` or `egress.pxi` is visible everywhere in `_client.pyx`.
- **Assess reachability before reporting.** For every potential bug, trace the actual callers and inputs. If a problem requires physically impossible conditions (a buffer larger than `SIZE_MAX`, a NUL injected through an API that already rejects it, a panic behind a validation guard), it is not a real finding — drop it. Focus on bugs that real workloads can trigger, not theoretical edge cases that exist only in the type system.
- **Explicit `except -1` / `noexcept` annotations are contracts, not noise.** This codebase declares the exception convention on nearly every `cdef`. Do NOT flag an explicit `noexcept` as "missing error handling" when the body provably cannot raise — that is the intended hot-path contract. Flag it only when the body *can* raise, because under `noexcept` the exception is printed as unraisable and execution continues with a garbage return value: wrong data, not a traceback.

## Review level

Parse `$ARGUMENTS` for a level token: `--level=N`, `-lN`, or a bare single digit `0`-`3`. **If no level is given, default to 0.** Strip the level token and any `--range=` token before feeding the remainder (PR number or URL) to `gh` commands.

The level controls how much of the review below actually runs. Lower levels keep the same review *spirit* — adversarial, blocking, no praise — but cut the breadth of the analysis. Higher levels have significantly higher token cost; reserve level 3 for high-stakes PRs (a `.pxd` change or `c-questdb-client` submodule bump, `rpyutils` changes, the DataFrame/Arrow ingestion path, the egress decode path, `nogil`/`with gil` sections, manual `malloc`/refcount code, pooled connection state machines, lock or finalizer changes, ILP/QWP wire format, auth/TLS, public API shape).

| Level | What runs |
|-------|-----------|
| **0 (default)** | Steps 1, 2, 2.4, 2.5f, 2.6, 4. Skip the rest of Step 2.5 and all agent fanout. Review the diff inline for correctness, Cython memory/refcount/GIL safety, C-ABI binding agreement, crash surface, algorithmic fitness, tests, public API ergonomics, and coding standards. Build the Step 2.6 coverage map inline. Every candidate still passes the Step 3b admission gate inline from a blank evidence form; do not draft severity, a fix, or report prose first. |
| **1** | Adds Step 2.5a and Step 2.5e when test code is present. Run Agent 1 plus at most **two** applicable roles chosen from Agents 2, 3, 7, 8, and 11. Run an independent falsification task for each surviving atomic candidate. |
| **2** | Full Step 2.5, with 2.5b restricted to public Python symbols (in `__all__` / `_client.pyi`) plus every `cdef`/`cpdef` function and every C-ABI symbol declared in a `.pxd`. Run Agent 1 plus at most **four** change-relevant roles from Agents 2-9 and 11-14. Run an independent falsification task for each surviving atomic candidate. |
| **3** | Full Step 2.5 and the complete admission protocol. Select at most **six** applicable discovery roles from Agents 1-14: Agent 1 always; Agent 9 for changed symbols with out-of-diff callers; Agents 2-8 and 11 only when their domain is touched; Agents 12-14 only for changed tests or a fix claim; Agent 10 only when a distinct adversarial pass is warranted. Depth comes from producer/reachability evidence and independent falsification, not agent count. |

Once Step 1 resolves the target, begin the final review report with its immutable identity. In PR mode the first line must be exactly:

`Reviewing PR #<number> at level <N>, head <full headRefOid>, base <full baseRefOid>.`

Use the complete lowercase 40-character GitHub OIDs; never abbreviate them. In range mode, identify the resolved base and head commits, or label an uncommitted head as the working tree. This identity line is mandatory even when the review has no findings because `approve-pr` and `reject-pr` consume it. If the level was defaulted, mention separately that level 3 exists for full review.

## Spawning review agents in pi

Steps 3 and 3b use fresh-context `reviewer` agents through `subagent(...)`, one task
per role or atomic falsification candidate. Each task is self-contained and read-only —
the child does not inherit the parent conversation. Discovery tasks receive the diff,
Step 2.4 provenance verdicts, the Step 2.5 surface map, the Step 2.5f build-profile
facts, the Step 2.6 coverage map, role instructions, and the candidate contract.
Agents 10 and 11 are deliberate reduced-context exceptions. Step 3b falsifiers receive
only the neutral proposition, revision identities, relevant files, and raw artifact
paths. The parent owns role selection, the private ledger, admission, severity, and
output.

Launch a fanout as **one** `subagent` call with a `workflowScript` that uses
`runs.all([...])`; do not issue a separate top-level `subagent` call per agent. State
the build-profile facts from Step 2.5f (Cython directives, exception-convention
default, submodule commit, native panic mode) in the first line of every agent prompt
so the child reasons from the right premise, and add an explicit "review only — do not
edit any files" constraint to every task.

Use a shared temporary artifact for large maps rather than pasting them repeatedly.
Never pass the discovery narrative, proposed severity/fix, votes, or verification
claims to a falsifier. The parent owns synthesis, deduplication, and the final report;
children return candidates or falsification evidence only.

## Step 1: Gather PR context

Every mode must end this step with **`$BASE`** set — the commit the change is measured against. `$BASE` is required by Step 2.4 and by every behavioral finding's same-trigger base check; a review that never established it cannot attribute anything.

### GitHub PR

Capture the PR target after stripping the level token, normalize it to numeric `$PR`, and snapshot both immutable revision identities **before** gathering the diff. Re-read the pair after gathering context and stop if either changed:

```bash
TARGET='<PR number or URL from $ARGUMENTS, with any level token removed>'
if ! PR=$(gh pr view "$TARGET" --json number --jq .number); then
  echo "Could not resolve PR target: $TARGET"
  exit 1
fi
if ! IDENTITY=$(gh pr view "$PR" --json baseRefOid,headRefOid --jq '[.baseRefOid, .headRefOid] | @tsv'); then
  echo "Could not resolve base/head OIDs for PR #$PR."
  exit 1
fi
IFS=$'\t' read -r BASE HEAD <<< "$IDENTITY"
if [[ ! "$BASE" =~ ^[0-9a-f]{40}$ || ! "$HEAD" =~ ^[0-9a-f]{40}$ ]]; then
  echo "Could not resolve full base/head OIDs for PR #$PR."
  exit 1
fi

gh pr view "$PR" --json number,title,body,labels,state,baseRefOid,headRefOid
gh pr diff "$PR"
gh pr view "$PR" --comments

if ! CURRENT_IDENTITY=$(gh pr view "$PR" --json baseRefOid,headRefOid --jq '[.baseRefOid, .headRefOid] | @tsv') ||
   [ "$CURRENT_IDENTITY" != "$IDENTITY" ]; then
  echo "PR #$PR changed while its diff was being gathered. Restart the review."
  exit 1
fi
```

Treat `$IDENTITY` as the review snapshot token. Revalidate it again immediately before Step 4; a review that ran while the PR moved must not emit a verdict.

If the diff touches the `c-questdb-client` submodule pointer, any `.pxd`, `rpyutils/`, or `setup.py`, note it now — these change the ABI or safety story for the whole extension, and Step 2.5f must re-verify its facts against the new state regardless of the requested level.

### Local range (`--range`)

When `--range=<base>..<head>` is given there is no PR, no description, and no
labels. Take the diff from Git instead:

```bash
BASE='<base from --range>'
HEAD='<head from --range, or empty for the working tree>'
git diff "$BASE"${HEAD:+"...$HEAD"} --stat
git diff "$BASE"${HEAD:+"...$HEAD"}
git diff "$BASE"${HEAD:+"...$HEAD"} --name-only
```

With `<head>` empty the diff includes uncommitted working-tree changes, which is
the normal case when reviewing a local fix before it is pushed. Untracked files do
not appear in `git diff` — list them with `git status --porcelain` and read any that
are part of the change, especially new test files, or the coverage map in Step 2.6
will silently miss them.

In range mode: **skip Step 2 entirely** (there is no title or description to
check) and say so in the report. Every other step runs unchanged — the diff is
still the entry point, callsite analysis still walks outward beyond the changed
files, and findings are still classified by the same rubric. Restricting the
review to the changed files would disable the out-of-diff breakage analysis that
is the most valuable part of this skill.

## Step 2: PR title and description

**Skipped in `--range` mode** — a local range has no PR metadata. State that it was skipped and continue at Step 2.4.

Check:
- Title follows the repository's prevailing Conventional Commits style: `type: description` or `type(scope): description` (`fix:`, `feat:`, `chore:`, `ci:`, `docs:`, `build(arrow):`), matching the prefixes already in use in `git log`
- Description speaks to end-user impact, not implementation internals, and explains the *why*
- If fixing an issue, `Fixes #NNN` or a link to the issue is present
- Tone is level-headed and analytical, with no superlatives or bold emphasis on numbers
- For public API changes (anything in `__all__` or `_client.pyi`, a new or changed method or keyword argument on `QuestDB` / `Sender` / `PooledSender` / `Buffer` / `QueryResult`, a changed default, a changed exception type, a changed configuration-string key), the description calls out the API change explicitly, and `CHANGELOG.rst` carries a matching entry under the unreleased version
- For a `c-questdb-client` submodule bump, the description states which upstream commit or PR is being pulled in and why; a bump to a commit that is not on the submodule's `main` is a `tandem` change and should carry that label
- For a change that touches a capability-gated or server-version-dependent path, the description says which CI leg (released server versus `linux-qdb-master`) actually exercises it

## Step 2.4: Submodule provenance (mandatory at every level)

A changed submodule pointer is not automatically a change this PR makes. Before reviewing **any** content inside the submodule, classify the pointer move. This step is cheap, runs at every level including 0, and gates whether an entire repository's worth of Rust diff is in scope. Skipping it is how a review attributes months of already-released upstream work to the PR in front of it.

This repository pins `questdb/c-questdb-client` at `c-questdb-client/` (default branch `main`). If the pointer moved, resolve the default branch and test whether the new commit is already on it:

```bash
git diff "$BASE...$HEAD" --submodule=short | grep -E '^(diff --git|[+-]Subproject commit)'

cd c-questdb-client
git fetch origin --quiet
DEF=$(git symbolic-ref --short refs/remotes/origin/HEAD 2>/dev/null | sed 's|^origin/||')   # main
git merge-base --is-ancestor <new-sha> "origin/$DEF" && echo UPSTREAM-SYNC || echo OFF-DEFAULT
git branch -r --contains <new-sha>
git log --oneline <old-sha>..<new-sha>
```

Classify the pointer move as exactly one of:

- **UPSTREAM-SYNC** — the new commit is an ancestor of `origin/main`. The Rust work inside it **already landed upstream**; this PR only advances the pointer to pick it up. **Its contents are not in-diff and are not this PR's responsibility.** Do not review them as changes, do not attribute their behaviour changes to this PR, and do not build findings out of them. The legitimate findings here are *integration* defects: a `.pxd` declaration that no longer matches the newly pinned header (type, `const`-ness, struct layout, enum order, a new or removed export), a Cython callsite that now disagrees with the native contract (ownership, error code, a new required call), or a behavioral change upstream that the Python layer, its docs or its tests still contradict. Those findings live in this repository's files, not inside the submodule.
- **OFF-DEFAULT** — the new commit exists only on a feature or PR branch. This is the `tandem` pattern: the Rust change is reviewed in `c-questdb-client` by **its own** `review-pr` skill, not here. In this review, treat the pinned headers as the contract and review the Python side's integration against them exactly as for UPSTREAM-SYNC, read the Rust only as far as needed to understand the contract the Python code relies on, and state that the pin must move to a `main` commit before release — that is merge mechanics (see Step 4), not a finding.
- **UNRESOLVED** — the branch cannot be determined (no network, shallow clone, submodule not checked out). Say so explicitly in the report, treat it as OFF-DEFAULT for safety, and state that the scope decision was made without provenance.

Record the verdict in one line and repeat it in the Step 4 report so the scope decision is auditable. Whatever the verdict, a moved pointer forces Step 2.5f to re-read `c-questdb-client/questdb-rs-ffi/Cargo.toml` and re-verify every `.pxd` the change touches against the **new** headers.

## Step 2.5: Map the change surface

Before launching review agents, produce a structured change surface map. This step is mandatory and must use `bash` (`rg`/`grep`/`find`) plus `read` — do not reason about callsites from memory. The output of this step is required input for every agent in Step 3.

### 2.5a Semantic delta per changed symbol

For every modified or added function (`def`, `cdef`, `cpdef`), method, class, `cdef class` attribute, module-level constant, enum member, `.pxd` declaration, or `rpyutils` export, write:

- **Symbol:** fully-qualified name (e.g., `questdb._client.Buffer.column`, `_dataframe_columnar_sync`, `_CursorHandle._free`, `c_err_to_py`, `line_sender_buffer_column_f64`, `qdb_pystr_buf_clear`)
- **Before:** signature, return type, **Cython exception convention** (`except -1` / `except? -1` / `except *` / `except +` / `noexcept` / implicit), what it raises and on which inputs, `nogil` / `with gil`-ness, whether it touches Python objects, allocation behavior (`malloc`/`calloc`/`realloc`, Python allocations), refcount effect (steals / borrows / owns), buffer-protocol or Arrow ownership, C-ABI ownership (who frees returned pointers, whether a failed call consumes the handle), which lock it expects to hold and whether it may re-enter, thread it runs on (caller, GC finalizer, Rust callback thread, pyarrow/polars consumer)
- **After:** same fields
- **Delta:** one line stating what semantically changed

"Refactored", "cleaned up", "improved", "simplified" are not acceptable deltas. State the actual behavioral difference. If nothing semantically changed, write "no behavioral change" — but only after checking, not as a default.

### 2.5b Callsite inventory

For every changed symbol that is public (in `__all__` / `_client.pyi`), `cdef`/`cpdef`, declared in a `.pxd`, exported from `rpyutils`, or referenced from the stub, shim, docs or tests, run a repository-wide search (`rg` via `bash`) to find every callsite, implementation, override, or reference outside the diff.

Produce a list grouped by file. Search at minimum:

- **Cython implementation and includes:** `rg -n 'symbol_name' src/questdb/*.pyx src/questdb/*.pxi`
- **C-ABI and helper declarations:** `rg -n 'symbol_name' src/questdb/*.pxd src/questdb/*.h src/questdb/_client_helper.inc`
- **Pinned C headers (source of truth):** `rg -n 'symbol_name' c-questdb-client/include/questdb/ rpyutils/include/`
- **Rust side of the helper crate:** `rg -n 'symbol_name' rpyutils/src/`
- **Type stub, package, shim:** `rg -n 'symbol_name' src/questdb/_client.pyi src/questdb/__init__.py src/questdb/ingress.py`
- **Mock-server and unit tests:** `rg -n 'symbol_name' test/test.py test/mock_server.py test/qwp_ws_ack_server.py test/test_tools.py`
- **Live suites:** `rg -n 'symbol_name' test/system_test.py test/test_client_row_egress.py`
- **DataFrame, capsule, fuzz and leak tests:** `rg -n 'symbol_name' test/test_dataframe.py test/test_client_capsule_path.py test/test_client_dataframe_failures.py test/test_client_dataframe_fuzz.py test/test_client_polars_fuzz.py test/test_dataframe_fuzz.py test/test_dataframe_leaks.py`
- **Benchmarks:** `rg -n 'symbol_name' test/benchmark*.py perf/`
- **Examples and docs:** `rg -n 'symbol_name' examples/ docs/ README.rst CHANGELOG.rst`

A changed public / `cdef` / `.pxd` symbol with zero recorded search commands in the trace is a skill violation. The model is not allowed to assert "this is only used here" without showing the search.

### 2.5c Implicit contract list

For each changed symbol, walk this checklist and write one line per item, stating before vs after:

- **Cython exception convention:** which `except` clause, whether the body can raise, and whether every caller still observes the error. A `noexcept` function that raises prints an unraisable exception and returns garbage; a `cdef extern` C function is `noexcept` by definition
- **Raises which exceptions on which inputs** (`QuestDBError` with which `QuestDBErrorCode`, `QuestDBServerRejectionError`, `UnsupportedDataFrameShapeError`, `ValueError`, `TypeError`, `KeyboardInterrupt`) and which callers catch versus propagate them
- **Native memory:** what it allocates and who frees it, on every path including the exception path
- **Reference counting:** `Py_INCREF`/`Py_DECREF`, borrowed `PyObject*` stored past the owner's lifetime, weakrefs (`PyWeakref_GetRef`), PyCapsule ownership, returned borrowed versus owned references
- **Buffer protocol:** `PyObject_GetBuffer` / `PyBuffer_Release` pairing; exporter kept alive while the raw pointer is in use; `Py_buffer_obj_is_set` checks on partially-acquired buffers
- **GIL:** runs under `nogil`? releases the GIL around a blocking native call? is a `noexcept with gil` callback entered from a Rust thread or from a pyarrow/polars consumer? checks `qdb_py_is_finalizing()` before touching the interpreter?
- **Locks and re-entrancy:** which `RLock`/`Condition` it expects to hold, whether it may be re-entered on the same thread by a GC finalizer (`__dealloc__`, a `uuid.UUID`-style user object's `__del__`, a weakref callback) or a signal handler while the outer frame still holds native pointers, and whether a pinned/in-use handle may be freed underneath a running decode
- **C-ABI ownership:** which `line_sender_*`, `qwp_*`, `questdb_*` handles cross the boundary and who owns them afterward; every `line_sender_error*` / `questdb_error*` / `questdb_conf_str_parse_err*` converted and freed exactly once (`c_err_to_py`, `_reader_err_to_py`, `c_parse_conf_err_to_py`)
- **`qdb_pystr_buf` arena lifetime:** are UTF-8 pointers obtained from the arena still valid after a subsequent `clear`/`truncate`/append that may reallocate?
- **Object state on error:** does a failed call leave the `Buffer` half-written, the `Sender` needing reconstruction, the `QueryResult` closed, the pooled connection recyclable or terminal, `_must_close`/terminal flags armed when nothing reached the wire?
- **`.pxd` ↔ C header agreement:** parameter types, `const`-ness, struct layout, enum discriminant order, return type, `nogil` on the declaration — against the **pinned** submodule commit and the regenerated `rpyutils.h`
- **`.pyi` / `__init__.py` / `ingress.py` ↔ implementation agreement:** signature, defaults, return type, `__all__`, deprecation shim parity
- **Wire format and units:** any change to the ILP bytes produced (protocol v1 / v2 / v3), QWP frames, timestamp units (micros vs nanos vs days), UUID byte order, decimal encoding, array dimensionality, symbol dictionaries
- **Durability:** what a flush return, an ack, a rejection event, a transaction commit, or `close()` means for rows already handed to the sender

### 2.5d Cross-context exposure list

End this step with an explicit list of "places this change is visible from but the diff does not touch". This is the highest-priority input for the bug-hunting agents in Step 3.

Group the callsites from 2.5b by execution context. Typical contexts in this codebase:

- **C-ABI binding surface:** every function declared in `line_sender.pxd`, `rpyutils.pxd`, `conf_str.pxd`, `arrow_c_data_interface.pxd`, `mpdecimal_compat.pxd` that the changed code calls, transitively
- **Row buffer build hot path:** `Buffer.row` / `symbol` / `column` / `at*`, `Sender.row`, `SenderTransaction`, and their `cdef` helpers and the `qdb_pystr_buf` arena
- **DataFrame / Arrow ingestion path:** `dataframe.pxi` — pandas / numpy / pyarrow / polars planners, column type resolution and overrides, the Arrow C Data Interface (`ArrowArray` / `ArrowSchema` / `ArrowArrayStream`) consumption and `release` callbacks, PyCapsule handling, `qwp_direct_sender` and `_dataframe_columnar_sync`
- **Egress / query path:** `egress.pxi` — `QuestDB.query` / `execute`, `QueryResult.iter_rows` / `to_pandas` / `to_arrow` / `to_polars` and the `iter_*` batch iterators, `_ReaderHandle` / `_CursorHandle` pin-and-free, `PooledReader` lease and recycle, `_QueryStreamProducer` and the `_qs_*` Arrow stream callbacks, cancellation, timeouts, `ExecDone`
- **Flush and transport path:** `Sender.flush`, auto-flush triggers (row count / byte threshold / interval), ILP HTTP/TCP, QWP WebSocket and UDP, the `with nogil` regions, transactions
- **Native-thread callbacks:** connection-event and rejection callbacks (`noexcept with gil`), the failover-reset callback, anything reachable while `qdb_py_is_finalizing()` may be true
- **Pooling, locking, and lifecycle:** `QuestDB._state_cond`, `PooledSender._lock`, `PooledReader._lock`, `_DISPATCH_THREAD`, the `active_senders` registry, every `__dealloc__`, every class marked `@cython.no_gc_clear`
- **Configuration parsing:** `questdb.connect()` / `QuestDB(...)` keyword arguments, `Sender.from_conf` / `from_env`, the `conf_str` parser, `QDB_CLIENT_CONF`
- **Authentication / TLS:** token, basic-auth and `TlsCa` configuration paths, `tls_verify`
- **Public surface files:** `_client.pyi`, `__init__.py` `__all__`, `ingress.py`
- **Tests:** `test/test.py`, `system_test.py`, `test_client_row_egress.py`, `test_dataframe.py`, capsule, fuzz and leak tests
- **Examples and docs:** `examples/*.py`, `examples.manifest.yaml`, `docs/*.rst`, `README.rst`

Every entry on this list must be reviewed in Step 3.

### 2.5e Test surface & helper inventory

Run this only when the PR adds or changes test code. It is the test-code counterpart to 2.5b and feeds Agents 12-14. Use real `rg`/`find` searches via `bash` — do not reason about helpers from memory.

- **Existing-infrastructure inventory:** search the changed test files' directory for shared fixtures, mock servers, and assertion helpers the new tests could reuse — `test/mock_server.py` (`Server`, `HttpServer`), `test/qwp_ws_ack_server.py` (`QwpAckServer`, `TLS_CA`), `test/test_tools.py` (`TimestampEncodingMixin`, ILP byte builders), `test/patch_path.py`, the QuestDB fixture and `_parse_version` from `c-questdb-client/system_test/fixture.py`, the `_LiveCase` / capability-gate pattern in `test_client_row_egress.py`, the per-class fixture pattern in `system_test.py`, the RSS/`gc` leak harness in `test_dataframe_leaks.py`, the seedable fuzz loops in `test_client_*_fuzz.py`. This list is the baseline Agent 13 uses to flag reinvented boilerplate — a "you stamped boilerplate instead of reusing helper X" finding requires X to appear in this inventory.
- **Changed shared helpers as symbols:** if the PR changes a mock server, the fixture, a capability gate, or a test utility, run the 2.5b callsite inventory for it too — a changed mock server or a widened skip condition can silently weaken every test that uses it.
- **Exercised-symbol map:** for each new or changed test, list which production symbols from 2.5a it actually exercises, so Agents 12 and 14 can check efficacy and regression value.

### 2.5f Build & binding profile facts (mandatory at every level)

**This sub-step runs at every level, including 0 and 1 where the rest of Step 2.5 is skipped.** A single Cython directive, a submodule bump, or a Cargo profile flips the safety story for the entire extension; agents must reason from the actual profile, not from defaults. Record each fact with a file:line citation, verified this run — do not copy the values from this text.

- **Cython compiler directives.** Read the directive comments at the top of `src/questdb/_client.pyx` and the `cythonize(...)` call in `setup.py`. At the time of writing the module sets only `language_level=3` and `binding=True`, `setup.py` passes no `compiler_directives`, and the only local decorators are `@cython.no_gc_clear` and `@cython.internal` — so `boundscheck`, `wraparound`, `initializedcheck` and `nonecheck` are Cython's defaults. **If any of those is ever turned off, globally or per function, out-of-range or negative indexing into a C array or typed memoryview becomes undefined behavior, not an `IndexError`.** State what applies.
- **Exception-convention default.** This is Cython 3 (`cython>=3.1.2` in `pyproject.toml`) with `legacy_implicit_noexcept` unset: a non-`extern` `cdef`/`cpdef` function returning a C type **propagates** Python exceptions by default, including `nogil` ones. `noexcept` is an explicit opt-out this codebase uses on hot helpers and on every C callback; inside a `noexcept` function a raised exception is printed as unraisable and execution continues with an undefined return value. Every function declared in a `cdef extern` block is a `noexcept` C function. Agents 1, 2, 3 and 4 must read the actual clause on every changed `cdef`, and must treat "can this `noexcept` body raise?" as a crash/wrong-data question.
- **Native panic mode.** Read `c-questdb-client/questdb-rs-ffi/Cargo.toml` at the pinned commit: at the time of writing it sets `panic = "abort"` in **both** `[profile.release]` and `[profile.dev]`, and `setup.py` builds it `--release` with features `confstr-ffi,arrow,insecure-skip-verify`. Any Rust panic reachable from a Python call aborts the interpreter with no traceback; no Python-level `try` can catch it. Read `rpyutils/Cargo.toml` too: it declares no `[profile]`, so it unwinds by default — but every export is `extern "C"`, and a panic reaching an `extern "C"` frame aborts on current Rust. Same consequence. The only defense is proving no panic path exists in the Rust the Python code can reach with the arguments it passes.
- **Submodule commit** (`git submodule status`). If the diff moves it, the headers under `c-questdb-client/include/questdb/` at the *new* commit are the source of truth every `.pxd` must match. Re-verify the `.pxd` ↔ `.h` agreement for every declaration the change calls.
- **`rpyutils` header freshness.** `rpyutils/include/rpyutils.h` is cbindgen output. If `rpyutils/src/**` changed and the header did not, or `rpyutils.pxd` disagrees with the header, record it.
- **Interpreter and dependency floors.** `pyproject.toml`: `requires-python = ">=3.10"`, `numpy>=1.21.0`, `NPY_NO_DEPRECATED_API=NPY_1_7_API_VERSION` in `setup.py`. Wheels ship for CPython 3.10–3.14, **free-threaded `cp313t`/`cp314t`**, and **PyPy 3.10/3.11** (`ci/cibuildwheel.yaml`, `pyproject.toml` `[tool.cibuildwheel]`). A CPython C-API symbol that PyPy's `cpyext` does not implement breaks the PyPy wheel build at compile time; a numpy C-API symbol newer than 1.21 breaks the oldest supported build. State the floors.
- **Free-threading declaration.** The module does not currently set `freethreading_compatible`, so on a `t` build CPython re-enables the GIL when `questdb._client` is imported. If the PR adds that directive, every "the GIL serializes this" assumption in the extension becomes live and Agent 4 must review it as such.
- **Link-time facts:** `-flto` on every platform except 32-bit Windows; the Rust static libraries are linked as `extra_objects`. Informational unless the PR changes `setup.py`.
- **GC and finalizer facts:** which `cdef class`es carry `@cython.no_gc_clear` (their `__dealloc__` sees intact attributes during cycle collection); `qdb_py_is_finalizing()` from `_client_helper.inc` is the shutdown guard native-thread callbacks must consult.

A review without this section is incomplete. State these facts in one line at the top of every Step 3 agent prompt.

## Step 2.6: Test coverage map (mandatory at every level)

This step runs at EVERY review level, for EVERY PR that touches production code — including (especially) PRs that add or change no test code at all. A PR with zero test changes does not skip test scrutiny; it concentrates it here. At level 0, derive the behavioral-change rows directly from the diff (2.5a is skipped); at level 1+ use the 2.5a semantic deltas.

Build a coverage table with one row per behavioral change: every changed symbol whose delta is not "no behavioral change", broken down further by every new or changed branch, error path, and `None`/NULL/boundary case inside it. For each row, record:

- **Change:** symbol + the specific behavior/branch/path.
- **Test:** the exact test file and test name that exercises it — found via real `rg`/`find` searches across `test/` (search for the symbol name, the error message text, the configuration key, the C export name, the `QuestDBErrorCode` member). Citing a test without a recorded search command in the trace is a skill violation, same as 2.5b. "Existing tests probably cover it" is banned.
- **Test tier:** whether that test runs (a) in the mock-only wheel-build runs, (b) in PR CI against the released server, (c) only on the `linux-qdb-master` leg, or (d) only locally (`proj.py test_fuzzing`, `valgrind_test`, benchmarks). **Read the test's skip gate** — `skipTest`, `skipUnless`, a `_server_capabilities` or `QDB_REPO_PATH` check — and say whether the released-server leg would skip it. A row whose only coverage skips on every PR leg is not the same as a covered row.
- **Failure link:** what that test asserts and why the assertion fails if this behavior regresses. "The test calls the function" is not a failure link. For a leak test: the measured quantity and its threshold. For a crash fix: what the test does that would segfault without the fix.
- **Reachability / population:** the supported operation, configuration, transport, interpreter (CPython / PyPy / free-threaded), optional dependency (pandas / pyarrow / polars present or absent) or DataFrame shape that reaches the changed path and the users affected. "Public" or "user-visible" is not a population.
- **Credible regression consequence:** a concrete plausible mutation or recurrence and what that population would observe. Distinguish material harm from cosmetic output or developer-only inconvenience.
- **Change risk:** semantic complexity, branch/callsite breadth, state/lock/finalizer sensitivity, manual memory or refcount content, `nogil`/`with gil` content, and the strength of existing surrounding coverage or compile-time safeguards.
- **Stable test design:** the least invasive assertion and observation seam considered — a mock-server assertion on the bytes sent, a `QwpAckServer` script, a unit test on a `cdef`-free helper, a capsule-path test, a leak test — before reaching for a live-server test.
- **Effort / fragility evidence:** setup cost, production seams required only for testing, timing/nondeterminism, server-version dependence, platform or interpreter dependence, and alternatives searched. Bare "hard to test" is not evidence.
- **Dimensions:** applicable happy, error, `None`/empty, boundary, concurrency/finalizer, interpreter, optional-dependency and resource-cleanup dimensions, each covered / uncovered / N-A with a reason.
- **Disposition rationale:** why the row is `COVERED`, `CRITICAL GAP`, `MODERATE GAP`, `ACCEPTED GAP`, or `EXEMPT`.

Rows with no effective test are marked **UNTESTED**, then classified by evidence rather than category:

- **Critical gap (blocking):** only when the changed path and affected population are supported and reachable, a credible regression would cause a material Critical consequence (data loss/corruption, interpreter crash or abort, undefined behavior, memory-safety failure, hang/deadlock/unbounded leak, wire-format or ABI or public-API compatibility break, or similarly material user harm), existing controls do not contain that risk, and the row passes Step 3b. The label "bug fix", "public API", "C ABI", "nogil", "refcount", or "concurrency" never makes a gap Critical by itself.
- **Moderate gap:** meaningful but bounded regression exposure, including most bug fixes without a regression test, internal/error paths with a distinct but non-Critical consequence, or material uncertainty that does not meet the Critical burden.
- **Accepted gap:** low-risk, localized or mechanical behavior where recorded analysis shows that the least invasive meaningful test is disproportionate or more fragile than the code under test, and existing safeguards make residual user risk small. Example: a docstring or error-message wording change whose stable assertion would be a regex on prose.
- **Exempt:** verified no-behavioral-change rows (pure rename, dead-code removal, comment/doc/CI-only).

A bug-fix label or zero test changes triggers this analysis; neither predetermines severity or verdict. Urgency cannot waive an actual defect or an admitted Critical gap. The coverage map is required internal evidence for Agent 7, Agents 12-14, and the Step 4 test gate. Publish only admitted gaps; keep COVERED, ACCEPTED, EXEMPT, and omitted rows private unless the user asks. At level 0, rows may be per-symbol to bound cost, but new error paths and `None`/boundary paths still get separate rows.

## Step 3: Change-specific candidate discovery

Run this step with the `subagent` tool using fresh-context `reviewer` agents. Select only roles whose domain is materially touched, obey the level's discovery cap, and launch those roles as fresh-context, read-only tasks. Agent count is never evidence and unused roles are skipped.

Every selected agent receives:
1. The PR or local-range diff
2. The change surface map from Step 2.5, including the 2.5f build-profile facts in the first line
3. The test coverage map from Step 2.6

The diff plus surface map can be large — write them to a shared temp file and point each task at it, rather than pasting the whole payload into every task. Agents 10 and 11 are deliberate exceptions and receive reduced context (see their entries).

### Candidate-discovery directive (applies to all agents)

- You are a **hypothesis generator**, not an authority to publish a finding. Output atomic propositions for independent falsification. Do not assign severity, propose fixes, write persuasive titles, or use "verified", "proved", or "confirmed". Any role text below that mentions a finding or severity describes what to inspect, not what you may conclude.
- For each candidate, cite the exact changed hunk or unchanged callsite contract allegedly broken. Out-of-diff impact is valuable only after the PR-caused contract delta is established.
- Name the **supported-state producer**: the exact Python call sequence, keyword arguments or configuration string, DataFrame shape, interpreter, optional dependency, server version or capability set, server response, or code path that creates every required trigger condition. If you cannot locate it, write `producer: unknown`; do not invent a deployment or state.
- Give the reachability chain, head observation, same-trigger merge-base observation, user-visible symptom, and raw evidence paths/commands. Mark anything not actually checked as `unknown`.
- Actively seek disproof: an earlier validation, a `None` check, a length check before the index, a `try/finally` that already frees, a `__dealloc__` that already releases, a pin that already defers the free, an unreachable branch, a `noexcept` body that provably cannot raise, or unchanged/better base behavior. Record the strongest counterevidence.
- Claims containing **never**, **only**, **exactly one**, **no retry**, or equivalent universal negatives require an exhaustive caller/transport inventory, not one traced path.
- A proposition with no independent consequence is evidence for its parent candidate, not a standalone candidate. If the parent falls, its dependent propositions fall with it.
- Pre-existing bugs and residual hardening whose same-trigger behavior is unchanged or better than base are outside this PR's findings and verdict. Never file them as findings and never propose them as changes to this PR. A fully proved one leaves as a Step 4 adjacent issue draft; an unproved one stays in the private ledger.
- Two agents repeating the same reasoning are one hypothesis, not corroboration. Corroboration requires independent evidence types and still does not bypass Step 3b.
- Returning no candidate is valid and preferred to returning a speculative one.

### Agents

Use the following as a role catalog. Select only the roles allowed by the chosen level and change surface; do not launch the whole catalog.

**Agent 1 — Correctness & bugs:** `None`/NULL handling at API boundaries, edge cases, logic errors, off-by-one, operator precedence, error paths. Integer correctness across the Python↔C boundary: Python `int` → `int64_t`/`size_t` conversion and overflow, `<int>` / `<Py_ssize_t>` / `<size_t>` casts that truncate or wrap, signed/unsigned mismatches, negative-length math, untyped wide literals that silently box typed arithmetic. NaN/Inf handling. Timestamp unit and epoch conversions (micros vs nanos vs days; naive vs aware `datetime`; `NaT`). ILP v1/v2/v3 and QWP wire-format correctness, UUID byte order, decimal scale, array shape and dimensionality. Arrow offset/validity/stride math for sliced and chunked arrays. Cross-reference every changed symbol against its callsite inventory and verify the new behavior is correct at each callsite. When the diff touches pooled connections, error classification, cancellation, timeouts or failover, also verify the "Pooled transports, failover, and error classification" checklist — a change that lets a transient failure permanently poison a healthy connection or report an undelivered batch as delivered is a Critical candidate.

**Agent 2 — Cython memory, refcount & crash surface:** In a Cython extension, anything that corrupts memory, raises inside a `noexcept` frame, or aborts the native side takes down or silently corrupts the host Python interpreter.

**The build profile is the foundation.** Before reasoning about any guard, state the 2.5f facts in your first sentence: directive defaults, Cython 3 implicit propagation with explicit `noexcept` opt-outs, `panic = "abort"` in `questdb-rs-ffi`, `extern "C"` panics in `rpyutils` aborting. Then examine every reachable instance of:

- **Native memory leaks / double-free / use-after-free:** every `malloc`/`calloc`/`realloc` must be `free`d on **all** paths — success, early `return`, and the exception path (prefer `try/finally`). A `realloc` whose return value is assigned back to the same pointer leaks the original on failure. Freeing a pointer twice, or using it after `free`, corrupts the heap. A native handle freed while another frame on the same thread still holds a pointer into it — because a GC finalizer or signal handler re-entered through an `RLock` — is a use-after-free.
- **Reference-count errors:** every `Py_INCREF` needs a matching `Py_DECREF` on all paths; a missing `DECREF` leaks, an extra `DECREF` is a later use-after-free. Borrowed references (`PyWeakref_GetRef` results, dict/list borrows, `PyObject*` stored without incref, `Py_buffer.obj`) must not outlive their owner. `Py_XDECREF` on a pointer that was never set is fine; on one that was already decref'd is not.
- **Buffer-protocol imbalance:** every `PyObject_GetBuffer` must have a matching `PyBuffer_Release` on all paths, and the raw pointer must not be used after the exporting object can be collected. Check `Py_buffer_obj_is_set` guards on partially-acquired buffer arrays.
- **Arrow C Data Interface ownership:** `release` called exactly once per `ArrowArray`/`ArrowSchema`/`ArrowArrayStream`; a consumed PyCapsule marked so it cannot be consumed twice; the `_qs_*` stream callbacks never raise and never outlive their producer.
- **Silent exception swallowing:** a `noexcept` function whose body can raise — directly or through a call to a function that can — drops the exception as unraisable and returns garbage. Trace every call inside each changed `noexcept` body.
- **Indexing under the active directives:** per 2.5f, verify whether `boundscheck`/`wraparound` are on; if a function or the module turns them off, every index needs an established bound.
- **Direct aborts and native panics:** any Rust panic reachable with the arguments the Cython code passes — an `unwrap` on a `None` the C ABI contract allows, an `assert!` on a length the Python side does not validate, arithmetic on an untrusted length — aborts the interpreter. Also any C-API call that aborts on misuse (`PyGILState_*` imbalance, a C-API call without the GIL, `PyEval_RestoreThread(NULL)`).
- **Finalizer and shutdown hazards:** a `__dealloc__` that touches Python attributes a `tp_clear` may already have cleared (check `@cython.no_gc_clear`), a `__dealloc__` that acquires a lock the current thread holds, a native-thread callback that acquires the GIL after `qdb_py_is_finalizing()`, a `__del__`-time call into a module whose globals are gone (`'Condition' object has no attribute ...` is the signature).
- **Uninitialized memory:** a struct field or `malloc`'d region read before it is written, especially partially-built per-column state on an error path that then gets freed.

Every candidate must be evaluated under the actual profile from 2.5f, not the textbook one.

**Agent 3 — C-ABI boundary safety:** Check every call into the `c-questdb-client` and `rpyutils` C ABI. Verify:
- **`.pxd` matches the pinned header.** For every changed or called C-ABI symbol, read the actual declaration in `c-questdb-client/include/questdb/ingress/*.h`, `c-questdb-client/include/questdb/egress/*.h` or `rpyutils/include/rpyutils.h` **at the pinned submodule commit** and confirm the `.pxd` declaration matches exactly: parameter types, pointer/`const`-ness, return type, struct field order and types, enum discriminant order, `nogil` on the declaration. The `.pxd` files are hand-written and nothing checks them — a mismatch is silent memory corruption or ABI breakage, not a build error. If the pointer moved, verify against the **new** commit and diff the headers between old and new pin for added, removed, or re-typed exports.
- **NULL handling:** every pointer returned from a C function checked before dereference; every pointer argument that could be NULL handled on the Cython side or documented as accepted on the C side.
- **Error object lifecycle:** every `line_sender_error*`, `questdb_error*` and `questdb_conf_str_parse_err*` obtained through an out-param is converted and freed exactly once, never leaked, never double-freed, never read after free; the right converter is used for the right error type; the `QuestDBErrorCode` mapping is complete for new native codes.
- **Ownership transfer:** `line_sender`, `line_sender_buffer`, `qwp_*` sender/reader/cursor handles, `qdb_pystr_buf` — who allocates, who frees, whether a failed call consumes or preserves the handle, and whether the lifetime is correct relative to the owning `cdef class` (`__cinit__`/`__dealloc__`) and to any pin or lease that may outlive the Python wrapper.
- **`qdb_pystr_buf` arena invalidation:** UTF-8 pointers handed to Rust must remain valid until the native write completes and must not be invalidated by an intervening `clear`/`truncate`/append.
- **String encoding:** Python `str` → UTF-8 by kind (UCS1/2/4), correct byte length passed, lone-surrogate rejection, embedded-NUL handling, `bytes`/`str` distinction, `const char*` versus `(ptr, len)` pairs.
- **ABI stability across the bump:** struct layouts not reordered, enum discriminants not renumbered, feature flags in `setup.py` still enabling every export the `.pxd` declares.

**Agent 4 — GIL, threads & re-entrancy:** Verify:
- **`nogil` correctness:** no `with nogil` block or `noexcept nogil` function touches a Python object, calls the CPython C-API, raises, or `INCREF`/`DECREF`s. Errors discovered under `nogil` must be deferred and raised after reacquiring the GIL.
- **GIL release around blocking calls:** flush/connect/query/fetch native calls release the GIL so other threads run; the released region references no Python state; the custom `PyEval_SaveThread`/`PyEval_RestoreThread` helpers are balanced on every path including exceptions.
- **Native-thread callbacks:** connection-event, rejection and failover-reset callbacks are `noexcept with gil`, check `qdb_py_is_finalizing()` before acquiring the interpreter, never raise, never block on a lock the delivering thread cannot satisfy, and never free an object the Python thread is using.
- **Same-thread re-entrancy through `RLock`:** `QuestDB`, `PooledSender` and `PooledReader` use re-entrant locks. A GC finalizer (`__dealloc__`, a user object's `__del__`, a weakref callback) or a signal handler (`KeyboardInterrupt`) can run on the *same* thread inside the lock body — including during a native decode that holds raw pointers into a cursor — and re-enter `close()`, `cancel()`, or `_free()` without blocking. Verify the pin/defer mechanism covers every decode site and every path that can free a handle.
- **Thread-safety contracts:** `Sender`/`Buffer` are documented single-threaded; pooled objects document which operations may be called from other threads. Verify the implementation matches the docs, and that shared mutable state reachable from more than one thread (pool state, event queues, the `active_senders` registry, `_DISPATCH_THREAD`) is synchronized. Cross-reference every callsite from 2.5b for violations of the new concurrency contract.
- **Free-threaded build:** if the change assumes the GIL serializes access, note whether it would hold on `cp313t`/`cp314t` once the module declares `freethreading_compatible` (see 2.5f for whether it does now).
- **Shutdown:** interpreter finalization while pooled connections, background native threads, or iterators are alive — hangs, unraisable noise, or callbacks into a torn-down module.

**Agent 5 — Resource management & lifecycle:** Leaks on all code paths (especially errors). Check `__cinit__`/`__dealloc__` pairing on every `cdef class` (does `__dealloc__` free everything `__cinit__` and methods allocated, and is it safe when `__cinit__` failed partway or when `tp_clear` ran first?). Native handle lifecycle (`line_sender`, `line_sender_buffer`, `qdb_pystr_buf`, QWP sender/reader/cursor handles) including pinned and leased handles whose free is deferred. Socket/connection/TLS teardown on error (Rust owns the socket, but verify the Cython side calls close/free exactly once and does not drop a healthy pooled connection it could recycle). **Arrow C Data Interface:** `release` callbacks invoked exactly once; PyCapsule consumption semantics correct; no double-release; batch iterators release the previous batch before fetching the next. Python-level retention: iterators and `QueryResult` dropping references to decoded batches once yielded; caches that grow per query. Walk every callsite from 2.5b that constructs, owns, or transfers ownership of a native handle and verify cleanup on all paths (success, exception, early return, GC finalizer, interpreter shutdown).

**Agent 6 — Performance & algorithmic fitness:** For every new or changed loop, traversal, data structure, or computation on a data path (per-row buffer build, per-cell DataFrame ingestion, flush, per-cell egress decode, Arrow conversion):

1. **Algorithm optimality:** State the time complexity, then ask whether a better algorithm exists. O(n) where O(1) is achievable (hash lookup vs linear scan, direct indexing vs search) is a candidate. O(n log n) where O(n) suffices is a candidate. The bar is "use the best known approach", not merely "avoid quadratic".
2. **Python-object operations on the hot path:** attribute lookups, `dict`/`list` access, method calls, `isinstance` chains, `str` re-encoding, exception setup, boxing of C values into Python objects — anything per row or per cell that belongs at C level or hoisted to setup. Read the Cython annotation mentally: an untyped literal in typed arithmetic, a `cdef` without a type, or an `object` intermediate turns a C expression into Python calls.
3. **Multi-pass vs single-pass:** multiple passes over the same column or batch (validate, then encode; scan, then copy) is a candidate unless each pass structurally depends on the completed output of the previous one.
4. **Redundant computation:** values recomputed per row that could be hoisted, repeated lookups of the same column key or dtype, re-validation of already-validated names, re-planning a schema per batch.
5. **Data structure choice:** linear search where a hash set gives O(1) membership; per-row allocation where a reusable scratch buffer suffices; a Python `list` where a typed memoryview or C array fits.
6. **Unnecessary allocations and copies:** any allocation on the per-row or per-cell path is a candidate even if it is a single one. Copying column data that the buffer protocol or Arrow already exposes zero-copy. Materializing a whole result when the iterator path exists.
7. **GIL churn:** acquiring/releasing the GIL per row or per cell, or holding it across a blocking native call.
8. **Setup vs data path:** allocations, planning and parsing during sender construction, configuration parsing, or once-per-DataFrame schema resolution are acceptable; the same work per row is not.
9. **Magnitude (required on every performance candidate):** state what the cost multiplies by — rows per flush, cells per DataFrame, bytes of payload, batches per query — or, if it does not scale with data, the fixed bound that caps it. Say plainly whether the cost is on a path the user waits for or repeats. A candidate with no magnitude cannot be classified and will be dropped.

For changed symbols now reachable from new contexts (per 2.5d), check whether any of those contexts is a hot path that amplifies an otherwise-acceptable cost. Realistic scale for this library: DataFrames of millions of rows and tens to low hundreds of columns, buffers up to a few hundred MB, symbol cardinality in the thousands, query results streamed in batches of tens of thousands of rows.

**Agent 7 — Test review & coverage:** Coverage gaps, error-path tests, `None`/edge-case tests, boundary conditions, regression tests, test quality. Check:
- Mock-server tests in `test/test.py` (against `mock_server.py` and `qwp_ws_ack_server.py`) — these run on every CI leg and every wheel build
- Live suites in `test/system_test.py` and `test/test_client_row_egress.py` — these run in PR CI against the released server and on `linux-qdb-master` against master; read each test's skip gate
- DataFrame tests in `test/test_dataframe.py`, capsule-path tests in `test_client_capsule_path.py`, failure-injection tests in `test_client_dataframe_failures.py`, seeded fuzz tests in `test_client_dataframe_fuzz.py` / `test_client_polars_fuzz.py`, and **leak tests** in `test_dataframe_leaks.py` and `system_test.TestEgressLeaks` (new native-memory, buffer-protocol, Arrow or refcount handling should have a leak test)
- `cargo test` in `rpyutils/` for Rust helper changes
- Examples in `examples/` still run; `examples.manifest.yaml` is consistent (`TestManifest`); `proj.py doc` still builds with `-nW` for docstring changes

Cross-reference 2.5d: every cross-context exposure should have a test that exercises the changed symbol from that context. For each missing cross-context test, add an `UNTESTED` Step 2.6 row; do not predetermine its severity or publication. Consume the Step 2.6 coverage map: re-verify every claimed test and failure link (read the assertion, don't trust the map), record which tier the test runs in and whether its gate would skip on the released-server leg, and hunt for behavioral changes the map missed. Then run a **mutation spot-check**: pick the 3-5 most dangerous changed lines (boundary comparisons, error handling, `None` checks, a `free` or `DECREF`, an `except` clause, a lock boundary) and ask, per line, "which test fails if this line is wrong — inverted condition, off-by-one, dropped free?" When no assertion would catch a mutation, add an `UNTESTED` map row even if a test nominally executes the line. Test *efficacy* and test-*code* quality are Agents 12-14; here, focus on whether coverage exists for every new or changed path.

**Agent 8 — Public API ergonomics & cross-surface consistency:** API ergonomics is mission-critical here, not cosmetic: an awkward or inconsistent public API leads users into data loss, silent misuse, and crashes. Review every public class, method, keyword argument, configuration-string key, enum member, exception type, return type, stub entry and docstring the PR adds or changes, against the surfaces that already exist. Use the "API ergonomics & cross-surface consistency" checklist below as your spec. Concretely:

- **Consistency first.** For each shared concept the change touches (table/column names, designated timestamp, batch and row limits, column-type overrides, timeouts, `at=` semantics, error reporting), confirm it is expressed the *same way* across every sibling entry point: `Buffer.row` vs `Sender.row`, `QuestDB.dataframe` vs `Sender.dataframe` vs the Arrow/PyCapsule path, `to_pandas`/`to_arrow`/`to_polars`/`iter_rows` and their `iter_*` siblings, keyword arguments vs configuration-string keys vs `from_env`. A new parameter that takes a `timedelta` where its sibling takes milliseconds, validates lazily where the sibling validates eagerly, or omits a control the sibling exposes, is a candidate.
- **Easy path = safe path.** Flag any API where the natural call is the unsafe one — e.g. a method that silently leaves rows unflushed or a query uncancelled unless the user remembers a separate call, while a sibling does it implicitly. Silent semantic divergence between similarly-shaped methods (commits vs defers, raises vs returns `None`, recycles the connection vs drops it, consumes the DataFrame vs leaves it) that the type system does not surface is the highest-severity ergonomic defect — graded by data-loss blast radius, never Minor.
- **Accept what callers hold; cut positional noise.** Accept `str`/`bytes`/`datetime`/`int` where the codebase already does; avoid sentinel arguments every call must pass; prefer keyword-only parameters for new knobs; return plain tuples/named tuples/dataclasses consistently with siblings (`ExecDone`, `ServerInfo`, `ConnectionEvent`).
- **Evolvability & docs.** Every public change is mirrored in `_client.pyi` (signature, defaults, `Optional`, overloads), `__init__.py` `__all__`, the `questdb.ingress` shim where a 4.x name is affected, `docs/api.rst` / `docs/sender.rst`, and `CHANGELOG.rst`. Public docstrings describe the contract the signature cannot encode and cross-reference siblings that differ.
- **Backward compatibility.** Renamed or removed keyword arguments, changed defaults, changed exception types or error codes, changed configuration-string semantics, and changed return types are breaking; they must be intentional, listed under "Breaking changes" in the changelog, and called out in the PR body. The deprecated `questdb.ingress` import surface must keep working.

Also cover the mechanical hygiene: no dead code or unused `import`/`cimport`, naming consistent with the codebase, comments that explain *why* rather than restate the code (a comment that restates the code is a finding; so is a multi-line comment on a one-line change), `proj.py doc` building cleanly.

**Agent 9 — Cross-context caller impact:** Walk the callsite inventory from 2.5b. For every callsite, fetch the surrounding code (the calling function plus its callers up two levels) and answer:

- Does this caller pass inputs the new behavior handles incorrectly?
- Does this caller depend on a contract from the implicit contract list (2.5c) that the change broke — the old `except` convention, the old ownership of a handle or error object, the old `qdb_pystr_buf` lifetime, the old refcount behavior, the old lock expectation, the old "handle is never freed while a decode is running" guarantee?
- Is this caller in a context (a `with nogil` block, a `noexcept` body, the per-row hot loop, an auto-flush trigger, an Arrow release or stream callback, a `__dealloc__`, a native-thread callback, a signal-interruptible lock body, an exception path, interpreter shutdown) where the new behavior misbehaves even if the inputs are valid?
- For a changed `cdef`/`cpdef` exception convention: do all callers still detect and propagate the error?
- For a changed `.pxd` declaration: does it still match the pinned header, and do all Cython callers pass the right types and ownership?
- For a changed object state machine (`Buffer`, `Sender`, `QueryResult`, pooled connection, cursor handle): do all callers respect the new state transitions (buffer cleared after error before reuse; flush only when flushable; cursor freed only when unpinned; connection recycled only when reusable)?

Output is structured per callsite, not per failure mode. Each callsite gets a verdict: SAFE / CANDIDATE / INSUFFICIENT_EVIDENCE. A CANDIDATE is only an atomic hypothesis for Step 3b; it has no severity yet.

Select this role whenever changed symbols have meaningful out-of-diff callers. It counts toward the level's discovery cap; small diffs to widely used symbols (`Buffer.column`, `Sender.flush`, the dataframe entry points, a `.pxd` declaration, `_CursorHandle._free`, an error converter) usually justify it.

**Agent 10 — Fresh-context adversarial:** Dispatched separately to escape checklist anchoring. This agent operates under different rules:

- It receives ONLY the diff and the names of the changed files. It does NOT receive the change surface map, the implicit contract list, the cross-context exposure list, the coverage map, or any of the review checklists below.
- Its sole instruction: "generate a small set of falsifiable ways this code could be wrong, and try to disprove each before returning it." No category list, no failure-mode taxonomy, no QuestDB-specific style guide.
- It is free to use `read` and `bash` (`rg`/`grep`/`find`) to explore the repository however it wants.
- Each surviving output follows the candidate contract: atomic proposition, changed attribution, producer, reachability, head/base observations, symptom, counterevidence, missing evidence. No severity, no fix.

The point is to escape the structured frame, not to create privileged candidates. A unique hypothesis is not high signal by itself, and overlap is not corroboration unless it supplies an independent evidence type. Select this role only when a distinct adversarial pass is warranted; it counts toward the level's discovery cap.

**Agent 11 — Adversarial performance:** Dispatched separately from Agent 6 to escape checklist anchoring.

- It receives the diff plus the full source files the diff touches (not just the changed lines). It does NOT receive the performance checklist, the change surface map, or Agent 6's output.
- For every function the diff adds or modifies, read the full implementation and ask one question: **"What is the fastest correct way to implement this in Cython, and does the code match it?"**
- Work bottom-up from the code: trace data flow through each function — what is read, how many times, in what order, and which operations compile to C versus Python calls. Look for passes that could be eliminated or fused; lookups that could be O(1); allocations avoidable by reusing a scratch buffer; Python objects created where a C value would do; `object`-typed intermediates in typed arithmetic; per-element loops over column data that could be `memcpy`'d or handled by numpy/Arrow in bulk; work done unconditionally that is only needed conditionally; intermediate collections built and iterated once; GIL transitions inside loops.
- Read callers to understand actual input sizes and access patterns — an O(n) scan that runs once per DataFrame is different from one that runs per cell.
- Each candidate states what the code does now (with complexity), what the better approach is (with complexity), and the magnitude: call frequency, data scale, or hot-path placement.
- Do not duplicate style findings — focus purely on algorithmic and computational efficiency.

Select this role only when the diff changes loops, algorithms, data structures, allocation behavior, type declarations on a hot path, or a plausible hot path; it counts toward the level's discovery cap.

**Test-code agents (Agents 12-14) — eligible only when the diff adds or changes test code or claims a bug fix.** A production change with no test code is still handled by the Step 2.6 gate. Select only the applicable test roles within the level's discovery cap. Each receives the diff, the change surface map, and the 2.5e test surface inventory. Tests are not second-class code — apply the same adversarial rigor.

**Agent 12 — Test efficacy & correctness (adversarial):** Prove each test actually exercises the production change and could fail if that change regressed.
- **Vacuous assertions:** `assertTrue(True)`, asserting a value against itself, asserting on a value the test itself just hard-coded, a test body with no assertion and no `assertRaises`, a `with self.assertRaises(...)` whose body raises for an unrelated reason, a regex so loose it matches any error.
- **Tests that don't reach the changed code:** the assertion passes whether or not the production change is present. Trace the data flow from the changed symbol to the assertion. A mock-server test that asserts on the mock's canned reply rather than on the bytes the client actually sent is the classic instance; so is a live test whose capability gate skips on every CI leg.
- **Happy-path-only:** no assertion on the error/`None`/empty path the production change added; no check of the raised `QuestDBErrorCode` or message; no check that the connection was recycled rather than dropped (pool stats), that memory was released (leak harness), or that the iterator stopped.
- **Concurrency and finalizer tests:** races in the harness itself, missing joins or barriers, an assertion on a spawned thread whose failure is swallowed, `time.sleep` used as synchronization, a GC-trigger test that does not actually force collection at the intended point (`gc.collect()` placement, reference cycles that are not cycles, batch sizes that never reach the decode site).
- **Setup/teardown resource handling:** fixtures or servers not stopped on the failure branch, sockets left open, fuzz seeds not printed so a failure cannot be reproduced, environment variables not restored.
- Each candidate states the exact assertion and why it cannot fail or what it fails to cover.

**Agent 13 — Test-code quality & maintainability:** Review the test as code.
- **No code reuse / boilerplate stamping:** before accepting repeated setup or assertion blocks, search for existing fixtures and helpers using the 2.5e inventory (`mock_server.Server`/`HttpServer`, `QwpAckServer`, `TimestampEncodingMixin`, the `_LiveCase` capability gate, the leak harness, the seeded fuzz loop). If a helper already exists that the new test reimplements inline, name it. Duplicated blocks across new test cases that should be a single helper or a `subTest`-driven loop are candidates.
- **Doc bloat:** multi-paragraph docstrings on a test that merely restate the test name; stacked or duplicated commentary. Test intent belongs in a precise test name plus, at most, a one-line comment.
- **Residue and smells:** dead code, commented-out code, copy-paste leftovers (a `test_foo` that actually tests bar), `print()` debugging left in, `skip` without a referenced issue or capability reason, magic constants with no explanation.
- **Which standards apply:** bare `assert` in tests is fine. Hard-coded ports, unbounded sleeps, dependence on a server version without a gate, and dependence on network access beyond the fixture are findings because they make CI flaky.

**Agent 14 — Regression-test efficacy verification:** For any PR that claims to fix a bug, verify the regression test would actually fail without the production change. Reason about reverting the production hunk and confirm the new or changed test's assertions would then fail — for a crash fix, that the test would segfault or abort; for a leak fix, that the measured growth would exceed the threshold; for a connection-handling fix, that the pool stats or error type would differ. If the test still passes with the fix reverted, it is not a regression test. State, per test, which production line the test depends on and what its assertion would do if that line were reverted. Run only when the PR is a fix; skip for pure features or refactors.

Combine agent outputs into a private **candidate ledger**. Split compound narratives into atomic propositions, deduplicate by proposition plus evidence, and record dependencies. Do not draft report prose, severity, or a suggested fix. A candidate is not a finding.

## Step 3b: Independently falsify, prove, and admit candidates

Use this state machine with no shortcuts:

`HYPOTHESIS → FALSIFYING → PROVEN → ADMITTED`

Any missing proof, unresolved contradiction, failed reproduction, unsupported producer, or dependence on an omitted premise ends at `OMITTED`. There is no `DOWNGRADED` state for an unproven behavioral claim, and "could not disprove" never means `PROVEN`.

Discovery agents work from the diff plus the surface map and frequently produce false positives — especially around native memory ownership, refcounting, GIL boundaries, Cython exception conventions, lock re-entrancy, and C-ABI lifecycle. Treat every candidate accordingly.

At levels 1-3, launch one fresh-context falsifier per atomic candidate. The falsifier receives only (a) the neutral proposition, (b) target repository, base/head revision identities (commit SHAs, or a captured diff hash for an uncommitted working tree) and relevant file names, and (c) raw evidence/artifact paths. **Do not send** the discovery narrative, proposed severity, suggested fix, author identity, other agents' votes, or statements that the claim was verified. At level 0, the parent applies the same protocol inline from a blank evidence form before writing any report prose.

The falsifier's first task is to construct the strongest disproof: find a missing producer, an unsupported configuration or dependency combination, an impossible server-version pairing, an omitted caller, a guard, a `try/finally`, a pin that defers the free, a `__dealloc__` that already cleans up, a `noexcept` body that cannot raise, or identical/better base behavior. Only if the candidate survives does it assemble affirmative proof.

A behavioral candidate is admitted only when every field below is backed by cited evidence:

- **Attribution:** exact changed hunk, or exact unchanged callsite plus the contract this PR changed.
- **Supported-state producer:** exact supported Python call sequence, keyword arguments or configuration string, interpreter, optional dependency, DataFrame or result shape, server version/capability or server response that creates every trigger condition. A reachable consumer branch is not proof that any producer can create its input.
- **Reachability:** complete producer-to-symptom path, including callers, transports, retries, guards, locks, finalizers, and validation.
- **Head observation:** executed trigger and observed output/state at the reviewed revision.
- **Base observation:** the identical trigger and observed output/state at `$BASE`, or `N/A — genuinely new surface` with proof.
- **User symptom:** independently observable consequence; a statement that merely justifies another candidate is not a finding.
- **Counterevidence search:** strongest attempted disproof and why it does not apply.
- **Artifact:** exact command/test, output, environment (interpreter, numpy/pandas/pyarrow/polars versions, server version), and revision identity (commit SHA or captured diff hash). Race, ordering, finalizer-timing, signal, reconnect, pool-state, leak, wire-compatibility, and on-the-wire-format claims always require runtime evidence; static source reading alone cannot admit them.

For static findings fully proved by source — a `.pxd` declaration that disagrees with the pinned header, a stub that disagrees with the signature, a C-API symbol absent from a supported interpreter, a Cython compile error, direct standards violations — mark producer/head/base/runtime fields `N/A — static` and cite the complete source proof. For a coverage gap, recorded searches may statically prove only that an effective test is absent; they never make the supported-state producer, reachability, affected population, credible regression consequence, or user impact `N/A`. A Critical coverage gap must prove those fields under Step 2.6. `N/A` is forbidden whenever a load-bearing premise concerns runtime shape, reachability, or impact.

Special burdens:

- A wire-format, server-compatibility, or ABI claim must identify an actual producer that emits the alleged bytes or layout in a supported deployment. A constant comparison or a reader guard is not a producer. For server-side behavior, cite the released server version the fixture downloads or the `linux-qdb-master` leg.
- A claim containing **never**, **only**, **exactly one**, **no retry**, or an equivalent universal negative must include an exhaustive inventory plus an executed probe. One path proves only that path.
- A concurrency, finalizer-timing, or signal-timing claim must force or observe the interleaving — a `gc.collect()` placed at the decode site, a signal raised inside the lock body, a thread barrier — timing prose is not evidence.
- A leak claim must show the measurement (RSS, `tracemalloc`, `sys.getrefcount`, pool stats) at head and base under the same loop count.
- A regression-test claim must run the test on head and against the reverted production hunk.
- A crash/abort claim must be evaluated under the 2.5f build profile: `panic = "abort"` in `questdb-rs-ffi`, `extern "C"` panics aborting in `rpyutils`, and the actual `except`/`noexcept` clause on every frame in the trace.
- If a parent premise is omitted, omit every candidate that depends on it; do not preserve its supporting propositions as Moderate findings.

If required execution is impossible (no live server of the needed version, no PyPy or free-threaded interpreter, no pandas/pyarrow/polars installed, extension not buildable), record the validation limitation in the private ledger and omit the candidate from the public findings. Never fall back from failed or unavailable execution to confident prose.

After a candidate satisfies this admission schema, apply the domain-specific checks below:

1. **Read the actual source code** at the exact lines cited, in the `.pyx`/`.pxi`/`.pxd`/`.pyi` — never the generated `_client.c`. Do not rely on the agent's description alone.
2. **Trace the full code path**: follow callers and `cdef` helpers across the `include`d files. A method called on a Python object may dispatch to a `cdef class` subtype; an `object`-typed parameter may hide a conversion.
3. **Check both sides of the C ABI**: if a candidate involves Cython↔Rust interaction, read the Cython call, the `.pxd` declaration, the header at the pinned commit, and — when the contract is unclear from the header docs — the Rust `questdb-rs-ffi` or `rpyutils` implementation. Verify ownership transfer, error propagation, and freeing on every side.
4. **For native-memory-leak claims**: trace every `malloc`/`calloc`/`realloc` and every native handle to its `free` on ALL paths (success, early return, exception unwind, finalizer, deferred free after unpin). Confirm the intervening code can actually raise before claiming the exception path leaks.
5. **For refcount claims**: count `Py_INCREF`/`Py_DECREF` on every path; confirm borrowed-vs-owned reasoning against the CPython C-API contract of each function used (Cython's `object` return declarations in the `.pxd` encode ownership — check them).
6. **For exception-swallowing claims**: check the actual clause on the `cdef` and whether the body — including everything it calls — can raise. Implicit propagation is the Cython 3 default; only an explicit `noexcept` or a `cdef extern` swallows.
7. **For GIL claims**: verify the cited code is actually inside a `nogil` region or a `with gil` callback and actually touches a Python object or C-API; a `cdef` called from `nogil` may itself be declared `with gil`.
8. **For re-entrancy and finalizer claims**: identify the exact object whose finalizer runs, prove it can be collected at that point (is it only reachable through a cycle? does the decode site allocate?), prove the lock is re-entrant for that thread, and show the inner call reaches the free while the outer frame still dereferences the handle. Then reproduce it: a `gc.collect()` hook or a `__del__` on a value object at the decode site is the standard artifact.
9. **For `.pxd` ↔ header mismatch claims**: read the exact declaration in the pinned header and compare field by field, including `const`, pointer depth, enum order, and struct padding-relevant field types. A claimed mismatch that actually matches is a false positive; a mismatch Cython tolerates (e.g. `int` vs `int32_t` on the same platform) still needs a stated consequence.
10. **For numeric overflow/truncation claims**: check reachability at realistic scale — DataFrames of millions of rows, buffers up to a few hundred MB, tens to low hundreds of columns, timestamps within the server's representable range. Drop overflows that require values beyond that scale. Classify by the consequence Cython actually produces: a C cast wraps silently; a Python `int` → C conversion raises `OverflowError`.
11. **For performance claims**: verify technical accuracy (correct complexity, correct hot/cold path identification, the operation really compiles to Python calls — check the annotation or the generated C if needed) **and then establish magnitude**. State what the cost multiplies by — rows, cells, bytes, batches — or the fixed bound if it does not scale. A performance claim with neither a multiplier nor a bound is not verified. Do not drop a technically correct finding because today's payloads are small — workloads grow. Do move it from Critical to Moderate when the cost is structurally bounded and off the data path. Exception: a Python-object operation or allocation on the per-row or per-cell path is always worth reporting, even a single one.
12. **For cross-context findings (Agent 9)**: re-read the callsite in full, including its callers up two levels, and confirm the broken behavior is reachable from production code paths or test paths users will exercise. Cross-context findings are high-value but the easiest to overstate — verify carefully.
13. **For API ergonomics / consistency findings (Agent 8)**: name and read the specific peer surface the finding is measured against — the sibling method, the other entry point, the configuration-string key, the stub, the shim — and quote both shapes side by side. An ergonomic complaint with no concrete peer surface to compare against, or one that contradicts an established codebase-wide convention, is bikeshedding: drop it or downgrade to Minor. Then grade by user impact, not appearance: a divergence that can cause data loss, an unflushed buffer, a dropped connection, or silent misuse is Critical/Moderate; a pure style/naming difference with no safety impact is Minor.
14. **For test-efficacy candidates (Agents 12, 14)**: re-read the cited assertion in full context and confirm it can fail for the claimed regression. For "would pass without the fix" claims, use a scratch `git worktree` (never the primary working tree): build with `proj.py build`, run the new test at the reviewed revision, then revert the production hunks (`git checkout <base> -- <files>`), rebuild, and run it again. Admission requires green-on-head and red-without-fix artifacts. If the environment cannot build or run the test — no live server of the needed version, no pandas/pyarrow/polars, no PyPy — omit the candidate and record the limitation privately; do not fall back to confident reasoning. The same rule applies to every dynamic Critical candidate.
15. **For coverage-gap candidates (UNTESTED rows from 2.6)**: verify the recorded test search and failure-link analysis, then try to falsify the risk with existing indirect assertions, guards, type/compile guarantees, constrained inputs, or downstream validation. Establish supported reachability, an affected population, a credible regression mode, and its material consequence before assigning Critical. Evaluate the least fragile meaningful test and concrete alternatives — a mock-server byte assertion or a capsule-path unit test is usually available where a live-server test is not. Reject bare "simple", "urgent", "hard to test", or "covered indirectly" claims. A Critical gap may be counterfactual about whether the code is currently wrong, but never about reachability or impact. Test difficulty does not downgrade an independently proved defect.
16. **For test-code-quality findings (Agent 13)**: confirm a "reinvented helper" finding by actually locating the helper and checking its signature fits the test's need.
17. **For "swallowed error → silent data loss / wrong state" claims**: an error-handling branch is defensive coding, not evidence that anything fails. Before admitting, name **all three** of: (a) the concrete error and the exact statement that produces it — quote the line, don't infer it from the presence of an `except`; (b) proof that this error is actually caught by the specific clause cited — `except ValueError` does not absorb a `QuestDBError`, and an uncaught exception means the call **fails loudly**, which inverts the finding; (c) that the failing statement is reachable with the arguments the callsite actually passes (constants, pre-validated inputs, and guarded early returns frequently make it unreachable). If any of (a)-(c) cannot be established, omit the candidate. Also check for the non-failing sibling: an early `return`/`continue` that silently drops a row, a batch, or a lease with no error at all is usually far more reachable than the error path and is not fixed by reordering statements around it.
18. **Verify the conjunction, not just the links.** A multi-step candidate ("A returns early → B leaves `_must_close` armed → C treats the lease as terminal → D drops a healthy connection → the user's next query reconnects") is only as true as its weakest step. Identify the single **load-bearing step** — usually "this state can actually occur" — and try to falsify it first. Per-line support for each isolated link does not prove their conjunction. Reading code is not verification when the load-bearing step is a runtime-shape claim — "the finalizer runs here", "the server returns this status", "the pool takes this branch" — or any claim about races, ordering, signals, reconnects, or pool state. Such a step requires an execution artifact produced or independently re-run by the falsifier at the cited revision. An agent's prose is not an artifact.
19. **Derive a fix only after admission, then verify it compiles and closes the window.** A plausible fix is never evidence that the finding is real. Once admitted, check that every referenced variable is in scope and typed, that ownership transfers do not create a double-free or leak, that the `.pxd`, `.pyi`, `__init__.py`, docs and changelog stay in sync, that the fix survives the Cython exception convention of the frame it lives in, and that it closes every admitted path.
20. **Determine net user impact, then classify.** Step 4 assigns severity only after this determination. A behavioral candidate missing it is `OMITTED` and never reaches Step 4.

    **(a) Net user impact — answer all five, in order:**
    - **Population** — who reaches it: every user of the library, every user of a named transport (ILP/HTTP, ILP/TCP, QWP/WebSocket, QWP/UDP), a named ingestion path (row API, pandas, pyarrow, polars, PyCapsule), a named egress path (`iter_rows`, `to_pandas`, `to_arrow`, `to_polars`), a specific interpreter or dependency version, a specific configuration, a specific schema or dtype. "Any user in principle" is not a population. If no supported population can execute the producer, omit the candidate; do not preserve it as Moderate.
    - **Delta vs base** — what that population observes differently from `$BASE` for the identical executed trigger. Static comparison is allowed only for a fully static finding; every behavioral claim requires observed head and base artifacts.
    - **Magnitude and frequency** — how much and how often: per row, per cell, per batch, per flush, per query, per reconnect, once per process. Reuse the 3b.11 multiplier or bound.
    - **Offsets** — what recovers this downstream before the user sees anything. Code offsets: a later validation, a retry, a server-side rejection, a caller that discards the value, a guard the same PR added elsewhere, a `finally` that releases anyway. **Process offsets count too**: a CI leg that exercises it (`linux-qdb-master`), a release convention (`RELEASING.rst`), a documented limitation. Name the offset, or write "none found, searched <where>".
    - **Net** — exactly one of:
      - **net-negative** — the population is measurably worse off than base. Only net-negative behavioral candidates can be admitted.
      - **net-neutral** — no observable regression versus base. Omit it from PR findings.
      - **net-positive** — the population is better off than base. Omit it from PR findings.

    A **coverage-gap** row is counterfactual only about whether an unobserved regression currently exists. Its producer, reachable path, population, credible regression consequence, magnitude, offsets, change risk, and stable-test feasibility must be evidenced under Step 2.6. Coverage absence affects confidence; it does not manufacture impact. Static code-quality findings are assessed directly from changed lines.

    A behavioral net determination missing a supported population or same-trigger base delta is not a determination. A coverage-gap Critical missing material reachable impact or test-feasibility evidence is not Critical; classify it Moderate, accept it with evidence, or omit it as the admission schema warrants.

    **(b) Classify ledger entries** as:
    - **ADMITTED in-diff** — every applicable admission field is proved and the defect is inside the diff
    - **ADMITTED out-of-diff-breakage** — every applicable field is proved, and an unchanged callsite is broken by a contract this PR changed
    - **OMITTED pre-existing/not-attributed** — base has the same or worse behavior and this PR does not expose a new path
    - **OMITTED false** — counterevidence disproves the proposition
    - **OMITTED unverified** — any required producer, reachability, observation, artifact, or dependency is missing

**Enumerated candidates are admitted per item.** Never sample N instances and publish the unverified remainder. Every rendered item needs its own producer/trigger and evidence; otherwise omit that item.

Keep omitted candidates and their disproofs in the private ledger. Do not publish a Downgraded, retracted, rejected, or "possible issue" section, and do not report candidate counts. **OMITTED pre-existing/not-attributed** is the one exception: an entry whose producer, reachability, and observation are all proved leaves the ledger as a Step 4 adjacent issue draft. **OMITTED false** and **OMITTED unverified** entries never do.

Fresh falsifiers may run in parallel, but each receives only its neutral proposition and raw evidence contract. The parent independently checks every returned admission form before writing Step 4.

## Review checklists

### Correctness & bugs
- `None`/NULL handling at API boundaries
- Edge cases and error paths
- Logic errors, off-by-one, incorrect bounds, wrong operator precedence
- Integer overflow and truncation across the Python↔C boundary (`int` → `int64_t`/`size_t`, `<int>`/`<Py_ssize_t>`/`<size_t>` casts, signed/unsigned, untyped wide literals in typed expressions)
- Float edge cases (NaN, Inf, `-0.0`), timestamp unit and epoch conversions (micros vs nanos vs days; naive vs aware), `NaT`
- Correct ILP wire format (v1 / v2 / v3), QWP framing, UUID byte order, decimal scale and precision, array dimensionality, symbol dictionaries carried across batches
- Arrow offset/validity/stride math for sliced, chunked, dictionary-encoded and large-offset arrays
- **Reachability expansion:** for each changed symbol, list the entry points, transports, interpreters, optional dependencies, finalizer/callback contexts, lock-held states and error paths it can now appear in but didn't before. Verify it works in each.

### Cython memory & refcount safety
- Every `malloc`/`calloc`/`realloc` freed on success, early-return, and exception paths (prefer `try/finally`); no double-free, no use-after-free; the `realloc`-failure path doesn't leak the original
- Every `Py_INCREF` matched by `Py_DECREF`; borrowed references not outliving their owner; weakref and PyCapsule handling correct
- Every `PyObject_GetBuffer` matched by `PyBuffer_Release`; exporter kept alive while the pointer is used; partially-acquired buffer arrays released only where set
- Correct exception convention on every `cdef`/`cpdef`: implicit propagation is the Cython 3 default; an explicit `noexcept` body must be provably unable to raise; `cdef extern` functions never raise
- No handle freed while a pin, lease, or in-flight decode on any thread still references it
- Indexing safe under the active `boundscheck`/`wraparound` directives (2.5f)
- No uninitialized struct or heap memory read (use `calloc` or init before use, especially on partially-built error paths)

### Crash surface
Anything that aborts the native side, corrupts the heap, or misuses the C-API takes the host interpreter down with no Python traceback. The first check is the build profile itself — everything else is downstream of it.

- **Native panic mode** (from Step 2.5f): `panic = "abort"` in `questdb-rs-ffi`; `extern "C"` panics in `rpyutils` abort. No Python `try` catches either. A finding that says "the error is caught in Python" is incorrect when the native side panics
- Any Rust `unwrap`/`expect`/`assert!`/index reachable with arguments the Cython side can actually pass is a crash, not a style nit
- C-API misuse: a C-API call or Python object access without the GIL, unbalanced `PyEval_SaveThread`/`PyEval_RestoreThread`, `PyGILState` imbalance, a callback into a finalizing interpreter
- A `noexcept` function that raises: unraisable warning plus undefined return value — wrong data, a NULL pointer dereference downstream, or a skipped free
- Finalizer hazards: `__dealloc__` touching cleared attributes, `__dealloc__` acquiring a lock the thread holds, a same-thread re-entrant free through an `RLock` during a native decode, a `__del__`-time call into torn-down module globals
- Signal hazards: `KeyboardInterrupt` landing inside a lock body or between a native allocation and its registration for cleanup
- Interpreter-specific C-API: a symbol PyPy's `cpyext` lacks is a wheel build failure; a numpy C-API symbol newer than the floor is an import failure on old numpy
- Stack overflow: unbounded recursion, recursive `__dealloc__`, deeply nested Arrow types decoded from untrusted server input
- SIGPIPE on broken sockets is handled on the Rust side — verify a new native call path does not bypass it

### C-ABI boundary
- `.pxd` declarations match `c-questdb-client/include/questdb/**/*.h` and `rpyutils/include/rpyutils.h` exactly — types, `const`, pointer depth, struct layout, enum order, return type — against the **pinned** submodule commit; these files are hand-written and nothing else checks them
- All pointers returned from C checked for NULL before dereference
- Every `line_sender_error*` / `questdb_error*` / `questdb_conf_str_parse_err*` converted with the right helper and freed exactly once, never double-freed or leaked
- Ownership semantics clear and correct (who allocates the handle, who frees it, what a failed call does to it, lifetime vs the owning `cdef class` and vs any pin or lease)
- `qdb_pystr_buf` arena pointers stay valid until consumed; not invalidated by an intervening `clear`/`truncate`/append
- String handling: `str` → UTF-8 by kind with correct byte length, lone-surrogate rejection, embedded-NUL handling, `bytes`/`str` distinction
- `rpyutils.h` regenerated when `rpyutils/src` changes; `rpyutils.pxd` matches it
- Feature flags in `setup.py` still enable every export the `.pxd` declares; a submodule bump that reorders a struct or renumbers an enum has matching `.pxd` updates

### GIL & concurrency
- No Python object access, C-API call, refcount op, or raise inside a `with nogil` block or `noexcept nogil` function
- GIL released around blocking native calls (connect, flush, query, fetch); the released region references no Python state; errors deferred and raised after reacquiring
- Native-thread callbacks are `noexcept with gil`, consult `qdb_py_is_finalizing()`, never raise, never block on a lock the Python thread holds while waiting on them
- Re-entrant locks: every path that can free a native handle is safe against same-thread re-entry from a GC finalizer or signal handler while a decode holds raw pointers
- `Sender`/`Buffer` single-threaded contract and `QuestDB`/`PooledSender`/`PooledReader` documented thread-safety match the implementation; shared mutable state synchronized
- Assumptions that the GIL serializes access re-checked for the free-threaded build if the module declares (or the PR adds) `freethreading_compatible`
- Interpreter shutdown with live pooled connections, iterators, or background native threads neither hangs nor crashes

### Pooled transports, failover, and error classification
A violation here is **Critical** by definition: the whole point of the pooled QWP transports is that a transient failure never loses data, never poisons healthy connections, and never hard-fails the application.

- **Transient failures recycle; only proven unrecoverability terminates.** For every path that marks a connection, lease, cursor or sender terminal (`_must_close`-style flags, `terminate`, dropping from the pool), verify the trigger is a wire-level or protocol-level fault, not a client-side refusal (unsupported capability, invalid argument, invalid bind, timeout rejected before send) or an ordinary server-side error reply on an otherwise healthy connection. Pool statistics after the operation are the assertion.
- **Error classification is the public contract.** `SenderErrorCategory`/`SenderErrorPolicy`, `QuestDBErrorCode`, `UnsupportedServer` versus `QueryTimeout` versus `ServerRejection`: a change that reclassifies an error changes what user code catches. Verify the mapping against the native error codes at the pinned commit and against the docs.
- **Rejections surface, never vanish.** A rejected batch must reach the user through the rejection handler, the error policy, or an exception; it must never be reported as delivered. A dropped event must be counted (`*_events_dropped`).
- **Cancellation and close are clean.** Cancelling or closing mid-stream stops the iterator, drains or terminates per the protocol, and leaves the pool consistent; `columns()`/metadata after close do not reopen or drop a connection.
- **In-place role changes are first-class.** A node can become a read-only replica while an existing connection stays open. Verify read-only/role-reject responses are classified as server/role outcomes under the public contract, not as protocol violations or unrecoverable pool failures; failover reset callbacks leave the Python-side state consistent.
- **Timeouts bound waits, not durability.** A query or request timeout returns control to the caller; acknowledged ingestion stays acknowledged and healthy connections stay pooled.
- **Replay is at-least-once.** Any oracle or row-count assertion over data produced through a reconnecting sender must account for replay duplicates — await durable acks before the disturbance, or use a DEDUP table — otherwise the test reports replay as corruption.

### Performance & algorithmic fitness
On the per-row buffer build, the per-cell DataFrame loops, the flush path and the per-cell decode path the standard is "use the best known approach", and a violation blocks the merge. Off those paths a *bounded* violation is Moderate, not a blocker. Every finding must say which category it lands in, per the magnitude rule in Step 4.

- Time complexity stated for every new or changed loop; better algorithm named when one exists
- No Python-object operations per row or per cell that belong at C level: attribute/dict lookups, method calls, `isinstance` chains, boxing from untyped literals or `object` intermediates, `str` re-encoding, exception setup
- Multi-pass work that could be fused into a single pass over the column or batch
- Redundant computation: per-row work that could be hoisted to setup; repeated dtype/column lookups; re-planning per batch
- Data structure fitness: linear search where a hash lookup is available; Python containers where typed memoryviews or C arrays suffice; per-row allocation where a reused scratch buffer suffices
- Zero-copy where the buffer protocol or Arrow already exposes the data; no materialization where the iterator path exists
- No GIL acquire/release per row or per cell; no blocking native call under the GIL
- Setup-path costs (sender construction, config parsing, once-per-DataFrame planning) are acceptable; the same work per row is not

### Resource management
- `__cinit__`/`__dealloc__` pair frees everything allocated, and `__dealloc__` is safe after a partially-failed `__cinit__` and after `tp_clear`
- Native handles released exactly once on all paths, including deferred release after a pin or lease ends
- Pooled connections recycled when reusable and dropped only when the native side says they are not
- Arrow `release` callbacks invoked exactly once; PyCapsules consumed once; batch iterators release the previous batch
- Python-level retention: yielded batches and first-row prefetches dropped once consumed; `close()` clears iterator state
- No leak through the C-ABI boundary — ownership documented and consistent in both directions
- Interpreter shutdown releases or detaches background native resources deliberately

### API ergonomics & cross-surface consistency

Mission-critical for a client library: a confusing or inconsistent public API causes data loss, silent misuse, and crashes, so ergonomic and consistency defects are graded by user impact — not automatically filed under Minor.

**Consistency across surfaces (the dominant concern):**
- **Same concept, same shape.** Table name, column name, designated timestamp (`at=`), batch/row limits, column-type overrides, timeouts, error reporting — each must be expressed identically across every method and entry point that touches it: same parameter type, name, keyword-ness, validation timing, defaults. Flag a new method that takes milliseconds where a sibling takes `timedelta`, validates lazily where a sibling validates eagerly, or names/orders a shared parameter differently.
- **Capability parity across entry points.** If one ingestion path (row API, pandas, pyarrow, polars, PyCapsule) or one egress path (`iter_rows`, `to_pandas`, `to_arrow`, `to_polars`, `iter_*`) exposes a control, the peers must expose the equivalent or the PR must state why not. A capability that exists only because of which entry point the user happened to pick is a finding.
- **Keyword arguments ↔ configuration string ↔ environment.** A new knob reachable through `connect()`/`QuestDB(...)` kwargs must have the matching conf-string key and `from_env` behavior, or a stated, documented omission.
- **Naming and verb parity.** The same operation uses the same verb everywhere (`to_*`, `iter_*`, `flush`, `close`, `cancel`). C/Rust names map to Python names per the existing convention.
- **Stub, package, shim, docs.** `_client.pyi`, `__init__.py` `__all__`, the `questdb.ingress` 4.x shim, `docs/api.rst`/`sender.rst`, and `CHANGELOG.rst` all reflect the change; the stub's `Optional`/overload shape matches the real defaults.

**Ergonomics (the easy path must be the safe path):**
- **No hidden mandatory second call.** If correctness requires the user to remember a follow-up (`flush`, `close`, `cancel`, `commit`) to avoid unflushed rows, a leaked connection or a hung query, and a sibling API does it implicitly, the asymmetry is a footgun — rank it by data-loss blast radius, not as a nit.
- **Silent semantic divergence is the worst defect.** Two similarly named/shaped entry points that differ in a behavior the type system does not surface (one commits, one defers; one is idempotent, one is not; one recycles the connection on error, one drops it; one consumes the DataFrame, one copies it) are a trap. Require the difference be made visible in the name or type, or documented loudly at *both* sites with a cross-reference. Never file these as Minor.
- **Accept the types callers already hold.** `str`/`bytes`, `datetime`/`pandas.Timestamp`/`numpy.datetime64`, `int`/`numpy` integers, `decimal.Decimal`, `uuid.UUID` — where the codebase already accepts a type on one path, a new path should too.
- **Positional noise and clunky knobs.** New knobs are keyword-only with safe defaults; sentinel arguments every call must pass are a smell.
- **Errors over crashes, consistently.** Invalid user input raises `QuestDBError` with an actionable message and the right `QuestDBErrorCode` (or the conventional `ValueError`/`TypeError` where the codebase already does), never a segfault, an abort, or an unraisable warning; no second, inconsistent error idiom.
- **Document the contract the signature can't encode.** Any flush/commit/ordering/ownership/durability/thread-safety semantic not expressible in the type must be documented on the public item, cross-referencing any sibling that behaves differently.

### Code quality
- Backward-compatible changes to the public Python API and the `questdb.ingress` shim, or breaking changes intentional, listed under "Breaking changes" in `CHANGELOG.rst`, and called out in the PR body
- Naming conventions consistent with the existing codebase
- No dead code or unused `import`/`cimport`
- Comments explain *why*, not *what* — a comment that restates the code, or a multi-line explanation of a one-line change, is a finding
- Docstrings on public classes and methods; `proj.py doc` builds cleanly under `-nW` (unknown roles, broken cross-references and spelling failures are errors)
- No new runtime dependency in `pyproject.toml` without an explicit rationale in the PR body; optional dependencies (pandas, pyarrow, polars) stay optional and fail with the existing actionable message when absent

### Test review
- **Coverage gaps are impact- and proportionality-assessed:** consume the Step 2.6 map. Missing tests alone are not blocking. For every uncovered path, establish user impact, change risk, existing safeguards, and the least fragile meaningful test before choosing Critical, Moderate, accepted, or exempt.
- **Test tier matters:** state whether the cited coverage runs mock-only, in PR CI against the released server, only on `linux-qdb-master`, or only locally — and whether its skip gate fires on the released-server leg. A capability-gated change whose only test skips everywhere a PR runs is a weaker position than the map's `COVERED` label suggests.
- **Cross-surface dimensions:** where the changed code is sensitive to them, demand coverage across the modes that alter its behavior — ILP/HTTP vs ILP/TCP vs QWP/WebSocket vs QWP/UDP, TLS vs plaintext, auth vs no auth, protocol v1 vs v2 vs v3, pandas 2 vs 3, numpy 1.x vs 2.x, pyarrow present vs absent, polars present vs absent, CPython vs PyPy vs free-threaded, row API vs DataFrame vs PyCapsule, `iter_rows` vs `to_pandas` vs `to_arrow` vs `to_polars`. A change tested on one mode is a gap on the others — name the untested ones.
- **Fuzz coverage:** for planner, encoder/decoder, Arrow or wire-format changes, check the seeded fuzzers (`test_client_dataframe_fuzz.py`, `test_client_polars_fuzz.py`, the `QDB_PY_QWP_FUZZ_*` loops in `system_test.py`, the atheris harness `test_dataframe_fuzz.py`). If one covers the changed surface and was neither extended nor mentioned as run against the change, add an `UNTESTED` Step 2.6 row.
- **Leak coverage:** new native-memory, buffer-protocol, Arrow-release or refcount handling has a test in `test_dataframe_leaks.py` or `system_test.TestEgressLeaks` (or an equivalent measured loop).
- **Cross-context coverage:** for every entry in 2.5d, verify a test exercises the changed symbol from that context. Record each miss as an `UNTESTED` row.
- **Error path coverage:** failure cases, partial writes, connection drops, TLS failures, auth failures, server rejections, role rejects, unsupported-capability refusals, timeouts, cancellation mid-stream — not just the happy path.
- **Pool and lifecycle coverage:** for pooled-transport changes, tests assert pool statistics after the operation, that a healthy connection is recycled, and that a terminal one is dropped exactly once.
- **Finalizer and signal coverage:** for lock, pin, or free-path changes, a test that forces GC at the decode site (a cycle-only object with `__del__`, `gc.collect()` from inside a value conversion) or raises a signal inside the lock body.
- **`None`/edge-case tests:** `None` cells and columns, empty DataFrames, zero-length strings and arrays, max-length symbols, boundary integers, NaN/Inf, naive and aware datetimes, non-UTF-8 and astral strings.
- **Binding and stub coverage:** `.pxd` changes covered by a live or mock test that calls the declaration; `.pyi` changes consistent with `TestIngressShim`-style import surface tests where applicable.
- **Test quality:** tests actually assert the right thing — watch for tests that pass trivially, assert on a mock's canned reply instead of the bytes sent, or assert on wrong values.
- **Regression tests:** a bug fix needs a test that reproduces the original bug and fails without the fix.

### Test code quality
- **No vacuous assertions.** Every assertion must be able to fail.
- **Reuse before reinventing.** Search for existing fixtures, mock servers, gates and helpers before accepting inline setup; name the helper when one exists.
- **No doc bloat.** Prefer a precise test name and at most a one-line comment over multi-paragraph commentary on a test.
- **No flakiness sources.** Hard-coded ports, unbounded sleeps, sleep-as-synchronization, unguarded server-version dependence, and unmocked network dependence are findings.
- **Reproducibility.** Fuzz and randomized tests print or accept their seed; environment variables they set are restored.
- **Test-appropriate standards.** Bare `assert` and broad `except` in test helpers are fine. Naming conventions still apply.
- **No debugging residue.** No leftover `print()`, no commented-out code, no `skip` without a referenced issue or capability reason.

### Unresolved TODOs and FIXMEs
- Scan the diff for `TODO`, `FIXME`, `HACK`, `XXX`, and `WORKAROUND` comments. For each:
  - Is it pre-existing and merely moved/reformatted, or newly introduced in this PR?
  - If newly introduced: does it represent unfinished work that should block the merge, or a known limitation acceptable to ship? Flag anything that looks like a deferred bug or an incomplete implementation.
  - If it references a ticket/issue number, verify the reference exists.

### Commit messages
- Conventional Commits prefix matching the repository's prevailing usage (`fix:`, `feat:`, `chore:`, `ci:`, `docs:`, optionally scoped like `build(arrow):`)
- Subject line short and in plain English after the prefix; active voice, naming the acting subject
- Body explains the *why* and wraps at a readable width; the rationale for a non-obvious one-liner belongs here, not in an inline comment

## Step 4: Output

In PR mode, revalidate the snapshot immediately before rendering any verdict:

```bash
if ! FINAL_IDENTITY=$(gh pr view "$PR" --json baseRefOid,headRefOid --jq '[.baseRefOid, .headRefOid] | @tsv') ||
   [ "$FINAL_IDENTITY" != "$IDENTITY" ]; then
  echo "PR #$PR changed during review. Discard the draft report and restart."
  exit 1
fi
```

Begin the final report with the exact immutable identity line required under **Review level**. Copy the full `$HEAD` and `$BASE` values captured in Step 1; do not reconstruct or shorten them.

Present only **ADMITTED** findings. Omitted candidates, disproofs, retractions, agent counts, candidate counts, and the private ledger never appear in the public review. Do not publish a hypothesis and retract it later; finish falsification first. It is valid to report no findings. The single exception is the **Adjacent findings** section below, which carries proved pre-existing bugs as issue drafts — not findings against this PR, and weightless in every gate.

**Proportionality.** Keep the report actionable in one sitting. If a normal-sized PR yields more than about seven total findings, re-run the admission gate on every item and remove dependent, duplicate, not-attributed, and low-value prose. Removing a not-attributed item means moving it to Adjacent findings, not discarding it. Review depth is demonstrated by evidence, not report length.

**Every finding — at every severity — opens with three one-line summaries, before any prose:**

- **Problem:** what is wrong. ≤ 12 words. No mechanism or fix.
- **Net impact:** supported population and magnitude. ≤ 12 words. A behavioral item with no net regression is omitted.
- **Evidence:** the decisive artifact or static proof, including the reviewed revision identity.

Write these lines last from the completed admission form, never first from a hunch. Then give only the minimal producer → path → symptom trace, base comparison, and suggested fix.

```
Problem: Lease finaliser frees the cursor while a batch is still decoding.
Net impact: Any to_pandas() caller whose lease is GC-collected mid-decode segfaults.
Evidence: gc.collect() hook at the decode site aborts with SIGSEGV at abc123; raises cleanly at base def456.

Problem: Timestamp decode boxes every cell through a Python int.
Net impact: ~3x slower iter_rows() on timestamp columns, per cell.
Evidence: benchmark_pandas_egress.py at abc123: 1.9s vs 0.6s at base def456 for 1M rows.
```

Structure as:

### Severity classification (impact-first — severity is the user's consequence, not the finding's category)

Severity is a function of **what the user loses**, not of which checklist the finding came from. Classify by the worst *user-visible* consequence on a *reachable* path. Do not classify up "to be safe": an inflated Critical costs exactly what a real one costs and teaches the author to skim the report.

**"The user" means an application developer or operator consuming this client library** — someone ingesting or querying data from Python, or running a service that embeds it. It does **not** mean a QuestDB client-library developer, a CI job, or the release process. A finding whose only affected population is the team — a slower build, a broken local setup, an awkward merge — is never Critical, whatever its symptom. Developer-experience problems are Moderate at most, and most are Minor.

**The Critical test — name the symptom.** A finding is Critical only if you can complete this sentence with something a user or operator would actually observe: *"Because of this, the user sees ___."* The valid completions are:

- **wrong, lost, or duplicated data** — rows silently dropped, unflushed or unacknowledged rows reported as sent, malformed ILP/QWP accepted by the server as different values, wrong timestamp units, reversed UUID bytes, wrong decimal scale, wrong column types on the server, a result column decoded to the wrong value, replay that duplicates without dedup;
- **a crash, hang, or unavailability** — a segfault or abort taking down the interpreter, a Rust panic, undefined behavior, use-after-free, double-free, a GIL deadlock, a hang at shutdown, an unbounded loop, an OOM, fd/thread/connection exhaustion, or a native or Python leak that grows without bound under a repeatable operation;
- **a security failure** — TLS verification bypassed or weakened, credentials logged or leaked, an auth token sent to the wrong peer, a trust-store regression;
- **a broken or misleading failure mode** — an operation that fails with no error or the wrong error, an exception swallowed by a `noexcept` frame so a failed write looks like a success, an error a user cannot act on, a healthy pooled connection dropped so the next call silently reconnects and replays;
- **a compatibility break** — public Python API, the `questdb.ingress` shim, configuration-string semantics, wire protocol, a supported interpreter or dependency floor (PyPy, free-threaded, numpy 1.21, Python 3.10), or a `.pxd`/header ABI mismatch, changed so existing applications or a rolling upgrade break;
- **a performance or IO regression the user can feel** — per the magnitude rule below;
- **an admitted Critical coverage gap** — the changed path is supported and reachable, a named population can execute it, a credible regression would produce one of the material consequences above, existing controls do not contain it, and Step 2.6 shows why stable coverage is warranted.

**Every completion needs a trigger.** A symptom sentence must name the concrete call sequence, keyword arguments or configuration string, DataFrame or result shape, interpreter, server version, or server response a user can produce: *"user does X → sees Y"*. For a coverage gap use *"user does X; if this changed path regressed as Y, the user would see Z"*. "Could theoretically corrupt the buffer" is not evidence.

If a behavioral candidate cannot name and execute a supported trigger with one of the consequences above, omit it; do not preserve the mechanism as Moderate. Concrete static standards, maintainability, and coverage findings may still be Moderate or Minor when fully established directly from changed source.

**Magnitude rule for performance and IO.** Cost blocks only when it is user-observable. Ask two questions: does the cost **scale with data** (per row, per cell, per byte, per batch), and is it on a path the user **waits for or repeats**?

- **Critical:** per-row/per-cell/per-byte work on the buffer-build, DataFrame-ingest, flush, or decode path; an added copy of a column or payload; an added pass over the batch; a Python-object operation per cell; an allocation per row; O(n²) in row or column count — anything that measurably moves ingestion throughput, query latency, or bytes on the wire.
- **Moderate:** a bounded, non-scaling cost off the data path — extra work during sender construction, config parsing, pool setup, or once-per-DataFrame planning; one extra allocation per flush or per batch (not per row); a linear scan over a small fixed set (column count, config keys). Worth reporting and worth fixing; not worth blocking a merge. **Sub-optimal but bounded is Moderate**, even when a better algorithm plainly exists — name the better algorithm and state the bound that makes it non-blocking.
- To file a performance finding as Critical you must state the magnitude: the multiplier and what it multiplies ("one Python int per timestamp cell", "a second full pass over every column before every flush", "O(n²) in symbol cardinality").

**Config-divergence rule.** "The same call succeeds over HTTP and fails over WebSocket", or "works with pyarrow installed and not without", or "works on CPython and not on PyPy", is a finding in its own right — an inconsistency a user can observe across transports, dependencies, or interpreters — and is classified on the consequence of the divergence itself. It does not inherit the severity of the worst case reachable through the more permissive configuration; that worst case is a separate finding that must pass the symptom test, the trigger requirement, and the base-behavior check on its own.

**Out of scope — these are not findings.** Four classes get reported constantly and are worth nothing. Drop them before they reach the report:

- **Merge mechanics.** The reviewed artifact is the code change, not the merge event. Submodule pin position, `tandem` pairing and merge order between the two repositories, branch existence, labels, and anything true only of the PR's in-flight state are properties of *how it lands*, not of *what it does*. A `tandem` PR necessarily pins the native commit it was built against — that is a property of the pointer, not a defect; the release process owns moving it.
- **Tautologies.** Before filing, ask: *"would this finding appear on every PR of this shape?"* If yes, it describes the workflow, not this change.
- **Overridden project decisions.** When the project's own tooling or documented convention explicitly permits something — a CI check that passes by design, a documented limitation, a convention the PR body already names — that is a decision, not an oversight. "CI allows this but I would not" is not a finding.
- **Upstream submodule content.** Anything inside `c-questdb-client` whose pointer move Step 2.4 classified **UPSTREAM-SYNC**, and the Rust content of an **OFF-DEFAULT** pin, which the native repository's own review owns. This PR did not write it and cannot be asked to fix it. The one exception is an integration defect at a callsite or declaration *inside this repository*, filed against that file with its own symptom and trigger.

**Moderate.** Admitted, attributable defects with bounded or developer-facing impact: a concrete changed-line standards violation, a proved weak test, missing internal-path coverage, a documentation or stub defect with no runtime consequence, an API inconsistency with no data-loss path, or a bounded off-data-path cost. An unreachable runtime theory, unchanged residual hardening opportunity, or proposition that only supports another candidate is not Moderate; omit it.

**Minor.** Cosmetics: naming, formatting, comment wording, import order, member ordering.

Do not inflate and do not deflate. Filing a real user-visible defect as Moderate is a review failure; so is filing a bounded setup-path micro-cost as Critical. Where two readings are defensible, pick the one you can evidence.

### Critical
Blocking issues introduced or exposed by this PR, ordered worst user impact first. Each must include:
- The three summary lines (**Problem** / **Net impact** / **Evidence**) before anything else
- The **net determination** from 3b.20(a): population, delta vs base, magnitude/frequency, offsets, and a net of **net-negative** — a Critical that is net-neutral or net-positive is mis-filed by definition
- Exact file path and line numbers (in `.pyx`/`.pxi`/`.pxd`/`.pyi`, never `_client.c`)
- The **symptom sentence** with its supported trigger: "user does X → sees Y". For a coverage-gap Critical: "user does X; if this changed path regressed as Y, the user would see Z"
- For a coverage-gap Critical: the credible mutation/recurrence, existing safeguards and offsets, change-risk assessment, least-fragile stable test considered, and concrete evidence that cheaper alternatives are inadequate
- Whether the finding is **in-diff** or **out-of-diff-breakage** — both are this PR's responsibility
- Code path trace showing why the bug is real and reachable, and under which build-profile facts (2.5f) if the consequence is a crash, a swallowed exception, or an ABI mismatch
- **Base behavior for the identical trigger** (required): executed at `$BASE`. If base shows the same or worse user-visible outcome, omit the candidate as not attributed to this PR. For a genuinely new surface, write `N/A — new surface` and prove base cannot express the trigger
- For out-of-diff-breakage: the callsite that triggers it, plus the violated contract — cite it from 2.5c at level 2+, or state it inline at levels 0-1
- For performance findings: the magnitude statement (the multiplier and what it multiplies)
- Suggested fix, written to be applied in THIS PR, including any `.pxd` / `.pyi` / `__init__.py` / shim / docs / changelog update the fix implies

Pre-existing/not-attributed observations are never Critical; a fully proved one belongs under Adjacent findings instead.

### Moderate
Non-blocking admitted issues worth fixing. Every item must still include the three summary lines and its decisive evidence. Dynamic behavioral speculation is not allowed here.

### Minor
Concrete cosmetics on changed lines. Non-blocking, optional.

### Adjacent findings (not blocking — file as GitHub issues)

Bugs that already exist on the merge base, found in code this review visited (changed files, callers from the callsite inventory, cross-context exposures), which this PR does not introduce, break, or worsen. They are **not findings against this PR**: they never appear under Critical/Moderate/Minor, never influence the verdict, and are never proposed as changes to this PR. Discarding them instead is pure waste — the investigation is already paid for, and nobody re-finds them later.

They are held to the same evidence bar as a published finding. An adjacent draft comes only from a candidate that reached **OMITTED pre-existing/not-attributed** with its producer, reachability, and observation proved. A candidate that ended **OMITTED false** or **OMITTED unverified** stays in the private ledger.

Report each as a ready-to-file issue draft:

- **Problem:** ≤ 12 words — doubles as the issue title
- **Net impact:** ≤ 12 words — population and magnitude, or "None — <reason>"
- **Location:** file path + line numbers
- **Symptom:** what a user would observe — or "latent: no user-visible impact today", naming the guard that prevents it
- **Reachability:** the path that reaches it, or why nothing does yet
- **Suggested fix:** one or two lines
- **Severity if filed standalone:** Critical / Moderate / Minor per the rubric above
- **Repository:** this repository, or `c-questdb-client` when the defect lives in the native layer

Offer to file them; do not file anything without being asked. Their count and severity sit outside the finding-proportionality budget and outside every gate in the Summary. If one is severe enough that shipping this PR without it is genuinely unsafe — because this PR moves code onto a path where the pre-existing bug now fires — then it is not adjacent: it is out-of-diff-breakage, it belongs under Critical, and you state that argument explicitly.

### Coverage map
State the test-gate result and the number of **admitted** coverage gaps only. Render admitted gap rows with their recorded search, failure link, and test tier (including whether the skip gate fires on the released-server leg). Do not expose counts for omitted candidates or private UNTESTED rows; keep the full Step 2.6 matrix private unless the user asks to see it.

### Summary
- **Verdict**, exactly one of:
  - **approve** — no open Critical findings and the test gate passes. Moderate and Minor items may remain open; list them and approve anyway. This is the expected outcome for competent work, and withholding it when both gates pass is itself a review failure.
  - **approve with comments** — both gates pass; you want specific Moderate items addressed but will not block on them. Name which ones.
  - **request changes** — at least one Critical is open, or the test gate fails.
  - **needs discussion** — the change requires a product, architecture, or compatibility decision a reviewer cannot make alone.
- **Correctness gate (hard rule):** the verdict cannot be "approve" while any **ADMITTED** Critical finding remains open, including an admitted Critical coverage gap. Omitted hypotheses never affect the verdict.

  Before finalizing, rerun the admission audit from evidence fields rather than from report prose:
  - **falsification:** state the strongest attempted disproof for each rendered behavioral finding;
  - **producer:** confirm a supported call sequence/configuration/interpreter/server state actually creates every trigger state;
  - **independence:** confirm the admitting falsifier did not receive the discovery narrative, severity, fix, or votes;
  - **dynamic evidence:** confirm races, finalizer timing, signals, reconnects, pool states, leaks, and wire-compatibility claims have an executed artifact at the reviewed revision and the same-trigger base result;
  - **build profile:** confirm every crash, swallowed-exception, or ABI finding was evaluated under the 2.5f facts, not the textbook ones;
  - **dependency:** remove every item whose parent premise was omitted;
  - **severity:** classify only after admission. Never promote missing evidence or uncertainty to Critical.

  If any field fails, omit the candidate and rerun the verdict. If the admitted Critical list is empty and the test gate passes, approve plainly; zero findings is expected for correct changes.
- **Test gate (hard rule):** the gate fails only while an **ADMITTED Critical coverage gap** remains open. Zero test changes, a bug-fix label, or missing regression coverage triggers the Step 2.6 analysis but never automatically forces `request changes`. Moderate gaps may accompany `approve with comments`; accepted gaps do not affect the verdict. Any independently admitted functional Critical still fails the correctness gate regardless of test effort or urgency.
- State the test-gate result and admitted coverage-gap count. Do not publish total UNTESTED or omitted-candidate counts.
- State the Step 2.5f build-profile facts in one line — Cython directive defaults, exception-convention default, native panic mode, pinned submodule commit — since every crash-related conclusion depends on them.
- State the Step 2.4 submodule provenance verdict if the pointer moved (e.g., "c-questdb-client: UPSTREAM-SYNC — out of scope" or "c-questdb-client: OFF-DEFAULT (`tandem`) — Rust reviewed upstream, integration reviewed here"). If a pointer moved and no verdict is stated, the scope of the review is unknown and the report is incomplete.
- If the change is capability-gated or server-version-dependent, state whether the released-server PR legs exercise it or only `linux-qdb-master` does, and whether that leg's result was available.
- If the change touches the C-API surface or `setup.py`, state whether the PyPy and free-threaded wheel legs were available as evidence.
- Highlight any regressions or tradeoffs.
- State only the admitted split: in-diff / out-of-diff-breakage. At levels 0-1, describe the limited callsite analysis rather than implying a clean bill of health.
- State the severity distribution. If the report is long or severity-heavy, re-run admission; do not compensate by preserving weak items at a lower severity.
- Never make the verdict conditional on splitting the PR. Pre-existing and not-attributed observations never affect the verdict.
- Do **not** state agent counts, candidate counts, rejected/false-positive counts, or retraction history.
