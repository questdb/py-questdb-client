---
name: review-pr
description: Review a GitHub pull request or local Git range against py-questdb-client (Cython + C-ABI) coding standards
argument-hint: "[PR number or URL | --range=<base>..<head>] [--level=0..3]"
allowed-tools: Bash, Read, Grep, Glob, Agent
---

Review the target in `$ARGUMENTS`. Parse exactly one PR number/URL or
`--range=<base>..<head>` target. An omitted range head reviews the working tree,
including uncommitted changes. Ask for a target when none is supplied and reject
ambiguous invocations containing both forms. Use Bash for read-only Git/GitHub
queries and test execution (`python3 proj.py build`, `python3 proj.py test`,
`python3 proj.py test all`, `python3 proj.py valgrind_test`); do not edit files,
commit, or push. Reproducing at the base revision happens in a throwaway
worktree, never by checking the review tree out (Step 3b).

## Repository map

The extension module is `questdb._client`. Establish the source of truth before
reviewing anything:

- **Cython sources:** `src/questdb/_client.pyx`, with `dataframe.pxi` and `egress.pxi` textually `include`d into it, plus `_client_helper.inc`
- **C-ABI and helper declarations:** `src/questdb/line_sender.pxd`, `conf_str.pxd`, `arrow_c_data_interface.pxd`, `mpdecimal_compat.pxd`, `rpyutils.pxd`, `_client_helper.pxd`, `extra_cpython.pxd`, `extra_numpy.pxd`
- **Type stub:** `src/questdb/_client.pyi`
- **Public Python surface:** `src/questdb/__init__.py` (`__all__`, `connect()`) and `src/questdb/ingress.py` (deprecated 4.x import shim)
- **Pinned C headers:** `c-questdb-client/include/questdb/ingress/line_sender.h` and `qwp_sender.h` (submodule); `rpyutils/include/rpyutils.h` (generated from `rpyutils/cbindgen.toml`); local `src/questdb/conf_str.h`, `arrow_c_data_interface.h`, `mpdecimal_compat.h`
- **Rust helper crate:** `rpyutils/src/` (`lib.rs`, `active_senders.rs`, `pystr_to_utf8.rs`, `mpd_to_bigendian.rs`)
- **Never review generated or build artifacts.** `src/questdb/_client.c`, `*.html` (Cython annotation), and `*.so` are build outputs. If the diff contains a regenerated `_client.c`, review the `.pyx`/`.pxi` change that produced it, not the generated C.

## Review mindset

You are a senior QuestDB engineer performing a blocking code review. `py-questdb-client` is mission-critical software: a **Cython** extension that wraps the **`c-questdb-client` (Rust) library** through its **C ABI**, and is used to ingest production data from customer Python applications. A bug here causes data loss, silent data corruption, segfaults that take down the host Python interpreter, reference-count leaks, or native memory leaks. There is zero tolerance for correctness issues, memory unsafety, refcount imbalance, GIL violations, or an FFI binding that disagrees with the C header it calls. Be critical, thorough, and opinionated. Your job is to catch problems before they ship, not to be nice.

**A review that blocks on everything blocks on nothing.** Report every verified
issue, but reserve blocking severity for reachable defects with material user
impact. Approval is the expected result when the correctness and test gates pass.

- **Assume nothing is correct until you've verified it.** Read surrounding code to understand context — don't just look at the diff in isolation.
- **The diff is a hint, not the boundary of the review.** The highest-value bugs almost always live at callsites outside the diff that depend on contracts the diff quietly changed (a `cdef` helper's error-return convention, a buffer's ownership, a `qdb_pystr_buf` arena's lifetime). Treat the diff as the entry point, not the scope.
- **Discovery is not a finding.** Treat every agent concern as an untrusted hypothesis until Step 3b establishes attribution, reachability, and evidence. Agent agreement is not proof.
- **Falsify before explaining.** Search for guards, validation, early returns, `try/finally` blocks, callers that cannot supply the offending input, and identical merge-base behavior before writing report prose.
- **Keep the PR blast radius small.** Do not attribute pre-existing behavior or residual hardening opportunities to this PR unless the change demonstrably exposes or worsens them.
- **Flag every issue you find**, no matter how small. Do not soften language or hedge. Say "this is wrong" not "this might be an issue".
- **Do not praise the code.** Skip "looks good", "nice work", "clever approach". Focus entirely on problems and risks.
- **Think adversarially.** For each change, work through:
  - Inputs: which values break this? Empty buffers, zero-length strings, `None`, NaN/inf floats, boundary integers (`INT64_MAX`/`INT64_MIN`), max-length symbols, non-UTF-8 `str`, `bytes` with embedded NULs, huge `int` that overflows `int64_t`.
  - Encoding: how does the code behave when a Python `str` contains lone surrogates, astral codepoints, or characters that fail UTF-8 encoding?
  - Memory: every `malloc`/`calloc`/`realloc` — is it freed on the error path, the exception path, and the early-return path? Every `Py_INCREF` — is there a matching `Py_DECREF`? Every `PyObject_GetBuffer` — a matching `PyBuffer_Release`?
  - GIL: does a `with nogil` block touch a Python object or call a CPython API function? Does a `cdef ... nogil` function need the GIL it doesn't hold?
  - Failure modes: connection dropping mid-flush, partial write, TLS handshake failure, auth rejection, server rejection — does the buffer/sender end in a usable state, and does native memory get released?
  - C-ABI callers: what happens when a C function returns `NULL`, returns an error via its out-param, or hands back a pointer the Cython side must free exactly once?
- **Check what's missing**, not just what's there. Missing tests, missing error handling, missing edge cases, missing `_client.pyi` stub updates for public API changes, `.pxd` declarations out of sync with the C header.
- **Untested behavior is a coverage risk, not proof of a functional defect.** Classify a gap by reachable impact and change risk; missing tests alone do not make it Critical.
- **Treat public API ergonomics and cross-surface consistency as correctness, not polish.** This package is the ingestion and query API customer production code is written against, and it now exposes several parallel surfaces: `questdb.connect()` / `QuestDB` (pooled, QWP/WebSocket), the standalone `Sender`, `Buffer` row building, `dataframe()` bulk load, and the deprecated `questdb.ingress` shim. An inconsistent, surprising, or footgun-prone public surface leads users into data loss and silent misuse just as surely as a logic bug. Every public symbol the PR adds or changes must (a) express each shared concept — table/column names, designated timestamp (the `at` argument and `ServerTimestamp`), auto-flush controls, transactional flush, acknowledgement/wait semantics, protocol version, column-type overrides, buffer ownership — the *same way* every sibling surface already does, and (b) make the easy path the safe path. An ergonomic inconsistency that can cause data loss or silent misuse is a blocking finding, not a Minor nit.
- **Buffered rows must not vanish silently.** Any PR touching pooled sender lease/return, `close`/drain, auto-flush triggers, reconnect, acknowledgement waits, or server-error classification must preserve this invariant: rows the user has handed to the client are either published, or their loss is surfaced to the user through an exception, the `error_handler`, or the `questdb` logger. A bounded wait may return control to the caller, but discarding queued rows without surfacing anything is data loss. Auto-triggered publishes that do not wait for acknowledgement are by design — verify the change does not extend that silence to explicit `flush(wait=True)` / `wait()` barriers.
- **Verify every claim.** If the PR title says "fix", verify the bug actually existed and the fix is correct. If it says "improve performance", look for benchmarks or reason about the change against the per-row hot path. If it says "simplify", verify the new code is actually simpler and doesn't drop behavior (e.g. a dropped `free` on an error branch). Treat the PR description as an unverified hypothesis.
- **Read the full context of changed files** when the diff alone is ambiguous. Use Read/Grep/Glob to inspect surrounding code, callers, and related tests.
- **Assess reachability before reporting.** For every potential bug, trace the actual callers and inputs. If a problem requires physically impossible conditions (a length larger than `SIZE_MAX`, a NUL injected through an API that already rejects it, a panic behind a validation guard), it is not a real finding — drop it. Focus on bugs that real workloads can trigger, not theoretical edge cases.

## Review level

Parse `$ARGUMENTS` for a level token: `--level=N`, `-lN`, or a bare single digit `0`-`3`. **If no level is given, default to 0.** Strip the level and range tokens before feeding a PR target to `gh` commands.

The level controls how much of the review below actually runs. Lower levels keep the same review *spirit* — adversarial, blocking, no praise — but cut the breadth of the analysis. Higher levels have significantly higher token cost; reserve level 3 for high-stakes PRs (C-ABI `.pxd` changes, a `c-questdb-client` submodule bump, the dataframe/Arrow ingestion path, `nogil` sections, manual `malloc`/refcount code, the pool/QWP lifecycle, ILP wire format, or auth/TLS configuration).

| Level | What runs |
|-------|-----------|
| **0 (default)** | Steps 1, 2, 2.4, 2.5e, 2.6, 4. Skip the rest of Step 2.5 and the agent fanout. Review inline, applying the Step 3b admission rules from a blank evidence record before writing each finding. |
| **1** | Adds Step 2.5a (semantic delta only — skip 2.5b/2.5c/2.5d). In Step 3, launch only Agent 1 (correctness), Agent 2 (Cython memory & refcount safety), Agent 7 (tests), and Agent 8 (public API ergonomics & cross-surface consistency) in parallel. Skip all other agents. Apply Step 3b inline to their candidates. |
| **2** | Full Step 2.5, but in 2.5b restrict the callsite inventory to public Python symbols (exported in `__all__` / `_client.pyi`) plus every `cdef`/`cpdef` function and every C-ABI symbol declared in the `.pxd` files. In Step 3, launch Agents 1-8. Skip Agent 9 (cross-context) and Agent 10 (adversarial fresh-context). Step 3b uses a single batched verification agent for all candidates instead of one per candidate. |
| **3** | Every step below as written, all 10 agents, per-candidate verification. The full mission-critical pass. |

State the chosen level in one line at the start of the review so the user knows what they're getting (e.g., "Reviewing PR #141 at level 2"). If the level was defaulted, mention that level 3 exists for full review.

## Step 1: Gather PR context

Every mode must establish `$BASE`, the revision that attribution is measured
against: the **merge base** of the target and the branch it lands on, not that
branch's current tip. Once the tip has advanced past the branch point, a
tip-relative comparison pulls unrelated commits into the review — inventing
regressions and masking real ones — and disagrees with `gh pr diff`, which is
itself merge-base-relative. A behavioral finding without an identical-trigger
base comparison is not attributable to the change.

For a PR, capture `$PR` after stripping the level token, then fetch context and
resolve both revisions in a single bash call so `$PR` is in scope throughout.
The merge base needs both commits in the local object store, so fetch the head
by its `pull/N/head` ref — a head that lives on a fork is otherwise absent. Name
whichever remote hosts the PR; `origin` below assumes the common case:

```bash
PR='<PR number or URL from $ARGUMENTS, with any --level=N / -lN / bare-digit level token removed>'
gh pr view "$PR" --json number,title,body,labels,state
gh pr diff "$PR"
gh pr view "$PR" --comments
PR_NUM=$(gh pr view "$PR" --json number --jq .number)
BASE_REF=$(gh pr view "$PR" --json baseRefName --jq .baseRefName)
HEAD=$(gh pr view "$PR" --json headRefOid --jq .headRefOid)
git fetch -q origin "pull/$PR_NUM/head"
git fetch -q origin "$BASE_REF"
BASE=$(git merge-base "$HEAD" "origin/$BASE_REF")
echo "BASE=$BASE"; echo "HEAD=$HEAD"
```

For `--range=<base>..<head>`, resolve the same way — `BASE=$(git merge-base <base> <head>)`
— then review `git diff "$BASE" "<head>"`. The `..` in the argument names the two
endpoints; the comparison is always against their merge base. With an omitted
head the head is the working tree: resolve `BASE=$(git merge-base <base> HEAD)`,
then inspect `git diff "$BASE"`, `git status --porcelain`, and relevant untracked
files because they do not appear in the diff.

**Write the resolved SHAs down.** Each Bash call runs in a fresh shell, so the
variables assigned above are gone by the next call. Record the literal `$BASE`
and `$HEAD` SHAs next to the level line and use those literals from Step 2.4
onward — Step 3b's base comparison depends on them.

If the diff modifies the `c-questdb-client` submodule pointer or any `.pxd` file, note it now — a submodule bump or binding change is the highest-risk class of change in this repo and forces level-3 scrutiny of the C-ABI surface regardless of the requested level.

## Step 2: PR title and description

Skip this step in local-range mode and state that no PR metadata exists.

Check:
- Title is clear and describes the change
- Description speaks to end-user impact, not implementation internals
- If fixing an issue, `Fixes #NNN` or a link to the issue is present
- Tone is level-headed and analytical
- For public API changes (anything in `__all__`, a new/changed method on `QuestDB`/`Sender`/`Buffer`/`PooledSender`/`PooledReader`, a new keyword argument, or a changed default), the description calls out the API change explicitly, and `CHANGELOG.rst` is updated
- For a `c-questdb-client` submodule bump, the description states which upstream change is being pulled in and why

## Step 2.4: Submodule provenance

The repo has one submodule, `c-questdb-client`. If its pointer moved, determine
whether the new commit is already on the submodule's default branch. `origin/HEAD`
tracks that branch, and ancestry against it is the exact question. The submodule
carries many long-lived topic branches, so a commit contained by *some* remote
branch is not thereby on the default one — test ancestry directly:

```bash
git -C c-questdb-client fetch -q origin
git -C c-questdb-client merge-base --is-ancestor <new-commit> origin/HEAD; echo "exit=$?"
```

Exit 0 means the commit is on the default branch, exit 1 means it is not, and any
other status is `UNRESOLVED` — including the exit 129 / `error: no such commit`
that a commit which never reached `origin` produces.

Classify the move as:

- **`UPSTREAM-SYNC`** — the commit is on the submodule's default branch. Its contents are out of scope; review only the integration in this diff (`.pxd` ↔ header agreement, changed enum/struct layouts, new or removed symbols, wire-format implications).
- **`OFF-DEFAULT`** — the commit is not on the default branch. Its contents are part of this logical change and must be reviewed as such.
- **`UNRESOLVED`** — provenance could not be established. Treat as `OFF-DEFAULT` and disclose the missing provenance.

Record the verdict for the final summary. Either way, the pinned commit's headers under `c-questdb-client/include/questdb/ingress/` become the **new** source of truth that every `.pxd` must match.

## Step 2.5: Map the change surface

Before launching review agents, produce a structured change surface map. This step is mandatory and must use Grep/Glob — do not reason about callsites from memory. The output of this step is required input for every agent in Step 3.

### 2.5a Semantic delta per changed symbol

For every modified or added function (`def`, `cdef`, `cpdef`), method, class, `cdef class` attribute, module-level constant, enum member, or C-ABI declaration in a `.pxd`, write:

- **Symbol:** fully-qualified name (e.g., `questdb._client.Buffer.row`, `Buffer._column_f64`, `_dataframe`, `c_err_to_py`, `line_sender_buffer_column_f64`)
- **Before:** signature, return type, **Cython exception convention** (`except -1` / `except *` / `except? -1` / `except +` / none / `noexcept`), what it raises and on which inputs, `nogil`-ness, whether it touches Python objects, allocation behavior (`malloc`/`calloc`/`realloc`), refcount effect (does it steal/borrow/own a reference?), C-ABI ownership semantics (who frees returned pointers), thread-safety
- **After:** same fields
- **Delta:** one line stating what semantically changed

"Refactored", "cleaned up", "improved", "simplified" are not acceptable deltas. State the actual behavioral difference. If nothing semantically changed, write "no behavioral change" — but only after checking, not as a default.

### 2.5b Callsite inventory

For every changed symbol that is public (in `__all__` / `_client.pyi`), `cdef`/`cpdef`, declared in a `.pxd`, or a C-ABI function, run Grep across the repository to find every callsite, override, or reference outside the diff.

Produce a list grouped by file. Search at minimum:

- **Cython implementation & includes:** `grep -rn 'symbol_name' src/questdb/_client.pyx src/questdb/*.pxi src/questdb/_client_helper.inc`
- **Cython C-ABI / helper declarations:** `grep -rn 'symbol_name' src/questdb/*.pxd`
- **Public Python surface & stub:** `grep -rn 'symbol_name' src/questdb/__init__.py src/questdb/ingress.py src/questdb/_client.pyi`
- **C-ABI headers (source of truth):** `grep -rn 'symbol_name' c-questdb-client/include/questdb/ingress/ src/questdb/*.h`
- **Rust helper crate:** `grep -rn 'symbol_name' rpyutils/src/ rpyutils/include/`
- **Unit & mock-server tests:** `grep -rn 'symbol_name' test/test.py test/mock_server.py test/qwp_ws_ack_server.py test/test_tools.py`
- **System / integration tests:** `grep -rn 'symbol_name' test/system_test.py`
- **DataFrame, fuzz, failure and leak tests:** `grep -rn 'symbol_name' test/test_dataframe.py test/test_client_dataframe_fuzz.py test/test_client_polars_fuzz.py test/test_dataframe_fuzz.py test/test_dataframe_leaks.py test/test_client_dataframe_failures.py test/test_client_capsule_path.py`
- **Examples:** `grep -rn 'symbol_name' examples/`
- **Docs:** `grep -rn 'symbol_name' docs/`

A changed public / `cdef` / `.pxd` symbol with zero recorded Grep calls in the trace is a skill violation. The model is not allowed to assert "this is only used here" without showing the search.

### 2.5c Implicit contract list

For each changed symbol, walk this checklist and write one line per item, stating before vs after:

- **Cython exception convention:** does the function return a C type with the right `except` clause? A `cdef` function returning `int`/`void`/a pointer with **no** `except` clause (or `noexcept`, the Cython 3 default for `nogil` functions) **silently swallows any Python exception raised inside it.** Did the convention change, and do all callers still propagate errors correctly?
- **Raises which exceptions on which inputs** (`QuestDBError`, `QuestDBServerRejectionError`, `UnsupportedDataFrameShapeError`, `SenderError`, `ValueError`, `TypeError`) and which callers catch vs propagate them
- **Native memory:** does the symbol allocate (`malloc`/`calloc`/`realloc`) and who frees it? Does it free on every path including the exception path?
- **Reference counting:** does it `Py_INCREF`/`Py_DECREF`/`Py_XDECREF`, store a borrowed `PyObject*`, hold a weakref (`PyWeakref_NewRef`/`PyWeakref_GetRef`) or capsule, or return a borrowed vs owned reference?
- **Buffer protocol:** does it call `PyObject_GetBuffer` (and the matching `PyBuffer_Release`)? Does it keep the exporter alive while the raw pointer is in use?
- **GIL:** does it run under `nogil`? Does it release the GIL around a blocking C call (flush/connect/query)? Does it reacquire to raise?
- **C-ABI ownership:** does it pass a `line_sender_buffer`/`line_sender_utf8`/`qdb_pystr_buf`/QWP handle pointer into Rust, and who owns it afterward? Is a returned `line_sender_error*` freed exactly once (`line_sender_error_free`)?
- **`qdb_pystr_buf` arena lifetime:** are UTF-8 pointers obtained from the arena still valid after a subsequent `clear`/append (which may reallocate and invalidate earlier pointers)?
- **Buffer/sender state on error:** does a failed call leave the `Buffer` half-written, or the `Sender`/`PooledSender` in an unusable state requiring reconstruction? Is a pooled lease returned to the pool in a clean state?
- **Callback and thread contracts:** does the symbol run on, or hand work to, the `connection_listener` / `error_handler` dispatcher threads, or the `active_senders` registry? What holds the GIL, and what may re-enter Python?
- **`.pxd` ↔ C header agreement:** parameter types, `const`-ness, struct layout, enum discriminant order, return type — does the Cython declaration still match `c-questdb-client/include/questdb/ingress/*.h`, `rpyutils/include/rpyutils.h`, or the local `src/questdb/*.h`?
- **`.pyi` ↔ implementation agreement:** does `_client.pyi` still match the real signature, defaults, and return type, and is `__all__` in `src/questdb/__init__.py` still accurate?
- **Wire format:** any change to the ILP bytes produced (protocol v1 / v2), the QWP frame layout, timestamp units, or column encoding.

### 2.5d Cross-context exposure list

End this step with an explicit list of "places this change is visible from but the diff does not touch". This is the highest-priority input for the bug-hunting agents in Step 3.

Group the callsites from 2.5b by execution context. Typical contexts in this codebase:

- **C-ABI binding surface:** every C-ABI function declared in `src/questdb/line_sender.pxd` / `conf_str.pxd` / `arrow_c_data_interface.pxd` / `mpdecimal_compat.pxd` / `rpyutils.pxd` / `_client_helper.pxd` that the changed code calls (transitively)
- **Buffer build hot path:** `Buffer.row` and `Buffer.dataframe`, and the `cdef` helpers behind them (`Buffer._symbol`, the `Buffer._column_*` family, `str_to_column_name`)
- **DataFrame / Arrow ingestion path:** everything in `dataframe.pxi`, the pandas/numpy/pyarrow/polars code paths, Arrow C Data Interface (`ArrowArray`/`ArrowSchema`/`ArrowArrayStream`) consumption and release callbacks, PyCapsule handling
- **Egress / query path:** `egress.pxi`, `QueryResult`, `PooledReader`
- **Pool and handle lifecycle:** `questdb.connect()`, `QuestDB`, `QuestDB.sender`/`reader`/`dataframe`/`query`, `PooledSender` lease and return, `QuestDB.close`
- **Flush path:** `Sender.flush`, `SenderTransaction`, `Buffer` → transport, the `with nogil` blocking sections, acknowledgement waits
- **Auto-flush logic:** any callsite that triggers flush implicitly (row count / byte threshold / interval)
- **Configuration parsing:** `connect()`, `from_conf` / `from_env`, the `conf_str` parser, keyword-argument handling and the string/keyword merge in `src/questdb/__init__.py`
- **Authentication / TLS:** auth token / basic-auth / `TlsCa` configuration paths
- **Callback / threading surface:** `connection_listener` and `error_handler` dispatcher threads, `ConnectionEvent`, `SenderError`, `QwpWsProgress`, the `active_senders` registry (`rpyutils/src/active_senders.rs`), any code reachable from multiple threads
- **`qdb_pystr_buf` arena users:** every function that obtains UTF-8 pointers from the per-`Buffer` string arena
- **Deprecated import shim:** `src/questdb/ingress.py` re-exports — a renamed or removed symbol breaks `from questdb.ingress import X`
- **Python type stubs:** `src/questdb/_client.pyi`
- **Tests:** `test/test.py`, `test/system_test.py`, `test/test_dataframe.py`, fuzz, failure and leak tests
- **Examples & docs:** `examples/*.py` (and `examples.manifest.yaml`), `docs/`

Every entry on this list must be reviewed in Step 3.

### 2.5e Build & binding profile facts

**This sub-step runs at every level, including levels 0 and 1 where the rest of Step 2.5 is skipped.** A single Cython directive or a submodule bump can flip the safety story for the entire extension; agents must reason from the actual profile, not from defaults.

Record, with file:line citations:

- **Cython compiler directives** in the `# cython:` header of `src/questdb/_client.pyx` and in the `cythonize(...)` call in `setup.py` (`language_level`, `binding`, and — if set — `boundscheck`, `wraparound`, `cdivision`, `initializedcheck`, `nonecheck`). State each one's effective value, including "not set, so Cython's default applies". If `boundscheck=False` / `wraparound=False` is in effect, **out-of-range or negative C-array/typed-memoryview indexing is undefined behavior, not an `IndexError`** — agents must treat indexing as a crash surface, not a guarded operation.
- **Cython exception-default fact:** in Cython 3, a `cdef`/`cpdef` function declared `nogil` (or any `cdef` returning a non-object type without an explicit `except` clause) defaults to `noexcept` — it **swallows Python exceptions silently**. Agents 1, 2, and 3 must check the actual `except` clause on every changed `cdef` and not assume exceptions propagate.
- **`c-questdb-client` submodule commit** (`git submodule status`) and its Step 2.4 provenance verdict. If the diff moves it, re-verify `.pxd` ↔ `.h` agreement against the new pinned commit.
- **`rpyutils` Rust crate:** if `rpyutils/src/**` or `rpyutils/Cargo.toml` changed, note its panic/profile behavior — a panic in `rpyutils` reached across the C ABI aborts the Python process. Its header (`rpyutils/include/rpyutils.h`, generated via `rpyutils/cbindgen.toml`) must match `rpyutils.pxd`.
- **Minimum numpy / Python versions** (`pyproject.toml`: `requires-python`, `numpy>=1.21.0`). Code that uses a newer numpy C-API or Python C-API symbol than the floor breaks the oldest supported build. State the floor.
- **Process-terminating paths:** any reachable `abort()` (check whether it is cimported from `libc.stdlib`), and any Rust panic that crosses the C ABI from `c-questdb-client` or `rpyutils`. Both terminate the host interpreter with no traceback. Flag the path.

A review without this section is incomplete. State the relevant facts (directives, exception default, submodule commit and provenance) in one line at the top of every Step 3 agent prompt so the agent reasons from the right premise.

## Step 2.6: Test coverage map

For every production-code behavioral change, record: the changed symbol/path;
the exact test and the search used to find it; why its assertion fails on
regression; supported reachability and affected users; applicable
happy/error/`None`/boundary/GIL-and-threading/native-resource dimensions; and a
disposition of `COVERED`, `CRITICAL GAP`, `MODERATE GAP`, `ACCEPTED GAP`, or
`EXEMPT`.

A Critical gap requires a reachable material consequence such as data loss,
interpreter crash or hang, native memory or refcount leak, security failure,
public API compatibility break, or unbounded resource loss. Keep covered,
accepted, and exempt rows private unless asked.

Where the change is testable locally, name the command that would exercise it
(`python3 proj.py build` then `python3 proj.py test`, `python3 proj.py test all`
for integration, `python3 proj.py valgrind_test`, `python3 proj.py test_fuzzing`)
and record whether it was actually run.

## Step 3: Parallel review

Every agent receives:
1. The PR or local-range diff
2. The change surface material available at that level from Step 2.5
3. The test coverage map from Step 2.6

### Anti-anchoring directive (applies to all agents)

- **Bugs at callsites outside the diff are high-priority candidates.** They become findings only after Step 3b proves the changed contract broke that caller; severity follows user impact.
- **"Looks correct in isolation" is not a valid conclusion.** Before clearing a changed symbol, the agent must walk the callsite inventory from 2.5b and explicitly state, per callsite, whether the new behavior is still correct there.
- **The diff is the entry point, not the scope.** If the change surface map shows the symbol is reachable from N other files, the review covers N+1 files.
- **Project-wide settings affect untouched code.** A change to a Cython directive in `_client.pyx` or `setup.py` (e.g. flipping `boundscheck` off), a `c-questdb-client` submodule bump, or a `.pxd` declaration change retroactively changes the safety/ABI story for **every** function that compiles under that directive or calls that binding — not just the diff. When directives, `setup.py`, `pyproject.toml`, or `.pxd`/submodule pointers appear in the diff, the review covers the affected surface of the whole extension, not just the touched lines.
- A single candidate of the form "in `dataframe.pxi` the new behavior of `Buffer._column_binary` leaks `b.validity` on the exception path" is worth more than five candidates inside the diff.

### Agents

Launch the following agents in parallel.

**Agent 1 — Correctness & bugs:** `None`/NULL handling, edge cases, logic errors, off-by-one, operator precedence, error paths. Integer correctness across the Python↔C boundary: Python `int` → `int64_t`/`size_t` conversion and overflow, `<int>` / `<Py_ssize_t>` / `<size_t>` casts that truncate or wrap, signed/unsigned mismatches, negative-length math. NaN/inf float handling. Timestamp unit conversions (micros vs nanos). Correct ILP wire format (v1 / v2) and QWP frame layout. Cross-reference every changed symbol against its callsite inventory and verify the new behavior is correct at each callsite.

**Agent 2 — Cython memory, refcount & crash surface:** In a Cython extension, anything that corrupts memory or aborts the native side takes down the host Python interpreter with no traceback. Flag every reachable instance of:

- **Native memory leaks / double-free / use-after-free:** every `malloc`/`calloc`/`realloc` must be `free`d on **all** paths — success, early `return`, and the exception/`except` path (prefer `try/finally`). A `realloc` whose return value is assigned back to the same pointer leaks the original on failure (it returns `NULL` without freeing). Freeing a pointer twice, or using it after `free`, corrupts the heap.
- **Reference-count errors:** every `Py_INCREF` needs a matching `Py_DECREF` on all paths; a missing `DECREF` leaks, an extra `DECREF` causes a later use-after-free crash. Borrowed references (dict/list borrows, `PyObject*` stored without incref) must not outlive their owner. Verify `PyCapsule` and weakref handling (`PyWeakref_NewRef` / `PyWeakref_GetRef` and their differing return conventions).
- **Buffer-protocol imbalance:** every `PyObject_GetBuffer` must have a matching `PyBuffer_Release` on all paths, and the raw pointer must not be used after the exporting object can be collected.
- **Indexing under the active directives:** per 2.5e, if `boundscheck`/`wraparound` are off, C-array and typed-memoryview indexing is unchecked — an out-of-range or negative index is UB, not an exception. Verify bounds are established before every index on the hot path.
- **Silent exception swallowing:** a `cdef` function returning a C type without the correct `except` clause (or `noexcept`) drops Python exceptions on the floor, turning an error into wrong data. Verify the `except` convention against what the body raises.
- **Direct aborts:** any reachable `abort()`, and any **Rust panic crossing the C ABI** (from `c-questdb-client` or `rpyutils`) — both terminate the interpreter. The only defense is that the native side returns an error code/`line_sender_error*`, never panics.
- **Re-entrancy into Python from native callbacks:** the `connection_listener` / `error_handler` dispatcher paths and Arrow release callbacks can re-enter Python while native state is half-updated; verify the object graph is consistent at every point a callback can run, and that a callback holding a reference to the owning handle cannot resurrect or free it.
- **Uninitialized memory:** a struct field or `malloc`'d region read before it is written (use `calloc` or explicit init), especially partially-built structs on an error path that then get freed.

State the relevant build facts (directives, exception default, submodule commit) from 2.5e in the agent's first sentence, and evaluate every candidate under the actual settings, not the textbook defaults.

**Agent 3 — C-ABI boundary safety:** Check every call into the `c-questdb-client` / `rpyutils` C ABI. Verify:
- **`.pxd` matches the C header.** For every changed or called C-ABI symbol, read the actual declaration in `c-questdb-client/include/questdb/ingress/line_sender.h` or `qwp_sender.h` (or `rpyutils/include/rpyutils.h`, or the local `src/questdb/*.h`) and confirm the `.pxd` declaration matches it exactly: parameter types, pointer/`const`-ness, return type, struct field order and types, enum discriminant order. A mismatch is silent memory corruption / ABI breakage. If the submodule pointer moved, verify against the **new** pinned commit.
- **NULL handling:** every pointer returned from a C function checked before dereference; every pointer argument that could be `NULL` handled.
- **Error object lifecycle:** every `line_sender_error*` obtained via an out-param is converted (`c_err_to_py`) and freed exactly once (`line_sender_error_free`) — never leaked, never double-freed, never freed then read.
- **Ownership transfer:** `line_sender_buffer`, `line_sender_utf8`, `qdb_pystr_buf`, `line_sender` and QWP handles — who allocates, who frees, and is the lifetime correct relative to the owning `cdef class` (`__cinit__`/`__dealloc__`) and to a pooled lease?
- **`qdb_pystr_buf` arena invalidation:** UTF-8 pointers handed to Rust must remain valid until the buffer write completes and must not be invalidated by an intervening arena `clear`/append.
- **String encoding:** Python `str` → UTF-8 (`line_sender_utf8`), correct length passed, no lone surrogates, embedded-NUL handling, `bytes` vs `str` distinction.

**Agent 4 — GIL & concurrency:** Verify:
- **`nogil` correctness:** no `with nogil` block (or `cdef ... nogil` function) touches a Python object, calls the CPython C-API, raises a Python exception, or `INCREF`/`DECREF`s — doing so without the GIL is a crash/corruption. Errors discovered under `nogil` must be deferred and raised after reacquiring the GIL.
- **GIL release around blocking calls:** the flush/connect/query/network C calls should release the GIL (`with nogil`) so other threads run; verify the released region doesn't reference Python state.
- **Thread-safety:** `QuestDB`, the sender/reader pools, `Sender`, `Buffer`, the `connection_listener` / `error_handler` dispatcher threads, and the `active_senders` registry (`rpyutils/src/active_senders.rs`) — verify documented thread-safety matches the implementation, and that shared mutable state reachable from multiple threads is synchronized. Check lease/return races: two threads borrowing the same pooled sender, or a handle closed while a lease is outstanding. Cross-reference every callsite from 2.5b for violations of the concurrency contract.
- **Free-threaded build:** if the change assumes the GIL serializes access, note whether it holds under a free-threaded (no-GIL) CPython build (the CI matrix includes `*t` free-threaded targets).

**Agent 5 — Resource management & lifecycle:** Leaks on all code paths (especially errors). Check `__cinit__`/`__dealloc__` pairing on every `cdef class` (does `__dealloc__` free everything `__cinit__` and methods allocated, and is it safe when `__cinit__` failed partway?). Native handle lifecycle (`line_sender`, `line_sender_buffer`, `qdb_pystr_buf`, QWP and pool handles), including whether a pooled lease is fully reset before returning to the pool and what happens to an outstanding lease at `QuestDB.close`. Dispatcher-thread shutdown and join. Socket/connection/TLS teardown on error (handled by Rust, but verify the Cython side calls close/free). **Arrow C Data Interface:** `ArrowArray`/`ArrowSchema`/`ArrowArrayStream` `release` callbacks invoked exactly once; PyCapsule consumption semantics correct; no double-release. Walk every callsite from 2.5b that constructs, owns, or transfers ownership of a native handle and verify cleanup on all paths (success, exception, early return).

**Agent 6 — Performance & allocations:** Unnecessary work on hot paths — the per-row buffer build (`Buffer.row` and its `_symbol` / `_column_*` helpers) and the per-column DataFrame loop (`dataframe.pxi`). Flag: Python-level operations (attribute lookups, `dict` access, object boxing, `str` re-encoding) inside the inner per-row/per-cell loop that should be hoisted or done at C level; allocations per row/cell that should be amortized; excessive copying of data that could be zero-copy via the buffer protocol / Arrow; O(n²) patterns over rows or columns. Analyze scaling at realistic volume: millions of rows per flush, hundreds of columns. Setup-path costs (sender construction, config parsing, pool warm-up, schema inspection done once per DataFrame) are acceptable; per-row/per-cell costs are not.

**Agent 7 — Test review & coverage:** Start from the Step 2.6 coverage map and either confirm or correct each disposition. Look for coverage gaps, error-path tests, `None`/edge-case tests, boundary conditions, regression tests, and test quality. Check:
- Unit / mock-server tests in `test/test.py` (uses `test/mock_server.py` and `test/qwp_ws_ack_server.py`)
- System / integration tests against a real QuestDB in `test/system_test.py`
- DataFrame tests in `test/test_dataframe.py`, failure-path tests in `test/test_client_dataframe_failures.py`, fuzz tests in `test/test_client_dataframe_fuzz.py` / `test/test_client_polars_fuzz.py` / `test/test_dataframe_fuzz.py`, and **leak tests** in `test/test_dataframe_leaks.py` (new native-memory or refcount handling should have a leak test)
- Capsule / Arrow path tests in `test/test_client_capsule_path.py`
- Examples in `examples/` still run (and `examples.manifest.yaml` is consistent)

Cross-reference 2.5d: every cross-context exposure should have a test that exercises the changed symbol from that context. A missing test is a coverage gap, graded by reachable impact — a new native-memory path without a leak test, or a new C-ABI binding without a system test, is a Critical gap; a missing test for a cosmetic or already-covered path is not.

**Agent 8 — Public API ergonomics & cross-surface consistency:** API ergonomics is mission-critical here, not cosmetic: an awkward or inconsistent public API leads users into data loss and silent misuse. Review every public symbol the PR adds or changes — anything in `__all__`, on `QuestDB`/`Sender`/`Buffer`/`SenderTransaction`/`PooledSender`/`PooledReader`/`QueryResult`, in `_client.pyi`, or re-exported by the deprecated `questdb.ingress` shim — against the surfaces that already exist. Use the **API ergonomics & cross-surface consistency** checklist below as your spec. Concretely:

- **Consistency first.** For each shared concept the change touches (table/column names, designated timestamp (the `at` argument, `ServerTimestamp`), auto-flush controls, transactional flush, acknowledgement/wait semantics, protocol version, column-type overrides, buffer ownership, TLS/auth settings), confirm it is expressed the *same way* across every sibling surface: `QuestDB.sender()`/`dataframe()`/`query()` vs the standalone `Sender`, row-at-a-time vs DataFrame, keyword arguments vs configuration string vs environment. A new entry point that names, orders, types, or defaults a shared parameter differently from its sibling, or that validates lazily where the sibling validates eagerly, is a finding.
- **Easy path = safe path.** Flag any API where the natural call is the unsafe one — a method that silently leaves rows unpublished or unacknowledged unless the user remembers a separate `flush`/`wait`/`close`, while a sibling does it implicitly. Silent semantic divergence between similarly shaped methods (publishes vs buffers, waits for ack vs not, clears the buffer on error vs leaves it dirty, retries vs not) that neither the name nor the type surfaces is the highest-severity ergonomic defect — graded by data-loss blast radius, never Minor.
- **Accept what callers hold; cut positional noise.** The API should accept the objects users already have (`str`, `datetime`, pandas/polars/pyarrow frames, `None` for defaults) without ceremony at every callsite. Replace always-passed sentinels and double-wrapped knobs with defaulted keyword arguments consistent with the rest of the package.
- **Configuration parity.** Every knob should be reachable the same way through `connect()` keywords, the configuration string, and `from_env` — or the PR must state why not. A setting available only through the string it happened to be implemented in is a finding.
- **Evolvability & docs.** Any commit/flush/ordering/ownership/lifetime contract the signature cannot encode must be documented on the item, cross-referencing siblings that behave differently. New public enums and dataclasses should tolerate future members without breaking callers.
- **Backward compatibility.** Renamed/removed keyword arguments, changed defaults, changed exception types, and symbols dropped from `__all__` or from the `questdb.ingress` re-export list are breaking changes; they must be intentional and called out in the PR body and `CHANGELOG.rst`.

Also cover the mechanical hygiene: `_client.pyi` matches the implementation (signatures, defaults, return types, new symbols in `__all__`), docstrings on public classes and methods, `docs/` updated for API changes, naming consistent with the codebase, no dead code, no unused `import`/`cimport`.

**Agent 9 — Cross-context caller impact:** Walk the callsite inventory from 2.5b. For every callsite, fetch the surrounding code (the calling function plus its callers up two levels) and answer:

- Does this caller pass inputs the new behavior handles incorrectly?
- Does this caller depend on a contract from the implicit contract list (2.5c) that the change broke — e.g. relying on the old `except` convention, the old ownership of a buffer, the old `qdb_pystr_buf` lifetime, the old refcount behavior?
- Is this caller in a context (a `with nogil` block, the per-row hot loop, an auto-flush trigger, an Arrow release callback, a dispatcher-thread callback, a pooled-lease return, a `__dealloc__`, an exception/error path) where the new behavior misbehaves even if the inputs are valid?
- For a changed `cdef`/`cpdef` exception convention: do all callers still detect and propagate the error?
- For a changed C-ABI declaration: does the `.pxd` still match the C header, and do all Cython callers pass the right types/ownership?
- For a changed buffer/sender state machine: do all callers respect the new state transitions (buffer cleared after error before reuse; flush only when flushable; lease reset before return)?

This agent's output is structured per callsite, not per failure mode. Each callsite gets a verdict: SAFE / CANDIDATE / NEEDS VERIFICATION. A CANDIDATE is an untrusted hypothesis for Step 3b.

This agent is not optional even when the diff is small. Small diffs to widely-used symbols (`Buffer.row`, `Sender.flush`, the dataframe entry point, a C-ABI binding) have the largest blast radius.

**Agent 10 — Fresh-context adversarial:** Dispatched separately from agents 1-9 to escape checklist anchoring. This agent operates under different rules from the rest:

- It receives ONLY the PR or local-range diff and the names of the changed files. It does NOT receive the change surface map from Step 2.5, the implicit contract list, the cross-context exposure list, or any of the review checklists below.
- Its sole instruction: "find ways this code is wrong". No category list, no failure-mode taxonomy, no project-specific style guide.
- It is free to use Read, Grep, and Glob to explore the repository however it wants.
- Candidates are not pre-classified by category. Each candidate states: what's wrong, why it's wrong, and the code path that demonstrates it.

The point of this agent is to surface hypotheses the structured agents cannot see because they are reasoning inside the same frame. Novelty and overlap determine investigation priority, not truth or severity.

Run this agent in parallel with agents 1-9. It is mandatory regardless of diff size.

Combine agent outputs into a private candidate ledger. Split compound claims,
deduplicate them, record dependencies, and do not draft severity or report prose yet.

## Step 3b: Falsify, prove, and admit candidates

The parallel review agents work from the diff plus the change surface map and frequently produce false positives — especially around native memory ownership, refcounting, GIL boundaries, Cython exception conventions, and C-ABI lifecycle. Every candidate MUST be verified before it is reported.

Use `HYPOTHESIS → FALSIFYING → PROVEN → ADMITTED`. Omit a candidate when any
required premise remains unsupported. Behavioral candidates require: the exact
changed hunk or broken unchanged caller; a supported input/state producer; the
complete reachable path; observed head behavior; identical-trigger `$BASE`
behavior (or proof of a genuinely new surface); user-visible impact; the
strongest counterevidence attempted; and the command/test artifact with revision
identity.

**Executed evidence is required** for claims about races, GIL and thread
interleaving, ordering, retry and reconnect, pool lease lifecycle, native memory
leaks and refcount imbalance, interpreter crashes, filesystem state, version
compatibility, and protocol state. These are exactly the classes where static
reasoning about this codebase is least reliable, and they are runnable here:
build with `python3 proj.py build` and reproduce with `python3 proj.py test`,
`python3 proj.py test all`, the leak tests, or `python3 proj.py valgrind_test`.
Static declaration mismatches (`.pxd` vs header, `.pyi` vs implementation,
`except` clause vs body) may cite complete source proof instead.

If base behaves the same or worse under an identical trigger, the issue is not a
finding against this PR.

**Run base comparisons in a throwaway worktree.** Reproducing at `$BASE` needs
base's sources checked out, and checking them out in the review tree moves it off
the revision under review — in working-tree mode that destroys the uncommitted
changes being reviewed. Check base out under the session scratchpad instead, and
build there:

```bash
git worktree add --detach <scratch>/base <BASE-sha>
git -C <scratch>/base submodule update --init --recursive
(cd <scratch>/base && python3 proj.py build && python3 proj.py test <selector>)
git worktree remove --force <scratch>/base   # when the review ends
```

Head-side reproduction runs in the review tree as it stands: `python3 proj.py build`
writes `src/questdb/_client.c` and the in-tree `.so`, both gitignored build
artifacts, so no tracked file and no uncommitted change is touched. The review
never checks the tree out, stages, commits, or pushes.

For each candidate in the private ledger:

1. **Read the actual source code** at the exact lines cited (in the `.pyx`/`.pxi`/`.pxd`/`.pyi`/`.inc`, never the generated `_client.c`). Do not rely on the agent's description alone.
2. **Trace the full code path:** follow callers and `cdef` helpers. Remember Cython's `include` model — `dataframe.pxi` and `egress.pxi` are textually included into `_client.pyx`, so symbols are shared across them.
3. **Check both sides of the C ABI:** if a candidate involves Cython↔Rust interaction, read both the Cython call and the C header in `c-questdb-client/include/questdb/ingress/` (or `rpyutils/include/`, or `src/questdb/*.h`). Verify ownership transfer, error propagation, and freeing on both sides.
4. **For native-memory-leak claims:** trace every `malloc`/`calloc`/`realloc` to its `free` on ALL paths (success, early return, `except`/exception unwind). Confirm the intervening code can actually raise before claiming the exception path leaks.
5. **For refcount claims:** count `Py_INCREF`/`Py_DECREF`/`Py_XDECREF` on every path; confirm borrowed-vs-owned reasoning against the CPython C-API contract of each function used.
6. **For exception-swallowing claims:** check the actual `except` clause on the `cdef` and whether the body can raise. Under Cython 3 a `nogil` `cdef` defaults to `noexcept` — confirm whether that's the real declaration.
7. **For GIL claims:** verify the cited code is actually inside a `nogil` region and actually touches a Python object / C-API; a `cdef` function called from `nogil` may itself acquire the GIL.
8. **For C-ABI / `.pxd` mismatch claims:** read the exact declaration in the pinned header and compare field-by-field. A claimed mismatch that actually matches is a false positive.
9. **For numeric overflow/truncation claims:** separate value reachability from volume reachability. A scalar the caller can actually pass is reachable no matter how small the buffer is — a Python `int` beyond `int64_t`, an `INT64_MAX`/`INT64_MIN` boundary, a negative length, a value that wraps on an `<int>`/`<Py_ssize_t>`/`<size_t>` cast — so verify it against the guards on the input path, not against volume. Volume-dependent claims are the ones bounded by realistic scale: ILP buffers up to a few hundred MB, millions of rows per flush, columns in the tens to low hundreds. Drop an overflow only when it needs buffer or row volume beyond that.
10. **For performance claims:** confirm the cost is on the per-row/per-cell hot path and measurable relative to surrounding I/O. Downgrade negligible savings to a nit. Exception: a per-row or per-cell allocation / Python-object operation on the buffer-build path is always worth flagging.
11. **For cross-context candidates (Agent 9):** re-read the callsite in full, including callers up two levels, and confirm the broken behavior is reachable from production or test paths users will exercise. Cross-context candidates are high-value but also the easiest to overstate — verify carefully.
12. **For API ergonomics / consistency candidates (Agent 8):** name and read the specific peer surface the candidate is measured against — the sibling method, the other ingestion path, the configuration-string equivalent, or the `questdb.ingress` re-export — and quote both shapes side by side. An ergonomic complaint with no concrete peer surface to compare against, or one that contradicts an established package-wide convention, is bikeshedding: drop it or downgrade to Minor. Then grade by user impact, not appearance: a divergence that can cause data loss, unpublished or unacknowledged rows, or silent misuse is Critical/Moderate; a pure style or naming difference with no safety impact is Minor.

**Classify each candidate** as:
- **ADMITTED in-diff** — the bug is real and inside the diff
- **ADMITTED out-of-diff-breakage** — an unchanged caller is broken by a contract this PR changed (cite the file and the contract from 2.5c that was violated)
- **OMITTED pre-existing/not-attributed** — base has the same or worse behavior
- **OMITTED false/unverified** — counterevidence disproves it or required proof is missing

**Move omitted false candidates to a separate "Downgraded" section** at the end of the report. For each, give a one-line explanation of why it was dismissed. This lets the PR author verify the reasoning and catch verification mistakes.

Launch verification agents in parallel where candidates are independent. Each verification agent should read surrounding source files, not just the diff.

## Review checklists

Review the diff for:

### Correctness & bugs
- `None`/NULL handling at API boundaries
- Edge cases and error paths
- Logic errors, off-by-one, incorrect bounds, wrong operator precedence
- Integer overflow/truncation across the Python↔C boundary (`int` → `int64_t`/`size_t`, `<int>`/`<Py_ssize_t>` casts, signed/unsigned)
- Float edge cases (NaN, inf), timestamp unit conversions (micros vs nanos)
- Correct ILP wire format (v1 / v2) and QWP frame layout
- **Reachability expansion:** for each changed symbol, list the new contexts it can appear in (DataFrame path, `nogil` section, auto-flush, Arrow callback, dispatcher-thread callback, pooled lease, error path) and verify it works in each.

### Cython memory & refcount safety
- Every `malloc`/`calloc`/`realloc` freed on success, early-return, and exception paths (prefer `try/finally`); no double-free, no use-after-free; `realloc`-failure path doesn't leak the original
- Every `Py_INCREF` matched by `Py_DECREF`/`Py_XDECREF`; borrowed references not outliving their owner; weakref/capsule handling correct
- Every `PyObject_GetBuffer` matched by `PyBuffer_Release`; exporter kept alive while the pointer is used
- Correct Cython `except` convention on every `cdef`/`cpdef` returning a C type (no silent exception swallowing; `noexcept` is the Cython-3 default for `nogil` `cdef`)
- No reachable `abort()`, and no Rust panic crossing the C ABI (both kill the interpreter)
- Indexing safe under the active `boundscheck`/`wraparound` directives
- Native callbacks that re-enter Python (Arrow release, connection/error dispatch) see a consistent object graph and cannot resurrect or free the owner
- No uninitialized struct/heap memory read (use `calloc` or init before use, especially on partially-built error paths)

### C-ABI boundary
- `.pxd` declarations match `c-questdb-client/include/questdb/ingress/*.h` (and `rpyutils/include/rpyutils.h`, and the local `src/questdb/*.h`) exactly — types, `const`, struct layout, enum order, return type — against the **pinned** submodule commit
- All pointers returned from C checked for NULL before dereference
- Every `line_sender_error*` freed exactly once (`line_sender_error_free`), never double-freed or leaked
- Ownership semantics clear and correct (who allocates the handle, who frees it, lifetime vs the owning `cdef class` and vs a pooled lease)
- `qdb_pystr_buf` arena pointers stay valid until consumed; not invalidated by an intervening `clear`/append
- String handling: `str` → UTF-8 with correct length, lone-surrogate rejection, embedded-NUL handling, `bytes`/`str` distinction
- ABI stability: a submodule bump that reorders a struct or renumbers an enum requires matching `.pxd` updates

### GIL & concurrency
- No Python object access / C-API call / refcount op / raise inside a `with nogil` block or `cdef ... nogil` function
- GIL released around blocking network/flush/query C calls; released region references no Python state; errors deferred and raised after reacquiring
- `QuestDB`, the sender/reader pools, `Sender`, `Buffer`, the dispatcher threads and `active_senders` thread-safety matches documentation; shared mutable state synchronized
- Pool lease/return races: no two threads holding the same pooled sender, no use of a lease after `QuestDB.close`, dispatcher threads joined on shutdown
- Assumptions that the GIL serializes access re-checked for the free-threaded CPython build

### Performance
- No per-row/per-cell Python-level operations (attribute/dict lookups, boxing, `str` re-encoding) in the buffer-build or DataFrame inner loops that belong at C level or hoisted to setup
- No per-row/per-cell allocations that should be amortized
- Zero-copy where possible (buffer protocol, Arrow) instead of copying
- No O(n²) over rows or columns at realistic scale (millions of rows, hundreds of columns)

### Resource management
- `__cinit__`/`__dealloc__` pair frees everything allocated, and `__dealloc__` is safe after a partially-failed `__cinit__`
- Native handles (`line_sender`, `line_sender_buffer`, `qdb_pystr_buf`, QWP and pool handles) released on all paths
- Pooled leases fully reset before return; outstanding leases handled correctly at `QuestDB.close`
- Socket/connection/TLS cleanup on error (Cython side invokes the Rust close/free)
- Arrow `release` callbacks invoked exactly once; PyCapsule consumed correctly; no double-release
- No leak through the C-ABI boundary (ownership documented and consistent)

### API ergonomics & cross-surface consistency

Mission-critical for a client library: a confusing or inconsistent public API causes data loss and silent misuse, so ergonomic and consistency defects are graded by user impact — not automatically filed under Minor. Review every public symbol the PR adds or changes against the surfaces that already exist.

**Consistency across surfaces (the dominant concern):**
- **Same concept, same shape.** A concept that already exists elsewhere — table name, column names, designated timestamp (the `at` argument and `ServerTimestamp`), auto-flush controls, transactional flush, acknowledgement/wait semantics, protocol version, column-type overrides, TLS/auth settings — must be expressed identically across every surface that touches it: same parameter name, same type, same argument order, same default, same validation timing (eager vs at-flush), same ownership. Flag a new method that names or orders a shared parameter differently from its sibling, or validates lazily where the sibling validates eagerly.
- **Capability parity across ingestion paths.** If one path (`Buffer` row building, `Sender`, `QuestDB.sender()`, `QuestDB.dataframe()`, pandas/polars/pyarrow entry points) exposes a control, the peers must expose the equivalent or the PR must state why not. A capability that exists only because of which entry point the user happened to pick is a finding.
- **Configuration parity.** Every knob should be reachable through `connect()` keywords, the configuration string, and `from_conf`/`from_env` alike — or the omission must be stated. Verify the keyword/string merge in `src/questdb/__init__.py` still rejects duplicates with the same error shape.
- **Naming and verb parity.** The same operation uses the same verb everywhere (`flush`, `row`, `column`, `at*`, `from_conf`, `from_env`). A new `write_frame` beside an existing `dataframe`, or `set_table` beside `table`, is an inconsistency.
- **Deprecated shim parity.** Symbols re-exported by `src/questdb/ingress.py` must keep working; a rename that drops one silently breaks `from questdb.ingress import X`.

**Ergonomics (the easy path must be the safe path):**
- **No hidden mandatory second call.** If correctness requires the user to remember a follow-up call (`flush`, `wait`, `close`) to avoid unpublished or unacknowledged rows, and a sibling API does it implicitly, the asymmetry is a footgun — rank it by data-loss blast radius, not as a nit.
- **Silent semantic divergence is the worst defect.** Two similarly named or shaped entry points that differ in a behavior neither the name nor the type surfaces (one publishes, one buffers; one waits for acknowledgement, one does not; one retries, one does not; one clears the buffer on error, one leaves it dirty) are a trap. Require the difference be made visible in the name or signature, or documented loudly at *both* sites with a cross-reference. Never file these as Minor.
- **Accept the types callers already hold.** `str`, `datetime`, pandas/polars/pyarrow frames, and `None` for defaults should work without ceremony at every callsite, matching what the rest of the package already accepts for that concept.
- **Positional noise and clunky knobs.** Sentinel arguments the common call must always pass are a smell; prefer a defaulted keyword argument consistent with the rest of the package.
- **Errors over silence, consistently.** Fallible validation raises the package's standard error types (`QuestDBError` and subclasses, `ValueError`, `TypeError`) with an actionable message, and must not introduce a second, inconsistent error idiom.
- **Document the contract the signature can't encode.** Any publish/flush/ordering/ownership/lifetime/threading semantic not expressible in the signature must be documented on the public item, cross-referencing any sibling that behaves differently.

### Code quality
- `_client.pyi` stub matches the implementation (signatures, defaults, return types) and `__all__` in `src/questdb/__init__.py` is accurate
- Backward compatibility of the Python API (renamed/removed kwargs, changed defaults, changed exception types, dropped re-exports) — breaking changes must be intentional and called out in the PR body
- `CHANGELOG.rst` updated for user-visible changes; `docs/` updated for API changes
- Docstrings on public classes/methods
- Naming consistent with the codebase; no dead code or unused `import`/`cimport`

### Test review
- **Coverage gaps:** every new/changed code path has a corresponding test; flag missing ones explicitly as "missing test for X" with a disposition from Step 2.6
- **Cross-context coverage:** every entry in the cross-context exposure list (2.5d) has a test exercising the changed symbol from that context
- **Leak coverage:** new native-memory or refcount-handling code has a test in `test/test_dataframe_leaks.py` (or equivalent)
- **Error-path coverage:** failure cases, partial writes, connection drops, TLS/auth failures, server rejections, and edge conditions tested — not just the happy path (see `test/test_client_dataframe_failures.py`)
- **Edge-case tests:** `None`, empty buffers, zero-length strings, max-length symbols, boundary integers, NaN/inf, non-UTF-8 strings
- **C-ABI / binding changes** covered by a system test in `test/system_test.py`
- **DataFrame / Arrow changes** covered in `test/test_dataframe.py` and the fuzz/capsule tests
- **Pool / QWP changes** covered against `test/mock_server.py` or `test/qwp_ws_ack_server.py`, including the acknowledgement and reconnect paths
- **Test quality:** tests assert the right thing; watch for trivially-passing tests
- **Regression tests:** a bug fix has a test that reproduces the original bug and fails without the fix

### Unresolved TODOs and FIXMEs
- Scan the diff for `TODO`, `FIXME`, `HACK`, `XXX`, `WORKAROUND`. For each:
  - Pre-existing (just moved/reformatted) or newly introduced in this PR?
  - If new: unfinished work that should block merge, or an acceptable known limitation? Flag deferred bugs or incomplete implementations.
  - If it references a ticket/issue, verify the reference exists.

### Commit messages
- Plain English titles, under 50 chars
- Active voice, naming the acting subject

## Step 4: Output

Present ONLY admitted findings (omitted candidates are excluded from Critical/Moderate/Minor). Structure as:

Classify by reachable user impact, not review category. **Critical** means material
data loss or corruption, interpreter crash or hang, native memory or refcount leak,
undefined behavior or security failure, public API compatibility break, unbounded
resource loss, or a material hot-path regression. **Moderate** is bounded or
developer-facing impact. **Minor** is cosmetic. Every behavioral finding must state
the problem, net impact, decisive evidence, and identical-trigger base behavior.

### Critical
Issues that must be fixed before merge. Each must include:
- Exact file path and line numbers (including out-of-diff files)
- Whether the finding is **in-diff** or **out-of-diff**
- Code path trace showing why the bug is real
- For out-of-diff findings: the contract from 2.5c that was violated and the callsite that triggers it
- Suggested fix

### Moderate
Issues worth addressing but not blocking.

### Minor
Style nits and suggestions.

### Coverage gaps
List only admitted Critical and Moderate gaps from Step 2.6, with the recorded
test search and the failure link. Critical gaps block through the same
correctness gate as defects.

### Downgraded (false positives)
Candidates from discovery that were dismissed after source code verification. For each, state:
- The original claim (one line)
- Why it was dismissed (one line, citing the specific code that disproves it)

### Summary
- One-line verdict: approve, approve with comments, request changes, or needs discussion
- Request changes while any admitted Critical finding or Critical coverage gap remains open
- Highlight any regressions or tradeoffs
- State the test-gate result (which commands were run, and their outcome) and the admitted coverage-gap count
- State the `c-questdb-client` submodule provenance verdict from Step 2.4
- State how many candidates were admitted vs omitted as false positives (e.g., "8 findings admitted, 4 false positives removed")
- State the in-diff vs out-of-diff split (e.g., "5 findings in-diff, 3 findings out-of-diff"). Agent 9 runs at level 3 only. When it was launched and completed, a non-trivial diff with zero out-of-diff findings means the cross-context pass likely underran — re-invoke Agent 9 with a wider grep before finalizing. At levels 0-2 Agent 9 is skipped, so state that the cross-context caller pass did not run at this level and draw no under-run conclusion from the zero count.

