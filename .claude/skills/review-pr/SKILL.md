---
name: review-pr
description: Review a GitHub pull request against py-questdb-client (Cython + C-ABI) coding standards
argument-hint: [PR number or URL] [--level=0..3]
allowed-tools: Bash(gh *), Bash(git *), Read, Grep, Glob, Agent
---

Review the pull request `$ARGUMENTS`.

## Review mindset

You are a senior QuestDB engineer performing a blocking code review. `py-questdb-client` is mission-critical software: a **Cython** extension that wraps the **`c-questdb-client` (Rust) library** through its **C ABI**, and is used to ingest production data from customer Python applications. A bug here causes data loss, silent data corruption, segfaults that take down the host Python interpreter, reference-count leaks, or native memory leaks. There is zero tolerance for correctness issues, memory unsafety, refcount imbalance, GIL violations, or an FFI binding that disagrees with the C header it calls. Be critical, thorough, and opinionated. Your job is to catch problems before they ship, not to be nice.

- **Assume nothing is correct until you've verified it.** Read surrounding code to understand context — don't just look at the diff in isolation.
- **The diff is a hint, not the boundary of the review.** The highest-value bugs almost always live at callsites outside the diff that depend on contracts the diff quietly changed (a `cdef` helper's error-return convention, a buffer's ownership, a `qdb_pystr_buf` arena's lifetime). Treat the diff as the entry point, not the scope.
- **Flag every issue you find**, no matter how small. Do not soften language or hedge. Say "this is wrong" not "this might be an issue".
- **Do not praise the code.** Skip "looks good", "nice work", "clever approach". Focus entirely on problems and risks.
- **Think adversarially.** For each change, work through:
  - Inputs: which values break this? Empty buffers, zero-length strings, `None`, NaN/inf floats, boundary integers (`INT64_MAX`/`INT64_MIN`), max-length symbols, non-UTF-8 `str`, `bytes` with embedded NULs, huge `int` that overflows `int64_t`.
  - Encoding: how does the code behave when a Python `str` contains lone surrogates, astral codepoints, or characters that fail UTF-8 encoding?
  - Memory: every `malloc`/`calloc`/`realloc` — is it freed on the error path, the exception path, and the early-return path? Every `Py_INCREF` — is there a matching `Py_DECREF`? Every `PyObject_GetBuffer` — a matching `PyBuffer_Release`?
  - GIL: does a `with nogil` block touch a Python object or call a CPython API function? Does a `cdef ... nogil` function need the GIL it doesn't hold?
  - Failure modes: connection dropping mid-flush, partial write, TLS handshake failure, auth rejection, server rejection — does the buffer/sender end in a usable state, and does native memory get released?
  - C-ABI callers: what happens when a C function returns `NULL`, returns an error via its out-param, or hands back a pointer the Cython side must free exactly once?
- **A finding is a sample, not the population.** Once a bug is confirmed, ask whether it is one instance of a mechanism the codebase repeats, whether the obvious fix only moves it to the next place that mechanism runs, and whether the code violates this invariant because it is holding a conflicting one — then ask what else would have to be true for the finding to be as small as it looks. Step 3c makes this check mandatory.
- **Check what's missing**, not just what's there. Missing tests, missing error handling, missing edge cases, missing `ingress.pyi` stub updates for public API changes, `.pxd` declarations out of sync with the C header.
- **Verify every claim.** If the PR title says "fix", verify the bug actually existed and the fix is correct. If it says "improve performance", look for benchmarks or reason about the change against the per-row hot path. If it says "simplify", verify the new code is actually simpler and doesn't drop behavior (e.g. a dropped `free` on an error branch). Treat the PR description as an unverified hypothesis.
- **Read the full context of changed files** when the diff alone is ambiguous. Use Read/Grep/Glob to inspect surrounding code, callers, and related tests.
- **Assess reachability before reporting.** For every potential bug, trace the actual callers and inputs. If a problem requires physically impossible conditions (a length larger than `SIZE_MAX`, a NUL injected through an API that already rejects it, a panic behind a validation guard), it is not a real finding — drop it. Focus on bugs that real workloads can trigger, not theoretical edge cases.
- **Never review generated or build artifacts.** `src/questdb/ingress.c`, `*.html` (Cython annotation), and `*.so` are build outputs. The source of truth is `*.pyx`, `*.pxi`, `*.pxd`, and `*.pyi`. If the diff contains a regenerated `ingress.c`, review the `.pyx`/`.pxi` change that produced it, not the generated C.

## Review level

Parse `$ARGUMENTS` for a level token: `--level=N`, `-lN`, or a bare single digit `0`-`3`. **If no level is given, default to 0.** Strip the level token before feeding the remainder (PR number or URL) to `gh` commands.

The level controls how much of the review below actually runs. Lower levels keep the same review *spirit* — adversarial, blocking, no praise — but cut the breadth of the analysis. Higher levels have significantly higher token cost; reserve level 3 for high-stakes PRs (C-ABI `.pxd` changes, a `c-questdb-client` submodule bump, the dataframe/Arrow ingestion path, `nogil` sections, manual `malloc`/refcount code, ILP wire format, or auth/TLS configuration).

| Level | What runs |
|-------|-----------|
| **0 (default)** | Steps 1, 2, 4. Skip Step 2.5. Skip Step 3 — no agent spawn; review the diff inline in the main loop, using Read/Grep on demand to resolve ambiguities. Skip Step 3b — verify each finding inline as you write it. Run Step 3c's probes inline on every confirmed finding; Probe A's grep is mandatory. Single-pass review covering correctness, Cython memory/refcount/GIL safety, C-ABI binding correctness, tests, and coding standards on the diff itself. |
| **1** | Adds Step 2.5a (semantic delta only — skip 2.5b/2.5c/2.5d). In Step 3, launch only Agent 1 (correctness), Agent 2 (Cython memory & refcount safety), and Agent 7 (tests) in parallel. Skip all other agents. Skip Step 3b — verify findings inline as you draft the report. Run Step 3c's probes inline; Probe A's grep is mandatory. |
| **2** | Full Step 2.5, but in 2.5b restrict the callsite inventory to public Python symbols (exported in `__all__` / `ingress.pyi`) plus every `cdef`/`cpdef` function and every C-ABI symbol declared in the `.pxd` files. In Step 3, launch Agents 1-8. Skip Agent 9 (cross-context) and Agent 10 (adversarial fresh-context). Step 3b uses a single batched verification agent for all findings instead of one per finding, and Step 3c uses a single batched broadening agent. |
| **3** | Every step below as written, all 10 agents, per-finding verification, per-finding broadening plus the Step 3c merge pass. The full mission-critical pass. |

State the chosen level in one line at the start of the review so the user knows what they're getting (e.g., "Reviewing PR #141 at level 2"). If the level was defaulted, mention that level 3 exists for full review.

## Step 1: Gather PR context

Capture the PR identifier in `$PR` (the part of `$ARGUMENTS` left after stripping the level token), then fetch metadata, diff, and review comments in a single bash call so `$PR` is in scope for all three `gh` invocations:

```bash
PR='<PR number or URL from $ARGUMENTS, with any --level=N / -lN / bare-digit level token removed>'
gh pr view "$PR" --json number,title,body,labels,state
gh pr diff "$PR"
gh pr view "$PR" --comments
```

If the diff modifies `c-questdb-client` (the git submodule pointer) or any `.pxd` file, note it now — a submodule bump or binding change is the highest-risk class of change in this repo and forces level-3 scrutiny of the C-ABI surface regardless of the requested level.

## Step 2: PR title and description

Check:
- Title is clear and describes the change
- Description speaks to end-user impact, not implementation internals
- If fixing an issue, `Fixes #NNN` or a link to the issue is present
- Tone is level-headed and analytical
- For public API changes (anything in `__all__`, a new/changed method on `Sender`/`Buffer`/`Client`, a new keyword argument, or a changed default), the description calls out the API change explicitly, and `CHANGELOG.rst` is updated
- For a `c-questdb-client` submodule bump, the description states which upstream change is being pulled in and why

## Step 2.5: Map the change surface

Before launching review agents, produce a structured change surface map. This step is mandatory and must use Grep/Glob — do not reason about callsites from memory. The output of this step is required input for every agent in Step 3.

### 2.5a Semantic delta per changed symbol

For every modified or added function (`def`, `cdef`, `cpdef`), method, class, `cdef class` attribute, module-level constant, enum member, or C-ABI declaration in a `.pxd`, write:

- **Symbol:** fully-qualified name (e.g., `questdb.ingress.Buffer.column`, `_dataframe`, `c_err_to_py`, `line_sender_buffer_column_f64`)
- **Before:** signature, return type, **Cython exception convention** (`except -1` / `except *` / `except? -1` / `except +` / none / `noexcept`), what it raises and on which inputs, `nogil`-ness, whether it touches Python objects, allocation behavior (`malloc`/`calloc`/`realloc`), refcount effect (does it steal/borrow/own a reference?), C-ABI ownership semantics (who frees returned pointers), thread-safety
- **After:** same fields
- **Delta:** one line stating what semantically changed

"Refactored", "cleaned up", "improved", "simplified" are not acceptable deltas. State the actual behavioral difference. If nothing semantically changed, write "no behavioral change" — but only after checking, not as a default.

### 2.5b Callsite inventory

For every changed symbol that is public (in `__all__` / `ingress.pyi`), `cdef`/`cpdef`, declared in a `.pxd`, or a C-ABI function, run Grep across the repository to find every callsite, override, or reference outside the diff.

Produce a list grouped by file. Search at minimum:

- **Cython implementation & includes:** `grep -rn 'symbol_name' src/questdb/*.pyx src/questdb/*.pxi`
- **Cython C-ABI / helper declarations:** `grep -rn 'symbol_name' src/questdb/*.pxd`
- **Type stubs:** `grep -rn 'symbol_name' src/questdb/ingress.pyi`
- **C-ABI header (source of truth):** `grep -rn 'symbol_name' c-questdb-client/include/questdb/ingress/`
- **Rust helper crate:** `grep -rn 'symbol_name' rpyutils/src/ rpyutils/include/`
- **Unit & mock-server tests:** `grep -rn 'symbol_name' test/test.py test/mock_server.py test/test_tools.py`
- **System / integration tests:** `grep -rn 'symbol_name' test/system_test.py`
- **DataFrame tests, fuzz tests, leak tests:** `grep -rn 'symbol_name' test/test_dataframe.py test/test_client_dataframe_fuzz.py test/test_dataframe_fuzz.py test/test_dataframe_leaks.py test/test_client_capsule_path.py`
- **Examples:** `grep -rn 'symbol_name' examples/`
- **Docs:** `grep -rn 'symbol_name' docs/`

A changed public / `cdef` / `.pxd` symbol with zero recorded Grep calls in the trace is a skill violation. The model is not allowed to assert "this is only used here" without showing the search.

### 2.5c Implicit contract list

For each changed symbol, walk this checklist and write one line per item, stating before vs after:

- **Cython exception convention:** does the function return a C type with the right `except` clause? A `cdef` function returning `int`/`void`/a pointer with **no** `except` clause (or `noexcept`, the Cython 3 default for `nogil` functions) **silently swallows any Python exception raised inside it.** Did the convention change, and do all callers still propagate errors correctly?
- **Raises which exceptions on which inputs** (`IngressError`, `ValueError`, `TypeError`, `IngressServerRejectionError`, `UnsupportedDataFrameShapeError`) and which callers catch vs propagate them
- **Native memory:** does the symbol allocate (`malloc`/`calloc`/`realloc`) and who frees it? Does it free on every path including the exception path?
- **Reference counting:** does it `Py_INCREF`/`Py_DECREF`, store a borrowed `PyObject*`, hold a weakref/capsule, or return a borrowed vs owned reference?
- **Buffer protocol:** does it call `PyObject_GetBuffer` (and the matching `PyBuffer_Release`)? Does it keep the exporter alive while the raw pointer is in use?
- **GIL:** does it run under `nogil`? Does it release the GIL around a blocking C call (flush/connect)? Does it reacquire to raise?
- **C-ABI ownership:** does it pass a `line_sender_buffer`/`line_sender_utf8`/`qdb_pystr_buf` pointer into Rust, and who owns it afterward? Is a returned `line_sender_error*` freed exactly once (`line_sender_error_free`)?
- **`qdb_pystr_buf` arena lifetime:** are UTF-8 pointers obtained from the arena still valid after a subsequent `clear`/append (which may reallocate and invalidate earlier pointers)?
- **Buffer/sender state on error:** does a failed call leave the `Buffer` half-written, or the `Sender` in an unusable state requiring reconstruction?
- **`.pxd` ↔ C header agreement:** parameter types, `const`-ness, struct layout, enum discriminant order, return type — does the Cython declaration still match `c-questdb-client/include/questdb/ingress/*.h`?
- **`.pyi` ↔ implementation agreement:** does the stub still match the real signature, defaults, and return type?
- **Wire format:** any change to the ILP bytes produced (protocol v1 / v2), timestamp units, or column encoding.

### 2.5d Cross-context exposure list

End this step with an explicit list of "places this change is visible from but the diff does not touch". This is the highest-priority input for the bug-hunting agents in Step 3.

Group the callsites from 2.5b by execution context. Typical contexts in this codebase:

- **C-ABI binding surface:** every C-ABI function declared in `src/questdb/line_sender.pxd` / `conf_str.pxd` / `arrow_c_data_interface.pxd` / `mpdecimal_compat.pxd` / `rpyutils.pxd` that the changed code calls (transitively)
- **Buffer build hot path:** `Buffer.column`, `Buffer.symbol`, `Buffer.row`, `Buffer.at*`, and their `cdef` helpers
- **DataFrame / Arrow ingestion path:** everything in `dataframe.pxi`, the pandas/numpy/pyarrow/polars code paths, Arrow C Data Interface (`ArrowArray`/`ArrowSchema`/`ArrowArrayStream`) consumption and release callbacks, PyCapsule handling
- **Egress / query path:** `egress.pxi`, `QueryResult`
- **Flush path:** `Sender.flush`, `Buffer` → transport, the `with nogil` blocking sections
- **Auto-flush logic:** any callsite that triggers flush implicitly (row count / byte threshold / interval)
- **Configuration parsing:** `Sender.from_conf` / `from_env`, the `conf_str` parser, keyword-argument handling
- **Authentication / TLS:** auth token / basic-auth / TLS-CA configuration paths
- **`nogil` / threading surface:** the `active_senders` registry (`rpyutils/src/active_senders.rs`), any code reachable from multiple threads
- **`qdb_pystr_buf` arena users:** every function that obtains UTF-8 pointers from the per-`Buffer` string arena
- **Python type stubs:** `ingress.pyi`
- **Tests:** `test/test.py`, `test/system_test.py`, `test/test_dataframe.py`, fuzz and leak tests
- **Examples & docs:** `examples/*.py`, `docs/`

Every entry on this list must be reviewed in Step 3.

### 2.5e Build & binding profile facts

**This sub-step runs at every level, including levels 0 and 1 where the rest of Step 2.5 is skipped.** A single Cython directive or a submodule bump can flip the safety story for the entire extension; agents must reason from the actual profile, not from defaults.

Record, with file:line citations:

- **Cython compiler directives** at the top of `ingress.pyx` and in `setup.py` (`language_level`, `binding`, and — if set — `boundscheck`, `wraparound`, `cdivision`, `initializedcheck`, `nonecheck`). If `boundscheck=False` / `wraparound=False`, **out-of-range or negative C-array/typed-memoryview indexing is undefined behavior, not an `IndexError`** — agents must treat indexing as a crash surface, not a guarded operation.
- **Cython exception-default fact:** in Cython 3, a `cdef`/`cpdef` function declared `nogil` (or any `cdef` returning a non-object type without an explicit `except` clause) defaults to `noexcept` — it **swallows Python exceptions silently**. Agents 1, 2, and 3 must check the actual `except` clause on every changed `cdef` and not assume exceptions propagate.
- **`c-questdb-client` submodule commit** (`git submodule status`) — if the diff moves it, the pinned commit's headers under `c-questdb-client/include/questdb/ingress/` are the *new* source of truth that every `.pxd` must match. Re-verify the `.pxd` ↔ `.h` agreement against the new commit.
- **`rpyutils` Rust crate:** if `rpyutils/src/**` or `rpyutils/Cargo.toml` changed, note its panic/profile behavior — a panic in `rpyutils` reached across the C ABI aborts the Python process. Its headers (`rpyutils/include/`, generated via `cbindgen.toml`) must match `rpyutils.pxd`.
- **Minimum numpy / Python versions** (`pyproject.toml`: `requires-python`, `numpy>=1.21.0`). Code that uses a newer numpy C-API or Python C-API symbol than the floor breaks the oldest supported build. State the floor.
- **`abort()` is imported** (`from libc.stdlib cimport ... abort`). Any reachable `abort()` call, or any Rust panic that crosses the C ABI, terminates the host interpreter with no traceback. Flag the path.

A review without this section is incomplete. State the relevant facts (directives, exception default, submodule commit) in one line at the top of every Step 3 agent prompt so the agent reasons from the right premise.

## Step 3: Parallel review

Every agent receives:
1. The PR diff
2. The full change surface map from Step 2.5 (semantic deltas, callsite inventory, implicit contracts, cross-context exposure list, build & binding profile facts)

### Anti-anchoring directive (applies to all agents)

- **Bugs at callsites outside the diff outrank bugs inside the diff.** A confirmed bug in a file the PR did not touch but that calls a changed symbol is a P0 finding.
- **"Looks correct in isolation" is not a valid conclusion.** Before clearing a changed symbol, the agent must walk the callsite inventory from 2.5b and explicitly state, per callsite, whether the new behavior is still correct there.
- **The diff is the entry point, not the scope.** If the change surface map shows the symbol is reachable from N other files, the review covers N+1 files.
- **Project-wide settings affect untouched code.** A change to a Cython directive in `ingress.pyx` or `setup.py` (e.g. flipping `boundscheck` off), a `c-questdb-client` submodule bump, or a `.pxd` declaration change retroactively changes the safety/ABI story for **every** function that compiles under that directive or calls that binding — not just the diff. When directives, `setup.py`, `pyproject.toml`, or `.pxd`/submodule pointers appear in the diff, the review covers the affected surface of the whole extension, not just the touched lines.
- A single finding of the form "in `dataframe.pxi` the new behavior of `Buffer.column` leaks `b.validity` on the exception path" is worth more than five findings inside the diff.

### Agents

Launch the following agents in parallel.

**Agent 1 — Correctness & bugs:** `None`/NULL handling, edge cases, logic errors, off-by-one, operator precedence, error paths. Integer correctness across the Python↔C boundary: Python `int` → `int64_t`/`size_t` conversion and overflow, `<int>` / `<Py_ssize_t>` / `<size_t>` casts that truncate or wrap, signed/unsigned mismatches, negative-length math. NaN/inf float handling. Timestamp unit conversions (micros vs nanos). Correct ILP wire format (v1 / v2). Cross-reference every changed symbol against its callsite inventory and verify the new behavior is correct at each callsite.

**Agent 2 — Cython memory, refcount & crash surface:** In a Cython extension, anything that corrupts memory or aborts the native side takes down the host Python interpreter with no traceback. Flag every reachable instance of:

- **Native memory leaks / double-free / use-after-free:** every `malloc`/`calloc`/`realloc` must be `free`d on **all** paths — success, early `return`, and the exception/`except` path (prefer `try/finally`). A `realloc` whose return value is assigned back to the same pointer leaks the original on failure (it returns `NULL` without freeing). Freeing a pointer twice, or using it after `free`, corrupts the heap.
- **Reference-count errors:** every `Py_INCREF` needs a matching `Py_DECREF` on all paths; a missing `DECREF` leaks, an extra `DECREF` causes a later use-after-free crash. Borrowed references (`PyWeakref_GetObject`, dict/list borrows, `PyObject*` stored without incref) must not outlive their owner. Verify `PyCapsule` and weakref handling.
- **Buffer-protocol imbalance:** every `PyObject_GetBuffer` must have a matching `PyBuffer_Release` on all paths, and the raw pointer must not be used after the exporting object can be collected.
- **Indexing under `boundscheck=False`:** per 2.5e, C-array and typed-memoryview indexing is unchecked — an out-of-range or negative index is UB, not an exception. Verify bounds are established before every index on the hot path.
- **Silent exception swallowing:** a `cdef` function returning a C type without the correct `except` clause (or `noexcept`) drops Python exceptions on the floor, turning an error into wrong data. Verify the `except` convention against what the body raises.
- **Direct aborts:** any reachable `abort()` (it is imported), and any **Rust panic crossing the C ABI** (from `c-questdb-client` or `rpyutils`) — both terminate the interpreter. The only defense is that the native side returns an error code/`line_sender_error*`, never panics.
- **Uninitialized memory:** a struct field or `malloc`'d region read before it is written (use `calloc` or explicit init), especially partially-built `pyobj_built_t`-style structs on an error path that then get freed.

State the relevant build facts (directives, exception default, submodule commit) from 2.5e in the agent's first sentence, and evaluate every finding under the actual settings, not the textbook defaults.

**Agent 3 — C-ABI boundary safety:** Check every call into the `c-questdb-client` / `rpyutils` C ABI. Verify:
- **`.pxd` matches the C header.** For every changed or called C-ABI symbol, read the actual declaration in `c-questdb-client/include/questdb/ingress/*.h` (or `rpyutils/include/`) and confirm the `.pxd` declaration matches it exactly: parameter types, pointer/`const`-ness, return type, struct field order and types, enum discriminant order. A mismatch is silent memory corruption / ABI breakage. If the submodule pointer moved, verify against the **new** pinned commit.
- **NULL handling:** every pointer returned from a C function checked before dereference; every pointer argument that could be `NULL` handled.
- **Error object lifecycle:** every `line_sender_error*` obtained via an out-param is converted (`c_err_to_py`) and freed exactly once (`line_sender_error_free`) — never leaked, never double-freed, never freed then read.
- **Ownership transfer:** `line_sender_buffer`, `line_sender_utf8`, `qdb_pystr_buf`, `line_sender` handles — who allocates, who frees, and is the lifetime correct relative to the owning `cdef class` (`__cinit__`/`__dealloc__`)?
- **`qdb_pystr_buf` arena invalidation:** UTF-8 pointers handed to Rust must remain valid until the buffer write completes and must not be invalidated by an intervening arena `clear`/append.
- **String encoding:** Python `str` → UTF-8 (`line_sender_utf8`), correct length passed, no lone surrogates, embedded-NUL handling, `bytes` vs `str` distinction.

**Agent 4 — GIL & concurrency:** Verify:
- **`nogil` correctness:** no `with nogil` block (or `cdef ... nogil` function) touches a Python object, calls the CPython C-API, raises a Python exception, or `INCREF`/`DECREF`s — doing so without the GIL is a crash/corruption. Errors discovered under `nogil` must be deferred and raised after reacquiring the GIL.
- **GIL release around blocking calls:** the flush/connect/network C calls should release the GIL (`with nogil`) so other threads run; verify the released region doesn't reference Python state.
- **Thread-safety:** `Sender`, `Buffer`, and the `active_senders` registry (`rpyutils/src/active_senders.rs`) — verify documented thread-safety matches the implementation, and that shared mutable state reachable from multiple threads is synchronized. Cross-reference every callsite from 2.5b for violations of the concurrency contract.
- **Free-threaded build:** if the change assumes the GIL serializes access, note whether it holds under a free-threaded (no-GIL) CPython build (the CI matrix includes `*t` free-threaded targets).

**Agent 5 — Resource management & lifecycle:** Leaks on all code paths (especially errors). Check `__cinit__`/`__dealloc__` pairing on every `cdef class` (does `__dealloc__` free everything `__cinit__` and methods allocated, and is it safe when `__cinit__` failed partway?). Native handle lifecycle (`line_sender`, `line_sender_buffer`, `qdb_pystr_buf`). Socket/connection/TLS teardown on error (handled by Rust, but verify the Cython side calls close/free). **Arrow C Data Interface:** `ArrowArray`/`ArrowSchema`/`ArrowArrayStream` `release` callbacks invoked exactly once; PyCapsule consumption semantics correct; no double-release. Walk every callsite from 2.5b that constructs, owns, or transfers ownership of a native handle and verify cleanup on all paths (success, exception, early return).

**Agent 6 — Performance & allocations:** Unnecessary work on hot paths — the per-row buffer build (`Buffer.column`/`symbol`/`row`) and the per-column DataFrame loop (`dataframe.pxi`). Flag: Python-level operations (attribute lookups, `dict` access, object boxing, `str` re-encoding) inside the inner per-row/per-cell loop that should be hoisted or done at C level; allocations per row/cell that should be amortized; excessive copying of data that could be zero-copy via the buffer protocol / Arrow; O(n²) patterns over rows or columns. Analyze scaling at realistic volume: millions of rows per flush, hundreds of columns. Setup-path costs (sender construction, config parsing, schema inspection done once per DataFrame) are acceptable; per-row/per-cell costs are not.

**Agent 7 — Test review & coverage:** Coverage gaps, error-path tests, `None`/edge-case tests, boundary conditions, regression tests, test quality. Check:
- Unit / mock-server tests in `test/test.py` (uses `test/mock_server.py`)
- System / integration tests against a real QuestDB in `test/system_test.py`
- DataFrame tests in `test/test_dataframe.py`, fuzz tests in `test/test_client_dataframe_fuzz.py` / `test/test_dataframe_fuzz.py`, and **leak tests** in `test/test_dataframe_leaks.py` (new native-memory or refcount handling should have a leak test)
- Capsule / Arrow path tests in `test/test_client_capsule_path.py`
- Examples in `examples/` still run (and `examples.manifest.yaml` is consistent)

Cross-reference 2.5d: every cross-context exposure should have a test that exercises the changed symbol from that context. Missing tests for cross-context callsites — especially a new native-memory path without a leak test, or a new C-ABI binding without a system test — is a high-priority finding.

**Agent 8 — Code quality & API design:** Public API ergonomics and consistency. **`ingress.pyi` stub must match the implementation** (signatures, defaults, return types, new symbols added to `__all__`). Docstrings on public classes/methods. `CHANGELOG.rst` updated for user-visible changes. Backward compatibility of the Python API (renamed/removed kwargs, changed defaults, changed exception types) — breaking changes must be intentional and called out in the PR body. Naming consistent with the codebase. No dead code, no unused `cimport`/`import`. Docs under `docs/` updated for API changes.

**Agent 9 — Cross-context caller impact:** Walk the callsite inventory from 2.5b. For every callsite, fetch the surrounding code (the calling function plus its callers up two levels) and answer:

- Does this caller pass inputs the new behavior handles incorrectly?
- Does this caller depend on a contract from the implicit contract list (2.5c) that the change broke — e.g. relying on the old `except` convention, the old ownership of a buffer, the old `qdb_pystr_buf` lifetime, the old refcount behavior?
- Is this caller in a context (a `with nogil` block, the per-row hot loop, an auto-flush trigger, an Arrow release callback, a `__dealloc__`, an exception/error path) where the new behavior misbehaves even if the inputs are valid?
- For a changed `cdef`/`cpdef` exception convention: do all callers still detect and propagate the error?
- For a changed C-ABI declaration: does the `.pxd` still match the C header, and do all Cython callers pass the right types/ownership?
- For a changed buffer/sender state machine: do all callers respect the new state transitions (buffer cleared after error before reuse; flush only when flushable)?

This agent's output is structured per callsite, not per failure mode. Each callsite gets a verdict: SAFE / BROKEN / NEEDS VERIFICATION. Every BROKEN entry is a P0 finding regardless of whether the file is in the diff.

This agent is not optional even when the diff is small. Small diffs to widely-used symbols (`Buffer.column`, `Sender.flush`, the dataframe entry point, a C-ABI binding) have the largest blast radius.

**Agent 10 — Fresh-context adversarial:** Dispatched separately from agents 1-9 to escape checklist anchoring. This agent operates under different rules from the rest:

- It receives ONLY the PR diff and the names of the changed files. It does NOT receive the change surface map from Step 2.5, the implicit contract list, the cross-context exposure list, or any of the review checklists below.
- Its sole instruction: "find ways this code is wrong". No category list, no failure-mode taxonomy, no project-specific style guide.
- It is free to use Read, Grep, and Glob to explore the repository however it wants.
- Findings are not pre-classified by category. Each finding states: what's wrong, why it's wrong, and the code path that demonstrates it.

The point of this agent is to surface bugs the structured agents cannot see because they are reasoning inside the same frame. A finding here that none of agents 1-9 produced is high signal — it means the structured review missed it. A finding here that overlaps with agents 1-9 is corroboration.

Run this agent in parallel with agents 1-9. It is mandatory regardless of diff size.

Combine all agent findings into a single deduplicated **draft** report. Do NOT present this draft to the user yet — it goes straight into verification.

## Step 3b: Verify every finding against source code

The parallel review agents work from the diff plus the change surface map and frequently produce false positives — especially around native memory ownership, refcounting, GIL boundaries, Cython exception conventions, and C-ABI lifecycle. Every finding MUST be verified before it is reported.

For each finding in the draft report:

1. **Read the actual source code** at the exact lines cited (in the `.pyx`/`.pxi`/`.pxd`/`.pyi`, never the generated `ingress.c`). Do not rely on the agent's description alone.
2. **Trace the full code path:** follow callers and `cdef` helpers. Remember Cython's `include` model — `dataframe.pxi` and `egress.pxi` are textually included into `ingress.pyx`, so symbols are shared across them.
3. **Check both sides of the C ABI:** if a finding involves Cython↔Rust interaction, read both the Cython call and the C header in `c-questdb-client/include/questdb/ingress/` (or `rpyutils/include/`). Verify ownership transfer, error propagation, and freeing on both sides.
4. **For native-memory-leak claims:** trace every `malloc`/`calloc`/`realloc` to its `free` on ALL paths (success, early return, `except`/exception unwind). Confirm the intervening code can actually raise before claiming the exception path leaks.
5. **For refcount claims:** count `Py_INCREF`/`Py_DECREF` on every path; confirm borrowed-vs-owned reasoning against the CPython C-API contract of each function used.
6. **For exception-swallowing claims:** check the actual `except` clause on the `cdef` and whether the body can raise. Under Cython 3 a `nogil` `cdef` defaults to `noexcept` — confirm whether that's the real declaration.
7. **For GIL claims:** verify the cited code is actually inside a `nogil` region and actually touches a Python object / C-API; a `cdef` function called from `nogil` may itself acquire the GIL.
8. **For C-ABI / `.pxd` mismatch claims:** read the exact declaration in the pinned header and compare field-by-field. A claimed mismatch that actually matches is a false positive.
9. **For numeric overflow/truncation claims:** check reachability at realistic scale — ILP buffers up to a few hundred MB, millions of rows per flush, columns in the tens to low hundreds. Drop overflows that require values beyond that scale.
10. **For performance claims:** confirm the cost is on the per-row/per-cell hot path and measurable relative to surrounding I/O. Downgrade negligible savings to a nit. Exception: a per-row or per-cell allocation / Python-object operation on the buffer-build path is always worth flagging.
11. **For cross-context findings (Agent 9):** re-read the callsite in full, including callers up two levels, and confirm the broken behavior is reachable from production or test paths users will exercise.

**Classify each finding** as:
- **CONFIRMED in-diff** — the bug is real and inside the diff
- **CONFIRMED at out-of-diff callsite** — the bug is in an unchanged file because the changed symbol is used there in a way that's now broken (cite the file and the contract from 2.5c that was violated)
- **FALSE POSITIVE** — the code is actually correct (explain why)
- **CONFIRMED with nuance** — the issue exists but is less severe than stated (explain)

**Move false positives to a separate "Downgraded" section** at the end of the report. For each, give a one-line explanation of why it was dismissed. This lets the PR author verify the reasoning and catch verification mistakes.

Launch verification agents in parallel where findings are independent. Each verification agent should read surrounding source files, not just the diff.

Every finding that survives verification goes into Step 3c before it reaches the report.

## Step 3c: Broaden every confirmed finding

Runs on every finding Step 3b classified **CONFIRMED in-diff**, **CONFIRMED at out-of-diff callsite**, or **CONFIRMED with nuance**. False positives skip this step.

A confirmed finding is a sample, not the population. Reported as it was found, it earns a one-line fix — and the next review of this repo finds the same bug two files over, or finds that the fix moved the bug rather than removing it. Before a finding reaches the report, run the four probes below against it and write a verdict for each. The first three are the patterns that recur; the fourth is the open one that keeps the list from being treated as complete.

Both verdicts of each probe are acceptable outcomes: a finding that survives all four unchanged is reported exactly as verified, with one line recording that the probes ran. What is not acceptable is asserting a probe's negative verdict without the search or the trace that supports it — the same evidence rule as 2.5b.

### 3c.A Instance or class?

The pattern: the review found one or two occurrences of a mechanism that the codebase repeats. Fix those two and the next round finds two more, indefinitely.

1. **Name the mechanism, not the line.** "A `qdb_pystr_buf` pointer held across an arena append." "A `cdef` returning `int` with no `except` clause." "A `malloc` freed only on the success path." "A `.pxd` block drifted from the pinned header." If the mechanism can only be stated by quoting the finding's own line numbers, it has not been named yet.
2. **Search the whole repository for that mechanism** — not for the changed symbol, and not only inside the diff. Match on the mechanism's shape:

| Mechanism in the finding | Search that finds the rest of the class |
|---|---|
| `cdef` returning a C type with a missing/wrong `except` clause | `grep -rn 'cdef .*)' src/questdb/*.pyx src/questdb/*.pxi src/questdb/*.pxd`, then filter to non-object return types and read each `except` clause |
| Allocation freed only on the success path | `grep -rn 'malloc\|calloc\|realloc\|free(' src/questdb/*.pyx src/questdb/*.pxi` and pair each allocation with its `free` on every path |
| `line_sender_error*` freed at the callsite rather than by one converting helper | `grep -rn 'line_sender_error\|c_err_to_py\|line_sender_error_free' src/questdb/` |
| `PyObject_GetBuffer` without a `finally`-guarded release | `grep -rn 'PyObject_GetBuffer\|PyBuffer_Release' src/questdb/` |
| Refcount op unbalanced on an error path | `grep -rn 'Py_INCREF\|Py_DECREF\|Py_XDECREF' src/questdb/` |
| `.pxd` declaration disagreeing with the C header | compare the **whole** declaration block, not the changed line: `grep -rn '<symbol>' c-questdb-client/include/questdb/ingress/*.h rpyutils/include/*.h` |
| Arena pointer held across a `qdb_pystr_buf` append/clear | `grep -rn 'qdb_pystr_buf\|_utf8' src/questdb/*.pyx src/questdb/*.pxi` |
| Arrow `release` callback invoked on some paths only | `grep -rn 'release\|ArrowArray\|ArrowSchema\|Capsule' src/questdb/dataframe.pxi` |
| Python-level operation in a per-row/per-cell loop | read the whole loop body and the sibling loops in `dataframe.pxi`, not the changed line |

3. **Write the verdict** with the search that backs it: `isolated — searched <command> over <files>, this is the only instance`, or `class — N instances at <file:line>, <file:line>, …`.
4. **A class-level finding is reported once, not N times.** List every instance, and suggest a fix that makes the whole class impossible — a shared helper, a `try/finally` at the allocation site, one `except` convention applied to the whole family of `cdef`s, a single converting function that owns the error lifecycle — rather than N spot fixes.
5. **Every additional instance goes back through Step 3b verification before it is reported.** Broadening does not get to skip verification; an unverified instance list re-introduces exactly the false positives 3b removed.

### 3c.B Leaf or mechanism?

The pattern: the finding names the exact place where a broken mechanism happens to surface. The obvious fix satisfies that place and leaves the mechanism free to fail at the next place it runs.

1. **Write the one-line fix the author would obviously apply.** Then ask what that fix actually asserts — a bound, an ownership rule, an initialization, a state transition.
2. **Ask where that assertion belongs.** Compare the leaf where the failure is visible against the deepest point every failing path shares: the `cdef` helper, `__cinit__`, the arena API, the one function that converts a `line_sender_error*`, the single place a row is committed to the buffer. A fix at the leaf holds only if the leaf is the sole entry to the mechanism.
3. **Enumerate the other entries from the callsite inventory (2.5b)** — do not assume there is one. For each, state whether the precondition the leaf fix asserts already holds there.
4. **Re-run the mechanism on the same object in your head:** the next row, the next column, the next flush, the retry after a failed flush, a second pass through `__dealloc__`. A fix that holds once and not on repetition is a leaf fix.
5. **Verdict:** `leaf fix sufficient — the mechanism has one entry at <file:line>`, or `mechanism lives at <file:line>; a leaf fix leaves <callsites> broken, so the fix belongs there`. When the mechanism verdict applies, the report states the mechanism, the leaf that exposed it, and the other callsites that stay broken under the obvious fix.

### 3c.C Conflicting invariants?

The pattern: the finding is real, and so is the reason the code is written that way. Two contracts cannot both hold in the current design, so the obvious fix restores one invariant by breaking the other.

1. **State the invariant the finding says is violated**, in one sentence.
2. **Find the invariant the current code holds instead.** Working code rarely breaks a rule for nothing. Read the surrounding code and the tests that cover it, and name what the present shape is protecting. Pairs that pull against each other in this codebase — examples, not a closed list:
   - The `Buffer` is usable after a failed flush ↔ rows already handed to the transport are not sent twice
   - `qdb_pystr_buf` pointers stay valid until the write completes ↔ the arena is cleared per row/flush to bound memory
   - The blocking flush runs under `nogil` ↔ an error discovered inside it has to become a Python exception
   - `__dealloc__` frees the native handle ↔ the sender may still be registered in `active_senders` and reachable from another thread
   - Auto-flush fires on a row/byte threshold ↔ a row reaches the wire only once complete
   - A `cdef` is `noexcept`/`nogil` for the hot path ↔ the error it detects has to reach Python
   - The `.pyi` stub and `CHANGELOG.rst` describe a stable public API ↔ the C-ABI binding must follow the pinned submodule's semantics
3. **Apply the proposed fix on paper and walk the opposite invariant's paths**, naming the callsite and the test that would break.
4. **Verdict:** `no conflict — the fix preserves <other invariant> because <reason>`, or `conflict — the fix breaks <other invariant> at <file:line>`.
5. **A conflict finding is reported as a design issue, not a patch.** State both invariants, where each is enforced, and why the obvious fix trades one bug for another; then give the design options rather than a one-line suggested fix. **Severity goes up, not down** — a fix that silently breaks the opposing invariant is worse than the bug as reported, because the second bug arrives without a review.

### 3c.D Any other way this is bigger?

A, B and C are the three patterns that recur most. They are not the closed set. The generative question for everything else: **what would have to be true elsewhere in the repo for this finding to be exactly as small as it looks?** — then check whether it is true.

Axes worth one pass each, with the evidence that settles them:

- **Mirror path.** This repo carries symmetric implementations: ILP wire format v1 ↔ v2, HTTP ↔ TCP sender, pandas ↔ polars ↔ pyarrow ↔ Arrow-capsule entry points, one `Buffer.column_*` per dtype, explicit flush ↔ auto-flush. A bug in one arm is usually sitting in its mirror. Evidence: read the sibling implementation in full — a grep for the changed symbol will not find it, because the mirror spells everything differently.
- **Inverse operation.** If the bug is in acquire, read release: `__cinit__` → `__dealloc__`, `PyObject_GetBuffer` → `PyBuffer_Release`, `Py_INCREF` → `Py_DECREF`, register in `active_senders` → unregister, encode → the decode in `test/mock_server.py`. Evidence: the paired function, read in full.
- **Escalated consequence.** The finding is filed as one class of failure — check whether the same trigger reaches a worse one under a condition that is also reachable: unchecked indexing under `boundscheck=False`, the same path entered under `nogil`, the free-threaded build, a Rust panic crossing the ABI where the draft says "leak". Evidence: the build facts from 2.5e plus the path trace. Deepening severity is a form of broadening, and it changes the merge decision.
- **Repo boundary.** Does the root cause live in the pinned `c-questdb-client` or `rpyutils` rather than here? Then the Cython change is a workaround: say so, name the upstream file, and state what happens at the next submodule bump. Evidence: the pinned header or Rust source, read.
- **Stated contract.** If the code is wrong, check whether `ingress.pyi`, the docstring, `docs/`, and `CHANGELOG.rst` describe the wrong behavior too. A code-only fix leaves users writing against a contract that documents the bug. Evidence: the four contract surfaces, checked.
- **History.** `git log -S'<mechanism>' -- src/questdb/` and `git log --grep`. A bug that was fixed before and came back means the real finding is that nothing guards it, and the fix is the guard, not the patch.

**The open slot.** If none of these fit and the finding still reads smaller in the report than it looked in the code, name the axis yourself and apply it. A self-named axis is reportable only when it produces the same artifact the named ones do — a search that was run, a file that was read, a callsite named. "This may also affect X", with nothing behind it, is not a broadening: drop it and report the finding as verified.

One pass over the axes, not a hunt for exhaustiveness. Stop at the first axis that changes the finding's scope, and if two axes point at the same larger finding, report it once.

### Level gating for this step

- **Levels 0 and 1:** run the probes inline in the main loop. Probe A's search is mandatory — it is one grep per finding. B and C are answered from the code already read during verification. From D, run the mirror-path and inverse-operation axes; they are the two that pay off most in this repo and both are a single file read.
- **Level 2:** one batched broadening agent for all confirmed findings, receiving the confirmed list, the change surface map from 2.5, and this section. It runs A, B, C and the full D axis pass.
- **Level 3:** one broadening agent per confirmed finding, in parallel, followed by a single merge pass that re-reads all broadened findings together and collapses any two that turn out to be the same mechanism.

Do not broaden without evidence. A finding that comes back isolated on all four probes is a complete finding, and padding it with speculative instances costs the report its credibility.

## Review checklists

Review the diff for:

### Correctness & bugs
- `None`/NULL handling at API boundaries
- Edge cases and error paths
- Logic errors, off-by-one, incorrect bounds, wrong operator precedence
- Integer overflow/truncation across the Python↔C boundary (`int` → `int64_t`/`size_t`, `<int>`/`<Py_ssize_t>` casts, signed/unsigned)
- Float edge cases (NaN, inf), timestamp unit conversions (micros vs nanos)
- Correct ILP wire format (v1 / v2)
- **Reachability expansion:** for each changed symbol, list the new contexts it can appear in (DataFrame path, `nogil` section, auto-flush, Arrow callback, error path) and verify it works in each.

### Cython memory & refcount safety
- Every `malloc`/`calloc`/`realloc` freed on success, early-return, and exception paths (prefer `try/finally`); no double-free, no use-after-free; `realloc`-failure path doesn't leak the original
- Every `Py_INCREF` matched by `Py_DECREF`; borrowed references not outliving their owner; weakref/capsule handling correct
- Every `PyObject_GetBuffer` matched by `PyBuffer_Release`; exporter kept alive while the pointer is used
- Correct Cython `except` convention on every `cdef`/`cpdef` returning a C type (no silent exception swallowing; `noexcept` is the Cython-3 default for `nogil` `cdef`)
- No reachable `abort()`, and no Rust panic crossing the C ABI (both kill the interpreter)
- Indexing safe under the active `boundscheck`/`wraparound` directives
- No uninitialized struct/heap memory read (use `calloc` or init before use, especially on partially-built error paths)

### C-ABI boundary
- `.pxd` declarations match `c-questdb-client/include/questdb/ingress/*.h` (and `rpyutils/include/`) exactly — types, `const`, struct layout, enum order, return type — against the **pinned** submodule commit
- All pointers returned from C checked for NULL before dereference
- Every `line_sender_error*` freed exactly once (`line_sender_error_free`), never double-freed or leaked
- Ownership semantics clear and correct (who allocates the handle, who frees it, lifetime vs the owning `cdef class`)
- `qdb_pystr_buf` arena pointers stay valid until consumed; not invalidated by an intervening `clear`/append
- String handling: `str` → UTF-8 with correct length, lone-surrogate rejection, embedded-NUL handling, `bytes`/`str` distinction
- ABI stability: a submodule bump that reorders a struct or renumbers an enum requires matching `.pxd` updates

### GIL & concurrency
- No Python object access / C-API call / refcount op / raise inside a `with nogil` block or `cdef ... nogil` function
- GIL released around blocking network/flush C calls; released region references no Python state; errors deferred and raised after reacquiring
- `Sender`/`Buffer`/`active_senders` thread-safety matches documentation; shared mutable state synchronized
- Assumptions that the GIL serializes access re-checked for the free-threaded CPython build

### Performance
- No per-row/per-cell Python-level operations (attribute/dict lookups, boxing, `str` re-encoding) in the buffer-build or DataFrame inner loops that belong at C level or hoisted to setup
- No per-row/per-cell allocations that should be amortized
- Zero-copy where possible (buffer protocol, Arrow) instead of copying
- No O(n²) over rows or columns at realistic scale (millions of rows, hundreds of columns)

### Resource management
- `__cinit__`/`__dealloc__` pair frees everything allocated, and `__dealloc__` is safe after a partially-failed `__cinit__`
- Native handles (`line_sender`, `line_sender_buffer`, `qdb_pystr_buf`) released on all paths
- Socket/connection/TLS cleanup on error (Cython side invokes the Rust close/free)
- Arrow `release` callbacks invoked exactly once; PyCapsule consumed correctly; no double-release
- No leak through the C-ABI boundary (ownership documented and consistent)

### Code quality
- `ingress.pyi` stub matches the implementation (signatures, defaults, return types, `__all__`)
- Public API consistent and ergonomic; backward-compatible (or breaking changes called out in the PR body)
- `CHANGELOG.rst` updated for user-visible changes; `docs/` updated for API changes
- Docstrings on public classes/methods
- Naming consistent with the codebase; no dead code or unused `import`/`cimport`

### Test review
- **Coverage gaps:** every new/changed code path has a corresponding test; flag missing ones explicitly as "missing test for X"
- **Cross-context coverage:** every entry in the cross-context exposure list (2.5d) has a test exercising the changed symbol from that context
- **Leak coverage:** new native-memory or refcount-handling code has a test in `test/test_dataframe_leaks.py` (or equivalent)
- **Error-path coverage:** failure cases, partial writes, connection drops, TLS/auth failures, server rejections, and edge conditions tested — not just the happy path
- **Edge-case tests:** `None`, empty buffers, zero-length strings, max-length symbols, boundary integers, NaN/inf, non-UTF-8 strings
- **C-ABI / binding changes** covered by a system test in `test/system_test.py`
- **DataFrame / Arrow changes** covered in `test/test_dataframe.py` and the fuzz/capsule tests
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

Present ONLY verified findings (false positives are excluded from Critical/Moderate/Minor). Structure as:

### Critical
Issues that must be fixed before merge. Each must include:
- Exact file path and line numbers (including out-of-diff files)
- Whether the finding is **in-diff** or **out-of-diff**
- A **Scope** line from Step 3c, one of:
  - `Scope: isolated` — plus the search that established it
  - `Scope: class — N instances` — every instance listed as `file:line`, with a fix that removes the class
  - `Scope: mechanism at <file:line>` — the leaf that exposed it, and the callsites a leaf fix leaves broken
  - `Scope: invariant conflict with <invariant>` — both invariants stated, design options instead of a one-line fix
  - `Scope: <axis> — <what it reached>` — a 3c.D outcome, naming the axis and its evidence (e.g. `Scope: mirror path — same bug in the polars arm at dataframe.pxi:812`)
- Code path trace showing why the bug is real
- For out-of-diff findings: the contract from 2.5c that was violated and the callsite that triggers it
- Suggested fix

### Moderate
Issues worth addressing but not blocking. Each carries its Step 3c `Scope` line.

### Minor
Style nits and suggestions. Each carries its Step 3c `Scope` line.

### Downgraded (false positives)
Findings from the initial review that were dismissed after source code verification. For each, state:
- The original claim (one line)
- Why it was dismissed (one line, citing the specific code that disproves it)

### Summary
- One-line verdict: approve, request changes, or needs discussion
- Highlight any regressions or tradeoffs
- State how many draft findings were verified vs dropped as false positives (e.g., "8 findings verified, 4 false positives removed")
- State the Step 3c outcome: how many findings broadened into a class (with the total instance count), how many were relocated to a mechanism deeper than the leaf, how many turned out to be invariant conflicts, and how many grew along a 3c.D axis (naming the axis). If every confirmed finding came back `isolated`, say so explicitly and name the searches — an all-isolated result on a non-trivial diff usually means Probe A searched for the changed symbol instead of the mechanism.
- State the in-diff vs out-of-diff split (e.g., "5 findings in-diff, 3 findings out-of-diff"). If the diff is non-trivial and out-of-diff is zero, the cross-context pass likely underran — re-invoke Agent 9 with a wider grep before finalizing.
