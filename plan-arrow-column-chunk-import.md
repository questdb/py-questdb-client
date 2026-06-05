# Design: fix use-after-free in the chunked Arrow columnar ingest path

Status: proposed
Branch: `jh_experiment_new_ilp`
Scope: `src/questdb/ingress.pyx`, `src/questdb/dataframe.pxi`, `c-questdb-client/questdb-rs`, `c-questdb-client/questdb-rs-ffi`, `c-questdb-client/include/questdb/ingress/column_sender.h`, `c-questdb-client/cpp_test/test_arrow_c.c`

---

## 1. TL;DR

The chunked pandas/columnar ingest path imports each Arrow‑backed column from the
Arrow C Data Interface **once per output chunk**, but that import is a destructive
ownership transfer into arrow‑rs. On any DataFrame large enough to span more than
one output chunk, the second chunk re‑imports an array whose buffers were already
freed when the previous chunk was cleared → **use‑after‑free**. It affects real
ingestion, not just the benchmark/test; the test
`test_bench_dataframe_plan_and_populate_aligns_nullable_chunks` is the smallest
reproducer.

The chosen fix changes the model: **import each Arrow‑routed column into a
plan‑owned Rust handle exactly once (memoized on first append), then append
slices of that imported column per output chunk.** `questdb-rs` owns the imported
column abstraction and keeps the Arrow `ArrayRef`, schema `Field`, and cached
QuestDB `ColumnKind` private. The Cython/FFI boundary sees only an opaque handle.
Validation and wire‑kind classification run once; ownership becomes a single
owned object whose lifetime is explicit.

---

## 2. Background: the chunked columnar path

For pandas / Arrow‑capsule input the client builds a `dataframe_plan_t` and emits
the frame as a sequence of fixed‑size **output chunks** so a huge frame never has
to be materialized as one wire buffer. The same `column_sender_chunk` object is
reused for every output chunk.

Driver loop (production flush, `src/questdb/ingress.pyx:4859-4876`; the bench
hook `_bench_dataframe_plan_and_populate_column_chunks` at `:4036-4050` is
identical):

```text
row_offset = 0
while row_offset < plan.row_count:
    column_sender_chunk_clear(chunk)          # reset & reuse the chunk
    chunk_rows = min(rows_per_chunk, remaining)
    _dataframe_columnar_populate_chunk(&plan, chunk, row_offset, chunk_rows)
    _dataframe_columnar_flush(conn, chunk, ...)   # (bench: count only)
    row_offset += chunk_rows
```

`_dataframe_columnar_populate_chunk` walks every column and dispatches by
`(target, source)`. There are two families of per‑column emitter:

- **Raw‑pointer columns** (i64, f64, numpy datetime, bool, designated timestamp,
  pre‑built PyObject buffers): the Cython side reads the underlying buffer pointer
  directly and offsets it by `row_offset`, e.g.
  `data = chunks[0].buffers[1]; append(data + row_offset, row_count, validity)`
  (`ingress.pyx:3317`, `:3573`). These **borrow** — they never take ownership, so
  the plan’s single teardown release covers them and they work for any number of
  chunks.

- **Arrow‑routed columns** (symbol/dictionary, utf8 / large_utf8 string,
  i8/i16/i32/f32, uuid/long256 from Arrow, tz‑aware datetime): too complex to
  hand‑encode on the Cython side, so they go through the generic Arrow importer
  `_dataframe_columnar_call_arrow_append` (`ingress.pyx:3246-3265`), which calls the
  FFI `column_sender_chunk_append_arrow_column(&chunks[0], &schema, row_offset,
  row_count)`. (The FFI importer is general enough for more types, but the pandas
  planner only routes this subset here — see §9.1; e.g. Arrow `i64`/`f64`/`u32` are
  emitted as raw‑buffer numpy appends, and `ipv4`/`decimal` don't reach the manual
  path at all.)

The plan **owns** the Arrow arrays. `_dataframe_export_arrow_chunks`
(`dataframe.pxi:1211-1223`) exports the pandas/pyarrow array into
`col.setup.chunks.chunks[0]` via `_export_to_c`, installing pyarrow’s `release`
callback. `col_t_release` (`dataframe.pxi:606-617`) calls `chunk.release(chunk)`
once at teardown. Note `n_chunks` is constrained to `1` for these columns
(`ingress.pyx:2147`): there is exactly one physical Arrow chunk, sub‑sliced per
output chunk by `row_offset`/`row_count`.

---

## 3. The bug

### 3.1 Mechanism

`column_sender_chunk_append_arrow_column` (`questdb-rs-ffi/src/column_sender.rs:1147`)
routes through `arrow_ffi_import_array_sliced` (`questdb-rs-ffi/src/lib.rs:4061`),
which **consumes** the caller’s array:

```rust
let imported_array = std::ptr::read(array);   // owned copy keeps real `release`
(*array).release = None;                       // null the source so the caller won't double-free
let array_data = arrow::ffi::from_ffi(imported_array, &*schema)?;
array_data.validate_full()?;                   // O(rows), runs every call
let full = make_array(array_data);
Ok(full.slice(row_offset, row_count))          // sliced ArrayRef pushed into the chunk
```

The resulting `ArrayRef`’s `Arc<FFI_ArrowArray>` keeper holds the *real* pyarrow
`release`. The chunk holds that `ArrayRef` until it is cleared/freed.

Now replay the loop for a 2‑output‑chunk column:

1. **Output chunk 1** (`row_offset=0`): import nulls `chunks[0].release`, builds
   `ArrayRef` A1 (keeper owns the real release), chunk holds A1.
2. **Output chunk 2** (`row_offset=k`): the loop first calls
   `column_sender_chunk_clear(chunk)`. `Chunk::clear()`
   (`questdb-rs/src/ingress/column_sender/chunk.rs:326-330`) does
   `self.columns.clear()` → drops A1 → `Arc` refcount hits 0 →
   `FFI_ArrowArray::drop` invokes the **original pyarrow release** → **the
   dictionary / offset / data buffers are freed.** Then `populate_chunk` re‑reads
   `chunks[0]` — `release` is now `None` but `buffers[*]` still point at the freed
   memory — and `from_ffi` + `validate_full()` walk freed memory.

That is the reported failure: "null_count value (3) doesn't match actual number of
nulls" and "bogus UTF-8 offsets". Being a heap‑state‑dependent UAF, it manifests
intermittently — hence it surfaces in wheel jobs.

### 3.2 Scope

This is **not** test‑only. The production flush loop (`ingress.pyx:4859-4876`) has
the identical `clear → populate → flush` structure, so any real ingestion of an
Arrow‑routed column on a frame larger than `rows_per_chunk` (default 16384) hits
the same UAF. The failing test merely forces a 2‑chunk split with
`max_rows_per_chunk=3` over a 10‑row categorical column.

### 3.3 Why the other paths are fine

Raw‑pointer columns never consume; they borrow `chunks[0].buffers[*]`, which stays
valid for the whole plan lifetime, and the plan releases once at teardown. Only the
Arrow‑routed path conflates "import" with "take ownership" and then repeats it per
chunk.

---

## 4. Goals and constraints

- **Correctness first:** eliminate the UAF on every Arrow‑routed column type for
  any number of output chunks.
- **Performance:** the chunked path exists for large frames; per‑chunk cost must
  not be O(rows). Validation should run once, not per chunk.
- **FFI is internal, but avoid unnecessary API churn:** add the handle functions
  needed by the Python chunked path, but keep the existing C/C++ consuming
  appender for compatibility. Hardening/deprecating/removing that public helper is
  a separate API cleanup.
- **Bigger changes are allowed, but keep the fix targeted:** it is fine to add a
  narrow `questdb-rs` imported-column abstraction so classification remains
  private to the Rust chunk/encoder layer. Do not collapse the whole manual
  pandas planner into the Rust Arrow capsule route in this fix; that is a
  separate behavior/API cleanup.
- Keep the raw‑pointer fast paths untouched (they are already correct and optimal).

---

## 5. Options considered

### Option A — Borrowed appender (localized, minimal)

Add a sibling `column_sender_chunk_append_arrow_column_borrowed` that does **not**
null the caller’s `release`; instead it nulls `release` on the *copy* handed to
`from_ffi` (or installs a no‑op release stub), so the chunk’s `ArrayRef` keeper
never frees the buffers and the plan releases them once at teardown. Validate only
when `row_offset == 0`.

- Pros: tiny diff; no struct/teardown changes; leaves the array struct inert across
  chunks, matching the existing borrow model.
- Cons: per‑chunk re‑import + `ArrayData` rebuild (O(n_buffers)); a
  `validate = (row_offset==0)` flag couples validation correctness to call order;
  correctness is a **non‑local invariant** (plan must outlive the chunk, struct must
  never be mutated, teardown order must hold); relies on arrow‑rs internals
  (`from_ffi` not checking `is_released`, `Drop` being a no‑op on `None`); the
  destructive consume primitive remains as a misusable foot‑gun.

### Option B2 — Slice the pyarrow array in Python, export per chunk

Hold the pyarrow `Array` alive for the plan lifetime; per output chunk do
`pa_array.slice(off, len)._export_to_c(&tmp)` and feed `tmp` to the existing
consuming appender (each export is a distinct array consumed exactly once).

- Pros: the consume contract stays correct and untouched; no Rust struct changes.
- Cons: per‑chunk Python object creation + export under the GIL in the hot loop;
  `validate_full` still runs per chunk → O(rows × n_chunks). Worse performance and
  more GIL work — contrary to the goals.

### Option B — Import once, memoize, slice per chunk in FFI

Import each Arrow‑routed column into a plan‑owned FFI handle holding `ArrayRef`
and `Field`, then append slices via existing `Chunk::push_arrow_column`.

- Pros: fixes ownership with a small Rust API diff; reuses the current public
  `Chunk::push_arrow_column` seam; keeps `ColumnKind` private.
- Cons: classification repeats for every output chunk even though the logical
  QuestDB column kind is stable; the imported handle is an FFI concept rather
  than a `questdb-rs` column abstraction.

### Option C — `questdb-rs` owns an imported Arrow column  ← chosen

Import each Arrow‑routed column into a plan‑owned Rust handle exactly once, then
append slices of that handle per chunk. The handle is implemented in `questdb-rs`
as an `ImportedArrowColumn` with private `ArrayRef`, `Field`, and `ColumnKind`.
`questdb-rs-ffi` wraps that Rust object behind `column_sender_arrow_import*`.

- Pros: fixes ownership; validates once; classifies once; keeps `ColumnKind` and
  `push_arrow_deferred` private to `questdb-rs`; gives the chunk/encoder layer a
  natural future place for range-aware or multi-physical-chunk optimizations.
- Cons: larger than Option B because it adds a small public Rust abstraction and
  chunk method.

---

## 6. Chosen design: `questdb-rs` imported column, memoized in the plan

### 6.1 Idea

The Arrow C Data Interface contract is that an exported array’s `release` is called
**once**. Honor that: perform the C‑Data‑Interface → arrow‑rs import exactly once
per column, into an owned `questdb-rs` `ImportedArrowColumn` stored on the plan via
an opaque FFI handle. The imported column validates the full Arrow array and
classifies the logical QuestDB column kind once. Each output chunk bounds-checks
`row_offset`/`row_count`, slices the cached `ArrayRef`, and asks `questdb-rs` to
push that slice with the cached private `ColumnKind`.

Do **not** expose `arrow_batch::ColumnKind` to `questdb-rs-ffi`. The Rust API
boundary is the imported-column object plus a narrow chunk method such as:

```rust
pub struct ImportedArrowColumn {
    field: arrow_schema::Field,
    array: arrow_array::ArrayRef,
    kind: arrow_batch::ColumnKind,
}

impl ImportedArrowColumn {
    pub fn import_from_ffi(
        array: &mut arrow::ffi::FFI_ArrowArray,
        schema: &arrow::ffi::FFI_ArrowSchema,
    ) -> Result<Self>;
}

impl Chunk {
    pub fn push_imported_arrow_slice(
        &mut self,
        name: &str,
        imported: &ImportedArrowColumn,
        row_offset: usize,
        row_count: usize,
    ) -> Result<&mut Self>;
}
```

Internally, `push_imported_arrow_slice` uses the existing deferred Arrow machinery
(`push_arrow_deferred`) with the cached `kind`. Externally, the C ABI still exposes
only an opaque pointer.

Make the import **lazy / memoized** so the import‑vs‑slice decision lives in one
place and no build‑time "which columns are Arrow" predicate is needed.

### 6.2 FFI surface (questdb-rs-ffi)

Add an explicit import handle for the chunked Python path:

All three functions are pure C/FFI with no Python interaction, so they are
declared `noexcept nogil` on the Cython side and called inside `with nogil` (see
§6.3) — the one-time `validate_full()` must not run under the GIL.

```c
/* Opaque FFI wrapper around questdb-rs ImportedArrowColumn. */
typedef struct column_sender_arrow_import column_sender_arrow_import;

/* Consume `array` (C Data Interface) into an owned handle. Validates once.
 *
 * GUARD: rejects an already-consumed array. If `array->release == NULL` the
 * function fails (ArrowIngest / InvalidApiCall) and returns NULL *without*
 * touching the array — a double import (e.g. a memoization regression) can
 * never re-`ptr::read` a consumed struct and re-introduce the UAF class.
 *
 * On success `array->release` is consumed (set NULL); `schema` is borrowed.
 *
 * Failure ownership:
 * - Pre-consume failures (NULL pointers, `release == NULL`, depth/schema
 *   pre-walk, schema-to-Field conversion) leave `array->release` intact.
 * - Post-consume failures (`from_ffi`, `validate_full`, classification) have
 *   already transferred ownership; the function drops the owned temporary before
 *   returning NULL, and the source `array->release` remains NULL so the caller
 *   does not release it again. */
column_sender_arrow_import* column_sender_arrow_import_new(
    struct ArrowArray* array,
    const struct ArrowSchema* schema,
    line_sender_error** err_out);

/* Append [row_offset, row_offset+row_count) of the imported column to `chunk`.
 * Slices the cached ArrayRef and pushes it with the cached QuestDB column kind.
 * The produced slice co-owns the buffers via Arc, so it is independent of
 * chunk/plan teardown order. */
bool column_sender_chunk_append_arrow_import(
    column_sender_chunk* chunk,
    const char* name, size_t name_len,
    const column_sender_arrow_import* imported,
    size_t row_offset, size_t row_count,
    line_sender_error** err_out);

/* Drop the handle's reference. The original Arrow release runs exactly once when
 * the last retained ArrayRef/slice is dropped; this may be after this call if a
 * chunk still holds a slice. */
void column_sender_arrow_import_free(column_sender_arrow_import* imported);
```

Rust sketch:

```rust
pub struct column_sender_arrow_import {
    imported: questdb::ingress::column_sender::ImportedArrowColumn,
}

// _new: same validation/import as today's arrow_ffi_import_array_sliced, but:
//       1. GUARD first — reject if (*ffi_array).release.is_none() (already
//          consumed) before any ptr::read, so a duplicate import fails cleanly.
//       2. Convert schema -> Field before consume where possible.
//       3. Delegate to ImportedArrowColumn::import_from_ffi, which consumes,
//          validates once, classifies once, and stores (field, array, kind).
// _append: inner.push_imported_arrow_slice(name, &imported.imported,
//          row_offset, row_count)
// _free:   drop(Box::from_raw(imported)); release is exactly-once but may be
//          delayed until chunk-held slices drop.
```

`column_sender_chunk_append_arrow_column` (the old per‑chunk consuming function)
is retained for C/C++ compatibility. It should receive the same `release != NULL`
pre-consume guard so duplicate use fails cleanly, but Python's chunked path stops
calling it. Deprecation/removal can be handled later with the C++ wrapper and C
tests in scope.

### 6.3 Cython changes

`col_setup_t` (`dataframe.pxi:574-578`) gains one field:

```cython
cdef struct col_setup_t:
    col_chunks_t chunks
    ArrowSchema arrow_schema
    column_sender_arrow_import* arrow_import  # NULL until first append
    ...
```

`_dataframe_columnar_call_arrow_append` (`ingress.pyx:3246`) becomes memoized.
Both the one-time import (which runs `validate_full()`, the only O(rows) step)
and the per-chunk append run **inside `with nogil`**, matching today's appender
at `:3253`; only error→exception conversion (`c_err_to_py`, which needs the GIL)
happens after reacquiring it:

```cython
cdef column_sender_arrow_import* imported = col.setup.arrow_import
with nogil:
    if imported == NULL:
        imported = column_sender_arrow_import_new(
            &col.setup.chunks.chunks[0], &col.setup.arrow_schema, &err)
    if imported != NULL:
        ok = column_sender_chunk_append_arrow_import(
            chunk, col.name.buf, col.name.len,
            imported, row_offset, row_count, &err)
col.setup.arrow_import = imported     # memoize (plain pointer store)
if imported == NULL or not ok:
    raise c_err_to_py(err)            # GIL reacquired; safe to build the exception
```

The FFI declarations in `line_sender.pxd` carry `noexcept nogil` so the calls are
legal inside the `with nogil` block.

`col_t_release` (`dataframe.pxi:606-620`) frees the handle:

```cython
if col.setup.arrow_import != NULL:
    column_sender_arrow_import_free(col.setup.arrow_import)
    col.setup.arrow_import = NULL
# existing chunks[].release loop still runs; for imported columns chunks[0].release
# was consumed by _new, so it is a no-op — exactly as today.
```

The raw‑pointer columns and the designated‑timestamp `at` path are unchanged:
`at` columns are never Arrow‑routed (they use `_dataframe_columnar_append_at`,
reading `chunks[0].buffers[1]` at `:3573`), so they never get a handle and
`chunks[0]` stays intact for them. `arrow_schema` is read for dispatch at build
time (e.g. category index type at `dataframe.pxi:1254`) before any import, and is
still released by `col_t_release` independently of the array.

### 6.4 Per‑chunk dataflow after the fix

```text
build:   _dataframe_export_arrow_chunks  →  chunks[0] holds pyarrow buffers
chunk 0: append → import_new (consume chunks[0], validate once) → store handle
                → append_import: imported.slice(0, k0) → push
         flush; clear  (drops chunk 0's slice; handle + buffers untouched)
chunk 1: append_import: imported.slice(k0, k1) → push
         flush; clear
...
teardown: chunk_free → plan release (col_t_release → import_free)
          (drops handle; original Arrow release runs when last ArrayRef drops)
```

---

## 7. Correctness analysis

- **No double‑free:** the original pyarrow `release` is owned by the imported
  column's retained `ArrayRef` graph and runs exactly once when the last handle or
  chunk-held slice drops. `import_free` drops the handle's reference; it does not
  promise immediate release if a chunk still holds a slice. `import_new` consumed
  `chunks[0].release` (→ NULL), so the `col_t_release` chunk loop skips it.
- **No leak:** every successful `import_new` stores the handle in `col_setup`;
  `col_t_release` (reached from `dataframe_plan_release` in every `finally`,
  including the production error/force‑drop path at `ingress.pyx:4880-4895`) frees
  it. If `import_new` fails it returns NULL and stores nothing.
- **No use‑after‑free:** per‑chunk slices are produced by `ArrayRef::slice`, which
  **clones the `Arc`**. The slice held by a chunk co‑owns the buffers, so it stays
  valid even if it momentarily outlived the handle. In practice the handle (on the
  plan) outlives every chunk because `column_sender_chunk_free`
  (`ingress.pyx:4894`) runs before `dataframe_plan_release` (`:4895`) — but unlike
  Option A, correctness no longer *depends* on that ordering.
- **Validation:** `validate_full` runs once inside `import_new`. The cached array is
  immutable; every append is a checked sub‑range (`checked_add` for
  `row_offset + row_count`, then `<= imported.len()`). No order‑coupled flag.
- **Classification:** `arrow_batch::classify` runs once inside the `questdb-rs`
  `ImportedArrowColumn` constructor and the resulting `ColumnKind` is cached there.
  `_append` calls `Chunk::push_imported_arrow_slice`; `questdb-rs-ffi` never names
  or exports `ColumnKind`.
- **UInt64 policy:** Arrow / pandas `UInt64` is accepted only as a source dtype
  for values that are exactly representable as QuestDB signed `LONG`. Every
  non-null `UInt64` value is checked before frame publication; values greater
  than `i64::MAX` fail with an ingest error. This does not introduce an unsigned
  `LONG` destination type and does not reinterpret high-bit values.
- **Public‑contract only:** uses `from_ffi`, `ArrayRef::slice`, and the existing
  Arrow chunk push machinery — no reliance on `from_ffi` tolerating a
  `release == None` array or on `Drop` internals (arrow‑rs `58`).
- **Double‑import guard:** `import_new` rejects an array whose `release` is already
  NULL *before* the `ptr::read` consume. `import_new` is the only consuming entry
  point and it is called exactly once per column (memoized on `arrow_import`), but
  the guard is defence‑in‑depth: if a future change ever double‑imports (a
  memoization regression, a copied call site), it fails with a clean error instead
  of re‑reading a consumed struct and re‑introducing the original UAF class. The
  retained old `column_sender_chunk_append_arrow_column` should get the same
  pre-consume guard; Python's chunked path no longer calls it.

---

## 8. Performance analysis

Per Arrow‑routed column with `R` rows split into `K` output chunks:

| | current (buggy) | Option A (borrow) | Option B (FFI `ArrayRef+Field`) | Option C (chosen) |
|---|---|---|---|---|
| import / `from_ffi` | K× | K× | 1× | 1× |
| `validate_full` | K× O(R) | 1× O(R) | 1× O(R) | 1× O(R) |
| classify / wire-kind decision | K× | K× | K× | 1× |
| per‑chunk append | rebuild + slice | rebuild + slice | slice + classify | slice + cached kind |
| public Rust API churn | none | none | none | small imported-column API |

The chosen design removes the expensive part: repeated full-array import,
`validate_full`, and `ArrayData` rebuilds. `ArrayRef::slice` still creates a
sliced array object, and nullable arrays may do null-count work over the slice
range; column descriptors still copy column names per chunk. Do not claim zero
per-chunk allocation or strictly O(1) work for all Arrow array shapes. The target
performance property is: no per-chunk C Data Interface import, no per-chunk
full-array validation, and no repeated logical QuestDB classification.

---

## 9. Test plan

### 9.1 Which sources actually route through the changed path

Only sources dispatched to `_dataframe_columnar_call_arrow_append` are affected.
Verified against `_dataframe_columnar_append_field` and the populate whitelist
(`ingress.pyx:3454-3542`, `:3618-3632`):

| Target | Arrow source(s) on the changed path | Notes |
|---|---|---|
| symbol | `str_{i8,i16,i32}_cat` | categorical dictionary |
| str | `str_utf8_arrow`, `str_lrg_utf8_arrow` | pyarrow string fields |
| ts | `dt64ns_tz_arrow`, `dt64us_tz_arrow` | tz‑aware datetime **field** |
| i8 / i16 / i32 | `i8_arrow` / `i16_arrow` / `i32_arrow` | narrow‑int targets |
| f32 | `f32_arrow` | |
| uuid | `fsb16_arrow` | (`uuid_pyobj` uses the prebuilt path) |
| long256 | `fsb32_arrow` | |

**Explicitly NOT on the changed path** (do not put these in the manual‑path
matrix):

- **ipv4** — the pandas planner only maps IPv4 to `col_source_ipv4_pyobj`
  (`dataframe.pxi:303-309`; plain Arrow `UInt32` resolves to `i64`). The Arrow
  branch for ipv4 is unreachable from pandas. Covered by the Rust Arrow (capsule)
  route instead.
- **decimal** — `col_target_column_decimal` is absent from the populate whitelist
  (`ingress.pyx:3618-3632`), so decimals never flow through the manual chunked path.
  Capsule‑route only.
- **i64 / f64 / u32 from Arrow** — handled as raw‑buffer numpy appends reading
  `chunks[0].buffers[1]` directly (`ingress.pyx:3353-3410`); they *borrow*, are
  unaffected by this bug, and need no new coverage here.
- **categorical-as-string (`symbols=False` / unlisted categorical)** — although
  populate would dispatch it through the Arrow appender, validation rejects this
  shape today (`ingress.pyx:2427-2434` and the existing fuzz comments). Only the
  categorical-symbol path is in the reachable changed-path matrix unless this
  plan also changes validation.

### 9.2 Forcing the manual chunked path

`Client.dataframe()` tries the Arrow capsule (Rust Arrow) route first and returns
before building the manual plan when it succeeds (`ingress.pyx:4818` →
`_dataframe_client_try_capsule_path`, which takes any object exposing
`__arrow_c_stream__` / `__arrow_c_array__`, `:4555-4560`). A naive round‑trip test
can therefore go entirely through the Rust route and **never** touch the changed
`_dataframe_columnar_call_arrow_append`. Coverage must force the fallback:

- **Primary — bench hook:** `_bench_dataframe_plan_and_populate_column_chunks`
  (`ingress.pyx:3967`) drives `_dataframe_plan_build` + `_dataframe_columnar_populate_chunk`
  directly with the same `_FIELD_TARGETS_QWP` map, bypassing dispatch. This is the
  guaranteed manual‑path exerciser for the §9.1 type matrix; keep and extend it.
- **Plus a real ingestion proof:** add at least one end‑to‑end `Client.dataframe()`
  multi‑chunk case that is expected to take the manual path and verifies stored
  rows/values. Use a concrete fallback shape the capsule route declines but manual
  v1 accepts, such as a pandas DataFrame with fixed `table_name`, timestamp column,
  `symbols=False`, and an Arrow-backed string field:
  `_resolve_symbols_to_overrides(..., symbols=False)` cannot introspect pandas
  directly, so `_dataframe_client_try_capsule_path` returns `False` and the manual
  planner runs. Treat `_debug_dataframe_columnar_io_stats` as supporting evidence
  only: flush/sync counters can be incremented by both manual and capsule paths.
  The bench hook is the primary proof that `_dataframe_columnar_populate_chunk`
  and `_dataframe_columnar_call_arrow_append` ran.

### 9.3 Cases

- Existing: `test_bench_dataframe_plan_and_populate_aligns_nullable_chunks` must
  pass (the original repro).
- Via the bench hook, one mixed multi‑chunk (`max_rows_per_chunk < row_count`)
  case with representative §9.1 Arrow-routed families; assert row/chunk counts and
  that no row-path cells were emitted. The bench hook never flushes, so it cannot
  assert round-trip values.
- Add targeted §9.1 per-type cases only where the new imported-column
  classification constructor changes behavior or lacks existing classifier/wire
  coverage.
- A 3+‑chunk nullable case to exercise repeated slice after the first import,
  including a null window that crosses output-chunk boundaries.
- At least one §9.2 real‑ingestion case proving the manual path executed; put
  value/round-trip assertions there.
- **Double‑import test** (guards #4): from Rust/C, call `column_sender_arrow_import_new`
  twice on the same `ArrowArray` and assert the second fails cleanly (non‑NULL→NULL,
  error set) with no UAF — i.e. the `release == NULL` guard fires.
- Error‑path test: force a failure on a later chunk and assert the plan/handle is
  released (run under ASan / a leak check in CI if available).
- Rust/C tests for the new handle API: `new`→`append`×N→`free`, including a
  slice‑twice case that would UAF under the old Python pattern.
- Keep existing `cpp_test/test_arrow_c.c` coverage for
  `column_sender_chunk_append_arrow_column`; add/update fixtures so hand-built
  Arrow arrays use valid no-op release callbacks when testing the new
  `release != NULL` guard.
- Run the Rust FFI unit tests; run the suite under ASan for the Arrow feature.

---

## 10. Files touched

- `c-questdb-client/questdb-rs/src/ingress/column_sender/` — add
  `ImportedArrowColumn` (module name TBD) and
  `Chunk::push_imported_arrow_slice`; keep `arrow_batch::ColumnKind` and
  `push_arrow_deferred` private.
- `c-questdb-client/questdb-rs-ffi/src/column_sender.rs` — opaque handle wrapper +
  3 fns; retain `column_sender_chunk_append_arrow_column` for compatibility and
  add the pre-consume `release != NULL` guard.
- `c-questdb-client/questdb-rs-ffi/src/lib.rs` — factor the import/validate out of
  `arrow_ffi_import_array_sliced` into the imported-column constructor path; add
  the `release != NULL` guard before the consume; document pre-consume vs
  post-consume failure ownership.
- `c-questdb-client/include/questdb/ingress/column_sender.h` — declare the handle
  API; keep the old per‑chunk appender decl.
- `c-questdb-client/include/questdb/ingress/column_sender.hpp` — no required
  wrapper migration; update comments only if the old appender's failure contract
  changes due to the guard.
- `c-questdb-client/cpp_test/test_arrow_c.c` — keep old appender smoke coverage;
  add handle smoke/slice-twice coverage if exposing the handle API to C tests is
  useful.
- `src/questdb/line_sender.pxd` — mirror the new FFI decls (`noexcept nogil`).
- `src/questdb/dataframe.pxi` — `col_setup_t.arrow_import`; free it in
  `col_t_release`.
- `src/questdb/ingress.pyx` — memoized `_dataframe_columnar_call_arrow_append`,
  with `import_new` + `append_import` called inside `with nogil`.
- `test/test_dataframe.py` — manual-path multi‑chunk coverage plus one real
  ingestion proof.

Per `c-questdb-client/CLAUDE.md`: run `cargo fmt` then plain `cargo clippy --tests`
(no `-D warnings`) on `questdb-rs-ffi` before committing.

---

## 11. Risks and open questions

- **GIL discipline:** the one‑time `import_new` (which runs `validate_full`) must be
  called inside `with nogil` (§6.3); the FFI decls are `noexcept nogil`. Putting the
  first import under the GIL would serialize the only O(rows) step against other
  threads — defeats the perf goal. Only `c_err_to_py` runs with the GIL held.
- **Two storage forms in `col_setup_t`** (raw `chunks` vs `arrow_import`). They are
  mutually exclusive per column; rule: `arrow_import != NULL ⇒ imported, else raw`.
  Low risk but worth a comment on the struct.
- **Error‑path ownership:** confirmed today every abandon path reaches
  `dataframe_plan_release` (production `finally` at `ingress.pyx:4886-4895`;
  bench at `:4051-4058`). The new owned handle makes it worth re‑checking there is no
  path that drops a plan without `dataframe_plan_release`.
- **Import-handle thread safety:** the opaque handle is borrowed by append and freed
  at plan teardown. Document it as not safe to use/free concurrently, matching the
  single-threaded Cython plan usage, unless the FFI grows ref-counting or an
  in-use guard for handles.
- **`n_chunks > 1`:** currently rejected for Arrow columns (`ingress.pyx:2147`). If
  multi‑physical‑chunk support is added later, `ImportedArrowColumn` can evolve to
  import and concatenate, hold a vector of imported arrays, or become range-aware;
  out of scope here.
- **Range descriptors:** avoiding `ArrayRef::slice` allocation/null-count work would
  require changing `ArrowDeferred` and encoder/symbol-resolution paths to carry
  `(ArrayRef, offset, len)`. That is a later optimization, not part of this fix.
- **Trade‑off vs Option B:** the chosen design is larger than an FFI-only
  `ArrayRef+Field` handle in exchange for one logical QuestDB classification per
  imported column, a clearer `questdb-rs` abstraction, and no leakage of
  `ColumnKind` across the FFI crate boundary. If a minimal backport is ever needed
  without touching `questdb-rs`, Option B is the fallback.
