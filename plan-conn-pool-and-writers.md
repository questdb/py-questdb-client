# Pooled QWP/WS Connection + Per-Source Writers — Architecture Plan

## TL;DR

Refactor the FFI so the pool holds **QWP/WS connections**, not writers.
`column_sender_chunk` becomes one writer over a borrowed connection
alongside two siblings: a generic Arrow per-column appender (Victor's
(B)) and a NumPy fast-path appender (Victor's NumPy / (C)). Egress
readers join the same pool when ready. The Python `Client` keeps its
public surface unchanged; per-column dispatch routes Arrow-backed
columns, NumPy-backed columns, and `PyObject` columns to the
appropriate writer, eliminating the v1 `UnsupportedDataFrameShapeError`
rejections that exist only because the current FFI can't normalize
without a Python-side copy.

## Motivation

Three forces converge on the same answer:

1. **Egress is on the roadmap.** A read side needs the same QWP/WS
   transport. Pooling writers forces a separate read pool or a
   write-anchored API; pooling connections lets readers and writers
   share one pool from day one.
2. **Victor's per-source writer design** wants Arrow inputs and NumPy
   inputs to take different fast paths. Both need access to a
   connection's wire buffer. Pooling at the connection level lets the
   writers be peer abstractions instead of nested specialisations of
   `column_sender`.
3. **`column_sender` recycle correctness.** [Round 3 review
   §1](plan-pandas-columnar-performance.md) identified an ambiguity
   where a mid-call flush failure returns a `column_sender` with
   in-flight uncommitted data, and the next borrower may commit that
   data alongside theirs. Re-anchoring `must_close` on the *connection*
   removes the writer-vs-connection-ownership question.

## Architecture

### Connection pool

```
questdb_db                       (the pool — read/write, name kept)
 ├─ qwpws_conn[1..pool_max]      (borrowed transport handle)
 ├─ pool_size  / pool_max
 └─ pool_idle_timeout_ms / pool_reap
```

A `qwpws_conn` owns:

- The TCP / TLS socket and the QWP/WS handshake state.
- The protocol-level in-flight slot reservation (currently owned by
  `column_sender`).
- A `must_close` flag set by any writer that hits an unrecoverable
  error.

Writers borrow a `qwpws_conn`, do their work, and return it. The pool
drops connections marked `must_close` on return regardless of which
writer set the flag.

### Writer families

Each writer is cheap to construct over a borrowed conn, holds no
long-lived state beyond its current chunk / query, and routes its
output through that conn's wire buffer.

| Writer | Source | Status |
|---|---|---|
| `column_sender_chunk` (per-type appends) | hand-rolled per-type calls; PyObject build output | PR #148, reanchored to `qwpws_conn` |
| `column_sender_chunk` `_append_arrow_column` | any Arrow C Data array (`ArrowArray*` + `ArrowSchema*`) | **NEW**, matches Victor's (B) |
| `column_sender_chunk` `_append_numpy_column` | NumPy buffer + `{dtype, stride_bytes, big_endian}` layout | **NEW**, matches Victor's (C); may collapse to direct-to-wire — see [Open Q1](#open-questions) |
| egress readers (TBD) | server → caller | separate effort; design out of scope |

The three append modes share one `column_sender_chunk` lifecycle —
they can be mixed within a single chunk (a frame with a NumPy `int64`,
an Arrow string, and a sniffed-and-built `PyObject` decimal all coexist
in one chunk). The row-count lock holds across writer types.

### Egress readers (forward-looking)

Out of scope for this plan, but the connection-pool shape is chosen so
the egress design has one obvious place to land:

```c
qwpws_conn* questdb_db_borrow_conn(...);
qwpws_reader* qwpws_reader_new(qwpws_conn*, query, ...);
... // iterate result
qwpws_reader_free(...);
questdb_db_return_conn(...);
```

No separate pool, no two-layer borrowing.

## FFI surface

### Connection pool (rename of existing borrow/return)

```c
/* Unchanged */
QUESTDB_CLIENT_API
questdb_db* questdb_db_connect(
    const char* conf, size_t conf_len,
    line_sender_error** err_out);

QUESTDB_CLIENT_API
void questdb_db_close(questdb_db* db);

QUESTDB_CLIENT_API
size_t questdb_db_reap_idle(questdb_db* db);

/* Renamed: borrow returns a connection, not a writer. */
typedef struct qwpws_conn qwpws_conn;

QUESTDB_CLIENT_API
qwpws_conn* questdb_db_borrow_conn(
    questdb_db* db,
    line_sender_error** err_out);

QUESTDB_CLIENT_API
void questdb_db_return_conn(
    questdb_db* db,
    qwpws_conn* conn);

QUESTDB_CLIENT_API
bool qwpws_conn_must_close(const qwpws_conn* conn);
```

`column_sender_must_close` is removed (the flag migrates to the conn).
The pool drops a conn marked `must_close` on return; until then it can
still be used by a final `column_sender_sync` etc.

### `column_sender_chunk` writer (existing, re-anchored)

Chunk lifecycle is unchanged. The only difference is that **flush and
sync take a `qwpws_conn`** rather than the (now removed) writer-bound
sender:

```c
QUESTDB_CLIENT_API
column_sender_chunk* column_sender_chunk_new(
    const char* table_name, size_t table_name_len,
    line_sender_error** err_out);

QUESTDB_CLIENT_API
void column_sender_chunk_free(column_sender_chunk* chunk);

QUESTDB_CLIENT_API
void column_sender_chunk_clear(column_sender_chunk* chunk);

QUESTDB_CLIENT_API
size_t column_sender_chunk_row_count(const column_sender_chunk* chunk);

/* Per-type appends — unchanged signatures from PR #148 */
QUESTDB_CLIENT_API
bool column_sender_chunk_column_i64(
    column_sender_chunk* chunk,
    const char* name, size_t name_len,
    const int64_t* data, size_t row_count,
    const column_sender_validity* validity,
    line_sender_error** err_out);
/* ... and i8 / i16 / i32 / f32 / f64 / bool / ts_nanos / ts_micros /
   varchar / symbol_dict_i8 / i16 / i32 unchanged ... */

/* Designated timestamp setters — unchanged */
QUESTDB_CLIENT_API
bool column_sender_chunk_designated_timestamp_nanos(
    column_sender_chunk* chunk,
    const int64_t* data, size_t row_count,
    line_sender_error** err_out);
QUESTDB_CLIENT_API
bool column_sender_chunk_designated_timestamp_micros(
    column_sender_chunk* chunk,
    const int64_t* data, size_t row_count,
    line_sender_error** err_out);

/* Flush + sync now take a borrowed connection. */
QUESTDB_CLIENT_API
bool column_sender_flush(
    qwpws_conn* conn,
    column_sender_chunk* chunk,
    line_sender_error** err_out);

QUESTDB_CLIENT_API
bool column_sender_sync(
    qwpws_conn* conn,
    column_sender_ack_level ack_level,
    line_sender_error** err_out);
```

### Arrow column appender (NEW — Victor's (B))

```c
/**
 * Append one column from an Arrow C Data array. The Rust writer
 * inspects the schema and writes source bytes directly into the wire
 * buffer. Normalisations (LargeUtf8 i64->i32 offsets, Dictionary code
 * widening, etc.) happen here, not on the caller side.
 *
 * The chunk's row count is locked by the first append; subsequent
 * appends must agree.
 */
QUESTDB_CLIENT_API
bool column_sender_chunk_append_arrow_column(
    column_sender_chunk* chunk,
    const char* name, size_t name_len,
    const ArrowArray* array,
    const ArrowSchema* schema,
    line_sender_error** err_out);
```

This subsumes today's piecemeal varchar, symbol-dict, validity-bitmap
plumbing. Where the source layout already matches the wire, the
internal write is `memcpy`; otherwise the writer streams source ->
wire per element with no intermediate column-format buffer.

### NumPy direct appender (NEW — Victor's (C), refined)

```c
typedef enum column_sender_numpy_dtype {
    cs_np_i8,  cs_np_i16, cs_np_i32, cs_np_i64,
    cs_np_u8,  cs_np_u16, cs_np_u32, cs_np_u64,
    cs_np_f32, cs_np_f64, cs_np_bool,
    cs_np_dt64_ns, cs_np_dt64_us, cs_np_dt64_ms,
    cs_np_dt64_s,  cs_np_dt64_D,
} column_sender_numpy_dtype;

typedef struct column_sender_numpy_layout {
    column_sender_numpy_dtype dtype;
    size_t stride_bytes;   /* 0 means contiguous (= sizeof(dtype)) */
    bool   big_endian;     /* false means native LE (the wire format) */
} column_sender_numpy_layout;

QUESTDB_CLIENT_API
bool column_sender_chunk_append_numpy_column(
    column_sender_chunk* chunk,
    const char* name, size_t name_len,
    column_sender_numpy_layout layout,
    const void* data,
    size_t row_count,
    const column_sender_validity* validity,
    line_sender_error** err_out);
```

Python passes `arr.strides[0]` and `arr.dtype.byteorder` straight
through. The Rust writer handles `uint*` widening to `i64` (with
overflow check), `dt64[ms,s,D]` rescaling to the wire unit, strided
gather (`stride != sizeof(dtype)`), and `bswap` on non-native-endian
in one pass into the wire buffer.

**Whether this is a separate FFI or whether it collapses into a fast
path inside `_append_arrow_column`** depends on [Open Q1](#open-questions).

### Mixed writers in one chunk

A `column_sender_chunk` can be populated by any combination of
per-type, Arrow, and NumPy appends. The row-count lock holds across
writer types. Example flow for a mixed-physical DataFrame:

```c
chunk = column_sender_chunk_new("trades", ...);

/* NumPy int64 column, contiguous, native LE -> memcpy. */
column_sender_chunk_append_numpy_column(chunk, "seq", 3,
    (column_sender_numpy_layout){cs_np_i64, 0, false},
    seq_ptr, n_rows, NULL, ...);

/* Arrow UTF-8 column, large_string is narrowed in Rust. */
column_sender_chunk_append_arrow_column(chunk, "note", 4,
    note_arrow_array, note_arrow_schema, ...);

/* Sniffed + built PyObject decimal -> per-type call. */
column_sender_chunk_column_decimal128(chunk, "price", 5,
    decimal_buf, n_rows, validity, ...);

/* Designated timestamp. */
column_sender_chunk_designated_timestamp_nanos(chunk, ts_ptr, n_rows, ...);

/* Flush + sync over a borrowed conn. */
column_sender_flush(conn, chunk, ...);
column_sender_sync(conn, ack_level_ok, ...);
column_sender_chunk_free(chunk);
```

## Python changes

### `Client` class — public surface unchanged

```python
class Client:
    @staticmethod
    def from_conf(conf_str: str) -> Client: ...
    def dataframe(self, df, *, table_name=None, table_name_col=None,
                  symbols='auto', at=ServerTimestamp) -> None: ...
    def reap_idle(self) -> int: ...
    def close(self) -> None: ...
    def __enter__(self) -> Client: ...
    def __exit__(self, *exc) -> None: ...
```

No method signature changes. The Cython-level `questdb_db_borrow_sender`
call becomes `questdb_db_borrow_conn` and the borrowed handle changes
type; that's invisible to Python callers.

### Per-column dispatch in `Client.dataframe`

`dataframe.pxi`'s extracted planner already classifies each column by
source (`col_source_*`). The dispatch table below replaces the v1
"reject unless on a whitelist" path:

| Column source | Path |
|---|---|
| `pa.Table` / `pa.RecordBatch` input (any column) | (B) per-column Arrow appender |
| Pandas `ArrowDtype` / `string[pyarrow]` / nullable extension / `Categorical` / `large_string` / Arrow-backed any | (B) |
| NumPy `int*` / `uint*` / `float*` / `bool` / `datetime64[any unit]` | (C) NumPy appender; layout struct carries `strides[0]` and `byteorder` |
| `object` / `StringDtype[python]` | (D) per-type call via Cython sniff + growable-buffer build (semantics match the current row-path ILP sniff) |
| Designated timestamp from a `TimestampNanos(literal)` / `datetime` / `df.index` | materialize once as a single-value or full Arrow column on the Python side, then route via (B) |

This subsumes today's planner-time rejections in
`_dataframe_columnar_plan_failures`:

- `int32` / `float32` / `uint*` / `bool` field columns → handled by
  (C). No more rejection.
- `string[python]` / `object` field columns → handled by the sniff +
  build pipeline via (D). No more rejection.
- `large_string` → handled by (B); the Python-side cast in
  `_dataframe_cast_large_string_chunks_to_utf8` goes away.
- `table_name_col` → still rejected for v1 (orthogonal to per-source
  dispatch; needs chunk-per-table or per-row table-name plumbing).
- `at = ServerTimestamp / TimestampNanos / datetime` → materialize as
  a column, then route normally.
- NaT in designated ts → either reject (current) or materialize a
  "valid mask" — out of scope here.

### Public-API stability

- `Client.dataframe()` keeps its current signature.
- `UnsupportedDataFrameShapeError` stays for the small set of cases
  the per-source dispatch still doesn't cover (`table_name_col`,
  unsupported `at` shapes once we decide on those).
- The Cython internal helpers `_debug_dataframe_columnar_plan` and
  `_bench_dataframe_plan_and_populate_column_chunks` keep their
  signatures; their internal dispatch broadens.
- The fuzz test
  (`test/test_client_dataframe_fuzz.py`) continues passing — what
  changes is *which* of its generator outputs hit the "supported"
  branch vs. the "rejected" branch.

## Migration sequence

Each step is independently mergeable. Tests at each step should still
go green.

### Step 1 — Rust: re-anchor PR #148 on `qwpws_conn` ✅ done

Submodule `7740b7a`, parent `9854f5e`.

- Rename `column_sender` -> `qwpws_conn` at the FFI surface (the
  internal `ColumnSender` / `OwnedSender` Rust types are doc-hidden
  and kept).
- `must_close` and in-flight-slot tracking already lived on the
  conn internally (via `ColumnSender::must_close() ->
  conn.must_close()`). The rename surfaces this at the type
  level but **does not** change the underlying behaviour.
- `column_sender_flush` / `column_sender_sync` now take
  `qwpws_conn*`.
- Pool API: `questdb_db_borrow_sender` -> `_borrow_conn`,
  `questdb_db_return_sender` -> `_return_conn`. `column_sender_must_close`
  -> `qwpws_conn_must_close`.

**Caveat — round-3 dirty-sender concern not yet resolved.** The
[`plan-pandas-columnar-performance.md`](plan-pandas-columnar-performance.md)
round-3 review §1 ("Pool-recycled dirty sender mixes data across
`Client.dataframe()` calls") is *not* fixed by this step. A
mid-call flush failure still recycles the conn with in-flight
uncommitted data, which the next borrower's first flush would
commit alongside their own. Fixing that needs either marking
`must_close = true` after every mid-call failure, exposing a
"discard in-flight" FFI primitive that Python's error-finally can
call, or force-dropping the conn on the Python side. Decision
needed in a separate follow-up.

**Success criterion** (met): 929 Python tests, 836 Rust unit
tests, 4 fuzz seeds × 200 iters all green.

### Step 2 — Rust + Python: Arrow column appender ✅ done

Implemented in three sub-steps.

**Step 2a** (submodule `632c647`, parent `66ba477`):
- `column_sender_chunk_append_arrow_column` added in the Rust FFI
  shim. Mirrors Apache Arrow C Data Interface (`ArrowArray` +
  `ArrowSchema`) as `#[repr(C)]` structs; no new crate dependency.
- Dispatches on schema format: `c/s/i/l/f/g/b/u/tsn:/tsu:` for
  primitives + utf8 + timestamps; dictionary-typed schemas with
  `c/s/i` indices + `u` value type route to `symbol_dict_i*`.
- Cython binding added to the `pxd`; no caller wired in yet.

**Step 2b** (submodule `6c53ea7`, parent `8c04c63`):
- `ColumnKind::VarcharLarge` + `Chunk::column_varchar_large` in the
  `questdb-rs` crate; `encode_varchar_large` reads i64 offsets and
  writes u32 LE to the wire frame in one pass, no scratch.
- `validate_varchar_offsets_i64` rejects offsets exceeding
  `u32::MAX` (the QWP wire offset table is uint32 LE).
- Arrow appender's `U` format now routes through this path —
  latent capability until the Python-side cast is removed (which
  needs the row-path serializer to learn `col_source_str_lrg_utf8_arrow`,
  out of scope for Step 2).

**Step 2c** (submodule `0650c40`, parent `ff0c909`):
- `column_sender_chunk_append_arrow_column` gains `row_offset` and
  `row_count` parameters for chunked-emission callers. Each format
  handler shifts the appropriate buffer pointers.
- Validity bitmap requires `row_offset % 8 == 0`; the Cython chunk
  planner already aligns to 8 when validity is present (see
  `_dataframe_columnar_rows_per_chunk`), so the Rust constraint
  is satisfied today. **Two-place enforcement** — fragile if the
  Cython aligner drifts; consider folding into Rust.
- `Client.dataframe()` routes varchar + symbol columns through the
  Arrow appender. Numeric / timestamp columns stay on the per-type
  path because they were already direct-write to wire (Q1).
- Per-type `column_sender_chunk_column_varchar` and `symbol_dict_i*`
  remain in the C ABI as **lower-level building blocks**, no longer
  called from py-questdb-client. Both header sections gained a
  doc-comment pointing callers at the Arrow appender first.
- Validator accepts both `col_source_str_utf8_arrow` and
  `col_source_str_lrg_utf8_arrow`. The `lrg` arm is **dead** today
  (the planner-shared cast keeps everything as `utf8_arrow`);
  reachable when Step 4 or a row-path serializer upgrade removes
  the cast.

**Success criterion** (met): 929 Python tests, 836 Rust unit
tests, 4 fuzz seeds × 200 iters all green. The
`test_multi_chunk_emission` fuzz still reports `flush_calls >= 2`
and `sync_calls == 1`.

**Known gaps surfaced during implementation**:
- No focused test for the Arrow appender's `row_offset > 0` path
  outside of `Client.dataframe`'s chunked emission. A direct call-
  site test would catch slicing-logic regressions faster.
- No focused test for the `U` format dispatch (the cast hides it).
- Removing the `_dataframe_cast_large_string_chunks_to_utf8` cast
  would unlock the Rust `U` path but breaks `Sender.dataframe`
  (the row-path serializer doesn't know `col_source_str_lrg_utf8_arrow`).
  Either Step 4 or a separate row-path patch must handle this.

### Step 3 — Rust + Python: NumPy widening + bool packing ✅ done

Submodule `ba0cf92`, parent `d420d79`.

Reframed per Q1: not a perf win for native NumPy (which is already
direct-write to wire), but covers the wide-set of NumPy dtypes the
v1 columnar path was rejecting:

- `i8/i16/i32` → `i64` (sign-extend), wire = LONG.
- `u8/u16/u32` → `i64` (zero-extend), wire = LONG.
- `i64` pass-through, wire = LONG.
- `u64` bit-reinterpret to `i64` (values > `i64::MAX` wrap to
  negative on the wire — **matches the row-path's C cast**).
- `f32` → `f64`, wire = DOUBLE.
- `f64` pass-through.
- NumPy native `bool` (byte-per-row) → Arrow LSB-first packed bitmap,
  wire = BOOLEAN.

Implementation:
- `Chunk::column_numpy(name, dtype, ptr, row_count, validity)` in
  the Rust crate. Widens / packs into a chunk-owned `NumpyScratch`
  arena keyed by destination type so the `ColumnDescriptor`'s raw
  pointer alignment matches the encoder's reads.
- FFI: `column_sender_chunk_append_numpy_column` with a
  `column_sender_numpy_dtype` enum.
- Cython: `_is_numpy_widening_source` + `_source_to_numpy_dtype` +
  `_numpy_dtype_element_size` route narrower NumPy sources through
  the new FFI. NumPy `int64` / `float64` continue using the per-type
  FFI directly.
- Validator accepts the new sources; fuzz moves the corresponding
  generators into `SUPPORTED_FIELD_GENS_WEIGHTED`.

Strided arrays and non-native-endian arrays are not supported in v1
— the Python wrapper consolidates upstream.

**Success criterion** (met): UNSUPPORTED_FIELD_GENS is empty (every
narrower numeric generator + native bool round-trips); 941 Python
tests, 836 Rust unit tests, 4 fuzz seeds × 200 iters all green.

### Step 4 — Python: PyObject sniff + build ✅ done

Submodule unaffected; parent `0d3b1d5` (str_pyobj), `e43783e`
(int/float/bool pyobj), `10dba21` (post-review null-alignment fix).

- `dataframe_plan_t` grew a `pyobj_built: pyobj_built_t**` field —
  one per column, NULL for non-pyobj sources, populated by the new
  prebuild phase that runs after `validate_plan` and before the
  chunk emission loop in `Client.dataframe()`.
- Four builders in `ingress.pyx`:
  - `_dataframe_columnar_build_str_pyobj`: Arrow Utf8-shaped int32
    offsets + uint8 bytes (encoded via Python's `str.encode('utf-8')`)
    + LSB validity. Rejects > 2 GiB up front.
  - `_dataframe_columnar_build_int_pyobj`: i64 + LSB validity.
    PyBool checked before PyLong (subclass).
  - `_dataframe_columnar_build_float_pyobj`: f64 + LSB validity.
    NaN cells treated as null (pandas convention).
  - `_dataframe_columnar_build_bool_pyobj`: LSB-packed bitmap of
    values. Nulls rejected (BOOLEAN has no row-level null).
- New `col_target_column_bool` emitter branch (was missing in the
  columnar path).
- Validator accepts pyobj sources for the corresponding wire
  targets.
- Fuzz: `object_str`, `string_python`, `object_int`, `object_float`,
  `object_bool` all in `SUPPORTED_FIELD_GENS_WEIGHTED`.

**Success criterion** (met): 941 Python tests pass; multi-seed fuzz
green.

### Round-3 must_close fix ✅ done

Submodule `45ce070`, parent `64cb920`.

Closes the round-3 review #1 concern that Step 1 had explicitly
not resolved. A mid-call flush failure left a conn with in-flight
uncommitted frames in the pool; the next borrower's first flush
("immediate commit") would commit those alongside their own.

Fix:
- Rust: `ColumnConn::mark_must_close(&mut self)` (pub(crate)) +
  `ColumnSender::mark_must_close(&mut self)` (pub) flip the
  existing terminal flag.
- FFI: new `questdb_db_drop_conn(db, conn)` marks must_close, then
  drops the box (the existing return-to-pool path drops conns
  marked terminal instead of recycling).
- Cython: `Client.dataframe()` gained a `force_drop_conn` cdef
  bint. Any exception escaping the chunk loop sets it; the
  defensive sync resets it to False on success. The finally
  branches: `questdb_db_drop_conn` vs. `questdb_db_return_conn`.

Limitation: no targeted regression test. The `QwpAckServer` doesn't
support mid-stream error injection. Validating end-to-end requires
either extending the ACK harness or running against a real QuestDB
that returns HALT mid-frame.

### Step 5 — Rust: egress readers (separate doc)

This plan defines the connection-pool *shape* the egress design will
borrow into. Reader API design is out of scope here; the only
commitment is that the pool API
(`questdb_db_borrow_conn` / `_return_conn`) is the entry point.

## Backward compatibility

- **Python public API**: no change to `Client.from_conf`,
  `Client.dataframe`, `Client.close`, `Client.reap_idle`, or to
  `Sender.dataframe`. Existing user code keeps working.
- **`UnsupportedDataFrameShapeError`** keeps its class identity and
  `BadDataFrame` code; the set of frames it gets raised for shrinks
  over Steps 2-4.
- **Existing fuzz seeds**: pre-existing failure seeds may now turn
  green (a frame that used to be rejected may now succeed), but no
  pre-existing green seed should turn red. The fuzz's
  `expected_supported` derivation will need updates as supported
  shapes broaden; the generator already keeps `expected_supported`
  consistent with the planner's rules so the necessary changes are
  local to the supported / unsupported gen lists.
- **FFI consumers outside this repo**: none today beyond Python. PR
  #148 is unreleased.

## Open questions

### Q1: does `column_sender_chunk_column_*` already do "direct to wire", or is there an intermediate buffer?

**Answered: direct to wire.** Reading
`c-questdb-client/questdb-rs/src/ingress/column_sender/`:

- `Chunk::column_i64` etc. store `data.as_ptr()` in a
  `ColumnDescriptor`. No copy on append.
  ([`chunk.rs:208-211`](c-questdb-client/questdb-rs/src/ingress/column_sender/chunk.rs))
- `encode_chunk_into(out: &mut Vec<u8>, chunk, ...)` is called at flush
  time. `out` is the wire frame buffer; the encoder writes column bytes
  directly into it. There is no intermediate column-format scratch
  buffer.
  ([`encoder.rs:82-95`](c-questdb-client/questdb-rs/src/ingress/column_sender/encoder.rs))
- Hot path for contiguous + native-LE + no-validity primitives is one
  `extend_from_slice` per column — i.e. one bulk memcpy from caller
  buffer to wire buffer.
  ([`encoder.rs:460-466`](c-questdb-client/questdb-rs/src/ingress/column_sender/encoder.rs))

**Implication for Step 3.** The "extra memcpy" framing in Victor's
note is moot for native NumPy data — the per-type FFI is already as
cheap as a direct-write would be. The NumPy appender's value
proposition shifts:

- **Native NumPy primitives, contiguous, LE**: no benefit over the
  existing per-type calls.
- **Narrower dtypes (`int8/16/32`, `uint*`, `float32`)**: the
  appender lets Rust widen / pack on the fly into wire bytes; otherwise
  Cython has to widen with a Python-side alloc.
- **Strided arrays (`strides[0] != itemsize`)**: appender does a
  gather. Without it, Cython has to materialise a contiguous copy.
- **Non-native-endian**: appender byte-swaps on read. Without it,
  Cython has to copy + bswap.

So Step 3 stays in the plan, but reframed as "remove three Python-side
copy paths" rather than "eliminate an FFI-level memcpy."

### Q2: writer-mixing in one chunk

Confirm that `column_sender_chunk_append_arrow_column`,
`_append_numpy_column`, and the existing per-type appends can all be
called on the same chunk and produce a valid wire frame.

### Q3: `must_close` propagation between writers

When two writers share a conn within one `Client.dataframe()` call (one
Arrow column then one NumPy column), and the second one fails, the
first one's data may already be in the wire buffer. Confirm the
conn-level `must_close` makes the next `flush` / `sync` a no-op /
error rather than committing partial data.

### Q4: Auth / TLS handshake state

Where does it live? Likely on the conn (handshake is per-socket), but
worth confirming that re-handshakes aren't needed on every borrow.

### Q5: read + write coexistence on one conn

Can a single conn alternate read and write operations sequentially?
This affects whether ingress and egress can share a borrowed conn or
need separate borrows.

### Q6: pool concurrency

Today `pool_max` caps physical connections; concurrent borrows beyond
that return `line_sender_error_invalid_api_call`. Egress readers may
hold a conn for the duration of a result iteration, which can be much
longer than a write. Reconsider whether the cap-based behaviour is
right for mixed workloads, or whether `borrow_conn` should block.

## Out of scope

- HTTP / TCP `Sender.dataframe` legacy text path (untouched).
- Egress query API design (separate doc).
- Concurrency model for multi-threaded `Client` access (round-1
  review's `close()`-vs-`dataframe()` TOCTOU stays open).
- Configuration grammar changes — `pool_size`, `pool_max`,
  `pool_idle_timeout_ms`, `pool_reap` keep their current semantics.

## References

- [`plan-pandas-columnar-performance.md`](plan-pandas-columnar-performance.md)
  — current benchmark plan and v1 scope.
- [`c-questdb-client/doc/COLUMN_SENDER_FFI_ABI.md`](c-questdb-client/doc/COLUMN_SENDER_FFI_ABI.md)
  — current FFI ABI doc.
- Slack: Victor's design doc (2025-05-27), Jaromir/Victor exchange on
  pool semantics (2025-05-27).
- Commit
  [`735aa96`](https://github.com/questdb/py-questdb-client/commit/735aa962c51be69e427e8b4a3fb040ab8db243cb)
  — current v1 `Client.dataframe()` implementation, baseline for this
  plan.
