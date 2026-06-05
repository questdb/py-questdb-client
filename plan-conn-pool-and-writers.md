# Pooled QWP/WS Connection + Per-Source Writers — Architecture Plan

## TL;DR

Refactor the FFI so the pool holds **QWP/WS connections**, not writers.
`column_sender_chunk` becomes one writer over a borrowed connection
alongside two siblings: a generic Arrow per-column appender (Victor's
(B)) and a NumPy fast-path appender (Victor's NumPy / (C)). A fourth
one-shot path — `column_sender_flush_arrow_batch[_at_column]` —
sends a whole Arrow `RecordBatch` without going through the chunk
lifecycle and is the natural entry point for Arrow-native sources.
Egress readers join the same pool when ready. The Python `Client`
keeps its public surface unchanged; Pandas DataFrames drive
per-column dispatch (Arrow / NumPy / PyObject / per-type) to
eliminate v1 `UnsupportedDataFrameShapeError` rejections, while
**Polars / PyArrow / any Arrow-PyCapsule-Interface source** (cudf /
duckdb / modin / …) ride the one-shot path through `__arrow_c_stream__`
— no per-column Cython dispatch, no hard pyarrow dependency.

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
| `column_sender_chunk` `_append_numpy_column` | NumPy buffer + `dtype` enum (+ `extras` for decimal scale / geohash bits / ndarray shape) | **NEW**, matches Victor's (C); direct-to-wire (see [Q1](#q1-does-column_sender_chunk_column_-already-do-direct-to-wire-or-is-there-an-intermediate-buffer)) |
| `column_sender_flush_arrow_batch[_at_column]` (one-shot) | whole `ArrowArray` Struct + `ArrowSchema` | **NEW**, used by Polars path; bypasses chunk lifecycle |
| egress readers (TBD) | server → caller | separate effort; design out of scope |

The three chunk-based append modes (per-type / `_append_arrow_column`
/ `_append_numpy_column`) share one `column_sender_chunk` lifecycle
— they can be mixed within a single chunk (a frame with a NumPy
`int64`, an Arrow string, and a sniffed-and-built `PyObject` decimal
all coexist in one chunk). The row-count lock holds across writer
types.

The fourth one-shot `flush_arrow_batch[_at_column]` path is
independent: it consumes a whole `ArrowArray` Struct + `ArrowSchema`
in a single FFI call, encodes one QWP frame, and pushes through the
borrowed conn. No chunk handle, no per-column dispatch — used when
the entire source is already Arrow-shaped (Polars / PyArrow / cudf /
…).

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
 *
 * `row_offset` + `row_count` slice the Arrow array on the Rust side
 * without taking a copy — used by `Client.dataframe()`'s chunked
 * emission so each chunk references a window of the same upstream
 * Arrow buffers. When `validity` is present, `row_offset` must be a
 * multiple of 8 (Arrow validity is bit-addressed; sub-byte windows
 * are unsupported).
 */
QUESTDB_CLIENT_API
bool column_sender_chunk_append_arrow_column(
    column_sender_chunk* chunk,
    const char* name,
    size_t name_len,
    struct ArrowArray* array,
    const struct ArrowSchema* schema,
    size_t row_offset,
    size_t row_count,
    line_sender_error** err_out);
```

This subsumes today's piecemeal varchar, symbol-dict, validity-bitmap
plumbing. Where the source layout already matches the wire, the
internal write is `memcpy`; otherwise the writer streams source ->
wire per element with no intermediate column-format buffer.

### Arrow RecordBatch one-shot path (NEW — beside (B))

For callers holding a whole `pa.RecordBatch` (or any Arrow source that
can produce one), a one-shot path bypasses the chunk lifecycle:

```c
QUESTDB_CLIENT_API
bool column_sender_flush_arrow_batch(
    qwpws_conn* conn,
    line_sender_table_name table,
    struct ArrowArray* array,    /* Struct array — one field per column */
    struct ArrowSchema* schema,
    line_sender_error** err_out);

QUESTDB_CLIENT_API
bool column_sender_flush_arrow_batch_at_column(
    qwpws_conn* conn,
    line_sender_table_name table,
    struct ArrowArray* array,
    struct ArrowSchema* schema,
    line_sender_column_name ts_column,
    line_sender_error** err_out);
```

Both are gated on `QUESTDB_CLIENT_ENABLE_ARROW`. Use when the source
is a single `pa.RecordBatch`-shaped object and the caller doesn't
need to mix in per-type / NumPy columns. The chunk-based
`_append_arrow_column` route is for cases where the chunk is being
populated from a mix of physical sources (Pandas DataFrame with
some Arrow + some NumPy + some PyObject columns).

**Recent semantic changes** worth noting for wrapper authors:

- **Multi-schema-per-conn supported.** A single `qwpws_conn` can
  receive successive `flush_arrow_batch` calls with **different
  schemas**. The `SchemaRegistry` interns each schema-signature on
  first emit (FULL wire mode), later sends reference its assigned
  id (REFERENCE mode). Implication: `Client.dataframe()` can be
  called repeatedly on the same conn with DataFrames of different
  shapes — no need to drop and reconnect. Validated by
  `arrow_ingress_fuzz.py::test_schema_grows_new_column_in_batch2_accepted`
  and `…_drops_column_in_batch2_accepted`.
- **Empty `ts_column_name` rejected as `invalid_name`** (was
  `invalid_api_call`). Wrapper should not pre-check empty strings
  on the Python side and synthesize an error itself; let the FFI
  emit the canonical error.
- **BYTE / SHORT wire null sentinel is `value=0`**. An Arrow
  `Int8` / `Int16` source value of 0 round-trips as NULL on the
  server. Same convention as the row-API `column_i8` / `column_i16`
  and the NumPy `i8` / `i16` appender. Callers needing literal-0
  fidelity must widen to INT (Arrow `Int32`) before the FFI call.
- **Designated TS column rejects nulls; field TS column accepts
  nulls.** Asymmetric on purpose — a designated TS is the row's
  identity timestamp (required), a field TS is a regular data
  column (optional). The Cython wrapper should reflect this when
  routing `at = column_name`: that column must be non-null,
  validated client-side before flush.
- **Decimal32 widens to Decimal64 wire.** No QWP `Decimal32` wire
  kind exists; `arrow_batch::classify` maps `Decimal32(p, s)` →
  `ColumnKind::Decimal32WidenToDecimal64` (8B/row, lossless since
  i32 ⊂ i64). Wrapper passes Decimal32 through without special
  handling.

### NumPy direct appender (NEW — Victor's (C), refined)

```c
typedef enum column_sender_numpy_dtype {
    /* Signed ints — identity wire width. BYTE / SHORT use value 0 as
       the wire null sentinel: source value 0 round-trips as NULL. */
    column_sender_numpy_i8 = 0,  /* → BYTE  (1B/row, sentinel = 0)        */
    column_sender_numpy_i16 = 1, /* → SHORT (2B/row, sentinel = 0)        */
    column_sender_numpy_i32 = 2, /* → INT   (4B/row, sentinel = i32::MIN) */
    column_sender_numpy_i64 = 3, /* → LONG  (8B/row, sentinel = i64::MIN) */

    /* Unsigned ints — widen to smallest signed wire that avoids the
       null sentinel. BYTE/SHORT use 0 as null so u8 can't fit there. */
    column_sender_numpy_u8 = 4,  /* → INT  (4B/row, widen u8→i32)         */
    column_sender_numpy_u16 = 5, /* → INT  (4B/row, widen u16→i32)        */
    column_sender_numpy_u32 = 6, /* → LONG (8B/row, widen u32→i64)        */
    column_sender_numpy_u64 = 7, /* → LONG (8B/row, bit-reinterpret;
                                    values > i64::MAX wrap to negative)   */

    column_sender_numpy_f32 = 8,  /* → DOUBLE (8B/row, widen)             */
    column_sender_numpy_f64 = 9,  /* → DOUBLE                             */
    column_sender_numpy_bool = 10,/* → BOOLEAN (LSB-packed bitmap)        */
    column_sender_numpy_f16 = 11, /* → FLOAT (per-row IEEE-754 widen)     */

    /* datetime64 — units ms/us/ns direct; s/m/h/D constant ×K to µs;
       Y/M proleptic-Gregorian calendar conversion (anchored at start
       of 1970-01 + N months / 1970 + N years). All emit TIMESTAMP. */
    column_sender_numpy_datetime64_s = 12,
    column_sender_numpy_datetime64_ms = 13,
    column_sender_numpy_datetime64_us = 14,
    column_sender_numpy_datetime64_ns = 15,
    column_sender_numpy_datetime64_m = 32,
    column_sender_numpy_datetime64_h = 33,
    column_sender_numpy_datetime64_D = 34,
    column_sender_numpy_datetime64_M = 35,
    column_sender_numpy_datetime64_Y = 36,

    column_sender_numpy_timedelta64_s = 16,
    column_sender_numpy_timedelta64_ms = 17,
    column_sender_numpy_timedelta64_us = 18,
    column_sender_numpy_timedelta64_ns = 19,

    column_sender_numpy_s16 = 20,  /* 16B/row → UUID    */
    column_sender_numpy_s32 = 21,  /* 32B/row → LONG256 */

    /* Metadata-disambiguated narrow ints (callers must opt-in;
       indistinguishable from plain int/uint by dtype alone). */
    column_sender_numpy_u32_ipv4 = 25, /* → IPV4 */
    column_sender_numpy_u16_char = 26, /* → CHAR */

    /* Need `extras.decimal_scale` ∈ 0..=N (18/38/76). */
    column_sender_numpy_decimal_s8 = 22,  /* → DECIMAL64  */
    column_sender_numpy_decimal_s16 = 23, /* → DECIMAL128 */
    column_sender_numpy_decimal_s32 = 24, /* → DECIMAL256 */

    /* Need `extras.geohash_bits` ∈ 1..=8 / 16 / 32 / 60. */
    column_sender_numpy_geohash_i8 = 27,
    column_sender_numpy_geohash_i16 = 28,
    column_sender_numpy_geohash_i32 = 29,
    column_sender_numpy_geohash_i64 = 30,

    /* Need `extras.array_ndim` + `extras.array_shape`. Rectangular
       per-row tensor; ragged data → use Arrow `List<Float64>`. */
    column_sender_numpy_f64_ndarray = 31,
} column_sender_numpy_dtype;

/* Carries dtype-specific parameters. Pass NULL for any dtype that
   doesn't list an `extras.*` requirement above. */
typedef struct column_sender_numpy_extras {
    int8_t   decimal_scale;
    uint8_t  geohash_bits;
    uint8_t  array_ndim;          /* 1..=32 */
    const uint32_t* array_shape;  /* array_ndim entries, each >= 1 */
} column_sender_numpy_extras;

QUESTDB_CLIENT_API
bool column_sender_chunk_append_numpy_column(
    column_sender_chunk* chunk,
    const char* name,
    size_t name_len,
    column_sender_numpy_dtype dtype,
    const uint8_t* data,
    size_t row_count,
    const column_sender_validity* validity,
    const column_sender_numpy_extras* extras,  /* NULL if not needed */
    line_sender_error** err_out);
```

**Two-class enum** — see [Q2: NumPy dtype auto-dispatch vs. user hint](#q2-numpy-dtype-auto-dispatch-vs-user-hint):

- **A class (numpy dtype-determinable)**: i8/16/32/64, u8/16/32/64,
  f16/32/64, bool, datetime64[*], timedelta64[*], s16, s32 — Python
  wrapper picks the enum value purely from `arr.dtype`.
- **B class (synthetic, requires user/schema hint)**: u32_ipv4,
  u16_char, decimal_s{8,16,32}, geohash_i{8,16,32,64}, f64_ndarray
  — `arr.dtype` alone cannot disambiguate; needs an explicit Python
  API entry (per-column method) or per-column schema dict.

**Constraints (single pass, no scratch arena):**

- Source buffer MUST be contiguous (`arr.strides[0] == arr.itemsize`)
  and native-endian. Strided / non-native-endian arrays must be
  consolidated upstream (`arr.copy()` / `arr.byteswap().newbyteorder()`).
- BYTE/SHORT wire types use value 0 as null sentinel — `column_i8` /
  `column_i16` and `numpy_i8` / `numpy_i16` share this constraint with
  the existing row API; callers wanting a literal 0 to round-trip
  must use INT (cast first).

### Mixed writers in one chunk

A `column_sender_chunk` can be populated by any combination of
per-type, Arrow, and NumPy appends. The row-count lock holds across
writer types. Example flow for a mixed-physical DataFrame:

```c
chunk = column_sender_chunk_new("trades", 6, &err);

/* NumPy int64 column, contiguous, native LE -> direct-write. */
column_sender_chunk_append_numpy_column(chunk, "seq", 3,
    column_sender_numpy_i64,
    seq_ptr, n_rows, NULL, NULL, &err);

/* NumPy decimal column with scale=2 -> DECIMAL64. */
column_sender_numpy_extras extras = {0};
extras.decimal_scale = 2;
column_sender_chunk_append_numpy_column(chunk, "price", 5,
    column_sender_numpy_decimal_s8,
    price_mantissa_ptr, n_rows, validity, &extras, &err);

/* Arrow UTF-8 column, large_string is narrowed in Rust.
   `row_offset = 0, row_count = n_rows` for the whole array. */
column_sender_chunk_append_arrow_column(chunk, "note", 4,
    note_arrow_array, note_arrow_schema,
    0, n_rows, &err);

/* Per-type IPv4 fallback (Cython sniff + build). */
column_sender_chunk_column_ipv4(chunk, "addr", 4,
    ipv4_buf, n_rows, validity, &err);

/* Designated timestamp. */
column_sender_chunk_designated_timestamp_nanos(chunk, ts_ptr, n_rows, &err);

/* Flush + sync over a borrowed conn. */
column_sender_flush(conn, chunk, &err);
column_sender_sync(conn, column_sender_ack_level_ok, &err);
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

**Top-level input dispatch** (whole-DataFrame; before per-column
dispatch kicks in):

| Input type | Path |
|---|---|
| `pl.DataFrame` / `pl.LazyFrame` | (E) one-shot Arrow path via `__arrow_c_stream__` + `column_sender_flush_arrow_batch[_at_column]`; polars-side `df.slice()` for `max_rows_per_batch` |
| `pa.Table` / `pa.RecordBatch` | (E) same one-shot path; pyarrow also exposes `__arrow_c_stream__`. `pa.Table.slice()` for `max_rows_per_batch` |
| Anything else with `__arrow_c_stream__` (cudf / duckdb / modin / …) | (E) same path; `max_rows_per_batch` left to producer chunking |
| `pd.DataFrame` | (A/B/C/D) per-column dispatch table below |

Pandas remains on the chunk-based per-column dispatch because its
columns can be physically heterogeneous (NumPy + Arrow + PyObject +
extension dtypes mixed in one frame). All other DataFrame libraries
are Arrow-native end-to-end, so the per-column dispatch is wasted
work for them — `flush_arrow_batch` ingests the whole batch in one
shot.

**Per-column dispatch (Pandas only)** — mirrors `dataframe.pxi`'s
`col_source_t` enum:

| `col_source_t` | Path | Notes |
|---|---|---|
| `bool_pyobj / bool_numpy / bool_arrow` | bool: (D)/(C)/(B) by source | BOOLEAN wire; numpy byte-per-row packed to LSB-first bitmap; pyobj rejects nulls (BOOLEAN has no row-level null) |
| `int_pyobj / float_pyobj / str_pyobj / decimal_pyobj` | (D) Cython sniff + growable-buffer build | i64 + LSB validity / f64 + LSB / utf8 + offsets / `mpdecimal` mantissa |
| `u8/i8/u16/i16/u32/i32/u64/i64 numpy` | (C) NumPy appender, auto-dispatch | i8→BYTE, i16→SHORT, i32→INT, i64→LONG (identity wire); u8/u16→INT, u32/u64→LONG (widen) |
| `u8/i8/u16/i16/u32/i32/u64/i64 arrow` | (B) per-column Arrow appender | Same identity-width wire mapping as numpy |
| `f32/f64 numpy / arrow` | (C)/(B) | F32→DOUBLE, F64→DOUBLE |
| `str_utf8_arrow / str_lrg_utf8_arrow` | (B) | LargeUtf8 normalized to u32 offsets in Rust |
| `str_i8_cat / str_i16_cat / str_i32_cat` | (B) | pandas Categorical / pyarrow Dictionary → SYMBOL (`symbol_dict_i*`) |
| `dt64ns_numpy / dt64us_numpy` | (C) | datetime64[ns]→TIMESTAMP_NANOS, datetime64[us]→TIMESTAMP |
| `dt64ns_tz_arrow / dt64us_tz_arrow` | (B) | tz-aware → wire as UTC µs/ns; tz only matters client-side |
| `arr_f64_numpyobj` | (D) per-row build into DOUBLE_ARRAY wire | numpy object array of f64 ndarrays; per-row shape may differ (ragged allowed via per-row ndim/shape headers in DOUBLE_ARRAY wire) |
| `decimal32/64/128/256 arrow` | (B) | Decimal32 widens to Decimal64 (no QWP Decimal32 wire kind); 64/128/256 direct |
| `fsb16_arrow → UUID, fsb32_arrow → LONG256` | (B) | Arrow FixedSizeBinary(16/32) |
| `bool_pyobj` etc. → bool target | (D) | already covered |
| Designated timestamp from a `TimestampNanos(literal)` / `datetime` / `df.index` | materialize once as a single-value or full Arrow column on the Python side, then route via (B) | — |

**Wire targets currently reachable** (`col_target_t`): table, symbol,
bool, i64, f64, str, ts, arr_f64, decimal, at, i8, i16, i32, f32,
uuid, long256, ipv4.

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

### Arrow-native DataFrame support (Polars / PyArrow / generic)

Polars, PyArrow, and any other Python object implementing the
[Arrow PyCapsule Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html)
(`__arrow_c_stream__`) take a **dedicated, simpler path (E)** than
Pandas: the one-shot `column_sender_flush_arrow_batch` / `_at_column`
FFI, bypassing `column_sender_chunk` entirely. No per-column Cython
dispatch, no chunk lifecycle, no hard pyarrow dependency.

**Sources that ride this path:**

- `pl.DataFrame` / `pl.LazyFrame` (polars ≥ 1.0)
- `pa.Table` / `pa.RecordBatch` (pyarrow ≥ 14)
- `cudf.DataFrame`, `duckdb.DuckDBPyRelation.arrow()`, `modin`,
  any future Arrow-native lib — works via the same PyCapsule
  protocol, no per-library code

**Why this works:**

1. Arrow-native libs already hold `ArrowArray` / `ArrowSchema` C
   structs internally; `__arrow_c_stream__` exposes the pointers
   zero-copy. Same structs our FFI consumes.
2. The Pandas A/B/C/D dispatch exists because Pandas columns can be
   physically heterogeneous (NumPy + Arrow + PyObject + extension
   dtypes in one frame). Arrow-native libs never have this problem.
3. `flush_arrow_batch_at_column` takes a `ts_column` name directly,
   matching `client.dataframe(df, at='my_ts_col')` cleanly without
   materialising the column separately.
4. `classify()` Rust-side already reads `questdb.*` field metadata
   for B-class wire types (IPv4 / CHAR / Geohash / Decimal /
   DOUBLE_ARRAY). Users attach metadata once on the schema/field
   level (Polars `pl.Field.metadata` / PyArrow `pa.field(..., metadata=...)`);
   `classify()` reads it during ingest. See
   [B-class metadata via Arrow Field metadata](#b-class-metadata-via-arrow-field-metadata)
   below.

**No pyarrow dependency — Arrow PyCapsule Interface route:**

Polars ≥ 1.0 implements the [Arrow PyCapsule Interface](
https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html)
(`__arrow_c_array__` / `__arrow_c_stream__` / `__arrow_c_schema__`).
These return `PyCapsule` objects that **directly wrap the Arrow C
Data Interface pointers** (`ArrowArray*` + `ArrowSchema*` /
`ArrowArrayStream*`). The wrapper extracts the raw pointers and
passes them to `column_sender_flush_arrow_batch` — `pyarrow` never
enters the picture.

The PyCapsule owns the Arrow `release` callback; the wrapper just
borrows the pointer for the duration of the FFI call. Standard
PyCapsule lifecycle — when the capsule is GC'd, `release` runs.

**Dispatch rule for the wrapper:**

```python
def dataframe(self, df, *, table_name=None, at=None,
              max_rows_per_batch=None, schema_overrides=None, ...):
    # Polars first — it gets `.slice()` for max_rows enforcement.
    if _is_polars_dataframe(df) or _is_polars_lazyframe(df):
        if _is_polars_lazyframe(df):
            df = df.collect()
        n = df.height
        max_rows = max_rows_per_batch or _DEFAULT_MAX_ROWS
        for off in range(0, n, max_rows):
            window = df.slice(off, max_rows)             # zero-copy
            self._ingest_arrow_capsule(window, table_name, at,
                                       schema_overrides)
        return
    # PyArrow / DuckDB / cudf / modin / anything else exposing the
    # capsule protocol. Pyarrow Table also has `.slice()` for
    # max_rows; for generic sources, trust producer chunking.
    if hasattr(df, '__arrow_c_stream__'):
        if hasattr(df, 'slice') and max_rows_per_batch:
            n = getattr(df, 'num_rows', None) or getattr(df, 'height', None)
            if n is not None:
                for off in range(0, n, max_rows_per_batch):
                    self._ingest_arrow_capsule(
                        df.slice(off, max_rows_per_batch),
                        table_name, at, schema_overrides)
                return
        self._ingest_arrow_capsule(df, table_name, at, schema_overrides)
        return
    if _is_pandas(df):
        return self._ingest_pandas(df, ...)   # existing chunk-based A/B/C/D
    raise UnsupportedDataFrameShapeError(...)

# Cython sketch — pseudocode for the inner loop. Same code for any
# source that exposes `__arrow_c_stream__`.
def _ingest_arrow_capsule(self, obj, table_name, at, schema_overrides):
    if schema_overrides:
        obj = _apply_schema_overrides(obj, schema_overrides)   # adds Arrow Field metadata
    schema_capsule, stream_capsule = obj.__arrow_c_stream__()
    cdef ArrowSchema* schema = <ArrowSchema*>PyCapsule_GetPointer(
        schema_capsule, b"arrow_schema")
    cdef ArrowArrayStream* stream = <ArrowArrayStream*>PyCapsule_GetPointer(
        stream_capsule, b"arrow_array_stream")
    conn = self._borrow_conn()
    try:
        cdef ArrowArray batch
        while True:
            if stream.get_next(stream, &batch) != 0:
                raise _stream_error(stream)
            if batch.release is NULL:
                break  # end of stream
            try:
                if at is None or at is ServerTimestamp:
                    ok = column_sender_flush_arrow_batch(
                        conn, table_name_view, &batch, schema, &err)
                else:
                    ok = column_sender_flush_arrow_batch_at_column(
                        conn, table_name_view, &batch, schema,
                        at_column_name_view, &err)
                if not ok:
                    raise _decode_error(err)
            finally:
                if batch.release is not NULL:
                    batch.release(&batch)
        column_sender_sync(conn, ack_level_ok, &err)
    finally:
        self._return_conn(conn, force_drop=had_exception)
```

**Why stream, not `__arrow_c_array__`:** Polars DataFrames are often
chunked internally (`df.n_chunks() > 1`). `__arrow_c_array__()` only
works on a single contiguous array. The stream interface yields one
batch per Polars chunk with no rechunk / copy. For single-chunk
inputs the stream still works — it just yields one batch then EOF.

**Per-batch row cap (`max_rows_per_batch`):**

Rust FFI safety bound caps each `flush_arrow_batch` call at 16M
rows. Wire-side: a single QWP frame is bounded too. The wrapper
exposes `max_rows_per_batch: int | None = None` on
`Client.dataframe()` (default 16384 set wrapper-side).

Enforcement is **upstream of the capsule** — `pl.DataFrame.slice()`
or `pa.Table.slice()` is zero-copy (just adjusts buffer offsets, no
rechunk). For generic `__arrow_c_stream__` sources without a `.slice`
method, the wrapper trusts producer chunking (the source's
`get_next` callback yields its own batch sizes).

**`at` parameter mapping:**

| `at` value | FFI |
|---|---|
| `ServerTimestamp` (default) | `column_sender_flush_arrow_batch` (server stamps) |
| column name `str` | `column_sender_flush_arrow_batch_at_column(... ts_column=name)` |
| `TimestampNanos(literal)` / `datetime` | materialize a 1-column Arrow array, append to the batch, then route via `_at_column` |

**Constraints (Polars-specific):**

- `pl.Object` (rare; pickled Python values) → rejected, no Arrow
  representation.
- `pl.Struct` → out of scope; QuestDB has no STRUCT wire type.
  Users flatten upstream.
- `pl.List(pl.Float64)` → DOUBLE_ARRAY (rectangular OK, ragged
  rejected with `ArrowIngest`). Already handled by `classify()`
  Rust-side.
- `pl.Categorical` → SYMBOL via dictionary path. Already handled.

**Trade-offs vs. chunk-based ingestion:**

| | `flush_arrow_batch` (Polars) | chunk + `_append_arrow_column` (Pandas-mixed) |
|---|---|---|
| Per-column Cython loop | none | yes |
| Mixed NumPy + Arrow + PyObject in one frame | not supported | supported |
| Splits large frame | polars-side `df.slice(offset, max_rows)`, zero-copy | chunk planner already does row-count slicing |
| Designated TS from a column | native (`_at_column` variant) | manually populate `_designated_timestamp_*` per chunk |
| Lines of Cython | ~30 | hundreds |

### B-class metadata via Arrow Field metadata

For B-class wire types (IPv4 / CHAR / Geohash / Decimal /
DOUBLE_ARRAY), the user **must** tell us "this i32 column is a
geohash" — neither `arr.dtype` nor Arrow type alone disambiguates.
Two opt-in mechanisms supported:

**(1) `schema_overrides` keyword on `Client.dataframe()`:**

```python
client.dataframe(df, schema_overrides={
    'addr': 'ipv4',
    'loc':  ('geohash', 20),   # (kind, bits)
    'price': ('decimal', 2),   # (kind, scale)
    'wave':  ('array', 1),     # (kind, ndim) for f64 DOUBLE_ARRAY
})
```

Wrapper-side: when extracting the PyCapsule, the wrapper rebuilds
the `ArrowSchema` with the corresponding `questdb.*` field metadata
injected on the matching fields:

| Override | Injected metadata |
|---|---|
| `'ipv4'` | `questdb.column_type=ipv4` |
| `'char'` | `questdb.column_type=char` |
| `('geohash', bits)` | `questdb.geohash_bits=<bits>` |
| `('decimal', scale)` | (scale lives on Arrow `Decimal` type itself; no metadata needed) |
| `('array', ndim)` | `questdb.array_ndim=<ndim>` (or just rely on Arrow `List<…<Float64>>` nesting depth) |

`classify()` Rust-side already reads all of these.

**(2) User-attached `Field` metadata** (no `schema_overrides` needed):

PyArrow:
```python
schema = pa.schema([
    pa.field('addr', pa.uint32(),
             metadata={b'questdb.column_type': b'ipv4'}),
    pa.field('loc', pa.int32(),
             metadata={b'questdb.geohash_bits': b'20'}),
    pa.field('price', pa.decimal128(38, 2)),
    pa.field('ts', pa.timestamp('ns')),
])
batch = pa.RecordBatch.from_arrays([...], schema=schema)
client.dataframe(batch, at='ts')
```

Polars (≥ 1.20 once `Field.metadata` lands; today via
`schema_overrides`):
```python
df = pl.DataFrame({...}).with_metadata({
    'addr': {'questdb.column_type': 'ipv4'},
    'loc':  {'questdb.geohash_bits': '20'},
})
client.dataframe(df, at='ts')
```

The wrapper passes the user's metadata through unchanged; Rust-side
`classify()` reads it during ingest.

**Validation:**

The wrapper validates `schema_overrides` keys at the entry point:
- Unknown override kind → `UnsupportedDataFrameShapeError`
- Override on a non-matching Arrow type (e.g. `geohash` on a Utf8
  column) → `UnsupportedDataFrameShapeError`
- Override on a column not present in the DataFrame → log warning,
  ignore (matches pandas's `astype(dict)` convention)

**Dependency:**

Polars is an **optional** dep — `pyproject.toml` adds `polars` to
`extras_require['polars']` (or `extras_require['all']`). The
`Client.dataframe()` polars path imports `polars` lazily on first
call; absence raises `ImportError("polars not installed; pip install
questdb-client[polars]")`.

**No pyarrow required.** Polars ≥ 1.0 exposes the Arrow PyCapsule
Interface natively (`__arrow_c_stream__`); we extract `ArrowSchema*`
+ `ArrowArrayStream*` directly from the capsules via
`PyCapsule_GetPointer` in Cython. Pure-polars install works
end-to-end. (`pyarrow` is *optional*-optional — only needed if the
user passes a `pa.Table` / `pa.RecordBatch` directly, or wants to
extract pyarrow-specific metadata.)

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

### Step 3 — Rust + Python: NumPy direct appender + identity-width wire ✅ done

Submodule `ba0cf92`, parent `d420d79`; **revised** in the
narrow-int identity follow-up (`doc/NUMPY_NARROW_INT_DESIGN.md`).

Covers the wide-set of NumPy dtypes the v1 columnar path was
rejecting, **without inflating wire size**:

- `i8 → BYTE` (1B identity), `i16 → SHORT` (2B identity),
  `i32 → INT` (4B identity), `i64 → LONG`. **Source value 0
  round-trips as NULL on BYTE/SHORT** — wire-protocol sentinel; same
  constraint as the existing row-API `column_i8` / `column_i16`.
- `u8 → INT`, `u16 → INT` (per-row widen). Naïve "u8 → SHORT" would
  be 2× narrower but SHORT's value-0 null sentinel collides with the
  most common u8 value (0). INT (`i32::MIN` sentinel) is the
  minimum signed wire that's collision-free.
- `u32 → LONG` (widen), `u64 → LONG` (bit-reinterpret; values >
  `i64::MAX` wrap negative — matches the row-path's C cast).
- `f32 → DOUBLE`, `f64 → DOUBLE`, `f16 → FLOAT`.
- `bool` (byte-per-row) → BOOLEAN (LSB-first packed bitmap).
- `datetime64[*]` → DATE / TIMESTAMP / TIMESTAMP_NANOS. Coverage:
  - `[ms]` → DATE (8B direct).
  - `[us]` → TIMESTAMP (8B direct).
  - `[ns]` → TIMESTAMP_NANOS (8B direct).
  - `[s/m/h/D]` → TIMESTAMP via constant ×K to µs (per-row, overflow
    rejected).
  - `[Y/M]` → TIMESTAMP via proleptic-Gregorian calendar conversion
    (anchored at start of 1970-01 + N months / start of 1970 + N
    years; per-row, capped at ±292_277 years to stay in i64 µs).
- `timedelta64[s/ms/us/ns]` → LONG (i64 reinterpret).
- `S16` → UUID, `S32` → LONG256 (16/32 raw bytes per row).
- DECIMAL64/128/256 via `column_sender_numpy_decimal_s{8,16,32}` +
  `extras.decimal_scale`.
- GEOHASH via `column_sender_numpy_geohash_i{8,16,32,64}` +
  `extras.geohash_bits`.
- DOUBLE_ARRAY via `column_sender_numpy_f64_ndarray` +
  `extras.array_ndim` + `extras.array_shape` (rectangular tensor;
  ragged → Arrow `List<Float64>` via the Arrow appender).

Implementation:
- Direct single-pass write into the conn's outbound frame at flush
  time. No chunk-side scratch arena; `data` is the raw NumPy buffer
  pointer and must stay alive until next `column_sender_flush /
  _sync` returns.
- FFI: `column_sender_chunk_append_numpy_column(chunk, name, name_len,
  dtype, data, row_count, validity, extras, err)`.
- Cython: split the dispatch into two classes —
  - A-class auto-dispatch from `arr.dtype` (numeric / bool /
    datetime64 / timedelta64 / fixed-size bytes).
  - B-class (IPv4 / CHAR / Decimal / Geohash / Ndarray) only
    reachable via explicit per-column hint or `df.cast()`-style
    schema metadata; never inferred from dtype alone.
- Validator accepts the new sources; fuzz moves the corresponding
  generators into `SUPPORTED_FIELD_GENS_WEIGHTED`.

Strided arrays and non-native-endian arrays are not supported — the
Python wrapper consolidates upstream (`arr.copy()` /
`arr.byteswap().newbyteorder()`).

**Success criterion** (met): UNSUPPORTED_FIELD_GENS is empty (every
narrower numeric generator + native bool round-trips); 941 Python
tests, 836 Rust unit tests, 4 fuzz seeds × 200 iters all green.

**Wire-size win vs. the pre-revision design**:

| Pandas/NumPy dtype | Pre-revision wire | Current wire | Savings |
|---|---|---|---|
| `int8`   | LONG (8B) | BYTE  (1B) | **8×** |
| `int16`  | LONG (8B) | SHORT (2B) | **4×** |
| `int32`  | LONG (8B) | INT   (4B) | **2×** |
| `uint8`  | LONG (8B) | INT   (4B) | **2×** |
| `uint16` | LONG (8B) | INT   (4B) | **2×** |
| `uint32` | LONG (8B) | LONG  (8B) | — (unchanged) |

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

### Step 5 — Python: Arrow-native DataFrame support ✅ done

Added Polars (`pl.DataFrame` / `pl.LazyFrame`), PyArrow (`pa.Table` /
`pa.RecordBatch`), and any other Arrow-PyCapsule-Interface source
(`__arrow_c_stream__`) as first-class `Client.dataframe()` inputs,
riding `column_sender_flush_arrow_batch[_at_column]`.

**Step 5a — wrapper hook + dispatcher** ✅:
`_is_polars_dataframe_or_lazy`, `hasattr(df, '__arrow_c_stream__')`,
`_is_pandas` predicates dispatched in order at the top of
`Client.dataframe()`. Polars import is lazy
(`_try_import_polars()` → cached in `_POLARS`). pyarrow only loaded
when `schema_overrides` is provided.

**Step 5b — Cython PyCapsule extraction + flush loop** ✅:
- `PyCapsule_GetPointer` + `PyCapsule_IsValid` cimported from
  `cpython.pycapsule`.
- `ArrowArrayStream` already in `arrow_c_data_interface.pxd`.
- `_ingest_arrow_capsule_stream(conn, b, obj, table, at, &any_flushed)`
  pulls batches from the stream via `stream.get_next` and calls
  `column_sender_flush_arrow_batch[_at_column]` per batch.
- `pl.DataFrame.slice()` / `pa.Table.slice()` zero-copy chunking for
  `max_rows_per_batch` enforcement (default 16384); generic
  sources trust producer chunking.

**Step 5c — schema_overrides kwarg** ✅:
- New `schema_overrides: dict | None` kwarg on `Client.dataframe()`.
- Routed directly to FFI as a flat `column_sender_arrow_override` array;
  Rust patches Field metadata internally before `classify()` runs.
  No pyarrow rebuild on the Python side.
- Supported kinds: `'symbol'`, `'ipv4'`, `'char'`, `('geohash', bits)`.
  Rejected at validation time (in `_validate_schema_overrides`) before
  any borrow / encode work begins.
- `schema_overrides` does **not** require pyarrow. Polars-only users get
  IPV4 / CHAR / SYMBOL / GEOHASH wire types without `pip install pyarrow`.

**Step 5d — tests** ✅ (minimum viable):
- `test/test_client_capsule_path.py` covers:
  - pyarrow Table with designated TS column
  - pyarrow RecordBatch via `Table.from_batches`
  - `max_rows_per_batch` splits a 64-row frame into 4 batches
  - polars DataFrame round-trip
  - polars LazyFrame collected + round-trip
  - `schema_overrides={'addr': 'ipv4'}` injects metadata
  - schema_overrides validation: unknown kind / bad geohash bits → reject
  - Non-capsule non-pandas object → fall-through reject
  - `_bench_dataframe_flush_arrow_batch` regression (uint8/uint16/uint64/
    f16 acceptance, uint64 > i64::MAX rejection)
- `_bench_dataframe_flush_arrow_batch` Python entry point added,
  replacing the old `_bench_dataframe_append_arrow_buffer` (which
  depended on removed `line_sender_buffer_append_arrow*` FFI).

**Step 5d — docs** (partial):
- `examples/polars_basic.py` ✅ (basic ingest + schema_overrides demo)
- `examples/pyarrow.py` ✅ (Table ingest + Field metadata demo)
- `README.md` / `docs/conf.rst` — **deferred** (separate PR)
- Per-dtype matrix tests mirroring `test_client_dataframe_pandas.py` —
  **deferred** (current smoke coverage protects the dispatch; per-dtype
  validation is incremental).

**Step 5e — pyarrow truly optional** ✅:
- `_dataframe_may_import_deps()` now imports only `pandas` + `numpy`.
- `_dataframe_require_pyarrow()` lazily imports pyarrow when actually
  needed (ArrowDtype columns, pyarrow Table/RecordBatch sources,
  Categorical / `string` dtype columns going through Arrow chunks,
  `schema_overrides=`).
- Pure-numpy pandas frames (numeric / object / bool / datetime64 /
  decimal) no longer trigger a pyarrow import. Polars frames also do
  not need pyarrow (polars-arrow does its own C export).
- Removed `_dataframe_client_try_arrow_path` and its 13 debug-stats
  globals + `_debug_dataframe_arrow_stats` Python entry. Pandas now
  goes directly: capsule path (pandas 2.2+ via `__arrow_c_stream__`)
  → chunk-based per-column dispatch (A/B/C/D).

### Step 6 — Rust: egress readers (separate doc)

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
  existing per-type calls in raw memcpy terms — but the appender
  unifies all integer / float / time dtypes behind one entry point,
  removing per-dtype Cython branches and the planner-time rejection
  of narrower / less-common dtypes.
- **Narrower dtypes (`int8/16/32`, `uint*`, `float32`, `float16`,
  `datetime64[Y/M/D/h/m]`)**: the appender lets Rust widen / pack /
  calendar-convert in one pass into wire bytes; otherwise Cython
  has to do it with a Python-side alloc.
- **Bool**: NumPy is byte-per-row; the appender packs LSB-first
  bitmap on the fly. Otherwise Cython has to pack into a scratch
  buffer.
- **Wider QuestDB-specific wire kinds** (UUID via S16, LONG256 via
  S32, DECIMAL via decimal_s{8,16,32}, GEOHASH via geohash_i{N},
  IPV4 via u32_ipv4, CHAR via u16_char, DOUBLE_ARRAY via
  f64_ndarray): no Cython-side support exists; the appender is the
  only path.

**Strided and non-native-endian buffers are out of scope** — the
Python wrapper is required to consolidate upstream (`arr.copy()` /
`arr.byteswap().newbyteorder()`) before the FFI call. Keeping the
Rust side single-pass-contiguous keeps the hot path simple and the
flush-time read pattern predictable.

So Step 3 stays in the plan, reframed as "broaden the dtype coverage
and unify the Cython dispatch" rather than "eliminate an FFI-level
memcpy."

### Q2: NumPy dtype auto-dispatch vs. user hint

`column_sender_numpy_dtype` is **two classes** of enum values, and
the Python wrapper must treat them differently:

| Class | Members | Wrapper behaviour |
|---|---|---|
| A (auto) | i8/16/32/64, u8/16/32/64, f16/32/64, bool, datetime64[*], timedelta64[*], s16, s32 | Cython picks the enum value from `arr.dtype` directly (`.kind`, `.itemsize`, `.unit`). Idempotent — same numpy column always lands on the same wire kind. |
| B (synthetic) | u32_ipv4, u16_char, decimal_s{8,16,32}, geohash_i{8,16,32,64}, f64_ndarray | `arr.dtype` cannot disambiguate. The wrapper **must** require either a per-column method call (e.g. `client.column_geohash("loc", arr, bits=20)`) or a schema dict at flush time. **Silent fallback to a default (numeric) dispatch would create invisible data loss** — once the server creates the column as INT (instead of GEOHASH or IPV4), it cannot be retroactively reinterpreted. |

For Polars / Pandas DataFrames driven through `Client.dataframe()`:

- A-class columns flow through dtype auto-dispatch — no API change.
- B-class columns either:
  - **Source-from-Arrow**: carry the hint as Arrow `Field` metadata
    (`questdb.geohash_bits=20`, `questdb.column_type=ipv4`, etc.) and
    route via the Arrow appender. Arrow's `classify()` already reads
    these tags.
  - **Source-from-NumPy raw**: require an extra Python-API entry
    (per-column setter) — out of scope until a user asks for it.

The system tests under `system_test/` MUST cover both classes
end-to-end: A-class through `Client.dataframe(np_df)`, B-class via
the Arrow-metadata path. The Arrow path covers the majority of
real Pandas/Polars workloads where dtype information is rich; the
raw-NumPy B-class hint API is a smaller surface, lower priority.

### Q3: writer-mixing in one chunk

Confirm that `column_sender_chunk_append_arrow_column`,
`_append_numpy_column`, and the existing per-type appends can all be
called on the same chunk and produce a valid wire frame.

### Q4: `must_close` propagation between writers

When two writers share a conn within one `Client.dataframe()` call (one
Arrow column then one NumPy column), and the second one fails, the
first one's data may already be in the wire buffer. Confirm the
conn-level `must_close` makes the next `flush` / `sync` a no-op /
error rather than committing partial data.

### Q5: Auth / TLS handshake state

Where does it live? Likely on the conn (handshake is per-socket), but
worth confirming that re-handshakes aren't needed on every borrow.

### Q6: read + write coexistence on one conn

Can a single conn alternate read and write operations sequentially?
This affects whether ingress and egress can share a borrowed conn or
need separate borrows.

### Q7: pool concurrency

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
