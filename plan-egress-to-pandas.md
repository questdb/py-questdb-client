# Egress → pandas — Architecture Plan

## TL;DR

Wire the existing c-questdb-client Rust egress (`sync-reader-ws` +
`arrow`) into the Python `Client` so that `client.query(sql).to_pandas()`
returns a `pd.DataFrame`. The Rust + FFI side **already exists** — PR
#150 (Victor, merged into our submodule branch as of parent commit
`392e05f`) ships per-batch Arrow C Data Interface export via
`line_reader_cursor_next_arrow_batch`. We bridge it to Python by
exposing `__arrow_c_stream__()` on a `QueryResult` class that loops the
per-batch API into an `ArrowArrayStream`. pandas 3.0 consumes that
stream natively via its PyCapsule protocol, so the entire type-
conversion layer lives in production Rust code — we add no per-column
Cython.

`Client` becomes bi-directional. Ingress (`Client.dataframe(...)`) is
unchanged; egress (`Client.query(...)`) is new. The same `Client`
manages both endpoints.

## Motivation

The primary goal is **shipping egress to Python users**. QuestDB users
who already ingest through this client want to read back through the
same client, with the same auth/TLS config, using idiomatic Python
data structures (pandas, pyarrow, polars). Today they have to drop to
HTTP `/exec` + JSON or open a separate Java/Rust path. Closing that
gap is the feature.

What makes the moment right:

1. **Production-grade Rust code already exists.**
   `questdb-rs/src/egress/` is ~14k LOC of production reader: WS
   transport, wire decoder, typed `ColumnView`, failover, TLS. The FFI
   mirror (`line_reader_*`) is ~3.9k LOC. The Python side is empty. We
   are wiring two well-tested layers together, not building a new
   protocol implementation.

2. **pandas 3.0 PyCapsule support collapses the integration cost.**
   `pd.DataFrame.from_arrow()` natively consumes any object that
   exposes `__arrow_c_stream__()`. We expose that one dunder method
   and pandas handles the rest. No per-column Python code, no
   `pyarrow.RecordBatchReader._import_from_c_stream` private-API risk.
   The same `__arrow_c_stream__` automatically gives pyarrow / polars
   / duckdb / anyone-else-in-the-PyCapsule-ecosystem a zero-extra-work
   integration with our query results. We do work once; multiple
   communities consume it.

3. **Round-trip identity is achievable.** Any `pd.DataFrame` that
   `Client.dataframe()` accepts on the way in comes back from
   `Client.query()` in the same shape, modulo QuestDB's well-known
   sentinel→null mapping. Users get an intuitive contract: write then
   read returns what you'd expect.

4. **Side benefit — fuzz content verification.** Today's
   `QwpAckServer`-based fuzz counts WS frames but does not decode
   payloads. Once egress lands, the Layer-3 fuzz becomes
   `df → Client.dataframe → QuestDB → Client.query →
   assert_frame_equal`, replacing several Tier-1 weaknesses (no
   content verification, no type-mapping check, no schema-drift check)
   with a single assertion. This is a nice side-effect, not the
   driver.

## Architecture

### Conceptual layout

```
QuestDB server
   │  (ws://host:9000/read/v1)
   ▼
questdb-rs    sync-reader-ws + arrow
   │  Reader → Cursor → BatchView → ColumnView
   │  egress/arrow/{schema,convert,reader}.rs (PR #150)
   ▼
questdb-rs-ffi   sync-reader-ws + arrow
   │  line_reader_cursor_next_arrow_batch → (ArrowArray, ArrowSchema)
   │  per-batch C Data Interface (PR #150)
   ▼
py-questdb-client Cython
   │  QueryResult.__arrow_c_stream__()  ← loops the per-batch API
   │  into a single ArrowArrayStream PyCapsule
   ▼
pandas 3.0 / pyarrow / polars / duckdb / anyone
   │  pd.DataFrame.from_arrow(QueryResult)
   ▼
user code
```

The integration point is the Arrow C Stream Interface, built **on the
Python side** by adapting Victor's per-batch C Data Interface export.
Everything below it is production Rust code we already have.
Everything above it is a consumer that doesn't need to know we exist.

### Unified `Client`

`Client` keeps its existing ingress methods (`dataframe`, `reap_idle`,
context-manager protocol). It gains:

```python
class Client:
    def query(self, sql: str) -> QueryResult: ...

class QueryResult:
    def __arrow_c_stream__(self, requested_schema=None) -> object: ...
    def to_pandas(
        self,
        *,
        dtype_backend: Literal["numpy_nullable", "pyarrow"] | None = None,
        types_mapper: Callable | None = None,
    ) -> pd.DataFrame: ...
    def to_arrow(self) -> pyarrow.Table: ...
    def iter_arrow(self) -> Iterator[pyarrow.RecordBatch]: ...
    def iter_pandas(self, **kwargs) -> Iterator[pd.DataFrame]: ...
    def cancel(self) -> None: ...
```

`to_pandas` and `to_arrow` are 3-line wrappers around the stream
protocol. `iter_*` exposes the underlying `pyarrow.RecordBatchReader`
for users who can't fit the whole result in memory. The
`dtype_backend` / `types_mapper` keywords on `to_pandas` follow the
pandas core convention (matching `pd.read_sql`, `pd.read_parquet`).

### Connection pooling

The existing `questdb_db` pool holds `qwpws_conn` (ingress
connections). Egress connects to a different URL path (`/read/v1` vs
`/write/v1`) and may live on a different host. Three options for the
first cut, in order of effort:

1. **Egress connections live separate from the pool.** Each
   `client.query(...)` opens, uses, and closes its own connection.
   Simple, correct, no shared state. Cost: handshake per query.
2. **Add a sibling pool for `line_reader` handles.** Same pool config,
   separate slots. Egress connections are pooled but isolated.
3. **One unified pool over both protocols.** Requires the c-questdb-
   client `questdb_db` pool to grow a second kind of connection. Bigger
   FFI surface.

**First cut: option (1).** It's the smallest delta and validates the
end-to-end story. Pool refactor is a follow-up once we know what the
query mix looks like.

### Mapper: QuestDB types → pandas dtypes

QuestDB's null model is **sentinel-based for primitives**, not
bitmap-based. This collapses the entire "Arrow-backed vs numpy-backed"
question for the primitives we serve, because we never need pandas
nullable extension types.

The mapping is determined by **what Victor's PR #150 emits at the Arrow
layer** plus `to_pandas()`'s default conversion behavior. Arrow types
listed below are exactly what `line_reader_cursor_next_arrow_batch`
hands us; pandas dtypes are what `pa.Table.to_pandas()` produces by
default. Sources: `questdb-rs/src/egress/arrow/schema.rs`,
`questdb-rs/src/egress/arrow/convert.rs`.

| QuestDB type | Arrow type (from PR #150) | Pandas dtype (default) | Null handling |
|---|---|---|---|
| BOOLEAN | `Boolean` (bit-packed) | `bool` (numpy) | n/a — QuestDB BOOLEAN is never null |
| BYTE | `Int8` | `int8` | n/a — never null |
| SHORT | `Int16` | `int16` | n/a — never null |
| INT | `Int32` | `int32` | sentinel `0x80000000` preserved |
| LONG | `Int64` | `int64` | sentinel `INT64_MIN` preserved |
| FLOAT | `Float32` | `float32` | NaN |
| DOUBLE | `Float64` | `float64` | NaN |
| DATE | `Timestamp(ms, "UTC")` | `datetime64[ms, UTC]` | `NaT` |
| TIMESTAMP | `Timestamp(µs, "UTC")` | `datetime64[us, UTC]` | `NaT` |
| TIMESTAMP_NS | `Timestamp(ns, "UTC")` | `datetime64[ns, UTC]` | `NaT` |
| CHAR | `UInt16` (raw codepoint) | `uint16` | sentinel `0x0000` |
| VARCHAR | `Utf8` | new `str` dtype | NaN per `str` dtype |
| STRING | `Utf8` | new `str` dtype | NaN per `str` dtype |
| SYMBOL | `Dictionary(UInt32, Utf8)` + metadata `questdb.symbol=true` | `pd.Categorical` | NaN code |
| UUID | `FixedSizeBinary(16)` + `ARROW:extension:name=arrow.uuid` | `pd.ArrowDtype(...)` | sentinel |
| LONG256 | `FixedSizeBinary(32)` | `pd.ArrowDtype(...)` | sentinel |
| IPV4 | `UInt32` | `uint32` | sentinel `0` |
| GEOHASH(n) | signed `Int8/16/32/64` per precision + metadata `questdb.geohash_bits` | `int8/16/32/64` | sentinel |
| BINARY | `Binary` | `pd.ArrowDtype(pa.binary())` | explicit null |
| ARRAY (Double) | nested `List<...>` + metadata `questdb.array_dim` | `pd.ArrowDtype(pa.list_(...))` | size=0 marker |
| ARRAY (Long) | nested `List<...>` + metadata `questdb.array_dim` | `pd.ArrowDtype(pa.list_(...))` | size=0 marker |
| DECIMAL64(s) | `Decimal64(18, s)` | `pd.ArrowDtype(pa.decimal64(18, s))` | sentinel |
| DECIMAL128(s) | `Decimal128(38, s)` | `pd.ArrowDtype(pa.decimal128(38, s))` | sentinel |
| DECIMAL256(s) | `Decimal256(76, s)` | `pd.ArrowDtype(pa.decimal256(76, s))` | sentinel |

Notes on Victor's Arrow type choices:

- **CHAR is `UInt16`, not a string.** Users get the raw codepoint
  column. If we want a `str`-typed Series in the default mapper we'd
  convert on the Python side via `types_mapper`. Cheap, but defer.
- **IPV4 is `UInt32`**, not int32. Pandas has no native IPv4 dtype; the
  user reads it as `uint32` and formats it themselves.
- **GEOHASH is signed `Int8/16/32/64`** sized by precision_bits.
  Precisions 60-63 overflow into negatives — user-visible.
- **Decimals are not widened.** Decimal64(18,s) stays Decimal64 (not
  Decimal128). Requires recent pyarrow that supports `pa.decimal64`.

Pandas 3.0's new default `str` dtype picks pyarrow or numpy-object
storage based on whether pyarrow is installed. Either is fine for us;
pandas decides.

QuestDB-specific Arrow field metadata Victor emits:
`questdb.column_type`, `questdb.designated_timestamp`,
`questdb.geohash_bits`, `questdb.symbol`, `questdb.array_dim`.
`pa.Table.to_pandas()` preserves these as `Field.metadata` but does
not surface them on the pandas frame; we expose them via the
`to_arrow()` path for users who need them.

`ArrowDtype` appears only for types pandas has no native equivalent
for. This matches what `pd.read_parquet()` does in 3.0 for the same
types — we adopt the same policy.

### Why numpy primitives, not nullable extension types or Arrow-backed

Three reasons converge on the same answer:

1. **Industry default in 2026 is numpy-backed pandas.** Every mature
   DB→pandas library defaults to numpy primitives even when the
   transport is 100% Arrow underneath: ADBC, DuckDB, Polars,
   ConnectorX, Snowflake, BigQuery, ClickHouse, PyMongoArrow, pyarrow
   itself, and pandas's own `read_sql` / `read_parquet`. Pandas 3.0
   made strings pyarrow-backed by default but explicitly kept
   numerics, booleans, and timestamps numpy-backed. We adopt the same
   policy — matching what users already see everywhere else.

2. **Arrow-backed null bitmaps don't actually buy fidelity through
   QuestDB.** The intuitive case for `pd.ArrowDtype` is that a
   validity bitmap distinguishes null from value `INT64_MIN`. That's
   true on the wire. It's *not* true after storage: QuestDB folds the
   bitmap into a sentinel value in the column file. So an Arrow-backed
   ingress that carefully preserves the bit gets silently flattened
   server-side. Egress reading back two `INT64_MIN` values cannot tell
   which was a null and which was a real value, regardless of what
   dtype it surfaces them in. Switching the mapper to Arrow-backed
   would mislead users into thinking the bit was preserved.

3. **Ecosystem compatibility costs are real.** sklearn, scipy, numba,
   matplotlib, statsmodels all assume numpy buffers; ArrowDtype inputs
   pay a copy + dtype conversion on every call. `Series.to_numpy()`
   becomes read-only or copy-allocating. `df.iloc[i] = x` triggers a
   full-column rebuild because Arrow buffers are immutable. None of
   these matter for analytical reads of static data, but they matter a
   lot for the mixed workloads users actually run.

Consequences for the default mapper:

- `Int64Dtype`, `Float64Dtype`, `BooleanDtype` never appear in default
  output.
- Round-trip identity holds: `int64` with `INT64_MIN` values goes in,
  comes back as `int64` with `INT64_MIN` values.
- Sentinel collisions are user-visible (see "Unavoidable lossy
  scenarios" below) — but this is QuestDB's contract, not ours.

### Unavoidable lossy scenarios

QuestDB's storage format folds nulls into sentinel values for most
primitives. These collisions are baked into the database, not into our
client, and **no choice of pandas dtype can recover the lost
distinction**. Document each of these in the egress user docs.

| QuestDB type | Sentinel | What's lost |
|---|---|---|
| INT | `0x80000000` (`INT32_MIN`) | A user value of `INT32_MIN` aliases null |
| LONG | `INT64_MIN` | A user value of `INT64_MIN` aliases null |
| FLOAT | NaN | An intentional NaN aliases null |
| DOUBLE | NaN | An intentional NaN aliases null |
| DATE | `INT64_MIN` ms | The instant `INT64_MIN` ms before epoch is unstorable |
| TIMESTAMP | `INT64_MIN` µs | Likewise for µs |
| TIMESTAMP_NS | `INT64_MIN` ns | Likewise for ns |
| CHAR | `0x0000` | The `'\0'` codepoint is unstorable as a value |
| UUID | `80000000-0000-0000-8000-000000000000` | That specific UUID aliases null |
| LONG256 | four `INT64_MIN` words | That specific 256-bit value aliases null |
| IPV4 | `0.0.0.0` | The address `0.0.0.0` aliases null |
| GEOHASH(n) | all-ones bit pattern | The all-ones geohash aliases null |
| SYMBOL | empty string (legacy paths) | Empty-string symbol aliases null on some server versions; recommend non-empty placeholders |

Types with explicit null markers (VARCHAR, STRING, BINARY, ARRAY,
DECIMAL) do **not** suffer from sentinel collisions — these round-trip
cleanly under any of our dtype options.

### Mapper customization

We adopt the **`dtype_backend` convention** already used by
`pd.read_sql`, `pd.read_parquet`, `pd.read_csv`, and
`google-cloud-bigquery`. Users who know one know all of them.

| Call | Behavior |
|---|---|
| `to_pandas()` (default) | numpy primitives + new `str` dtype + Categorical + ArrowDtype for unmappable types (the table above) |
| `to_pandas(dtype_backend="numpy_nullable")` | Pandas nullable extensions (`Int64Dtype`, `Float64Dtype`, `BooleanDtype`, etc.) for nullable types |
| `to_pandas(dtype_backend="pyarrow")` | Every column wrapped in `pd.ArrowDtype(...)` — full Arrow-backed pandas |
| `to_pandas(types_mapper=callable)` | User supplies a pyarrow `types_mapper`; full control |

The `dtype_backend` accepted values match the pandas core convention
exactly (`"numpy_nullable"`, `"pyarrow"`). `types_mapper` is pyarrow's
standard knob; passing both raises. None of these knobs can recover
data lost to QuestDB's sentinel folding — they only change how the
client surfaces it.

## FFI surface

### What PR #150 already ships (we consume, not add)

**Per-batch Arrow C Data Interface export**:

```c
typedef enum {
    line_reader_arrow_batch_ok = 0,
    line_reader_arrow_batch_end = 1,
    line_reader_arrow_batch_error = 2,
} line_reader_arrow_batch_result;

line_reader_arrow_batch_result line_reader_cursor_next_arrow_batch(
    line_reader_cursor* cursor,
    struct ArrowArray* out_array,
    struct ArrowSchema* out_schema,
    line_reader_error** err_out);
```

Gated behind the `arrow` Cargo feature on `questdb-rs-ffi`. Three
outcomes: `_ok` populates the caller-owned `ArrowArray` + `ArrowSchema`
(caller invokes the `release()` callback when done); `_end` signals
clean end-of-stream; `_error` writes a `line_reader_error*`. The
caller drives the loop.

The Rust side (`questdb-rs/src/egress/arrow/{schema,convert,reader}.rs`)
provides per-`ColumnView` Arrow emit, schema-drift detection, and an
internal `CursorRecordBatchReader` adapter implementing
`arrow_array::RecordBatchReader`. The arrow-rs crate handles the
`ArrowArray` / `ArrowSchema` release callbacks.

### New error codes from PR #150

- `line_reader_error_schema_drift` (22) — schema changed mid-stream;
  cursor remains usable, caller must re-snapshot.
- `line_reader_error_no_schema` (23) — cursor terminated before any
  batch produced; nothing to consume.
- `line_reader_error_arrow_export` (24) — arrow-rs rejected the
  produced `ArrayData`'s invariants. Client bug, not user-recoverable.

### Other existing FFI we use

- `line_reader_from_conf` / `line_reader_close` (connection lifecycle)
- `line_reader_prepare` / `line_reader_execute` (query setup)
- `line_reader_cursor_next_batch` / `_cancel` / `_free` (cursor
  lifecycle — usable underneath the per-batch Arrow loop)
- `line_reader_error_get_code` / `_msg` / `_free`
- 80+ accessor functions for power users (timing, server info,
  failover, binds) — not on the pandas path; expose later if needed.

### What we add to the FFI

**Nothing.** PR #150 covers the Rust + FFI side completely. Our work
sits entirely above the FFI line, in Cython + Python.

## Python / Cython changes

### Build

In `setup.py`, add `sync-reader-ws,arrow` to the cargo feature list:

```python
cargo_args + ['--features',
    'confstr-ffi,insecure-skip-verify,sync-reader-ws,arrow'],
```

Binary size grows by the WebSocket transport (tungstenite), zstd
decompression, and `arrow-rs` (`arrow_array`, `arrow_schema`,
`arrow_data`, `arrow::ffi`). Acceptable cost — egress + arrow together
are the second protocol we offer; this is the dependency cost. Worth
measuring the wheel-size delta before publishing.

### `.pxd` bindings

Add bindings to `line_sender.pxd` (which already houses both ingress
and egress-sender protocol decls — keep one file). Bindings needed:

- `line_reader` opaque struct + `from_conf` / `close`
- `line_reader_prepare` / `line_reader_execute`
- `line_reader_query` opaque + `_free` / `_execute`
- `line_reader_cursor` opaque + `_free` / `_cancel`
- `line_reader_cursor_next_arrow_batch` (PR #150)
- `line_reader_arrow_batch_result` enum (PR #150)
- `line_reader_error` + `_get_code` / `_msg` / `_free`
- `ArrowArray` / `ArrowSchema` mirror structs (already declared in our
  pxd for the ingress Arrow appender — reuse)

~13 functions, two opaque structs, two mirror structs (one already
present). Small surface.

### Python classes

```python
cdef class QueryResult:
    cdef line_reader_cursor* _cursor
    cdef bint _consumed

    def __arrow_c_stream__(self, requested_schema=None):
        # Build an ArrowArrayStream PyCapsule on the fly. The stream's
        # get_next callback calls line_reader_cursor_next_arrow_batch
        # to pull one (ArrowArray, ArrowSchema) at a time; the stream's
        # release callback frees the cursor.
        # Single-use: subsequent calls raise.
        ...

    def to_pandas(self, *, dtype_backend=None, types_mapper=None):
        # Materialize via pyarrow then convert; pandas 3.0's
        # DataFrame.from_arrow() works directly but goes through
        # pa.Table internally either way, and we need the
        # types_mapper hook.
        import pyarrow as pa
        return pa.table(self).to_pandas(
            types_mapper=types_mapper, dtype_backend=dtype_backend)

    def to_arrow(self):
        import pyarrow as pa
        return pa.table(self)  # pyarrow Table consumes the PyCapsule

    def iter_arrow(self):
        import pyarrow as pa
        reader = pa.RecordBatchReader.from_stream(self)
        yield from reader

    def iter_pandas(self, **to_pandas_kwargs):
        for rb in self.iter_arrow():
            yield rb.to_pandas(**to_pandas_kwargs)

    def cancel(self): ...
```

The `__arrow_c_stream__` method constructs a fresh `ArrowArrayStream`
whose `get_next` callback wraps `line_reader_cursor_next_arrow_batch`,
mapping the three-way result enum (`ok`/`end`/`error`) onto the
stream's success/null-terminator/error contract. The stream's
`release` callback frees the cursor. The returned PyCapsule uses the
spec-defined name `"arrow_array_stream"`. pandas / pyarrow / polars /
duckdb all unwrap this dunder protocol.

**Schema-drift handling.** If the cursor surfaces
`line_reader_error_schema_drift` mid-stream, the wrapper poisons the
stream and propagates a clear Python exception. Matches Victor's
`CursorRecordBatchReader::poisoned` semantics on the Rust side.

### `Client` integration

`Client.query(sql)` opens a `line_reader`, prepares + executes, wraps
the resulting cursor in `QueryResult`, returns it. The reader connection
is owned by the `QueryResult` and closed when it's released or
exhausted.

```python
class Client:
    # existing ingress methods unchanged
    def query(self, sql: str) -> QueryResult: ...
```

Egress endpoint URL derives from the same `addr=` in the client's
configuration string. The `path` differs (`/read/v1` vs `/write/v1`);
either we hard-code the path on the egress side or expose a
`reader_path` conf knob. First cut: hard-coded.

### Conf-string

Existing ingress uses `qwpws::addr=...;`. Egress in the Rust library
uses `ws::addr=...;`. For a unified Python `Client` we have two
options:

**Option A: one conf-string, derive both URLs.**
```python
Client.from_conf("qwpws::addr=host:9000;username=u;password=p")
```
The Python wrapper extracts `addr`, builds an ingress URL with
`/write/v1` and an egress URL with `/read/v1`, passes each to the
appropriate FFI constructor. Auth/TLS knobs apply to both.

**Option B: separate conf-strings.**
```python
Client.from_conf(write="qwpws::...", read="ws::...")
```
Explicit. Allows different endpoints. More verbose for the common case.

**First cut: Option A.** Common case is one server, one auth config,
two ports/paths. Users with mixed endpoints can construct manually
later.

## Implementation sequence

### Prerequisite — c-questdb-client merged ✅

Submodule pin already updated in parent commit `392e05f` (pins
`c-questdb-client@3aab56a`, which is `jh_conn_pool_refactor` merged
with `origin/arrow_polars`). Build verified across feature sets, all
existing test suites pass (608 mock-server + 321 dataframe + 12 fuzz +
87 live system_test). PR #150 is still **OPEN** upstream — if Victor
force-pushes or rebases, we'd need to re-sync the submodule branch.

### Step 1 — Build infrastructure

- Flip `setup.py` to enable `sync-reader-ws,arrow`.
- Verify build succeeds, `.so` grows, `line_reader_*` symbols including
  `line_reader_cursor_next_arrow_batch` exposed.
- Measure wheel-size delta.
- Smoke test: `nm` / `objdump` shows the new symbols.

Acceptance: `python -c "from questdb.ingress import Client; Client"`
still imports; the .so contains `line_reader_cursor_next_arrow_batch`.

### Step 2 — Cython bindings + `ArrowArrayStream` adapter

This step is the **only non-trivial Cython work** in the plan.

- Add `line_reader_*` declarations to `line_sender.pxd` (or split into
  a new `line_reader.pxd` if the file gets unwieldy).
- Add the `ArrowArray` / `ArrowSchema` mirror declarations if not
  already present from the ingress Arrow appender; add the
  `ArrowArrayStream` mirror.
- Implement a Cython helper that constructs an `ArrowArrayStream`
  whose:
  - `get_next(stream, out_array)` calls
    `line_reader_cursor_next_arrow_batch`, maps the three-way result
    onto the stream contract (`_ok` → fill `out_array`, return 0;
    `_end` → mark array released, return 0; `_error` → stash error,
    return non-zero).
  - `get_schema(stream, out_schema)` returns the schema captured from
    the first batch.
  - `get_last_error(stream)` returns the stashed error string.
  - `release(stream)` frees the cursor and the stashed state.
- Verify `cythonize` passes.

Acceptance: extension builds, the stream-construction helper works on
a hand-rolled fake `line_reader_cursor` (no live server needed).

### Step 3 — Python: `QueryResult` + `Client.query`

- `QueryResult` class with `__arrow_c_stream__`, `to_pandas`,
  `to_arrow`, `iter_arrow`, `iter_pandas`, `cancel`.
- `Client.query(sql)`.
- Conf-string derivation (one ingress string, two endpoint URLs).
- Single-reader-connection-per-query lifecycle (no pool yet).
- Schema-drift error handling (poison stream + raise).

Acceptance: end-to-end smoke test against a local QuestDB (the
existing `system_test.py` fixture):
```python
client = Client.from_conf("qwpws::addr=localhost:9000")
client.dataframe(df, table_name='t', at='ts')
# wait for WAL apply
pdf = client.query("SELECT * FROM t").to_pandas()
assert_frame_equal(df, pdf)
```

### Step 4 — Layer-3 fuzz oracle

Two routes, complementary:

**A.** Port `c-questdb-client/system_test/arrow_round_trip_fuzz.py`
(305 LOC, ships with PR #150) into our Python test harness, swapping
its `ctypes` FFI calls for our `Client` API. It already exercises
boolean / byte / short / int / long / float / double / varchar /
binary / uuid / long256 / symbol / timestamp / timestamp_ns under a
seed-controlled fuzz.

**B.** Extend `test/test_client_dataframe_fuzz.py` with a new test
class `TestClientDataframeRoundTrip` gated on `QDB_RUN_LAYER3=1` that
reuses our existing `_build_frame` generator and asserts
`assert_frame_equal(df_in, df_out, check_dtype=False, check_like=True)`.

Acceptance: Layer-3 fuzz passes with `QDB_RUN_LAYER3=1` against a real
QuestDB across 100 master-seed iterations.

### Step 5 — Coverage round-out + ergonomics

PR #150's Arrow emit covers every QuestDB type. Open items on our side:

- Decide whether to wrap CHAR (`UInt16`) and IPV4 (`UInt32`) into more
  ergonomic pandas dtypes via a built-in `types_mapper` (str for CHAR,
  ipaddress.IPv4Address for IPV4). Defer; opt-in via user mapper.
- Surface Arrow field-metadata (`questdb.geohash_bits`,
  `questdb.symbol`, `questdb.array_dim`) somewhere users can find it —
  most likely as a `result.schema` accessor on `QueryResult`.
- Test high-cardinality symbol streaming end-to-end (validates
  Victor's full-snapshot-per-batch dictionary semantics with
  `combine_chunks()`).

Acceptance: round-trip fuzz covers all types Victor emits; user docs
list which dtype each QuestDB column lands in.

### Step 6 — Streaming / iter_pandas hardening

Currently `iter_pandas` is "decode batch, convert, yield." For very
large results the materialization cost compounds. Profile against a
real workload (10M+ row table) and decide whether per-batch dict
reconciliation or batch concatenation needs optimization. Likely a
no-op until users surface a performance issue.

### Out of scope (deferred)

- Connection pooling for egress (separate follow-up).
- Bind parameters.
- Failover event surfacing.
- Server info / capability accessors.
- Async / non-blocking API.
- Cursor.cancel() under load — needs careful test design.
- Egress + ingress sharing the same TCP connection (out of scope until
  the protocol explicitly supports it).
- **Ingress whole-RecordBatch path** via PR #150's
  `line_sender_buffer_append_arrow` / `Buffer::append_arrow` (the (A)
  family — accept `pyarrow.Table` / `polars.DataFrame` directly). The
  Rust+FFI is now available; a separate plan-doc covers the Python
  side. Not blocking the pandas egress story.

## Risks

### `__arrow_c_stream__` consumer interop

The PyCapsule protocol is well-specified, but consumer behavior varies:

- pandas 3.0: `pd.DataFrame.from_arrow()` consumes it natively. ✅
- pyarrow ≥ 14: `pa.RecordBatchReader.from_stream()` consumes it
  natively. ✅
- polars ≥ 0.20: `pl.DataFrame()` constructor accepts it. ✅
- duckdb: `duckdb.from_arrow()` consumes it. ✅

If a downstream library's PyCapsule support has a bug, we fall back to
`pyarrow.RecordBatchReader._import_from_c_stream(capsule)` (the
private API that adbc and duckdb use today). Mitigation cost is one
helper function.

### Symbol dictionary growth across batches

Arrow C Data Interface does not natively model delta dictionaries.
Emitting the full cumulative dictionary per batch duplicates entries.
For very high-cardinality symbol columns over many batches this could
add up.

Mitigations:
- pyarrow `Table.combine_chunks()` dedupes when the user materializes
  the whole table.
- If profiling shows this hurts, emit "dictionary delta" + "dictionary
  full" alternating batches — but this requires consumer support.
- Worst case: emit symbols as plain Varchar (no dict). Lose the
  categorical, keep correctness.

Decision deferred until benchmarks show this is a real cost.

### Pandas 3.0 `from_arrow` API stability

`pd.DataFrame.from_arrow()` and `__arrow_c_stream__` landed in pandas
3.0 (current as of this writing). On pandas < 3.0 the user gets an
`AttributeError`. We document this as the minimum supported pandas
version for egress (the ingress side still supports older pandas).

### Sentinel collisions visible to user

QuestDB's storage format folds nulls into sentinel values for almost
every primitive type. The full list lives under "Unavoidable lossy
scenarios" in the mapper section above. The summary for the egress
docstring: a user value that happens to equal QuestDB's null sentinel
for that column type (e.g. `INT64_MIN` for LONG, NaN for DOUBLE, the
zero IPv4 address) round-trips through the database as a null.

This is QuestDB's contract, not ours. No dtype-backend choice can
recover the lost distinction — switching to `pd.ArrowDtype` would
preserve a validity bitmap on the wire but QuestDB flattens it
server-side anyway. Document loudly; ship as-is.

### Ingress already collapses TIMESTAMP at `INT64_MIN` to null — verify

A round-trip-correctness review of `src/questdb/dataframe.pxi` found
that the numpy `datetime64[ns]/[us]` ingress path treats a cell equal
to `INT64_MIN` (the `NaT` value) as null and **skips** that cell on
the wire. This is consistent with QuestDB's storage contract (same
sentinel server-side), but it means an explicit `pd.Timestamp` at the
`INT64_MIN` instant disappears on ingress, not just on egress.

Before Step 4, confirm this is intentional and document it. If
intentional: add to "Unavoidable lossy scenarios". If unintentional:
file a separate fix; egress shouldn't paper over an ingress bug.

### PR #150 still OPEN upstream

Our submodule pin sits on a merged copy of Victor's `arrow_polars`
branch (`c-questdb-client@3aab56a`), but the corresponding PR #150 has
not landed on `c-questdb-client` `main`. Two implications:

- If Victor rebases or force-pushes before merge, we re-sync our
  submodule branch — manageable, same workflow we used for our
  `jh_conn_pool_refactor` branch during the column-sender refactor.
- If the API surface changes during PR review (e.g. an FFI rename),
  our pxd + Cython code chases it. Keep an eye on the PR thread and
  mirror Step 2's reviewers as needed.

Best to land PR #150 upstream before we publish a wheel that depends
on it.

### Build size growth

Enabling `sync-reader-ws` adds the WebSocket transport (tungstenite),
zstd decompression, and ~70 exported symbols. The Python wheel grows
proportionally. Worth measuring before / after; if it's significant we
can publish a smaller `questdb` wheel without egress and a
`questdb[egress]` extra. Defer until we have numbers.

## Known limitations (document in user-facing API)

- **No row-count cap on `to_pandas()`.** The whole result materializes
  in memory. For large results use `iter_pandas()` (per-batch).
- **Sentinel-value collisions with QuestDB null markers.** See the
  full list in "Unavoidable lossy scenarios" — INT/LONG `_MIN`,
  FLOAT/DOUBLE NaN, the `INT64_MIN` instant for DATE/TIMESTAMP, CHAR
  `'\0'`, the sentinel UUID, IPv4 `0.0.0.0`, all-ones GEOHASH, all-
  `INT64_MIN` LONG256. Inherited from QuestDB; no client-side fix.
- **Egress requires pandas ≥ 3.0** for `to_pandas()` via PyCapsule.
  Older pandas can still use `to_arrow().to_pandas()` through pyarrow,
  at the cost of one extra materialization.
- **No reconnect / failover on a live cursor.** First cut surfaces
  failover errors to the user; recovery is the user's responsibility
  (re-issue the query). Failover-aware cursors come later.

## Open questions

### Q1 — One conf-string or two?

Lean: one (Option A above). Implementation: extract `addr` /
`username` / `password` / `tls_*` from the ingress conf-string, build
both URLs internally. Confirm before Step 4.

### ~~Q2 — BOOLEAN wire format~~ ✅ resolved by PR #150

Victor's `egress/arrow/schema.rs` emits `DataType::Boolean` directly;
we consume Arrow's standard bit-packed boolean from
`line_reader_cursor_next_arrow_batch`. No bit-packing concern at our
layer.

### ~~Q3 — Symbol dict reconciliation strategy~~ ✅ resolved by PR #150

Victor emits `Dictionary(UInt32, Utf8)` per batch with the full
cumulative `SymbolDict.entries()` snapshot. Open follow-up: validate
high-cardinality memory behavior with `combine_chunks()` (rolled into
Step 5).

### Q4 — Where does `cancel()` belong?

`QueryResult.cancel()` calls `line_reader_cursor_cancel()`. Open
question: do we expose it at all in the first cut, or wait until
fuzz / users need it? Lean: expose it (one method, easy), leave
testing to Step 5.

### Q5 — Multiple concurrent queries on one Client

The Rust `Reader` is single-cursor. Two `client.query(...)` calls in
sequence: the first must complete (or `cancel()` + drop) before the
second runs. Two queries from different threads sharing one `Client`:
needs a mutex around the reader, or one reader per thread.

First cut: serialize via mutex on the Python side. Doesn't preclude
adding a reader pool later.

### Q6 — `Client.query` GIL release

The egress decoder runs in Rust under `nogil` once the cursor is
spinning. The schema-fetch + first-batch fetch hold the GIL. Worth
profiling before committing to a specific pattern, but the shape
matches the ingress path (`Client.dataframe` releases GIL during
flush).
