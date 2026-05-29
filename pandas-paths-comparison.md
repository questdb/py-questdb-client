# Pandas ingress vs. egress paths

Comparison of every Python-facing path that moves a pandas DataFrame
between the user and a QuestDB server, written to ground a design
discussion about what to align next. File:line citations point at the
load-bearing code; everything else is interpretation.

Scope: pandas / Arrow / NumPy data shapes only. The text-only
`Sender.row(...)` path and the `SenderTransaction` chaining mode are
out of scope.

## Entry points

Three ingress entry points, one egress entry point:

| API                                   | Path                    | Wire                  | Location              |
|---------------------------------------|-------------------------|-----------------------|-----------------------|
| `Sender.dataframe(df, ...)`           | row-major ILP           | text                  | `ingress.pyx:4697`    |
| `Buffer.dataframe(df, ...)`           | row-major ILP (raw)     | text                  | `ingress.pyx:1394`    |
| `Client.dataframe(df, ...)`           | column-major QWP/WS     | binary, Arrow-shaped  | `ingress.pyx:3581`    |
| `Client.query(sql).to_pandas()`       | column-major QWP/WS     | binary, Arrow         | `ingress.pyx:3700`    |

Consequences:

- `Sender` only writes. `Client` reads and writes (column-mode only).
- A user who wants both read and write must hold both objects; the two
  share a `questdb_db` pool under the hood but at different ends of the
  API.
- The two `dataframe(...)` methods have identical names, different
  signatures, and a very different accepted-type matrix. See "Type
  support" below.

## What is symmetric

Column-major QWP is genuinely symmetric at the Arrow C Data Interface
boundary:

- **Ingress (`Client.dataframe`)**: pandas `Series.chunks` →
  `_export_to_c()` → raw `ArrowArray*` + `ArrowSchema*` →
  `column_sender_chunk_append_*` FFI → Rust holds raw pointers → QWP
  binary frames. The `col_setup_t` struct (`ingress.pyx:434`) pins the
  chunks for the duration of the flush.
- **Egress (`Client.query`)**: Rust `Cursor` →
  `line_reader_cursor_next_arrow_batch` (`egress.rs`) → raw
  `ArrowArray*` + `ArrowSchema*` → `pa.RecordBatch._import_from_c()`
  (`egress.pxi:147-148`) → pyarrow takes ownership.

Both directions are zero-copy for numeric and Arrow-backed columns;
both honour Arrow validity bitmaps; both pool through `questdb_db`
(writer free list + reader free list, separate but sharing conf string
and reaper — `questdb-rs/src/ingress/column_sender/db.rs`).

## Where it is not symmetric

### 1. Type support — column-ingress is the bottleneck

Column-ingress is the conservative v1 subset; egress accepts whatever
the server emits:

- Ingress (column path) rejects with `UnsupportedDataFrameShapeError`
  (`ingress.pyx:206-216`) anything outside int64 / float64 / bool /
  utf8 / timestamp{ns,us} plus a narrow set of dictionary and
  large-string variants. See the `col_source_t` enum at
  `dataframe.pxi:115-159` and the `_TARGET_TO_SOURCES` dispatch at
  `dataframe.pxi:185-266`.
- Ingress (row / ILP path) supports more: decimals
  (`decimal32/64/128/256_arrow`), float64 arrays
  (`arr_f64_numpyobj`), int<64 widening, categorical → SYMBOL. But it
  goes out as ILP text, not Arrow.
- Egress maps Arrow → pandas via pyarrow's `to_pandas` plus an
  optional `numpy_nullable` mapper for the primitives
  (`egress.pxi:280-309`). Anything pyarrow can decode, egress will
  surface; UUID, IPV4, GEOHASH, LONG256 come back as pyarrow's
  natural representation.

**Practical consequence**: a DataFrame coming out of
`Client.query(...).to_pandas()` may carry dtypes (UUID extension type,
decimal arrays, IPV4) that cannot be round-tripped back in via
`Client.dataframe(...)` — only via `Sender.dataframe(...)` over ILP.
This is the most user-visible asymmetry, and it is the blocker for
turning egress into a fuzz oracle for ingress.

### 2. Streaming

- Egress is lazily streamed: `iter_arrow()` is a generator pulling one
  batch at a time (`egress.pxi:120-159`); `__arrow_c_stream__` exposes
  the underlying RecordBatchReader directly.
- Ingress materialises the full plan upfront (`_dataframe_plan_build`,
  `dataframe.pxi`) then chunks rows per batch (typically 100k). There
  is no `iter_dataframe(generator)` that takes a stream of frames —
  the user has to chunk on their side and call `.dataframe()` per
  batch.

### 3. Null model

Symmetric where Arrow is used (validity bitmaps round-trip), asymmetric
on the row-ILP path:

- Row ILP also recognises `None`, `pd.NA`, `np.nan`, NumPy `NaT` as
  `INT64_MIN` via `_dataframe_is_null_pyobj` and the timestamp
  sentinel `_NAT` (`dataframe.pxi:60, 2658`). These are Python-level
  concepts; egress never sees them — egress only emits Arrow nulls.
- The QuestDB-specific sentinel discussion (`INT64_MIN` for `LONG` is
  indistinguishable from a real value) is documented for egress in
  `Client.query`'s docstring (`ingress.pyx:3720-3726`) but there is no
  matching prose on the ingress side.

### 4. Lifetime and ownership

- Ingress: `Sender` owns one `Buffer`; `Client.dataframe` borrows a
  connection from `questdb_db` per chunk batch and force-drops it on
  exception (`ingress.pyx:3673-3682`) so a dirty sender cannot poison
  the pool (the round-3 must-close fix).
- Egress: `Client.query` borrows a reader from `questdb_db`; the
  `QueryResult` / cursor are single-use; abandoned cursors latch
  `must_close=True` so the broken reader is dropped, not recycled
  (`egress.pxi:_ReaderHandle._close`).
- Both directions now use the same `questdb_db` pool, with the same
  `Arc<DbInner>` lifeline that lets cursors and buffers survive
  `Client.close()`. This part is genuinely symmetric.

### 5. Error type model

- Ingress raises `IngressError(IngressErrorCode, msg)` plus the
  `UnsupportedDataFrameShapeError` subclass carrying a
  `column_failures` tuple (`ingress.pyx:206-216`).
- Egress raises `IngressError` only — reader-side error codes are
  mapped to the ingress enum in `egress.pxi:9-30`, with a broad
  fall-through to `ServerFlushError` for codes that have no clear
  ingress analog.
- There is no `EgressError` or `BadQueryShapeError` symmetric to the
  column-failures container. If query results contain types the
  mapper cannot handle, the failure surfaces from pyarrow, not from
  our layer.

### 6. Docs

- Ingress: `src/questdb/dataframe.md` (718 lines) — type mapping
  table, null model, cursor design, the case for column-major vs
  row-major.
- Egress: nothing equivalent. Design lives in inline comments, header
  doc blocks, and chat transcripts. `plan-egress-to-pandas.md` exists
  at the repo root but is a working plan, not user-facing.

### 7. Test coverage

- Ingress column path has fuzz coverage
  (`test_client_dataframe_fuzz.py`) and a wide round-trip matrix; row
  path has the deep coverage (`test_dataframe.py`).
- Egress: `TestEgressWithDatabase` (round-trip + write rejection +
  dead-endpoint failover + schema evolution + multi-batch streaming +
  ~30 tests), `TestEgressPool` (the seven structural tests covering
  pool reuse, the `Arc<DbInner>` lifeline, must-close on abandoned
  cursors, `pool_max` exhaustion, conf-key acceptance, and
  Barrier-synced concurrency). No fuzz oracle yet — that was the
  stated side benefit of shipping egress in the first place but has
  not been wired up.

## Closing the type-support gap

Strategic frame: the goal is reliable ETL round-trip (read from
QuestDB, transform, write back). A fuzz oracle (`write X →
query X → assert equal`) is the prerequisite for "reliable" — and
the oracle is what forces the type-support work. Two related
policy decisions follow.

### Policy 1 — client-side dispatch is a pure function of Arrow input type

No content sniffing, no column-name conventions, no server-schema
lookup, no per-call type hints. `pa.string()` → VARCHAR.
`pa.fixed_size_binary(16)` → UUID. `pa.uint32()` → INT.
`pa.decimal128(p,s)` → DECIMAL. Closed-form, deterministic,
fuzz-friendly. The exhaustive consideration that landed this
decision: there are only four candidate sources of truth for "is
this `pa.string()` a UUID or a STRING?" — content sniffing
(Heisenbug-grade), column-name convention (no escape hatch),
server-schema lookup (circular for create-on-first-write), or an
explicit user hint. The first three are non-starters; the fourth
costs the user about as much code as just handing us the canonical
Arrow shape, but is more permanent in the user's code. Strict mirror
wins on every axis except one — and that one is covered by:

### Policy 2 — target-column coercion is server-side

When the client sends VARCHAR and the target column is UUID,
QuestDB's existing INSERT type-coercion narrows the value. The
Python client does not know or care. This already works for SQL
`INSERT INTO ... SELECT`; the column-sender INSERT path goes
through the same engine. We do not block it; we do not implement
it. Users who have UUIDs as strings just write them; the server
narrows. Users who want max throughput on large batches convert to
FSB(16) client-side and avoid the per-row server-side parse.

This factoring keeps the fuzz oracle simple. The oracle generates
inputs in canonical Arrow shapes (FSB(16) for UUID, decimal for
DECIMAL, etc.) and `to_arrow() → dataframe() → query() → to_arrow()`
is an identity function at the Arrow level. Server-side coercion is
exercised by *separate* tests (the SQL coercion test suite already
covers it), not by the oracle. Two distinct contracts, two distinct
test surfaces.

### What's left to build, by category

**A — Narrow primitives.** BYTE / SHORT / INT, narrow uints, float32,
CHAR (uint16), DATE (timestamp[ms]). Egress emits these natively.
The recent step-3 commit (`d420d79`) routed narrow NumPy dtypes
through `column_numpy`; the Arrow analogs (`pa.int8()`, `pa.int16()`,
`pa.int32()`, `pa.float32()`, `pa.timestamp('ms')`) still need
dispatch entries. Days of work; no new wire support.

**B — Types row-ILP already handles.** DECIMAL{32,64,128,256}
(`decimal32/64/128/256_arrow` exists in `col_source_t`), float64
ARRAY (`arr_f64_numpyobj`). Wire support is there in QuestDB.
Column-sender protocol needs new FFI shims
(`column_sender_chunk_append_decimal`, `_append_list`), but the
type-handling logic can crib from the row-ILP planner. Weeks per
family.

**C — QuestDB-extension types (split under the policy).**
- *Canonical-mirror dispatch*: FSB(16) → UUID column, uint32 → IPV4
  column, FSB(32) → LONG256 column, sized int → GEOHASH column. Plus
  the `arrow.uuid` extension type (storage = FSB(16)): strip the
  extension wrapper on the Cython side and dispatch on the storage
  type. Client-side work, mechanical once each wire-type code is
  confirmed.
- *String → extension column*: **no client-side work needed.**
  Server narrows. Document the perf trade-off so users with large
  batches know to convert client-side.

**D — Multi-dim arrays.** Nested ListArray dispatch. Genuinely new
machinery; defer until there is a real user.

### What to verify before locking

1. **STRING vs VARCHAR on the wire.** Confirm column-ingress already
   emits VARCHAR (not legacy STRING) for `pa.string()` /
   `pa.large_string()`. If still STRING, one-line wire-type change.
2. **Server coercion actually fires on column-sender INSERTs.** SQL
   coercion is known to work; the column-sender path *should* go
   through the same engine but is worth a system test — write a
   `pa.string()` column to a UUID target and read it back, asserting
   the UUID round-trip.
3. **`arrow.uuid` extension type on input.** Confirm that when a user
   hands us the extension type (the same shape egress emits) we can
   strip the wrapper on the Cython side and dispatch on the storage
   type. The egress test already round-trips this in the
   read-only direction (`system_test.py:1996-2002`); ingress needs
   the symmetric path.
4. **Negative path: bad string-to-UUID.** Write `'not-a-uuid'` to a
   UUID column. Server rejects. Confirm the rejection surfaces as
   `IngressError` (probably `ServerRejection`) and does not poison
   the pooled connection. This is the new failure mode for users who
   lean on server-side coercion.

### Suggested order of work

DECIMAL first (B-family beachhead with the strongest row-path
precedent), then category A in one PR (narrow Arrow dispatch +
ms-timestamp; the smallest unit of meaningful progress), then UUID
(C-family beachhead; sets the canonical-mirror + extension-type
pattern that IPV4 / LONG256 / GEOHASH will follow), then float64
ARRAY. After UUID the fuzz oracle can start expanding its
generated-type set incrementally — each new category-C type added
to column-ingress widens the oracle's coverage in the next CI run.

## Headline gaps worth addressing

In rough priority order — these are the asymmetries with concrete
follow-on work, not just architectural notes.

1. **Type-support gap on column-ingress.** See "Closing the
   type-support gap" above for the policy decision (strict Arrow
   mirror client-side, server-side coercion for everything else)
   and the categorised work. The fuzz oracle is the forcing function.

2. **No streaming ingress.** A user with a 10M-row DataFrame has to
   chunk by hand; egress streams natively. `Client.iter_dataframe`
   (taking a generator of frames) would close that gap and would
   naturally pair with `QueryResult.iter_pandas` for ETL-style "read
   N, transform, write N" loops.

3. **No egress design doc.** `dataframe.md` carries the ingress design
   conversation; nothing equivalent grounds egress decisions
   (sentinel handling, `dtype_backend` choice, single-use cursor
   contract, the pool architecture). Worth writing now while the
   model is fresh — fuzz oracle work will reference it.

4. **Asymmetric error model.** `UnsupportedDataFrameShapeError` is
   shape-validation rich; egress just raises `IngressError` from a
   mapping table. If we extend egress with column-by-column
   dtype-mapper failures (e.g. user passes a `types_mapper` that
   throws on one column), a symmetric `column_failures` payload would
   help.

5. **`Client.dataframe` vs `Sender.dataframe` API split.** Two
   methods with the same name, on different classes, with
   different type-support matrices. The split is justified
   architecturally (different wire, different pool), but a user
   discovering the library will pick one and be surprised when it
   rejects half their data. A common rejection-mode prose section in
   `dataframe.md` would help.

The fuzz oracle motivation cited in the original plan only closes
once gap 1 is real — column-ingress and egress need to handle the
same dtype set before "write X, read X back, assert equal" is a
meaningful check.
