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

## Headline gaps worth addressing

In rough priority order — these are the asymmetries with concrete
follow-on work, not just architectural notes.

1. **Type-support gap on column-ingress.** Egress can emit decimal /
   array / IPV4 / UUID / GEOHASH / LONG256; column ingress cannot
   accept them. If we want round-trip parity (the prerequisite for a
   meaningful fuzz oracle) this is the work. Decimals are the
   closest — Arrow-decimal handling already exists on the row path
   (`decimal32/64/128/256_arrow` in `col_source_t`); column-ingress
   needs to import that dispatch.

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
