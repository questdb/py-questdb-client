# Client.dataframe findings

Scope: focused review of `questdb.ingress.Client.dataframe` after merging the
`c-questdb-client` submodule changes from PR #150. This deliberately ignores the
row-oriented `Buffer.dataframe`, `Sender.dataframe`, and transaction dataframe
paths except where they explain shared planner behavior.

## Current shape

`Client.dataframe` is the pooled QWP/WebSocket columnar ingestion path. It now
has two ingestion routes:

1. For fixed-table frames using a designated timestamp column name, first try
   the Rust Arrow batch route when the symbol policy can be represented in
   Arrow metadata:
   `pyarrow.RecordBatch.from_pandas` -> `line_sender_buffer_append_arrow*` ->
   `column_sender_flush_buffer` -> `column_sender_sync`.
2. If that route is not applicable or Rust rejects the frame before any flush,
   fall back to the older Python dataframe planner:
   `_FIELD_TARGETS_QWP` plan -> columnar-v1 validation -> prebuild object
   columns -> chunk rows -> populate `column_sender_chunk` -> flush with
   `column_sender_flush` -> finish with `column_sender_sync`.

Main implementation references:

- `src/questdb/ingress.pyx:3996` - public Arrow-route attempt.
- `src/questdb/ingress.pyx:4251` - public `Client.dataframe`.
- `src/questdb/ingress.pyx:4310` - Arrow route is tried before the manual
  planner.
- `src/questdb/ingress.pyx:4323` - fallback plan build using
  `_FIELD_TARGETS_QWP`.
- `src/questdb/ingress.pyx:2539` - v1 fixed-table / timestamp-column
  constraints.
- `src/questdb/ingress.pyx:3486` - buffer flush helper for Arrow route.
- `src/questdb/line_sender.pxd:997` - `column_sender_flush_buffer` binding.

The buffer-level Arrow APIs are bound, exercised by an internal benchmark hook,
and now used by the public compatible route:

- `src/questdb/line_sender.pxd:205` - `line_sender_buffer_append_arrow`.
- `src/questdb/line_sender.pxd:213` - `line_sender_buffer_append_arrow_at_column`.
- `src/questdb/ingress.pyx:3612` - `_dataframe_append_arrow_record_batch`.
- `src/questdb/ingress.pyx:3674` - `_bench_dataframe_append_arrow_buffer`.
- `c-questdb-client/include/questdb/ingress/column_sender.h:631` - pooled
  buffer flush FFI contract.
- `c-questdb-client/questdb-rs-ffi/src/column_sender.rs:1697` - FFI bridge.
- `c-questdb-client/questdb-rs/src/ingress/column_sender/sender.rs:141` -
  Rust pooled buffer flush implementation.

## Findings

### 1. Public `Client.dataframe` still has partial Rust Arrow duplication

Status: partially resolved.

Resolved parts:

- Python now binds and maps the Arrow-specific C error codes.
- Plain `LargeUtf8` and categorical `LargeUtf8` are preserved instead of cast.
- The buffer-level Arrow APIs are bound and exercised by an internal benchmark
  hook.
- The pooled Rust FFI path can now flush a `line_sender_buffer` through a
  borrowed QWP/WebSocket connection.
- Public `Client.dataframe` now tries the Rust Arrow batch route before the
  manual planner for fixed-table, timestamp-column-name frames using
  `symbols='auto'`, `symbols=True`, or explicit symbol lists that do not need
  categorical de-dictionarizing.
- Real QuestDB round-trip tests now cover `LargeUtf8`, categorical
  `LargeUtf8`, and timestamp unit semantics.
- Public route tests now cover Rust-only Arrow numeric/timestamp cases:
  `UInt8`, `UInt16`, `UInt64`, `Float16`, and `timestamp[ms, tz]`.
- Public route tests now cover explicit symbol-list routing for plain string
  columns; Python marks the selected Arrow fields with `questdb.symbol=true`
  and Rust builds the QWP SYMBOL dictionary.

Remaining issue:

The default compatible public path no longer relies on the Python dataframe
planner for Arrow classification, but the route is still intentionally narrow.
The manual planner still handles, and therefore still duplicates
classification for, non-default public shapes:

- `symbols=False`.
- explicit symbol lists where non-listed pandas categoricals would need to be
  converted back to VARCHAR rather than auto-emitted as SYMBOL.
- `table_name_col`.
- non-string `at` values.
- frames that cannot be converted to one Arrow `RecordBatch`.
- frames Rust rejects before any flush and that are still valid under the older
  Python compatibility surface.

The Rust Arrow classifier also supports more cases than the new public route has
real-server coverage for, including:

- Date, time, and duration values.
- `Utf8View` and binary variants.
- Raw Arrow dictionary symbols, distinct from pandas `CategoricalDtype`.
- Arrow `Float64` list arrays.

References:

- `c-questdb-client/questdb-rs/src/ingress/arrow.rs:1964` - Rust Arrow
  classifier.
- `src/questdb/dataframe.pxi:1232` - Python Arrow resolver used by fallback
  planner.
- `src/questdb/ingress.pyx:2539` - Python columnar validation used by fallback
  planner.
- `src/questdb/ingress.pyx:3326` - Python per-column emission dispatch used by
  fallback planner.
- `src/questdb/ingress.pyx:3996` - public Rust Arrow route.
- `test/test.py:272` - public QWP ack-server explicit-symbol route test.
- `test/system_test.py:3341` - real QuestDB explicit-symbol route test.
- `test/test_client_dataframe_fuzz.py:683` - local QWP dataframe fuzz for
  route/fallback contracts.

Impact: new Rust Arrow ingestion capabilities now become public for the narrow
compatible route without a Python per-column emitter update, but broader public
shapes still need either more routing coverage or separate fallback planner
updates.

Recommended next step: add real-server round-trip coverage for more
Rust-classified families, then decide whether `table_name_col`, `symbols=False`,
or categorical de-dictionarizing are worth moving into the Arrow route.

## Suggested priority

1. Add real-server round-trip tests for the remaining Rust-classified families
   before widening public claims for them.
2. Decide whether `table_name_col`, `symbols=False`, or categorical
   de-dictionarizing should move to the Rust Arrow route.
3. Keep benchmarking large representative frames when route coverage changes;
   the real-client benchmark now measures `client.dataframe()` without the old
   manual preflight contamination.
