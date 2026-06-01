# Client.dataframe findings

Scope: focused review of `questdb.ingress.Client.dataframe` after merging the
`c-questdb-client` submodule changes from PR #150. This deliberately ignores the
row-oriented `Buffer.dataframe`, `Sender.dataframe`, and transaction dataframe
paths except where they explain shared planner behavior.

## Current shape

`Client.dataframe` is the pooled QWP/WebSocket columnar ingestion path:

1. Build a shared dataframe plan with `_FIELD_TARGETS_QWP`.
2. Validate the plan as columnar-v1 compatible.
3. Prebuild object columns.
4. Split rows into chunks.
5. Populate a `column_sender_chunk`.
6. Flush each chunk with `column_sender_flush`.
7. Finish with `column_sender_sync`.

Main implementation references:

- `src/questdb/ingress.pyx:3683` - public `Client.dataframe`.
- `src/questdb/ingress.pyx:3740` - plan build using `_FIELD_TARGETS_QWP`.
- `src/questdb/ingress.pyx:3752` - columnar validation and prebuild.
- `src/questdb/ingress.pyx:3762` - row chunk loop.
- `src/questdb/ingress.pyx:3781` - per-chunk flush.
- `src/questdb/ingress.pyx:3790` - final sync.
- `src/questdb/ingress.pyx:2328` - v1 fixed-table / timestamp-column constraints.

The path does not currently call the new buffer-level Arrow APIs added in the
submodule:

- `c-questdb-client/include/questdb/ingress/line_sender.h:2038`
  `line_sender_buffer_append_arrow`.
- `c-questdb-client/include/questdb/ingress/line_sender.h:2061`
  `line_sender_buffer_append_arrow_at_column`.

Instead, Python still performs its own pandas/Arrow dtype resolution and emits
typed chunk columns through `column_sender_chunk_*` APIs. Strings and symbols use
the narrower generic `column_sender_chunk_append_arrow_column` helper.

## Findings

### 1. Python error enum is stale after the submodule merge

The C header now defines Arrow-specific error codes:

- `line_sender_error_arrow_unsupported_column_kind`.
- `line_sender_error_arrow_ingest`.

Reference:

- `c-questdb-client/include/questdb/ingress/line_sender.h:130`.

Python's `.pxd` enum still stops at `line_sender_error_server_rejection`.

Reference:

- `src/questdb/line_sender.pxd:37`.

Python's public enum then defines synthetic values as:

- `BadDataFrame = server_rejection + 1`.
- `Cancelled = server_rejection + 2`.

Reference:

- `src/questdb/ingress.pyx:149`.

Impact: after the submodule merge, those synthetic Python values collide with
real C error values. If Python binds or reaches the new Arrow ingestion APIs,
new C errors can be misclassified or fail conversion.

Recommended fix: update `line_sender.pxd`, `IngressErrorCode`, and
`c_err_code_to_py`, then move synthetic Python-only values after the real C enum
range.

### 2. LargeUtf8 still copies on the columnar path

The planner casts Arrow `large_string` to regular `string` for the legacy row
serializer, even though the columnar FFI helper supports Arrow C format `U`
natively.

References:

- `src/questdb/dataframe.pxi:1199` - `large_string` cast to `string`.
- `src/questdb/dataframe.pxi:1222` - categorical LargeUtf8 dictionary cast.
- `src/questdb/ingress.pyx:3213` - string columns use generic Arrow appender.
- `c-questdb-client/questdb-rs-ffi/src/column_sender.rs:1066` - generic appender
  supports `U`.

Impact: unnecessary copy for `Client.dataframe` string-heavy frames.

Recommended fix: split row-path and columnar-path normalization, or preserve the
original Arrow chunks for columnar emission while keeping the row serializer's
cast.

### 3. `Client.dataframe` duplicates type classification that PR #150 already moved into Rust

The merged Rust Arrow classifier supports a broader matrix than Python currently
accepts through `Client.dataframe`, including:

- `UInt8` and `UInt16` widened to signed integer types.
- `UInt64` reinterpretation as `I64`.
- `Float16` widened to `FLOAT`.
- timestamp seconds, millis, micros, nanos.
- dates, times, durations.
- `Utf8View`, binary variants, dictionary symbols, decimals, and float64 arrays.

Reference:

- `c-questdb-client/questdb-rs/src/ingress/arrow.rs:1827`.

Python has a narrower manual resolver and validation matrix:

- `src/questdb/dataframe.pxi:1261` - Arrow resolver.
- `src/questdb/ingress.pyx:2316` - columnar validation.
- `src/questdb/ingress.pyx:2947` - per-column emission dispatch.

Impact: every new Rust Arrow ingestion capability requires a separate Python
planner update or the two paths drift.

Recommended fix: expose and benchmark a Python path that converts the dataframe
to Arrow batches and delegates ingestion to `line_sender_buffer_append_arrow` /
`line_sender_buffer_append_arrow_at_column`. Keep the current manual path only
for cases where it is measurably faster or supports Python-object semantics that
Arrow cannot represent cleanly.

### 4. Test coverage catches shape support but not enough payload semantics

Existing tests cover planner accept/reject behavior and some live server cases.

References:

- `test/test_dataframe.py:500` - planner-level support tests.
- `test/test_client_dataframe_fuzz.py` - deterministic shape fuzzing.

The fuzz harness is useful for planner stability and frame-count behavior, but
it does not decode payloads or compare round-tripped values against egress.

Impact: timestamp unit changes and string normalization copies are easy to miss
until live ingestion tests.

Recommended fix: add a round-trip suite using real QuestDB plus QWP egress
`to_arrow()` / `to_pandas()` as the oracle. Start with the mismatched matrix:
LargeUtf8, dictionary LargeUtf8, and timestamp unit variants.

## Suggested priority

1. Fix the Python C error enum drift before exposing any new Arrow ingestion API.
2. Remove the `LargeUtf8` copy on `Client.dataframe`.
3. Prototype an Arrow-batch delegation path using the new buffer-level APIs.
4. Add real round-trip tests using egress as the semantic oracle.
