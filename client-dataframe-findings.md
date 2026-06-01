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

- `src/questdb/ingress.pyx:3832` - public `Client.dataframe`.
- `src/questdb/ingress.pyx:3889` - plan build using `_FIELD_TARGETS_QWP`.
- `src/questdb/ingress.pyx:3901` - columnar validation and prebuild.
- `src/questdb/ingress.pyx:3913` - row chunk loop.
- `src/questdb/ingress.pyx:3930` - per-chunk flush.
- `src/questdb/ingress.pyx:3939` - final sync.
- `src/questdb/ingress.pyx:2322` - v1 fixed-table / timestamp-column
  constraints.

The buffer-level Arrow APIs are now bound and have an internal benchmark hook:

- `src/questdb/line_sender.pxd:205` - `line_sender_buffer_append_arrow`.
- `src/questdb/line_sender.pxd:213` - `line_sender_buffer_append_arrow_at_column`.
- `src/questdb/ingress.pyx:3497` - `_dataframe_append_arrow_record_batch`.
- `src/questdb/ingress.pyx:3559` - `_bench_dataframe_append_arrow_buffer`.

Public `Client.dataframe` still does not route through those buffer-level Arrow
APIs. It still performs its own pandas/Arrow dtype resolution and emits typed
chunk columns through `column_sender_chunk_*` APIs. Strings and symbols use the
generic `column_sender_chunk_append_arrow_column` helper.

## Findings

### 1. Public `Client.dataframe` still duplicates Rust Arrow classification

Status: partially resolved.

Resolved parts:

- Python now binds and maps the Arrow-specific C error codes.
- Plain `LargeUtf8` and categorical `LargeUtf8` are preserved instead of cast.
- The buffer-level Arrow APIs are bound and exercised by an internal benchmark
  hook.
- Real QuestDB round-trip tests now cover `LargeUtf8`, categorical
  `LargeUtf8`, and timestamp unit semantics.

Remaining issue:

The public `Client.dataframe` path still uses the Python dataframe planner and
manual columnar validation. The Rust Arrow classifier supports a broader matrix
than public `Client.dataframe` currently accepts, including:

- Arrow `UInt8` and `UInt16` widened to signed integer types.
- Arrow `UInt64` reinterpretation as `I64`.
- Arrow `Float16` widened to `FLOAT`.
- Arrow timestamp seconds and milliseconds.
- Date, time, and duration values.
- `Utf8View` and binary variants.
- Raw Arrow dictionary symbols, distinct from pandas `CategoricalDtype`.
- Arrow `Float64` list arrays.

References:

- `c-questdb-client/questdb-rs/src/ingress/arrow.rs:1827` - Rust Arrow
  classifier.
- `src/questdb/dataframe.pxi:1232` - Python Arrow resolver.
- `src/questdb/ingress.pyx:2322` - Python columnar validation.
- `src/questdb/ingress.pyx:3016` - Python per-column emission dispatch.
- `src/questdb/ingress.pyx:3497` - internal Rust Arrow batch append helper.

Impact: every new Rust Arrow ingestion capability still needs a separate public
Python planner update, unless `Client.dataframe` starts delegating compatible
frames to the Rust Arrow batch path.

Recommended next step: decide the routing policy. Benchmark and semantics-check
the internal Arrow batch hook against the current manual chunk path, then either
route public Arrow-compatible frames through `line_sender_buffer_append_arrow` /
`line_sender_buffer_append_arrow_at_column`, or explicitly keep the manual path
as the public compatibility surface and narrow the supported-type claims around
that decision.

## Suggested priority

1. Benchmark the internal Rust Arrow batch path against the current manual chunk
   path on representative frames.
2. If the results are acceptable, route public Arrow-compatible frames through
   the Rust batch API and keep the manual path for Python-object semantics and
   other cases Rust Arrow cannot represent cleanly.
3. Add real-server round-trip tests for any newly public Rust-classified types.
