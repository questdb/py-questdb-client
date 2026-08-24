# Native captures that outlive a call into Python

Every entry point that hands a native pointer or struct to a run which
then executes caller Python is listed here, with what keeps that pointer
valid.

The rule this table exists to enforce:

> **No guard may be the sole thing between caller Python and a crash, a
> hang, or silent data loss.**

Guards stay — a clean refusal is better UX than a mysterious failure —
but every row below has to be *ownership*: something that keeps the
capture valid no matter what the caller does. The test is blunt: delete
every check on the route and ask whether a freed pointer can still be
read. If it can, the row is not done.

Five review rounds produced four use-after-free or hang findings on these
routes, and each was a guard that did not name every door. The columns
that say `ownership` are the ones that stopped producing findings.

## Where the caller's Python runs

Three windows, and they are the reason this table exists:

- **The plan build** — `_dataframe_plan_build` and
  `_dataframe_client_try_capsule_path` read `df.attrs`, sniff object
  cells, call `__arrow_c_stream__` / `__arrow_c_array__`, and collect a
  polars `LazyFrame`. All caller code, all before a byte is written.
- **Cell serialization** — a column of Python objects runs a conversion
  per cell: `UUID.int`, `IPv4Address.__int__`, `datetime` arithmetic,
  `tzinfo.utcoffset`, `__index__`, `__buffer__`.
- **Arrow production** — a stream's `get_next` runs between batches, for
  the whole length of the send.

Anything reachable from those can call `close()`, `clear()`, `flush()`,
drop the last reference to a `Buffer`, or return a lease to its pool.

## The table

| # | Site | Capture | Kept valid by | Class |
|---|------|---------|---------------|-------|
| 1 | `Sender.dataframe`, row route (`_client.pyx`) | `af.sender_slot = &self._impl` | The receiver `self` outlives the call; the slot is read at each auto-flush, and a mid-run `close()` leaves it `NULL`, which the auto-flush already reads as "nothing to flush" | ownership |
| 2 | `Sender.dataframe`, row route | `af.last_flush_ms = self._last_flush_ms` | `calloc`ed in `__init__`, freed only in `__dealloc__`; `self` outlives the call | ownership |
| 3 | `_dataframe` (all three callers) | `owner` — the `Buffer` whose `ls_buf` is being written | Passed as a strong reference, so dropping every other reference mid-frame cannot free the buffer | ownership |
| 4 | `_dataframe` | `b` — the string arena the plan's names live in | Created and freed by the run itself. `qdb_pystr_buf_clear` drops every chunk past the first, so a shared arena was one re-entrant `clear()` from a freed read | ownership |
| 5 | `Sender.dataframe`, QWP/WebSocket route | `src.opts` | `line_sender_opts_clone` of the sender's options, freed by the run; a mid-plan `close()` frees the sender's copy, not this one | ownership |
| 6 | `Sender.dataframe`, QWP/WebSocket route | `ws_b` — its own arena | Created and freed by the run | ownership |
| 7 | `QuestDB.dataframe` / `PooledSender.dataframe` | `src.db` | `_begin_db_use` holds an active use for the whole call, so `questdb_db_close` waits; a same-thread `close()` is refused because it would wait on its own caller | ownership (+ hang guard) |
| 8 | `QuestDB.dataframe` | `b` — its own arena | Created and freed by the call | ownership |
| 9 | `_dataframe_client_try_capsule_path` | `conn` — `qwp_direct_sender*` | Opened after the plan build and closed by the same call. Pooled borrow or poolless connection; either way nothing else can hand it back | ownership |
| 10 | `_dataframe_client_try_capsule_path` | `c_overrides[i].column` — borrowed `PyBytes` storage | `merged_overrides` is a live local list holding every `bytes` object for the length of the call | ownership |
| 11 | `_dataframe_client_try_capsule_path` | `c_schema` — `ArrowSchema` | Stack-allocated by the call, released by the call | ownership |
| 12 | `PooledSender.row` | `buf = self._buffer` | Strong local reference taken before the row starts, so a mid-row `close()` that nulls the lease's field cannot free the buffer the row is writing into | ownership |
| 13 | `Buffer._column` | `c_name` — `line_sender_column_name` borrowed from the arena | Every branch whose value needs Python to convert takes the name as a `str` and encodes it *after* the conversion, so the borrow never spans caller code | ownership by construction |
| 14 | `Buffer._row` | the rewind marker | `_row_depth`, and the native buffer refuses a half-written row on its own | guard, no crash path — the worst case is a refused call |
| 15 | `QueryResult.to_pandas` / `iter_pandas` with a `types_mapper`, and `__arrow_c_stream__` (`egress.pxi`) | `reader` — a `pyarrow.RecordBatchReader` over the `_CursorHandle` the read is streaming from | `_take_cursor_handle` moves the handle into the reader, so the reader owns it. `_CursorHandle._free` nulls `_cursor` inside the re-entrant lock that every `_fetch_one_batch` re-reads it under, so a `close()` or `cancel()` from inside the mapper makes the next fetch refuse rather than read freed memory | ownership (+ clean refusal) |

## What each guard is still for

The guards on these routes are not redundant, they just have a smaller
job now: they turn "this would do something surprising" into a clean
error rather than standing between the caller and a segfault.

- `Buffer._check_not_in_row` — refuses a `clear()`, `flush()`, `close()`,
  `commit()` or `dataframe()` that arrives mid-row. Without it the call
  would succeed and reorder or discard the caller's rows. Data loss, not
  memory unsafety.
- `PooledSender._check_not_mid_call` — refuses returning a lease to the
  pool from inside one of its own calls. Without it the rest of the call
  works against a closed lease.
- `QuestDB.close`'s per-thread depth — refuses a close that would wait on
  its own caller. Without it the caller hangs forever.
- `Sender._check_not_in_own_callback` — refuses re-entering the sender
  from its own dispatcher callback. This one **is** memory safety and is
  not convertible: the native sender is borrowed by the dispatcher for
  the length of the callback, and there is no second copy to hand out.

## Keeping this honest

`test_the_native_capture_inventory_matches_the_sources` reads this file:
it fails if any row is classified as guard-only, and if a capture named
here has gone from the sources under that name. Add a row when you add a
capture — the re-entrancy grid is what will tell you if you did not.
