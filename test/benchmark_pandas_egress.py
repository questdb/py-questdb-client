#!/usr/bin/env python3
"""Step 2 pandas egress benchmark (QWP_DATAFRAME_BENCH_PLAN.md s5).

Mirror of the ingress harness (``benchmark_pandas_columnar.py``): reads the
s1-narrow table back from a real QuestDB over QWP/WebSocket and measures the
decode -> DataFrame paths, emitting the identical JSON metric contract with
``direction="egress"``.

Paths (plan s5.3):

* ``decode-only``    -- iterate the cursor's Arrow batches without building a
                        DataFrame (the egress floor; analog of
                        ``columnar-populate``).
* ``to-pandas``      -- default numpy materialise (the headline).
* ``to-polars``      -- Polars output (shares the Arrow path).
* ``arrow-c-stream`` -- ``__arrow_c_stream__`` -> ``polars.from_arrow`` (no
                        pyarrow on the consumer side).
* ``iter-pandas``    -- lazy per-batch materialise vs ``to-pandas`` full.

The headline run pairs ``decode-only`` (floor) + ``to-pandas`` (e2e) and reports
the honest ``decode_plus_assemble`` sum (plan s3.6).

Because egress decodes a *server* result, every path needs a populated table;
there is no server-free floor here (the server-free RESULT_BATCH replay server
is deferred to plan Step 5). Point this at a table the ingress side already
filled, or use ``run_pandas_egress_layer3.py`` which ingests then reads back in
one shot.
"""

import argparse
import gc
import json
import os
import platform
import sys
import time

sys.dont_write_bytecode = True

import numpy as np
import pandas as pd

try:
    import pyarrow as pa
except ImportError:
    pa = None

import patch_path
import questdb.ingress as qi

# Reuse the ingress spine: schema generator, the JSON-contract helpers, SQL
# helpers, timing, and the table-name convention all stay shared so the two
# directions emit the same shape and the parity aggregator sees one schema.
from benchmark_pandas_columnar import (
    DEFAULT_SYM_CARD,
    DEFAULT_VARCHAR_LEN,
    _bench_table_name,
    _commits_block,
    _env_int,
    _machine_block,
    _path_summary,
    build_schema_df,
    execute_sql,
    summarize,
    timed_call,
)


# Egress path phases (plan s3.2): decode-only is the no-assemble floor; the
# materialise paths are the end-to-end decode+assemble.
PATH_PHASE = {
    "decode-only": "floor",
    "to-pandas": "e2e",
    "to-polars": "e2e",
    "arrow-c-stream": "e2e",
    "iter-pandas": "e2e",
}

ALL_PATHS = list(PATH_PHASE)


def _require_pyarrow():
    if pa is None:
        raise RuntimeError("pyarrow is not installed")


def _drain_arrow(result):
    """Floor: pull every Arrow RecordBatch and touch it, but build no
    DataFrame. This is the decode cost with zero assembly."""
    rows = 0
    cols = 0
    for batch in result.iter_arrow():
        rows += batch.num_rows
        cols = batch.num_columns
    return {"rows": rows, "columns": cols}


def _to_pandas(result):
    df = result.to_pandas()
    return {"rows": len(df), "columns": len(df.columns)}


def _to_polars(result):
    df = result.to_polars()
    return {"rows": df.height, "columns": df.width}


def _arrow_c_stream(result, pl):
    # Consume the native __arrow_c_stream__ capsule with polars (no pyarrow on
    # the consumer side). polars.from_arrow accepts any object exposing the
    # Arrow C stream protocol.
    df = pl.from_arrow(result)
    return {"rows": df.height, "columns": df.width}


def _iter_pandas(result):
    rows = 0
    cols = 0
    for df in result.iter_pandas():
        rows += len(df)
        cols = len(df.columns)
    return {"rows": rows, "columns": cols}


def _make_runner(path, pl):
    if path == "decode-only":
        _require_pyarrow()
        return _drain_arrow
    if path == "to-pandas":
        _require_pyarrow()
        return _to_pandas
    if path == "to-polars":
        return _to_polars
    if path == "arrow-c-stream":
        if pl is None:
            raise RuntimeError("polars is required for arrow-c-stream")
        return lambda result: _arrow_c_stream(result, pl)
    if path == "iter-pandas":
        _require_pyarrow()
        return _iter_pandas
    raise ValueError(f"unknown egress path: {path}")


def run_egress_path(
        path,
        *,
        client,
        sql,
        rows,
        iterations,
        warmups,
        pl=None):
    """Time one egress path. Each iteration issues a fresh query (QueryResult
    is single-use) and materialises it via ``path``; the timed region covers
    the query round-trip + decode (+ assemble for the e2e paths)."""
    runner = _make_runner(path, pl)

    def once():
        with client.query(sql) as result:
            out = runner(result)
        if out["rows"] != rows:
            raise AssertionError(
                f"{path}: read back {out['rows']} rows, expected {rows}")
        return out

    for _ in range(warmups):
        once()

    samples = []
    cpu_samples = []
    last = None
    for _ in range(iterations):
        elapsed, cpu_elapsed, last = timed_call(once)
        samples.append(elapsed)
        cpu_samples.append(cpu_elapsed)
    return samples, cpu_samples, last


def measure_egress_wire_bytes(client, sql):
    """Per-query wire payload size for the mib_per_s metric (plan s3.2).

    Uses the materialised Arrow table's nbytes as the on-wire payload proxy:
    it is the decoded column-buffer size the server streamed, deterministic for
    a given table, and the natural egress analog of the ingress wire_bytes.
    """
    _require_pyarrow()
    with client.query(sql) as result:
        table = result.to_arrow()
        return int(table.nbytes)


def verify_zero_copy(client, sql):
    """Characterisation deliverable (plan s5.4): confirm the fixed-width fast
    path is zero-copy on the Arrow side.

    The numpy ``to_pandas`` path bulk-copies each fixed column out of the
    transient wire buffer (``_numpy_fixed_chunk`` -> ``np.frombuffer(...).copy()``)
    because the buffer is recycled. The genuine zero-copy surface is the Arrow
    batch (``iter_arrow`` / ``__arrow_c_stream__``): its column buffers are the
    decoded buffers exposed through the Arrow C Data Interface. We assert that a
    numpy view built from a fixed-width Arrow column buffer shares memory with a
    numpy array sliced from the same pyarrow column (no copy in between).
    """
    _require_pyarrow()
    report = {"checked_columns": [], "zero_copy": None}
    with client.query(sql) as result:
        reader = result.iter_arrow()
        try:
            batch = next(reader)
        except StopIteration:
            report["zero_copy"] = False
            report["note"] = "no batches returned"
            return report
        # Fixed-width numeric columns (id LONG, price DOUBLE) decode to
        # contiguous Arrow buffers we can view zero-copy.
        ok_any = False
        for name in ("id", "price"):
            if name not in batch.schema.names:
                continue
            col = batch.column(batch.schema.names.index(name))
            # pyarrow zero-copy to numpy for a no-null primitive column.
            arr = col.to_numpy(zero_copy_only=True)
            # The Arrow buffer underlying the column; build a second numpy view
            # straight off its address and assert it aliases the same memory.
            buffers = col.buffers()
            data_buf = buffers[-1]
            view = np.frombuffer(data_buf, dtype=arr.dtype, count=len(arr))
            shares = bool(np.shares_memory(arr, view))
            report["checked_columns"].append({
                "column": name,
                "zero_copy_to_numpy": True,
                "shares_memory_with_buffer": shares,
            })
            ok_any = ok_any or shares
        # Drain the rest so the cursor releases cleanly.
        for _ in reader:
            pass
        report["zero_copy"] = ok_any
    return report


def build_egress_report(
        *,
        client,
        table_name,
        rows,
        columns,
        iterations,
        warmups,
        run_mode,
        paths,
        wire_bytes,
        zero_copy=None,
        extra=None):
    sql = f"SELECT * FROM {table_name}"
    try:
        import polars as pl
    except ImportError:
        pl = None

    path_results = {}
    for path in paths:
        samples, cpu_samples, last = run_egress_path(
            path,
            client=client,
            sql=sql,
            rows=rows,
            iterations=iterations,
            warmups=warmups,
            pl=pl)
        phase = PATH_PHASE.get(path, "e2e")
        path_results[path] = _path_summary(
            samples, cpu_samples, rows, columns,
            phase=phase, warm=warmups > 0, wire_bytes=wire_bytes, last=last)

    # Honest sum (plan s3.6): decode_plus_assemble = the to-pandas e2e (it
    # already includes decode + assemble); decode-only is the floor; the
    # marginal assemble is the difference. Headline the sum, never the floor.
    headline = {}
    if "decode-only" in path_results and "to-pandas" in path_results:
        decode_s = path_results["decode-only"]["median_s"]
        assemble_e2e_s = path_results["to-pandas"]["median_s"]
        headline = {
            "decode_floor_s": decode_s,
            "assemble_plus_io_s": max(assemble_e2e_s - decode_s, 0.0),
            "decode_plus_assemble_s": assemble_e2e_s,
            "decode_plus_assemble_rows_per_s": (
                rows / assemble_e2e_s if assemble_e2e_s else None),
            "decode_plus_assemble_mib_per_s": (
                (wire_bytes / (1024.0 * 1024.0)) / assemble_e2e_s
                if wire_bytes and assemble_e2e_s else None),
        }
        for alt in ("to-polars", "arrow-c-stream"):
            if alt in path_results:
                headline[f"{alt}_rows_per_s"] = (
                    path_results[alt]["rows_per_s_median"])
                headline[f"{alt}_mib_per_s"] = path_results[alt]["mib_per_s"]

    report = {
        "schema": "s1-narrow",
        "rows": rows,
        "columns": columns,
        "direction": "egress",
        "client": "py-pandas",
        "run_mode": run_mode,
        "warmups": warmups,
        "wire_bytes": wire_bytes,
        "machine": _machine_block(),
        "commits": _commits_block(),
        "headline": headline,
        "paths": path_results,
    }
    if zero_copy is not None:
        report["zero_copy_check"] = zero_copy
    if extra:
        report.update(extra)
    return report


def fetch_row_count(http_base, table_name):
    result = execute_sql(http_base, f"SELECT count() FROM {table_name}")
    parsed = json.loads(result["body"])
    return parsed["dataset"][0][0]


def main():
    parser = argparse.ArgumentParser(
        description=(
            "pandas egress benchmark: read the s1-narrow table back from a "
            "real QuestDB and measure decode -> DataFrame paths."))
    parser.add_argument(
        "--rows",
        type=int,
        default=_env_int("QUESTDB_COLUMN_BENCH_ROWS", 100_000),
        help="Rows expected in the table (env: QUESTDB_COLUMN_BENCH_ROWS).")
    parser.add_argument("--iterations", type=int, default=10)
    parser.add_argument("--warmups", type=int, default=2)
    parser.add_argument(
        "--run-mode", choices=["quick", "full"], default="full")
    parser.add_argument(
        "--real-conf",
        required=True,
        help="QWP/WebSocket configuration string for the real server.")
    parser.add_argument(
        "--real-http",
        help="QuestDB HTTP base URL (for the count() sanity check).")
    parser.add_argument(
        "--real-table",
        help="Table to read back (defaults to the s1-narrow bench table).")
    parser.add_argument(
        "--path",
        choices=ALL_PATHS,
        action="append",
        help="Egress path(s) to run. Defaults to all paths.")
    parser.add_argument(
        "--zero-copy-check",
        action="store_true",
        help="Assert the fixed-width fast path is zero-copy on the Arrow side.")
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args()

    table_name = args.real_table or _bench_table_name("s1-narrow")
    paths = args.path or ALL_PATHS

    with qi.Client.from_conf(args.real_conf) as client:
        if args.real_http is not None:
            actual = fetch_row_count(args.real_http, table_name)
            if actual != args.rows:
                raise AssertionError(
                    f"table {table_name} has {actual} rows, expected "
                    f"{args.rows}; ingest the s1-narrow table first")
        sql = f"SELECT * FROM {table_name}"
        wire_bytes = measure_egress_wire_bytes(client, sql)
        zero_copy = verify_zero_copy(client, sql) if args.zero_copy_check \
            else None
        report = build_egress_report(
            client=client,
            table_name=table_name,
            rows=args.rows,
            columns=5,
            iterations=args.iterations,
            warmups=args.warmups,
            run_mode=args.run_mode,
            paths=paths,
            wire_bytes=wire_bytes,
            zero_copy=zero_copy)

    print(json.dumps(report, indent=2 if args.pretty else None, sort_keys=True))


if __name__ == "__main__":
    main()
