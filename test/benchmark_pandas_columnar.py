#!/usr/bin/env python3

import argparse
import gc
import json
import os
import platform
import statistics
import subprocess
import sys
import time
import urllib.parse
import urllib.request

sys.dont_write_bytecode = True

import numpy as np
import pandas as pd

try:
    import pyarrow as pa
except ImportError:
    pa = None

import patch_path
import questdb.ingress as qi
from qwp_ws_ack_server import QwpAckServer


def git_rev(path):
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=path,
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except Exception:
        return None


def execute_sql(http_base, sql):
    if not http_base:
        raise ValueError("--real-http is required when SQL hooks are used")
    query = urllib.parse.urlencode({"query": sql})
    url = http_base.rstrip("/") + "/exec?" + query
    with urllib.request.urlopen(url, timeout=60) as response:
        body = response.read().decode("utf-8", errors="replace")
        return {
            "status": response.status,
            "body": body,
        }


def execute_sqls(http_base, sqls):
    return [execute_sql(http_base, sql) for sql in sqls]


def strip_conf_keys(conf, keys):
    if "::" not in conf:
        return conf
    prefix, rest = conf.split("::", 1)
    kept = []
    for item in rest.split(";"):
        if not item:
            continue
        key = item.split("=", 1)[0]
        if key not in keys:
            kept.append(item)
    return prefix + "::" + "".join(f"{item};" for item in kept)


def make_timestamp_series(rows):
    base = np.int64(1_704_067_200_000_000_000)
    values = base + np.arange(rows, dtype=np.int64)
    return pd.Series(values.view("datetime64[ns]"))


def make_numeric_core(rows):
    return pd.DataFrame({
        "ts": make_timestamp_series(rows),
        "seq": pd.Series(np.arange(rows, dtype=np.int64)),
        "price": pd.Series(np.arange(rows, dtype=np.float64) * 0.25),
        "qty": pd.Series((np.arange(rows, dtype=np.int64) % 1_000) + 1),
    })


def make_numeric_wide(rows):
    data = {"ts": make_timestamp_series(rows)}
    base_i = np.arange(rows, dtype=np.int64)
    base_f = np.arange(rows, dtype=np.float64)
    for index in range(8):
        data[f"i{index:02}"] = pd.Series(base_i + index)
    for index in range(8):
        data[f"f{index:02}"] = pd.Series(base_f * 0.25 + index)
    return pd.DataFrame(data)


def make_categorical_symbols(rows):
    symbols = np.array([f"sym_{index:04}" for index in range(1000)])
    venues = np.array([f"venue_{index:02}" for index in range(16)])
    indexes = np.arange(rows, dtype=np.int64)
    return pd.DataFrame({
        "ts": make_timestamp_series(rows),
        "symbol": pd.Categorical(symbols[indexes % len(symbols)]),
        "venue": pd.Categorical(venues[indexes % len(venues)]),
        "price": pd.Series(indexes.astype(np.float64) * 0.25),
        "qty": pd.Series((indexes % 1_000) + 1, dtype=np.int64),
    })


def make_arrow_strings(rows):
    if pa is None:
        raise RuntimeError("pyarrow is not installed")
    indexes = np.arange(rows, dtype=np.int64)
    messages = [f"message_{index % 1024:04}" for index in range(rows)]
    payloads = [
        f"payload_{index % 1024:04}_{index % 31:02}_{index % 127:03}"
        for index in range(rows)]
    return pd.DataFrame({
        "ts": make_timestamp_series(rows),
        "id": pd.Series(indexes, dtype=np.int64),
        "message": pd.Series(
            pa.array(messages, type=pa.string()),
            dtype=pd.ArrowDtype(pa.string())),
        "payload": pd.Series(
            pa.array(payloads, type=pa.string()),
            dtype=pd.ArrowDtype(pa.string())),
    })


def make_arrow_large_strings(rows):
    if pa is None:
        raise RuntimeError("pyarrow is not installed")
    values = [f"label_{index % 1024:04}" for index in range(rows)]
    return pd.DataFrame({
        "ts": make_timestamp_series(rows),
        "label": pd.Series(
            pa.array(values, type=pa.large_string()),
            dtype=pd.ArrowDtype(pa.large_string())),
        "seq": pd.Series(np.arange(rows, dtype=np.int64)),
        "price": pd.Series(np.arange(rows, dtype=np.float64) * 0.25),
    })


def make_mixed_physical(rows):
    if pa is None:
        raise RuntimeError("pyarrow is not installed")
    symbols = np.array([f"sym_{index:04}" for index in range(1000)])
    venues = np.array([f"venue_{index:02}" for index in range(16)])
    indexes = np.arange(rows, dtype=np.int64)
    notes = [f"note_{index % 1024:04}_{index % 31:02}" for index in range(rows)]
    return pd.DataFrame({
        "ts": make_timestamp_series(rows),
        "seq": pd.Series(indexes, dtype=np.int64),
        "price": pd.Series(indexes.astype(np.float64) * 0.25),
        "qty": pd.Series((indexes % 1_000) + 1, dtype=np.int64),
        "symbol": pd.Categorical(symbols[indexes % len(symbols)]),
        "venue": pd.Categorical(venues[indexes % len(venues)]),
        "note": pd.Series(
            pa.array(notes, type=pa.string()),
            dtype=pd.ArrowDtype(pa.string())),
    })


def make_nullable_extension(rows):
    return pd.DataFrame({
        "ts": make_timestamp_series(rows),
        "seq": pd.Series(np.arange(rows, dtype=np.int64), dtype="Int64"),
        "price": pd.Series(
            np.arange(rows, dtype=np.float64) * 0.25,
            dtype="Float64"),
        "active": pd.Series(
            np.arange(rows, dtype=np.int64) % 2 == 0,
            dtype="boolean"),
    })


def make_bool_unsigned_decision(rows):
    return pd.DataFrame({
        "ts": make_timestamp_series(rows),
        "active": pd.Series(np.arange(rows, dtype=np.int64) % 2 == 0),
        "u8": pd.Series(np.arange(rows, dtype=np.uint8)),
        "u16": pd.Series(np.arange(rows, dtype=np.uint16)),
        "u32": pd.Series(np.arange(rows, dtype=np.uint32)),
        "u64": pd.Series(np.arange(rows, dtype=np.uint64)),
    })


def make_unsupported_object(rows):
    return pd.DataFrame({
        "ts": make_timestamp_series(rows),
        "name": pd.Series(
            [f"name_{index % 1024}" for index in range(rows)],
            dtype=object),
        "qty": pd.Series(
            [int(index % 1000) for index in range(rows)],
            dtype=object),
        "price": pd.Series(
            [float(index) * 0.25 for index in range(rows)],
            dtype=object),
    })


SUPPORTED_SCHEMAS = {
    "arrow-large-strings": make_arrow_large_strings,
    "arrow-strings": make_arrow_strings,
    "categorical-symbols": make_categorical_symbols,
    "mixed-physical": make_mixed_physical,
    "numeric-core": make_numeric_core,
    "numeric-wide": make_numeric_wide,
}

REJECTION_SCHEMAS = {
    "bool-unsigned-decision": make_bool_unsigned_decision,
    "nullable-extension": make_nullable_extension,
    "unsupported-object": make_unsupported_object,
}

SCHEMAS = dict(SUPPORTED_SCHEMAS)
SCHEMAS.update(REJECTION_SCHEMAS)


SCHEMA_CREATE_SQL = {
    "arrow-large-strings": """
CREATE TABLE {table} (
  label VARCHAR,
  seq LONG,
  price DOUBLE,
  ts TIMESTAMP
) TIMESTAMP(ts) PARTITION BY DAY WAL
""",
    "arrow-strings": """
CREATE TABLE {table} (
  id LONG,
  message VARCHAR,
  payload VARCHAR,
  ts TIMESTAMP
) TIMESTAMP(ts) PARTITION BY DAY WAL
""",
    "bool-unsigned-decision": """
CREATE TABLE {table} (
  active BOOLEAN,
  u8 LONG,
  u16 LONG,
  u32 LONG,
  u64 LONG,
  ts TIMESTAMP
) TIMESTAMP(ts) PARTITION BY DAY WAL
""",
    "categorical-symbols": """
CREATE TABLE {table} (
  symbol SYMBOL,
  venue SYMBOL,
  price DOUBLE,
  qty LONG,
  ts TIMESTAMP
) TIMESTAMP(ts) PARTITION BY DAY WAL
""",
    "mixed-physical": """
CREATE TABLE {table} (
  seq LONG,
  price DOUBLE,
  qty LONG,
  symbol SYMBOL,
  venue SYMBOL,
  note VARCHAR,
  ts TIMESTAMP
) TIMESTAMP(ts) PARTITION BY DAY WAL
""",
    "nullable-extension": """
CREATE TABLE {table} (
  seq LONG,
  price DOUBLE,
  active BOOLEAN,
  ts TIMESTAMP
) TIMESTAMP(ts) PARTITION BY DAY WAL
""",
    "numeric-core": """
CREATE TABLE {table} (
  seq LONG,
  price DOUBLE,
  qty LONG,
  ts TIMESTAMP
) TIMESTAMP(ts) PARTITION BY DAY WAL
""",
    "numeric-wide": """
CREATE TABLE {table} (
  i00 LONG, i01 LONG, i02 LONG, i03 LONG,
  i04 LONG, i05 LONG, i06 LONG, i07 LONG,
  f00 DOUBLE, f01 DOUBLE, f02 DOUBLE, f03 DOUBLE,
  f04 DOUBLE, f05 DOUBLE, f06 DOUBLE, f07 DOUBLE,
  ts TIMESTAMP
) TIMESTAMP(ts) PARTITION BY DAY WAL
""",
    "unsupported-object": """
CREATE TABLE {table} (
  name VARCHAR,
  qty LONG,
  price DOUBLE,
  ts TIMESTAMP
) TIMESTAMP(ts) PARTITION BY DAY WAL
""",
}


def percentile(sorted_values, pct):
    if not sorted_values:
        return None
    index = int(round((len(sorted_values) - 1) * pct))
    return sorted_values[index]


def summarize(samples_ns):
    samples = [sample / 1_000_000_000 for sample in samples_ns]
    samples_sorted = sorted(samples)
    mean = statistics.fmean(samples)
    stdev = statistics.stdev(samples) if len(samples) > 1 else 0.0
    return {
        "iterations": len(samples),
        "median_s": statistics.median(samples),
        "mean_s": mean,
        "min_s": samples_sorted[0],
        "max_s": samples_sorted[-1],
        "p95_s": percentile(samples_sorted, 0.95),
        "stdev_s": stdev,
        "cov": stdev / mean if mean else 0.0,
    }


def timed_call(fn):
    gc.collect()
    was_enabled = gc.isenabled()
    gc.disable()
    try:
        cpu_start = time.process_time_ns()
        start = time.perf_counter_ns()
        result = fn()
        end = time.perf_counter_ns()
        cpu_end = time.process_time_ns()
    finally:
        if was_enabled:
            gc.enable()
    return end - start, cpu_end - cpu_start, result


def run_row_path(df, rows, iterations, warmups):
    buf = qi.Buffer.qwp()

    def once():
        buf.clear()
        buf.dataframe(df, table_name="bench_numeric", at="ts")
        return {"encoded_bytes": len(buf)}

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


def _make_ack_conf(server):
    return (
        f"qwpws::addr=127.0.0.1:{server.port};"
        "pool_size=1;"
        "pool_max=1;"
        "pool_reap=manual;")


def _finish_columnar_io_stats(timed_calls):
    stats = dict(qi._debug_dataframe_columnar_io_stats(enabled=False))
    if timed_calls:
        stats["flush_s_per_call"] = stats["flush_s"] / timed_calls
        stats["sync_s_per_call"] = stats["sync_s"] / timed_calls
        stats["flush_calls_per_call"] = (
            stats["flush_calls"] / timed_calls)
        stats["sync_calls_per_call"] = stats["sync_calls"] / timed_calls
    else:
        stats["flush_s_per_call"] = None
        stats["sync_s_per_call"] = None
        stats["flush_calls_per_call"] = None
        stats["sync_calls_per_call"] = None
    return stats


def run_client_ack(
        df,
        rows,
        iterations,
        warmups,
        *,
        min_calls=0,
        max_seconds=None,
        ack_delay_s=0.0):
    samples = []
    cpu_samples = []
    last = None
    with QwpAckServer(ack_delay_s=ack_delay_s) as server:
        conf = _make_ack_conf(server)
        with qi.Client.from_conf(conf) as client:
            qi._debug_dataframe_columnar_io_stats(enabled=False, reset=True)
            for _ in range(warmups):
                client.dataframe(df, table_name="bench_numeric", at="ts")

            qi._debug_dataframe_columnar_io_stats(enabled=True, reset=True)
            try:
                start = time.perf_counter()
                for _ in range(iterations):
                    elapsed, cpu_elapsed, _ = timed_call(
                        lambda: client.dataframe(
                            df,
                            table_name="bench_numeric",
                            at="ts"))
                    samples.append(elapsed)
                    cpu_samples.append(cpu_elapsed)
                total_s = time.perf_counter() - start
            finally:
                columnar_io_stats = _finish_columnar_io_stats(iterations)

        stats = server.snapshot()
        reconnects_after_first = max(0, stats["accepted_connections"] - 1)
        if reconnects_after_first:
            raise AssertionError(
                "pooled Client opened extra physical connections: "
                f"{stats['accepted_connections']} accepts")
        if stats["errors"]:
            raise AssertionError(
                "ACK server observed errors: " + "; ".join(stats["errors"]))
        if min_calls and iterations < min_calls:
            raise AssertionError(
                f"client-ack-reuse requires at least {min_calls} timed calls, "
                f"got {iterations}")
        if max_seconds is not None and iterations >= min_calls:
            if total_s > max_seconds:
                raise AssertionError(
                    f"{iterations} Client.dataframe calls took "
                    f"{total_s:.3f}s, over {max_seconds:.3f}s")
        last = {
            "ack_server": stats,
            "ack_delay_s": ack_delay_s,
            "columnar_io_stats": columnar_io_stats,
            "pool_conf": conf,
            "reconnects_after_first": reconnects_after_first,
            "timed_calls": iterations,
            "total_calls": iterations + warmups,
            "timed_total_s": total_s,
            "rows_ingested": rows * iterations,
        }
    return samples, cpu_samples, last


def run_columnar_populate(
        df, rows, iterations, warmups, max_rows_per_chunk=None):
    def once():
        kwargs = {}
        if max_rows_per_chunk is not None:
            kwargs["max_rows_per_chunk"] = max_rows_per_chunk
        return qi._bench_dataframe_plan_and_populate_column_chunks(
            df,
            table_name="bench_numeric",
            at="ts",
            **kwargs)

    for _ in range(warmups):
        result = once()
        if result["row_path_cell_emissions"] != 0:
            raise AssertionError(
                "columnar benchmark emitted row-path cells during warmup")

    samples = []
    cpu_samples = []
    last = None
    for _ in range(iterations):
        elapsed, cpu_elapsed, last = timed_call(once)
        if last["row_path_cell_emissions"] != 0:
            raise AssertionError(
                "columnar benchmark emitted row-path cells during timed run")
        if last["populated_rows_total"] != rows:
            raise AssertionError(
                f"expected {rows} populated rows, got "
                f"{last['populated_rows_total']}")
        samples.append(elapsed)
        cpu_samples.append(cpu_elapsed)
    return samples, cpu_samples, last


def run_arrow_materialize(df, rows, iterations, warmups):
    if pa is None:
        raise RuntimeError("pyarrow is not installed")

    def once():
        table = pa.Table.from_pandas(df, preserve_index=False)
        return {
            "arrow_rows": table.num_rows,
            "arrow_columns": table.num_columns,
            "arrow_bytes": table.nbytes,
        }

    for _ in range(warmups):
        once()

    samples = []
    cpu_samples = []
    last = None
    for _ in range(iterations):
        elapsed, cpu_elapsed, last = timed_call(once)
        if last["arrow_rows"] != rows:
            raise AssertionError(
                f"expected {rows} Arrow rows, got {last['arrow_rows']}")
        samples.append(elapsed)
        cpu_samples.append(cpu_elapsed)
    return samples, cpu_samples, last


def run_real_row_path(
        df,
        rows,
        iterations,
        warmups,
        *,
        conf,
        table_name,
        http_base=None,
        setup_sqls=(),
        reset_sqls=(),
        await_ack_ms=30000):
    row_conf = strip_conf_keys(conf, {"pool_size", "pool_max", "pool_reap"})
    setup_results = execute_sqls(http_base, setup_sqls)
    reset_count = 0
    samples = []
    cpu_samples = []
    last = None

    def reset():
        nonlocal reset_count
        execute_sqls(http_base, reset_sqls)
        reset_count += len(reset_sqls)

    with qi.Sender.from_conf(row_conf, auto_flush=False) as sender:
        def once():
            sender.dataframe(df, table_name=table_name, at="ts")
            fsn = sender.flush_and_get_fsn()
            acked = True
            if fsn is not None:
                acked = sender.await_acked_fsn(fsn, await_ack_ms)
                if not acked:
                    raise TimeoutError(
                        f"QWP/WebSocket ACK timeout waiting for FSN {fsn}")
            return {
                "acked": acked,
                "flushes": 1 if fsn is not None else 0,
                "fsn": fsn,
                "table_name": table_name,
            }

        for _ in range(warmups):
            reset()
            once()

        for _ in range(iterations):
            reset()
            elapsed, cpu_elapsed, last = timed_call(once)
            samples.append(elapsed)
            cpu_samples.append(cpu_elapsed)

    if last is None:
        last = {}
    last.update({
        "await_ack_ms": await_ack_ms,
        "conf": row_conf,
        "path": "real-row",
        "reset_sql_count": reset_count,
        "rows_ingested": rows * iterations,
        "setup_sql_count": len(setup_sqls),
        "setup_sql_results": setup_results,
        "total_calls": iterations + warmups,
    })
    return samples, cpu_samples, last


def run_real_client_path(
        df,
        rows,
        iterations,
        warmups,
        *,
        conf,
        table_name,
        http_base=None,
        setup_sqls=(),
        reset_sqls=()):
    setup_results = execute_sqls(http_base, setup_sqls)
    reset_count = 0
    samples = []
    cpu_samples = []
    last = None

    def reset():
        nonlocal reset_count
        execute_sqls(http_base, reset_sqls)
        reset_count += len(reset_sqls)

    with qi.Client.from_conf(conf) as client:
        def once():
            client.dataframe(df, table_name=table_name, at="ts")
            return {
                "table_name": table_name,
            }

        qi._debug_dataframe_columnar_io_stats(enabled=False, reset=True)
        for _ in range(warmups):
            reset()
            once()

        qi._debug_dataframe_columnar_io_stats(enabled=True, reset=True)
        try:
            for _ in range(iterations):
                reset()
                elapsed, cpu_elapsed, last = timed_call(once)
                samples.append(elapsed)
                cpu_samples.append(cpu_elapsed)
        finally:
            columnar_io_stats = _finish_columnar_io_stats(iterations)

    if last is None:
        last = {}
    last.update({
        "columnar_io_stats": columnar_io_stats,
        "conf": conf,
        "path": "real-client",
        "reset_sql_count": reset_count,
        "rows_ingested": rows * iterations,
        "setup_sql_count": len(setup_sqls),
        "setup_sql_results": setup_results,
        "total_calls": iterations + warmups,
    })
    return samples, cpu_samples, last


def _exception_report(exc):
    return {
        "type": type(exc).__name__,
        "message": str(exc),
        "column_failures": list(getattr(exc, "column_failures", ())),
    }


def _bench_table_name(schema_name):
    return f"bench_{schema_name.replace('-', '_')}"


def schema_sql_report(schema_name):
    table_name = _bench_table_name(schema_name)
    return {
        "schema": schema_name,
        "table_name": table_name,
        "drop_sql": f"DROP TABLE IF EXISTS {table_name}",
        "create_sql": (
            SCHEMA_CREATE_SQL[schema_name]
            .strip()
            .format(table=table_name)),
        "truncate_sql": f"TRUNCATE TABLE {table_name}",
    }


def columnar_support_report(schema_name, rows, max_rows_per_chunk=None):
    df = SCHEMAS[schema_name](rows)
    table_name = _bench_table_name(schema_name)
    plan = qi._debug_dataframe_columnar_plan(
        df,
        table_name=table_name,
        at="ts")
    report = {
        "schema": schema_name,
        "rows": rows,
        "columns": len(df.columns),
        "dtypes": {name: str(dtype) for name, dtype in df.dtypes.items()},
        "columnar_plan": plan,
    }
    if plan["supported"]:
        kwargs = {}
        if max_rows_per_chunk is not None:
            kwargs["max_rows_per_chunk"] = max_rows_per_chunk
        chunk_plan = qi._bench_dataframe_plan_and_populate_column_chunks(
            df,
            table_name=table_name,
            at="ts",
            **kwargs)
        report["chunk_plan"] = chunk_plan
        report["fast_path_assertion"] = {
            "row_path_cell_emissions": chunk_plan["row_path_cell_emissions"],
            "passed": chunk_plan["row_path_cell_emissions"] == 0,
        }
    else:
        with QwpAckServer() as server:
            try:
                with qi.Client.from_conf(_make_ack_conf(server)) as client:
                    client.dataframe(df, table_name=table_name, at="ts")
            except qi.UnsupportedDataFrameShapeError as exc:
                report["client_rejection"] = _exception_report(exc)
            stats = server.snapshot()
        report["rejection_publication_check"] = {
            "accepted_connections": stats["accepted_connections"],
            "binary_frames": stats["binary_frames"],
            "qwp1_frames": stats["qwp1_frames"],
            "binary_bytes": stats["binary_bytes"],
            "errors": stats["errors"],
            "passed": (
                stats["binary_frames"] == 0 and
                stats["qwp1_frames"] == 0 and
                not stats["errors"] and
                report.get("client_rejection", {}).get("type") ==
                "UnsupportedDataFrameShapeError"),
        }
    return report


PATHS = {
    "row": run_row_path,
    "client-ack": run_client_ack,
    "client-ack-reuse": run_client_ack,
    "columnar-populate": run_columnar_populate,
    "arrow-materialize": run_arrow_materialize,
    "real-client": run_real_client_path,
    "real-row": run_real_row_path,
}


def add_rates(summary, rows, columns):
    median = summary["median_s"]
    summary["rows_per_s_median"] = rows / median if median else None
    summary["cells_per_s_median"] = rows * columns / median if median else None


def add_cpu_summary(summary, cpu_samples, rows, columns):
    cpu_summary = summarize(cpu_samples)
    add_rates(cpu_summary, rows, columns)
    summary["process_cpu"] = cpu_summary


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Layer 1 pandas columnar benchmark: row-buffer serialization "
            "versus #148 chunk population, plus Arrow materialization."))
    parser.add_argument(
        "--schema",
        choices=sorted(SCHEMAS) + ["all"],
        default="numeric-core")
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--iterations", type=int, default=20)
    parser.add_argument("--warmups", type=int, default=3)
    parser.add_argument(
        "--max-rows-per-chunk",
        type=int,
        help="Override the internal columnar row chunk cap.")
    parser.add_argument(
        "--ack-delay-ms",
        type=float,
        default=0.0,
        help="Delay each local QWP/WebSocket ACK by this many milliseconds.")
    parser.add_argument(
        "--ack-reuse-min-calls",
        type=int,
        default=100,
        help="Minimum timed calls for the client-ack-reuse path.")
    parser.add_argument(
        "--ack-reuse-max-seconds",
        type=float,
        default=10.0,
        help="Maximum timed seconds for the client-ack-reuse path.")
    parser.add_argument(
        "--real-conf",
        help="QWP/WebSocket configuration string for real-server runs.")
    parser.add_argument(
        "--real-http",
        help=(
            "QuestDB HTTP base URL for setup/reset SQL in real-server runs."))
    parser.add_argument(
        "--real-table",
        help="Target table name for real-server runs.")
    parser.add_argument(
        "--real-await-ack-ms",
        type=int,
        default=30000,
        help="ACK timeout for the real-row path.")
    parser.add_argument(
        "--real-setup-sql",
        action="append",
        default=[],
        help="SQL executed once before real-server warmups.")
    parser.add_argument(
        "--real-reset-sql",
        action="append",
        default=[],
        help="SQL executed before each real-server warmup/timed iteration.")
    parser.add_argument(
        "--path",
        choices=sorted(PATHS),
        action="append",
        help="Path to run. Defaults to all paths.")
    parser.add_argument(
        "--pretty",
        action="store_true",
        help="Pretty-print JSON output.")
    parser.add_argument(
        "--support-report",
        action="store_true",
        help=(
            "Report Client.dataframe v1 eligibility, chunk planning, and "
            "pre-publication rejection details instead of timing paths."))
    parser.add_argument(
        "--schema-sql",
        action="store_true",
        help=(
            "Print QuestDB DROP/CREATE/TRUNCATE SQL metadata for selected "
            "benchmark schemas and exit."))
    args = parser.parse_args()

    if args.schema_sql:
        schema_names = (
            sorted(SCHEMAS) if args.schema == "all" else [args.schema])
        output = {
            "schemas": [
                schema_sql_report(schema_name)
                for schema_name in schema_names
            ],
        }
        print(json.dumps(
            output,
            indent=2 if args.pretty else None,
            sort_keys=True))
        return

    if args.support_report:
        schema_names = (
            sorted(SCHEMAS) if args.schema == "all" else [args.schema])
        reports = [
            columnar_support_report(
                schema_name,
                args.rows,
                args.max_rows_per_chunk)
            for schema_name in schema_names
        ]
        output = {
            "rows": args.rows,
            "reports": reports,
        }
        print(json.dumps(
            output,
            indent=2 if args.pretty else None,
            sort_keys=True))
        return

    if args.schema == "all":
        parser.error("--schema all requires --support-report")

    paths = args.path or [
        "row",
        "columnar-populate",
        "arrow-materialize",
        "client-ack"]
    real_table = args.real_table or _bench_table_name(args.schema)
    if any(path.startswith("real-") for path in paths):
        if not args.real_conf:
            parser.error("real-server paths require --real-conf")
        if (args.real_setup_sql or args.real_reset_sql) and not args.real_http:
            parser.error("--real-http is required with real setup/reset SQL")
    df = SCHEMAS[args.schema](args.rows)

    results = {
        "schema": args.schema,
        "rows": args.rows,
        "columns": len(df.columns),
        "dtypes": {name: str(dtype) for name, dtype in df.dtypes.items()},
        "warmups": args.warmups,
        "machine": {
            "python": sys.version,
            "platform": platform.platform(),
            "processor": platform.processor(),
            "pandas": pd.__version__,
            "numpy": np.__version__,
            "pyarrow": pa.__version__ if pa is not None else None,
        },
        "commits": {
            "py_questdb_client": git_rev(os.getcwd()),
            "c_questdb_client": git_rev(
                os.path.join(os.getcwd(), "c-questdb-client")),
        },
        "paths": {},
    }

    for path in paths:
        if path == "columnar-populate":
            samples, cpu_samples, last = run_columnar_populate(
                df,
                args.rows,
                args.iterations,
                args.warmups,
                args.max_rows_per_chunk)
        elif path == "client-ack":
            samples, cpu_samples, last = run_client_ack(
                df,
                args.rows,
                args.iterations,
                args.warmups,
                ack_delay_s=args.ack_delay_ms / 1000.0)
        elif path == "client-ack-reuse":
            samples, cpu_samples, last = run_client_ack(
                df,
                args.rows,
                max(args.iterations, args.ack_reuse_min_calls),
                args.warmups,
                min_calls=args.ack_reuse_min_calls,
                max_seconds=args.ack_reuse_max_seconds,
                ack_delay_s=args.ack_delay_ms / 1000.0)
        elif path == "real-row":
            samples, cpu_samples, last = run_real_row_path(
                df,
                args.rows,
                args.iterations,
                args.warmups,
                conf=args.real_conf,
                table_name=real_table,
                http_base=args.real_http,
                setup_sqls=args.real_setup_sql,
                reset_sqls=args.real_reset_sql,
                await_ack_ms=args.real_await_ack_ms)
        elif path == "real-client":
            samples, cpu_samples, last = run_real_client_path(
                df,
                args.rows,
                args.iterations,
                args.warmups,
                conf=args.real_conf,
                table_name=real_table,
                http_base=args.real_http,
                setup_sqls=args.real_setup_sql,
                reset_sqls=args.real_reset_sql)
        else:
            samples, cpu_samples, last = PATHS[path](
                df,
                args.rows,
                args.iterations,
                args.warmups)
        summary = summarize(samples)
        add_rates(summary, args.rows, len(df.columns))
        add_cpu_summary(summary, cpu_samples, args.rows, len(df.columns))
        summary["last"] = last
        results["paths"][path] = summary

    print(json.dumps(results, indent=2 if args.pretty else None, sort_keys=True))


if __name__ == "__main__":
    main()
