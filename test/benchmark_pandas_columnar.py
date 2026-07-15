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
import questdb._client as qi
from qwp_ws_ack_server import QwpAckServer


def _env_int(name, default):
    """Read an int knob from the environment, falling back to ``default``.

    Mirrors the Rust column-sender suite knob names (plan s3.3) so a single
    environment can drive both clients.
    """
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return int(raw)


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


# QuestDB's designated TIMESTAMP is microsecond resolution, so the generated
# datetime64[ns] values must be spaced at least 1 microsecond (1000 ns) apart
# to stay distinct once stored. Nanosecond-spaced timestamps collapse to ~1000
# distinct microseconds, which DEDUP UPSERT KEYS(ts) then folds to ~1000 rows
# (breaking the count() == rows invariant, plan s3.4).
_TS_STEP_NS = np.int64(1000)


def make_timestamp_series(rows):
    base = np.int64(1_704_067_200_000_000_000)
    values = base + np.arange(rows, dtype=np.int64) * _TS_STEP_NS
    return pd.Series(values.view("datetime64[ns]"))


# Defaults mirror the Rust column-sender suite (COLUMN_SENDER_PERF.md): the
# headline S1 schema uses a low-cardinality symbol (card 8) and a short
# (~16 byte) varchar so the numbers line up cross-client.
DEFAULT_SYM_CARD = 8
DEFAULT_VARCHAR_LEN = 16
# S2-wide high-cardinality SYMBOL columns (s1..s5): default matches the Go
# qwp-egress-read-wide anchor (100k distinct/col, uniform). Pass a length-5
# sequence instead for the plan's 10k-100k spread (dict-scale characterisation).
DEFAULT_HI_SYM_CARD = 100_000
S2_SPREAD_HI_SYM_CARD = (10_000, 25_000, 50_000, 75_000, 100_000)


def _build_note_series(rows, varchar_len, varchar_charset):
    """VARCHAR ``note`` column shared by S1/S2 (plan s3.1).

    Fixed-width ~``varchar_len`` notes from a low-cardinality rotating template
    (neither the numpy nor the Arrow egress path dedups a plain VARCHAR, so
    low-card text flatters neither). ``varchar_charset="unicode"`` shifts every
    codepoint into Latin Extended-A (U+0100+): same per-index distinctness and
    codepoint count as ascii, but every codepoint is non-ASCII (2 UTF-8 bytes),
    so the numpy to_pandas loop (``PyUnicode_FromStringAndSize`` per row) cannot
    take CPython's ASCII fast path and must build wider (UCS-2) str objects.
    rows/s stays the apples-to-apples metric (unicode is ~2x the on-wire bytes
    for the same row/codepoint count).
    """
    if pa is None:
        raise RuntimeError("pyarrow is not installed")
    if varchar_len < 1:
        raise ValueError("--varchar-len must be at least 1")
    if varchar_charset not in ("ascii", "unicode"):
        raise ValueError("varchar_charset must be 'ascii' or 'unicode'")
    ascii_templates = [
        (f"note_{index:03}_" * varchar_len)[:varchar_len]
        for index in range(min(rows, 1024) or 1)]
    if varchar_charset == "unicode":
        note_templates = [
            "".join(chr(ord(ch) + 0x100) for ch in tmpl)
            for tmpl in ascii_templates]
    else:
        note_templates = ascii_templates
    notes = [note_templates[index % len(note_templates)]
             for index in range(rows)]
    return pd.Series(
        pa.array(notes, type=pa.string()), dtype=pd.ArrowDtype(pa.string()))


def make_s1_narrow(rows, *, sym_card=DEFAULT_SYM_CARD,
                   varchar_len=DEFAULT_VARCHAR_LEN,
                   varchar_charset="ascii"):
    """S1 headline schema (QWP_DATAFRAME_BENCH_PLAN.md s3.1).

    5 columns matching the Go/Rust ``qwp-egress-read`` narrow schema so the
    cross-client parity table lines up:

    * ``ts``    -> TIMESTAMP (designated), ``datetime64[ns]``, monotonic-unique
    * ``id``    -> LONG, ``int64``
    * ``price`` -> DOUBLE, ``float64``
    * ``sym``   -> SYMBOL, pandas ``Categorical`` (cardinality ``sym_card``)
    * ``note``  -> VARCHAR, Arrow-backed string of length ~``varchar_len``
                  (``varchar_charset="ascii"`` default; ``"unicode"`` for
                  non-ASCII content that defeats the numpy ASCII fast path)

    ``ts`` is monotonic and unique *at microsecond resolution* (the designated
    TIMESTAMP precision), so the DEDUP ``UPSERT KEYS(ts)`` table can assert
    ``count() == rows`` even though QWP/WS is at-least-once on reconnect
    (see plan s3.4 and ``make_timestamp_series``).
    """
    if pa is None:
        raise RuntimeError("pyarrow is not installed")
    if sym_card < 1:
        raise ValueError("--sym-card must be at least 1")
    indexes = np.arange(rows, dtype=np.int64)
    symbols = np.array([f"sym_{index:04}" for index in range(sym_card)])
    return pd.DataFrame({
        "ts": make_timestamp_series(rows),
        "id": pd.Series(indexes, dtype=np.int64),
        "price": pd.Series(indexes.astype(np.float64) * 0.25),
        "sym": pd.Categorical(symbols[indexes % len(symbols)]),
        "note": _build_note_series(rows, varchar_len, varchar_charset),
    })


def make_s2_wide(rows, *, sym_card=DEFAULT_SYM_CARD,
                 varchar_len=DEFAULT_VARCHAR_LEN, varchar_charset="ascii",
                 hi_sym_card=DEFAULT_HI_SYM_CARD):
    """S2 wide schema (QWP_DATAFRAME_BENCH_PLAN.md s8), matching the Go
    ``qwp-egress-read-wide`` anchor so the wide parity number lines up: it is
    S1-narrow plus 5 DOUBLE and 5 high-cardinality SYMBOL columns (15 total).

    * ``ts``/``id``/``price``/``sym``/``note`` -> identical to S1-narrow
      (``sym`` stays low-cardinality, ``card sym_card``)
    * ``d1``..``d5`` -> DOUBLE, ``float64`` (widen the fixed-width payload)
    * ``s1``..``s5`` -> SYMBOL, pandas ``Categorical``, high cardinality

    ``hi_sym_card`` sets the cardinality of ``s1``..``s5``: an int applies
    uniformly (default 100k, the anchor) or a length-5 sequence gives a spread
    (``S2_SPREAD_HI_SYM_CARD`` = 10k-100k). The 5 high-card SYMBOLs are the
    connection-scoped delta-dict stress (plan s3.5); the extra DOUBLEs plus the
    wider row are the "QWP wins on wide rows" axis.
    """
    if pa is None:
        raise RuntimeError("pyarrow is not installed")
    if sym_card < 1:
        raise ValueError("--sym-card must be at least 1")
    cards = ([int(hi_sym_card)] * 5 if isinstance(hi_sym_card, int)
             else [int(c) for c in hi_sym_card])
    if len(cards) != 5 or any(c < 1 for c in cards):
        raise ValueError(
            "hi_sym_card must be a positive int or 5 positive ints")
    indexes = np.arange(rows, dtype=np.int64)
    symbols = np.array([f"sym_{index:04}" for index in range(sym_card)])
    cols = {
        "ts": make_timestamp_series(rows),
        "id": pd.Series(indexes, dtype=np.int64),
        "price": pd.Series(indexes.astype(np.float64) * 0.25),
        "sym": pd.Categorical(symbols[indexes % len(symbols)]),
        "note": _build_note_series(rows, varchar_len, varchar_charset),
    }
    for d in range(1, 6):
        cols[f"d{d}"] = pd.Series(indexes.astype(np.float64) * (0.5 + d))
    # from_codes avoids materialising rows*5 symbol strings: codes = index mod
    # card, categories built once per column.
    for i, card in enumerate(cards):
        codes = (indexes % card).astype(np.int32)
        cats = pd.Index([f"s{i}_{v:06d}" for v in range(card)], dtype="object")
        cols[f"s{i + 1}"] = pd.Categorical.from_codes(codes, categories=cats)
    return pd.DataFrame(cols)


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
    "s1-narrow": make_s1_narrow,
    "s2-wide": make_s2_wide,
}

# Schemas whose generator accepts the --sym-card / --varchar-len knobs
# (s2-wide additionally accepts --hi-sym-card).
KNOB_SCHEMAS = frozenset({"s1-narrow", "s2-wide"})


def build_schema_df(schema_name, rows, *, sym_card=DEFAULT_SYM_CARD,
                    varchar_len=DEFAULT_VARCHAR_LEN,
                    varchar_charset="ascii",
                    hi_sym_card=DEFAULT_HI_SYM_CARD):
    """Build a benchmark DataFrame, threading knobs to schemas that accept them.

    Most generators take only ``rows``; the S1/S2 schemas additionally accept
    ``sym_card`` / ``varchar_len`` / ``varchar_charset`` (and ``hi_sym_card``
    for s2-wide's high-cardinality s1..s5). Keeping the registry uniform lets
    every call site build any schema without special-casing.
    """
    generator = SCHEMAS[schema_name]
    if schema_name in KNOB_SCHEMAS:
        kwargs = dict(sym_card=sym_card, varchar_len=varchar_len,
                      varchar_charset=varchar_charset)
        if schema_name == "s2-wide":
            kwargs["hi_sym_card"] = hi_sym_card
        return generator(rows, **kwargs)
    return generator(rows)

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
    # Headline S1 schema. DEDUP UPSERT KEYS(ts) + monotonic-unique ts keeps
    # count() == rows even though QWP/WS replays frames on reconnect
    # (at-least-once inflates 5-16%; see plan s3.4).
    "s1-narrow": """
CREATE TABLE {table} (
  id LONG,
  price DOUBLE,
  sym SYMBOL,
  note VARCHAR,
  ts TIMESTAMP
) TIMESTAMP(ts) PARTITION BY HOUR WAL DEDUP UPSERT KEYS(ts)
""",
    # S2 wide schema (plan s8), matching the Go qwp-egress-read-wide anchor:
    # S1-narrow + 5 DOUBLE + 5 high-cardinality SYMBOL (CAPACITY 200000 fits the
    # 100k distinct values/col with slack). DEDUP added (harness requirement,
    # plan s3.4) on top of the anchor's column layout.
    "s2-wide": """
CREATE TABLE {table} (
  id LONG,
  price DOUBLE,
  sym SYMBOL,
  note VARCHAR,
  d1 DOUBLE, d2 DOUBLE, d3 DOUBLE, d4 DOUBLE, d5 DOUBLE,
  s1 SYMBOL CAPACITY 200000, s2 SYMBOL CAPACITY 200000,
  s3 SYMBOL CAPACITY 200000, s4 SYMBOL CAPACITY 200000,
  s5 SYMBOL CAPACITY 200000,
  ts TIMESTAMP
) TIMESTAMP(ts) PARTITION BY HOUR WAL DEDUP UPSERT KEYS(ts)
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
    buf = qi.Buffer._new_qwp()

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
        f"ws::addr=127.0.0.1:{server.port};"
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
        with qi.QuestDB.from_conf(conf) as client:
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
                "pooled QuestDB opened extra physical connections: "
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
                    f"{iterations} QuestDB.dataframe calls took "
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


def run_cold_warm_split(df, rows, warm_iters, *, ack_delay_s=0.0):
    """Measure the cold first-flush vs warm steady-state on one connection.

    The cold/warm axis is the symbol delta-dict + commit mode (plan s3.5):
    the first frame on a fresh connection sends the full symbol dict from id 0
    with an immediate commit (warming the server cache); later frames on the
    same pooled connection send deltas with deferred commit. ``first_frame_sent``
    travels with the pool slot, so this runs with **zero warmups** to capture
    the genuine cold flush, then ``warm_iters`` warm flushes on the same slot.
    """
    cold_sample = None
    cold_cpu = None
    warm_samples = []
    warm_cpu = []
    with QwpAckServer(ack_delay_s=ack_delay_s) as server:
        conf = _make_ack_conf(server)
        with qi.QuestDB.from_conf(conf) as client:
            qi._debug_dataframe_columnar_io_stats(enabled=True, reset=True)

            def once():
                return client.dataframe(
                    df, table_name="bench_numeric", at="ts")

            # Cold: the very first flush on a fresh pooled connection.
            cold_sample, cold_cpu, _ = timed_call(once)
            # Warm: subsequent flushes reuse the same connection / symbol cache.
            for _ in range(warm_iters):
                elapsed, cpu_elapsed, _ = timed_call(once)
                warm_samples.append(elapsed)
                warm_cpu.append(cpu_elapsed)
            columnar_io_stats = _finish_columnar_io_stats(1 + warm_iters)

        stats = server.snapshot()
        reconnects_after_first = max(0, stats["accepted_connections"] - 1)
        if reconnects_after_first:
            raise AssertionError(
                "cold/warm split opened extra physical connections: "
                f"{stats['accepted_connections']} accepts (warm flushes must "
                "reuse the cold connection)")
        if stats["errors"]:
            raise AssertionError(
                "ACK server observed errors: " + "; ".join(stats["errors"]))
    last = {
        "ack_server": stats,
        "ack_delay_s": ack_delay_s,
        "columnar_io_stats": columnar_io_stats,
        "pool_conf": conf,
        "warm_iters": warm_iters,
        "rows_ingested": rows * (1 + warm_iters),
    }
    return cold_sample, cold_cpu, warm_samples, warm_cpu, last


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
        row_buf = sender.new_buffer()

        def once():
            row_buf.dataframe(df, table_name=table_name, at="ts")
            fsn = sender.flush_and_get_fsn(row_buf)
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

    with qi.QuestDB.from_conf(conf) as client:
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


def columnar_support_report(schema_name, rows, max_rows_per_chunk=None,
                            *, sym_card=DEFAULT_SYM_CARD,
                            varchar_len=DEFAULT_VARCHAR_LEN):
    df = build_schema_df(
        schema_name, rows, sym_card=sym_card, varchar_len=varchar_len)
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
                with qi.QuestDB.from_conf(_make_ack_conf(server)) as client:
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

# JSON contract v1 (plan s3.2): each path is tagged with a phase. "floor" is a
# no-network measurement (populate/encode/materialize); "e2e" includes the
# round trip to a (mock or real) server.
PATH_PHASE = {
    "row": "floor",
    "columnar-populate": "floor",
    "arrow-materialize": "floor",
    "client-ack": "e2e",
    "client-ack-reuse": "e2e",
    "real-client": "e2e",
    "real-row": "e2e",
}


_MIB = 1024.0 * 1024.0


def add_rates(summary, rows, columns, wire_bytes=None):
    median = summary["median_s"]
    summary["rows_per_s_median"] = rows / median if median else None
    summary["cells_per_s_median"] = rows * columns / median if median else None
    # mib_per_s is only meaningful when bytes actually crossed the wire; the
    # no-network floor paths leave wire_bytes None (plan s3.2).
    if wire_bytes and median:
        summary["mib_per_s"] = (wire_bytes / _MIB) / median
    else:
        summary["mib_per_s"] = None


def add_cpu_summary(summary, cpu_samples, rows, columns, wire_bytes=None):
    cpu_summary = summarize(cpu_samples)
    add_rates(cpu_summary, rows, columns, wire_bytes)
    summary["process_cpu"] = cpu_summary


def compute_wire_bytes(df):
    """Encode the DataFrame once to a QWP buffer to learn the per-flush wire
    size, used for the mib_per_s metric in the JSON contract (plan s3.2).

    This is the bytes pushed per ``QuestDB.dataframe`` flush for this schema; it
    is deterministic for a given DataFrame, so one encode suffices. Paths that
    talk to a server prefer the bytes the server actually observed (see
    ``measured_wire_bytes_per_call``); this is the fallback estimate.
    """
    buf = qi.Buffer._new_qwp()
    buf.dataframe(df, table_name="bench_wire_size", at="ts")
    return len(buf)


def measured_wire_bytes_per_call(last):
    """Per-flush wire bytes observed by the mock ACK server, if available.

    The ACK server counts every binary frame byte it received; dividing by the
    timed call count gives the real bytes-on-wire per flush, which is more
    honest than the one-shot buffer estimate (it includes WS framing and the
    warm symbol-dict). Returns ``None`` when no ACK snapshot is present.
    """
    if not isinstance(last, dict):
        return None
    ack = last.get("ack_server")
    timed = last.get("timed_calls")
    if not ack or not timed:
        return None
    total = ack.get("binary_bytes")
    if not total:
        return None
    return total / timed


def _machine_block():
    return {
        "python": sys.version,
        "platform": platform.platform(),
        "processor": platform.processor(),
        "pandas": pd.__version__,
        "numpy": np.__version__,
        "pyarrow": pa.__version__ if pa is not None else None,
    }


def _commits_block():
    return {
        "py_questdb_client": git_rev(os.getcwd()),
        "c_questdb_client": git_rev(
            os.path.join(os.getcwd(), "c-questdb-client")),
    }


def _path_summary(samples, cpu_samples, rows, columns, *, phase, warm,
                  wire_bytes, last=None):
    """Build a contract-conformant per-path summary block (plan s3.2)."""
    rate_wire_bytes = wire_bytes if phase == "e2e" else None
    summary = summarize(samples)
    add_rates(summary, rows, columns, rate_wire_bytes)
    add_cpu_summary(summary, cpu_samples, rows, columns, rate_wire_bytes)
    summary["phase"] = phase
    summary["warm"] = warm
    summary["wire_bytes"] = wire_bytes
    if last is not None:
        summary["last"] = last
    return summary


def pandas_to_questdb_throughput(
        *,
        rows,
        iterations,
        warmups,
        sym_card=DEFAULT_SYM_CARD,
        varchar_len=DEFAULT_VARCHAR_LEN,
        run_mode="full",
        real_conf=None,
        real_http=None,
        real_table=None,
        real_setup_sql=(),
        real_reset_sql=(),
        max_rows_per_chunk=None,
        schema="s1-narrow"):
    """WS-7 headline deliverable (plan s4): one call that yields S1 ingress
    rows/s + MiB/s for the no-network floor *and* the end-to-end path, plus the
    cold first-flush vs warm steady-state split and the honest
    populate_plus_encode sum (plan s3.6).

    * ``columnar-populate`` is the populate floor (descriptor building only).
    * the cold/warm split runs against the in-process mock ACK server so the
      encode + flush cost is measured without needing a server; the warm median
      is the honest ``populate_plus_encode`` headline.
    * when ``real_conf`` is given, ``real-client`` adds the true end-to-end
      number against a live QuestDB and the DEDUP ``count() == rows`` gate.

    Ack level is ``Ok`` (the mock server and the default ws conf). ``Durable``
    is Enterprise (``request_durable_ack=on``) and is deferred (plan s13).
    """
    df = build_schema_df(
        schema, rows, sym_card=sym_card, varchar_len=varchar_len)
    columns = len(df.columns)
    try:
        wire_bytes = compute_wire_bytes(df)
    except Exception:
        wire_bytes = None

    paths = {}

    # Floor: populate only (no encode, no wire).
    populate_samples, populate_cpu, populate_last = run_columnar_populate(
        df, rows, iterations, warmups, max_rows_per_chunk)
    paths["columnar-populate"] = _path_summary(
        populate_samples, populate_cpu, rows, columns,
        phase="floor", warm=warmups > 0, wire_bytes=wire_bytes,
        last=populate_last)

    # Cold/warm split over the in-process mock server (server-free e2e).
    cold_s, cold_cpu, warm_samples, warm_cpu, split_last = run_cold_warm_split(
        df, rows, max(iterations, 1))
    measured = measured_wire_bytes_per_call(split_last)
    e2e_wire_bytes = measured if measured is not None else wire_bytes
    cold_summary = _path_summary(
        [cold_s], [cold_cpu], rows, columns,
        phase="e2e", warm=False, wire_bytes=e2e_wire_bytes)
    warm_summary = _path_summary(
        warm_samples, warm_cpu, rows, columns,
        phase="e2e", warm=True, wire_bytes=e2e_wire_bytes, last=split_last)
    paths["mock-cold-first-flush"] = cold_summary
    paths["mock-warm-steady-state"] = warm_summary

    # Optional true end-to-end against a live QuestDB (DEDUP count()==rows gate
    # is enforced by the layer3 fixture; here we just record the rate).
    if real_conf:
        table_name = real_table or _bench_table_name(schema)
        e2e_samples, e2e_cpu, e2e_last = run_real_client_path(
            df, rows, iterations, warmups,
            conf=real_conf, table_name=table_name, http_base=real_http,
            setup_sqls=real_setup_sql, reset_sqls=real_reset_sql)
        real_measured = measured_wire_bytes_per_call(e2e_last)
        paths["real-client"] = _path_summary(
            e2e_samples, e2e_cpu, rows, columns,
            phase="e2e", warm=warmups > 0,
            wire_bytes=real_measured if real_measured is not None
            else wire_bytes,
            last=e2e_last)

    # Honest sum (plan s3.6): the warm e2e flush already includes populate +
    # encode + flush, so it *is* populate_plus_encode. We surface the populate
    # floor and the marginal encode+io cost alongside, never headlining the
    # near-free descriptor append on its own.
    populate_s = paths["columnar-populate"]["median_s"]
    warm_e2e_s = warm_summary["median_s"]
    encode_plus_io_s = max(warm_e2e_s - populate_s, 0.0)
    headline = {
        "populate_floor_s": populate_s,
        "encode_plus_io_s": encode_plus_io_s,
        "populate_plus_encode_s": warm_e2e_s,
        "populate_plus_encode_rows_per_s": (
            rows / warm_e2e_s if warm_e2e_s else None),
        "populate_plus_encode_mib_per_s": (
            (e2e_wire_bytes / _MIB) / warm_e2e_s
            if e2e_wire_bytes and warm_e2e_s else None),
        "cold_first_flush_s": cold_summary["median_s"],
        "warm_steady_state_s": warm_e2e_s,
        "cold_over_warm_ratio": (
            cold_summary["median_s"] / warm_e2e_s if warm_e2e_s else None),
        "warm_from_pool": True,
    }
    if real_conf:
        headline["real_client_s"] = paths["real-client"]["median_s"]
        headline["real_client_rows_per_s"] = (
            paths["real-client"]["rows_per_s_median"])
        headline["real_client_mib_per_s"] = (
            paths["real-client"]["mib_per_s"])

    return {
        "schema": schema,
        "rows": rows,
        "columns": columns,
        "dtypes": {name: str(dtype) for name, dtype in df.dtypes.items()},
        "direction": "ingress",
        "client": "py-pandas",
        "run_mode": run_mode,
        "warmups": warmups,
        "wire_bytes": wire_bytes,
        "ack_level": "Ok",
        "machine": _machine_block(),
        "commits": _commits_block(),
        "headline": headline,
        "paths": paths,
    }


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Layer 1 pandas columnar benchmark: row-buffer serialization "
            "versus #148 chunk population, plus Arrow materialization."))
    parser.add_argument(
        "--schema",
        choices=sorted(SCHEMAS) + ["all"],
        default="numeric-core")
    parser.add_argument(
        "--rows",
        type=int,
        default=_env_int("QUESTDB_COLUMN_BENCH_ROWS", 100_000),
        help="Rows per DataFrame (env: QUESTDB_COLUMN_BENCH_ROWS).")
    parser.add_argument("--iterations", type=int, default=20)
    parser.add_argument("--warmups", type=int, default=3)
    parser.add_argument(
        "--sym-card",
        type=int,
        default=_env_int("QUESTDB_COLUMN_BENCH_SYM_CARD", DEFAULT_SYM_CARD),
        help=(
            "SYMBOL cardinality for the s1-narrow schema "
            "(env: QUESTDB_COLUMN_BENCH_SYM_CARD)."))
    parser.add_argument(
        "--varchar-len",
        type=int,
        default=_env_int(
            "QUESTDB_COLUMN_BENCH_VARCHAR_LEN", DEFAULT_VARCHAR_LEN),
        help=(
            "VARCHAR byte length for the s1-narrow schema "
            "(env: QUESTDB_COLUMN_BENCH_VARCHAR_LEN)."))
    parser.add_argument(
        "--run-mode",
        choices=["quick", "full"],
        default="full",
        help=(
            "Recorded in the JSON contract (plan s3.2). 'quick' is the CI "
            "shape; 'full' is the headline shape."))
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
            "Report QuestDB.dataframe v1 eligibility, chunk planning, and "
            "pre-publication rejection details instead of timing paths."))
    parser.add_argument(
        "--schema-sql",
        action="store_true",
        help=(
            "Print QuestDB DROP/CREATE/TRUNCATE SQL metadata for selected "
            "benchmark schemas and exit."))
    parser.add_argument(
        "--headline",
        action="store_true",
        help=(
            "Run the pandas_to_questdb_throughput headline (plan s4): the "
            "columnar-populate floor + the cold/warm e2e split (mock server) + "
            "the populate_plus_encode sum, on the selected schema (default "
            "s1-narrow). Add --real-conf to include the live-server "
            "real-client number."))
    args = parser.parse_args()

    if args.headline:
        schema = "s1-narrow" if args.schema == "numeric-core" else args.schema
        if any(opt and not args.real_http
               for opt in (args.real_setup_sql, args.real_reset_sql)):
            parser.error("--real-http is required with real setup/reset SQL")
        result = pandas_to_questdb_throughput(
            rows=args.rows,
            iterations=args.iterations,
            warmups=args.warmups,
            sym_card=args.sym_card,
            varchar_len=args.varchar_len,
            run_mode=args.run_mode,
            real_conf=args.real_conf,
            real_http=args.real_http,
            real_table=args.real_table,
            real_setup_sql=args.real_setup_sql,
            real_reset_sql=args.real_reset_sql,
            max_rows_per_chunk=args.max_rows_per_chunk,
            schema=schema)
        print(json.dumps(
            result,
            indent=2 if args.pretty else None,
            sort_keys=True))
        return

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
                args.max_rows_per_chunk,
                sym_card=args.sym_card,
                varchar_len=args.varchar_len)
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
    df = build_schema_df(
        args.schema,
        args.rows,
        sym_card=args.sym_card,
        varchar_len=args.varchar_len)

    results = {
        "schema": args.schema,
        "rows": args.rows,
        "columns": len(df.columns),
        "dtypes": {name: str(dtype) for name, dtype in df.dtypes.items()},
        "direction": "ingress",
        "client": "py-pandas",
        "run_mode": args.run_mode,
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

    # Per-flush wire size for this schema (used by mib_per_s in the contract).
    # Only DataFrames the columnar/row path can encode have a meaningful size;
    # rejection schemas are skipped (they never reach the wire).
    try:
        wire_bytes = compute_wire_bytes(df)
    except Exception:
        wire_bytes = None
    results["wire_bytes"] = wire_bytes

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
        phase = PATH_PHASE.get(path, "e2e")
        # Prefer the bytes the mock server actually observed; fall back to the
        # one-shot encode estimate. mib_per_s is wire throughput, so it only
        # applies to e2e paths; floor paths record wire_bytes for reference but
        # report no rate.
        measured = measured_wire_bytes_per_call(last)
        path_wire_bytes_report = measured if measured is not None else wire_bytes
        rate_wire_bytes = path_wire_bytes_report if phase == "e2e" else None
        summary = summarize(samples)
        add_rates(summary, args.rows, len(df.columns), rate_wire_bytes)
        add_cpu_summary(
            summary, cpu_samples, args.rows, len(df.columns), rate_wire_bytes)
        summary["phase"] = phase
        summary["wire_bytes"] = path_wire_bytes_report
        # The timed samples are warm steady-state whenever warmups ran; the
        # cold first-flush is reported separately by the cold/warm split.
        summary["warm"] = args.warmups > 0
        summary["last"] = last
        results["paths"][path] = summary

    print(json.dumps(results, indent=2 if args.pretty else None, sort_keys=True))


if __name__ == "__main__":
    main()
