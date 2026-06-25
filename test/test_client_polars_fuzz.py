#!/usr/bin/env python3
"""Fuzz tests for polars DataFrame ingestion via ``Client.dataframe()``.

Polars frames take the Arrow C Stream capsule path
(``__arrow_c_stream__``) — pyarrow-free. Every iteration builds a random
polars frame and ingests it to a local ``QwpAckServer``, asserting:

  * the client's accept / reject decision matches the static rule, and
  * an accepted non-empty frame publishes at least one binary frame while
    a rejection (or an empty frame) publishes none.

Unlike the pandas fuzz, every polars *field* dtype the capsule path sees
(ints, uints, floats, bool, utf8, categorical, enum, date, datetime) is
supported, so the only accept/reject axis is the designated-timestamp
column: it must contain no nulls and no pre-epoch values.

Reproduce one failing iteration with its seed:

    QDB_CLIENT_FUZZ_ITER_SEED=0x... \\
        python -m unittest test.test_client_polars_fuzz
"""
import sys

sys.dont_write_bytecode = True

import datetime
import decimal
import math
import os
import tempfile
import time
import unittest
import uuid

import patch_path
patch_path.patch()

import questdb._client as qi

PROJ_ROOT = patch_path.PROJ_ROOT
sys.path.append(str(PROJ_ROOT / 'c-questdb-client' / 'system_test'))
from qwp_ws_ack_server import QwpAckServer

# Shared seed/RNG helpers from the pandas fuzz; its pandas/pyarrow imports
# are lazy, so reusing them keeps this module pyarrow-free.
from test_client_dataframe_fuzz import (
    Rng,
    _random_strings,
    _weighted_pick_value,
    _weighted_pick_kv,
    _parse_int_env,
    _derive_master_seed,
    _format_seed,
    _sfa_conf,
    _sfa_file_count,
    ITER_SEED_ENV,
    ITERS_ENV,
    ROW_COUNT_CHOICES,
)

try:
    import polars as pl
except ImportError:
    pl = None


# ---------------------------------------------------------------------------
# Field generators. Each returns a polars Series of length n (named later by
# the DataFrame dict key). All produce capsule-path-supported columns.
# ---------------------------------------------------------------------------


def _int_series(rng, n, dtype, lo, hi):
    null_prob = 0.2 if rng.next_bool() else 0.0
    specials = (lo, hi, 0, 1, -1 if lo < 0 else 0)
    span = hi - lo + 1
    out = []
    for _ in range(n):
        if null_prob and rng.chance(null_prob):
            out.append(None)
        elif rng.chance(0.05):
            out.append(rng.choice(specials))
        else:
            out.append(lo + rng.next_int(span))
    return pl.Series(out, dtype=dtype)


def _gen_i8(rng, n):
    return _int_series(rng, n, pl.Int8, -128, 127)


def _gen_i16(rng, n):
    return _int_series(rng, n, pl.Int16, -32768, 32767)


def _gen_i32(rng, n):
    return _int_series(rng, n, pl.Int32, -(1 << 31), (1 << 31) - 1)


def _gen_i64(rng, n):
    return _int_series(rng, n, pl.Int64, -(1 << 50), (1 << 50))


def _gen_u8(rng, n):
    return _int_series(rng, n, pl.UInt8, 0, 255)


def _gen_u16(rng, n):
    return _int_series(rng, n, pl.UInt16, 0, 65535)


def _gen_u32(rng, n):
    return _int_series(rng, n, pl.UInt32, 0, (1 << 32) - 1)


def _gen_u64(rng, n):
    # Keep below i64::MAX; QuestDB QWP encodes integers as signed i64.
    return _int_series(rng, n, pl.UInt64, 0, (1 << 62))


_FLOAT_SPECIALS = (
    0.0, -0.0, 1.0, -1.0,
    float('nan'), float('inf'), float('-inf'),
    1e-300, 1e300)


def _float_series(rng, n, dtype):
    null_prob = 0.2 if rng.next_bool() else 0.0
    out = []
    for _ in range(n):
        if null_prob and rng.chance(null_prob):
            out.append(None)
        elif rng.chance(0.05):
            out.append(rng.choice(_FLOAT_SPECIALS))
        else:
            out.append(rng.uniform(-1e6, 1e6))
    return pl.Series(out, dtype=dtype)


def _gen_f32(rng, n):
    return _float_series(rng, n, pl.Float32)


def _gen_f64(rng, n):
    return _float_series(rng, n, pl.Float64)


def _gen_bool(rng, n):
    null_prob = 0.2 if rng.next_bool() else 0.0
    out = [None if (null_prob and rng.chance(null_prob)) else rng.next_bool()
           for _ in range(n)]
    return pl.Series(out, dtype=pl.Boolean)


def _gen_utf8(rng, n):
    null_prob = 0.2 if rng.next_bool() else 0.0
    return pl.Series(_random_strings(rng, n, 16, null_prob), dtype=pl.Utf8)


def _cat_pool(rng):
    cardinality = max(2, rng.next_int(16) + 2)
    pool = list(dict.fromkeys(_random_strings(rng, cardinality * 2, 8, 0.0)))
    while len(pool) < 2:
        pool.append(f'_pad_{len(pool)}')
    return pool


def _gen_categorical(rng, n):
    pool = _cat_pool(rng)
    null_prob = 0.2 if rng.next_bool() else 0.0
    out = [None if (null_prob and rng.chance(null_prob))
           else pool[rng.next_int(len(pool))]
           for _ in range(n)]
    return pl.Series(out, dtype=pl.Categorical)


def _gen_enum(rng, n):
    pool = _cat_pool(rng)
    null_prob = 0.2 if rng.next_bool() else 0.0
    out = [None if (null_prob and rng.chance(null_prob))
           else pool[rng.next_int(len(pool))]
           for _ in range(n)]
    return pl.Series(out, dtype=pl.Enum(pool))


def _gen_date(rng, n):
    # Datetime/Date map to a non-nullable QuestDB TIMESTAMP, so no nulls.
    base = datetime.date(2020, 1, 1)
    out = [base + datetime.timedelta(days=rng.next_int(4000))
           for _ in range(n)]
    return pl.Series(out, dtype=pl.Date)


def _gen_dt_us(rng, n):
    base = datetime.datetime(2024, 1, 1)
    out = [base + datetime.timedelta(seconds=rng.next_int(1 << 20))
           for _ in range(n)]
    return pl.Series(out, dtype=pl.Datetime('us'))


def _gen_dt_tz(rng, n):
    base = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
    out = [base + datetime.timedelta(seconds=rng.next_int(1 << 20))
           for _ in range(n)]
    return pl.Series(out, dtype=pl.Datetime('us', time_zone='UTC'))


def _gen_time(rng, n):
    out = [datetime.time(rng.next_int(24), rng.next_int(60), rng.next_int(60))
           for _ in range(n)]
    return pl.Series(out, dtype=pl.Time)


def _gen_decimal(rng, n):
    out = [decimal.Decimal(rng.next_int(2_000_000) - 1_000_000).scaleb(-2)
           for _ in range(n)]
    return pl.Series(out, dtype=pl.Decimal(18, 2))


def _gen_binary(rng, n):
    out = [bytes(rng.next_int(256) for _ in range(rng.next_int(8)))
           for _ in range(n)]
    return pl.Series(out, dtype=pl.Binary)


def _gen_list_f64(rng, n):
    out = [[rng.uniform(-1e3, 1e3) for _ in range(rng.next_int(4))]
           for _ in range(n)]
    return pl.Series(out, dtype=pl.List(pl.Float64))


def _gen_array_f64(rng, n):
    width = rng.next_int(3) + 1
    out = [[rng.uniform(-1e3, 1e3) for _ in range(width)] for _ in range(n)]
    return pl.Series(out, dtype=pl.Array(pl.Float64, width))


# (kind, gen, weight, string_like)
_FIELD_GENS = [
    ('i8', _gen_i8, 6, False),
    ('i16', _gen_i16, 6, False),
    ('i32', _gen_i32, 6, False),
    ('i64', _gen_i64, 8, False),
    ('u8', _gen_u8, 5, False),
    ('u16', _gen_u16, 5, False),
    ('u32', _gen_u32, 5, False),
    ('u64', _gen_u64, 5, False),
    ('f32', _gen_f32, 6, False),
    ('f64', _gen_f64, 8, False),
    ('bool', _gen_bool, 6, False),
    ('utf8', _gen_utf8, 14, True),
    ('categorical', _gen_categorical, 16, True),
    ('enum', _gen_enum, 10, True),
    ('date', _gen_date, 6, False),
    ('dt_us', _gen_dt_us, 6, False),
    ('dt_tz', _gen_dt_tz, 5, False),
    ('time', _gen_time, 4, False),
    ('decimal', _gen_decimal, 5, False),
    ('binary', _gen_binary, 5, False),
    ('list_f64', _gen_list_f64, 6, False),
    ('array_f64', _gen_array_f64, 6, False),
]


# ---------------------------------------------------------------------------
# Designated-timestamp generators. Return (series, at_ok).
# ---------------------------------------------------------------------------


def _ts_valid(rng, n):
    base = datetime.datetime(2024, 1, 1)
    vals = [base + datetime.timedelta(seconds=i) for i in range(n)]
    unit = 'ns' if rng.next_bool() else 'us'
    return pl.Series(vals, dtype=pl.Datetime(unit)), True


def _ts_null(rng, n):
    if n == 0:
        return _ts_valid(rng, n)
    base = datetime.datetime(2024, 1, 1)
    vals = [base + datetime.timedelta(seconds=i) for i in range(n)]
    for i in rng.sample(range(n), max(1, n // 8)):
        vals[i] = None
    return pl.Series(vals, dtype=pl.Datetime('us')), False


def _ts_pre_epoch(rng, n):
    if n == 0:
        return _ts_valid(rng, n)
    base = datetime.datetime(1900, 1, 1)
    vals = [base + datetime.timedelta(seconds=i) for i in range(n)]
    return pl.Series(vals, dtype=pl.Datetime('us')), False


def _ts_wrong_type(rng, n):
    return pl.Series(list(range(n)), dtype=pl.Int64), False


_AT_GENS = [
    (_ts_valid, 76),
    (_ts_null, 8),
    (_ts_pre_epoch, 8),
    (_ts_wrong_type, 8),
]


def _build_frame(rng):
    """Return (frame, kwargs, expected_supported, n_rows)."""
    n_rows = rng.choice(ROW_COUNT_CHOICES)

    at_gen = _weighted_pick_value(rng, _AT_GENS)
    ts, at_ok = at_gen(rng, n_rows)

    cols = {'ts': ts}
    string_like = []
    gen_pool = [(k, g, w) for k, g, w, _ in _FIELD_GENS]
    n_field_cols = rng.next_int(5)
    for c in range(n_field_cols):
        kind, gen = _weighted_pick_kv(rng, gen_pool)
        name = f'c{c}_{kind}'
        cols[name] = gen(rng, n_rows)
        if next(s for k, _, _, s in _FIELD_GENS if k == kind):
            string_like.append(name)

    # Empty -> no-op accept. Otherwise reject when the ts is invalid / wrong
    # type, or the frame is ts-only (the capsule needs a non-ts column).
    expected_supported = (n_rows == 0) or (at_ok and n_field_cols > 0)

    df = pl.DataFrame(cols)
    if rng.next_bool():
        order = list(df.columns)
        rng.shuffle(order)
        df = df.select(order)

    sym_mode = _weighted_pick_value(
        rng, [('auto', 6), (False, 3), ('list', 3)])
    if sym_mode == 'list' and string_like:
        k = rng.next_int(len(string_like)) + 1
        symbols = rng.sample(string_like, k)
    elif sym_mode == 'list':
        symbols = 'auto'
    else:
        symbols = sym_mode

    kwargs = {'table_name': 'polars_fuzz', 'at': 'ts', 'symbols': symbols}
    mrpb = _weighted_pick_value(rng, [(None, 5), (2, 2), (8, 2), (64, 2)])
    if mrpb is not None:
        kwargs['max_rows_per_batch'] = mrpb

    frame = df.lazy() if rng.chance(0.25) else df
    return frame, kwargs, expected_supported, n_rows


@unittest.skipUnless(pl is not None, 'polars not installed')
class TestClientPolarsDataframeFuzz(unittest.TestCase):
    """Round-trip fuzz: each iteration ingests a random polars frame through
    ``Client.dataframe()`` (capsule path) to a local ``QwpAckServer``."""

    DEFAULT_ITERS = 100

    @classmethod
    def setUpClass(cls):
        cls.iter_seed_override = _parse_int_env(ITER_SEED_ENV)
        if cls.iter_seed_override is not None:
            cls.master_seed = None
            cls.iters = 1
            sys.stderr.write(
                f'>>>> polars dataframe fuzz: '
                f'iter_seed_override={_format_seed(cls.iter_seed_override)}, '
                f'iters=1\n')
            return
        cls.master_seed = _derive_master_seed()
        cls.iters = _parse_int_env(ITERS_ENV) or cls.DEFAULT_ITERS
        sys.stderr.write(
            f'>>>> polars dataframe fuzz: master_seed='
            f'{_format_seed(cls.master_seed)}, iters={cls.iters}\n')

    def setUp(self):
        self.server = QwpAckServer()
        self.server.start()
        self.conf = (
            f'qwpws::addr=127.0.0.1:{self.server.port};'
            'pool_size=1;pool_max=1;pool_reap=manual;')

    def tearDown(self):
        self.server.stop()

    def _seed_msg(self, iter_seed):
        if self.master_seed is None:
            return f'iter={_format_seed(iter_seed)}'
        return (f'master={_format_seed(self.master_seed)}, '
                f'iter={_format_seed(iter_seed)}')

    def _master_label(self):
        if self.master_seed is None:
            return f'iter_seed_override={_format_seed(self.iter_seed_override)}'
        return f'master_seed={_format_seed(self.master_seed)}'

    def _check_one(self, client, df, kwargs, expected_supported, n_rows,
                   iter_seed, prev_binary_frames):
        try:
            client.dataframe(df, **kwargs)
        except (qi.UnsupportedDataFrameShapeError, qi.QuestDBError) as exc:
            self.assertFalse(
                expected_supported,
                f'client rejected an expected-supported frame; '
                f'{self._seed_msg(iter_seed)}: {exc}')
            # Under a small max_rows_per_batch a mid-stream reject can have
            # already flushed earlier batches, so we don't assert the count.
            return self.server.snapshot()['binary_frames']
        self.assertTrue(
            expected_supported,
            f'client accepted an expected-rejected frame; '
            f'{self._seed_msg(iter_seed)}')
        cur = self.server.snapshot()['binary_frames']
        if n_rows == 0:
            self.assertEqual(
                cur, prev_binary_frames,
                f'empty df published a binary frame; '
                f'{self._seed_msg(iter_seed)}')
        else:
            self.assertGreater(
                cur, prev_binary_frames,
                f'accepted non-empty df published no binary frame; '
                f'{self._seed_msg(iter_seed)}')
        return cur

    def _iter_seeds(self):
        if self.iter_seed_override is not None:
            return [self.iter_seed_override]
        master = Rng(self.master_seed)
        return [master.next_long() for _ in range(self.iters)]

    def test_fuzz_round_trip(self):
        seeds = self._iter_seeds()
        client = qi.Client.from_conf(self.conf)
        failures = []
        try:
            prev = 0
            for iter_seed in seeds:
                rng = Rng(iter_seed)
                try:
                    df, kwargs, expected_supported, n_rows = _build_frame(rng)
                    prev = self._check_one(
                        client, df, kwargs, expected_supported, n_rows,
                        iter_seed, prev)
                except AssertionError as exc:
                    failures.append((iter_seed, type(exc).__name__, str(exc)))
                    prev = self.server.snapshot()['binary_frames']
                except Exception as exc:  # noqa: BLE001 — fuzz triage
                    failures.append(
                        (iter_seed, type(exc).__name__, repr(exc)))
                    prev = self.server.snapshot()['binary_frames']
        finally:
            client.close()

        stats = self.server.snapshot()
        self.assertEqual(
            stats['errors'], [],
            f'server saw protocol errors: {stats["errors"]}; '
            f'{self._master_label()}')

        if failures:
            preview = '\n'.join(
                f'  iter={_format_seed(s)} [{cls}]: {m}'
                for s, cls, m in failures[:5])
            self.fail(
                f'{len(failures)}/{len(seeds)} iterations failed.\n'
                f'{self._master_label()}\n{preview}')


@unittest.skipUnless(pl is not None, 'polars not installed')
class TestClientPolarsDataframeSfaFuzz(TestClientPolarsDataframeFuzz):
    """Same polars fuzz generator, through the columnar SFA backend."""

    DEFAULT_ITERS = 25

    def setUp(self):
        self.server = QwpAckServer()
        self.server.start()
        self._sf_tmp = tempfile.TemporaryDirectory(
            prefix='client-polars-sfa-fuzz-')
        self.sender_id = 'py-pl-fuzz-' + uuid.uuid4().hex[:8]
        self.conf = _sfa_conf(
            self.server.port,
            self.sender_id,
            self._sf_tmp.name)

    def tearDown(self):
        try:
            self.assertEqual(
                _sfa_file_count(self._sf_tmp.name, self.sender_id),
                0,
                f'SFA files left after polars dataframe fuzz; '
                f'{self._master_label()}')
        finally:
            self.server.stop()
            self._sf_tmp.cleanup()


def _norm_col(series):
    out = []
    for v in series.to_list():
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            out.append(float(v))
        else:
            out.append(v)
    return out


def _val_match(a, b):
    if a is None or b is None:
        return a is None and b is None
    if isinstance(a, float) and isinstance(b, float):
        return math.isclose(a, b, rel_tol=1e-9, abs_tol=1e-12)
    return a == b


class _RunningQuestDb:
    """Adapter over an already-running QuestDB (its HTTP host:port) so the
    round-trip can target a live instance for debugging."""

    def __init__(self, host, port):
        self.host = host
        self.http_server_port = port

    def http_sql_query(self, sql_query):
        import json
        import urllib.error
        import urllib.parse
        import urllib.request
        url = (f'http://{self.host}:{self.http_server_port}/exec?'
               + urllib.parse.urlencode({'query': sql_query}))
        try:
            buf = urllib.request.urlopen(url, timeout=10).read()
        except urllib.error.HTTPError as exc:
            buf = exc.read()
        data = json.loads(buf)
        if 'error' in data:
            raise RuntimeError(data['error'])
        return data

    def stop(self):
        pass


@unittest.skipUnless(pl is not None, 'polars not installed')
class TestClientPolarsDataframeRoundTrip(unittest.TestCase):
    """Ingest a random polars frame via ``Client.dataframe()`` → real
    QuestDB → read back via ``Client.query`` → assert value equivalence.

    Point at a running QuestDB with ``QDB_HTTP_ADDR=host:port`` (handy for
    debugging), or set ``QDB_REPO_PATH=/path/to/questdb`` to spawn a
    class-scoped fixture. Tables are dropped between iterations. Write and
    read-back both stay in polars (pyarrow-free); the comparison is
    value-by-value to tolerate QuestDB's widening (SYMBOL, LONG, ...)."""

    DEFAULT_ITERS = 8

    @classmethod
    def setUpClass(cls):
        addr = os.environ.get('QDB_HTTP_ADDR')
        if addr:
            host, _, port = addr.partition(':')
            cls.qdb = _RunningQuestDb(host or 'localhost', int(port or '9000'))
            cls._owns_qdb = False
        else:
            repo = os.environ.get('QDB_REPO_PATH')
            if not repo:
                raise unittest.SkipTest(
                    'set QDB_HTTP_ADDR=host:port for a running QuestDB, '
                    'or QDB_REPO_PATH=/path/to/questdb to spawn one')
            import importlib
            import pathlib
            import shutil
            cls._fixture_mod = importlib.import_module('fixture')
            install_path = cls._fixture_mod.install_questdb_from_repo(
                pathlib.Path(repo))
            plain_dir = PROJ_ROOT / 'build' / 'questdb' / 'layer3_polars'
            plain_dir.mkdir(parents=True, exist_ok=True)
            shutil.copytree(install_path, plain_dir, dirs_exist_ok=True)
            cls.qdb = cls._fixture_mod.QuestDbFixture(
                plain_dir, auth=False, http=True)
            cls.qdb.start()
            cls._owns_qdb = True

        cls.iter_seed_override = _parse_int_env(ITER_SEED_ENV)
        if cls.iter_seed_override is not None:
            cls.master_seed = None
            cls.iters = 1
        else:
            cls.master_seed = _derive_master_seed()
            cls.iters = _parse_int_env(ITERS_ENV) or cls.DEFAULT_ITERS
        sys.stderr.write(
            f'>>>> polars round-trip fuzz vs real QuestDB: '
            f'master='
            f'{_format_seed(cls.master_seed) if cls.master_seed else "n/a"}, '
            f'iter_override='
            f'{_format_seed(cls.iter_seed_override) if cls.iter_seed_override else "n/a"}, '
            f'iters={cls.iters}\n')

    @classmethod
    def tearDownClass(cls):
        if getattr(cls, '_owns_qdb', False) and getattr(cls, 'qdb', None):
            cls.qdb.stop()

    @property
    def conf(self):
        return f'qwpws::addr={self.qdb.host}:{self.qdb.http_server_port};'

    def _wait_for_rows(self, table_name, expected, timeout_s=30):
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            try:
                res = self.qdb.http_sql_query(
                    f'SELECT count() FROM {table_name}')
            except Exception:
                time.sleep(0.1)
                continue
            rows = res.get('dataset') or []
            if rows and rows[0][0] >= expected:
                return
            time.sleep(0.1)
        raise RuntimeError(
            f'WAL apply timed out: {expected} rows expected on {table_name}')

    def _drop_table(self, table_name):
        try:
            self.qdb.http_sql_query(f'DROP TABLE IF EXISTS {table_name}')
        except Exception:
            pass

    # Lossless values only: INT64_MIN / NaN alias QuestDB LONG / DOUBLE null.
    def _build_simple_frame(self, rng):
        n_rows = max(rng.choice(ROW_COUNT_CHOICES), 1)
        base = datetime.datetime(2024, 1, 1)
        cols = {
            'ts': pl.Series(
                [base + datetime.timedelta(seconds=i) for i in range(n_rows)],
                dtype=pl.Datetime('us')),
            'id': pl.Series(list(range(1, n_rows + 1)), dtype=pl.Int64),
        }
        shape = rng.choice(['numeric', 'string', 'categorical', 'mixed'])
        if shape in ('numeric', 'mixed'):
            cols['price'] = pl.Series(
                [rng.uniform(-1e6, 1e6) for _ in range(n_rows)],
                dtype=pl.Float64)
            cols['count'] = pl.Series(
                [int(rng.uniform(-(1 << 50), 1 << 50)) for _ in range(n_rows)],
                dtype=pl.Int64)
        if shape in ('string', 'mixed'):
            cols['note'] = pl.Series(
                _random_strings(rng, n_rows, 8, 0.0, ascii_only=True,
                                empty_prob=0.0),
                dtype=pl.Utf8)
        if shape in ('categorical', 'mixed'):
            pool = list(dict.fromkeys(
                _random_strings(rng, 16, 6, 0.0, ascii_only=True,
                                empty_prob=0.0)))
            while len(pool) < 2:
                pool.append(f'p{len(pool)}')
            null_prob = 0.2 if rng.next_bool() else 0.0
            vals = [None if (null_prob and rng.chance(null_prob))
                    else pool[rng.next_int(len(pool))]
                    for _ in range(n_rows)]
            cols['sym'] = pl.Series(vals, dtype=pl.Categorical)
        return pl.DataFrame(cols), shape, n_rows

    def _iter_seeds(self):
        if self.iter_seed_override is not None:
            return [self.iter_seed_override]
        master = Rng(self.master_seed)
        return [master.next_long() for _ in range(self.iters)]

    def test_round_trip(self):
        seeds = self._iter_seeds()
        failures = []
        for iter_idx, iter_seed in enumerate(seeds):
            rng = Rng(iter_seed)
            shape = '?'
            table_name = f'plrt_{iter_idx}_{iter_seed:016x}'
            try:
                df, shape, n_rows = self._build_simple_frame(rng)
                self._drop_table(table_name)
                with qi.Client.from_conf(self.conf) as client:
                    client.dataframe(df, table_name=table_name, at='ts')
                self._wait_for_rows(table_name, n_rows)

                cols = [c for c in df.columns if c != 'ts']
                df_in = df.select(cols).sort('id')
                sql = (f"SELECT {','.join(cols)} FROM {table_name} "
                       f"ORDER BY id")
                with qi.Client.from_conf(self.conf) as client:
                    df_out = client.query(sql).to_polars().sort('id')

                mismatch = None
                for c in sorted(cols):
                    a = _norm_col(df_in.get_column(c))
                    b = _norm_col(df_out.get_column(c))
                    if len(a) != len(b):
                        mismatch = f'{c}: {len(a)} vs {len(b)} rows'
                        break
                    for i, (x, y) in enumerate(zip(a, b)):
                        if not _val_match(x, y):
                            mismatch = f'{c}[{i}]: {x!r} != {y!r}'
                            break
                    if mismatch:
                        break
                if mismatch:
                    raise AssertionError(mismatch)
            except Exception as exc:  # noqa: BLE001 — fuzz triage
                failures.append(
                    (iter_seed, shape, type(exc).__name__, repr(exc)))
                self._drop_table(table_name)

        if failures:
            preview = '\n'.join(
                f'  iter={_format_seed(s)} shape={sh} [{cls}]: {m}'
                for s, sh, cls, m in failures[:5])
            self.fail(
                f'{len(failures)}/{len(seeds)} iterations failed.\n'
                f'(showing first 5)\n{preview}')


if __name__ == '__main__':
    unittest.main()
