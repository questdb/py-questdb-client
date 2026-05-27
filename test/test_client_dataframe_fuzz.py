"""
Deterministic, seed-controlled fuzz coverage for Client.dataframe().

Mirrors the seed-and-replay convention from
``c-questdb-client/system_test/qwp_ws_fuzz.py``:

  - Pick a master 64-bit seed at ``setUpClass``: random by default via
    ``secrets.randbits(64)``, or explicit via ``QDB_CLIENT_FUZZ_SEED``
    (hex with ``0x`` prefix, or decimal).
  - Print the master seed to stderr once per run so failing CI logs are
    reproducible.
  - Each iteration draws its own child seed from the master, so a single
    failing iteration can be reproduced by setting
    ``QDB_CLIENT_FUZZ_ITER_SEED=<seed>`` to run that one iteration alone.

Every iteration drives ``Client.dataframe()`` round-trip through a local
``QwpAckServer`` fixture (no real QuestDB required) and asserts:

  - Frames the v1 planner rejects raise ``UnsupportedDataFrameShapeError``
    BEFORE any QWP/WebSocket binary frame is published.
  - Frames the v1 planner accepts complete without raising and produce at
    least one QWP1 binary frame at the server (unless the frame is empty,
    in which case ``Client.dataframe()`` is a no-op).
  - The pool reuses a single TCP accept across the whole iteration loop
    (``pool_size=pool_max=1``, ``pool_reap=manual``).
  - The server reports no protocol-level errors at any point.

Usage::

    venv/bin/python -m unittest test.test_client_dataframe_fuzz

    # Reproduce a master sequence:
    QDB_CLIENT_FUZZ_SEED=0xdeadbeefdeadbeef \\
        venv/bin/python -m unittest test.test_client_dataframe_fuzz

    # Run a longer sweep:
    QDB_CLIENT_FUZZ_ITERS=500 \\
        venv/bin/python -m unittest test.test_client_dataframe_fuzz

    # Reproduce a single iteration:
    QDB_CLIENT_FUZZ_ITER_SEED=0x... \\
        venv/bin/python -m unittest \\
        test.test_client_dataframe_fuzz.TestClientDataframeFuzz.test_fuzz_round_trip
"""

import sys

sys.dont_write_bytecode = True

import datetime
import os
import random
import secrets
import unittest

import numpy as np

import patch_path
patch_path.patch()

import questdb.ingress as qi

PROJ_ROOT = patch_path.PROJ_ROOT
sys.path.append(str(PROJ_ROOT / 'c-questdb-client' / 'system_test'))

try:
    import pandas as pd
    import pyarrow as pa
except ImportError:
    pd = None
    pa = None

from qwp_ws_ack_server import QwpAckServer


SEED_ENV = 'QDB_CLIENT_FUZZ_SEED'
ITER_SEED_ENV = 'QDB_CLIENT_FUZZ_ITER_SEED'
ITERS_ENV = 'QDB_CLIENT_FUZZ_ITERS'


def _parse_int_env(name):
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return None
    raw = raw.strip()
    if raw.lower().startswith('0x'):
        return int(raw, 16)
    return int(raw)


def _derive_master_seed():
    parsed = _parse_int_env(SEED_ENV)
    if parsed is not None:
        return parsed
    return secrets.randbits(64)


def _format_seed(seed):
    return f'0x{seed:016x}'


class Rng:
    """``random.Random`` wrapper. Seed and helpers shaped for fuzz reuse."""

    __slots__ = ('_impl', 'seed')

    def __init__(self, seed):
        self.seed = seed & ((1 << 64) - 1)
        self._impl = random.Random(self.seed)

    def next_int(self, bound):
        if bound <= 0:
            raise ValueError('bound must be positive')
        return self._impl.randrange(bound)

    def next_bool(self):
        return self._impl.getrandbits(1) == 1

    def next_long(self):
        return self._impl.getrandbits(64)

    def choice(self, seq):
        return self._impl.choice(seq)

    def shuffle(self, seq):
        self._impl.shuffle(seq)

    def sample(self, seq, k):
        return self._impl.sample(list(seq), k)

    def uniform(self, lo, hi):
        return self._impl.uniform(lo, hi)

    def chance(self, prob):
        return self._impl.random() < prob


# ---------------------------------------------------------------------------
# Helpers for column data.
# ---------------------------------------------------------------------------


def _build_test_alphabet():
    """Multi-script alphabet for stressing UTF-8 handling in varchar /
    categorical columns. Restricted to letter-ish ranges so values don't
    collide with QWP wire-format reserved bytes or break the server's
    UTF-8 validator on round-trip."""
    ranges = [
        (0x0041, 0x005A),  # A-Z
        (0x0061, 0x007A),  # a-z
        (0x0030, 0x0039),  # 0-9
        (0x00C0, 0x00FF),  # Latin-1 supplement letters
        (0x0100, 0x017F),  # Latin Extended-A
        (0x0370, 0x03FF),  # Greek
        (0x0400, 0x04FF),  # Cyrillic
    ]
    return [chr(cp) for r in ranges for cp in range(r[0], r[1] + 1)]


_TEST_ALPHABET = _build_test_alphabet()
_ASCII_LETTERS = [chr(c) for c in range(ord('A'), ord('Z') + 1)]


def _random_strings(rng, n, max_len, null_prob, *, ascii_only=False):
    """Generate n strings, possibly with nulls. ``ascii_only`` forces the
    ASCII-letter subset (useful where the planner / FFI layer reserves a
    code point or path)."""
    pool = _ASCII_LETTERS if ascii_only else _TEST_ALPHABET
    out = []
    for _ in range(n):
        if null_prob > 0 and rng.chance(null_prob):
            out.append(None)
            continue
        length = max(1, rng.next_int(max_len))
        out.append(''.join(rng.choice(pool) for _ in range(length)))
    return out


def _datetime_array(n, unit='ns'):
    base = np.datetime64('2024-01-01T00:00:00', unit).astype('int64')
    step = {'ns': 1_000_000_000, 'us': 1_000_000}[unit]
    return (base + step * np.arange(n, dtype=np.int64)).astype(
        f'datetime64[{unit}]')


# ---------------------------------------------------------------------------
# Supported field generators. Each returns a pd.Series with n_rows rows.
# ---------------------------------------------------------------------------


def _gen_int64(rng, n):
    return pd.Series(np.array(
        [int(rng.uniform(-(1 << 50), 1 << 50)) for _ in range(n)],
        dtype=np.int64))


def _gen_float64(rng, n):
    return pd.Series(np.array(
        [rng.uniform(-1e6, 1e6) for _ in range(n)], dtype=np.float64))


def _gen_dt64ns_field(rng, n):
    return pd.Series(_datetime_array(n, 'ns'))


def _gen_dt64us_field(rng, n):
    return pd.Series(_datetime_array(n, 'us'))


def _gen_categorical(rng, n):
    # Force a string-typed categories index. With the default constructor,
    # an all-None categorical infers float64 categories — which the row
    # path's argument resolver rejects with "Expected a category of
    # strings". That rejection happens upstream of the columnar planner,
    # so we'd see neither the v1 reject nor the v1 accept paths.
    if n == 0:
        return pd.Series(pd.Categorical(
            [], categories=pd.Index([], dtype=object)))
    cardinality = max(2, min(n, rng.next_int(16) + 2))
    # Categories must be unique. Oversample then dedup; fall back to a
    # deterministic pad if the alphabet collisions leave us short.
    raw_pool = _random_strings(rng, cardinality * 2, 8, 0.0)
    pool = list(dict.fromkeys(raw_pool))[:cardinality]
    while len(pool) < 2:
        pool.append(f'_pad_{len(pool)}')
    null_prob = 0.2 if rng.next_bool() else 0.0
    choices = [
        None if rng.chance(null_prob) else pool[rng.next_int(len(pool))]
        for _ in range(n)]
    return pd.Series(pd.Categorical(
        choices, dtype=pd.CategoricalDtype(categories=pool)))


def _gen_string_pyarrow(rng, n):
    null_prob = 0.2 if rng.next_bool() else 0.0
    items = _random_strings(rng, n, 16, null_prob)
    return pd.Series(items, dtype='string[pyarrow]')


def _gen_large_string(rng, n):
    null_prob = 0.2 if rng.next_bool() else 0.0
    items = _random_strings(rng, n, 8, null_prob)
    arr = pa.array(items, type=pa.large_string())
    return pd.Series(arr, dtype=pd.ArrowDtype(pa.large_string()))


# (kind, generator, weight). Weights bias toward the variable-width and
# nullable types (categorical, string varieties) because those exercise
# more emitter code paths than fixed-width numerics.
SUPPORTED_FIELD_GENS_WEIGHTED = [
    ('int64', _gen_int64, 10),
    ('float64', _gen_float64, 10),
    ('dt64ns_field', _gen_dt64ns_field, 8),
    ('dt64us_field', _gen_dt64us_field, 8),
    ('categorical', _gen_categorical, 18),
    ('string_pyarrow', _gen_string_pyarrow, 18),
    ('large_string', _gen_large_string, 12),
]


# ---------------------------------------------------------------------------
# Unsupported field generators. Mere presence of one should make the whole
# plan reject.
# ---------------------------------------------------------------------------


def _gen_int32(rng, n):
    return pd.Series(np.array(
        [rng.next_int(1 << 30) for _ in range(n)], dtype=np.int32))


def _gen_float32(rng, n):
    return pd.Series(np.array(
        [rng.uniform(-1e6, 1e6) for _ in range(n)], dtype=np.float32))


def _gen_bool(rng, n):
    return pd.Series(np.array(
        [rng.next_bool() for _ in range(n)], dtype=bool))


def _gen_uint8(rng, n):
    return pd.Series(np.array(
        [rng.next_int(256) for _ in range(n)], dtype=np.uint8))


def _gen_uint64(rng, n):
    return pd.Series(np.array(
        [rng.next_int(1 << 32) for _ in range(n)], dtype=np.uint64))


def _gen_object_str(rng, n):
    items = _random_strings(rng, n, 8, 0.0)
    return pd.Series(items, dtype='object')


def _gen_string_python(rng, n):
    # Force python-backed storage. With pyarrow installed, modern pandas
    # defaults `dtype='string'` to pyarrow storage, which IS supported by
    # the columnar planner — so the generator name would be misleading
    # without the explicit `storage='python'` here.
    items = _random_strings(rng, n, 8, 0.0)
    return pd.Series(items, dtype=pd.StringDtype(storage='python'))


UNSUPPORTED_FIELD_GENS = [
    ('int32', _gen_int32),
    ('float32', _gen_float32),
    ('bool', _gen_bool),
    ('uint8', _gen_uint8),
    ('uint64', _gen_uint64),
    ('object_str', _gen_object_str),
    ('string_python', _gen_string_python),
]


# ---------------------------------------------------------------------------
# Designated-timestamp generators.
# ---------------------------------------------------------------------------


def _gen_at_dt64ns(rng, n):
    return pd.Series(_datetime_array(n, 'ns')), True


def _gen_at_dt64us(rng, n):
    return pd.Series(_datetime_array(n, 'us')), True


def _gen_at_dt64ns_nat(rng, n):
    if n == 0:
        return pd.Series(_datetime_array(0, 'ns')), True
    s = pd.Series(_datetime_array(n, 'ns')).copy()
    n_nat = max(1, n // 8)
    idx = rng.sample(range(n), min(n_nat, n))
    s.iloc[idx] = pd.NaT
    return s, False


def _gen_at_dt64ns_negative(rng, n):
    if n == 0:
        return pd.Series(_datetime_array(0, 'ns')), True
    base = (-1_000_000_000) * np.arange(1, n + 1, dtype=np.int64)
    return pd.Series(base.astype('datetime64[ns]')), False


# (generator, weight). Heavy bias toward the happy-path units so the
# fuzz mostly drives the supported flow; the planner-rejection variants
# (NaT, negative timestamp) are kept rare to leave room for column-side
# rejection cases to be the more interesting axis. Tweak weights for
# targeted reproduction by setting QDB_CLIENT_FUZZ_ITER_SEED instead.
AT_GENS_WEIGHTED = [
    (_gen_at_dt64ns, 70),
    (_gen_at_dt64us, 20),
    (_gen_at_dt64ns_nat, 5),
    (_gen_at_dt64ns_negative, 5),
]


def _weighted_pick_value(rng, weighted_seq):
    """Pick an item from ``[(value, weight), ...]``."""
    total = sum(w for _, w in weighted_seq)
    pick = rng.next_int(total)
    accum = 0
    for item, w in weighted_seq:
        accum += w
        if pick < accum:
            return item
    return weighted_seq[-1][0]


def _weighted_pick_kv(rng, weighted_triples):
    """Pick an item from ``[(key, value, weight), ...]``."""
    total = sum(t[-1] for t in weighted_triples)
    pick = rng.next_int(total)
    accum = 0
    for triple in weighted_triples:
        accum += triple[-1]
        if pick < accum:
            return triple[0], triple[1]
    return weighted_triples[-1][0], weighted_triples[-1][1]


# Row counts deliberately chosen to hit chunk-boundary edges:
#  - 0      empty df no-op
#  - 1, 7   < the 8-row validity alignment floor
#  - 8, 16  exact multiples of 8
#  - 9, 17  multiple-of-8 + 1 -> tail chunk
#  - others a few larger sizes
ROW_COUNT_CHOICES = [0, 1, 2, 7, 8, 9, 15, 16, 17, 32, 63, 64, 100, 257]


# Symbols-argument variants picked per iteration. Kept as named modes
# so the categorical-routing constraints stay obvious:
#   - 'auto'   : every categorical is a symbol.
#   - False    : no symbols; categoricals fall to the string-field path
#                which v1 rejects.
#   - 'all'    : explicit list of every categorical (equivalent to auto
#                but exercises the list-symbols code path).
#   - 'partial': drop one categorical from the symbol list when there
#                are at least two cats present; the unlisted cat falls
#                to the string-field path and the planner rejects.
SYMBOL_MODES_WEIGHTED = [
    ('auto', 6),
    (False, 3),
    ('all', 3),
    ('partial', 2),
]


def _build_frame(rng):
    """
    Return (df, kwargs, expected_supported).

    ``expected_supported`` describes the static v1 planner's accept/reject
    decision. If True and ``len(df) == 0``, ``Client.dataframe()`` returns
    early without sending; otherwise an accepted frame produces at least
    one binary frame on the wire.

    Column generation and the ``symbols`` argument are kept consistent
    so ``expected_supported`` actually reflects the planner's rules
    (categoricals route through the symbol path only when 'auto' or
    explicitly listed; an unlisted categorical falls to the string-field
    path which v1 rejects).
    """
    n_rows = rng.choice(ROW_COUNT_CHOICES)

    at_gen = _weighted_pick_value(rng, AT_GENS_WEIGHTED)
    ts, at_ok = at_gen(rng, n_rows)
    expected_supported = at_ok

    # Decide the symbols mode up front so we know whether to allow
    # categorical columns at all.
    sym_mode = _weighted_pick_value(rng, SYMBOL_MODES_WEIGHTED)
    allow_categorical = sym_mode is not False

    cols = {'ts': ts}

    # ~25% of frames include an explicitly unsupported field column.
    # An empty df short-circuits validation entirely, so a 0-row frame
    # with an unsupported column is still a no-op accept.
    if rng.chance(0.25):
        kind, gen = _weighted_pick_kv(
            rng, [(k, g, 1) for k, g in UNSUPPORTED_FIELD_GENS])
        cols[f'bad_{kind}'] = gen(rng, n_rows)
        if n_rows > 0:
            expected_supported = False

    gen_pool = SUPPORTED_FIELD_GENS_WEIGHTED
    if not allow_categorical:
        gen_pool = [(k, g, w) for k, g, w in SUPPORTED_FIELD_GENS_WEIGHTED
                    if k != 'categorical']

    n_field_cols = rng.next_int(4) + 1
    cat_col_names = []
    for c in range(n_field_cols):
        kind, gen = _weighted_pick_kv(rng, gen_pool)
        name = f'c{c}_{kind}'
        cols[name] = gen(rng, n_rows)
        if kind == 'categorical':
            cat_col_names.append(name)

    df = pd.DataFrame(cols)

    # Column order shouldn't affect correctness; randomise to flush out
    # planner ordering bugs.
    if rng.next_bool():
        order = list(df.columns)
        rng.shuffle(order)
        df = df[order]

    # Resolve the symbols mode into a concrete argument now that we know
    # which categoricals exist.
    if sym_mode == 'auto':
        symbols = 'auto'
    elif sym_mode is False:
        symbols = False
    elif sym_mode == 'all':
        symbols = cat_col_names if cat_col_names else 'auto'
    elif sym_mode == 'partial':
        if len(cat_col_names) >= 2:
            listed = list(cat_col_names)
            rng.shuffle(listed)
            symbols = listed[:-1]
            # At least one categorical is unlisted; planner rejects it.
            if n_rows > 0:
                expected_supported = False
        else:
            # No second categorical to drop -> degenerate; equivalent to
            # listing all (or 'auto' when none exist).
            symbols = cat_col_names if cat_col_names else 'auto'
    else:
        raise RuntimeError(f'unknown sym_mode={sym_mode!r}')

    kwargs = {'table_name': 'fuzz_table', 'at': 'ts', 'symbols': symbols}
    return df, kwargs, expected_supported


# ---------------------------------------------------------------------------
# Tests.
# ---------------------------------------------------------------------------


@unittest.skipIf(pd is None or pa is None, 'pandas/pyarrow not installed')
class TestClientDataframeFuzz(unittest.TestCase):
    """Round-trip fuzz: every iteration goes through Client.dataframe() to
    a local QwpAckServer."""

    DEFAULT_ITERS = 100

    @classmethod
    def setUpClass(cls):
        cls.iter_seed_override = _parse_int_env(ITER_SEED_ENV)
        if cls.iter_seed_override is not None:
            # In override mode the master seed never feeds anything;
            # report only the iter seed so the log isn't misleading.
            cls.master_seed = None
            cls.iters = 1
            sys.stderr.write(
                f'>>>> Client.dataframe fuzz: '
                f'iter_seed_override={_format_seed(cls.iter_seed_override)}, '
                f'iters=1\n')
            return
        cls.master_seed = _derive_master_seed()
        cls.iters = _parse_int_env(ITERS_ENV) or cls.DEFAULT_ITERS
        sys.stderr.write(
            f'>>>> Client.dataframe fuzz: master_seed='
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
        return (
            f'master={_format_seed(self.master_seed)}, '
            f'iter={_format_seed(iter_seed)}')

    def _check_one(self, client, df, kwargs, expected_supported,
                   iter_seed, prev_binary_frames):
        """Run one iteration. Returns the new ``binary_frames`` count so
        the loop can advance ``prev`` without an extra snapshot."""
        try:
            client.dataframe(df, **kwargs)
        except qi.UnsupportedDataFrameShapeError as exc:
            self.assertEqual(
                exc.code, qi.IngressErrorCode.BadDataFrame,
                f'UnsupportedDataFrameShapeError did not carry '
                f'BadDataFrame code; {self._seed_msg(iter_seed)}')
            self.assertFalse(
                expected_supported,
                f'Client rejected an expected-supported frame; '
                f'{self._seed_msg(iter_seed)}: {exc}')
            cur = self.server.snapshot()['binary_frames']
            self.assertEqual(
                cur, prev_binary_frames,
                f'rejection published a binary frame; '
                f'{self._seed_msg(iter_seed)}')
            return cur
        # Accept path.
        self.assertTrue(
            expected_supported,
            f'Client accepted an expected-rejected frame; '
            f'{self._seed_msg(iter_seed)}')
        cur = self.server.snapshot()['binary_frames']
        if len(df) == 0:
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

    def _master_label(self):
        if self.master_seed is None:
            return f'iter_seed_override={_format_seed(self.iter_seed_override)}'
        return f'master_seed={_format_seed(self.master_seed)}'

    def test_fuzz_round_trip(self):
        seeds = self._iter_seeds()
        client = qi.Client.from_conf(self.conf)
        failures = []
        try:
            prev = 0
            for iter_seed in seeds:
                rng = Rng(iter_seed)
                try:
                    df, kwargs, expected_supported = _build_frame(rng)
                    prev = self._check_one(
                        client, df, kwargs, expected_supported,
                        iter_seed, prev)
                except AssertionError as exc:
                    failures.append((iter_seed, type(exc).__name__, str(exc)))
                    prev = self.server.snapshot()['binary_frames']
                except qi.IngressError as exc:
                    # Unexpected IngressError (not Unsupported...): real
                    # finding. Record with seed and keep going so we
                    # surface every failing seed in one run.
                    failures.append((
                        iter_seed, type(exc).__name__,
                        f'{exc.code}: {exc}'))
                    prev = self.server.snapshot()['binary_frames']
                except Exception as exc:  # noqa: BLE001 — fuzz triage
                    failures.append((
                        iter_seed, type(exc).__name__, repr(exc)))
                    prev = self.server.snapshot()['binary_frames']
        finally:
            client.close()

        stats = self.server.snapshot()
        self.assertEqual(
            stats['errors'], [],
            f'server saw protocol errors: {stats["errors"]}; '
            f'{self._master_label()}')
        self.assertEqual(
            stats['accepted_connections'], 1,
            f'expected 1 TCP accept across {len(seeds)} iterations, '
            f'saw {stats["accepted_connections"]}; '
            f'{self._master_label()}')

        if failures:
            preview = '\n'.join(
                f'  iter={_format_seed(s)} [{cls}]: {m}'
                for s, cls, m in failures[:5])
            self.fail(
                f'{len(failures)}/{len(seeds)} iterations failed.\n'
                f'{self._master_label()}\n'
                f'(showing first 5)\n{preview}')

    # ------- Focused property tests below. Reliable, non-fuzz. -------

    def test_rejects_non_column_at_arguments(self):
        df = pd.DataFrame({
            'ts': pd.Series(_datetime_array(2)),
            'seq': pd.Series([1, 2], dtype='int64'),
        })
        client = qi.Client.from_conf(self.conf)
        try:
            for at_val in (
                    qi.ServerTimestamp,
                    qi.TimestampNanos(1_700_000_000_000_000_000),
                    datetime.datetime(2024, 1, 1)):
                with self.assertRaises(
                        qi.UnsupportedDataFrameShapeError,
                        msg=f'at={at_val!r} should be rejected'):
                    client.dataframe(df, table_name='t', at=at_val)
        finally:
            client.close()
        self.assertEqual(self.server.snapshot()['binary_frames'], 0)

    def test_rejects_table_name_col(self):
        df = pd.DataFrame({
            'ts': pd.Series(_datetime_array(2)),
            'tbl': pd.Series(['a', 'b'], dtype='string[pyarrow]'),
            'seq': pd.Series([1, 2], dtype='int64'),
        })
        client = qi.Client.from_conf(self.conf)
        try:
            with self.assertRaises(qi.UnsupportedDataFrameShapeError):
                client.dataframe(df, table_name_col='tbl', at='ts')
        finally:
            client.close()
        self.assertEqual(self.server.snapshot()['binary_frames'], 0)

    def test_closed_client_methods_reject(self):
        client = qi.Client.from_conf(self.conf)
        client.close()
        df = pd.DataFrame({
            'ts': pd.Series(_datetime_array(1)),
            'seq': pd.Series([1], dtype='int64'),
        })

        def _call_dataframe(c):
            c.dataframe(df, table_name='t', at='ts')

        def _call_reap(c):
            c.reap_idle()

        def _call_enter(c):
            c.__enter__()

        for op in (_call_dataframe, _call_reap, _call_enter):
            with self.assertRaises(qi.IngressError) as cm:
                op(client)
            self.assertEqual(
                cm.exception.code, qi.IngressErrorCode.InvalidApiCall,
                f'{op.__name__} on closed client should raise InvalidApiCall')

        # close() must remain idempotent on a closed client.
        client.close()
        client.close()

    def test_multi_chunk_emission(self):
        """Force ``len(df)`` above the planner's per-chunk row cap so the
        chunk-split loop, deferred-flush path, and final sync are all
        exercised. The Arrow-string planner cap is 32 000, so 32 001
        rows guarantees two chunks (32 000 + 1)."""
        n_rows = 32_001
        rng = Rng(0xc4_c0_de_b1_05_1d_75_3d)  # deterministic, arbitrary
        items = _random_strings(rng, n_rows, 8, 0.0)
        df = pd.DataFrame({
            'ts': pd.Series(_datetime_array(n_rows, 'ns')),
            's': pd.Series(items, dtype='string[pyarrow]'),
            'seq': pd.Series(np.arange(n_rows, dtype=np.int64)),
        })
        client = qi.Client.from_conf(self.conf)
        try:
            qi._debug_dataframe_columnar_io_stats(enabled=True, reset=True)
            try:
                client.dataframe(df, table_name='multi_chunk', at='ts',
                                 symbols=False)
            finally:
                io_stats = qi._debug_dataframe_columnar_io_stats(
                    enabled=False)
        finally:
            client.close()
        self.assertGreaterEqual(
            io_stats['flush_calls'], 2,
            f'multi-chunk emission expected >=2 flushes; '
            f'got io_stats={io_stats}')
        self.assertEqual(
            io_stats['sync_calls'], 1,
            f'expected exactly one sync per Client.dataframe() call; '
            f'got io_stats={io_stats}')
        stats = self.server.snapshot()
        self.assertEqual(stats['errors'], [])
        self.assertGreaterEqual(stats['binary_frames'], 2)

    def test_empty_dataframe_is_noop(self):
        df = pd.DataFrame({
            'ts': pd.Series([], dtype='datetime64[ns]'),
            'seq': pd.Series([], dtype='int64'),
        })
        client = qi.Client.from_conf(self.conf)
        try:
            client.dataframe(df, table_name='t', at='ts')
        finally:
            client.close()
        stats = self.server.snapshot()
        self.assertEqual(stats['binary_frames'], 0)
        self.assertEqual(stats['errors'], [])

    def test_from_conf_rejects_non_qwp_websocket(self):
        with self.assertRaises(qi.IngressError) as cm:
            qi.Client.from_conf('tcp::addr=localhost:9009;')
        self.assertEqual(cm.exception.code, qi.IngressErrorCode.ConfigError)

    def test_from_conf_requires_addr(self):
        with self.assertRaises(qi.IngressError) as cm:
            qi.Client.from_conf('qwpws::pool_size=1;')
        self.assertEqual(cm.exception.code, qi.IngressErrorCode.ConfigError)


if __name__ == '__main__':
    unittest.main()
