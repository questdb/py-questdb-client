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

  - Frames the v1 planner rejects raise before any QWP/WebSocket binary frame
    is published. Most shape rejections raise
    ``UnsupportedDataFrameShapeError``; Arrow validation rejections surface as
    ``QuestDBError``.
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
import uuid

import numpy as np

import patch_path
patch_path.patch()

import questdb._client as qi

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


def _random_strings(rng, n, max_len, null_prob, *,
                    ascii_only=False, empty_prob=0.05):
    """Generate n strings, possibly with nulls and zero-length values.

    ``ascii_only`` forces the ASCII-letter subset. ``empty_prob`` is the
    chance of emitting ``''`` for a non-null slot — empty strings
    exercise the zero-length offset slice in the varchar wire path."""
    pool = _ASCII_LETTERS if ascii_only else _TEST_ALPHABET
    out = []
    for _ in range(n):
        if null_prob > 0 and rng.chance(null_prob):
            out.append(None)
            continue
        if empty_prob > 0 and rng.chance(empty_prob):
            out.append('')
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


_INT64_MIN = -(1 << 63)
_INT64_MAX = (1 << 63) - 1
_INT64_SPECIALS = (
    0, 1, -1,
    _INT64_MIN, _INT64_MIN + 1,
    _INT64_MAX, _INT64_MAX - 1)

_FLOAT64_SPECIALS = (
    0.0, -0.0, 1.0, -1.0,
    float('nan'), float('inf'), float('-inf'),
    1e-300, 1e300)


def _gen_int64(rng, n):
    # 5% special values to exercise wire-edge cases: INT64_MIN
    # (QuestDB's NULL sentinel for LONG — should still flow through
    # the wire), INT64_MAX, zero, etc.
    out = np.empty(n, dtype=np.int64)
    for i in range(n):
        if rng.chance(0.05):
            out[i] = rng.choice(_INT64_SPECIALS)
        else:
            out[i] = int(rng.uniform(-(1 << 50), 1 << 50))
    return pd.Series(out)


def _gen_float64(rng, n):
    # 5% IEEE-754 special values: NaN, ±Inf, ±0.0, subnormals. None of
    # these should crash the wire encoder; the server may reject them
    # semantically but the QwpAckServer doesn't validate value content.
    out = np.empty(n, dtype=np.float64)
    for i in range(n):
        if rng.chance(0.05):
            out[i] = rng.choice(_FLOAT64_SPECIALS)
        else:
            out[i] = rng.uniform(-1e6, 1e6)
    return pd.Series(out)


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
    # object-dtype str is appended below, once `_gen_object_str` is defined.
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
    # Step 3 moved every narrower-dtype generator into the supported
    # list below. The only remaining intentional rejection cases live
    # in dedicated focused tests (e.g. table_name_col, NaT designated
    # ts, non-column at), not in the fuzz frame builder.
]


# Step 3 added a Rust-side widening / packing appender. Every narrower
# NumPy numeric dtype + native bool is now supported via
# column_sender_chunk_append_numpy_column.
SUPPORTED_FIELD_GENS_WEIGHTED.append(('int32', _gen_int32, 8))
SUPPORTED_FIELD_GENS_WEIGHTED.append(('float32', _gen_float32, 8))
SUPPORTED_FIELD_GENS_WEIGHTED.append(('bool_numpy', _gen_bool, 6))
SUPPORTED_FIELD_GENS_WEIGHTED.append(('uint8', _gen_uint8, 6))
SUPPORTED_FIELD_GENS_WEIGHTED.append(('uint64', _gen_uint64, 6))


# Object-dtype int / float / bool generators.

def _gen_object_int(rng, n):
    items = [int(rng.uniform(-(1 << 30), 1 << 30)) for _ in range(n)]
    return pd.Series(items, dtype='object')


def _gen_object_float(rng, n):
    items = [rng.uniform(-1e6, 1e6) for _ in range(n)]
    return pd.Series(items, dtype='object')


def _gen_object_bool(rng, n):
    items = [bool(rng.next_bool()) for _ in range(n)]
    return pd.Series(items, dtype='object')


# Step 4 added PyObject support via the sniff+build path. Both
# object-dtype str and pd.StringDtype(storage='python') resolve to
# col_source_str_pyobj; int / float / bool flow through their own
# pyobj sources.
SUPPORTED_FIELD_GENS_WEIGHTED.append(('object_str', _gen_object_str, 10))
SUPPORTED_FIELD_GENS_WEIGHTED.append(('string_python', _gen_string_python, 6))
SUPPORTED_FIELD_GENS_WEIGHTED.append(('object_int', _gen_object_int, 8))
SUPPORTED_FIELD_GENS_WEIGHTED.append(('object_float', _gen_object_float, 8))
SUPPORTED_FIELD_GENS_WEIGHTED.append(('object_bool', _gen_object_bool, 6))


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

    # Step 3 emptied UNSUPPORTED_FIELD_GENS — every previously-rejected
    # narrow NumPy dtype is now accepted via the widening appender, and
    # PyObject sources via the sniff+build path. The unsupported-column
    # injection is preserved here for forward use if a new
    # never-accepted dtype shows up in the future.
    if UNSUPPORTED_FIELD_GENS and rng.chance(0.25):
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
            # An unlisted categorical is not rejected: like the row/numpy
            # path, it falls through to a plain VARCHAR field, so the frame
            # stays supported.
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
            f'ws::addr=127.0.0.1:{self.server.port};'
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
                exc.code, qi.QuestDBErrorCode.BadDataFrame,
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
        except qi.QuestDBError as exc:
            self.assertFalse(
                expected_supported,
                f'Client raised QuestDBError for an expected-supported frame; '
                f'{self._seed_msg(iter_seed)}: {exc.code}: {exc}')
            cur = self.server.snapshot()['binary_frames']
            self.assertEqual(
                cur, prev_binary_frames,
                f'QuestDBError rejection published a binary frame; '
                f'{self._seed_msg(iter_seed)}: {exc.code}: {exc}')
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
            with self.assertRaises(qi.QuestDBError) as cm:
                op(client)
            self.assertEqual(
                cm.exception.code, qi.QuestDBErrorCode.InvalidApiCall,
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

    def test_multi_chunk_with_nulls(self):
        """Force multi-chunk emission with a nullable categorical so the
        validity bitmap must be sliced across chunk boundaries.

        The categorical-symbols planner cap is 100 000 and the planner
        rounds chunk size to a multiple of 8 when validity is present.
        Using > 100 000 rows guarantees at least two chunks; randomly
        sprinkled nulls verify ``(<uint8_t*>arr.buffers[0]) + (row_offset
        // 8)`` lands on the correct byte for the second chunk."""
        n_rows = 100_003  # > 100k cap + force a 3-row tail chunk
        rng = Rng(0xa17e_4c91_55_42_99_03)
        sym_pool = [f'S{i:04d}' for i in range(64)]
        choices = [
            None if rng.chance(0.15) else sym_pool[rng.next_int(64)]
            for _ in range(n_rows)]
        df = pd.DataFrame({
            'ts': pd.Series(_datetime_array(n_rows, 'ns')),
            'sym': pd.Series(pd.Categorical(
                choices, dtype=pd.CategoricalDtype(categories=sym_pool))),
            'seq': pd.Series(np.arange(n_rows, dtype=np.int64)),
        })
        client = qi.Client.from_conf(self.conf)
        try:
            qi._debug_dataframe_columnar_io_stats(enabled=True, reset=True)
            try:
                client.dataframe(df, table_name='mc_nulls', at='ts')
            finally:
                io_stats = qi._debug_dataframe_columnar_io_stats(
                    enabled=False)
        finally:
            client.close()
        self.assertGreaterEqual(
            io_stats['flush_calls'], 2,
            f'expected >=2 flushes; io_stats={io_stats}')
        self.assertEqual(io_stats['sync_calls'], 1)
        stats = self.server.snapshot()
        self.assertEqual(stats['errors'], [])
        self.assertGreaterEqual(stats['binary_frames'], 2)

    def test_high_cardinality_symbol_i16(self):
        """A categorical with > 128 categories forces the i16-codes
        path. The default rng-driven fuzz almost never produces enough
        cardinality to reach this branch."""
        n_rows = 1_000
        cardinality = 200  # > 128 -> i16
        rng = Rng(0xc1d_7e_4f_55_42_de_ad)
        pool = [f'C{i:04d}_{chr(0x0391 + (i % 24))}' for i in range(cardinality)]
        choices = [pool[rng.next_int(cardinality)] for _ in range(n_rows)]
        df = pd.DataFrame({
            'ts': pd.Series(_datetime_array(n_rows, 'ns')),
            'sym': pd.Series(pd.Categorical(
                choices, dtype=pd.CategoricalDtype(categories=pool))),
            'seq': pd.Series(np.arange(n_rows, dtype=np.int64)),
        })
        # Sanity: pandas should have picked an int16 code width.
        self.assertEqual(
            df['sym'].cat.codes.dtype, np.int16,
            'expected i16 code width for cardinality > 128')
        client = qi.Client.from_conf(self.conf)
        try:
            client.dataframe(df, table_name='hi_card_sym', at='ts')
        finally:
            client.close()
        stats = self.server.snapshot()
        self.assertEqual(stats['errors'], [])
        self.assertGreaterEqual(stats['binary_frames'], 1)

    def test_wide_frame_multi_chunk(self):
        """A frame with > 8 field columns hits the planner's
        ``rows_per_chunk = 64_000`` branch. Using 64 001 rows guarantees
        chunk-split through the wide-frame path (distinct from the
        Arrow-string and categorical-symbols caps exercised elsewhere)."""
        n_rows = 64_001
        n_int_cols = 12
        df_cols = {'ts': pd.Series(_datetime_array(n_rows, 'ns'))}
        seq = np.arange(n_rows, dtype=np.int64)
        for i in range(n_int_cols):
            df_cols[f'i{i:02d}'] = pd.Series(seq + i * 1_000_000)
        df = pd.DataFrame(df_cols)
        client = qi.Client.from_conf(self.conf)
        try:
            qi._debug_dataframe_columnar_io_stats(enabled=True, reset=True)
            try:
                client.dataframe(df, table_name='wide', at='ts',
                                 symbols=False)
            finally:
                io_stats = qi._debug_dataframe_columnar_io_stats(
                    enabled=False)
        finally:
            client.close()
        self.assertGreaterEqual(
            io_stats['flush_calls'], 2,
            f'expected >=2 flushes for wide-frame multi-chunk; '
            f'io_stats={io_stats}')
        self.assertEqual(io_stats['sync_calls'], 1)
        stats = self.server.snapshot()
        self.assertEqual(stats['errors'], [])
        self.assertGreaterEqual(stats['binary_frames'], 2)

    def test_sequential_client_lifecycle(self):
        """Open, use, and close a fresh Client many times in succession.
        Each cycle opens a new TCP connection (because the prior Client
        was closed); we verify that lifecycle is clean across repeated
        open/close cycles, no leaks, no server-side protocol errors."""
        n_cycles = 30
        rng = Rng(0x115ec_2_f0a_55_42)
        df = pd.DataFrame({
            'ts': pd.Series(_datetime_array(8, 'ns')),
            'seq': pd.Series(np.arange(8, dtype=np.int64)),
            's': pd.Series(_random_strings(rng, 8, 8, 0.0),
                           dtype='string[pyarrow]'),
        })
        for _ in range(n_cycles):
            client = qi.Client.from_conf(self.conf)
            try:
                client.dataframe(df, table_name='seq_lifecycle', at='ts',
                                 symbols=False)
            finally:
                client.close()
        stats = self.server.snapshot()
        self.assertEqual(
            stats['errors'], [],
            f'server saw protocol errors across {n_cycles} cycles: '
            f'{stats["errors"]}')
        self.assertEqual(
            stats['accepted_connections'], n_cycles,
            f'expected {n_cycles} accepts, saw '
            f'{stats["accepted_connections"]}')
        self.assertGreaterEqual(stats['binary_frames'], n_cycles)

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
        with self.assertRaises(qi.QuestDBError) as cm:
            qi.Client.from_conf('tcp::addr=localhost:9009;')
        self.assertEqual(cm.exception.code, qi.QuestDBErrorCode.ConfigError)

    def test_from_conf_requires_addr(self):
        with self.assertRaises(qi.QuestDBError) as cm:
            qi.Client.from_conf('ws::pool_size=1;')
        self.assertEqual(cm.exception.code, qi.QuestDBErrorCode.ConfigError)


# ---------------------------------------------------------------------------
# Round-trip fuzz against a real QuestDB. Gated on QDB_REPO_PATH, matching
# system_test.py's convention.
# ---------------------------------------------------------------------------


def _normalize_for_compare(df):
    """Project a DataFrame onto a representation that compares cleanly
    across the QuestDB round-trip.

    Drops the QuestDB-renamed designated-timestamp column (caller is
    expected to compare it separately if needed). Coerces categorical
    and any string-flavoured dtype to plain `object` strings. Sorts
    columns alphabetically.
    """
    df = df.copy()
    df = df.reindex(sorted(df.columns), axis=1)
    out = {}
    for col in df.columns:
        s = df[col]
        if (isinstance(s.dtype, pd.CategoricalDtype)
                or pd.api.types.is_string_dtype(s.dtype)):
            out[col] = s.astype('object')
        elif pd.api.types.is_datetime64_any_dtype(s.dtype):
            # Strip timezone (QuestDB always returns UTC; source may be
            # tz-naive) and normalise to microsecond resolution to match
            # QuestDB's TIMESTAMP precision on round-trip.
            v = s.dt.tz_convert(None) if s.dt.tz is not None else s
            out[col] = v.astype('datetime64[us]')
        else:
            out[col] = s
    return pd.DataFrame(out)


@unittest.skipUnless(
    os.environ.get('QDB_REPO_PATH') and pd is not None and pa is not None,
    'Round-trip fuzz needs a real QuestDB. Set QDB_REPO_PATH=<questdb checkout> '
    'to enable. Matches the gating convention in system_test.py.')
class TestClientDataframeRoundTrip(unittest.TestCase):
    """Ingest via Client.dataframe → real QuestDB → read back via
    Client.query → assert frame equivalence.

    Set ``QDB_REPO_PATH=/path/to/questdb`` to enable. Uses a class-scoped
    QuestDB fixture (one process; tables are dropped between iterations).
    """

    DEFAULT_ITERS = 8

    @classmethod
    def setUpClass(cls):
        # Import the heavy fixture infra only when this test class runs.
        import importlib
        cls._fixture_mod = importlib.import_module('fixture')
        repo = os.environ.get('QDB_REPO_PATH')
        if not repo:
            raise unittest.SkipTest(
                'QDB_REPO_PATH required for Layer-3 fuzz')
        install_path = cls._fixture_mod.install_questdb_from_repo(
            __import__('pathlib').Path(repo))
        import shutil
        plain_dir = PROJ_ROOT / 'build' / 'questdb' / 'layer3'
        plain_dir.mkdir(parents=True, exist_ok=True)
        shutil.copytree(install_path, plain_dir, dirs_exist_ok=True)
        cls.qdb = cls._fixture_mod.QuestDbFixture(
            plain_dir, auth=False, http=True)
        cls.qdb.start()

        cls.iter_seed_override = _parse_int_env(ITER_SEED_ENV)
        if cls.iter_seed_override is not None:
            cls.master_seed = None
            cls.iters = 1
        else:
            cls.master_seed = _derive_master_seed()
            cls.iters = _parse_int_env(ITERS_ENV) or cls.DEFAULT_ITERS
        sys.stderr.write(
            f'>>>> Round-trip fuzz vs real QuestDB: '
            f'master={_format_seed(cls.master_seed) if cls.master_seed else "n/a"}, '
            f'iter_override='
            f'{_format_seed(cls.iter_seed_override) if cls.iter_seed_override else "n/a"}, '
            f'iters={cls.iters}\n')

    @classmethod
    def tearDownClass(cls):
        if getattr(cls, 'qdb', None) is not None:
            cls.qdb.stop()

    @property
    def conf(self):
        return (f'ws::addr={self.qdb.host}:'
                f'{self.qdb.http_server_port};')

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

    # Round-trip generators avoid QuestDB's sentinel-value collisions:
    # INT64_MIN aliases LONG null, NaN aliases DOUBLE null. The fuzz
    # generators in this module deliberately sprinkle those values to
    # exercise the wire encoder; for the Layer-3 round-trip oracle
    # we need lossless inputs.
    @staticmethod
    def _gen_int64_safe(rng, n):
        out = np.empty(n, dtype=np.int64)
        for i in range(n):
            out[i] = int(rng.uniform(-(1 << 50), 1 << 50))
        return pd.Series(out)

    @staticmethod
    def _gen_float64_safe(rng, n):
        out = np.empty(n, dtype=np.float64)
        for i in range(n):
            out[i] = rng.uniform(-1e6, 1e6)
        return pd.Series(out)

    def _build_simple_frame(self, rng):
        """Hand-picked frame shapes for round-trip. Each is a
        type-coverage probe rather than a max-entropy fuzz; this
        keeps normalisation tractable for first-cut Layer-3."""
        n_rows = max(rng.choice(ROW_COUNT_CHOICES), 1)
        cols = {
            'ts': pd.Series(_datetime_array(n_rows, 'ns')),
            'id': pd.Series(np.arange(1, n_rows + 1, dtype=np.int64)),
        }
        shape = rng.choice(['numeric', 'string', 'categorical', 'mixed'])
        if shape in ('numeric', 'mixed'):
            cols['price'] = self._gen_float64_safe(rng, n_rows)
            cols['count'] = self._gen_int64_safe(rng, n_rows)
        if shape in ('string', 'mixed'):
            cols['note'] = pd.Series(
                _random_strings(rng, n_rows, 8, 0.0, ascii_only=True),
                dtype='string[pyarrow]')
        if shape in ('categorical', 'mixed'):
            cols['sym'] = _gen_categorical(rng, n_rows)
        return pd.DataFrame(cols), shape, n_rows

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
            table_name = f'rt_{iter_idx}_{iter_seed:016x}'
            try:
                df, shape, n_rows = self._build_simple_frame(rng)
                self._drop_table(table_name)
                with qi.Client.from_conf(self.conf) as client:
                    client.dataframe(df, table_name=table_name, at='ts')
                self._wait_for_rows(table_name, n_rows)

                # Read back. Project out 'ts' (renamed to 'timestamp')
                # so the comparison stays tractable.
                cols = [c for c in df.columns if c != 'ts']
                sql = (f"SELECT {','.join(cols)} FROM {table_name} "
                       f"ORDER BY id")
                with qi.Client.from_conf(self.conf) as client:
                    result = client.query(sql)
                    df_out = result.to_pandas()

                df_in_norm = _normalize_for_compare(
                    df[cols].sort_values('id').reset_index(drop=True))
                df_out_norm = _normalize_for_compare(
                    df_out.sort_values('id').reset_index(drop=True))
                pd.testing.assert_frame_equal(
                    df_in_norm, df_out_norm,
                    check_dtype=False, check_like=True)
            except Exception as exc:
                failures.append(
                    (iter_seed, shape if 'shape' in locals() else '?',
                     type(exc).__name__, repr(exc)))
                # Try to drop the table to keep iterations independent.
                self._drop_table(table_name)

        if failures:
            preview = '\n'.join(
                f'  iter={_format_seed(s)} shape={sh} [{cls}]: {m}'
                for s, sh, cls, m in failures[:5])
            self.fail(
                f'{len(failures)}/{len(seeds)} iterations failed.\n'
                f'(showing first 5)\n{preview}')

    def test_targeted_payload_semantics(self):
        table_name = f'rt_payload_{uuid.uuid4().hex[:8]}'
        ts_values = np.array([
            '2024-01-01T00:00:00.123456',
            '2024-01-01T00:00:01.654321',
            '2024-01-01T00:00:02.000000',
            '2024-01-01T00:00:03.999999',
        ], dtype='datetime64[us]')
        large_text_values = ['alpha', 'bravo', None, 'cafe']
        dict_text_values = ['EUR', 'USD', None, 'EUR']
        df = pd.DataFrame({
            'ts': pd.Series(ts_values),
            'seq': pd.Series([1, 2, 3, 4], dtype=np.int64),
            'large_text': pd.Series(
                pa.array(large_text_values, type=pa.large_string()),
                dtype=pd.ArrowDtype(pa.large_string())),
            'dict_text': pd.Series(
                pa.array(dict_text_values, type=pa.large_string()),
                dtype=pd.ArrowDtype(pa.large_string())).astype('category'),
        })

        try:
            self._drop_table(table_name)
            with qi.Client.from_conf(self.conf) as client:
                client.dataframe(df, table_name=table_name, at='ts')
            self._wait_for_rows(table_name, len(df))

            with qi.Client.from_conf(self.conf) as client:
                table = client.query(
                    f'SELECT timestamp, seq, large_text, dict_text '
                    f'FROM {table_name} ORDER BY seq').to_arrow()

            self.assertEqual(table.num_rows, len(df))
            actual_ts = table.column('timestamp').to_pandas()
            if actual_ts.dt.tz is not None:
                actual_ts = actual_ts.dt.tz_convert(None)
            expected_ts = pd.Series(ts_values)
            pd.testing.assert_series_equal(
                actual_ts.astype('datetime64[us]').reset_index(drop=True),
                expected_ts.astype('datetime64[us]'),
                check_names=False)
            self.assertEqual(
                table.column('large_text').to_pylist(),
                large_text_values)
            self.assertEqual(
                table.column('dict_text').to_pylist(),
                dict_text_values)
        finally:
            self._drop_table(table_name)

    def test_targeted_timestamp_units_round_trip(self):
        source_values = [
            '2024-01-01T00:00:00.000000',
            '2024-01-01T00:00:01.123000',
            '2024-01-01T00:00:02.456000',
        ]
        for unit in ('s', 'ms', 'us', 'ns'):
            table_name = f'rt_ts_{unit}_{uuid.uuid4().hex[:8]}'
            ts_values = np.array(source_values, dtype=f'datetime64[{unit}]')
            df = pd.DataFrame({
                'ts': pd.Series(ts_values),
                'seq': pd.Series([1, 2, 3], dtype=np.int64),
            })

            try:
                self._drop_table(table_name)
                with qi.Client.from_conf(self.conf) as client:
                    client.dataframe(df, table_name=table_name, at='ts')
                self._wait_for_rows(table_name, len(df))

                with qi.Client.from_conf(self.conf) as client:
                    table = client.query(
                        f'SELECT timestamp, seq FROM {table_name} '
                        f'ORDER BY seq').to_arrow()

                actual_ts = table.column('timestamp').to_pandas()
                if actual_ts.dt.tz is not None:
                    actual_ts = actual_ts.dt.tz_convert(None)
                expected_ts = pd.Series(ts_values).astype('datetime64[us]')
                pd.testing.assert_series_equal(
                    actual_ts.astype('datetime64[us]').reset_index(drop=True),
                    expected_ts.reset_index(drop=True),
                    check_names=False)
            finally:
                self._drop_table(table_name)

    def test_targeted_rust_arrow_classifier_numeric_round_trip(self):
        table_name = f'rt_arrow_numeric_{uuid.uuid4().hex[:8]}'
        ts_type = pa.timestamp('ms', tz='UTC')
        df = pd.DataFrame({
            'ts': pd.Series(
                pa.array(
                    [1704067200000, 1704067201000, 1704067202000],
                    type=ts_type),
                dtype=pd.ArrowDtype(ts_type)),
            'seq': pd.Series([1, 2, 3], dtype=np.int64),
            'u8': pd.Series(
                pa.array([1, 2, 255], type=pa.uint8()),
                dtype=pd.ArrowDtype(pa.uint8())),
            'u16': pd.Series(
                pa.array([1000, 2000, 3000], type=pa.uint16()),
                dtype=pd.ArrowDtype(pa.uint16())),
            'u64': pd.Series(
                pa.array([1, 2 ** 31, 2 ** 40], type=pa.uint64()),
                dtype=pd.ArrowDtype(pa.uint64())),
            'f16': pd.Series(
                pa.array(np.array([1.5, 2.5, 3.5], dtype=np.float16),
                         type=pa.float16()),
                dtype=pd.ArrowDtype(pa.float16())),
        })

        try:
            self._drop_table(table_name)
            with qi.Client.from_conf(self.conf) as client:
                client.dataframe(df, table_name=table_name, at='ts')
            self._wait_for_rows(table_name, len(df))

            with qi.Client.from_conf(self.conf) as client:
                table = client.query(
                    f'SELECT timestamp, seq, u8, u16, u64, f16 '
                    f'FROM {table_name} ORDER BY seq').to_arrow()

            actual_ts = table.column('timestamp').to_pandas()
            if actual_ts.dt.tz is not None:
                actual_ts = actual_ts.dt.tz_convert(None)
            expected_ts = pd.Series(
                ['2024-01-01T00:00:00.000000',
                 '2024-01-01T00:00:01.000000',
                 '2024-01-01T00:00:02.000000'],
                dtype='datetime64[us]')
            pd.testing.assert_series_equal(
                actual_ts.astype('datetime64[us]').reset_index(drop=True),
                expected_ts.reset_index(drop=True),
                check_names=False)
            self.assertEqual(table.column('u8').to_pylist(), [1, 2, 255])
            self.assertEqual(
                table.column('u16').to_pylist(), [1000, 2000, 3000])
            self.assertEqual(
                table.column('u64').to_pylist(), [1, 2 ** 31, 2 ** 40])
            np.testing.assert_allclose(
                np.array(table.column('f16').to_pylist(), dtype=np.float32),
                np.array([1.5, 2.5, 3.5], dtype=np.float32))
        finally:
            self._drop_table(table_name)


# Late imports for the round-trip class.
import time  # noqa: E402


if __name__ == '__main__':
    unittest.main()
