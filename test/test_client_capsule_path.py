#!/usr/bin/env python3
"""Smoke tests for the Arrow PyCapsule Interface dispatch path
(`__arrow_c_stream__`) used by polars / pyarrow / generic Arrow-native
DataFrame inputs to `Client.dataframe()`.
"""

import sys
sys.dont_write_bytecode = True
import datetime
import os
import unittest

import patch_path

PROJ_ROOT = patch_path.PROJ_ROOT
sys.path.append(str(PROJ_ROOT / 'c-questdb-client' / 'system_test'))

import questdb as qi
from qwp_ws_ack_server import QwpAckServer

try:
    import polars as pl
except ImportError:
    pl = None

try:
    import pyarrow as pa
except ImportError:
    pa = None


def _client_conf(port):
    return (
        f'qwpws::addr=127.0.0.1:{port};'
        'pool_size=1;'
        'pool_max=1;'
        'pool_reap=manual;')


def _ts_us(year, month, day, hour=0, minute=0, second=0):
    return int(datetime.datetime(
        year, month, day, hour, minute, second,
        tzinfo=datetime.timezone.utc).timestamp() * 1_000_000)


class TestCapsulePathPyArrow(unittest.TestCase):

    @unittest.skipIf(pa is None, 'pyarrow not installed')
    def test_pyarrow_table_designated_ts_column(self):
        schema = pa.schema([
            pa.field('symbol', pa.string()),
            pa.field('price', pa.float64()),
            pa.field('ts', pa.timestamp('us')),
        ])
        table = pa.Table.from_pydict({
            'symbol': ['ETH-USD', 'BTC-USD', 'ETH-USD'],
            'price':  [2615.54, 67234.12, 2620.88],
            'ts':     [_ts_us(2025, 1, 1, 12, 0, 0),
                       _ts_us(2025, 1, 1, 12, 0, 1),
                       _ts_us(2025, 1, 1, 12, 0, 2)],
        }, schema=schema)
        with QwpAckServer() as server:
            client = qi.Client.from_conf(_client_conf(server.port))
            try:
                client.dataframe(table, table_name='trades', at='ts')
            finally:
                client.close()
            stats = server.snapshot()
        self.assertEqual(stats['errors'], [])
        self.assertEqual(stats['accepted_connections'], 1)
        self.assertGreaterEqual(stats['qwp1_frames'], 1)

    @unittest.skipIf(pa is None, 'pyarrow not installed')
    def test_pyarrow_record_batch_via_table_from_batches(self):
        schema = pa.schema([
            pa.field('seq', pa.int64()),
            pa.field('ts', pa.timestamp('us')),
        ])
        batch = pa.RecordBatch.from_pydict({
            'seq': [1, 2],
            'ts':  [_ts_us(2025, 1, 1), _ts_us(2025, 1, 2)],
        }, schema=schema)
        table = pa.Table.from_batches([batch])
        with QwpAckServer() as server:
            client = qi.Client.from_conf(_client_conf(server.port))
            try:
                client.dataframe(table, table_name='seq_log', at='ts')
            finally:
                client.close()
            stats = server.snapshot()
        self.assertEqual(stats['errors'], [])

    @unittest.skipIf(pa is None, 'pyarrow not installed')
    def test_pyarrow_max_rows_per_batch_splits(self):
        n = 64
        schema = pa.schema([
            pa.field('v', pa.int64()),
            pa.field('ts', pa.timestamp('us')),
        ])
        table = pa.Table.from_pydict({
            'v':  list(range(n)),
            'ts': [_ts_us(2025, 1, 1) + i for i in range(n)],
        }, schema=schema)
        with QwpAckServer() as server:
            client = qi.Client.from_conf(_client_conf(server.port))
            try:
                client.dataframe(
                    table,
                    table_name='split',
                    at='ts',
                    max_rows_per_batch=16)
            finally:
                client.close()
            stats = server.snapshot()
        self.assertEqual(stats['errors'], [])
        self.assertGreaterEqual(stats['qwp1_frames'], 4)


class TestCapsulePathPolars(unittest.TestCase):

    @unittest.skipIf(pl is None, 'polars not installed')
    def test_polars_dataframe_designated_ts(self):
        df = pl.DataFrame({
            'symbol': ['ETH-USD', 'BTC-USD'],
            'price':  [2615.54, 67234.12],
            'ts': [
                datetime.datetime(2025, 1, 1, 12, 0, 0),
                datetime.datetime(2025, 1, 1, 12, 0, 1),
            ],
        }, schema={
            'symbol': pl.Utf8,
            'price':  pl.Float64,
            'ts':     pl.Datetime('us'),
        })
        with QwpAckServer() as server:
            client = qi.Client.from_conf(_client_conf(server.port))
            try:
                client.dataframe(df, table_name='trades', at='ts')
            finally:
                client.close()
            stats = server.snapshot()
        self.assertEqual(stats['errors'], [])

    @unittest.skipIf(pl is None, 'polars not installed')
    def test_polars_lazyframe_is_collected(self):
        lf = pl.LazyFrame({
            'v':  [1, 2, 3],
            'ts': [
                datetime.datetime(2025, 1, 1),
                datetime.datetime(2025, 1, 2),
                datetime.datetime(2025, 1, 3),
            ],
        }, schema={'v': pl.Int64, 'ts': pl.Datetime('us')})
        with QwpAckServer() as server:
            client = qi.Client.from_conf(_client_conf(server.port))
            try:
                client.dataframe(lf, table_name='lazy_t', at='ts')
            finally:
                client.close()
            stats = server.snapshot()
        self.assertEqual(stats['errors'], [])


class TestSchemaOverrides(unittest.TestCase):

    @unittest.skipIf(pl is None, 'polars not installed')
    def test_polars_schema_overrides_ipv4_no_pyarrow(self):
        if qi._debug_dataframe_pyarrow_loaded():
            self.skipTest(
                'an earlier test already lazy-loaded pyarrow inside this '
                'process; the no-pyarrow assertion only holds when this '
                'test runs first.')
        df = pl.DataFrame({
            'addr': pl.Series('addr', [0x0A000001, 0xC0A80101],
                              dtype=pl.UInt32),
            'ts':   pl.Series('ts', [
                datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc),
                datetime.datetime(2025, 1, 2, tzinfo=datetime.timezone.utc),
            ], dtype=pl.Datetime('us', time_zone='UTC')),
        })
        with QwpAckServer() as server:
            client = qi.Client.from_conf(_client_conf(server.port))
            try:
                client.dataframe(
                    df,
                    table_name='polars_ipv4',
                    at='ts',
                    schema_overrides={'addr': 'ipv4'})
            finally:
                client.close()
            stats = server.snapshot()
        self.assertEqual(stats['errors'], [])
        self.assertFalse(
            qi._debug_dataframe_pyarrow_loaded(),
            'polars + schema_overrides path must not lazy-import pyarrow')

    @unittest.skipIf(pl is None, 'polars not installed')
    def test_polars_schema_overrides_symbol_no_pyarrow(self):
        if qi._debug_dataframe_pyarrow_loaded():
            self.skipTest(
                'an earlier test already lazy-loaded pyarrow inside this '
                'process; the no-pyarrow assertion only holds when this '
                'test runs first.')
        df = pl.DataFrame({
            'region': pl.Series('region', ['us-east', 'us-west', 'us-east']),
            'price':  pl.Series('price', [1.0, 2.0, 3.0], dtype=pl.Float64),
            'ts':     pl.Series('ts', [
                datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc),
                datetime.datetime(2025, 1, 2, tzinfo=datetime.timezone.utc),
                datetime.datetime(2025, 1, 3, tzinfo=datetime.timezone.utc),
            ], dtype=pl.Datetime('us', time_zone='UTC')),
        })
        with QwpAckServer() as server:
            client = qi.Client.from_conf(_client_conf(server.port))
            try:
                client.dataframe(
                    df,
                    table_name='polars_symbol',
                    at='ts',
                    schema_overrides={'region': 'symbol'})
            finally:
                client.close()
            stats = server.snapshot()
        self.assertEqual(stats['errors'], [])
        self.assertFalse(
            qi._debug_dataframe_pyarrow_loaded(),
            'polars + schema_overrides path must not lazy-import pyarrow')

    def test_schema_overrides_rejects_decimal_kind(self):
        with self.assertRaisesRegex(ValueError, 'kind'):
            with QwpAckServer() as server:
                client = qi.Client.from_conf(_client_conf(server.port))
                try:
                    client.dataframe(
                        object(),
                        table_name='t',
                        at='ts',
                        schema_overrides={'x': ('decimal', 2)})
                finally:
                    client.close()

    @unittest.skipIf(pa is None, 'pyarrow not installed')
    def test_schema_overrides_ipv4(self):
        schema = pa.schema([
            pa.field('addr', pa.uint32()),
            pa.field('ts', pa.timestamp('us')),
        ])
        table = pa.Table.from_pydict({
            'addr': [0x0A000001, 0xC0A80101],
            'ts':   [_ts_us(2025, 1, 1), _ts_us(2025, 1, 2)],
        }, schema=schema)
        with QwpAckServer() as server:
            client = qi.Client.from_conf(_client_conf(server.port))
            try:
                client.dataframe(
                    table,
                    table_name='ipv4_log',
                    at='ts',
                    schema_overrides={'addr': 'ipv4'})
            finally:
                client.close()
            stats = server.snapshot()
        self.assertEqual(stats['errors'], [])

    @unittest.skipIf(pa is None, 'pyarrow not installed')
    def test_schema_overrides_rejects_unknown_kind(self):
        schema = pa.schema([
            pa.field('x', pa.int64()),
            pa.field('ts', pa.timestamp('us')),
        ])
        table = pa.Table.from_pydict({
            'x': [1, 2], 'ts': [_ts_us(2025, 1, 1), _ts_us(2025, 1, 2)],
        }, schema=schema)
        with QwpAckServer() as server:
            client = qi.Client.from_conf(_client_conf(server.port))
            try:
                with self.assertRaisesRegex(ValueError, 'kind'):
                    client.dataframe(
                        table,
                        table_name='t',
                        at='ts',
                        schema_overrides={'x': 'bogus'})
            finally:
                client.close()

    @unittest.skipIf(pa is None, 'pyarrow not installed')
    def test_schema_overrides_rejects_bad_geohash_bits(self):
        schema = pa.schema([
            pa.field('loc', pa.int32()),
            pa.field('ts', pa.timestamp('us')),
        ])
        table = pa.Table.from_pydict({
            'loc': [1, 2], 'ts': [_ts_us(2025, 1, 1), _ts_us(2025, 1, 2)],
        }, schema=schema)
        with QwpAckServer() as server:
            client = qi.Client.from_conf(_client_conf(server.port))
            try:
                with self.assertRaisesRegex(ValueError, 'geohash bits'):
                    client.dataframe(
                        table,
                        table_name='t',
                        at='ts',
                        schema_overrides={'loc': ('geohash', 0)})
            finally:
                client.close()


class TestPyArrowRecordBatchDirect(unittest.TestCase):

    @unittest.skipIf(pa is None, 'pyarrow not installed')
    def test_bare_record_batch_routes_via_table_wrap(self):
        schema = pa.schema([
            pa.field('v', pa.int64()),
            pa.field('ts', pa.timestamp('us')),
        ])
        batch = pa.RecordBatch.from_pydict({
            'v':  [1, 2, 3],
            'ts': [_ts_us(2025, 1, 1) + i for i in range(3)],
        }, schema=schema)
        with QwpAckServer() as server:
            client = qi.Client.from_conf(_client_conf(server.port))
            try:
                client.dataframe(batch, table_name='from_rb', at='ts')
            finally:
                client.close()
            stats = server.snapshot()
        self.assertEqual(stats['errors'], [])
        self.assertGreaterEqual(stats['qwp1_frames'], 1)


class TestSchemaOverridesPandas(unittest.TestCase):

    @unittest.skipIf(pa is None or pl is None, 'pyarrow + polars required')
    def test_pandas_dataframe_with_schema_overrides_ipv4(self):
        import pandas as pd
        df = pd.DataFrame({
            'addr': pd.Series([0x0A000001, 0xC0A80101], dtype='uint32'),
            'ts': pd.Series(
                pa.array([_ts_us(2025, 1, 1), _ts_us(2025, 1, 2)],
                         type=pa.timestamp('us')),
                dtype=pd.ArrowDtype(pa.timestamp('us'))),
        })
        with QwpAckServer() as server:
            client = qi.Client.from_conf(_client_conf(server.port))
            try:
                client.dataframe(
                    df,
                    table_name='ipv4_pandas',
                    at='ts',
                    schema_overrides={'addr': 'ipv4'})
            finally:
                client.close()
            stats = server.snapshot()
        self.assertEqual(stats['errors'], [])


class TestBenchFlushArrowBatch(unittest.TestCase):
    """Regression coverage equivalent to the old
    `_bench_dataframe_append_arrow_buffer` tests, migrated to the new
    `column_sender_flush_arrow_batch` path."""

    @unittest.skipIf(pa is None, 'pyarrow not installed')
    def test_uses_rust_classifier_accepts_uint_and_f16(self):
        import pandas as pd
        import numpy as np
        ts_type = pa.timestamp('ms', tz='UTC')
        df = pd.DataFrame({
            'ts': pd.Series(
                pa.array(
                    [1704067200000, 1704067201000, 1704067202000],
                    type=ts_type),
                dtype=pd.ArrowDtype(ts_type)),
            'u8': pd.Series(
                pa.array([1, 2, None], type=pa.uint8()),
                dtype=pd.ArrowDtype(pa.uint8())),
            'u16': pd.Series(
                pa.array([1000, None, 3000], type=pa.uint16()),
                dtype=pd.ArrowDtype(pa.uint16())),
            'u64': pd.Series(
                pa.array([1, 2 ** 63 - 1, None], type=pa.uint64()),
                dtype=pd.ArrowDtype(pa.uint64())),
            'f16': pd.Series(
                pa.array(np.array([1.5, 2.5, 3.5], dtype=np.float16),
                         type=pa.float16()),
                dtype=pd.ArrowDtype(pa.float16())),
        })
        batch = pa.RecordBatch.from_pandas(df, preserve_index=False)
        with QwpAckServer() as server:
            result = qi._bench_dataframe_flush_arrow_batch(
                batch,
                table_name='trades',
                at='ts',
                conf=_client_conf(server.port),
                iterations=2)
        self.assertEqual(result['iterations'], 2)
        self.assertEqual(result['row_count'], 3)
        self.assertEqual(result['col_count'], 5)
        self.assertEqual(result['completed'], 2)

    @unittest.skipIf(pa is None, 'pyarrow not installed')
    def test_rejects_uint64_above_i64_max(self):
        import pandas as pd
        ts_type = pa.timestamp('ms', tz='UTC')
        df = pd.DataFrame({
            'ts': pd.Series(
                pa.array([1704067200000, 1704067201000], type=ts_type),
                dtype=pd.ArrowDtype(ts_type)),
            'u64': pd.Series(
                pa.array([1, 2 ** 63], type=pa.uint64()),
                dtype=pd.ArrowDtype(pa.uint64())),
        })
        batch = pa.RecordBatch.from_pandas(df, preserve_index=False)
        with QwpAckServer() as server:
            with self.assertRaisesRegex(
                    qi.QuestDBError,
                    r'UInt64 value 9223372036854775808 .* does not fit QuestDB LONG'):
                qi._bench_dataframe_flush_arrow_batch(
                    batch,
                    table_name='trades',
                    at='ts',
                    conf=_client_conf(server.port),
                    iterations=1)


class TestCapsulePathPolarsMissing(unittest.TestCase):

    def test_non_polars_non_arrow_falls_through(self):
        """A bare object without `__arrow_c_stream__` and not polars / not
        pandas falls through the capsule + pyarrow paths and raises.
        """
        with QwpAckServer() as server:
            client = qi.Client.from_conf(_client_conf(server.port))
            try:
                with self.assertRaises((TypeError, qi.QuestDBError)):
                    client.dataframe(object(), table_name='t', at=None)
            finally:
                client.close()


class TestWriterMixingInOneChunk(unittest.TestCase):
    """Plan Q3: confirm a pandas DataFrame containing simultaneously a
    pyobj-sniffed column (string), an Arrow-backed narrow integer
    (i8_arrow), and a numpy-direct column (int64) all coexist in one
    `column_sender_chunk` and produce a valid wire frame."""

    @unittest.skipIf(pa is None, 'pyarrow not installed')
    def test_pyobj_str_arrow_i8_numpy_i64_mix(self):
        import pandas as pd
        import numpy as np
        df = pd.DataFrame({
            'name': pd.Series(['alpha', 'beta', None], dtype='object'),
            'rank': pd.Series(
                pa.array([1, -1, 7], type=pa.int8()),
                dtype=pd.ArrowDtype(pa.int8())),
            'qty':  pd.Series([100, 200, 300], dtype='int64'),
            'ts':   pd.Series([
                pd.Timestamp('2025-01-01 00:00:00'),
                pd.Timestamp('2025-01-01 00:00:01'),
                pd.Timestamp('2025-01-01 00:00:02')],
                dtype='datetime64[ns]'),
        })
        with QwpAckServer() as server:
            client = qi.Client.from_conf(_client_conf(server.port))
            try:
                client.dataframe(df, table_name='mixed', at='ts')
            finally:
                client.close()
            stats = server.snapshot()
        self.assertEqual(stats['errors'], [])
        self.assertGreaterEqual(stats['qwp1_frames'], 1)


class TestPandasPlannerRouting(unittest.TestCase):
    """Pandas object columns use the manual dataframe planner; Arrow-backed
    pandas columns can use the capsule route."""

    @unittest.skipIf(pa is None, 'pyarrow not installed')
    def test_arrow_backed_pandas_uses_capsule_without_overrides(self):
        import numpy as np
        import pandas as pd
        ts_type = pa.timestamp('ms', tz='UTC')
        df = pd.DataFrame({
            'ts': pd.Series(
                pa.array(
                    [1704067200000, 1704067201000, 1704067202000],
                    type=ts_type),
                dtype=pd.ArrowDtype(ts_type)),
            'u64': pd.Series(
                pa.array([1, 2 ** 63 - 1, None], type=pa.uint64()),
                dtype=pd.ArrowDtype(pa.uint64())),
            'f16': pd.Series(
                pa.array(np.array([1.5, 2.5, 3.5], dtype=np.float16),
                         type=pa.float16()),
                dtype=pd.ArrowDtype(pa.float16())),
        })
        with QwpAckServer() as server:
            client = qi.Client.from_conf(_client_conf(server.port))
            try:
                client.dataframe(df, table_name='arrow_pandas', at='ts')
            finally:
                client.close()
            stats = server.snapshot()
        self.assertEqual(stats['errors'], [])
        self.assertEqual(stats['accepted_connections'], 1)
        self.assertGreaterEqual(stats['qwp1_frames'], 1)

    @unittest.skipIf(pa is None, 'pyarrow not installed')
    def test_arrow_backed_pandas_symbol_override_uses_capsule(self):
        import pandas as pd
        ts_type = pa.timestamp('us', tz='UTC')
        df = pd.DataFrame({
            'ts': pd.Series(
                pa.array([1704067200000000, 1704067201000000],
                         type=ts_type),
                dtype=pd.ArrowDtype(ts_type)),
            'region': pd.array(
                pa.array(['us-east', 'us-west'], type=pa.string()),
                dtype=pd.ArrowDtype(pa.string())),
            'v': pd.Series(
                pa.array([1, 2], type=pa.int64()),
                dtype=pd.ArrowDtype(pa.int64())),
        })
        with QwpAckServer() as server:
            client = qi.Client.from_conf(_client_conf(server.port))
            try:
                client.dataframe(
                    df, table_name='arrow_pandas_symbols',
                    at='ts', symbols=['region'])
            finally:
                client.close()
            stats = server.snapshot()
        self.assertEqual(stats['errors'], [])
        self.assertEqual(stats['accepted_connections'], 1)
        self.assertGreaterEqual(stats['qwp1_frames'], 1)

    @unittest.skipIf(pa is None, 'pyarrow not installed')
    def test_mixed_arrow_numpy_symbol_override_uses_manual(self):
        # A numpy column routes the whole frame to the manual planner; the
        # Arrow string column overridden to SYMBOL is ingested via the
        # arrow-import symbol path (force_symbol).
        import pandas as pd
        ts_type = pa.timestamp('us', tz='UTC')
        df = pd.DataFrame({
            'ts': pd.Series(
                pa.array([1704067200000000, 1704067201000000],
                         type=ts_type),
                dtype=pd.ArrowDtype(ts_type)),
            'region': pd.array(
                pa.array(['us-east', 'us-west'], type=pa.string()),
                dtype=pd.ArrowDtype(pa.string())),
            'v': pd.Series([1, 2], dtype='int64'),
        })
        with QwpAckServer() as server:
            client = qi.Client.from_conf(_client_conf(server.port))
            try:
                client.dataframe(
                    df, table_name='mixed_arrow_symbols',
                    at='ts', symbols=['region'])
            finally:
                client.close()
            stats = server.snapshot()
        self.assertEqual(stats['errors'], [])
        self.assertEqual(stats['accepted_connections'], 1)
        self.assertGreaterEqual(stats['qwp1_frames'], 1)

    @unittest.skipIf(pa is None, 'pyarrow not installed')
    def test_pyobj_str_bad_cell_fails_before_borrowing_conn(self):
        import pandas as pd
        df_bad = pd.DataFrame({
            'name': pd.Series(['alpha', 12345, None], dtype='object'),
            'ts': pd.Series([
                pd.Timestamp('2025-01-01 00:00:00'),
                pd.Timestamp('2025-01-01 00:00:01'),
                pd.Timestamp('2025-01-01 00:00:02')],
                dtype='datetime64[ns]'),
        })
        df_good = pd.DataFrame({
            'name': pd.Series(['x', 'y'], dtype='object'),
            'ts': pd.Series([
                pd.Timestamp('2025-01-02 00:00:00'),
                pd.Timestamp('2025-01-02 00:00:01')],
                dtype='datetime64[ns]'),
        })
        with QwpAckServer() as server:
            client = qi.Client.from_conf(_client_conf(server.port))
            try:
                with self.assertRaises(qi.QuestDBError):
                    client.dataframe(df_bad, table_name='t', at='ts')
                client.dataframe(df_good, table_name='t', at='ts')
            finally:
                client.close()
            stats = server.snapshot()
        # The bad pandas frame fails during manual-plan validation before a
        # connection is borrowed. The good frame is the only publish.
        self.assertEqual(stats['errors'], [])
        self.assertEqual(stats['accepted_connections'], 1)
        self.assertGreaterEqual(stats['qwp1_frames'], 1)


if __name__ == '__main__':
    unittest.main()
