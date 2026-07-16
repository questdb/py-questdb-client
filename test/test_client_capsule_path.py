#!/usr/bin/env python3
"""Smoke tests for the Arrow PyCapsule Interface dispatch path
(`__arrow_c_stream__`) used by polars / pyarrow / generic Arrow-native
DataFrame inputs to `QuestDB.dataframe()`.
"""

import sys
sys.dont_write_bytecode = True
import datetime
import os
import struct
import unittest

import patch_path

PROJ_ROOT = patch_path.PROJ_ROOT
sys.path.append(str(PROJ_ROOT / 'c-questdb-client' / 'system_test'))

import questdb._client as qi
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
        f'ws::addr=127.0.0.1:{port};'
        'sender_pool_min=1;'
        'sender_pool_max=1;'
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
            client = qi.QuestDB.from_conf(_client_conf(server.port))
            try:
                client.dataframe(table, table_name='trades', at='ts')
            finally:
                client.close()
            stats = server.snapshot()
        self.assertEqual(stats['errors'], [])
        self.assertEqual(stats['accepted_connections'], 1)
        self.assertGreaterEqual(stats['qwp1_frames'], 1)

    @unittest.skipIf(pa is None, 'pyarrow not installed')
    def test_pyarrow_designated_ts_by_int_index(self):
        schema = pa.schema([
            pa.field('symbol', pa.string()),
            pa.field('price', pa.float64()),
            pa.field('ts', pa.timestamp('us')),
        ])
        table = pa.Table.from_pydict({
            'symbol': ['ETH-USD', 'BTC-USD'],
            'price':  [2615.54, 67234.12],
            'ts':     [_ts_us(2025, 1, 1, 12, 0, 0),
                       _ts_us(2025, 1, 1, 12, 0, 1)],
        }, schema=schema)
        for at in (2, -1):
            with QwpAckServer() as server:
                client = qi.QuestDB.from_conf(_client_conf(server.port))
                try:
                    client.dataframe(table, table_name='trades', at=at)
                finally:
                    client.close()
                stats = server.snapshot()
            self.assertEqual(stats['errors'], [])
            self.assertGreaterEqual(stats['qwp1_frames'], 1)

    @unittest.skipIf(pa is None, 'pyarrow not installed')
    def test_pyarrow_at_int_index_out_of_range(self):
        schema = pa.schema([
            pa.field('v', pa.int64()),
            pa.field('ts', pa.timestamp('us')),
        ])
        table = pa.Table.from_pydict({
            'v':  [1],
            'ts': [_ts_us(2025, 1, 1)],
        }, schema=schema)
        with QwpAckServer() as server:
            client = qi.QuestDB.from_conf(_client_conf(server.port))
            try:
                with self.assertRaisesRegex(IndexError, 'index out of range'):
                    client.dataframe(table, table_name='oob', at=5)
            finally:
                client.close()

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
            client = qi.QuestDB.from_conf(_client_conf(server.port))
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
            client = qi.QuestDB.from_conf(_client_conf(server.port))
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
            client = qi.QuestDB.from_conf(_client_conf(server.port))
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
            client = qi.QuestDB.from_conf(_client_conf(server.port))
            try:
                client.dataframe(lf, table_name='lazy_t', at='ts')
            finally:
                client.close()
            stats = server.snapshot()
        self.assertEqual(stats['errors'], [])


class TestServerTimestampAt(unittest.TestCase):
    """`at=ServerTimestamp` on the Arrow batch route: the frame carries no
    designated timestamp column and the server stamps each row on arrival
    (`flush_arrow_batch_at_now`). An explicit opt-in mirroring the row API."""

    @unittest.skipIf(pl is None, 'polars not installed')
    def test_polars_server_timestamp(self):
        df = pl.DataFrame({
            'symbol': ['ETH-USD', 'BTC-USD'],
            'price':  [2615.54, 67234.12],
        }, schema={'symbol': pl.Utf8, 'price': pl.Float64})
        with QwpAckServer() as server:
            client = qi.QuestDB.from_conf(_client_conf(server.port))
            try:
                client.dataframe(
                    df, table_name='trades', at=qi.ServerTimestamp)
            finally:
                client.close()
            stats = server.snapshot()
        self.assertEqual(stats['errors'], [])
        self.assertGreaterEqual(stats['qwp1_frames'], 1)

    @unittest.skipIf(pa is None, 'pyarrow not installed')
    def test_pyarrow_server_timestamp(self):
        table = pa.table({
            'symbol': ['ETH-USD', 'BTC-USD'],
            'price':  [2615.54, 67234.12],
        })
        with QwpAckServer() as server:
            client = qi.QuestDB.from_conf(_client_conf(server.port))
            try:
                client.dataframe(
                    table, table_name='trades', at=qi.ServerTimestamp)
            finally:
                client.close()
            stats = server.snapshot()
        self.assertEqual(stats['errors'], [])
        self.assertGreaterEqual(stats['qwp1_frames'], 1)

    @unittest.skipIf(pl is None, 'polars not installed')
    def test_polars_ts_column_stays_field_with_server_timestamp(self):
        # A timestamp column in the frame is an ordinary field when the
        # caller opts into server stamping; it must not be promoted to
        # the designated timestamp.
        df = pl.DataFrame({
            'v': [1, 2],
            'ts': [
                datetime.datetime(2025, 1, 1),
                datetime.datetime(2025, 1, 2),
            ],
        }, schema={'v': pl.Int64, 'ts': pl.Datetime('us')})
        with QwpAckServer() as server:
            client = qi.QuestDB.from_conf(_client_conf(server.port))
            try:
                client.dataframe(
                    df, table_name='t', at=qi.ServerTimestamp)
            finally:
                client.close()
            stats = server.snapshot()
        self.assertEqual(stats['errors'], [])
        self.assertGreaterEqual(stats['qwp1_frames'], 1)

    @unittest.skipIf(pl is None, 'polars not installed')
    def test_empty_frame_server_timestamp_is_noop(self):
        df = pl.DataFrame({
            'symbol': pl.Series([], dtype=pl.Utf8),
            'price': pl.Series([], dtype=pl.Float64),
        })
        with QwpAckServer() as server:
            client = qi.QuestDB.from_conf(_client_conf(server.port))
            try:
                client.dataframe(
                    df, table_name='t', at=qi.ServerTimestamp)
            finally:
                client.close()
            stats = server.snapshot()
        self.assertEqual(stats['errors'], [])
        self.assertEqual(stats['binary_frames'], 0)

    def test_numpy_pandas_server_timestamp(self):
        # NumPy-backed pandas routes through the chunk planner, whose
        # encoder supports server stamping via the chunk at_now opt-in.
        import pandas as pd
        df = pd.DataFrame({'sym': ['a', 'b'], 'x': [1, 2]})
        with QwpAckServer() as server:
            client = qi.QuestDB.from_conf(_client_conf(server.port))
            try:
                client.dataframe(
                    df, table_name='t', at=qi.ServerTimestamp)
            finally:
                client.close()
            stats = server.snapshot()
        self.assertEqual(stats['errors'], [])
        self.assertGreaterEqual(stats['qwp1_frames'], 1)

    def test_numpy_pandas_server_timestamp_multi_chunk(self):
        # Server stamping must survive the max_rows_per_batch split: every
        # chunk carries the at_now opt-in, none carries a ts column.
        import pandas as pd
        n = 64
        df = pd.DataFrame({'sym': ['s'] * n, 'x': list(range(n))})
        with QwpAckServer() as server:
            client = qi.QuestDB.from_conf(_client_conf(server.port))
            try:
                client.dataframe(
                    df, table_name='t', at=qi.ServerTimestamp,
                    max_rows_per_batch=16)
            finally:
                client.close()
            stats = server.snapshot()
        self.assertEqual(stats['errors'], [])
        self.assertGreaterEqual(stats['qwp1_frames'], 4)

    @unittest.skipIf(pa is None, 'pyarrow not installed')
    def test_arrow_backed_pandas_server_timestamp(self):
        import pandas as pd
        df = pd.DataFrame({'sym': ['a', 'b'], 'x': [1, 2]}).convert_dtypes(
            dtype_backend='pyarrow')
        with QwpAckServer() as server:
            client = qi.QuestDB.from_conf(_client_conf(server.port))
            try:
                client.dataframe(
                    df, table_name='t', at=qi.ServerTimestamp)
            finally:
                client.close()
            stats = server.snapshot()
        self.assertEqual(stats['errors'], [])
        self.assertGreaterEqual(stats['qwp1_frames'], 1)

    @unittest.skipIf(pl is None, 'polars not installed')
    def test_none_at_rejected_and_mentions_sentinel(self):
        df = pl.DataFrame({'v': [1]}, schema={'v': pl.Int64})
        with QwpAckServer() as server:
            client = qi.QuestDB.from_conf(_client_conf(server.port))
            try:
                with self.assertRaisesRegex(
                        qi.QuestDBError,
                        r'`ServerTimestamp` sentinel') as raised:
                    client.dataframe(df, table_name='t', at=None)
                self.assertEqual(
                    raised.exception.code,
                    qi.QuestDBErrorCode.InvalidTimestamp)
            finally:
                client.close()


class TestScalarAt(unittest.TestCase):
    """`at=TimestampNanos(...)` / `at=datetime(...)`: one fixed designated
    timestamp shared by every row, encoded as a repeated constant on both
    the Arrow batch route and the numpy chunk route."""

    AT_NANOS = 1_700_000_000_123_456_789

    def _ingest(self, df, at, **kw):
        with QwpAckServer() as server:
            client = qi.QuestDB.from_conf(_client_conf(server.port))
            try:
                client.dataframe(df, table_name='t', at=at, **kw)
            finally:
                client.close()
            return server.snapshot()

    @unittest.skipIf(pl is None, 'polars not installed')
    def test_polars_scalar_at(self):
        for at in (qi.TimestampNanos(self.AT_NANOS),
                   datetime.datetime(2024, 1, 1,
                                     tzinfo=datetime.timezone.utc)):
            stats = self._ingest(
                pl.DataFrame({'v': [1, 2]}, schema={'v': pl.Int64}), at)
            self.assertEqual(stats['errors'], [])
            self.assertGreaterEqual(stats['qwp1_frames'], 1)

    @unittest.skipIf(pa is None, 'pyarrow not installed')
    def test_pyarrow_scalar_at(self):
        stats = self._ingest(
            pa.table({'v': [1, 2]}), qi.TimestampNanos(self.AT_NANOS))
        self.assertEqual(stats['errors'], [])
        self.assertGreaterEqual(stats['qwp1_frames'], 1)

    def test_numpy_pandas_scalar_at(self):
        import pandas as pd
        for at in (qi.TimestampNanos(self.AT_NANOS),
                   datetime.datetime(2024, 1, 1,
                                     tzinfo=datetime.timezone.utc)):
            stats = self._ingest(pd.DataFrame({'v': [1, 2]}), at)
            self.assertEqual(stats['errors'], [])
            self.assertGreaterEqual(stats['qwp1_frames'], 1)

    def test_numpy_pandas_scalar_at_multi_chunk(self):
        import pandas as pd
        stats = self._ingest(
            pd.DataFrame({'v': list(range(64))}),
            qi.TimestampNanos(self.AT_NANOS), max_rows_per_batch=16)
        self.assertEqual(stats['errors'], [])
        self.assertGreaterEqual(stats['qwp1_frames'], 4)

    def _ingest_recorded(self, df, at):
        with QwpAckServer(record_payloads=True) as server:
            client = qi.QuestDB.from_conf(_client_conf(server.port))
            try:
                client.dataframe(df, table_name='t', at=at)
            finally:
                client.close()
            server.wait_binary_frames_settled()
            stats = server.snapshot()
        self.assertEqual(stats['errors'], [])
        return b''.join(stats['binary_payloads'])

    @unittest.skipIf(pa is None, 'pyarrow not installed')
    def test_scalar_at_nanos_on_wire(self):
        # The sentinel's little-endian int64 nanos must land in the frame
        # bytes when `at` is a fixed timestamp, and must be absent when
        # the server assigns each row's timestamp. Covers the Arrow batch
        # route (pyarrow table) and the numpy chunk route (pandas frame).
        import pandas as pd
        sentinel = struct.pack('<q', self.AT_NANOS)
        for df in (pa.table({'v': [1, 2]}),
                   pd.DataFrame({'v': [1, 2]})):
            scalar_bytes = self._ingest_recorded(
                df, qi.TimestampNanos(self.AT_NANOS))
            server_bytes = self._ingest_recorded(df, qi.ServerTimestamp)
            self.assertIn(sentinel, scalar_bytes)
            self.assertNotIn(sentinel, server_bytes)

    @unittest.skipIf(pl is None, 'polars not installed')
    def test_pre_epoch_datetime_rejected(self):
        with QwpAckServer() as server:
            client = qi.QuestDB.from_conf(_client_conf(server.port))
            try:
                with self.assertRaisesRegex(
                        ValueError, 'before the Unix epoch'):
                    client.dataframe(
                        pl.DataFrame({'v': [1]}, schema={'v': pl.Int64}),
                        table_name='t',
                        at=datetime.datetime(
                            1969, 1, 1, tzinfo=datetime.timezone.utc))
            finally:
                client.close()
            self.assertEqual(server.snapshot()['binary_frames'], 0)

    @unittest.skipIf(pl is None, 'polars not installed')
    def test_empty_frame_scalar_at_is_noop(self):
        stats = self._ingest(
            pl.DataFrame({'v': pl.Series([], dtype=pl.Int64)}),
            qi.TimestampNanos(self.AT_NANOS))
        self.assertEqual(stats['errors'], [])
        self.assertEqual(stats['binary_frames'], 0)


class TestNdarrayArrayColumns(unittest.TestCase):
    """Object-dtype columns of float64 numpy-array cells land as QuestDB
    ARRAY(DOUBLE) through the columnar manual-planner route (promoted to
    Arrow list<double> and shipped via the Rust Arrow importer)."""

    def _ingest(self, df, **kw):
        with QwpAckServer() as server:
            client = qi.QuestDB.from_conf(_client_conf(server.port))
            try:
                client.dataframe(df, table_name='t', at='ts', **kw)
            finally:
                client.close()
            return server.snapshot()

    @staticmethod
    def _df(cells):
        import numpy as np
        import pandas as pd
        return pd.DataFrame({
            'ts': pd.date_range('2024-01-01', periods=len(cells), freq='s'),
            'vec': pd.Series(cells, dtype=object),
        })

    def test_1d_uniform_ragged_empty_noncontiguous(self):
        import numpy as np
        cells = [
            np.array([1.5, 2.5, 3.5]),
            np.array([4.5]),
            np.array([], dtype=np.float64),
            np.arange(10.0)[::2],
        ]
        stats = self._ingest(self._df(cells))
        self.assertEqual(stats['errors'], [])
        self.assertGreaterEqual(stats['qwp1_frames'], 1)

    def test_2d_cells(self):
        import numpy as np
        stats = self._ingest(
            self._df([np.ones((2, 3)), np.zeros((3, 2))]))
        self.assertEqual(stats['errors'], [])
        self.assertGreaterEqual(stats['qwp1_frames'], 1)

    def test_none_cell_is_null_row(self):
        import numpy as np
        stats = self._ingest(self._df([np.array([1.0]), None]))
        self.assertEqual(stats['errors'], [])
        self.assertGreaterEqual(stats['qwp1_frames'], 1)

    def test_multi_chunk_split(self):
        import numpy as np
        cells = [np.array([float(i)]) for i in range(64)]
        stats = self._ingest(self._df(cells), max_rows_per_batch=16)
        self.assertEqual(stats['errors'], [])
        self.assertGreaterEqual(stats['qwp1_frames'], 4)

    def test_mixed_cell_types_rejected(self):
        import numpy as np
        with QwpAckServer() as server:
            client = qi.QuestDB.from_conf(_client_conf(server.port))
            try:
                with self.assertRaisesRegex(
                        qi.QuestDBError, 'mixed object cells'):
                    client.dataframe(
                        self._df([np.array([1.0]), 'oops']),
                        table_name='t', at='ts')
            finally:
                client.close()
            self.assertEqual(server.snapshot()['binary_frames'], 0)


@unittest.skipUnless(
    os.environ.get('QDB_HTTP_ADDR'),
    'set QDB_HTTP_ADDR=host:port for a running QuestDB')
class TestServerInfoLive(unittest.TestCase):

    def test_server_info_snapshot(self):
        import time
        addr = os.environ['QDB_HTTP_ADDR']
        with qi.QuestDB.from_conf(f'ws::addr={addr};') as client:
            info = client.server_info()
            self.assertIsInstance(info, qi.ServerInfo)
            self.assertIsInstance(info.role, qi.ServerRole)
            self.assertEqual(info.role_byte, info.role.c_value if
                             info.role is not qi.ServerRole.Other
                             else info.role_byte)
            self.assertGreaterEqual(info.epoch, 0)
            self.assertIsInstance(info.cluster_id, str)
            self.assertIsInstance(info.node_id, str)
            # Handshake wall-clock within 5 minutes of local clock.
            self.assertLess(
                abs(time.time_ns() - info.server_wall_ns), 300 * 10**9)
            # zone_id is None unless the server advertises CAP_ZONE.
            self.assertTrue(info.zone_id is None
                            or isinstance(info.zone_id, str))
            # Second snapshot reuses the pooled connection.
            info2 = client.server_info()
            self.assertEqual(info2.cluster_id, info.cluster_id)


class TestConnectionEvents(unittest.TestCase):
    """Connection lifecycle narration: `connection_listener` receives
    ConnectionEvents on a dedicated dispatcher thread."""

    @staticmethod
    def _collect():
        import threading
        events = []
        lock = threading.Lock()

        def listener(event):
            with lock:
                events.append(event)
        return events, listener

    @staticmethod
    def _wait_for(events, kinds, timeout=5.0):
        import time
        deadline = time.time() + timeout
        while time.time() < deadline:
            seen = {e.kind for e in events}
            if all(k in seen for k in kinds):
                return True
            time.sleep(0.01)
        return False

    @unittest.skipIf(pl is None, 'polars not installed')
    def test_connected_fires_with_endpoint_and_counters(self):
        events, listener = self._collect()
        with QwpAckServer() as server:
            client = qi.QuestDB.from_conf(
                _client_conf(server.port), connection_listener=listener)
            try:
                client.dataframe(
                    pl.DataFrame({'v': [1]}, schema={'v': pl.Int64}),
                    table_name='t', at=qi.ServerTimestamp)
                self.assertTrue(
                    self._wait_for(
                        events, [qi.ConnectionEventKind.Connected]),
                    [e.kind for e in events])
                connected = [
                    e for e in events
                    if e.kind is qi.ConnectionEventKind.Connected][0]
                self.assertEqual(connected.host, '127.0.0.1')
                self.assertEqual(connected.port, str(server.port))
                self.assertIsNone(connected.cause_code)
                self.assertGreater(connected.timestamp_millis, 0)
                self.assertGreaterEqual(
                    client.connection_events_delivered, 1)
                self.assertEqual(client.connection_events_dropped, 0)
            finally:
                client.close()

    def test_unreachable_fires_attempt_failed_and_unreachable(self):
        import socket
        events, listener = self._collect()
        blk = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        blk.bind(('127.0.0.1', 0))
        port = blk.getsockname()[1]
        blk.close()
        client = qi.QuestDB.from_conf(
            f'ws::addr=127.0.0.1:{port};connect_timeout=100;'
            f'reconnect_max_duration_millis=200;sender_pool_min=1;sender_pool_max=1;',
            connection_listener=listener)
        try:
            import pandas as pd
            with self.assertRaises(qi.QuestDBError):
                # Reader-pool paths (e.g. server_info/query) are not yet
                # instrumented; ingest exercises the instrumented ingress
                # pool walk.
                client.dataframe(
                    pd.DataFrame({'v': [1]}), table_name='t',
                    at=qi.ServerTimestamp)
            self.assertTrue(
                self._wait_for(events, [
                    qi.ConnectionEventKind.EndpointAttemptFailed,
                    qi.ConnectionEventKind.AllEndpointsUnreachable]),
                [e.kind for e in events])
            attempt = [
                e for e in events
                if e.kind is qi.ConnectionEventKind.EndpointAttemptFailed][0]
            self.assertEqual(attempt.host, '127.0.0.1')
            self.assertIsNotNone(attempt.attempt_number)
            self.assertIsNotNone(attempt.cause_code)
        finally:
            client.close()

    @unittest.skipIf(pl is None, 'polars not installed')
    def test_failover_fires_failed_over_with_previous_endpoint(self):
        import time
        events, listener = self._collect()
        server_a = QwpAckServer()
        server_a.start()
        server_b = QwpAckServer()
        server_b.start()
        try:
            client = qi.QuestDB.from_conf(
                f'ws::addr=127.0.0.1:{server_a.port},'
                f'127.0.0.1:{server_b.port};'
                f'connect_timeout=200;sender_pool_min=1;sender_pool_max=1;'
                f'pool_reap=manual;',
                connection_listener=listener)
            try:
                df = pl.DataFrame({'v': [1]}, schema={'v': pl.Int64})
                client.dataframe(
                    df, table_name='t', at=qi.ServerTimestamp)
                self.assertTrue(self._wait_for(
                    events, [qi.ConnectionEventKind.Connected]))
                first_port = [
                    e for e in events
                    if e.kind is qi.ConnectionEventKind.Connected][0].port
                if first_port == str(server_a.port):
                    server_a.stop()
                else:
                    server_b.stop()
                deadline = time.time() + 5
                while time.time() < deadline and not any(
                        e.kind is qi.ConnectionEventKind.FailedOver
                        for e in events):
                    try:
                        client.dataframe(
                            df, table_name='t', at=qi.ServerTimestamp)
                    except qi.QuestDBError:
                        pass
                    time.sleep(0.05)
                failed_over = [
                    e for e in events
                    if e.kind is qi.ConnectionEventKind.FailedOver]
                self.assertTrue(
                    failed_over, [e.kind for e in events])
                self.assertEqual(
                    failed_over[0].previous_port, first_port)
            finally:
                client.close()
        finally:
            server_a.stop()
            server_b.stop()

    def test_listener_must_be_callable(self):
        with self.assertRaisesRegex(TypeError, 'must be callable'):
            qi.QuestDB.from_conf(
                'ws::addr=127.0.0.1:9000;',
                connection_listener='not-callable')

    @unittest.skipIf(pl is None, 'polars not installed')
    def test_listener_exception_is_swallowed(self):
        def bad_listener(event):
            raise RuntimeError('listener bug')
        with QwpAckServer() as server:
            client = qi.QuestDB.from_conf(
                _client_conf(server.port), connection_listener=bad_listener)
            try:
                client.dataframe(
                    pl.DataFrame({'v': [1]}, schema={'v': pl.Int64}),
                    table_name='t', at=qi.ServerTimestamp)
                import time
                deadline = time.time() + 5
                while (time.time() < deadline
                        and client.connection_events_delivered < 1):
                    time.sleep(0.01)
                self.assertGreaterEqual(
                    client.connection_events_delivered, 1)
            finally:
                client.close()


class TestSenderConnectionEvents(unittest.TestCase):
    """Sender-level connection narration: `connection_listener` on
    Sender.from_conf, mirroring Java's builder.connectionListener."""

    def test_establish_fires_connected_and_counters(self):
        import time
        events = []
        with QwpAckServer() as server:
            with qi.Sender.from_conf(
                    f'ws::addr=127.0.0.1:{server.port};',
                    connection_listener=events.append) as sender:
                deadline = time.time() + 5
                while time.time() < deadline and not events:
                    time.sleep(0.01)
                self.assertTrue(events)
                self.assertIs(events[0].kind,
                              qi.ConnectionEventKind.Connected)
                self.assertEqual(events[0].host, '127.0.0.1')
                self.assertEqual(events[0].port, str(server.port))
                self.assertGreaterEqual(
                    sender.connection_events_delivered, 1)
                self.assertEqual(sender.connection_events_dropped, 0)

    def test_unreachable_establish_fires_failure_events(self):
        import socket
        import time
        blk = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        blk.bind(('127.0.0.1', 0))
        port = blk.getsockname()[1]
        blk.close()
        events = []
        sender = qi.Sender.from_conf(
            f'ws::addr=127.0.0.1:{port};connect_timeout=100;'
            f'reconnect_max_duration_millis=200;',
            connection_listener=events.append)
        with self.assertRaises(qi.QuestDBError):
            sender.establish()
        deadline = time.time() + 5
        while time.time() < deadline and not any(
                e.kind is qi.ConnectionEventKind.AllEndpointsUnreachable
                for e in events):
            time.sleep(0.01)
        kinds = {e.kind for e in events}
        self.assertIn(qi.ConnectionEventKind.EndpointAttemptFailed, kinds)
        self.assertIn(qi.ConnectionEventKind.AllEndpointsUnreachable, kinds)
        attempt = [e for e in events if e.kind is
                   qi.ConnectionEventKind.EndpointAttemptFailed][0]
        self.assertIsNotNone(attempt.attempt_number)
        self.assertIsNotNone(attempt.cause_code)

    def test_non_ws_sender_listener_rejected(self):
        with self.assertRaisesRegex(
                qi.QuestDBError, 'only supported for QWP/WebSocket'):
            qi.Sender.from_conf(
                'http::addr=127.0.0.1:9000;',
                connection_listener=lambda event: None)

    def test_non_callable_listener_rejected(self):
        with self.assertRaisesRegex(TypeError, 'must be callable'):
            qi.Sender.from_conf(
                'ws::addr=127.0.0.1:9000;',
                connection_listener='nope')


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
            client = qi.QuestDB.from_conf(_client_conf(server.port))
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
            client = qi.QuestDB.from_conf(_client_conf(server.port))
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
                client = qi.QuestDB.from_conf(_client_conf(server.port))
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
            client = qi.QuestDB.from_conf(_client_conf(server.port))
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
            client = qi.QuestDB.from_conf(_client_conf(server.port))
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
            client = qi.QuestDB.from_conf(_client_conf(server.port))
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
            client = qi.QuestDB.from_conf(_client_conf(server.port))
            try:
                client.dataframe(batch, table_name='from_rb', at='ts')
            finally:
                client.close()
            stats = server.snapshot()
        self.assertEqual(stats['errors'], [])
        self.assertGreaterEqual(stats['qwp1_frames'], 1)


class TestSchemaOverridesPandas(unittest.TestCase):

    @unittest.skipIf(pa is None, 'pyarrow not installed')
    def test_arrow_backed_pandas_with_schema_overrides_ipv4(self):
        import pandas as pd
        df = pd.DataFrame({
            'addr': pd.Series(
                pa.array([0x0A000001, 0xC0A80101], type=pa.uint32()),
                dtype=pd.ArrowDtype(pa.uint32())),
            'ts': pd.Series(
                pa.array([_ts_us(2025, 1, 1), _ts_us(2025, 1, 2)],
                         type=pa.timestamp('us')),
                dtype=pd.ArrowDtype(pa.timestamp('us'))),
        })
        with QwpAckServer() as server:
            client = qi.QuestDB.from_conf(_client_conf(server.port))
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

    @unittest.skipIf(pa is None, 'pyarrow not installed')
    def test_numpy_backed_pandas_with_schema_overrides_raises(self):
        # A NumPy-backed column routes the frame to the NumPy planner,
        # which does not apply schema_overrides: passing them must raise
        # rather than silently ship the column with its default type.
        import pandas as pd
        df = pd.DataFrame({
            'addr': pd.Series([0x0A000001, 0xC0A80101], dtype='uint32'),
            'ts': pd.Series(
                pa.array([_ts_us(2025, 1, 1), _ts_us(2025, 1, 2)],
                         type=pa.timestamp('us')),
                dtype=pd.ArrowDtype(pa.timestamp('us'))),
        })
        with QwpAckServer() as server:
            client = qi.QuestDB.from_conf(_client_conf(server.port))
            try:
                with self.assertRaisesRegex(
                        qi.UnsupportedDataFrameShapeError,
                        'schema_overrides requires the Arrow columnar path'):
                    client.dataframe(
                        df,
                        table_name='ipv4_pandas',
                        at='ts',
                        schema_overrides={'addr': 'ipv4'})
                self.assertEqual(server.snapshot()['qwp1_frames'], 0)
            finally:
                client.close()


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
            client = qi.QuestDB.from_conf(_client_conf(server.port))
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
            client = qi.QuestDB.from_conf(_client_conf(server.port))
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
            client = qi.QuestDB.from_conf(_client_conf(server.port))
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
            client = qi.QuestDB.from_conf(_client_conf(server.port))
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
            client = qi.QuestDB.from_conf(_client_conf(server.port))
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
            client = qi.QuestDB.from_conf(_client_conf(server.port))
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
