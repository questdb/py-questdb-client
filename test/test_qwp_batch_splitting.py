"""Python regressions for QWP batches that need splits below eight rows."""

import os
import time
import unittest

import patch_path
import questdb._client as qi
from qwp_ws_ack_server import QwpAckServer

try:
    import pandas as pd
    import pyarrow as pa
except ImportError:
    pd = pa = None


def _frame(rows, arrow):
    values = {
        'seq': list(range(rows)),
        'padding': [chr(97 + i % 26) * 700 for i in range(rows)],
        'flag': [i % 3 == 1 for i in range(rows)],
        'nullable_flag': [None if i % 3 == 0 else i % 2 == 0
                          for i in range(rows)],
        'nullable_int': [None if i % 4 == 1 else i * 17
                         for i in range(rows)],
    }
    if arrow:
        return pa.table(values)
    return pd.DataFrame({
        'seq': pd.Series(values['seq'], dtype='int64'),
        'padding': pd.Series(values['padding'], dtype=object),
        'flag': pd.Series(values['flag'], dtype=bool),
        # The manual pandas planner rejects null booleans. Exercise its
        # packed object-bool values; Arrow above also exercises bool validity.
        'nullable_flag': pd.Series([bool(v) for v in values['nullable_flag']],
                                   dtype=object),
        'nullable_int': pd.Series(values['nullable_int'], dtype='Int64'),
    })


@unittest.skipIf(pd is None or pa is None, 'pandas/pyarrow not installed')
class TestQwpBatchSplitting(unittest.TestCase):
    CAP = 1024

    def _capture(self, frames, *, standalone=False, max_rows_per_batch=32000):
        with QwpAckServer(
                max_batch_size=self.CAP, defer_aware_acks=True,
                record_payloads=True) as server:
            conf = f'ws::addr=127.0.0.1:{server.port};'
            if not standalone:
                conf += 'lazy_connect=true;pool_reap=manual;'
            factory = qi.Sender.from_conf if standalone else qi.QuestDB.from_conf
            with factory(conf) as client:
                for frame in frames:
                    client.dataframe(
                        frame, table_name='split', symbols=False,
                        at=qi.TimestampNanos(1_700_000_000_000_000_000),
                        max_rows_per_batch=max_rows_per_batch)
            stats = server.snapshot()
        self.assertEqual(stats['errors'], [])
        payloads = [p for p in stats['binary_payloads']
                    if p[:4] == b'QWP1' and int.from_bytes(p[6:8], 'little')]
        self.assertTrue(all(len(p) <= self.CAP for p in payloads))
        # Independent calls commit each row; an internally split batch only
        # commits its last frame. Ignore that flag, not any encoded row data.
        return [p[:5] + bytes([p[5] & ~1]) + p[6:] for p in payloads]

    def test_small_batch_that_fits_is_not_split(self):
        for arrow in (False, True):
            with self.subTest(arrow=arrow):
                strings = ['x' * 100] * 8
                frame = (pa.table({'s': strings}) if arrow else
                         pd.DataFrame({'s': pd.Series(strings, dtype=object)}))
                payloads = self._capture([frame])
                self.assertEqual(len(payloads), 1)
                self.assertGreater(len(payloads[0]), self.CAP * 0.9)

    def test_object_booleans_match_arrow_without_splitting(self):
        frame = pd.DataFrame({
            'flag': pd.Series([True, False, True], dtype=object),
            'number': pd.Series([7, True, False], dtype=object),
        })
        reference = pa.table({'flag': [True, False, True], 'number': [7, 1, 0]})
        actual = self._capture([frame])
        self.assertEqual(len(actual), 1)
        self.assertEqual(actual, self._capture([reference]))

    def test_singleton_splits_preserve_values_and_null_bits(self):
        # Each row fits, but no pair fits. Compare the complete encoded rows
        # with independent one-row submissions, not just the number of frames.
        # 17 rows exercises both aligned multi-row splits and unaligned tails.
        for arrow in (False, True):
            for rows in (2, 5, 7, 8, 9, 17):
                with self.subTest(arrow=arrow, rows=rows):
                    frame = _frame(rows, arrow)
                    actual = self._capture([frame])
                    reference = (frame if arrow else
                                 pa.Table.from_pandas(frame, preserve_index=False))
                    expected = self._capture(
                        [reference.slice(i, 1) for i in range(rows)])
                    self.assertEqual(len(actual), rows)
                    self.assertEqual(actual, expected)

    def test_pandas_small_batch_request_and_standalone_sender(self):
        # Object columns make Python round a request of four up to eight.
        # The native layer must still split those eight rows to fit the cap.
        for standalone in (False, True):
            for arrow in (False, True):
                with self.subTest(standalone=standalone, arrow=arrow):
                    frame = _frame(8, arrow)
                    actual = self._capture(
                        [frame], standalone=standalone, max_rows_per_batch=4)
                    reference = (frame if arrow else
                                 pa.Table.from_pandas(frame, preserve_index=False))
                    expected = self._capture(
                        [reference.slice(i, 1) for i in range(8)])
                    self.assertEqual(len(actual), 8)
                    self.assertEqual(actual, expected)

    def test_oversized_single_row_is_not_retried_or_committed(self):
        for arrow in (False, True):
            for oversized in (0, 2, 4):
                with self.subTest(arrow=arrow, oversized=oversized):
                    strings = ['x' * (2048 if i == oversized else 700)
                               for i in range(5)]
                    frame = (pa.table({'s': strings}) if arrow else
                             pd.DataFrame({'s': pd.Series(strings, dtype=object)}))
                    with QwpAckServer(
                            max_batch_size=self.CAP, defer_aware_acks=True,
                            record_payloads=True) as server:
                        with qi.QuestDB.from_conf(
                                f'ws::addr=127.0.0.1:{server.port};'
                                'lazy_connect=true;pool_reap=manual;') as client:
                            with self.assertRaises(qi.QuestDBError) as raised:
                                client.dataframe(frame, table_name='split',
                                                 at=qi.ServerTimestamp)
                        server.wait_binary_frames_settled()
                        stats = server.snapshot()
                    self.assertEqual(raised.exception.code,
                                     qi.QuestDBErrorCode.BatchTooLarge)
                    self.assertFalse(raised.exception.in_doubt)
                    self.assertEqual(stats['accepted_connections'], 1)
                    self.assertEqual(len(stats['binary_payloads']), oversized)
                    # No commit boundary: any prefix must be discarded when
                    # the failed direct connection closes, never retried.
                    self.assertTrue(all(p[5] & 1
                                        for p in stats['binary_payloads']))
                    self.assertEqual(stats['errors'], [])


@unittest.skipUnless(
    os.environ.get('QDB_HTTP_ADDR') and pd is not None and pa is not None,
    'set QDB_HTTP_ADDR=host:port for a running QuestDB with pandas/pyarrow')
class TestQwpBatchSplittingLive(unittest.TestCase):
    def test_original_two_mib_reproductions(self):
        import uuid
        conf = f'ws::addr={os.environ["QDB_HTTP_ADDR"]};'
        for standalone in (False, True):
            for arrow, rows, width, batch_rows in (
                    (False, 8, 256 * 1024, 4),
                    (True, 5, 500000, 32000)):
                with self.subTest(standalone=standalone, arrow=arrow):
                    table = 'split_' + uuid.uuid4().hex
                    values = [chr(97 + i) * width for i in range(rows)]
                    frame = (pa.table({'s': values}) if arrow else
                             pd.DataFrame({'s': pd.Series(values, dtype=object)}))
                    factory = (qi.Sender.from_conf if standalone else
                               qi.QuestDB.from_conf)
                    with factory(conf) as sender:
                        sender.dataframe(frame, table_name=table,
                                         at=qi.ServerTimestamp,
                                         max_rows_per_batch=batch_rows)
                    with qi.QuestDB.from_conf(conf) as client:
                        try:
                            deadline = time.monotonic() + 10
                            while True:
                                got = client.query(
                                    f'SELECT left(s, 1) prefix, length(s) size '
                                    f'FROM {table} ORDER BY prefix').to_arrow()
                                if got.num_rows == rows:
                                    break
                                self.assertLess(time.monotonic(), deadline)
                                time.sleep(0.05)
                            self.assertEqual(got.column('prefix').to_pylist(),
                                             [chr(97 + i) for i in range(rows)])
                            self.assertEqual(got.column('size').to_pylist(),
                                             [width] * rows)
                        finally:
                            client.execute(f'DROP TABLE {table}')

    def test_split_values_round_trip(self):
        import uuid
        conf = f'ws::addr={os.environ["QDB_HTTP_ADDR"]};max_buf_size=1024;'
        # Read on a separate connection without the tiny ingestion cap.
        read_conf = f'ws::addr={os.environ["QDB_HTTP_ADDR"]};'
        for arrow in (False, True):
            with self.subTest(arrow=arrow):
                table = 'split_' + uuid.uuid4().hex
                frame = _frame(17, arrow)
                with qi.QuestDB.from_conf(conf) as client:
                    client.dataframe(frame, table_name=table,
                                     symbols=False, at=qi.ServerTimestamp)
                with qi.QuestDB.from_conf(read_conf) as client:
                    try:
                        deadline = time.monotonic() + 10
                        while True:
                            got = client.query(
                                f'SELECT seq, padding, flag, nullable_flag, '
                                f'nullable_int FROM {table} ORDER BY seq').to_arrow()
                            if got.num_rows == 17:
                                break
                            self.assertLess(time.monotonic(), deadline)
                            time.sleep(0.05)
                        expected = _frame(17, True).to_pydict()
                        # QuestDB BOOLEAN represents null as false.
                        expected['nullable_flag'] = [
                            bool(v) for v in expected['nullable_flag']]
                        self.assertEqual(got.to_pydict(), expected)
                    finally:
                        client.execute(f'DROP TABLE {table}')
