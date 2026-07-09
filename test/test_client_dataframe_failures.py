#!/usr/bin/env python3

"""
Direct-path unhappy-path coverage for ``Client.dataframe``.

Each test injects a precise failure through ``QwpAckServer``'s fault
knobs (per-connection close plans, advertised batch-size caps,
defer-aware acks) and pins the failover contract: what is re-sent, what
raises, and what must never reach the server.
"""

import sys

sys.dont_write_bytecode = True

import itertools
import time
import unittest

import patch_path
import questdb._client as qi

from qwp_ws_ack_server import QwpAckServer

try:
    import pyarrow as pa
except ImportError:
    pa = None


def _conf(port, **extra):
    conf = (
        f'qwpws::addr=127.0.0.1:{port};'
        'pool_size=1;pool_max=1;pool_reap=manual;')
    for key, value in extra.items():
        conf += f'{key}={value};'
    return conf


def _table(n, str_len=0):
    ts = pa.array(
        [1_700_000_000_000_000 + i for i in range(n)],
        type=pa.timestamp('us', tz='UTC'))
    cols = {'ts': ts, 'v': pa.array(list(range(n)), type=pa.int64())}
    if str_len:
        cols['s'] = pa.array(['x' * str_len] * n, type=pa.string())
    return pa.table(cols)


@unittest.skipIf(pa is None, 'pyarrow not installed')
class TestClientDataframeDirectFailures(unittest.TestCase):

    def test_empty_unknown_length_stream_sends_nothing(self):
        schema = pa.schema([
            ('ts', pa.timestamp('us', tz='UTC')),
            ('v', pa.int64()),
        ])
        reader = pa.RecordBatchReader.from_batches(schema, [])
        with QwpAckServer() as server:
            with qi.Client.from_conf(_conf(server.port)) as client:
                client.dataframe(reader, table_name='t_empty', at='ts')
            stats = server.snapshot()
        self.assertEqual(stats['binary_frames'], 0)
        self.assertEqual(stats['errors'], [])

    def test_transient_failure_before_checkpoint_resends_whole_frame(self):
        # Connection 1 acks 3 data frames then closes; no commit checkpoint
        # has landed, so the loop re-borrows and re-sends the whole frame:
        # 10 data frames + 1 trailing commit on connection 2.
        with QwpAckServer(close_plan=[3]) as server:
            with qi.Client.from_conf(_conf(server.port)) as client:
                client.dataframe(
                    _table(10), table_name='t_resend', at='ts',
                    max_rows_per_batch=1)
            stats = server.snapshot()
        self.assertEqual(stats['accepted_connections'], 2)
        self.assertEqual(stats['binary_frames'], 3 + 10 + 1)

    def test_committed_prefix_failure_raises_without_resend(self):
        # 110 one-row batches: frames 1-100 are data, frame 101 is the
        # intermediate commit checkpoint (every 100 batches), frames
        # 102+ resume data. Closing after frame 105 lands the failure
        # past the checkpoint, so a whole-frame re-send would duplicate
        # the committed prefix: the call must raise instead of retrying.
        with QwpAckServer(close_plan=[105]) as server:
            with qi.Client.from_conf(_conf(server.port)) as client:
                with self.assertRaises(qi.QuestDBError) as raised:
                    client.dataframe(
                        _table(110), table_name='t_prefix', at='ts',
                        max_rows_per_batch=1)
            stats = server.snapshot()
        self.assertEqual(
            raised.exception.code, qi.QuestDBErrorCode.FailoverRetry)
        self.assertEqual(stats['accepted_connections'], 1)
        self.assertEqual(stats['binary_frames'], 105)

    def test_capacity_exhaustion_drains_and_completes(self):
        # The 1024-byte advertised cap splits every 16-row batch into
        # two deferred frames, so the frame count outruns the client's
        # per-batch counter; with defer-aware acks nothing drains until
        # a commit boundary, so the 127-slot window fills mid-frame.
        # The split drains it internally (commit + retry) and the ingest
        # completes: 160 data frames plus at least one mid-split drain
        # commit and the trailing commit.
        with QwpAckServer(
                max_batch_size=1024, defer_aware_acks=True) as server:
            with qi.Client.from_conf(_conf(server.port)) as client:
                client.dataframe(
                    _table(1280, str_len=64), table_name='t_cap',
                    at='ts', max_rows_per_batch=16)
            stats = server.snapshot()
        self.assertGreaterEqual(
            stats['qwp1_frames'], 162,
            'batches must have split and the window must have been '
            'drained mid-frame')
        self.assertEqual(stats['errors'], [])

    def test_reconnect_budget_exhaustion_raises(self):
        # Every connection dies after its first frame and no checkpoint
        # ever lands, so the loop keeps re-sending until the reconnect
        # budget expires and the transient error surfaces.
        started = time.monotonic()
        with QwpAckServer(close_plan=itertools.repeat(1)) as server:
            with qi.Client.from_conf(
                    _conf(server.port,
                          reconnect_max_duration_millis=1200)) as client:
                with self.assertRaises(qi.QuestDBError) as raised:
                    client.dataframe(
                        _table(5), table_name='t_budget', at='ts',
                        max_rows_per_batch=1)
            stats = server.snapshot()
        elapsed = time.monotonic() - started
        self.assertIn(
            raised.exception.code,
            (qi.QuestDBErrorCode.FailoverRetry,
             qi.QuestDBErrorCode.SocketError))
        self.assertGreaterEqual(stats['accepted_connections'], 2)
        self.assertLess(elapsed, 30.0)


if __name__ == '__main__':
    unittest.main()
