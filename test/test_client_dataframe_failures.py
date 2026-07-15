#!/usr/bin/env python3

"""
Direct-path unhappy-path coverage for ``QuestDB.dataframe``.

Each test injects a precise failure through ``QwpAckServer``'s fault
knobs (per-connection close plans, advertised batch-size caps,
defer-aware acks) and pins the failover contract: what is re-sent, what
raises, and what must never reach the server.
"""

import sys

sys.dont_write_bytecode = True

import socket
import time
import unittest

import patch_path
import questdb._client as qi

from qwp_ws_ack_server import QwpAckServer
from test_client_dataframe_fuzz import (
    Rng,
    _derive_master_seed,
    _format_seed,
    _parse_int_env,
    ITER_SEED_ENV,
    ITERS_ENV,
)

try:
    import numpy as np
    import pandas as pd
except ImportError:
    pd = None

try:
    import pyarrow as pa
except ImportError:
    pa = None


def _conf(port, **extra):
    conf = (
        f'ws::addr=127.0.0.1:{port};'
        'sender_pool_min=1;sender_pool_max=1;pool_reap=manual;')
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
            with qi.QuestDB.from_conf(_conf(server.port)) as client:
                client.dataframe(reader, table_name='t_empty', at='ts')
            stats = server.snapshot()
        self.assertEqual(stats['binary_frames'], 0)
        self.assertEqual(stats['errors'], [])

    def test_consumed_stream_failure_raises_instead_of_empty_replay(self):
        # A one-shot RecordBatchReader cannot be re-exported for the
        # whole-frame replay: after the server drops the connection with
        # the stream already drained, a retry would send nothing and
        # report success. The call must raise instead — on the original
        # connection only, with a message pointing at the fresh-reader fix.
        schema = pa.schema([
            ('ts', pa.timestamp('us', tz='UTC')),
            ('v', pa.int64()),
        ])

        def batches():
            for i in range(10):
                yield pa.record_batch(
                    [pa.array([1_700_000_000_000_000 + i],
                              type=schema[0].type),
                     pa.array([i], type=pa.int64())], schema=schema)
            # Let the server's close land before the trailing sync so the
            # failure is the retry gate's decision, not a flush error.
            time.sleep(0.15)

        with QwpAckServer(close_plan=[10], defer_aware_acks=True) as server:
            with qi.QuestDB.from_conf(_conf(server.port)) as client:
                reader = pa.RecordBatchReader.from_batches(schema, batches())
                with self.assertRaises(qi.QuestDBError) as raised:
                    client.dataframe(reader, table_name='t_stream', at='ts')
            stats = server.snapshot()
        self.assertIn('cannot be replayed', str(raised.exception))
        self.assertFalse(raised.exception.in_doubt)
        self.assertEqual(stats['accepted_connections'], 1)
        self.assertEqual(stats['binary_frames'], 10)

    def test_transient_failure_before_publication_resends_whole_frame(self):
        # Operation 1 (11 frames: 10 data + 1 commit) completes, then the
        # server closes the pooled connection; the barrier below waits for
        # its FIN before operation 2 starts. Racing the close against a
        # single in-flight operation instead (e.g. close_plan=[0]) is
        # unsound: whether the client notices before or after writing the
        # commit frame is thread scheduling, and the post-commit surface is
        # a legitimate in-doubt raise, not a resend. Here nothing of
        # operation 2 can have been delivered, so it must transparently
        # move to connection 2 and send all 11 frames again.
        with QwpAckServer(close_plan=[11]) as server:
            with qi.QuestDB.from_conf(_conf(server.port)) as client:
                client.dataframe(
                    _table(10), table_name='t_resend', at='ts',
                    max_rows_per_batch=1)
                deadline = time.monotonic() + 5.0
                while (server.snapshot()['finished_connections'] < 1
                        and time.monotonic() < deadline):
                    time.sleep(0.005)
                self.assertEqual(
                    server.snapshot()['finished_connections'], 1)
                client.dataframe(
                    _table(10), table_name='t_resend', at='ts',
                    max_rows_per_batch=1)
            stats = server.snapshot()
        self.assertEqual(stats['accepted_connections'], 2)
        self.assertEqual(stats['binary_frames'], 11 + 11)

    def test_committed_prefix_failure_raises_without_resend(self):
        # 110 one-row batches: frames 1-100 are data, frame 101 is the
        # intermediate commit checkpoint (every 100 batches), frames
        # 102+ resume data. Closing after frame 105 lands the failure
        # past the checkpoint, so a whole-frame re-send would duplicate
        # the committed prefix: the call must raise instead of retrying.
        with QwpAckServer(close_plan=[105]) as server:
            with qi.QuestDB.from_conf(_conf(server.port)) as client:
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
            with qi.QuestDB.from_conf(_conf(server.port)) as client:
                client.dataframe(
                    _table(1280, str_len=64), table_name='t_cap',
                    at='ts', max_rows_per_batch=16)
            stats = server.snapshot()
        self.assertGreaterEqual(
            stats['qwp1_frames'], 162,
            'batches must have split and the window must have been '
            'drained mid-frame')
        self.assertEqual(stats['errors'], [])

    @unittest.skipIf(pd is None, 'pandas not installed')
    def test_in_doubt_split_failure_raises_without_resend(self):
        # One 1280-row batch splits into 160 data frames under this cap; the
        # native split drains its 128-frame deferred window with an internal
        # commit near frame 129, so by the server's close at frame 140 a
        # prefix is already committed server-side. The whole flush usually
        # survives (the remaining frames land in the socket buffer) and the
        # failure surfaces in the trailing sync. Whether the peer reset is
        # seen by that sync's non-blocking entry drain or by its blocking
        # ack wait is a race — both must classify as delivery-unknown
        # (`in_doubt`), or the client would blindly resend the operation and
        # duplicate the committed prefix. Both the NumPy and Arrow routes
        # must raise instead of reconnecting and resending.
        for arrow_route in (False, True):
            with self.subTest(arrow_route=arrow_route):
                with QwpAckServer(
                        close_plan=[140],
                        max_batch_size=1024,
                        defer_aware_acks=True) as server:
                    with qi.QuestDB.from_conf(
                            _conf(
                                server.port,
                                reconnect_max_duration_millis=3000)) as client:
                        with self.assertRaises(qi.QuestDBError) as raised:
                            client.dataframe(
                                _fault_frame(1280, 64, arrow_route),
                                table_name='t_in_doubt',
                                at='ts',
                                max_rows_per_batch=1280)
                    stats = server.snapshot()

                self.assertEqual(
                    raised.exception.code,
                    qi.QuestDBErrorCode.FailoverRetry)
                self.assertTrue(raised.exception.in_doubt)
                self.assertEqual(stats['accepted_connections'], 1)
                self.assertEqual(stats['binary_frames'], 140)
                self.assertEqual(stats['errors'], [])

    def test_reconnect_budget_exhaustion_raises(self):
        # A bound-but-non-listening local port never completes the TCP
        # handshake before publication, so the native reconnect loop
        # exhausts the configured budget without making delivery uncertain.
        #
        # `connect_timeout` is essential here and must be small: a
        # foreground connect defaults to no connect timeout (mirroring
        # Java's effectiveConnectTimeoutMs), so each dial inherits the OS
        # default. On Linux a bound-not-listening port refuses instantly
        # (ECONNREFUSED) and the loop spins fast, but on macOS the SYN goes
        # unanswered and a single connect() blocks ~7.8s (ETIMEDOUT) —
        # overshooting the reconnect budget, which can only bound the loop
        # *between* attempts, not one in-flight connect. Bounding the dial
        # makes the reconnect budget the governing deadline on every OS.
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as blocker:
            blocker.bind(('127.0.0.1', 0))
            port = blocker.getsockname()[1]
            started = time.monotonic()
            with qi.QuestDB.from_conf(
                    _conf(port, reconnect_max_duration_millis=300,
                          connect_timeout=100)) as client:
                with self.assertRaises(qi.QuestDBError) as raised:
                    client.dataframe(
                        _table(5), table_name='t_budget', at='ts',
                        max_rows_per_batch=1)
            elapsed = time.monotonic() - started
        self.assertEqual(
            raised.exception.code, qi.QuestDBErrorCode.SocketError)
        self.assertFalse(raised.exception.in_doubt)
        self.assertIn('all endpoints unreachable', str(raised.exception))
        self.assertLess(elapsed, 5.0)


def _fault_frame(n_rows, str_len, arrow_route):
    ts = [1_700_000_000_000_000 + i * 1_000 for i in range(n_rows)]
    vs = list(range(n_rows))
    if arrow_route:
        cols = {
            'ts': pa.array(ts, type=pa.timestamp('us', tz='UTC')),
            'v': pa.array(vs, type=pa.int64()),
        }
        if str_len:
            cols['s'] = pa.array(['x' * str_len] * n_rows, type=pa.string())
        return pa.table(cols)
    frame = pd.DataFrame({
        'ts': pd.to_datetime(ts, unit='us'),
        'v': np.array(vs, dtype=np.int64),
    })
    if str_len:
        frame['s'] = ['x' * str_len] * n_rows
    return frame


@unittest.skipIf(
    pa is None or pd is None, 'pandas/pyarrow not installed')
class TestClientDataframeFaultFuzz(unittest.TestCase):
    """Random fault timing x frame shape over the direct dataframe path.

    The deterministic tests above pin the boundaries we could derive by
    hand; this sweep drives random disconnect points, server batch caps
    (forcing frame splits under defer-aware acks), and their combination,
    asserting invariants only: the call terminates promptly, ends in
    success or a known error code, never corrupts the wire, and a
    post-checkpoint failure never re-sends.
    """

    DEFAULT_ITERS = 100

    ALLOWED_CODES = None

    @classmethod
    def setUpClass(cls):
        cls.ALLOWED_CODES = (
            qi.QuestDBErrorCode.FailoverRetry,
            qi.QuestDBErrorCode.SocketError,
            qi.QuestDBErrorCode.InvalidApiCall,
            qi.QuestDBErrorCode.BatchTooLarge,
        )
        cls.iter_seed_override = _parse_int_env(ITER_SEED_ENV)
        if cls.iter_seed_override is not None:
            cls.master_seed = None
            cls.iters = 1
            sys.stderr.write(
                f'>>>> dataframe fault fuzz: '
                f'iter_seed_override={_format_seed(cls.iter_seed_override)}, '
                f'iters=1\n')
            return
        cls.master_seed = _derive_master_seed()
        cls.iters = _parse_int_env(ITERS_ENV) or cls.DEFAULT_ITERS
        sys.stderr.write(
            f'>>>> dataframe fault fuzz: master_seed='
            f'{_format_seed(cls.master_seed)}, iters={cls.iters}\n')

    def _iter_seeds(self):
        if self.iter_seed_override is not None:
            return [self.iter_seed_override]
        master = Rng(self.master_seed)
        return [master.next_long() for _ in range(self.iters)]

    def _master_label(self):
        if self.master_seed is None:
            return f'iter_seed_override={_format_seed(self.iter_seed_override)}'
        return f'master_seed={_format_seed(self.master_seed)}'

    def _run_one(self, rng):
        max_rows_per_batch = rng.choice([1, 2, 4, 16])
        batches = rng.choice([1, 3, 30, 130])
        n_rows = max(1, max_rows_per_batch * batches - rng.next_int(
            max_rows_per_batch))
        str_len = rng.choice([0, 8, 64])
        arrow_route = rng.next_bool()
        mode = rng.choice(['disconnect', 'cap', 'combo'])
        cap = rng.choice([512, 1024, 2048]) if mode != 'disconnect' else 0
        close_plan = None
        if mode != 'cap':
            close_plan = [rng.next_int(batches * 4 + 4)]
        frame = _fault_frame(n_rows, str_len, arrow_route)

        started = time.monotonic()
        with QwpAckServer(
                close_plan=close_plan,
                max_batch_size=cap,
                defer_aware_acks=(mode != 'disconnect')) as server:
            with qi.QuestDB.from_conf(
                    _conf(server.port,
                          reconnect_max_duration_millis=3000)) as client:
                err = None
                try:
                    client.dataframe(
                        frame, table_name='t_fault', at='ts',
                        max_rows_per_batch=max_rows_per_batch)
                except qi.QuestDBError as exc:
                    err = exc
            stats = server.snapshot()
        elapsed = time.monotonic() - started

        label = (
            f'mode={mode} cap={cap} close={close_plan} rows={n_rows} '
            f'mrpb={max_rows_per_batch} str={str_len} arrow={arrow_route}')
        assert elapsed < 30.0, f'{label}: took {elapsed:.1f}s'
        assert stats['errors'] == [], f'{label}: server saw {stats["errors"]}'
        if err is not None:
            assert err.code in self.ALLOWED_CODES, (
                f'{label}: unexpected {err.code}: {err}')
        else:
            # The numpy chunk path rounds the batch size up to the 8-row
            # bitmap alignment, so the frame-count floor uses that stride.
            min_frames = -(-n_rows // max(max_rows_per_batch, 8))
            assert stats['binary_frames'] >= min_frames, (
                f'{label}: only {stats["binary_frames"]} frames for '
                f'{min_frames} batches')

    def test_fuzz_fault_injection(self):
        failures = []
        seeds = self._iter_seeds()
        for iter_seed in seeds:
            rng = Rng(iter_seed)
            try:
                self._run_one(rng)
            except AssertionError as exc:
                failures.append((iter_seed, type(exc).__name__, str(exc)))
            except Exception as exc:  # noqa: BLE001 — fuzz triage
                failures.append((iter_seed, type(exc).__name__, repr(exc)))
        if failures:
            preview = '\n'.join(
                f'  iter={_format_seed(s)} [{cls}]: {m}'
                for s, cls, m in failures[:5])
            self.fail(
                f'{len(failures)}/{len(seeds)} iterations failed.\n'
                f'{self._master_label()}\n{preview}')


if __name__ == '__main__':
    unittest.main()
