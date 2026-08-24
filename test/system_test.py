#!/usr/bin/env python3

import sys
sys.dont_write_bytecode = True
import os
import datetime
import importlib.util
import random
import shutil
import socket
import tempfile
import threading
import time
import unittest
import uuid
import pathlib
import numpy as np
import decimal

import patch_path
PROJ_ROOT = patch_path.PROJ_ROOT
sys.path.append(str(PROJ_ROOT / 'c-questdb-client' / 'system_test'))
from fixture import \
    QuestDbFixture, install_questdb, install_questdb_from_repo, CA_PATH, AUTH


try:
    import pandas as pd
    import numpy
    import pyarrow
except ImportError:
    pd = None
    pyarrow = None


import questdb._client as qi


# The released server the integration suite downloads. A CI leg pins a
# different one through `QDB_VERSION`; `QDB_REPO_PATH` overrides both and
# builds from a checkout instead.
QUESTDB_VERSION = os.environ.get('QDB_VERSION') or '9.4.3'

# Set by the CI legs whose job is to cover the QWP-only column types.
# Where it is set, a server too old for them fails the run instead of
# skipping it: a leg that silently covers nothing is indistinguishable
# from one that passes.
REQUIRE_QWP_ROW_TYPES = (
    os.environ.get('TEST_QUESTDB_REQUIRE_QWP_ROW_TYPES') == '1')
QUESTDB_PLAIN_INSTALL_PATH = None
QUESTDB_AUTH_INSTALL_PATH = None
FIRST_ARRAY_RELEASE = (8, 4, 0)
FIRST_DECIMAL_RELEASE = (9, 2, 0)
#: The first release that speaks QWP at all. It is a beta, and the
#: integration suite runs against it so the protocol keeps working
#: there; what this client supports is QuestDB 10 and newer. Tests gated
#: on this one are those the beta serves.
FIRST_QWP_WS_RELEASE = (9, 4, 3)
FIRST_QWP_GAP_HALT_RELEASE = (9, 4, 4)
#: The QWP-only column types are supported from QuestDB 10, the first
#: production QWP implementation. 9.4.3 shipped QWP as a beta and the
#: integration suite still runs against it, so `FIRST_QWP_WS_RELEASE`
#: means "this server speaks QWP at all" and this one means "this
#: server speaks the QWP we support".
#:
#: The beta accepts these types, so the two cannot be told apart by
#: running the tests. Measured on 2026-08-24 against the released 9.4.3
#: (commit 33fa1320) and 10.0.0 (commit d71e25a5): all fifteen tests in
#: the three classes gated on this constant pass on both, including
#: every NULL sentinel and GEOHASH precisions 1b to 60b. This is a
#: support boundary rather than a capability one, so lowering it to see
#: what breaks shows nothing breaking.
FIRST_QWP_ROW_TYPES_RELEASE = (10, 0, 0)

def may_install_questdb():
    global QUESTDB_PLAIN_INSTALL_PATH
    global QUESTDB_AUTH_INSTALL_PATH
    if QUESTDB_PLAIN_INSTALL_PATH:
        return

    install_path = None
    if os.environ.get('QDB_REPO_PATH'):
        repo = pathlib.Path(os.environ['QDB_REPO_PATH'])
        install_path = install_questdb_from_repo(repo)
    else:
        url = ('https://github.com/questdb/questdb/releases/download/' +
            QUESTDB_VERSION +
            '/questdb-' +
            QUESTDB_VERSION +
            '-no-jre-bin.tar.gz')
        install_path = install_questdb(QUESTDB_VERSION, url)

    QUESTDB_PLAIN_INSTALL_PATH = PROJ_ROOT / 'build' / 'questdb' / 'plain'
    shutil.copytree(
        install_path, QUESTDB_PLAIN_INSTALL_PATH, dirs_exist_ok=True)

    QUESTDB_AUTH_INSTALL_PATH = PROJ_ROOT / 'build' / 'questdb' / 'auth'
    shutil.copytree(
        install_path, QUESTDB_AUTH_INSTALL_PATH, dirs_exist_ok=True)


class TestWithDatabase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        may_install_questdb()

        cls.qdb_plain = None
        cls.qdb_auth = None
        cls._qwp_udp_enabled = bool(os.environ.get('QDB_REPO_PATH'))

        cls.qdb_plain = QuestDbFixture(
            QUESTDB_PLAIN_INSTALL_PATH,
            auth=False,
            wrap_tls=True,
            http=True,
            qwp_udp=cls._qwp_udp_enabled)
        cls.qdb_plain.start()

        cls.qdb_auth = QuestDbFixture(
            QUESTDB_AUTH_INSTALL_PATH, auth=True, wrap_tls=True)
        cls.qdb_auth.start()

    @classmethod
    def tearDownClass(cls):
        if cls.qdb_auth:
            cls.qdb_auth.stop()
        if cls.qdb_plain:
            cls.qdb_plain.stop()

    def _require_qwp_udp(self):
        if not self.qdb_plain.qwp_udp:
            self.skipTest(
                'QWP/UDP integration tests require repo-backed QWP receiver support')

    def _mk_udp_sender(self, **kwargs):
        self._require_qwp_udp()
        return qi.Sender(
            qi.Protocol.Udp,
            self.qdb_plain.host,
            self.qdb_plain.qwp_udp_port,
            **kwargs)

    def _mk_udp_conf(self, **kwargs):
        self._require_qwp_udp()
        conf = f'udp::addr={self.qdb_plain.host}:{self.qdb_plain.qwp_udp_port};'
        for key, value in kwargs.items():
            conf += f'{key}={value};'
        return conf

    def _require_qwp_ws(self):
        if self.qdb_plain.version < FIRST_QWP_WS_RELEASE:
            self.skipTest(
                'QWP/WebSocket integration tests require QuestDB 9.4.3+')

    def _require_qwp_row_types(self):
        """UUID, IPV4, BINARY, CHAR, DATE, LONG256 and GEOHASH columns
        need a QWP sender on QuestDB 10 or newer, whichever API
        produced them. The 9.4.3 beta accepts them; it is not a
        configuration this client supports.

        Called from `setUp`, so the classes built around it start their
        fixtures and then skip every test on the legs that pin no
        version. Asking earlier means asking before `start()`, where
        the only version available is the one parsed out of the install
        directory's name; the gate reads the one the running server
        reports, and the two differ exactly on the leg built from the
        repo, which is the leg with the newest server. A wrong guess
        there drops the coverage in silence, which is worse than the
        two fixtures."""
        self._require_qwp_ws()
        if self.qdb_plain.version < FIRST_QWP_ROW_TYPES_RELEASE:
            version = '.'.join(str(part) for part in self.qdb_plain.version)
            if REQUIRE_QWP_ROW_TYPES:
                self.fail(
                    f'This run is supposed to cover the QWP-only column '
                    f'types, but the server reports {version} and they '
                    f'need '
                    f'{".".join(str(p) for p in FIRST_QWP_ROW_TYPES_RELEASE)} '
                    f'or newer. Point QDB_VERSION or QDB_REPO_PATH at a '
                    f'server that has them, or clear '
                    f'TEST_QUESTDB_REQUIRE_QWP_ROW_TYPES.')
            self.skipTest(
                f'QWP-only column types require QuestDB '
                f'{".".join(str(p) for p in FIRST_QWP_ROW_TYPES_RELEASE)}'
                f'+ (the first production QWP), server reports {version}')

    def _require_qwp_fuzz(self):
        self._require_qwp_ws()

    def _mk_qwpws_conf(self, sender_id, sf_dir, endpoints=None, **kwargs):
        self._require_qwp_ws()
        if endpoints is None:
            endpoints = [
                (self.qdb_plain.host, self.qdb_plain.http_server_port)]
        addr = ','.join(
            f'{endpoint_host}:{endpoint_port}'
            for endpoint_host, endpoint_port in endpoints)
        conf = (
            f'ws::addr={addr};'
            f'sender_id={sender_id};'
            f'sf_dir={sf_dir};')
        for key, value in kwargs.items():
            conf += f'{key}={value};'
        return conf

    @staticmethod
    def _micros_to_qdb_date(timestamp_us):
        secs, remaining_us = divmod(timestamp_us, 1_000_000)
        return datetime.datetime.fromtimestamp(
            secs, datetime.timezone.utc).replace(
            microsecond=remaining_us).strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    @staticmethod
    def _nanos_to_qdb_date(timestamp_ns):
        secs, remaining_ns = divmod(timestamp_ns, 1_000_000_000)
        base = datetime.datetime.fromtimestamp(
            secs, datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')
        return f'{base}.{remaining_ns:09d}Z'

    @staticmethod
    def _sfa_file_count(sf_dir, sender_id):
        slot_dir = pathlib.Path(sf_dir) / sender_id
        if not slot_dir.exists():
            return 0
        return sum(1 for path in slot_dir.iterdir()
                   if path.name.endswith('.sfa'))

    @staticmethod
    def _unused_tcp_port():
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(('127.0.0.1', 0))
            return sock.getsockname()[1]

    def _retry_poll_error(self, sender, timeout_sec=10):
        import time as _time
        deadline = _time.monotonic() + timeout_sec
        while _time.monotonic() < deadline:
            diagnostic = sender.poll_error()
            if diagnostic is not None:
                return diagnostic
            _time.sleep(0.05)
        self.fail('Timed out waiting for QWP/WebSocket diagnostic')

    @staticmethod
    def _qwp_fuzz_seed():
        seed_text = os.environ.get('QDB_PY_QWP_FUZZ_SEED')
        if seed_text:
            return int(seed_text, 0)
        return 0x5151

    def _test_scenario(self, qdb, protocol, **kwargs):
        protocol = qi.Protocol.parse(protocol)
        port = qdb.tls_line_tcp_port if protocol.tls_enabled else qdb.line_tcp_port
        pending = None
        table_name = uuid.uuid4().hex
        with qi.Sender(protocol, 'localhost', port, **kwargs) as sender:
            for _ in range(3):
                sender.row(
                    table_name,
                    symbols={
                        'name_a': 'val_a'},
                    columns={
                        'name_b': True,
                        'name_c': 42,
                        'name_d': 2.5,
                        'name_e': 'val_b'},
                   at=qi.ServerTimestamp)
            pending = bytes(sender)

        resp = qdb.retry_check_table(table_name, min_rows=3, log_ctx=pending)
        exp_columns = [
            {'name': 'name_a', 'type': 'SYMBOL'},
            {'name': 'name_b', 'type': 'BOOLEAN'},
            {'name': 'name_c', 'type': 'LONG'},
            {'name': 'name_d', 'type': 'DOUBLE'},
            {'name': 'name_e', 'type': 'VARCHAR'},
            {'name': 'timestamp', 'type': 'TIMESTAMP'}]
        self.assertEqual(resp['columns'], exp_columns)

        exp_dataset = [  # Comparison excludes timestamp column.
            ['val_a', True, 42, 2.5, 'val_b'],
            ['val_a', True, 42, 2.5, 'val_b'],
            ['val_a', True, 42, 2.5, 'val_b']]
        scrubbed_dataset = [row[:-1] for row in resp['dataset']]
        self.assertEqual(scrubbed_dataset, exp_dataset)

    def test_plain(self):
        self._test_scenario(self.qdb_plain, 'tcp')

    def test_plain_tls_insecure_skip_verify(self):
        self._test_scenario(self.qdb_plain, 'tcps', tls_verify=False)

    def test_plain_tls_insecure_skip_verify_str(self):
        self._test_scenario(self.qdb_plain, 'tcps', tls_verify='unsafe_off')

    def test_plain_tls_ca(self):
        self._test_scenario(self.qdb_plain, 'tcps', tls_roots=CA_PATH)

    def test_plain_tls_ca_str(self):
        self._test_scenario(self.qdb_plain, 'tcps', tls_roots=str(CA_PATH))

    def test_auth(self):
        self._test_scenario(self.qdb_auth, 'tcp', **AUTH, auth_timeout=5000)

    def test_auth_tls_insecure_skip_verify(self):
        self._test_scenario(self.qdb_auth, 'tcps', tls_verify=False, **AUTH)

    def test_auth_tls_insecure_skip_verify_str(self):
        self._test_scenario(self.qdb_auth, 'tcps', tls_verify=False, **AUTH)

    def test_auth_tls_ca(self):
        self._test_scenario(self.qdb_auth, 'tcps', tls_verify=True, tls_roots=CA_PATH, **AUTH)

    def test_auth_tls_ca_str(self):
        self._test_scenario(self.qdb_auth, 'tcps', tls_verify='on', tls_roots=str(CA_PATH), **AUTH)

    @unittest.skipIf(not pd, 'pandas not installed')
    def test_basic_dataframe(self):
        port = self.qdb_plain.line_tcp_port
        pending = None
        table_name = uuid.uuid4().hex
        df = pd.DataFrame({
            'col_a': [1, 2, 3],
            'col_b': ['a', 'b', 'c'],
            'col_c': [True, False, True],
            'col_d': [1.5, 2.5, 3.5],
            'col_e': pd.Categorical(['A', 'B', 'C']),
            'col_f': [
                numpy.datetime64('2021-01-01'),
                numpy.datetime64('2021-01-02'),
                numpy.datetime64('2021-01-03')]})
        df.index.name = table_name
        with qi.Sender('tcp', 'localhost', port) as sender:
            sender.dataframe(df, at=qi.ServerTimestamp)
            pending = bytes(sender)

        resp = self.qdb_plain.retry_check_table(
            table_name, min_rows=3, log_ctx=pending)
        exp_columns = [
            {'name': 'col_e', 'type': 'SYMBOL'},
            {'name': 'col_a', 'type': 'LONG'},
            {'name': 'col_b', 'type': 'VARCHAR'},
            {'name': 'col_c', 'type': 'BOOLEAN'},
            {'name': 'col_d', 'type': 'DOUBLE'},
            {'name': 'col_f', 'type': 'TIMESTAMP'},
            {'name': 'timestamp', 'type': 'TIMESTAMP'}]
        self.assertEqual(resp['columns'], exp_columns)

        exp_dataset = [  # Comparison excludes timestamp column.
            ['A', 1, 'a', True, 1.5, '2021-01-01T00:00:00.000000Z'],
            ['B', 2, 'b', False, 2.5, '2021-01-02T00:00:00.000000Z'],
            ['C', 3, 'c', True, 3.5, '2021-01-03T00:00:00.000000Z']]
        scrubbed_dataset = [row[:-1] for row in resp['dataset']]
        self.assertEqual(scrubbed_dataset, exp_dataset)

    def test_http(self):
        port = self.qdb_plain.http_server_port
        table_name = uuid.uuid4().hex
        with qi.Sender('http', 'localhost', port) as sender:
            for _ in range(3):
                sender.row(
                    table_name,
                    symbols={
                        'name_a': 'val_a'},
                    columns={
                        'name_b': True,
                        'name_c': 42,
                        'name_d': 2.5,
                        'name_e': 'val_b'},
                    at=qi.TimestampNanos.now())

            if self.qdb_plain.version <= (7, 3, 7):
                with self.assertRaisesRegex(
                        qi.QuestDBError,
                        r'.*HTTP endpoint does not support ILP.*'):
                    sender.flush()
                return

        resp = self.qdb_plain.retry_check_table(table_name, min_rows=3)

        exp_ts_type = 'TIMESTAMP' if self.qdb_plain.version < (9, 1, 0) else 'TIMESTAMP_NS'

        exp_columns = [
            {'name': 'name_a', 'type': 'SYMBOL'},
            {'name': 'name_b', 'type': 'BOOLEAN'},
            {'name': 'name_c', 'type': 'LONG'},
            {'name': 'name_d', 'type': 'DOUBLE'},
            {'name': 'name_e', 'type': 'VARCHAR'},
            {'name': 'timestamp', 'type': exp_ts_type}]
        self.assertEqual(resp['columns'], exp_columns)

        exp_dataset = [  # Comparison excludes timestamp column.
            ['val_a', True, 42, 2.5, 'val_b'],
            ['val_a', True, 42, 2.5, 'val_b'],
            ['val_a', True, 42, 2.5, 'val_b']]
        scrubbed_dataset = [row[:-1] for row in resp['dataset']]
        self.assertEqual(scrubbed_dataset, exp_dataset)

    def test_qwp_websocket_single_batch_round_trip(self):
        self._require_qwp_ws()
        table_name = uuid.uuid4().hex
        sender_id = 'py-smoke-' + uuid.uuid4().hex[:8]
        self.qdb_plain.http_sql_query(
            f'CREATE TABLE "{table_name}" '
            '(id LONG, val DOUBLE, timestamp TIMESTAMP) '
            'TIMESTAMP(timestamp) PARTITION BY DAY WAL '
            'DEDUP UPSERT KEYS(timestamp, id)')
        with tempfile.TemporaryDirectory(prefix='py-qwp-ws-smoke-') as sf_dir:
            conf = self._mk_qwpws_conf(
                sender_id,
                sf_dir,
                reconnect_max_duration_millis=30000,
                close_flush_timeout_millis=30000)
            sender = qi.Sender.from_conf(conf)
            try:
                sender.establish()
                for row_id in range(3):
                    sender.row(
                        table_name,
                        columns={
                            'id': row_id,
                            'val': row_id * 0.5},
                        at=qi.TimestampMicros(
                            1_700_000_000_000_000 + row_id * 1000))
                fsn = sender.flush_and_get_fsn()
                self.assertEqual(fsn, 0)
                self.assertTrue(sender.await_acked_fsn(fsn, 30000))
                sender.close_drain()
            finally:
                sender.close(False)

            self.assertEqual(self._sfa_file_count(sf_dir, sender_id), 0)

        self.qdb_plain.retry_check_table(table_name, min_rows=3)
        resp = self.qdb_plain.http_sql_query(
            f"select id, val from '{table_name}' order by id")
        self.assertEqual(resp['dataset'], [[0, 0.0], [1, 0.5], [2, 1.0]])

    def test_qwp_websocket_dead_endpoint_failover_and_ack_progresses(self):
        self._require_qwp_ws()
        table_name = uuid.uuid4().hex
        sender_id = 'py-failover-' + uuid.uuid4().hex[:8]
        self.qdb_plain.http_sql_query(
            f'CREATE TABLE "{table_name}" '
            '(id LONG, val DOUBLE, timestamp TIMESTAMP) '
            'TIMESTAMP(timestamp) PARTITION BY DAY WAL '
            'DEDUP UPSERT KEYS(timestamp, id)')
        endpoints = [
            (self.qdb_plain.host, self._unused_tcp_port()),
            (self.qdb_plain.host, self.qdb_plain.http_server_port)]

        with tempfile.TemporaryDirectory(prefix='py-qwp-ws-failover-') as sf_dir:
            sender = qi.Sender.from_conf(self._mk_qwpws_conf(
                sender_id,
                sf_dir,
                endpoints=endpoints,
                reconnect_max_duration_millis=30000,
                close_flush_timeout_millis=30000))
            try:
                sender.establish()
                sender.row(
                    table_name,
                    columns={'id': 0, 'val': 0.5},
                    at=qi.TimestampMicros(1_700_000_000_000_000))
                fsn = sender.flush_and_get_fsn()
                self.assertEqual(fsn, 0)
                self.assertTrue(sender.await_acked_fsn(fsn, 30000))
                self.assertEqual(sender.acked_fsn(), fsn)
                sender.close_drain()
            finally:
                sender.close(False)

            self.assertEqual(self._sfa_file_count(sf_dir, sender_id), 0)

        self.qdb_plain.retry_check_table(table_name, min_rows=1)
        resp = self.qdb_plain.http_sql_query(
            f"select id, val from '{table_name}'")
        self.assertEqual(resp['dataset'], [[0, 0.5]])

    def test_qwp_websocket_schema_evolution_across_batches(self):
        self._require_qwp_ws()
        table_name = uuid.uuid4().hex
        sender_id = 'py-schema-' + uuid.uuid4().hex[:8]

        with tempfile.TemporaryDirectory(prefix='py-qwp-ws-schema-') as sf_dir:
            sender = qi.Sender.from_conf(self._mk_qwpws_conf(
                sender_id,
                sf_dir,
                reconnect_max_duration_millis=30000,
                close_flush_timeout_millis=30000))
            try:
                sender.establish()
                sender.row(
                    table_name,
                    symbols={'host': 'r1'},
                    at=qi.TimestampMicros(1_700_000_000_000_000))
                first_fsn = sender.flush_and_get_fsn()
                self.assertEqual(first_fsn, 0)

                sender.row(
                    table_name,
                    symbols={'host': 'r2'},
                    columns={'qty': 2, 'note': 'two'},
                    at=qi.TimestampMicros(1_700_000_000_001_000))
                second_fsn = sender.flush_and_get_fsn()
                self.assertEqual(second_fsn, 1)

                sender.row(
                    table_name,
                    symbols={'host': 'r3'},
                    columns={'note': 'three'},
                    at=qi.TimestampMicros(1_700_000_000_002_000))
                third_fsn = sender.flush_and_get_fsn()
                self.assertEqual(third_fsn, 2)

                self.assertTrue(sender.await_acked_fsn(third_fsn, 30000))
                sender.close_drain()
            finally:
                sender.close(False)

            self.assertEqual(self._sfa_file_count(sf_dir, sender_id), 0)

        self.qdb_plain.retry_check_table(table_name, min_rows=3)
        resp = self.qdb_plain.http_sql_query(
            f"select host, qty, note from '{table_name}' order by host")
        self.assertEqual(resp['dataset'], [
            ['r1', None, None],
            ['r2', 2, 'two'],
            ['r3', None, 'three']])

    def test_qwp_websocket_schema_rejection_reports_terminal_and_retains_sf(self):
        self._require_qwp_ws()
        table_name = uuid.uuid4().hex
        sender_id = 'py-reject-' + uuid.uuid4().hex[:8]
        self.qdb_plain.http_sql_query(
            f'CREATE TABLE "{table_name}" '
            '(id LONG, px DOUBLE, bad LONG, timestamp TIMESTAMP) '
            'TIMESTAMP(timestamp) PARTITION BY DAY WAL')

        with tempfile.TemporaryDirectory(prefix='py-qwp-ws-reject-') as sf_dir:
            sender = qi.Sender.from_conf(self._mk_qwpws_conf(
                sender_id,
                sf_dir,
                reconnect_max_duration_millis=30000,
                close_flush_timeout_millis=30000))
            try:
                sender.establish()
                sender.row(
                    table_name,
                    columns={'id': 0, 'px': 10.5},
                    at=qi.TimestampMicros(1_700_000_000_000_000))
                first_fsn = sender.flush_and_get_fsn()

                sender.row(
                    table_name,
                    columns={'id': 1, 'bad': 'not-a-long'},
                    at=qi.TimestampMicros(1_700_000_000_001_000))
                rejected_fsn = sender.flush_and_get_fsn()

                sender.row(
                    table_name,
                    columns={'id': 2, 'px': 20.5},
                    at=qi.TimestampMicros(1_700_000_000_002_000))
                final_fsn = sender.flush_and_get_fsn()

                self.assertEqual(
                    (first_fsn, rejected_fsn, final_fsn),
                    (0, 1, 2))
                with self.assertRaises(
                        qi.QuestDBServerRejectionError) as raised:
                    sender.await_acked_fsn(final_fsn, 30000)
                diagnostic = raised.exception.sender_error
                self.assertIsNotNone(diagnostic)
                self.assertEqual(
                    diagnostic.category,
                    qi.SenderErrorCategory.SchemaMismatch)
                self.assertEqual(
                    diagnostic.applied_policy,
                    qi.SenderErrorPolicy.Terminal)
                self.assertEqual(diagnostic.status, 0x03)
                self.assertEqual(diagnostic.from_fsn, rejected_fsn)
                self.assertEqual(diagnostic.to_fsn, rejected_fsn)
            finally:
                sender.close(False)

            self.assertGreater(self._sfa_file_count(sf_dir, sender_id), 0)

        self.qdb_plain.retry_check_table(table_name, min_rows=1)
        resp = self.qdb_plain.http_sql_query(
            f"select id, px from '{table_name}' order by id")
        if self.qdb_plain.version < FIRST_QWP_GAP_HALT_RELEASE:
            self.assertIn([0, 10.5], resp['dataset'])
        else:
            self.assertEqual(resp['dataset'], [[0, 10.5]])

    def test_qwp_websocket_error_handler_does_not_hide_terminal_error(self):
        self._require_qwp_ws()
        table_name = uuid.uuid4().hex
        sender_id = 'py-reject-cb-' + uuid.uuid4().hex[:8]
        self.qdb_plain.http_sql_query(
            f'CREATE TABLE "{table_name}" '
            '(id LONG, px DOUBLE, bad LONG, timestamp TIMESTAMP) '
            'TIMESTAMP(timestamp) PARTITION BY DAY WAL')

        captured = []
        with tempfile.TemporaryDirectory(prefix='py-qwp-ws-reject-cb-') as sf_dir:
            sender = qi.Sender.from_conf(
                self._mk_qwpws_conf(
                    sender_id,
                    sf_dir,
                    reconnect_max_duration_millis=30000,
                    close_flush_timeout_millis=30000),
                error_handler=captured.append)
            try:
                sender.establish()
                sender.row(
                    table_name,
                    columns={'id': 0, 'px': 10.5},
                    at=qi.TimestampMicros(1_700_000_000_000_000))
                sender.flush_and_get_fsn()
                sender.row(
                    table_name,
                    columns={'id': 1, 'bad': 'not-a-long'},
                    at=qi.TimestampMicros(1_700_000_000_001_000))
                rejected_fsn = sender.flush_and_get_fsn()
                sender.row(
                    table_name,
                    columns={'id': 2, 'px': 20.5},
                    at=qi.TimestampMicros(1_700_000_000_002_000))
                final_fsn = sender.flush_and_get_fsn()
                with self.assertRaises(
                        qi.QuestDBServerRejectionError) as raised:
                    sender.await_acked_fsn(final_fsn, 30000)
                diagnostic = raised.exception.sender_error
                self.assertIsNotNone(diagnostic)
                self.assertEqual(
                    diagnostic.category,
                    qi.SenderErrorCategory.SchemaMismatch)
                self.assertEqual(
                    diagnostic.applied_policy,
                    qi.SenderErrorPolicy.Terminal)
                self.assertEqual(diagnostic.status, 0x03)
                self.assertEqual(diagnostic.from_fsn, rejected_fsn)
                self.assertEqual(diagnostic.to_fsn, rejected_fsn)
                deadline = time.monotonic() + 10.0
                while not captured and time.monotonic() < deadline:
                    time.sleep(0.05)
                self.assertTrue(
                    captured,
                    'error_handler was never invoked for the '
                    'terminal rejection')
                callback_diagnostic = captured[0]
                self.assertEqual(
                    callback_diagnostic.category, diagnostic.category)
                self.assertEqual(
                    callback_diagnostic.applied_policy,
                    diagnostic.applied_policy)
                self.assertEqual(
                    callback_diagnostic.status, diagnostic.status)
                self.assertEqual(
                    callback_diagnostic.from_fsn, diagnostic.from_fsn)
                self.assertEqual(
                    callback_diagnostic.to_fsn, diagnostic.to_fsn)
            finally:
                sender.close(False)

            self.assertGreater(self._sfa_file_count(sf_dir, sender_id), 0)

        self.qdb_plain.retry_check_table(table_name, min_rows=1)
        resp = self.qdb_plain.http_sql_query(
            f"select id, px from '{table_name}' order by id")
        if self.qdb_plain.version < FIRST_QWP_GAP_HALT_RELEASE:
            self.assertIn([0, 10.5], resp['dataset'])
        else:
            self.assertEqual(resp['dataset'], [[0, 10.5]])

    def test_qwp_websocket_raising_error_handler_is_swallowed(self):
        # A handler that raises must not crash the process, leak the
        # exception across the C callback boundary, or prevent the
        # terminal rejection from surfacing on the next sender call.
        self._require_qwp_ws()
        table_name = uuid.uuid4().hex
        sender_id = 'py-raise-cb-' + uuid.uuid4().hex[:8]
        self.qdb_plain.http_sql_query(
            f'CREATE TABLE "{table_name}" '
            '(id LONG, px DOUBLE, bad LONG, timestamp TIMESTAMP) '
            'TIMESTAMP(timestamp) PARTITION BY DAY WAL')

        invoked = []

        def raising_handler(error):
            invoked.append(error)
            raise RuntimeError('handler exploded')

        with tempfile.TemporaryDirectory(
                prefix='py-qwp-ws-raise-cb-') as sf_dir:
            sender = qi.Sender.from_conf(
                self._mk_qwpws_conf(
                    sender_id,
                    sf_dir,
                    reconnect_max_duration_millis=30000,
                    close_flush_timeout_millis=30000),
                error_handler=raising_handler)
            try:
                sender.establish()
                sender.row(
                    table_name,
                    columns={'id': 0, 'px': 10.5},
                    at=qi.TimestampMicros(1_700_000_000_000_000))
                sender.flush_and_get_fsn()
                sender.row(
                    table_name,
                    columns={'id': 1, 'bad': 'not-a-long'},
                    at=qi.TimestampMicros(1_700_000_000_001_000))
                rejected_fsn = sender.flush_and_get_fsn()
                with self.assertRaises(qi.QuestDBServerRejectionError):
                    sender.await_acked_fsn(rejected_fsn, 30000)
                deadline = time.monotonic() + 10.0
                while not invoked and time.monotonic() < deadline:
                    time.sleep(0.05)
                self.assertTrue(
                    invoked,
                    'raising error_handler was never invoked')
            finally:
                sender.close(False)

    def test_qwp_websocket_schema_fuzz(self):
        self._require_qwp_fuzz()
        seed = self._qwp_fuzz_seed()
        rng = random.Random(seed)
        sys.stderr.write(f'[qwp-python-fuzz seed] {seed:#x}\n')
        sys.stderr.flush()

        rows = int(os.environ.get('QDB_PY_QWP_FUZZ_ROWS', '64'))
        rows = max(8, rows)
        table_count = int(os.environ.get('QDB_PY_QWP_FUZZ_TABLES', '2'))
        table_count = max(1, table_count)
        tables = [
            'py_qwp_fuzz_' + uuid.uuid4().hex[:8]
            for _ in range(table_count)]
        expected = {table: [] for table in tables}
        sender_id = 'py-fuzz-' + uuid.uuid4().hex[:8]
        base_ts = 1_700_000_100_000_000
        host_values = ['alpha', 'beta value', 'Zürich', '東京']
        region_values = ['eu', 'us west', 'apac', 'münchen']
        note_values = ['plain', 'two words', '你好世界', 'emoji-🚀']

        def append_row(sender, table, row_id, include_all=False):
            row_ts = base_ts + row_id
            row = {
                'id': row_id,
                'host': None,
                'region': None,
                'qty': None,
                'px': None,
                'note': None,
                'event_ts': None,
                'timestamp': self._micros_to_qdb_date(row_ts)}
            symbols = {}
            columns = {'id': row_id}

            if include_all or rng.randrange(4) != 0:
                value = rng.choice(host_values)
                symbols['host'] = value
                row['host'] = value
            if include_all or rng.randrange(2) == 0:
                value = rng.choice(region_values)
                symbols['region'] = value
                row['region'] = value

            candidates = [
                ('qty', lambda: rng.randrange(-1000, 1000)),
                ('px', lambda: round(rng.uniform(-1000.0, 1000.0), 6)),
                ('note', lambda: rng.choice(note_values) + f'-{row_id}'),
                ('event_ts', lambda: qi.TimestampMicros(row_ts + 123))]
            rng.shuffle(candidates)
            for name, value_factory in candidates:
                if include_all or rng.randrange(3) != 0:
                    value = value_factory()
                    columns[name] = value
                    row[name] = (
                        self._micros_to_qdb_date(value.value)
                        if isinstance(value, qi.TimestampMicros)
                        else value)

            sender.row(
                table,
                symbols=symbols,
                columns=columns,
                at=qi.TimestampMicros(row_ts))
            expected[table].append(row)

        with tempfile.TemporaryDirectory(prefix='py-qwp-ws-fuzz-') as sf_dir:
            sender = qi.Sender.from_conf(self._mk_qwpws_conf(
                sender_id,
                sf_dir,
                reconnect_max_duration_millis=30000,
                close_flush_timeout_millis=30000))
            last_fsn = None
            pending = 0
            try:
                sender.establish()
                next_flush_at = rng.randrange(3, 11)
                for row_id in range(rows):
                    table = (
                        tables[row_id % table_count]
                        if row_id < table_count
                        else rng.choice(tables))
                    append_row(sender, table, row_id)
                    pending += 1
                    if pending >= next_flush_at:
                        fsn = sender.flush_and_get_fsn()
                        self.assertIsNotNone(fsn)
                        if last_fsn is not None:
                            self.assertEqual(fsn, last_fsn + 1)
                        last_fsn = fsn
                        pending = 0
                        next_flush_at = rng.randrange(3, 11)

                for table in tables:
                    append_row(
                        sender,
                        table,
                        rows + tables.index(table),
                        include_all=True)
                    pending += 1

                if pending:
                    fsn = sender.flush_and_get_fsn()
                    self.assertIsNotNone(fsn)
                    if last_fsn is not None:
                        self.assertEqual(fsn, last_fsn + 1)
                    last_fsn = fsn

                self.assertIsNotNone(last_fsn)
                self.assertTrue(sender.await_acked_fsn(last_fsn, 30000))
                self.assertIsNone(sender.poll_error())
                self.assertEqual(sender.error_events_dropped(), 0)
                sender.close_drain()
            finally:
                sender.close(False)

            self.assertEqual(self._sfa_file_count(sf_dir, sender_id), 0)

        for table in tables:
            self.qdb_plain.retry_check_table(
                table,
                min_rows=len(expected[table]))
            resp = self.qdb_plain.http_sql_query(
                f"select id, host, region, qty, px, note, event_ts, timestamp "
                f"from '{table}' order by id")
            expected_rows = [
                [
                    row['id'],
                    row['host'],
                    row['region'],
                    row['qty'],
                    row['px'],
                    row['note'],
                    row['event_ts'],
                    row['timestamp']]
                for row in sorted(expected[table], key=lambda item: item['id'])]
            self.assertEqual(resp['dataset'], expected_rows)

    def test_qwp_udp_protocol_enum(self):
        self.assertEqual(qi.Protocol.parse('udp'), qi.Protocol.Udp)
        self.assertFalse(qi.Protocol.Udp.tls_enabled)

    def test_qwp_udp_basic(self):
        self._require_qwp_udp()
        table_name = uuid.uuid4().hex
        with self._mk_udp_sender() as sender:
            sender.row(
                table_name,
                symbols={'name_a': 'val_a'},
                columns={'name_b': True, 'name_c': 42, 'name_d': 2.5},
                at=qi.ServerTimestamp)
            self.assertEqual(bytes(sender), b'')
            self.assertGreater(len(sender), 0)
            sender.flush()
            self.assertEqual(len(sender), 0)

        resp = self.qdb_plain.retry_check_table(table_name, min_rows=1)
        exp_columns = [
            {'name': 'name_a', 'type': 'SYMBOL'},
            {'name': 'name_b', 'type': 'BOOLEAN'},
            {'name': 'name_c', 'type': 'LONG'},
            {'name': 'name_d', 'type': 'DOUBLE'},
            {'name': 'timestamp', 'type': 'TIMESTAMP'}]
        self.assertEqual(resp['columns'], exp_columns)
        scrubbed_dataset = [row[:-1] for row in resp['dataset']]
        self.assertEqual(scrubbed_dataset, [['val_a', True, 42, 2.5]])

    def test_qwp_udp_from_conf_with_opts(self):
        self._require_qwp_udp()
        table_name = uuid.uuid4().hex
        conf = self._mk_udp_conf(max_datagram_size=1200, multicast_ttl=2)
        with qi.Sender.from_conf(conf) as sender:
            self.assertEqual(sender.auto_flush_bytes, 1200)
            sender.row(
                table_name,
                columns={'price': 1.5},
                at=qi.ServerTimestamp)

        resp = self.qdb_plain.retry_check_table(table_name, min_rows=1)
        scrubbed_dataset = [row[:-1] for row in resp['dataset']]
        self.assertEqual(scrubbed_dataset, [[1.5]])

    def test_qwp_udp_from_conf_override(self):
        self._require_qwp_udp()
        table_name = uuid.uuid4().hex
        conf = self._mk_udp_conf()
        with qi.Sender.from_conf(
                conf,
                max_datagram_size=1200,
                multicast_ttl=2) as sender:
            self.assertEqual(sender.auto_flush_bytes, 1200)
            sender.row(
                table_name,
                columns={'price': 2.5},
                at=qi.ServerTimestamp)

        resp = self.qdb_plain.retry_check_table(table_name, min_rows=1)
        scrubbed_dataset = [row[:-1] for row in resp['dataset']]
        self.assertEqual(scrubbed_dataset, [[2.5]])

    def test_qwp_udp_from_env_override(self):
        self._require_qwp_udp()
        table_name = uuid.uuid4().hex
        old_conf = os.environ.get('QDB_CLIENT_CONF')
        os.environ['QDB_CLIENT_CONF'] = self._mk_udp_conf()
        try:
            with qi.Sender.from_env(
                    max_datagram_size=1200,
                    multicast_ttl=2) as sender:
                self.assertEqual(sender.auto_flush_bytes, 1200)
                sender.row(
                    table_name,
                    columns={'price': 4.5},
                    at=qi.ServerTimestamp)
        finally:
            if old_conf is None:
                del os.environ['QDB_CLIENT_CONF']
            else:
                os.environ['QDB_CLIENT_CONF'] = old_conf

        resp = self.qdb_plain.retry_check_table(table_name, min_rows=1)
        scrubbed_dataset = [row[:-1] for row in resp['dataset']]
        self.assertEqual(scrubbed_dataset, [[4.5]])

    def test_qwp_udp_from_conf_override_conflict(self):
        self._require_qwp_udp()
        conf = self._mk_udp_conf(max_datagram_size=1200)
        with self.assertRaisesRegex(
                ValueError,
                r'"max_datagram_size" is already present in the conf_str'):
            qi.Sender.from_conf(conf, max_datagram_size=900)

    def test_qwp_udp_auto_flush_bytes_default(self):
        self._require_qwp_udp()
        sender = self._mk_udp_sender()
        try:
            self.assertTrue(sender.auto_flush)
            self.assertEqual(sender.auto_flush_bytes, 1400)
        finally:
            sender.close(flush=False)

        sender = self._mk_udp_sender(max_datagram_size=1200)
        try:
            self.assertEqual(sender.auto_flush_bytes, 1200)
        finally:
            sender.close(flush=False)

    def test_qwp_udp_new_buffer(self):
        self._require_qwp_udp()
        table_name = uuid.uuid4().hex
        with self._mk_udp_sender(init_buf_size=1024, max_name_len=64) as sender:
            buffer = sender.new_buffer()
            self.assertEqual(buffer.init_buf_size, 1024)
            self.assertEqual(buffer.max_name_len, 64)
            buffer.row(
                table_name,
                columns={'price': 3.5},
                at=qi.ServerTimestamp)
            self.assertEqual(bytes(buffer), b'')
            self.assertGreater(len(buffer), 0)
            sender.flush(buffer)
            self.assertEqual(len(buffer), 0)

        resp = self.qdb_plain.retry_check_table(table_name, min_rows=1)
        scrubbed_dataset = [row[:-1] for row in resp['dataset']]
        self.assertEqual(scrubbed_dataset, [[3.5]])

    def test_qwp_udp_new_buffer_requires_establish(self):
        self._require_qwp_udp()
        sender = self._mk_udp_sender()
        try:
            with self.assertRaisesRegex(
                    qi.QuestDBError,
                    r"new_buffer\(\) can't be called before establish\(\)"):
                sender.new_buffer()
        finally:
            sender.close(flush=False)

    def test_qwp_udp_new_buffer_rejects_closed_sender(self):
        self._require_qwp_udp()
        sender = self._mk_udp_sender()
        sender.close(flush=False)
        with self.assertRaisesRegex(
                qi.QuestDBError,
                r"new_buffer\(\) can't be called: Sender is closed"):
            sender.new_buffer()

    def test_qwp_udp_transaction_rejected(self):
        self._require_qwp_udp()
        with self._mk_udp_sender() as sender:
            with self.assertRaisesRegex(
                    qi.QuestDBError,
                    'Transactions are only supported for ILP/HTTP'):
                sender.transaction('trades')

    def test_qwp_udp_protocol_version_rejected(self):
        self._require_qwp_udp()
        with self._mk_udp_sender() as sender:
            with self.assertRaisesRegex(
                    qi.QuestDBError,
                    'protocol_version is not applicable for QWP/UDP senders'):
                sender.protocol_version

    def test_qwp_udp_example(self):
        self._require_qwp_udp()
        table_name = uuid.uuid4().hex
        example_path = PROJ_ROOT / 'examples' / 'qwp_udp.py'
        spec = importlib.util.spec_from_file_location(
            'questdb_qwp_udp_example',
            example_path)
        self.assertIsNotNone(spec)
        self.assertIsNotNone(spec.loader)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)

        mod.example(
            host=self.qdb_plain.host,
            port=self.qdb_plain.qwp_udp_port,
            table_name=table_name)

        resp = self.qdb_plain.retry_check_table(table_name, min_rows=1)
        scrubbed_dataset = [row[:-1] for row in resp['dataset']]
        self.assertEqual(scrubbed_dataset, [['ETH-USD', 'sell', 2615.54, 0.00044]])

    @unittest.skipIf(not pd, 'pandas not installed')
    def test_qwp_udp_dataframe(self):
        self._require_qwp_udp()
        table_name = uuid.uuid4().hex
        df = pd.DataFrame({
            'name_a': ['a', 'b'],
            'name_b': [True, False],
            'name_c': [1, 2],
            'name_d': [1.5, 2.5],
        })
        with self._mk_udp_sender() as sender:
            sender.dataframe(df, table_name=table_name, at=qi.ServerTimestamp)

        resp = self.qdb_plain.retry_check_table(table_name, min_rows=2)
        exp_columns = [
            {'name': 'name_a', 'type': 'VARCHAR'},
            {'name': 'name_b', 'type': 'BOOLEAN'},
            {'name': 'name_c', 'type': 'LONG'},
            {'name': 'name_d', 'type': 'DOUBLE'},
            {'name': 'timestamp', 'type': 'TIMESTAMP'}]
        self.assertEqual(resp['columns'], exp_columns)
        scrubbed_dataset = [row[:-1] for row in resp['dataset']]
        self.assertEqual(scrubbed_dataset, [['a', True, 1, 1.5], ['b', False, 2, 2.5]])

    def test_qwp_udp_timestamp_columns(self):
        self._require_qwp_udp()
        table_name = uuid.uuid4().hex
        ts_micros = qi.TimestampMicros(1_700_000_000_000_000)
        ts_nanos = qi.TimestampNanos(1_700_000_000_123_456_789)
        dt = datetime.datetime(2024, 6, 15, 12, 0, 0, tzinfo=datetime.timezone.utc)
        with self._mk_udp_sender() as sender:
            sender.row(
                table_name,
                columns={
                    'ts_micros': ts_micros,
                    'ts_nanos': ts_nanos,
                    'ts_dt': dt},
                at=qi.TimestampNanos.now())
            sender.flush()

        resp = self.qdb_plain.retry_check_table(table_name, min_rows=1)
        col_types = {c['name']: c['type'] for c in resp['columns']}
        self.assertEqual(col_types['ts_micros'], 'TIMESTAMP')
        self.assertEqual(col_types['ts_nanos'], 'TIMESTAMP_NS')
        self.assertEqual(col_types['ts_dt'], 'TIMESTAMP')
        row = resp['dataset'][0]
        # ts_micros: 1_700_000_000_000_000 micros
        self.assertEqual(row[0], '2023-11-14T22:13:20.000000Z')
        # ts_dt: 2024-06-15T12:00:00Z
        self.assertEqual(row[2], '2024-06-15T12:00:00.000000Z')

    def test_qwp_udp_timestamp_columns_convert_into_existing_table_types(self):
        self._require_qwp_udp()
        micros_table = uuid.uuid4().hex
        nanos_table = uuid.uuid4().hex
        event_ts_us = 123_456
        event_ts_ns = 123_456_789
        row_ts_us = 1_700_000_000_000_123
        self.qdb_plain.http_sql_query(
            f'CREATE TABLE {micros_table} '
            f'(host SYMBOL, event_ts TIMESTAMP_NS, timestamp TIMESTAMP) '
            f'TIMESTAMP(timestamp) PARTITION BY DAY;')
        self.qdb_plain.http_sql_query(
            f'CREATE TABLE {nanos_table} '
            f'(host SYMBOL, event_ts TIMESTAMP, timestamp TIMESTAMP) '
            f'TIMESTAMP(timestamp) PARTITION BY DAY;')

        with self._mk_udp_sender() as sender:
            sender.row(
                micros_table,
                symbols={'host': 'micro'},
                columns={'event_ts': qi.TimestampMicros(event_ts_us)},
                at=qi.TimestampMicros(row_ts_us))
            sender.row(
                nanos_table,
                symbols={'host': 'nano'},
                columns={'event_ts': qi.TimestampNanos(event_ts_ns)},
                at=qi.TimestampMicros(row_ts_us))
            sender.flush()

        self.qdb_plain.retry_check_table(micros_table, min_rows=1)
        self.qdb_plain.retry_check_table(nanos_table, min_rows=1)
        micros_resp = self.qdb_plain.http_sql_query(
            f"select host, event_ts, timestamp from '{micros_table}'")
        nanos_resp = self.qdb_plain.http_sql_query(
            f"select host, event_ts, timestamp from '{nanos_table}'")
        self.assertEqual(micros_resp['dataset'], [[
            'micro',
            self._nanos_to_qdb_date(event_ts_us * 1000),
            self._micros_to_qdb_date(row_ts_us)]])
        self.assertEqual(nanos_resp['dataset'], [[
            'nano',
            self._micros_to_qdb_date(event_ts_ns // 1000),
            self._micros_to_qdb_date(row_ts_us)]])

    def test_qwp_udp_mixed_timestamp_precisions_rejected(self):
        self._require_qwp_udp()
        with self.assertRaisesRegex(
                qi.QuestDBError,
                'designated timestamp changes type within a batched table'):
            with self._mk_udp_sender() as sender:
                sender.row(
                    'mixed_ts_designated',
                    columns={'qty': 1},
                    at=qi.TimestampMicros(123_456))
                sender.row(
                    'mixed_ts_designated',
                    columns={'qty': 2},
                    at=qi.TimestampNanos(789_000))
                sender.flush()

        with self.assertRaisesRegex(
                qi.QuestDBError,
                'column "event_ts" changes type within a batched table'):
            with self._mk_udp_sender() as sender:
                sender.row(
                    'mixed_ts_column',
                    columns={'event_ts': qi.TimestampMicros(123_456)},
                    at=qi.ServerTimestamp)
                sender.row(
                    'mixed_ts_column',
                    columns={'event_ts': qi.TimestampNanos(789_000)},
                    at=qi.ServerTimestamp)
                sender.flush()

    def test_qwp_udp_f64_array(self):
        self._require_qwp_udp()
        if self.qdb_plain.version < FIRST_ARRAY_RELEASE:
            self.skipTest('old server does not support array')
        table_name = uuid.uuid4().hex
        array1 = np.array([[1.1, 2.2], [3.3, 4.4]], dtype=np.float64)
        array2 = array1.T  # non-contiguous
        with self._mk_udp_sender() as sender:
            sender.row(
                table_name,
                columns={
                    'arr_c': array1,
                    'arr_t': array2},
                at=qi.TimestampNanos.now())
            sender.flush()

        resp = self.qdb_plain.retry_check_table(table_name, min_rows=1)
        col_types = {c['name']: c['type'] for c in resp['columns']}
        self.assertEqual(col_types['arr_c'], 'ARRAY')
        self.assertEqual(col_types['arr_t'], 'ARRAY')
        scrubbed = [row[:-1] for row in resp['dataset']]
        self.assertEqual(scrubbed, [[[[1.1, 2.2], [3.3, 4.4]],
                                     [[1.1, 3.3], [2.2, 4.4]]]])

    def test_qwp_udp_decimal(self):
        self._require_qwp_udp()
        if self.qdb_plain.version < FIRST_DECIMAL_RELEASE:
            self.skipTest('old server does not support decimal')
        table_name = uuid.uuid4().hex
        self.qdb_plain.http_sql_query(
            f'CREATE TABLE {table_name} '
            f'(price DECIMAL(18,3), timestamp TIMESTAMP) '
            f'TIMESTAMP(timestamp) PARTITION BY DAY;')
        with self._mk_udp_sender() as sender:
            sender.row(
                table_name,
                columns={'price': decimal.Decimal('12345.678')},
                at=qi.TimestampNanos.now())
            sender.flush()

        resp = self.qdb_plain.retry_check_table(table_name, min_rows=1)
        exp_columns = [
            {'name': 'price', 'type': 'DECIMAL(18,3)'},
            {'name': 'timestamp', 'type': 'TIMESTAMP'}]
        self.assertEqual(resp['columns'], exp_columns)
        scrubbed = [row[:-1] for row in resp['dataset']]
        self.assertEqual(scrubbed, [['12345.678']])

    def test_qwp_udp_string_column(self):
        self._require_qwp_udp()
        table_name = uuid.uuid4().hex
        with self._mk_udp_sender() as sender:
            sender.row(
                table_name,
                columns={'label': 'hello world', 'value': 42},
                at=qi.TimestampNanos.now())
            sender.flush()

        resp = self.qdb_plain.retry_check_table(table_name, min_rows=1)
        col_types = {c['name']: c['type'] for c in resp['columns']}
        self.assertEqual(col_types['label'], 'VARCHAR')
        self.assertEqual(col_types['value'], 'LONG')
        scrubbed = [row[:-1] for row in resp['dataset']]
        self.assertEqual(scrubbed, [['hello world', 42]])

    def test_qwp_udp_auto_flush_bytes_triggers(self):
        self._require_qwp_udp()
        table_name = uuid.uuid4().hex
        # Close with flush=False so only auto-flushed rows reach the server;
        # a broken byte trigger leaves rows buffered and the check fails.
        sender = self._mk_udp_sender(
            max_datagram_size=200,
            auto_flush_rows=False,
            auto_flush_interval=False)
        sender.establish()
        try:
            self.assertEqual(sender.auto_flush_bytes, 200)
            for i in range(20):
                sender.row(
                    table_name,
                    symbols={'tag': f'v_{i}'},
                    columns={'value': i},
                    at=qi.TimestampNanos.now())
        finally:
            sender.close(flush=False)
        resp = self.qdb_plain.retry_check_table(table_name, min_rows=10)
        self.assertGreaterEqual(resp['count'], 10)

    def test_qwp_udp_auto_flush_rows_triggers(self):
        self._require_qwp_udp()
        table_name = uuid.uuid4().hex
        # Close with flush=False so only auto-flushed rows reach the server;
        # a broken row trigger leaves rows buffered and the check fails.
        sender = self._mk_udp_sender(
            auto_flush_rows=5,
            auto_flush_bytes=False,
            auto_flush_interval=False)
        sender.establish()
        try:
            for i in range(10):
                sender.row(
                    table_name,
                    columns={'value': i},
                    at=qi.TimestampNanos.now())
        finally:
            sender.close(flush=False)
        resp = self.qdb_plain.retry_check_table(table_name, min_rows=10)
        self.assertEqual(resp['count'], 10)

    def test_qwp_udp_auto_flush_disabled(self):
        self._require_qwp_udp()
        table_name = uuid.uuid4().hex
        sender = self._mk_udp_sender(auto_flush=False)
        sender.establish()
        try:
            for i in range(5):
                sender.row(
                    table_name,
                    columns={'value': i},
                    at=qi.TimestampNanos.now())
            self.assertGreater(len(sender), 0)
            sender.flush()
        finally:
            sender.close(flush=False)
        resp = self.qdb_plain.retry_check_table(table_name, min_rows=5)
        self.assertEqual(resp['count'], 5)

    def test_qwp_udp_multi_table(self):
        self._require_qwp_udp()
        t1 = uuid.uuid4().hex
        t2 = uuid.uuid4().hex
        with self._mk_udp_sender() as sender:
            sender.row(t1, columns={'x': 1}, at=qi.TimestampNanos.now())
            sender.row(t2, columns={'y': 2}, at=qi.TimestampNanos.now())
            sender.row(t1, columns={'x': 3}, at=qi.TimestampNanos.now())
            sender.flush()
        r1 = self.qdb_plain.retry_check_table(t1, min_rows=2)
        r2 = self.qdb_plain.retry_check_table(t2, min_rows=1)
        self.assertEqual(r1['count'], 2)
        self.assertEqual(r2['count'], 1)

    def test_qwp_udp_buffer_reuse_after_flush(self):
        self._require_qwp_udp()
        t1 = uuid.uuid4().hex
        t2 = uuid.uuid4().hex
        with self._mk_udp_sender() as sender:
            buf = sender.new_buffer()
            buf.row(t1, columns={'batch': 1}, at=qi.TimestampNanos.now())
            sender.flush(buf)
            self.assertEqual(len(buf), 0)
            buf.row(t2, columns={'batch': 2}, at=qi.TimestampNanos.now())
            sender.flush(buf)
        r1 = self.qdb_plain.retry_check_table(t1, min_rows=1)
        r2 = self.qdb_plain.retry_check_table(t2, min_rows=1)
        self.assertEqual([row[:-1] for row in r1['dataset']], [[1]])
        self.assertEqual([row[:-1] for row in r2['dataset']], [[2]])

    def test_qwp_udp_independent_buffers(self):
        self._require_qwp_udp()
        t1 = uuid.uuid4().hex
        t2 = uuid.uuid4().hex
        with self._mk_udp_sender() as sender:
            buf_a = sender.new_buffer()
            buf_b = sender.new_buffer()
            buf_a.row(t1, columns={'src': 'a'}, at=qi.TimestampNanos.now())
            buf_b.row(t2, columns={'src': 'b'}, at=qi.TimestampNanos.now())
            sender.flush(buf_a)
            self.assertEqual(len(buf_a), 0)
            self.assertGreater(len(buf_b), 0)
            sender.flush(buf_b)
        r1 = self.qdb_plain.retry_check_table(t1, min_rows=1)
        r2 = self.qdb_plain.retry_check_table(t2, min_rows=1)
        self.assertEqual([row[:-1] for row in r1['dataset']], [['a']])
        self.assertEqual([row[:-1] for row in r2['dataset']], [['b']])

    def test_qwp_udp_flush_clear_false(self):
        self._require_qwp_udp()
        table_name = uuid.uuid4().hex
        row_ts = qi.TimestampMicros(1_700_000_000_200_000)
        with self._mk_udp_sender() as sender:
            buf = sender.new_buffer()
            buf.row(table_name, columns={'val': 99}, at=row_ts)
            sender.flush(buf, clear=False)
            self.assertGreater(len(buf), 0)
            sender.flush(buf)
            self.assertEqual(len(buf), 0)
        resp = self.qdb_plain.retry_check_table(table_name, min_rows=2)
        self.assertEqual(resp['dataset'], [
            [99, self._micros_to_qdb_date(row_ts.value)],
            [99, self._micros_to_qdb_date(row_ts.value)]])

    def test_qwp_udp_unicode(self):
        self._require_qwp_udp()
        table_name = uuid.uuid4().hex
        with self._mk_udp_sender() as sender:
            sender.row(
                table_name,
                symbols={'city': 'Zürich'},
                columns={'greeting': '你好世界', 'emoji': '🚀'},
                at=qi.TimestampNanos.now())
            sender.flush()
        resp = self.qdb_plain.retry_check_table(table_name, min_rows=1)
        row = resp['dataset'][0]
        self.assertEqual(row[0], 'Zürich')
        self.assertEqual(row[1], '你好世界')
        self.assertEqual(row[2], '🚀')

    def test_qwp_udp_none_columns_skipped(self):
        self._require_qwp_udp()
        table_name = uuid.uuid4().hex
        with self._mk_udp_sender() as sender:
            sender.row(
                table_name,
                symbols={'tag': 'a', 'skip_sym': None},
                columns={'present': 42, 'absent': None},
                at=qi.TimestampNanos.now())
            sender.flush()
        resp = self.qdb_plain.retry_check_table(table_name, min_rows=1)
        col_names = [c['name'] for c in resp['columns']]
        self.assertIn('present', col_names)
        self.assertNotIn('absent', col_names)
        self.assertNotIn('skip_sym', col_names)

    def test_qwp_udp_schema_expansion_backfills_rows(self):
        self._require_qwp_udp()
        table_name = uuid.uuid4().hex
        with self._mk_udp_sender() as sender:
            sender.row(table_name, symbols={'host': 'r1'}, at=qi.ServerTimestamp)
            sender.row(
                table_name,
                symbols={'host': 'r2'},
                columns={'qty': 2, 'note': 'two'},
                at=qi.ServerTimestamp)
            sender.row(
                table_name,
                symbols={'host': 'r3'},
                columns={'note': 'three'},
                at=qi.ServerTimestamp)
            sender.flush()

        self.qdb_plain.retry_check_table(table_name, min_rows=3)
        resp = self.qdb_plain.http_sql_query(
            f"select host, qty, note from '{table_name}' order by host")
        self.assertEqual(resp['dataset'], [
            ['r1', None, None],
            ['r2', 2, 'two'],
            ['r3', None, 'three']])

    def test_qwp_udp_sparse_boolean_columns_fill_false(self):
        self._require_qwp_udp()
        table_name = uuid.uuid4().hex
        with self._mk_udp_sender() as sender:
            sender.row(table_name, symbols={'host': 'r1'}, at=qi.ServerTimestamp)
            sender.row(
                table_name,
                symbols={'host': 'r2'},
                columns={'active': True},
                at=qi.ServerTimestamp)
            sender.row(
                table_name,
                symbols={'host': 'r3'},
                columns={'active': False},
                at=qi.ServerTimestamp)
            sender.flush()

        self.qdb_plain.retry_check_table(table_name, min_rows=3)
        resp = self.qdb_plain.http_sql_query(
            f"select host, active from '{table_name}' order by host")
        self.assertEqual(resp['dataset'], [
            ['r1', False],
            ['r2', True],
            ['r3', False]])

    def test_qwp_udp_sparse_numeric_and_timestamp_columns_fill_null(self):
        self._require_qwp_udp()
        table_name = uuid.uuid4().hex
        event_ts = qi.TimestampMicros(123_456)
        with self._mk_udp_sender() as sender:
            sender.row(table_name, symbols={'host': 'r1'}, at=qi.ServerTimestamp)
            sender.row(
                table_name,
                symbols={'host': 'r2'},
                columns={'qty': 2, 'event_ts': event_ts},
                at=qi.ServerTimestamp)
            sender.row(
                table_name,
                symbols={'host': 'r3'},
                columns={'temp': 33.5},
                at=qi.ServerTimestamp)
            sender.flush()

        self.qdb_plain.retry_check_table(table_name, min_rows=3)
        resp = self.qdb_plain.http_sql_query(
            f"select host, qty, temp, event_ts from '{table_name}' order by host")
        self.assertEqual(resp['dataset'], [
            ['r1', None, None, None],
            ['r2', 2, None, self._micros_to_qdb_date(event_ts.value)],
            ['r3', None, 33.5, None]])

    def test_qwp_udp_empty_flush(self):
        self._require_qwp_udp()
        with self._mk_udp_sender() as sender:
            self.assertEqual(len(sender), 0)
            sender.flush()
            sender.flush()
            buf = sender.new_buffer()
            sender.flush(buf)

    def test_qwp_udp_double_close(self):
        self._require_qwp_udp()
        sender = self._mk_udp_sender()
        sender.establish()
        sender.close(flush=False)
        sender.close(flush=False)

    def test_qwp_udp_context_manager_flush_on_exit(self):
        self._require_qwp_udp()
        table_name = uuid.uuid4().hex
        with self._mk_udp_sender(auto_flush=False) as sender:
            sender.row(
                table_name, columns={'val': 7},
                at=qi.TimestampNanos.now())
        resp = self.qdb_plain.retry_check_table(table_name, min_rows=1)
        self.assertEqual([row[:-1] for row in resp['dataset']], [[7]])

    def test_qwp_udp_server_vs_explicit_timestamp(self):
        self._require_qwp_udp()
        t1 = uuid.uuid4().hex
        t2 = uuid.uuid4().hex
        explicit_ts = qi.TimestampNanos(1_700_000_000_000_000_000)
        with self._mk_udp_sender() as sender:
            sender.row(t1, columns={'x': 1}, at=qi.ServerTimestamp)
            sender.row(t2, columns={'x': 2}, at=explicit_ts)
            sender.flush()
        r2 = self.qdb_plain.retry_check_table(t2, min_rows=1)
        ts = r2['dataset'][0][1]
        self.assertIn('2023-11-14', ts)

    def test_qwp_udp_many_rows(self):
        self._require_qwp_udp()
        table_name = uuid.uuid4().hex
        with self._mk_udp_sender() as sender:
            for i in range(500):
                sender.row(
                    table_name,
                    symbols={'batch': 'stress'},
                    columns={'seq': i, 'payload': f'row_{i:04d}'},
                    at=qi.TimestampNanos.now())
        resp = self.qdb_plain.retry_check_table(table_name, min_rows=500)
        self.assertEqual(resp['count'], 500)

    def test_qwp_udp_max_name_len(self):
        self._require_qwp_udp()
        with self._mk_udp_sender(max_name_len=20) as sender:
            buf = sender.new_buffer()
            buf.row('t', columns={'a' * 20: 1}, at=qi.ServerTimestamp)
            self.assertGreater(len(buf), 0)

            buf2 = sender.new_buffer()
            with self.assertRaises(qi.QuestDBError):
                buf2.row('t', columns={'a' * 21: 1}, at=qi.ServerTimestamp)

    def test_qwp_udp_standalone_buffer_reuse(self):
        self._require_qwp_udp()
        t1 = uuid.uuid4().hex
        t2 = uuid.uuid4().hex
        buf = qi.Buffer._new_qwp()
        buf.row(t1, columns={'round': 1}, at=qi.TimestampNanos.now())
        with self._mk_udp_sender() as sender:
            sender.flush(buf)
            self.assertEqual(len(buf), 0)
            buf.row(t2, columns={'round': 2}, at=qi.TimestampNanos.now())
            sender.flush(buf)
        r1 = self.qdb_plain.retry_check_table(t1, min_rows=1)
        r2 = self.qdb_plain.retry_check_table(t2, min_rows=1)
        self.assertEqual([row[:-1] for row in r1['dataset']], [[1]])
        self.assertEqual([row[:-1] for row in r2['dataset']], [[2]])

    def test_qwp_udp_auto_flush_interval(self):
        self._require_qwp_udp()
        table_name = uuid.uuid4().hex
        import time as _time
        # Close with flush=False so arrival reflects only the interval
        # trigger firing on the second row, not the context-manager close.
        sender = self._mk_udp_sender(
            auto_flush_rows=False,
            auto_flush_bytes=False,
            auto_flush_interval=500)
        sender.establish()
        try:
            sender.row(
                table_name, columns={'seq': 1},
                at=qi.TimestampNanos.now())
            self.assertGreater(len(sender), 0)
            _time.sleep(0.7)
            sender.row(
                table_name, columns={'seq': 2},
                at=qi.TimestampNanos.now())
        finally:
            sender.close(flush=False)
        resp = self.qdb_plain.retry_check_table(table_name, min_rows=1)
        self.assertGreaterEqual(resp['count'], 1)

    def test_qwp_udp_datagram_splitting(self):
        self._require_qwp_udp()
        table_name = uuid.uuid4().hex
        with self._mk_udp_sender(
                max_datagram_size=200,
                auto_flush=False) as sender:
            for i in range(30):
                sender.row(
                    table_name,
                    symbols={'tag': f'val_{i:03d}'},
                    columns={'seq': i, 'data': f'payload_{i:06d}'},
                    at=qi.TimestampNanos.now())
            sender.flush()
        resp = self.qdb_plain.retry_check_table(table_name, min_rows=30)
        self.assertEqual(resp['count'], 30)

    def test_qwp_udp_interleave_with_http(self):
        self._require_qwp_udp()
        t_http = uuid.uuid4().hex
        t_qwp = uuid.uuid4().hex
        with qi.Sender(
                qi.Protocol.Http, self.qdb_plain.host,
                self.qdb_plain.http_server_port) as http_sender, \
             self._mk_udp_sender() as qwp_sender:
            http_sender.row(
                t_http, columns={'src': 'http', 'val': 1},
                at=qi.TimestampNanos.now())
            qwp_sender.row(
                t_qwp, columns={'src': 'qwp', 'val': 2},
                at=qi.TimestampNanos.now())
            qwp_sender.flush()
        r_http = self.qdb_plain.retry_check_table(t_http, min_rows=1)
        r_qwp = self.qdb_plain.retry_check_table(t_qwp, min_rows=1)
        self.assertEqual(r_http['dataset'][0][0], 'http')
        self.assertEqual(r_qwp['dataset'][0][0], 'qwp')

    def test_qwp_udp_from_env(self):
        self._require_qwp_udp()
        table_name = uuid.uuid4().hex
        old = os.environ.get('QDB_CLIENT_CONF')
        os.environ['QDB_CLIENT_CONF'] = self._mk_udp_conf()
        try:
            with qi.Sender.from_env() as sender:
                sender.row(
                    table_name, columns={'val': 123},
                    at=qi.TimestampNanos.now())
                sender.flush()
        finally:
            if old is None:
                del os.environ['QDB_CLIENT_CONF']
            else:
                os.environ['QDB_CLIENT_CONF'] = old
        resp = self.qdb_plain.retry_check_table(table_name, min_rows=1)
        self.assertEqual(
            [row[:-1] for row in resp['dataset']], [[123]])

    def test_qwp_udp_sender_reuse(self):
        self._require_qwp_udp()
        t1 = uuid.uuid4().hex
        t2 = uuid.uuid4().hex
        with self._mk_udp_sender() as sender:
            sender.row(t1, columns={'session': 1},
                       at=qi.TimestampNanos.now())
            sender.flush()
        with self._mk_udp_sender() as sender:
            sender.row(t2, columns={'session': 2},
                       at=qi.TimestampNanos.now())
            sender.flush()
        r1 = self.qdb_plain.retry_check_table(t1, min_rows=1)
        r2 = self.qdb_plain.retry_check_table(t2, min_rows=1)
        self.assertEqual([row[:-1] for row in r1['dataset']], [[1]])
        self.assertEqual([row[:-1] for row in r2['dataset']], [[2]])

    def test_qwp_udp_large_string(self):
        self._require_qwp_udp()
        table_name = uuid.uuid4().hex
        big_str = 'x' * 1000
        with self._mk_udp_sender() as sender:
            sender.row(
                table_name, columns={'payload': big_str},
                at=qi.TimestampNanos.now())
            sender.flush()
        resp = self.qdb_plain.retry_check_table(table_name, min_rows=1)
        self.assertEqual(resp['dataset'][0][0], big_str)

    def test_qwp_udp_symbols_only(self):
        self._require_qwp_udp()
        table_name = uuid.uuid4().hex
        with self._mk_udp_sender() as sender:
            sender.row(
                table_name,
                symbols={'exchange': 'NYSE', 'ticker': 'AAPL'},
                at=qi.TimestampNanos.now())
            sender.flush()
        resp = self.qdb_plain.retry_check_table(table_name, min_rows=1)
        col_types = {c['name']: c['type'] for c in resp['columns']}
        self.assertEqual(col_types['exchange'], 'SYMBOL')
        self.assertEqual(col_types['ticker'], 'SYMBOL')
        self.assertEqual(resp['dataset'][0][0], 'NYSE')
        self.assertEqual(resp['dataset'][0][1], 'AAPL')

    def test_qwp_udp_mixed_timestamps(self):
        self._require_qwp_udp()
        table_name = uuid.uuid4().hex
        explicit = qi.TimestampNanos(1_700_000_000_000_000_000)
        with self._mk_udp_sender() as sender:
            sender.row(table_name, columns={'seq': 1},
                       at=qi.ServerTimestamp)
            sender.row(table_name, columns={'seq': 2}, at=explicit)
            sender.row(table_name, columns={'seq': 3},
                       at=qi.ServerTimestamp)
            sender.flush()
        resp = self.qdb_plain.retry_check_table(table_name, min_rows=3)
        self.assertEqual(resp['count'], 3)
        rows = sorted(resp['dataset'], key=lambda row: row[0])
        self.assertIn('2023-11-14', rows[1][1])

    @unittest.skipIf(not pd, 'pandas not installed')
    def test_qwp_udp_dataframe_ts_column(self):
        self._require_qwp_udp()
        table_name = uuid.uuid4().hex
        df = pd.DataFrame({
            'sensor': ['A', 'B'],
            'temp': [22.5, 23.1],
            'ts': pd.to_datetime(
                ['2024-01-01 12:00:00', '2024-01-01 12:01:00'],
                utc=True),
        })
        with self._mk_udp_sender() as sender:
            sender.dataframe(df, table_name=table_name, at='ts')
        resp = self.qdb_plain.retry_check_table(table_name, min_rows=2)
        self.assertEqual(resp['count'], 2)
        col_names = [c['name'] for c in resp['columns']]
        self.assertIn('timestamp', col_names)
        for row in resp['dataset']:
            self.assertIn('2024-01-01', row[-1])

    def test_qwp_udp_new_buffer_inherits_settings(self):
        self._require_qwp_udp()
        with self._mk_udp_sender(
                init_buf_size=2048, max_name_len=32) as sender:
            buf = sender.new_buffer()
            self.assertEqual(buf.init_buf_size, 2048)
            self.assertEqual(buf.max_name_len, 32)
            buf.row('t', columns={'a' * 32: 1}, at=qi.ServerTimestamp)
            self.assertGreater(len(buf), 0)
            with self.assertRaises(qi.QuestDBError):
                buf.row('t', columns={'a' * 33: 1}, at=qi.ServerTimestamp)

    def test_qwp_udp_ilp_buffer_rejected(self):
        self._require_qwp_udp()
        buf = qi.Buffer(protocol_version=2)
        buf.row('t', columns={'x': 1}, at=qi.ServerTimestamp)
        with self._mk_udp_sender() as sender:
            with self.assertRaisesRegex(
                    qi.QuestDBError, 'QWP sender requires a QWP buffer'):
                sender.flush(buf)

    def test_qwp_udp_buffer_rejected_by_http(self):
        self._require_qwp_udp()
        buf = qi.Buffer._new_qwp()
        buf.row('t', columns={'x': 1}, at=qi.ServerTimestamp)
        with qi.Sender(
                qi.Protocol.Http, self.qdb_plain.host,
                self.qdb_plain.http_server_port) as sender:
            with self.assertRaisesRegex(
                    qi.QuestDBError,
                    'ILP sender requires an ILP buffer'):
                sender.flush(buf)

    def test_qwp_udp_wrong_port_silent(self):
        """UDP flush to wrong port succeeds silently (fire-and-forget)."""
        self._require_qwp_udp()
        with qi.Sender(
                qi.Protocol.Udp,
                self.qdb_plain.host, 19007) as sender:
            sender.row('t', columns={'x': 1}, at=qi.TimestampNanos.now())
            sender.flush()  # no error — data goes nowhere

    def test_qwp_udp_unresolvable_host(self):
        """Unresolvable host fails at establish()."""
        self._require_qwp_udp()
        with self.assertRaisesRegex(qi.QuestDBError, 'Could not resolve'):
            with qi.Sender(
                    qi.Protocol.Udp,
                    'this.host.does.not.exist.invalid', 9007) as sender:
                pass

    def test_qwp_udp_wide_row(self):
        """50 columns + 5 symbols in a single row."""
        self._require_qwp_udp()
        table_name = uuid.uuid4().hex
        cols = {f'col_{i:02d}': float(i) for i in range(50)}
        syms = {f'sym_{i}': f'val_{i}' for i in range(5)}
        with self._mk_udp_sender() as sender:
            sender.row(table_name, symbols=syms, columns=cols,
                       at=qi.TimestampNanos.now())
            sender.flush()
        resp = self.qdb_plain.retry_check_table(table_name, min_rows=1)
        # 50 cols + 5 syms + 1 timestamp = 56
        self.assertEqual(len(resp['columns']), 56)

    def test_qwp_udp_row_ordering(self):
        """100 rows with explicit timestamps, split across datagrams."""
        self._require_qwp_udp()
        table_name = uuid.uuid4().hex
        n = 100
        base_ts = 1_700_000_000_000_000_000
        with self._mk_udp_sender(
                max_datagram_size=200, auto_flush=False) as sender:
            for i in range(n):
                sender.row(
                    table_name, columns={'seq': i},
                    at=qi.TimestampNanos(base_ts + i * 1000))
            sender.flush()
        resp = self.qdb_plain.retry_check_table(table_name, min_rows=n)
        seqs = sorted(row[0] for row in resp['dataset'])
        self.assertEqual(seqs, list(range(n)))

    def test_qwp_udp_tiny_datagram_rejected(self):
        """max_datagram_size=1: row exceeds datagram, flush errors."""
        self._require_qwp_udp()
        with self._mk_udp_sender(
                max_datagram_size=1, auto_flush=False) as sender:
            sender.row('t', columns={'x': 1}, at=qi.TimestampNanos.now())
            with self.assertRaisesRegex(
                    qi.QuestDBError, 'exceeds maximum datagram size'):
                sender.flush()

    def test_qwp_udp_rapid_fire_auto_flush(self):
        """2000 rows with pure auto-flush, no explicit flush.
        UDP may drop datagrams under load, so we accept >= 90% arrival."""
        self._require_qwp_udp()
        table_name = uuid.uuid4().hex
        n = 2000
        # Close with flush=False so arrival reflects continuous auto-flush,
        # not a single context-manager close flushing the whole buffer. The
        # unflushed residual is one datagram (<< 10% of n).
        sender = self._mk_udp_sender()
        sender.establish()
        try:
            for i in range(n):
                sender.row(
                    table_name, columns={'seq': i},
                    at=qi.TimestampNanos.now())
        finally:
            sender.close(flush=False)
        import time
        time.sleep(3)
        resp = self.qdb_plain.retry_check_table(
            table_name, min_rows=int(n * 0.9))
        self.assertGreaterEqual(resp['count'], int(n * 0.9))

    def test_qwp_udp_protocol_version_in_conf_rejected(self):
        self._require_qwp_udp()
        conf = self._mk_udp_conf(protocol_version=2)
        with self.assertRaisesRegex(
                qi.QuestDBError,
                'protocol_version.*not supported.*QWP'):
            qi.Sender.from_conf(conf)

    def test_qwp_udp_concurrent_senders(self):
        """Two senders from different threads to the same port."""
        self._require_qwp_udp()
        import threading
        t1 = uuid.uuid4().hex
        t2 = uuid.uuid4().hex
        errors = []
        def writer(table, n=50):
            try:
                with self._mk_udp_sender() as sender:
                    for i in range(n):
                        sender.row(
                            table, columns={'seq': i},
                            at=qi.TimestampNanos.now())
                    sender.flush()
            except Exception as e:
                errors.append(e)
        th1 = threading.Thread(target=writer, args=(t1,))
        th2 = threading.Thread(target=writer, args=(t2,))
        th1.start()
        th2.start()
        th1.join()
        th2.join()
        self.assertEqual(errors, [])
        r1 = self.qdb_plain.retry_check_table(t1, min_rows=50)
        r2 = self.qdb_plain.retry_check_table(t2, min_rows=50)
        self.assertEqual(r1['count'], 50)
        self.assertEqual(r2['count'], 50)

    def test_qwp_udp_double_establish_rejected(self):
        self._require_qwp_udp()
        sender = self._mk_udp_sender()
        sender.establish()
        try:
            with self.assertRaisesRegex(
                    qi.QuestDBError, "establish.*can't be called"):
                sender.establish()
        finally:
            sender.close(flush=False)

    def test_qwp_udp_establish_after_close_rejected(self):
        self._require_qwp_udp()
        sender = self._mk_udp_sender()
        sender.establish()
        sender.close(flush=False)
        with self.assertRaisesRegex(
                qi.QuestDBError, "establish.*can't be called"):
            sender.establish()

    def test_qwp_udp_decimal_zero_and_negative(self):
        self._require_qwp_udp()
        if self.qdb_plain.version < FIRST_DECIMAL_RELEASE:
            self.skipTest('old server does not support decimal')
        table_name = uuid.uuid4().hex
        self.qdb_plain.http_sql_query(
            f'CREATE TABLE {table_name} '
            f'(val DECIMAL(18,3), timestamp TIMESTAMP) '
            f'TIMESTAMP(timestamp) PARTITION BY DAY;')
        with self._mk_udp_sender() as sender:
            sender.row(table_name,
                       columns={'val': decimal.Decimal('0.000')},
                       at=qi.TimestampNanos.now())
            sender.row(table_name,
                       columns={'val': decimal.Decimal('-0.000')},
                       at=qi.TimestampNanos.now())
            sender.row(table_name,
                       columns={'val': decimal.Decimal('-123456789.012')},
                       at=qi.TimestampNanos.now())
            sender.flush()
        resp = self.qdb_plain.retry_check_table(table_name, min_rows=3)
        vals = [row[0] for row in resp['dataset']]
        self.assertEqual(vals[0], '0.000')
        self.assertIn(vals[1], ('0.000', '-0.000'))
        self.assertEqual(vals[2], '-123456789.012')

    def test_qwp_udp_decimal_max_precision(self):
        self._require_qwp_udp()
        if self.qdb_plain.version < FIRST_DECIMAL_RELEASE:
            self.skipTest('old server does not support decimal')
        table_name = uuid.uuid4().hex
        self.qdb_plain.http_sql_query(
            f'CREATE TABLE {table_name} '
            f'(val DECIMAL(18,3), timestamp TIMESTAMP) '
            f'TIMESTAMP(timestamp) PARTITION BY DAY;')
        with self._mk_udp_sender() as sender:
            sender.row(table_name,
                       columns={'val': decimal.Decimal('999999999999999.999')},
                       at=qi.TimestampNanos.now())
            sender.row(table_name,
                       columns={'val': decimal.Decimal('0.001')},
                       at=qi.TimestampNanos.now())
            sender.flush()
        resp = self.qdb_plain.retry_check_table(table_name, min_rows=2)
        vals = [row[0] for row in resp['dataset']]
        self.assertEqual(vals[0], '999999999999999.999')
        self.assertEqual(vals[1], '0.001')

    def test_qwp_udp_decimal_nan_inf_rejected(self):
        self._require_qwp_udp()
        if self.qdb_plain.version < FIRST_DECIMAL_RELEASE:
            self.skipTest('old server does not support decimal')
        table_name = uuid.uuid4().hex
        self.qdb_plain.http_sql_query(
            f'CREATE TABLE {table_name} '
            f'(val DECIMAL(18,3), timestamp TIMESTAMP) '
            f'TIMESTAMP(timestamp) PARTITION BY DAY;')
        with self._mk_udp_sender() as sender:
            with self.assertRaises(qi.QuestDBError):
                sender.row(table_name,
                           columns={'val': decimal.Decimal('NaN')},
                           at=qi.TimestampNanos.now())
            with self.assertRaises(qi.QuestDBError):
                sender.row(table_name,
                           columns={'val': decimal.Decimal('Inf')},
                           at=qi.TimestampNanos.now())

    def test_qwp_udp_decimal_multiple_columns(self):
        self._require_qwp_udp()
        if self.qdb_plain.version < FIRST_DECIMAL_RELEASE:
            self.skipTest('old server does not support decimal')
        table_name = uuid.uuid4().hex
        self.qdb_plain.http_sql_query(
            f'CREATE TABLE {table_name} '
            f'(price DECIMAL(18,2), fee DECIMAL(18,6), timestamp TIMESTAMP) '
            f'TIMESTAMP(timestamp) PARTITION BY DAY;')
        with self._mk_udp_sender() as sender:
            sender.row(table_name,
                       columns={
                           'price': decimal.Decimal('199.99'),
                           'fee': decimal.Decimal('0.000123')},
                       at=qi.TimestampNanos.now())
            sender.flush()
        resp = self.qdb_plain.retry_check_table(table_name, min_rows=1)
        row = resp['dataset'][0]
        self.assertEqual(row[0], '199.99')
        self.assertEqual(row[1], '0.000123')

    @unittest.skipIf(not pyarrow, 'pyarrow not installed')
    @unittest.skipIf(not pd, 'pandas not installed')
    def test_qwp_udp_decimal_pyarrow_nulls(self):
        self._require_qwp_udp()
        if self.qdb_plain.version < FIRST_DECIMAL_RELEASE:
            self.skipTest('old server does not support decimal')
        table_name = uuid.uuid4().hex
        self.qdb_plain.http_sql_query(
            f'CREATE TABLE {table_name} '
            f'(val DECIMAL(18,3), seq LONG, timestamp TIMESTAMP) '
            f'TIMESTAMP(timestamp) PARTITION BY DAY;')
        df = pd.DataFrame({
            'val': pd.array(
                [decimal.Decimal('1.5'), None, decimal.Decimal('3.25')],
                dtype=pd.ArrowDtype(pyarrow.decimal128(18, 3))),
            'seq': [1, 2, 3],
        })
        with self._mk_udp_sender() as sender:
            sender.dataframe(df, table_name=table_name, at=qi.ServerTimestamp)
        resp = self.qdb_plain.retry_check_table(table_name, min_rows=3)
        vals = [row[0] for row in resp['dataset']]
        self.assertIn('1.500', vals)
        self.assertIn(None, vals)
        self.assertIn('3.250', vals)

    @unittest.skipIf(not pyarrow, 'pyarrow not installed')
    @unittest.skipIf(not pd, 'pandas not installed')
    def test_qwp_udp_decimal_pyarrow(self):
        self._require_qwp_udp()
        if self.qdb_plain.version < FIRST_DECIMAL_RELEASE:
            self.skipTest('old server does not support decimal')
        table_name = uuid.uuid4().hex
        self.qdb_plain.http_sql_query(
            f'CREATE TABLE {table_name} '
            f'(prices DECIMAL(18,3), timestamp TIMESTAMP) '
            f'TIMESTAMP(timestamp) PARTITION BY DAY;')
        df = pd.DataFrame({
            'prices': pd.array(
                [
                    decimal.Decimal('-99999.99'),
                    decimal.Decimal('-678'),
                ],
                dtype=pd.ArrowDtype(pyarrow.decimal128(18, 2))
            )
        })
        with self._mk_udp_sender() as sender:
            sender.dataframe(df, table_name=table_name, at=qi.ServerTimestamp)
        resp = self.qdb_plain.retry_check_table(table_name, min_rows=2)
        exp_columns = [
            {'name': 'prices', 'type': 'DECIMAL(18,3)'},
            {'name': 'timestamp', 'type': 'TIMESTAMP'}]
        self.assertEqual(resp['columns'], exp_columns)
        scrubbed = [row[:-1] for row in resp['dataset']]
        self.assertEqual(scrubbed, [['-99999.990'], ['-678.000']])

    def test_f64_arr(self):
        if self.qdb_plain.version < FIRST_ARRAY_RELEASE:
            self.skipTest('old server does not support array')
        table_name = uuid.uuid4().hex
        array1 = np.array(
            [
                [[1.1, 2.2], [3.3, 4.4]],
                [[5.5, 6.6], [7.7, 8.8]]
            ],
            dtype=np.float64
        )
        array2 = array1.T
        array3 = array1[::-1, ::-1]
        with qi.Sender('http', 'localhost', self.qdb_plain.http_server_port) as sender:
            sender.row(
                table_name,
                columns={
                    'f64_arr1': array1,
                    'f64_arr2': array2,
                    'f64_arr3': array3},
                at=qi.ServerTimestamp)
        resp = self.qdb_plain.retry_check_table(table_name)
        exp_columns = [{'dim': 3, 'elemType': 'DOUBLE', 'name': 'f64_arr1', 'type': 'ARRAY'},
                       {'dim': 3, 'elemType': 'DOUBLE', 'name': 'f64_arr2', 'type': 'ARRAY'},
                       {'dim': 3, 'elemType': 'DOUBLE', 'name': 'f64_arr3', 'type': 'ARRAY'},
                       {'name': 'timestamp', 'type': 'TIMESTAMP'}]
        self.assertEqual(resp['columns'], exp_columns)
        expected_data = [[[[[1.1, 2.2], [3.3, 4.4]], [[5.5, 6.6], [7.7, 8.8]]],
                          [[[1.1, 5.5], [3.3, 7.7]], [[2.2, 6.6], [4.4, 8.8]]],
                          [[[7.7, 8.8], [5.5, 6.6]], [[3.3, 4.4], [1.1, 2.2]]]]]
        scrubbed_data = [row[:-1] for row in resp['dataset']]
        self.assertEqual(scrubbed_data, expected_data)

    def test_decimal_py_obj(self):
        if self.qdb_plain.version < FIRST_DECIMAL_RELEASE:
            self.skipTest('old server does not support decimal')

        table_name = uuid.uuid4().hex
        self.qdb_plain.http_sql_query(f'CREATE TABLE {table_name} (dec_col DECIMAL(18,3), timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;')

        pending = None
        with qi.Sender('http', 'localhost', self.qdb_plain.http_server_port) as sender:
            sender.row(
                table_name,
                columns={
                    'dec_col': decimal.Decimal('12345.678')},
                at=qi.ServerTimestamp)
            pending = bytes(sender)
        
        resp = self.qdb_plain.retry_check_table(table_name, min_rows=1, log_ctx=pending)
        exp_columns = [{'name': 'dec_col', 'type': 'DECIMAL(18,3)'},
                       {'name': 'timestamp', 'type': 'TIMESTAMP'}]
        self.assertEqual(resp['columns'], exp_columns)
        expected_data = [['12345.678']]
        scrubbed_data = [row[:-1] for row in resp['dataset']]
        self.assertEqual(scrubbed_data, expected_data)

    @unittest.skipIf(not pyarrow, 'pyarrow not installed')
    @unittest.skipIf(not pd, 'pandas not installed')
    def test_decimal_pyarrow(self):
        if self.qdb_plain.version < FIRST_DECIMAL_RELEASE:
            self.skipTest('old server does not support decimal')

        table_name = uuid.uuid4().hex
        self.qdb_plain.http_sql_query(f'CREATE TABLE {table_name} (prices DECIMAL(18,3), timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;')

        df = pd.DataFrame({
            'prices': pd.array(
                [
                    decimal.Decimal('-99999.99'),
                    decimal.Decimal('-678'),
                ],
                dtype=pd.ArrowDtype(pyarrow.decimal128(18, 2))
            )
        })

        pending = None
        with qi.Sender('http', 'localhost', self.qdb_plain.http_server_port) as sender:
            sender.dataframe(df, table_name=table_name, at=qi.ServerTimestamp)
            pending = bytes(sender)

        resp = self.qdb_plain.retry_check_table(table_name, min_rows=2, log_ctx=pending)
        exp_columns = [{'name': 'prices', 'type': 'DECIMAL(18,3)'},
                       {'name': 'timestamp', 'type': 'TIMESTAMP'}]
        self.assertEqual(resp['columns'], exp_columns)
        expected_data = [
            ['-99999.990'],
            ['-678.000'],
        ]
        scrubbed_data = [row[:-1] for row in resp['dataset']]
        self.assertEqual(scrubbed_data, expected_data)


class TestEgressWithDatabase(unittest.TestCase):
    """Live-server coverage for ``QuestDB.query(...)``.

    Reuses ``TestWithDatabase`` fixture setup. The egress reader path
    is HTTP/QWP-only; we don't replicate the TLS+auth ingress matrix
    since the auth fixture's QWP/HTTP endpoint is unauthenticated
    (``http_auth=False``). Conf-string + TLS plumbing for egress is
    derived from the ingress side; if it breaks there the existing
    ingress matrix catches it.
    """

    @classmethod
    def setUpClass(cls):
        # Reuse the fixture lifecycle from TestWithDatabase.
        TestWithDatabase.setUpClass.__func__(cls)

    @classmethod
    def tearDownClass(cls):
        TestWithDatabase.tearDownClass.__func__(cls)

    # Borrowed rather than restated, like `setUpClass` above. A copy
    # of a version gate drifts from the one it copied, and the drift
    # reads as a passing run.
    _require_qwp_ws = TestWithDatabase._require_qwp_ws
    _require_qwp_row_types = TestWithDatabase._require_qwp_row_types

    def setUp(self):
        # Writes of the QWP-only column types live in
        # `TestEgressQwpRowTypes`, whose `setUp` asks for the QuestDB 10
        # those are supported on. This class holds the reads, which the
        # QWP beta serves too.
        self._require_qwp_ws()

    def _conf(self):
        return (f'ws::addr={self.qdb_plain.host}:'
                f'{self.qdb_plain.http_server_port};')

    def _exec(self, sql):
        return self.qdb_plain.http_sql_query(sql)

    def test_one_client_rows_dataframe_and_query(self):
        """One QuestDB config owns all three intended QWP paths."""
        table_name = 't_client_unified_' + uuid.uuid4().hex[:8]
        try:
            self._exec(
                f'CREATE TABLE {table_name} ('
                'ts TIMESTAMP, source SYMBOL, value LONG'
                ') TIMESTAMP(ts) PARTITION BY DAY WAL')
            frame = pd.DataFrame({
                'ts': pd.to_datetime(
                    [1_700_000_001_000_000], unit='us'),
                'source': pd.Categorical(['dataframe']),
                'value': np.array([2], dtype=np.int64),
            })

            with qi.QuestDB.from_conf(self._conf()) as client:
                with client.sender() as sender:
                    sender.row(
                        table_name,
                        symbols={'source': 'row'},
                        columns={'value': 1},
                        at=qi.TimestampNanos(
                            1_700_000_000_000_000_000))
                    sender.flush(wait=True)

                client.dataframe(
                    frame,
                    table_name=table_name,
                    symbols=['source'],
                    at='ts')
                self.qdb_plain.retry_check_table(table_name, min_rows=2)
                result = client.query(
                    f'SELECT source, value FROM {table_name} '
                    'ORDER BY value').to_arrow()

            self.assertEqual(
                result.column('source').to_pylist(), ['row', 'dataframe'])
            self.assertEqual(result.column('value').to_pylist(), [1, 2])
        finally:
            try:
                self._exec(f'DROP TABLE IF EXISTS {table_name}')
            except Exception:
                pass

    def test_standalone_ws_sender_dataframe_lands_rows(self):
        """A standalone ws ``Sender.dataframe()`` bulk-loads through a
        poolless direct connection opened from the sender's own config."""
        table_name = 't_standalone_ws_df_' + uuid.uuid4().hex[:8]
        try:
            self._exec(
                f'CREATE TABLE {table_name} ('
                'ts TIMESTAMP, source SYMBOL, value LONG'
                ') TIMESTAMP(ts) PARTITION BY DAY WAL')
            frame = pd.DataFrame({
                'ts': pd.to_datetime([1_700_000_003_000_000], unit='us'),
                'source': pd.Categorical(['standalone']),
                'value': np.array([7], dtype=np.int64),
            })
            with qi.Sender.from_conf(self._conf()) as sender:
                sender.dataframe(
                    frame,
                    table_name=table_name,
                    symbols=['source'],
                    at='ts')
            self.qdb_plain.retry_check_table(table_name, min_rows=1)
            resp = self._exec(f'SELECT source, value FROM {table_name}')
            self.assertEqual(resp['dataset'], [['standalone', 7]])
        finally:
            try:
                self._exec(f'DROP TABLE IF EXISTS {table_name}')
            except Exception:
                pass

    def test_pooled_sender_dataframe_lands_rows(self):
        """``PooledSender.dataframe()`` borrows a direct connection from the
        pool for the call and commits on return; the lease stays usable for
        row-buffered writes afterwards."""
        table_name = 't_pooled_ws_df_' + uuid.uuid4().hex[:8]
        try:
            self._exec(
                f'CREATE TABLE {table_name} ('
                'ts TIMESTAMP, source SYMBOL, value LONG'
                ') TIMESTAMP(ts) PARTITION BY DAY WAL')
            frame = pd.DataFrame({
                'ts': pd.to_datetime([1_700_000_004_000_000], unit='us'),
                'source': pd.Categorical(['pooled']),
                'value': np.array([8], dtype=np.int64),
            })
            with qi.QuestDB.from_conf(self._conf()) as client:
                with client.sender() as sender:
                    sender.dataframe(
                        frame,
                        table_name=table_name,
                        symbols=['source'],
                        at='ts')
                    sender.row(
                        table_name,
                        symbols={'source': 'pooled'},
                        columns={'value': 9},
                        at=qi.TimestampNanos(1_700_000_005_000_000_000))
                    sender.flush(wait=True)
            self.qdb_plain.retry_check_table(table_name, min_rows=2)
            resp = self._exec(
                f'SELECT source, value FROM {table_name} ORDER BY value')
            self.assertEqual(resp['dataset'], [['pooled', 8], ['pooled', 9]])
        finally:
            try:
                self._exec(f'DROP TABLE IF EXISTS {table_name}')
            except Exception:
                pass

    def test_type_coverage_round_trip(self):
        """One row, every QuestDB type we can express in SQL, read back
        via ``QuestDB.query``. Single WAL apply, one query, per-column
        assertions on Arrow dtype and value.

        Decimal / Array are deferred: their SQL literal syntax varies
        across QuestDB versions and they're better verified once
        ingress writes them too.
        """
        import pyarrow as pa
        table_name = 't_egress_types_' + uuid.uuid4().hex[:8]
        try:
            self._exec(
                f'CREATE TABLE {table_name} ('
                'ts TIMESTAMP, '
                'b BOOLEAN, by BYTE, sh SHORT, i INT, lg LONG, '
                'fl FLOAT, db DOUBLE, '
                'ts_ns TIMESTAMP_NS, dt DATE, '
                'sym SYMBOL, vc VARCHAR, st STRING, ch CHAR, '
                'uu UUID, l256 LONG256, ip IPV4, gh GEOHASH(8c)'
                ') TIMESTAMP(ts) PARTITION BY DAY WAL')
            self._exec(
                f"INSERT INTO {table_name} VALUES ("
                "'2024-01-01T00:00:00.000000Z', "
                "true, 7, 700, 70000, 7000000000, "
                "3.5, 6.5, "
                "'2024-01-01T00:00:00.123456789Z', "
                "'2024-01-02', "
                "'AAA', 'varchar-value', 'string-value', 'C', "
                "'11111111-2222-3333-4444-555555555555', "
                "'0x0001020304050607080910111213141516171819202122232425262728293031', "
                "'192.168.1.10', "
                "'s00twy01'"
                ")")
            self.qdb_plain.retry_check_table(table_name, min_rows=1)

            with qi.QuestDB.from_conf(self._conf()) as client:
                table = client.query(
                    f'SELECT * FROM {table_name}').to_arrow()

            self.assertEqual(table.num_rows, 1)
            sch = table.schema
            # Numeric / boolean primitives.
            self.assertEqual(sch.field('b').type, pa.bool_())
            self.assertEqual(sch.field('by').type, pa.int8())
            self.assertEqual(sch.field('sh').type, pa.int16())
            self.assertEqual(sch.field('i').type, pa.int32())
            self.assertEqual(sch.field('lg').type, pa.int64())
            self.assertEqual(sch.field('fl').type, pa.float32())
            self.assertEqual(sch.field('db').type, pa.float64())
            # Temporal.
            self.assertEqual(
                sch.field('ts').type,
                pa.timestamp('us', tz='UTC'))
            self.assertEqual(
                sch.field('ts_ns').type,
                pa.timestamp('ns', tz='UTC'))
            self.assertEqual(
                sch.field('dt').type,
                pa.timestamp('ms', tz='UTC'))
            # Strings.
            self.assertEqual(
                sch.field('sym').type,
                pa.dictionary(pa.uint32(), pa.utf8()))
            self.assertEqual(sch.field('vc').type, pa.utf8())
            self.assertEqual(sch.field('st').type, pa.utf8())
            self.assertEqual(sch.field('ch').type, pa.uint16())
            # Fixed-size / extension. UUID is surfaced as pyarrow's
            # registered Arrow `arrow.uuid` extension type (storage =
            # FixedSizeBinary(16)).
            uu_type = sch.field('uu').type
            if isinstance(uu_type, pa.BaseExtensionType):
                self.assertEqual(uu_type.extension_name, 'arrow.uuid')
                self.assertEqual(uu_type.storage_type, pa.binary(16))
            else:
                self.assertEqual(uu_type, pa.binary(16))
            self.assertEqual(
                sch.field('l256').type, pa.binary(32))
            self.assertEqual(sch.field('ip').type, pa.uint32())
            # Geohash precision_bits=40 (8 chars × 5 bits) → int64.
            self.assertEqual(sch.field('gh').type, pa.int64())

            # Spot-check a few values.
            row = table.to_pylist()[0]
            self.assertIs(row['b'], True)
            self.assertEqual(row['by'], 7)
            self.assertEqual(row['sh'], 700)
            self.assertEqual(row['i'], 70000)
            self.assertEqual(row['lg'], 7000000000)
            self.assertAlmostEqual(row['fl'], 3.5)
            self.assertAlmostEqual(row['db'], 6.5)
            self.assertEqual(row['sym'], 'AAA')
            self.assertEqual(row['vc'], 'varchar-value')
            self.assertEqual(row['st'], 'string-value')
            self.assertEqual(row['ch'], ord('C'))
            # UUID storage is canonical RFC 4122 big-endian, so it equals
            # `uuid.UUID.bytes`. pyarrow surfaces the cell raw or wrapped
            # in a `uuid.UUID` depending on whether it has the `arrow.uuid`
            # extension registered.
            expect_uuid = uuid.UUID('11111111-2222-3333-4444-555555555555')
            raw_uu = (row['uu'] if isinstance(row['uu'], bytes)
                      else row['uu'].bytes)
            self.assertEqual(raw_uu, expect_uuid.bytes)

            # The pyarrow-free `to_pandas` decoder builds `uuid.UUID`
            # objects from the same bytes; it has its own reader path, so
            # check it agrees rather than assuming it does.
            with qi.QuestDB.from_conf(self._conf()) as client:
                pdf = client.query(
                    f'SELECT uu FROM {table_name}').to_pandas()
            self.assertEqual(pdf['uu'][0], expect_uuid)
        finally:
            try:
                self._exec(f'DROP TABLE IF EXISTS {table_name}')
            except Exception:
                pass

    def test_numpy_uuid_decode_across_rows_and_nulls(self):
        """`_numpy_uuid_chunk` walks the column with raw pointer
        arithmetic, a row stride and a byte swap of each half. One
        non-null row exercises none of that: not the stride past row 0,
        not the validity bitmap, not a batch boundary. Read the same
        column through both decoders -- the pyarrow-free one and the
        Arrow one, which share no code -- and require them to agree
        across many rows, mixed nulls, and batch-at-a-time reads.
        """
        table_name = 't_uuid_rows_' + uuid.uuid4().hex[:8]
        # Values that differ in every 64-bit half, so a stride or swap
        # that is off by a row or a half shows up as a mismatch rather
        # than as the same bytes read twice.
        rows = 257
        expected = [
            None if i % 7 == 3
            else uuid.UUID(
                int=((i * 0x0123456789ABCDEF0FEDCBA987654321) % (1 << 128)))
            for i in range(rows)]
        try:
            self._exec(
                f'CREATE TABLE {table_name} (ts TIMESTAMP, uu UUID) '
                'TIMESTAMP(ts) PARTITION BY DAY WAL')
            values = ', '.join(
                "('2024-01-01T00:00:00Z', "
                + ('null)' if value is None else f"'{value}')")
                for value in expected)
            self._exec(f'INSERT INTO {table_name} VALUES {values}')
            self.qdb_plain.retry_check_table(table_name, min_rows=rows)

            sql = f'SELECT uu FROM {table_name}'
            with qi.QuestDB.from_conf(self._conf()) as client:
                native = list(client.query(sql).to_pandas()['uu'])
            with qi.QuestDB.from_conf(self._conf()) as client:
                arrow = client.query(sql).to_arrow().column('uu').to_pylist()
            with qi.QuestDB.from_conf(self._conf()) as client:
                batched = [
                    value
                    for batch in client.query(sql).iter_pandas()
                    for value in batch['uu']]

            def norm(value):
                if value is None or value != value:
                    return None
                if isinstance(value, uuid.UUID):
                    return value
                return uuid.UUID(bytes=value)

            self.assertEqual(len(native), rows)
            self.assertEqual([norm(v) for v in native], expected)
            self.assertEqual([norm(v) for v in arrow], expected)
            self.assertEqual([norm(v) for v in batched], expected)
        finally:
            try:
                self._exec(f'DROP TABLE IF EXISTS {table_name}')
            except Exception:
                pass

    def test_empty_result(self):
        """A SELECT matching zero rows. Per the QWP egress spec the server
        still ships one zero-row RESULT_BATCH carrying the schema, so the
        result is an empty DataFrame that keeps its columns and types."""
        import pandas as pd
        table_name = 't_egress_empty_' + uuid.uuid4().hex[:8]
        try:
            self._exec(
                f'CREATE TABLE {table_name} '
                '(ts TIMESTAMP, x LONG) '
                'TIMESTAMP(ts) PARTITION BY DAY WAL')
            with qi.QuestDB.from_conf(self._conf()) as client:
                pdf = client.query(
                    f'SELECT * FROM {table_name} WHERE x = 1'
                ).to_pandas()
            self.assertIsInstance(pdf, pd.DataFrame)
            self.assertEqual(len(pdf), 0)
            self.assertEqual(list(pdf.columns), ['ts', 'x'])
        finally:
            try:
                self._exec(f'DROP TABLE IF EXISTS {table_name}')
            except Exception:
                pass

    def test_polars_from_arrow_consumes_capsule(self):
        """``QuestDB.query`` exposes ``__arrow_c_stream__`` directly off
        the Rust cursor, so polars can consume it without pyarrow being
        the import-time mediator. Pins that contract: the polars frame
        round-trips the rows and our lazy ``_PYARROW`` global stays
        unset by the call."""
        try:
            import polars as pl
        except ImportError:
            self.skipTest('polars not installed')
        table_name = 't_egress_polars_' + uuid.uuid4().hex[:8]
        try:
            self._exec(
                f'CREATE TABLE {table_name} '
                '(ts TIMESTAMP, lg LONG, vc VARCHAR) '
                'TIMESTAMP(ts) PARTITION BY DAY WAL')
            self._exec(
                f"INSERT INTO {table_name} VALUES "
                f"('2024-01-01T00:00:00Z', 42, 'hello'), "
                f"('2024-01-02T00:00:00Z', 7, 'world')")
            self.qdb_plain.retry_check_table(table_name, min_rows=2)
            with qi.QuestDB.from_conf(self._conf()) as client:
                with client.query(
                        f'SELECT lg, vc FROM {table_name} ORDER BY lg DESC'
                        ) as result:
                    df = pl.from_arrow(result)
            self.assertEqual(df.shape, (2, 2))
            self.assertEqual(df['lg'].to_list(), [42, 7])
            self.assertEqual(df['vc'].to_list(), ['hello', 'world'])
        finally:
            try:
                self._exec(f'DROP TABLE IF EXISTS {table_name}')
            except Exception:
                pass

    def test_to_polars_and_iter_polars_symbol_categorical(self):
        """SYMBOL egresses as a polars ``Categorical`` (codes + dict via the
        registry, no per-row remap), nulls preserved. ``iter_polars`` over a
        multi-batch result stitches via ``pl.concat`` to the same frame —
        every batch shares one ``Categories`` identity."""
        try:
            import polars as pl
        except ImportError:
            self.skipTest('polars not installed')
        import numpy as np
        n = 100000
        table_name = 't_egress_iterpolars_' + uuid.uuid4().hex[:8]
        exp = [None if i % 11 == 0 else f'sym_{i % 100}' for i in range(n)]
        try:
            self._exec(
                f'CREATE TABLE {table_name} '
                '(ts TIMESTAMP, sym SYMBOL, v LONG) '
                'TIMESTAMP(ts) PARTITION BY DAY WAL')
            df = pd.DataFrame({
                'ts': pd.to_datetime(np.arange(n), unit='s', utc=True),
                'sym': pd.Series(exp, dtype='string[pyarrow]'),
                'v': np.arange(n, dtype=np.int64),
            })
            with qi.QuestDB.from_conf(self._conf()) as client:
                client.dataframe(
                    df, table_name=table_name, at='ts', symbols=['sym'])
            self.qdb_plain.retry_check_table(table_name, min_rows=n)
            sql = f'SELECT sym, v FROM {table_name} ORDER BY v'
            with qi.QuestDB.from_conf(self._conf()) as client:
                full = client.query(sql).to_polars()
            self.assertEqual(full.shape, (n, 2))
            self.assertIsInstance(full.schema['sym'], pl.Categorical)
            self.assertGreater(full['sym'].null_count(), 0)
            self.assertEqual(full['v'].to_list(), list(range(n)))
            self.assertEqual(full['sym'].cast(pl.Utf8).to_list(), exp)
            with qi.QuestDB.from_conf(self._conf()) as client:
                frames = list(client.query(sql).iter_polars())
            self.assertGreater(len(frames), 1)
            stitched = pl.concat(frames, how='vertical')
            self.assertIsInstance(stitched.schema['sym'], pl.Categorical)
            self.assertEqual(stitched['v'].to_list(), list(range(n)))
            self.assertEqual(stitched['sym'].cast(pl.Utf8).to_list(), exp)
        finally:
            try:
                self._exec(f'DROP TABLE IF EXISTS {table_name}')
            except Exception:
                pass

    def test_reentering_the_client_from_a_types_mapper(self):
        """A ``types_mapper`` runs the caller's code once per column per
        batch while the reader is still streaming, so it is a way back
        into this client that nothing else reaches.

        `QueryResult` sits on the re-entrancy grid's allow-list because
        the offline fixtures serve no read endpoint, and this is where
        that excuse is redeemed. It holds what the grid holds: the
        re-entered call answers -- cleanly or by refusing -- and the
        handle works afterwards. A hang or a dead interpreter is what
        this is watching for.

        Closing the result from inside its own read is the case row 15
        of `native_captures.md` records. The cursor handle nulls its
        pointer under the lock that every fetch re-reads it under, so
        the read that follows is refused rather than reading freed
        memory.
        """
        rows = 300
        table_name = 't_mapper_reentry_' + uuid.uuid4().hex[:8]
        try:
            self._exec(
                f'CREATE TABLE {table_name} '
                '(ts TIMESTAMP, lg LONG) '
                'TIMESTAMP(ts) PARTITION BY DAY WAL')
            self._exec(
                f'INSERT INTO {table_name} SELECT '
                "dateadd('s', (x - 1)::int, "
                "'2024-01-01T00:00:00.000000Z'::timestamp), x "
                f'FROM long_sequence({rows})')
            self.qdb_plain.retry_check_table(table_name, min_rows=rows)

            sql = f'SELECT ts, lg FROM {table_name} ORDER BY ts'
            # Small batches, so the mapper runs while there is still
            # more of the result to come.
            conf = self._conf() + 'max_batch_rows=64;'

            cases = (
                ('a second query',
                 lambda client, result: client.query('SELECT 1').to_pandas(),
                 True),
                ('reap_idle',
                 lambda client, result: client.reap_idle(),
                 True),
                ('closing the result being read',
                 lambda client, result: result.close(),
                 False),
                ('cancelling the result being read',
                 lambda client, result: result.cancel(),
                 False),
            )

            for label, reenter, read_completes in cases:
                with self.subTest(reentered=label):
                    seen = []
                    held = []

                    with qi.QuestDB.from_conf(conf) as client:
                        def mapper(arrow_type):
                            if not seen:
                                try:
                                    reenter(client, held[0])
                                except qi.QuestDBError as exc:
                                    seen.append(('refused', str(exc)))
                                else:
                                    seen.append(('clean', ''))
                            return None

                        total = 0
                        batches = 0
                        result = client.query(sql)
                        held.append(result)
                        try:
                            for batch in result.iter_pandas(
                                    types_mapper=mapper):
                                total += len(batch)
                                batches += 1
                        except qi.QuestDBError:
                            # Pulling the cursor out from under the read
                            # ends it. Refusing is the good answer.
                            self.assertFalse(read_completes, label)

                        # The handle still works once the read is over.
                        self.assertEqual(
                            len(client.query('SELECT 1').to_pandas()), 1)

                    self.assertEqual(len(seen), 1, 'the mapper never ran')
                    self.assertIn(seen[0][0], ('refused', 'clean'), seen)
                    if read_completes:
                        self.assertGreater(
                            batches, 1, 'the read was not streamed')
                        self.assertEqual(total, rows)
                    else:
                        self.assertLessEqual(total, rows)
        finally:
            self._exec(f'DROP TABLE IF EXISTS {table_name}')

    def test_a_batch_disagreeing_with_the_pinned_schema_is_refused(self):
        """The NumPy backend decodes every batch after the first against
        the first one's columns, and the claim it hands back names them
        too. A batch that disagrees would be decoded with the wrong
        dtype and described by a claim that does not match its own
        values.

        A result whose schema really changes mid-stream cannot be
        arranged from a test, so the pin is perturbed instead: the
        check is a comparison, and a good batch against a doctored pin
        exercises it exactly as a doctored batch against a good pin
        would. Each of the four things it compares gets its own case,
        because a check that fires on the count alone would pass a test
        that only ever moved the count -- and the byte widths agree for
        any same-width type swap, which is what left this open.
        """
        rows = 256
        table_name = 't_egress_drift_' + uuid.uuid4().hex[:8]
        try:
            self._exec(
                f'CREATE TABLE {table_name} '
                '(ts TIMESTAMP, ip IPV4, gh GEOHASH(4c), lg LONG) '
                'TIMESTAMP(ts) PARTITION BY DAY WAL')
            self._exec(
                f'INSERT INTO {table_name} SELECT '
                "dateadd('s', (x - 1)::int, "
                "'2024-01-01T00:00:00.000000Z'::timestamp), "
                "'1.2.3.4'::ipv4, #u33d, x "
                f'FROM long_sequence({rows})')
            self.qdb_plain.retry_check_table(table_name, min_rows=rows)

            sql = f'SELECT ip, gh, lg FROM {table_name}'
            conf = self._conf() + 'max_batch_rows=64;'

            with qi.QuestDB.from_conf(conf) as client:
                probe = client.query(sql).iter_pandas()
                next(probe)
                meta, names = qi._debug_numpy_pinned_meta(probe)
            col_names, kinds, scales, precision, has_symbol = meta
            gh_index = list(col_names).index('gh')

            def dropped_column():
                return ((col_names[:-1], kinds[:-1], scales[:-1],
                         precision[:-1], has_symbol), names[:-1])

            def renamed_column():
                doctored = list(names)
                doctored[0] = b'not_' + doctored[0]
                return (meta, doctored)

            def changed_kind():
                doctored = list(kinds)
                # Any other kind will do; the point is that it differs.
                doctored[0] = kinds[0] + 1
                return ((col_names, doctored, scales, precision,
                         has_symbol), names)

            def changed_geohash_precision():
                doctored = list(precision)
                doctored[gh_index] = (precision[gh_index] or 20) + 5
                return ((col_names, kinds, scales, doctored,
                         has_symbol), names)

            cases = {
                'a column dropped': dropped_column,
                'a column renamed': renamed_column,
                'a column retyped': changed_kind,
                'a geohash precision changed': changed_geohash_precision,
            }
            for label, build in cases.items():
                with self.subTest(drift=label):
                    doctored_meta, doctored_names = build()
                    with qi.QuestDB.from_conf(conf) as client:
                        stream = client.query(sql).iter_pandas()
                        qi._debug_numpy_force_pin(
                            stream, tuple(doctored_meta),
                            list(doctored_names))
                        with self.assertRaises(qi.QuestDBError) as caught:
                            next(stream)
                    self.assertEqual(
                        caught.exception.code,
                        qi.QuestDBErrorCode.SchemaDrift,
                        f'{label}: wrong error code')
                    self.assertIn(
                        'schema changed between batches',
                        str(caught.exception))

            # A pin that agrees is not refused -- otherwise the four
            # cases above would pass against a check that always fires.
            with qi.QuestDB.from_conf(conf) as client:
                stream = client.query(sql).iter_pandas()
                qi._debug_numpy_force_pin(stream, meta, list(names))
                first = next(stream)
            self.assertEqual(len(first.columns), len(col_names))
        finally:
            self._exec(f'DROP TABLE IF EXISTS {table_name}')

    def test_iter_pandas_shares_one_round_trip_claim(self):
        """Every batch of a streaming read carries the same claim
        object. One schema covers the whole result, so building the
        claim per batch re-walked the schema and re-froze one entry per
        column every time -- 1.2 ms a batch at 1024 columns on the
        pyarrow-backed variant, on the path a large result takes.
        Handing every batch the same claim is sound for the reason
        pandas can share one between copies of a frame: the claim
        declines to be copied and cannot be edited.
        """
        rows = 1000
        table_name = 't_egress_claim_' + uuid.uuid4().hex[:8]
        try:
            self._exec(
                f'CREATE TABLE {table_name} '
                '(ts TIMESTAMP, ip IPV4, gh GEOHASH(4c), lg LONG) '
                'TIMESTAMP(ts) PARTITION BY DAY WAL')
            # The rows are generated server-side. `_exec` carries the
            # statement in the query string of a GET, and a VALUES list
            # this long makes a URL the server rejects as too long.
            self._exec(
                f'INSERT INTO {table_name} SELECT '
                "dateadd('s', (x - 1)::int, "
                "'2024-01-01T00:00:00.000000Z'::timestamp), "
                "'1.2.3.4'::ipv4, #u33d, x "
                f'FROM long_sequence({rows})')
            self.qdb_plain.retry_check_table(table_name, min_rows=rows)

            sql = f'SELECT ip, gh, lg FROM {table_name}'
            # Small batches so the stream really has several of them.
            conf = self._conf() + 'max_batch_rows=128;'
            for backend in (None, 'pyarrow', 'numpy_nullable'):
                kwargs = {} if backend is None else {'dtype_backend': backend}
                with self.subTest(dtype_backend=backend):
                    with qi.QuestDB.from_conf(conf) as client:
                        claims = [
                            batch.attrs['questdb'] for batch
                            in client.query(sql).iter_pandas(**kwargs)]
                    self.assertGreater(len(claims), 1)
                    for claim in claims[1:]:
                        self.assertIs(claim, claims[0])

                    columns = claims[0]['columns']
                    self.assertEqual(columns['ip']['kind'], 'ipv4')
                    self.assertEqual(columns['gh']['kind'], 'geohash')
                    self.assertEqual(columns['gh']['precision_bits'], 20)
                    self.assertEqual(columns['lg']['kind'], 'long')

                    # The whole-result read says the same thing.
                    with qi.QuestDB.from_conf(conf) as client:
                        whole = client.query(sql).to_pandas(**kwargs)
                    self.assertEqual(whole.attrs['questdb'], claims[0])
        finally:
            try:
                self._exec(f'DROP TABLE IF EXISTS {table_name}')
            except Exception:
                pass

    def _make_table(self, table_name, rows):
        self._exec(
            f'CREATE TABLE {table_name} '
            '(ts TIMESTAMP, lg LONG) '
            'TIMESTAMP(ts) PARTITION BY DAY WAL')
        values = ', '.join(
            f"('2024-01-01T00:00:0{i % 10}.000000Z', {i})"
            for i in range(rows))
        self._exec(f'INSERT INTO {table_name} VALUES {values}')
        self.qdb_plain.retry_check_table(table_name, min_rows=rows)

    def test_query_result_single_use(self):
        """A ``QueryResult`` is single-use: a second materialisation, or
        any materialisation after ``close()``, raises ``InvalidApiCall``.
        Also pins the ``__arrow_c_stream__`` ``requested_schema``
        rejection."""
        table_name = 't_egress_single_' + uuid.uuid4().hex[:8]
        try:
            self._make_table(table_name, 1)
            sql = f'SELECT lg FROM {table_name}'
            with qi.QuestDB.from_conf(self._conf()) as client:
                result = client.query(sql)
                result.to_arrow()
                with self.assertRaises(qi.QuestDBError) as cm:
                    result.to_arrow()
                self.assertEqual(
                    cm.exception.code, qi.QuestDBErrorCode.InvalidApiCall)

                closed = client.query(sql)
                closed.close()
                with self.assertRaises(qi.QuestDBError):
                    closed.to_pandas()

                stream = client.query(sql)
                with self.assertRaises(NotImplementedError):
                    stream.__arrow_c_stream__(requested_schema=object())
                stream.close()
        finally:
            try:
                self._exec(f'DROP TABLE IF EXISTS {table_name}')
            except Exception:
                pass

    def test_query_binds(self):
        """Positional ``$1``..``$N`` bind parameters: the full supported
        type matrix round-trips through WHERE-clause equality, and the
        client-side rejections (container type, int overflow, unsupported
        value type) raise before any server round-trip."""
        table_name = 't_egress_binds_' + uuid.uuid4().hex[:8]
        try:
            self._exec(
                f'CREATE TABLE {table_name} '
                '(ts TIMESTAMP, lg LONG, dbl DOUBLE, sym SYMBOL, '
                'flag BOOLEAN, u UUID) '
                'TIMESTAMP(ts) PARTITION BY DAY WAL')
            bound_uuid = uuid.UUID('123e4567-e89b-12d3-a456-426614174000')
            self._exec(
                f'INSERT INTO {table_name} VALUES '
                f"('2024-01-01T00:00:01.000000Z', 7, 1.5, 'BTC-USD', "
                f"true, '{bound_uuid}')")
            self.qdb_plain.retry_check_table(table_name, min_rows=1)

            def count(sql, binds):
                with qi.QuestDB.from_conf(self._conf()) as client:
                    frame = client.query(sql, binds).to_pandas()
                return int(frame['n'][0])

            ts = datetime.datetime(
                2024, 1, 1, 0, 0, 1, tzinfo=datetime.timezone.utc)
            checks = [
                (f'SELECT count() AS n FROM {table_name} '
                 'WHERE lg = $1', [7], 1),
                (f'SELECT count() AS n FROM {table_name} '
                 'WHERE lg = $1', (8,), 0),
                (f'SELECT count() AS n FROM {table_name} '
                 'WHERE dbl = $1', [1.5], 1),
                (f'SELECT count() AS n FROM {table_name} '
                 'WHERE sym = $1', ['BTC-USD'], 1),
                (f'SELECT count() AS n FROM {table_name} '
                 'WHERE flag = $1', [True], 1),
                (f'SELECT count() AS n FROM {table_name} '
                 'WHERE ts = $1', [ts], 1),
                (f'SELECT count() AS n FROM {table_name} '
                 'WHERE ts = $1',
                 [qi.TimestampMicros(1_704_067_201_000_000)], 1),
                (f'SELECT count() AS n FROM {table_name} '
                 'WHERE ts = $1',
                 [qi.TimestampNanos(1_704_067_201_000_000_000)], 1),
                (f'SELECT count() AS n FROM {table_name} '
                 'WHERE u = $1', [bound_uuid], 1),
                (f'SELECT count() AS n FROM {table_name} '
                 'WHERE u = $1', [uuid.uuid4()], 0),
                (f'SELECT count() AS n FROM {table_name} '
                 'WHERE $1 IS NULL', [None], 1),
                (f'SELECT count() AS n FROM {table_name} '
                 'WHERE ts > $1 AND sym = $2',
                 [datetime.datetime(
                     2020, 1, 1, tzinfo=datetime.timezone.utc),
                  'BTC-USD'], 1),
            ]
            for sql, binds, expect in checks:
                self.assertEqual(count(sql, binds), expect, (sql, binds))

            with qi.QuestDB.from_conf(self._conf()) as client:
                with self.assertRaisesRegex(
                        TypeError, '"binds" must be a list or tuple'):
                    client.query('SELECT $1', {'a': 1})
                with self.assertRaises(OverflowError):
                    client.query(
                        f'SELECT count() AS n FROM {table_name} '
                        'WHERE lg = $1', [2 ** 63])
                with self.assertRaisesRegex(TypeError, r'\$1'):
                    client.query(
                        f'SELECT count() AS n FROM {table_name} '
                        'WHERE lg = $1', [object()])
                # `uuid.UUID.bytes` is a property, so a subclass can
                # hand back a buffer shorter than the 16 bytes the bind
                # reads. Caught here rather than read off the end.
                class ShortUuid(uuid.UUID):
                    @property
                    def bytes(self):
                        return b'\x01'

                with self.assertRaisesRegex(ValueError, r'expected 16'):
                    client.query(
                        f'SELECT count() AS n FROM {table_name} '
                        'WHERE u = $1', [ShortUuid(int=0)])
        finally:
            try:
                self._exec(f'DROP TABLE IF EXISTS {table_name}')
            except Exception:
                pass

    def test_execute(self):
        """``execute()`` runs statements without the drain ceremony:
        DDL and DML complete, the connection returns to the pool, a
        stray SELECT's output is discarded, binds are plumbed through,
        and a reader lease stays usable after interleaved statements."""
        table_name = 't_execute_' + uuid.uuid4().hex[:8]
        try:
            with qi.QuestDB.from_conf(self._conf()) as client:
                self.assertIsNone(client.execute(
                    f'CREATE TABLE {table_name} '
                    '(ts TIMESTAMP, lg LONG) '
                    'TIMESTAMP(ts) PARTITION BY DAY WAL'))
                self.assertIsNone(client.execute(
                    f'INSERT INTO {table_name} VALUES '
                    "('2024-01-01T00:00:00.000000Z', 7)"))
                self.qdb_plain.retry_check_table(table_name, min_rows=1)
                self.assertIsNone(client.execute(
                    f'SELECT * FROM {table_name} WHERE lg = $1', [7]))
                frame = client.query(
                    f'SELECT count() AS n FROM {table_name}').to_pandas()
                self.assertEqual(int(frame['n'][0]), 1)
                with client.reader() as r:
                    self.assertIsNone(r.execute(
                        f'INSERT INTO {table_name} VALUES '
                        "('2024-01-01T00:00:01.000000Z', 8)"))
                    self.qdb_plain.retry_check_table(
                        table_name, min_rows=2)
                    frame = r.query(
                        f'SELECT count() AS n FROM {table_name}'
                    ).to_pandas()
                    self.assertEqual(int(frame['n'][0]), 2)
        finally:
            try:
                self._exec(f'DROP TABLE IF EXISTS {table_name}')
            except Exception:
                pass

    def test_cancel_is_safe_and_idempotent(self):
        table_name = 't_egress_cancel_' + uuid.uuid4().hex[:8]
        try:
            self._make_table(table_name, 8)
            sql = f'SELECT lg FROM {table_name}'
            with qi.QuestDB.from_conf(self._conf()) as client:
                with client.query(sql) as result:
                    it = result.iter_arrow()
                    next(it)
                    result.cancel()
                    result.cancel()
                    try:
                        next(it)
                    except StopIteration:
                        pass
                    except qi.QuestDBError as exc:
                        self.assertEqual(
                            exc.code, qi.QuestDBErrorCode.Cancelled)
                    else:
                        self.fail(
                            'post-cancel pull returned another batch')

                closed = client.query(sql)
                closed.close()
                closed.cancel()
        finally:
            try:
                self._exec(f'DROP TABLE IF EXISTS {table_name}')
            except Exception:
                pass

    def test_capsule_path_no_leak(self):
        """Loop the native ``__arrow_c_stream__`` paths — full consume,
        abandoned (un-consumed) capsule, and empty result — and assert no
        ``QueryResult`` is leaked. Exercises the producer refcount dance
        and the capsule destructor under repetition for leak detectors."""
        import gc
        table_name = 't_egress_leak_' + uuid.uuid4().hex[:8]
        empty_name = 't_egress_leak_empty_' + uuid.uuid4().hex[:8]
        try:
            self._make_table(table_name, 4)
            self._exec(
                f'CREATE TABLE {empty_name} '
                '(ts TIMESTAMP, lg LONG) '
                'TIMESTAMP(ts) PARTITION BY DAY WAL')
            sql = f'SELECT lg FROM {table_name}'
            empty_sql = f'SELECT lg FROM {empty_name} WHERE lg = -1'
            with qi.QuestDB.from_conf(self._conf()) as client:
                gc.collect()
                before = sum(
                    1 for o in gc.get_objects()
                    if type(o) is qi.QueryResult)
                for _ in range(64):
                    client.query(sql).to_arrow()
                    abandoned = client.query(sql)
                    capsule = abandoned.__arrow_c_stream__()
                    del capsule
                    del abandoned
                    client.query(empty_sql).to_arrow()
                gc.collect()
                after = sum(
                    1 for o in gc.get_objects()
                    if type(o) is qi.QueryResult)
            self.assertEqual(after, before)
        finally:
            for name in (table_name, empty_name):
                try:
                    self._exec(f'DROP TABLE IF EXISTS {name}')
                except Exception:
                    pass

    def test_bad_sql_raises_ingress_error(self):
        """Server-side parse error surfaces as an ``QuestDBError`` from
        ``client.query`` with a usable message."""
        with qi.QuestDB.from_conf(self._conf()) as client:
            with self.assertRaises(qi.QuestDBError) as cm:
                client.query(
                    'SELECT * FROM nonexistent_table_xyz_abc_123'
                ).to_arrow()
        msg = str(cm.exception)
        # Don't pin the exact message — just check the user gets
        # something informative about the missing table.
        self.assertTrue(
            'nonexistent_table_xyz' in msg.lower()
            or 'does not exist' in msg.lower()
            or 'not found' in msg.lower()
            or 'invalid' in msg.lower(),
            f'expected error message to mention the missing table; '
            f'got {msg!r}')

    def test_dtype_backend_variants(self):
        """Validate the three `to_pandas` mappings: default (numpy
        primitives + new ``str`` dtype), ``pyarrow`` (ArrowDtype-backed),
        and ``numpy_nullable`` (pandas extension types).

        QuestDB BYTE column → int8/Int8Dtype/ArrowDtype(int8); LONG →
        int64/Int64Dtype/ArrowDtype(int64); VARCHAR → str/StringDtype/
        ArrowDtype(string). One iteration, three reads against the same
        table.
        """
        import pandas as pd
        import pyarrow as pa
        table_name = 't_egress_dtype_' + uuid.uuid4().hex[:8]
        try:
            self._exec(
                f'CREATE TABLE {table_name} '
                '(ts TIMESTAMP, lg LONG, vc VARCHAR) '
                'TIMESTAMP(ts) PARTITION BY DAY WAL')
            self._exec(
                f"INSERT INTO {table_name} VALUES "
                f"('2024-01-01T00:00:00Z', 42, 'hello')")
            self.qdb_plain.retry_check_table(table_name, min_rows=1)

            sql = f'SELECT lg, vc FROM {table_name}'
            with qi.QuestDB.from_conf(self._conf()) as client:
                default = client.query(sql).to_pandas()
                arrow_backed = client.query(sql).to_pandas(
                    dtype_backend='pyarrow')
                nullable = client.query(sql).to_pandas(
                    dtype_backend='numpy_nullable')

            # Default: object labels, numpy int64, pandas 3.0 str data.
            self.assertEqual(default.columns.dtype, np.dtype(object))
            self.assertEqual(default['lg'].dtype, np.int64)
            self.assertTrue(
                pd.api.types.is_string_dtype(default['vc'].dtype),
                f'expected str dtype, got {default["vc"].dtype!r}')

            # pyarrow: ArrowDtype-wrapped.
            self.assertIsInstance(
                arrow_backed['lg'].dtype, pd.ArrowDtype)
            self.assertEqual(
                arrow_backed['lg'].dtype.pyarrow_dtype, pa.int64())
            self.assertIsInstance(
                arrow_backed['vc'].dtype, pd.ArrowDtype)
            self.assertEqual(
                arrow_backed['vc'].dtype.pyarrow_dtype, pa.string())

            # numpy_nullable: pandas extension dtypes for primitives.
            self.assertIsInstance(nullable['lg'].dtype, pd.Int64Dtype)
            self.assertIsInstance(nullable['vc'].dtype, pd.StringDtype)

            # Mutual-exclusion + invalid-value rejection.
            with qi.QuestDB.from_conf(self._conf()) as client:
                with self.assertRaises(ValueError):
                    client.query(sql).to_pandas(
                        dtype_backend='pyarrow', types_mapper=lambda t: None)
                with self.assertRaises(ValueError):
                    client.query(sql).to_pandas(
                        dtype_backend='not_a_thing')
        finally:
            try:
                self._exec(f'DROP TABLE IF EXISTS {table_name}')
            except Exception:
                pass

    def test_symbol_column_to_pandas(self):
        """SYMBOL egresses as dictionary(uint32, utf8); pandas rejects
        unsigned dictionary indices, so to_pandas / iter_pandas must
        recast the index to int32. Covers the three dtype_backend
        variants plus the streaming iter_pandas path.
        """
        import pandas as pd
        import pyarrow as pa
        table_name = 't_egress_symbol_' + uuid.uuid4().hex[:8]
        try:
            self._exec(
                f'CREATE TABLE {table_name} '
                '(ts TIMESTAMP, sym SYMBOL, lg LONG) '
                'TIMESTAMP(ts) PARTITION BY DAY WAL')
            self._exec(
                f"INSERT INTO {table_name} VALUES "
                f"('2024-01-01T00:00:00Z', 'aa', 1), "
                f"('2024-01-01T00:00:01Z', 'bb', 2), "
                f"('2024-01-01T00:00:02Z', 'aa', 3)")
            self.qdb_plain.retry_check_table(table_name, min_rows=3)

            sql = f'SELECT sym, lg FROM {table_name} ORDER BY ts'

            # Wire format: SYMBOL arrives as a dictionary with an
            # unsigned index — the input that breaks pandas conversion.
            with qi.QuestDB.from_conf(self._conf()) as client:
                table = client.query(sql).to_arrow()
            sym_type = table.schema.field('sym').type
            self.assertTrue(
                pa.types.is_dictionary(sym_type),
                f'expected dictionary type for SYMBOL; got {sym_type}')
            self.assertTrue(
                pa.types.is_unsigned_integer(sym_type.index_type),
                f'expected unsigned dict index; got {sym_type.index_type}')

            # default to_pandas: must not raise; SYMBOL -> Categorical.
            with qi.QuestDB.from_conf(self._conf()) as client:
                default = client.query(sql).to_pandas()
            self.assertEqual(str(default['sym'].dtype), 'category')
            self.assertEqual(list(default['sym']), ['aa', 'bb', 'aa'])
            self.assertEqual(list(default['lg']), [1, 2, 3])

            # pyarrow + numpy_nullable backends: also must not raise.
            with qi.QuestDB.from_conf(self._conf()) as client:
                arrow_backed = client.query(sql).to_pandas(
                    dtype_backend='pyarrow')
            self.assertEqual(list(arrow_backed['sym']), ['aa', 'bb', 'aa'])
            with qi.QuestDB.from_conf(self._conf()) as client:
                nullable = client.query(sql).to_pandas(
                    dtype_backend='numpy_nullable')
            self.assertEqual(list(nullable['sym']), ['aa', 'bb', 'aa'])

            # streaming iter_pandas exercises the same per-batch recast.
            with qi.QuestDB.from_conf(self._conf()) as client:
                syms = []
                for chunk in client.query(sql).iter_pandas():
                    syms.extend(chunk['sym'].tolist())
            self.assertEqual(syms, ['aa', 'bb', 'aa'])
        finally:
            try:
                self._exec(f'DROP TABLE IF EXISTS {table_name}')
            except Exception:
                pass

    def test_numpy_egress_round_trip(self):
        """The native (default) ``to_pandas()`` output feeds straight back
        into ``QuestDB.dataframe`` and reproduces the same values for the
        types that round-trip through the numpy path
        (long/double/bool/varchar/symbol/timestamp). Also checks the
        ``df.attrs['questdb']`` round-trip metadata is attached.
        """
        import numpy as np
        src = 't_rt_src_' + uuid.uuid4().hex[:8]
        dst = 't_rt_dst_' + uuid.uuid4().hex[:8]
        cols = 'ts, lg, db, bl, vc, sym'
        try:
            self._exec(
                f'CREATE TABLE {src} '
                '(ts TIMESTAMP, lg LONG, db DOUBLE, bl BOOLEAN, '
                'vc VARCHAR, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL')
            self._exec(
                f"INSERT INTO {src} VALUES "
                f"('2024-01-01T00:00:00Z', 1, 1.5, true, 'aa', 's1'), "
                f"('2024-01-01T00:00:01Z', 2, 2.5, false, 'bb', 's2'), "
                f"('2024-01-01T00:00:02Z', 3, 3.5, true, 'cc', 's1')")
            self.qdb_plain.retry_check_table(src, min_rows=3)

            with qi.QuestDB.from_conf(self._conf()) as client:
                df = client.query(
                    f'SELECT {cols} FROM {src} ORDER BY ts').to_pandas()

            meta = df.attrs['questdb']['columns']
            self.assertEqual(meta['lg']['kind'], 'long')
            self.assertEqual(meta['db']['kind'], 'double')
            self.assertEqual(meta['sym']['kind'], 'symbol')
            self.assertEqual(meta['vc']['kind'], 'varchar')
            self.assertEqual(meta['ts']['kind'], 'timestamp')
            self.assertEqual(df['lg'].dtype, np.int64)
            self.assertEqual(str(df['sym'].dtype), 'category')

            self._exec(
                f'CREATE TABLE {dst} '
                '(ts TIMESTAMP, lg LONG, db DOUBLE, bl BOOLEAN, '
                'vc VARCHAR, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL')
            with qi.QuestDB.from_conf(self._conf()) as client:
                client.dataframe(df, table_name=dst, at='ts')
            self.qdb_plain.retry_check_table(dst, min_rows=3)

            with qi.QuestDB.from_conf(self._conf()) as client:
                back = client.query(
                    f'SELECT {cols} FROM {dst} ORDER BY ts').to_pandas()
            self.assertEqual(list(back['lg']), [1, 2, 3])
            self.assertEqual(list(back['db']), [1.5, 2.5, 3.5])
            self.assertEqual([bool(x) for x in back['bl']], [True, False, True])
            self.assertEqual(list(back['vc']), ['aa', 'bb', 'cc'])
            self.assertEqual(list(back['sym']), ['s1', 's2', 's1'])
        finally:
            for t in (src, dst):
                try:
                    self._exec(f'DROP TABLE IF EXISTS {t}')
                except Exception:
                    pass

    def test_numpy_egress_hybrid_nulls(self):
        """Default (hybrid) null handling: a nullable LONG with nulls
        becomes pandas ``Int64`` (``pd.NA``, analysis-safe); a LONG without
        nulls stays plain ``int64``; DOUBLE null -> ``float64`` NaN; VARCHAR
        null -> ``object`` None.
        """
        import pandas as pd
        import numpy as np
        table_name = 't_egress_hybrid_' + uuid.uuid4().hex[:8]
        try:
            self._exec(
                f'CREATE TABLE {table_name} '
                '(ts TIMESTAMP, lg LONG, lg2 LONG, db DOUBLE, vc VARCHAR) '
                'TIMESTAMP(ts) PARTITION BY DAY WAL')
            self._exec(
                f"INSERT INTO {table_name} VALUES "
                f"('2024-01-01T00:00:00Z', 7, 10, 1.5, 'x'), "
                f"('2024-01-01T00:00:01Z', NULL, 20, NULL, NULL)")
            self.qdb_plain.retry_check_table(table_name, min_rows=2)
            with qi.QuestDB.from_conf(self._conf()) as client:
                df = client.query(
                    f'SELECT lg, lg2, db, vc FROM {table_name} ORDER BY ts'
                ).to_pandas()
            # nullable LONG with a null -> Int64 (pd.NA)
            self.assertEqual(str(df['lg'].dtype), 'Int64')
            self.assertEqual(df['lg'].iloc[0], 7)
            self.assertTrue(df['lg'].iloc[1] is pd.NA)
            # LONG with no nulls -> plain int64
            self.assertEqual(df['lg2'].dtype, np.int64)
            self.assertEqual(list(df['lg2']), [10, 20])
            # DOUBLE null -> float64 NaN; VARCHAR null -> object None
            self.assertTrue(pd.api.types.is_float_dtype(df['db'].dtype))
            self.assertTrue(pd.isna(df['db'].iloc[1]))
            self.assertEqual(df['vc'].iloc[0], 'x')
            self.assertTrue(pd.isna(df['vc'].iloc[1]))
        finally:
            try:
                self._exec(f'DROP TABLE IF EXISTS {table_name}')
            except Exception:
                pass

    def test_numpy_egress_nullable_round_trip(self):
        """A nullable LONG round-trips through the default hybrid output:
        query -> to_pandas (Int64 with pd.NA) -> QuestDB.dataframe (normalised
        to object + validity) -> query reproduces the value and the null.
        """
        import pandas as pd
        src = 't_rtn_src_' + uuid.uuid4().hex[:8]
        dst = 't_rtn_dst_' + uuid.uuid4().hex[:8]
        try:
            self._exec(
                f'CREATE TABLE {src} (ts TIMESTAMP, lg LONG) '
                'TIMESTAMP(ts) PARTITION BY DAY WAL')
            self._exec(
                f"INSERT INTO {src} VALUES "
                f"('2024-01-01T00:00:00Z', 7), "
                f"('2024-01-01T00:00:01Z', NULL), "
                f"('2024-01-01T00:00:02Z', 9)")
            self.qdb_plain.retry_check_table(src, min_rows=3)
            with qi.QuestDB.from_conf(self._conf()) as client:
                df = client.query(
                    f'SELECT ts, lg FROM {src} ORDER BY ts').to_pandas()
            self.assertEqual(str(df['lg'].dtype), 'Int64')
            self._exec(
                f'CREATE TABLE {dst} (ts TIMESTAMP, lg LONG) '
                'TIMESTAMP(ts) PARTITION BY DAY WAL')
            with qi.QuestDB.from_conf(self._conf()) as client:
                client.dataframe(df, table_name=dst, at='ts')
            self.qdb_plain.retry_check_table(dst, min_rows=3)
            with qi.QuestDB.from_conf(self._conf()) as client:
                back = client.query(
                    f'SELECT lg FROM {dst} ORDER BY ts').to_pandas()
            self.assertEqual(back['lg'].iloc[0], 7)
            self.assertTrue(back['lg'].iloc[1] is pd.NA)
            self.assertEqual(back['lg'].iloc[2], 9)
        finally:
            for t in (src, dst):
                try:
                    self._exec(f'DROP TABLE IF EXISTS {t}')
                except Exception:
                    pass

    def _column_type(self, table_name, column):
        return dict(
            (row[0], row[1]) for row in
            self.qdb_plain.http_sql_query(
                f'SHOW COLUMNS FROM {table_name}')['dataset'])[column]

    def test_null_round_trip_per_dtype_backend(self):
        """Pin the null contract across the three dtype_backend variants.

        The QuestDB QWP egress wire carries an explicit validity bitmap
        (questdb-rs/src/egress/decoder.rs::ColumnBuffer.validity), so
        Arrow consumers see real nulls — not sentinel masquerade. This
        test inserts SQL NULL values and verifies what each mapper
        surfaces:

          - default (native hybrid): a nullable LONG with nulls becomes
            pandas Int64 (pd.NA); DOUBLE null is NaN; VARCHAR null is None.
          - dtype_backend="pyarrow": ArrowDtype preserves null as pd.NA.
          - dtype_backend="numpy_nullable": Int64Dtype/Float64Dtype/
            StringDtype preserve null as pd.NA.

        Also verifies that QuestDB's storage sentinel-collision
        contract holds in the other direction: a real INT64_MIN
        ingested as a value comes back as null.
        """
        import pandas as pd
        import pyarrow as pa
        import numpy as np
        table_name = 't_egress_nulls_' + uuid.uuid4().hex[:8]
        try:
            self._exec(
                f'CREATE TABLE {table_name} '
                '(ts TIMESTAMP, lg LONG, db DOUBLE, vc VARCHAR) '
                'TIMESTAMP(ts) PARTITION BY DAY WAL')
            # Row 0: all values populated.
            # Row 1: all nullable columns NULL.
            self._exec(
                f"INSERT INTO {table_name} VALUES "
                f"('2024-01-01T00:00:00Z', 42, 3.5, 'hello'), "
                f"('2024-01-01T00:00:01Z', NULL, NULL, NULL)")
            self.qdb_plain.retry_check_table(table_name, min_rows=2)

            sql = f'SELECT lg, db, vc FROM {table_name} ORDER BY ts'

            # 1. Arrow level: verify the validity bitmap arrived.
            with qi.QuestDB.from_conf(self._conf()) as client:
                table = client.query(sql).to_arrow()
            lg_col = table.column('lg')
            self.assertEqual(lg_col.null_count, 1,
                f'expected 1 null on row 1; got {lg_col.null_count}')
            self.assertFalse(lg_col.is_null()[0].as_py())
            self.assertTrue(lg_col.is_null()[1].as_py())
            self.assertEqual(table.column('db').null_count, 1)
            self.assertEqual(table.column('vc').null_count, 1)

            # 2. default to_pandas — native hybrid: a nullable LONG with
            #    nulls becomes Int64 (pd.NA); DOUBLE null is NaN; VARCHAR
            #    null is None.
            with qi.QuestDB.from_conf(self._conf()) as client:
                default = client.query(sql).to_pandas()
            self.assertEqual(str(default['lg'].dtype), 'Int64')
            self.assertEqual(default['lg'].iloc[0], 42)
            self.assertTrue(default['lg'].iloc[1] is pd.NA)
            self.assertTrue(pd.api.types.is_float_dtype(default['db'].dtype))
            self.assertEqual(default['db'].iloc[0], 3.5)
            self.assertTrue(pd.isna(default['db'].iloc[1]))
            self.assertEqual(default['vc'].iloc[0], 'hello')
            self.assertTrue(pd.isna(default['vc'].iloc[1]))

            # 3. pyarrow-backed to_pandas — pd.NA preserved.
            with qi.QuestDB.from_conf(self._conf()) as client:
                arrow_backed = client.query(sql).to_pandas(
                    dtype_backend='pyarrow')
            self.assertIsInstance(arrow_backed['lg'].dtype, pd.ArrowDtype)
            self.assertEqual(arrow_backed['lg'].iloc[0], 42)
            self.assertTrue(arrow_backed['lg'].iloc[1] is pd.NA)
            self.assertTrue(arrow_backed['db'].iloc[1] is pd.NA)
            self.assertTrue(arrow_backed['vc'].iloc[1] is pd.NA)

            # 4. numpy_nullable to_pandas — pd.NA preserved via
            #    Int64Dtype / Float64Dtype / StringDtype.
            with qi.QuestDB.from_conf(self._conf()) as client:
                nullable = client.query(sql).to_pandas(
                    dtype_backend='numpy_nullable')
            self.assertIsInstance(nullable['lg'].dtype, pd.Int64Dtype)
            self.assertEqual(nullable['lg'].iloc[0], 42)
            self.assertTrue(nullable['lg'].iloc[1] is pd.NA)
            self.assertIsInstance(nullable['db'].dtype, pd.Float64Dtype)
            self.assertTrue(nullable['db'].iloc[1] is pd.NA)
            self.assertIsInstance(nullable['vc'].dtype, pd.StringDtype)
            self.assertTrue(nullable['vc'].iloc[1] is pd.NA)
        finally:
            try:
                self._exec(f'DROP TABLE IF EXISTS {table_name}')
            except Exception:
                pass

    def test_sentinel_collision_is_documented_lossy(self):
        """Verify QuestDB's storage-level sentinel-collision contract:
        a user-supplied INT64_MIN value ingested as a LONG is folded
        into NULL by the server. This is QuestDB's docs (see
        plan-egress-to-pandas.md "Unavoidable lossy scenarios"); we
        pin it here so a future server-side fix would be flagged.
        """
        import pandas as pd
        import numpy as np
        table_name = 't_egress_sentinel_' + uuid.uuid4().hex[:8]
        try:
            self._exec(
                f'CREATE TABLE {table_name} '
                '(ts TIMESTAMP, lg LONG) '
                'TIMESTAMP(ts) PARTITION BY DAY WAL')
            # Ingest via QuestDB.dataframe — the python int range
            # accepts INT64_MIN cleanly, sidestepping the SQL
            # parser ambiguity around the literal.
            # Also exercise tz-aware ingest (was rejected by columnar v1
            # until commit 9db3325 follow-up). Use the trailing 'Z' form
            # that pd.to_datetime infers as DatetimeTZDtype.
            df = pd.DataFrame({
                'ts': pd.to_datetime([
                    '2024-01-01T00:00:00Z',
                    '2024-01-01T00:00:01Z']),
                'lg': np.array(
                    [42, np.iinfo(np.int64).min], dtype=np.int64),
            })
            with qi.QuestDB.from_conf(self._conf()) as client:
                client.dataframe(df, table_name=table_name, at='ts')
            self.qdb_plain.retry_check_table(table_name, min_rows=2)

            sql = f'SELECT lg FROM {table_name} ORDER BY ts'
            with qi.QuestDB.from_conf(self._conf()) as client:
                table = client.query(sql).to_arrow()

            # The INT64_MIN row collapses to NULL server-side.
            self.assertEqual(
                table.column('lg').null_count, 1,
                'expected the INT64_MIN row to be folded into NULL '
                'by QuestDB storage; a non-zero null_count of 1 '
                'pins that contract')
            self.assertFalse(table.column('lg').is_null()[0].as_py())
            self.assertTrue(table.column('lg').is_null()[1].as_py())
            self.assertEqual(table.column('lg')[0].as_py(), 42)
        finally:
            try:
                self._exec(f'DROP TABLE IF EXISTS {table_name}')
            except Exception:
                pass

    def test_sequential_queries_on_one_client(self):
        """Open one QuestDB handle, run several queries in sequence. Catches
        regressions in any per-call reader/cursor lifecycle assumption.
        Pool-reuse assertions live in ``TestEgressPool`` so this test
        stays focused on the per-query result shape.
        """
        table_name = 't_egress_seq_' + uuid.uuid4().hex[:8]
        try:
            self._exec(
                f'CREATE TABLE {table_name} '
                '(ts TIMESTAMP, x LONG) '
                'TIMESTAMP(ts) PARTITION BY DAY WAL')
            self._exec(
                f"INSERT INTO {table_name} VALUES "
                f"('2024-01-01T00:00:00Z', 1), "
                f"('2024-01-01T00:00:01Z', 2), "
                f"('2024-01-01T00:00:02Z', 3)")
            self.qdb_plain.retry_check_table(table_name, min_rows=3)

            with qi.QuestDB.from_conf(self._conf()) as client:
                first = client.query(
                    f'SELECT count() FROM {table_name}').to_arrow()
                self.assertEqual(first.num_rows, 1)
                self.assertEqual(first.column(0).to_pylist(), [3])

                second = client.query(
                    f'SELECT x FROM {table_name} ORDER BY x').to_arrow()
                self.assertEqual(second.num_rows, 3)
                self.assertEqual(
                    second.column('x').to_pylist(), [1, 2, 3])

                third = client.query(
                    f'SELECT x FROM {table_name} WHERE x > 1 '
                    f'ORDER BY x').to_arrow()
                self.assertEqual(third.num_rows, 2)
                self.assertEqual(
                    third.column('x').to_pylist(), [2, 3])
        finally:
            try:
                self._exec(f'DROP TABLE IF EXISTS {table_name}')
            except Exception:
                pass




class TestEgressQwpRowTypes(unittest.TestCase):
    """Round trips that put a QWP-only column type back on the wire.

    Membership in this class is the version gate. `setUp` asks for
    `FIRST_QWP_ROW_TYPES_RELEASE`, so a test written here is covered
    whether or not whoever wrote it thought about the server version --
    which is the half a per-test call leaves to be remembered, and the
    half that kept being forgotten.

    The gate is what this client supports, not what a server happens to
    accept: the QWP beta in 9.4.3 takes these types too, so a run
    against it proves nothing about the implementation this client is
    written for. On a leg that sets
    `TEST_QUESTDB_REQUIRE_QWP_ROW_TYPES`, the skip becomes a failure, so
    a run that covers none of this cannot look like one that passed.
    """

    @classmethod
    def setUpClass(cls):
        TestWithDatabase.setUpClass.__func__(cls)

    @classmethod
    def tearDownClass(cls):
        TestWithDatabase.tearDownClass.__func__(cls)

    # Borrowed rather than restated, like `setUpClass` above.
    _require_qwp_ws = TestWithDatabase._require_qwp_ws
    _require_qwp_row_types = TestWithDatabase._require_qwp_row_types
    _conf = TestEgressWithDatabase._conf
    _exec = TestEgressWithDatabase._exec
    _column_type = TestEgressWithDatabase._column_type

    def setUp(self):
        # `_require_qwp_row_types` asks for QWP/WebSocket first.
        self._require_qwp_row_types()

    def _run_example(self, file_name, table_name):
        """Run one of the `examples/` scripts against this fixture.

        Loaded and called rather than copied, so the test fails when the
        example does. An example that only lives in the docs is an
        example that stops working quietly.
        """
        spec = importlib.util.spec_from_file_location(
            f'questdb_example_{file_name}',
            PROJ_ROOT / 'examples' / f'{file_name}.py')
        self.assertIsNotNone(spec)
        self.assertIsNotNone(spec.loader)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        module.example(
            host=self.qdb_plain.host,
            port=self.qdb_plain.http_server_port,
            table_name=table_name)

    def test_qwp_column_types_example(self):
        """`examples/qwp_column_types.py` writes all seven QWP-only
        types through `row()` and lands them as those types."""
        table_name = 'ex_row_types_' + uuid.uuid4().hex[:8]
        try:
            self._run_example('qwp_column_types', table_name)
            self.qdb_plain.retry_check_table(table_name, min_rows=1)
            self.assertEqual(self._column_type(table_name, 'device_id'), 'UUID')
            self.assertEqual(self._column_type(table_name, 'address'), 'IPv4')
            self.assertEqual(self._column_type(table_name, 'payload'), 'BINARY')
            self.assertEqual(self._column_type(table_name, 'grade'), 'CHAR')
            self.assertEqual(self._column_type(table_name, 'last_seen'), 'DATE')
            self.assertEqual(self._column_type(table_name, 'checksum'), 'LONG256')
            self.assertEqual(
                self._column_type(table_name, 'location'), 'GEOHASH(5c)')
        finally:
            self._exec(f'DROP TABLE IF EXISTS {table_name}')

    def test_qwp_column_types_dataframe_example(self):
        """`examples/qwp_column_types_dataframe.py` writes the same
        types from a frame and then writes a read-back of that frame
        out again, which is the round trip the claim exists for. Both
        writes land, so the table holds four rows."""
        if pd is None:
            self.skipTest('pandas not installed')
        try:
            import pyarrow  # noqa: F401
        except ImportError:
            self.skipTest('pyarrow not installed')
        table_name = 'ex_row_types_df_' + uuid.uuid4().hex[:8]
        try:
            self._run_example('qwp_column_types_dataframe', table_name)
            self.qdb_plain.retry_check_table(table_name, min_rows=4)
            self.assertEqual(self._column_type(table_name, 'device_id'), 'UUID')
            self.assertEqual(self._column_type(table_name, 'address'), 'IPv4')
            self.assertEqual(self._column_type(table_name, 'grade'), 'CHAR')
            self.assertEqual(self._column_type(table_name, 'last_seen'), 'DATE')
            self.assertEqual(self._column_type(table_name, 'checksum'), 'LONG256')
            self.assertEqual(
                self._column_type(table_name, 'location'), 'GEOHASH(5c)')
        finally:
            self._exec(f'DROP TABLE IF EXISTS {table_name}')

    @unittest.skipIf(pd is None, 'pandas not installed')
    def test_types_mapper_keeps_the_claim_and_the_dtype_decides(self):
        """A custom ``types_mapper`` gets the same
        ``df.attrs['questdb']`` claim the built-in backends get, and
        whether the claim can be applied depends on the dtype the mapper
        chose.

        The docstring promises exactly that and nothing tested it. A
        mapper that leaves the claimed columns Arrow-backed round-trips
        the types; one that maps a claimed column to a float dtype
        leaves the claim unread and the column lands as that dtype
        implies.
        """
        import pyarrow as pa
        src = 't_mapper_src_' + uuid.uuid4().hex[:8]
        made = []
        try:
            self._exec(
                f'CREATE TABLE {src} (ts TIMESTAMP, ip IPV4, lg LONG) '
                'TIMESTAMP(ts) PARTITION BY DAY WAL')
            self._exec(
                f"INSERT INTO {src} VALUES "
                "('2024-01-01T00:00:00Z', '1.2.3.4', 7)")
            self.qdb_plain.retry_check_table(src, min_rows=1)
            sql = f'SELECT ts, ip, lg FROM {src}'

            # A mapper that keeps every column Arrow-backed: the claim
            # applies and IPV4 comes back as IPV4.
            with qi.QuestDB.from_conf(self._conf()) as client:
                kept = client.query(sql).to_pandas(
                    types_mapper=pd.ArrowDtype)
            self.assertEqual(
                kept.attrs['questdb']['columns']['ip']['kind'], 'ipv4')
            dst = 't_mapper_kept_' + uuid.uuid4().hex[:8]
            made.append(dst)
            with qi.QuestDB.from_conf(self._conf()) as client:
                client.dataframe(kept, table_name=dst, at='ts')
            self.qdb_plain.retry_check_table(dst, min_rows=1)
            self.assertEqual(self._column_type(dst, 'ip'), 'IPv4')

            # A mapper that retypes the claimed column past every shape
            # that can hold the claim. The claim is still in `attrs`,
            # and the column lands as the dtype implies.
            def to_float(arrow_type):
                return (pd.Float64Dtype()
                        if pa.types.is_uint32(arrow_type) else None)

            with qi.QuestDB.from_conf(self._conf()) as client:
                retyped = client.query(sql).to_pandas(types_mapper=to_float)
            self.assertEqual(
                retyped.attrs['questdb']['columns']['ip']['kind'], 'ipv4')
            self.assertIsInstance(retyped['ip'].dtype, pd.Float64Dtype)
            dst = 't_mapper_float_' + uuid.uuid4().hex[:8]
            made.append(dst)
            with qi.QuestDB.from_conf(self._conf()) as client:
                client.dataframe(retyped, table_name=dst, at='ts')
            self.qdb_plain.retry_check_table(dst, min_rows=1)
            self.assertEqual(self._column_type(dst, 'ip'), 'DOUBLE')
        finally:
            for name in [src] + made:
                try:
                    self._exec(f'DROP TABLE IF EXISTS {name}')
                except Exception:
                    pass

    @unittest.skipIf(pd is None, 'pandas not installed')
    def test_date_column_survives_a_read_modify_write(self):
        """A DATE column read back and written straight out again lands
        as DATE, on all three pandas backends.

        DATE is claimed by the column's Arrow type, and plain
        `to_pandas()` hands the column back as a NumPy `datetime64[ms]`,
        which has no route of its own to DATE. Without the claim putting
        the Arrow type back on, that frame created the destination table
        with a microsecond TIMESTAMP column and said nothing -- the
        outcome the round-trip claim exists to prevent.
        """
        src = 't_date_rt_src_' + uuid.uuid4().hex[:8]
        made = []
        try:
            self._exec(
                f'CREATE TABLE {src} (ts TIMESTAMP, d DATE) '
                'TIMESTAMP(ts) PARTITION BY DAY WAL')
            self._exec(
                f"INSERT INTO {src} VALUES "
                "('2024-01-01T00:00:00Z', '2024-01-02T03:04:05.678Z'), "
                "('2024-01-01T00:00:01Z', '1969-07-20T20:17:40.000Z')")
            self.qdb_plain.retry_check_table(src, min_rows=2)
            sql = f'SELECT ts, d FROM {src} ORDER BY ts'

            for backend in (None, 'pyarrow', 'numpy_nullable'):
                kwargs = {} if backend is None else {'dtype_backend': backend}
                with self.subTest(dtype_backend=backend):
                    dst = 't_date_rt_dst_' + uuid.uuid4().hex[:8]
                    made.append(dst)
                    with qi.QuestDB.from_conf(self._conf()) as client:
                        df = client.query(sql).to_pandas(**kwargs)
                    self.assertEqual(
                        df.attrs['questdb']['columns']['d']['kind'], 'date')
                    with qi.QuestDB.from_conf(self._conf()) as client:
                        client.dataframe(df, table_name=dst, at='ts')
                    self.qdb_plain.retry_check_table(dst, min_rows=2)
                    self.assertEqual(self._column_type(dst, 'd'), 'DATE')
                    with qi.QuestDB.from_conf(self._conf()) as client:
                        back = client.query(
                            f'SELECT d FROM {dst} ORDER BY timestamp'
                        ).to_pandas()
                    self.assertEqual(
                        back.attrs['questdb']['columns']['d']['kind'], 'date')
                    self.assertEqual(
                        self.qdb_plain.http_sql_query(
                            f'SELECT d FROM {dst} ORDER BY timestamp')[
                                'dataset'],
                        self.qdb_plain.http_sql_query(
                            f'SELECT d FROM {src} ORDER BY ts')['dataset'])
        finally:
            for name in [src] + made:
                try:
                    self._exec(f'DROP TABLE IF EXISTS {name}')
                except Exception:
                    pass

    def test_numpy_egress_round_trip_overrides(self):
        """uuid / long256 / ipv4 / char / geohash round-trip through the
        native numpy path driven by df.attrs metadata (no pyarrow). UUID
        comes back as ``uuid.UUID`` cells and LONG256 as Python ints —
        the widest shape available without pyarrow — and the claim is
        what turns those ints back into 32-byte values. The destination
        column types are verified by re-querying and checking the egress
        metadata reports the same kinds.
        """
        src = 't_rto_src_' + uuid.uuid4().hex[:8]
        dst = 't_rto_dst_' + uuid.uuid4().hex[:8]
        value = uuid.UUID('123e4567-e89b-12d3-a456-426614174000')
        cols = 'ts, u, l, ip, gh, c'
        try:
            self._exec(
                f'CREATE TABLE {src} '
                '(ts TIMESTAMP, u UUID, l LONG256, ip IPV4, '
                'gh GEOHASH(4c), c CHAR) '
                'TIMESTAMP(ts) PARTITION BY DAY WAL')
            self._exec(
                f"INSERT INTO {src} VALUES "
                f"('2024-01-01T00:00:00Z', '{value}', "
                "'0x0001020304050607080910111213141516171819202122232425262728293031', "
                f"'1.2.3.4', #u33d, 'A'), "
                f"('2024-01-01T00:00:01Z', '{value}', "
                "'0x01', "
                f"'255.0.0.1', #u33e, 'B')")
            self.qdb_plain.retry_check_table(src, min_rows=2)

            with qi.QuestDB.from_conf(self._conf()) as client:
                df = client.query(
                    f'SELECT {cols} FROM {src} ORDER BY ts').to_pandas()
            meta = df.attrs['questdb']['columns']
            self.assertEqual(meta['u']['kind'], 'uuid')
            self.assertEqual(meta['l']['kind'], 'long256')
            self.assertEqual(meta['ip']['kind'], 'ipv4')
            self.assertEqual(meta['c']['kind'], 'char')
            self.assertEqual(meta['gh']['kind'], 'geohash')
            self.assertEqual(meta['gh']['precision_bits'], 20)
            self.assertEqual(list(df['u']), [value, value])
            self.assertEqual(list(df['l']), [
                0x0001020304050607080910111213141516171819202122232425262728293031,
                1])

            with qi.QuestDB.from_conf(self._conf()) as client:
                client.dataframe(df, table_name=dst, at='ts')
            self.qdb_plain.retry_check_table(dst, min_rows=2)

            with qi.QuestDB.from_conf(self._conf()) as client:
                back = client.query(
                    f'SELECT u, l, ip, gh, c FROM {dst}').to_pandas()
            bmeta = back.attrs['questdb']['columns']
            self.assertEqual(bmeta['u']['kind'], 'uuid')
            self.assertEqual(bmeta['l']['kind'], 'long256')
            self.assertEqual(bmeta['ip']['kind'], 'ipv4')
            self.assertEqual(bmeta['c']['kind'], 'char')
            self.assertEqual(bmeta['gh']['kind'], 'geohash')
            self.assertEqual(bmeta['gh']['precision_bits'], 20)

            # An auto-created table names its designated timestamp
            # column `timestamp`, so the destination orders by that name
            # and the source by the `ts` it was created with.
            self.assertEqual(
                self.qdb_plain.http_sql_query(
                    f'SELECT u, l, ip, gh, c FROM {dst} '
                    'ORDER BY timestamp')['dataset'],
                self.qdb_plain.http_sql_query(
                    f'SELECT u, l, ip, gh, c FROM {src} ORDER BY ts')[
                        'dataset'])
        finally:
            for t in (src, dst):
                try:
                    self._exec(f'DROP TABLE IF EXISTS {t}')
                except Exception:
                    pass

    def test_geohash_claim_survives_convert_dtypes_to_pyarrow(self):
        """A GEOHASH column keeps its type through
        ``to_pandas()`` followed by
        ``df.convert_dtypes(dtype_backend='pyarrow')``.

        That conversion is what the ``QuestDB.dataframe`` docstring
        names, and it moves the frame from the NumPy planner to the
        Arrow columnar path. The two read the claim in different places,
        and the Arrow path carries a ``geohash`` claim only on a signed
        Arrow integer -- an unsigned column is refused by the native
        importer, so its claim is dropped and the column would land as a
        plain LONG. The NumPy egress therefore hands the column back
        signed, at the width the precision needs, which is the same
        shape the Arrow egress gives it.
        """
        src = 't_ghs_src_' + uuid.uuid4().hex[:8]
        dst = 't_ghs_dst_' + uuid.uuid4().hex[:8]
        try:
            self._exec(
                f'CREATE TABLE {src} '
                '(ts TIMESTAMP, gh GEOHASH(4c)) '
                'TIMESTAMP(ts) PARTITION BY DAY WAL')
            self._exec(
                f"INSERT INTO {src} VALUES "
                f"('2024-01-01T00:00:00Z', #u33d), "
                f"('2024-01-01T00:00:01Z', #u33e)")
            self.qdb_plain.retry_check_table(src, min_rows=2)

            with qi.QuestDB.from_conf(self._conf()) as client:
                df = client.query(
                    f'SELECT ts, gh FROM {src} ORDER BY ts').to_pandas()
            self.assertEqual(df.attrs['questdb']['columns']['gh'], {
                'kind': 'geohash', 'precision_bits': 20})
            # 20 bits needs a signed slot wide enough to hold
            # 0 .. 2**20-1, which is int32 -- the same width the Arrow
            # egress picks for that precision.
            self.assertEqual(df['gh'].dtype, np.dtype(np.int32))
            self.assertTrue((df['gh'] >= 0).all())

            converted = df.convert_dtypes(dtype_backend='pyarrow')
            self.assertEqual(
                converted.attrs['questdb']['columns']['gh'],
                {'kind': 'geohash', 'precision_bits': 20})

            with qi.QuestDB.from_conf(self._conf()) as client:
                client.dataframe(converted, table_name=dst, at='ts')
            self.qdb_plain.retry_check_table(dst, min_rows=2)

            # The claim survived the conversion, so the auto-created
            # column is a GEOHASH and not the LONG its storage type
            # alone would imply.
            self.assertEqual(
                self.qdb_plain.http_sql_query(
                    f"SELECT typeOf(gh) FROM {dst} LIMIT 1")['dataset'],
                [['GEOHASH(4c)']])
            self.assertEqual(
                self.qdb_plain.http_sql_query(
                    f'SELECT gh FROM {dst} ORDER BY timestamp')['dataset'],
                self.qdb_plain.http_sql_query(
                    f'SELECT gh FROM {src} ORDER BY ts')['dataset'])
        finally:
            for t in (src, dst):
                try:
                    self._exec(f'DROP TABLE IF EXISTS {t}')
                except Exception:
                    pass

    def test_arrow_backed_egress_round_trip_overrides(self):
        """uuid / long256 / ipv4 / char / geohash round-trip through
        ``to_pandas(dtype_backend='pyarrow')`` and through
        ``to_pandas(dtype_backend='numpy_nullable')``. The claims the
        egress writes as Arrow field metadata cannot ride on a pandas
        frame, which holds Arrow types and no fields, so they travel in
        ``df.attrs['questdb']`` and are turned back into column types on
        the way in. The two backends hand the same five columns back in
        different shapes — Arrow-backed dtypes on one, masked extension
        and object-dtype ``bytes`` columns on the other — and the claim
        has to survive both. The destination table is auto-created, so
        its column types are exactly what the client asked for.
        """
        for backend in ('pyarrow', 'numpy_nullable'):
            with self.subTest(dtype_backend=backend):
                self._check_arrow_backed_round_trip(backend)

    def _check_arrow_backed_round_trip(self, backend):
        src = 't_rta_src_' + uuid.uuid4().hex[:8]
        dst = 't_rta_dst_' + uuid.uuid4().hex[:8]
        value = uuid.UUID('123e4567-e89b-12d3-a456-426614174000')
        value_cols = 'u, l, ip, gh, c'
        cols = f'ts, {value_cols}'
        try:
            self._exec(
                f'CREATE TABLE {src} '
                '(ts TIMESTAMP, u UUID, l LONG256, ip IPV4, '
                'gh GEOHASH(4c), c CHAR) '
                'TIMESTAMP(ts) PARTITION BY DAY WAL')
            self._exec(
                f"INSERT INTO {src} VALUES "
                f"('2024-01-01T00:00:00Z', '{value}', "
                "'0x0001020304050607080910111213141516171819202122232425262728293031', "
                "'1.2.3.4', #u33d, 'A')")
            self.qdb_plain.retry_check_table(src, min_rows=1)

            with qi.QuestDB.from_conf(self._conf()) as client:
                df = client.query(
                    f'SELECT {cols} FROM {src} ORDER BY ts'
                ).to_pandas(dtype_backend=backend)
            meta = df.attrs['questdb']['columns']
            self.assertEqual(meta['u']['kind'], 'uuid')
            self.assertEqual(meta['l']['kind'], 'long256')
            self.assertEqual(meta['ip']['kind'], 'ipv4')
            self.assertEqual(meta['c']['kind'], 'char')
            self.assertEqual(meta['gh']['kind'], 'geohash')
            self.assertEqual(meta['gh']['precision_bits'], 20)

            with qi.QuestDB.from_conf(self._conf()) as client:
                client.dataframe(df, table_name=dst, at='ts')
            self.qdb_plain.retry_check_table(dst, min_rows=1)

            # An auto-created table names its designated timestamp column
            # `timestamp`, so only the value columns carry over by name.
            with qi.QuestDB.from_conf(self._conf()) as client:
                back = client.query(
                    f'SELECT {value_cols} FROM {dst}').to_pandas(
                        dtype_backend=backend)
            bmeta = back.attrs['questdb']['columns']
            self.assertEqual(bmeta['u']['kind'], 'uuid')
            self.assertEqual(bmeta['l']['kind'], 'long256')
            self.assertEqual(bmeta['ip']['kind'], 'ipv4')
            self.assertEqual(bmeta['c']['kind'], 'char')
            self.assertEqual(bmeta['gh']['kind'], 'geohash')
            self.assertEqual(bmeta['gh']['precision_bits'], 20)

            rows = self.qdb_plain.http_sql_query(
                f'SELECT {value_cols} FROM {dst}')['dataset']
            self.assertEqual(
                rows,
                self.qdb_plain.http_sql_query(
                    f'SELECT {value_cols} FROM {src}')['dataset'])
        finally:
            for t in (src, dst):
                try:
                    self._exec(f'DROP TABLE IF EXISTS {t}')
                except Exception:
                    pass


class TestEgressPool(unittest.TestCase):
    """Structural tests for the ``questdb_db`` egress reader pool.

    Asserts behaviours the per-feature tests in
    ``TestEgressWithDatabase`` exercise the code path of but don't
    individually pin. Concurrency tests use ``threading.Barrier`` +
    fixed iteration counts so they're deterministic — no ``sleep``
    or wall-clock dependencies. All tests run whenever the system-
    test fixture is available (``QDB_REPO_PATH`` set); no separate
    stress-mode gate.
    """

    @classmethod
    def setUpClass(cls):
        TestWithDatabase.setUpClass.__func__(cls)

    @classmethod
    def tearDownClass(cls):
        TestWithDatabase.tearDownClass.__func__(cls)

    def _require_qwp_ws(self):
        if self.qdb_plain.version < FIRST_QWP_WS_RELEASE:
            self.skipTest(
                'QWP/WebSocket integration tests require QuestDB 9.4.3+')

    def setUp(self):
        self._require_qwp_ws()

    def _conf(self, **extra):
        conf = (f'ws::addr={self.qdb_plain.host}:'
                f'{self.qdb_plain.http_server_port};')
        for k, v in extra.items():
            conf += f'{k}={v};'
        return conf

    def _seed_table(self, n_rows=3):
        """Create a small table and return its name. The pool tests
        below all just need *something* queryable; one shared shape
        keeps them simple."""
        table = 't_egress_pool_' + uuid.uuid4().hex[:8]
        self.qdb_plain.http_sql_query(
            f'CREATE TABLE {table} '
            '(ts TIMESTAMP, x LONG) '
            'TIMESTAMP(ts) PARTITION BY DAY WAL')
        # Use one-second steps but stay within a single minute by
        # rolling over via minutes — keeps SQL literals trivially
        # valid for n_rows up to 60*60.
        values = ','.join(
            f"('2024-01-01T00:{i // 60:02d}:{i % 60:02d}Z', {i})"
            for i in range(n_rows))
        self.qdb_plain.http_sql_query(
            f'INSERT INTO {table} VALUES {values}')
        self.qdb_plain.retry_check_table(table, min_rows=n_rows)
        self.addCleanup(
            lambda: self._drop_quietly(table))
        return table

    def _drop_quietly(self, table):
        try:
            self.qdb_plain.http_sql_query(f'DROP TABLE IF EXISTS {table}')
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Pool reuse — the architecture's primary promise
    # ------------------------------------------------------------------

    def test_idle_grows_on_sequential_use(self):
        """After N sequential queries on one QuestDB handle the pool holds
        exactly one idle reader. (The lifted-out pool-reuse assertion
        previously in test_sequential_queries_on_one_client.)
        """
        table = self._seed_table(n_rows=3)
        with qi.QuestDB.from_conf(self._conf()) as client:
            for _ in range(5):
                client.query(f'SELECT count() FROM {table}').to_arrow()
            in_use, idle = qi._debug_egress_pool_stats(client)
            self.assertEqual(in_use, 0)
            self.assertEqual(
                idle, 1,
                f'expected 1 idle reader cached across 5 queries; '
                f'got in_use={in_use}, idle={idle}')

    def test_arrow_capsule_callbacks_migrate_between_workers(self):
        """Drive one Arrow C stream through two synchronized worker
        threads: worker A fetches schema + the first batch, worker B
        drains and releases it. This pins the actual third-party callback
        route, including foreign-thread ``qwp_reader_cursor_free``.
        """
        import ctypes

        class ArrowArray(ctypes.Structure):
            pass

        class ArrowSchema(ctypes.Structure):
            pass

        class ArrowArrayStream(ctypes.Structure):
            pass

        ArrowArray._fields_ = [
            ('length', ctypes.c_int64),
            ('null_count', ctypes.c_int64),
            ('offset', ctypes.c_int64),
            ('n_buffers', ctypes.c_int64),
            ('n_children', ctypes.c_int64),
            ('buffers', ctypes.POINTER(ctypes.c_void_p)),
            ('children', ctypes.POINTER(ctypes.POINTER(ArrowArray))),
            ('dictionary', ctypes.POINTER(ArrowArray)),
            ('release', ctypes.c_void_p),
            ('private_data', ctypes.c_void_p),
        ]
        ArrowSchema._fields_ = [
            ('format', ctypes.c_void_p),
            ('name', ctypes.c_void_p),
            ('metadata', ctypes.c_void_p),
            ('flags', ctypes.c_int64),
            ('n_children', ctypes.c_int64),
            ('children', ctypes.POINTER(ctypes.POINTER(ArrowSchema))),
            ('dictionary', ctypes.POINTER(ArrowSchema)),
            ('release', ctypes.c_void_p),
            ('private_data', ctypes.c_void_p),
        ]
        ArrowArrayStream._fields_ = [
            ('get_schema', ctypes.c_void_p),
            ('get_next', ctypes.c_void_p),
            ('get_last_error', ctypes.c_void_p),
            ('release', ctypes.c_void_p),
            ('private_data', ctypes.c_void_p),
        ]

        stream_ptr_t = ctypes.POINTER(ArrowArrayStream)
        get_schema_t = ctypes.CFUNCTYPE(
            ctypes.c_int,
            stream_ptr_t,
            ctypes.POINTER(ArrowSchema))
        get_next_t = ctypes.CFUNCTYPE(
            ctypes.c_int,
            stream_ptr_t,
            ctypes.POINTER(ArrowArray))
        stream_release_t = ctypes.CFUNCTYPE(None, stream_ptr_t)
        schema_release_t = ctypes.CFUNCTYPE(
            None, ctypes.POINTER(ArrowSchema))
        array_release_t = ctypes.CFUNCTYPE(
            None, ctypes.POINTER(ArrowArray))

        table = self._seed_table(n_rows=64)
        with qi.QuestDB.from_conf(self._conf()) as client:
            result = client.query(f'SELECT x FROM {table} ORDER BY x')
            capsule = result.__arrow_c_stream__()
            capsule_get = ctypes.pythonapi.PyCapsule_GetPointer
            capsule_get.argtypes = [ctypes.py_object, ctypes.c_char_p]
            capsule_get.restype = ctypes.c_void_p
            stream_addr = capsule_get(capsule, b'arrow_array_stream')
            self.assertTrue(stream_addr)
            stream = ctypes.cast(stream_addr, stream_ptr_t)

            ready = threading.Event()
            finished = threading.Event()
            errors = []
            worker_ids = []
            row_counts = []

            def release_schema(schema):
                if schema.release:
                    schema_release_t(schema.release)(
                        ctypes.byref(schema))

            def release_array(array):
                if array.release:
                    array_release_t(array.release)(
                        ctypes.byref(array))

            def worker_a():
                worker_ids.append(('a', threading.get_ident()))
                try:
                    schema = ArrowSchema()
                    get_schema = get_schema_t(
                        stream.contents.get_schema)
                    rc = get_schema(stream, ctypes.byref(schema))
                    if rc != 0:
                        raise RuntimeError(f'get_schema returned {rc}')
                    if not schema.release:
                        raise RuntimeError(
                            'get_schema returned no release callback')
                    release_schema(schema)

                    array = ArrowArray()
                    get_next = get_next_t(stream.contents.get_next)
                    rc = get_next(stream, ctypes.byref(array))
                    if rc != 0:
                        raise RuntimeError(f'first get_next returned {rc}')
                    if array.release:
                        row_counts.append(array.length)
                        release_array(array)
                except BaseException as exc:
                    errors.append(('a', repr(exc)))
                finally:
                    ready.set()
                    finished.wait(timeout=30)

            def worker_b():
                if not ready.wait(timeout=30):
                    errors.append(('b', 'worker A did not publish'))
                    finished.set()
                    return
                worker_ids.append(('b', threading.get_ident()))
                try:
                    if not errors:
                        get_next = get_next_t(stream.contents.get_next)
                        while True:
                            array = ArrowArray()
                            rc = get_next(stream, ctypes.byref(array))
                            if rc != 0:
                                raise RuntimeError(
                                    f'drain get_next returned {rc}')
                            if not array.release:
                                break
                            row_counts.append(array.length)
                            release_array(array)
                except BaseException as exc:
                    errors.append(('b', repr(exc)))
                finally:
                    if stream.contents.release:
                        stream_release_t(stream.contents.release)(stream)
                    finished.set()

            thread_a = threading.Thread(target=worker_a)
            thread_b = threading.Thread(target=worker_b)
            thread_a.start()
            thread_b.start()
            thread_a.join(timeout=30)
            thread_b.join(timeout=30)
            self.assertFalse(thread_a.is_alive(), 'worker A hung')
            self.assertFalse(thread_b.is_alive(), 'worker B hung')
            self.assertEqual(errors, [])
            self.assertEqual(sum(row_counts), 64)
            self.assertEqual(len(worker_ids), 2)
            self.assertNotEqual(worker_ids[0][1], worker_ids[1][1])
            self.assertNotIn(
                threading.get_ident(), [ident for _, ident in worker_ids])

            # Worker B released the producer and returned the clean reader.
            # The capsule destructor now only frees its tiny stream struct.
            result.close()
            del capsule
            self.assertEqual(
                qi._debug_egress_pool_stats(client), (0, 1))
            after = client.query(
                f'SELECT count() FROM {table}').to_arrow()
            self.assertEqual(after.column(0).to_pylist(), [64])

    def test_server_info_recycles_pooled_reader(self):
        """server_info() borrows a pristine reader (no cursor, no wire
        traffic) and must return it to the pool, not drop it."""
        with qi.QuestDB.from_conf(self._conf()) as client:
            for _ in range(3):
                self.assertIsNotNone(client.server_info())
                in_use, idle = qi._debug_egress_pool_stats(client)
                self.assertEqual(in_use, 0)
                self.assertEqual(
                    idle, 1,
                    'server_info() must recycle its pristine reader; '
                    f'got in_use={in_use}, idle={idle}')

    # ------------------------------------------------------------------
    # Arc<DbInner> lifeline — silent UAF if it regresses
    # ------------------------------------------------------------------

    def test_query_after_client_close_via_held_iterator(self):
        """The architecture promises that ``QuestDB.close()`` can free
        the user-facing handle while a still-streaming cursor exists.
        The ``Arc<DbInner>`` inside ``line_reader.ownership.Pooled``
        is what keeps the pool's transport alive across that window.

        We exercise it directly: open a client, start consuming a
        query lazily, close the client mid-stream, then drain the
        rest. A regression that replaced the Arc with a raw pointer
        would surface as a use-after-free here.
        """
        table = self._seed_table(n_rows=64)
        client = qi.QuestDB.from_conf(self._conf())
        try:
            result = client.query(f'SELECT x FROM {table} ORDER BY x')
            it = result.iter_arrow()
            first = next(it)
            client.close()
            rest = list(it)
            total_rows = first.num_rows + sum(b.num_rows for b in rest)
            self.assertEqual(total_rows, 64)
        finally:
            client.close()

    # ------------------------------------------------------------------
    # must_close — silent corruption if a broken reader gets recycled
    # ------------------------------------------------------------------

    def test_must_close_drops_broken_reader_from_pool(self):
        """Abandoning a cursor mid-stream causes the Rust
        ``Cursor::Drop`` to close the transport (because
        ``cursor_active`` is still true at drop time). The Python
        ``_ReaderHandle`` defaults to ``_must_close=True``, so on
        dealloc the reader is dropped — not recycled — and the next
        borrower gets a fresh handshake instead of a broken pipe.
        """
        import gc
        table = self._seed_table(n_rows=64)
        with qi.QuestDB.from_conf(self._conf()) as client:
            # Seed the pool with a fully-drained reader so idle==1.
            client.query(f'SELECT count() FROM {table}').to_arrow()
            in_use, idle = qi._debug_egress_pool_stats(client)
            self.assertEqual((in_use, idle), (0, 1))

            # Abandon a cursor mid-stream. The generator's `finally`
            # frees the cursor, but `cursor_active` was still true at
            # the time of free — so the Rust transport was torn down.
            # The reader handle must NOT be returned to the idle list.
            result = client.query(f'SELECT x FROM {table} ORDER BY x')
            it = result.iter_arrow()
            next(it)
            del it
            del result
            gc.collect()

            in_use, idle = qi._debug_egress_pool_stats(client)
            self.assertEqual(
                in_use, 0,
                f'leaked in-use after abandon; got '
                f'in_use={in_use}, idle={idle}')
            self.assertEqual(
                idle, 0,
                f'broken reader was recycled instead of dropped; got '
                f'in_use={in_use}, idle={idle}. A subsequent query '
                f'would have hit a broken pipe.')

            # Next query must succeed against a fresh reader.
            result = client.query(
                f'SELECT count() FROM {table}').to_arrow()
            self.assertEqual(result.column(0).to_pylist(), [64])
            # Pool re-grew by one for the fresh borrow.
            in_use, idle = qi._debug_egress_pool_stats(client)
            self.assertEqual((in_use, idle), (0, 1))

    # ------------------------------------------------------------------
    # Deterministic release — close() / abandonment lifecycle
    # ------------------------------------------------------------------

    def test_close_after_partial_consume_releases_reader_without_gc(self):
        """``QueryResult.close()`` releases the pooled reader on the
        spot: ``in_use`` is back to baseline immediately after the
        ``with`` block, with no ``gc.collect()`` and while an
        unfinished iterator still holds the cursor handle alive. The
        mid-stream reader is dropped, not recycled (``cursor_active``
        was still set), so ``idle`` stays 0.
        """
        table = self._seed_table(n_rows=64)
        with qi.QuestDB.from_conf(self._conf()) as client:
            with client.query(
                    f'SELECT x FROM {table} ORDER BY x') as result:
                it = result.iter_arrow()
                next(it)
                in_use, _ = qi._debug_egress_pool_stats(client)
                self.assertEqual(in_use, 1)
            in_use, idle = qi._debug_egress_pool_stats(client)
            self.assertEqual(
                (in_use, idle), (0, 0),
                f'close() did not release the reader immediately; '
                f'got in_use={in_use}, idle={idle}')
            del it

    def test_cancel_then_close_recycles_reader(self):
        table = self._seed_table(n_rows=64)
        with qi.QuestDB.from_conf(self._conf()) as client:
            result = client.query(f'SELECT x FROM {table} ORDER BY x')
            it = result.iter_arrow()
            next(it)

            result.cancel()
            result.cancel()
            in_use, idle = qi._debug_egress_pool_stats(client)
            self.assertEqual(
                (in_use, idle), (1, 0),
                'cancel() must keep the result open until close()')

            result.close()
            result.cancel()
            in_use, idle = qi._debug_egress_pool_stats(client)
            self.assertEqual(
                (in_use, idle), (0, 1),
                'close() after cancel() must recycle the clean reader')

            with self.assertRaises(qi.QuestDBError) as cm:
                next(it)
            self.assertEqual(
                cm.exception.code, qi.QuestDBErrorCode.InvalidApiCall)

            after = client.query(
                f'SELECT count() FROM {table}').to_arrow()
            self.assertEqual(after.column(0).to_pylist(), [64])

    def test_gc_abandoned_result_warns_and_releases(self):
        """A never-consumed ``QueryResult`` abandoned inside a
        reference cycle holds its reader until the garbage collector
        runs on a worker thread; the ``__del__`` backstop then emits a
        ``ResourceWarning`` and releases the reader on that worker. The
        cycle keeps refcounting from cleaning it up early, so this
        observes the true GC-timed cross-thread path.
        """
        import gc
        import warnings
        table = self._seed_table(n_rows=64)
        with qi.QuestDB.from_conf(self._conf()) as client:
            gc_was_enabled = gc.isenabled()
            gc.disable()
            try:
                result = client.query(
                    f'SELECT x FROM {table} ORDER BY x')
                result._cycle = result
                in_use, _ = qi._debug_egress_pool_stats(client)
                self.assertEqual(in_use, 1)
                with warnings.catch_warnings(record=True) as caught:
                    warnings.simplefilter('always')
                    del result
                    in_use, _ = qi._debug_egress_pool_stats(client)
                    self.assertEqual(
                        in_use, 1,
                        'the cycle should defer release until gc runs')
                    collector_threads = []

                    def collect_abandoned():
                        collector_threads.append(threading.get_ident())
                        gc.collect()

                    collector = threading.Thread(target=collect_abandoned)
                    collector.start()
                    collector.join(timeout=30)
                    self.assertFalse(
                        collector.is_alive(), 'worker gc.collect() hung')
            finally:
                if gc_was_enabled:
                    gc.enable()

            self.assertEqual(len(collector_threads), 1)
            self.assertNotEqual(
                collector_threads[0], threading.get_ident())
            in_use, idle = qi._debug_egress_pool_stats(client)
            self.assertEqual(
                (in_use, idle), (0, 0),
                f'worker GC backstop did not release the reader; '
                f'got in_use={in_use}, idle={idle}')
            resource_warnings = [
                w for w in caught
                if issubclass(w.category, ResourceWarning)]
            self.assertEqual(
                len(resource_warnings), 1,
                f'expected one ResourceWarning; got {caught!r}')
            self.assertIn(
                'neither drained nor closed',
                str(resource_warnings[0].message))

            # The foreign-thread finalizer dropped the partial connection;
            # the next query must open a fresh one successfully.
            after = client.query(
                f'SELECT count() FROM {table}').to_arrow()
            self.assertEqual(after.column(0).to_pylist(), [64])

    def test_abandoned_iterator_releases_reader_on_del(self):
        """Abandoning a partially-consumed stream releases the reader
        as soon as the iterator and result are dropped — no
        ``gc.collect()`` needed — and emits no ``ResourceWarning``
        (the cursor was handed off to the iterator, which released it
        deterministically on finalisation).
        """
        import gc
        import warnings
        table = self._seed_table(n_rows=64)
        with qi.QuestDB.from_conf(self._conf()) as client:
            result = client.query(f'SELECT x FROM {table} ORDER BY x')
            it = result.iter_arrow()
            next(it)
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter('always')
                del it
                del result
                in_use, idle = qi._debug_egress_pool_stats(client)
                self.assertEqual(
                    (in_use, idle), (0, 0),
                    f'expected immediate release on iterator del; '
                    f'got in_use={in_use}, idle={idle}')
                gc.collect()
            self.assertEqual(
                [w for w in caught
                 if issubclass(w.category, ResourceWarning)],
                [])

    def test_drained_result_does_not_warn_at_del(self):
        """A fully-drained ``QueryResult`` has nothing left to release:
        deleting it emits no ``ResourceWarning`` and the reader was
        already returned to the pool by the drain.
        """
        import gc
        import warnings
        table = self._seed_table(n_rows=3)
        with qi.QuestDB.from_conf(self._conf()) as client:
            result = client.query(f'SELECT count() FROM {table}')
            result.to_arrow()
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter('always')
                del result
                gc.collect()
            self.assertEqual(
                [w for w in caught
                 if issubclass(w.category, ResourceWarning)],
                [])
            in_use, idle = qi._debug_egress_pool_stats(client)
            self.assertEqual((in_use, idle), (0, 1))

    def test_cancelled_result_does_not_warn_at_del(self):
        import gc
        import warnings
        table = self._seed_table(n_rows=64)
        with qi.QuestDB.from_conf(self._conf()) as client:
            result = client.query(f'SELECT x FROM {table} ORDER BY x')
            result.cancel()
            result._cycle = result
            in_use, idle = qi._debug_egress_pool_stats(client)
            self.assertEqual((in_use, idle), (1, 0))
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter('always')
                del result
                gc.collect()
            self.assertEqual(
                [w for w in caught
                 if issubclass(w.category, ResourceWarning)],
                [])
            in_use, idle = qi._debug_egress_pool_stats(client)
            self.assertEqual((in_use, idle), (0, 1))

    # ------------------------------------------------------------------
    # Reader lease — QuestDB.reader() (PooledReader)
    # ------------------------------------------------------------------

    def test_query_lease_runs_sequential_queries_on_one_reader(self):
        """Two queries through one lease reuse a single borrowed reader:
        ``in_use`` stays 1 between them (no per-query pool round-trip)
        and drops to 0 after ``close()``, which returns the cleanly-
        drained reader to the idle list.
        """
        table = self._seed_table(n_rows=3)
        with qi.QuestDB.from_conf(self._conf()) as client:
            lease = client.reader()
            try:
                in_use, _ = qi._debug_egress_pool_stats(client)
                self.assertEqual(in_use, 1)
                first = lease.query(
                    f'SELECT count() FROM {table}').to_arrow()
                self.assertEqual(first.column(0).to_pylist(), [3])
                in_use, idle = qi._debug_egress_pool_stats(client)
                self.assertEqual(
                    (in_use, idle), (1, 0),
                    f'lease must hold its reader between queries; '
                    f'got in_use={in_use}, idle={idle}')
                second = lease.query(
                    f'SELECT x FROM {table} ORDER BY x').to_arrow()
                self.assertEqual(
                    second.column('x').to_pylist(), [0, 1, 2])
                in_use, _ = qi._debug_egress_pool_stats(client)
                self.assertEqual(in_use, 1)
            finally:
                lease.close()
            in_use, idle = qi._debug_egress_pool_stats(client)
            self.assertEqual(
                (in_use, idle), (0, 1),
                f'close() must return the drained reader to the pool; '
                f'got in_use={in_use}, idle={idle}')

    def test_query_lease_result_handoff_to_worker_then_next_query(self):
        """The lease stays on its creating thread while one result moves
        to a worker. Joining that worker publishes the drained cursor
        before the lease starts its next query on the same reader.
        """
        table = self._seed_table(n_rows=64)
        with qi.QuestDB.from_conf(self._conf()) as client:
            with client.reader() as lease:
                result = lease.query(
                    f'SELECT x FROM {table} ORDER BY x')
                worker_values = []
                worker_errors = []
                worker_ids = []

                def consume_result():
                    worker_ids.append(threading.get_ident())
                    try:
                        worker_values.extend(
                            result.to_arrow().column('x').to_pylist())
                    except BaseException as exc:
                        worker_errors.append(repr(exc))

                worker = threading.Thread(target=consume_result)
                worker.start()
                worker.join(timeout=30)
                self.assertFalse(worker.is_alive(), 'result worker hung')
                self.assertEqual(worker_errors, [])
                self.assertNotEqual(
                    worker_ids, [threading.get_ident()])
                self.assertEqual(worker_values, list(range(64)))

                after = lease.query(
                    f'SELECT count() FROM {table}').to_arrow()
                self.assertEqual(after.column(0).to_pylist(), [64])

            self.assertEqual(
                qi._debug_egress_pool_stats(client), (0, 1))

    def test_query_lease_reset_symbol_dict_false(self):
        """A follow-up lease query with ``reset_symbol_dict=False``
        keeps the connection's SYMBOL dictionary warm and still
        resolves symbol values correctly.
        """
        table = 't_egress_pool_' + uuid.uuid4().hex[:8]
        self.qdb_plain.http_sql_query(
            f'CREATE TABLE {table} '
            '(ts TIMESTAMP, sym SYMBOL, x LONG) '
            'TIMESTAMP(ts) PARTITION BY DAY WAL')
        self.qdb_plain.http_sql_query(
            f"INSERT INTO {table} VALUES "
            f"('2024-01-01T00:00:00Z', 'a', 0),"
            f"('2024-01-01T00:00:01Z', 'b', 1),"
            f"('2024-01-01T00:00:02Z', 'a', 2)")
        self.qdb_plain.retry_check_table(table, min_rows=3)
        self.addCleanup(lambda: self._drop_quietly(table))
        with qi.QuestDB.from_conf(self._conf()) as client:
            with client.reader() as lease:
                first = lease.query(
                    f'SELECT sym, x FROM {table} ORDER BY x').to_pandas()
                self.assertEqual(list(first['sym']), ['a', 'b', 'a'])
                second = lease.query(
                    f'SELECT sym, x FROM {table} ORDER BY x',
                    reset_symbol_dict=False).to_pandas()
                self.assertEqual(list(second['sym']), ['a', 'b', 'a'])
                self.assertEqual(list(second['x']), [0, 1, 2])

    def test_query_lease_rejects_query_while_result_undrained(self):
        """One live result at a time: a second ``lease.query()`` while the
        first result's cursor is still streaming raises
        ``InvalidApiCall``. Draining the first result unblocks the
        lease.
        """
        table = self._seed_table(n_rows=64)
        with qi.QuestDB.from_conf(self._conf()) as client:
            with client.reader() as lease:
                result = lease.query(f'SELECT x FROM {table} ORDER BY x')
                it = result.iter_arrow()
                next(it)
                with self.assertRaisesRegex(
                        qi.QuestDBError,
                        'still open') as cm:
                    lease.query(f'SELECT count() FROM {table}')
                self.assertEqual(
                    cm.exception.code,
                    qi.QuestDBErrorCode.InvalidApiCall)
                list(it)
                after = lease.query(
                    f'SELECT count() FROM {table}').to_arrow()
                self.assertEqual(after.column(0).to_pylist(), [64])

    def test_query_lease_reuses_reader_after_cancel_then_close(self):
        table = self._seed_table(n_rows=64)
        with qi.QuestDB.from_conf(self._conf()) as client:
            with client.reader() as lease:
                result = lease.query(
                    f'SELECT x FROM {table} ORDER BY x')
                it = result.iter_arrow()
                next(it)
                result.cancel()

                with self.assertRaisesRegex(
                        qi.QuestDBError,
                        'still open') as cm:
                    lease.query(f'SELECT count() FROM {table}')
                self.assertEqual(
                    cm.exception.code,
                    qi.QuestDBErrorCode.InvalidApiCall)

                result.close()
                with self.assertRaises(qi.QuestDBError) as cm:
                    next(it)
                self.assertEqual(
                    cm.exception.code,
                    qi.QuestDBErrorCode.InvalidApiCall)

                after = lease.query(
                    f'SELECT count() FROM {table}').to_arrow()
                self.assertEqual(after.column(0).to_pylist(), [64])

            in_use, idle = qi._debug_egress_pool_stats(client)
            self.assertEqual((in_use, idle), (0, 1))

    def test_query_lease_survives_server_error_during_cancel(self):
        with qi.QuestDB.from_conf(self._conf()) as client:
            with client.reader() as lease:
                cancel_error = None
                for _ in range(10):
                    result = lease.query(
                        'SELECT missing_cancel_column '
                        'FROM long_sequence(1)')
                    # Let QUERY_ERROR reach the socket before CANCEL.
                    time.sleep(0.02)
                    try:
                        result.cancel()
                    except qi.QuestDBError as exc:
                        cancel_error = exc
                        break
                    finally:
                        result.close()
                self.assertIsNotNone(
                    cancel_error,
                    'cancel never observed the queued server QUERY_ERROR')

                after = lease.query('SELECT 42 AS v').to_arrow()
                self.assertEqual(after.column('v').to_pylist(), [42])

            in_use, idle = qi._debug_egress_pool_stats(client)
            self.assertEqual((in_use, idle), (0, 1))

    def test_query_lease_terminal_after_undrained_close(self):
        """Closing a result before its clean end tears down the lease's
        transport (Rust ``Cursor::Drop``): the next ``lease.query()``
        raises ``InvalidApiCall``, ``close()`` drops the reader instead
        of recycling it, and the pool refills on demand.
        """
        table = self._seed_table(n_rows=64)
        with qi.QuestDB.from_conf(self._conf()) as client:
            lease = client.reader()
            result = lease.query(f'SELECT x FROM {table} ORDER BY x')
            it = result.iter_arrow()
            next(it)
            result.close()
            del it
            with self.assertRaisesRegex(
                    qi.QuestDBError,
                    'terminal') as cm:
                lease.query(f'SELECT count() FROM {table}')
            self.assertEqual(
                cm.exception.code, qi.QuestDBErrorCode.InvalidApiCall)
            lease.close()
            in_use, idle = qi._debug_egress_pool_stats(client)
            self.assertEqual(
                (in_use, idle), (0, 0),
                f'mid-stream reader must be dropped, not recycled; '
                f'got in_use={in_use}, idle={idle}')
            fresh = client.query(f'SELECT count() FROM {table}').to_arrow()
            self.assertEqual(fresh.column(0).to_pylist(), [64])
            in_use, idle = qi._debug_egress_pool_stats(client)
            self.assertEqual((in_use, idle), (0, 1))

    def test_query_lease_survives_client_side_bind_error(self):
        """A client-side bind failure (unsupported Python type) raises
        before any network round-trip, so it must not mark the lease's
        healthy connection terminal: the next valid query still succeeds.
        """
        table = self._seed_table(n_rows=3)
        with qi.QuestDB.from_conf(self._conf()) as client:
            with client.reader() as lease:
                first = lease.query(
                    f'SELECT count() FROM {table}').to_arrow()
                self.assertEqual(first.column(0).to_pylist(), [3])
                with self.assertRaises(TypeError):
                    lease.query(
                        f'SELECT x FROM {table} WHERE x > $1', [object()])
                after = lease.query(
                    f'SELECT count() FROM {table}').to_arrow()
                self.assertEqual(after.column(0).to_pylist(), [3])

    def test_query_lease_arrow_c_stream_frees_cursor_for_reuse(self):
        """Consuming a lease result to end-of-stream through the native
        Arrow C stream (``__arrow_c_stream__``) must free its cursor, so
        the next query on the lease is accepted rather than rejected as
        'still open'.
        """
        import pyarrow as pa
        table = self._seed_table(n_rows=3)
        with qi.QuestDB.from_conf(self._conf()) as client:
            with client.reader() as lease:
                result = lease.query(f'SELECT x FROM {table} ORDER BY x')
                got = pa.table(result)
                self.assertEqual(got.column('x').to_pylist(), [0, 1, 2])
                after = lease.query(
                    f'SELECT count() FROM {table}').to_arrow()
                self.assertEqual(after.column(0).to_pylist(), [3])

    def test_query_lease_with_block_releases_and_close_is_prompt(self):
        """Leaving the lease's ``with`` block releases the reader and
        the active-use it held on the handle, so a subsequent
        ``QuestDB.close()`` returns promptly instead of looping on the
        5s outstanding-lease warning.
        """
        table = self._seed_table(n_rows=3)
        client = qi.QuestDB.from_conf(self._conf())
        try:
            with client.reader() as lease:
                r = lease.query(f'SELECT count() FROM {table}').to_arrow()
                self.assertEqual(r.column(0).to_pylist(), [3])
                in_use, _ = qi._debug_egress_pool_stats(client)
                self.assertEqual(in_use, 1)
            in_use, idle = qi._debug_egress_pool_stats(client)
            self.assertEqual((in_use, idle), (0, 1))
            start = time.monotonic()
            client.close()
            self.assertLess(
                time.monotonic() - start, 5.0,
                'close() blocked as if a lease were still outstanding')
        finally:
            client.close()

    # ------------------------------------------------------------------
    # query_pool_max — the InvalidApiCall("pool exhausted") error path
    # ------------------------------------------------------------------

    def test_pool_max_exhausted_raises_not_hangs(self):
        """When the reader pool is at ``query_pool_max`` and a second
        borrow is attempted with ``acquire_timeout_ms=0``, the Rust side
        returns ``InvalidApiCall("Reader pool exhausted")``. Verify it
        surfaces as an ``QuestDBError``, not a hang or generic
        socket error."""
        table = self._seed_table(n_rows=64)
        conf = self._conf(
            query_pool_min='1',
            query_pool_max='1',
            acquire_timeout_ms='0')
        with qi.QuestDB.from_conf(conf) as client:
            # Hold one reader by starting an iterator and not
            # exhausting it.
            held_result = client.query(
                f'SELECT x FROM {table} ORDER BY x')
            held_it = held_result.iter_arrow()
            next(held_it)
            try:
                in_use, _ = qi._debug_egress_pool_stats(client)
                self.assertEqual(
                    in_use, 1,
                    f'expected 1 in-use reader for the held cursor; '
                    f'got in_use={in_use}')

                # Second borrow must error, not block.
                with self.assertRaises(qi.QuestDBError) as cm:
                    client.query(
                        f'SELECT count() FROM {table}').to_arrow()
                msg = str(cm.exception).lower()
                self.assertTrue(
                    'exhausted' in msg or 'pool' in msg,
                    f'expected pool-exhaustion message; got '
                    f'{cm.exception!r}')
            finally:
                # Drain the held iterator so the pool is releaseable.
                list(held_it)

    # ------------------------------------------------------------------
    # Conf-string acceptance — BLOCKER 1 of the thermo-nuclear review
    # ------------------------------------------------------------------

    def test_pool_conf_keys_accepted_by_reader(self):
        """The reader's conf parser accepts ``ws::`` / ``wss::`` schemes and
        ignores ``pool_*`` keys. Verify
        that a pool-configured QuestDB handle produces a working egress
        reader (a regression in the accept list would surface as a
        ConfigError on the first ``query()``).
        """
        table = self._seed_table(n_rows=3)
        conf = self._conf(
            sender_pool_min='2',
            sender_pool_max='4',
            query_pool_min='2',
            query_pool_max='4',
            acquire_timeout_ms='5000',
            idle_timeout_ms='30000',
            pool_reap='manual')
        with qi.QuestDB.from_conf(conf) as client:
            r = client.query(f'SELECT count() FROM {table}').to_arrow()
            self.assertEqual(r.column(0).to_pylist(), [3])

    # ------------------------------------------------------------------
    # Concurrency — Barrier-synced, no sleep, deterministic
    # ------------------------------------------------------------------

    def test_concurrent_queries_share_pool(self):
        """N threads × M queries on one QuestDB handle with ``query_pool_min=K``.
        Asserts: no exceptions; pool grew at most to ``K``; all
        readers returned (``in_use==0`` at end); pool stays under
        ``query_pool_max``.
        """
        import threading
        table = self._seed_table(n_rows=3)
        conf = self._conf(query_pool_min='4', query_pool_max='8')
        n_threads = 8
        per_thread = 25
        sql = f'SELECT count() FROM {table}'

        errors = []
        ready = threading.Barrier(n_threads)

        def worker(client):
            try:
                ready.wait(timeout=30)
                for _ in range(per_thread):
                    client.query(sql).to_arrow()
            except BaseException as e:
                errors.append(repr(e))

        with qi.QuestDB.from_conf(conf) as client:
            threads = [
                threading.Thread(target=worker, args=(client,))
                for _ in range(n_threads)]
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=60)

            self.assertEqual(
                errors, [],
                f'{len(errors)}/{n_threads} workers errored: '
                f'{errors[:3]}')
            for t in threads:
                self.assertFalse(t.is_alive(), 'worker thread hung')

            in_use, idle = qi._debug_egress_pool_stats(client)
            self.assertEqual(
                in_use, 0,
                f'workers returned but in_use={in_use}, '
                f'idle={idle}')
            self.assertGreaterEqual(idle, 1)
            self.assertLessEqual(
                idle, 8,
                f'idle={idle} exceeds query_pool_max=8 — auto-grow '
                f'overshot or returns leaked readers')

    def test_long_running_stream_does_not_starve_other_queries(self):
        """Thread A holds a streaming cursor across a Barrier (one
        batch pulled, one pending). Thread B runs M short queries on
        the same QuestDB handle. The pool must auto-grow to a second reader
        for B; B must not wait for A. Pure correctness assertion;
        no timing comparison.
        """
        import threading
        table = self._seed_table(n_rows=64)
        conf = self._conf(query_pool_min='2', query_pool_max='4')

        a_progress = threading.Event()
        b_done = threading.Event()
        errors = []
        b_query_count = 16

        with qi.QuestDB.from_conf(conf) as client:
            def slow_a():
                try:
                    result = client.query(
                        f'SELECT x FROM {table} ORDER BY x')
                    it = result.iter_arrow()
                    next(it)
                    a_progress.set()
                    # Wait for B to finish before draining the rest.
                    self.assertTrue(b_done.wait(timeout=60))
                    list(it)  # drain
                except BaseException as e:
                    errors.append(('A', repr(e)))

            def fast_b():
                try:
                    self.assertTrue(a_progress.wait(timeout=30))
                    for _ in range(b_query_count):
                        client.query(
                            f'SELECT count() FROM {table}').to_arrow()
                    b_done.set()
                except BaseException as e:
                    errors.append(('B', repr(e)))
                    b_done.set()

            ta = threading.Thread(target=slow_a)
            tb = threading.Thread(target=fast_b)
            ta.start()
            tb.start()
            ta.join(timeout=60)
            tb.join(timeout=60)

            self.assertEqual(
                errors, [],
                f'thread errored: {errors[:3]}')
            self.assertFalse(ta.is_alive(), 'thread A hung')
            self.assertFalse(tb.is_alive(), 'thread B hung')

            in_use, idle = qi._debug_egress_pool_stats(client)
            self.assertEqual(in_use, 0)
            # Pool must have grown to at least 2 (A held one, B
            # borrowed at least one more).
            self.assertGreaterEqual(
                idle, 1,
                f'pool did not retain any idle reader; '
                f'in_use={in_use}, idle={idle}')


class TestEgressLeaks(unittest.TestCase):
    """RSS-plateau leak checks for the query/egress path.

    Every result-consumption route owns native memory (reader cursor,
    Arrow batch buffers, schema clones, the stream producer's caches);
    each must free it on its own teardown path. Reuses the shape-based
    plateau harness from ``test_dataframe_leaks``.
    """

    @classmethod
    def setUpClass(cls):
        TestWithDatabase.setUpClass.__func__(cls)

    @classmethod
    def tearDownClass(cls):
        TestWithDatabase.tearDownClass.__func__(cls)

    def setUp(self):
        if self.qdb_plain.version < FIRST_QWP_WS_RELEASE:
            self.skipTest(
                'QWP/WebSocket integration tests require QuestDB 9.4.3+')
        try:
            import psutil  # noqa: F401
        except ImportError:
            self.skipTest('psutil not installed')
        try:
            import pyarrow  # noqa: F401
        except ImportError:
            self.skipTest('pyarrow not installed')

    def _conf(self, **extra):
        conf = (f'ws::addr={self.qdb_plain.host}:'
                f'{self.qdb_plain.http_server_port};')
        for k, v in extra.items():
            conf += f'{k}={v};'
        return conf

    def _seed_table(self, n_rows):
        table = 't_egress_leaks_' + uuid.uuid4().hex[:8]
        self.qdb_plain.http_sql_query(
            f'CREATE TABLE {table} '
            '(ts TIMESTAMP, x LONG, s VARCHAR) '
            'TIMESTAMP(ts) PARTITION BY DAY WAL')
        values = ','.join(
            f"('2024-01-01T00:{i // 60:02d}:{i % 60:02d}Z', {i}, "
            f"'value_{i:06d}')"
            for i in range(n_rows))
        self.qdb_plain.http_sql_query(
            f'INSERT INTO {table} VALUES {values}')
        self.qdb_plain.retry_check_table(table, min_rows=n_rows)
        self.addCleanup(
            lambda: self.qdb_plain.http_sql_query(
                f'DROP TABLE IF EXISTS {table}'))
        return table

    def test_query_consumption_routes_do_not_leak(self):
        from test_dataframe_leaks import _assert_no_leak
        import pyarrow as pa
        # 64 rows span multiple Arrow batches (see the lease tests), so
        # the partial-drain route genuinely abandons a live cursor.
        table = self._seed_table(n_rows=64)
        sql = f'SELECT * FROM {table}'
        with qi.QuestDB.from_conf(self._conf()) as client:
            def work():
                client.query(sql).to_arrow()
                client.query(sql).to_pandas()
                result = client.query(sql)
                for _ in result.iter_arrow():
                    pass
                pa.table(client.query(sql))
                result = client.query(sql)
                next(result.iter_arrow())
                result.close()
                client.query(sql).close()

            _assert_no_leak(self, work, warmup=40, measure=240)


class TestColumnIngressNarrowTypes(unittest.TestCase):
    """End-to-end tests for the narrow Arrow primitive types added to
    ``QuestDB.dataframe`` column ingress: ``pa.int8/16/32`` →
    BYTE/SHORT/INT, unsigned Arrow integers, ``pa.float16/32``,
    and Arrow timestamp units through the QWP/WebSocket classifier.

    The contract: client-side dispatch is a pure function of the
    Arrow input dtype (no content sniffing, no schema hints), and
    target-column coercion (e.g. BYTE landing in a LONG column) is
    handled server-side. Each happy-path test asserts the
    round-trip identity through a fresh table; the coercion tests
    pre-create the target column with a wider type and verify the
    server narrows / widens correctly.
    """

    @classmethod
    def setUpClass(cls):
        TestWithDatabase.setUpClass.__func__(cls)

    @classmethod
    def tearDownClass(cls):
        TestWithDatabase.tearDownClass.__func__(cls)

    # Borrowed rather than restated, like `setUpClass` above. One
    # definition of a version gate is the whole point of it: a copy
    # answers whatever it was copied from at the time, and the drift
    # reads as a passing run.
    _require_qwp_ws = TestWithDatabase._require_qwp_ws

    def setUp(self):
        # The QWP-only column types are written from
        # `TestColumnIngressQwpRowTypes`, which asks for the QuestDB 10
        # they are supported on. This class holds the Arrow primitive
        # types, which QWP has carried since 9.4.3.
        self._require_qwp_ws()

    def _conf(self):
        return (f'ws::addr={self.qdb_plain.host}:'
                f'{self.qdb_plain.http_server_port};')

    def _table(self, prefix='t_narrow_'):
        name = prefix + uuid.uuid4().hex[:8]
        self.addCleanup(lambda: self._drop_quietly(name))
        return name

    def _drop_quietly(self, table):
        try:
            self.qdb_plain.http_sql_query(
                f'DROP TABLE IF EXISTS {table}')
        except Exception:
            pass

    def _create_table(self, table, value_col_sql):
        """Pre-create the table with an explicit ``ts TIMESTAMP``
        designated column plus one value column. Pre-create rather
        than rely on auto-infer so the timestamp column is
        guaranteed to be named ``ts`` (auto-create renames to
        ``timestamp``) and so the server pins the value column type
        for the coercion / round-trip tests."""
        self.qdb_plain.http_sql_query(
            f'CREATE TABLE {table} '
            f'(ts TIMESTAMP, {value_col_sql}) '
            'TIMESTAMP(ts) PARTITION BY DAY WAL')

    def _make_df_with_ts(self, value_col_name, value_arr, n):
        """Build a DataFrame with a designated-timestamp column and
        a single value column. Keeps the per-test setup terse."""
        import pyarrow as pa
        ts = pa.array(
            [1700000000_000000 + i * 1_000_000 for i in range(n)],
            type=pa.timestamp('us', tz='UTC'))
        return pd.DataFrame({
            'ts': pd.array(ts, dtype=pd.ArrowDtype(ts.type)),
            value_col_name: pd.array(
                value_arr, dtype=pd.ArrowDtype(value_arr.type)),
        })

    def _arrow_series(self, values, arrow_type):
        import pyarrow as pa
        arr = pa.array(values, type=arrow_type)
        return pd.array(arr, dtype=pd.ArrowDtype(arr.type))

    def _assert_table_empty(self, table):
        with qi.QuestDB.from_conf(self._conf()) as client:
            got = client.query(f'SELECT count() FROM {table}').to_arrow()
        self.assertEqual(got.column(0).to_pylist(), [0])

    # ---------- happy-path round-trips ----------

    def test_mixed_numpy_and_arrow_decimal_round_trip(self):
        """An Arrow-backed decimal column sharing a frame with a plain
        numpy column forces the manual columnar planner; the decimal
        must still ingest via the Arrow importer rather than be rejected
        as an unsupported column type.
        """
        import pyarrow as pa
        self._require_qwp_ws()
        if self.qdb_plain.version < FIRST_DECIMAL_RELEASE:
            self.skipTest('old server does not support decimal')
        table = self._table()
        self._create_table(table, 'x LONG, amt DECIMAL(18,2)')
        ts = pa.array(
            [1700000000_000000 + i * 1_000_000 for i in range(3)],
            type=pa.timestamp('us', tz='UTC'))
        df = pd.DataFrame({
            'ts': pd.array(ts, dtype=pd.ArrowDtype(ts.type)),
            'x': np.array([1, 2, 3], dtype=np.int64),
            'amt': pd.array(
                [decimal.Decimal('1.50'),
                 decimal.Decimal('-2.25'),
                 decimal.Decimal('3.75')],
                dtype=pd.ArrowDtype(pa.decimal128(18, 2))),
        })
        with qi.QuestDB.from_conf(self._conf()) as client:
            client.dataframe(df, table_name=table, at='ts')
        self.qdb_plain.retry_check_table(table, min_rows=3)
        with qi.QuestDB.from_conf(self._conf()) as client:
            got = client.query(
                f'SELECT x, amt FROM {table} ORDER BY ts').to_arrow()
        self.assertEqual(got.column('x').to_pylist(), [1, 2, 3])
        self.assertEqual(
            got.column('amt').to_pylist(),
            [decimal.Decimal('1.50'),
             decimal.Decimal('-2.25'),
             decimal.Decimal('3.75')])

    def test_timestamp_field_null_and_pre_epoch(self):
        """TIMESTAMP *field* columns ingest NaT as NULL and keep pre-epoch
        values, for datetime64[us] and datetime64[ns] alike. The [ns] case
        is re-exported through Arrow so its INT64_MIN null sentinel survives
        (the zero-copy ns->us path would corrupt it into a 1677 timestamp),
        and both units must yield identical values for the same instant.
        """
        import datetime as _dt
        self._require_qwp_ws()
        utc = _dt.timezone.utc
        at = np.array(
            ['2024-01-01T00:00:00', '2024-01-01T00:00:01',
             '2024-01-01T00:00:02'], dtype='datetime64[us]')
        for unit in ('us', 'ns'):
            table = self._table()
            self._create_table(table, 'vts TIMESTAMP')
            df = pd.DataFrame({
                'ts': at,
                'vts': np.array(
                    ['1960-01-01', 'NaT', '2024-01-03'],
                    dtype=f'datetime64[{unit}]'),
            })
            with qi.QuestDB.from_conf(self._conf()) as client:
                client.dataframe(df, table_name=table, at='ts')
            self.qdb_plain.retry_check_table(table, min_rows=3)
            with qi.QuestDB.from_conf(self._conf()) as client:
                got = client.query(
                    f'SELECT vts FROM {table} ORDER BY ts').to_arrow()
            vals = got.column('vts').to_pylist()
            self.assertEqual(
                vals[0], _dt.datetime(1960, 1, 1, tzinfo=utc), unit)
            self.assertIsNone(vals[1], unit)
            self.assertEqual(
                vals[2], _dt.datetime(2024, 1, 3, tzinfo=utc), unit)

    def test_object_decimal_column_round_trip(self):
        """Object-dtype ``decimal.Decimal`` columns ingest on the columnar
        path (re-exported through Arrow, width/scale inferred), including
        nulls and in a frame mixed with a plain numpy column.
        """
        import decimal as _decimal
        self._require_qwp_ws()
        if self.qdb_plain.version < FIRST_DECIMAL_RELEASE:
            self.skipTest('old server does not support decimal')
        table = self._table()
        self._create_table(table, 'x LONG, amt DECIMAL(18,3)')
        df = pd.DataFrame({
            'ts': np.array(
                ['2024-01-01T00:00:00', '2024-01-01T00:00:01',
                 '2024-01-01T00:00:02'], dtype='datetime64[us]'),
            'x': np.array([1, 2, 3], dtype=np.int64),
            'amt': pd.Series(
                [_decimal.Decimal('1.500'), None,
                 _decimal.Decimal('-2.250')], dtype=object),
        })
        with qi.QuestDB.from_conf(self._conf()) as client:
            client.dataframe(df, table_name=table, at='ts')
        self.qdb_plain.retry_check_table(table, min_rows=3)
        with qi.QuestDB.from_conf(self._conf()) as client:
            got = client.query(
                f'SELECT x, amt FROM {table} ORDER BY ts').to_arrow()
        self.assertEqual(got.column('x').to_pylist(), [1, 2, 3])
        self.assertEqual(
            got.column('amt').to_pylist(),
            [_decimal.Decimal('1.500'), None, _decimal.Decimal('-2.250')])

    def test_int8_round_trip(self):
        """pa.int8 → BYTE wire → server stores as BYTE → egress
        emits pa.int8. QuestDB BYTE is non-nullable; we stay inside
        the value range [-127, 127] to avoid any sentinel ambiguity.
        """
        import pyarrow as pa
        self._require_qwp_ws()
        table = self._table()
        self._create_table(table, 'v BYTE')
        values = pa.array([-127, -1, 0, 1, 127], type=pa.int8())
        df = self._make_df_with_ts('v', values, 5)
        with qi.QuestDB.from_conf(self._conf()) as client:
            client.dataframe(df, table_name=table, at='ts')
        self.qdb_plain.retry_check_table(table, min_rows=5)
        with qi.QuestDB.from_conf(self._conf()) as client:
            got = client.query(
                f'SELECT v FROM {table} ORDER BY ts').to_arrow()
        self.assertEqual(got.column('v').type, pa.int8())
        self.assertEqual(
            got.column('v').to_pylist(), [-127, -1, 0, 1, 127])

    def test_int16_round_trip(self):
        """pa.int16 → SHORT wire. SHORT is non-nullable; stay
        inside [-32767, 32767] to avoid sentinel ambiguity."""
        import pyarrow as pa
        self._require_qwp_ws()
        table = self._table()
        self._create_table(table, 'v SHORT')
        values = pa.array(
            [-32767, -1, 0, 1, 32767], type=pa.int16())
        df = self._make_df_with_ts('v', values, 5)
        with qi.QuestDB.from_conf(self._conf()) as client:
            client.dataframe(df, table_name=table, at='ts')
        self.qdb_plain.retry_check_table(table, min_rows=5)
        with qi.QuestDB.from_conf(self._conf()) as client:
            got = client.query(
                f'SELECT v FROM {table} ORDER BY ts').to_arrow()
        self.assertEqual(got.column('v').type, pa.int16())
        self.assertEqual(
            got.column('v').to_pylist(),
            [-32767, -1, 0, 1, 32767])

    def test_int32_round_trip(self):
        """pa.int32 → INT wire. QuestDB INT uses INT32_MIN as the
        null sentinel; we avoid it here and pin the sentinel
        collision contract separately in
        ``test_int32_min_collapses_to_null``.
        """
        import pyarrow as pa
        self._require_qwp_ws()
        table = self._table()
        self._create_table(table, 'v INT')
        values = pa.array(
            [-2147483647, -1, 0, 1, 2147483647], type=pa.int32())
        df = self._make_df_with_ts('v', values, 5)
        with qi.QuestDB.from_conf(self._conf()) as client:
            client.dataframe(df, table_name=table, at='ts')
        self.qdb_plain.retry_check_table(table, min_rows=5)
        with qi.QuestDB.from_conf(self._conf()) as client:
            got = client.query(
                f'SELECT v FROM {table} ORDER BY ts').to_arrow()
        self.assertEqual(got.column('v').type, pa.int32())
        self.assertEqual(
            got.column('v').to_pylist(),
            [-2147483647, -1, 0, 1, 2147483647])

    def test_float32_round_trip(self):
        import pyarrow as pa
        self._require_qwp_ws()
        table = self._table()
        self._create_table(table, 'v FLOAT')
        values = pa.array(
            [-1.5, 0.0, 0.5, 1.0, 3.14], type=pa.float32())
        df = self._make_df_with_ts('v', values, 5)
        with qi.QuestDB.from_conf(self._conf()) as client:
            client.dataframe(df, table_name=table, at='ts')
        self.qdb_plain.retry_check_table(table, min_rows=5)
        with qi.QuestDB.from_conf(self._conf()) as client:
            got = client.query(
                f'SELECT v FROM {table} ORDER BY ts').to_arrow()
        self.assertEqual(got.column('v').type, pa.float32())
        self.assertEqual(
            got.column('v').to_pylist(),
            [-1.5, 0.0, 0.5, 1.0, 3.140000104904175])

    def test_arrow_wide_numeric_sources_round_trip(self):
        import pyarrow as pa
        self._require_qwp_ws()
        table = self._table()
        self.qdb_plain.http_sql_query(
            f'CREATE TABLE {table} '
            '(ts TIMESTAMP, arrow_l LONG, nullable_l LONG, '
            'arrow_d DOUBLE, nullable_d DOUBLE) '
            'TIMESTAMP(ts) PARTITION BY DAY WAL')
        ts = pa.array(
            [1700000000_000000, 1700000001_000000, 1700000002_000000],
            type=pa.timestamp('us', tz='UTC'))
        df = pd.DataFrame({
            'ts': pd.array(ts, dtype=pd.ArrowDtype(ts.type)),
            'arrow_l': pd.Series(
                pa.array([1, None, -3], type=pa.int64()),
                dtype=pd.ArrowDtype(pa.int64())),
            'nullable_l': pd.Series(
                [4, pd.NA, -6], dtype=pd.Int64Dtype()),
            'arrow_d': pd.Series(
                pa.array([1.5, None, -3.25], type=pa.float64()),
                dtype=pd.ArrowDtype(pa.float64())),
            'nullable_d': pd.Series(
                [4.5, pd.NA, -6.25], dtype=pd.Float64Dtype()),
        })
        with qi.QuestDB.from_conf(self._conf()) as client:
            client.dataframe(df, table_name=table, at='ts')
        self.qdb_plain.retry_check_table(table, min_rows=3)
        with qi.QuestDB.from_conf(self._conf()) as client:
            got = client.query(
                f'SELECT arrow_l, nullable_l, arrow_d, nullable_d '
                f'FROM {table} ORDER BY ts').to_arrow()
        self.assertEqual(got.column('arrow_l').type, pa.int64())
        self.assertEqual(got.column('nullable_l').type, pa.int64())
        self.assertEqual(got.column('arrow_d').type, pa.float64())
        self.assertEqual(got.column('nullable_d').type, pa.float64())
        self.assertEqual(got.column('arrow_l').to_pylist(), [1, None, -3])
        self.assertEqual(got.column('nullable_l').to_pylist(), [4, None, -6])
        self.assertEqual(
            got.column('arrow_d').to_pylist(), [1.5, None, -3.25])
        self.assertEqual(
            got.column('nullable_d').to_pylist(), [4.5, None, -6.25])

    def test_large_utf8_round_trip(self):
        import pyarrow as pa
        self._require_qwp_ws()
        table = self._table()
        self._create_table(table, 'v VARCHAR')
        values = pa.array(
            ['alpha', None, 'gamma', 'delta', 'epsilon'],
            type=pa.large_string())
        df = self._make_df_with_ts('v', values, 5)
        with qi.QuestDB.from_conf(self._conf()) as client:
            client.dataframe(df, table_name=table, at='ts')
        self.qdb_plain.retry_check_table(table, min_rows=5)
        with qi.QuestDB.from_conf(self._conf()) as client:
            got = client.query(
                f'SELECT v FROM {table} ORDER BY ts').to_arrow()
        self.assertEqual(got.column('v').to_pylist(), values.to_pylist())

    # ---------- null handling ----------

    def test_short_is_non_nullable_nulls_become_zero(self):
        """QuestDB SHORT is non-nullable: Arrow nulls written to a
        SHORT column come back as 0, not preserved. This is a
        QuestDB storage contract (no sentinel value for SHORT in
        the existing schema), not a client-side bug. Pinned so a
        future server-side fix (e.g., adding a SHORT null
        sentinel) is flagged."""
        import pyarrow as pa
        self._require_qwp_ws()
        table = self._table()
        self._create_table(table, 'v SHORT')
        values = pa.array(
            [-100, None, 0, None, 200], type=pa.int16())
        df = self._make_df_with_ts('v', values, 5)
        with qi.QuestDB.from_conf(self._conf()) as client:
            client.dataframe(df, table_name=table, at='ts')
        self.qdb_plain.retry_check_table(table, min_rows=5)
        with qi.QuestDB.from_conf(self._conf()) as client:
            got = client.query(
                f'SELECT v FROM {table} ORDER BY ts').to_arrow()
        self.assertEqual(got.column('v').type, pa.int16())
        # Nulls flatten to 0; non-null values round-trip cleanly.
        self.assertEqual(
            got.column('v').to_pylist(),
            [-100, 0, 0, 0, 200])
        self.assertEqual(
            got.column('v').null_count, 0,
            'SHORT is non-nullable; nulls should be erased server-side')

    def test_int32_min_collapses_to_null(self):
        """QuestDB INT uses INT32_MIN as the null sentinel — a
        legitimate user value of INT32_MIN gets folded into NULL
        on read. Same lossy contract as INT64_MIN → LONG NULL
        pinned in ``test_sentinel_collision_is_documented_lossy``;
        repeated here for INT so a regression on either type is
        caught."""
        import pyarrow as pa
        self._require_qwp_ws()
        table = self._table()
        self._create_table(table, 'v INT')
        # INT32_MIN at index 0, ordinary value at index 1.
        values = pa.array(
            [-2147483648, 42], type=pa.int32())
        df = self._make_df_with_ts('v', values, 2)
        with qi.QuestDB.from_conf(self._conf()) as client:
            client.dataframe(df, table_name=table, at='ts')
        self.qdb_plain.retry_check_table(table, min_rows=2)
        with qi.QuestDB.from_conf(self._conf()) as client:
            got = client.query(
                f'SELECT v FROM {table} ORDER BY ts').to_arrow()
        self.assertEqual(got.column('v').type, pa.int32())
        self.assertEqual(
            got.column('v').null_count, 1,
            'expected the INT32_MIN row to be folded into NULL '
            'by QuestDB INT storage')
        self.assertTrue(got.column('v').is_null()[0].as_py())
        self.assertFalse(got.column('v').is_null()[1].as_py())
        self.assertEqual(got.column('v')[1].as_py(), 42)

    # ---------- server-side coercion ----------

    def test_int8_into_existing_long_column_widens_server_side(self):
        """Pre-create a LONG column and write ``pa.int8`` into it.
        The server widens to LONG on insert (the policy-2 contract:
        target-column coercion is the server's job)."""
        import pyarrow as pa
        self._require_qwp_ws()
        table = self._table()
        # Pre-create the table with v as LONG, not BYTE.
        self.qdb_plain.http_sql_query(
            f'CREATE TABLE {table} '
            '(ts TIMESTAMP, v LONG) '
            'TIMESTAMP(ts) PARTITION BY DAY WAL')
        values = pa.array([1, 2, 3, 4, 5], type=pa.int8())
        df = self._make_df_with_ts('v', values, 5)
        with qi.QuestDB.from_conf(self._conf()) as client:
            client.dataframe(df, table_name=table, at='ts')
        self.qdb_plain.retry_check_table(table, min_rows=5)
        with qi.QuestDB.from_conf(self._conf()) as client:
            got = client.query(
                f'SELECT v FROM {table} ORDER BY ts').to_arrow()
        self.assertEqual(got.column('v').type, pa.int64())
        self.assertEqual(
            got.column('v').to_pylist(), [1, 2, 3, 4, 5])

    def test_float32_into_existing_double_column_widens(self):
        import pyarrow as pa
        self._require_qwp_ws()
        table = self._table()
        self.qdb_plain.http_sql_query(
            f'CREATE TABLE {table} '
            '(ts TIMESTAMP, v DOUBLE) '
            'TIMESTAMP(ts) PARTITION BY DAY WAL')
        values = pa.array([0.5, 1.5, 2.5], type=pa.float32())
        df = self._make_df_with_ts('v', values, 3)
        with qi.QuestDB.from_conf(self._conf()) as client:
            client.dataframe(df, table_name=table, at='ts')
        self.qdb_plain.retry_check_table(table, min_rows=3)
        with qi.QuestDB.from_conf(self._conf()) as client:
            got = client.query(
                f'SELECT v FROM {table} ORDER BY ts').to_arrow()
        self.assertEqual(got.column('v').type, pa.float64())
        self.assertEqual(
            got.column('v').to_pylist(), [0.5, 1.5, 2.5])

    # ---------- unhappy paths ----------

    # ---------- UUID (Category C — canonical mirror + extension type) ----------

    @staticmethod
    def _extract_uuid_storage(col):
        """Return the FSB(16) storage bytes from an egress UUID
        column, whether or not pyarrow has the `arrow.uuid`
        extension type registered."""
        import pyarrow as pa
        if isinstance(col.type, pa.BaseExtensionType):
            return col.combine_chunks().storage.to_pylist()
        return col.to_pylist()

    def test_uuid_claim_on_wrong_width_is_rejected(self):
        """A UUID claim requires 16-byte values; an 8-byte column
        fails client-side rather than sending malformed rows."""
        import pyarrow as pa
        self._require_qwp_ws()
        table = self._table()
        values = pa.array([b'\x00' * 8, b'\xff' * 8], type=pa.binary(8))
        df = self._make_df_with_ts('v', values, 2)
        with qi.QuestDB.from_conf(self._conf()) as client:
            with self.assertRaises(qi.QuestDBError):
                client.dataframe(df, table_name=table, at='ts',
                                 schema_overrides={'v': 'uuid'})

    def test_uuid_string_into_uuid_column_via_server_coercion(self):
        """Strict-mirror policy: `pa.string()` always maps to
        VARCHAR on the wire. When the target column is UUID,
        QuestDB's server-side INSERT coercion narrows the VARCHAR
        string into a UUID — the canonical "policy-2" contract
        from the design doc."""
        import pyarrow as pa
        import uuid as uuid_mod
        self._require_qwp_ws()
        table = self._table()
        self._create_table(table, 'v UUID')
        uuids = [uuid_mod.uuid4() for _ in range(3)]
        values = pa.array([str(u) for u in uuids], type=pa.string())
        df = self._make_df_with_ts('v', values, 3)
        with qi.QuestDB.from_conf(self._conf()) as client:
            client.dataframe(df, table_name=table, at='ts')
        self.qdb_plain.retry_check_table(table, min_rows=3)
        with qi.QuestDB.from_conf(self._conf()) as client:
            got = client.query(
                f'SELECT v FROM {table} ORDER BY ts').to_arrow()
        # Server-side coercion lands the value as a UUID; egress emits
        # the FSB(16) storage as canonical RFC 4122 bytes, the same as
        # the claimed-binary path.
        expected = [u.bytes for u in uuids]
        self.assertEqual(self._extract_uuid_storage(got.column('v')),
                         expected)

    def test_invalid_uuid_string_is_rejected_by_server(self):
        """Bad UUID strings written into a UUID target surface as
        an QuestDBError (server rejection), not a silent corruption
        or a connection poisoning. Verification item #4 from the
        design doc."""
        import pyarrow as pa
        self._require_qwp_ws()
        table = self._table()
        self._create_table(table, 'v UUID')
        values = pa.array(
            ['not-a-uuid', 'also-not'], type=pa.string())
        df = self._make_df_with_ts('v', values, 2)
        with qi.QuestDB.from_conf(self._conf()) as client:
            with self.assertRaises(qi.QuestDBError):
                client.dataframe(df, table_name=table, at='ts')

    def test_fsb16_rejected_by_row_ilp(self):
        """Row-ILP (`Sender.dataframe`) genuinely does not support
        fixed-size binary columns. `_FIELD_TARGETS_ROW` includes
        neither `col_target_column_uuid` nor
        `col_target_column_arrow`, so the resolver fails to map an
        FSB(16) column to any target. This pins that
        protocol-asymmetry contract."""
        import pyarrow as pa
        import uuid as uuid_mod
        self._require_qwp_ws()
        values = pa.array(
            [uuid_mod.uuid4().bytes for _ in range(2)],
            type=pa.binary(16))
        df = self._make_df_with_ts('v', values, 2)
        conf = (
            f'tcp::addr={self.qdb_plain.host}:'
            f'{self.qdb_plain.line_tcp_port};')
        with qi.Sender.from_conf(conf) as sender:
            with self.assertRaises(qi.QuestDBError):
                sender.dataframe(df, table_name='dummy', at='ts')

    # ---------- UInt32 / IPV4 policy ----------

    def test_pa_uint32_round_trip_as_long(self):
        """Plain ``pa.uint32()`` widens to LONG on QuestDB.dataframe.

        The Rust Arrow ingestion path reserves IPV4 for UInt32 fields
        with ``questdb.column_type=ipv4`` metadata. Pandas drops Arrow
        field metadata before it reaches this planner, so this path
        follows the plain-UInt32 rule.
        """
        import pyarrow as pa
        self._require_qwp_ws()
        table = self._table()
        self._create_table(table, 'v LONG')
        ints = [1, 2, 3, 0, 4294967295]
        values = pa.array(ints, type=pa.uint32())
        df = self._make_df_with_ts('v', values, 5)
        with qi.QuestDB.from_conf(self._conf()) as client:
            client.dataframe(df, table_name=table, at='ts')
        self.qdb_plain.retry_check_table(table, min_rows=5)
        with qi.QuestDB.from_conf(self._conf()) as client:
            got = client.query(
                f'SELECT v FROM {table} ORDER BY ts').to_arrow()
        self.assertEqual(got.column('v').type, pa.int64())
        self.assertEqual(got.column('v').to_pylist(), ints)

    def test_pa_uint64_within_i64_range_round_trips_as_long(self):
        import pyarrow as pa
        self._require_qwp_ws()
        table = self._table()
        self._create_table(table, 'v LONG')
        ints = [0, 2 ** 63 - 1, 42]
        values = pa.array(ints, type=pa.uint64())
        df = self._make_df_with_ts('v', values, 3)
        with qi.QuestDB.from_conf(self._conf()) as client:
            client.dataframe(df, table_name=table, at='ts')
        self.qdb_plain.retry_check_table(table, min_rows=3)
        with qi.QuestDB.from_conf(self._conf()) as client:
            got = client.query(
                f'SELECT v FROM {table} ORDER BY ts').to_arrow()
        self.assertEqual(got.column('v').type, pa.int64())
        self.assertEqual(got.column('v').to_pylist(), ints)

    def test_arrow_classifier_numeric_mix_round_trips_on_real_server(self):
        import pyarrow as pa
        self._require_qwp_ws()
        table = self._table()
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
            'u32': pd.Series(
                pa.array([1, 2 ** 31, 2 ** 32 - 1], type=pa.uint32()),
                dtype=pd.ArrowDtype(pa.uint32())),
            'u64': pd.Series(
                pa.array([1, 2 ** 63 - 1, None], type=pa.uint64()),
                dtype=pd.ArrowDtype(pa.uint64())),
            'f16': pd.Series(
                pa.array(np.array([1.5, 2.5, 3.5], dtype=np.float16),
                         type=pa.float16()),
                dtype=pd.ArrowDtype(pa.float16())),
        })

        with qi.QuestDB.from_conf(self._conf()) as client:
            client.dataframe(df, table_name=table, at='ts')
        self.qdb_plain.retry_check_table(table, min_rows=3)
        with qi.QuestDB.from_conf(self._conf()) as client:
            got = client.query(
                f'SELECT u8, u16, u32, u64, f16 FROM {table} '
                'ORDER BY timestamp'
            ).to_arrow()

        self.assertEqual(got.column('u8').type, pa.int32())
        self.assertEqual(got.column('u16').type, pa.int32())
        self.assertEqual(got.column('u32').type, pa.int64())
        self.assertEqual(got.column('u64').type, pa.int64())
        self.assertEqual(got.column('f16').type, pa.float32())
        self.assertEqual(got.column('u8').to_pylist(), [1, 2, None])
        self.assertEqual(got.column('u16').to_pylist(), [1000, None, 3000])
        self.assertEqual(got.column('u32').to_pylist(), [1, 2 ** 31, 2 ** 32 - 1])
        self.assertEqual(got.column('u64').to_pylist(), [1, 2 ** 63 - 1, None])
        self.assertEqual(got.column('f16').to_pylist(), [1.5, 2.5, 3.5])

    def test_pa_uint64_above_i64_max_rejected_before_publish(self):
        import pyarrow as pa
        self._require_qwp_ws()
        table = self._table()
        self._create_table(table, 'v LONG')
        values = pa.array([0, 2 ** 63], type=pa.uint64())
        df = self._make_df_with_ts('v', values, 2)
        with qi.QuestDB.from_conf(self._conf()) as client:
            with self.assertRaisesRegex(
                    qi.QuestDBError,
                    r'UInt64 value 9223372036854775808 .* does not fit QuestDB LONG'):
                client.dataframe(df, table_name=table, at='ts')
        self._assert_table_empty(table)

    # ---------- TIMESTAMP validation policy ----------

    def test_arrow_designated_timestamp_null_rejected_before_publish(self):
        import pyarrow as pa
        self._require_qwp_ws()
        table = self._table()
        self._create_table(table, 'v LONG')
        ts_type = pa.timestamp('us', tz='UTC')
        df = pd.DataFrame({
            'ts': self._arrow_series(
                [1700000000_000000, None], ts_type),
            'v': self._arrow_series([1, 2], pa.int64()),
        })
        with qi.QuestDB.from_conf(self._conf()) as client:
            with self.assertRaises(qi.QuestDBError) as cm:
                client.dataframe(df, table_name=table, at='ts')
        self.assertIn('null', str(cm.exception).lower())
        self._assert_table_empty(table)

    def test_arrow_designated_timestamp_negative_rejected_before_publish(self):
        import pyarrow as pa
        self._require_qwp_ws()
        table = self._table()
        self._create_table(table, 'v LONG')
        ts_type = pa.timestamp('us', tz='UTC')
        df = pd.DataFrame({
            'ts': self._arrow_series([-1, 1700000000_000000], ts_type),
            'v': self._arrow_series([1, 2], pa.int64()),
        })
        with qi.QuestDB.from_conf(self._conf()) as client:
            with self.assertRaises(qi.QuestDBError) as cm:
                client.dataframe(df, table_name=table, at='ts')
        self.assertIn('unix epoch', str(cm.exception).lower())
        self._assert_table_empty(table)

    def test_arrow_designated_timestamp_ms_s_units_widen_to_micros(self):
        """ArrowDtype ``timestamp('ms')`` / ``timestamp('s')``
        designated-``at`` columns are widened to microseconds in Rust by
        the millis/seconds designated-timestamp FFI (no client-side
        cast). A mixed frame (numpy value column) forces the manual
        columnar planner, the path that routes these units to the new
        FFI. The sub-second 'ms' value proves the scale is applied rather
        than the raw value copied straight onto the micros wire."""
        import pyarrow as pa
        import numpy as np
        self._require_qwp_ws()
        # 2023-06-15T12:34:56(.789) UTC, expressed in each unit.
        for unit, raw, scale in (('ms', 1686832496789, 1000),
                                 ('s', 1686832496, 1_000_000)):
            table = self._table()
            self._create_table(table, 'v LONG')
            df = pd.DataFrame({
                'ts': self._arrow_series(
                    [raw], pa.timestamp(unit, tz='UTC')),
                'v': pd.Series([1], dtype=np.int64),  # numpy -> manual
            })
            with qi.QuestDB.from_conf(self._conf()) as client:
                client.dataframe(df, table_name=table, at='ts')
            self.qdb_plain.retry_check_table(table, min_rows=1)
            with qi.QuestDB.from_conf(self._conf()) as client:
                got = client.query(f'SELECT ts FROM {table}').to_arrow()
            self.assertEqual(
                got.column('ts').type, pa.timestamp('us', tz='UTC'))
            got_us = got.column('ts').cast(pa.int64()).to_pylist()[0]
            self.assertEqual(got_us, raw * scale)

    def test_arrow_designated_timestamp_ms_null_rejected_before_publish(self):
        """The plan validator's designated-timestamp null guard covers
        the widened ms/s sources too, not just us/ns. A numpy value
        column keeps the frame on the manual planner where the guard
        runs."""
        import pyarrow as pa
        import numpy as np
        self._require_qwp_ws()
        table = self._table()
        self._create_table(table, 'v LONG')
        df = pd.DataFrame({
            'ts': self._arrow_series(
                [1686832496789, None], pa.timestamp('ms', tz='UTC')),
            'v': pd.Series([1, 2], dtype=np.int64),  # numpy -> manual
        })
        with qi.QuestDB.from_conf(self._conf()) as client:
            with self.assertRaises(
                    qi.UnsupportedDataFrameShapeError) as cm:
                client.dataframe(df, table_name=table, at='ts')
        reasons = ' '.join(
            f['reason'] for f in cm.exception.column_failures).lower()
        self.assertIn('null', reasons)
        self._assert_table_empty(table)

    def test_arrow_timestamp_field_null_is_ingested(self):
        """Only the designated ``at`` timestamp is validated; a
        non-designated TIMESTAMP *field* column is passed through, so a
        null field timestamp is ingested as NULL (not rejected)."""
        import pyarrow as pa
        self._require_qwp_ws()
        table = self._table()
        self._create_table(table, 'event_ts TIMESTAMP, v LONG')
        ts_type = pa.timestamp('us', tz='UTC')
        df = pd.DataFrame({
            'ts': self._arrow_series(
                [1700000000_000000, 1700000001_000000], ts_type),
            'event_ts': self._arrow_series(
                [1700000002_000000, None], ts_type),
            'v': self._arrow_series([1, 2], pa.int64()),
        })
        with qi.QuestDB.from_conf(self._conf()) as client:
            client.dataframe(df, table_name=table, at='ts')
        self.qdb_plain.retry_check_table(table, min_rows=2)
        with qi.QuestDB.from_conf(self._conf()) as client:
            got = client.query(
                f'SELECT event_ts FROM {table} ORDER BY ts').to_arrow()
        self.assertEqual(
            got.column('event_ts').cast(pa.int64()).to_pylist(),
            [1700000002_000000, None])

    def test_arrow_timestamp_field_negative_is_ingested(self):
        """Only the designated ``at`` timestamp rejects pre-epoch values;
        a non-designated TIMESTAMP *field* column is passed through, so a
        negative (pre-1970) field timestamp is ingested verbatim."""
        import pyarrow as pa
        self._require_qwp_ws()
        table = self._table()
        self._create_table(table, 'event_ts TIMESTAMP, v LONG')
        ts_type = pa.timestamp('us', tz='UTC')
        df = pd.DataFrame({
            'ts': self._arrow_series(
                [1700000000_000000, 1700000001_000000], ts_type),
            'event_ts': self._arrow_series(
                [-1, 1700000002_000000], ts_type),
            'v': self._arrow_series([1, 2], pa.int64()),
        })
        with qi.QuestDB.from_conf(self._conf()) as client:
            client.dataframe(df, table_name=table, at='ts')
        self.qdb_plain.retry_check_table(table, min_rows=2)
        with qi.QuestDB.from_conf(self._conf()) as client:
            got = client.query(
                f'SELECT event_ts FROM {table} ORDER BY ts').to_arrow()
        self.assertEqual(
            got.column('event_ts').cast(pa.int64()).to_pylist(),
            [-1, 1700000002_000000])

    def test_arrow_multi_chunk_buffer_reuse_boundary_rows(self):
        import pyarrow as pa
        self._require_qwp_ws()
        table = self._table()
        self.qdb_plain.http_sql_query(
            f'CREATE TABLE {table} '
            '(ts TIMESTAMP, seq LONG, price DOUBLE) '
            'TIMESTAMP(ts) PARTITION BY DAY WAL')
        rows = 64_001
        ts_values = 1_700_000_000_000_000 + np.arange(rows, dtype=np.int64)
        seq_values = np.arange(rows, dtype=np.int64)
        df = pd.DataFrame({
            'ts': self._arrow_series(
                ts_values,
                pa.timestamp('us', tz='UTC')),
            'seq': self._arrow_series(seq_values, pa.int64()),
            'price': self._arrow_series(
                seq_values.astype(np.float64) * 0.25,
                pa.float64()),
        })
        with qi.QuestDB.from_conf(self._conf()) as client:
            qi._debug_dataframe_columnar_io_stats(enabled=True, reset=True)
            try:
                client.dataframe(
                    df, table_name=table, at='ts',
                    max_rows_per_batch=32000)
            finally:
                io_stats = qi._debug_dataframe_columnar_io_stats(
                    enabled=False)
        self.assertEqual(io_stats['flush_calls'], 3)
        self.assertEqual(io_stats['sync_calls'], 1)

        self.qdb_plain.retry_check_table(table, min_rows=rows)
        with qi.QuestDB.from_conf(self._conf()) as client:
            count = client.query(
                f'SELECT count() FROM {table}').to_arrow()
            expected_seq = [0, 31999, 32000, 32001, 63999, 64000]
            got = client.query(
                f'SELECT seq, price FROM {table} '
                f'WHERE seq IN ({", ".join(str(v) for v in expected_seq)}) '
                f'ORDER BY seq').to_arrow()
        self.assertEqual(count.column(0).to_pylist(), [rows])
        self.assertEqual(got.column('seq').to_pylist(), expected_seq)
        self.assertEqual(
            got.column('price').to_pylist(),
            [value * 0.25 for value in expected_seq])

    def test_dataframe_oversize_batch_splits_and_lands_all_rows(self):
        """A batch larger than the negotiated cap is split by the column
        sender into multiple cap-sized frames; every row must still land. A
        tiny ``max_buf_size`` forces the split without a huge frame."""
        import pyarrow as pa
        self._require_qwp_ws()
        table = self._table()
        self.qdb_plain.http_sql_query(
            f'CREATE TABLE {table} (ts TIMESTAMP, seq LONG) '
            'TIMESTAMP(ts) PARTITION BY DAY WAL')
        rows = 5000  # ~16 B/row -> ~80 KB, vs the 4096 B cap -> many frames
        ts = pa.array(
            [1_700_000_000_000_000 + i * 1_000_000 for i in range(rows)],
            type=pa.timestamp('us', tz='UTC'))
        seq = pa.array(list(range(rows)), type=pa.int64())
        df = pd.DataFrame({
            'ts': pd.array(ts, dtype=pd.ArrowDtype(ts.type)),
            'seq': pd.array(seq, dtype=pd.ArrowDtype(seq.type)),
        })
        conf = (f'ws::addr={self.qdb_plain.host}:'
                f'{self.qdb_plain.http_server_port};max_buf_size=4096;')
        with qi.QuestDB.from_conf(conf) as client:
            # One logical batch (max_rows_per_batch == rows) forces the core to
            # split it, rather than the Python chunker pre-splitting by rows.
            client.dataframe(df, table_name=table, at='ts',
                             max_rows_per_batch=rows)
        self.qdb_plain.retry_check_table(table, min_rows=rows)
        with qi.QuestDB.from_conf(conf) as client:
            got = client.query(
                f'SELECT count(), min(seq), max(seq) FROM {table}').to_arrow()
        self.assertEqual(got.column(0).to_pylist(), [rows])
        self.assertEqual(got.column(1).to_pylist(), [0])
        self.assertEqual(got.column(2).to_pylist(), [rows - 1])

    def test_dataframe_single_oversize_value_raises_batch_too_large(self):
        """A single value larger than the cap is irreducible: the split
        bottoms out and surfaces batch_too_large rather than silently
        dropping data."""
        import pyarrow as pa
        self._require_qwp_ws()
        table = self._table()
        self.qdb_plain.http_sql_query(
            f'CREATE TABLE {table} (ts TIMESTAMP, v VARCHAR) '
            'TIMESTAMP(ts) PARTITION BY DAY WAL')
        big = 'x' * 8192  # one value > the 4096 cap
        ts = pa.array(
            [1_700_000_000_000_000, 1_700_000_001_000_000],
            type=pa.timestamp('us', tz='UTC'))
        v = pa.array(['small', big], type=pa.string())
        df = pd.DataFrame({
            'ts': pd.array(ts, dtype=pd.ArrowDtype(ts.type)),
            'v': pd.array(v, dtype=pd.ArrowDtype(v.type)),
        })
        conf = (f'ws::addr={self.qdb_plain.host}:'
                f'{self.qdb_plain.http_server_port};max_buf_size=4096;')
        with qi.QuestDB.from_conf(conf) as client:
            with self.assertRaises(qi.QuestDBError) as ctx:
                client.dataframe(df, table_name=table, at='ts')
        self.assertEqual(
            ctx.exception.code, qi.QuestDBErrorCode.BatchTooLarge)

    def test_arrow_explicit_symbol_list_auto_creates_symbol_column(self):
        import pyarrow as pa
        self._require_qwp_ws()
        table = self._table()
        df = pd.DataFrame({
            'ts': self._arrow_series(
                [1700000000_000000, 1700000001_000000, 1700000002_000000],
                pa.timestamp('us', tz='UTC')),
            'region': self._arrow_series(
                ['us-east', 'us-west', 'us-east'], pa.string()),
            'note': self._arrow_series(
                ['alpha', 'beta', 'gamma'], pa.string()),
            'seq': pd.Series([1, 2, 3], dtype='int64'),
        })
        with qi.QuestDB.from_conf(self._conf()) as client:
            client.dataframe(
                df,
                table_name=table,
                at='ts',
                symbols=['region'])

        resp = self.qdb_plain.retry_check_table(table, min_rows=3)
        col_types = {c['name']: c['type'] for c in resp['columns']}
        self.assertEqual(col_types['region'], 'SYMBOL')
        self.assertEqual(col_types['note'], 'VARCHAR')
        self.assertEqual(col_types['seq'], 'LONG')
        scrubbed = [row[:-1] for row in resp['dataset']]
        self.assertEqual(
            scrubbed,
            [['us-east', 'alpha', 1],
             ['us-west', 'beta', 2],
             ['us-east', 'gamma', 3]])

    def test_arrow_symbols_false_forces_dict_columns_to_varchar(self):
        import pyarrow as pa
        self._require_qwp_ws()
        table = self._table()
        dict_type = pa.dictionary(pa.int32(), pa.string())
        df = pd.DataFrame({
            'ts': self._arrow_series(
                [1700000000_000000, 1700000001_000000, 1700000002_000000],
                pa.timestamp('us', tz='UTC')),
            'region': self._arrow_series(
                ['us-east', 'us-west', 'us-east'], dict_type),
            'note': self._arrow_series(
                ['alpha', 'beta', 'gamma'], dict_type),
        })
        with qi.QuestDB.from_conf(self._conf()) as client:
            client.dataframe(df, table_name=table, at='ts', symbols=False)

        resp = self.qdb_plain.retry_check_table(table, min_rows=3)
        col_types = {c['name']: c['type'] for c in resp['columns']}
        self.assertEqual(col_types['region'], 'VARCHAR')
        self.assertEqual(col_types['note'], 'VARCHAR')

    def test_arrow_partial_symbol_list_demotes_unlisted_dict_to_varchar(self):
        import pyarrow as pa
        self._require_qwp_ws()
        table = self._table()
        dict_type = pa.dictionary(pa.int32(), pa.string())
        df = pd.DataFrame({
            'ts': self._arrow_series(
                [1700000000_000000, 1700000001_000000, 1700000002_000000],
                pa.timestamp('us', tz='UTC')),
            'region': self._arrow_series(
                ['us-east', 'us-west', 'us-east'], dict_type),
            'note': self._arrow_series(
                ['alpha', 'beta', 'gamma'], dict_type),
        })
        with qi.QuestDB.from_conf(self._conf()) as client:
            client.dataframe(
                df, table_name=table, at='ts', symbols=['region'])

        resp = self.qdb_plain.retry_check_table(table, min_rows=3)
        col_types = {c['name']: c['type'] for c in resp['columns']}
        self.assertEqual(col_types['region'], 'SYMBOL')
        self.assertEqual(col_types['note'], 'VARCHAR')

    def test_ipv4_string_coercion_is_unsupported(self):
        """Unlike UUID (where the server parses VARCHAR strings
        into UUIDs), QuestDB does NOT currently support VARCHAR →
        IPV4 coercion at insert time. Writing `pa.string()` IP
        addresses into an IPV4 column surfaces a server rejection
        ("type coercion from VARCHAR to IPv4 is not supported").
        Pin this contract — if a future QuestDB release adds the
        coercion, this test flips and the IPV4 path joins UUID's
        string-coercion ergonomics."""
        import pyarrow as pa
        self._require_qwp_ws()
        table = self._table()
        self._create_table(table, 'v IPV4')
        ips = ['192.168.1.10', '10.0.0.1', '127.0.0.1']
        values = pa.array(ips, type=pa.string())
        df = self._make_df_with_ts('v', values, 3)
        with qi.QuestDB.from_conf(self._conf()) as client:
            with self.assertRaises(qi.QuestDBError) as cm:
                client.dataframe(df, table_name=table, at='ts')
            self.assertIn('ipv4', str(cm.exception).lower())

    def test_invalid_ipv4_string_is_rejected_by_server(self):
        import pyarrow as pa
        self._require_qwp_ws()
        table = self._table()
        self._create_table(table, 'v IPV4')
        values = pa.array(
            ['not-an-ip', '999.999.999.999'], type=pa.string())
        df = self._make_df_with_ts('v', values, 2)
        with qi.QuestDB.from_conf(self._conf()) as client:
            with self.assertRaises(qi.QuestDBError):
                client.dataframe(df, table_name=table, at='ts')

    def test_pa_uint32_is_routed_to_long_not_ipv4(self):
        """Plain ``pa.uint32()`` is not enough to select IPV4."""
        import pyarrow as pa
        self._require_qwp_ws()
        table = self._table()
        # Pre-create with an IPV4 column. The server will reject LONG
        # wire values landing in IPV4 with a schema mismatch.
        self._create_table(table, 'v IPV4')
        values = pa.array([1, 2, 3], type=pa.uint32())
        df = self._make_df_with_ts('v', values, 3)
        with qi.QuestDB.from_conf(self._conf()) as client:
            with self.assertRaises(qi.QuestDBError):
                client.dataframe(df, table_name=table, at='ts')

    # ---------- LONG256 (Category C — FixedSizeBinary(32)) ----------

    def test_fsb32_rejected_by_row_ilp(self):
        """`Sender.dataframe` runs the NumPy planner, which refuses
        FSB(32) outright: nothing on that planner can claim the column
        as LONG256, and row-ILP has no target for opaque bytes either
        (`_FIELD_TARGETS_ROW` lists neither `col_target_column_arrow`
        nor a LONG256 target). Symmetric to the FSB(16) row-ILP
        rejection test."""
        import pyarrow as pa
        self._require_qwp_ws()
        values = pa.array(
            [bytes(range(32)), bytes(range(32, 64))],
            type=pa.binary(32))
        df = self._make_df_with_ts('v', values, 2)
        conf = (
            f'tcp::addr={self.qdb_plain.host}:'
            f'{self.qdb_plain.line_tcp_port};')
        with qi.Sender.from_conf(conf) as sender:
            with self.assertRaises(qi.QuestDBError):
                sender.dataframe(df, table_name='dummy', at='ts')

    def test_pa_uint8_auto_creates_as_int(self):
        """``pa.uint8()`` widens to INT. Auto-create (no ``_create_table``)
        pins the wire type; a pre-created SHORT column would mask it via
        server coercion. Order by ``v``: auto-create renames ``ts`` to
        ``timestamp``."""
        import pyarrow as pa
        self._require_qwp_ws()
        table = self._table()
        values = pa.array([0, 1, 255], type=pa.uint8())
        df = self._make_df_with_ts('v', values, 3)
        with qi.QuestDB.from_conf(self._conf()) as client:
            client.dataframe(df, table_name=table, at='ts')
        self.qdb_plain.retry_check_table(table, min_rows=3)
        with qi.QuestDB.from_conf(self._conf()) as client:
            got = client.query(
                f'SELECT v FROM {table} ORDER BY v').to_arrow()
        self.assertEqual(got.column('v').type, pa.int32())
        self.assertEqual(got.column('v').to_pylist(), [0, 1, 255])

    def test_pa_uint16_auto_creates_as_int(self):
        """``pa.uint16()`` widens to INT (CHAR only with
        ``questdb.column_type=char`` metadata, which pandas drops). Like the
        uint8 case, auto-create pins the wire type."""
        import pyarrow as pa
        self._require_qwp_ws()
        table = self._table()
        values = pa.array([0, 1, 65535], type=pa.uint16())
        df = self._make_df_with_ts('v', values, 3)
        with qi.QuestDB.from_conf(self._conf()) as client:
            client.dataframe(df, table_name=table, at='ts')
        self.qdb_plain.retry_check_table(table, min_rows=3)
        with qi.QuestDB.from_conf(self._conf()) as client:
            got = client.query(
                f'SELECT v FROM {table} ORDER BY v').to_arrow()
        self.assertEqual(got.column('v').type, pa.int32())
        self.assertEqual(got.column('v').to_pylist(), [0, 1, 65535])




class TestColumnIngressQwpRowTypes(unittest.TestCase):
    """Column ingress of the QWP-only column types.

    As `TestEgressQwpRowTypes`: the gate is `setUp`, so it covers every
    test in the class rather than the ones somebody remembered to mark,
    and it asks for the QuestDB 10 these types are supported on.
    `TestColumnIngressNarrowTypes` next door keeps the plain
    QWP/WebSocket gate, which is what its Arrow primitive types need.
    """

    @classmethod
    def setUpClass(cls):
        TestWithDatabase.setUpClass.__func__(cls)

    @classmethod
    def tearDownClass(cls):
        TestWithDatabase.tearDownClass.__func__(cls)

    _require_qwp_ws = TestWithDatabase._require_qwp_ws
    _require_qwp_row_types = TestWithDatabase._require_qwp_row_types
    _conf = TestColumnIngressNarrowTypes._conf
    _table = TestColumnIngressNarrowTypes._table
    _drop_quietly = TestColumnIngressNarrowTypes._drop_quietly
    _create_table = TestColumnIngressNarrowTypes._create_table
    _make_df_with_ts = TestColumnIngressNarrowTypes._make_df_with_ts
    # Reading a `staticmethod` off the class hands back the plain
    # function, which would bind as an instance method here and take
    # `self` as its first argument.
    _extract_uuid_storage = staticmethod(
        TestColumnIngressNarrowTypes._extract_uuid_storage)

    def setUp(self):
        self._require_qwp_row_types()

    def test_uuid_round_trip_via_fsb16(self):
        """``pa.fixed_size_binary(16)`` claimed as UUID via
        ``schema_overrides`` → UUID wire → server stores as UUID →
        egress emits the same FSB(16) storage bytes. A 16-byte width
        claims nothing on its own, so the override is what selects
        UUID over BINARY. Round-trip is byte-identity on canonical
        RFC 4122 bytes."""
        import pyarrow as pa
        import uuid as uuid_mod
        self._require_qwp_ws()
        table = self._table()
        self._create_table(table, 'v UUID')
        uuids = [uuid_mod.uuid4() for _ in range(5)]
        canonical = [u.bytes for u in uuids]
        values = pa.array(canonical, type=pa.binary(16))
        df = self._make_df_with_ts('v', values, 5)
        with qi.QuestDB.from_conf(self._conf()) as client:
            client.dataframe(df, table_name=table, at='ts',
                             schema_overrides={'v': 'uuid'})
        self.qdb_plain.retry_check_table(table, min_rows=5)
        with qi.QuestDB.from_conf(self._conf()) as client:
            got = client.query(
                f'SELECT v FROM {table} ORDER BY ts').to_arrow()
        self.assertEqual(self._extract_uuid_storage(got.column('v')),
                         canonical)

    def test_uuid_round_trip_via_arrow_uuid_extension(self):
        """If pyarrow has registered the `arrow.uuid` extension type,
        the label itself claims the column as UUID — no
        ``schema_overrides`` needed. Per the Arrow spec the storage
        bytes are RFC 4122 big-endian, which is what the client
        byte-swaps into wire order."""
        import pyarrow as pa
        import uuid as uuid_mod
        self._require_qwp_ws()
        try:
            uuid_type = pa.uuid()
        except (AttributeError, TypeError):
            self.skipTest(
                'pyarrow.uuid() not available in this pyarrow build')
        table = self._table()
        self._create_table(table, 'v UUID')
        uuids = [uuid_mod.uuid4() for _ in range(3)]
        canonical = [u.bytes for u in uuids]
        values = pa.ExtensionArray.from_storage(
            uuid_type,
            pa.array(canonical, type=pa.binary(16)))
        df = self._make_df_with_ts('v', values, 3)
        with qi.QuestDB.from_conf(self._conf()) as client:
            client.dataframe(df, table_name=table, at='ts')
        self.qdb_plain.retry_check_table(table, min_rows=3)
        with qi.QuestDB.from_conf(self._conf()) as client:
            got = client.query(
                f'SELECT v FROM {table} ORDER BY ts').to_arrow()
        self.assertEqual(self._extract_uuid_storage(got.column('v')),
                         canonical)

    def test_uuid_with_nulls_round_trip(self):
        """UUID validity bitmap round-trips: nulls stay null."""
        import pyarrow as pa
        import uuid as uuid_mod
        self._require_qwp_ws()
        table = self._table()
        self._create_table(table, 'v UUID')
        w0 = uuid_mod.uuid4().bytes
        w2 = uuid_mod.uuid4().bytes
        w4 = uuid_mod.uuid4().bytes
        values = pa.array(
            [w0, None, w2, None, w4], type=pa.binary(16))
        df = self._make_df_with_ts('v', values, 5)
        with qi.QuestDB.from_conf(self._conf()) as client:
            client.dataframe(df, table_name=table, at='ts',
                             schema_overrides={'v': 'uuid'})
        self.qdb_plain.retry_check_table(table, min_rows=5)
        with qi.QuestDB.from_conf(self._conf()) as client:
            got = client.query(
                f'SELECT v FROM {table} ORDER BY ts').to_arrow()
        col = got.column('v')
        self.assertEqual(self._extract_uuid_storage(col),
                         [w0, None, w2, None, w4])
        self.assertEqual(col.null_count, 2)

    def test_long256_round_trip(self):
        """``pa.fixed_size_binary(32)`` claimed as LONG256 via
        ``schema_overrides`` → LONG256 wire → server stores as
        LONG256 → egress emits FSB(32). Bytes are forwarded
        verbatim; the 32-byte width alone claims nothing, so without
        the override the column would be opaque BINARY."""
        import pyarrow as pa
        self._require_qwp_ws()
        table = self._table()
        self._create_table(table, 'v LONG256')
        # Use distinct 32-byte patterns. The QuestDB wire format
        # for LONG256 is 4 LE 64-bit limbs, least-significant first.
        v0 = bytes(range(32))
        v1 = bytes([i ^ 0xFF for i in range(32)])
        v2 = bytes([0] * 32)
        values = pa.array([v0, v1, v2], type=pa.binary(32))
        df = self._make_df_with_ts('v', values, 3)
        with qi.QuestDB.from_conf(self._conf()) as client:
            client.dataframe(df, table_name=table, at='ts',
                             schema_overrides={'v': 'long256'})
        self.qdb_plain.retry_check_table(table, min_rows=3)
        with qi.QuestDB.from_conf(self._conf()) as client:
            got = client.query(
                f'SELECT v FROM {table} ORDER BY ts').to_arrow()
        col = got.column('v')
        if isinstance(col.type, pa.BaseExtensionType):
            got_bytes = col.combine_chunks().storage.to_pylist()
        else:
            got_bytes = col.to_pylist()
        # v2 (all zeros) is the LONG256 null sentinel — server reads
        # it back as NULL. Document this with the assertion.
        self.assertEqual(got_bytes[0], v0)
        self.assertEqual(got_bytes[1], v1)
        # Index 2 may be None (null sentinel) — pin that contract.
        self.assertIn(got_bytes[2], (v2, None))

    def test_long256_with_nulls_round_trip(self):
        import pyarrow as pa
        self._require_qwp_ws()
        table = self._table()
        self._create_table(table, 'v LONG256')
        v0 = bytes(range(32))
        v2 = bytes(range(32, 64))
        v4 = bytes([0xAB] * 32)
        values = pa.array(
            [v0, None, v2, None, v4], type=pa.binary(32))
        df = self._make_df_with_ts('v', values, 5)
        with qi.QuestDB.from_conf(self._conf()) as client:
            client.dataframe(df, table_name=table, at='ts',
                             schema_overrides={'v': 'long256'})
        self.qdb_plain.retry_check_table(table, min_rows=5)
        with qi.QuestDB.from_conf(self._conf()) as client:
            got = client.query(
                f'SELECT v FROM {table} ORDER BY ts').to_arrow()
        col = got.column('v')
        if isinstance(col.type, pa.BaseExtensionType):
            got_bytes = col.combine_chunks().storage.to_pylist()
        else:
            got_bytes = col.to_pylist()
        self.assertEqual(got_bytes, [v0, None, v2, None, v4])


    def test_unclaimed_fsb16_lands_as_binary(self):
        """A bare ``pa.fixed_size_binary(16)`` column carries no UUID
        claim, so it is opaque bytes: writing it into a UUID column is
        a type mismatch the server rejects."""
        import pyarrow as pa
        import uuid as uuid_mod
        table = self._table()
        self._create_table(table, 'v UUID')
        values = pa.array(
            [uuid_mod.uuid4().bytes for _ in range(3)],
            type=pa.binary(16))
        df = self._make_df_with_ts('v', values, 3)
        with qi.QuestDB.from_conf(self._conf()) as client:
            with self.assertRaises(qi.QuestDBError):
                client.dataframe(df, table_name=table, at='ts')

    def test_fsb_other_size_lands_as_binary(self):
        """``FixedSizeBinary(k)`` carries no QuestDB type claim at any
        width, so it is opaque bytes and auto-creates a BINARY column
        holding the rows verbatim. Auto-create (no ``_create_table``)
        pins the wire type; a pre-created BINARY column would assert
        only that the server accepted the rows. Order by ``timestamp``:
        auto-create renames ``ts``, and BINARY is not orderable."""
        import pyarrow as pa
        table = self._table()
        rows = [b'\x00' * 8, b'\xff' * 8]
        values = pa.array(rows, type=pa.binary(8))
        df = self._make_df_with_ts('v', values, 2)
        with qi.QuestDB.from_conf(self._conf()) as client:
            client.dataframe(df, table_name=table, at='ts')
        self.qdb_plain.retry_check_table(table, min_rows=2)
        with qi.QuestDB.from_conf(self._conf()) as client:
            got = client.query(
                f'SELECT v FROM {table} ORDER BY timestamp').to_arrow()
        self.assertEqual(got.column('v').type, pa.binary())
        self.assertEqual(got.column('v').to_pylist(), rows)


class TestColumnIngressFailover(unittest.TestCase):
    """Within-call failover for ``QuestDB.dataframe`` (the column path).

    Connect-time / between-operation failover is automatic in Rust (the
    next pool borrow auto-selects the live primary); these tests pin the
    two cases the Python wrapper is responsible for: a dead+live endpoint
    list (the borrow must skip the dead endpoint and land on the live
    primary) and a mid-stream server bounce (the transient
    ``FailoverRetry`` re-sends the whole df). Both routes — pandas/numpy
    and the Arrow capsule (polars / pyarrow) — are covered.
    """

    @classmethod
    def setUpClass(cls):
        TestWithDatabase.setUpClass.__func__(cls)

    @classmethod
    def tearDownClass(cls):
        TestWithDatabase.tearDownClass.__func__(cls)

    def _require_qwp_ws(self):
        if self.qdb_plain.version < FIRST_QWP_WS_RELEASE:
            self.skipTest(
                'QWP/WebSocket integration tests require QuestDB 9.4.3+')

    def setUp(self):
        self._require_qwp_ws()

    @staticmethod
    def _unused_tcp_port():
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(('127.0.0.1', 0))
            return sock.getsockname()[1]

    def _conf(self, endpoints=None, **extra):
        if endpoints is None:
            endpoints = [
                (self.qdb_plain.host, self.qdb_plain.http_server_port)]
        addr = ','.join(f'{h}:{p}' for h, p in endpoints)
        conf = f'ws::addr={addr};'
        for k, v in extra.items():
            conf += f'{k}={v};'
        return conf

    def _sfa_conf(self, sender_id, sf_dir, endpoints=None, **extra):
        sfa_extra = {
            'sender_id': sender_id,
            'sf_dir': sf_dir,
            'sender_pool_min': '1',
            'sender_pool_max': '1',
            'pool_reap': 'manual',
            'reconnect_max_duration_millis': '30000',
            'close_flush_timeout_millis': '30000',
        }
        sfa_extra.update(extra)
        return self._conf(endpoints=endpoints, **sfa_extra)

    @staticmethod
    def _sfa_file_count(sf_dir, sender_id):
        slot_dir = pathlib.Path(sf_dir) / sender_id
        if not slot_dir.exists():
            return 0
        return sum(1 for path in slot_dir.iterdir()
                   if path.name.endswith('.sfa'))

    def _table(self, prefix='t_fo_'):
        name = prefix + uuid.uuid4().hex[:8]
        self.addCleanup(lambda: self._drop_quietly(name))
        return name

    def _drop_quietly(self, table):
        try:
            self.qdb_plain.http_sql_query(f'DROP TABLE IF EXISTS {table}')
        except Exception:
            pass

    def _create_table(self, table):
        self.qdb_plain.http_sql_query(
            f'CREATE TABLE {table} (ts TIMESTAMP, v LONG) '
            'TIMESTAMP(ts) PARTITION BY DAY WAL '
            'DEDUP UPSERT KEYS(ts, v)')

    def _pandas_df(self, n):
        ts = [1700000000_000000 + i * 1_000_000 for i in range(n)]
        return pd.DataFrame({
            'ts': pd.to_datetime(ts, unit='us'),
            'v': np.arange(n, dtype=np.int64),
        })

    def _arrow_df(self, n):
        import pyarrow as pa
        ts = pa.array(
            [1700000000_000000 + i * 1_000_000 for i in range(n)],
            type=pa.timestamp('us', tz='UTC'))
        return pd.DataFrame({
            'ts': pd.array(ts, dtype=pd.ArrowDtype(ts.type)),
            'v': pd.array(
                pa.array(list(range(n)), type=pa.int64()),
                dtype=pd.ArrowDtype(pa.int64())),
        })

    def _read_back_v(self, table):
        with qi.QuestDB.from_conf(self._conf()) as client:
            got = client.query(
                f'SELECT v FROM {table} ORDER BY ts').to_arrow()
        return got.column('v').to_pylist()

    def test_sf_conf_dataframe_stays_direct_numpy(self):
        """``QuestDB.dataframe`` ignores ``sf_dir``: the NumPy path stays on
        the direct column sender and never touches the store-and-forward
        spool."""
        table = self._table('t_sf_conf_df_np_')
        sender_id = 'py-df-np-' + uuid.uuid4().hex[:8]
        self.qdb_plain.http_sql_query(
            f'CREATE TABLE {table} '
            '(ts TIMESTAMP, v LONG, sym SYMBOL) '
            'TIMESTAMP(ts) PARTITION BY DAY WAL '
            'DEDUP UPSERT KEYS(ts, v)')
        df = pd.DataFrame({
            'ts': pd.to_datetime([
                1_700_000_000_000_000,
                1_700_000_000_001_000,
                1_700_000_000_002_000,
            ], unit='us'),
            'v': np.array([0, 1, 2], dtype=np.int64),
            'sym': pd.Categorical(['alpha', 'bravo', 'alpha']),
        })

        with tempfile.TemporaryDirectory(prefix='py-df-sf-conf-np-') as sf_dir:
            with qi.QuestDB.from_conf(
                    self._sfa_conf(sender_id, sf_dir)) as client:
                client.dataframe(
                    df, table_name=table, at='ts', symbols=['sym'])
            self.assertFalse(
                (pathlib.Path(sf_dir) / sender_id).exists(),
                'dataframe ingestion must not open the store-and-forward '
                'slot')

        self.qdb_plain.retry_check_table(table, min_rows=3)
        resp = self.qdb_plain.http_sql_query(
            f'SELECT v, sym FROM {table} ORDER BY v')
        self.assertEqual(
            resp['dataset'],
            [[0, 'alpha'], [1, 'bravo'], [2, 'alpha']])

    def test_sf_conf_dataframe_stays_direct_arrow(self):
        """``QuestDB.dataframe`` ignores ``sf_dir``: the Arrow capsule path
        stays on the direct column sender and never touches the
        store-and-forward spool."""
        if pyarrow is None:
            self.skipTest('pyarrow not installed')

        table = self._table('t_sf_conf_df_arrow_')
        sender_id = 'py-df-arrow-' + uuid.uuid4().hex[:8]
        self.qdb_plain.http_sql_query(
            f'CREATE TABLE {table} '
            '(ts TIMESTAMP, v LONG, sym SYMBOL) '
            'TIMESTAMP(ts) PARTITION BY DAY WAL '
            'DEDUP UPSERT KEYS(ts, v)')

        ts_type = pyarrow.timestamp('us', tz='UTC')
        df = pd.DataFrame({
            'ts': pd.Series(
                pyarrow.array([
                    1_700_000_000_000_000,
                    1_700_000_000_001_000,
                    1_700_000_000_002_000,
                ], type=ts_type),
                dtype=pd.ArrowDtype(ts_type)),
            'v': pd.Series(
                pyarrow.array([10, 11, 12], type=pyarrow.int64()),
                dtype=pd.ArrowDtype(pyarrow.int64())),
            'sym': pd.Series(
                pyarrow.array(['xray', 'yankee', 'xray'],
                              type=pyarrow.string()),
                dtype=pd.ArrowDtype(pyarrow.string())),
        })

        with tempfile.TemporaryDirectory(
                prefix='py-df-sf-conf-arrow-') as sf_dir:
            with qi.QuestDB.from_conf(
                    self._sfa_conf(sender_id, sf_dir)) as client:
                client.dataframe(
                    df,
                    table_name=table,
                    at='ts',
                    schema_overrides={'sym': 'symbol'})
            self.assertFalse(
                (pathlib.Path(sf_dir) / sender_id).exists(),
                'dataframe ingestion must not open the store-and-forward '
                'slot')

        self.qdb_plain.retry_check_table(table, min_rows=3)
        resp = self.qdb_plain.http_sql_query(
            f'SELECT v, sym FROM {table} ORDER BY v')
        self.assertEqual(
            resp['dataset'],
            [[10, 'xray'], [11, 'yankee'], [12, 'xray']])

    def test_dataframe_rejection_is_terminal_and_next_call_recovers(self):
        """A server rejection surfaces as a terminal error (not retried by
        the failover loop) and the rejected connection is dropped, so the
        next ``dataframe`` call succeeds on a fresh borrow."""
        table = self._table('t_df_reject_')
        self.qdb_plain.http_sql_query(
            f'CREATE TABLE {table} '
            '(ts TIMESTAMP, v LONG, bad LONG) '
            'TIMESTAMP(ts) PARTITION BY DAY WAL')

        valid1 = pd.DataFrame({
            'ts': pd.to_datetime([1_700_000_000_000_000], unit='us'),
            'v': np.array([0], dtype=np.int64),
        })
        rejected = pd.DataFrame({
            'ts': pd.to_datetime([1_700_000_000_001_000], unit='us'),
            'bad': pd.Series(['not-a-long'], dtype=object),
        })
        valid2 = pd.DataFrame({
            'ts': pd.to_datetime([1_700_000_000_002_000], unit='us'),
            'v': np.array([2], dtype=np.int64),
        })

        with qi.QuestDB.from_conf(self._conf()) as client:
            client.dataframe(valid1, table_name=table, at='ts')
            with self.assertRaises(qi.QuestDBError) as raised:
                client.dataframe(rejected, table_name=table, at='ts')
            self.assertEqual(
                raised.exception.code, qi.QuestDBErrorCode.InvalidApiCall)
            client.dataframe(valid2, table_name=table, at='ts')

        self.qdb_plain.retry_check_table(table, min_rows=2)
        resp = self.qdb_plain.http_sql_query(
            f'SELECT v FROM {table} ORDER BY v')
        self.assertEqual(resp['dataset'], [[0], [2]])

    def test_dead_then_live_endpoint_numpy_route(self):
        """A dead first endpoint + the live primary: the pool borrow
        rotates past the dead endpoint, the whole df lands. NumPy
        (pandas) route."""
        table = self._table()
        self._create_table(table)
        endpoints = [
            (self.qdb_plain.host, self._unused_tcp_port()),
            (self.qdb_plain.host, self.qdb_plain.http_server_port)]
        conf = self._conf(
            endpoints=endpoints,
            reconnect_max_duration_millis='30000')
        df = self._pandas_df(2000)
        with qi.QuestDB.from_conf(conf) as client:
            client.dataframe(df, table_name=table, at='ts')
        self.qdb_plain.retry_check_table(table, min_rows=2000)
        self.assertEqual(self._read_back_v(table), list(range(2000)))

    def test_dead_then_live_endpoint_arrow_route(self):
        """Same, via the Arrow capsule (pyarrow-backed) route."""
        table = self._table()
        self._create_table(table)
        endpoints = [
            (self.qdb_plain.host, self._unused_tcp_port()),
            (self.qdb_plain.host, self.qdb_plain.http_server_port)]
        conf = self._conf(
            endpoints=endpoints,
            reconnect_max_duration_millis='30000')
        df = self._arrow_df(2000)
        with qi.QuestDB.from_conf(conf) as client:
            client.dataframe(df, table_name=table, at='ts')
        self.qdb_plain.retry_check_table(table, min_rows=2000)
        self.assertEqual(self._read_back_v(table), list(range(2000)))

    def test_polars_dataframe_round_trip(self):
        """``pl.DataFrame`` (the Arrow capsule route) lands every row;
        pins that the polars source feeds the same whole-df path."""
        try:
            import polars as pl
        except ImportError:
            self.skipTest('polars not installed')
        table = self._table()
        self._create_table(table)
        df = pl.DataFrame({
            'ts': [
                datetime.datetime(2023, 11, 14, 22, 13, 20,
                                  tzinfo=datetime.timezone.utc)
                + datetime.timedelta(seconds=i)
                for i in range(1500)],
            'v': list(range(1500)),
        })
        with qi.QuestDB.from_conf(self._conf()) as client:
            client.dataframe(df, table_name=table, at='ts')
        self.qdb_plain.retry_check_table(table, min_rows=1500)
        self.assertEqual(self._read_back_v(table), list(range(1500)))

    def test_mid_stream_bounce_resends_whole_df_numpy(self):
        """Bounce the server mid-call: the transient ``FailoverRetry``
        re-sends the whole df on a fresh conn. DEDUP collapses any
        duplicate prefix; the final row set is exact (no loss / no dup).
        NumPy route."""
        table = self._table()
        self._create_table(table)
        df = self._pandas_df(20000)
        with qi.QuestDB.from_conf(
                self._conf(reconnect_max_duration_millis='60000')) as client:
            # Warm the pool so a live conn is idle, then bounce: the next
            # borrow hands back that now-stale conn, the flush hits a dead
            # socket -> FailoverRetry -> whole-df re-send on a reconnected
            # primary. DEDUP collapses any duplicate prefix.
            client.dataframe(self._pandas_df(2), table_name=table, at='ts')
            self.qdb_plain.stop()
            self.qdb_plain.start()
            client.dataframe(df, table_name=table, at='ts')
        self.qdb_plain.retry_check_table(table, min_rows=20000)
        self.assertEqual(self._read_back_v(table), list(range(20000)))

    def test_mid_stream_bounce_resends_whole_df_arrow(self):
        """Same bounce, Arrow capsule route."""
        table = self._table()
        self._create_table(table)
        df = self._arrow_df(20000)
        with qi.QuestDB.from_conf(
                self._conf(reconnect_max_duration_millis='60000')) as client:
            client.dataframe(self._arrow_df(2), table_name=table, at='ts')
            self.qdb_plain.stop()
            self.qdb_plain.start()
            client.dataframe(df, table_name=table, at='ts')
        self.qdb_plain.retry_check_table(table, min_rows=20000)
        self.assertEqual(self._read_back_v(table), list(range(20000)))

    def test_bounce_without_dedup_is_at_least_once(self):
        """Mid-stream bounce on a table WITHOUT dedup keys: the whole-df
        re-send must lose nothing; the committed-but-unobserved prefix
        may duplicate. Pins the at-least-once contract explicitly rather
        than letting DEDUP mask it."""
        warm_table = self._table('t_fo_warm_')
        self._create_table(warm_table)
        table = self._table('t_fo_nodedup_')
        self.qdb_plain.http_sql_query(
            f'CREATE TABLE {table} (ts TIMESTAMP, v LONG) '
            'TIMESTAMP(ts) PARTITION BY DAY WAL')
        df = self._pandas_df(20000)
        with qi.QuestDB.from_conf(
                self._conf(reconnect_max_duration_millis='60000')) as client:
            client.dataframe(
                self._pandas_df(2), table_name=warm_table, at='ts')
            self.qdb_plain.stop()
            self.qdb_plain.start()
            client.dataframe(df, table_name=table, at='ts')
        self.qdb_plain.retry_check_table(table, min_rows=20000)
        rows = self._read_back_v(table)
        self.assertEqual(
            sorted(set(rows)), list(range(20000)), 'no row may be lost')
        self.assertLessEqual(
            len(rows), 2 * 20000, 'duplication must stay bounded')

    def test_failed_dataframe_call_leaves_only_the_eager_first_batch(self):
        """A dataframe call that fails mid-stream must not land its
        pipelined (deferred) batches: the failed connection is dropped,
        never committed. The first batch of a fresh connection is
        immediate-commit on the wire, so exactly that much may land."""
        import time
        if pyarrow is None:
            self.skipTest('pyarrow not installed')
        table = self._table('t_fo_partial_')
        self.qdb_plain.http_sql_query(
            f'CREATE TABLE {table} (ts TIMESTAMP, v LONG) '
            'TIMESTAMP(ts) PARTITION BY DAY WAL')

        ts_type = pyarrow.timestamp('us', tz='UTC')
        schema = pyarrow.schema([('ts', ts_type), ('v', pyarrow.int64())])

        def batches():
            for start in (0, 3):
                yield pyarrow.record_batch(
                    [
                        pyarrow.array(
                            [1_700_000_000_000_000 + i * 1_000_000
                             for i in range(start, start + 3)],
                            type=ts_type),
                        pyarrow.array(
                            list(range(start, start + 3)),
                            type=pyarrow.int64()),
                    ],
                    schema=schema)
            raise ValueError('source stream failed')

        reader = pyarrow.RecordBatchReader.from_batches(schema, batches())
        with qi.QuestDB.from_conf(self._conf()) as client:
            with self.assertRaises(qi.QuestDBError):
                client.dataframe(reader, table_name=table, at='ts')

        # Batch 0 (v=0,1,2) went out immediate-commit on the fresh
        # connection and lands; batch 1 (v=3,4,5) was deferred and must
        # not survive the dropped connection.
        self.qdb_plain.retry_check_table(table, min_rows=3)
        time.sleep(1.0)
        self.assertEqual(self._read_back_v(table), [0, 1, 2])


class TestEgressFailover(unittest.TestCase):
    """Egress read failover: materialise-whole = transparent (the reset
    callback discards the partial accumulation and replays from
    batch-0); streaming = explicit ``FailoverWouldDuplicate``."""

    @classmethod
    def setUpClass(cls):
        TestWithDatabase.setUpClass.__func__(cls)

    @classmethod
    def tearDownClass(cls):
        TestWithDatabase.tearDownClass.__func__(cls)

    def _require_qwp_ws(self):
        if self.qdb_plain.version < FIRST_QWP_WS_RELEASE:
            self.skipTest(
                'QWP/WebSocket integration tests require QuestDB 9.4.3+')

    def setUp(self):
        self._require_qwp_ws()

    @staticmethod
    def _unused_tcp_port():
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(('127.0.0.1', 0))
            return sock.getsockname()[1]

    def _conf(self, endpoints=None, **extra):
        if endpoints is None:
            endpoints = [
                (self.qdb_plain.host, self.qdb_plain.http_server_port)]
        addr = ','.join(f'{host}:{port}' for host, port in endpoints)
        conf = f'ws::addr={addr};'
        for k, v in extra.items():
            conf += f'{k}={v};'
        return conf

    def _exec(self, sql):
        return self.qdb_plain.http_sql_query(sql)

    def _drop_quietly(self, table):
        try:
            self._exec(f'DROP TABLE IF EXISTS {table}')
        except Exception:
            pass

    def _seed(self, n_rows):
        """A multi-batch result: enough rows that QuestDB streams more
        than one record batch, so a mid-stream bounce lands after the
        first batch is delivered."""
        table = 't_egress_fo_' + uuid.uuid4().hex[:8]
        self.addCleanup(lambda: self._drop_quietly(table))
        self._exec(
            f'CREATE TABLE {table} (ts TIMESTAMP, v LONG) '
            'TIMESTAMP(ts) PARTITION BY DAY WAL')
        base = '2024-01-01T00:00:00.000000Z'
        # Bulk-insert via a generator series keeps the SQL compact.
        # long_sequence(n) yields x = 1..n; v = x - 1 gives 0..n-1.
        self._exec(
            f"INSERT INTO {table} "
            f"SELECT timestamp_sequence('{base}', 1000) AS ts, x - 1 AS v "
            f"FROM long_sequence({n_rows})")
        self.qdb_plain.retry_check_table(table, min_rows=n_rows)
        return table

    def test_materialise_whole_transparent_across_bounce(self):
        """``to_arrow`` / ``to_pandas`` / ``to_polars`` complete with the
        full, in-order result even when the server bounces mid-stream:
        the installed reset callback discards the partial accumulation
        and the query replays from batch-0."""
        n = 200000
        table = self._seed(n)
        expected = list(range(n))

        with qi.QuestDB.from_conf(
                self._conf(failover_max_duration_ms='60000')) as client:
            result = client.query(f'SELECT v FROM {table} ORDER BY ts')
            # Bounce before the (single-use) materialisation drains the
            # stream: a mid-query failover re-executes and the reset
            # discards anything already buffered.
            self.qdb_plain.stop()
            self.qdb_plain.start()
            table_out = result.to_arrow()
        self.assertEqual(table_out.column('v').to_pylist(), expected)

    def test_to_pandas_numpy_transparent_across_bounce(self):
        """Default ``to_pandas`` (the numpy accumulator we own) is
        likewise transparent across a bounce."""
        n = 200000
        table = self._seed(n)
        expected = list(range(n))
        with qi.QuestDB.from_conf(
                self._conf(failover_max_duration_ms='60000')) as client:
            result = client.query(f'SELECT v FROM {table} ORDER BY ts')
            self.qdb_plain.stop()
            self.qdb_plain.start()
            df = result.to_pandas()
        self.assertEqual(df['v'].tolist(), expected)

    def test_to_polars_transparent_across_bounce(self):
        try:
            import polars  # noqa: F401
        except ImportError:
            self.skipTest('polars not installed')
        n = 200000
        table = self._seed(n)
        expected = list(range(n))
        with qi.QuestDB.from_conf(
                self._conf(failover_max_duration_ms='60000')) as client:
            result = client.query(f'SELECT v FROM {table} ORDER BY ts')
            self.qdb_plain.stop()
            self.qdb_plain.start()
            df = result.to_polars()
        self.assertEqual(df['v'].to_list(), expected)

    def test_to_polars_dead_then_live_endpoint(self):
        """Polars materialisation uses the same reader failover walk as
        the other egress adapters: a dead first endpoint is skipped and
        the live standalone server satisfies ``target=primary``."""
        try:
            import polars  # noqa: F401
        except ImportError:
            self.skipTest('polars not installed')
        n = 2048
        table = self._seed(n)
        endpoints = [
            (self.qdb_plain.host, self._unused_tcp_port()),
            (self.qdb_plain.host, self.qdb_plain.http_server_port)]
        with qi.QuestDB.from_conf(
                self._conf(endpoints=endpoints,
                           target='primary',
                           failover_max_duration_ms='60000')) as client:
            df = client.query(f'SELECT v FROM {table} ORDER BY ts').to_polars()
        self.assertEqual(df['v'].to_list(), list(range(n)))

    def test_polars_from_arrow_dead_then_live_endpoint(self):
        """The pyarrow-free Polars capsule path also borrows through the
        same multi-endpoint reader pool before Polars starts consuming
        the Arrow stream."""
        try:
            import polars as pl
        except ImportError:
            self.skipTest('polars not installed')
        n = 2048
        table = self._seed(n)
        endpoints = [
            (self.qdb_plain.host, self._unused_tcp_port()),
            (self.qdb_plain.host, self.qdb_plain.http_server_port)]
        with qi.QuestDB.from_conf(
                self._conf(endpoints=endpoints,
                           target='primary',
                           failover_max_duration_ms='60000')) as client:
            with client.query(f'SELECT v FROM {table} ORDER BY ts') as result:
                df = pl.from_arrow(result)
        self.assertEqual(df['v'].to_list(), list(range(n)))

    def test_iter_arrow_surfaces_failover_would_duplicate(self):
        """Streaming ``iter_arrow`` installs no reset: a mid-stream
        failover after the first batch is delivered surfaces a clean,
        catchable ``FailoverWouldDuplicate`` rather than silently
        re-reading."""
        # Use a generated result large enough that the server is still
        # producing after the first small batch. A pre-seeded table can
        # finish and buffer before the graceful fixture bounce breaks the
        # WebSocket, making the test depend on timing.
        n = 100000000
        with qi.QuestDB.from_conf(
                self._conf(failover_max_duration_ms='60000',
                           max_batch_rows='1024')) as client:
            it = client.query(
                f'SELECT x - 1 AS v FROM long_sequence({n})').iter_arrow()
            first = next(it)
            self.assertGreater(first.num_rows, 0)
            # First batch delivered; bounce so the next pull fails over.
            self.qdb_plain.stop()
            self.qdb_plain.start()
            with self.assertRaises(qi.QuestDBError) as cm:
                for _ in it:
                    pass
            self.assertEqual(
                cm.exception.code,
                qi.QuestDBErrorCode.FailoverWouldDuplicate)

    def test_iter_pandas_surfaces_failover_would_duplicate(self):
        """Same contract for the numpy streaming ``iter_pandas``."""
        n = 100000000
        with qi.QuestDB.from_conf(
                self._conf(failover_max_duration_ms='60000',
                           max_batch_rows='1024')) as client:
            it = client.query(
                f'SELECT x - 1 AS v FROM long_sequence({n})').iter_pandas()
            first = next(it)
            self.assertGreater(len(first), 0)
            self.qdb_plain.stop()
            self.qdb_plain.start()
            with self.assertRaises(qi.QuestDBError) as cm:
                for _ in it:
                    pass
            self.assertEqual(
                cm.exception.code,
                qi.QuestDBErrorCode.FailoverWouldDuplicate)


class _FakeStatusServer:
    """Port of ``QwpQueryClientMultiHostFailoverTest.FakeStatusServer``: a
    raw loopback socket that answers every probe with a fixed HTTP status
    (and optional ``X-QuestDB-Role`` header) and counts how many times it
    was connected to. A real QuestDB always advertises a single role, so
    role-negotiation failover can only be exercised against an in-process
    fake that can pretend to be a REPLICA / return 401."""

    def __init__(self, status_code, role_header=None):
        self.status_code = status_code
        self.role_header = role_header
        self.connections = 0
        self._lock = threading.Lock()
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind(('127.0.0.1', 0))
        self._sock.listen(50)
        self._running = True

    @property
    def port(self):
        return self._sock.getsockname()[1]

    def start(self):
        threading.Thread(target=self._loop, daemon=True).start()

    def _loop(self):
        while self._running:
            try:
                conn, _ = self._sock.accept()
            except OSError:
                return
            threading.Thread(
                target=self._handle, args=(conn,), daemon=True).start()

    def _handle(self, conn):
        with conn:
            # Increment before responding: the client cannot observe the
            # HTTP status (and thus rotate / surface its error) until the
            # response has been written, so a count read after the connect
            # walk returns is guaranteed to have seen this probe.
            with self._lock:
                self.connections += 1
            try:
                conn.recv(8192)
                reason = {401: 'Unauthorized',
                          421: 'Misdirected Request'}.get(
                    self.status_code, 'Status')
                lines = [f'HTTP/1.1 {self.status_code} {reason}']
                if self.role_header:
                    lines.append(self.role_header)
                lines.append('Content-Length: 0')
                lines.append('Connection: close')
                conn.sendall(
                    ('\r\n'.join(lines) + '\r\n\r\n').encode('ascii'))
            except OSError:
                pass

    def close(self):
        self._running = False
        try:
            self._sock.close()
        except OSError:
            pass


class TestEgressFailoverRoleNegotiation(unittest.TestCase):
    """Pooled reader connect-time role/auth failover, ported from Java's
    ``QwpQueryClientMultiHostFailoverTest``. These tests exercise eager reader
    prewarming through ``QuestDB.from_conf``. The in-process fakes implement
    only the reader upgrade, so sender prewarming is disabled explicitly."""

    def _server(self, status_code, role_header=None):
        srv = _FakeStatusServer(status_code, role_header)
        self.addCleanup(srv.close)
        srv.start()
        return srv

    @staticmethod
    def _conf(servers, **extra):
        addr = ','.join(f'127.0.0.1:{s.port}' for s in servers)
        conf = (f'ws::addr={addr};'
                'sender_pool_min=0;query_pool_min=1;')
        for key, value in extra.items():
            conf += f'{key}={value};'
        return conf

    def test_replica_then_401_fails_fast_with_auth(self):
        """``[REPLICA(421), auth(401)]``: the 421 rotates past the replica,
        the 401 on the second endpoint short-circuits the walk with an auth
        error (not a generic socket/role error). Both endpoints are probed."""
        replica = self._server(421, 'X-QuestDB-Role: REPLICA')
        auth = self._server(401)
        conf = self._conf(
            [replica, auth],
            auth_timeout_ms=2000, failover='off', target='any')
        with self.assertRaises(qi.QuestDBError) as cm:
            qi.QuestDB.from_conf(conf)
        self.assertEqual(cm.exception.code, qi.QuestDBErrorCode.AuthError)
        self.assertIn('401', str(cm.exception))
        self.assertGreaterEqual(replica.connections, 1)
        self.assertGreaterEqual(auth.connections, 1)

    def test_all_replica_fails_with_role_mismatch(self):
        """Every endpoint role-rejects: the surfaced error is a distinct
        ``RoleMismatch`` (naming the unsuitable role), *not* ``AuthError``
        and *not* the generic ``SocketError`` used for "all unreachable".
        This is the typed distinction Java draws with
        ``QwpRoleMismatchException`` -- an operator can tell "no primary
        elected yet" from "bad credentials" and from "everything is down".
        Both replicas are probed."""
        r1 = self._server(421, 'X-QuestDB-Role: REPLICA')
        r2 = self._server(421, 'X-QuestDB-Role: REPLICA')
        conf = self._conf(
            [r1, r2],
            auth_timeout_ms=2000, failover='off', target='any')
        with self.assertRaises(qi.QuestDBError) as cm:
            qi.QuestDB.from_conf(conf)
        self.assertEqual(cm.exception.code, qi.QuestDBErrorCode.RoleMismatch)
        self.assertIn('REPLICA', str(cm.exception))
        self.assertGreaterEqual(r1.connections, 1)
        self.assertGreaterEqual(r2.connections, 1)

    def test_connect_does_not_double_walk_on_first_failure(self):
        """With ``failover=off`` eager reader prewarming walks the address list
        exactly once: each role-rejecting endpoint is probed a single time
        before the walk fails terminally -- no re-walking the list."""
        r1 = self._server(421, 'X-QuestDB-Role: REPLICA')
        r2 = self._server(421, 'X-QuestDB-Role: REPLICA')
        r3 = self._server(421, 'X-QuestDB-Role: REPLICA')
        conf = self._conf(
            [r1, r2, r3],
            auth_timeout_ms=2000, failover='off', target='any')
        with self.assertRaises(qi.QuestDBError) as cm:
            qi.QuestDB.from_conf(conf)
        self.assertEqual(cm.exception.code, qi.QuestDBErrorCode.RoleMismatch)
        self.assertEqual(r1.connections, 1)
        self.assertEqual(r2.connections, 1)
        self.assertEqual(r3.connections, 1)


if __name__ == '__main__':
    unittest.main()
