#!/usr/bin/env python3
import sys

sys.dont_write_bytecode = True
import os
import unittest
from unittest import mock
import datetime
import ipaddress
import array
import timeit
import time
import threading
import uuid
from enum import Enum
import random
import pathlib
import tempfile
import warnings
import numpy as np

import patch_path

from test_tools import (
    _float_binary_bytes,
    _array_binary_bytes,
    TimestampEncodingMixin)

PROJ_ROOT = patch_path.PROJ_ROOT
sys.path.append(str(PROJ_ROOT / 'c-questdb-client' / 'system_test'))

from mock_server import (Server, HttpServer, SETTINGS_WITHOUT_PROTOCOL_VERSION,
                         SETTINGS_WITH_PROTOCOL_VERSION_V1, SETTINGS_WITH_PROTOCOL_VERSION_V2,
                         SETTINGS_WITH_PROTOCOL_VERSION_V1_V2_V3,SETTINGS_WITH_PROTOCOL_VERSION_V4)
from qwp_ws_ack_server import QwpAckServer, TLS_CA

import questdb._client as qi

if os.environ.get('TEST_QUESTDB_INTEGRATION') == '1':
    from system_test import (
        TestWithDatabase,
        TestEgressWithDatabase,
        TestEgressPool,
        TestEgressLeaks,
        TestColumnIngressNarrowTypes,
        TestColumnIngressFailover,
        TestEgressFailover,
        TestEgressFailoverRoleNegotiation)

from fixture import _parse_version

NUMPY_VERSION = _parse_version(np.__version__)

try:
    import pandas as pd
    import numpy
except ImportError:
    pd = None

try:
    import pyarrow
except ImportError:
    pyarrow = None


def _read_qwp_varint(payload, pos):
    value = 0
    shift = 0
    while True:
        if pos >= len(payload):
            raise AssertionError('truncated QWP varint')
        byte = payload[pos]
        pos += 1
        value |= (byte & 0x7f) << shift
        if byte & 0x80 == 0:
            return value, pos
        shift += 7
        if shift >= 64:
            raise AssertionError('oversize QWP varint')


def _first_qwp_table_row_count(payload):
    """Decode the first table's row count from a captured QWP1 frame."""
    if len(payload) < 12 or payload[:4] != b'QWP1':
        raise AssertionError('not a QWP1 frame')
    if int.from_bytes(payload[6:8], 'little') < 1:
        raise AssertionError('QWP1 frame contains no table')

    # Pooled row frames carry a delta-symbol-dictionary prefix before the
    # table blocks: start id, entry count, then length-prefixed entries.
    pos = 12
    _delta_start, pos = _read_qwp_varint(payload, pos)
    delta_count, pos = _read_qwp_varint(payload, pos)
    for _ in range(delta_count):
        entry_len, pos = _read_qwp_varint(payload, pos)
        pos += entry_len
        if pos > len(payload):
            raise AssertionError('truncated QWP symbol dictionary')

    table_name_len, pos = _read_qwp_varint(payload, pos)
    pos += table_name_len
    if pos > len(payload):
        raise AssertionError('truncated QWP table name')
    row_count, _ = _read_qwp_varint(payload, pos)
    return row_count

from test_client_capsule_path import (
    TestCapsulePathPyArrow,
    TestCapsulePathPolars,
    TestServerTimestampAt,
    TestScalarAt,
    TestNdarrayArrayColumns,
    TestServerInfoLive,
    TestConnectionEvents,
    TestSenderConnectionEvents,
    TestSchemaOverrides,
    TestPyArrowRecordBatchDirect,
    TestSchemaOverridesPandas,
    TestBenchFlushArrowBatch,
    TestCapsulePathPolarsMissing,
    TestWriterMixingInOneChunk,
    TestPandasPlannerRouting,
)
from test_client_dataframe_failures import (
    TestClientDataframeArgValidation,
    TestClientDataframeDirectFailures,
    TestClientDataframeFaultFuzz,
)
from test_client_dataframe_fuzz import (
    TestClientDataframeFuzz,
    TestClientDataframeRoundTrip,
)
from test_client_polars_fuzz import (
    TestClientPolarsDataframeFuzz,
    TestClientPolarsDataframeRoundTrip,
)
from test_dataframe_leaks import TestCategoricalArrowLeak, TestPyobjColumnarLeak

if pd is not None and pyarrow is not None:
    from test_dataframe import TestPandasProtocolVersionV1
    from test_dataframe import TestPandasProtocolVersionV2
    from test_dataframe import TestPandasProtocolVersionV3
    from test_dataframe import TestNaTScalarDatetime
    from test_dataframe import TestColumnarPlanWithoutPyarrow
elif pd is None:
    class TestNoPandas(unittest.TestCase):
        def test_no_pandas(self):
            buf = qi.Buffer(protocol_version=2)
            exp = 'Missing.*`pandas`.*`numpy`.*readthedocs.*installation.html.'
            with self.assertRaisesRegex(qi.QuestDBError, exp):
                buf.dataframe(None, at=qi.ServerTimestamp)


class TestManifest(unittest.TestCase):
    def test_valid_yaml(self):
        try:
            import yaml
        except ImportError:
            self.skipTest('Python version does not support yaml')
        repo_root = pathlib.Path(__file__).parent.parent
        with open(repo_root / 'examples.manifest.yaml', 'r') as f:
            manifest = yaml.safe_load(f)
        for entry in manifest:
            self.assertTrue(
                (repo_root / entry['path']).is_file(),
                f"manifest entry {entry['name']!r} points at a missing "
                f"file: {entry['path']}")


class TestQwpWebSocketApi(unittest.TestCase):
    def test_protocol_enum(self):
        self.assertEqual(qi.Protocol.parse('ws'), qi.Protocol.Ws)
        self.assertEqual(qi.Protocol.parse('wss'), qi.Protocol.Wss)
        self.assertFalse(qi.Protocol.Ws.tls_enabled)
        self.assertTrue(qi.Protocol.Wss.tls_enabled)
        for old_scheme in ('qwpws', 'qwpwss'):
            with self.subTest(old_scheme=old_scheme):
                with self.assertRaises(ValueError):
                    qi.Protocol.parse(old_scheme)

    def test_progress_enum(self):
        self.assertEqual(
            qi.QwpWsProgress.parse('background'),
            qi.QwpWsProgress.Background)
        self.assertEqual(
            qi.QwpWsProgress.parse('manual'),
            qi.QwpWsProgress.Manual)

    def test_connection_event_enum_and_shape(self):
        self.assertEqual(qi.ConnectionEventKind.parse('connected'),
                         qi.ConnectionEventKind.Connected)
        self.assertEqual(qi.ConnectionEventKind.Connected.c_value, 0)
        self.assertEqual(qi.ConnectionEventKind.AuthFailed.c_value, 6)
        event = qi.ConnectionEvent(
            kind=qi.ConnectionEventKind.FailedOver,
            host='b', port='2', previous_host='a', previous_port='1',
            attempt_number=3, cause_code=None, cause_msg=None,
            timestamp_millis=1_700_000_000_000)
        self.assertEqual(event.previous_host, 'a')
        with self.assertRaises(Exception):
            event.host = 'c'  # frozen

    def test_server_role_enum_and_server_info_shape(self):
        self.assertEqual(qi.ServerRole.parse('standalone'),
                         qi.ServerRole.Standalone)
        self.assertEqual(qi.ServerRole.Primary.c_value, 1)
        self.assertEqual(qi.ServerRole.Replica.c_value, 2)
        self.assertEqual(qi.ServerRole.PrimaryCatchup.c_value, 3)
        self.assertEqual(qi.ServerRole.Other.c_value, 0xFF)
        info = qi.ServerInfo(
            role=qi.ServerRole.Primary, role_byte=1, epoch=7,
            capabilities=2, server_wall_ns=1_700_000_000_000_000_000,
            cluster_id='c1', node_id='n1', zone_id=None)
        self.assertIsNone(info.zone_id)
        self.assertEqual(info.role, qi.ServerRole.Primary)
        self.assertEqual(info.epoch, 7)
        with self.assertRaises(Exception):
            info.epoch = 8  # frozen

    def test_connection_types_exported_from_package(self):
        from questdb import (
            ConnectionEvent, ConnectionEventKind, ServerInfo, ServerRole)
        self.assertIs(ConnectionEvent, qi.ConnectionEvent)
        self.assertIs(ConnectionEventKind, qi.ConnectionEventKind)
        self.assertIs(ServerInfo, qi.ServerInfo)
        self.assertIs(ServerRole, qi.ServerRole)

    def test_pooled_lease_types_exported_from_package(self):
        from questdb import PooledReader, PooledSender
        self.assertIs(PooledSender, qi.PooledSender)
        self.assertIs(PooledReader, qi.PooledReader)

    def test_ingress_error_can_carry_qwpws_diagnostic(self):
        err = qi.QuestDBError(
            qi.QuestDBErrorCode.SocketError,
            'sender halted',
            (
                qi.SenderErrorCategory.ParseError.c_value,
                qi.SenderErrorPolicy.Terminal.c_value,
                2,
                'bad line',
                44,
                5,
                6,
            ))

        diagnostic = err.sender_error

        self.assertEqual(diagnostic.category, qi.SenderErrorCategory.ParseError)
        self.assertEqual(diagnostic.applied_policy, qi.SenderErrorPolicy.Terminal)
        self.assertEqual(diagnostic.status, 2)
        self.assertEqual(diagnostic.message, 'bad line')
        self.assertEqual(diagnostic.message_sequence, 44)
        self.assertEqual(diagnostic.from_fsn, 5)
        self.assertEqual(diagnostic.to_fsn, 6)
        self.assertIs(err.sender_error, diagnostic)
        self.assertFalse(err.in_doubt)

    def test_ingress_error_can_report_delivery_unknown(self):
        err = qi.QuestDBError(
            qi.QuestDBErrorCode.FailoverRetry,
            'delivery status unknown',
            in_doubt=True)

        self.assertTrue(err.in_doubt)

    def test_server_rejection_error_is_specific_subclass(self):
        err = qi.QuestDBServerRejectionError(
            qi.QuestDBErrorCode.ServerRejection,
            'sender halted',
            (
                qi.SenderErrorCategory.ParseError.c_value,
                qi.SenderErrorPolicy.Terminal.c_value,
                2,
                'bad line',
                44,
                5,
                6,
            ))

        self.assertIsInstance(err, qi.QuestDBError)
        self.assertEqual(err.code, qi.QuestDBErrorCode.ServerRejection)
        self.assertEqual(err.sender_error.category, qi.SenderErrorCategory.ParseError)

    def test_python_only_error_codes_do_not_overlap_ffi_codes(self):
        # The error model is unified: every reader/query code (Cancelled,
        # FailoverWouldDuplicate, the server-side codes, ...) is now a real
        # FFI code in the contiguous low range. ``BadDataFrame`` is the sole
        # Python-only code, parked in a reserved high band strictly above
        # every FFI code so an appended FFI variant can never collide.
        members = qi.QuestDBErrorCode.__members__
        values = [c.value for c in members.values()]
        # ``__members__`` includes aliases; equal counts prove no member
        # collided onto another's value.
        self.assertEqual(
            len(values), len(set(values)),
            'QuestDBErrorCode has aliased members (value collision)')
        ffi_values = [
            c.value for name, c in members.items() if name != 'BadDataFrame']
        self.assertTrue(
            all(v < qi.QuestDBErrorCode.BadDataFrame.value for v in ffi_values),
            'an FFI code is not below the BadDataFrame sentinel band')
        # Codes that were Python-only sentinels before unification are now
        # contiguous FFI codes (below the sentinel band).
        self.assertLess(
            qi.QuestDBErrorCode.Cancelled.value,
            qi.QuestDBErrorCode.BadDataFrame.value)
        self.assertLess(
            qi.QuestDBErrorCode.FailoverWouldDuplicate.value,
            qi.QuestDBErrorCode.BadDataFrame.value)
        self.assertEqual(
            qi.QuestDBErrorCode.StoreResendRequired.value,
            36)
        self.assertEqual(qi.QuestDBErrorCode.SymbolDictFull.value, 37)
        self.assertIs(
            qi._debug_error_code_to_py(37),
            qi.QuestDBErrorCode.SymbolDictFull)

    def test_default_max_chunk_rows_matches_core_literal(self):
        # Pinned to the Rust core's DEFAULT_MAX_CHUNK_ROWS. Both sides hardcode
        # the literal (the C ABI does not expose it), so this guards the Python
        # side from silently drifting; the core has a matching compile-time pin.
        self.assertEqual(qi.DEFAULT_MAX_CHUNK_ROWS, 16384)

    def test_unsupported_dataframe_shape_error_carries_failures(self):
        err = qi.UnsupportedDataFrameShapeError(
            'unsupported frame',
            [{'column': 'active', 'reason': 'bool_requires_packing'},
             {'column': None, 'reason': 'needs a data column'}])

        self.assertIsInstance(err, qi.QuestDBError)
        self.assertEqual(err.code, qi.QuestDBErrorCode.BadDataFrame)
        self.assertEqual(
            err.column_failures,
            ({'column': 'active', 'reason': 'bool_requires_packing'},
             {'column': None, 'reason': 'needs a data column'}))

        # The per-column reasons must be folded into str(exc), not stranded
        # on the attribute. A non-timestamp failure gets no `at` remedy and
        # must not steer the user off Client to the row path.
        text = str(err)
        self.assertIn('unsupported frame', text)
        self.assertIn("column 'active': bool_requires_packing", text)
        self.assertIn('needs a data column', text)
        self.assertNotIn('Sender', text)
        self.assertNotIn('at=ServerTimestamp', text)

    def test_unsupported_dataframe_shape_error_timestamp_hint(self):
        # A designated-timestamp failure adds the Client-side remedy
        # (valid column, or ServerTimestamp) — and still no Sender pointer.
        err = qi.UnsupportedDataFrameShapeError(
            'unsupported frame',
            [{'column': 'ts', 'target': 'designated timestamp',
              'reason': 'cannot contain timestamps before the Unix epoch.'}])
        text = str(err)
        self.assertIn("column 'ts':", text)
        self.assertIn("at='<column>'", text)
        self.assertIn('at=ServerTimestamp', text)
        self.assertNotIn('Sender', text)

    def test_unsupported_dataframe_shape_error_without_failures_is_verbatim(self):
        # A message-only rejection (e.g. a scalar `at`) must stay verbatim.
        err = qi.UnsupportedDataFrameShapeError('scalar at not supported')
        self.assertEqual(str(err), 'scalar at not supported')
        self.assertEqual(err.column_failures, ())

    def test_client_from_conf_rejects_non_qwp_websocket(self):
        with self.assertRaisesRegex(
                qi.QuestDBError,
                'requires a QWP/WebSocket configuration string'):
            qi.QuestDB.from_conf('tcp::addr=localhost:9009;')

    def test_client_from_conf_requires_addr(self):
        with self.assertRaisesRegex(
                qi.QuestDBError,
                'Missing "addr" parameter'):
            qi.QuestDB.from_conf('ws::sender_pool_min=1;')

    def test_removed_qwp_flight_window_keys_are_rejected(self):
        for scheme in ('ws', 'wss'):
            for key in ('max_in_flight', 'in_flight_window'):
                with self.subTest(scheme=scheme, key=key):
                    with self.assertRaisesRegex(
                            qi.QuestDBError,
                            f'Unknown config key "{key}"') as cm:
                        qi.Sender.from_conf(
                            f'{scheme}::addr=127.0.0.1:1;'
                            f'lazy_connect=true;{key}=1;')
                    self.assertIs(
                        cm.exception.code,
                        qi.QuestDBErrorCode.ConfigError)

    def test_client_close_is_idempotent(self):
        client = qi.QuestDB.__new__(qi.QuestDB)
        client.close()
        client.close()

    def test_closed_client_methods_reject(self):
        client = qi.QuestDB.__new__(qi.QuestDB)

        with self.assertRaisesRegex(
                qi.QuestDBError,
                "__enter__\\(\\) can't be called: QuestDB is closed"):
            with client:
                pass
        with self.assertRaisesRegex(
                qi.QuestDBError,
                "reap_idle\\(\\) can't be called: QuestDB is closed"):
            client.reap_idle()
        with self.assertRaisesRegex(
                qi.QuestDBError,
                "dataframe\\(\\) can't be called: QuestDB is closed"):
            client.dataframe([], table_name='tbl', at=qi.ServerTimestamp)
        with self.assertRaisesRegex(
                qi.QuestDBError,
                "sender\\(\\) can't be called: QuestDB is closed"):
            client.sender()
        with self.assertRaisesRegex(
                qi.QuestDBError,
                "execute\\(\\) can't be called: QuestDB is closed"):
            client.execute('SELECT 1')

    def test_query_binds_container_validation(self):
        # Rejected before any reader is borrowed, so no server is needed
        # (pools connect lazily).
        with qi.QuestDB.from_conf('ws::addr=127.0.0.1:1;lazy_connect=true;') as client:
            for bad in ({'a': 1}, 42, 'x', {1, 2}):
                with self.assertRaisesRegex(
                        TypeError, '"binds" must be a list or tuple'):
                    client.query('SELECT $1', bad)
                with self.assertRaisesRegex(
                        TypeError, '"binds" must be a list or tuple'):
                    client.execute('SELECT $1', bad)

    def test_query_requires_sql(self):
        # Rejected before any reader is borrowed, so no server is needed
        # (pools connect lazily).
        with qi.QuestDB.from_conf('ws::addr=127.0.0.1:1;lazy_connect=true;') as client:
            with self.assertRaises(TypeError):
                client.query()
            with self.assertRaisesRegex(
                    TypeError, 'lease a PooledReader with reader()'):
                client.query(None)

    def test_closed_client_rejects_reader_lease(self):
        client = qi.QuestDB.__new__(qi.QuestDB)
        with self.assertRaisesRegex(
                qi.QuestDBError,
                "reader\\(\\) can't be called: QuestDB is closed"):
            client.reader()

    def test_unattached_reader_lease_surface(self):
        # A live lease needs a real reader borrow (no offline fixture
        # serves the QWP read endpoint), but the class itself and its
        # closed-state behaviour are constructible offline.
        lease = qi.PooledReader.__new__(qi.PooledReader)
        lease.close()
        lease.close()
        with self.assertRaisesRegex(
                qi.QuestDBError,
                "query\\(\\) can't be called: the reader lease is closed"):
            lease.query('SELECT 1')
        with self.assertRaisesRegex(
                qi.QuestDBError,
                "__enter__\\(\\) can't be called: "
                "the reader lease is closed"):
            with lease:
                pass

    def test_module_connect_factory(self):
        import questdb
        with QwpAckServer() as server:
            conf = (
                f'ws::addr=127.0.0.1:{server.port};lazy_connect=true;'
                'sender_pool_min=1;'
                'pool_reap=manual;')
            with questdb.connect(conf, sender_pool_max=1) as db:
                self.assertIsInstance(db, questdb.QuestDB)
                with db.sender() as sender:
                    sender.row(
                        'events', columns={'value': 1},
                        at=qi.ServerTimestamp)
            stats = server.snapshot()
        self.assertEqual(stats['errors'], [])
        self.assertEqual(stats['binary_frames'], 1)

    def test_module_connect_keyword_form(self):
        import questdb
        with QwpAckServer() as server:
            with questdb.connect(
                    host='127.0.0.1',
                    port=server.port,
                    lazy_connect=True,
                    sender_pool_min=1,
                    sender_pool_max=1,
                    pool_reap='manual') as db:
                with db.sender() as sender:
                    sender.row(
                        'events', columns={'value': 1},
                        at=qi.ServerTimestamp)
            stats = server.snapshot()
        self.assertEqual(stats['errors'], [])
        self.assertEqual(stats['binary_frames'], 1)

    def test_module_connect_argument_validation(self):
        import questdb
        with self.assertRaisesRegex(TypeError, 'but not both'):
            questdb.connect('ws::addr=localhost:9000;lazy_connect=true;', host='localhost')
        with self.assertRaisesRegex(TypeError, 'but not both'):
            questdb.connect()
        with self.assertRaisesRegex(
                ValueError, '"sender_pool_max" is already present'):
            questdb.connect(
                'ws::addr=localhost:9000;lazy_connect=true;sender_pool_max=2;',
                sender_pool_max=4)
        with self.assertRaisesRegex(TypeError, 'invalid settings keyword'):
            questdb.connect('ws::addr=localhost:9000;lazy_connect=true;', **{'a;b': 'x'})
        with self.assertRaisesRegex(TypeError, 'tls'):
            questdb.connect('ws::addr=localhost:9000;lazy_connect=true;', tls=True)
        with self.assertRaisesRegex(TypeError, 'port'):
            questdb.connect('ws::addr=localhost:9000;lazy_connect=true;', port=9009)
        with self.assertRaisesRegex(TypeError, 'port, tls'):
            questdb.connect(
                'ws::addr=localhost:9000;lazy_connect=true;', tls=False, port=9000)
        with self.assertRaisesRegex(TypeError, '"host" must be a str'):
            questdb.connect(host=123)
        with self.assertRaisesRegex(TypeError, 'invalid settings keyword'):
            questdb.connect(host='localhost', **{'a;b': 'x'})
        with self.assertRaisesRegex(ValueError, 'must not include a port'):
            questdb.connect(host='localhost:9000')
        with self.assertRaisesRegex(ValueError, 'must not include a port'):
            questdb.connect(host='[::1]:9000')

    def test_module_connect_conf_building(self):
        import questdb
        self.assertEqual(questdb._conf_value('failover', True), 'on')
        self.assertEqual(questdb._conf_value('failover', False), 'off')
        self.assertEqual(
            questdb._conf_value('tls_verify', False), 'unsafe_off')
        self.assertEqual(questdb._conf_value('password', 'a;b'), 'a;;b')
        self.assertEqual(questdb._conf_value('sender_pool_max', 4), '4')

        built = []

        class _Capture:
            @staticmethod
            def from_conf(conf_str, **kwargs):
                built.append(conf_str)
                return None

        with mock.patch.object(questdb, 'QuestDB', _Capture):
            questdb.connect(host='localhost')
            questdb.connect(host='localhost', port=9009, tls=True)
            questdb.connect(host='::1', port=1)
            questdb.connect(host='[::1]', port=1)
            questdb.connect(
                host='h', password='p;w', sender_pool_max=2, tls=False)
            questdb.connect(
                'ws::addr=h:9000;', sender_pool_max=2, tls_verify=False)
        self.assertEqual(built, [
            'ws::addr=localhost:9000;',
            'wss::addr=localhost:9009;',
            'ws::addr=[::1]:1;',
            'ws::addr=[::1]:1;',
            'ws::addr=h:9000;password=p;;w;sender_pool_max=2;',
            'ws::sender_pool_max=2;tls_verify=unsafe_off;addr=h:9000;',
        ])

    def test_module_connect_tls_verify_false_uses_unsafe_off(self):
        import questdb
        # Succeeding proves the bool was serialized using the native
        # `unsafe_off` spelling (not the invalid `off`) and that the build
        # ships the insecure-skip-verify native feature, as all builds must.
        db = questdb.connect(
            'wss::addr=127.0.0.1:1;', tls_verify=False, lazy_connect=True)
        db.close()

    def test_from_conf_rejects_non_callable_error_handler(self):
        with self.assertRaisesRegex(
                TypeError, '"error_handler" must be callable'):
            qi.QuestDB.from_conf(
                'ws::addr=127.0.0.1:1;lazy_connect=true;', error_handler=42)

    def test_pooled_sender_fsn_receipts(self):
        with QwpAckServer(ack_delay_s=0.2) as server:
            conf = (
                f'ws::addr=127.0.0.1:{server.port};lazy_connect=true;'
                'sender_pool_min=1;'
                'sender_pool_max=1;'
                'pool_reap=manual;')
            with qi.QuestDB.from_conf(conf) as client:
                with client.sender() as sender:
                    self.assertIsNone(sender.published_fsn())
                    self.assertIsNone(sender.flush_and_get_fsn())
                    sender.row(
                        'events', columns={'value': 1},
                        at=qi.ServerTimestamp)
                    fsn = sender.flush_and_get_fsn()
                    self.assertEqual(fsn, 0)
                    self.assertEqual(sender.published_fsn(), 0)
                    self.assertFalse(sender.await_acked_fsn(fsn, 25))
                    self.assertTrue(sender.await_acked_fsn(fsn, 10_000))
                    self.assertEqual(sender.acked_fsn(), 0)
                    self.assertTrue(sender.await_acked_fsn(fsn, 25))
                    sender.row(
                        'events', columns={'value': 2},
                        at=qi.ServerTimestamp)
                    keep_fsn = sender.flush_and_keep_and_get_fsn()
                    self.assertEqual(keep_fsn, 1)
                    self.assertGreater(len(sender), 0)
                    self.assertTrue(sender.await_acked_fsn(keep_fsn, 10_000))
                    sender.close(flush=False)
                    with self.assertRaises(TypeError):
                        sender.await_acked_fsn('0')
            stats = server.snapshot()
        self.assertEqual(stats['errors'], [])
        self.assertEqual(stats['binary_frames'], 2)

    def test_pooled_sender_auto_flush_rows_from_connect_kwargs(self):
        import questdb

        with QwpAckServer(record_payloads=True) as server:
            with questdb.connect(
                    host='127.0.0.1',
                    port=server.port,
                    lazy_connect=True,
                    sender_pool_min=1,
                    sender_pool_max=1,
                    pool_reap='manual',
                    auto_flush_rows=3,
                    auto_flush_bytes='off',
                    auto_flush_interval='off') as client:
                with client.sender() as sender:
                    for value in range(2):
                        sender.row(
                            'events', columns={'value': value},
                            at=qi.ServerTimestamp)
                    self.assertEqual(len(sender), 2)
                    self.assertEqual(server.snapshot()['binary_frames'], 0)

                    sender.row(
                        'events', columns={'value': 2},
                        at=qi.ServerTimestamp)
                    self.assertEqual(len(sender), 0)
                    sender.wait(5000)

            stats = server.snapshot()

        self.assertEqual(stats['errors'], [])
        data_frames = [
            payload for payload in stats['binary_payloads']
            if (payload[:4] == b'QWP1'
                and int.from_bytes(payload[6:8], 'little') > 0)]
        self.assertEqual(len(data_frames), 1)
        self.assertEqual(_first_qwp_table_row_count(data_frames[0]), 3)

    def test_pooled_sender_auto_flush_interval_on_next_row(self):
        with QwpAckServer(record_payloads=True) as server:
            conf = (
                f'ws::addr=127.0.0.1:{server.port};lazy_connect=true;'
                'sender_pool_min=1;'
                'sender_pool_max=1;'
                'pool_reap=manual;'
                'auto_flush_rows=off;'
                'auto_flush_bytes=off;'
                'auto_flush_interval=50;')
            with qi.QuestDB.from_conf(conf) as client:
                with client.sender() as sender:
                    sender.row(
                        'events', columns={'value': 1},
                        at=qi.ServerTimestamp)
                    self.assertEqual(len(sender), 1)
                    time.sleep(0.1)
                    self.assertEqual(len(sender), 1)

                    sender.row(
                        'events', columns={'value': 2},
                        at=qi.ServerTimestamp)
                    self.assertEqual(len(sender), 0)
                    sender.wait(5000)

            stats = server.snapshot()

        self.assertEqual(stats['errors'], [])
        data_frames = [
            payload for payload in stats['binary_payloads']
            if (payload[:4] == b'QWP1'
                and int.from_bytes(payload[6:8], 'little') > 0)]
        self.assertEqual(len(data_frames), 1)
        self.assertEqual(_first_qwp_table_row_count(data_frames[0]), 2)

    def test_pooled_sender_auto_flush_interval_starts_on_first_row(self):
        with QwpAckServer(record_payloads=True) as server:
            conf = (
                f'ws::addr=127.0.0.1:{server.port};lazy_connect=true;'
                'sender_pool_min=1;'
                'sender_pool_max=1;'
                'pool_reap=manual;'
                'auto_flush_rows=off;'
                'auto_flush_bytes=off;'
                'auto_flush_interval=50;')
            with qi.QuestDB.from_conf(conf) as client:
                with client.sender() as sender:
                    # Holding an unused lease does not consume the interval.
                    time.sleep(0.1)
                    sender.row(
                        'events', columns={'value': 1},
                        at=qi.ServerTimestamp)
                    self.assertEqual(len(sender), 1)
                    self.assertEqual(server.snapshot()['binary_frames'], 0)

                    time.sleep(0.1)
                    sender.row(
                        'events', columns={'value': 2},
                        at=qi.ServerTimestamp)
                    self.assertEqual(len(sender), 0)
                    sender.wait(5000)

            stats = server.snapshot()

        self.assertEqual(stats['errors'], [])
        data_frames = [
            payload for payload in stats['binary_payloads']
            if (payload[:4] == b'QWP1'
                and int.from_bytes(payload[6:8], 'little') > 0)]
        self.assertEqual(len(data_frames), 1)
        self.assertEqual(_first_qwp_table_row_count(data_frames[0]), 2)

    def test_pooled_sender_auto_flush_defaults_to_1000_rows(self):
        with QwpAckServer(record_payloads=True) as server:
            conf = (
                f'ws::addr=127.0.0.1:{server.port};lazy_connect=true;'
                'sender_pool_min=1;'
                'sender_pool_max=1;'
                'pool_reap=manual;'
                'auto_flush_interval=off;')
            with qi.QuestDB.from_conf(conf) as client:
                with client.sender() as sender:
                    for value in range(999):
                        sender.row(
                            'events', columns={'value': value},
                            at=qi.ServerTimestamp)
                    self.assertEqual(len(sender), 999)
                    self.assertEqual(server.snapshot()['binary_frames'], 0)

                    sender.row(
                        'events', columns={'value': 999},
                        at=qi.ServerTimestamp)
                    self.assertEqual(len(sender), 0)
                    sender.wait(5000)

            stats = server.snapshot()

        self.assertEqual(stats['errors'], [])
        data_frames = [
            payload for payload in stats['binary_payloads']
            if (payload[:4] == b'QWP1'
                and int.from_bytes(payload[6:8], 'little') > 0)]
        self.assertEqual(len(data_frames), 1)
        self.assertEqual(_first_qwp_table_row_count(data_frames[0]), 1000)

    def test_pooled_sender_auto_flush_defaults_to_100ms(self):
        with QwpAckServer(record_payloads=True) as server:
            conf = (
                f'ws::addr=127.0.0.1:{server.port};lazy_connect=true;'
                'sender_pool_min=1;'
                'sender_pool_max=1;'
                'pool_reap=manual;')
            with qi.QuestDB.from_conf(conf) as client:
                with client.sender() as sender:
                    sender.row(
                        'events', columns={'value': 1},
                        at=qi.ServerTimestamp)
                    self.assertEqual(len(sender), 1)
                    time.sleep(0.15)
                    sender.row(
                        'events', columns={'value': 2},
                        at=qi.ServerTimestamp)
                    self.assertEqual(len(sender), 0)
                    sender.wait(5000)

            stats = server.snapshot()

        self.assertEqual(stats['errors'], [])
        data_frames = [
            payload for payload in stats['binary_payloads']
            if (payload[:4] == b'QWP1'
                and int.from_bytes(payload[6:8], 'little') > 0)]
        self.assertEqual(len(data_frames), 1)
        self.assertEqual(_first_qwp_table_row_count(data_frames[0]), 2)

    def test_pooled_sender_auto_flush_default_bytes_uses_server_cap(self):
        cap = 4096
        connected = threading.Event()

        def on_event(event):
            if event.kind is qi.ConnectionEventKind.Connected:
                connected.set()

        with QwpAckServer(
                max_batch_size=cap, record_payloads=True) as server:
            conf = (
                f'ws::addr=127.0.0.1:{server.port};lazy_connect=true;'
                'sender_pool_min=1;'
                'sender_pool_max=1;'
                'pool_reap=manual;'
                'auto_flush_rows=off;'
                'auto_flush_interval=off;')
            with qi.QuestDB.from_conf(
                    conf, connection_listener=on_event) as client:
                with client.sender() as sender:
                    self.assertTrue(connected.wait(5))
                    flushed_after = None
                    for row_count in range(1, 33):
                        sender.row(
                            'events', columns={'value': 'x' * 300},
                            at=qi.ServerTimestamp)
                        if len(sender) == 0:
                            flushed_after = row_count
                            break
                    self.assertIsNotNone(flushed_after)
                    self.assertGreater(flushed_after, 1)
                    sender.wait(5000)

            stats = server.snapshot()

        self.assertEqual(stats['errors'], [])
        data_frames = [
            payload for payload in stats['binary_payloads']
            if (payload[:4] == b'QWP1'
                and int.from_bytes(payload[6:8], 'little') > 0)]
        self.assertEqual(len(data_frames), 1)
        self.assertLessEqual(len(data_frames[0]), cap)
        self.assertEqual(
            _first_qwp_table_row_count(data_frames[0]), flushed_after)

    def test_pooled_sender_auto_flush_bytes_off_ignores_server_cap(self):
        connected = threading.Event()

        def on_event(event):
            if event.kind is qi.ConnectionEventKind.Connected:
                connected.set()

        with QwpAckServer(max_batch_size=4096) as server:
            conf = (
                f'ws::addr=127.0.0.1:{server.port};lazy_connect=true;'
                'sender_pool_min=1;'
                'sender_pool_max=1;'
                'pool_reap=manual;'
                'auto_flush_rows=off;'
                'auto_flush_bytes=off;'
                'auto_flush_interval=off;')
            with qi.QuestDB.from_conf(
                    conf, connection_listener=on_event) as client:
                with client.sender() as sender:
                    self.assertTrue(connected.wait(5))
                    for _ in range(20):
                        sender.row(
                            'events', columns={'value': 'x' * 300},
                            at=qi.ServerTimestamp)
                    self.assertEqual(len(sender), 20)
                    self.assertEqual(server.snapshot()['binary_frames'], 0)
                    sender.close(flush=False)

            stats = server.snapshot()

        self.assertEqual(stats['errors'], [])
        self.assertEqual(stats['binary_frames'], 0)

    def test_pooled_sender_auto_flush_bytes_is_clamped_to_server_cap(self):
        cap = 4096
        connected = threading.Event()

        def on_event(event):
            if event.kind is qi.ConnectionEventKind.Connected:
                connected.set()

        with QwpAckServer(
                max_batch_size=cap, record_payloads=True) as server:
            conf = (
                f'ws::addr=127.0.0.1:{server.port};lazy_connect=true;'
                'sender_pool_min=1;'
                'sender_pool_max=1;'
                'pool_reap=manual;'
                'auto_flush_rows=off;'
                'auto_flush_bytes=100000;'
                'auto_flush_interval=off;')
            with qi.QuestDB.from_conf(
                    conf, connection_listener=on_event) as client:
                with client.sender() as sender:
                    self.assertTrue(connected.wait(5))
                    for row_count in range(1, 33):
                        sender.row(
                            'events', columns={'value': 'x' * 300},
                            at=qi.ServerTimestamp)
                        if len(sender) == 0:
                            break
                    else:
                        self.fail('the cap-clamped byte threshold did not fire')
                    sender.wait(5000)

            stats = server.snapshot()

        self.assertEqual(stats['errors'], [])
        data_frames = [
            payload for payload in stats['binary_payloads']
            if (payload[:4] == b'QWP1'
                and int.from_bytes(payload[6:8], 'little') > 0)]
        self.assertEqual(len(data_frames), 1)
        self.assertLessEqual(len(data_frames[0]), cap)
        self.assertEqual(
            _first_qwp_table_row_count(data_frames[0]), row_count)

    def test_pooled_sender_auto_flush_default_bytes_falls_back_to_8_mib(self):
        with QwpAckServer() as server:
            conf = (
                f'ws::addr=127.0.0.1:{server.port};lazy_connect=true;'
                'sender_pool_min=1;'
                'sender_pool_max=1;'
                'pool_reap=manual;'
                'sf_max_segment_bytes=16mb;'
                'auto_flush_rows=off;'
                'auto_flush_interval=off;')
            with qi.QuestDB.from_conf(conf) as client:
                with client.sender() as sender:
                    sender.row(
                        'events', columns={'value': 'x' * (8 * 1024 * 1024)},
                        at=qi.ServerTimestamp)
                    self.assertEqual(len(sender), 0)
                    sender.wait(30000)

            stats = server.snapshot()

        self.assertEqual(stats['errors'], [])
        self.assertEqual(stats['binary_frames'], 1)

    def test_pooled_sender_auto_flush_oversize_row_error_propagates(self):
        connected = threading.Event()

        def on_event(event):
            if event.kind is qi.ConnectionEventKind.Connected:
                connected.set()

        with QwpAckServer(max_batch_size=1024) as server:
            conf = (
                f'ws::addr=127.0.0.1:{server.port};lazy_connect=true;'
                'sender_pool_min=1;'
                'sender_pool_max=1;'
                'pool_reap=manual;'
                'auto_flush_rows=off;'
                'auto_flush_interval=off;')
            with qi.QuestDB.from_conf(
                    conf, connection_listener=on_event) as client:
                sender = client.sender()
                try:
                    self.assertTrue(connected.wait(5))
                    with self.assertRaises(qi.QuestDBError) as raised:
                        sender.row(
                            'events', columns={'value': 'x' * 2048},
                            at=qi.ServerTimestamp)
                    self.assertEqual(
                        raised.exception.code,
                        qi.QuestDBErrorCode.BatchTooLarge)
                    self.assertEqual(
                        len(sender), 0,
                        'an irreducibly oversized row must be removed')
                finally:
                    sender.close(flush=False)

            stats = server.snapshot()

        self.assertEqual(stats['errors'], [])
        self.assertEqual(stats['binary_frames'], 0)

    def test_pooled_sender_auto_flush_multirow_overshoot_keeps_batch(self):
        connected = threading.Event()

        def on_event(event):
            if event.kind is qi.ConnectionEventKind.Connected:
                connected.set()

        with QwpAckServer(max_batch_size=4096) as server:
            conf = (
                f'ws::addr=127.0.0.1:{server.port};lazy_connect=true;'
                'sender_pool_min=1;'
                'sender_pool_max=1;'
                'pool_reap=manual;'
                'auto_flush_rows=off;'
                'auto_flush_interval=off;')
            with qi.QuestDB.from_conf(
                    conf, connection_listener=on_event) as client:
                sender = client.sender()
                try:
                    self.assertTrue(connected.wait(5))
                    sender.row(
                        'events', columns={'value': 'x' * 3000},
                        at=qi.ServerTimestamp)
                    self.assertEqual(len(sender), 1)

                    with self.assertRaises(qi.QuestDBError) as raised:
                        sender.row(
                            'events', columns={'value': 'y' * 1500},
                            at=qi.ServerTimestamp)
                    self.assertEqual(
                        raised.exception.code,
                        qi.QuestDBErrorCode.BatchTooLarge)
                    self.assertEqual(
                        len(sender), 2,
                        'a failed multi-row publish must retain the full batch')
                finally:
                    sender.close(flush=False)

            stats = server.snapshot()

        self.assertEqual(stats['errors'], [])
        self.assertEqual(stats['binary_frames'], 0)

    def test_pooled_sender_auto_flush_can_be_disabled(self):
        with QwpAckServer() as server:
            conf = (
                f'ws::addr=127.0.0.1:{server.port};lazy_connect=true;'
                'sender_pool_min=1;'
                'sender_pool_max=1;'
                'pool_reap=manual;'
                'auto_flush_bytes=1000;'
                'auto_flush=off;')
            with qi.QuestDB.from_conf(conf) as client:
                with client.sender() as sender:
                    sender.row(
                        'events', columns={'value': 'x' * 2000},
                        at=qi.ServerTimestamp)
                    time.sleep(0.15)
                    sender.row(
                        'events', columns={'value': 'y'},
                        at=qi.ServerTimestamp)
                    self.assertEqual(len(sender), 2)
                    self.assertEqual(server.snapshot()['binary_frames'], 0)
                    sender.close(flush=False)

            stats = server.snapshot()

        self.assertEqual(stats['errors'], [])
        self.assertEqual(stats['binary_frames'], 0)

    def test_pooled_sender_auto_flush_close_publishes_partial_buffer(self):
        with QwpAckServer(record_payloads=True) as server:
            conf = (
                f'ws::addr=127.0.0.1:{server.port};lazy_connect=true;'
                'sender_pool_min=1;'
                'sender_pool_max=1;'
                'pool_reap=manual;'
                'auto_flush_rows=3;'
                'auto_flush_bytes=off;'
                'auto_flush_interval=off;')
            with qi.QuestDB.from_conf(conf) as client:
                with client.sender() as sender:
                    sender.row(
                        'events', columns={'value': 1},
                        at=qi.ServerTimestamp)
                    self.assertEqual(len(sender), 1)

            stats = server.snapshot()

        self.assertEqual(stats['errors'], [])
        data_frames = [
            payload for payload in stats['binary_payloads']
            if (payload[:4] == b'QWP1'
                and int.from_bytes(payload[6:8], 'little') > 0)]
        self.assertEqual(len(data_frames), 1)
        self.assertEqual(_first_qwp_table_row_count(data_frames[0]), 1)

    def test_pooled_sender_auto_flush_error_propagates_from_row(self):
        rejected = threading.Event()

        def on_rejection(_error):
            rejected.set()

        with QwpAckServer(error_status=0x03) as server:
            conf = (
                f'ws::addr=127.0.0.1:{server.port};lazy_connect=true;'
                'sender_pool_min=1;'
                'sender_pool_max=1;'
                'pool_reap=manual;'
                'close_flush_timeout_millis=0;'
                'auto_flush_rows=1;'
                'auto_flush_bytes=off;'
                'auto_flush_interval=off;')
            with qi.QuestDB.from_conf(
                    conf, error_handler=on_rejection) as client:
                with client.sender() as sender:
                    sender.row(
                        'events', columns={'value': 1},
                        at=qi.ServerTimestamp)
                    self.assertTrue(
                        rejected.wait(5),
                        'the terminal rejection must be latched')
                    with self.assertRaises(qi.QuestDBError) as raised:
                        sender.row(
                            'events', columns={'value': 2},
                            at=qi.ServerTimestamp)
                    self.assertEqual(len(sender), 1)
                    sender.close(flush=False)

            stats = server.snapshot()

        self.assertEqual(stats['errors'], [])
        self.assertIsInstance(
            raised.exception, qi.QuestDBServerRejectionError)

    def test_pooled_sender_await_acked_fsn_validates_timeout(self):
        # Regression: timeout_millis must be validated and converted while
        # holding the GIL; converting it inside the no-GIL wait region
        # crashes the interpreter on overflow or a non-int.
        with QwpAckServer(ack_delay_s=0.2) as server:
            conf = (
                f'ws::addr=127.0.0.1:{server.port};lazy_connect=true;'
                'sender_pool_min=1;'
                'sender_pool_max=1;'
                'pool_reap=manual;')
            with qi.QuestDB.from_conf(conf) as client:
                with client.sender() as sender:
                    sender.row(
                        'events', columns={'value': 1},
                        at=qi.ServerTimestamp)
                    fsn = sender.flush_and_get_fsn()
                    with self.assertRaises(OverflowError):
                        sender.await_acked_fsn(fsn, 2 ** 64)
                    with self.assertRaises(TypeError):
                        sender.await_acked_fsn(fsn, 1.5)
                    with self.assertRaises(TypeError):
                        sender.await_acked_fsn(fsn, True)
                    with self.assertRaises(ValueError):
                        sender.await_acked_fsn(fsn, -1)
                    self.assertTrue(sender.await_acked_fsn(fsn, 10_000))

    def test_pooled_sender_poll_error_is_lease_scoped(self):
        with QwpAckServer(error_status=0x09) as server:  # retriable
            conf = (
                f'ws::addr=127.0.0.1:{server.port};lazy_connect=true;'
                'sender_pool_min=1;'
                'sender_pool_max=1;'
                'pool_reap=manual;'
                'reconnect_max_duration_millis=200;'
                'close_flush_timeout_millis=0;')
            with qi.QuestDB.from_conf(conf) as client:
                with client.sender() as sender:
                    self.assertIsNone(sender.poll_error())
                    self.assertEqual(sender.error_events_dropped(), 0)
                    sender.row(
                        'events', columns={'value': 1},
                        at=qi.ServerTimestamp)
                    sender.flush()
                    deadline = time.monotonic() + 5
                    error = None
                    while error is None:
                        self.assertLess(time.monotonic(), deadline)
                        error = sender.poll_error()
                        time.sleep(0.01)
                    self.assertIsInstance(error, qi.SenderError)
                    self.assertIs(
                        error.category, qi.SenderErrorCategory.WriteError)

    def test_from_conf_rejects_bad_inbox_capacities(self):
        # Regression: these conversions must raise while holding the GIL
        # instead of crashing in the no-GIL connect region.
        for kwargs in (
                {'connection_event_inbox_capacity': -1},
                {'error_event_inbox_capacity': -1},
                {'connection_event_inbox_capacity': 'lots'},
                {'error_event_inbox_capacity': None}):
            with self.assertRaises((TypeError, OverflowError)):
                qi.QuestDB.from_conf('ws::addr=127.0.0.1:1;lazy_connect=true;', **kwargs)

    def test_pool_rejection_handler_receives_server_rejection(self):
        rejections = []
        delivered = threading.Event()

        def on_rejection(error):
            rejections.append(error)
            delivered.set()

        with QwpAckServer(error_status=0x03) as server:
            conf = (
                f'ws::addr=127.0.0.1:{server.port};lazy_connect=true;'
                'sender_pool_min=1;'
                'sender_pool_max=1;'
                'pool_reap=manual;'
                'close_flush_timeout_millis=0;')
            with qi.QuestDB.from_conf(
                    conf, error_handler=on_rejection) as client:
                with client.sender() as sender:
                    sender.row(
                        'events', columns={'value': 1},
                        at=qi.ServerTimestamp)
                    sender.flush()
                self.assertTrue(
                    delivered.wait(5),
                    'the rejection must reach the handler')
                deadline = time.monotonic() + 5
                while client.error_events_delivered < 1:
                    self.assertLess(time.monotonic(), deadline)
                    time.sleep(0.01)
                self.assertEqual(client.error_events_dropped, 0)

        error = rejections[0]
        self.assertIsInstance(error, qi.SenderError)
        self.assertIs(error.category, qi.SenderErrorCategory.SchemaMismatch)
        self.assertIs(error.applied_policy, qi.SenderErrorPolicy.Terminal)
        self.assertEqual(error.message, 'mock rejection')

    def test_pool_rejection_default_handler_logs(self):
        with QwpAckServer(error_status=0x03) as server:
            conf = (
                f'ws::addr=127.0.0.1:{server.port};lazy_connect=true;'
                'sender_pool_min=1;'
                'sender_pool_max=1;'
                'pool_reap=manual;'
                'close_flush_timeout_millis=0;')
            with self.assertLogs('questdb', level='ERROR') as logs:
                with qi.QuestDB.from_conf(conf) as client:
                    with client.sender() as sender:
                        sender.row(
                            'events', columns={'value': 1},
                            at=qi.ServerTimestamp)
                        sender.flush()
                    deadline = time.monotonic() + 5
                    while client.error_events_delivered < 1:
                        self.assertLess(time.monotonic(), deadline)
                        time.sleep(0.01)
        self.assertTrue(
            any('server rejection' in line for line in logs.output),
            logs.output)

    def test_pool_rejection_handler_exception_is_logged(self):
        delivered = threading.Event()

        def on_rejection(error):
            delivered.set()
            raise RuntimeError('handler boom')

        with QwpAckServer(error_status=0x03) as server:
            conf = (
                f'ws::addr=127.0.0.1:{server.port};lazy_connect=true;'
                'sender_pool_min=1;'
                'sender_pool_max=1;'
                'pool_reap=manual;'
                'close_flush_timeout_millis=0;')
            with self.assertLogs('questdb', level='ERROR') as logs:
                with qi.QuestDB.from_conf(
                        conf, error_handler=on_rejection) as client:
                    with client.sender() as sender:
                        sender.row(
                            'events', columns={'value': 1},
                            at=qi.ServerTimestamp)
                        sender.flush()
                    self.assertTrue(
                        delivered.wait(5),
                        'the rejection must reach the handler')
                    deadline = time.monotonic() + 5
                    while client.error_events_delivered < 1:
                        self.assertLess(time.monotonic(), deadline)
                        time.sleep(0.01)
        self.assertTrue(
            any('error handler failed' in line for line in logs.output),
            logs.output)

    def test_close_from_rejection_handler_does_not_deadlock(self):
        received = threading.Event()
        handler_done = threading.Event()

        def on_rejection(error):
            received.set()
            # Give the main thread time to enter close() and block on
            # joining this dispatcher thread before closing from here.
            time.sleep(0.2)
            client.close()
            handler_done.set()

        with QwpAckServer(error_status=0x03) as server:
            conf = (
                f'ws::addr=127.0.0.1:{server.port};lazy_connect=true;'
                'sender_pool_min=1;'
                'sender_pool_max=1;'
                'pool_reap=manual;'
                'close_flush_timeout_millis=0;')
            client = qi.QuestDB.from_conf(
                conf, error_handler=on_rejection)
            with client.sender() as sender:
                sender.row(
                    'events', columns={'value': 1},
                    at=qi.ServerTimestamp)
                sender.flush()
            self.assertTrue(received.wait(5))
            closer = threading.Thread(target=client.close, daemon=True)
            closer.start()
            closer.join(10)
            self.assertFalse(
                closer.is_alive(),
                'close() must not deadlock against the handler thread')
            self.assertTrue(handler_done.wait(5))

    def test_close_from_rejection_handler_as_primary_closer(self):
        handler_done = threading.Event()

        def on_rejection(error):
            client.close()
            handler_done.set()

        with QwpAckServer(error_status=0x03) as server:
            conf = (
                f'ws::addr=127.0.0.1:{server.port};lazy_connect=true;'
                'sender_pool_min=1;'
                'sender_pool_max=1;'
                'pool_reap=manual;'
                'close_flush_timeout_millis=0;')
            client = qi.QuestDB.from_conf(
                conf, error_handler=on_rejection)
            with client.sender() as sender:
                sender.row(
                    'events', columns={'value': 1},
                    at=qi.ServerTimestamp)
                sender.flush()
            self.assertTrue(
                handler_done.wait(10),
                'the handler must be able to run close() itself')
            client.close()
            with self.assertRaises(qi.QuestDBError):
                client.sender()

    def test_close_from_connection_listener_does_not_deadlock(self):
        connected = threading.Event()
        start_close = threading.Event()
        listener_done = threading.Event()

        def on_event(event):
            connected.set()
            if not start_close.wait(10):
                return
            # Give the main thread time to enter close() and block on
            # joining this dispatcher thread before closing from here.
            time.sleep(0.2)
            client.close()
            listener_done.set()

        with QwpAckServer() as server:
            conf = (
                f'ws::addr=127.0.0.1:{server.port};lazy_connect=true;'
                'sender_pool_min=1;'
                'sender_pool_max=1;'
                'pool_reap=manual;')
            client = qi.QuestDB.from_conf(
                conf, connection_listener=on_event)
            with client.sender() as sender:
                sender.row(
                    'events', columns={'value': 1},
                    at=qi.ServerTimestamp)
                sender.flush(wait=True)
            self.assertTrue(connected.wait(5))
            start_close.set()
            closer = threading.Thread(target=client.close, daemon=True)
            closer.start()
            closer.join(10)
            self.assertFalse(
                closer.is_alive(),
                'close() must not deadlock against the listener thread')
            self.assertTrue(listener_done.wait(5))

    def test_connection_listener_exception_is_logged(self):
        delivered = threading.Event()

        def on_event(event):
            delivered.set()
            raise RuntimeError('listener boom')

        with QwpAckServer() as server:
            conf = (
                f'ws::addr=127.0.0.1:{server.port};lazy_connect=true;'
                'sender_pool_min=1;'
                'sender_pool_max=1;'
                'pool_reap=manual;')
            with self.assertLogs('questdb', level='ERROR') as logs:
                with qi.QuestDB.from_conf(
                        conf, connection_listener=on_event) as client:
                    with client.sender() as sender:
                        sender.row(
                            'events', columns={'value': 1},
                            at=qi.ServerTimestamp)
                        sender.flush(wait=True)
                    self.assertTrue(
                        delivered.wait(5),
                        'the connection event must reach the listener')
        self.assertTrue(
            any('listener failed' in line for line in logs.output),
            logs.output)

    def test_error_event_inbox_overflow_drops_oldest(self):
        """With the handler blocked and a 1-slot inbox, a continuous
        rejection stream must overflow the inbox and count drops.

        Status 0x0C (not-writable) maps to the RetriableOther policy,
        so the same frame is replayed without consuming poison strikes.
        A 1 ms reconnect backoff keeps that replay deterministic and fast
        while the handler remains blocked.
        """
        release = threading.Event()
        first_delivered = threading.Event()

        def on_error(error):
            first_delivered.set()
            release.wait(30)

        with QwpAckServer(error_status=0x0C) as server:
            conf = (
                f'ws::addr=127.0.0.1:{server.port};lazy_connect=true;'
                'sender_pool_min=1;'
                'sender_pool_max=1;'
                'pool_reap=manual;'
                'close_flush_timeout_millis=0;')
            conf += (
                'reconnect_initial_backoff_millis=1;'
                'reconnect_max_backoff_millis=1;')
            with qi.QuestDB.from_conf(
                    conf,
                    error_handler=on_error,
                    error_event_inbox_capacity=1) as client:
                try:
                    with client.sender() as sender:
                        sender.row(
                            'events', columns={'value': 1},
                            at=qi.ServerTimestamp)
                        sender.flush()
                    self.assertTrue(
                        first_delivered.wait(10),
                        'the first rejection must reach the handler')
                    deadline = time.monotonic() + 15
                    while client.error_events_dropped < 1:
                        self.assertLess(
                            time.monotonic(), deadline,
                            'inbox overflow must drop the oldest event')
                        time.sleep(0.01)
                    release.set()
                    # Delivery is counted once the handler returns.
                    deadline = time.monotonic() + 10
                    while client.error_events_delivered < 1:
                        self.assertLess(time.monotonic(), deadline)
                        time.sleep(0.01)
                finally:
                    release.set()

    def test_standalone_sender_error_ring_overflow_counts_drops(self):
        """A standalone ws sender's per-connection diagnostic ring
        (``error_inbox_capacity``, minimum 16) must evict oldest un-polled
        diagnostics and count them once a replayable rejection outruns
        polling. Status 0x0C is strike-exempt, so one frame is enough."""
        with QwpAckServer(error_status=0x0C) as server:
            conf = (
                f'ws::addr=127.0.0.1:{server.port};lazy_connect=true;'
                'error_inbox_capacity=16;'
                'close_flush_timeout_millis=0;'
                'reconnect_initial_backoff_millis=1;'
                'reconnect_max_backoff_millis=1;'
                'reconnect_max_duration_millis=30000;')
            sender = qi.Sender.from_conf(conf)
            try:
                sender.establish()
                sender.row(
                    'events', columns={'value': 1},
                    at=qi.ServerTimestamp)
                sender.flush()
                deadline = time.monotonic() + 15
                while sender.error_events_dropped() < 1:
                    self.assertLess(
                        time.monotonic(), deadline,
                        'ring overflow must count dropped diagnostics')
                    time.sleep(0.01)
            finally:
                sender.close(False)

    def test_standalone_sender_method_reentrancy_from_handler_raises(self):
        """In manual-progress mode a standalone ws sender delivers its error
        handler synchronously on the driving thread. Calling any sender method
        from inside that handler must raise QuestDBError(InvalidApiCall) rather
        than reenter the live native sender (which would abort the
        interpreter)."""
        holder = []
        captured = []
        fired = threading.Event()

        def on_error(error):
            sender = holder[0]
            for call in (
                    sender.published_fsn,
                    sender.acked_fsn,
                    sender.drive_once,
                    sender.flush,
                    sender.close):
                try:
                    call()
                except qi.QuestDBError as e:
                    captured.append(e.code)
                else:
                    captured.append(None)
            fired.set()

        with QwpAckServer(error_status=0x0C) as server:
            conf = (
                f'ws::addr=127.0.0.1:{server.port};lazy_connect=true;'
                'qwp_ws_progress=manual;'
                'close_flush_timeout_millis=0;'
                'reconnect_max_duration_millis=30000;')
            sender = qi.Sender.from_conf(conf, error_handler=on_error)
            holder.append(sender)
            try:
                sender.establish()
                sender.row(
                    'events', columns={'value': 1}, at=qi.ServerTimestamp)
                sender.flush()
                deadline = time.monotonic() + 15
                while not fired.is_set():
                    self.assertLess(
                        time.monotonic(), deadline,
                        'the error handler must fire')
                    try:
                        sender.drive_once()
                    except qi.QuestDBError:
                        pass
                    time.sleep(0.01)
            finally:
                sender.close(False)

        self.assertTrue(captured, 'the handler must run reentrant calls')
        self.assertTrue(
            all(code is qi.QuestDBErrorCode.InvalidApiCall
                for code in captured),
            captured)

    def test_sender_pool_concurrent_borrow_flush(self):
        """Deterministic multi-thread exerciser for the sender pool:
        8 threads share a 2-connection pool while another thread reaps
        concurrently. Every flush must land exactly one frame and the
        pool accounting must let close() finish promptly. The client is
        closed only once every thread is proven finished, so a wedged
        worker fails the test instead of hanging close()."""
        n_threads = 8
        iterations = 25
        errors = []
        barrier = threading.Barrier(n_threads)
        reap_stop = threading.Event()

        with QwpAckServer() as server:
            conf = (
                f'ws::addr=127.0.0.1:{server.port};lazy_connect=true;'
                'sender_pool_min=1;'
                'sender_pool_max=2;'
                'pool_reap=manual;'
                'acquire_timeout_ms=30000;')
            client = qi.QuestDB.from_conf(conf)

            def worker(thread_index):
                try:
                    barrier.wait(30)
                    for i in range(iterations):
                        with client.sender() as sender:
                            sender.row(
                                'events',
                                columns={'t': thread_index, 'i': i},
                                at=qi.ServerTimestamp)
                            sender.flush(wait=True)
                except BaseException as e:
                    errors.append(e)

            def reaper():
                while not reap_stop.is_set():
                    client.reap_idle()
                    time.sleep(0.005)

            threads = [
                threading.Thread(target=worker, args=(t,), daemon=True)
                for t in range(n_threads)]
            reap_thread = threading.Thread(target=reaper, daemon=True)
            for thread in threads:
                thread.start()
            reap_thread.start()
            for thread in threads:
                thread.join(90)
            reap_stop.set()
            reap_thread.join(10)
            self.assertFalse(
                any(t.is_alive() for t in threads + [reap_thread]),
                'worker or reaper thread did not finish')
            self.assertEqual(errors, [])
            client.close()
            self.assertEqual(
                server.wait_binary_frames_settled(),
                n_threads * iterations)
            stats = server.snapshot()
        self.assertEqual(stats['errors'], [])

    def test_client_sender_publishes_rows_without_dataframe_surface(self):
        with QwpAckServer() as server:
            conf = (
                f'ws::addr=127.0.0.1:{server.port};lazy_connect=true;'
                'sender_pool_min=1;'
                'sender_pool_max=1;'
                'pool_reap=manual;')
            with qi.QuestDB.from_conf(conf) as client:
                with client.sender() as sender:
                    self.assertIsInstance(sender, qi.PooledSender)
                    self.assertNotIsInstance(sender, qi.Sender)
                    self.assertTrue(callable(sender.dataframe))
                    for name in (
                            'establish', 'transaction', 'new_buffer',
                            'drive_once', 'close_drain',
                            'protocol_version', 'auto_flush'):
                        self.assertFalse(hasattr(sender, name), name)
                    for name in (
                            'flush_and_get_fsn',
                            'flush_and_keep_and_get_fsn',
                            'published_fsn', 'acked_fsn', 'await_acked_fsn',
                            'poll_error', 'error_events_dropped'):
                        self.assertTrue(callable(getattr(sender, name)), name)
                    sender.row(
                        'weather',
                        symbols={'city': 'London'},
                        columns={'temperature': 21.5, 'sample': 1},
                        at=qi.TimestampNanos(1_700_000_000_000_000_000))
                    self.assertGreater(len(sender), 0)
                    self.assertIs(sender.flush(wait=True), sender)
                    self.assertEqual(len(sender), 0)

            stats = server.snapshot()

        self.assertEqual(stats['errors'], [])
        self.assertEqual(stats['accepted_connections'], 1)
        self.assertEqual(stats['binary_frames'], 1)
        self.assertEqual(stats['qwp1_frames'], 1)
        with self.assertRaisesRegex(
                qi.QuestDBError,
                "row\\(\\) can't be called: Sender is closed"):
            sender.row(
                'weather', columns={'temperature': 22.0},
                at=qi.ServerTimestamp)

    def test_client_close_waits_for_sender_lease_return(self):
        with QwpAckServer() as server:
            conf = (
                f'ws::addr=127.0.0.1:{server.port};lazy_connect=true;'
                'sender_pool_min=1;'
                'sender_pool_max=1;'
                'pool_reap=manual;')
            client = qi.QuestDB.from_conf(conf)
            sender = client.sender()
            close_started = threading.Event()
            close_done = threading.Event()
            errors = []

            def close_client():
                close_started.set()
                try:
                    client.close()
                except Exception as exc:
                    errors.append(exc)
                finally:
                    close_done.set()

            thread = threading.Thread(target=close_client)
            thread.start()
            self.assertTrue(close_started.wait(1))
            self.assertFalse(
                close_done.wait(0.05),
                'Client.close() returned while a sender lease was active')
            sender.close(flush=False)
            self.assertTrue(close_done.wait(2))
            thread.join(timeout=1)

        self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])

    def test_client_sender_context_flushes_success_and_discards_exception(self):
        with QwpAckServer() as server:
            conf = (
                f'ws::addr=127.0.0.1:{server.port};lazy_connect=true;'
                'sender_pool_min=1;'
                'sender_pool_max=1;'
                'pool_reap=manual;')
            with qi.QuestDB.from_conf(conf) as client:
                with client.sender() as sender:
                    sender.row(
                        'events', columns={'value': 1},
                        at=qi.ServerTimestamp)
                with client.sender() as sender:
                    sender.wait(5000)

                with self.assertRaisesRegex(RuntimeError, 'abort row'):
                    with client.sender() as sender:
                        sender.row(
                            'events', columns={'value': 2},
                            at=qi.ServerTimestamp)
                        raise RuntimeError('abort row')

            stats = server.snapshot()

        self.assertEqual(stats['errors'], [])
        self.assertEqual(stats['binary_frames'], 1)

    @unittest.skipIf(pd is None, 'pandas not installed')
    def test_dataframe_protocol_matrix(self):
        df = pd.DataFrame({'value': [1]})

        buffer = qi.Buffer(protocol_version=2)
        with mock.patch.object(warnings, 'warn') as warn:
            buffer.dataframe(
                df, table_name='trades', at=qi.ServerTimestamp)
        warn.assert_not_called()
        self.assertGreater(len(buffer), 0)

        standalone = qi.Sender.from_conf(
            'http::addr=127.0.0.1:9000;', auto_flush=False)
        try:
            with mock.patch.object(warnings, 'warn') as warn:
                with self.assertRaisesRegex(
                        qi.QuestDBError,
                        "dataframe\\(\\) can't be called: Sender is closed"):
                    standalone.dataframe(
                        df, table_name='trades', at=qi.ServerTimestamp)
            warn.assert_not_called()
        finally:
            standalone.close(flush=False)

        # A ws sender's dataframe() is available regardless of how it was
        # built (from_conf or constructor); both retain a clone of the opts.
        # The actual round-trip needs a live server (see the e2e suite);
        # here just confirm the method exists and a closed sender rejects it.
        ws_ctor = qi.Sender(qi.Protocol.Ws, '127.0.0.1', 9000)
        self.assertTrue(callable(ws_ctor.dataframe))
        ws_ctor.close(flush=False)
        with self.assertRaisesRegex(
                qi.QuestDBError,
                r"can't be called: Sender is closed"):
            ws_ctor.dataframe(df, table_name='trades', at=qi.ServerTimestamp)

    @unittest.skipIf(pd is None, 'pandas not installed')
    @unittest.skipIf(pyarrow is None, 'pyarrow not installed')
    def test_client_dataframe_uses_pooled_qwp_websocket_connection(self):
        df = pd.DataFrame({
            'ts': pd.Series([
                pd.Timestamp('2024-01-01 00:00:00'),
                pd.Timestamp('2024-01-01 00:00:01')], dtype='datetime64[ns]'),
            'seq': pd.Series([1, 2], dtype='int64'),
            'price': pd.Series([10.5, 11.5], dtype='float64'),
        })

        with QwpAckServer() as server:
            conf = (
                f'ws::addr=127.0.0.1:{server.port};lazy_connect=true;'
                'sender_pool_min=1;'
                'sender_pool_max=1;'
                'pool_reap=manual;')
            client = qi.QuestDB.from_conf(conf)
            try:
                for _ in range(3):
                    client.dataframe(df, table_name='trades', at='ts')
            finally:
                client.close()

            stats = server.snapshot()

        self.assertEqual(stats['errors'], [])
        self.assertEqual(stats['accepted_connections'], 1)
        self.assertGreaterEqual(stats['qwp1_frames'], 3)
        self.assertEqual(stats['binary_frames'], stats['qwp1_frames'])
        self.assertGreater(stats['binary_bytes'], 0)

    @unittest.skipIf(pd is None, 'pandas not installed')
    def test_client_dataframe_rejects_timestamp_only_before_publication(self):
        df = pd.DataFrame({
            'ts': pd.Series([
                pd.Timestamp('2024-01-01 00:00:00'),
                pd.Timestamp('2024-01-01 00:00:01')],
                dtype='datetime64[ns]'),
        })

        with QwpAckServer() as server:
            conf = (
                f'ws::addr=127.0.0.1:{server.port};lazy_connect=true;'
                'sender_pool_min=1;'
                'sender_pool_max=1;'
                'pool_reap=manual;')
            client = qi.QuestDB.from_conf(conf)
            try:
                with self.assertRaises(qi.UnsupportedDataFrameShapeError) as cm:
                    client.dataframe(df, table_name='trades', at='ts')
            finally:
                client.close()

            stats = server.snapshot()

        self.assertEqual(
            cm.exception.column_failures,
            ({'column': None,
              'target': None,
              'source_code': None,
              'reason': 'v1 requires at least one non-timestamp data column.'},))
        self.assertEqual(stats['errors'], [])
        self.assertEqual(stats['binary_frames'], 0)
        self.assertEqual(stats['qwp1_frames'], 0)

    @unittest.skipIf(pyarrow is None, 'pyarrow not installed')
    def test_client_dataframe_capsule_proactive_sync(self):
        n = 140
        table = pyarrow.table({
            'v': pyarrow.array(list(range(n)), type=pyarrow.int64()),
            'ts': pyarrow.array(
                [i * 1_000_000 for i in range(n)],
                type=pyarrow.timestamp('us')),
        })

        with QwpAckServer() as server:
            conf = (
                f'ws::addr=127.0.0.1:{server.port};lazy_connect=true;'
                'sender_pool_min=1;'
                'sender_pool_max=1;'
                'pool_reap=manual;')
            qi._debug_dataframe_columnar_io_stats(enabled=True, reset=True)
            try:
                client = qi.QuestDB.from_conf(conf)
                try:
                    client.dataframe(
                        table, table_name='trades', at='ts',
                        max_rows_per_batch=1)
                finally:
                    client.close()
                stats = qi._debug_dataframe_columnar_io_stats()
            finally:
                qi._debug_dataframe_columnar_io_stats(enabled=False, reset=True)

            snap = server.snapshot()

        self.assertEqual(snap['errors'], [])
        self.assertGreaterEqual(stats['flush_calls'], n)
        self.assertGreaterEqual(stats['sync_calls'], 2)
        self.assertGreaterEqual(snap['binary_frames'], n)

    @unittest.skipIf(pd is None, 'pandas not installed')
    def test_client_close_waits_for_active_dataframe(self):
        df = pd.DataFrame({
            'ts': pd.Series([
                pd.Timestamp('2024-01-01 00:00:00'),
                pd.Timestamp('2024-01-01 00:00:01')],
                dtype='datetime64[ns]'),
            'seq': pd.Series([1, 2], dtype='int64'),
        })

        with QwpAckServer(ack_delay_s=0.2) as server:
            conf = (
                f'ws::addr=127.0.0.1:{server.port};lazy_connect=true;'
                'sender_pool_min=1;'
                'sender_pool_max=1;'
                'pool_reap=manual;')
            client = qi.QuestDB.from_conf(conf)
            errors = []

            def ingest():
                try:
                    client.dataframe(df, table_name='trades', at='ts')
                except Exception as exc:
                    errors.append(exc)

            thread = threading.Thread(target=ingest)
            thread.start()
            deadline = time.monotonic() + 2
            while (server.snapshot()['binary_frames'] == 0 and
                   time.monotonic() < deadline):
                time.sleep(0.01)
            self.assertGreater(server.snapshot()['binary_frames'], 0)
            self.assertTrue(thread.is_alive())

            close_started = time.monotonic()
            client.close()
            close_elapsed = time.monotonic() - close_started
            thread.join(timeout=1)

        self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        self.assertGreater(close_elapsed, 0.05)
        with self.assertRaisesRegex(
                qi.QuestDBError,
                "reap_idle\\(\\) can't be called: QuestDB is closed"):
            client.reap_idle()

    @unittest.skipIf(pd is None or pyarrow is None,
                     'pandas + pyarrow not installed')
    def test_client_dataframe_late_flush_error_is_terminal(self):
        labels = ['a'] * 64000
        labels.append('x' * 1_200_000)
        df = pd.DataFrame({
            'ts': pd.date_range(
                '2024-01-01 00:00:00',
                periods=len(labels),
                freq='ns'),
            'label': pd.Series(
                pyarrow.array(labels, type=pyarrow.string()),
                dtype='string[pyarrow]'),
        })

        with QwpAckServer() as server:
            conf = (
                f'ws::addr=127.0.0.1:{server.port};lazy_connect=true;'
                'sender_pool_min=1;'
                'sender_pool_max=1;'
                'pool_reap=manual;'
                'max_buf_size=1000000;')
            client = qi.QuestDB.from_conf(conf)
            try:
                with self.assertRaises(qi.QuestDBError) as ctx:
                    client.dataframe(df, table_name='trades', at='ts')
            finally:
                client.close()

            stats = server.snapshot()

        # The 1.2 MB single row is irreducible: the column sender splits the
        # oversized final chunk down to one row and still cannot fit it under
        # the 1 MB cap, so it surfaces batch_too_large (asserted by code, not
        # message text, to stay robust to wording changes). The failed call
        # drops its connection, discarding the already-pipelined uncommitted
        # chunks instead of committing them — how many frames reached the
        # socket before the drop is a race, so no frame-count assertion here;
        # the landed-rows contract is pinned by system_test's
        # test_failed_dataframe_call_leaves_only_the_eager_first_batch.
        self.assertEqual(ctx.exception.code, qi.QuestDBErrorCode.BatchTooLarge)
        self.assertEqual(stats['errors'], [])

    @unittest.skipIf(pd is None, 'pandas not installed')
    @unittest.skipIf(pyarrow is None, 'pyarrow not installed')
    def test_real_benchmark_paths_use_qwp_websocket_ack_flow(self):
        from benchmark_pandas_columnar import (
            make_numeric_core,
            run_real_client_path,
            run_real_row_path)

        df = make_numeric_core(2)

        with QwpAckServer() as server:
            conf = (
                f'ws::addr=127.0.0.1:{server.port};lazy_connect=true;'
                'sender_pool_min=1;'
                'sender_pool_max=1;'
                'pool_reap=manual;')
            _samples, _cpu_samples, last = run_real_client_path(
                df,
                2,
                1,
                1,
                conf=conf,
                table_name='trades')
            client_stats = server.snapshot()

        self.assertEqual(last['path'], 'real-client')
        self.assertNotIn('manual_chunk_plan', last)
        self.assertNotIn('manual_chunk_plan_error', last)
        self.assertEqual(last['rows_ingested'], 2)
        self.assertFalse(last['columnar_io_stats']['enabled'])
        self.assertGreaterEqual(last['columnar_io_stats']['flush_calls'], 1)
        self.assertEqual(last['columnar_io_stats']['sync_calls'], 1)
        self.assertGreaterEqual(last['columnar_io_stats']['flush_s'], 0.0)
        self.assertGreaterEqual(last['columnar_io_stats']['sync_s'], 0.0)
        self.assertEqual(client_stats['errors'], [])
        self.assertEqual(client_stats['accepted_connections'], 1)
        self.assertGreaterEqual(client_stats['qwp1_frames'], 2)

        with QwpAckServer() as server:
            conf = (
                f'ws::addr=127.0.0.1:{server.port};lazy_connect=true;'
                'sender_pool_min=1;'
                'sender_pool_max=1;'
                'pool_reap=manual;')
            _samples, _cpu_samples, last = run_real_row_path(
                df,
                2,
                1,
                1,
                conf=conf,
                table_name='trades',
                await_ack_ms=5000)
            row_stats = server.snapshot()

        self.assertEqual(last['path'], 'real-row')
        self.assertTrue(last['acked'])
        self.assertEqual(last['rows_ingested'], 2)
        self.assertNotIn('sender_pool_min', last['conf'])
        self.assertNotIn('sender_pool_max', last['conf'])
        self.assertNotIn('pool_reap', last['conf'])
        self.assertEqual(row_stats['errors'], [])
        self.assertEqual(row_stats['accepted_connections'], 1)
        self.assertGreaterEqual(row_stats['qwp1_frames'], 1)

    @unittest.skipIf(pd is None, 'pandas not installed')
    def test_benchmark_schema_sql_report_uses_schema_table(self):
        from benchmark_pandas_columnar import schema_sql_report

        report = schema_sql_report('numeric-core')

        self.assertEqual(report['schema'], 'numeric-core')
        self.assertEqual(report['table_name'], 'bench_numeric_core')
        self.assertEqual(
            report['drop_sql'],
            'DROP TABLE IF EXISTS bench_numeric_core')
        self.assertIn(
            'CREATE TABLE bench_numeric_core',
            report['create_sql'])
        self.assertIn('seq LONG', report['create_sql'])
        self.assertIn('ts TIMESTAMP', report['create_sql'])
        self.assertEqual(
            report['truncate_sql'],
            'TRUNCATE TABLE bench_numeric_core')

    def test_from_conf_preserves_qwpws_progress(self):
        sender = qi.Sender.from_conf(
            'ws::addr=localhost:9000;lazy_connect=true;qwp_ws_progress=manual;')
        try:
            with self.assertRaisesRegex(
                    qi.QuestDBError,
                    r'drive_once\(\) can\'t be called: Sender is closed'):
                sender.drive_once()
        finally:
            sender.close(False)

    def test_published_fsn_rejects_when_not_connected(self):
        sender = qi.Sender.from_conf('ws::addr=localhost:9000;lazy_connect=true;')
        try:
            with self.assertRaisesRegex(
                    qi.QuestDBError,
                    r'published_fsn\(\) can\'t be called: Sender is closed'):
                sender.published_fsn()
        finally:
            sender.close(False)

    def test_from_conf_preserves_c_only_qwpws_keys(self):
        with self.assertRaisesRegex(
                qi.QuestDBError,
                'invalid sf_max_segment_bytes'):
            qi.Sender.from_conf('ws::addr=localhost:9000;lazy_connect=true;sf_max_segment_bytes=64mi;')

    def test_from_conf_accepts_wss_tls_roots_password(self):
        with tempfile.NamedTemporaryFile() as roots:
            sender = qi.Sender.from_conf(
                'wss::addr=localhost:9000;'
                f'tls_roots={roots.name};'
                'tls_roots_password=secret;')
            try:
                self.assertIsInstance(sender, qi.Sender)
            finally:
                sender.close(False)

    def test_tls_roots_password_rejects_non_qwp_websocket(self):
        with tempfile.NamedTemporaryFile() as roots:
            with self.assertRaisesRegex(
                    qi.QuestDBError,
                    'only supported for QWP/WebSocket'):
                qi.Sender.from_conf(
                    'tcps::addr=localhost:9009;'
                    f'tls_roots={roots.name};'
                    'tls_roots_password=secret;')

    def test_from_conf_preserves_http_retry_max_backoff(self):
        with self.assertRaisesRegex(
                qi.QuestDBError,
                'retry_max_backoff_millis.*at least 10'):
            qi.Sender.from_conf(
                'http::addr=localhost:9000;retry_max_backoff_millis=3;')

    def test_retry_max_backoff_rejects_non_http_protocol(self):
        with self.assertRaisesRegex(
                qi.QuestDBError,
                'retry_max_backoff_millis is supported only in ILP over HTTP'):
            qi.Sender(
                qi.Protocol.Tcp,
                '127.0.0.1',
                9009,
                retry_max_backoff=250)

    def test_duration_options_reject_bool(self):
        cases = {
            'auth_timeout': '"auth_timeout" must be an int or a timedelta',
            'retry_timeout': '"retry_timeout" must be an int or a timedelta',
            'retry_max_backoff': (
                '"retry_max_backoff" must be an int or a timedelta'),
            'request_timeout': (
                '"request_timeout" must be an int or a timedelta'),
        }
        for option, message in cases.items():
            for value in (False, True):
                with self.subTest(option=option, value=value):
                    with self.assertRaisesRegex(TypeError, message):
                        qi.Sender(
                            qi.Protocol.Http,
                            '127.0.0.1',
                            9000,
                            **{option: value})

    def test_duration_options_reject_negative_timedelta(self):
        options = (
            'auth_timeout',
            'retry_timeout',
            'retry_max_backoff',
            'request_timeout',
            'auto_flush_interval',
        )
        for option in options:
            with self.subTest(option=option):
                with self.assertRaisesRegex(
                        ValueError, 'Negative timedelta not allowed'):
                    qi.Sender(
                        qi.Protocol.Http,
                        '127.0.0.1',
                        9000,
                        **{option: datetime.timedelta(milliseconds=-5)})

    def test_sub_millisecond_duration_is_not_truncated_to_zero(self):
        sender = qi.Sender(
            qi.Protocol.Http,
            '127.0.0.1',
            9000,
            auth_timeout=datetime.timedelta(microseconds=500))
        sender.close(False)

    def test_from_conf_preserves_escaped_semicolon_in_c_only_qwpws_key(self):
        sender = qi.Sender.from_conf(
            'ws::addr=localhost:9000;lazy_connect=true;sf_dir=/tmp/qdb;;sf;')
        try:
            self.assertIsInstance(sender, qi.Sender)
        finally:
            sender.close(False)

    def test_qwpws_progress_rejects_non_websocket_protocol(self):
        with self.assertRaisesRegex(
                qi.QuestDBError,
                'only supported for QWP/WebSocket'):
            qi.Sender(
                qi.Protocol.Udp,
                '127.0.0.1',
                9009,
                qwp_ws_progress=qi.QwpWsProgress.Manual)

    def test_qwpws_progress_rejects_invalid_value(self):
        with self.assertRaisesRegex(
                qi.QuestDBError, '"qwp_ws_progress" has invalid value'):
            qi.Sender(
                qi.Protocol.Ws, '127.0.0.1', 9000,
                qwp_ws_progress='bogus')

    def test_max_datagram_size_bounds(self):
        for bad in (0, -1, 65508):
            with self.assertRaisesRegex(
                    ValueError,
                    '"max_datagram_size" must be an int between 1 and 65507'):
                qi.Sender(qi.Protocol.Udp, '127.0.0.1', 9009,
                          max_datagram_size=bad)
        with self.assertRaisesRegex(
                TypeError, '"max_datagram_size" must be a positive int'):
            qi.Sender(qi.Protocol.Udp, '127.0.0.1', 9009,
                      max_datagram_size=True)

    def test_multicast_ttl_bounds(self):
        for bad in (-1, 256):
            with self.assertRaisesRegex(
                    ValueError, '"multicast_ttl" must be an int'):
                qi.Sender(qi.Protocol.Udp, '127.0.0.1', 9009,
                          multicast_ttl=bad)
        with self.assertRaisesRegex(
                TypeError, '"multicast_ttl" must be an int'):
            qi.Sender(qi.Protocol.Udp, '127.0.0.1', 9009,
                      multicast_ttl=True)

    def test_qwpws_error_handler_can_be_registered(self):
        sender = qi.Sender(
            qi.Protocol.Ws,
            '127.0.0.1',
            9000,
            error_handler=lambda error: None)
        try:
            self.assertIsInstance(sender, qi.Sender)
        finally:
            sender.close(False)

    def test_qwpws_error_handler_rejects_non_websocket_protocol(self):
        with self.assertRaisesRegex(
                qi.QuestDBError,
                'only supported for QWP/WebSocket'):
            qi.Sender(
                qi.Protocol.Udp,
                '127.0.0.1',
                9009,
                error_handler=lambda error: None)

    def test_qwpws_fsn_helpers_reject_non_websocket_sender_even_when_empty(self):
        sender = qi.Sender(
            qi.Protocol.Udp,
            '127.0.0.1',
            9009)
        try:
            sender.establish()
            with self.assertRaisesRegex(
                    qi.QuestDBError,
                    'only supported for QWP/WebSocket'):
                sender.flush_and_get_fsn()
            with self.assertRaisesRegex(
                    qi.QuestDBError,
                    'only supported for QWP/WebSocket'):
                sender.flush_and_keep_and_get_fsn()
        finally:
            sender.close(False)

    def test_qwpws_progress_conf_override_conflict(self):
        with self.assertRaisesRegex(ValueError, '"qwp_ws_progress" is already present'):
            qi.Sender.from_conf(
                'ws::addr=localhost:9000;lazy_connect=true;qwp_ws_progress=manual;',
                qwp_ws_progress=qi.QwpWsProgress.Background)

    def test_dataframe_schema_overrides_rejects_non_websocket_protocol(self):
        sender = qi.Sender(qi.Protocol.Tcp, '127.0.0.1', 9009)
        try:
            with self.assertRaisesRegex(
                    qi.QuestDBError,
                    'schema_overrides is only supported over QWP/WebSocket'):
                sender.dataframe(
                    object(),
                    table_name='t',
                    at=qi.ServerTimestamp,
                    schema_overrides={'x': 'symbol'})
        finally:
            sender.close(False)

    def test_qwpws_flush_and_keep_and_get_fsn_happy_path(self):
        with QwpAckServer() as server:
            with qi.Sender.from_conf(
                    f'ws::addr=127.0.0.1:{server.port};lazy_connect=true;',
                    auto_flush=False) as sender:
                buf = sender.new_buffer()
                buf.row(
                    't', columns={'v': 1},
                    at=qi.TimestampNanos(1_700_000_000_000_000_000))
                size_before = len(buf)
                self.assertGreater(size_before, 0)
                fsn = sender.flush_and_keep_and_get_fsn(buf)
                self.assertIsNotNone(fsn)
                self.assertEqual(len(buf), size_before)
                self.assertTrue(sender.await_acked_fsn(fsn, 10000))
                self.assertEqual(sender.published_fsn(), fsn)
                self.assertGreaterEqual(sender.acked_fsn(), fsn)
            stats = server.snapshot()
        self.assertEqual(stats['errors'], [])
        self.assertGreaterEqual(stats['qwp1_frames'], 1)

    def test_qwpws_drive_once_manual_progress_happy_path(self):
        with QwpAckServer() as server:
            sender = qi.Sender.from_conf(
                f'ws::addr=127.0.0.1:{server.port};lazy_connect=true;'
                'qwp_ws_progress=manual;',
                auto_flush=False)
            try:
                sender.establish()
                buf = sender.new_buffer()
                buf.row(
                    't', columns={'v': 1},
                    at=qi.TimestampNanos(1_700_000_000_000_000_000))
                fsn = sender.flush_and_get_fsn(buf)
                self.assertIsNotNone(fsn)
                deadline = time.monotonic() + 10.0
                acked = sender.acked_fsn()
                while ((acked is None or acked < fsn)
                        and time.monotonic() < deadline):
                    sender.drive_once()
                    acked = sender.acked_fsn()
                self.assertIsNotNone(acked)
                self.assertGreaterEqual(acked, fsn)
            finally:
                sender.close(False)
            stats = server.snapshot()
        self.assertEqual(stats['errors'], [])
        self.assertGreaterEqual(stats['qwp1_frames'], 1)


class TestQwpWebSocketTls(unittest.TestCase):
    """wss:: against a TLS-wrapped mock server, using the self-signed
    certificate under test/certs as its own trust root."""

    TLS_CA = TLS_CA

    def _pool_keys(self):
        return ('sender_pool_min=1;'
                'sender_pool_max=1;'
                'pool_reap=manual;'
                'lazy_connect=true;')

    def _publish_one_row(self, client):
        with client.sender() as sender:
            sender.row(
                'events', columns={'value': 1}, at=qi.ServerTimestamp)
            sender.flush(wait=True)

    def test_wss_with_pinned_root_publishes(self):
        with QwpAckServer(tls=True) as server:
            conf = (
                f'wss::addr=127.0.0.1:{server.port};'
                f'tls_roots={self.TLS_CA};'
                + self._pool_keys())
            with qi.QuestDB.from_conf(conf) as client:
                self._publish_one_row(client)
                self.assertEqual(server.wait_binary_frames_settled(), 1)
            stats = server.snapshot()
        self.assertEqual(stats['errors'], [])
        self.assertEqual(stats['tls_handshake_failures'], 0)

    def test_connect_tls_keyword_form_publishes(self):
        import questdb
        with QwpAckServer(tls=True) as server:
            with questdb.connect(
                    host='127.0.0.1',
                    port=server.port,
                    tls=True,
                    tls_roots=str(self.TLS_CA),
                    lazy_connect=True,
                    sender_pool_min=1,
                    sender_pool_max=1,
                    pool_reap='manual') as db:
                self._publish_one_row(db)
                self.assertEqual(server.wait_binary_frames_settled(), 1)
            stats = server.snapshot()
        self.assertEqual(stats['errors'], [])

    def test_wss_rejects_untrusted_certificate(self):
        with QwpAckServer(tls=True) as server:
            conf = (
                f'wss::addr=127.0.0.1:{server.port};'
                'reconnect_max_duration_millis=1000;'
                'close_flush_timeout_millis=0;'
                + self._pool_keys())
            with qi.QuestDB.from_conf(conf) as client:
                with self.assertRaises(qi.QuestDBError):
                    with client.sender() as sender:
                        sender.row(
                            'events', columns={'value': 1},
                            at=qi.ServerTimestamp)
                        sender.flush()
                        sender.wait(timeout_millis=2000)
            deadline = time.monotonic() + 10
            while server.tls_handshake_failures < 1:
                self.assertLess(
                    time.monotonic(), deadline,
                    'the server must observe a failed TLS handshake')
                time.sleep(0.01)
            stats = server.snapshot()
        self.assertEqual(stats['binary_frames'], 0)

    def test_wss_unsafe_off_skips_verification(self):
        with QwpAckServer(tls=True) as server:
            conf = (
                f'wss::addr=127.0.0.1:{server.port};'
                'tls_verify=unsafe_off;'
                + self._pool_keys())
            client = qi.QuestDB.from_conf(conf)
            with client:
                self._publish_one_row(client)
                self.assertEqual(server.wait_binary_frames_settled(), 1)
            stats = server.snapshot()
        self.assertEqual(stats['errors'], [])


class TestQwpOnlyRowTypes(unittest.TestCase):
    UUID_VALUE = uuid.UUID('123e4567-e89b-12d3-a456-426614174000')

    def test_wrappers_exported(self):
        import questdb
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', DeprecationWarning)
            import questdb.ingress as ingress
        for name in ('Char', 'DateMillis', 'Long256', 'Geohash'):
            self.assertIn(name, questdb.__all__)
            self.assertIn(name, ingress.__all__)
            self.assertIs(getattr(questdb, name), getattr(qi, name))
            self.assertIs(getattr(ingress, name), getattr(qi, name))

    def test_wrapper_validation(self):
        for value in ('', 'ab'):
            with self.subTest(char=value), self.assertRaises(ValueError):
                qi.Char(value)
        with self.assertRaisesRegex(ValueError, 'surrogate pair.*cannot fit CHAR'):
            qi.Char('😀')
        with self.assertRaises(TypeError):
            qi.Char(1)
        self.assertEqual(qi.Char('\x00').value, '\x00')
        self.assertEqual(qi.Char('\ud800').value, '\ud800')

        for value in (-1, 2 ** 256):
            with self.subTest(long256=value), self.assertRaises(ValueError):
                qi.Long256(value)
        for value in (False, True):
            with self.subTest(long256_bool=value), self.assertRaises(TypeError):
                qi.Long256(value)
        self.assertEqual(qi.Long256(0).value, 0)
        self.assertEqual(qi.Long256(2 ** 256 - 1).value, 2 ** 256 - 1)

        for value in (False, True):
            with self.subTest(geohash_bits_bool=value), self.assertRaises(TypeError):
                qi.Geohash(value, 1)
            with self.subTest(geohash_precision_bool=value), self.assertRaises(TypeError):
                qi.Geohash(0, value)
        for precision in (0, 61):
            with self.subTest(precision=precision), self.assertRaises(ValueError):
                qi.Geohash(0, precision)
        for precision in (1, 5, 60):
            with self.subTest(excess_bits=precision), self.assertRaises(ValueError):
                qi.Geohash(2 ** precision, precision)
        for value in ('', 'x' * 13, 'a', 'i', 'l', 'o', 'ß', 'ſ', 'K'):
            with self.subTest(geohash=value), self.assertRaises(ValueError):
                qi.Geohash.from_string(value)
        upper = qi.Geohash.from_string('U33D8')
        lower = qi.Geohash.from_string('u33d8')
        self.assertEqual((upper.bits, upper.precision),
                         (lower.bits, lower.precision))
        self.assertEqual(upper.precision, 25)

        for value in (False, True):
            with self.subTest(date_millis_bool=value), self.assertRaises(TypeError):
                qi.DateMillis(value)
        self.assertEqual(qi.DateMillis(-1).value, -1)
        utc = datetime.timezone.utc
        epoch = datetime.datetime(1970, 1, 1, tzinfo=utc)
        self.assertEqual(
            qi.DateMillis.from_datetime(
                epoch + datetime.timedelta(microseconds=1999)).value,
            1)
        self.assertEqual(
            qi.DateMillis.from_datetime(
                epoch - datetime.timedelta(microseconds=1)).value,
            -1)
        qi._NAIVE_DATETIME_WARNED = False
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always')
            naive = datetime.datetime(1969, 12, 31, 23, 59, 59, 999999)
            self.assertEqual(qi.DateMillis.from_datetime(naive).value, -1)
        self.assertEqual(
            len([w for w in caught if issubclass(w.category, UserWarning)]),
            1)

    def test_binary_memoryview_validation_and_ipv6_error(self):
        buffer = qi.Buffer._new_qwp()
        for i, value in enumerate((b'', bytearray(b'x'), memoryview(b'yz'))):
            buffer.row(
                'binary_values', columns={'value': value},
                at=qi.TimestampNanos(i + 1))

        before = len(buffer)
        for value in (
                memoryview(b'abcd')[::2],
                memoryview(np.array([1, 2], dtype=np.uint16))):
            with self.subTest(value=value), self.assertRaisesRegex(
                    ValueError, 'C-contiguous.*one-byte'):
                buffer.row(
                    'binary_values', columns={'value': value},
                    at=qi.TimestampNanos(10))
            self.assertEqual(len(buffer), before)

        with self.assertRaisesRegex(
                TypeError, 'IPv6 is not supported.*no IPv6 column type'):
            buffer.row(
                'binary_values',
                columns={'value': ipaddress.IPv6Address('::1')},
                at=qi.TimestampNanos(10))
        self.assertEqual(len(buffer), before)

        with self.assertRaisesRegex(
                TypeError, 'Unsupported type: ipaddress.IPv4Interface'):
            buffer.row(
                'binary_values',
                columns={'value': ipaddress.IPv4Interface('192.0.2.1/24')},
                at=qi.TimestampNanos(10))
        self.assertEqual(len(buffer), before)

    @unittest.skipIf(pd is None, 'pandas not installed')
    @unittest.skipIf(pyarrow is None, 'pyarrow not installed')
    def test_dataframe_binary_accepts_row_binary_types(self):
        values = [
            None,
            b'',
            b'bytes-value',
            bytearray(b'bytearray-value'),
            memoryview(b'memoryview-value'),
        ]
        for value in values[1:]:
            with self.subTest(first_non_null=type(value).__name__):
                frame = pd.DataFrame({
                    'value': pd.Series([None, value], dtype=object)})
                plan = qi._debug_dataframe_columnar_plan(
                    frame, table_name='binary_values',
                    at=qi.ServerTimestamp)
                self.assertTrue(plan['supported'], plan['failures'])

        with QwpAckServer(record_payloads=True) as server:
            conf = (
                f'ws::addr=127.0.0.1:{server.port};lazy_connect=true;'
                'sender_pool_min=1;sender_pool_max=1;pool_reap=manual;')
            frame = pd.DataFrame({
                'value': pd.Series(values, dtype=object)})
            with qi.QuestDB.from_conf(conf) as client:
                client.dataframe(
                    frame, table_name='binary_values',
                    at=qi.ServerTimestamp)
            stats = server.snapshot()

        self.assertEqual(stats['errors'], [])
        self.assertGreaterEqual(stats['qwp1_frames'], 1)
        for value in values[2:]:
            self.assertTrue(any(
                bytes(value) in payload
                for payload in stats['binary_payloads']))

        payload = next(
            payload for payload in stats['binary_payloads']
            if int.from_bytes(payload[6:8], 'little') > 0)
        pos = 12
        _delta_start, pos = _read_qwp_varint(payload, pos)
        delta_count, pos = _read_qwp_varint(payload, pos)
        for _ in range(delta_count):
            entry_len, pos = _read_qwp_varint(payload, pos)
            pos += entry_len
        table_len, pos = _read_qwp_varint(payload, pos)
        pos += table_len
        row_count, pos = _read_qwp_varint(payload, pos)
        column_count, pos = _read_qwp_varint(payload, pos)
        self.assertEqual((row_count, column_count), (len(values), 1))
        column_name_len, pos = _read_qwp_varint(payload, pos)
        pos += column_name_len
        self.assertEqual(payload[pos], 0x17)  # QWP BINARY
        pos += 1

        self.assertEqual(payload[pos], 1)  # nullable column
        pos += 1
        self.assertEqual(payload[pos], 0b00000001)  # only row 0 is NULL
        pos += 1
        encoded_values = [bytes(value) for value in values[1:]]
        expected_offsets = [0]
        for value in encoded_values:
            expected_offsets.append(expected_offsets[-1] + len(value))
        actual_offsets = [
            int.from_bytes(payload[pos + 4 * i:pos + 4 * (i + 1)], 'little')
            for i in range(len(expected_offsets))]
        self.assertEqual(actual_offsets, expected_offsets)
        pos += 4 * len(expected_offsets)
        self.assertEqual(payload[pos:], b''.join(encoded_values))

    @unittest.skipIf(pd is None, 'pandas not installed')
    @unittest.skipIf(pyarrow is None, 'pyarrow not installed')
    def test_dataframe_binary_rejects_invalid_memoryviews_and_buffers(self):
        invalid_values = (
            memoryview(b'abcd')[::2],
            memoryview(np.array([1, 2], dtype=np.uint16)),
        )
        with QwpAckServer(record_payloads=True) as server:
            conf = (
                f'ws::addr=127.0.0.1:{server.port};lazy_connect=true;'
                'sender_pool_min=1;sender_pool_max=1;pool_reap=manual;')
            with qi.QuestDB.from_conf(conf) as client:
                for value in invalid_values:
                    frame = pd.DataFrame({
                        'value': pd.Series([b'ok', value], dtype=object)})
                    with self.subTest(value=value), self.assertRaisesRegex(
                            qi.QuestDBError,
                            "Bad column 'value' at row 1: memoryview BINARY "
                            'values must be C-contiguous with one-byte items'):
                        client.dataframe(
                            frame, table_name='binary_values',
                            at=qi.ServerTimestamp)

                generic_buffer = array.array('B', [1, 2])
                frame = pd.DataFrame({
                    'value': pd.Series([generic_buffer], dtype=object)})
                with self.assertRaisesRegex(
                        qi.QuestDBError,
                        'Unsupported object column.*array.array'):
                    client.dataframe(
                        frame, table_name='binary_values',
                        at=qi.ServerTimestamp)
            stats = server.snapshot()

        self.assertEqual(stats['binary_frames'], 0)
        self.assertEqual(stats['errors'], [])

    def _assert_ilp_rejections(self, sender):
        values = (
            ('UUID', self.UUID_VALUE),
            ('IPV4', ipaddress.IPv4Address('192.0.2.1')),
            ('BINARY', b'abc'),
            ('CHAR', qi.Char('Q')),
            ('DATE', qi.DateMillis(-1)),
            ('LONG256', qi.Long256(42)),
            ('GEOHASH', qi.Geohash.from_string('u33d8')),
        )
        for type_name, value in values:
            with self.subTest(type_name=type_name):
                buffer = sender.new_buffer()
                before = len(buffer)
                with self.assertRaises(qi.QuestDBError) as cm:
                    buffer.row(
                        'wrong_transport', columns={'value': value},
                        at=qi.ServerTimestamp)
                self.assertEqual(cm.exception.code,
                                 qi.QuestDBErrorCode.InvalidApiCall)
                message = str(cm.exception)
                self.assertIn(type_name, message)
                for protocol in ("'udp'", "'ws'", "'wss'"):
                    self.assertIn(protocol, message)
                self.assertEqual(len(buffer), before)
                buffer.row(
                    'wrong_transport', columns={'value': 1},
                    at=qi.ServerTimestamp)
                self.assertGreater(len(buffer), before)

    def test_ilp_http_and_tcp_reject_qwp_only_types_atomically(self):
        with HttpServer() as server, qi.Sender(
                qi.Protocol.Http, '127.0.0.1', server.port,
                auto_flush=False) as sender:
            self._assert_ilp_rejections(sender)

        with Server() as server, qi.Sender(
                qi.Protocol.Tcp, '127.0.0.1', server.port,
                auto_flush=False) as sender:
            server.accept()
            self._assert_ilp_rejections(sender)

    def test_transaction_rejects_uuid_and_preserves_buffer(self):
        with HttpServer() as server, qi.Sender(
                qi.Protocol.Http, '127.0.0.1', server.port,
                auto_flush=False) as sender:
            with sender.transaction('transaction_types') as txn:
                before = bytes(sender)
                with self.assertRaisesRegex(
                        qi.QuestDBError, 'UUID columns require a QWP sender'):
                    txn.row(
                        columns={'value': self.UUID_VALUE},
                        at=qi.ServerTimestamp)
                self.assertEqual(bytes(sender), before)
                txn.row(columns={'value': 1}, at=qi.ServerTimestamp)
                self.assertGreater(len(sender), len(before))
                txn.rollback()

    def test_qwp_websocket_accepts_all_row_types(self):
        with QwpAckServer(record_payloads=True) as server:
            with qi.Sender.from_conf(
                    f'ws::addr=127.0.0.1:{server.port};lazy_connect=true;',
                    auto_flush=False) as sender:
                sender.row(
                    'qwp_row_types',
                    columns={
                        'uuid_col': self.UUID_VALUE,
                        'ipv4_col': ipaddress.IPv4Address('192.0.2.1'),
                        'binary_col': b'',
                        'char_col': qi.Char('Q'),
                        'date_col': qi.DateMillis(-1),
                        'long256_col': qi.Long256(2 ** 255 + 7),
                        'geohash_col': qi.Geohash.from_string('u33d8'),
                    },
                    at=qi.TimestampNanos(1_700_000_000_000_000_000))
                fsn = sender.flush_and_get_fsn()
                self.assertTrue(sender.await_acked_fsn(fsn, 10000))
            stats = server.snapshot()
        self.assertEqual(stats['errors'], [])
        self.assertEqual(stats['qwp1_frames'], 1)

    @unittest.skipIf(pd is None, 'pandas not installed')
    @unittest.skipIf(pyarrow is None, 'pyarrow not installed')
    def test_row_uuid_wire_bytes_match_dataframe_object_column(self):
        expected = self.UUID_VALUE.int.to_bytes(16, 'little')

        with QwpAckServer(record_payloads=True) as row_server:
            conf = (
                f'ws::addr=127.0.0.1:{row_server.port};lazy_connect=true;'
                'sender_pool_min=1;sender_pool_max=1;pool_reap=manual;')
            with qi.QuestDB.from_conf(conf) as client:
                with client.sender() as sender:
                    sender.row(
                        'uuid_wire', columns={'value': self.UUID_VALUE},
                        at=qi.ServerTimestamp)
                    sender.flush(wait=True)
            row_payload = next(
                payload
                for payload in row_server.snapshot()['binary_payloads']
                if int.from_bytes(payload[6:8], 'little') > 0)

        with QwpAckServer(record_payloads=True) as dataframe_server:
            conf = (
                f'ws::addr=127.0.0.1:{dataframe_server.port};'
                'lazy_connect=true;sender_pool_min=1;sender_pool_max=1;'
                'pool_reap=manual;')
            frame = pd.DataFrame({
                'value': pd.Series([self.UUID_VALUE], dtype=object)})
            with qi.QuestDB.from_conf(conf) as client:
                client.dataframe(
                    frame, table_name='uuid_wire', at=qi.ServerTimestamp)
            dataframe_payload = next(
                payload
                for payload in dataframe_server.snapshot()['binary_payloads']
                if int.from_bytes(payload[6:8], 'little') > 0)

        self.assertTrue(row_payload.endswith(b'\x00' + expected))
        self.assertEqual(row_payload, dataframe_payload)


if os.environ.get('TEST_QUESTDB_INTEGRATION') == '1':
    class TestQwpOnlyRowTypesIntegration(TestWithDatabase):
        def test_round_trip_sentinels_precisions_and_mixed_precision_error(self):
            self._require_qwp_ws()
            table_name = 'qwp_row_types_' + uuid.uuid4().hex[:8]
            mixed_table = 'qwp_mixed_gh_' + uuid.uuid4().hex[:8]
            normal_uuid = uuid.UUID('123e4567-e89b-12d3-a456-426614174000')
            null_uuid = uuid.UUID('80000000-0000-0000-8000-000000000000')
            null_long256 = sum(
                0x8000000000000000 << (64 * limb) for limb in range(4))
            self.qdb_plain.http_sql_query(
                f'CREATE TABLE {table_name} ('
                'u UUID, ip IPV4, bin BINARY, ch CHAR, dt DATE, l LONG256, '
                'gh1 GEOHASH(1b), gh5 GEOHASH(5b), gh7 GEOHASH(7b), '
                'gh8 GEOHASH(8b), gh60 GEOHASH(60b), ts TIMESTAMP) '
                'TIMESTAMP(ts) PARTITION BY DAY WAL')
            self.qdb_plain.http_sql_query(
                f'CREATE TABLE {mixed_table} ('
                'gh GEOHASH(5b), ts TIMESTAMP) '
                'TIMESTAMP(ts) PARTITION BY DAY WAL')

            with tempfile.TemporaryDirectory(prefix='py-qwp-row-types-') as sf_dir:
                conf = self._mk_qwpws_conf(
                    'py-row-types-' + uuid.uuid4().hex[:8], sf_dir,
                    reconnect_max_duration_millis=30000,
                    close_flush_timeout_millis=30000)
                sender = qi.Sender.from_conf(conf, auto_flush=False)
                try:
                    sender.establish()
                    sender.row(
                        table_name,
                        columns={
                            'u': normal_uuid,
                            'ip': ipaddress.IPv4Address('192.0.2.1'),
                            'bin': b'',
                            'ch': qi.Char('Q'),
                            'dt': qi.DateMillis(-1),
                            'l': qi.Long256(7),
                            'gh1': qi.Geohash(1, 1),
                            'gh5': qi.Geohash(31, 5),
                            'gh7': qi.Geohash(85, 7),
                            'gh8': qi.Geohash(170, 8),
                            'gh60': qi.Geohash((1 << 60) - 1, 60),
                        },
                        at=qi.TimestampMicros(1))
                    sender.row(
                        table_name,
                        columns={
                            'u': null_uuid,
                            'ip': ipaddress.IPv4Address('0.0.0.0'),
                            'bin': b'x',
                            'ch': qi.Char('\x00'),
                            'dt': qi.DateMillis(-(1 << 63)),
                            'l': qi.Long256(null_long256),
                            'gh1': qi.Geohash(0, 1),
                            'gh5': qi.Geohash(0, 5),
                            'gh7': qi.Geohash(0, 7),
                            'gh8': qi.Geohash(0, 8),
                            'gh60': qi.Geohash(0, 60),
                        },
                        at=qi.TimestampMicros(2))
                    fsn = sender.flush_and_get_fsn()
                    self.assertTrue(sender.await_acked_fsn(fsn, 30000))

                    # Mixed precisions within one batch are unrepresentable
                    # on the wire (one precision per column chunk), so the
                    # client pins the precision at the first value and
                    # rejects the mismatch at row() time.
                    sender.row(
                        mixed_table, columns={'gh': qi.Geohash(1, 1)},
                        at=qi.TimestampMicros(3))
                    with self.assertRaisesRegex(
                            qi.QuestDBError,
                            'precision mismatch within column'):
                        sender.row(
                            mixed_table, columns={'gh': qi.Geohash(1, 5)},
                            at=qi.TimestampMicros(4))
                finally:
                    sender.close(False)

                self.qdb_plain.retry_check_table(table_name, min_rows=2)
                with qi.QuestDB.from_conf(conf) as client:
                    rows = client.query(
                        f'SELECT u, ip, bin, ch, dt, l, gh1, gh5, gh7, '
                        f'gh8, gh60 FROM {table_name} ORDER BY ts').to_arrow().to_pylist()

            first, second = rows
            # Egress forwards UUID bytes verbatim in QuestDB's wire layout
            # (lo half LE, hi half LE) even under the `arrow.uuid` label --
            # the documented c-questdb-client family convention -- so decode
            # little-endian regardless of whether pyarrow surfaces the cell
            # as raw bytes or wraps it in a `uuid.UUID`.
            raw_u = (first['u'] if isinstance(first['u'], bytes)
                     else first['u'].bytes)
            self.assertEqual(int.from_bytes(raw_u, 'little'), normal_uuid.int)
            self.assertEqual(first['ip'], int(ipaddress.IPv4Address('192.0.2.1')))
            self.assertEqual(first['bin'], b'')
            self.assertEqual(first['ch'], ord('Q'))
            self.assertEqual(first['dt'].timestamp(), -0.001)
            self.assertEqual(int.from_bytes(first['l'], 'little'), 7)
            self.assertEqual(
                [first[name] for name in ('gh1', 'gh5', 'gh7', 'gh8', 'gh60')],
                [1, 31, 85, 170, (1 << 60) - 1])
            for name in ('u', 'ip', 'dt', 'l'):
                self.assertIsNone(second[name])
            # CHAR has no NULL in QuestDB: the '\x00' sentinel is stored as
            # code unit 0 (rendered '' in text output, 0 in Arrow egress).
            self.assertEqual(second['ch'], 0)
            self.assertEqual(second['bin'], b'x')
            self.assertEqual(
                [second[name] for name in ('gh1', 'gh5', 'gh7', 'gh8', 'gh60')],
                [0, 0, 0, 0, 0])


class TestBases:
    """
    Dummy class that's only used so that we can create subclasses of testcases.

    By nesting these base classes within another class, Python's `unittest` will
    not find them.

    The discoverable subclasses can drive extra parameters.
    """

    class TestBuffer(unittest.TestCase, TimestampEncodingMixin):
        def _test_buffer_row_ts(self, ts):
            buffer = qi.Buffer(protocol_version=self.version)
            buffer.row('trades', columns={'t': ts}, at=ts)
            ec = self.enc_ts
            ed = self.enc_des_ts
            exp = f'trades t={ec(ts)} {ed(ts)}\n'.encode()
            self.assertEqual(bytes(buffer), exp)

        def test_buffer_row_ts_micros(self):
            self._test_buffer_row_ts(qi.TimestampMicros(10001))

        def test_buffer_row_ts_nanos(self):
            self._test_buffer_row_ts(qi.TimestampNanos(10000333))

        def test_buffer_row_ts_datetime(self):
            self._test_buffer_row_ts(datetime.datetime.now())

        def test_buffer_row_at_disallows_none(self):
            with self.assertRaisesRegex(
                    qi.QuestDBError,
                    'must be of type TimestampNanos, datetime, or ServerTimestamp'):
                buffer = qi.Buffer(protocol_version=self.version)
                buffer.row('tbl1', symbols={'sym1': 'val1'}, at=None)
            with self.assertRaisesRegex(
                    TypeError,
                    'needs keyword-only argument at'):
                buffer = qi.Buffer(protocol_version=self.version)
                buffer.row('tbl1', symbols={'sym1': 'val1'})

        @unittest.skipIf(not pd, 'pandas not installed')
        def test_buffer_dataframe_at_disallows_none(self):
            with self.assertRaisesRegex(
                    qi.QuestDBError,
                    'must be of type TimestampNanos, datetime, or ServerTimestamp'):
                buffer = qi.Buffer(protocol_version=self.version)
                buffer.dataframe(pd.DataFrame(), at=None)
            with self.assertRaisesRegex(
                    TypeError,
                    'needs keyword-only argument at'):
                buffer = qi.Buffer(protocol_version=self.version)
                buffer.dataframe(pd.DataFrame())

        def test_new(self):
            buf = qi.Buffer(protocol_version=self.version)
            self.assertEqual(len(buf), 0)
            self.assertEqual(buf.capacity(), 64 * 1024)

        def test_basic(self):
            buf = qi.Buffer(protocol_version=self.version)
            buf.row('tbl1', symbols={'sym1': 'val1', 'sym2': 'val2'}, at=qi.ServerTimestamp)
            self.assertEqual(len(buf), 25)
            self.assertEqual(bytes(buf), b'tbl1,sym1=val1,sym2=val2\n')

        def test_bad_table(self):
            buf = qi.Buffer(protocol_version=self.version)
            with self.assertRaisesRegex(
                    qi.QuestDBError,
                    'Table names must have a non-zero length'):
                buf.row('', symbols={'sym1': 'val1'}, at=qi.ServerTimestamp)
            with self.assertRaisesRegex(
                    qi.QuestDBError,
                    'Bad string "x..y": Found invalid dot `.` at position 2.'):
                buf.row('x..y', symbols={'sym1': 'val1'}, at=qi.ServerTimestamp)

        def test_symbol(self):
            buf = qi.Buffer(protocol_version=self.version)
            buf.row('tbl1', symbols={'sym1': 'val1', 'sym2': 'val2'}, at=qi.ServerTimestamp)
            self.assertEqual(bytes(buf), b'tbl1,sym1=val1,sym2=val2\n')

        def test_bad_symbol_column_name(self):
            buf = qi.Buffer(protocol_version=self.version)
            with self.assertRaisesRegex(
                    qi.QuestDBError,
                    'Column names must have a non-zero length.'):
                buf.row('tbl1', symbols={'': 'val1'}, at=qi.ServerTimestamp)
            with self.assertRaisesRegex(
                    qi.QuestDBError,
                    'Bad string "sym.bol": '
                    'Column names can\'t contain a \'.\' character, '
                    'which was found at byte position 3.'):
                buf.row('tbl1', symbols={'sym.bol': 'val1'}, at=qi.ServerTimestamp)

        def test_column(self):
            two_h_after_epoch = datetime.datetime(
                1970, 1, 1, 2, tzinfo=datetime.timezone.utc)
            buf = qi.Buffer(protocol_version=self.version)
            buf.row('tbl1', columns={
                'col1': True,
                'col2': False,
                'col3': -1,
                'col4': 0.5,
                'col5': 'val',
                'col6': qi.TimestampMicros(12345),
                'col7': qi.TimestampNanos(12345678),
                'col8': two_h_after_epoch,
                'col9': None}, at=qi.ServerTimestamp)
            et = self.enc_ts_t
            en = self.enc_ts_n
            exp = (
                b'tbl1 col1=t,col2=f,col3=-1i,col4' + _float_binary_bytes(0.5, self.version == 1) +
                f',col5="val",col6={et(12345)},col7={en(12345678)},col8={et(7200000000)}\n'.encode())
            self.assertEqual(bytes(buf), exp)

        def test_none_symbol(self):
            buf = qi.Buffer(protocol_version=self.version)
            buf.row('tbl1', symbols={'sym1': 'val1', 'sym2': None}, at=qi.ServerTimestamp)
            exp = b'tbl1,sym1=val1\n'
            self.assertEqual(bytes(buf), exp)
            self.assertEqual(len(buf), len(exp))

            # No fields to write, no fields written, therefore a no-op.
            buf.row('tbl1', symbols={'sym1': None, 'sym2': None}, at=qi.ServerTimestamp)
            self.assertEqual(bytes(buf), exp)
            self.assertEqual(len(buf), len(exp))

        def test_none_column(self):
            buf = qi.Buffer(protocol_version=self.version)
            buf.row('tbl1', columns={'col1': 1}, at=qi.ServerTimestamp)
            exp = b'tbl1 col1=1i\n'
            self.assertEqual(bytes(buf), exp)
            self.assertEqual(len(buf), len(exp))

            # No fields to write, no fields written, therefore a no-op.
            buf.row('tbl1', columns={'col1': None, 'col2': None}, at=qi.ServerTimestamp)
            self.assertEqual(bytes(buf), exp)
            self.assertEqual(len(buf), len(exp))

        def test_no_symbol_or_col_args(self):
            buf = qi.Buffer(protocol_version=self.version)
            buf.row('table_name', at=qi.ServerTimestamp)
            self.assertEqual(bytes(buf), b'')

        def test_unicode(self):
            buf = qi.Buffer(protocol_version=self.version)
            buf.row(
                'tbl1',  # ASCII
                symbols={'questdb1': 'q❤️p'},  # Mixed ASCII and UCS-2
                columns={'questdb2': '❤️' * 1200},
                at=qi.ServerTimestamp)  # Over the 1024 buffer prealloc.
            buf.row(
                'tbl1',
                symbols={
                    'Questo è il nome di una colonna':  # Non-ASCII UCS-1
                        'Це символьне значення'},  # UCS-2, 2 bytes for UTF-8.
                columns={
                    'questdb1': '',  # Empty string
                    'questdb2': '嚜꓂',  # UCS-2, 3 bytes for UTF-8.
                    'questdb3': '💩🦞'},
                at=qi.ServerTimestamp)  # UCS-4, 4 bytes for UTF-8.
            self.assertEqual(bytes(buf),
                             (f'tbl1,questdb1=q❤️p questdb2="{"❤️" * 1200}"\n' +
                             'tbl1,Questo\\ è\\ il\\ nome\\ di\\ una\\ colonna=' +
                             'Це\\ символьне\\ значення ' +
                             'questdb1="",questdb2="嚜꓂",questdb3="💩🦞"\n').encode('utf-8'))

            buf.clear()
            buf.row('tbl1', symbols={'questdb1': 'q❤️p'}, at=qi.ServerTimestamp)
            self.assertEqual(bytes(buf), 'tbl1,questdb1=q❤️p\n'.encode('utf-8'))

            # A bad char in Python.
            with self.assertRaisesRegex(
                    qi.QuestDBError,
                    '.*codepoint 0xd800 in string .*'):
                buf.row('tbl1', symbols={'questdb1': 'a\ud800'}, at=qi.ServerTimestamp)

            # Strong exception safety: no partial writes.
            # Ensure we can continue using the buffer after an error.
            buf.row('tbl1', symbols={'questdb1': 'another line of input'}, at=qi.ServerTimestamp)
            self.assertEqual(
                bytes(buf),
                ('tbl1,questdb1=q❤️p\n' +
                # Note: No partially written failed line here.
                'tbl1,questdb1=another\\ line\\ of\\ input\n').encode('utf-8'))

        def test_float(self):
            buf = qi.Buffer(protocol_version=self.version)
            buf.row('tbl1', columns={'num': 1.2345678901234567}, at=qi.ServerTimestamp)
            self.assertEqual(bytes(buf), b'tbl1 num' + _float_binary_bytes(1.2345678901234567, self.version == 1) + b'\n')

        def test_array_basic(self):
            if self.version == 1:
                self.skipTest('Protocol version v1 doesn\'t support arrays')
            buf = qi.Buffer(protocol_version=self.version)
            arr = np.array([1.2345678901234567, 2.3456789012345678], dtype=np.float64)
            buf.row('tbl1', columns={'array': arr}, at=qi.ServerTimestamp)
            self.assertEqual(bytes(buf), b'tbl1 array=' + _array_binary_bytes(arr) + b'\n')

        def test_array_edge_cases(self):
            if self.version == 1:
                self.skipTest('Protocol version v1 doesn\'t support arrays')
            # empty array
            buf = qi.Buffer(protocol_version=self.version)
            empty_arr = np.array([], dtype=np.float64)
            buf.row('empty_table', columns={'col': empty_arr}, at=qi.ServerTimestamp)
            empty_expected = b'empty_table col=' + _array_binary_bytes(empty_arr) + b'\n'
            self.assertEqual(bytes(buf), empty_expected)

            # non contigious array
            base = np.arange(6, dtype=np.float64).reshape(2, 3)
            non_contig_arr = base[:, ::2]  # shape (2, 2), strides (24, 16)
            buf = qi.Buffer(protocol_version=self.version)
            buf.row('non_contig_table', columns={'col': non_contig_arr}, at=qi.ServerTimestamp)
            non_contig_expected = b'non_contig_table col=' + _array_binary_bytes(non_contig_arr) + b'\n'
            self.assertEqual(bytes(buf), non_contig_expected)

            # minus stride
            reversed_arr = np.array([1.1, 2.2, 3.3], dtype=np.float64)[::-1]  # strides -8
            buf = qi.Buffer(protocol_version=self.version)
            buf.row('reversed_table', columns={'col': reversed_arr}, at=qi.ServerTimestamp)
            reversed_expected = b'reversed_table col=' + _array_binary_bytes(reversed_arr) + b'\n'
            self.assertEqual(bytes(buf), reversed_expected)

            # zero dimensional array
            with self.assertRaisesRegex(qi.QuestDBError, "Zero-dimensional arrays are not supported"):
                scalar_arr = np.array(42.0, dtype=np.float64)
                buf = qi.Buffer(protocol_version=self.version)
                buf.row('scalar_table', columns={'col': scalar_arr}, at=qi.ServerTimestamp)

            # not f64 dtype array
            with self.assertRaisesRegex(qi.QuestDBError, "Only float64 numpy arrays are supported, got dtype: complex64"):
                complex_arr = np.array([1 + 2j], dtype=np.complex64)
                buf.row('invalid_table', columns={'col': complex_arr}, at=qi.ServerTimestamp)

        def test_int_range(self):
            buf = qi.Buffer(protocol_version=self.version)
            buf.row('tbl1', columns={'num': 0}, at=qi.ServerTimestamp)
            self.assertEqual(bytes(buf), f'tbl1 num=0i\n'.encode('utf-8'))
            buf.clear()

            # 32-bit int range.
            buf.row('tbl1', columns={'min': -2 ** 31, 'max': 2 ** 31 - 1}, at=qi.ServerTimestamp)
            self.assertEqual(bytes(buf), f'tbl1 min=-2147483648i,max=2147483647i\n'.encode('utf-8'))
            buf.clear()

            # 64-bit int range.
            buf.row('tbl1', columns={'min': -2 ** 63, 'max': 2 ** 63 - 1}, at=qi.ServerTimestamp)
            self.assertEqual(bytes(buf), f'tbl1 min=-9223372036854775808i,max=9223372036854775807i\n'.encode('utf-8'))
            buf.clear()

            # Overflow.
            with self.assertRaises(OverflowError):
                buf.row('tbl1', columns={'num': 2 ** 63}, at=qi.ServerTimestamp)

            # Underflow.
            with self.assertRaises(OverflowError):
                buf.row('tbl1', columns={'num': -2 ** 63 - 1}, at=qi.ServerTimestamp)

    class TestSender(unittest.TestCase, TimestampEncodingMixin):
        def test_transaction_row_at_disallows_none(self):
            with HttpServer() as server, self.builder('http', '127.0.0.1', server.port) as sender:
                with self.assertRaisesRegex(
                        qi.QuestDBError,
                        'must be of type TimestampNanos, datetime, or ServerTimestamp'):
                    with sender.transaction("foo") as txn:
                        txn.row(symbols={'sym1': 'val1'}, at=None)
                with self.assertRaisesRegex(
                        TypeError,
                        'needs keyword-only argument at'):
                    with sender.transaction("foo") as txn:
                        txn.row(symbols={'sym1': 'val1'})

        @unittest.skipIf(not pd, 'pandas not installed')
        def test_transaction_dataframe_at_disallows_none(self):
            with HttpServer() as server, self.builder('http', '127.0.0.1', server.port) as sender:
                with self.assertRaisesRegex(
                        qi.QuestDBError,
                        'must be of type TimestampNanos, datetime, or ServerTimestamp'):
                    with sender.transaction("foo") as txn:
                        txn.dataframe(pd.DataFrame(), at=None)
                with self.assertRaisesRegex(
                        TypeError,
                        'needs keyword-only argument at'):
                    with sender.transaction("foo") as txn:
                        txn.dataframe(pd.DataFrame())

        def test_sender_row_at_disallows_none(self):
            with Server() as server, self.builder('tcp', '127.0.0.1', server.port) as sender:
                with self.assertRaisesRegex(
                        qi.QuestDBError,
                        'must be of type TimestampNanos, datetime, or ServerTimestamp'):
                    sender.row('tbl1', symbols={'sym1': 'val1'}, at=None)
                with self.assertRaisesRegex(
                        TypeError,
                        'needs keyword-only argument at'):
                    sender.row('tbl1', symbols={'sym1': 'val1'})

        @unittest.skipIf(not pd, 'pandas not installed')
        def test_sender_dataframe_at_disallows_none(self):
            with Server() as server, self.builder('tcp', '127.0.0.1', server.port) as sender:
                with self.assertRaisesRegex(
                        qi.QuestDBError,
                        'must be of type TimestampNanos, datetime, or ServerTimestamp'):
                    sender.dataframe(pd.DataFrame(), at=None)
                with self.assertRaisesRegex(
                        TypeError,
                        'needs keyword-only argument at'):
                    sender.dataframe(pd.DataFrame())

        def test_basic(self):
            with Server() as server, \
                    self.builder(
                        'tcp',
                        '127.0.0.1',
                        server.port,
                        bind_interface='0.0.0.0',
                        protocol_version='2') as sender:
                server.accept()
                self.assertEqual(server.recv(), [])
                sender.row(
                    'tab1',
                    symbols={
                        't1': 'val1',
                        't2': 'val2'},
                    columns={
                        'f1': True,
                        'f2': 12345,
                        'f3': 10.75,
                        'f4': 'val3'},
                    at=qi.TimestampNanos(111222233333))
                sender.row(
                    'tab1',
                    symbols={
                        'tag3': 'value 3',
                        'tag4': 'value:4'},
                    columns={
                        'field5': False},
                    at=qi.ServerTimestamp)
                sender.flush()
                msgs = server.recv()
                self.assertEqual(msgs, [
                    (b'tab1,t1=val1,t2=val2 '
                     b'f1=t,f2=12345i,f3' + _float_binary_bytes(10.75) + b',f4="val3" ' +
                     self.enc_des_ts_n(111222233333, v=2).encode()),
                    b'tab1,tag3=value\\ 3,tag4=value:4 field5=f'])
                
        def test_bad_protocol_versions(self):
            bad_versions = [
                '0',
                'automatic',
                0,
                4,
                '4',
                '1.5',
                '2.0',
            ]

            for version in bad_versions:
                with self.assertRaisesRegex(
                        qi.QuestDBError,
                        '"protocol_version" must be None, "auto", 1-3'):
                    self.builder('tcp', '127.0.0.1', 12345, protocol_version=version)
                    self.fail('Should not have reached here - constructing sender')

            bad_versions.append(None)
            for version in bad_versions:
                with self.assertRaises(Exception) as capture:
                    qi.Buffer(protocol_version=version)
                    self.fail('Should not have reached here - constructing buffer')

                self.assertIn(type(capture.exception), (qi.QuestDBError, TypeError))

                if isinstance(capture.exception, qi.QuestDBError):
                    self.assertEqual(capture.exception.code, qi.QuestDBErrorCode.ProtocolVersionError)
                    self.assertIn('Invalid protocol version', str(capture.exception))

        def test_connect_close(self):
            with Server() as server:
                sender = None
                try:
                    sender = self.builder('tcp', '127.0.0.1', server.port)
                    sender.establish()
                    server.accept()
                    self.assertEqual(server.recv(), [])
                    sender.row('tbl1', symbols={'sym1': 'val1'}, at=qi.ServerTimestamp)
                    sender.flush()
                    msgs = server.recv()
                    self.assertEqual(msgs, [b'tbl1,sym1=val1'])
                finally:
                    sender.close()

        def test_row_before_connect(self):
            try:
                sender = self.builder('tcp', '127.0.0.1', 12345)
                with self.assertRaisesRegex(qi.QuestDBError, 'Sender is closed'):
                    sender.row('tbl1', symbols={'sym1': 'val1'}, at=qi.ServerTimestamp)
            finally:
                sender.close()

        def test_flush_1(self):
            with Server() as server:
                with self.builder('tcp', '127.0.0.1', server.port) as sender:
                    server.accept()
                    with self.assertRaisesRegex(qi.QuestDBError, 'Column names'):
                        sender.row('tbl1', symbols={'...bad name..': 'val1'}, at=qi.ServerTimestamp)
                    self.assertEqual(bytes(sender), b'')
                    sender.flush()
                    self.assertEqual(bytes(sender), b'')
                msgs = server.recv()
                self.assertEqual(msgs, [])

        def test_flush_2(self):
            with Server() as server:
                with self.builder('tcp', '127.0.0.1', server.port) as sender:
                    server.accept()
                    server.close()

                    # We enter a bad state where we can't flush again.
                    with self.assertRaises(qi.QuestDBError):
                        for _ in range(1000):
                            time.sleep(0.01)
                            sender.row('tbl1', symbols={'a': 'b'}, at=qi.ServerTimestamp)
                            sender.flush()

                    # We should still be in a bad state.
                    with self.assertRaises(qi.QuestDBError):
                        sender.row('tbl1', symbols={'a': 'b'}, at=qi.ServerTimestamp)
                        sender.flush()

                # Leaving the `with` scope will call __exit__ and here we test
                # that a prior exception will not cause subsequent problems.

        def test_flush_3(self):
            # Same as test_flush_2, but we catch the exception _outside_ the
            # sender's `with` block, to ensure no exceptions get trapped.
            with Server() as server:
                with self.assertRaises(qi.QuestDBError):
                    with self.builder('tcp', '127.0.0.1', server.port) as sender:
                        server.accept()
                        server.close()
                        for _ in range(1000):
                            time.sleep(0.01)
                            sender.row('tbl1', symbols={'a': 'b'}, at=qi.ServerTimestamp)
                            sender.flush()

        def test_flush_4(self):
            # Clearing of the internal buffer is not allowed.
            with Server() as server:
                with self.assertRaises(ValueError):
                    with self.builder('tcp', '127.0.0.1', server.port) as sender:
                        server.accept()
                        sender.row('tbl1', symbols={'a': 'b'}, at=qi.ServerTimestamp)
                        sender.flush(buffer=None, clear=False)

        def test_two_rows_explicit_buffer(self):
            with Server() as server, self.builder('tcp', '127.0.0.1', server.port, protocol_version='2') as sender:
                server.accept()
                self.assertEqual(server.recv(), [])
                buffer = sender.new_buffer()
                buffer.row(
                    'line_sender_buffer_example2',
                    symbols={'id': 'Hola'},
                    columns={'price': '111222233333i', 'qty': 3.5},
                    at=qi.TimestampNanos(111222233333))
                buffer.row(
                    'line_sender_example',
                    symbols={'id': 'Adios'},
                    columns={'price': '111222233343i', 'qty': 2.5},
                    at=qi.TimestampNanos(111222233343))
                exp = (
                    b'line_sender_buffer_example2,id=Hola price="111222233333i",qty' + _float_binary_bytes(3.5) + b' 111222233333n\n'
                    b'line_sender_example,id=Adios price="111222233343i",qty' + _float_binary_bytes(2.5) + b' 111222233343n\n')
                self.assertEqual(bytes(buffer), exp)
                sender.flush(buffer)
                msgs = server.recv()
                bexp = [msg for msg in exp.rstrip().split(b'\n')]
                self.assertEqual(msgs, bexp)

        def test_independent_buffer(self):
            buf = qi.Buffer(protocol_version=2)
            buf.row('tbl1', symbols={'sym1': 'val1'}, at=qi.ServerTimestamp)
            exp = b'tbl1,sym1=val1\n'
            self.assertEqual(bytes(buf), exp)

            with Server() as server1, Server() as server2:
                with self.builder('tcp', '127.0.0.1', server1.port, protocol_version='2') as sender1, \
                        self.builder('tcp', '127.0.0.1', server2.port, protocol_version='2') as sender2:
                    server1.accept()
                    server2.accept()

                    sender1.flush(buf, clear=False)
                    self.assertEqual(bytes(buf), exp)

                    sender2.flush(buf, clear=False)
                    self.assertEqual(bytes(buf), exp)

                    msgs1 = server1.recv()
                    msgs2 = server2.recv()
                    self.assertEqual(msgs1, [exp[:-1]])
                    self.assertEqual(msgs2, [exp[:-1]])

                    sender1.flush(buf)
                    self.assertEqual(server1.recv(), [exp[:-1]])

                    # The buffer is now auto-cleared.
                    self.assertEqual(bytes(buf), b'')

        def test_auto_flush_settings_defaults(self):
            for protocol in ('tcp', 'tcps', 'http', 'https'):
                sender = self.builder(protocol, '127.0.0.1', 9009)
                self.assertTrue(sender.auto_flush)
                self.assertEqual(sender.auto_flush_bytes, None)
                self.assertEqual(
                    sender.auto_flush_rows,
                    75000 if protocol.startswith('http') else 600)
                self.assertEqual(sender.auto_flush_interval, datetime.timedelta(seconds=1))

        def test_auto_flush_settings_off(self):
            for protocol in ('tcp', 'tcps', 'http', 'https'):
                sender = self.builder(protocol, '127.0.0.1', 9009, auto_flush=False)
                self.assertFalse(sender.auto_flush)
                self.assertEqual(sender.auto_flush_bytes, None)
                self.assertEqual(sender.auto_flush_rows, None)
                self.assertEqual(sender.auto_flush_interval, None)

        def test_auto_flush_settings_on(self):
            for protocol in ('tcp', 'tcps', 'http', 'https'):
                sender = self.builder(protocol, '127.0.0.1', 9009, auto_flush=True)
                # Same as default.
                self.assertEqual(sender.auto_flush_bytes, None)
                self.assertEqual(
                    sender.auto_flush_rows,
                    75000 if protocol.startswith('http') else 600)
                self.assertEqual(sender.auto_flush_interval, datetime.timedelta(seconds=1))

        def test_auto_flush_settings_specified(self):
            for protocol in ('tcp', 'tcps', 'http', 'https'):
                sender = self.builder(
                    protocol,
                    '127.0.0.1',
                    9009,
                    auto_flush_bytes=1024,
                    auto_flush_rows=100,
                    auto_flush_interval=datetime.timedelta(milliseconds=50))
                self.assertTrue(sender.auto_flush)
                self.assertEqual(sender.auto_flush_bytes, 1024)
                self.assertEqual(sender.auto_flush_rows, 100)
                self.assertEqual(sender.auto_flush_interval, datetime.timedelta(milliseconds=50))

        def test_auto_flush(self):
            with Server() as server:
                with self.builder(
                        'tcp',
                        '127.0.0.1',
                        server.port,
                        auto_flush_bytes=4,
                        auto_flush_rows=False,
                        auto_flush_interval=False) as sender:
                    server.accept()
                    sender.row('tbl1', symbols={'sym1': 'val1'}, at=qi.ServerTimestamp)
                    self.assertEqual(len(sender), 0)  # auto-flushed buffer.
                    msgs = server.recv()
                    self.assertEqual(msgs, [b'tbl1,sym1=val1'])

        def test_immediate_auto_flush(self):
            with Server() as server:
                with self.builder('tcp', '127.0.0.1', server.port, auto_flush_rows=1) as sender:
                    server.accept()
                    sender.row('tbl1', symbols={'sym1': 'val1'}, at=qi.ServerTimestamp)
                    self.assertEqual(len(sender), 0)  # auto-flushed buffer.
                    msgs = server.recv()
                    self.assertEqual(msgs, [b'tbl1,sym1=val1'])

        def test_auto_flush_on_closed_socket(self):
            with Server() as server:
                with self.builder('tcp', '127.0.0.1', server.port, auto_flush_rows=1) as sender:
                    server.accept()
                    server.close()
                    exp_err = 'Could not flush buffer.* - See https'
                    with self.assertRaisesRegex(qi.QuestDBError, exp_err):
                        for _ in range(1000):
                            time.sleep(0.01)
                            sender.row('tbl1', symbols={'a': 'b'}, at=qi.ServerTimestamp)

        def test_dont_auto_flush(self):
            msg_counter = 0
            with Server() as server:
                with self.builder('tcp', '127.0.0.1', server.port, auto_flush=False) as sender:
                    server.accept()
                    while len(sender) < 32768:  # 32KiB
                        sender.row('tbl1', symbols={'sym1': 'val1'}, at=qi.ServerTimestamp)
                        msg_counter += 1
                    msgs = server.recv()
                    self.assertEqual(msgs, [])
                start = time.monotonic()
                msgs = []
                while len(msgs) < msg_counter:
                    msgs += server.recv()
                    elapsed = time.monotonic() - start
                    if elapsed > 30.0:
                        raise TimeoutError()

        def test_dont_flush_on_exception(self):
            with Server() as server:
                with self.assertRaises(RuntimeError):
                    with self.builder('tcp', '127.0.0.1', server.port) as sender:
                        server.accept()
                        sender.row('tbl1', symbols={'sym1': 'val1'}, at=qi.ServerTimestamp)
                        self.assertEqual(bytes(sender), b'tbl1,sym1=val1\n')
                        raise RuntimeError('Test exception')
                msgs = server.recv()
                self.assertEqual(msgs, [])

        @unittest.skipIf(not pd, 'pandas not installed')
        def test_dataframe(self):
            with Server() as server:
                with self.builder('tcp', '127.0.0.1', server.port, protocol_version='2') as sender:
                    server.accept()
                    df = pd.DataFrame({'a': [1, 2], 'b': [3.0, 4.0]})
                    sender.dataframe(df, table_name='tbl1', at=qi.ServerTimestamp)
                msgs = server.recv()
                self.assertEqual(
                    msgs,
                    [b'tbl1 a=1i,b' + _float_binary_bytes(3.0),
                     b'tbl1 a=2i,b' + _float_binary_bytes(4.0)])

        @unittest.skipIf(not pd, 'pandas not installed')
        def test_dataframe_auto_flush(self):
            with Server() as server:
                # An auto-flush size of 25 bytes is enough to auto-flush the first
                # row, but not the second.
                with self.builder(
                        'tcp',
                        '127.0.0.1',
                        server.port,
                        auto_flush_bytes=25,
                        auto_flush_rows=False,
                        auto_flush_interval=False,
                        protocol_version=2) as sender:
                    server.accept()
                    df = pd.DataFrame({'a': [100000, 2], 'b': [3.0, 4.0]})
                    sender.dataframe(df, table_name='tbl1', at=qi.ServerTimestamp)
                    msgs = server.recv()
                    self.assertEqual(
                        msgs,
                        [b'tbl1 a=100000i,b' + _float_binary_bytes(3.0),])

                    # The second row is still pending send.
                    self.assertEqual(len(sender), 23)

                    # So we give it some more data and we should see it flush.
                    sender.row('tbl1', columns={'a': 3, 'b': 5.0}, at=qi.ServerTimestamp)
                    msgs = server.recv()
                    self.assertEqual(
                        msgs,
                        [b'tbl1 a=2i,b' + _float_binary_bytes(4.0),
                         b'tbl1 a=3i,b' + _float_binary_bytes(5.0)])

                    self.assertEqual(len(sender), 0)

                    # We can now disconnect the server and see auto flush failing.
                    server.close()

                    exp_err = 'Could not flush buffer.* - See https'
                    with self.assertRaisesRegex(qi.QuestDBError, exp_err):
                        for _ in range(1000):
                            time.sleep(0.01)
                            sender.dataframe(df.head(1), table_name='tbl1', at=qi.ServerTimestamp)

        def test_new_buffer(self):
            with Server() as server:
                with self.builder(
                protocol='tcp',
                host='127.0.0.1',
                port=server.port,
                init_buf_size=1024,
                max_name_len=20) as sender:
                    buffer = sender.new_buffer()
                    self.assertEqual(buffer.init_buf_size, 1024)
                    self.assertEqual(buffer.max_name_len, 20)
                    self.assertEqual(buffer.init_buf_size, sender.init_buf_size)
                    self.assertEqual(buffer.max_name_len, sender.max_name_len)

        def test_connect_after_close(self):
            with Server() as server, self.builder('tcp', '127.0.0.1', server.port) as sender:
                server.accept()
                sender.row('tbl1', symbols={'sym1': 'val1'}, at=qi.ServerTimestamp)
                sender.close()
                with self.assertRaises(qi.QuestDBError):
                    sender.establish()

        def test_bad_init_args(self):
            with self.assertRaises(OverflowError):
                self.builder(protocol='tcp', host='127.0.0.1', port=9009, auth_timeout=-1)

            with self.assertRaises(OverflowError):
                self.builder(protocol='tcp', host='127.0.0.1', port=9009, init_buf_size=-1)

            with self.assertRaises(OverflowError):
                self.builder(protocol='tcp', host='127.0.0.1', port=9009, max_name_len=-1)

        def test_transaction_over_tcp(self):
            with Server() as server, self.builder('tcp', '127.0.0.1', server.port) as sender:
                server.accept()
                self.assertRaisesRegex(
                    qi.QuestDBError,
                    'Transactions are only supported for ILP/HTTP.',
                    sender.transaction, 'table_name')

        def test_transaction_basic(self):
            ts = qi.TimestampNanos.now()
            e = lambda ts: self.enc_des_ts(ts, v=2)
            expected = (
                    f'table_name,sym1=val1 {e(ts)}\n' +
                    f'table_name,sym2=val2 {e(ts)}\n').encode('utf-8')
            with HttpServer() as server, self.builder('http', '127.0.0.1', server.port) as sender:
                with sender.transaction('table_name') as txn:
                    self.assertIs(txn.row(symbols={'sym1': 'val1'}, at=ts), txn)
                    self.assertIs(txn.row(symbols={'sym2': 'val2'}, at=ts), txn)
                self.assertEqual(len(server.requests), 1)
                self.assertEqual(server.requests[0], expected)

        @unittest.skipIf(not pd, 'pandas not installed')
        def test_transaction_basic_df(self):
            ts = qi.TimestampNanos.now()
            e = lambda num: self.enc_des_ts(num, v=2)
            expected = (
                    f'table_name,sym1=val1 {e(ts)}\n' +
                    f'table_name,sym2=val2 {e(ts)}\n').encode('utf-8')
            with HttpServer() as server, self.builder('http', '127.0.0.1', server.port) as sender:
                with sender.transaction('table_name') as txn:
                    df = pd.DataFrame({'sym1': ['val1', None], 'sym2': [None, 'val2']})
                    self.assertIs(txn.dataframe(df, symbols=['sym1', 'sym2'], at=ts), txn)
                self.assertEqual(len(server.requests), 1)
                self.assertEqual(server.requests[0], expected)

        def test_transaction_no_auto_flush(self):
            ts = qi.TimestampNanos.now()
            e = lambda ts: self.enc_des_ts(ts, v=2)
            expected = (
                    f'table_name,sym1=val1 {e(ts)}\n' +
                    f'table_name,sym2=val2 {e(ts)}\n').encode('utf-8')
            with HttpServer() as server, self.builder('http', '127.0.0.1', server.port, auto_flush=False) as sender:
                with sender.transaction('table_name') as txn:
                    txn.row(symbols={'sym1': 'val1'}, at=ts)
                    txn.row(symbols={'sym2': 'val2'}, at=ts)
                self.assertEqual(len(server.requests), 1)
                self.assertEqual(server.requests[0], expected)

        @unittest.skipIf(not pd, 'pandas not installed')
        def test_transaction_no_auto_flush_df(self):
            ts = qi.TimestampNanos.now()
            e = lambda ts: self.enc_des_ts(ts, v=2)
            expected = (
                    f'table_name,sym1=val1 {e(ts)}\n' +
                    f'table_name,sym2=val2 {e(ts)}\n').encode('utf-8')
            with HttpServer() as server, self.builder('http', '127.0.0.1', server.port, auto_flush=False) as sender:
                with sender.transaction('table_name') as txn:
                    df = pd.DataFrame({'sym1': ['val1', None], 'sym2': [None, 'val2']})
                    txn.dataframe(df, symbols=['sym1', 'sym2'], at=ts)
                self.assertEqual(len(server.requests), 1)
                self.assertEqual(server.requests[0], expected)

        def test_transaction_auto_flush_pending_buf(self):
            ts = qi.TimestampNanos.now()
            e = lambda ts: self.enc_des_ts(ts, v=2)
            expected1 = (
                    f'tbl1,sym1=val1 {e(ts)}\n' +
                    f'tbl1,sym2=val2 {e(ts)}\n').encode('utf-8')
            expected2 = (
                    f'tbl2,sym3=val3 {e(ts)}\n' +
                    f'tbl2,sym4=val4 {e(ts)}\n').encode('utf-8')
            with HttpServer() as server, self.builder('http', '127.0.0.1', server.port, auto_flush=True) as sender:
                self.assertIs(sender.row('tbl1', symbols={'sym1': 'val1'}, at=ts), sender)
                self.assertIs(sender.row('tbl1', symbols={'sym2': 'val2'}, at=ts), sender)
                with sender.transaction('tbl2') as txn:
                    txn.row(symbols={'sym3': 'val3'}, at=ts)
                    txn.row(symbols={'sym4': 'val4'}, at=ts)
                self.assertEqual(len(server.requests), 2)
                self.assertEqual(server.requests[0], expected1)
                self.assertEqual(server.requests[1], expected2)

        def test_transaction_no_auto_flush_pending_buf(self):
            ts = qi.TimestampNanos.now()
            exp_err = (
                    'Sender buffer must be clear when starting a transaction. ' +
                    'You must call ..flush... before this call.')
            with HttpServer() as server, self.builder('http', '127.0.0.1', server.port, auto_flush=False) as sender:
                self.assertIs(sender.row('tbl1', symbols={'sym1': 'val1'}, at=ts), sender)
                self.assertIs(sender.row('tbl1', symbols={'sym2': 'val2'}, at=ts), sender)
                with self.assertRaisesRegex(qi.QuestDBError, exp_err):
                    with sender.transaction('tbl2') as _txn:
                        pass

        def test_transaction_immediate_auto_flush(self):
            ts = qi.TimestampNanos.now()
            e = lambda num: self.enc_des_ts(num, v=2)
            expected1 = f'tbl1,sym1=val1 {e(ts)}\n'.encode('utf-8')
            expected2 = f'tbl2,sym2=val2 {e(ts)}\n'.encode('utf-8')
            expected3 = (
                    f'tbl3,sym3=val3 {e(ts)}\n' +
                    f'tbl3,sym4=val4 {e(ts)}\n').encode('utf-8')
            with HttpServer() as server, self.builder('http', '127.0.0.1', server.port, auto_flush_rows=1) as sender:
                self.assertIs(sender.row('tbl1', symbols={'sym1': 'val1'}, at=ts), sender)
                self.assertIs(sender.row('tbl2', symbols={'sym2': 'val2'}, at=ts), sender)
                with sender.transaction('tbl3') as txn:
                    # The transaction is not broken up by the auto-flush logic.
                    txn.row(symbols={'sym3': 'val3'}, at=ts)
                    txn.row(symbols={'sym4': 'val4'}, at=ts)
                self.assertEqual(len(server.requests), 3)
                self.assertEqual(server.requests[0], expected1)
                self.assertEqual(server.requests[1], expected2)
                self.assertEqual(server.requests[2], expected3)

        @unittest.skipIf(not pd, 'pandas not installed')
        def test_transaction_immediate_auto_flush_df(self):
            ts = qi.TimestampNanos.now()
            e = lambda ts: self.enc_des_ts(ts, v=2)
            expected1 = f'tbl1,sym1=val1 {e(ts)}\n'.encode('utf-8')
            expected2 = f'tbl2,sym2=val2 {e(ts)}\n'.encode('utf-8')
            expected3 = (
                    f'tbl3,sym3=val3 {e(ts)}\n' +
                    f'tbl3,sym4=val4 {e(ts)}\n').encode('utf-8')
            with HttpServer() as server, self.builder('http', '127.0.0.1', server.port, auto_flush_rows=1) as sender:
                self.assertIs(sender.row('tbl1', symbols={'sym1': 'val1'}, at=ts), sender)
                self.assertIs(sender.row('tbl2', symbols={'sym2': 'val2'}, at=ts), sender)
                with sender.transaction('tbl3') as txn:
                    df = pd.DataFrame({'sym3': ['val3', None], 'sym4': [None, 'val4']})
                    txn.dataframe(df, symbols=['sym3', 'sym4'], at=ts)
                self.assertEqual(len(server.requests), 3)
                self.assertEqual(server.requests[0], expected1)
                self.assertEqual(server.requests[1], expected2)
                self.assertEqual(server.requests[2], expected3)

        @unittest.skipIf(not pd, 'pandas not installed')
        def test_http_illegal_ops_in_txn(self):
            with HttpServer() as server, self.builder('http', '127.0.0.1', server.port, auto_flush_rows=1) as sender:
                with sender.transaction('tbl1') as txn:
                    txn.row(symbols={'sym1': 'val1'}, at=qi.ServerTimestamp)
                    txn.row(symbols={'sym2': 'val2'}, at=qi.ServerTimestamp)

                    with self.assertRaisesRegex(qi.QuestDBError, 'Cannot append rows explicitly inside a transaction'):
                        sender.row('tbl2', symbols={'sym3': 'val3'}, at=qi.ServerTimestamp)

                    with self.assertRaisesRegex(qi.QuestDBError, 'Cannot append rows explicitly inside a transaction'):
                        sender.dataframe(None, at=qi.ServerTimestamp)

                    with self.assertRaisesRegex(qi.QuestDBError, 'Cannot flush explicitly inside a transaction'):
                        sender.flush()

                    with self.assertRaisesRegex(qi.QuestDBError, 'Already inside a transaction, can\'t start another.'):
                        with sender.transaction('tbl2') as _txn2:
                            pass

                    txn.commit()
                    with self.assertRaisesRegex(qi.QuestDBError, 'Transaction already completed, can\'t commit'):
                        txn.commit()
                    with self.assertRaisesRegex(qi.QuestDBError, 'Transaction already completed, can\'t rollback.'):
                        txn.rollback()
                self.assertEqual(len(server.requests), 1)

        def test_auto_flush_rows(self):
            auto_flush_rows = 3

            def into_requests(xs):
                return [
                    b''.join(xs[i:i + auto_flush_rows])
                    for i in range(0, len(xs), auto_flush_rows)]

            expected = []
            with HttpServer() as server, self.builder(
                    'http',
                    '127.0.0.1',
                    server.port,
                    auto_flush_rows=auto_flush_rows,
                    auto_flush_interval=False,
                    auto_flush_bytes=False) as sender:
                for i in range(10):
                    sender.row('tbl1', columns={'x': i}, at=qi.ServerTimestamp)
                    expected.append(f'tbl1 x={i}i\n'.encode('utf-8'))

                # Before the end of the `with` block we should already have 3 requests.
                self.assertEqual(len(server.requests), 3)
                self.assertEqual(server.requests, into_requests(expected)[:3])

            # Closing the buffer should flush the last remaining row.
            self.assertEqual(len(server.requests), 4)
            self.assertEqual(server.requests, into_requests(expected))

        def _do_test_auto_flush_interval(self):
            with HttpServer() as server, self.builder(
                    'http',
                    '127.0.0.1',
                    server.port,
                    auto_flush_interval=10,
                    auto_flush_rows=False,
                    auto_flush_bytes=False) as sender:
                start_time = timeit.default_timer()
                while True:
                    sender.row('tbl1', columns={'x': 1}, at=qi.ServerTimestamp)
                    elapsed_ms = int((timeit.default_timer() - start_time) * 1000)
                    if elapsed_ms < 5:
                        self.assertEqual(len(server.requests), 0)
                    if elapsed_ms >= 15:  # 5ms grace period.
                        break
                    time.sleep(1 / 1000)  # 1ms

                return len(server.requests)

        def test_auto_flush_interval(self):
            # This test is timing-sensitive,
            # so it has a tendency to go wrong in CI.
            # To work around this we'll repeat the test up to 10 times
            # until it passes.
            for _ in range(10):
                requests_len = self._do_test_auto_flush_interval()
                if requests_len > 0:
                    break

            # If this fails, it failed 10 attempts.
            # Due to CI timing delays there may have been multiple flushes.
            self.assertGreaterEqual(requests_len, 1)

        def _do_test_auto_flush_interval2(self):
            with HttpServer() as server, self.builder(
                    'http',
                    '127.0.0.1',
                    server.port,
                    auto_flush_interval=100,
                    auto_flush_rows=False,
                    auto_flush_bytes=False) as sender:
                sender.row('t', columns={'x': 1}, at=qi.ServerTimestamp)
                sender.row('t', columns={'x': 2}, at=qi.ServerTimestamp)
                time.sleep(0.2)
                sender.row('t', columns={'x': 3}, at=qi.ServerTimestamp)
                sender.row('t', columns={'x': 4}, at=qi.ServerTimestamp)
                time.sleep(0.2)
                sender.row('t', columns={'x': 5}, at=qi.ServerTimestamp)
                sender.row('t', columns={'x': 6}, at=qi.ServerTimestamp)
            return server.requests

        def test_auto_flush_interval2(self):
            # This test is timing-sensitive,
            # so it has a tendency to go wrong in CI.
            # To work around this we'll repeat the test up to 10 times
            # until it passes.
            for _ in range(10):
                requests = self._do_test_auto_flush_interval2()
                if len(requests) == 3:
                    self.assertEqual(requests, [
                        b't x=1i\nt x=2i\nt x=3i\n',
                        b't x=4i\nt x=5i\n',
                        b't x=6i\n'])
                    break

            # If this fails, it failed 10 attempts.
            # Due to CI timing delays there may have been multiple flushes.
            self.assertEqual(len(requests), 3)

        def test_http_username_password(self):
            with HttpServer() as server, self.builder('http', '127.0.0.1', server.port, username='user',
                                                      password='pass') as sender:
                sender.row('tbl1', columns={'x': 42}, at=qi.ServerTimestamp)
            self.assertEqual(len(server.requests), 1)
            self.assertEqual(server.requests[0], b'tbl1 x=42i\n')
            self.assertEqual(server.headers[1]['authorization'], 'Basic dXNlcjpwYXNz')

        def test_http_token(self):
            with HttpServer() as server, self.builder('http', '127.0.0.1', server.port, token='Yogi') as sender:
                sender.row('tbl1', columns={'x': 42}, at=qi.ServerTimestamp)
            self.assertEqual(len(server.requests), 1)
            self.assertEqual(server.requests[0], b'tbl1 x=42i\n')
            self.assertEqual(server.headers[1]['authorization'], 'Bearer Yogi')

        def test_max_buf_size(self):
            with HttpServer() as server, self.builder('http', '127.0.0.1', server.port, max_buf_size=1024,
                                                      auto_flush=False) as sender:
                while len(sender) < 1024:
                    sender.row('tbl1', columns={'x': 42}, at=qi.ServerTimestamp)
                with self.assertRaisesRegex(qi.QuestDBError, 'Could not flush .*exceeds maximum'):
                    sender.flush()

        def test_http_err(self):
            with HttpServer() as server, self.builder(
                    'http',
                    '127.0.0.1',
                    server.port,
                    retry_timeout=datetime.timedelta(milliseconds=1)) as sender:
                server.responses.append((0, 500, 'text/plain', b'Internal Server Error'))
                with self.assertRaisesRegex(qi.QuestDBError, 'Could not flush.*: Internal Server'):
                    sender.row('tbl1', columns={'x': 42}, at=qi.ServerTimestamp)
                    sender.flush()
                self.assertEqual(len(sender), 0)  # buffer is still cleared after error.

        def test_http_err_retry(self):
            exp_payload = b'tbl1 x=42i\n'
            with HttpServer() as server, self.builder(
                    'http',
                    '127.0.0.1',
                    server.port,
                    retry_timeout=datetime.timedelta(seconds=1)) as sender:
                server.responses.append((0, 500, 'text/plain', b'retriable error'))
                server.responses.append((0, 200, 'text/plain', b'OK'))
                sender.row('tbl1', columns={'x': 42}, at=qi.ServerTimestamp)
                sender.flush()
                self.assertEqual(len(server.requests), 2)
                self.assertEqual(server.requests[0], exp_payload)
                self.assertEqual(server.requests[1], exp_payload)

        def test_http_request_min_throughput(self):
            with HttpServer(delay_seconds=2) as server, self.builder(
                    'http',
                    '127.0.0.1',
                    server.port,
                    request_timeout=1000,
                    protocol_version='2',
                    # request_timeout is sufficiently high since it's also used as a connect timeout and we want to
                    # survive hiccups on CI. it should be lower than the server delay though to actually test the
                    # effect of request_min_throughput.
                    request_min_throughput=1) as sender:
                sender.row('tbl1', columns={'x': 42}, at=qi.ServerTimestamp)
                sender.flush()
                self.assertEqual(len(server.requests), 1)

        def test_http_request_min_throughput_timeout(self):
            with HttpServer() as server, self.builder(
                    'http',
                    '127.0.0.1',
                    server.port,
                    auto_flush='off',
                    request_timeout=100,
                    retry_timeout=0,
                    # effectively calculates a ~1ms timeout
                    request_min_throughput=100000000,
                    protocol_version=2) as sender:
                buffer = sender.new_buffer()
                buffer.row('tbl1', columns={'x': 42}, at=qi.ServerTimestamp)
                buffer.row('tbl1', columns={'x': 42}, at=qi.ServerTimestamp)
                buffer.row('tbl1', columns={'x': 42}, at=qi.ServerTimestamp)
                buffer.row('tbl1', columns={'x': 42}, at=qi.ServerTimestamp)
                buffer.row('tbl1', columns={'x': 42}, at=qi.ServerTimestamp)

                # wait 50ms in the server to simulate a slow response
                with self.assertRaisesRegex(qi.QuestDBError, 'timeout: per call') as cm:
                    for _ in range(10):
                        server.responses.append((500, 200, 'text/plain', b'OK'))
                        # We retry in case the network thread gets descheduled
                        # and is only rescheduled after the timeout elapsed.
                        sender.flush(buffer, clear=False)

        def test_http_request_timeout(self):
            with HttpServer() as server, self.builder(
                    'http',
                    '127.0.0.1',
                    server.port,
                    retry_timeout=0,
                    request_min_throughput=0,  # disable
                    protocol_version=2,
                    request_timeout=datetime.timedelta(milliseconds=50)) as sender:
                # Server waits 500ms before responding; the client should
                # time out at 50ms, well before the response arrives.
                server.responses.append((500, 200, 'text/plain', b'OK'))
                sender.row('tbl1', columns={'x': 42}, at=qi.ServerTimestamp)
                with self.assertRaisesRegex(qi.QuestDBError, 'timeout: per call'):
                    sender.flush()

        def test_http_server_not_serve(self):
            with self.assertRaisesRegex(qi.QuestDBError, 'Could not detect server\'s line protocol version, settings url: http://127.0.0.1:1234/settings'):
                with self.builder(
                    'http',
                    '127.0.0.1',
                    1234,
                    protocol_version='auto') as sender:
                        sender.row('tbl1', columns={'x': 42})

        def test_http_auto_protocol_version_only_v1(self):
            self._test_sender_http_auto_protocol_version(SETTINGS_WITH_PROTOCOL_VERSION_V1, 1)

        def test_http_auto_protocol_version_only_v2(self):
            self._test_sender_http_auto_protocol_version(SETTINGS_WITH_PROTOCOL_VERSION_V2, 2)

        def test_http_auto_protocol_version_v1_v2_v3(self):
            self._test_sender_http_auto_protocol_version(SETTINGS_WITH_PROTOCOL_VERSION_V1_V2_V3, 3)

        def test_http_auto_protocol_version_without_version(self):
            self._test_sender_http_auto_protocol_version(SETTINGS_WITHOUT_PROTOCOL_VERSION, 1)

        def _test_sender_http_auto_protocol_version(self, settings, expected_version: int):
            with HttpServer(settings) as server, self.builder('http', '127.0.0.1', server.port) as sender:
                self.assertEqual(sender.protocol_version, expected_version)
                buffer = sender.new_buffer()
                buffer.row(
                    'line_sender_buffer_old_server2',
                    symbols={'id': 'Hola'},
                    columns={'price': '111222233333i', 'qty': 3.5},
                    at=qi.TimestampNanos(111222233333))
                e = lambda num: self.enc_des_ts_n(num, v=expected_version)
                exp = b'line_sender_buffer_old_server2,id=Hola price="111222233333i",qty' + _float_binary_bytes(
                    3.5, expected_version == 1) + f' {e(111222233333)}\n'.encode()
                self.assertEqual(bytes(buffer), exp)
                sender.flush(buffer)
                self.assertEqual(len(server.requests), 1)
                self.assertEqual(server.requests[0], exp)

        def test_http_auto_protocol_version_unsupported_client(self):
            with self.assertRaisesRegex(qi.QuestDBError, r'Server does not support any of the client protocol versions.*'):
                with HttpServer(SETTINGS_WITH_PROTOCOL_VERSION_V4) as server, self.builder('http', '127.0.0.1', server.port) as sender:
                    sender.row('tbl1', columns={'x': 42})

        def test_specify_line_protocol_explicitly(self):
            with HttpServer() as server, self.builder('http', '127.0.0.1', server.port, protocol_version='1') as sender:
                buffer = sender.new_buffer()
                buffer.row(
                    'line_sender_buffer',
                    symbols={'id': 'Hola'},
                    columns={'qty': 3.5},
                    at=qi.TimestampNanos(111222233333))
                exp = b'line_sender_buffer,id=Hola qty' + _float_binary_bytes(
                    3.5, True) + b' 111222233333\n'
                self.assertEqual(bytes(buffer), exp)
                sender.flush(buffer)
                self.assertEqual(len(server.requests), 1)
                self.assertEqual(server.requests[0], exp)

        def test_line_protocol_version_on_tcp(self):
            with Server() as server, self.builder('tcp', '127.0.0.1', server.port, protocol_version='1') as sender:
                server.accept()
                self.assertEqual(server.recv(), [])
                buffer = sender.new_buffer()
                buffer.row(
                    'line_sender_buffer_tcp_v1',
                    symbols={'id': 'Hola'},
                    columns={'qty': 3.5},
                    at=qi.TimestampNanos(111222233333))
                exp = b'line_sender_buffer_tcp_v1,id=Hola qty=3.5 111222233333\n'
                self.assertEqual(bytes(buffer), exp)
                sender.flush(buffer)
                self.assertEqual(server.recv()[0] + b'\n', exp)

            with Server() as server, self.builder('tcp', '127.0.0.1', server.port, protocol_version='2') as sender:
                server.accept()
                self.assertEqual(server.recv(), [])
                buffer = sender.new_buffer()
                buffer.row(
                    'line_sender_buffer_tcp_v1',
                    symbols={'id': 'Hola'},
                    columns={'qty': 3.5},
                    at=qi.TimestampNanos(111222233333))
                exp = b'line_sender_buffer_tcp_v1,id=Hola qty' + _float_binary_bytes(3.5) + b' 111222233333n\n'
                self.assertEqual(bytes(buffer), exp)
                sender.flush(buffer)
                self.assertEqual(server.recv()[0] + b'\n', exp)

            with Server() as server, self.builder('tcp', '127.0.0.1', server.port, protocol_version='auto') as sender:
                server.accept()
                self.assertEqual(server.recv(), [])
                buffer = sender.new_buffer()
                buffer.row(
                    'line_sender_buffer_tcp_v1',
                    symbols={'id': 'Hola'},
                    columns={'qty': 3.5},
                    at=qi.TimestampNanos(111222233333))
                exp = b'line_sender_buffer_tcp_v1,id=Hola qty=3.5 111222233333\n'
                self.assertEqual(bytes(buffer), exp)
                sender.flush(buffer)
                self.assertEqual(server.recv()[0] + b'\n', exp)\

        def _test_array_basic(self, arr: np.ndarray):
            # http
            with HttpServer() as server, self.builder('http', '127.0.0.1', server.port) as sender:
                sender.row(
                    'array_test',
                    columns={'array': arr},
                    at=qi.TimestampNanos(11111))
                exp = b'array_test array=' + _array_binary_bytes(arr) + b' 11111n\n'
                sender.flush()
                self.assertEqual(len(server.requests), 1)
                self.assertEqual(server.requests[0], exp)

            #tcp
            with Server() as server, self.builder('tcp', '127.0.0.1', server.port, protocol_version=2) as sender:
                server.accept()
                self.assertEqual(server.recv(), [])
                sender.row(
                    'array_test',
                    columns={'array': arr},
                    at=qi.TimestampNanos(11111))
                exp = b'array_test array=' + _array_binary_bytes(arr) + b' 11111n\n'
                self.assertEqual(bytes(sender), exp)
                sender.flush()
                self.assertEqual(server.recv()[0] + b'\n', exp)

        def test_array_basic(self):
            self._test_array_basic(np.array([1.2345678901234567, 2.3456789012345678], dtype=np.float64))

        def test_empty_array(self):
            self._test_array_basic(np.array([], dtype=np.float64))

        def test_non_contigious_array(self):
            base = np.arange(6, dtype=np.float64).reshape(2, 3)
            non_contig_arr = base[:, ::2]
            self._test_array_basic(non_contig_arr)

        def test_minus_stride_array(self):
            self._test_array_basic(np.array([1.1, 2.2, 3.3], dtype=np.float64)[::-1])

        def test_array_error_cases(self):
            # zero dimensional array
            with self.assertRaisesRegex(qi.QuestDBError, "Zero-dimensional arrays are not supported"):
                scalar_arr = np.array(42.0, dtype=np.float64)
                with HttpServer() as server, self.builder('http', '127.0.0.1', server.port) as sender:
                    sender.row(
                        'array_test',
                        columns={'array': scalar_arr},
                        at=qi.TimestampNanos(11111))

            # not f64 dtype array
            with self.assertRaisesRegex(qi.QuestDBError, "Only float64 numpy arrays are supported, got dtype: complex64"):
                complex_arr = np.array([1 + 2j], dtype=np.complex64)
                with HttpServer() as server, self.builder('http', '127.0.0.1', server.port) as sender:
                    sender.row(
                        'array_test',
                        columns={'array': complex_arr},
                        at=qi.TimestampNanos(11111))

            # max dims
            if NUMPY_VERSION >= (2,):
                # Note: Older numpy versions don't support more than 32 dimensions.
                with self.assertRaisesRegex(qi.QuestDBError, "Array dimension mismatch: expected at most 32 dimensions, but got 33"):
                    dims = (1,) * 33
                    array = np.empty(dims, dtype=np.float64)
                    with Server() as server, self.builder('tcp', '127.0.0.1', server.port, protocol_version="2") as sender:
                        sender.row(
                            'array_test',
                            columns={'array': array},
                            at=qi.TimestampNanos(11111))

            # default protocol version is v1, which does not support array datatype.
            with self.assertRaisesRegex(qi.QuestDBError, "Protocol version v1 does not support array datatype"):
                array = np.zeros([1,2], dtype=np.float64)
                with Server() as server, self.builder('tcp', '127.0.0.1', server.port) as sender:
                    sender.row(
                        'array_test',
                        columns={'array': array},
                        at=qi.TimestampNanos(11111))

    class Timestamp(unittest.TestCase):
        def test_from_int(self):
            ns = 1670857929778202000
            num = ns // self.ns_scale
            ts = self.timestamp_cls(num)
            self.assertEqual(ts.value, num)

            ts0 = self.timestamp_cls(0)
            self.assertEqual(ts0.value, 0)

            with self.assertRaisesRegex(
                    ValueError, 'value must be a non-negative'):
                self.timestamp_cls(-1)

        def test_from_datetime(self):
            utc = datetime.timezone.utc

            dt1 = datetime.datetime(2022, 1, 1, 12, 0, 0, 0, tzinfo=utc)
            ts1 = self.timestamp_cls.from_datetime(dt1)
            self.assertEqual(ts1.value, 1641038400000000000 // self.ns_scale)
            self.assertEqual(
                ts1.value,
                int(dt1.timestamp() * 1000000000 // self.ns_scale))

            dt2 = datetime.datetime(1970, 1, 1, tzinfo=utc)
            ts2 = self.timestamp_cls.from_datetime(dt2)
            self.assertEqual(ts2.value, 0)

            with self.assertRaisesRegex(
                    ValueError, 'value must be a non-negative'):
                self.timestamp_cls.from_datetime(
                    datetime.datetime(1969, 12, 31, tzinfo=utc))

            dt_naive = datetime.datetime(2022, 1, 1, 12, 0, 0, 0)
            with warnings.catch_warnings():
                warnings.simplefilter('ignore', UserWarning)
                ts3 = self.timestamp_cls.from_datetime(dt_naive)
            self.assertEqual(ts3.value, 1641038400000000000 // self.ns_scale)

        def test_now(self):
            expected = time.time_ns() // self.ns_scale
            actual = self.timestamp_cls.now().value
            delta = abs(expected - actual)
            one_sec = 1000000000 // self.ns_scale
            self.assertLess(delta, one_sec)

        def test_repr(self):
            self.assertEqual(
                repr(self.timestamp_cls(123)),
                f'{self.timestamp_cls.__name__}(123)')
            self.assertEqual(
                repr(self.timestamp_cls(0)),
                f'{self.timestamp_cls.__name__}(0)')


class TestTimestampMicros(TestBases.Timestamp):
    timestamp_cls = qi.TimestampMicros
    ns_scale = 1000

    def test_from_datetime_exact_far_future(self):
        # A float `dt.timestamp() * 1e6` conversion loses microsecond
        # precision this far out; the result must be exact.
        utc = datetime.timezone.utc
        ts = qi.TimestampMicros.from_datetime(
            datetime.datetime(2600, 1, 1, 0, 0, 0, 999999, tzinfo=utc))
        self.assertEqual(ts.value, 19880899200999999)

    def test_from_datetime_pre_epoch_rejected(self):
        # Half a second before the epoch converts to -500000 micros and
        # must trip the non-negative check, not wrap or truncate to 0.
        utc = datetime.timezone.utc
        with self.assertRaisesRegex(
                ValueError, 'value must be a non-negative'):
            qi.TimestampMicros.from_datetime(
                datetime.datetime(
                    1969, 12, 31, 23, 59, 59, 500000, tzinfo=utc))


class TestTimestampNanos(TestBases.Timestamp):
    timestamp_cls = qi.TimestampNanos
    ns_scale = 1

    def test_naive_datetime_is_utc(self):
        naive = datetime.datetime(2026, 7, 16, 12, 0)
        aware = naive.replace(tzinfo=datetime.timezone.utc)
        self.assertEqual(
            self.timestamp_cls.from_datetime(naive).value,
            self.timestamp_cls.from_datetime(aware).value)

    def test_naive_datetime_warns_once_per_process(self):
        qi._NAIVE_DATETIME_WARNED = False
        naive = datetime.datetime(2026, 7, 16, 12, 0)
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always')
            self.timestamp_cls.from_datetime(naive)
            self.timestamp_cls.from_datetime(naive)
        emitted = [w for w in caught if issubclass(w.category, UserWarning)]
        self.assertEqual(len(emitted), 1)
        self.assertIn('interpreted as UTC', str(emitted[0].message))
        self.assertIn('If you meant "now"', str(emitted[0].message))

    def test_from_datetime_out_of_int64_range(self):
        utc = datetime.timezone.utc
        ts = qi.TimestampNanos.from_datetime(
            datetime.datetime(2262, 4, 11, tzinfo=utc))
        self.assertGreater(ts.value, 0)
        with self.assertRaisesRegex(ValueError, 'out of range'):
            qi.TimestampNanos.from_datetime(
                datetime.datetime(2262, 4, 12, tzinfo=utc))
        with self.assertRaisesRegex(ValueError, 'out of range'):
            qi.TimestampNanos.from_datetime(
                datetime.datetime(2300, 1, 1, tzinfo=utc))


def build_conf(protocol, host, port, **kwargs):
    protocol = qi.Protocol.parse(protocol)

    def encode_duration(v):
        if isinstance(v, datetime.timedelta):
            return str(v.seconds * 1000 + v.microseconds // 1000)
        return str(v)

    def encode_duration_or_off(v):
        if v is False:
            return 'off'
        return encode_duration(v)

    def encode_int_or_off(v):
        if v is False:
            return 'off'
        return str(v)

    encoders = {
        'bind_interface': str,
        'username': str,
        'password': str,
        'token': str,
        'token_x': str,
        'token_y': str,
        'auth_timeout': encode_duration,
        'tls_verify': lambda v: 'on' if v else 'unsafe_off',
        'tls_ca': str,
        'tls_roots': str,
        'tls_roots_password': str,
        'max_buf_size': str,
        'retry_timeout': encode_duration,
        'retry_max_backoff': encode_duration,
        'request_min_throughput': str,
        'request_timeout': encode_duration,
        'auto_flush': lambda v: 'on' if v else 'off',
        'auto_flush_rows': encode_int_or_off,
        'auto_flush_bytes': encode_int_or_off,
        'auto_flush_interval': encode_duration_or_off,
        'protocol_version': str,
        'init_buf_size': str,
        'max_name_len': str,
    }

    def encode(k, v):
        encoder = encoders.get(k, str)
        return encoder(v)

    def conf_key(k):
        if k == 'retry_max_backoff':
            return 'retry_max_backoff_millis'
        return k

    return f'{protocol.tag}::addr={host}:{port};' + ''.join(
        f'{conf_key(k)}={encode(k, v)};'
        for k, v in kwargs.items()
        if v is not None)


def split_dict_randomly(original, seed=None):
    if seed is None:
        seed = random.randint(0, 2 ** 32 - 1)
    sys.stderr.write(f'\nsplit_dict_randomly seed {seed}\n')
    random.seed(seed)
    keys = list(original.keys())
    random.shuffle(keys)
    split_point = random.randint(0, len(keys))
    return (
        {k: original[k] for k in keys[:split_point]},
        {k: original[k] for k in keys[split_point:]})


class Builder(Enum):
    INIT = 1
    CONF = 2
    ENV = 3

    def __call__(self, protocol, host, port, **kwargs):
        if self is Builder.INIT:
            return qi.Sender(protocol, host, port, **kwargs)
        else:
            # Specify some of the params via the conf string,
            # and the rest via the API.
            via_conf, via_params = split_dict_randomly(kwargs)
            conf = build_conf(protocol, host, port, **via_conf)
            if self is Builder.CONF:
                return qi.Sender.from_conf(conf, **via_params)
            elif self is Builder.ENV:
                os.environ['QDB_CLIENT_CONF'] = conf
                sender = qi.Sender.from_env(**via_params)
                del os.environ['QDB_CLIENT_CONF']
                return sender


class TestSenderInit(TestBases.TestSender):
    name = 'init'
    builder = Builder.INIT


class TestSenderConf(TestBases.TestSender):
    name = 'conf'
    builder = Builder.CONF


class TestSenderEnv(TestBases.TestSender):
    name = 'env'
    builder = Builder.ENV


class TestBufferProtocolVersionV1(TestBases.TestBuffer):
    name = 'protocol version 1'
    version = 1


class TestBufferProtocolVersionV2(TestBases.TestBuffer):
    name = 'protocol version 2'
    version = 2


class TestBufferProtocolVersionV3(TestBases.TestBuffer):
    name = 'protocol version 3'
    version = 3


class TestUninitializedBuffer(unittest.TestCase):
    """Verify that Buffer.__new__(Buffer) raises instead of segfaulting."""

    def _make_uninit(self):
        return qi.Buffer.__new__(qi.Buffer)

    def test_len(self):
        with self.assertRaisesRegex(qi.QuestDBError, 'Buffer is not initialized'):
            len(self._make_uninit())

    def test_bytes(self):
        with self.assertRaisesRegex(qi.QuestDBError, 'Buffer is not initialized'):
            bytes(self._make_uninit())

    def test_capacity(self):
        with self.assertRaisesRegex(qi.QuestDBError, 'Buffer is not initialized'):
            self._make_uninit().capacity()

    def test_clear(self):
        with self.assertRaisesRegex(qi.QuestDBError, 'Buffer is not initialized'):
            self._make_uninit().clear()

    def test_reserve(self):
        with self.assertRaisesRegex(qi.QuestDBError, 'Buffer is not initialized'):
            self._make_uninit().reserve(1)

    def test_row(self):
        with self.assertRaisesRegex(qi.QuestDBError, 'Buffer is not initialized'):
            self._make_uninit().row('t', columns={'x': 1}, at=qi.ServerTimestamp)

    @unittest.skipIf(not pd, 'pandas not installed')
    def test_dataframe(self):
        import pandas as _pd
        with self.assertRaisesRegex(qi.QuestDBError, 'Buffer is not initialized'):
            self._make_uninit().dataframe(
                _pd.DataFrame({'x': [1]}),
                table_name='t',
                at=qi.ServerTimestamp)

    def test_flush(self):
        with self.assertRaisesRegex(qi.QuestDBError, 'Buffer is not initialized'):
            with Server() as server, \
                    qi.Sender(qi.Protocol.Tcp, '127.0.0.1', server.port) as sender:
                server.accept()
                sender.flush(self._make_uninit())


class TestIngressShim(unittest.TestCase):
    def test_import_warns_once_and_reexports(self):
        sys.modules.pop('questdb.ingress', None)
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always')
            import questdb.ingress as ingress
        deprecations = [
            w for w in caught
            if issubclass(w.category, DeprecationWarning)]
        self.assertEqual(len(deprecations), 1)
        self.assertIs(ingress.IngressError, qi.QuestDBError)
        self.assertIs(ingress.IngressErrorCode, qi.QuestDBErrorCode)
        self.assertIs(ingress.Sender, qi.Sender)
        self.assertIs(ingress.Buffer, qi.Buffer)
        self.assertIs(ingress.Protocol, qi.Protocol)

    def test_legacy_buffer_flow(self):
        import questdb.ingress as ingress
        buf = ingress.Buffer(2)
        buf.row(
            'tbl', columns={'x': 1.5},
            at=ingress.TimestampNanos(1_700_000_000_000_000_000))
        self.assertGreater(len(bytes(buf)), 0)

    def test_no_ws_era_names(self):
        import questdb.ingress as ingress
        for name in ('connect', 'QuestDB', 'QueryResult'):
            with self.assertRaises(AttributeError):
                getattr(ingress, name)

    def test_tagged_enum_importable(self):
        from questdb.ingress import TaggedEnum
        self.assertIs(TaggedEnum, qi.TaggedEnum)

    def test_version_reexported(self):
        import questdb.ingress as ingress
        self.assertEqual(ingress.VERSION, qi.VERSION)

    def test_warn_high_reconnects_writes_through(self):
        import questdb
        import questdb.ingress as ingress
        original = qi.WARN_HIGH_RECONNECTS
        try:
            ingress.WARN_HIGH_RECONNECTS = not original
            self.assertEqual(qi.WARN_HIGH_RECONNECTS, not original)
            self.assertEqual(ingress.WARN_HIGH_RECONNECTS, not original)
            self.assertEqual(questdb.WARN_HIGH_RECONNECTS, not original)
            questdb.WARN_HIGH_RECONNECTS = original
            self.assertEqual(ingress.WARN_HIGH_RECONNECTS, original)
        finally:
            qi.WARN_HIGH_RECONNECTS = original


class TestQueryResultFinalizer(unittest.TestCase):
    """Offline checks for the ``QueryResult.__del__`` backstop. A live
    cursor needs a real server, so the warning-emission path is pinned
    in ``system_test.py``; here we pin that the finalizer stays silent
    when there is nothing to release and never raises."""

    def _no_resource_warnings(self, caught):
        self.assertEqual(
            [w for w in caught
             if issubclass(w.category, ResourceWarning)],
            [])

    def test_closed_result_does_not_warn_at_del(self):
        import gc
        result = qi.QueryResult(qi._CursorHandle())
        result.close()
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always')
            del result
            gc.collect()
        self._no_resource_warnings(caught)

    def test_dead_cursor_result_does_not_warn_at_del(self):
        import gc
        result = qi.QueryResult(qi._CursorHandle())
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always')
            del result
            gc.collect()
        self._no_resource_warnings(caught)

    def test_del_swallows_finalizer_errors(self):
        import gc
        result = qi.QueryResult(qi._CursorHandle())
        del result._cursor_handle
        unraisable = []
        original_hook = sys.unraisablehook
        sys.unraisablehook = lambda args: unraisable.append(args)
        try:
            del result
            gc.collect()
        finally:
            sys.unraisablehook = original_hook
        self.assertEqual(unraisable, [])


class TestBufferConstruction(unittest.TestCase):
    def test_ilp_construction(self):
        with mock.patch.object(warnings, 'warn') as warn:
            buf = qi.Buffer(protocol_version=2)
        warn.assert_not_called()
        self.assertEqual(len(buf), 0)
        buf.row('tbl', columns={'x': 1}, at=qi.ServerTimestamp)
        self.assertGreater(len(bytes(buf)), 0)

    def test_not_exported_from_package(self):
        import questdb
        self.assertNotIn('Buffer', questdb.__all__)

    def test_ilp_invalid_version(self):
        for bad in (0, 4):
            with self.assertRaises(qi.QuestDBError) as cm:
                qi.Buffer(bad)
            self.assertEqual(
                cm.exception.code, qi.QuestDBErrorCode.ProtocolVersionError)

    def test_qwp_construction(self):
        buf = qi.Buffer._new_qwp()
        self.assertIsInstance(buf, qi.Buffer)
        self.assertEqual(len(buf), 0)
        self.assertGreater(buf.capacity(), 0)


class TestReinitRejected(unittest.TestCase):
    """Calling `__init__` a second time on an already-initialized native
    object must raise instead of leaking or corrupting the impl state."""

    def test_buffer_reinit(self):
        buf = qi.Buffer(protocol_version=2)
        buf.row('tbl', columns={'x': 1}, at=qi.ServerTimestamp)
        with self.assertRaisesRegex(
                qi.QuestDBError, 'already initialized') as cm:
            buf.__init__(protocol_version=2)
        self.assertEqual(
            cm.exception.code, qi.QuestDBErrorCode.InvalidApiCall)
        self.assertGreater(len(buf), 0)

    def test_sender_reinit(self):
        sender = qi.Sender('tcp', '127.0.0.1', 9009)
        with self.assertRaisesRegex(
                qi.QuestDBError, 'already initialized') as cm:
            sender.__init__('tcp', '127.0.0.1', 9009)
        self.assertEqual(
            cm.exception.code, qi.QuestDBErrorCode.InvalidApiCall)


if __name__ == '__main__':
    if os.environ.get('TEST_QUESTDB_PROFILE') == '1':
        import cProfile

        cProfile.run('unittest.main()', sort='cumtime')
    else:
        unittest.main()
