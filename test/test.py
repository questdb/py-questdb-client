#!/usr/bin/env python3
import sys

sys.dont_write_bytecode = True
import os
import unittest
from unittest import mock
import datetime
import ipaddress
import array
import ctypes
import struct
import timeit
import time
import threading
import uuid
import ast
import copy
import inspect
import json
import logging
import pickle
from enum import Enum
import random
import re
import types
import pathlib
import tempfile
import warnings
import numpy as np
import subprocess

import patch_path

from test_tools import (
    _float_binary_bytes,
    _array_binary_bytes,
    TimestampEncodingMixin)
from forged_arrow import (
    FORGED_CASES,
    FORGED_EXPECTATIONS,
    MARKER as FORGED_ARROW_MARKER,
    _RawArrowStream)

PROJ_ROOT = patch_path.PROJ_ROOT
sys.path.append(str(PROJ_ROOT / 'c-questdb-client' / 'system_test'))

from mock_server import (Server, HttpServer, SETTINGS_WITHOUT_PROTOCOL_VERSION,
                         SETTINGS_WITH_PROTOCOL_VERSION_V1, SETTINGS_WITH_PROTOCOL_VERSION_V2,
                         SETTINGS_WITH_PROTOCOL_VERSION_V1_V2_V3,SETTINGS_WITH_PROTOCOL_VERSION_V4)
from qwp_ws_ack_server import QwpAckServer, TLS_CA
import api_surface
import qwp_wire

import questdb._client as qi

if os.environ.get('TEST_QUESTDB_INTEGRATION') == '1':
    from system_test import (
        TestWithDatabase,
        TestEgressWithDatabase,
        TestEgressQwpRowTypes,
        TestEgressPool,
        TestEgressLeaks,
        TestColumnIngressNarrowTypes,
        TestColumnIngressQwpRowTypes,
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


# The QWP frame decoder lives in `qwp_wire` so the claim grid and these
# tests read a captured frame the same way. A test that asks what the
# client sent has to read the bytes, not the client's opinion of them.
_read_qwp_varint = qwp_wire.read_varint
_first_qwp_table_row_count = qwp_wire.first_table_row_count
_first_qwp_table_column_types = qwp_wire.first_table_column_types


from test_client_capsule_path import (
    TestArrowFfiProducerLayoutMatrix,
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
from test_ci_wiring import TestPinnedCiDependencies, TestProjCibuildwheelRouting
from test_dataframe_leaks import (
    TestCategoricalArrowLeak,
    TestPyobjColumnarLeak,
    TestBinaryBufferRelease,
    TestCapsuleOverridesLeak,
    TestClosedHandleDataframeLeak,
)

if pd is not None and pyarrow is not None:
    from test_dataframe import TestPandasProtocolVersionV1
    from test_dataframe import TestPandasProtocolVersionV2
    from test_dataframe import TestPandasProtocolVersionV3
    from test_dataframe import TestNaTScalarDatetime
    from test_dataframe import TestColumnarPlanWithoutPyarrow
    from test_dataframe import TestDecimalWithoutCAccelerator
    from test_dataframe import TestDecimalImpostorRefused
elif pd is None:
    class TestNoPandas(unittest.TestCase):
        def test_no_pandas(self):
            buf = qi.Buffer(protocol_version=2)
            exp = 'Missing.*`pandas`.*`numpy`.*readthedocs.*installation.html.'
            with self.assertRaisesRegex(qi.QuestDBError, exp):
                buf.dataframe(None, at=qi.ServerTimestamp)


class TestManifest(unittest.TestCase):
    def _manifest(self):
        try:
            import yaml
        except ImportError:
            self.skipTest('Python version does not support yaml')
        repo_root = pathlib.Path(__file__).parent.parent
        with open(repo_root / 'examples.manifest.yaml', 'r') as f:
            return repo_root, yaml.safe_load(f)

    def test_valid_yaml(self):
        repo_root, manifest = self._manifest()
        for entry in manifest:
            self.assertTrue(
                (repo_root / entry['path']).is_file(),
                f"manifest entry {entry['name']!r} points at a missing "
                f"file: {entry['path']}")

    def test_every_example_is_published_somewhere(self):
        """An example nobody references is an example nobody reads.

        The manifest carries the set the QuestDB docs site embeds and
        `docs/examples.rst` carries the rest, so every file in
        `examples/` has to turn up in one of them. Checked from the
        files rather than from the manifest: reading the manifest and
        asking whether each entry exists can only find an entry that
        went stale, never a file that was never listed.

        Published is not the same as run. Three of these are executed
        against a live server -- `qwp_udp.py`, `qwp_column_types.py`
        and `qwp_column_types_dataframe.py`, by tests in
        `system_test.py` -- and the other eighteen would break in
        silence. What this holds is that a reader can find each one,
        not that each one still works.
        """
        repo_root, manifest = self._manifest()
        listed = {entry['path'] for entry in manifest}
        rst = (repo_root / 'docs' / 'examples.rst').read_text()

        orphans = []
        for path in sorted((repo_root / 'examples').glob('*.py')):
            rel = f'examples/{path.name}'
            if rel in listed or f'../{rel}' in rst:
                continue
            orphans.append(rel)
        self.assertEqual(
            orphans, [],
            'these examples are in neither examples.manifest.yaml nor '
            'docs/examples.rst, so nothing points a reader at them:\n  '
            + '\n  '.join(orphans))


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
            # The refusal names the two writes that do apply overrides,
            # rather than the protocol family: `udp::` is QWP too and is
            # refused here, so "over QWP" would read as a contradiction.
            with self.assertRaisesRegex(
                    qi.QuestDBError,
                    'schema_overrides is applied when a frame is written '
                    'a column at a time'):
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


# Handing out an Arrow C stream means handing out a real PyCapsule,
# and building one from Python goes through the interpreter's own C
# API. `ctypes.pythonapi` is the only route to it, and CPython is the
# only interpreter that exposes it -- PyPy's ctypes has no such
# attribute, so every test below that hand-rolls a stream is CPython's.
_CAN_BUILD_CAPSULE = hasattr(ctypes, 'pythonapi')
_needs_capsule_builder = unittest.skipUnless(
    _CAN_BUILD_CAPSULE, 'ctypes.pythonapi not available')


class TestQwpOnlyRowTypes(unittest.TestCase):
    UUID_VALUE = uuid.UUID('123e4567-e89b-12d3-a456-426614174000')

    @_needs_capsule_builder
    def test_every_forged_arrow_shape_has_its_declared_subprocess_outcome(self):
        script = pathlib.Path(__file__).with_name('forged_arrow.py')
        self.assertEqual(
            set(FORGED_CASES), set(FORGED_EXPECTATIONS),
            'every forged Arrow case must declare its expected outcome')
        for name in FORGED_CASES:
            with self.subTest(case=name), QwpAckServer(
                    record_payloads=True) as server:
                env = os.environ.copy()
                env['QUESTDB_FORGED_ARROW_CASE'] = name
                env['QUESTDB_FORGED_ARROW_PORT'] = str(server.port)
                try:
                    child = subprocess.run(
                        [sys.executable, str(script)],
                        env=env,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        text=True,
                        timeout=10)
                except subprocess.TimeoutExpired as exc:
                    self.fail(
                        f'{name}: forged-Arrow child timed out after '
                        f'{exc.timeout} seconds')
                if child.returncode != 0:
                    self.fail(
                        f'{name}: forged-Arrow child failed\n'
                        f'returncode: {child.returncode}\n'
                        f'stdout:\n{child.stdout}\n'
                        f'stderr:\n{child.stderr}')
                if FORGED_ARROW_MARKER not in child.stdout:
                    self.fail(
                        f'{name}: forged-Arrow child did not print the '
                        f'success marker\nstdout:\n{child.stdout}\n'
                        f'stderr:\n{child.stderr}')
                frames = server.wait_binary_frames_settled()
                stats = server.snapshot()
                expected = FORGED_EXPECTATIONS[name]
                if expected['wire']:
                    self.assertGreaterEqual(
                        frames, 1,
                        f'{name}: accepted Arrow produced no binary frame')
                    data_frames = [
                        payload for payload in stats['binary_payloads']
                        if (payload[:4] == b'QWP1'
                            and int.from_bytes(payload[6:8], 'little') > 0)]
                    self.assertGreaterEqual(len(data_frames), 1)
                    self.assertEqual(
                        _first_qwp_table_row_count(data_frames[0]),
                        expected['row_count'])
                    payload = b''.join(data_frames)
                    for encoded in expected['wire_contains']:
                        self.assertIn(
                            encoded, payload,
                            f'{name}: representative encoded values missing')
                else:
                    self.assertEqual(
                        frames, 0,
                        f'{name}: rejected Arrow produced a binary frame')
                    self.assertEqual(
                        stats['binary_payloads'], [],
                        f'{name}: rejected Arrow payload reached the wire')
                    self.assertEqual(stats['binary_bytes'], 0)
                self.assertGreaterEqual(stats['accepted_connections'], 1)
                self.assertEqual(stats['errors'], [])

    @unittest.skipIf(
        pd is None or pyarrow is None,
        'pandas and pyarrow are required for real-export metadata coverage')
    def test_wide_real_pandas_pyarrow_schema_passes_arrow_preflight(self):
        column_count = 4_095
        frame = pd.DataFrame({
            f'c{i}': pd.Series([i], dtype='int64')
            for i in range(column_count)
        })
        with QwpAckServer(record_payloads=True) as server:
            conf = (
                f'ws::addr=127.0.0.1:{server.port};lazy_connect=true;'
                'sender_pool_min=1;sender_pool_max=1;pool_reap=manual;')
            with qi.QuestDB.from_conf(conf) as client:
                client.dataframe(
                    frame,
                    table_name='wide_arrow_schema',
                    at=qi.ServerTimestamp,
                    symbols=False)
            self.assertGreaterEqual(server.wait_binary_frames_settled(), 1)
            stats = server.snapshot()
        self.assertEqual(stats['errors'], [])
        self.assertGreaterEqual(stats['accepted_connections'], 1)

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

    def test_wrapper_edges_and_repr(self):
        # The four wrappers are in `docs/api.rst` through `autoclass`,
        # so every constructor branch and every `__repr__` is a surface
        # a reader can reach.
        self.assertEqual(qi.DateMillis(-2 ** 63).value, -2 ** 63)
        self.assertEqual(qi.DateMillis(2 ** 63 - 1).value, 2 ** 63 - 1)
        for value in (2 ** 63, -2 ** 63 - 1):
            with self.subTest(date_millis=value):
                with self.assertRaisesRegex(ValueError, 'signed 64-bit'):
                    qi.DateMillis(value)
        with self.assertRaisesRegex(TypeError, 'dt must be a datetime'):
            qi.DateMillis.from_datetime('2025-01-01')
        before = int(time.time() * 1000)
        now = qi.DateMillis.now()
        after = int(time.time() * 1000)
        self.assertIsInstance(now, qi.DateMillis)
        # A second of slack either way: the clock the wrapper reads and
        # `time.time()` are the same wall clock, not the same call.
        self.assertGreaterEqual(now.value, before - 1000)
        self.assertLessEqual(now.value, after + 1000)

        with self.assertRaisesRegex(TypeError, 'value must be a str'):
            qi.Geohash.from_string(5)
        single = qi.Geohash.from_string('u')
        self.assertEqual((single.bits, single.precision), (26, 5))
        widest = qi.Geohash.from_string('z' * 12)
        self.assertEqual(
            (widest.bits, widest.precision), (2 ** 60 - 1, 60))

        self.assertEqual(repr(qi.Char('Q')), "Char('Q')")
        self.assertEqual(repr(qi.DateMillis(-1)), 'DateMillis(-1)')
        self.assertEqual(repr(qi.Long256(2 ** 255)), f'Long256({2 ** 255})')
        self.assertEqual(repr(qi.Geohash(26, 5)), 'Geohash(26, 5)')

    @unittest.skipIf(pd is None, 'pandas not installed')
    @unittest.skipIf(pyarrow is None, 'pyarrow not installed')
    def test_roundtrip_claim_survives_a_pickle(self):
        # `df.to_pickle`, multiprocessing and dask all pickle the frame,
        # and pandas pickles `attrs` with it. `__reduce__` is written by
        # hand so that what comes back is a claim rather than the plain
        # dict its payload travels as -- an unpickled frame that had
        # become editable would let one holder of it change what every
        # other holder claims.
        frame = self._nullable_roundtrip_frame()
        restored = pickle.loads(pickle.dumps(frame))
        claim = restored.attrs['questdb']
        self.assertIsInstance(claim, qi._RoundtripClaim)
        self.assertEqual(claim, frame.attrs['questdb'])
        # Frozen at every depth, the same as the claim it came from.
        with self.assertRaisesRegex(TypeError, 'cannot be edited in place'):
            claim['version'] = 2
        with self.assertRaisesRegex(TypeError, 'cannot be edited in place'):
            claim['columns']['u']['kind'] = 'long256'
        self.assertIs(copy.deepcopy(claim), claim)
        # And it still reads as a claim on the way back in.
        self.assertEqual(
            self._dataframe_column_types(
                restored, table_name='attrs_pickled', at='ts')['u'],
            0x0C)

    @unittest.skipIf(pd is None, 'pandas not installed')
    @unittest.skipIf(pyarrow is None, 'pyarrow not installed')
    def test_roundtrip_claim_uuid_lands_in_wire_order(self):
        # A `uuid` claim reads the column as canonical RFC 4122
        # big-endian and byte-swaps it into QWP wire order (lo half LE,
        # then hi half LE). Forwarding the bytes unchanged would corrupt
        # every value, and two paths agreeing with each other would not
        # notice if they swapped together -- so pin the bytes.
        payload = self._dataframe_wire_payload(
            self._nullable_roundtrip_frame(),
            table_name='attrs_uuid_order', at='ts')
        self.assertIn(self.UUID_VALUE.bytes[::-1], payload)
        self.assertNotIn(self.UUID_VALUE.bytes, payload)
        # LONG256 is already little-endian limbs, low limb first, so its
        # 32 bytes go out untouched.
        self.assertIn(bytes(range(32)), payload)

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
        naive_datetime_warned = qi._NAIVE_DATETIME_WARNED
        try:
            qi._NAIVE_DATETIME_WARNED = False
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter('always')
                naive = datetime.datetime(
                    1969, 12, 31, 23, 59, 59, 999999)
                self.assertEqual(
                    qi.DateMillis.from_datetime(naive).value, -1)
            self.assertEqual(
                len([w for w in caught
                     if issubclass(w.category, UserWarning)]),
                1)
        finally:
            qi._NAIVE_DATETIME_WARNED = naive_datetime_warned

    def test_long256_ignores_overridden_to_bytes(self):
        # The column write reads exactly 32 bytes from the pointer, so the
        # unbound `int.to_bytes` is called and a subclass override cannot
        # change what is stored or how wide it is.
        def bad_int(value, result):
            return type('BadInt', (int,), {
                'to_bytes': lambda self, *a, **k: result})(value)

        for result in (b'AB', b'\x00' * 64, None, bytearray(32)):
            with self.subTest(to_bytes_returns=result):
                self.assertEqual(qi.Long256(bad_int(5, result)).value, 5)

        # A subclass that lies through its comparison operators cannot
        # get a value of the wrong magnitude through either. The value
        # is taken as a plain `int` before the range check, so the check
        # and the encoding read the same number.
        def lying_int(value):
            return type('LyingInt', (int,), {
                '__lt__': lambda self, other: False,
                '__ge__': lambda self, other: False,
                'to_bytes': lambda self, *a, **k: b'\xff' * 32})(value)

        for value in (-1, 2 ** 256):
            with self.subTest(lying=value), self.assertRaisesRegex(
                    ValueError, r'0 <= value < 2\*\*256'):
                qi.Long256(lying_int(value))

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

        # `IPv4Interface` subclasses `IPv4Address`, so the generic
        # message's list of accepted types appeared to contain the very
        # thing it had just rejected, and never mentioned the prefix
        # that is the actual reason.
        interface = ipaddress.IPv4Interface('192.0.2.1/24')
        with self.assertRaisesRegex(
                TypeError, 'carries a network prefix') as caught:
            buffer.row(
                'binary_values', columns={'value': interface},
                at=qi.TimestampNanos(10))
        self.assertNotIn('Unsupported type', str(caught.exception))
        self.assertEqual(len(buffer), before)

        # The remedy the message names has to work. It goes to its own
        # column, since `value` already holds BINARY in this buffer.
        buffer.row(
            'binary_values', columns={'addr': interface.ip},
            at=qi.TimestampNanos(11))
        self.assertGreater(len(buffer), before)

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

                # A released memoryview refuses even the `itemsize`
                # read, so this is the usual way into the
                # `invalid memoryview` branch.
                released = memoryview(bytearray(b'gone'))
                released.release()
                frame = pd.DataFrame({
                    'value': pd.Series([b'ok', released], dtype=object)})
                with self.assertRaisesRegex(
                        qi.QuestDBError,
                        "Bad column 'value' at row 1: invalid memoryview "
                        'BINARY value: .*released memoryview'):
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

    @unittest.skipIf(pd is None, 'pandas not installed')
    @unittest.skipIf(pyarrow is None, 'pyarrow not installed')
    def test_dataframe_rejects_ipv4_interface(self):
        # Both generic messages read as a contradiction here, because
        # `IPv4Interface` subclasses `IPv4Address`: the planner's names
        # the types it accepts, one of which is the parent of the thing
        # it just turned away, and the per-cell one asks for an
        # `ipaddress.IPv4Address` having been handed a subclass of
        # exactly that. Neither said anything about the prefix, which is
        # the reason. Both now do, wherever in the column it sits.
        address = ipaddress.IPv4Address('192.0.2.129')
        interface = ipaddress.IPv4Interface('192.0.2.129/24')
        reason = (
            'ipaddress.IPv4Interface carries a network prefix, which a '
            'QuestDB IPV4 column has nowhere to keep.')
        remedy = (
            "Pass the addresses instead, e.g. df['value'] = "
            "df['value'].map(lambda value: value.ip).")
        cases = (
            # The first non-null cell decides the column's type, so an
            # interface there is refused by the planner...
            ([interface], "Bad column 'value': "),
            # ...and one further down by the writer, which names the row.
            ([address, interface], "Bad column 'value' at row 1: "),
        )
        with QwpAckServer(record_payloads=True) as server:
            conf = (
                f'ws::addr=127.0.0.1:{server.port};lazy_connect=true;'
                'sender_pool_min=1;sender_pool_max=1;pool_reap=manual;')
            with qi.QuestDB.from_conf(conf) as client:
                for values, prefix in cases:
                    frame = pd.DataFrame({
                        'value': pd.Series(values, dtype=object)})
                    with self.subTest(values=values):
                        with self.assertRaises(qi.QuestDBError) as cm:
                            client.dataframe(
                                frame, table_name='ipv4_values',
                                at=qi.ServerTimestamp)
                        self.assertEqual(
                            cm.exception.code,
                            qi.QuestDBErrorCode.BadDataFrame)
                        message = str(cm.exception)
                        self.assertIn(prefix + reason, message)
                        self.assertIn(remedy, message)
                        self.assertNotIn(
                            'Unsupported object column', message)
                        self.assertNotIn(
                            'expected ipaddress.IPv4Address', message)
            stats = server.snapshot()

        self.assertEqual(stats['binary_frames'], 0)
        self.assertEqual(stats['errors'], [])

        # `row()` gives the same reason for the same cell, with the
        # remedy a single value calls for. The two are worded from one
        # string so they cannot drift apart.
        buffer = qi.Buffer._new_qwp()
        with self.assertRaises(TypeError) as caught:
            buffer.row(
                'ipv4_values', columns={'value': interface},
                at=qi.ServerTimestamp)
        self.assertIn(reason, str(caught.exception))
        self.assertIn('`value.ip`', str(caught.exception))

    @unittest.skipIf(pd is None, 'pandas not installed')
    @unittest.skipIf(pyarrow is None, 'pyarrow not installed')
    def test_ipv4_interface_remedy_quotes_an_awkward_column_name(self):
        # The remedy is a line of code naming the column, so the name
        # goes in as a repr rather than bare: a name with a space in it
        # would otherwise print advice that does not parse.
        frame = pd.DataFrame({
            'my col': pd.Series(
                [ipaddress.IPv4Interface('192.0.2.129/24')], dtype=object)})
        with QwpAckServer(record_payloads=True) as server:
            conf = (
                f'ws::addr=127.0.0.1:{server.port};lazy_connect=true;'
                'sender_pool_min=1;sender_pool_max=1;pool_reap=manual;')
            with qi.QuestDB.from_conf(conf) as client:
                with self.assertRaises(qi.QuestDBError) as cm:
                    client.dataframe(
                        frame, table_name='ipv4_values',
                        at=qi.ServerTimestamp)
        self.assertIn(
            "df['my col'] = df['my col'].map(lambda value: value.ip)",
            str(cm.exception))

    @unittest.skipIf(pd is None, 'pandas not installed')
    @unittest.skipIf(pyarrow is None, 'pyarrow not installed')
    def test_ipv4_address_subclass_accepted(self):
        """Only ``IPv4Interface`` carries a prefix the IPV4 column
        cannot hold. A plain subclass is an ordinary address and is
        accepted by the planner, by the per-cell writer, and by
        ``row()``."""
        class MyAddr(ipaddress.IPv4Address):
            pass

        addresses = [MyAddr('192.0.2.1'), ipaddress.IPv4Address('192.0.2.2')]
        with QwpAckServer(record_payloads=True) as server:
            conf = (
                f'ws::addr=127.0.0.1:{server.port};lazy_connect=true;'
                'sender_pool_min=1;sender_pool_max=1;pool_reap=manual;')
            with qi.QuestDB.from_conf(conf) as client:
                frame = pd.DataFrame({
                    'value': pd.Series(addresses, dtype=object)})
                client.dataframe(
                    frame, table_name='ipv4_values', at=qi.ServerTimestamp)
                with client.sender() as sender:
                    sender.row(
                        'ipv4_rows',
                        columns={'value': MyAddr('192.0.2.3')},
                        at=qi.ServerTimestamp)
                    sender.flush(wait=True)
            stats = server.snapshot()

        self.assertEqual(stats['errors'], [])
        self.assertGreaterEqual(stats['binary_frames'], 2)

    @unittest.skipIf(pd is None, 'pandas not installed')
    @unittest.skipIf(pyarrow is None, 'pyarrow not installed')
    def test_numpy_planner_refuses_unlabelled_fixed_size_binary(self):
        """Sixteen and thirty-two bytes are the two fixed-size binary
        widths that mean something else in QuestDB. The NumPy planner
        has no `schema_overrides` and so cannot tell "I want opaque
        bytes" from "I meant UUID and the claim did not survive
        pandas"; it refuses both rather than auto-create a table with
        the wrong column type. The refusal does not depend on the
        pyarrow version — `pa.uuid()` decides which remedies the
        message can name, not which columns are accepted — so this
        runs with the extension type present and hidden."""
        uuid_factory = getattr(pyarrow, 'uuid', None)
        for hide_uuid in (False, True):
            if hide_uuid and uuid_factory is None:
                continue
            if hide_uuid:
                del pyarrow.uuid
            try:
                for width in (16, 32):
                    with self.subTest(width=width, hide_uuid=hide_uuid):
                        # One plain numpy column sends the whole frame
                        # down the NumPy planner.
                        frame = pd.DataFrame({
                            'v': pd.Series(
                                [bytes(range(width))] * 2,
                                dtype=pd.ArrowDtype(pyarrow.binary(width))),
                            'n': [1, 2],
                            'ts': pd.to_datetime(
                                ['2025-01-01', '2025-01-02']),
                        })
                        with self.assertRaisesRegex(
                                qi.QuestDBError,
                                f"Bad column 'v': a {width}-byte "
                                'fixed_size_binary column claims no '
                                'QuestDB type'):
                            self._dataframe_column_types(
                                frame, table_name='blobs', at='ts')
            finally:
                if hide_uuid:
                    pyarrow.uuid = uuid_factory

    @unittest.skipIf(pd is None, 'pandas not installed')
    @unittest.skipIf(pyarrow is None, 'pyarrow not installed')
    def test_fixed_size_binary_refusal_names_remedies_that_work(self):
        """Every route the two refusals name is executed here and the
        wire type it produces is asserted. A message that sends someone
        to a route which cannot work on the path they are already on is
        worse than the plain "could not map" it replaced."""
        value16 = uuid.UUID(bytes=bytes(range(16)))
        value32 = bytes(range(32))
        stamps = pd.to_datetime(['2025-01-01', '2025-01-02'])

        # "pass the values as an object-dtype column of `uuid.UUID`"
        self.assertEqual(
            self._dataframe_column_types(
                pd.DataFrame({'v': pd.Series([value16] * 2, dtype=object),
                              'n': [1, 2], 'ts': stamps}),
                table_name='blobs', at='ts')['v'],
            0x0C)

        # "build the column as `pa.uuid()`"
        if hasattr(pyarrow, 'uuid'):
            self.assertEqual(
                self._dataframe_column_types(
                    pd.DataFrame({
                        'v': pd.Series([value16.bytes] * 2,
                                       dtype=pd.ArrowDtype(pyarrow.uuid())),
                        'n': [1, 2], 'ts': stamps}),
                    table_name='blobs', at='ts')['v'],
                0x0C)

        # "claim the type with `schema_overrides=...`, which needs a
        # fully Arrow-backed frame"
        for width, kind, wire in ((16, 'uuid', 0x0C), (32, 'long256', 0x0D)):
            with self.subTest(schema_overrides=kind):
                arrow_frame = pd.DataFrame({
                    'v': pd.Series(
                        [bytes(range(width))] * 2,
                        dtype=pd.ArrowDtype(pyarrow.binary(width))),
                    'n': pd.array([1, 2], dtype=pd.ArrowDtype(
                        pyarrow.int64())),
                    'ts': pd.array(stamps, dtype=pd.ArrowDtype(
                        pyarrow.timestamp('us'))),
                })
                self.assertEqual(
                    self._dataframe_column_types(
                        arrow_frame, table_name='blobs', at='ts',
                        schema_overrides={'v': kind})['v'],
                    wire)

        # "to store the bytes as BINARY, pass them as an object column
        # of bytes"
        for width in (16, 32):
            with self.subTest(binary_width=width):
                self.assertEqual(
                    self._dataframe_column_types(
                        pd.DataFrame({
                            'v': pd.Series([bytes(range(width))] * 2,
                                           dtype=object),
                            'n': [1, 2], 'ts': stamps}),
                        table_name='blobs', at='ts')['v'],
                    0x17)

    @unittest.skipIf(pd is None, 'pandas not installed')
    @unittest.skipIf(pyarrow is None, 'pyarrow not installed')
    def test_capsule_path_takes_unlabelled_fixed_size_binary_as_binary(self):
        """The Arrow columnar path makes the opposite choice, and means
        it: `schema_overrides` is there to say otherwise, so an
        unlabelled column of either width is opaque bytes rather than an
        error. This asymmetry with the NumPy planner is deliberate and
        is what `QuestDB.dataframe`'s docstring describes."""
        stamps = pd.to_datetime(['2025-01-01', '2025-01-02'])
        for width in (16, 32):
            with self.subTest(width=width):
                arrow_frame = pd.DataFrame({
                    'v': pd.Series(
                        [bytes(range(width))] * 2,
                        dtype=pd.ArrowDtype(pyarrow.binary(width))),
                    'ts': pd.array(stamps, dtype=pd.ArrowDtype(
                        pyarrow.timestamp('us'))),
                })
                self.assertEqual(
                    self._dataframe_column_types(
                        arrow_frame, table_name='blobs', at='ts')['v'],
                    0x17)

    @unittest.skipIf(pd is None, 'pandas not installed')
    @unittest.skipIf(pyarrow is None, 'pyarrow not installed')
    def test_row_ilp_planner_gets_no_qwp_advice(self):
        """The two refusals are QWP advice, and every part of it is a
        dead end on a row-serializing protocol: LONG256 and BINARY do
        not exist there, `schema_overrides` is turned down, and
        `QuestDB.dataframe()` is a different class. An ILP buffer gets
        the resolver's own message instead."""
        for width in (16, 32):
            with self.subTest(width=width):
                frame = pd.DataFrame({
                    'v': pd.Series(
                        [bytes(range(width))] * 2,
                        dtype=pd.ArrowDtype(pyarrow.binary(width))),
                    'ts': pd.to_datetime(['2025-01-01', '2025-01-02']),
                })
                with self.assertRaisesRegex(
                        qi.QuestDBError,
                        'Could not map column source type') as caught:
                    qi.Buffer(protocol_version=2).dataframe(
                        frame, table_name='blobs', at='ts')
                message = str(caught.exception)
                self.assertNotIn('schema_overrides', message)
                self.assertNotIn('QuestDB.dataframe', message)

    @unittest.skipIf(pd is None, 'pandas not installed')
    @unittest.skipIf(pyarrow is None, 'pyarrow not installed')
    def test_arrow_uuid_claim_needs_the_extension_type(self):
        """`ARROW:extension:name` written as plain field metadata stays
        on the field: `pa.Table.from_arrays` leaves the type as bare
        `fixed_size_binary(16)`, while a C-stream import rebuilds the
        extension type from the same key. A pandas `ArrowDtype` carries
        the type and no field, so only the second spelling claims UUID
        on this planner."""
        if not hasattr(pyarrow, 'uuid'):
            self.skipTest('pyarrow.uuid() not available in this build')
        md = {b'ARROW:extension:name': b'arrow.uuid',
              b'ARROW:extension:metadata': b''}
        schema = pyarrow.schema(
            [pyarrow.field('u', pyarrow.binary(16), metadata=md)])
        storage = pyarrow.array([bytes(range(16))], type=pyarrow.binary(16))
        built = pyarrow.Table.from_arrays([storage], schema=schema)
        streamed = pyarrow.RecordBatchReader.from_stream(built).read_all()

        def planner_dtype(table):
            return table.to_pandas(
                types_mapper=pd.ArrowDtype).dtypes['u'].pyarrow_dtype

        self.assertEqual(planner_dtype(built), pyarrow.binary(16))
        streamed_type = planner_dtype(streamed)
        self.assertIsInstance(streamed_type, pyarrow.BaseExtensionType)
        self.assertEqual(streamed_type.extension_name, 'arrow.uuid')

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
        date_millis = -0x0102030405060708
        long256 = 2 ** 255 + 7
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
                        'date_col': qi.DateMillis(date_millis),
                        'long256_col': qi.Long256(long256),
                        'geohash_col': qi.Geohash.from_string('u33d8'),
                    },
                    at=qi.TimestampNanos(1_700_000_000_000_000_000))
                fsn = sender.flush_and_get_fsn()
                self.assertTrue(sender.await_acked_fsn(fsn, 10000))
            stats = server.snapshot()
        self.assertEqual(stats['errors'], [])
        self.assertEqual(stats['qwp1_frames'], 1)

        payload = next(
            payload for payload in stats['binary_payloads']
            if (payload[:4] == b'QWP1'
                and int.from_bytes(payload[6:8], 'little') > 0))
        pos = 12

        delta_start, pos = _read_qwp_varint(payload, pos)
        delta_count, pos = _read_qwp_varint(payload, pos)
        self.assertEqual((delta_start, delta_count), (0, 0))

        table_name_len, pos = _read_qwp_varint(payload, pos)
        self.assertEqual(
            payload[pos:pos + table_name_len], b'qwp_row_types')
        pos += table_name_len
        row_count, pos = _read_qwp_varint(payload, pos)
        column_count, pos = _read_qwp_varint(payload, pos)
        self.assertEqual((row_count, column_count), (1, 8))

        schema = []
        for _ in range(column_count):
            name_len, pos = _read_qwp_varint(payload, pos)
            name = payload[pos:pos + name_len]
            pos += name_len
            schema.append((name, payload[pos]))
            pos += 1
        self.assertEqual(schema, [
            (b'uuid_col', 0x0C),
            (b'ipv4_col', 0x18),
            (b'binary_col', 0x17),
            (b'char_col', 0x16),
            (b'date_col', 0x0B),
            (b'long256_col', 0x0D),
            (b'geohash_col', 0x0E),
            (b'', 0x10),  # designated TIMESTAMP_NS
        ])

        def assert_dense_column(expected):
            nonlocal pos
            self.assertEqual(payload[pos], 0)  # no null bitmap
            pos += 1
            self.assertEqual(payload[pos:pos + len(expected)], expected)
            pos += len(expected)

        assert_dense_column(self.UUID_VALUE.int.to_bytes(16, 'little'))
        assert_dense_column(b'\x01\x02\x00\xc0')
        assert_dense_column(b'\x00' * 8)  # empty BINARY offsets [0, 0]
        assert_dense_column(b'Q\x00')
        assert_dense_column(date_millis.to_bytes(8, 'little', signed=True))
        assert_dense_column(b'\x07' + b'\x00' * 30 + b'\x80')
        # precision=25, then ceil(25/8) little-endian value bytes
        assert_dense_column(b'\x19\x88\x8d\xa1\x01')
        assert_dense_column(
            (1_700_000_000_000_000_000).to_bytes(
                8, 'little', signed=True))
        self.assertEqual(pos, len(payload))

    def test_non_ascii_uuid_and_ipv4_column_names(self):
        """A UUID or IPV4 value reaches C through Python — `UUID.int` is a
        slot a subclass can turn into a property, and `IPv4Address.__int__`
        is pure Python — so `row()` converts the value first and encodes
        the column name afterwards. A non-ASCII name is the case that
        matters: it lands in the string arena, whereas an ASCII name is
        borrowed straight from the `str` object."""
        class MyAddr(ipaddress.IPv4Address):
            pass

        class MyUuid(uuid.UUID):
            pass

        names = ('ũuid', 'ĩpv4')
        with QwpAckServer(record_payloads=True) as server:
            with qi.Sender.from_conf(
                    f'ws::addr=127.0.0.1:{server.port};lazy_connect=true;',
                    auto_flush=False) as sender:
                sender.row(
                    'qwp_unicode_names',
                    columns={
                        names[0]: MyUuid(str(self.UUID_VALUE)),
                        names[1]: MyAddr('192.0.2.1'),
                    },
                    at=qi.TimestampNanos(1_700_000_000_000_000_000))
                fsn = sender.flush_and_get_fsn()
                self.assertTrue(sender.await_acked_fsn(fsn, 10000))
            stats = server.snapshot()

        self.assertEqual(stats['errors'], [])
        self.assertEqual(stats['qwp1_frames'], 1)
        payload = next(
            payload for payload in stats['binary_payloads']
            if (payload[:4] == b'QWP1'
                and int.from_bytes(payload[6:8], 'little') > 0))
        for name in names:
            self.assertIn(name.encode('utf-8'), payload)
        self.assertIn(self.UUID_VALUE.int.to_bytes(16, 'little'), payload)
        self.assertIn(
            int(ipaddress.IPv4Address('192.0.2.1')).to_bytes(4, 'little'),
            payload)

    def test_clear_from_inside_uuid_or_ipv4_conversion(self):
        """The conversion of a UUID or IPV4 value can re-enter the buffer
        while the row is half-written. `clear()` refuses to run there and
        says why; the half-written row then rewinds normally and the rows
        buffered before it are untouched."""
        buffer = qi.Buffer._new_qwp()

        class HostileAddr(ipaddress.IPv4Address):
            def __int__(self):
                buffer.clear()
                return 0x01020304

        class HostileUuid(uuid.UUID):
            @property
            def int(self):
                buffer.clear()
                return 0x0102030405060708090a0b0c0d0e0f10

            @int.setter
            def int(self, value):
                pass

        for value in (HostileAddr('192.0.2.1'),
                      HostileUuid(str(self.UUID_VALUE))):
            with self.subTest(value=type(value).__name__):
                buffer.clear()
                buffer.row(
                    'good', columns={'ok': 1}, at=qi.ServerTimestamp)
                before = len(buffer)

                with self.assertRaises(qi.QuestDBError) as cm:
                    buffer.row(
                        'hostile', columns={'ñame': value},
                        at=qi.ServerTimestamp)
                self.assertEqual(
                    cm.exception.code, qi.QuestDBErrorCode.InvalidApiCall)
                self.assertIn(
                    "clear() can't be called while a row is being "
                    'written into this buffer',
                    str(cm.exception))

                self.assertEqual(len(buffer), before)
                buffer.row(
                    'good', columns={'ok': 2}, at=qi.ServerTimestamp)
                self.assertGreater(len(buffer), before)

    @unittest.skipIf(pd is None, 'pandas not installed')
    def test_dataframe_from_inside_a_row_leaves_the_row_rewindable(self):
        """`dataframe()` clears the marker the outer row rewinds to, so
        entering it from a half-written row would strand that row in the
        buffer once the row went on to fail. It is refused there, and a
        failing row rewinds exactly as it does on its own."""
        buffer = qi.Buffer._new_qwp()

        refused = []

        class HostileAddr(ipaddress.IPv4Address):
            def __int__(self):
                frame = pd.DataFrame({
                    'x': [1],
                    'ts': pd.to_datetime([0], unit='s')})
                try:
                    buffer.dataframe(
                        frame, table_name='inner', at='ts')
                except qi.QuestDBError as e:
                    # Recorded and swallowed, so the row carries on to
                    # the cell that fails it -- what this test is about
                    # is what the outer row does afterwards.
                    refused.append(e)
                return 0x01020304

        for hostile in (True, False):
            with self.subTest(hostile=hostile):
                del refused[:]
                buffer.clear()
                buffer.row('good', columns={'ok': 1}, at=qi.ServerTimestamp)
                before = len(buffer)

                value = (HostileAddr('192.0.2.1') if hostile
                         else ipaddress.IPv4Address('192.0.2.1'))
                # `object()` is unsupported, so the row fails after the
                # address cell has already been written.
                with self.assertRaises(Exception):
                    buffer.row(
                        'hostile',
                        columns={'addr': value, 'bad': object()},
                        at=qi.ServerTimestamp)

                # Both subtests fail at the same cell -- the
                # `object()` one -- so they leave the same part-written
                # row behind, and it is gone either way.
                self.assertEqual(len(refused), 1 if hostile else 0)
                self.assertEqual(len(buffer), before)
                buffer.row('good', columns={'ok': 2}, at=qi.ServerTimestamp)
                self.assertGreater(len(buffer), before)

    @unittest.skipIf(pd is None, 'pandas not installed')
    def test_dataframe_is_refused_while_a_row_is_being_written(self):
        """The refusal itself, and what it says."""
        buffer = qi.Buffer._new_qwp()
        seen = []

        class HostileAddr(ipaddress.IPv4Address):
            def __int__(self):
                frame = pd.DataFrame({
                    'x': [1],
                    'ts': pd.to_datetime([0], unit='s')})
                try:
                    buffer.dataframe(frame, table_name='inner', at='ts')
                except qi.QuestDBError as e:
                    seen.append(e)
                return 0x01020304

        buffer.row(
            'hostile', columns={'addr': HostileAddr('192.0.2.1')},
            at=qi.ServerTimestamp)
        self.assertEqual(len(seen), 1)
        self.assertEqual(seen[0].code, qi.QuestDBErrorCode.InvalidApiCall)
        self.assertIn(
            "dataframe() can't be called while a row is being "
            'written into this buffer',
            str(seen[0]))

    @unittest.skipIf(pd is None, 'pandas not installed')
    def test_clear_is_refused_while_a_dataframe_is_being_written(self):
        """The row-serializing `dataframe()` path drives the same
        native buffer as `row()`, rewind point and all. Anything it
        calls that comes back into the buffer meets the same refusal,
        and the rows buffered before it are untouched."""
        buffer = qi.Buffer(2)
        buffer.row('good', columns={'ok': 1}, at=qi.ServerTimestamp)
        before = len(buffer)
        refused = []

        class ReentrantFrame(pd.DataFrame):
            @property
            def _constructor(self):
                return ReentrantFrame

            def items(self):
                try:
                    buffer.clear()
                except qi.QuestDBError as exc:
                    refused.append(exc)
                else:
                    refused.append(None)
                return super().items()

        buffer.dataframe(
            ReentrantFrame({'a': [1, 2]}),
            table_name='frame', at=qi.ServerTimestamp)

        self.assertTrue(refused)
        for exc in refused:
            self.assertIsNotNone(exc, 'clear() was allowed mid-frame')
            self.assertEqual(
                exc.code, qi.QuestDBErrorCode.InvalidApiCall)
            self.assertIn(
                "clear() can't be called while a row is being written "
                'into this buffer', str(exc))
        self.assertGreater(len(buffer), before)
        # And the guard lifts once the frame is written.
        buffer.clear()
        self.assertEqual(len(buffer), 0)

    def test_geohash_precision_is_pinned_per_column_per_buffer(self):
        """`row()` pins a column's GEOHASH precision at the first cell
        written to it and refuses a later disagreement -- within one
        buffer's worth of rows. It knows nothing about the precision
        the server's column already has: another table in the same
        buffer, and the same table in a fresh buffer, both start over.
        That is what makes a wrong precision against an existing column
        a server-side error at flush time."""
        buffer = qi.Buffer._new_qwp()
        buffer.row(
            'pinned', columns={'g': qi.Geohash.from_string('u33d8')},
            at=qi.ServerTimestamp)
        before = len(buffer)
        with self.assertRaisesRegex(
                qi.QuestDBError, 'precision mismatch within column'):
            buffer.row(
                'pinned', columns={'g': qi.Geohash.from_string('u3')},
                at=qi.ServerTimestamp)
        self.assertEqual(len(buffer), before)

        # A different table in the same buffer carries its own pin.
        buffer.row(
            'other', columns={'g': qi.Geohash(1, 10)},
            at=qi.ServerTimestamp)

        # And the pin does not outlive the buffer.
        fresh = qi.Buffer._new_qwp()
        fresh.row(
            'pinned', columns={'g': qi.Geohash(1, 10)},
            at=qi.ServerTimestamp)
        self.assertGreater(len(fresh), 0)

    def test_clear_is_allowed_between_rows(self):
        """The guard on `clear()` only covers a row that is part-way
        through being written. Ordinary use — clearing a buffer that
        holds finished rows, and clearing one whose last `row()` call
        failed — keeps working."""
        buffer = qi.Buffer._new_qwp()
        buffer.row('t', columns={'ok': 1}, at=qi.ServerTimestamp)
        self.assertGreater(len(buffer), 0)
        buffer.clear()
        self.assertEqual(len(buffer), 0)

        with self.assertRaises(TypeError):
            buffer.row('t', columns={'bad': object()}, at=qi.ServerTimestamp)
        buffer.clear()
        self.assertEqual(len(buffer), 0)
        buffer.row('t', columns={'ok': 1}, at=qi.ServerTimestamp)
        self.assertGreater(len(buffer), 0)

    def test_close_from_inside_a_row_keeps_the_rows_already_buffered(self):
        """`close()` runs `_close()` from a `finally`, so refusing only
        the flush it attempts would still close the sender and destroy
        every finished row in the buffer -- and `close(flush=False)`
        attempts no flush to refuse. Both are refused up front."""
        for flush in (True, False):
            with self.subTest(flush=flush):
                with QwpAckServer() as server:
                    with qi.Sender.from_conf(
                            f'ws::addr=127.0.0.1:{server.port};'
                            'lazy_connect=true;auto_flush=off;') as sender:
                        refused = []

                        class HostileUuid(uuid.UUID):
                            @property
                            def int(self):
                                try:
                                    sender.close(flush=flush)
                                except qi.QuestDBError as exc:
                                    refused.append(exc)
                                else:
                                    refused.append(None)
                                return self.UUID_INT

                            @int.setter
                            def int(self, value):
                                pass

                        HostileUuid.UUID_INT = self.UUID_VALUE.int

                        sender.row(
                            'good', columns={'ok': 1}, at=qi.ServerTimestamp)
                        buffered = len(sender)
                        self.assertGreater(buffered, 0)

                        sender.row(
                            'hostile',
                            columns={
                                'value': HostileUuid(str(self.UUID_VALUE))},
                            at=qi.ServerTimestamp)

                        self.assertEqual(len(refused), 1)
                        self.assertIsNotNone(
                            refused[0], 'close() was allowed mid-row')
                        self.assertEqual(
                            refused[0].code,
                            qi.QuestDBErrorCode.InvalidApiCall)
                        self.assertIn(
                            "close() can't be called while a row is being "
                            'written into this buffer',
                            str(refused[0]))

                        # The sender is still open and still holds both
                        # rows: the finished one and the hostile one.
                        self.assertGreater(len(sender), buffered)
                        sender.flush()

    def test_flush_from_inside_a_row_keeps_the_rows_already_buffered(self):
        """A flush attempted while a row is half-written cannot succeed
        -- the native buffer refuses it -- and the failure path clears
        the internal buffer, which would take every finished row in it
        along with the part-written one. The flush is refused up front
        instead, and the buffered rows survive."""
        with QwpAckServer() as server:
            with qi.Sender.from_conf(
                    f'ws::addr=127.0.0.1:{server.port};'
                    'lazy_connect=true;auto_flush=off;') as sender:
                refused = []

                class HostileUuid(uuid.UUID):
                    @property
                    def int(self):
                        for method in ('flush', 'flush_and_get_fsn',
                                       'flush_and_keep_and_get_fsn'):
                            try:
                                getattr(sender, method)()
                            except qi.QuestDBError as exc:
                                refused.append((method, exc))
                            else:
                                refused.append((method, None))
                        return self.UUID_INT

                    @int.setter
                    def int(self, value):
                        pass

                HostileUuid.UUID_INT = self.UUID_VALUE.int

                sender.row(
                    'good', columns={'ok': 1}, at=qi.ServerTimestamp)
                buffered = len(sender)
                self.assertGreater(buffered, 0)

                sender.row(
                    'hostile',
                    columns={'value': HostileUuid(str(self.UUID_VALUE))},
                    at=qi.ServerTimestamp)

                self.assertEqual(len(refused), 3)
                for method, exc in refused:
                    self.assertIsNotNone(
                        exc, f'{method}() was allowed mid-row')
                    self.assertEqual(
                        exc.code, qi.QuestDBErrorCode.InvalidApiCall)
                    self.assertIn(
                        "can't be called while a row is being written",
                        str(exc))
                # The row that ran the hostile conversion completed, and
                # the row buffered before it is still there.
                self.assertGreater(len(sender), buffered)
                sender.flush()

    def test_lease_cannot_be_closed_or_flushed_from_inside_a_row(self):
        """A pooled lease holds the only reference to its buffer, so
        returning it to the pool from inside a UUID or IPV4 conversion
        would free the buffer the row is still writing into. Every entry
        point that flushes or releases the lease refuses while a row is
        part-way through, and the rows buffered before it survive."""
        with QwpAckServer() as server:
            conf = (
                f'ws::addr=127.0.0.1:{server.port};lazy_connect=true;'
                'sender_pool_min=1;sender_pool_max=1;pool_reap=manual;')
            with qi.QuestDB.from_conf(conf) as client:
                sender = client.sender()
                reentered = []

                def reenter(method):
                    try:
                        getattr(sender, method)()
                    except qi.QuestDBError as exc:
                        reentered.append((method, exc))
                    else:
                        reentered.append((method, None))

                class HostileAddr(ipaddress.IPv4Address):
                    def __int__(self):
                        reenter('close')
                        reenter('flush')
                        reenter('flush_and_get_fsn')
                        reenter('flush_and_keep_and_get_fsn')
                        return 0x01020304

                class HostileUuid(uuid.UUID):
                    @property
                    def int(self):
                        reenter('close')
                        reenter('flush')
                        return 0x0102030405060708090a0b0c0d0e0f10

                    @int.setter
                    def int(self, value):
                        pass

                sender.row(
                    'good', columns={'ok': 1}, at=qi.ServerTimestamp)
                self.assertEqual(len(sender), 1)

                for value in (HostileAddr('192.0.2.1'),
                              HostileUuid(str(self.UUID_VALUE))):
                    with self.subTest(value=type(value).__name__):
                        reentered.clear()
                        try:
                            sender.row(
                                'hostile', columns={'value': value},
                                at=qi.ServerTimestamp)
                        except qi.QuestDBError:
                            pass
                        self.assertTrue(reentered)
                        for method, exc in reentered:
                            self.assertIsNotNone(
                                exc, f'{method}() was allowed mid-row')
                            self.assertEqual(
                                exc.code,
                                qi.QuestDBErrorCode.InvalidApiCall)
                            self.assertIn(
                                "can't be called while a row is being "
                                "written",
                                str(exc))

                self.assertGreaterEqual(len(sender), 1)
                sender.flush()
                sender.close()

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

    @unittest.skipIf(pd is None, 'pandas not installed')
    @unittest.skipIf(pyarrow is None, 'pyarrow not installed')
    def test_dataframe_uuid_ignores_replaced_int_to_bytes(self):
        # `UUID.int` is a slot a caller can overwrite, and the column build
        # copies exactly 16 bytes out of whatever `to_bytes` hands back. The
        # unbound `int.to_bytes` keeps a short return from narrowing the copy
        # and putting the bytes that follow the object on the wire.
        class ShortInt(int):
            def to_bytes(self, *a, **k):
                return b''

        def capture(value):
            with QwpAckServer(record_payloads=True) as server:
                conf = (
                    f'ws::addr=127.0.0.1:{server.port};lazy_connect=true;'
                    'sender_pool_min=1;sender_pool_max=1;pool_reap=manual;')
                frame = pd.DataFrame({
                    'value': pd.Series([value], dtype=object)})
                with qi.QuestDB.from_conf(conf) as client:
                    client.dataframe(
                        frame, table_name='uuid_wire', at=qi.ServerTimestamp)
                return next(
                    payload
                    for payload in server.snapshot()['binary_payloads']
                    if int.from_bytes(payload[6:8], 'little') > 0)

        poisoned = uuid.UUID(str(self.UUID_VALUE))
        object.__setattr__(poisoned, 'int', ShortInt(poisoned.int))
        # The cell still passes `isinstance(cell, uuid.UUID)`, and the bound
        # call it would otherwise reach returns nothing to copy.
        self.assertIs(type(poisoned), uuid.UUID)
        self.assertEqual(poisoned.int.to_bytes(16, 'big'), b'')

        self.assertEqual(capture(poisoned), capture(self.UUID_VALUE))

    def _dataframe_wire_payload(self, df, **kwargs):
        kwargs.setdefault('table_name', 'date_wire')
        with QwpAckServer(record_payloads=True) as server:
            conf = (
                f'ws::addr=127.0.0.1:{server.port};lazy_connect=true;'
                'sender_pool_min=1;sender_pool_max=1;pool_reap=manual;')
            with qi.QuestDB.from_conf(conf) as client:
                client.dataframe(df, **kwargs)
            stats = server.snapshot()
        self.assertEqual(stats['errors'], [])
        return next(
            payload for payload in stats['binary_payloads']
            if int.from_bytes(payload[6:8], 'little') > 0)

    def _dataframe_column_types(self, df, **kwargs):
        return dict(_first_qwp_table_column_types(
            self._dataframe_wire_payload(df, **kwargs)))

    @unittest.skipIf(pd is None, 'pandas not installed')
    @unittest.skipIf(pyarrow is None, 'pyarrow not installed')
    def test_dataframe_date_columns_wire_types(self):
        # A DataFrame claims DATE through the column's Arrow type, so
        # millisecond timestamps and both Arrow date types land as DATE
        # while microsecond timestamps stay TIMESTAMP. There is no DATE
        # cell type and no 'date' kind for `schema_overrides`, and the
        # NumPy planner has no route to DATE at all.
        millis = 1704164645678
        micros = millis * 1000
        types = self._dataframe_column_types(
            pyarrow.table({
                'ms': pyarrow.array([millis], pyarrow.timestamp('ms')),
                'ms_utc': pyarrow.array(
                    [millis], pyarrow.timestamp('ms', 'UTC')),
                'd32': pyarrow.array([19724], pyarrow.date32()),
                'd64': pyarrow.array([millis], pyarrow.date64()),
                'us': pyarrow.array([micros], pyarrow.timestamp('us')),
                'ts': pyarrow.array([micros], pyarrow.timestamp('us')),
            }),
            at='ts')
        self.assertEqual(types['ms'], 0x0B)
        self.assertEqual(types['ms_utc'], 0x0B)
        self.assertEqual(types['d32'], 0x0B)
        self.assertEqual(types['d64'], 0x0B)
        self.assertEqual(types['us'], 0x0A)
        # The designated timestamp is a TIMESTAMP whatever its unit.
        self.assertEqual(types[''], 0x0A)
        self.assertEqual(
            self._dataframe_column_types(
                pyarrow.table({
                    'v': pyarrow.array([1]),
                    'ts': pyarrow.array([millis], pyarrow.timestamp('ms')),
                }),
                at='ts')[''],
            0x0A)

        # A fully Arrow-backed pandas frame takes the same route.
        stamps = pd.to_datetime([millis], unit='ms')
        arrow_frame = pd.DataFrame({
            'ms': pd.array(
                stamps, dtype=pd.ArrowDtype(pyarrow.timestamp('ms'))),
            'ts': pd.array(
                stamps, dtype=pd.ArrowDtype(pyarrow.timestamp('us'))),
        })
        self.assertEqual(
            self._dataframe_column_types(arrow_frame, at='ts')['ms'], 0x0B)

        # The NumPy planner widens `datetime64[ms]` to a microsecond
        # TIMESTAMP, and refuses the tz-aware dtype outright.
        numpy_frame = pd.DataFrame({
            'ms': stamps.astype('datetime64[ms]'),
            'ts': stamps.astype('datetime64[us]'),
        })
        self.assertEqual(
            self._dataframe_column_types(numpy_frame, at='ts')['ms'], 0x0A)
        tz_frame = pd.DataFrame({
            'ms': stamps.tz_localize('UTC').astype('datetime64[ms, UTC]'),
            'ts': stamps.tz_localize('UTC').astype('datetime64[us, UTC]'),
        })
        with self.assertRaisesRegex(
                qi.QuestDBError, r'datetime64\[ms, UTC\] unit ms'):
            self._dataframe_column_types(tz_frame, at='ts')

        # `schema_overrides` has no 'date' kind: the Arrow type is the
        # only claim.
        with self.assertRaisesRegex(ValueError, r"kind 'date' not in"):
            self._dataframe_column_types(
                pyarrow.table({
                    'ms': pyarrow.array([micros], pyarrow.timestamp('us')),
                    'ts': pyarrow.array([micros], pyarrow.timestamp('us')),
                }),
                at='ts', schema_overrides={'ms': 'date'})

    @unittest.skipIf(pyarrow is None, 'pyarrow not installed')
    @unittest.skipIf(pd is None, 'pandas not installed')
    def test_arrow_egress_metadata_becomes_roundtrip_attrs(self):
        # The read side of the pandas round trip: the QuestDB claims the
        # egress stamps on each Arrow field are copied into
        # `df.attrs['questdb']`, spelled the way the native numpy path
        # spells them, because a pandas dtype carries the Arrow type and
        # not the field it came on.
        def field(name, arrow_type, **md):
            return pyarrow.field(
                name, arrow_type,
                metadata={k.encode(): v.encode() for k, v in md.items()})

        schema = pyarrow.schema([
            field('u', pyarrow.binary(16),
                  **{'questdb.column_type': 'uuid',
                     'ARROW:extension:name': 'arrow.uuid'}),
            field('l', pyarrow.binary(32),
                  **{'questdb.column_type': 'long256'}),
            field('ip', pyarrow.uint32(), **{'questdb.column_type': 'ipv4'}),
            field('ch', pyarrow.uint16(), **{'questdb.column_type': 'char'}),
            field('gh', pyarrow.int32(),
                  **{'questdb.column_type': 'geohash',
                     'questdb.geohash_bits': '20'}),
            field('sym', pyarrow.dictionary(pyarrow.int32(), pyarrow.utf8()),
                  **{'questdb.column_type': 'symbol',
                     'questdb.symbol': 'true'}),
            field('tns', pyarrow.timestamp('ns', 'UTC'),
                  **{'questdb.column_type': 'timestamp_nanos'}),
            field('dec', pyarrow.decimal128(38, 4),
                  **{'questdb.column_type': 'decimal128'}),
            pyarrow.field('plain', pyarrow.int64()),
        ])
        self.assertEqual(
            qi._debug_arrow_roundtrip_columns_meta(schema),
            {
                'u': {'kind': 'uuid'},
                'l': {'kind': 'long256'},
                'ip': {'kind': 'ipv4'},
                'ch': {'kind': 'char'},
                'gh': {'kind': 'geohash', 'precision_bits': 20},
                'sym': {'kind': 'symbol'},
                # The reader reports these two kinds under shorter names;
                # `attrs` uses the reader's spelling on either backend.
                'tns': {'kind': 'timestamp_ns'},
                'dec': {'kind': 'decimal', 'scale': 4},
            })
        # A field with no QuestDB claim contributes no entry.
        self.assertNotIn('plain', qi._debug_arrow_roundtrip_columns_meta(schema))

    @unittest.skipIf(pd is None, 'pandas not installed')
    @unittest.skipIf(pyarrow is None, 'pyarrow not installed')
    def test_roundtrip_attrs_restore_column_types(self):
        # The write side of the pandas round trip. An Arrow-backed frame
        # takes the capsule path, where the field metadata is already
        # gone, so `df.attrs['questdb']` is the only thing left claiming
        # the column types. Without it these five land as BINARY and
        # plain integers on a table the server auto-creates.
        frame = self._roundtrip_frame()
        types = self._dataframe_column_types(
            frame, table_name='attrs_round_trip', at='ts')
        self.assertEqual(types['u'], 0x0C)
        self.assertEqual(types['l'], 0x0D)
        self.assertEqual(types['ip'], 0x18)
        self.assertEqual(types['ch'], 0x16)
        self.assertEqual(types['gh'], 0x0E)

        # Drop the metadata and the same frame degrades — this is what
        # the claim is buying.
        bare = frame.copy()
        bare.attrs = {}
        types = self._dataframe_column_types(
            bare, table_name='attrs_round_trip', at='ts')
        self.assertEqual(types['u'], 0x17)
        self.assertEqual(types['l'], 0x17)
        self.assertEqual(types['ip'], 0x05)
        self.assertEqual(types['ch'], 0x04)
        self.assertEqual(types['gh'], 0x05)

    @unittest.skipIf(pd is None, 'pandas not installed')
    @unittest.skipIf(pyarrow is None, 'pyarrow not installed')
    def test_roundtrip_attrs_restore_types_on_a_nullable_frame(self):
        # `to_pandas(dtype_backend='numpy_nullable')` returns IPV4, CHAR
        # and GEOHASH as pandas masked columns — which normalisation
        # turns into object-dtype Python ints — and UUID and LONG256 as
        # object columns of `bytes`. None of those shapes claims a
        # QuestDB type on its own, so `df.attrs['questdb']` is the whole
        # claim, and it has to survive the object columns to be worth
        # attaching.
        nullable = self._nullable_roundtrip_frame()
        types = self._dataframe_column_types(
            nullable, table_name='attrs_round_trip', at='ts')
        self.assertEqual(types['u'], 0x0C)
        self.assertEqual(types['l'], 0x0D)
        self.assertEqual(types['ip'], 0x18)
        self.assertEqual(types['ch'], 0x16)
        self.assertEqual(types['gh'], 0x0E)

        # Same rows, same claims, same bytes: a result read back with
        # `numpy_nullable` and one read with `pyarrow` write the same
        # frame out, byte for byte, though they take different planners.
        self.assertEqual(
            self._dataframe_wire_payload(
                nullable, table_name='attrs_round_trip', at='ts'),
            self._dataframe_wire_payload(
                self._roundtrip_frame(), table_name='attrs_round_trip',
                at='ts'))

        # Drop the claims and all five degrade.
        bare = nullable.copy()
        bare.attrs = {}
        types = self._dataframe_column_types(
            bare, table_name='attrs_round_trip', at='ts')
        self.assertEqual(types['u'], 0x17)
        self.assertEqual(types['l'], 0x17)
        self.assertEqual(types['ip'], 0x05)
        self.assertEqual(types['ch'], 0x05)
        self.assertEqual(types['gh'], 0x05)

    @unittest.skipIf(pd is None, 'pandas not installed')
    @unittest.skipIf(pyarrow is None, 'pyarrow not installed')
    def test_roundtrip_claim_refuses_a_value_that_is_not_one(self):
        # An object column carries no width or range of its own, so the
        # claim is checked value by value. A value the claimed type
        # cannot hold is refused rather than written into a column of
        # that type.
        frame = self._nullable_roundtrip_frame()
        cases = (
            ('ip', [2 ** 32], 'IPV4 values must be in the range'),
            ('ch', [70000], 'CHAR values must be in the range'),
            ('u', [b'short'], 'a UUID value is exactly 16 bytes'),
            ('l', [b'short'], 'a LONG256 value is exactly 32 bytes'),
        )
        for name, values, message in cases:
            with self.subTest(column=name):
                odd = frame.copy()
                odd[name] = pd.array(values, dtype=object)
                odd.attrs = dict(frame.attrs)
                with self.assertRaisesRegex(qi.QuestDBError, message):
                    self._dataframe_column_types(
                        odd, table_name='attrs_round_trip', at='ts')

    @unittest.skipIf(pd is None, 'pandas not installed')
    @unittest.skipIf(pyarrow is None, 'pyarrow not installed')
    def test_roundtrip_attrs_survive_a_frame_that_mixes_backends(self):
        # One NumPy column routes the whole frame to the manual planner,
        # where the Arrow-backed columns are the ones carrying claims.
        # `df = result.to_pandas(dtype_backend='pyarrow')` followed by
        # `df['x'] = ...` is the ordinary shape of a read-modify-write,
        # and the five have to keep their types through it.
        frame = self._roundtrip_frame()
        mixed = frame.copy()
        mixed['x'] = np.arange(len(mixed), dtype=np.int64)
        mixed.attrs = dict(frame.attrs)
        types = self._dataframe_column_types(
            mixed, table_name='attrs_round_trip', at='ts')
        self.assertEqual(types['u'], 0x0C)
        self.assertEqual(types['l'], 0x0D)
        self.assertEqual(types['ip'], 0x18)
        self.assertEqual(types['ch'], 0x16)
        self.assertEqual(types['gh'], 0x0E)
        self.assertEqual(types['x'], 0x05)

        # And the same rows read back with the other backend, plus the
        # same added column, still write the same bytes.
        nullable = self._nullable_roundtrip_frame()
        mixed_nullable = nullable.copy()
        mixed_nullable['x'] = np.arange(len(nullable), dtype=np.int64)
        mixed_nullable.attrs = dict(nullable.attrs)
        self.assertEqual(
            self._dataframe_wire_payload(
                mixed, table_name='attrs_round_trip', at='ts'),
            self._dataframe_wire_payload(
                mixed_nullable, table_name='attrs_round_trip', at='ts'))

        # Drop the claims and the two fixed-size binary columns state
        # no type at all, which this planner refuses rather than guesses
        # at -- the asymmetry with the Arrow path, which takes them as
        # BINARY because `schema_overrides` gives a way to say otherwise
        # there.
        bare = mixed.copy()
        bare.attrs = {}
        with self.assertRaisesRegex(
                qi.QuestDBError, '16-byte fixed_size_binary column'):
            self._dataframe_column_types(
                bare, table_name='attrs_round_trip', at='ts')

        # The three integer-backed ones degrade quietly to the types
        # their storage implies, which is what the claim is buying.
        bare = bare.drop(columns=['u', 'l'])
        bare.attrs = {}
        types = self._dataframe_column_types(
            bare, table_name='attrs_round_trip', at='ts')
        self.assertEqual(types['ip'], 0x05)
        self.assertEqual(types['ch'], 0x04)
        self.assertEqual(types['gh'], 0x05)

    @unittest.skipIf(pd is None, 'pandas not installed')
    @unittest.skipIf(pyarrow is None, 'pyarrow not installed')
    def test_roundtrip_attrs_on_a_mixed_frame_keep_the_zero_copy_shapes(self):
        # Arrow uint32 and int64 resolve to the integer target, so they
        # reach the wire as raw buffers and carry their claim on the
        # dtype the append call names -- no object-dtype copy. A
        # `pa.uuid()` column states its own type to the native importer
        # and needs no claim at all. All three have to keep working on a
        # frame that also holds a NumPy column.
        frame = self._roundtrip_frame()
        mixed = frame.copy()
        mixed['gh'] = pd.array(
            [100], dtype=pd.ArrowDtype(pyarrow.int64()))
        mixed['x'] = np.arange(len(mixed), dtype=np.int64)
        mixed.attrs = dict(frame.attrs)
        types = self._dataframe_column_types(
            mixed, table_name='attrs_round_trip', at='ts')
        self.assertEqual(types['ip'], 0x18)
        self.assertEqual(types['gh'], 0x0E)

        if hasattr(pyarrow, 'uuid'):
            ext = mixed.copy()
            ext['u'] = pd.array(
                [self.UUID_VALUE.bytes], dtype=pd.ArrowDtype(pyarrow.uuid()))
            ext.attrs = dict(frame.attrs)
            self.assertEqual(
                self._dataframe_column_types(
                    ext, table_name='attrs_round_trip', at='ts')['u'],
                0x0C)

    def _geohash_frame(self, values, dtype, bits=20, mixed=False):
        """A minimal claimed-GEOHASH frame in one storage shape.

        ``mixed`` adds a NumPy column, which routes the frame off the
        zero-copy Arrow path and onto the manual planner. The two encode
        through different native paths, so both are worth exercising.
        """
        frame = pd.DataFrame({
            'gh': pd.array(values, dtype=dtype),
            'ts': pd.array(
                [datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc)]
                * len(values),
                dtype=pd.ArrowDtype(pyarrow.timestamp('us', 'UTC'))),
        })
        if mixed:
            frame['x'] = np.arange(len(values), dtype=np.int64)
        frame.attrs['questdb'] = {'version': 1, 'columns': {
            'gh': {'kind': 'geohash', 'precision_bits': bits}}}
        return frame

    @unittest.skipIf(pd is None, 'pandas not installed')
    @unittest.skipIf(pyarrow is None, 'pyarrow not installed')
    def test_roundtrip_claim_forwards_arbitrary_geohash_values(self):
        # Bulk GEOHASH claims validate the precision and carrier width,
        # not individual values. Every supported integer shape and both
        # planners therefore forward values outside the logical range.
        shapes = (
            ('arrow int8', pd.ArrowDtype(pyarrow.int8()), 4),
            ('arrow int16', pd.ArrowDtype(pyarrow.int16()), 12),
            ('arrow int32', pd.ArrowDtype(pyarrow.int32()), 20),
            ('arrow int64', pd.ArrowDtype(pyarrow.int64()), 20),
            ('numpy int64', np.int64, 20),
            ('numpy int32', np.int32, 20),
            ('numpy int16', np.int16, 12),
            ('object int', object, 20),
        )
        for label, dtype, bits in shapes:
            for mixed in (False, True):
                with self.subTest(shape=label, mixed=mixed):
                    self.assertEqual(
                        self._dataframe_column_types(
                            self._geohash_frame(
                                [-1, 1 << bits], dtype, bits, mixed=mixed),
                            table_name='attrs_round_trip', at='ts')['gh'],
                        0x0E)

    @unittest.skipIf(pd is None, 'pandas not installed')
    @unittest.skipIf(pyarrow is None, 'pyarrow not installed')
    def test_geohash_bulk_values_and_nulls_are_forwarded(self):
        for mixed in (False, True):
            with self.subTest(mixed=mixed):
                self.assertEqual(
                    self._dataframe_column_types(
                        self._geohash_frame(
                            [None, 3, 1 << 40, -1],
                            pd.ArrowDtype(pyarrow.int64()), mixed=mixed),
                        table_name='attrs_round_trip', at='ts')['gh'],
                    0x0E)

    @unittest.skipIf(pd is None, 'pandas not installed')
    @unittest.skipIf(pyarrow is None, 'pyarrow not installed')
    def test_geohash_widest_precision_forwards_excess_bits(self):
        top = (1 << 60) - 1
        for mixed in (False, True):
            with self.subTest(mixed=mixed):
                self.assertEqual(
                    self._dataframe_column_types(
                        self._geohash_frame(
                            [top, top + 1, -1],
                            pd.ArrowDtype(pyarrow.int64()),
                            bits=60, mixed=mixed),
                        table_name='attrs_round_trip', at='ts')['gh'],
                    0x0E)

    @unittest.skipIf(pd is None, 'pandas not installed')
    @unittest.skipIf(pyarrow is None, 'pyarrow not installed')
    def test_schema_overrides_outrank_a_geohash_claim(self):
        # The override still decides the precision on the wire, but no
        # chosen precision triggers a per-value range scan.
        frame = self._geohash_frame(
            [1000, 2000], pd.ArrowDtype(pyarrow.int32()), bits=5)
        for override in (None, {'gh': ('geohash', 10)},
                         {'gh': ('geohash', 32)}):
            with self.subTest(override=override):
                self.assertEqual(
                    self._dataframe_column_types(
                        frame, table_name='attrs_round_trip', at='ts',
                        schema_overrides=override)['gh'],
                    0x0E)

    @unittest.skipIf(pd is None, 'pandas not installed')
    @unittest.skipIf(pyarrow is None, 'pyarrow not installed')
    def test_a_column_with_too_few_arrow_buffers_is_refused(self):
        """pyarrow allocates exactly `n_buffers` pointers, so reading
        the value buffer of a struct array (1) or a null array (0)
        reads past the allocation, and whatever is there decides
        whether the column is accepted. The refusal has to come from
        the buffer count instead, the same answer every time."""
        stamp = datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc)
        arrays = (
            ('struct', pyarrow.array(
                [{'a': 1}], pyarrow.struct([('a', pyarrow.int64())]))),
            ('null', pyarrow.nulls(1, pyarrow.null())))
        for label, array in arrays:
            frame = pd.DataFrame({
                'v': pd.array(array, dtype=pd.ArrowDtype(array.type)),
                'ts': pd.array(
                    [stamp],
                    dtype=pd.ArrowDtype(pyarrow.timestamp('us', 'UTC'))),
                # One NumPy column routes the frame onto the manual
                # planner, which is the one that reads the buffers.
                'x': np.arange(1, dtype=np.int64)})
            seen = set()
            for _ in range(20):
                with self.assertRaises(
                        qi.UnsupportedDataFrameShapeError) as caught:
                    self._dataframe_column_types(
                        frame, table_name='too_few_buffers', at='ts')
                seen.add(str(caught.exception))
            with self.subTest(array=label):
                self.assertEqual(len(seen), 1, seen)

    @unittest.skipIf(pyarrow is None, 'pyarrow not installed')
    @unittest.skipIf(pd is None, 'pandas not installed')
    def test_geohash_claim_at_the_cap_reaches_its_widest_value(self):
        # A signed carrier preserves its raw sign bit, so byte-aligned
        # precisions use the full width. The all-ones representation is
        # written as -1 by NumPy/Arrow and remains data via the explicit
        # validity bitmap.
        caps = (
            (pyarrow.int8(), np.int8, 8),
            (pyarrow.int16(), np.int16, 16),
            (pyarrow.int32(), np.int32, 32),
            (pyarrow.int64(), np.int64, 60))
        for arrow_ty, numpy_ty, bits in caps:
            for label, dtype in (('arrow', pd.ArrowDtype(arrow_ty)),
                                 ('numpy', numpy_ty)):
                for mixed in (False, True):
                    with self.subTest(width=str(arrow_ty), bits=bits,
                                      backing=label, mixed=mixed):
                        self.assertEqual(
                            self._dataframe_column_types(
                                self._geohash_frame(
                                    [0, -1], dtype, bits, mixed=mixed),
                                table_name='geo_cap', at='ts')['gh'],
                            0x0E)

    @unittest.skipIf(pd is None, 'pandas not installed')
    @unittest.skipIf(pyarrow is None, 'pyarrow not installed')
    def test_geohash_claim_wider_than_the_column_is_dropped(self):
        # A precision the column's own width cannot hold is a claim
        # that no longer fits, and a claim that no longer fits is
        # dropped rather than carried to the wire. Both planners have
        # to answer that the same way: the same frame differing only in
        # dtype backend cannot land as GEOHASH on one and as a plain
        # integer -- or a mid-flush error -- on the other.
        widths = (
            (pyarrow.int8(), 8),
            (pyarrow.int16(), 16),
            (pyarrow.int32(), 32),
            (pyarrow.int64(), 60))
        for ty, cap in widths:
            for bits, fits in ((cap, True), (cap + 1, False)):
                if bits > 60:
                    continue
                seen = {}
                for mixed in (False, True):
                    planner = 'numpy' if mixed else 'arrow'
                    with self.subTest(width=str(ty), bits=bits,
                                      planner=planner):
                        # `[0, 1]` sits inside every precision here, so
                        # the only thing under test is the claim.
                        seen[planner] = self._dataframe_column_types(
                            self._geohash_frame(
                                [0, 1], pd.ArrowDtype(ty), bits=bits,
                                mixed=mixed),
                            table_name='geo_claim_width', at='ts')['gh']
                        if fits:
                            self.assertEqual(seen[planner], 0x0E)
                        else:
                            self.assertNotEqual(seen[planner], 0x0E)
                self.assertEqual(
                    seen['arrow'], seen['numpy'],
                    f'the two planners disagree on a {ty} column '
                    f'claimed at {bits} bits')

    @unittest.skipIf(pd is None, 'pandas not installed')
    @unittest.skipIf(pyarrow is None, 'pyarrow not installed')
    def test_claim_a_column_type_cannot_carry_is_said_out_loud(self):
        """A claim the column's type can never carry is announced.

        The write still goes ahead as the column's own type implies --
        a claim is a recollection, and a frame retyped since it was read
        must not fail on that account. But an unsigned integer can never
        carry a ``geohash``: the native Arrow importer takes the claim on
        a signed column only, so the claim is guaranteed to do nothing.
        That is a mistake rather than drift, and the two are
        indistinguishable unless the write says which claim it dropped.
        """
        shapes = (
            ('arrow-backed frame', pd.ArrowDtype(pyarrow.uint32()), False),
            ('arrow column, mixed frame',
             pd.ArrowDtype(pyarrow.uint32()), True),
            ('numpy column', np.dtype(np.uint32), False),
            ('numpy column, mixed frame', np.dtype(np.uint32), True),
        )
        for label, dtype, mixed in shapes:
            with self.subTest(shape=label):
                frame = self._geohash_frame(
                    [0, 1], dtype, bits=20, mixed=mixed)
                with self.assertLogs('questdb', level='WARNING') as caught:
                    types = self._dataframe_column_types(
                        frame, table_name='geo_unsigned', at='ts')
                # 0x0E is GEOHASH: the claim really was dropped.
                self.assertNotEqual(types['gh'], 0x0E)
                said = [
                    record.getMessage() for record in caught.records
                    if "df.attrs['questdb']" in record.getMessage()]
                self.assertEqual(len(said), 1, said)
                self.assertIn("column 'gh'", said[0])
                self.assertIn("'geohash'", said[0])
                self.assertIn('uint32', said[0])

    @unittest.skipIf(pd is None, 'pandas not installed')
    @unittest.skipIf(pyarrow is None, 'pyarrow not installed')
    def test_a_claim_the_column_carries_or_has_outlived_stays_quiet(self):
        """Silence is kept for the two cases that are not mistakes.

        A claim the column does carry has nothing to report, and a claim
        whose column is gone is the schema drift the claim is designed
        to survive without a word.
        """
        stamp = datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc)
        cases = (
            ('claim is carried', self._geohash_frame(
                [0, 1], pd.ArrowDtype(pyarrow.int32()), bits=20)),
        )
        # A frame whose claimed column is no longer in it at all.
        dropped = pd.DataFrame({
            'other': pd.array([1, 2], dtype=pd.ArrowDtype(pyarrow.int64())),
            'ts': pd.array(
                [stamp] * 2,
                dtype=pd.ArrowDtype(pyarrow.timestamp('us', 'UTC'))),
        })
        dropped.attrs['questdb'] = {'version': 1, 'columns': {
            'gh': {'kind': 'geohash', 'precision_bits': 20}}}
        cases += (('claimed column is gone', dropped),)

        for label, frame in cases:
            with self.subTest(case=label):
                with self.assertNoLogs('questdb', level='WARNING'):
                    self._dataframe_column_types(
                        frame, table_name='geo_quiet', at='ts')

    def _geohash_arrow_table(self, values, bits=None, ty=None, md=None):
        """A GEOHASH frame as a `pa.Table`, claiming the type through
        Arrow field metadata when `bits` is given. `md` carries the
        field metadata verbatim instead, for the claims that are not
        the pair of keys `bits` writes."""
        if md is None and bits is not None:
            md = {b'questdb.column_type': b'geohash',
                  b'questdb.geohash_bits': str(bits).encode()}
        stamp = datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc)
        ty = ty or pyarrow.int32()
        schema = pyarrow.schema([
            pyarrow.field('gh', ty, metadata=md),
            pyarrow.field('ts', pyarrow.timestamp('us', 'UTC'))])
        return pyarrow.table(
            [pyarrow.array(values, ty),
             pyarrow.array([stamp] * len(values),
                           pyarrow.timestamp('us', 'UTC'))],
            schema=schema)

    def _raw_geohash_stream(self, values, row_count=None,
                            batch_offset=0, bits=b'5', claimed=True,
                            **column):
        """A hand-rolled Arrow stream of one GEOHASH column and its
        designated timestamp."""
        stamp = struct.pack('<q', 1735689600000000)
        metadata = {b'questdb.column_type': b'geohash',
                    b'questdb.geohash_bits': bits} if claimed else None
        columns = [
            dict(format=b'i', name=b'gh',
                 data=struct.pack('<%di' % len(values), *values),
                 metadata=metadata, **column),
            dict(format=b'tsu:UTC', name=b'ts',
                 data=stamp * len(values))]
        if row_count is None:
            row_count = len(values) - batch_offset
        return _RawArrowStream(columns, row_count, batch_offset)

    def _date_claim_frame(self, series, claim=True):
        stamp = pd.to_datetime([1704164645678000], unit='us', utc=True)
        df = pd.DataFrame({'d': series, 'ts': stamp})
        if claim:
            df.attrs['questdb'] = {
                'version': 1, 'columns': {'d': {'kind': 'date'}}}
        return df

    @unittest.skipIf(pd is None, 'pandas not installed')
    @unittest.skipIf(pyarrow is None, 'pyarrow not installed')
    def test_roundtrip_claim_restores_a_date_column(self):
        # Plain `to_pandas()` hands a DATE column back as a NumPy
        # `datetime64[ms]`, which has no route of its own to DATE and
        # widens to a microsecond TIMESTAMP. Writing such a frame back
        # therefore created the destination table with a TIMESTAMP
        # column where the source had DATE, and said nothing. The claim
        # names the column DATE, and that is what puts the Arrow type
        # back on it.
        millis = 1704164645678
        naive = pd.Series(numpy.array([millis], dtype='datetime64[ms]'))
        self.assertEqual(
            self._dataframe_column_types(
                self._date_claim_frame(naive),
                table_name='date_claim', at='ts')['d'],
            0x0B)
        self.assertEqual(
            self._dataframe_column_types(
                self._date_claim_frame(naive, claim=False),
                table_name='date_claim', at='ts')['d'],
            0x0A)
        # The claimed NumPy frame writes what the Arrow-backed frame
        # writes, down to the bytes -- the point of the claim is that
        # the three backends answer the same read the same way.
        arrow_backed = pd.Series(
            [millis], dtype=pd.ArrowDtype(pyarrow.timestamp('ms')))
        self.assertEqual(
            self._dataframe_wire_payload(
                self._date_claim_frame(naive),
                table_name='date_claim', at='ts'),
            self._dataframe_wire_payload(
                self._date_claim_frame(arrow_backed, claim=False),
                table_name='date_claim', at='ts'))
        # On its own the tz-aware millisecond dtype is refused; claimed,
        # it lands as the type the claim names.
        aware = naive.dt.tz_localize('UTC')
        self.assertEqual(
            self._dataframe_column_types(
                self._date_claim_frame(aware),
                table_name='date_claim', at='ts')['d'],
            0x0B)
        with self.assertRaises(qi.QuestDBError):
            self._dataframe_wire_payload(
                self._date_claim_frame(aware, claim=False),
                table_name='date_claim', at='ts')
        # A column retyped to another unit since it was read is drift,
        # and drift goes out as the column's own type implies.
        self.assertEqual(
            self._dataframe_column_types(
                self._date_claim_frame(
                    pd.Series(numpy.array(
                        [millis * 1000], dtype='datetime64[us]'))),
                table_name='date_claim', at='ts')['d'],
            0x0A)

    @_needs_capsule_builder
    def test_geohash_bulk_accepts_an_uncounted_null_count(self):
        # The Arrow C data interface lets a producer leave `null_count`
        # at -1, meaning nobody has counted, and a column that never
        # held a null carries no validity buffer to count from. That
        # pairing is an ordinary export. Arbitrary values pass through
        # without a validity buffer or a range scan.
        self._dataframe_wire_payload(
            self._raw_geohash_stream([7, 8, 9], null_count=-1),
            table_name='raw_unknown_nulls', at='ts')
        self._dataframe_wire_payload(
            self._raw_geohash_stream([7, 1000, -1], null_count=-1),
            table_name='raw_unknown_nulls', at='ts')

    @_needs_capsule_builder
    def test_geohash_bulk_follows_a_batch_level_offset(self):
        # A slice can put its start row on the batch rather than on
        # each column, and the importer reads every column from there.
        # Arbitrary values in the selected slice are forwarded too.
        sliced = self._dataframe_wire_payload(
            self._raw_geohash_stream([30, 7, 8], batch_offset=1),
            table_name='raw_batch_offset', at='ts')
        whole = self._dataframe_wire_payload(
            self._raw_geohash_stream([7, 8]),
            table_name='raw_batch_offset', at='ts')
        self.assertEqual(sliced, whole)
        self._dataframe_wire_payload(
            self._raw_geohash_stream([7, 8, 1000], batch_offset=1),
            table_name='raw_batch_offset', at='ts')

    @_needs_capsule_builder
    def test_malformed_geohash_buffers_are_left_to_the_importer(self):
        # Python no longer walks GEOHASH values. Malformed Arrow
        # structures are rejected by the native importer in its own
        # words, whether or not the field claims GEOHASH.
        unreadable = (
            ('no value buffer', dict(n_buffers=1)),
            ('fewer rows than the batch', dict(length=2)))
        for name, shape in unreadable:
            with self.subTest(shape=name):
                with self.assertRaises(qi.QuestDBError):
                    self._dataframe_wire_payload(
                        self._raw_geohash_stream([7, 8, 9], **shape),
                        table_name='raw_unreadable', at='ts')
        # The same shapes claim nothing, so they are the importer's to
        # judge and it turns them away in its own words.
        for name, shape in unreadable:
            with self.subTest(shape=name, claimed=False):
                with self.assertRaises(qi.QuestDBError):
                    self._dataframe_wire_payload(
                        self._raw_geohash_stream(
                            [7, 8, 9], claimed=False, **shape),
                        table_name='raw_unreadable', at='ts')

    @unittest.skipIf(pyarrow is None, 'pyarrow not installed')
    @unittest.skipIf(pd is None, 'pandas not installed')
    def test_geohash_unchecked_values_cover_every_input_shape(self):
        values = [-1, 7, 1000]
        shapes = [('pyarrow Table', self._geohash_arrow_table)]
        if pd is not None:
            shapes.append(
                ('pandas ArrowDtype',
                 lambda values: self._geohash_frame(
                     values, pd.ArrowDtype(pyarrow.int32()), bits=5)))
        try:
            import polars as pl
        except ImportError:
            pl = None
        if pl is not None:
            def as_polars(values):
                return pl.from_arrow(self._geohash_arrow_table(values))
            shapes.append(('polars DataFrame', as_polars))
        for label, build in shapes:
            with self.subTest(shape=label):
                self._dataframe_wire_payload(
                    build(values), table_name='geo_shapes', at='ts',
                    schema_overrides={'gh': ('geohash', 5)})

    @unittest.skipIf(pyarrow is None, 'pyarrow not installed')
    def test_unchecked_geohash_honours_arrow_field_metadata(self):
        # `questdb.column_type=geohash` on the field claims the type
        # without any `schema_overrides`; arbitrary values are forwarded.
        self._dataframe_wire_payload(
            self._geohash_arrow_table([-1, 1000], bits=5),
            table_name='geo_md', at='ts')
        # An override still outranks the field metadata.
        self._dataframe_wire_payload(
            self._geohash_arrow_table([1000], bits=5),
            table_name='geo_md', at='ts',
            schema_overrides={'gh': ('geohash', 20)})

    @unittest.skipIf(pyarrow is None, 'pyarrow not installed')
    def test_unchecked_geohash_reads_every_metadata_claim(self):
        # `questdb.geohash_bits` is what claims the type: the native
        # importer reads a column carrying those bits as a GEOHASH
        # whatever else the field says, and reads them with
        # `u8::from_str`, so a leading `+` and a run of leading zeros
        # name a precision just as a bare number does.
        spellings = (
            {b'questdb.geohash_bits': b'5'},
            {b'questdb.geohash_bits': b'005'},
            {b'questdb.geohash_bits': b'+5'},
            {b'questdb.column_type': b'geohash',
             b'questdb.geohash_bits': b'005'},
            {b'questdb.column_type': b'geohash',
             b'questdb.geohash_bits': b'+5'},
            {b'questdb.column_type': b'geohash5b',
             b'questdb.geohash_bits': b'5'})
        for md in spellings:
            with self.subTest(md=md):
                self.assertEqual(
                    self._dataframe_column_types(
                        self._geohash_arrow_table([-1, 1000], md=md),
                        table_name='geo_md_claims', at='ts')['gh'],
                    0x0E)

    @unittest.skipIf(pyarrow is None, 'pyarrow not installed')
    def test_invalid_geohash_metadata_is_rejected_by_the_importer(self):
        # Malformed and structurally invalid claims are rejected by the
        # native importer before any values reach the wire. Permissive
        # value handling does not relax the metadata contract.
        refused = (
            ({b'questdb.geohash_bits': b'-5'},
             "invalid 'questdb.geohash_bits' metadata '-5'"),
            ({b'questdb.geohash_bits': b'300'},
             "invalid 'questdb.geohash_bits' metadata '300'"),
            ({b'questdb.geohash_bits': b'5b'},
             "invalid 'questdb.geohash_bits' metadata '5b'"),
            ({b'questdb.geohash_bits': b''},
             "invalid 'questdb.geohash_bits' metadata ''"),
            ({b'questdb.geohash_bits': b'0'},
             'geohash precision_bits 0 out of range'),
            ({b'questdb.geohash_bits': b'61'},
             'geohash precision_bits 61 out of range'),
            ({b'questdb.column_type': b'int',
              b'questdb.geohash_bits': b'5'},
             "carries 'questdb.geohash_bits' but column_type='int'"),
            ({b'questdb.column_type': b'geohash'},
             "missing 'questdb.geohash_bits' metadata"))
        for md, message in refused:
            with self.subTest(md=md):
                with self.assertRaises(qi.QuestDBError) as caught:
                    self._dataframe_wire_payload(
                        self._geohash_arrow_table([1000], md=md),
                        table_name='geo_md_refused', at='ts')
                self.assertIn(message, str(caught.exception))

    def test_a_commit_whose_flush_fails_at_the_wire_ends_the_transaction(self):
        """A flush that reached the wire clears the buffer whether it
        succeeded or not, so there is nothing left to commit. Keeping
        the transaction open there stranded the sender inside it: the
        next `close(flush=True)` raised over the caller's own error, and
        a retried commit found an empty buffer and reported success."""
        conf = ('http::addr=127.0.0.1:1;retry_timeout=0;'
                'auto_flush=off;protocol_version=2;')

        # The sender survives, and `__exit__` does not raise over the
        # commit failure the caller already handled.
        with qi.Sender.from_conf(conf) as sender:
            with self.assertRaises(qi.QuestDBError):
                with sender.transaction('t') as txn:
                    txn.row(columns={'ok': 1}, at=qi.ServerTimestamp)

        # A retried commit says the transaction is over rather than
        # reporting a success that sent nothing.
        sender = qi.Sender.from_conf(conf)
        sender.establish()
        txn = sender.transaction('t')
        txn.row(columns={'ok': 1}, at=qi.ServerTimestamp)
        with self.assertRaises(qi.QuestDBError):
            txn.commit()
        self.assertEqual(len(sender), 0)
        with self.assertRaisesRegex(
                qi.QuestDBError, 'Transaction already completed'):
            txn.commit()
        # And the sender is usable: it is no longer inside a
        # transaction, so a plain row and a new transaction both work.
        sender.row('t', columns={'a': 1}, at=qi.ServerTimestamp)
        sender.transaction('t2')
        sender.close(flush=False)

    def test_commit_on_a_closed_sender_says_so(self):
        """`len(self._sender._buffer)` raised `TypeError: object of type
        'NoneType' has no len()`, where `row()` and `dataframe()` both
        name the closed sender."""
        conf = ('http::addr=127.0.0.1:1;retry_timeout=0;'
                'auto_flush=off;protocol_version=2;')
        sender = qi.Sender.from_conf(conf)
        sender.establish()
        txn = sender.transaction('t')
        sender.close(flush=False)
        with self.assertRaisesRegex(
                qi.QuestDBError, "commit\\(\\) can't be called: Sender is closed"):
            txn.commit()

    def test_a_commit_that_cannot_flush_leaves_the_transaction_open(self):
        """`commit()` marked the transaction complete before the flush
        that carries it, so a refused flush closed the transaction with
        its rows still buffered -- and the next ordinary flush sent them
        outside the transaction the caller asked for. The transaction
        stays open instead, and the rows stay with it."""
        with HttpServer() as server, qi.Sender(
                qi.Protocol.Http, '127.0.0.1', server.port,
                auto_flush=False) as sender:
                refused = []

                class HostileTz(datetime.tzinfo):
                    def utcoffset(self, dt):
                        try:
                            sender.transaction('t').commit()
                        except qi.QuestDBError as exc:
                            refused.append(exc)
                        else:
                            refused.append(None)
                        return datetime.timedelta(0)

                    def tzname(self, dt):
                        return 'HOSTILE'

                    def dst(self, dt):
                        return datetime.timedelta(0)

                with sender.transaction('t') as txn:
                    txn.row(columns={'ok': 1}, at=qi.ServerTimestamp)
                    buffered = len(sender)
                    self.assertGreater(buffered, 0)
                    # The commit re-entered from here is refused, and
                    # must not close the transaction behind our back.
                    txn.row(
                        columns={'ts': datetime.datetime(
                            2025, 1, 1, tzinfo=HostileTz())},
                        at=qi.ServerTimestamp)
                    self.assertEqual(len(refused), 1)
                    self.assertIsNotNone(
                        refused[0], 'commit() was allowed mid-row')
                    # Still inside the transaction, rows intact.
                    self.assertGreaterEqual(len(sender), buffered)
                    # The transaction was not closed behind our back:
                    # it still commits, and that commit drains it.
                    txn.commit()
                    self.assertEqual(len(sender), 0)

    def test_a_rollback_reentered_from_a_row_is_terminal_and_discards_it(self):
        """Rollback is a terminal request, not just a call to `clear()`.

        A value conversion can call it while the buffer is holding a rewind
        marker. Clearing at that instant is unsafe, but refusing the clear
        used to leave both transaction flags live. If the conversion swallowed
        that refusal, `__exit__` then committed the rows it had asked to roll
        back. The clear is deferred until the row unwinds instead: no row from
        the transaction can survive to a later sender flush.
        """
        rolled_back = []
        with HttpServer() as server:
            with qi.Sender(
                    qi.Protocol.Http, '127.0.0.1', server.port,
                    auto_flush=False) as sender:

                class HostileTz(datetime.tzinfo):
                    fired = False

                    def utcoffset(self, dt):
                        if not HostileTz.fired:
                            HostileTz.fired = True
                            txn.rollback()
                            rolled_back.append(True)
                        return datetime.timedelta(0)

                    def tzname(self, dt):
                        return 'HOSTILE'

                    def dst(self, dt):
                        return datetime.timedelta(0)

                with sender.transaction('rolled_back') as txn:
                    txn.row(columns={'before': 1}, at=qi.ServerTimestamp)
                    txn.row(
                        columns={'ts': datetime.datetime(
                            2025, 1, 1, tzinfo=HostileTz())},
                        at=qi.ServerTimestamp)

                self.assertEqual(rolled_back, [True])
                self.assertEqual(len(sender), 0)
                sender.row(
                    'after', columns={'kept': 1}, at=qi.ServerTimestamp)

            self.assertEqual(server.requests, [b'after kept=1i\n'])

    @unittest.skipIf(pd is None, 'pandas not installed')
    def test_a_rollback_reentered_from_a_dataframe_discards_the_frame(self):
        """The deferred clear belongs to the shared buffer lifecycle.

        A dataframe holds `_row_depth` across both its Python plan build and
        serialization, so it must discharge the same rollback request when it
        unwinds even if no individual `Buffer._row` frame exists.
        """
        rolled_back = []
        with HttpServer() as server, qi.Sender(
                qi.Protocol.Http, '127.0.0.1', server.port,
                auto_flush=False) as sender:
            txn = sender.transaction('rolled_back')

            class HostileFrame(pd.DataFrame):
                armed = False
                fired = False

                @property
                def attrs(self):
                    if HostileFrame.armed and not HostileFrame.fired:
                        HostileFrame.fired = True
                        txn.rollback()
                        rolled_back.append(True)
                    return {}

                @attrs.setter
                def attrs(self, value):
                    pass

            frame = HostileFrame({
                'v': [1, 2],
                'ts': pd.to_datetime([0, 1], unit='s')})
            HostileFrame.armed = True
            with txn:
                txn.dataframe(frame, at='ts')

            self.assertEqual(rolled_back, [True])
            self.assertEqual(len(sender), 0)
            self.assertEqual(server.requests, [])

    def test_a_completed_transaction_cannot_be_reused(self):
        """Completion is an absorbing state for every transaction method.

        Checking it only in `commit()` and `rollback()` allowed an ended
        transaction to append rows outside any transaction, or to be entered
        again and strand the sender when its already-complete `__exit__` did
        nothing.
        """
        with HttpServer() as server, qi.Sender(
                qi.Protocol.Http, '127.0.0.1', server.port,
                auto_flush=False) as sender:
            rolled_back = sender.transaction('rolled_back')
            rolled_back.rollback()
            with sender.transaction('committed') as committed:
                pass

            for outcome, txn in (
                    ('rollback', rolled_back), ('commit', committed)):
                calls = (
                    ('row', lambda: txn.row(
                        columns={'v': 1}, at=qi.ServerTimestamp)),
                    ('__enter__', txn.__enter__),
                )
                if pd is not None:
                    frame = pd.DataFrame({
                        'v': [1], 'ts': pd.to_datetime([0], unit='s')})
                    calls += (('dataframe', lambda: txn.dataframe(
                        frame, at='ts')),)

                for method, call in calls:
                    with self.subTest(outcome=outcome, method=method):
                        with self.assertRaisesRegex(
                                qi.QuestDBError,
                                'Transaction already completed'):
                            call()

            self.assertEqual(len(sender), 0)
            # In particular, a refused second `__enter__` did not mark the
            # sender as being in a transaction.
            with sender.transaction('next'):
                pass

    def test_rollback_cannot_interrupt_a_commit_in_progress(self):
        """Only one terminal transition may own the transaction buffer.

        `commit()` releases the GIL during HTTP I/O. A rollback on another
        thread used to clear the buffer borrowed by that flush and overwrite
        its transaction flags. It is refused until commit has published its
        final state.
        """
        with HttpServer() as server, qi.Sender(
                qi.Protocol.Http, '127.0.0.1', server.port,
                auto_flush=False) as sender:
            server.responses.append((500, 200, None, None))
            txn = sender.transaction('t')
            txn.row(columns={'v': 1}, at=qi.ServerTimestamp)
            commit_errors = []

            def commit():
                try:
                    txn.commit()
                except Exception as exc:
                    commit_errors.append(exc)

            commit_thread = threading.Thread(target=commit)
            commit_thread.start()
            deadline = time.monotonic() + 5
            while not server.requests and time.monotonic() < deadline:
                time.sleep(0.001)
            self.assertTrue(server.requests, 'commit never reached the server')

            with self.assertRaisesRegex(
                    qi.QuestDBError, 'commit is already in progress'):
                txn.rollback()

            commit_thread.join(5)
            self.assertFalse(commit_thread.is_alive())
            self.assertEqual(commit_errors, [])
            with self.assertRaisesRegex(
                    qi.QuestDBError, 'Transaction already completed'):
                txn.rollback()

    def test_flush_clears_the_buffer_on_a_wire_failure(self):
        """`SenderTransaction.commit` depends on this by name.

        Its recovery rule reads the buffer after a failed flush to tell
        "the flush reached the wire and failed, so there is nothing left
        to commit" from "the flush was refused before it got there, so
        the rows are still the caller's to commit or roll back". That
        discriminator is `Sender.flush`'s doing, not `commit()`'s, and it
        is pinned here so a change to it breaks a test rather than a
        transaction guarantee."""
        # Reached the wire and failed: the internal buffer is cleared.
        with HttpServer() as server, qi.Sender(
                qi.Protocol.Http, '127.0.0.1', server.port,
                auto_flush=False,
                retry_timeout=datetime.timedelta(milliseconds=1)) as sender:
            server.responses.append(
                (0, 500, 'text/plain', b'Internal Server Error'))
            sender.row('t', columns={'x': 1}, at=qi.ServerTimestamp)
            self.assertGreater(len(sender), 0)
            with self.assertRaises(qi.QuestDBError):
                sender.flush()
            self.assertEqual(
                len(sender), 0,
                'commit() reads an empty buffer as "the transaction is '
                'over", so a flush that failed on the wire has to leave '
                'it empty')

        # Refused before the wire: the rows stay where they were.
        with HttpServer() as server, qi.Sender(
                qi.Protocol.Http, '127.0.0.1', server.port,
                auto_flush=False) as sender:
            with sender.transaction('t') as txn:
                txn.row(columns={'x': 1}, at=qi.ServerTimestamp)
                buffered = len(sender)
                self.assertGreater(buffered, 0)
                with self.assertRaisesRegex(
                        qi.QuestDBError,
                        'Cannot flush explicitly inside a transaction'):
                    sender.flush()
                self.assertEqual(
                    len(sender), buffered,
                    'commit() reads a non-empty buffer as "still the '
                    "caller's transaction\", so a refusal that never "
                    'reached the wire has to leave the rows alone')
                txn.commit()

        # And the conclusion commit() draws from the first case.
        with HttpServer() as server, qi.Sender(
                qi.Protocol.Http, '127.0.0.1', server.port,
                auto_flush=False,
                retry_timeout=datetime.timedelta(milliseconds=1)) as sender:
            server.responses.append(
                (0, 500, 'text/plain', b'Internal Server Error'))
            txn = sender.transaction('t')
            txn.row(columns={'x': 1}, at=qi.ServerTimestamp)
            with self.assertRaises(qi.QuestDBError):
                txn.commit()
            self.assertEqual(len(sender), 0)
            with self.assertRaisesRegex(
                    qi.QuestDBError, 'Transaction already completed'):
                txn.commit()

    @unittest.skipIf(pd is None, 'pandas not installed')
    def test_a_row_written_inside_a_frame_defers_its_auto_flush(self):
        """A `row()` written from inside a `dataframe()` leaves the
        buffer's depth above zero when it returns, so an auto-flush
        there would cut the frame's own work in half. It is left to the
        call still running.

        Attempting it instead reaches `flush()`'s in-row guard, which
        refuses -- correctly, but with a message naming `flush()` and a
        column value's conversion, to a caller who wrote a row. The row
        lands either way; this is about what the caller is told.
        """
        with Server() as server:
            with qi.Sender.from_conf(
                    f'tcp::addr=localhost:{server.port};'
                    'auto_flush_rows=1;') as sender:
                server.accept()

                class HostileFrame(pd.DataFrame):
                    fired = False

                    @property
                    def attrs(self):
                        if not HostileFrame.fired:
                            HostileFrame.fired = True
                            # `_dataframe` has already counted into the
                            # buffer's depth by the time it reads this.
                            sender.row(
                                'nested', columns={'v': 1},
                                at=qi.TimestampNanos(1))
                        return {}

                    @attrs.setter
                    def attrs(self, value):
                        pass

                frame = HostileFrame({
                    'v': [2, 3],
                    'ts': pd.to_datetime([0, 1], unit='s')})
                # No error, and in particular not one about `flush()`.
                sender.dataframe(frame, table_name='framed', at='ts')
                sender.flush()

            received = server.recv()
        text = b'\n'.join(received).decode('utf-8')
        self.assertIn('nested', text)
        self.assertIn('framed', text)

    @unittest.skipIf(pd is None, 'pandas not installed')
    def test_a_close_racing_a_refused_close_cannot_report_success(self):
        """`QuestDB.close()` decides its refusal before it publishes
        anything, and under the lock that publishes it.

        A close from inside one of the handle's own calls is refused:
        it is waiting on the very frame that would release the use.
        Publishing `_db = NULL` and `_closing = True` first would leave
        a window where a `close()` on another thread sees the handle
        closing, waits for `_closing` to clear, and returns -- against a
        handle the first call hands straight back, still open, its
        config string still set and its callback references still held.
        The same window is a deadlock read the other way round, with
        each close waiting on the other.

        Raced rather than sequenced, because a window is only visible
        from inside it. The invariant each round: whichever thread
        `close()` returned on, the handle really is closed.
        """
        for attempt in range(25):
            with self.subTest(attempt=attempt):
                with QwpAckServer() as server:
                    conf = (
                        f'ws::addr=127.0.0.1:{server.port};lazy_connect=true;'
                        'sender_pool_min=1;sender_pool_max=1;'
                        'pool_reap=manual;')
                    db = qi.QuestDB.from_conf(conf)
                    start = threading.Barrier(2)
                    inner = []
                    outer = []

                    class HostileFrame(pd.DataFrame):
                        fired = False

                        @property
                        def attrs(self):
                            if not HostileFrame.fired:
                                HostileFrame.fired = True
                                start.wait(timeout=20)
                                try:
                                    db.close()
                                except qi.QuestDBError:
                                    inner.append('refused')
                                else:
                                    inner.append('returned')
                            return {}

                        @attrs.setter
                        def attrs(self, value):
                            pass

                    def close_on_another_thread():
                        start.wait(timeout=20)
                        try:
                            db.close()
                        except qi.QuestDBError:
                            outer.append('refused')
                        else:
                            outer.append('returned')

                    closer = threading.Thread(target=close_on_another_thread)
                    closer.start()
                    try:
                        try:
                            db.dataframe(
                                HostileFrame({
                                    'v': [1, 2],
                                    'ts': pd.to_datetime([0, 1], unit='s')}),
                                table_name='t', at='ts')
                        except qi.QuestDBError:
                            # The other thread won and closed the handle
                            # first, which is a legitimate outcome.
                            pass
                        closer.join(timeout=30)
                        self.assertFalse(
                            closer.is_alive(),
                            'close() on the other thread never returned')
                        self.assertNotEqual(
                            inner, ['returned'],
                            'a close from inside the handle\'s own call '
                            'reported success')
                        if outer == ['returned']:
                            with self.assertRaisesRegex(
                                    qi.QuestDBError, 'is closed'):
                                db.reap_idle()
                    finally:
                        db.close()

    @unittest.skipIf(pd is None, 'pandas not installed')
    def test_a_refused_close_never_publishes_that_the_handle_is_closing(self):
        """A `close()` from inside one of the handle's own calls is
        refused, and the refusal is decided before anything is
        published. No other thread ever sees the handle as closed on
        account of it.

        The test above races two closes and checks the outcome of each.
        That holds the guarantee but it samples the window once per
        round, and the window is a few instructions wide: with the
        refusal moved back below the publish, that test still passes.
        Widen the window by 50ms and it fails every round, so the
        assertion is right and the sampling is what is missing.

        So this samples instead of racing. One thread calls the refused
        `close()` many times over, each one a window if the refusal is
        decided too late; another asks the handle to do something
        ordinary as fast as it can, and every "QuestDB is closed" it
        gets back is the handle answering for a close that was refused.

        The switch interval comes down for the duration. Both threads
        hold the GIL through their whole turn at the lock otherwise, so
        the refusing thread releases the state lock and takes it again
        without the sampler ever being scheduled between the two, and
        the window goes unvisited however many times it opens.
        """
        switch_interval = sys.getswitchinterval()
        sys.setswitchinterval(1e-5)
        try:
            self._sample_a_refused_close()
        finally:
            sys.setswitchinterval(switch_interval)

    def _sample_a_refused_close(self):
        with QwpAckServer() as server:
            conf = (f'ws::addr=127.0.0.1:{server.port};lazy_connect=true;'
                    'sender_pool_min=0;sender_pool_max=1;pool_reap=manual;')
            with qi.QuestDB.from_conf(conf) as db:
                start = threading.Barrier(2)
                stop = threading.Event()
                looked_closed = []
                refusals = []

                def sample():
                    # `reap_idle()` reads the published handle under the
                    # lock that publishes it, and refuses a closed one,
                    # so it reports the window without changing it.
                    start.wait(timeout=20)
                    while not stop.is_set():
                        try:
                            db.reap_idle()
                        except qi.QuestDBError as exc:
                            looked_closed.append(str(exc))
                            return

                sampler = threading.Thread(target=sample)
                sampler.start()

                class HostileFrame(pd.DataFrame):
                    fired = False

                    @property
                    def attrs(self):
                        if not HostileFrame.fired:
                            HostileFrame.fired = True
                            start.wait(timeout=20)
                            for _ in range(2000):
                                try:
                                    db.close()
                                except qi.QuestDBError:
                                    refusals.append('refused')
                                else:
                                    refusals.append('returned')
                                    break
                        return {}

                    @attrs.setter
                    def attrs(self, value):
                        pass

                try:
                    db.dataframe(
                        HostileFrame({
                            'v': [1, 2],
                            'ts': pd.to_datetime([0, 1], unit='s')}),
                        table_name='t', at='ts')
                except qi.QuestDBError:
                    pass
                finally:
                    stop.set()
                    sampler.join(timeout=30)

                self.assertFalse(
                    sampler.is_alive(), 'the sampler never returned')
                self.assertTrue(refusals, 'the close was never attempted')
                self.assertNotIn(
                    'returned', refusals,
                    "a close from inside the handle's own call reported "
                    'success')
                self.assertEqual(
                    looked_closed, [],
                    'another thread saw the handle as closed while a '
                    'close on it was being refused: '
                    + '; '.join(looked_closed[:1]))
                # And the handle is still open, as the refusals said.
                db.reap_idle()

    def test_a_dropped_lease_returns_its_sender_to_the_pool(self):
        """`PooledSender.__dealloc__` returns the borrowed sender, and
        nothing on that path can raise.

        Every refusal a lease makes belongs to `close()`, which runs
        them before it releases. `_release_locked` is `except *` and
        `__dealloc__` cannot report anything, so a guard standing there
        would skip the return to the pool and leave `QuestDB.close()`
        waiting on a lease that can never come back, until the bound
        turns the wait into an error and the handle is left closing.
        `PooledReader._release_locked` is the same shape.

        Nothing reachable from Python drops a lease part-way through a
        row -- the frame writing the row holds a reference to the lease
        it is writing on -- so this holds the invariant the release
        path now has unconditionally rather than reproducing a failure.
        """
        import gc
        with QwpAckServer() as server:
            conf = (f'ws::addr=127.0.0.1:{server.port};lazy_connect=true;'
                    'sender_pool_min=0;sender_pool_max=1;pool_reap=manual;')
            with qi.QuestDB.from_conf(conf) as db:
                lease = db.sender()
                lease.row('t', columns={'v': 1},
                          at=qi.TimestampNanos(1))
                # Dropped without `close()`, part-way through a batch.
                # Only a refcounting interpreter deallocates at the
                # `del`; elsewhere a collection is what runs the
                # release, and a cpyext proxy can need a second pass.
                del lease
                gc.collect()
                gc.collect()

                # The pool holds one sender, so a second lease can only
                # be handed out if the first came back.
                second = db.sender()
                second.close()

                # And the handle closes rather than waiting on a lease
                # that was never returned. A wait says so through the
                # `questdb` logger every five seconds, so silence there
                # is the evidence that it did not wait.
                with self.assertNoLogs('questdb', level='WARNING'):
                    db.close()

    @unittest.skipIf(pd is None, 'pandas not installed')
    def test_a_ws_dataframe_survives_a_close_from_inside_itself(self):
        """The WebSocket `dataframe()` route hands connection options to
        the run and then builds the plan, which is caller Python. A
        `close()` from there would free those options while the run
        still has to read them -- a use-after-free that takes the
        interpreter down -- so the run owns a clone for its whole
        length. The close is simply harmless, on an established sender
        and on one that was never established (where there is no row
        buffer to hang a guard on)."""
        for established in (True, False):
            with self.subTest(established=established):
                closed = []

                class HostileFrame(pd.DataFrame):
                    fired = False

                    @property
                    def attrs(self):
                        if not HostileFrame.fired:
                            HostileFrame.fired = True
                            try:
                                sender.close(flush=False)
                                closed.append(True)
                            except qi.QuestDBError:
                                closed.append(False)
                        return {}

                    @attrs.setter
                    def attrs(self, value):
                        pass

                with QwpAckServer() as server:
                    sender = qi.Sender.from_conf(
                        f'ws::addr=127.0.0.1:{server.port};'
                        'auto_flush=off;')
                    if established:
                        sender.establish()
                    frame = HostileFrame({
                        'v': [1, 2],
                        'ts': pd.to_datetime([0, 1], unit='s')})
                    # Completes, or fails as a closed sender would --
                    # what it must not do is read freed memory.
                    try:
                        sender.dataframe(frame, table_name='t', at='ts')
                    except qi.QuestDBError:
                        pass
                    self.assertEqual(len(closed), 1)

    @unittest.skipIf(pd is None, 'pandas not installed')
    def test_ws_sender_dataframe_is_refused_while_a_row_is_being_written(self):
        """`Sender.dataframe` over QWP/WebSocket takes its own
        connection and never reaches `_dataframe`, where the guard
        stands for every other route. Re-entered from a half-written
        row it published the inner frame ahead of the row it
        interrupted, inverting the order of the two."""
        with QwpAckServer() as server:
            with qi.Sender.from_conf(
                    f'ws::addr=127.0.0.1:{server.port};'
                    'auto_flush=off;') as sender:
                seen = []

                class HostileUuid(uuid.UUID):
                    @property
                    def int(self):
                        frame = pd.DataFrame({
                            'x': [1],
                            'ts': pd.to_datetime([0], unit='s')})
                        try:
                            sender.dataframe(
                                frame, table_name='inner', at='ts')
                        except qi.QuestDBError as exc:
                            seen.append(exc)
                        else:
                            seen.append(None)
                        return self.UUID_INT

                    @int.setter
                    def int(self, value):
                        pass

                HostileUuid.UUID_INT = self.UUID_VALUE.int

                sender.row(
                    'outer',
                    columns={'value': HostileUuid(str(self.UUID_VALUE))},
                    at=qi.ServerTimestamp)

                self.assertEqual(len(seen), 1)
                self.assertIsNotNone(
                    seen[0], 'dataframe() was allowed mid-row over ws::')
                self.assertEqual(
                    seen[0].code, qi.QuestDBErrorCode.InvalidApiCall)
                self.assertIn(
                    "dataframe() can't be called while a row is being "
                    'written into this buffer',
                    str(seen[0]))


    @unittest.skipIf(pd is None, 'pandas not installed')
    def test_commit_is_refused_from_inside_a_dataframe(self):
        """`commit()` reached the flush's in-row guard only when the
        buffer had something in it. `dataframe()` counts into
        `_row_depth` before it writes anything, so a commit re-entered
        from the plan build found nothing to flush, ended the
        transaction, and left the frame's rows to go out afterwards
        outside it."""
        refused = []

        class HostileFrame(pd.DataFrame):
            fired = False

            @property
            def attrs(self):
                if not HostileFrame.fired:
                    HostileFrame.fired = True
                    try:
                        txn.commit()
                    except qi.QuestDBError as exc:
                        refused.append(exc)
                    else:
                        refused.append(None)
                return {}

            @attrs.setter
            def attrs(self, value):
                pass

        with HttpServer() as server, qi.Sender(
                qi.Protocol.Http, '127.0.0.1', server.port,
                auto_flush=False) as sender:
            txn = sender.transaction('t')
            frame = HostileFrame({
                'v': [1, 2],
                'ts': pd.to_datetime([0, 1], unit='s')})
            txn.dataframe(frame, at='ts')

            self.assertEqual(len(refused), 1)
            self.assertIsNotNone(
                refused[0], 'commit() was allowed mid-dataframe')
            self.assertEqual(
                refused[0].code, qi.QuestDBErrorCode.InvalidApiCall)
            self.assertIn(
                "commit() can't be called while a row is being written",
                str(refused[0]))
            # Still inside the transaction: the frame's rows commit
            # with it rather than leaking out on their own.
            txn.commit()

    def test_close_drain_is_refused_while_a_row_is_being_written(self):
        """`close_drain()` was the one closing entry point with no
        in-progress-row check. Draining stops the sender accepting
        anything further, so the row being written could never be
        finished and the complete rows buffered before it went with
        it -- and the `close(flush=True)` that followed reported
        success having sent nothing."""
        refused = []
        with QwpAckServer() as server:
            with qi.Sender.from_conf(
                    f'ws::addr=127.0.0.1:{server.port};'
                    'auto_flush=off;') as sender:

                class HostileAddr(ipaddress.IPv4Address):
                    def __int__(self):
                        try:
                            sender.close_drain()
                        except qi.QuestDBError as exc:
                            refused.append(exc)
                        else:
                            refused.append(None)
                        return 0x01020304

                sender.row(
                    'good', columns={'ok': 1}, at=qi.ServerTimestamp)
                buffered = len(sender)
                sender.row(
                    'hostile', columns={'addr': HostileAddr('192.0.2.1')},
                    at=qi.ServerTimestamp)

                self.assertEqual(len(refused), 1)
                self.assertIsNotNone(
                    refused[0], 'close_drain() was allowed mid-row')
                self.assertIn(
                    "close_drain() can't be called while a row is being "
                    'written into this buffer',
                    str(refused[0]))
                # Both rows survive and can still be sent.
                self.assertGreater(len(sender), buffered)
                sender.flush()

    @unittest.skipIf(pd is None, 'pandas not installed')
    def test_a_transaction_cannot_open_inside_a_dataframe(self):
        """A transaction needs a clear buffer, which a `dataframe()`
        part-way through its plan build still has -- it has written
        nothing yet. Opening one there put the frame's rows inside a
        transaction the caller never asked for, and the rollback that
        ended it threw the whole frame away with nothing said."""
        refused = []

        class HostileFrame(pd.DataFrame):
            fired = False

            @property
            def attrs(self):
                if not HostileFrame.fired:
                    HostileFrame.fired = True
                    try:
                        sender.transaction('inner')
                    except qi.QuestDBError as exc:
                        refused.append(exc)
                    else:
                        refused.append(None)
                return {}

            @attrs.setter
            def attrs(self, value):
                pass

        with HttpServer() as server, qi.Sender(
                qi.Protocol.Http, '127.0.0.1', server.port,
                auto_flush=False) as sender:
            frame = HostileFrame({
                'v': [1, 2],
                'ts': pd.to_datetime([0, 1], unit='s')})
            sender.dataframe(frame, table_name='t', at='ts')

            self.assertEqual(len(refused), 1)
            self.assertIsNotNone(
                refused[0], 'transaction() was allowed mid-dataframe')
            self.assertIn(
                "transaction() can't be called while a row is being "
                'written into this buffer',
                str(refused[0]))
            # The frame is still there, and still ordinary rows.
            self.assertGreater(len(sender), 0)
            sender.flush()
            self.assertEqual(len(server.requests), 1)

    def test_a_lease_returned_on_another_thread_still_lets_close_run(self):
        """The per-thread count that stops a self-waiting close must
        only cover calls that begin and end on one thread. A lease is
        handed out by `sender()` and may be returned from anywhere, so
        counting it per thread left the borrower's count standing and
        the returner's below zero -- and `close()`, which is what
        `__exit__` calls, was then refused for good on those threads."""
        with QwpAckServer() as server:
            conf = (f'ws::addr=127.0.0.1:{server.port};'
                    'sender_pool_min=0;sender_pool_max=4;'
                    'pool_reap=manual;query_pool_min=0;')

            # Borrowed here, returned on other threads.
            db = qi.QuestDB.from_conf(conf)
            leases = [db.sender() for _ in range(3)]
            for lease in leases:
                thread = threading.Thread(target=lease.close)
                thread.start()
                thread.join()
            db.close()

            # And the other way round.
            db = qi.QuestDB.from_conf(conf)
            borrowed = []
            thread = threading.Thread(
                target=lambda: borrowed.append(db.sender()))
            thread.start()
            thread.join()
            borrowed[0].close()
            db.close()

    #: Public members the re-entrancy grid cannot reach, each with the
    #: reason. An entry here is a claim somebody made and can be argued
    #: with; a member that is neither here nor in the grid is a member
    #: nobody has thought about, which is how every missed door started.
    REENTRANCY_ALLOW_LIST = {
        'PooledReader.__enter__': (
            'a reader lease needs a live QuestDB read endpoint to '
            'borrow from; the offline fixtures are sender-side mocks. '
            'Covered by system_test.py against a real server.'),
        'PooledReader.__exit__': 'as PooledReader.__enter__',
        'PooledReader.close': 'as PooledReader.__enter__',
        'PooledReader.execute': 'as PooledReader.__enter__',
        'PooledReader.query': 'as PooledReader.__enter__',
        'QueryResult.__arrow_c_stream__': (
            'a query result comes from a read endpoint, which the '
            'offline sender-side mocks do not serve. The route that '
            'matters is a `types_mapper` running the caller\'s code '
            'per batch while the cursor streams, and '
            '`TestEgressWithDatabase.'
            'test_reentering_the_client_from_a_types_mapper` drives it '
            'in system_test.py against a real server.'),
        'QueryResult.__enter__': 'as QueryResult.__arrow_c_stream__',
        'QueryResult.__exit__': 'as QueryResult.__arrow_c_stream__',
        'QueryResult.cancel': 'as QueryResult.__arrow_c_stream__',
        'QueryResult.close': 'as QueryResult.__arrow_c_stream__',
        'QueryResult.iter_arrow': 'as QueryResult.__arrow_c_stream__',
        'QueryResult.iter_pandas': 'as QueryResult.__arrow_c_stream__',
        'QueryResult.iter_polars': 'as QueryResult.__arrow_c_stream__',
        'QueryResult.to_arrow': 'as QueryResult.__arrow_c_stream__',
        'QueryResult.to_pandas': 'as QueryResult.__arrow_c_stream__',
        'QueryResult.to_polars': 'as QueryResult.__arrow_c_stream__',
    }

    def test_every_public_class_is_guarded_or_excused_in_writing(self):
        """`GUARDED_CLASSES` decides which classes the re-entrancy grid
        walks at all, and it used to be six names somebody typed under a
        docstring saying that lists somebody typed here have been wrong
        every time. It is read out of the module now, so a class added
        later arrives guarded and its methods arrive unclassified in the
        test below, which names them.

        What that leaves to check is the other side of the rule: an
        excuse has to name a class the module still defines, and it has
        to say something."""
        for name, reason in api_surface.NOT_GUARDED.items():
            self.assertTrue(
                inspect.isclass(getattr(qi, name, None)),
                f'{name} is excused from the guarded classes, but the '
                f'client no longer has a class by that name')
            self.assertTrue(
                reason,
                f'{name} is excused from the guarded classes with no '
                f'reason')

        # Every class that holds a native handle while it runs the
        # caller's Python. Losing one from the module's public surface
        # would shrink the grid rather than fail anything, so it is
        # spelled out here.
        guarded = {cls.__name__ for cls in api_surface.GUARDED_CLASSES}
        self.assertEqual(
            set(),
            {'QuestDB', 'Sender', 'PooledSender', 'PooledReader',
             'SenderTransaction', 'Buffer', 'QueryResult'} - guarded)

    def test_every_public_member_is_classified_for_re_entrancy(self):
        """Every guard this client has added came from someone listing
        the routes they could think of, and every round of review found
        the route they could not. So the list comes from reflection, and
        a member that no grid cell exercises has to be excused in
        writing.

        A newly added method fails here until it is either driven by the
        grid or excused with a reason -- unclassified at review time
        rather than uncovered a few rounds later."""
        table = json.loads(
            (PROJ_ROOT / 'test' /
             'reentrancy_matrix_expected.json').read_text())
        exercised = set()
        for key, record in table.items():
            member = key.split(' | ', 1)[1]
            if record['result'] in ('refused', 'clean'):
                exercised.add(member)

        surface = api_surface.qualified_members()
        self.assertGreater(len(surface), 50, 'the reflected surface shrank')

        unclassified = []
        for name, _cls, _member, kind in surface:
            if name in exercised:
                continue
            if name in self.REENTRANCY_ALLOW_LIST:
                self.assertTrue(
                    self.REENTRANCY_ALLOW_LIST[name],
                    f'{name} is excused from the grid with no reason')
                continue
            unclassified.append(f'{name} ({kind})')
        self.assertEqual(
            unclassified, [],
            'these public members are never exercised by the '
            're-entrancy grid and are not excused. Either add an outer '
            'scenario that can reach them in test/reentrancy_matrix.py, '
            'or add them to REENTRANCY_ALLOW_LIST with the reason:\n  '
            + '\n  '.join(unclassified))

        # And the other direction: an excuse for a member that no longer
        # exists is an excuse nobody will notice is stale.
        known = {name for name, _, _, _ in surface}
        stale = sorted(set(self.REENTRANCY_ALLOW_LIST) - known)
        self.assertEqual(
            stale, [],
            f'these members are excused from the grid but no longer '
            f'exist: {stale}')

    def test_the_reentrancy_grid_records_no_hang_and_no_crash(self):
        """The stored table is the grid's result, so it is also where a
        regression would show. A `HANG` or a `CRASH` in it means someone
        checked one in."""
        table = json.loads(
            (PROJ_ROOT / 'test' /
             'reentrancy_matrix_expected.json').read_text())
        bad = {key: record for key, record in table.items()
               if record['result'] in ('HANG', 'CRASH')}
        self.assertEqual(bad, {}, f'the checked-in grid records {bad}')
        outcomes = {record['outer_outcome'] for record in table.values()}
        self.assertEqual(
            outcomes, {'completed'},
            'every cell must have run its outer call to completion, '
            'otherwise a `clean` re-entry may only be clean because '
            f'nothing else happened: {sorted(outcomes)}')

    #: The two held states in which a `close()` is still draining, and
    #: the one in which it has finished. Named here because what the
    #: handle owes a caller differs between them by exactly one thing:
    #: whether a second `close()` has anything left to wait for.
    CONCURRENCY_DRAINING = (
        'QuestDB.close/lease-wait', 'QuestDB.close/call-wait')
    CONCURRENCY_CLOSED = 'QuestDB.close/done'

    #: The two members that are allowed to return cleanly against a
    #: closed handle, because `close()` is documented idempotent and
    #: `__exit__` is `close()`.
    CONCURRENCY_IDEMPOTENT = frozenset(
        {'QuestDB.close', 'QuestDB.__exit__'})

    @staticmethod
    def _concurrency_table():
        return json.loads(
            (PROJ_ROOT / 'test' /
             'concurrency_matrix_expected.json').read_text())

    def test_the_concurrency_grid_records_no_hang_and_no_crash(self):
        """The stored table is the grid's result, so it is also where a
        regression would show. A `HANG` or a `CRASH` in it means
        someone checked one in.

        `HANG` covers one shape the re-entrancy grid cannot express:
        the asking thread got its answer and the thread that was
        holding the state never came back. The grid scores that as a
        hang rather than letting it hide behind the answer."""
        table = self._concurrency_table()
        bad = {key: record for key, record in table.items()
               if record['result'] in ('HANG', 'CRASH')}
        self.assertEqual(bad, {}, f'the checked-in grid records {bad}')

    def test_the_concurrency_grid_asks_about_every_public_member(self):
        """The grid's member axis is reflected from `api_surface`, but
        the table is a file, so the two drift the moment someone adds a
        method and does not re-run the grid. A member with no row is a
        member no held state has ever been asked about."""
        table = self._concurrency_table()
        asked = {key.split(' | ', 1)[1] for key in table}
        surface = {name for name, _, _, _ in api_surface.qualified_members()}
        self.assertEqual(
            sorted(surface - asked), [],
            'these public members have no row in the concurrency grid; '
            're-run `./proj.py grid concurrency --update`')
        self.assertEqual(
            sorted(asked - surface), [],
            'these rows in the concurrency grid are for members that no '
            'longer exist; re-run `./proj.py grid concurrency --update`')

        import concurrency_matrix
        held = {key.split(' | ', 1)[0] for key in table}
        self.assertEqual(
            held, set(concurrency_matrix.HOLDER_SCENARIOS),
            'the held states in the table and the ones the grid can '
            'drive have drifted; re-run '
            '`./proj.py grid concurrency --update`')

    def test_a_closing_handle_turns_away_every_call_that_starts_work(self):
        """Closing is one-way, so every refusal it causes has to hold
        for the life of the handle. That promise is only worth as much
        as the set of calls it actually covers, and the way it was got
        wrong before was a route nobody listed.

        So the set comes from reflection: every public member of
        `QuestDB` is checked against the grid's three closing states,
        and a new one is covered the day it is added. Only `close` and
        `__exit__` may answer cleanly, and only once the close has
        finished -- while one is still draining, a second has something
        left to wait for and says so."""
        table = self._concurrency_table()
        members = sorted(
            name for name, _, _, _ in api_surface.qualified_members()
            if name.startswith('QuestDB.'))
        self.assertGreater(len(members), 10, 'the QuestDB surface shrank')

        wrong = []
        for held in self.CONCURRENCY_DRAINING + (self.CONCURRENCY_CLOSED,):
            closed = held == self.CONCURRENCY_CLOSED
            for member in members:
                record = table.get(f'{held} | {member}')
                self.assertIsNotNone(
                    record, f'no grid row for {held} | {member}')
                want = (
                    'clean'
                    if closed and member in self.CONCURRENCY_IDEMPOTENT
                    else 'refused')
                if record['result'] != want:
                    wrong.append(
                        f'{held} | {member}: wanted {want}, got '
                        f'{record["result"]} ({record["reason"][:70]})')
        self.assertEqual(
            wrong, [],
            'a handle another thread has put into closing, or closed, '
            'answered these differently than the one-way contract '
            'says:\n  ' + '\n  '.join(wrong))

    #: Integration test classes whose `setUp` asks for
    #: `FIRST_QWP_ROW_TYPES_RELEASE` -- QuestDB 10, the first production
    #: QWP. Membership is the gate, so every test in one of these is
    #: covered without anyone remembering to mark it.
    #: What a test says in its own source when it builds one of the
    #: QWP-only column types, and what it says when it sends something.
    #: A test that does both is a test that can put one of these types
    #: on the wire.
    QWP_ROW_TYPE_TOKENS = (
        ' LONG256', 'GEOHASH(', ' IPV4', ' DATE', ' CHAR', ' BINARY',
        ' UUID', 'qi.Geohash(', 'qi.Long256(', 'qi.DateMillis(',
        'qi.Char(', "'long256'", "'geohash'", "'ipv4'", "'char'")
    QWP_ROW_TYPE_WRITE_TOKENS = (
        '.dataframe(', '.row(', 'schema_overrides', '_run_example(')

    #: Tests the scan reaches that name one of these types without
    #: putting one on the wire, each with the reason. An entry here is
    #: a claim somebody made and can be argued with.
    QWP_ROW_TYPE_EXEMPT = {
        'test_uuid_claim_on_wrong_width_is_rejected':
            'the claim is refused client-side; no frame is sent.',
        'test_fsb32_rejected_by_row_ilp':
            'the NumPy planner refuses the column client-side; no '
            'frame is sent.',
        'test_uuid_string_into_uuid_column_via_server_coercion':
            'the wire type is VARCHAR. UUID is only the type of the '
            'destination column, which QuestDB has had for years.',
        'test_invalid_uuid_string_is_rejected_by_server':
            'as test_uuid_string_into_uuid_column_via_server_coercion.',
        'test_ipv4_string_coercion_is_unsupported':
            'the wire type is VARCHAR; IPV4 is the column it is '
            'refused by.',
        'test_invalid_ipv4_string_is_rejected_by_server':
            'as test_ipv4_string_coercion_is_unsupported.',
        'test_pa_uint32_round_trip_as_long':
            'the wire type is LONG. IPV4 appears in the docstring '
            'saying which rule this column does not take.',
        'test_pa_uint32_is_routed_to_long_not_ipv4':
            'as test_pa_uint32_round_trip_as_long.',
    }

    @staticmethod
    def _gates_on_questdb_10(cls, lines):
        """Whether every test in a class is covered by the QuestDB 10
        gate, read from the class's own fixture methods.

        Membership in the class is the gate, so the class is where the
        question is answered: a `setUp` or `setUpClass` that calls
        `_require_qwp_row_types()` covers every test in the class,
        whether or not whoever wrote one thought about the server
        version.
        """
        for fn in cls.body:
            if not (isinstance(fn, ast.FunctionDef)
                    and fn.name in ('setUp', 'setUpClass')):
                continue
            body = ''.join(lines[fn.lineno - 1:fn.end_lineno])
            if '_require_qwp_row_types(' in body:
                return True
        return False

    def test_every_integration_test_that_writes_a_qwp_only_type_is_gated(self):
        """A test that puts UUID, IPV4, BINARY, CHAR, DATE, LONG256 or
        GEOHASH on the wire needs QuestDB 10, and five of the
        integration legs run the 9.4.3 QWP beta.

        The gate is class membership, so what this holds is the other
        half: a test that writes one of these types from a class with
        no gate. Reading the tests rather than remembering them is the
        point -- a per-test call to `_require_qwp_row_types()` is a call
        someone has to think to write, and six review rounds found tests
        where nobody had.

        The beta accepts these types, so an ungated test passes on those
        legs rather than going red. That is the harder failure to see:
        the coverage looks the same either way, and what is actually
        being exercised is an implementation this client does not
        support.

        Which classes gate is read from their own `setUp` rather than
        listed here. A list of class names is the same kind of thing as
        a list of call sites: it says a class gates without asking
        whether it does, so deleting the gate from a listed class would
        leave this green.

        Both files that hold integration tests are read, and every class
        in them, at any nesting. `test.py`'s live-server tests sit inside
        an `if os.environ...` block, so a scan of top-level classes in
        one file walks straight past them.

        A test the scan reaches that does not really write one of these
        types goes on `QWP_ROW_TYPE_EXEMPT` with the reason."""
        sources = (
            # Every class in the integration suite takes a live server.
            # In `test.py` only some do, and they say so by deriving
            # from `TestWithDatabase`.
            ('system_test.py', None),
            ('test.py', 'TestWithDatabase'),
        )

        scanned = 0
        ungated = []
        reached = set()
        gated_names = set()
        for file_name, base_name in sources:
            text = (PROJ_ROOT / 'test' / file_name).read_text(
                encoding='utf-8')
            lines = text.splitlines(keepends=True)
            classes = [
                node for node in ast.walk(ast.parse(text))
                if isinstance(node, ast.ClassDef)
                and (base_name is None
                     or any(isinstance(b, ast.Name) and b.id == base_name
                            for b in node.bases))]

            gated = {cls.name for cls in classes
                     if self._gates_on_questdb_10(cls, lines)}
            gated_names |= gated

            for cls in classes:
                for fn in cls.body:
                    if not (isinstance(fn, ast.FunctionDef)
                            and fn.name.startswith('test')):
                        continue
                    scanned += 1
                    body = ''.join(lines[fn.lineno - 1:fn.end_lineno])
                    if not any(token in body
                               for token in self.QWP_ROW_TYPE_TOKENS):
                        continue
                    if not any(token in body
                               for token in self.QWP_ROW_TYPE_WRITE_TOKENS):
                        continue
                    reached.add(fn.name)
                    if cls.name in gated:
                        continue
                    if fn.name in self.QWP_ROW_TYPE_EXEMPT:
                        self.assertTrue(
                            self.QWP_ROW_TYPE_EXEMPT[fn.name],
                            f'{fn.name} is exempt with no reason')
                        continue
                    ungated.append(f'{file_name}:{cls.name}.{fn.name}')

        self.assertTrue(
            gated_names, 'no class gates its tests on QuestDB 10 any more')
        self.assertGreater(scanned, 100, 'the integration suite shrank')
        self.assertEqual(
            ungated, [],
            'these integration tests can put a QWP-only column type on '
            'the wire from a class that does not require QuestDB 10, so '
            'they run against the 9.4.3 QWP beta on five legs. Move each '
            f'into one of {sorted(gated_names)}, or add it to '
            'QWP_ROW_TYPE_EXEMPT with the reason it writes no such '
            'type:\n  ' + '\n  '.join(ungated))

        # And the other direction: an exemption for a test the scan no
        # longer reaches is an exemption nobody will notice is stale.
        stale = sorted(set(self.QWP_ROW_TYPE_EXEMPT) - reached)
        self.assertEqual(
            stale, [],
            f'these tests are exempt from the QuestDB 10 gate but the '
            f'scan no longer reaches them: {stale}')

    #: Classes the capture inventory names that the re-entrancy grid
    #: cannot drive, each with the reason. The grid's outer axis is a
    #: hand-registered decorator list, so this is what stops it drifting
    #: from the inventory the repo already keeps.
    OUTER_AXIS_ALLOW_LIST = {
        'QueryResult':
            'every outer scenario needs a live endpoint the fixture can '
            'serve, and the offline fixtures are sender-side mocks with '
            'nothing to read from. Driven against a real server by '
            'TestEgressWithDatabase.'
            'test_reentering_the_client_from_a_types_mapper, which '
            're-enters from a types_mapper and covers row 15.',
    }

    def test_every_capture_site_has_an_outer_grid_scenario(self):
        """`native_captures.md` says it lists every entry point that
        hands native state to a run which then executes caller Python.
        The re-entrancy grid's outer axis asks the same question, and it
        is twelve entries registered by hand, so the two drift.

        They drifted: the inventory had no row for the read side, and
        the grid has no read-side scenario, so a `types_mapper` running
        the caller's code over a live cursor was in neither. This holds
        the axis to the table - a class the table names has to be
        drivable, or excused in writing.

        Only that direction is checked. The table names shared helpers
        like `_dataframe` where the grid names each caller, so a
        scenario without a row of its own is normal."""
        inventory = (PROJ_ROOT / 'src' / 'questdb' /
                     'native_captures.md').read_text()
        sites = [
            line.strip('|').split('|')[1]
            for line in inventory.splitlines()
            if line.startswith('| ') and not line.startswith('| # ')]

        import reentrancy_matrix
        outer = reentrancy_matrix.OUTER_SCENARIOS
        driven = {name.split('.', 1)[0] for name in outer}
        self.assertGreaterEqual(len(outer), 12, 'the outer axis shrank')

        named = set()
        for site in sites:
            for token in re.findall(r'`([A-Z][A-Za-z]*)\.', site):
                named.add(token)

        undriven = []
        for cls_name in sorted(named):
            if cls_name in driven:
                continue
            reason = self.OUTER_AXIS_ALLOW_LIST.get(cls_name)
            if reason:
                self.assertTrue(reason, f'{cls_name} excused with no reason')
                continue
            undriven.append(cls_name)
        self.assertEqual(
            undriven, [],
            'these classes hold native state across caller Python, by '
            "`native_captures.md`'s own account, and no outer scenario "
            'in test/reentrancy_matrix.py starts a call on them. Add a '
            'scenario, or add the class to OUTER_AXIS_ALLOW_LIST with '
            f'the reason it cannot have one: {undriven}')

        stale = sorted(set(self.OUTER_AXIS_ALLOW_LIST) - named)
        self.assertEqual(
            stale, [],
            f'these classes are excused from the grid\'s outer axis but '
            f'the capture inventory no longer names them: {stale}')

    #: Beside a Python cap that is deliberately tighter than the
    #: importer's, so that "lower on purpose" is a thing the source
    #: says rather than a thing this test infers from nearby prose.


    def test_the_native_capture_inventory_matches_the_sources(self):
        """`src/questdb/native_captures.md` is the checked-in answer to
        "which native pointer is live while the caller's Python runs, and
        what keeps it valid". Five review rounds produced four
        use-after-free or hang findings on exactly those captures, every
        one of them a guard that did not name some door.

        The table is only worth having if it tracks the code, so this
        holds it to two things: no row may rest on a guard alone, and
        every capture it names must still exist under that name."""
        inventory = (PROJ_ROOT / 'src' / 'questdb' /
                     'native_captures.md').read_text()
        rows = [
            line for line in inventory.splitlines()
            if line.startswith('| ') and not line.startswith('| # ')]
        self.assertGreaterEqual(
            len(rows), 14, 'the capture table lost rows')

        # The rows that are allowed not to say `ownership`, and why. A row
        # reaches this list only by review: it has to be a capture whose
        # worst case is a refused call rather than a freed read. Keyed by
        # site and capture so that renaming either brings the row back for
        # a fresh decision.
        NON_OWNERSHIP_ROWS = {
            ('`Buffer._row`', 'the rewind marker'):
                'The marker is spent only by a flush this row triggered. '
                'Every other route to it is refused, and the native buffer '
                'refuses a half-written row on its own, so the worst case '
                'is a refused call rather than a freed read.',
        }

        for row in rows:
            cells = [cell.strip() for cell in row.strip('|').split('|')]
            site, capture, kept_by, klass = cells[1], cells[2], cells[3], cells[4]
            self.assertTrue(kept_by, f'{site}: nothing keeps {capture} valid')
            if klass.startswith('ownership'):
                continue
            self.assertIn(
                (site, capture), NON_OWNERSHIP_ROWS,
                f'{site}: {capture} is classified {klass!r}, which does not '
                'begin with "ownership". A guard may refuse cleanly; it may '
                'not be the only thing between the caller and a freed '
                'pointer. Either give the row an ownership story, or add it '
                'to NON_OWNERSHIP_ROWS with the reason its worst case is a '
                'refused call.')

        # An entry that no longer matches a row is an excuse for a capture
        # that has moved or gone, and has to be re-decided rather than left
        # standing.
        present = {
            tuple(cell.strip() for cell in row.strip('|').split('|'))[1:3]
            for row in rows}
        for key in NON_OWNERSHIP_ROWS:
            self.assertIn(
                key, present,
                f'NON_OWNERSHIP_ROWS excuses {key}, which the capture table '
                'no longer lists under that name.')

        # Each capture, spelled as the sources spell it. A rename that
        # forgets the table fails here rather than leaving the table
        # describing code that no longer exists.
        sources = '\n'.join(
            (PROJ_ROOT / 'src' / 'questdb' / name).read_text()
            for name in ('_client.pyx', 'dataframe.pxi'))
        for token in (
                'af.sender_slot = &self._impl',
                'af.last_flush_ms = self._last_flush_ms',
                'ws_opts = line_sender_opts_clone(self._qwp_ws_opts)',
                'c_overrides[i].column = PyBytes_AsString(name_bytes)',
                'cdef qdb_pystr_buf* b = NULL',
                'buf = self._buffer'):
            self.assertIn(
                token, sources,
                f'the capture table names {token!r}, which the sources '
                'no longer contain')

        # The one capture the table records as *not* convertible has to
        # still be there too, with its guard.
        self.assertIn('_check_not_in_own_callback', sources)

    #: Every `_column_*` helper that cannot run the caller's Python while
    #: it holds native state, and the reason. A helper not listed here has
    #: to be covered by a `hostile_<kind>` fixture in the re-entrancy grid
    #: instead. Keeping the two sets exhaustive between them is what makes
    #: the grid's value axis derived rather than remembered: adding a
    #: `_column_*` helper fails this test until somebody decides which
    #: side it belongs on.
    COLUMN_HELPERS_WITHOUT_CALLER_PYTHON = {
        'bool': 'reached only under PyBool_Check, so the value is exactly '
                'True or False and the bint conversion is a pointer compare',
        'int': 'reached only under PyLong_CheckExact; '
               'PyLong_AsLongLongAndOverflow is a C call',
        'i64': 'takes an int64_t the caller already converted',
        'f64': 'reached only under PyFloat_CheckExact',
        'str': 'reached only under PyUnicode_CheckExact; str_to_utf8 is C',
        'ts_micros': 'reads a cdef field of TimestampMicros',
        'ts_nanos': 'reads a cdef field of TimestampNanos',
        'numpy': 'reached only under PyArray_CheckExact; every PyArray_* '
                 'access is a C macro and arr.dtype is read only when '
                 'raising',
        'decimal': 'PyObject_TypeCheck and the _decimal memory layout are '
                   'both C; a Decimal subclass has no method called here',
        'binary': 'bytes and bytearray are read through C macros, and '
                  'memoryview is not subclassable, so no __getbuffer__ of '
                  "the caller's can run",
        'char': 'reads a cdef field of Char',
        'date_millis': 'reads a cdef field of DateMillis',
        'long256': 'reads a cdef field of Long256',
        'geohash': 'reads cdef fields of Geohash',
        'qwp_only': 'a dispatch chain of type checks; every branch it '
                    'picks is itself a _column_* helper',
    }

    def test_every_column_helper_that_runs_caller_python_has_a_hostile_cell(self):
        """The re-entrancy grid takes the calls it re-enters from
        `api_surface` by reflection, but the values that *run* the
        caller's Python inside those calls were a hand-written set. A
        column value's conversion is one of the three windows where
        caller code runs mid-row, and which `_column_*` branch a value
        reaches decides what native state is live while it does.

        So the value axis is derived here too: every `_column_*` helper
        is either declared unable to run caller Python, with the reason,
        or has a `hostile_<kind>` fixture in the grid."""
        source = (PROJ_ROOT / 'src' / 'questdb' / '_client.pyx').read_text()
        helpers = set(re.findall(
            r'cdef\s+(?:inline\s+)?void_int\s+_column_(\w+)\s*\(', source))
        # `_column` itself is the dispatch, not a helper.
        helpers.discard('')
        self.assertTrue(helpers, 'no _column_* helpers found; the pattern '
                                 'this test scans for has changed')

        grid = (PROJ_ROOT / 'test' / 'reentrancy_matrix.py').read_text()
        fixtures = set(re.findall(r'^def hostile_(\w+)\(', grid, re.M))

        declared = set(self.COLUMN_HELPERS_WITHOUT_CALLER_PYTHON)
        uncovered = helpers - declared - fixtures
        self.assertFalse(
            uncovered,
            f'{sorted(uncovered)}: these _column_* helpers neither declare '
            'why they cannot run the caller\'s Python nor have a '
            'hostile_<kind> fixture in the re-entrancy grid. Add the '
            'fixture, or add the helper to '
            'COLUMN_HELPERS_WITHOUT_CALLER_PYTHON with its reason.')

        stale = declared - helpers
        self.assertFalse(
            stale,
            f'{sorted(stale)}: declared unable to run caller Python, but no '
            '_column_* helper goes by that name any more.')

        # A fixture must be reachable from at least one outer scenario,
        # or it measures nothing.
        for kind in sorted(fixtures & helpers):
            self.assertIn(
                f'hostile_{kind}(hook)', grid,
                f'hostile_{kind} is defined but no outer scenario calls it, '
                'so no grid cell exercises that branch.')

    def test_a_row_serializing_dataframe_says_which_claims_it_drops(self):
        """`df.attrs['questdb']` is read by the columnar writers. A frame
        serialized a row at a time never reaches them, and the kinds the
        claim names all ride on integers or blobs, so the column lands as
        a LONG or a BINARY and a table created by the write gets that
        type. Nothing about the values says otherwise, so the log notice is
        the only signal."""
        if pd is None:
            self.skipTest('pandas not installed')
        df = pd.DataFrame({
            'ip': np.array([0xC0A8012A], np.uint32),
            'c': np.array([65], np.uint16),
            'g': np.array([1], np.int8),
            'ts': pd.to_datetime(['2020-01-01'])})
        df.attrs['questdb'] = {'version': 1, 'columns': {
            'ip': {'kind': 'ipv4'},
            'c': {'kind': 'char'},
            'g': {'kind': 'geohash', 'precision_bits': 7}}}

        with warnings.catch_warnings():
            warnings.simplefilter('error')
            with self.assertLogs('questdb', level='WARNING') as caught:
                with Server() as server:
                    with qi.Sender.from_conf(
                            f'tcp::addr=localhost:{server.port};') as sender:
                        sender.dataframe(df, table_name='t', at='ts')
        messages = [record.getMessage() for record in caught.records]
        dropped = [m for m in messages if m.startswith('questdb: column')]
        self.assertEqual(
            len(dropped), 3,
            f'expected one log notice per claimed column, got: {messages}')
        for kind in ("'ipv4'", "'char'", "'geohash'"):
            self.assertTrue(
                any(kind in m for m in dropped),
                f'no log notice named kind {kind}: {dropped}')

    def test_a_frame_without_a_claim_logs_no_notice(self):
        """The notice above is about a claim this route cannot apply.
        A frame carrying none has nothing to say, and a write that logs
        on every frame trains the reader to filter it."""
        if pd is None:
            self.skipTest('pandas not installed')
        df = pd.DataFrame({
            'v': [1], 'ts': pd.to_datetime(['2020-01-01'])})
        with Server() as server:
            with self.assertNoLogs('questdb', level='WARNING'):
                with qi.Sender.from_conf(
                        f'tcp::addr=localhost:{server.port};') as sender:
                    sender.dataframe(df, table_name='t', at='ts')

    def test_a_batch_reporting_a_negative_row_count_is_refused(self):
        """`ArrowArray.length` arrives from the caller's own producer and
        must be rejected by the native importer before it reaches pointer
        arithmetic or value encoding.

        Forged here rather than found: `__arrow_c_array__` is re-imported
        through pyarrow, which normalises the struct, so only a
        hand-built `__arrow_c_stream__` reaches the importer with the batch
        the producer actually wrote."""
        try:
            import pyarrow as pa
        except ImportError:
            self.skipTest('pyarrow not installed')

        class ArrowArrayStream(ctypes.Structure):
            pass

        get_schema_t = ctypes.CFUNCTYPE(
            ctypes.c_int, ctypes.POINTER(ArrowArrayStream), ctypes.c_void_p)
        get_next_t = ctypes.CFUNCTYPE(
            ctypes.c_int, ctypes.POINTER(ArrowArrayStream), ctypes.c_void_p)
        get_err_t = ctypes.CFUNCTYPE(
            ctypes.c_char_p, ctypes.POINTER(ArrowArrayStream))
        release_t = ctypes.CFUNCTYPE(None, ctypes.POINTER(ArrowArrayStream))
        ArrowArrayStream._fields_ = [
            ('get_schema', get_schema_t), ('get_next', get_next_t),
            ('get_last_error', get_err_t), ('release', release_t),
            ('private_data', ctypes.c_void_p)]

        capsule_ptr = ctypes.pythonapi.PyCapsule_GetPointer
        capsule_ptr.restype = ctypes.c_void_p
        capsule_ptr.argtypes = [ctypes.py_object, ctypes.c_char_p]
        capsule_new = ctypes.pythonapi.PyCapsule_New
        capsule_new.restype = ctypes.py_object
        capsule_new.argtypes = [
            ctypes.c_void_p, ctypes.c_char_p, ctypes.c_void_p]

        table = pa.table({
            'g': pa.array([1, 2], pa.int8()),
            'ts': pa.array([0, 1_000_000_000], pa.timestamp('ns'))})
        inner_capsule = table.__arrow_c_stream__()
        inner = ctypes.cast(
            capsule_ptr(inner_capsule, b'arrow_array_stream'),
            ctypes.POINTER(ArrowArrayStream))
        forged = ArrowArrayStream()

        def _schema(_self, out_ptr):
            return inner.contents.get_schema(inner, out_ptr)

        def _next(_self, out_ptr):
            rc = inner.contents.get_next(inner, out_ptr)
            if rc == 0 and out_ptr:
                # `length` is the first field of `ArrowArray`.
                length = ctypes.c_int64.from_address(out_ptr)
                if length.value > 0:
                    length.value = -1
            return rc

        def _last_error(_self):
            return inner.contents.get_last_error(inner)

        def _release(_self):
            pass  # `inner_capsule`'s own destructor releases the real stream

        callbacks = (get_schema_t(_schema), get_next_t(_next),
                     get_err_t(_last_error), release_t(_release))
        (forged.get_schema, forged.get_next,
         forged.get_last_error, forged.release) = callbacks

        class Producer:
            def __arrow_c_stream__(self, requested_schema=None):
                return capsule_new(
                    ctypes.addressof(forged), b'arrow_array_stream', None)

        with QwpAckServer() as server:
            with qi.Sender.from_conf(
                    f'ws::addr=127.0.0.1:{server.port};') as sender:
                with self.assertRaises(qi.QuestDBError) as caught:
                    sender.dataframe(
                        Producer(), table_name='t', at='ts',
                        schema_overrides={'g': ('geohash', 5)})
        self.assertIn(
            'Arrow array root: length -1 is negative',
            str(caught.exception))
        # Keep the trampolines alive until the send is over.
        self.assertIsNotNone(callbacks)
        self.assertIsNotNone(inner_capsule)

    def test_close_stops_waiting_on_a_lease_that_can_never_come_back(self):
        """`close()` waits for outstanding leases, and a lease comes back
        from whichever thread holds it. A caller holding the last lease
        itself waits on its own frame, so the wait cannot end. The bound
        turns that into an error the caller can act on. Closing is
        one-way: from the `close()` that proceeds on, the handle
        refuses new work, the lease keeps working so its holder can
        finish and return it, and a second `close()` completes the
        teardown."""
        original = qi._debug_close_lease_wait_limit_s()
        qi._debug_set_close_lease_wait_limit_s(0.5)
        try:
            with QwpAckServer() as server:
                conf = (f'ws::addr=127.0.0.1:{server.port};'
                        'lazy_connect=true;'
                        'sender_pool_min=1;sender_pool_max=2;')
                with warnings.catch_warnings():
                    warnings.simplefilter('ignore')
                    db = qi.QuestDB.from_conf(conf)
                    lease = db.sender()
                    lease.row('t', columns={'v': 1},
                              at=qi.TimestampNanos(1))
                    with self.assertRaises(qi.QuestDBError) as caught:
                        db.close()
                    self.assertIn(
                        'outstanding sender()/reader() lease',
                        str(caught.exception))
                    self.assertIn('close() again', str(caught.exception))
                    # One-way: the handle takes no new work from the
                    # first close() on, and the refusal is permanent.
                    with self.assertRaises(qi.QuestDBError) as refused:
                        db.sender()
                    self.assertIn('closing', str(refused.exception))
                    # The lease keeps working while the handle drains,
                    # so its holder can finish and return it.
                    lease.row('t', columns={'v': 2},
                              at=qi.TimestampNanos(2))
                    lease.close()
                    # With nothing outstanding, close() finishes the
                    # teardown.
                    db.close()
                    with self.assertRaises(qi.QuestDBError) as closed:
                        db.sender()
                    self.assertIn('closed', str(closed.exception))
        finally:
            qi._debug_set_close_lease_wait_limit_s(original)

    def test_no_close_reports_success_while_a_lease_is_outstanding(self):
        """Two threads close a handle whose one lease is never returned
        while either waits. The teardown runs only when nothing is
        using the handle, so success is not a possible outcome for
        either thread, whatever the interleaving: both raise the
        still-draining error. The handle stays closing, and returning
        the lease and closing once more finishes the teardown."""
        original = qi._debug_close_lease_wait_limit_s()
        qi._debug_set_close_lease_wait_limit_s(1.0)
        try:
            with QwpAckServer() as server:
                conf = (f'ws::addr=127.0.0.1:{server.port};'
                        'lazy_connect=true;'
                        'sender_pool_min=1;sender_pool_max=3;')
                with warnings.catch_warnings():
                    warnings.simplefilter('ignore')
                    db = qi.QuestDB.from_conf(conf)
                    lease = db.sender()
                    lease.row('t', columns={'v': 1},
                              at=qi.TimestampNanos(1))
                    outcome = {}

                    def close_into(tag):
                        def run():
                            try:
                                db.close()
                                outcome[tag] = 'returned'
                            except qi.QuestDBError as exc:
                                outcome[tag] = str(exc)
                        return run

                    first = threading.Thread(target=close_into('first'))
                    first.start()
                    time.sleep(0.3)
                    second = threading.Thread(target=close_into('second'))
                    second.start()
                    first.join(timeout=30)
                    second.join(timeout=30)
                    self.assertFalse(
                        first.is_alive() or second.is_alive(),
                        'a close() never returned')

                    # The lease is returned only after both joins, so
                    # neither close() can have found the handle idle.
                    for tag in ('first', 'second'):
                        self.assertIn(
                            'outstanding sender()/reader() lease',
                            outcome.get(tag, '<no outcome>'),
                            f'{tag} close() did not report the '
                            f'outstanding lease: {outcome!r}')
                    # Closing, not open: no new lease is handed out.
                    with self.assertRaises(qi.QuestDBError) as refused:
                        db.sender()
                    self.assertIn('closing', str(refused.exception))
                    lease.close()
                    db.close()
                    with self.assertRaises(qi.QuestDBError) as closed:
                        db.sender()
                    self.assertIn('closed', str(closed.exception))
        finally:
            qi._debug_set_close_lease_wait_limit_s(original)

    def test_close_names_a_running_call_rather_than_blaming_a_lease(self):
        """`dataframe()` counts as a call in progress, not a lease.
        A `close()` that stops waiting while one is running says so --
        advice to close leases would point at nothing -- and the call
        itself runs to completion. A later `close()` then finishes the
        teardown."""
        if pyarrow is None:
            self.skipTest('pyarrow not installed')
        original = qi._debug_close_lease_wait_limit_s()
        qi._debug_set_close_lease_wait_limit_s(0.5)
        try:
            with QwpAckServer() as server:
                conf = (f'ws::addr=127.0.0.1:{server.port};'
                        'lazy_connect=true;'
                        'sender_pool_min=1;sender_pool_max=3;')
                with warnings.catch_warnings():
                    warnings.simplefilter('ignore')
                    db = qi.QuestDB.from_conf(conf)
                    table = pyarrow.table(
                        {'v': pyarrow.array([1, 2, 3], pyarrow.int64())})
                    started = threading.Event()
                    unblock = threading.Event()
                    outcome = {}

                    class SlowProducer:
                        """An ordinary Arrow producer whose first batch
                        takes a while -- a slow scan, a remote fetch."""
                        def __arrow_c_stream__(self, requested_schema=None):
                            started.set()
                            unblock.wait(timeout=30)
                            return table.__arrow_c_stream__(
                                requested_schema)

                    def load():
                        try:
                            db.dataframe(SlowProducer(), table_name='t',
                                         at=qi.ServerTimestamp)
                            outcome['load'] = 'ok'
                        except qi.QuestDBError as exc:
                            outcome['load'] = str(exc)

                    loader = threading.Thread(target=load)
                    loader.start()
                    try:
                        self.assertTrue(
                            started.wait(timeout=30),
                            'the dataframe() call never started')
                        with self.assertRaises(qi.QuestDBError) as caught:
                            db.close()
                        self.assertIn(
                            '1 call(s) in progress',
                            str(caught.exception))
                        self.assertIn(
                            '0 outstanding sender()/reader() lease(s)',
                            str(caught.exception))
                    finally:
                        unblock.set()
                        loader.join(timeout=30)
                    self.assertFalse(loader.is_alive(),
                                     'dataframe() never returned')
                    # The call that was in flight when close() was
                    # asked runs to completion.
                    self.assertEqual(outcome.get('load'), 'ok')
                    db.close()
                    with self.assertRaises(qi.QuestDBError) as closed:
                        db.sender()
                    self.assertIn('closed', str(closed.exception))
        finally:
            qi._debug_set_close_lease_wait_limit_s(original)

    def test_leaving_a_with_block_on_an_error_keeps_the_users_exception(self):
        """The frames unwinding out of a `with` block are often the ones
        holding the leases `close()` waits for, so the close can fail on
        account of the very exception being reported. `__exit__` reports
        that failure through the `questdb` logger and lets the original
        through: an `except` clause around the block is written for the
        original, not for a shutdown complaint that follows from it.
        The close is still attempted -- the handle comes out closing."""
        class Boom(Exception):
            pass

        original = qi._debug_close_lease_wait_limit_s()
        qi._debug_set_close_lease_wait_limit_s(0.5)
        try:
            with QwpAckServer() as server:
                conf = (f'ws::addr=127.0.0.1:{server.port};'
                        'lazy_connect=true;'
                        'sender_pool_min=1;sender_pool_max=2;')
                db = qi.QuestDB.from_conf(conf)
                lease = db.sender()
                with self.assertLogs('questdb', level='ERROR') as logged:
                    # The lease is held by this frame, so the close on
                    # the way out cannot finish.
                    with self.assertRaises(Boom):
                        with db:
                            lease.row('t', columns={'v': 1},
                                      at=qi.TimestampNanos(1))
                            raise Boom("the user's error")
                self.assertTrue(
                    any('close()' in line for line in logged.output),
                    'the close failure was not reported through the '
                    f'questdb logger: {logged.output}')
                # Attempted, not skipped: the handle is closing.
                with self.assertRaises(qi.QuestDBError) as refused:
                    db.sender()
                self.assertIn('closing', str(refused.exception))
                lease.close()
                db.close()
        finally:
            qi._debug_set_close_lease_wait_limit_s(original)

    def test_the_close_wait_reports_its_progress_through_the_logger(self):
        """A wait long enough to notice says what it is still waiting
        for every five seconds. The notice goes to the `questdb`
        logger rather than through `warnings`, because a warning
        becomes an exception under `-W error` -- which would end the
        wait at the first notice instead of at the bound, and hand the
        caller a `UserWarning` where the contract promises a
        `QuestDBError`."""
        original = qi._debug_close_lease_wait_limit_s()
        qi._debug_set_close_lease_wait_limit_s(0.5)
        try:
            with QwpAckServer() as server:
                conf = (f'ws::addr=127.0.0.1:{server.port};'
                        'lazy_connect=true;'
                        'sender_pool_min=1;sender_pool_max=2;')
                db = qi.QuestDB.from_conf(conf)
                lease = db.sender()
                lease.row('t', columns={'v': 1},
                          at=qi.TimestampNanos(1))
                with warnings.catch_warnings():
                    warnings.simplefilter('error')
                    with self.assertLogs('questdb',
                                         level='WARNING') as logged:
                        with self.assertRaises(qi.QuestDBError) as caught:
                            db.close()
                self.assertIn(
                    'outstanding sender()/reader() lease',
                    str(caught.exception))
                self.assertTrue(
                    any('is waiting for' in line
                        for line in logged.output),
                    'the wait reported no progress through the '
                    f'questdb logger: {logged.output}')
                lease.close()
                db.close()
        finally:
            qi._debug_set_close_lease_wait_limit_s(original)

    def test_the_close_notice_is_emitted_without_the_handle_lock(self):
        """The progress notice runs the caller's logging handlers, and
        a handler is the caller's code: it may block, or reach this
        very handle from another thread -- a handler that ingests its
        log lines into QuestDB does both. Each notice is emitted with
        the handle's state lock released, so while a handler runs, a
        call on the handle from another thread is answered rather than
        queued behind the closer. Queued, it would stand until the
        handler finished -- and a handler waiting on the handle would
        keep that from ever happening."""
        original = qi._debug_close_lease_wait_limit_s()
        qi._debug_set_close_lease_wait_limit_s(0.5)
        log = logging.getLogger('questdb')
        outcome = {}

        class TouchesTheHandle(logging.Handler):
            def __init__(self, db):
                super().__init__()
                self.db = db

            def emit(self, record):
                if 'answered' in outcome:
                    return
                answered = threading.Event()

                def probe():
                    try:
                        self.db.reap_idle()
                    except qi.QuestDBError:
                        pass
                    answered.set()

                threading.Thread(target=probe, daemon=True).start()
                outcome['answered'] = answered.wait(5.0)

        try:
            with QwpAckServer() as server:
                conf = (f'ws::addr=127.0.0.1:{server.port};'
                        'lazy_connect=true;'
                        'sender_pool_min=1;sender_pool_max=2;')
                db = qi.QuestDB.from_conf(conf)
                lease = db.sender()
                lease.row('t', columns={'v': 1},
                          at=qi.TimestampNanos(1))
                handler = TouchesTheHandle(db)
                propagate = log.propagate
                log.addHandler(handler)
                log.propagate = False
                try:
                    with self.assertRaises(qi.QuestDBError) as caught:
                        db.close()
                finally:
                    log.removeHandler(handler)
                    log.propagate = propagate
                self.assertIn(
                    'outstanding sender()/reader() lease',
                    str(caught.exception))
                self.assertTrue(
                    outcome.get('answered'),
                    'a call on the handle was not answered while the '
                    f'close notice was being handled: {outcome!r}')
                lease.close()
                db.close()
        finally:
            qi._debug_set_close_lease_wait_limit_s(original)

    def test_leaving_a_with_block_cleanly_still_raises_on_a_stuck_close(self):
        """Nothing is being reported on a clean exit, so there is no
        exception for a close failure to displace -- and a lease left
        open there is a leak worth hearing about. `__exit__` therefore
        raises just as `close()` does."""
        original = qi._debug_close_lease_wait_limit_s()
        qi._debug_set_close_lease_wait_limit_s(0.5)
        try:
            with QwpAckServer() as server:
                conf = (f'ws::addr=127.0.0.1:{server.port};'
                        'lazy_connect=true;'
                        'sender_pool_min=1;sender_pool_max=2;')
                db = qi.QuestDB.from_conf(conf)
                lease = db.sender()
                with self.assertLogs('questdb', level='WARNING'):
                    with self.assertRaises(qi.QuestDBError) as caught:
                        with db:
                            lease.row('t', columns={'v': 1},
                                      at=qi.TimestampNanos(1))
                self.assertIn(
                    'outstanding sender()/reader() lease',
                    str(caught.exception))
                lease.close()
                db.close()
        finally:
            qi._debug_set_close_lease_wait_limit_s(original)

    def test_a_callback_close_on_a_busy_handle_refuses_without_waiting(self):
        """`close()` from inside the handle's own `connection_listener`
        can wait on nothing: while the callback runs, the dispatch
        thread delivers no events, including whatever the wait would
        need. With a lease outstanding it refuses at once -- at the
        shipped 60s bound, so a wait would be unmistakable -- and the
        refusal publishes nothing: the handle keeps lending, and an
        ordinary thread still closes it."""
        box = {}

        def listener(event):
            if 'res' in box:
                return
            t0 = time.monotonic()
            try:
                box['db'].close()
                box['res'] = 'returned'
            except qi.QuestDBError as exc:
                box['res'] = str(exc)
            box['dt'] = time.monotonic() - t0

        with QwpAckServer() as server:
            conf = (f'ws::addr=127.0.0.1:{server.port};'
                    'lazy_connect=true;'
                    'sender_pool_min=1;sender_pool_max=2;')
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                db = qi.QuestDB.from_conf(
                    conf, connection_listener=listener)
                box['db'] = db
                lease = db.sender()
                lease.row('t', columns={'v': 1},
                          at=qi.TimestampNanos(1))
                # `lazy_connect` defers the connection to this flush,
                # whose Connected event runs the listener while the
                # lease is still outstanding.
                lease.flush()
                for _ in range(200):
                    if 'res' in box:
                        break
                    time.sleep(0.05)
                self.assertIn(
                    "from inside this handle's own",
                    box.get('res', '<listener never ran>'))
                self.assertIn('another thread', box['res'])
                # A refusal, not a wait: anything near the 60s bound
                # means the guard did not fire.
                self.assertLess(box['dt'], 2.0)
                # And it published nothing: the handle still lends.
                spare = db.sender()
                spare.close()
                lease.close()
                db.close()
                with self.assertRaises(qi.QuestDBError) as closed:
                    db.sender()
                self.assertIn('closed', str(closed.exception))

    def test_no_column_helper_takes_a_pre_encoded_name(self):
        """A `line_sender_column_name` borrows the string arena, which
        `Buffer.clear()` recycles, so it stays valid only while no
        Python runs. Each `_column_*` helper therefore takes the name as
        a `str` and encodes it itself, immediately before its own native
        call and after any conversion that runs the caller's code.

        Passing one across a function boundary is what puts a live
        borrow either side of a conversion, so the signatures are where
        this is held."""
        source = (PROJ_ROOT / 'src' / 'questdb' / '_client.pyx').read_text()
        offenders = []
        for match in re.finditer(
                r'cdef\s+(?:inline\s+)?void_int\s+(_column_\w+)\s*\('
                r'(?P<params>[^)]*)\)', source):
            if 'line_sender_column_name' in match.group('params'):
                offenders.append(match.group(1))
        self.assertFalse(
            offenders,
            f'{sorted(offenders)}: these take a pre-encoded '
            'line_sender_column_name. The arena borrow then spans the '
            'call boundary, and any conversion inside that runs the '
            "caller's Python can recycle it. Take `str name` and encode "
            'inside the helper instead.')

    def test_a_lease_cannot_be_released_from_inside_its_own_dataframe(self):
        """`dataframe()` on a lease runs the caller's Python for the whole
        plan build. Returning the lease to the pool from there left the
        rest of the call working against a closed lease, so every later
        use in the enclosing block raised `Sender is closed`."""
        if pd is None:
            self.skipTest('pandas not installed')
        outcome = []

        with QwpAckServer() as server:
            with qi.QuestDB.from_conf(
                    f'ws::addr=127.0.0.1:{server.port};'
                    'sender_pool_min=0;sender_pool_max=2;'
                    'query_pool_min=0;pool_reap=manual;') as db:
                lease = db.sender()
                other = db.sender()

                class HostileFrame(pd.DataFrame):
                    fired = False

                    @property
                    def attrs(self):
                        if not HostileFrame.fired:
                            HostileFrame.fired = True
                            for name, target in (('own', lease),
                                                 ('other', other)):
                                try:
                                    target.close()
                                except qi.QuestDBError as exc:
                                    outcome.append((name, exc))
                                else:
                                    outcome.append((name, None))
                        return {}

                    @attrs.setter
                    def attrs(self, value):
                        pass

                lease.dataframe(
                    HostileFrame({
                        'v': [1],
                        'ts': pd.to_datetime([0], unit='s')}),
                    table_name='t', at='ts')

                results = dict(outcome)
                self.assertIsNotNone(
                    results['own'],
                    'the lease was released from inside its own call')
                self.assertEqual(
                    results['own'].code,
                    qi.QuestDBErrorCode.InvalidApiCall)
                self.assertIn(
                    "close() can't be called from inside a call on this "
                    'sender lease',
                    str(results['own']))
                # A lease that is not the one running the call has
                # nothing to do with it and must still close.
                self.assertIsNone(
                    results['other'],
                    'an unrelated lease was refused a legitimate close')

                # The lease survived the refusal and still works.
                lease.row('after', columns={'v': 1}, at=qi.ServerTimestamp)
                lease.close()

    def test_closing_a_sender_mid_row_covers_the_buffer_it_flushes(self):
        """`close(flush=True)` flushes the sender's internal buffer, so
        that is the buffer it holds to "no row part-way through". A
        buffer of the caller's own is neither flushed nor checked: the
        sender is never told which buffers exist. Its bytes survive; what
        it loses is the sender that could have sent them."""
        internal = []
        external = []
        own_buffer = None

        class HostileTz(datetime.tzinfo):
            """`at` is written last, so its conversion runs with the row
            still part-way through."""

            def __init__(self, log):
                self.log = log

            def utcoffset(self, dt):
                if not self.log:
                    try:
                        sender.close()
                    except qi.QuestDBError as exc:
                        self.log.append(exc)
                    else:
                        self.log.append(None)
                return datetime.timedelta(0)

            def dst(self, dt):
                return datetime.timedelta(0)

            def tzname(self, dt):
                return 'UTC'

        with Server() as server_sock:
            sender = qi.Sender.from_conf(
                f'tcp::addr=localhost:{server_sock.port};')
            sender.establish()
            server_sock.accept()
            sender.row(
                'internal', columns={'a': 1},
                at=datetime.datetime(2020, 1, 1, tzinfo=HostileTz(internal)))
            self.assertEqual(len(internal), 1)
            self.assertIsNotNone(
                internal[0], 'close() was allowed mid-row on the buffer '
                'it flushes')
            self.assertIn(
                "can't be called while a row is being written",
                str(internal[0]))
            sender.close(flush=False)

        with Server() as server_sock:
            sender = qi.Sender.from_conf(
                f'tcp::addr=localhost:{server_sock.port};')
            sender.establish()
            server_sock.accept()
            own_buffer = sender.new_buffer()
            own_buffer.row(
                'external', columns={'a': 1},
                at=datetime.datetime(2020, 1, 1, tzinfo=HostileTz(external)))
            self.assertEqual(len(external), 1)
            self.assertIsNone(
                external[0],
                'a buffer the sender was never handed is not its to '
                'refuse on behalf of')
            # The decision, pinned: no loss. The bytes are all there,
            # and only the sender is gone.
            self.assertIn(b'external a=1i', bytes(own_buffer))

    def test_reading_a_sender_mid_row_is_allowed_and_may_end_mid_line(self):
        """`bytes()` and `len()` are debug surfaces: they act on nothing,
        so they are answered mid-row rather than refused. The answer is
        the buffer as it stands, with the last line unterminated."""
        snapshots = []

        class HostileTz(datetime.tzinfo):
            def utcoffset(self, dt):
                if not snapshots:
                    snapshots.append((bytes(sender), len(sender)))
                return datetime.timedelta(0)

            def dst(self, dt):
                return datetime.timedelta(0)

            def tzname(self, dt):
                return 'UTC'

        with Server() as server_sock:
            sender = qi.Sender.from_conf(
                f'tcp::addr=localhost:{server_sock.port};')
            sender.establish()
            server_sock.accept()
            sender.row('outer', columns={'a': 1}, at=qi.TimestampNanos(1))
            sender.row(
                'inner', columns={'b': 2},
                at=datetime.datetime(2020, 1, 1, tzinfo=HostileTz()))
            self.assertEqual(len(snapshots), 1)
            mid_row_bytes, mid_row_len = snapshots[0]
            self.assertEqual(mid_row_bytes, b'outer a=1i 1\ninner b=2i')
            self.assertEqual(mid_row_len, len(mid_row_bytes))
            self.assertFalse(
                mid_row_bytes.endswith(b'\n'),
                'the row was still being written, so its line is not '
                'terminated yet')
            # And the finished row does terminate.
            self.assertTrue(bytes(sender).endswith(b'\n'))
            sender.close(flush=False)

    def test_closing_a_handle_from_inside_a_lease_call_is_refused(self):
        """A lease is counted in the handle's total alone -- it is handed
        out by `sender()` and may be returned from another thread -- so
        the per-thread count that catches a self-waiting `close()` could
        not see one, and a `close()` from inside a lease's own call waited
        forever on the frame that called it.

        A lease *method call* is scoped in the sense that count means: it
        begins and ends inside one call on one thread. Counting it there
        makes the existing refusal fire."""
        outcome = []
        hostile_ran = threading.Event()

        class HostileAddr(ipaddress.IPv4Address):
            """Its conversion to an int is pure Python, so it runs inside
            `row()`, between the row starting and the row finishing."""

            def __int__(self):
                hostile_ran.set()
                try:
                    db.close()
                except qi.QuestDBError as exc:
                    outcome.append(exc)
                else:
                    outcome.append(None)
                return 0x01020304

        with QwpAckServer() as server:
            db = qi.QuestDB.from_conf(
                f'ws::addr=127.0.0.1:{server.port};'
                'sender_pool_min=0;sender_pool_max=1;'
                'query_pool_min=0;pool_reap=manual;')
            lease = db.sender()
            worker = threading.Thread(
                target=lambda: lease.row(
                    'hostile',
                    columns={'ip': HostileAddr('192.0.2.1')},
                    at=qi.ServerTimestamp),
                daemon=True)
            worker.start()
            worker.join(20)
            self.assertTrue(
                hostile_ran.is_set(), 'the conversion never ran')
            # A hung close() holds the lease's lock and the handle's
            # condition, so nothing below could clean up. Report the
            # hang and leave the daemon thread to die with the process.
            self.assertFalse(
                worker.is_alive(),
                'close() hung inside a lease call instead of being '
                'refused')
            self.assertEqual(len(outcome), 1)
            self.assertIsNotNone(
                outcome[0], 'close() was allowed inside a lease call')
            self.assertEqual(
                outcome[0].code, qi.QuestDBErrorCode.InvalidApiCall)
            self.assertIn(
                "close() can't be called from inside a call on this "
                'QuestDB handle',
                str(outcome[0]))
            # The handle survives the refusal and still works.
            lease.row('after', columns={'v': 1}, at=qi.ServerTimestamp)
            lease.close()
            db.close()

    def test_a_lease_call_does_not_refuse_a_close_from_another_thread(self):
        """Only the thread running the lease call is waiting on itself.
        A `close()` on any other thread is a legitimate wait, and must
        still wait rather than be refused."""
        in_row = threading.Event()
        release = threading.Event()
        closed = []

        class SlowAddr(ipaddress.IPv4Address):
            def __int__(self):
                in_row.set()
                release.wait(20)
                return 0x01020304

        with QwpAckServer() as server:
            db = qi.QuestDB.from_conf(
                f'ws::addr=127.0.0.1:{server.port};'
                'sender_pool_min=0;sender_pool_max=1;'
                'query_pool_min=0;pool_reap=manual;')
            lease = db.sender()
            writer = threading.Thread(
                target=lambda: lease.row(
                    'slow', columns={'ip': SlowAddr('192.0.2.1')},
                    at=qi.ServerTimestamp),
                daemon=True)
            writer.start()
            self.assertTrue(in_row.wait(20))

            def close_from_elsewhere():
                try:
                    db.close()
                except qi.QuestDBError as exc:
                    closed.append(exc)
                else:
                    closed.append(None)

            closer = threading.Thread(
                target=close_from_elsewhere, daemon=True)
            closer.start()
            # It must still be waiting on the lease, not refused.
            closer.join(1.0)
            self.assertTrue(
                closer.is_alive(),
                'close() on another thread was refused instead of '
                'waiting for the lease')
            release.set()
            writer.join(20)
            lease.close()
            closer.join(20)
            self.assertFalse(closer.is_alive(), 'close() never returned')
            self.assertEqual(closed, [None])

    @unittest.skipIf(pd is None, 'pandas not installed')
    def test_closing_a_handle_from_inside_its_own_call_is_refused(self):
        """`QuestDB.close()` waits for outstanding uses to be released.
        Called from inside one of this handle's own calls it is waiting
        on the frame that would release it, so it never returned -- a
        warning every five seconds, forever. It is refused instead."""
        closed = []

        class HostileFrame(pd.DataFrame):
            fired = False

            @property
            def attrs(self):
                if not HostileFrame.fired:
                    HostileFrame.fired = True
                    try:
                        db.close()
                    except qi.QuestDBError as exc:
                        closed.append(exc)
                    else:
                        closed.append(None)
                return {}

            @attrs.setter
            def attrs(self, value):
                pass

        with QwpAckServer() as server:
            with qi.QuestDB.from_conf(
                    f'ws::addr=127.0.0.1:{server.port};'
                    'sender_pool_min=0;query_pool_min=0;'
                    'pool_reap=manual;') as db:
                frame = HostileFrame({
                    'v': [1, 2],
                    'ts': pd.to_datetime([0, 1], unit='s')})
                db.dataframe(frame, table_name='t', at='ts')

                self.assertEqual(len(closed), 1)
                self.assertIsNotNone(
                    closed[0], 'close() hung instead of refusing')
                self.assertEqual(
                    closed[0].code, qi.QuestDBErrorCode.InvalidApiCall)
                self.assertIn(
                    "close() can't be called from inside a call on this "
                    'QuestDB handle',
                    str(closed[0]))
                # The handle survives the refusal and still works.
                db.dataframe(
                    pd.DataFrame({
                        'v': [3],
                        'ts': pd.to_datetime([2], unit='s')}),
                    table_name='t', at='ts')

    @unittest.skipIf(pd is None, 'pandas not installed')
    @unittest.skipIf(pyarrow is None, 'pyarrow not installed')
    def test_every_number_in_the_claim_vocabulary_reads_the_same_way(self):
        """One predicate covers every number this client reads out of a
        round-trip claim, a `schema_overrides` entry, or one of the
        QWP-only wrapper classes: a whole number that is not a boolean.

        So `numpy.int64` works wherever a plain `int` does -- which it
        has to, because a claim rebuilt from array metadata carries one
        and a pandas cell hands one back -- and `True` works nowhere.

        The table is the point. These two rules arrived one field at a
        time, each fixed where it was reported and nowhere else, which
        left `numpy.int64` accepted for a claim's version and dropped
        for the precision two functions away. A field of this
        vocabulary that is not a row here is a field nobody has
        checked."""
        def numpy_frame(bits):
            frame = pd.DataFrame({
                'gh': np.array([0], dtype=np.int32),
                'ts': pd.to_datetime([0], unit='s')})
            frame.attrs['questdb'] = {
                'version': 1,
                'columns': {'gh': {'kind': 'geohash',
                                   'precision_bits': bits}}}
            return frame

        def arrow_frame(bits):
            frame = pd.DataFrame({
                'gh': pd.array([0], dtype=pd.ArrowDtype(pyarrow.int32())),
                'ts': pd.array(
                    pyarrow.array([0], pyarrow.timestamp('ns')),
                    dtype=pd.ArrowDtype(pyarrow.timestamp('ns')))})
            frame.attrs['questdb'] = {
                'version': 1,
                'columns': {'gh': {'kind': 'geohash',
                                   'precision_bits': bits}}}
            return frame

        def all_null_frame(bits):
            frame = pd.DataFrame({
                'gh': pd.array([None], dtype=object),
                'ts': pd.to_datetime([0], unit='s')})
            frame.attrs['questdb'] = {
                'version': 1,
                'columns': {'gh': {'kind': 'geohash',
                                   'precision_bits': bits}}}
            return frame

        def wire_type(frame, **kwargs):
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                return self._dataframe_column_types(
                    frame, table_name='int_vocab', at='ts',
                    **kwargs).get('gh')

        def version_claim(value):
            frame = numpy_frame(20)
            frame.attrs['questdb'] = dict(
                frame.attrs['questdb'], version=value)
            return wire_type(frame)

        # (name, good value, what one value produces at that site)
        vocabulary = (
            ("a claim's version", 1, version_claim),
            ('precision_bits, numpy planner', 20,
             lambda value: wire_type(numpy_frame(value))),
            ('precision_bits, arrow planner', 20,
             lambda value: wire_type(arrow_frame(value))),
            ('precision_bits, all-null column', 20,
             lambda value: wire_type(all_null_frame(value))),
            ('schema_overrides geohash bits', 20,
             lambda value: wire_type(
                 arrow_frame(None),
                 schema_overrides={'gh': ('geohash', value)})),
            ('Long256 value', 1, lambda value: qi.Long256(value).value),
            ('DateMillis millis', 1, lambda value: qi.DateMillis(value).value),
            ('Geohash bits', 1, lambda value: qi.Geohash(value, 5).bits),
            ('Geohash precision', 1,
             lambda value: qi.Geohash(0, value).precision),
        )

        def answer(apply, value):
            """What one site does with one value: its result, or the
            kind of refusal it raises."""
            try:
                return ('returned', apply(value))
            except (TypeError, ValueError, qi.QuestDBError) as exc:
                return ('refused', type(exc).__name__)

        # A row per call site, matched by count. The table's whole
        # point is that it is complete, and a table nobody holds to the
        # sources is the remembered list this predicate replaced.
        sites = []
        for file_name in ('_client.pyx', 'dataframe.pxi'):
            source = PROJ_ROOT / 'src' / 'questdb' / file_name
            for number, line in enumerate(source.read_text().splitlines(), 1):
                calls = line.count('_is_integral_not_bool(')
                if not calls or line.lstrip().startswith('cdef bint'):
                    continue  # no call here, or the definition
                sites.extend(
                    [f'{file_name}:{number}: {line.strip()}'] * calls)
        self.assertEqual(
            len(sites), len(vocabulary),
            f'the claim vocabulary reads a number at {len(sites)} places '
            f'and this table has {len(vocabulary)} rows. Every place has '
            f'to be a row, or nobody has checked it:\n  '
            + '\n  '.join(sites))

        for name, good, apply in vocabulary:
            with self.subTest(site=name):
                plain = answer(apply, good)
                self.assertEqual(
                    plain[0], 'returned',
                    f'{name}: a plain int should be accepted, got {plain}')
                for widened in (np.int64(good), np.int32(good),
                                np.uint8(good)):
                    self.assertEqual(
                        answer(apply, widened), plain,
                        f'{name}: {widened!r} should read as {good!r}')
                self.assertNotEqual(
                    answer(apply, True), answer(apply, 1),
                    f'{name}: True was taken as the number 1')

    @unittest.skipIf(pd is None, 'pandas not installed')
    @unittest.skipIf(pyarrow is None, 'pyarrow not installed')
    def test_the_claim_version_takes_any_whole_number_but_not_a_bool(self):
        """`True == 1`, so the version gate has to reject it by type,
        and a claim rebuilt from array metadata carries a `numpy.int64`
        the gate must still accept. Holding it to `int` alone dropped
        the whole claim over the type of one field."""
        cases = ((1, True), (np.int64(1), True), (np.int32(1), True),
                 (True, False), ('1', False), (2, False), (None, False))
        for version, applies in cases:
            with self.subTest(version=repr(version)):
                df = pd.DataFrame({
                    'c': pd.array([1], dtype=pd.ArrowDtype(pyarrow.uint32())),
                    'ts': pd.to_datetime([0], unit='s')})
                df.attrs['questdb'] = {
                    'version': version,
                    'columns': {'c': {'kind': 'ipv4'}}}
                types = self._dataframe_column_types(
                    df, table_name='claim_version', at='ts')
                # 0x18 is IPV4, which only the claim can produce here;
                # without it the uint32 column goes out as a LONG.
                self.assertEqual(
                    types['c'] == 0x18, applies,
                    f'version={version!r} should '
                    f'{"apply" if applies else "not apply"}')

    @unittest.skipIf(pd is None, 'pandas not installed')
    def test_a_claim_the_column_does_carry_stays_quiet(self):
        """An object column of `uuid.UUID` or `ipaddress.IPv4Address`
        is written as UUID or IPV4 by its source alone, so no override
        is set for it and none is missing. That is the shape plain
        `to_pandas()` hands back, so a log notice there would put a false
        'claim ignored' on every round trip -- the one path the claim
        exists to serve."""
        cases = (
            ('uuid', uuid.UUID('12345678-1234-5678-1234-567812345678'),
             0x0C),
            ('ipv4', ipaddress.IPv4Address('1.2.3.4'),
             0x18),
        )
        for kind, value, wire_type in cases:
            for claimed in (True, False):
                with self.subTest(kind=kind, claimed=claimed):
                    df = pd.DataFrame({
                        'c': pd.array([value], dtype=object),
                        'ts': pd.to_datetime([0], unit='s')})
                    if claimed:
                        df.attrs['questdb'] = {
                            'version': 1,
                            'columns': {'c': {'kind': kind}}}
                    with self.assertNoLogs('questdb', level='WARNING'):
                        types = self._dataframe_column_types(
                            df, table_name='claim_carried', at='ts')
                    # And the claim changes nothing: the column goes out
                    # as its own type either way.
                    self.assertEqual(types['c'], wire_type)

    @unittest.skipIf(pd is None, 'pandas not installed')
    @unittest.skipIf(pyarrow is None, 'pyarrow not installed')
    def test_a_claim_no_column_can_carry_is_said_out_loud_on_both_planners(self):
        """A claim the column's type can never carry is a mistake
        rather than drift, so both planners say so, for all five kinds
        and on either backing. Exactly one log notice either way -- an
        Arrow-backed column is answered by
        `_dataframe_normalize_claimed_arrow`, and saying it twice for
        one claim would be worse than either."""
        kinds = ('ipv4', 'char', 'uuid', 'long256', 'geohash')
        backings = (
            ('numpy', lambda: np.array([1.5], dtype=np.float64)),
            ('arrow', lambda: pd.array(
                [1.5], dtype=pd.ArrowDtype(pyarrow.float64()))),
        )
        for kind in kinds:
            for label, make in backings:
                with self.subTest(kind=kind, backing=label):
                    meta = {'kind': kind}
                    if kind == 'geohash':
                        meta['precision_bits'] = 20
                    df = pd.DataFrame({
                        'g': make(),
                        'ts': pd.to_datetime([0], unit='s')})
                    df.attrs['questdb'] = {
                        'version': 1, 'columns': {'g': meta}}
                    with warnings.catch_warnings():
                        warnings.simplefilter('error')
                        with self.assertLogs(
                                'questdb', level='WARNING') as caught:
                            self._dataframe_column_types(
                                df, table_name='claim_impossible', at='ts')
                    dropped = [record.getMessage()
                               for record in caught.records
                               if 'questdb: column' in record.getMessage()]
                    self.assertEqual(
                        len(dropped), 1,
                        f'expected exactly one dropped-claim log notice, '
                        f'got {dropped}')
                    self.assertIn(kind, dropped[0])

    @unittest.skipIf(pd is None, 'pandas not installed')
    def test_geohash_claim_too_wide_for_a_numpy_column_is_said_out_loud(self):
        """A geohash claim wider than the NumPy column carrying it is
        one the column can never hold, not drift, so it is said out
        loud -- the Arrow planner says the same thing through
        `_attrs_override_fits`. Silently it went out as a plain LONG
        and created the destination column with that type."""
        # Each signed width carries a byte-aligned precision using its
        # raw sign bit; one bit past the storage width is rejected.
        cases = ((np.int8, 8, False), (np.int8, 9, True),
                 (np.int16, 16, False), (np.int16, 17, True),
                 (np.int32, 32, False), (np.int32, 33, True),
                 (np.int64, 60, False), (np.int64, 61, True))
        for dtype, bits, expect_notice in cases:
            with self.subTest(dtype=dtype.__name__, bits=bits):
                df = pd.DataFrame({
                    'g': np.array([1], dtype=dtype),
                    'ts': pd.to_datetime([0], unit='s')})
                df.attrs['questdb'] = {
                    'version': 1,
                    'columns': {
                        'g': {'kind': 'geohash', 'precision_bits': bits}}}
                log = logging.getLogger('questdb')
                with mock.patch.object(log, 'handle') as handle:
                    types = self._dataframe_column_types(
                        df, table_name='geo_numpy_wide', at='ts')
                dropped = [call.args[0].getMessage()
                           for call in handle.call_args_list
                           if 'geohash' in call.args[0].getMessage()]
                if expect_notice:
                    self.assertEqual(
                        len(dropped), 1,
                        f'expected a dropped-claim log notice, got {dropped}')
                    self.assertIn('g', dropped[0])
                else:
                    self.assertEqual(
                        len(dropped), 0,
                        f'claim fits, should be quiet: '
                        f'{dropped}')
                    self.assertEqual(types['g'], 0x0E)

    @unittest.skipIf(pyarrow is None, 'pyarrow not installed')
    def test_big_metadata_on_a_non_geohash_column_does_not_stop_the_send(self):
        """Large metadata below the 1 MiB cap remains valid input."""
        padding = {f'pad.{i}'.encode(): b'x' * 400 for i in range(2000)}
        stamp = datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc)
        schema = pyarrow.schema([
            pyarrow.field('s', pyarrow.string(), metadata=padding),
            pyarrow.field('ts', pyarrow.timestamp('us', 'UTC'))])
        table = pyarrow.table(
            [pyarrow.array(['a']),
             pyarrow.array([stamp], pyarrow.timestamp('us', 'UTC'))],
            schema=schema)
        # Reaches the wire rather than raising over a GEOHASH it has no
        # way of holding.
        self._dataframe_wire_payload(
            table, table_name='big_md_string', at='ts')

    @unittest.skipIf(pyarrow is None, 'pyarrow not installed')
    def test_a_full_width_geohash_claim_reaches_its_top_values(self):
        """A precision that fills the column's width uses every bit,
        including the one that reads as a sign, so its top half sits in
        the column as negative numbers. Reading such a column as signed
        refused exactly those values while quoting a range the column
        cannot express."""
        widths = ((pyarrow.int8(), 8), (pyarrow.int16(), 16),
                  (pyarrow.int32(), 32))
        for ty, bits in widths:
            with self.subTest(width=str(ty), bits=bits):
                # The widest value at this precision: every bit set,
                # which the column spells as -1.
                types = self._dataframe_column_types(
                    self._geohash_arrow_table([-1], ty=ty),
                    table_name='geo_full_width', at='ts',
                    schema_overrides={'gh': ('geohash', bits)})
                self.assertEqual(types['gh'], 0x0E)

        # One bit past the width is a claim the column cannot hold at
        # all, and is refused by name.
        for ty, bits in ((pyarrow.int8(), 9), (pyarrow.int16(), 17),
                         (pyarrow.int32(), 33)):
            with self.subTest(width=str(ty), bits=bits):
                with self.assertRaisesRegex(
                        qi.QuestDBError,
                        rf'geohash precision_bits {bits} out of range for '
                        rf'Int{ty.bit_width} column'):
                    self._dataframe_wire_payload(
                        self._geohash_arrow_table([0], ty=ty),
                        table_name='geo_too_wide', at='ts',
                        schema_overrides={'gh': ('geohash', bits)})

        # A narrower claim forwards the same raw signed value without a
        # per-value range check.
        self._dataframe_wire_payload(
            self._geohash_arrow_table([-1], ty=pyarrow.int8()),
            table_name='geo_narrow', at='ts',
            schema_overrides={'gh': ('geohash', 7)})

    @unittest.skipIf(pyarrow is None, 'pyarrow not installed')
    def test_schema_overrides_geohash_bits_refuses_a_bool(self):
        """`True` is an `int`, and would have been taken as a 1-bit
        precision. Every other bits check in this module rejects it."""
        with self.assertRaisesRegex(
                ValueError,
                r'geohash bits must be a whole number in 1\.\.=60'):
            self._dataframe_wire_payload(
                self._geohash_arrow_table([0], ty=pyarrow.int32()),
                table_name='geo_bool', at='ts',
                schema_overrides={'gh': ('geohash', True)})

    @unittest.skipIf(pyarrow is None, 'pyarrow not installed')
    def test_large_geohash_metadata_uses_the_native_importer(self):
        # Python no longer duplicates the importer's metadata parser or
        # walks the values. Large metadata below the 1 MiB safety cap is
        # handled natively and wide values are forwarded under the same
        # unchecked contract.
        padding = {f'pad.{i}'.encode(): b'x' for i in range(5000)}
        md = dict(padding)
        md[b'questdb.column_type'] = b'geohash'
        md[b'questdb.geohash_bits'] = b'20'
        self.assertEqual(
            self._dataframe_column_types(
                self._geohash_arrow_table([2 ** 21], md=md),
                table_name='geo_md_long', at='ts')['gh'],
            0x0E)
        # A large allowed blob that claims nothing remains an ordinary
        # integer column rather than a hidden geohash.
        self.assertEqual(
            self._dataframe_column_types(
                self._geohash_arrow_table([2 ** 21], md=dict(padding)),
                table_name='geo_md_long', at='ts')['gh'],
            0x05)

        # The native bounded parser rejects an individual blob above 1 MiB,
        # independently of whether it carries a QuestDB type claim.
        too_large = {b'pad.big': b'x' * (2 << 20)}
        for claim in (False, True):
            with self.subTest(above_blob_cap=True, claim=claim):
                oversized = dict(too_large)
                if claim:
                    oversized[b'questdb.column_type'] = b'geohash'
                    oversized[b'questdb.geohash_bits'] = b'20'
                with self.assertRaisesRegex(
                        qi.QuestDBError,
                        r'Arrow schema root\.children\[0\]: metadata blob '
                        r'exceeds 1048576 bytes'):
                    self._dataframe_wire_payload(
                        self._geohash_arrow_table([2 ** 21], md=oversized),
                        table_name='geo_md_too_large', at='ts')
        # `schema_overrides` still outranks field metadata.
        md = dict(padding)
        md[b'questdb.column_type'] = b'geohash'
        md[b'questdb.geohash_bits'] = b'20'
        self._dataframe_wire_payload(
            self._geohash_arrow_table([2 ** 21], md=md),
            table_name='geo_md_long', at='ts',
            schema_overrides={'gh': ('geohash', 20)})

    @unittest.skipIf(pyarrow is None, 'pyarrow not installed')
    def test_unchecked_geohash_values_cross_batch_boundaries(self):
        values = [7] * 2500 + [1000] + [7] * 10
        self._dataframe_wire_payload(
            self._geohash_arrow_table(values),
            table_name='geo_batched', at='ts',
            max_rows_per_batch=1000,
            schema_overrides={'gh': ('geohash', 5)})

    @unittest.skipIf(pyarrow is None, 'pyarrow not installed')
    def test_unchecked_geohash_values_cover_a_one_shot_stream(self):
        def reader(values):
            table = self._geohash_arrow_table(values)
            return pyarrow.RecordBatchReader.from_batches(
                table.schema, table.to_batches(max_chunksize=500))

        self._dataframe_wire_payload(
            reader([7] * 1200 + [1000, -1] + [7] * 50),
            table_name='geo_stream', at='ts',
            schema_overrides={'gh': ('geohash', 5)})

    @unittest.skipIf(pyarrow is None, 'pyarrow not installed')
    def test_unchecked_geohash_values_include_nulls_and_edges(self):
        for values in ([None, 7], [0], [31], [None, None],
                       [0, 31, 99, -1, None]):
            with self.subTest(values=values):
                self._dataframe_wire_payload(
                    self._geohash_arrow_table(values),
                    table_name='geo_edges', at='ts',
                    schema_overrides={'gh': ('geohash', 5)})

    @unittest.skipIf(pyarrow is None, 'pyarrow not installed')
    def test_unchecked_geohash_values_span_the_signed_widths(self):
        widths = (
            (pyarrow.int8(), 5, 31),
            (pyarrow.int16(), 12, (1 << 12) - 1),
            (pyarrow.int32(), 20, (1 << 20) - 1),
            (pyarrow.int64(), 60, (1 << 60) - 1))
        for ty, bits, top in widths:
            with self.subTest(width=str(ty)):
                self._dataframe_wire_payload(
                    self._geohash_arrow_table([0, top, -1], ty=ty),
                    table_name='geo_widths', at='ts',
                    schema_overrides={'gh': ('geohash', bits)})

    @unittest.skipIf(pd is None, 'pandas not installed')
    @unittest.skipIf(pyarrow is None, 'pyarrow not installed')
    def test_roundtrip_claim_keeps_an_all_null_column(self):
        # A column of nothing but nulls names no type of its own, so
        # the planner skips it and the destination table is created
        # without it. The claim names one, which is the whole point of
        # feeding a query result back in -- so the column goes out as
        # the claimed type, in the same bytes the zero-copy Arrow path
        # sends for the same frame.
        stamps = [
            datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc)] * 3
        shapes = (
            ('char', pd.ArrowDtype(pyarrow.uint16()),
             {'kind': 'char'}, 0x16),
            ('long256', pd.ArrowDtype(pyarrow.binary(32)),
             {'kind': 'long256'}, 0x0D),
            ('uuid', pd.ArrowDtype(pyarrow.binary(16)),
             {'kind': 'uuid'}, 0x0C),
            ('geohash', pd.ArrowDtype(pyarrow.int32()),
             {'kind': 'geohash', 'precision_bits': 20}, 0x0E),
            ('ipv4', pd.ArrowDtype(pyarrow.uint32()),
             {'kind': 'ipv4'}, 0x18),
        )
        for label, dtype, meta, type_tag in shapes:
            with self.subTest(claim=label):
                def frame(ts_col):
                    df = pd.DataFrame({
                        'c': pd.array([None] * len(stamps), dtype=dtype),
                        'ts': ts_col,
                    })
                    df.attrs['questdb'] = {
                        'version': 1, 'columns': {'c': meta}}
                    return df

                arrow_frame = frame(pd.array(
                    stamps,
                    dtype=pd.ArrowDtype(pyarrow.timestamp('us', 'UTC'))))
                # A NumPy timestamp column routes the frame off the
                # zero-copy path and onto the manual planner. The unit is
                # pinned because `to_datetime` picks its own -- ns on
                # pandas 2, us on pandas 3 -- and the two would otherwise
                # reach the wire as different timestamp types.
                mixed_frame = frame(pd.to_datetime(stamps).as_unit('us'))
                self.assertEqual(
                    self._dataframe_column_types(
                        mixed_frame, table_name='attrs_round_trip',
                        at='ts')['c'],
                    type_tag)
                self.assertEqual(
                    self._dataframe_wire_payload(
                        mixed_frame, table_name='attrs_round_trip', at='ts'),
                    self._dataframe_wire_payload(
                        arrow_frame, table_name='attrs_round_trip', at='ts'))

    @unittest.skipIf(pd is None, 'pandas not installed')
    @unittest.skipIf(pyarrow is None, 'pyarrow not installed')
    def test_a_reshaped_claimed_column_matches_the_arrow_path(self):
        # A claimed Arrow column the manual planner cannot read as it
        # stands is copied to the NumPy width that carries the claim,
        # which stays a raw buffer; a column holding a null is copied to
        # object dtype instead, since a NumPy integer dtype has no null.
        # Both routes owe the same bytes the zero-copy Arrow path sends.
        stamps = [
            datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc)] * 4
        shapes = (
            ('char', pyarrow.uint16(), {'kind': 'char'}, [65, 66, 67, 68]),
            ('geohash int8', pyarrow.int8(),
             {'kind': 'geohash', 'precision_bits': 5}, [1, 2, 3, 4]),
            ('geohash int16', pyarrow.int16(),
             {'kind': 'geohash', 'precision_bits': 12}, [1, 2, 3, 4]),
            ('geohash int32', pyarrow.int32(),
             {'kind': 'geohash', 'precision_bits': 20}, [1, 2, 3, 4]),
        )
        for label, arrow_type, meta, values in shapes:
            for holds_null in (False, True):
                with self.subTest(shape=label, holds_null=holds_null):
                    cells = list(values)
                    if holds_null:
                        cells[1] = None

                    def frame(ts_col):
                        df = pd.DataFrame({
                            'c': pd.array(
                                cells, dtype=pd.ArrowDtype(arrow_type)),
                            'ts': ts_col,
                        })
                        df.attrs['questdb'] = {
                            'version': 1, 'columns': {'c': meta}}
                        return df

                    self.assertEqual(
                        self._dataframe_wire_payload(
                            # A NumPy timestamp column routes the frame
                            # onto the manual planner. The unit is pinned
                            # because `to_datetime` picks its own -- ns on
                            # pandas 2, us on pandas 3 -- and the `at`
                            # column's precision would otherwise reach the
                            # wire as TIMESTAMP_NS on one and TIMESTAMP on
                            # the other, failing a comparison that is about
                            # the claimed column.
                            frame(pd.to_datetime(stamps).as_unit('us')),
                            table_name='attrs_round_trip', at='ts'),
                        self._dataframe_wire_payload(
                            frame(pd.array(stamps, dtype=pd.ArrowDtype(
                                pyarrow.timestamp('us', 'UTC')))),
                            table_name='attrs_round_trip', at='ts'))

    @unittest.skipIf(pd is None, 'pandas not installed')
    @unittest.skipIf(pyarrow is None, 'pyarrow not installed')
    def test_an_all_null_column_keeps_only_a_claim_it_can_carry(self):
        # The object and masked shapes reach the planner the same way,
        # and a claim it cannot use leaves the column where it found
        # it: skipped, the same silence an unusable claim meets
        # everywhere else.
        stamps = [
            datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc)] * 2

        def frame(nulls, meta):
            df = pd.DataFrame({
                'c': nulls,
                'v': np.arange(2, dtype=np.int64),
                'ts': pd.to_datetime(stamps),
            })
            if meta is not None:
                df.attrs['questdb'] = {'version': 1, 'columns': {'c': meta}}
            return df

        shapes = (
            ('object', lambda: pd.Series([None] * 2, dtype=object)),
            ('masked', lambda: pd.array([None] * 2, dtype='UInt16')),
        )
        for label, nulls in shapes:
            with self.subTest(shape=label):
                self.assertEqual(
                    self._dataframe_column_types(
                        frame(nulls(), {'kind': 'char'}),
                        table_name='attrs_round_trip', at='ts')['c'],
                    0x16)
                # 0 bits describes no GEOHASH column.
                self.assertNotIn(
                    'c',
                    self._dataframe_column_types(
                        frame(nulls(),
                              {'kind': 'geohash', 'precision_bits': 0}),
                        table_name='attrs_round_trip', at='ts'))
                self.assertNotIn(
                    'c',
                    self._dataframe_column_types(
                        frame(nulls(), None),
                        table_name='attrs_round_trip', at='ts'))

    @unittest.skipIf(pd is None, 'pandas not installed')
    @unittest.skipIf(pyarrow is None, 'pyarrow not installed')
    def test_roundtrip_claim_on_a_mixed_frame_drops_a_retyped_column(self):
        # The claim recalls what the frame held when it was read, so a
        # column since retyped past what the claim can describe loses it
        # and lands as its own dtype implies -- the same silence the
        # Arrow path answers a retyped column with, not an error.
        frame = self._roundtrip_frame()
        retyped = frame.copy()
        retyped['ip'] = pd.array(
            [1.5], dtype=pd.ArrowDtype(pyarrow.float64()))
        retyped['x'] = np.arange(len(retyped), dtype=np.int64)
        retyped.attrs = dict(frame.attrs)
        types = self._dataframe_column_types(
            retyped, table_name='attrs_round_trip', at='ts')
        self.assertEqual(types['ip'], 0x07)
        # The columns beside it keep theirs.
        self.assertEqual(types['u'], 0x0C)
        self.assertEqual(types['ch'], 0x16)

    def _nullable_roundtrip_frame(self):
        """The rows of :meth:`_roundtrip_frame` in the shape
        ``to_pandas(dtype_backend='numpy_nullable')`` returns them:
        masked extension columns for the integer-backed kinds and
        object columns of ``bytes`` for the two fixed-size binary
        ones."""
        frame = pd.DataFrame({
            'u': pd.array([self.UUID_VALUE.bytes], dtype=object),
            'l': pd.array([bytes(range(32))], dtype=object),
            'ip': pd.array([0x01020304], dtype=pd.UInt32Dtype()),
            'ch': pd.array([ord('Q')], dtype=pd.UInt16Dtype()),
            'gh': pd.array([100], dtype=pd.Int32Dtype()),
            'ts': pd.array(
                [datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc)],
                dtype='datetime64[us, UTC]'),
        })
        frame.attrs['questdb'] = qi._RoundtripClaim({
            'version': 1, 'columns': {
                'u': {'kind': 'uuid'},
                'l': {'kind': 'long256'},
                'ip': {'kind': 'ipv4'},
                'ch': {'kind': 'char'},
                'gh': {'kind': 'geohash', 'precision_bits': 20},
                'ts': {'kind': 'timestamp'},
            }})
        return frame

    @unittest.skipIf(pd is None, 'pandas not installed')
    @unittest.skipIf(pyarrow is None, 'pyarrow not installed')
    def test_long256_round_trips_through_an_object_int_column(self):
        # Plain `to_pandas()` has no pyarrow to hold 32 raw bytes, so it
        # hands a LONG256 column back as Python ints. Going the other
        # way the claim is the only thing separating those ints from the
        # LONG column they would otherwise become.
        value = 2 ** 255 + 7
        frame = pd.DataFrame({
            'l': pd.array([value, 5, None], dtype=object),
            'ts': pd.to_datetime(
                ['2025-01-01', '2025-01-02', '2025-01-03']),
        })
        frame.attrs['questdb'] = {'version': 1, 'columns': {
            'l': {'kind': 'long256'}}}
        payload = self._dataframe_wire_payload(
            frame, table_name='long256_attrs', at='ts')
        self.assertEqual(
            dict(_first_qwp_table_column_types(payload))['l'], 0x0D)
        # 32 unsigned little-endian bytes a row — the order the reader
        # took the integer out of.
        self.assertIn(value.to_bytes(32, 'little'), payload)
        self.assertIn((5).to_bytes(32, 'little'), payload)

        # Without the claim the same column is a plain LONG, and a value
        # too wide for one names the claim that would carry it.
        bare = frame.iloc[1:].copy()
        bare.attrs = {}
        self.assertEqual(
            self._dataframe_column_types(
                bare, table_name='long256_attrs', at='ts')['l'],
            0x05)
        wide = frame.copy()
        wide.attrs = {}
        with self.assertRaisesRegex(
                qi.QuestDBError, "'kind': 'long256'") as caught:
            self._dataframe_column_types(
                wide, table_name='long256_attrs', at='ts')
        self.assertIn('out of range for a LONG column', str(caught.exception))

        # The claim says LONG256, so a value that is not one is refused
        # rather than truncated.
        for bad in (-1, 2 ** 256):
            with self.subTest(value=bad):
                odd = frame.copy()
                odd['l'] = pd.array([bad, 5, None], dtype=object)
                odd.attrs = dict(frame.attrs)
                with self.assertRaisesRegex(
                        qi.QuestDBError, r'0 <= value < 2\*\*256'):
                    self._dataframe_column_types(
                        odd, table_name='long256_attrs', at='ts')

    def test_row_names_long256_for_an_oversized_int(self):
        # A bare int is a 64-bit LONG. CPython's own "int too large to
        # convert" says nothing about what carries a wider value.
        buffer = qi.Buffer._new_qwp()
        buffer.row('long256_row', columns={'v': 1}, at=qi.TimestampNanos(1))
        before = len(buffer)
        for value in (2 ** 255 + 7, 2 ** 63, -(2 ** 63) - 1):
            with self.subTest(value=value):
                with self.assertRaisesRegex(
                        OverflowError, 'Long256') as caught:
                    buffer.row(
                        'long256_row', columns={'v': value},
                        at=qi.TimestampNanos(2))
                self.assertIn("Bad column 'v'", str(caught.exception))
                self.assertEqual(len(buffer), before)
        for value in (2 ** 63 - 1, -(2 ** 63)):
            buffer.row(
                'long256_row', columns={'v': value},
                at=qi.TimestampNanos(3))
        self.assertGreater(len(buffer), before)

    def test_ilp_row_keeps_its_oversized_int_error(self):
        # LONG256 is a QWP type, so an ILP buffer has no remedy to name
        # and keeps the error it always raised. Naming the wrapper there
        # would hand back advice the very next call turns down.
        ilp = qi.Buffer(protocol_version=2)
        for value in (2 ** 255 + 7, 2 ** 63, -(2 ** 63) - 1):
            with self.subTest(value=value):
                with self.assertRaises(OverflowError) as caught:
                    ilp.row(
                        'long256_row', columns={'v': value},
                        at=qi.TimestampNanos(1))
                message = str(caught.exception)
                self.assertNotIn('Long256', message)
                self.assertNotIn('out of range for a LONG column', message)
        # The advice the QWP message gives, on the buffer that cannot act
        # on it.
        with self.assertRaisesRegex(
                qi.QuestDBError, 'require a QWP sender'):
            ilp.row(
                'long256_row', columns={'v': qi.Long256(2 ** 255 + 7)},
                at=qi.TimestampNanos(1))
        # In-range values still land.
        ilp.row(
            'long256_row', columns={'v': 2 ** 63 - 1},
            at=qi.TimestampNanos(1))
        self.assertGreater(len(ilp), 0)

    @unittest.skipIf(pd is None, 'pandas not installed')
    @unittest.skipIf(pyarrow is None, 'pyarrow not installed')
    def test_roundtrip_attrs_are_a_hint_not_a_command(self):
        # The claim recalls what the frame held when it was read, so it
        # is dropped wherever the frame has moved on. Rejecting it would
        # break frames that ingest fine today.
        frame = self._roundtrip_frame()
        retyped = frame.astype({'ip': pd.ArrowDtype(pyarrow.int64())})
        retyped.attrs = dict(frame.attrs)
        types = self._dataframe_column_types(
            retyped, table_name='attrs_stale', at='ts')
        self.assertEqual(types['ip'], 0x05)

        renamed = frame.rename(columns={'ch': 'ch2'})
        renamed.attrs = dict(frame.attrs)
        types = self._dataframe_column_types(
            renamed, table_name='attrs_stale', at='ts')
        self.assertEqual(types['ch2'], 0x04)

        # A GEOHASH precision the storage type cannot hold is dropped
        # rather than sent as an out-of-range claim.
        narrow = frame.astype({'gh': pd.ArrowDtype(pyarrow.int8())})
        narrow.attrs = dict(frame.attrs)
        types = self._dataframe_column_types(
            narrow, table_name='attrs_stale', at='ts')
        self.assertEqual(types['gh'], 0x04)

        # A stray argument on a kind that takes none is ignored; only
        # GEOHASH reads `precision_bits`.
        for junk in (10 ** 30, 'x', None, -1):
            with self.subTest(precision_bits=junk):
                odd = frame.copy()
                odd.attrs = {'questdb': {'version': 1, 'columns': {
                    'ip': {'kind': 'ipv4', 'precision_bits': junk}}}}
                types = self._dataframe_column_types(
                    odd, table_name='attrs_stale', at='ts')
                self.assertEqual(types['ip'], 0x18)

        # Metadata of the wrong shape is ignored, not diagnosed. The
        # version is part of the shape: it is checked rather than
        # merely carried, so a mapping written without one is not a
        # claim this client reads, and neither is a future one whose
        # vocabulary it would have to guess at.
        for junk in ('nonsense',
                     {'version': 1, 'columns': 'nope'},
                     {'version': 1, 'columns': {'ip': {'kind': 42}}},
                     {'version': 1, 'columns': {7: {'kind': 'ipv4'}}},
                     {'columns': {'ip': {'kind': 'ipv4'}}},
                     {'version': 2, 'columns': {'ip': {'kind': 'ipv4'}}}):
            with self.subTest(junk=junk):
                odd = frame.copy()
                odd.attrs = {'questdb': junk}
                types = self._dataframe_column_types(
                    odd, table_name='attrs_stale', at='ts')
                self.assertEqual(types['ip'], 0x05)

    def _assert_nothing_can_edit(self, target):
        """Every way `dict` offers to change a mapping, refused.

        The names come from `dict` itself rather than from a list this
        test keeps in step by hand: a ninth mutator in some future
        Python shows up here as a failure instead of as a claim that
        changes under every frame sharing it.
        """
        before = json.dumps(target, sort_keys=True)

        # The statement forms, each of which reaches a different slot.
        for label, edit in (
                ("claim['x'] = 1", lambda t: t.__setitem__('x', 1)),
                ("del claim['kind']", lambda t: t.__delitem__('kind')),
                ("claim |= {...}", lambda t: t.__ior__({'x': 1})),
                ('claim.update({...})', lambda t: t.update({'x': 1}))):
            with self.subTest(edit=label):
                with self.assertRaisesRegex(
                        TypeError, 'cannot be edited in place'):
                    edit(target)

        # And the whole of `dict`'s surface, called with the argument
        # shapes its methods take. Anything that gets through leaves the
        # contents changed, whether or not it raised on the way.
        for name in sorted(dir(dict)):
            attr = getattr(target, name, None)
            if not callable(attr):
                continue
            # Every shape, not just up to the first that returns: a
            # call that succeeds harmlessly on one shape says nothing
            # about the others. `__init__()` with no arguments is the
            # case that hid this -- it succeeds, while
            # `__init__({...})` rewrites the claim in place.
            for args in ((), ('kind',), ('x', 1), ({'x': 1},)):
                try:
                    attr(*args)
                except Exception:
                    pass
                self.assertEqual(
                    json.dumps(target, sort_keys=True), before,
                    f'{name}{args!r} changed the claim')

        # An empty claim is a claim: a result with no columns builds
        # one, and it has to refuse re-initialisation too. Checking
        # contents alone cannot see this -- there are none to change.
        empty = type(target)({})
        with self.assertRaisesRegex(TypeError, 'cannot be edited in place'):
            empty.__init__({'version': 999})
        self.assertEqual(dict(empty), {})
        self.assertEqual(json.dumps(target, sort_keys=True), before)

        # An ordinary editable copy of this one level is one unpacking
        # away. Unpacking is shallow, so anything nested under it is
        # still the frozen mapping -- which is why the documented
        # recipe unpacks `columns` too. See
        # `test_the_documented_way_to_edit_a_claim_works`.
        loose = {**target}
        loose['x'] = 1
        self.assertNotIn('x', target)

    @unittest.skipIf(pd is None, 'pandas not installed')
    @unittest.skipIf(pyarrow is None, 'pyarrow not installed')
    def test_the_documented_way_to_edit_a_claim_works(self):
        """The recipe the docstrings and the `TypeError` hand out has
        to run. Unpacking is shallow, so unpacking the claim alone
        leaves `columns` -- the only part anyone edits -- still frozen,
        and the advice would raise the same error one level down."""
        frame = self._roundtrip_frame()
        claim = frame.attrs['questdb']
        self.assertIn('ip', claim['columns'])

        # Unpacking the outer mapping alone is not enough.
        with self.assertRaisesRegex(TypeError, 'cannot be edited in place'):
            {**claim}['columns']['grade'] = {'kind': 'char'}

        # What the docstrings and the error message actually say.
        frame.attrs['questdb'] = {
            'version': 1,
            'columns': {
                **claim['columns'],
                'grade': {'kind': 'char'}}}
        edited = frame.attrs['questdb']
        edited['columns']['grade'] = {'kind': 'char'}
        self.assertEqual(edited['columns']['grade'], {'kind': 'char'})
        self.assertIn('ip', edited['columns'])
        # The claim it was built from is untouched.
        self.assertNotIn('grade', claim['columns'])

    @unittest.skipIf(pd is None, 'pandas not installed')
    @unittest.skipIf(pyarrow is None, 'pyarrow not installed')
    def test_the_round_trip_claim_is_frozen_and_never_copied(self):
        """pandas deep-copies the whole of `df.attrs` every time it
        propagates it, and that runs once per column while a frame is
        sliced or exported. A per-column claim would therefore make
        that pass quadratic in the column count: `pyarrow.table(df)` on
        a 1024-column frame spends 710 ms of its 730 ms inside
        `copy.deepcopy`, against 26 ms once the claim declines to copy.
        Declining is only sound because the claim cannot be edited."""
        frame = self._roundtrip_frame()
        claim = frame.attrs['questdb']

        # The two ways pandas would otherwise pay for it.
        self.assertIs(copy.deepcopy(frame.attrs)['questdb'], claim)
        self.assertIs(frame.head(1).attrs['questdb'], claim)

        # Sharing is unobservable because nothing can be edited, at any
        # depth.
        for target in (claim, claim['columns'], claim['columns']['ip']):
            with self.subTest(level=repr(sorted(target))):
                self._assert_nothing_can_edit(target)

        # To everything that reads it, it is the mapping it replaced.
        self.assertIsInstance(claim, dict)
        self.assertEqual(claim['columns']['ip'], {'kind': 'ipv4'})
        self.assertEqual(claim['version'], 1)
        self.assertEqual(json.loads(json.dumps(claim)), dict(claim))

        # Replacing it is how a frame's claim is changed, and a plain
        # hand-written dict is read just the same.
        edited = frame.copy()
        edited.attrs = dict(frame.attrs)
        edited.attrs['questdb'] = {
            'version': 1, 'columns': {'ip': {'kind': 'char'}}}
        self.assertEqual(
            self._dataframe_column_types(
                edited, table_name='attrs_frozen', at='ts')['ip'],
            0x05)
        self.assertEqual(frame.attrs['questdb'], claim)

    @unittest.skipIf(pd is None, 'pandas not installed')
    @unittest.skipIf(pyarrow is None, 'pyarrow not installed')
    def test_malformed_roundtrip_attrs_are_ignored_by_both_readers(self):
        """`df.attrs` is user data — hand-written, or reloaded from JSON
        where a string became a list — and the claim promises to be
        skipped when it no longer fits. There are two readers, one per
        planner, so every shape is run through both: hardening one and
        not the other is how this went wrong before.

        The version is part of it. Both producers stamp one, so a frame
        carrying a version this client does not know must lose its
        claim rather than have it applied under the old vocabulary.
        """
        cases = (
            ('a known version', {
                'version': 1, 'columns': {'ip': {'kind': 'ipv4'}}}, 0x18),
            ('a future version', {
                'version': 99, 'columns': {'ip': {'kind': 'ipv4'}}}, 0x05),
            ('no version at all', {
                'columns': {'ip': {'kind': 'ipv4'}}}, 0x05),
            ('an unhashable kind', {
                'version': 1, 'columns': {'ip': {'kind': ['ipv4']}}}, 0x05),
            ('a non-string kind', {
                'version': 1, 'columns': {'ip': {'kind': 42}}}, 0x05),
            ('a non-dict entry', {
                'version': 1, 'columns': {'ip': 'ipv4'}}, 0x05),
            ('a non-dict columns', {
                'version': 1, 'columns': 'nope'}, 0x05),
            ('attrs that are not a dict', 'nonsense', 0x05),
        )
        stamps = pd.to_datetime(['2025-01-01'])
        planners = {
            # Fully Arrow-backed: the capsule path.
            'capsule': lambda: pd.DataFrame({
                'ip': pd.array(
                    [0x01020304], dtype=pd.ArrowDtype(pyarrow.uint32())),
                'ts': pd.array(
                    stamps, dtype=pd.ArrowDtype(pyarrow.timestamp('us'))),
            }),
            # Plain numpy dtypes: the NumPy planner.
            'numpy': lambda: pd.DataFrame({
                'ip': pd.array([0x01020304], dtype='uint32'),
                'ts': stamps,
            }),
        }
        for planner, build in planners.items():
            for label, meta, expected in cases:
                with self.subTest(planner=planner, attrs=label):
                    frame = build()
                    frame.attrs['questdb'] = meta
                    self.assertEqual(
                        self._dataframe_column_types(
                            frame, table_name='attrs_junk', at='ts')['ip'],
                        expected)

    @unittest.skipIf(pd is None, 'pandas not installed')
    @unittest.skipIf(pyarrow is None, 'pyarrow not installed')
    def test_explicit_claims_outrank_roundtrip_attrs(self):
        # `schema_overrides` and `symbols` state a type outright and
        # `attrs` only recalls one, so the two are merged with the
        # explicit claim winning: a column named by both produces one
        # override, not the duplicate the native client rejects, and
        # `symbols=False` still turns the SYMBOL column into a VARCHAR.
        frame = self._roundtrip_frame()
        frame['sym'] = pd.array(
            ['a'] * len(frame),
            dtype=pd.ArrowDtype(pyarrow.dictionary(
                pyarrow.int32(), pyarrow.utf8())))
        claim = dict(frame.attrs['questdb'])
        claim['columns'] = dict(claim['columns'])
        claim['columns']['sym'] = {'kind': 'symbol'}
        frame.attrs['questdb'] = qi._RoundtripClaim(claim)
        types = self._dataframe_column_types(
            frame, table_name='attrs_precedence', at='ts',
            symbols=False, schema_overrides={'ip': 'ipv4'})
        self.assertEqual(types['ip'], 0x18)
        self.assertEqual(types['sym'], 0x0F)

    @unittest.skipIf(pd is None, 'pandas not installed')
    @unittest.skipIf(pyarrow is None, 'pyarrow not installed')
    def test_table_name_col_names_the_split_remedy(self):
        # QuestDB.dataframe() writes one table per call, so it turns
        # `table_name_col` down. It does that before it looks at any
        # column, so the 32-byte binary column here — which the NumPy
        # planner cannot type as LONG256 by itself — does not raise
        # first and point at the wrong problem. Splitting the frame and
        # making one call per table is the fix, and LONG256 goes
        # through that way.
        value = bytes(range(32))
        frame = pd.DataFrame({
            'tbl': pd.array(
                ['split_a', 'split_b'],
                dtype=pd.ArrowDtype(pyarrow.string())),
            'l': pd.array(
                [value, value],
                dtype=pd.ArrowDtype(pyarrow.binary(32))),
            'ts': pd.array(
                [datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc),
                 datetime.datetime(2025, 1, 2, tzinfo=datetime.timezone.utc)],
                dtype=pd.ArrowDtype(pyarrow.timestamp('us', 'UTC'))),
        })
        with QwpAckServer(record_payloads=True) as server:
            conf = (
                f'ws::addr=127.0.0.1:{server.port};lazy_connect=true;'
                'sender_pool_min=1;sender_pool_max=1;pool_reap=manual;')
            with qi.QuestDB.from_conf(conf) as client:
                with self.assertRaises(
                        qi.UnsupportedDataFrameShapeError) as caught:
                    client.dataframe(frame, table_name_col='tbl', at='ts')
                with self.assertRaises(
                        qi.UnsupportedDataFrameShapeError):
                    client.dataframe(
                        frame, table_name_col='tbl', at='ts',
                        schema_overrides={'l': 'long256'})
        message = str(caught.exception)
        self.assertIn('does not accept `table_name_col`', message)
        self.assertIn('table_name=name', message)
        # The problem is `table_name_col`, not the column's type.
        self.assertNotIn('32-byte', message)

        for name, group in frame.groupby('tbl'):
            types = self._dataframe_column_types(
                group.drop(columns='tbl'), table_name=name, at='ts',
                schema_overrides={'l': 'long256'})
            self.assertEqual(types['l'], 0x0D)

    def _roundtrip_frame(self):
        """A fully Arrow-backed frame shaped like what
        ``to_pandas(dtype_backend='pyarrow')`` returns for a table of
        UUID / LONG256 / IPV4 / CHAR / GEOHASH columns."""
        frame = pd.DataFrame({
            'u': pd.array(
                [self.UUID_VALUE.bytes],
                dtype=pd.ArrowDtype(pyarrow.binary(16))),
            'l': pd.array(
                [bytes(range(32))],
                dtype=pd.ArrowDtype(pyarrow.binary(32))),
            'ip': pd.array(
                [0x01020304], dtype=pd.ArrowDtype(pyarrow.uint32())),
            'ch': pd.array(
                [ord('Q')], dtype=pd.ArrowDtype(pyarrow.uint16())),
            'gh': pd.array(
                [100], dtype=pd.ArrowDtype(pyarrow.int32())),
            'ts': pd.array(
                [datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc)],
                dtype=pd.ArrowDtype(pyarrow.timestamp('us', 'UTC'))),
        })
        frame.attrs['questdb'] = qi._RoundtripClaim({
            'version': 1, 'columns': {
                'u': {'kind': 'uuid'},
                'l': {'kind': 'long256'},
                'ip': {'kind': 'ipv4'},
                'ch': {'kind': 'char'},
                'gh': {'kind': 'geohash', 'precision_bits': 20},
                'ts': {'kind': 'timestamp'},
            }})
        return frame


if os.environ.get('TEST_QUESTDB_INTEGRATION') == '1':
    class TestQwpOnlyRowTypesIntegration(TestWithDatabase):
        """The QWP-only row types against a live server.

        The whole class needs QuestDB 10, so `setUp` asks for it once
        rather than each test asking for itself.
        """

        def setUp(self):
            self._require_qwp_row_types()

        def test_round_trip_sentinels_precisions_and_mixed_precision_error(self):
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
            # Egress emits UUID storage as canonical RFC 4122 big-endian
            # bytes, matching what the `arrow.uuid` label promises, so the
            # storage bytes equal `uuid.UUID.bytes` regardless of whether
            # pyarrow surfaces the cell raw or wraps it in a `uuid.UUID`.
            raw_u = (first['u'] if isinstance(first['u'], bytes)
                     else first['u'].bytes)
            self.assertEqual(raw_u, normal_uuid.bytes)
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
            # CHAR has no physical/QWP NULL representation: '\x00' is stored
            # as code unit 0 (rendered '' in text output, 0 in Arrow egress).
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


class TestTypeStub(unittest.TestCase):
    """`_client.pyi` is the type surface every editor and type checker
    reads, and nothing else in the tree looks at it -- no mypy run, no
    stubtest, in `test/`, `proj.py` or `ci/`. It can therefore promise
    names the extension does not have, which is not a typing nicety: the
    two `row()` value unions were annotated from the stub, type-checked
    cleanly, and raised `ImportError` at runtime because they existed
    only there.

    Every name the stub declares is resolved against the built module
    here, so a name that lives only in the stub fails as a test rather
    than at a caller's import.
    """

    @staticmethod
    def _stub_tree():
        import ast
        path = PROJ_ROOT / 'src' / 'questdb' / '_client.pyi'
        return ast.parse(path.read_text(encoding='utf-8'))

    @staticmethod
    def _declared(node):
        """The names a stub class or module body declares."""
        import ast
        out = []
        for statement in node.body:
            if isinstance(statement, (ast.FunctionDef, ast.AsyncFunctionDef,
                                      ast.ClassDef)):
                out.append(statement.name)
            elif (isinstance(statement, ast.AnnAssign)
                    and isinstance(statement.target, ast.Name)):
                out.append(statement.target.id)
            elif isinstance(statement, ast.Assign):
                out.extend(
                    target.id for target in statement.targets
                    if isinstance(target, ast.Name))
        return out

    def _implementation_sources(self):
        source_dir = PROJ_ROOT / 'src' / 'questdb'
        return '\n'.join(
            path.read_text(encoding='utf-8')
            for path in sorted(source_dir.glob('*.pyx'))
            + sorted(source_dir.glob('*.pxi')))

    def test_every_stub_name_exists_at_runtime(self):
        import ast
        tree = self._stub_tree()
        sources = self._implementation_sources()
        missing = []
        for name in self._declared(tree):
            if not hasattr(qi, name):
                missing.append(name)
        for node in tree.body:
            if not isinstance(node, ast.ClassDef):
                continue
            owner = getattr(qi, node.name, None)
            if owner is None:
                continue
            for member in self._declared(node):
                if hasattr(owner, member):
                    continue
                # A dataclass field and an attribute assigned in
                # `__init__` are both invisible on the class itself.
                if member in getattr(owner, '__annotations__', {}):
                    continue
                if f'self.{member} =' in sources:
                    continue
                missing.append(f'{node.name}.{member}')
        self.assertEqual(
            missing, [],
            'declared in _client.pyi and absent from the extension')

    #: Members whose runtime signature cannot be read, with the reason.
    #: `__cinit__` has no `__text_signature__`, and a few slots are
    #: implemented by CPython rather than by this module.
    _SIGNATURE_UNREADABLE = {'__init__', '__cinit__', '__new__'}

    def test_every_stub_signature_matches_the_extension(self):
        """A name that exists is not the same promise as a signature
        that matches. A stub naming a parameter the extension does not
        take type-checks cleanly and raises `TypeError` at the call, so
        the parameter lists are compared here too.

        Only the shape is compared -- names, order, and whether a
        default exists. Annotations are the stub's own business; it
        exists to carry types the extension has no way to express.
        """
        import ast
        import inspect
        tree = self._stub_tree()
        mismatches = []

        def stub_params(fn):
            args = fn.args
            out = []
            positional = args.posonlyargs + args.args
            n_defaults = len(args.defaults)
            first_default = len(positional) - n_defaults
            for i, arg in enumerate(positional):
                out.append((arg.arg, i >= first_default))
            if args.vararg:
                out.append(('*' + args.vararg.arg, False))
            elif args.kwonlyargs:
                out.append(('*', False))
            for arg, default in zip(args.kwonlyargs, args.kw_defaults):
                out.append((arg.arg, default is not None))
            if args.kwarg:
                out.append(('**' + args.kwarg.arg, False))
            return out

        def runtime_params(obj):
            try:
                signature = inspect.signature(obj)
            except (TypeError, ValueError):
                return None
            out = []
            for name, param in signature.parameters.items():
                if param.kind is param.VAR_POSITIONAL:
                    out.append(('*' + name, False))
                elif param.kind is param.VAR_KEYWORD:
                    out.append(('**' + name, False))
                else:
                    if (param.kind is param.KEYWORD_ONLY
                            and not any(p[0].startswith('*')
                                        for p in out)):
                        out.append(('*', False))
                    out.append(
                        (name, param.default is not param.empty))
            return out

        for node in tree.body:
            if not isinstance(node, ast.ClassDef):
                continue
            owner = getattr(qi, node.name, None)
            if owner is None:
                continue
            for statement in node.body:
                if not isinstance(statement, ast.FunctionDef):
                    continue
                name = statement.name
                if name in self._SIGNATURE_UNREADABLE:
                    continue
                member = inspect.getattr_static(owner, name, None)
                if member is None:
                    continue
                decorators = {
                    d.id for d in statement.decorator_list
                    if isinstance(d, ast.Name)}
                if 'property' in decorators:
                    # A `@property` on a cdef class is a getset
                    # descriptor rather than a `property` object.
                    self.assertIsInstance(
                        member,
                        (property, types.GetSetDescriptorType,
                         types.MemberDescriptorType),
                        f'{node.name}.{name} is a property in the stub '
                        f'and a {type(member).__name__} at runtime')
                    continue
                target = member
                if isinstance(member, (classmethod, staticmethod)):
                    target = member.__func__
                expected = stub_params(statement)
                if isinstance(member, (classmethod, staticmethod)):
                    # The stub spells the bound first parameter; the
                    # runtime signature of the underlying function
                    # keeps it too.
                    pass
                actual = runtime_params(target)
                if actual is None:
                    continue
                if expected != actual:
                    mismatches.append(
                        f'{node.name}.{name}: stub {expected} != '
                        f'runtime {actual}')

        self.assertEqual(
            mismatches, [],
            'signatures in _client.pyi that the extension does not have')

    def test_stub_and_module_export_the_same_names(self):
        import ast
        for node in self._stub_tree().body:
            if (isinstance(node, ast.Assign)
                    and any(isinstance(target, ast.Name)
                            and target.id == '__all__'
                            for target in node.targets)):
                declared = sorted(ast.literal_eval(node.value))
                break
        else:
            self.fail('_client.pyi declares no __all__')
        self.assertEqual(declared, sorted(qi.__all__))


class TestEveryTestClassIsReachable(unittest.TestCase):
    """`proj.py test` and CI both run this file and nothing else, so a
    `TestCase` defined in a sibling module only runs if it is imported
    here by name. Three review rounds in a row found a class that was
    not: the one asserting the new buffer-protocol code releases what it
    borrows, and the one covering the `_decimal` guard. Both were
    written for exactly the regression they then could not catch.

    A class may opt out by starting its name with an underscore, which
    is how a shared base gets excluded.
    """

    #: Modules whose classes are expected to be reachable from here.
    SIBLINGS = (
        'test_client_capsule_path',
        'test_client_dataframe_failures',
        'test_client_dataframe_fuzz',
        'test_client_polars_fuzz',
        'test_dataframe',
        'test_dataframe_fuzz',
        'test_dataframe_leaks',
    )

    def test_every_sibling_test_class_is_imported_here(self):
        import importlib
        this_module = sys.modules[__name__]
        missing = []
        for module_name in self.SIBLINGS:
            try:
                module = importlib.import_module(module_name)
            except ImportError:
                # Optional dependency absent; the import guard above
                # already skipped the module for this run.
                continue
            for name in dir(module):
                if name.startswith('_'):
                    continue
                obj = getattr(module, name)
                if (isinstance(obj, type)
                        and issubclass(obj, unittest.TestCase)
                        and obj.__module__ == module_name
                        and getattr(this_module, name, None) is not obj):
                    missing.append(f'{module_name}.{name}')
        self.assertEqual(
            missing, [],
            'These TestCase classes are defined in a sibling module but '
            'never imported into test/test.py, so neither `proj.py test` '
            'nor CI runs them. Add them to the import list, or prefix the '
            'class name with an underscore if it is a base class.')


if __name__ == '__main__':
    if os.environ.get('TEST_QUESTDB_PROFILE') == '1':
        import cProfile

        cProfile.run('unittest.main()', sort='cumtime')
    else:
        unittest.main()
