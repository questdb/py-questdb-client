#!/usr/bin/env python3

import sys
sys.dont_write_bytecode = True
import os
import importlib.util
import shutil
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


import questdb.ingress as qi


QUESTDB_VERSION = '9.2.0'
QUESTDB_PLAIN_INSTALL_PATH = None
QUESTDB_AUTH_INSTALL_PATH = None
FIRST_ARRAY_RELEASE = (8, 4, 0)
FIRST_DECIMAL_RELEASE = (9, 2, 0)

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

    def _mk_qwpudp_sender(self, **kwargs):
        self._require_qwp_udp()
        return qi.Sender(
            qi.Protocol.QwpUdp,
            self.qdb_plain.host,
            self.qdb_plain.qwp_udp_port,
            **kwargs)

    def _mk_qwpudp_conf(self, **kwargs):
        self._require_qwp_udp()
        conf = f'qwpudp::addr={self.qdb_plain.host}:{self.qdb_plain.qwp_udp_port};'
        for key, value in kwargs.items():
            conf += f'{key}={value};'
        return conf

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
                        qi.IngressError,
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

    def test_qwp_udp_protocol_enum(self):
        self.assertEqual(qi.Protocol.parse('qwpudp'), qi.Protocol.QwpUdp)
        self.assertFalse(qi.Protocol.QwpUdp.tls_enabled)

    def test_qwp_udp_basic(self):
        self._require_qwp_udp()
        table_name = uuid.uuid4().hex
        with self._mk_qwpudp_sender() as sender:
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
        conf = self._mk_qwpudp_conf(max_datagram_size=1200, multicast_ttl=2)
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
        conf = self._mk_qwpudp_conf()
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
        os.environ['QDB_CLIENT_CONF'] = self._mk_qwpudp_conf()
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
        conf = self._mk_qwpudp_conf(max_datagram_size=1200)
        with self.assertRaisesRegex(
                ValueError,
                r'"max_datagram_size" is already present in the conf_str'):
            qi.Sender.from_conf(conf, max_datagram_size=900)

    def test_qwp_udp_auto_flush_bytes_default(self):
        self._require_qwp_udp()
        sender = self._mk_qwpudp_sender()
        try:
            self.assertTrue(sender.auto_flush)
            self.assertEqual(sender.auto_flush_bytes, 1400)
        finally:
            sender.close(flush=False)

        sender = self._mk_qwpudp_sender(max_datagram_size=1200)
        try:
            self.assertEqual(sender.auto_flush_bytes, 1200)
        finally:
            sender.close(flush=False)

    def test_qwp_udp_new_buffer(self):
        self._require_qwp_udp()
        table_name = uuid.uuid4().hex
        with self._mk_qwpudp_sender(init_buf_size=1024, max_name_len=64) as sender:
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
        sender = self._mk_qwpudp_sender()
        try:
            with self.assertRaisesRegex(
                    qi.IngressError,
                    r"new_buffer\(\) can't be called before establish\(\)"):
                sender.new_buffer()
        finally:
            sender.close(flush=False)

    def test_qwp_udp_new_buffer_rejects_closed_sender(self):
        self._require_qwp_udp()
        sender = self._mk_qwpudp_sender()
        sender.close(flush=False)
        with self.assertRaisesRegex(
                qi.IngressError,
                r"new_buffer\(\) can't be called: Sender is closed"):
            sender.new_buffer()

    def test_qwp_udp_transaction_rejected(self):
        self._require_qwp_udp()
        with self._mk_qwpudp_sender() as sender:
            with self.assertRaisesRegex(
                    qi.IngressError,
                    'Transactions are only supported for ILP/HTTP'):
                sender.transaction('trades')

    def test_qwp_udp_protocol_version_rejected(self):
        self._require_qwp_udp()
        with self._mk_qwpudp_sender() as sender:
            with self.assertRaisesRegex(
                    qi.IngressError,
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
        with self._mk_qwpudp_sender() as sender:
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

if __name__ == '__main__':
    unittest.main()
