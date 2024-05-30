#!/usr/bin/env python3

import sys
sys.dont_write_bytecode = True
import os
import shutil
import unittest
import uuid
import pathlib

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


import questdb.ingress as qi


QUESTDB_VERSION = '7.3.7'
QUESTDB_PLAIN_INSTALL_PATH = None
QUESTDB_AUTH_INSTALL_PATH = None


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

        cls.qdb_plain = QuestDbFixture(
            QUESTDB_PLAIN_INSTALL_PATH, auth=False, wrap_tls=True, http=True)
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
            pending = str(sender)

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
            pending = str(sender)

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


if __name__ == '__main__':
    unittest.main()