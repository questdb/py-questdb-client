#!/usr/bin/env python3

import sys
sys.dont_write_bytecode = True
import shutil
import unittest
import uuid

import patch_path
PROJ_ROOT = patch_path.PROJ_ROOT
sys.path.append(str(PROJ_ROOT / 'c-questdb-client' / 'system_test'))
from fixture import QuestDbFixture, install_questdb, CA_PATH, AUTH


import questdb.ilp as ilp


QUESTDB_VERSION = '6.4.2'
QUESTDB_PLAIN_INSTALL_PATH = None
QUESTDB_AUTH_INSTALL_PATH = None


def may_install_questdb():
    global QUESTDB_PLAIN_INSTALL_PATH
    global QUESTDB_AUTH_INSTALL_PATH
    if QUESTDB_PLAIN_INSTALL_PATH:
        return

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
            QUESTDB_PLAIN_INSTALL_PATH, auth=False, wrap_tls=True)
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

    def _test_scenario(self, qdb, auth, tls):
        port = qdb.tls_line_tcp_port if tls else qdb.line_tcp_port
        pending = None
        table_name = uuid.uuid4().hex
        with ilp.Sender('localhost', port, auth=auth, tls=tls) as sender:
            for _ in range(3):
                sender.row(
                    table_name,
                    symbols={
                        'name_a': 'val_a'},
                    columns={
                        'name_b': True,
                        'name_c': 42,
                        'name_d': 2.5,
                        'name_e': 'val_b'})
            pending = str(sender)

        resp = qdb.retry_check_table(table_name, min_rows=3, log_ctx=pending)
        exp_columns = [
            {'name': 'name_a', 'type': 'SYMBOL'},
            {'name': 'name_b', 'type': 'BOOLEAN'},
            {'name': 'name_c', 'type': 'LONG'},
            {'name': 'name_d', 'type': 'DOUBLE'},
            {'name': 'name_e', 'type': 'STRING'},
            {'name': 'timestamp', 'type': 'TIMESTAMP'}]
        self.assertEqual(resp['columns'], exp_columns)

        exp_dataset = [  # Comparison excludes timestamp column.
            ['val_a', True, 42, 2.5, 'val_b'],
            ['val_a', True, 42, 2.5, 'val_b'],
            ['val_a', True, 42, 2.5, 'val_b']]
        scrubbed_dataset = [row[:-1] for row in resp['dataset']]
        self.assertEqual(scrubbed_dataset, exp_dataset)

    def test_plain(self):
        self._test_scenario(self.qdb_plain, None, False)

    def test_plain_tls_insecure_skip_verify(self):
        self._test_scenario(self.qdb_plain, None, 'insecure_skip_verify')

    def test_plain_tls_ca(self):
        self._test_scenario(self.qdb_plain, None, CA_PATH)

    def test_plain_tls_ca_str(self):
        self._test_scenario(self.qdb_plain, None, str(CA_PATH))

    def test_auth(self):
        self._test_scenario(self.qdb_auth, AUTH, False)

    def test_auth_tls_insecure_skip_verify(self):
        self._test_scenario(self.qdb_auth, AUTH, 'insecure_skip_verify')

    def test_auth_tls_ca(self):
        self._test_scenario(self.qdb_auth, AUTH, CA_PATH)

    def test_auth_tls_ca_str(self):
        self._test_scenario(self.qdb_auth, AUTH, str(CA_PATH))


if __name__ == '__main__':
    unittest.main()