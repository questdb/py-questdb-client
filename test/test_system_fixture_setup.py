"""Tests for the CI-facing QuestDB system-test setup."""

import pathlib
import urllib.error
import unittest
from unittest import mock

import system_test


class TestSystemFixtureSetup(unittest.TestCase):
    def test_release_download_retries_transient_dns(self):
        transient = urllib.error.URLError('temporary DNS failure')
        with mock.patch.object(system_test, 'QUESTDB_PLAIN_INSTALL_PATH', None), \
                mock.patch.object(system_test, 'QUESTDB_AUTH_INSTALL_PATH', None), \
                mock.patch.dict(system_test.os.environ, {'QDB_REPO_PATH': ''}), \
                mock.patch.object(system_test, 'install_questdb', side_effect=[
                    transient, transient, pathlib.Path('downloaded')]) as install, \
                mock.patch.object(system_test.shutil, 'copytree') as copy, \
                mock.patch.object(system_test.time, 'sleep') as sleep:
            system_test.may_install_questdb()
            self.assertIsNotNone(system_test.QUESTDB_PLAIN_INSTALL_PATH)
            self.assertIsNotNone(system_test.QUESTDB_AUTH_INSTALL_PATH)
        self.assertEqual(install.call_count, 3)
        self.assertEqual(sleep.call_args_list, [mock.call(1), mock.call(2)])
        self.assertEqual(copy.call_count, 2)

    def test_release_download_does_not_retry_http_errors(self):
        missing = urllib.error.HTTPError('https://example.test', 404,
                                         'Not Found', {}, None)
        with mock.patch.object(system_test, 'QUESTDB_PLAIN_INSTALL_PATH', None), \
                mock.patch.dict(system_test.os.environ, {'QDB_REPO_PATH': ''}), \
                mock.patch.object(system_test, 'install_questdb',
                                  side_effect=missing) as install, \
                mock.patch.object(system_test.shutil, 'copytree') as copy, \
                mock.patch.object(system_test.time, 'sleep') as sleep:
            with self.assertRaises(urllib.error.HTTPError):
                system_test.may_install_questdb()
        install.assert_called_once()
        copy.assert_not_called()
        sleep.assert_not_called()

    def test_release_download_retry_budget_is_bounded(self):
        transient = urllib.error.URLError('temporary DNS failure')
        with mock.patch.object(system_test, 'QUESTDB_PLAIN_INSTALL_PATH', None), \
                mock.patch.dict(system_test.os.environ, {'QDB_REPO_PATH': ''}), \
                mock.patch.object(system_test, 'install_questdb',
                                  side_effect=transient) as install, \
                mock.patch.object(system_test.shutil, 'copytree') as copy, \
                mock.patch.object(system_test.time, 'sleep') as sleep:
            with self.assertRaises(urllib.error.URLError):
                system_test.may_install_questdb()
        self.assertEqual(install.call_count, 4)
        copy.assert_not_called()
        self.assertEqual(sleep.call_args_list,
                         [mock.call(1), mock.call(2), mock.call(4)])
