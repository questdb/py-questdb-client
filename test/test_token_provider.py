#!/usr/bin/env python3

################################################################################
##     ___                  _   ____  ____
##    / _ \ _   _  ___  ___| |_|  _ \| __ )
##   | | | | | | |/ _ \/ __| __| | | |  _ \
##   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
##    \__\_\\__,_|\___||___/\__|____/|____/
##
##  Copyright (c) 2014-2019 Appsicle
##  Copyright (c) 2019-2025 QuestDB
##
##  Licensed under the Apache License, Version 2.0 (the "License");
##  you may not use this file except in compliance with the License.
##  You may obtain a copy of the License at
##
##  http://www.apache.org/licenses/LICENSE-2.0
##
##  Unless required by applicable law or agreed to in writing, software
##  distributed under the License is distributed on an "AS IS" BASIS,
##  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
##  See the License for the specific language governing permissions and
##  limitations under the License.
##
################################################################################

"""Rotating Bearer-token providers: ``RefreshingTokenProvider``, application
providers, and ``bearer_token_provider=`` on senders and ``questdb.connect``.

Requires the compiled extension: run with ``PYTHONPATH=src python -m unittest
test_token_provider`` after building in place.
"""

import collections
import datetime
import gc
import threading
import time
import unittest

import questdb
from questdb import _client
from questdb.auth import (
    BearerTokenProvider,
    ExpiringToken,
    RefreshingTokenProvider,
    TokenSource,
    TokenUnavailableError,
)

from qwp_ws_ack_server import QwpAckServer

UTC = datetime.timezone.utc


def _counting_source(lifetime=3600):
    calls = []

    def source():
        calls.append(1)
        return ExpiringToken.expires_in(f'T{len(calls)}', lifetime)

    return source, calls


def _await(predicate, timeout=10.0):
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            raise AssertionError('timed out')
        time.sleep(0.005)


class ExpiringTokenTest(unittest.TestCase):
    def test_expiry_forms(self):
        at = datetime.datetime(2030, 1, 1, tzinfo=UTC)
        self.assertEqual(ExpiringToken('t', at).expires_at, at.timestamp())
        self.assertEqual(ExpiringToken('t', 1.5e9).expires_at, 1.5e9)
        token = ExpiringToken.expires_in(
            't', datetime.timedelta(minutes=5), refresh_at=1.6e9)
        self.assertAlmostEqual(
            token.expires_at, time.time() + 300, delta=5)
        self.assertEqual(token.refresh_at, 1.6e9)

    def test_invalid_values_are_rejected(self):
        with self.assertRaisesRegex(ValueError, 'timezone-aware'):
            ExpiringToken('t', datetime.datetime(2030, 1, 1))
        with self.assertRaises(TypeError):
            ExpiringToken(b't', 1e9)
        with self.assertRaises(TypeError):
            ExpiringToken('t', '2030-01-01')
        with self.assertRaises(ValueError):
            ExpiringToken.expires_in('t', -1)

    def test_repr_never_shows_the_token(self):
        text = repr(ExpiringToken('SENTINEL-secret', 2e9))
        self.assertNotIn('SENTINEL', text)
        self.assertIn('15 chars', text)


class TokenUnavailableErrorTest(unittest.TestCase):
    def test_classification(self):
        retryable = TokenUnavailableError('blip', retry_after=2.5)
        self.assertIsInstance(retryable, questdb.QuestDBError)
        self.assertTrue(retryable.retryable)
        self.assertEqual(retryable.retry_after, 2.5)
        self.assertEqual(retryable.code, questdb.QuestDBErrorCode.SocketError)
        permanent = TokenUnavailableError(
            'misconfigured', retryable=False,
            retry_after=datetime.timedelta(seconds=1))
        self.assertFalse(permanent.retryable)
        self.assertEqual(permanent.retry_after, 1.0)
        self.assertEqual(permanent.code, questdb.QuestDBErrorCode.AuthError)


class RefreshingTokenProviderTest(unittest.TestCase):
    def test_serves_the_sources_token(self):
        source, calls = _counting_source()
        with RefreshingTokenProvider(source, name='test') as provider:
            self.assertTrue(provider.await_ready(10))
            self.assertEqual(provider.token(), 'T1')
            self.assertEqual(
                provider.headers(), {'Authorization': 'Bearer T1'})
            status = provider.status
            self.assertFalse(status.closed)
            self.assertEqual(status.consecutive_failures, 0)
            self.assertIsNone(status.last_failure)
            self.assertGreater(
                status.token_expires_at,
                datetime.datetime.now(UTC) + datetime.timedelta(minutes=50))
            self.assertEqual(len(calls), 1)
        self.assertTrue(provider.closed)

    def test_accepts_every_source_shape(self):
        AccessToken = collections.namedtuple(
            'AccessToken', ['token', 'expires_on'])
        AccessTokenInfo = collections.namedtuple(
            'AccessTokenInfo', ['token', 'expires_on', 'refresh_on'])
        now = int(time.time())

        class Source:
            def fetch_token(self):
                return ('object-source', now + 3600)

        shapes = [
            (lambda: ExpiringToken('expiring', now + 3600), 'expiring'),
            (lambda: ('tuple', now + 3600), 'tuple'),
            (lambda: ('tuple3', now + 3600, now + 1800), 'tuple3'),
            (lambda: AccessToken('azure-like', now + 3600), 'azure-like'),
            (lambda: AccessTokenInfo('azure-info', now + 3600, now + 1800),
             'azure-info'),
            (Source(), 'object-source'),
        ]
        for source, expected in shapes:
            with self.subTest(expected=expected):
                with RefreshingTokenProvider(source) as provider:
                    self.assertEqual(provider.token(), expected)

    def test_a_classified_failure_reaches_the_caller(self):
        def source():
            raise TokenUnavailableError(
                'identity not assigned', retryable=False, retry_after=5)

        with RefreshingTokenProvider(source, cold_wait=0.2) as provider:
            with self.assertRaises(TokenUnavailableError) as caught:
                provider.token()
            self.assertFalse(caught.exception.retryable)
            self.assertIsNotNone(caught.exception.retry_after)
            self.assertIn('identity not assigned', str(caught.exception))
            failure = provider.status.last_failure
            self.assertFalse(failure.retryable)
            self.assertEqual(failure.message, 'identity not assigned')
            self.assertEqual(failure.retry_after, 5.0)

    def test_an_unclassified_exception_is_a_retryable_failure(self):
        def source():
            raise RuntimeError('idp unreachable')

        with RefreshingTokenProvider(
                source, cold_wait=0.2, backoff_initial=0.01,
                backoff_max=0.02) as provider:
            with self.assertRaises(TokenUnavailableError) as caught:
                provider.token()
            self.assertTrue(caught.exception.retryable)
            failure = provider.status.last_failure
            self.assertTrue(failure.retryable)
            self.assertIn('RuntimeError: idp unreachable', failure.message)

    def test_a_wrong_result_is_a_retryable_failure(self):
        with RefreshingTokenProvider(
                lambda: 'just-a-string', cold_wait=0.2) as provider:
            with self.assertRaises(TokenUnavailableError):
                provider.token()
            self.assertIn('ExpiringToken', provider.status.last_failure.message)

    def test_on_rejected_forces_a_refresh(self):
        source, calls = _counting_source()
        with RefreshingTokenProvider(source) as provider:
            self.assertEqual(provider.token(), 'T1')
            provider.on_rejected('T1', 401)
            self.assertEqual(provider.token(), 'T2')
            # Rate limited, and stale or non-401 rejections are ignored.
            provider.on_rejected('T2', 401)
            provider.on_rejected('T1', 401)
            provider.on_rejected('T2', 403)
            self.assertEqual(provider.token(), 'T2')
            self.assertEqual(len(calls), 2)

    def test_close_ends_every_holder(self):
        source, _ = _counting_source()
        provider = RefreshingTokenProvider(source)
        self.assertTrue(provider.await_ready(10))
        provider.close()
        provider.close()
        self.assertTrue(provider.closed)
        with self.assertRaises(TokenUnavailableError) as caught:
            provider.token()
        self.assertFalse(caught.exception.retryable)
        self.assertIn('closed', str(caught.exception))
        self.assertFalse(provider.await_ready(0.1))

    def test_the_gil_is_released_while_waiting(self):
        gate = threading.Event()

        def slow_source():
            gate.wait(10)
            return ExpiringToken.expires_in('slow', 3600)

        ticks = []

        def ticker():
            while not gate.is_set():
                ticks.append(1)
                time.sleep(0.005)

        with RefreshingTokenProvider(slow_source) as provider:
            thread = threading.Thread(target=ticker)
            thread.start()
            threading.Timer(0.2, gate.set).start()
            self.assertEqual(provider.token(), 'slow')
            thread.join()
        self.assertGreater(len(ticks), 5, 'Python could not run meanwhile')

    def test_the_source_is_released_with_the_provider(self):
        baseline = _client._debug_token_callbacks_size()
        source, _ = _counting_source()
        provider = RefreshingTokenProvider(source)
        self.assertEqual(_client._debug_token_callbacks_size(), baseline + 1)
        provider.close()
        del provider
        gc.collect()
        self.assertEqual(_client._debug_token_callbacks_size(), baseline)

    def test_invalid_arguments_are_rejected(self):
        with self.assertRaises(TypeError):
            RefreshingTokenProvider(42)
        with self.assertRaises(ValueError):
            RefreshingTokenProvider(lambda: None, cold_wait=-1)
        with self.assertRaises(ValueError):
            RefreshingTokenProvider(lambda: None, name='  ')
        with self.assertRaises(questdb.QuestDBError) as caught:
            RefreshingTokenProvider(
                lambda: None, backoff_initial=10, backoff_max=1)
        self.assertEqual(
            caught.exception.code, questdb.QuestDBErrorCode.ConfigError)

    def test_the_token_is_never_rendered(self):
        with RefreshingTokenProvider(
                lambda: ExpiringToken.expires_in('SENTINEL-c20', 3600),
                name='c20') as provider:
            self.assertEqual(provider.token(), 'SENTINEL-c20')
            for text in (repr(provider), repr(provider.status)):
                self.assertNotIn('SENTINEL', text)

    def test_protocols(self):
        source, _ = _counting_source()

        class Provider:
            def token(self):
                return 't'

        class Source:
            def fetch_token(self):
                return ('t', 2e9)

        self.assertIsInstance(Provider(), BearerTokenProvider)
        self.assertIsInstance(Source(), TokenSource)


class _RecordingProvider:
    """An application provider that answers "fresh" once told the server
    rejected "stale"."""

    def __init__(self):
        self.rejected = []
        self.calls = 0

    def token(self):
        self.calls += 1
        return 'fresh' if self.rejected else 'stale'

    def on_rejected(self, token, http_status):
        self.rejected.append((token, http_status))


class BearerTokenProviderAttachTest(unittest.TestCase):
    def _send_one_row(self, server, provider, extra=''):
        conf = (
            f'ws::addr=127.0.0.1:{server.port};'
            'reconnect_initial_backoff_millis=1;'
            'reconnect_max_backoff_millis=1;' + extra)
        with questdb.Sender.from_conf(
                conf, bearer_token_provider=provider,
                auto_flush=False) as sender:
            sender.row(
                'events', columns={'value': 42}, at=questdb.ServerTimestamp)
            fsn = sender.flush_and_get_fsn()
            self.assertTrue(sender.await_acked_fsn(fsn, 5000))
        return server.snapshot()['upgrade_authorizations']

    def test_a_refreshing_provider_on_a_sender(self):
        source, _ = _counting_source()
        with RefreshingTokenProvider(source) as provider, \
                QwpAckServer(required_authorization='Bearer T1') as server:
            seen = self._send_one_row(server, provider)
        self.assertEqual(seen, ['Bearer T1'])

    def test_a_callable_on_a_sender(self):
        with QwpAckServer(required_authorization='Bearer fixed') as server:
            seen = self._send_one_row(server, lambda: 'fixed')
        self.assertEqual(seen, ['Bearer fixed'])

    def test_an_application_provider_hears_a_401_and_is_retried(self):
        provider = _RecordingProvider()
        with QwpAckServer(required_authorization='Bearer fresh') as server:
            seen = self._send_one_row(server, provider)
        self.assertEqual(seen, ['Bearer stale', 'Bearer fresh'])
        self.assertEqual(provider.rejected, [('stale', 401)])

    def test_a_refreshing_provider_refreshes_a_rejected_token(self):
        source, calls = _counting_source()
        with RefreshingTokenProvider(source) as provider, \
                QwpAckServer(required_authorization='Bearer T2') as server:
            self.assertEqual(provider.token(), 'T1')
            seen = self._send_one_row(server, provider)
        self.assertEqual(seen, ['Bearer T1', 'Bearer T2'])
        self.assertEqual(len(calls), 2)

    def test_a_pool_shares_the_provider(self):
        source, calls = _counting_source()
        with RefreshingTokenProvider(source) as provider, \
                QwpAckServer(required_authorization='Bearer T1') as server:
            with questdb.connect(
                    f'ws::addr=127.0.0.1:{server.port};lazy_connect=true;',
                    bearer_token_provider=provider) as db:
                with db.sender() as sender:
                    sender.row(
                        'events', columns={'value': 1},
                        at=questdb.ServerTimestamp)
                    sender.flush()
            seen = server.snapshot()['upgrade_authorizations']
        self.assertTrue(seen)
        self.assertTrue(all(auth == 'Bearer T1' for auth in seen), seen)
        self.assertEqual(len(calls), 1)

    def test_a_permanent_failure_ends_a_synchronous_connect(self):
        calls = []

        def provider():
            calls.append(1)
            raise TokenUnavailableError(
                'identity not assigned', retryable=False)

        with QwpAckServer() as server:
            conf = (
                f'ws::addr=127.0.0.1:{server.port};'
                'initial_connect_retry=sync;'
                'reconnect_initial_backoff_millis=10;'
                'reconnect_max_backoff_millis=10;'
                'reconnect_max_duration_millis=2000;')
            started = time.monotonic()
            with self.assertRaises(TokenUnavailableError) as caught:
                # Entering the block establishes the connection.
                with questdb.Sender.from_conf(
                        conf, bearer_token_provider=provider):
                    pass
            self.assertLess(time.monotonic() - started, 1.5)
        self.assertFalse(caught.exception.retryable)
        self.assertEqual(len(calls), 1)

    def test_a_retryable_failure_is_retried_by_a_synchronous_connect(self):
        calls = []

        def provider():
            calls.append(1)
            if len(calls) < 3:
                raise TokenUnavailableError('idp unreachable')
            return 'recovered'

        with QwpAckServer(required_authorization='Bearer recovered') as server:
            conf = (
                f'ws::addr=127.0.0.1:{server.port};'
                'initial_connect_retry=sync;'
                'reconnect_initial_backoff_millis=10;'
                'reconnect_max_backoff_millis=10;'
                'reconnect_max_duration_millis=5000;')
            # Entering the block establishes the connection.
            with questdb.Sender.from_conf(
                    conf, bearer_token_provider=provider, auto_flush=False):
                pass
            seen = server.snapshot()['upgrade_authorizations']
        self.assertEqual(len(calls), 3)
        self.assertEqual(seen, ['Bearer recovered'])

    def test_conflicts_are_rejected(self):
        provider = lambda: 't'
        with self.assertRaises(questdb.QuestDBError) as caught:
            questdb.Sender.from_conf(
                'ws::addr=127.0.0.1:9000;token=static;',
                bearer_token_provider=provider)
        self.assertIn('"token"', str(caught.exception))
        with self.assertRaises(questdb.QuestDBError):
            questdb.connect(
                'ws::addr=127.0.0.1:9000;username=u;password=p;'
                'lazy_connect=true;',
                bearer_token_provider=provider)
        with self.assertRaises(TypeError):
            questdb.Sender.from_conf(
                'ws::addr=127.0.0.1:9000;', bearer_token_provider=42)

    def test_the_reserved_connect_string_keys_are_rejected(self):
        for conf in (
                'http::addr=127.0.0.1:9000;token_provider=azure;',
                'ws::addr=127.0.0.1:9000;azure_resource=api://questdb;',
                'wss::addr=127.0.0.1:9000;azure_credential=default;'):
            with self.subTest(conf=conf):
                with self.assertRaises(questdb.QuestDBError) as caught:
                    questdb.Sender.from_conf(conf)
                self.assertEqual(
                    caught.exception.code,
                    questdb.QuestDBErrorCode.ConfigError)
                self.assertIn('bearer_token_provider', str(caught.exception))
        with self.assertRaises(questdb.QuestDBError) as caught:
            questdb.connect(
                'ws::addr=127.0.0.1:9000;lazy_connect=true;'
                'token_provider=azure;')
        self.assertIn('token_provider', str(caught.exception))

    def test_the_holder_lives_with_the_sender(self):
        baseline = _client._debug_token_callbacks_size()
        with QwpAckServer(required_authorization='Bearer fixed') as server:
            self._send_one_row(server, lambda: 'fixed')
        gc.collect()
        self.assertEqual(_client._debug_token_callbacks_size(), baseline)


class CleartextWarningTest(unittest.TestCase):
    """Spec section 9: a provider used without TLS is warned about once per
    process."""

    def setUp(self):
        self._warned = _client._tp_cleartext_warned
        _client._tp_cleartext_warned = False

    def tearDown(self):
        _client._tp_cleartext_warned = self._warned

    def test_a_provider_without_tls_is_warned_about_once(self):
        with self.assertLogs('questdb', level='WARNING') as captured:
            questdb.Sender.from_conf(
                'ws::addr=127.0.0.1:9000;',
                bearer_token_provider=lambda: 't').close()
            questdb.Sender.from_conf(
                'http::addr=127.0.0.1:9000;',
                bearer_token_provider=lambda: 't').close()
            questdb.connect(
                'ws::addr=127.0.0.1:9000;lazy_connect=true;',
                bearer_token_provider=lambda: 't').close()
        self.assertEqual(len(captured.records), 1, captured.output)
        self.assertIn('over ws:: (no TLS)', captured.output[0])
        self.assertIn('use wss:: in production', captured.output[0])

    def test_a_pool_without_tls_is_warned_about(self):
        with self.assertLogs('questdb', level='WARNING') as captured:
            questdb.connect(
                'ws::addr=127.0.0.1:9000;lazy_connect=true;',
                bearer_token_provider=lambda: 't').close()
        self.assertEqual(len(captured.records), 1, captured.output)

    def test_tls_or_no_provider_is_not_warned_about(self):
        with self.assertNoLogs('questdb', level='WARNING'):
            questdb.Sender.from_conf(
                'wss::addr=127.0.0.1:9000;',
                bearer_token_provider=lambda: 't').close()
            questdb.Sender.from_conf('ws::addr=127.0.0.1:9000;').close()
        self.assertFalse(_client._tp_cleartext_warned)


if __name__ == '__main__':
    unittest.main()
