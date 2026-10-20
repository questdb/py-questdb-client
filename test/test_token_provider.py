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
import importlib.util
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import textwrap
import threading
import time
import unittest
import weakref

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
from qwp_egress_failover_server import EgressFailoverServer

try:
    import pandas as pd
except ImportError:
    pd = None

try:
    import pyarrow as pa
except ImportError:
    pa = None

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


def _collect_until(predicate):
    def collected():
        gc.collect()
        return predicate()

    _await(collected)


def _last_failure(provider):
    """The provider's most recent failed fetch, once one is recorded.

    A ``token()`` that gives up after ``cold_wait`` can return before the
    refresher thread has run the source at all, so the failure may not be
    recorded yet when it raises.
    """
    _await(lambda: provider.status.last_failure is not None)
    return provider.status.last_failure


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

    def test_expiry_must_fit_a_datetime_snapshot(self):
        for keyword in ('expires_at', 'refresh_at'):
            for value in (1e12, -1e12):
                with self.subTest(keyword=keyword, value=value):
                    args = {'expires_at': 2e9, keyword: value}
                    with self.assertRaisesRegex(ValueError, 'Python datetime'):
                        ExpiringToken('t', **args)

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

        # A `cold_wait` below the source's `retry_after` returns as soon as
        # the first fetch fails; it only has to outlast a slow first fetch.
        with RefreshingTokenProvider(source, cold_wait=2) as provider:
            with self.assertRaises(TokenUnavailableError) as caught:
                provider.token()
            self.assertFalse(caught.exception.retryable)
            self.assertIsNotNone(caught.exception.retry_after)
            self.assertIn('identity not assigned', str(caught.exception))
            failure = _last_failure(provider)
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
            failure = _last_failure(provider)
            self.assertTrue(failure.retryable)
            self.assertIn('RuntimeError: idp unreachable', failure.message)

    def test_a_wrong_result_is_a_retryable_failure(self):
        with RefreshingTokenProvider(
                lambda: 'just-a-string', cold_wait=0.2) as provider:
            with self.assertRaises(TokenUnavailableError):
                provider.token()
            self.assertIn('ExpiringToken', _last_failure(provider).message)

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

    def test_close_can_drain_an_active_source_with_a_deadline(self):
        entered = threading.Event()
        release = threading.Event()

        def source():
            entered.set()
            release.wait(10)
            return ExpiringToken.expires_in('valid', 3600)

        provider = RefreshingTokenProvider(source)
        try:
            self.assertTrue(entered.wait(5))
            with self.assertRaises(TimeoutError):
                provider.close(wait=True, timeout=0.01)
            self.assertTrue(provider.closed)
        finally:
            release.set()
            provider.close(wait=True, timeout=datetime.timedelta(seconds=5))

    def test_source_cannot_wait_for_its_own_close(self):
        ready = threading.Event()
        completed = threading.Event()
        errors = []

        def source():
            ready.wait(5)
            try:
                provider.close(wait=True)
            except BaseException as exc:
                errors.append(exc)
            finally:
                completed.set()
            return ExpiringToken.expires_in('valid', 3600)

        provider = RefreshingTokenProvider(source)
        ready.set()
        try:
            self.assertTrue(completed.wait(5))
            self.assertEqual(len(errors), 1)
            self.assertIsInstance(errors[0], questdb.QuestDBError)
            self.assertIs(errors[0].code, questdb.QuestDBErrorCode.InvalidApiCall)
        finally:
            provider.close(wait=True, timeout=5)

    def test_real_shutdown_after_draining_a_native_source(self):
        script = textwrap.dedent('''
            import sys, threading
            from questdb.auth import RefreshingTokenProvider, ExpiringToken
            print('__QDB_BEGIN_FINALIZATION__', file=sys.stderr, flush=True)
            entered = threading.Event()
            release = threading.Event()
            def source():
                entered.set()
                assert release.wait(5)
                return ExpiringToken.expires_in('valid', 3600)
            provider = RefreshingTokenProvider(source)
            assert entered.wait(5)
            try:
                provider.close(wait=True, timeout=0)
            except TimeoutError:
                pass
            else:
                raise AssertionError('active source was not observed')
            release.set()
            provider.close(wait=True, timeout=5)
        ''')
        # In-place CI builds add src to sys.path only in the parent. Resolve
        # the same package in the child, whether built in place or wheel-installed.
        env = os.environ.copy()
        env['PYTHONPATH'] = os.pathsep.join(filter(None, (
            os.path.dirname(os.path.dirname(questdb.__file__)),
            env.get('PYTHONPATH'))))
        result = subprocess.run(
            [sys.executable, '-c', script], env=env, capture_output=True,
            text=True, timeout=15)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        # Import-time PyPy/Cython warnings are unrelated to source draining or
        # shutdown. Keep checking every diagnostic after the imports, including
        # unraisable exceptions emitted during interpreter finalization.
        _, marker, diagnostics = result.stderr.partition(
            '__QDB_BEGIN_FINALIZATION__\n')
        self.assertTrue(marker, result.stderr)
        self.assertEqual(diagnostics, '')

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
        # token() waits for a source that only another Python thread releases,
        # after running for a while. Were the GIL held for the wait, neither
        # that thread nor the source could run, and token() would fail after
        # cold_wait. Causal, so a slow or loaded machine only makes it slower.
        gate = threading.Event()

        def slow_source():
            gate.wait(10)
            return ExpiringToken.expires_in('slow', 3600)

        def run_python_then_release():
            for _ in range(10):
                time.sleep(0.005)  # Each wake-up re-acquires the GIL.
            gate.set()

        with RefreshingTokenProvider(slow_source, cold_wait=10) as provider:
            thread = threading.Thread(target=run_python_then_release)
            thread.start()
            self.assertEqual(provider.token(), 'slow')
            thread.join()

    def test_the_source_is_released_with_the_provider(self):
        before = _client._debug_token_callbacks_ids()
        source, _ = _counting_source()
        watch = weakref.ref(source)
        provider = RefreshingTokenProvider(source)
        registered = _client._debug_token_callbacks_ids() - before
        self.assertEqual(len(registered), 1)
        del source
        self.assertIsNotNone(watch())
        provider.close()
        del provider
        _collect_until(lambda: watch() is None and not (
            registered & _client._debug_token_callbacks_ids()))

    def test_owning_source_cycles_are_collected_even_without_close(self):
        class Credentials:
            def __init__(self):
                self.tokens = RefreshingTokenProvider(self.fetch_token)

            def fetch_token(self):
                return ExpiringToken.expires_in('t', 3600)

        for close in (False, True):
            with self.subTest(close=close):
                before = _client._debug_token_callbacks_ids()
                owner = Credentials()
                self.assertTrue(owner.tokens.await_ready(10))
                registered = _client._debug_token_callbacks_ids() - before
                self.assertEqual(len(registered), 1)
                if close:
                    owner.tokens.close()
                watch = weakref.ref(owner)
                del owner
                _collect_until(lambda: watch() is None and not (
                    registered & _client._debug_token_callbacks_ids()))

    def test_dispatch_keeps_an_owning_source_alive_during_collection(self):
        started = threading.Event()
        release = threading.Event()

        class Credentials:
            def __init__(self):
                self.tokens = RefreshingTokenProvider(self.fetch_token)

            def fetch_token(self):
                started.set()
                release.wait(10)
                return ExpiringToken.expires_in('t', 3600)

        before = _client._debug_token_callbacks_ids()
        owner = Credentials()
        watch = weakref.ref(owner)
        registered = _client._debug_token_callbacks_ids() - before
        try:
            self.assertTrue(started.wait(5))
            del owner
            gc.collect()
            self.assertIsNotNone(watch(), 'dispatch must retain its callback owner')
        finally:
            release.set()
        _collect_until(lambda: watch() is None and not (
            registered & _client._debug_token_callbacks_ids()))

    def test_far_future_source_results_are_classified_not_broken_snapshots(self):
        token = ExpiringToken('t', 2e9)
        token.expires_at = 1e12  # Mutated instances are validated at fetch too.
        for source in (lambda: ('t', 1e12), lambda: token):
            with self.subTest(source=source):
                with RefreshingTokenProvider(source, cold_wait=0.02) as provider:
                    with self.assertRaises(TokenUnavailableError):
                        provider.token()
                    self.assertFalse(provider.closed)
                    self.assertIn('Python datetime', _last_failure(provider).message)
                self.assertTrue(provider.closed)

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

    def test_http_callback_interrupt_is_delivered_once_after_sending(self):
        # Use a subprocess so a deferred pending-call interrupt cannot escape
        # into the test runner if the callback delivery regresses.
        script = textwrap.dedent('''
            import signal
            import os
            import questdb
            from questdb import _client
            from questdb.auth import TokenUnavailableError
            from mock_server import HttpServer
            _client._debug_oidc_fail_pending_calls(
                int(os.environ['QDB_TEST_PENDING_FAILURES']))
            for kind in ('interrupt', 'deadline', 'classified_deadline', 'rejected'):
                if 'deadline' in kind and not hasattr(signal, 'SIGALRM'):
                    continue
                calls = []
                rejected = []
                def alarm(signum, frame):
                    # Classification must use the pre-callback snapshot, not
                    # only handlers that remain registered after the raise.
                    signal.signal(signal.SIGALRM, signal.SIG_IGN)
                    if kind == 'classified_deadline':
                        raise TokenUnavailableError('signal deadline', retryable=False)
                    raise TimeoutError('signal deadline')
                class Provider:
                    def token(self):
                        calls.append(1)
                        if len(calls) == 1:
                            if kind == 'interrupt':
                                raise KeyboardInterrupt()
                            if 'deadline' in kind:
                                signal.raise_signal(signal.SIGALRM)
                        return 'fresh' if rejected else 'stale'
                    def on_rejected(self, token, status):
                        rejected.append((token, status))
                        raise KeyboardInterrupt()
                if 'deadline' in kind:
                    signal.signal(signal.SIGALRM, alarm)
                with HttpServer() as server:
                    if kind == 'rejected':
                        server.responses.extend([(0, 401, None, None),
                                                 (0, 204, None, None)])
                    sender = questdb.Sender(
                        questdb.Protocol.Http, '127.0.0.1', server.port,
                        bearer_token_provider=Provider(), auto_flush=False,
                        retry_timeout=1000, protocol_version=2)
                    sender.establish()
                    sender.row('t', columns={'v': 1}, at=questdb.ServerTimestamp)
                    expected = (TokenUnavailableError if kind == 'classified_deadline'
                                else TimeoutError if kind == 'deadline'
                                else KeyboardInterrupt)
                    try:
                        try:
                            sender.flush()
                        except expected:
                            pass
                        else:
                            raise AssertionError('signal was swallowed')
                        assert len(calls) == 2, (kind, calls)
                        assert len(server.requests) == (2 if kind == 'rejected' else 1)
                        assert len(sender) == 0
                        # Pending delivery has completed, not merely moved to
                        # the next token invocation or client call.
                        sender.flush()
                    finally:
                        sender.close(flush=False)
        ''')
        env = os.environ.copy()
        env['PYTHONPATH'] = os.pathsep.join((
            str(Path(questdb.__file__).resolve().parent.parent),
            str(Path(__file__).resolve().parent), env.get('PYTHONPATH', '')))
        for pending_failures in (0, 64):
            with self.subTest(pending_failures=pending_failures):
                env['QDB_TEST_PENDING_FAILURES'] = str(pending_failures)
                result = subprocess.run(
                    [sys.executable, '-c', script], env=env,
                    capture_output=True, text=True, timeout=20)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_shared_application_provider_has_one_native_identity(self):
        class Provider:
            # Neither hashing nor equality is needed for identity sharing.
            __hash__ = None

            def __eq__(self, other):
                raise AssertionError('providers must not be compared')

            def token(self):
                return 'valid'

        for bound_method in (False, True):
            with self.subTest(bound_method=bound_method):
                provider = Provider()
                before = _client._debug_token_callbacks_ids()
                first = questdb.Sender.from_conf(
                    'ws::addr=127.0.0.1:9000;lazy_connect=true;',
                    bearer_token_provider=provider.token if bound_method else provider)
                second = questdb.connect(
                    'ws::addr=127.0.0.1:9000;lazy_connect=true;',
                    bearer_token_provider=provider.token if bound_method else provider)
                try:
                    registered = _client._debug_token_callbacks_ids() - before
                    self.assertEqual(len(registered), 1)
                    first.close(flush=False)
                    self.assertTrue(registered <= _client._debug_token_callbacks_ids())
                finally:
                    first.close(flush=False)
                    second.close()
                _collect_until(lambda: not (
                    registered & _client._debug_token_callbacks_ids()))

    def test_concurrent_provider_attachments_publish_one_holder(self):
        barrier = threading.Barrier(2, timeout=5)
        clients = []
        errors = []

        class Provider:
            @property
            def token(self):
                # Both constructors must be able to inspect the application
                # target without holding the callback-registry lock.
                barrier.wait()
                return lambda: 'valid'

        provider = Provider()
        before = _client._debug_token_callbacks_ids()

        def attach():
            try:
                clients.append(questdb.Sender.from_conf(
                    'ws::addr=127.0.0.1:9000;lazy_connect=true;',
                    bearer_token_provider=provider))
            except BaseException as exc:
                errors.append(exc)

        threads = [threading.Thread(target=attach) for _ in range(2)]
        try:
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(10)
                self.assertFalse(thread.is_alive())
            self.assertEqual(errors, [])
            _collect_until(lambda: len(_client._debug_token_callbacks_ids() - before) == 1)
        finally:
            for client in clients:
                client.close(flush=False)
        _collect_until(lambda: _client._debug_token_callbacks_ids() == before)

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
        class Provider:
            def __call__(self):
                return 'fixed'

        before = _client._debug_token_callbacks_ids()
        provider = Provider()
        watch = weakref.ref(provider)
        with QwpAckServer(required_authorization='Bearer fixed') as server:
            sender = questdb.Sender.from_conf(
                f'ws::addr=127.0.0.1:{server.port};',
                bearer_token_provider=provider, auto_flush=False)
            registered = _client._debug_token_callbacks_ids() - before
            self.assertEqual(len(registered), 1)
            del provider
            gc.collect()
            self.assertIsNotNone(watch())
            with sender:
                sender.row('t', columns={'v': 1}, at=questdb.ServerTimestamp)
                sender.flush()
            del sender
        _collect_until(lambda: watch() is None and not (
            registered & _client._debug_token_callbacks_ids()))

    def test_closing_a_sender_releases_its_provider_while_still_alive(self):
        class Provider:
            def __call__(self):
                return 'fixed'

        class SenderSubclass(questdb.Sender):
            pass

        # Explicit close must clear the live Python dictionary, including on
        # PyPy and Python subclasses (which need not share the C dict layout).
        for sender_type in (questdb.Sender, SenderSubclass):
            with self.subTest(sender_type=sender_type):
                before = _client._debug_token_callbacks_ids()
                provider = Provider()
                watch = weakref.ref(provider)
                sender = sender_type(
                    questdb.Protocol.Ws, '127.0.0.1', 9000,
                    bearer_token_provider=provider)
                self.assertIsInstance(sender, sender_type)
                registered = _client._debug_token_callbacks_ids() - before
                self.assertEqual(len(registered), 1)
                del provider
                gc.collect()
                self.assertIsNotNone(watch())
                sender.close(flush=False)
                sender.close(flush=False)
                _collect_until(lambda: watch() is None and not (
                    registered & _client._debug_token_callbacks_ids()))

    def test_sender_finalization_does_not_emit_unraisable_errors(self):
        # Ordinary attribute access in __dealloc__ tries to resurrect a dying
        # PyPy cpyext wrapper. The process can exit successfully while printing
        # an ignored exception, so check stderr as well as the exit status.
        script = textwrap.dedent('''
            import gc, sys
            import questdb
            from questdb import _client
            print('__QDB_BEGIN_FINALIZATION__', file=sys.stderr, flush=True)
            class SenderSubclass(questdb.Sender):
                pass
            for sender_type in (questdb.Sender, SenderSubclass):
                for initialized in (False, True):
                    for clear_dict in (False, True):
                        if initialized:
                            sender = sender_type(
                                questdb.Protocol.Wss, '127.0.0.1', 9000,
                                bearer_token_provider=lambda: 'fixed')
                        else:
                            sender = sender_type.__new__(sender_type)
                        if clear_dict:
                            sender.__dict__.clear()
                        del sender
                        for _ in range(20):
                            gc.collect()
            assert not _client._debug_token_callbacks_ids()
        ''')
        env = os.environ.copy()
        env['PYTHONPATH'] = os.pathsep.join(filter(None, (
            os.path.dirname(os.path.dirname(questdb.__file__)),
            env.get('PYTHONPATH'))))
        result = subprocess.run(
            [sys.executable, '-c', script], env=env,
            capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        _, marker, diagnostics = result.stderr.partition(
            '__QDB_BEGIN_FINALIZATION__\n')
        self.assertTrue(marker, result.stderr)
        self.assertEqual(diagnostics, '')

    def test_an_unclosed_provider_client_cycle_is_collected(self):
        # Neither the pool nor the standalone sender may hide this ownership
        # edge in a cpyext field: PyPy would keep the entire cycle alive.
        for factory in (questdb.connect, questdb.Sender.from_conf):
            with self.subTest(factory=factory):
                class Owner:
                    def __init__(self):
                        self.client = factory(
                            'ws::addr=127.0.0.1:9000;lazy_connect=true;',
                            bearer_token_provider=self)

                    def token(self):
                        return 't'

                before = _client._debug_token_callbacks_ids()
                owner = Owner()
                registered = _client._debug_token_callbacks_ids() - before
                self.assertEqual(len(registered), 1)
                watch = weakref.ref(owner)
                del owner
                _collect_until(lambda: watch() is None and not (
                    registered & _client._debug_token_callbacks_ids()))

    def test_closing_a_query_releases_its_provider_while_still_alive(self):
        class Provider:
            def __call__(self):
                return 'valid'

        with EgressFailoverServer() as server:
            before = _client._debug_token_callbacks_ids()
            provider = Provider()
            watch = weakref.ref(provider)
            db = questdb.connect(
                f'ws::addr=127.0.0.1:{server.port};lazy_connect=true;',
                bearer_token_provider=provider)
            result = db.query('select v from t')
            registered = _client._debug_token_callbacks_ids() - before
            self.assertEqual(len(registered), 1)
            db.close()
            del provider, db
            gc.collect()
            self.assertIsNotNone(watch())
            result.close()
            result.close()
            _collect_until(lambda: watch() is None and not (
                registered & _client._debug_token_callbacks_ids()))

    @unittest.skipIf(pd is None, 'pandas not installed')
    def test_query_retains_provider_through_failover_after_pool_close(self):
        for refreshing in (False, True):
            with self.subTest(refreshing=refreshing), EgressFailoverServer() as server:
                class Provider:
                    def __call__(self):
                        if refreshing:
                            return ExpiringToken.expires_in('valid', 3600)
                        return 'valid'

                before = _client._debug_token_callbacks_ids()
                source = Provider()
                watch = weakref.ref(source)
                provider = RefreshingTokenProvider(source) if refreshing else source
                db = questdb.connect(
                    f'ws::addr=127.0.0.1:{server.port};lazy_connect=true;'
                    'reconnect_max_duration_millis=1000;',
                    bearer_token_provider=provider)
                result = db.query('select v from t')
                registered = _client._debug_token_callbacks_ids() - before
                self.assertEqual(len(registered), 1)
                del provider, source
                db.close()
                del db
                gc.collect()
                self.assertIsNotNone(watch())
                self.assertTrue(registered <= _client._debug_token_callbacks_ids())
                try:
                    server.release_first.set()
                    self.assertEqual(result.to_pandas()['v'].tolist(), [1, 2, 3])
                    self.assertGreaterEqual(len(server.authorizations), 2)
                    self.assertTrue(all(
                        value == 'Bearer valid' for value in server.authorizations))
                finally:
                    result.close()
                # Closing the reader must release the holder even while the
                # Python result wrapper remains alive.
                _collect_until(lambda: watch() is None and not (
                    registered & _client._debug_token_callbacks_ids()))

    @unittest.skipIf(pd is None, 'pandas not installed')
    def test_permanent_failure_stops_pandas_dataframe_retries(self):
        self._assert_permanent_dataframe_failure(pd.DataFrame({'v': [1]}))

    @unittest.skipIf(pa is None, 'pyarrow not installed')
    def test_permanent_failure_stops_arrow_dataframe_retries(self):
        self._assert_permanent_dataframe_failure(pa.table({'v': [1]}))

    def _assert_permanent_dataframe_failure(self, frame):
        calls = []

        def provider():
            calls.append(1)
            raise TokenUnavailableError('identity not assigned', retryable=False)

        with questdb.connect(
                'ws::addr=127.0.0.1:9000;lazy_connect=true;'
                'reconnect_max_duration_millis=250;',
                bearer_token_provider=provider) as db:
            with self.assertRaises(TokenUnavailableError) as caught:
                db.dataframe(frame, table_name='t', at=questdb.ServerTimestamp)
        self.assertFalse(caught.exception.retryable)
        self.assertEqual(len(calls), 1)

    @unittest.skipIf(pd is None, 'pandas not installed')
    def test_dataframe_auto_flush_preserves_provider_classification(self):
        def provider():
            raise TokenUnavailableError(
                'identity not assigned', retryable=False, retry_after=2.5)

        sender = questdb.Sender(
            questdb.Protocol.Http, '127.0.0.1', 9000,
            bearer_token_provider=provider, protocol_version=2,
            auto_flush_rows=1, auto_flush_bytes=False, auto_flush_interval=False)
        sender.establish()
        try:
            with self.assertRaises(TokenUnavailableError) as caught:
                sender.dataframe(
                    pd.DataFrame({'v': [1]}), table_name='t',
                    at=questdb.ServerTimestamp)
            self.assertFalse(caught.exception.retryable)
            self.assertEqual(caught.exception.retry_after, 2.5)
            self.assertIs(caught.exception.code, questdb.QuestDBErrorCode.SocketError)
            self.assertIn(' - See https://', str(caught.exception))
        finally:
            sender.close(flush=False)


@unittest.skipUnless(hasattr(os, 'fork'), 'os.fork required')
class TokenCallbackForkTest(unittest.TestCase):
    def test_child_cleanup_does_not_acquire_an_inherited_registry_lock(self):
        # No native provider/thread creation in the child: only exercise Python
        # registry cleanup. Native fork+thread continuation is not supported.
        script = textwrap.dedent('''
            import os, select, signal, threading, warnings
            from questdb import _client
            locked = threading.Event()
            release = threading.Event()
            def hold():
                with _client._debug_token_callbacks_lock():
                    locked.set()
                    release.wait(10)
            thread = threading.Thread(target=hold)
            thread.start()
            assert locked.wait(2)
            r, w = os.pipe()
            with warnings.catch_warnings():
                warnings.simplefilter('ignore', DeprecationWarning)
                pid = os.fork()
            if pid == 0:
                os.close(r)
                _client._debug_token_callbacks_size()
                os.write(w, b'1')
                os._exit(0)
            os.close(w)
            ready = select.select([r], [], [], 3)[0]
            data = os.read(r, 1) if ready else b''
            os.close(r)
            release.set()
            thread.join(2)
            if not ready:
                os.kill(pid, signal.SIGKILL)
            _, status = os.waitpid(pid, 0)
            assert data == b'1' and status == 0, (data, status)
        ''')
        env = os.environ.copy()
        env['PYTHONPATH'] = os.pathsep.join(filter(None, (
            os.path.dirname(os.path.dirname(questdb.__file__)),
            env.get('PYTHONPATH'))))
        result = subprocess.run(
            [sys.executable, '-c', script], env=env,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, timeout=15)
        self.assertEqual(result.returncode, 0, result.stdout)


@unittest.skipUnless(importlib.util.find_spec('mypy'), 'optional mypy not installed')
class TokenProviderTypingTest(unittest.TestCase):
    def test_supported_sources_snapshots_and_adapters_typecheck(self):
        root = Path(__file__).resolve().parent.parent
        env = os.environ.copy()
        env['MYPYPATH'] = str(root / 'src')
        with tempfile.TemporaryDirectory() as cache:
            result = subprocess.run(
                [sys.executable, '-m', 'mypy', '--no-incremental',
                 '--cache-dir', cache, '--follow-imports=silent',
                 # Existing TaggedEnum stub diagnostic, unrelated to this API.
                 '--disable-error-code=misc',
                 str(root / 'test' / 'typing_token_providers.py')],
                cwd=root, env=env, stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT, text=True, timeout=60)
        self.assertEqual(result.returncode, 0, result.stdout)


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
