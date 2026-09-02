################################################################################
##     ___                  _   ____  ____
##    / _ \ _   _  ___  ___| |_|  _ \| __ )
##   | | | | | | |/ _ \/ __| __| | | |  _ \
##   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
##    \__\_\\__,_|\___||___/\__|____/|____/
##
##  Copyright (c) 2014-2019 Appsicle
##  Copyright (c) 2019-2026 QuestDB
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

"""Python binding tests for the native OIDC implementation."""

import gc
import io
import os
import platform
import sys
import tempfile
import threading
import time
import types
import unittest
import weakref
from unittest import mock

import questdb
from questdb.auth import (
    FileTokenStore,
    OidcCancelledError,
    OidcConfigError,
    OidcDeviceAuth,
    OidcDeviceFlowError,
    OidcError,
    OidcNetworkError,
    OidcInteractionRequired,
    OidcTimeoutError,
    Renderer,
)
from questdb.auth import _adapters
from questdb.auth import _render
from questdb.auth._render import (
    TerminalRenderer,
    _verification_target,
    in_ipython_kernel,
    make_renderer,
)
from oidc_test_server import OidcTestServer
from qwp_ws_ack_server import QwpAckServer

try:
    import pandas as pd
except ImportError:
    pd = None


def make_auth(**kwargs):
    options = dict(interactive=False, open_browser=False)
    options.update(kwargs)
    return OidcDeviceAuth(
        'questdb',
        'https://idp.example/device',
        'https://idp.example/token',
        **options)


def make_discovered_auth(server, **kwargs):
    options = dict(
        interactive=True, open_browser=False, timeout=5,
        renderer=Renderer())
    options.update(kwargs)
    return OidcDeviceAuth.from_questdb(server.url, **options)


def _live_weakref_count():
    """Live ``weakref.ref`` objects, after draining pending finalizers.

    Each ``OidcDeviceAuth`` hands the native event handler a Py_INCREF'd
    weakref to the provider; the native release callback must Py_DECREF it
    exactly once. A leaked one stays alive (held by the native +1) and, since
    ``weakref.ref`` instances are cyclic-GC tracked, shows up here. Collect
    until the count settles rather than a fixed number of passes: CPython
    stabilises in one or two, while PyPy stages cpyext finalization across
    several, and a fixed count could undercount live-then-freed objects there.
    """
    prev = None
    for _ in range(30):
        gc.collect()
        count = sum(1 for obj in gc.get_objects()
                    if isinstance(obj, weakref.ReferenceType))
        if count == prev:
            break
        prev = count
    return count


class RecordingRenderer(Renderer):
    def __init__(self):
        self.prompts = []
        self.waiting = []
        self.successes = []
        self.failures = []

    def on_prompt(self, response):
        self.prompts.append(response)

    def on_waiting(self, seconds_left):
        self.waiting.append(seconds_left)

    def on_success(self, identity, expires_in):
        self.successes.append((identity, expires_in))

    def on_failure(self, message):
        self.failures.append(message)


EXPIRED_ACCESS_TOKEN = 'e30.eyJleHAiOjF9.'


class NativeOidcTest(unittest.TestCase):
    def test_boolean_options_require_actual_bool_values(self):
        for name in (
                'groups_in_token', 'insecure', 'open_browser', 'interactive',
                'qr'):
            invalid_values = (0, 1, 'false', object())
            if name != 'interactive':
                invalid_values += (None,)
            for value in invalid_values:
                with self.subTest(name=name, value=value):
                    with self.assertRaisesRegex(OidcConfigError, name):
                        make_auth(**{name: value})

    def test_from_questdb_validates_booleans_before_url(self):
        for name in (
                'groups_in_token', 'insecure', 'open_browser', 'interactive',
                'qr'):
            with self.subTest(name=name):
                with self.assertRaisesRegex(OidcConfigError, name):
                    OidcDeviceAuth.from_questdb(
                        object(), **{name: 'false'})

    def test_documented_optional_booleans_retain_none(self):
        auth = make_auth(interactive=None)
        self.assertEqual(auth.config.client_id, 'questdb')
        with self.assertRaisesRegex(OidcConfigError, 'url'):
            OidcDeviceAuth.from_questdb(
                object(), groups_in_token=None, interactive=None)

    def test_terminal_ipython_uses_terminal_renderer(self):
        ipython = types.ModuleType('IPython')
        shell = type('TerminalInteractiveShell', (), {})()
        ipython.get_ipython = lambda: shell

        with mock.patch.dict(sys.modules, {'IPython': ipython}):
            self.assertFalse(in_ipython_kernel())
            self.assertIsInstance(make_renderer(), TerminalRenderer)

    def test_zmq_ipython_is_detected_as_kernel(self):
        ipython = types.ModuleType('IPython')
        shell = type('ZMQInteractiveShell', (), {})()
        ipython.get_ipython = lambda: shell

        with mock.patch.dict(sys.modules, {'IPython': ipython}):
            self.assertTrue(in_ipython_kernel())

    def test_zmq_shell_subclasses_are_detected_as_kernels(self):
        # Google Colab (google.colab._shell.Shell) and Spyder
        # (spyder_kernels.console.shell.SpyderShell) subclass
        # ZMQInteractiveShell, so an exact class-name test reported False for
        # them: detect_interactive() then fell through to the isatty() check,
        # which is False in any ZMQ kernel, and sign-in was refused outright
        # before a device code was ever requested. The bundled tests missed it
        # because they fake the shell as a name-only class with no base.
        zmq_base = type('ZMQInteractiveShell', (), {})
        for name in ('Shell', 'SpyderShell'):
            with self.subTest(shell=name):
                shell = type(name, (zmq_base,), {})()
                ipython = types.ModuleType('IPython')
                ipython.get_ipython = lambda shell=shell: shell
                with mock.patch.dict(sys.modules, {'IPython': ipython}):
                    self.assertTrue(in_ipython_kernel())
                    # ...so a ZMQ kernel is never mistaken for a bare TTY.
                    self.assertTrue(_render.detect_interactive())

    def test_live_kernel_attribute_identifies_a_kernel_shell(self):
        # The primary signal is the live kernel, which every ZMQ frontend
        # carries regardless of its class name.
        shell = type('SomeVendorShell', (), {})()
        shell.kernel = types.SimpleNamespace(_allow_stdin=True)
        ipython = types.ModuleType('IPython')
        ipython.get_ipython = lambda: shell
        with mock.patch.dict(sys.modules, {'IPython': ipython}):
            self.assertTrue(in_ipython_kernel())

    def test_interactivity_follows_stderr_not_stdout(self):
        # TerminalRenderer writes the prompt to stderr, and so does the native
        # auto-detect this overrides. Gating on stdout refused sign-in for
        # `python job.py > results.csv` at a real terminal, where the prompt
        # would have been perfectly visible.
        ipython = types.ModuleType('IPython')
        ipython.get_ipython = lambda: None

        class Stream:
            def __init__(self, tty):
                self._tty = tty

            def isatty(self):
                return self._tty

        cases = [
            # (stdout, stderr, expected)
            (False, True, True),   # redirected stdout, terminal stderr
            (True, False, False),  # redirected stderr: prompt goes nowhere
            (True, True, True),
            (False, False, False),
        ]
        for out_tty, err_tty, expected in cases:
            with self.subTest(stdout=out_tty, stderr=err_tty):
                with mock.patch.dict(sys.modules, {'IPython': ipython}), \
                        mock.patch.object(_render.sys, 'stdout', Stream(out_tty)), \
                        mock.patch.object(_render.sys, 'stderr', Stream(err_tty)):
                    self.assertEqual(_render.detect_interactive(), expected)

    def test_notebook_executor_without_stdin_is_non_interactive(self):
        # A notebook executor (papermill / nbclient / nbconvert --execute) runs a
        # real ZMQ kernel -- in_ipython_kernel() is True -- but with
        # allow_stdin=False: no human can authorize. detect_interactive() must
        # then be False so sign-in fails fast with OidcInteractionRequired instead
        # of polling to the device-code deadline (a silent CI/notebook hang).
        for allow, expected in [(False, False), (True, True), (None, True)]:
            with self.subTest(allow_stdin=allow):
                ipython = types.ModuleType('IPython')
                shell = type('ZMQInteractiveShell', (), {})()
                shell.kernel = types.SimpleNamespace(_allow_stdin=allow)
                ipython.get_ipython = lambda shell=shell: shell
                with mock.patch.dict(sys.modules, {'IPython': ipython}):
                    self.assertTrue(in_ipython_kernel())
                    self.assertEqual(
                        _render._kernel_allows_stdin(), expected)
                    self.assertEqual(_render.detect_interactive(), expected)

    def test_terminal_shell_is_treated_as_stdin_capable(self):
        # A terminal IPython shell has no `kernel` attribute; a human is at the
        # REPL, so stdin is assumed available (fail-open, not fail-fast).
        ipython = types.ModuleType('IPython')
        shell = type('TerminalInteractiveShell', (), {})()
        ipython.get_ipython = lambda: shell
        with mock.patch.dict(sys.modules, {'IPython': ipython}):
            self.assertTrue(_render._kernel_allows_stdin())

    def test_explicit_config_round_trip(self):
        auth = make_auth(
            scope='groups',
            groups_in_token=True,
            audience='questdb-api',
            issuer='https://idp.example/')
        config = auth.config
        self.assertEqual(config.client_id, 'questdb')
        self.assertEqual(config.token_endpoint, 'https://idp.example/token')
        self.assertEqual(
            config.device_authorization_endpoint,
            'https://idp.example/device')
        # Groups mode selects the ID token but preserves scope exactly, matching
        # Java's request and persisted token-store identity.
        self.assertEqual(config.scope, 'groups')
        self.assertTrue(config.groups_in_token)
        self.assertEqual(config.audience, 'questdb-api')
        self.assertEqual(config.issuer, 'https://idp.example/')

    def test_token_is_never_interactive(self):
        auth = make_auth()
        with self.assertRaises(OidcInteractionRequired):
            auth.token()
        with self.assertRaises(OidcInteractionRequired):
            auth.headers()

    def test_sign_in_is_the_interactive_entry_point(self):
        auth = make_auth()
        with self.assertRaises(OidcInteractionRequired):
            auth.sign_in()

    def test_clear_is_idempotent_without_token(self):
        auth = make_auth()
        auth.clear()
        auth.clear()

    def test_native_config_errors_are_typed(self):
        cases = [
            dict(client_id=''),
            dict(device_authorization_endpoint=''),
            dict(token_endpoint=''),
        ]
        for change in cases:
            args = dict(
                client_id='questdb',
                device_authorization_endpoint='https://idp.example/device',
                token_endpoint='https://idp.example/token')
            args.update(change)
            with self.subTest(change=change), self.assertRaises(OidcConfigError):
                OidcDeviceAuth(**args, interactive=False)

    def test_timeout_validation(self):
        for value in (
                0, -1, True, float('inf'), float('nan'), 121, 10**1000):
            with self.subTest(value=value), self.assertRaises(OidcConfigError):
                make_auth(timeout=value)

    def test_interval_overflow_is_typed(self):
        with self.assertRaises(OidcConfigError):
            make_auth(default_interval=1 << 80)

    def test_default_interval_validation(self):
        # Complements test_interval_overflow_is_typed (the > uint64-max branch):
        # cover the remaining branches of the same guard -- <= 0, bool (an int
        # subclass but never a meaningful interval), and non-int.
        for value in (0, -1, True, 1.5, 'x', None):
            with self.subTest(value=value), self.assertRaises(OidcConfigError):
                make_auth(default_interval=value)

    def test_oidc_error_propagates_in_doubt(self):
        # The OIDC error path (_oidc_err_to_py) must carry the native in-doubt
        # flag through to the raised OidcError, exactly as the non-OIDC
        # c_err_to_py path does; otherwise a retry / dead-letter handler keying
        # on QuestDBError.in_doubt could replay a possibly-delivered write. The
        # native classification does not pair an OIDC view with in_doubt today,
        # so this constructor plumbing is the reachable regression surface.
        self.assertFalse(OidcError('x').in_doubt)
        self.assertTrue(OidcError('x', in_doubt=True).in_doubt)
        # in_doubt is a QuestDBError-level property, so an ``except
        # QuestDBError`` handler observes it on an OidcError too.
        self.assertIsInstance(OidcError('x'), questdb.QuestDBError)
        # Every typed subclass forwards it: the plain ones share
        # OidcError.__init__; the device-flow ones super() into it.
        for factory in (
                lambda **k: OidcConfigError('x', **k),
                lambda **k: OidcCancelledError('x', **k),
                lambda **k: OidcNetworkError('x', **k),
                lambda **k: OidcInteractionRequired('x', **k),
                lambda **k: OidcDeviceFlowError('x', error='e', **k),
                lambda **k: OidcTimeoutError('x', error='e', **k)):
            with self.subTest(factory=factory):
                self.assertFalse(factory().in_doubt)
                self.assertTrue(factory(in_doubt=True).in_doubt)

    def test_closed_provider_is_rejected(self):
        # A provider built with __new__ but never __init__'d has _raw == NULL
        # ("closed"). Its own token-flow ops raise RuntimeError, and attaching it
        # to a transport raises ValueError -- never a native NULL deref. clear()
        # is a documented no-op on a closed provider.
        closed = OidcDeviceAuth.__new__(OidcDeviceAuth)
        for op in ('sign_in', 'token', 'headers'):
            with self.subTest(op=op), self.assertRaisesRegex(
                    RuntimeError, 'closed'):
                getattr(closed, op)()
        with self.assertRaisesRegex(RuntimeError, 'closed'):
            closed.config
        closed.clear()  # idempotent no-op on a closed provider
        with self.assertRaisesRegex(ValueError, 'closed'):
            questdb.Sender(
                questdb.Protocol.Http, '127.0.0.1', 9000,
                oidc_auth=closed, auto_flush=False)
        with self.assertRaisesRegex(ValueError, 'closed'):
            questdb.connect(
                'ws::addr=127.0.0.1:9000;lazy_connect=true;',
                oidc_auth=closed)

    def test_invalid_unicode_is_typed(self):
        with self.assertRaises(OidcConfigError):
            OidcDeviceAuth(
                '\ud800',
                'https://idp.example/device',
                'https://idp.example/token',
                interactive=False)

    def test_non_oidc_native_error_maps_to_base_oidc_error(self):
        # A field over the native 1 MiB input cap fails in the builder setter
        # with a plain (non-OIDC) native error, so questdb_error_oidc_get_view
        # returns false and the binding takes its no-view fallback: a *base*
        # OidcError (not a typed subclass) carrying no status / retry_after.
        # Covers the defensive dispatch branch for a native error without an
        # OIDC view. (Its sibling UNKNOWN-kind branch is unreachable today: every
        # native OidcErrorKind maps to a specific view kind, never UNKNOWN.)
        oversized = 'x' * (2 * 1024 * 1024)  # over MAX_OIDC_INPUT_BYTES (1 MiB)
        with self.assertRaises(OidcError) as ctx:
            OidcDeviceAuth(
                oversized,
                'https://idp.example/device',
                'https://idp.example/token',
                interactive=False)
        err = ctx.exception
        self.assertIs(type(err), OidcError)  # base class, not a typed subclass
        self.assertIsNone(err.status)
        self.assertIsNone(err.retry_after)

    def test_oidc_errors_are_questdb_errors(self):
        # OidcError subclasses QuestDBError so an existing `except QuestDBError`
        # ingestion / retry / dead-letter handler keeps catching auth failures
        # routed through c_err_to_py, while the typed subclasses stay catchable
        # specifically. Pin the hierarchy and the AuthError code directly (the
        # transport-path behaviour is covered by the attachment tests).
        self.assertTrue(issubclass(OidcError, questdb.QuestDBError))
        for exc_type in (
                OidcConfigError, OidcNetworkError, OidcInteractionRequired,
                OidcDeviceFlowError, OidcTimeoutError):
            with self.subTest(exc_type=exc_type.__name__):
                self.assertTrue(issubclass(exc_type, OidcError))
                err = exc_type('x')
                self.assertIsInstance(err, questdb.QuestDBError)
                self.assertIs(err.code, questdb.QuestDBErrorCode.AuthError)

    def test_custom_store_is_rejected(self):
        with self.assertRaisesRegex(OidcConfigError, 'FileTokenStore'):
            make_auth(token_store=object())

    def test_file_store_directory_must_be_path_like(self):
        # A falsy directory is "required"; a truthy non-path-like one must raise
        # the package's typed OidcConfigError, not a bare TypeError from
        # os.fspath escaping the contract every sibling honors.
        with self.assertRaises(OidcConfigError):
            FileTokenStore('')
        for bad in (123, ['x'], object()):
            with self.subTest(bad=bad), self.assertRaises(OidcConfigError):
                FileTokenStore(bad)

    def test_native_file_store_is_accepted(self):
        with tempfile.TemporaryDirectory() as directory:
            auth = make_auth(token_store=FileTokenStore.at(directory))
            self.assertEqual(auth.config.client_id, 'questdb')

    @unittest.skipUnless(os.name == 'posix', 'POSIX bytes paths only')
    def test_non_utf8_file_store_path_is_typed(self):
        with tempfile.TemporaryDirectory() as directory:
            path = os.fsencode(directory) + b'/tokens-\xff'
            token_store = FileTokenStore.at(path)
            with self.assertRaisesRegex(
                    OidcConfigError, 'token_store directory'):
                make_auth(token_store=token_store)

    def test_default_file_store_environment_override(self):
        with mock.patch.dict(
                os.environ,
                {'questdb.client.oidc.token.store.dir':
                 '/tmp/qdb-oidc-test'}):
            self.assertEqual(
                FileTokenStore.at_default_location().directory,
                '/tmp/qdb-oidc-test')

    def test_renderer_must_implement_interface(self):
        with self.assertRaisesRegex(OidcConfigError, 'renderer'):
            make_auth(renderer=object())

    def test_renderer_requires_all_callbacks_to_be_callable(self):
        callbacks = {
            name: lambda *args: None
            for name in (
                'on_prompt', 'on_waiting', 'on_success', 'on_failure')
        }
        for callback_name in callbacks:
            missing = callbacks.copy()
            del missing[callback_name]
            with self.subTest(callback=callback_name, value='missing'):
                with self.assertRaisesRegex(OidcConfigError, callback_name):
                    make_auth(renderer=types.SimpleNamespace(**missing))

            non_callable = callbacks.copy()
            non_callable[callback_name] = None
            with self.subTest(callback=callback_name, value='non-callable'):
                with self.assertRaisesRegex(OidcConfigError, callback_name):
                    make_auth(renderer=types.SimpleNamespace(**non_callable))

    def test_renderer_browser_target_uses_native_vetted_value(self):
        self.assertEqual(
            _verification_target({
                'verification_uri': 'https://shown.example/device',
                'verification_uri_complete': 'https://shown.example/complete',
                'browser_target': 'https://vetted.example/target',
            }),
            'https://vetted.example/target')

    def test_renderer_base_is_accepted(self):
        auth = make_auth(renderer=Renderer())
        with self.assertRaises(OidcInteractionRequired):
            auth.token()

    @unittest.skipIf(
        platform.python_implementation() == 'PyPy',
        'PyPy cpyext does not reliably collect a reference cycle that spans a '
        'C extension object with a finalizer, no matter how many gc.collect() '
        'passes run. The invariant this test checks -- the native side holds '
        'only a weakref to the provider, so nothing keeps it alive once the '
        'strong references are gone -- is still exercised on PyPy by '
        'test_sender_keeps_renderer_alive_until_close (the provider is '
        'collected after the sender drops it) and by the weakref-release leak '
        'tests below.')
    def test_renderer_provider_cycle_is_collected(self):
        renderer = Renderer()
        auth = make_auth(renderer=renderer)
        renderer.auth = auth
        renderer_ref = weakref.ref(renderer)
        auth_ref = weakref.ref(auth)

        del renderer, auth
        # On CPython the provider<->renderer cycle is reclaimed by the cyclic
        # collector -- its finalizer (OidcDeviceAuth.__dealloc__, which frees
        # the native handle) runs under PEP 442 -- precisely because the native
        # side holds only a weakref, so no C-level strong reference pins it. A
        # couple of passes suffice; the break exits as soon as it is collected.
        for _ in range(5):
            gc.collect()
            if renderer_ref() is None and auth_ref() is None:
                break

        self.assertIsNone(renderer_ref())
        self.assertIsNone(auth_ref())

    # A missing release leaks one weakref deterministically per construction,
    # so a small count cleanly separates 0 (correct) from N (leaking); native
    # build() is ~100ms, so keep N modest. Threshold well below N, above any
    # PyPy staged-collection transient.
    _LEAK_ITERS = 20

    def test_event_handler_weakref_released_on_success(self):
        # Each OidcDeviceAuth installs a native event handler that owns a
        # Py_INCREF'd weakref to the provider (oidc.pxi _finish_builder). When
        # the provider is dropped, questdb_oidc_auth_free must fire the native
        # release callback exactly once, Py_DECREF'ing that weakref. A missing
        # release leaks one weakref object per construction.
        make_auth(renderer=Renderer())  # warm one-time module state
        before = _live_weakref_count()
        for _ in range(self._LEAK_ITERS):
            make_auth(renderer=Renderer())  # constructed and immediately dropped
        after = _live_weakref_count()
        self.assertLess(
            after - before, 10,
            f'live weakref objects grew by {after - before} over '
            f'{self._LEAK_ITERS} constructions; the native event-handler '
            f'weakref is leaking')

    def test_event_handler_weakref_released_on_failed_build(self):
        # An empty client_id passes the native string setters (they only
        # validate UTF-8) but fails the native build() -- which runs AFTER the
        # event handler is installed. The Py_INCREF'd weakref is then owned by
        # the builder, and `finally: questdb_oidc_builder_free` (oidc.pxi
        # __init__) must fire the release callback so it is not leaked once per
        # failed construction. Confirms the no-auth-built ownership path.
        def construct_and_fail():
            with self.assertRaises(OidcConfigError):
                OidcDeviceAuth(
                    '',  # empty client_id: stored by the setter, rejected by build()
                    'https://idp.example/device',
                    'https://idp.example/token',
                    interactive=False, open_browser=False,
                    renderer=Renderer())
        construct_and_fail()  # warm
        before = _live_weakref_count()
        for _ in range(self._LEAK_ITERS):
            construct_and_fail()
        after = _live_weakref_count()
        self.assertLess(
            after - before, 10,
            f'live weakref objects grew by {after - before} over '
            f'{self._LEAK_ITERS} failed constructions; the native '
            f'event-handler weakref is leaking on the failed-build path')

    @unittest.skipIf(
        platform.python_implementation() == 'PyPy',
        'Exact weakref bookkeeping (getweakrefcount over a native '
        'PyWeakref_NewRef) is CPython refcount semantics; PyPy cpyext does not '
        'guarantee the same count.')
    def test_provider_holds_exactly_one_native_weakref(self):
        # Directly observe the native machinery: the event handler owns exactly
        # one weakref to the provider (event_user_data, oidc.pxi
        # _finish_builder), Py_INCREF'd and handed to the native side. Nothing
        # else references the provider weakly, so getweakrefcount sees exactly
        # that one -- the +1 the release callback must later Py_DECREF.
        auth = make_auth(renderer=Renderer())
        self.assertEqual(weakref.getweakrefcount(auth), 1)
        (native_ref,) = weakref.getweakrefs(auth)  # exactly one; unpack asserts it
        self.assertIs(native_ref(), auth)
        # A second provider gets its own independent weakref, not a shared one.
        other = make_auth(renderer=Renderer())
        self.assertEqual(weakref.getweakrefcount(other), 1)
        self.assertIsNot(weakref.getweakrefs(other)[0], native_ref)


class NativeOidcIntegrationTest(unittest.TestCase):
    def test_discovery_device_flow_and_renderer_callbacks(self):
        renderer = RecordingRenderer()
        with OidcTestServer() as server:
            auth = make_discovered_auth(server, renderer=renderer)
            self.assertEqual(auth.config.client_id, 'discovered-client')
            self.assertEqual(
                auth.config.device_authorization_endpoint,
                server.url + '/device')

            auth.sign_in()
            self.assertEqual(auth.token(), 'AT-initial')

            settings = server.requests('/settings', 'GET')
            device = server.requests('/device', 'POST')
            tokens = server.requests('/token', 'POST')

        self.assertEqual(len(settings), 1)
        self.assertEqual(device[0]['form']['client_id'], ['discovered-client'])
        self.assertEqual(device[0]['form']['scope'], [
            'openid offline_access'])
        self.assertEqual(tokens[0]['form']['device_code'], ['DEV-CODE-123'])
        self.assertEqual(renderer.prompts, [{
            'user_code': 'WXYZ-1234',
            'verification_uri': server.url + '/verify',
            'verification_uri_complete': (
                server.url + '/verify?user_code=WXYZ-1234'),
            'expires_in': 600,
            'interval': 5,
            'browser_target': (
                server.url + '/verify?user_code=WXYZ-1234'),
        }])
        self.assertEqual(len(renderer.successes), 1)
        self.assertIsNone(renderer.successes[0][0])
        self.assertGreater(renderer.successes[0][1], 0)
        self.assertEqual(renderer.failures, [])

    def test_discovery_config_view_strips_control_chars(self):
        # The native config view is not display-sanitized (only the device-flow
        # event text is), so OidcDeviceAuth.config is the sink that strips
        # control / bidi / zero-width characters a MITM'd or hostile /settings
        # could smuggle into the resolved client id. OidcConfig's repr can reach
        # a terminal, a notebook cell, or a logged traceback, so a dropped
        # _strip_control() in the config property would reintroduce an ANSI /
        # bidi injection there -- and otherwise pass every existing test.
        # ESC + BEL (C0 control), U+202E RIGHT-TO-LEFT OVERRIDE (bidi),
        # U+200B ZERO WIDTH SPACE (zero-width) -- all removed by _strip_control.
        hostile_client_id = 'disc\x1b\x07lient‮​'
        with OidcTestServer(settings_config_overrides={
                'acl.oidc.client.id': hostile_client_id,
        }) as server:
            auth = make_discovered_auth(server)
            client_id = auth.config.client_id
        self.assertEqual(client_id, 'disclient')
        for ch in ('\x1b', '\x07', '‮', '​'):
            self.assertNotIn(ch, client_id)

    def test_discovery_inherits_groups_in_token_from_server(self):
        # from_questdb defaults groups_in_token=None => inherit the server's
        # advertised acl.oidc.groups.encoded.in.token (unlike the direct
        # constructor, which defaults to False). The default fixture advertises
        # False; a server advertising True must be inherited as True.
        with OidcTestServer() as server:
            self.assertFalse(
                make_discovered_auth(server).config.groups_in_token)
        with OidcTestServer(settings_config_overrides={
                'acl.oidc.groups.encoded.in.token': True}) as server:
            self.assertTrue(
                make_discovered_auth(server).config.groups_in_token)

    def test_discovery_groups_in_token_override_beats_server(self):
        # An explicit groups_in_token wins over discovery, in both directions.
        with OidcTestServer(settings_config_overrides={
                'acl.oidc.groups.encoded.in.token': True}) as server:
            self.assertFalse(
                make_discovered_auth(
                    server, groups_in_token=False).config.groups_in_token)
        with OidcTestServer() as server:  # advertises False
            self.assertTrue(
                make_discovered_auth(
                    server, groups_in_token=True).config.groups_in_token)

    def test_discovery_explicit_kwargs_override_discovered_values(self):
        # "Explicit keyword arguments override discovery." The builder-override
        # path that runs after questdb_oidc_builder_from_questdb is otherwise
        # only ever exercised with None (skipped) across the suite; confirm a
        # non-None client_id / scope / audience wins over the server-advertised
        # /settings values (default: discovered-client / openid offline_access).
        with OidcTestServer() as server:
            config = make_discovered_auth(
                server,
                client_id='overridden-client',
                scope='openid custom-scope',
                audience='questdb-api').config
        self.assertEqual(config.client_id, 'overridden-client')
        self.assertEqual(config.scope, 'openid custom-scope')
        self.assertEqual(config.audience, 'questdb-api')

    def test_discovery_on_non_oidc_server_raises_config_error(self):
        # Pointing from_questdb at a QuestDB that does not advertise OIDC (OSS,
        # or OIDC disabled) is the #1 real-world discovery failure: the native
        # QUESTDB_OIDC_ERROR_CONFIG result must surface as OidcConfigError, and
        # only via discovery is that native config-error branch reached.
        with OidcTestServer(settings_config_overrides={
                'acl.oidc.enabled': False}) as server:
            with self.assertRaises(OidcConfigError):
                make_discovered_auth(server)

    def test_renderer_on_waiting_fires_while_authorization_pending(self):
        # The native poll loop emits WAITING between polls while the IdP replies
        # authorization_pending. The binding must map event.seconds_left ->
        # on_waiting (a struct-field mis-map would silently drop the countdown).
        # The first poll returns pending (one poll-interval wait follows), the
        # second falls through to the default success.
        renderer = RecordingRenderer()
        with OidcTestServer(device_token_responses=[
                (400, {'error': 'authorization_pending'}, None)]) as server:
            auth = make_discovered_auth(server, renderer=renderer)
            auth.sign_in()
            self.assertEqual(auth.token(), 'AT-initial')
        self.assertTrue(
            renderer.waiting,
            'on_waiting was never called during authorization_pending')
        for seconds_left in renderer.waiting:
            self.assertIsInstance(seconds_left, float)
            self.assertGreater(seconds_left, 0.0)
        # The flow still completed: SUCCESS fired, FAILURE did not.
        self.assertEqual(len(renderer.successes), 1)
        self.assertEqual(renderer.failures, [])

    def test_close_cancels_device_polling_and_is_permanent(self):
        waiting = threading.Event()

        class WaitingRenderer(RecordingRenderer):
            def on_waiting(self, seconds_left):
                super().on_waiting(seconds_left)
                waiting.set()

        renderer = WaitingRenderer()
        result = []
        pending = (400, {'error': 'authorization_pending'}, None)
        with OidcTestServer(device_token_response=pending) as server:
            auth = make_discovered_auth(server, renderer=renderer)

            def sign_in():
                try:
                    auth.sign_in()
                except BaseException as exc:
                    result.append(exc)

            worker = threading.Thread(target=sign_in, daemon=True)
            worker.start()
            self.assertTrue(
                waiting.wait(5), 'sign-in did not enter its polling wait')
            started = time.monotonic()
            auth.close()
            self.assertLess(time.monotonic() - started, 2)
            worker.join(2)
            self.assertFalse(worker.is_alive())

        self.assertEqual(len(result), 1)
        self.assertIsInstance(result[0], OidcCancelledError)
        for op in (auth.sign_in, auth.token, auth.headers, auth.clear):
            with self.subTest(op=op), self.assertRaisesRegex(
                    OidcCancelledError, 'closed'):
                op()
        auth.close()
        with self.assertRaisesRegex(OidcCancelledError, 'closed'):
            with auth:
                pass

    def test_keyboard_interrupt_in_renderer_aborts_sign_in(self):
        # sign_in() releases the GIL for the whole native device flow, so a
        # renderer callback is the only place CPython can deliver a pending
        # SIGINT on the caller's thread. `except BaseException` used to swallow
        # it: Ctrl-C printed an "OIDC renderer callback failed" traceback and
        # sign_in() kept polling to the device-code deadline, eating every
        # later Ctrl-C the same way.
        class InterruptingRenderer(RecordingRenderer):
            def on_waiting(self, seconds_left):
                super().on_waiting(seconds_left)
                raise KeyboardInterrupt

        pending = (400, {'error': 'authorization_pending'}, None)
        with OidcTestServer(device_token_response=pending) as server:
            auth = make_discovered_auth(
                server, renderer=InterruptingRenderer())
            started = time.monotonic()
            with self.assertRaises(KeyboardInterrupt):
                auth.sign_in()
            # Promptly, not after the device code expires.
            self.assertLess(time.monotonic() - started, 10)
        # The interrupt cancels the flow, which closes the provider.
        with self.assertRaisesRegex(OidcCancelledError, 'closed'):
            auth.token()

    def test_renderer_can_close_the_provider_from_its_callback(self):
        # close() is the only cancellation lever a renderer has -- a notebook
        # "Cancel" button in on_waiting has nothing else to call. The native
        # callback-reentry guard used to reject it, so the affordance could not
        # be built at all. Native now publishes the close lock-free and skips
        # only the drain, so this must succeed.
        outcome = []
        holder = []

        class CancellingRenderer(RecordingRenderer):
            def on_waiting(self, seconds_left):
                super().on_waiting(seconds_left)
                try:
                    holder[0].close()
                    outcome.append('closed')
                except BaseException as exc:  # noqa: BLE001
                    outcome.append(exc)

        pending = (400, {'error': 'authorization_pending'}, None)
        with OidcTestServer(device_token_response=pending) as server:
            auth = make_discovered_auth(server, renderer=CancellingRenderer())
            holder.append(auth)
            started = time.monotonic()
            with self.assertRaises(OidcCancelledError):
                auth.sign_in()
            self.assertLess(time.monotonic() - started, 10)
        self.assertEqual(outcome[:1], ['closed'])

    def test_renderer_on_failure_receives_native_message(self):
        # A terminal device-flow error emits FAILURE. The binding must map
        # event.message -> on_failure; event.identity is NULL for FAILURE, so a
        # mis-map to it would surface the 'OIDC sign-in failed.' fallback instead
        # of the real message. Assert the sanitized IdP-derived text reaches it.
        renderer = RecordingRenderer()
        with OidcTestServer(device_token_response=(400, {
                'error': 'access_denied',
                'error_description': 'The user denied the request.'},
                None)) as server:
            auth = make_discovered_auth(server, renderer=renderer)
            with self.assertRaises(OidcDeviceFlowError):
                auth.sign_in()
        self.assertEqual(len(renderer.failures), 1)
        message = renderer.failures[0]
        self.assertIsInstance(message, str)
        self.assertIn('The user denied the request.', message)
        # SUCCESS must not fire on the failure path.
        self.assertEqual(renderer.successes, [])

    def test_renderer_callback_that_raises_is_logged_not_fatal(self):
        # A buggy user renderer that raises inside a callback must not abort an
        # otherwise-successful sign-in. The native event trampoline is
        # `noexcept nogil`, so an exception leaking out of dispatch would be
        # crash-adjacent; the binding's dispatch guard swallows it and logs to
        # the 'questdb' logger instead.
        class RaisingRenderer(Renderer):
            def on_prompt(self, response):
                raise RuntimeError('boom-prompt')

            def on_waiting(self, seconds_left):
                raise RuntimeError('boom-waiting')

            def on_success(self, identity, expires_in):
                raise RuntimeError('boom-success')

            def on_failure(self, message):
                raise RuntimeError('boom-failure')

        with OidcTestServer() as server:
            auth = make_discovered_auth(server, renderer=RaisingRenderer())
            with self.assertLogs('questdb', level='ERROR') as logs:
                auth.sign_in()
                self.assertEqual(auth.token(), 'AT-initial')
        self.assertTrue(
            any('OIDC renderer callback failed' in line
                for line in logs.output),
            logs.output)

    def test_expired_token_is_refreshed_without_device_flow(self):
        with OidcTestServer(
                initial_access_token=EXPIRED_ACCESS_TOKEN) as server:
            auth = make_discovered_auth(server)
            auth.sign_in()
            self.assertEqual(auth.token(), 'AT-refreshed')
            self.assertEqual(
                auth.headers(), {'Authorization': 'Bearer AT-refreshed'})
            token_requests = server.requests('/token', 'POST')
            device_requests = server.requests('/device', 'POST')

        self.assertEqual(len(device_requests), 1)
        self.assertEqual(len(token_requests), 2)
        self.assertEqual(
            token_requests[1]['form']['grant_type'], ['refresh_token'])
        self.assertEqual(
            token_requests[1]['form']['refresh_token'], ['RT-1'])

    def test_terminal_renderer_receives_native_callbacks(self):
        output = io.StringIO()
        with OidcTestServer() as server:
            auth = make_discovered_auth(
                server, renderer=TerminalRenderer(stream=output))
            auth.sign_in()

        rendered = output.getvalue()
        self.assertIn(server.url + '/verify', rendered)
        self.assertIn('WXYZ-1234', rendered)
        self.assertIn('Signed in', rendered)

    def test_file_store_round_trip_avoids_second_device_flow(self):
        with tempfile.TemporaryDirectory() as directory:
            with OidcTestServer() as server:
                auth = make_discovered_auth(
                    server, token_store=FileTokenStore.at(directory))
                auth.sign_in()
                self.assertEqual(auth.token(), 'AT-initial')

                restored = make_discovered_auth(
                    server, token_store=FileTokenStore.at(directory))
                self.assertEqual(restored.token(), 'AT-initial')
                self.assertEqual(len(server.requests('/device', 'POST')), 1)
                self.assertEqual(len(server.requests('/token', 'POST')), 1)
                restored.clear()

    def test_clear_removes_persisted_token(self):
        # clear() must delete the persisted store entry, not only the in-memory
        # copy: a fresh provider over the same store then finds nothing and
        # re-runs the device flow (a SECOND /device request). If clear() left
        # the file, the fresh provider would load it and /device stays at 1.
        with tempfile.TemporaryDirectory() as directory:
            with OidcTestServer() as server:
                auth = make_discovered_auth(
                    server, token_store=FileTokenStore.at(directory))
                auth.sign_in()
                self.assertEqual(auth.token(), 'AT-initial')
                self.assertEqual(len(server.requests('/device', 'POST')), 1)

                auth.clear()

                fresh = make_discovered_auth(
                    server, token_store=FileTokenStore.at(directory))
                fresh.sign_in()
                self.assertEqual(fresh.token(), 'AT-initial')
                self.assertEqual(len(server.requests('/device', 'POST')), 2)

    def test_http_sender_authenticates_retry_and_flush(self):
        with OidcTestServer(write_statuses=(500, 204)) as server:
            auth = make_discovered_auth(server)
            auth.sign_in()
            with questdb.Sender(
                    questdb.Protocol.Http,
                    '127.0.0.1',
                    server.port,
                    oidc_auth=auth,
                    auto_flush=False,
                    retry_timeout=1000) as sender:
                sender.row(
                    'events', columns={'value': 42},
                    at=questdb.ServerTimestamp)
                sender.flush()
            writes = server.requests('/write', 'POST')

        self.assertEqual(len(writes), 2)
        self.assertEqual(writes[0]['body'], writes[1]['body'])
        self.assertEqual(
            [request['headers'].get('authorization') for request in writes],
            ['Bearer AT-initial', 'Bearer AT-initial'])

    def test_qwp_sender_authenticates_establish_reconnect_and_flush(self):
        with OidcTestServer() as oidc_server:
            auth = make_discovered_auth(oidc_server)
            auth.sign_in()
            with QwpAckServer(
                    close_plan=(0, None),
                    required_authorization='Bearer AT-initial') as qwp_server:
                conf = (
                    f'ws::addr=127.0.0.1:{qwp_server.port};'
                    'lazy_connect=true;'
                    'reconnect_initial_backoff_millis=1;'
                    'reconnect_max_backoff_millis=1;'
                    'reconnect_max_duration_millis=5000;'
                    'close_flush_timeout_millis=5000;')
                sender = questdb.Sender.from_conf(
                    conf, oidc_auth=auth, auto_flush=False)
                try:
                    sender.establish()
                    sender.row(
                        'events', columns={'value': 42},
                        at=questdb.ServerTimestamp)
                    fsn = sender.flush_and_get_fsn()
                    self.assertTrue(sender.await_acked_fsn(fsn, 5000))
                finally:
                    sender.close(flush=False)
                stats = qwp_server.snapshot()

        self.assertGreaterEqual(stats['accepted_connections'], 2)
        self.assertEqual(stats['binary_frames'], 1)
        self.assertGreaterEqual(len(stats['upgrade_authorizations']), 2)
        self.assertTrue(all(
            value == 'Bearer AT-initial'
            for value in stats['upgrade_authorizations']))
        self.assertEqual(stats['errors'], [])

    def test_qwp_pool_authenticates_and_flushes(self):
        # The pool opens its QWP connection through questdb_db_connect_ex -- a
        # different native path than the standalone Sender's
        # line_sender_opts_oidc_auth -- so exercise the Bearer token over the
        # pool too (previously only system-tested). required_authorization makes
        # the mock reject (401) any un-authenticated upgrade, so a committed
        # frame proves the pool authenticated.
        with OidcTestServer() as oidc_server:
            auth = make_discovered_auth(oidc_server)
            auth.sign_in()
            with QwpAckServer(
                    required_authorization='Bearer AT-initial') as qwp_server:
                conf = (
                    f'ws::addr=127.0.0.1:{qwp_server.port};'
                    'lazy_connect=true;sender_pool_min=1;pool_reap=manual;')
                with questdb.connect(conf, oidc_auth=auth) as db:
                    with db.sender() as sender:
                        sender.row(
                            'events', columns={'value': 42},
                            at=questdb.ServerTimestamp)
                stats = qwp_server.snapshot()

        self.assertEqual(stats['binary_frames'], 1)
        self.assertGreaterEqual(len(stats['upgrade_authorizations']), 1)
        self.assertTrue(all(
            value == 'Bearer AT-initial'
            for value in stats['upgrade_authorizations']))
        self.assertEqual(stats['errors'], [])

    def test_sqlalchemy_listener_uses_native_refreshed_token(self):
        with OidcTestServer(
                initial_access_token=EXPIRED_ACCESS_TOKEN) as server:
            auth = make_discovered_auth(server)
            auth.sign_in()
            engine = types.SimpleNamespace(listeners={})
            sqlalchemy = types.ModuleType('sqlalchemy')
            sqlalchemy.create_engine = mock.Mock(return_value=engine)

            class Event:
                @staticmethod
                def listens_for(target, name):
                    def register(listener):
                        target.listeners[name] = listener
                        return listener
                    return register

            class URL:
                create = mock.Mock(return_value='postgresql-url')

            sqlalchemy.event = Event
            sqlalchemy_engine_module = types.ModuleType('sqlalchemy.engine')
            sqlalchemy_engine_module.URL = URL
            modules = {
                'sqlalchemy': sqlalchemy,
                'sqlalchemy.engine': sqlalchemy_engine_module,
            }
            with mock.patch.dict(sys.modules, modules):
                returned = _adapters.sqlalchemy_engine(
                    auth,
                    server.url,
                    drivername='postgresql+test')
            params = {}
            returned.listeners['do_connect'](None, None, [], params)
            token_requests = server.requests('/token', 'POST')

        self.assertIs(returned, engine)
        self.assertEqual(params['password'], 'AT-refreshed')
        self.assertEqual(len(token_requests), 2)
        self.assertEqual(
            token_requests[1]['form']['grant_type'], ['refresh_token'])

    def test_device_flow_error_maps_to_typed_error_with_idp_fields(self):
        # A terminal OAuth error at the token endpoint during polling maps to
        # OidcDeviceFlowError with the IdP error / description / HTTP status
        # populated from the native error view (line_sender.pxd mirroring
        # oidc.h). A struct-field mis-map would drop these silently.
        with OidcTestServer(device_token_response=(400, {
                'error': 'access_denied',
                'error_description': 'The user denied the request.'}, None)) as server:
            auth = make_discovered_auth(server)
            with self.assertRaises(OidcDeviceFlowError) as ctx:
                auth.sign_in()
        err = ctx.exception
        self.assertEqual(err.error, 'access_denied')
        self.assertEqual(err.error_description, 'The user denied the request.')
        self.assertEqual(err.status, 400)

    def test_expired_device_code_maps_to_timeout_with_idp_error(self):
        # `expired_token` maps to OidcTimeoutError (a OidcDeviceFlowError), and
        # the native-attached IdP error must be carried through, not dropped.
        with OidcTestServer(device_token_response=(
                400, {'error': 'expired_token'}, None)) as server:
            auth = make_discovered_auth(server)
            with self.assertRaises(OidcTimeoutError) as ctx:
                auth.sign_in()
        self.assertIsInstance(ctx.exception, OidcDeviceFlowError)
        self.assertEqual(ctx.exception.error, 'expired_token')

    def test_transient_refresh_error_maps_to_network_with_retry_after(self):
        # A transient status on the refresh call maps to OidcNetworkError with
        # HTTP status and parsed Retry-After preserved (the poll/refresh loop
        # uses these to schedule a retry). Exercises has_status / has_retry_after
        # + uint16_t status / uint64_t retry_after_seconds in the error view.
        with OidcTestServer(
                initial_access_token=EXPIRED_ACCESS_TOKEN,
                refresh_token_response=(
                    429, {'error': 'slow_down'}, {'Retry-After': '7'})) as server:
            auth = make_discovered_auth(server)
            auth.sign_in()  # obtains the (expired) access token + refresh token
            with self.assertRaises(OidcNetworkError) as ctx:
                auth.token()  # triggers the refresh, which hits the 429
        self.assertEqual(ctx.exception.status, 429)
        self.assertEqual(ctx.exception.retry_after, 7)

    def test_full_lifecycle_loop_does_not_leak_weakrefs(self):
        # Construct -> sign_in -> token -> drop, repeatedly, against the mock
        # IdP. Exercises the native token acquisition/free path
        # (questdb_oidc_token_free) and the event-handler weakref release across
        # the FULL lifecycle, not just bare construction; a leaked
        # event_user_data weakref would accumulate.
        with OidcTestServer() as server:
            make_discovered_auth(server).sign_in()  # warm
            before = _live_weakref_count()
            for _ in range(12):
                auth = make_discovered_auth(server)
                auth.sign_in()
                self.assertEqual(auth.token(), 'AT-initial')
                del auth
            after = _live_weakref_count()
        self.assertLess(
            after - before, 10,
            f'live weakref objects grew by {after - before} over 12 '
            f'sign-in/token lifecycles; a native weakref is leaking')


class NativeTransportAttachmentTest(unittest.TestCase):
    def setUp(self):
        self.auth = make_auth()

    def _assert_retains_and_releases(self, build):
        # build(provider) -> (transport, close). The transport must pin the
        # provider while open -- proving oidc_auth was actually attached, not
        # silently ignored -- and release it on close.
        provider = make_auth()
        ref = weakref.ref(provider)
        transport, close = build(provider)
        del provider
        gc.collect()
        self.assertIsNotNone(
            ref(), 'the transport must retain the OIDC provider while open')
        close()
        for _ in range(4):
            gc.collect()
            if ref() is None:
                break
        self.assertIsNone(
            ref(), 'closing the transport must release the OIDC provider')

    def test_sender_accepts_shared_provider(self):
        def build(provider):
            sender = questdb.Sender(
                questdb.Protocol.Ws, 'localhost', 9000,
                oidc_auth=provider, auto_flush=False)
            return sender, lambda: sender.close(flush=False)
        self._assert_retains_and_releases(build)

    def test_sender_keeps_renderer_alive_until_close(self):
        renderer = Renderer()
        renderer_ref = weakref.ref(renderer)
        auth = make_auth(renderer=renderer)
        sender = questdb.Sender(
            questdb.Protocol.Ws,
            'localhost',
            9000,
            oidc_auth=auth,
            auto_flush=False)
        del renderer, auth
        gc.collect()
        self.assertIsNotNone(renderer_ref())
        sender.close(flush=False)
        # PyPy's tracing GC may need another collection after finalizing the
        # provider and releasing its renderer reference.
        for _ in range(3):
            gc.collect()
            if renderer_ref() is None:
                break
        self.assertIsNone(renderer_ref())

    def test_sender_from_conf_accepts_shared_provider(self):
        def build(provider):
            sender = questdb.Sender.from_conf(
                'https::addr=localhost:9000;', oidc_auth=provider,
                auto_flush=False)
            return sender, lambda: sender.close(flush=False)
        self._assert_retains_and_releases(build)

    def test_sender_rejects_fixed_and_rotating_token(self):
        with self.assertRaises(questdb.QuestDBError):
            questdb.Sender(
                questdb.Protocol.Http,
                'localhost',
                9000,
                token='fixed',
                oidc_auth=self.auth)

    def test_sender_from_conf_rejects_fixed_and_rotating_token(self):
        # The conf-string token (params.get('token')) hits the same mutual
        # exclusion as the direct token= kwarg.
        with self.assertRaises(questdb.QuestDBError):
            questdb.Sender.from_conf(
                'http::addr=localhost:9000;token=fixed;', oidc_auth=self.auth)

    def test_pool_rejects_fixed_and_rotating_token(self):
        # The pool's connect_ex path enforces the same exclusion.
        with self.assertRaises(questdb.QuestDBError):
            questdb.connect(
                'ws::addr=localhost:9000;lazy_connect=true;token=fixed;',
                oidc_auth=self.auth)

    def test_sender_rejects_wrong_auth_type(self):
        with self.assertRaisesRegex(TypeError, 'OidcDeviceAuth'):
            questdb.Sender(
                questdb.Protocol.Http,
                'localhost',
                9000,
                oidc_auth=object())

    def test_lazy_pool_accepts_shared_provider(self):
        def build(provider):
            db = questdb.connect(
                'ws::addr=localhost:9000;lazy_connect=true;',
                oidc_auth=provider)
            return db, db.close
        self._assert_retains_and_releases(build)

    def test_pool_rejects_wrong_auth_type(self):
        with self.assertRaisesRegex(TypeError, 'OidcDeviceAuth'):
            questdb.connect(
                'ws::addr=localhost:9000;lazy_connect=true;',
                oidc_auth=object())

    @unittest.skipIf(pd is None, 'pandas not installed')
    def test_dataframe_auto_flush_preserves_oidc_error(self):
        auth = make_auth()
        with questdb.Sender(
                questdb.Protocol.Http,
                '127.0.0.1',
                9000,
                oidc_auth=auth,
                auto_flush_rows=1,
                auto_flush_bytes=False,
                auto_flush_interval=False,
                protocol_version=2) as sender:
            with self.assertRaises(OidcInteractionRequired):
                sender.dataframe(
                    pd.DataFrame({'value': [1]}),
                    table_name='oidc_auto_flush',
                    at=questdb.ServerTimestamp)

    def test_flush_surfaces_typed_oidc_error(self):
        # The plain row()/flush() path (not the dataframe path) whose OIDC token
        # pull fails at connect surfaces the typed OidcError through c_err_to_py.
        # OidcError subclasses QuestDBError, so an existing `except QuestDBError`
        # flush handler still catches it while `except OidcInteractionRequired`
        # can react specifically. Complements
        # test_dataframe_auto_flush_preserves_oidc_error (the dataframe path).
        auth = make_auth()  # never signed in
        with questdb.Sender(
                questdb.Protocol.Http,
                '127.0.0.1',
                9000,
                oidc_auth=auth,
                auto_flush=False,
                protocol_version=2) as sender:
            sender.row(
                'oidc_flush', columns={'value': 1},
                at=questdb.ServerTimestamp)
            with self.assertRaises(OidcInteractionRequired) as ctx:
                sender.flush()
            self.assertIsInstance(ctx.exception, questdb.QuestDBError)

    def test_query_and_connect_surface_typed_oidc_error(self):
        # The read/connect side, like flush/dataframe above: an unsigned
        # provider's token pull fails at reader-borrow / pool-connect and
        # surfaces the typed OidcError. It subclasses QuestDBError, so an
        # existing `except QuestDBError` handler still catches it while the typed
        # OidcInteractionRequired stays available. Deterministic for the same
        # reason as the flush test -- the native provider pulls the (absent)
        # token before the socket, so the result is OidcInteractionRequired
        # regardless of whether 127.0.0.1:9000 is up. The borrow-time failure is
        # typed on every output path; __arrow_c_stream__ needs no optional
        # dependency, so use it as the primary assertion. (The zero-copy stream's
        # untyped-OSError limitation applies only to a refresh that fails
        # mid-stream, after iteration has begun -- not unit-triggerable.)
        with questdb.connect(
                'ws::addr=127.0.0.1:9000;lazy_connect=true;',
                oidc_auth=make_auth()) as client:
            with self.assertRaises(OidcInteractionRequired) as ctx:
                client.query('select 1').__arrow_c_stream__()
            self.assertIsInstance(ctx.exception, questdb.QuestDBError)
            if pd is not None:
                with self.assertRaises(OidcInteractionRequired):
                    client.query('select 1').to_pandas()
        # Non-lazy connect surfaces the same typed error at pool-open time.
        with self.assertRaises(OidcInteractionRequired) as ctx:
            questdb.connect('ws::addr=127.0.0.1:9000;', oidc_auth=make_auth())
        self.assertIsInstance(ctx.exception, questdb.QuestDBError)

    def test_sender_from_env_accepts_shared_provider(self):
        def build(provider):
            with mock.patch.dict(
                    os.environ,
                    {'QDB_CLIENT_CONF': 'https::addr=localhost:9000;'}):
                sender = questdb.Sender.from_env(
                    oidc_auth=provider, auto_flush=False)
            return sender, lambda: sender.close(flush=False)
        self._assert_retains_and_releases(build)


class RenderSanitizerTest(unittest.TestCase):
    """Adversarial tests for the anti-phishing sanitizers in ``_render``.

    The device-authorization fields (verification_uri, user_code, IdP error
    strings) are untrusted and MITM-tamperable; these pure-Python sanitizers
    are the only defense between them and a terminal / notebook DOM. Each
    documented protection gets a direct test so a regression (or a future
    refactor that quietly weakens one) fails loudly. No compiled extension is
    exercised here -- only ``questdb.auth._render``.
    """

    # -- _strip_control: strip control / bidi / zero-width / format chars --
    def test_strip_control_removes_bidi_override(self):
        # U+202E RIGHT-TO-LEFT OVERRIDE can visually reverse a host/URL.
        self.assertEqual(_render._strip_control('ab‮cd'), 'abcd')

    def test_strip_control_removes_zero_width(self):
        # ZWSP / ZWNJ / BOM render as nothing and can hide/segment text.
        self.assertEqual(_render._strip_control('a​b‌c﻿d'), 'abcd')

    def test_strip_control_removes_ansi_escape(self):
        # A raw ESC could spoof a terminal prompt via ANSI control sequences.
        self.assertEqual(_render._strip_control('\x1b[31mX'), '[31mX')

    def test_strip_control_folds_exotic_spaces_to_ascii_space(self):
        # NBSP / ideographic space are invisible-as-space (hide trailing text);
        # fold them, but keep the ordinary ASCII space of a legitimate identity.
        self.assertEqual(_render._strip_control('a b　c'), 'a b c')
        self.assertEqual(_render._strip_control('a b'), 'a b')

    def test_format_prompt_missing_user_code_renders_empty(self):
        # oidc.pxi passes user_code=None (key present, value None) when the
        # native pointer is NULL. The plain-text prompt must then render an
        # empty code, not the literal 'None' -- a `.get('user_code', '')`
        # (rather than `... or ''`) regression would surface 'None' to the user.
        for resp in (
                {'user_code': None,
                 'verification_uri': 'https://idp.example/verify'},
                {'verification_uri': 'https://idp.example/verify'}):
            prompt = _render.format_prompt(resp)
            self.assertNotIn('None', prompt)
            self.assertTrue(prompt.rstrip().endswith('enter code:'))

    def test_strip_control_removes_variation_selectors_and_enclosing_marks(self):
        self.assertEqual(_render._strip_control('a️b'), 'ab')   # VS16
        self.assertEqual(_render._strip_control('a⃠b'), 'ab')   # enclosing mark

    def test_strip_control_caps_combining_run(self):
        # A long "Zalgo" combining stack smears across prompt lines; cap it,
        # while keeping a couple of legitimate accents.
        self.assertEqual(_render._strip_control('a' + '́' * 20),
                         'a' + '́' * 4)
        self.assertEqual(_render._strip_control('é'), 'é')  # é survives

    def test_strip_control_cap_not_reset_by_interleaved_zero_width(self):
        # Interleaving stripped chars must not reset the combining counter.
        text = 'a' + ''.join('́​' for _ in range(20))
        self.assertEqual(_render._strip_control(text), 'a' + '́' * 4)

    def test_strip_control_coerces_non_str_and_none(self):
        # Total by design: a hostile IdP could put a JSON number/object in an
        # error field; it must be coerced, not raise (typed-error contract).
        self.assertEqual(_render._strip_control(123), '123')
        self.assertEqual(_render._strip_control(None), '')

    def test_strip_control_removes_braille_blank(self):
        # U+2800 BRAILLE PATTERN BLANK renders as a blank, cell-width glyph
        # (category So, so the category rule and the Zs space-fold both miss it)
        # and can pad or hide trailing text in a user_code / identity / error.
        self.assertEqual(
            _render._strip_control('ab' + chr(0x2800) + 'cd'), 'abcd')

    # -- _safe_link_url / _safe_target: what may be linkified / opened / QR'd --
    def test_safe_link_url_rejects_dangerous_schemes(self):
        self.assertIsNone(_render._safe_link_url('javascript:alert(1)'))
        self.assertIsNone(_render._safe_link_url('data:text/html,<script>'))
        self.assertIsNone(_render._safe_link_url('file:///etc/passwd'))

    def test_safe_link_url_rejects_userinfo(self):
        # https://trusted@evil connects to evil while reading as trusted.
        self.assertIsNone(
            _render._safe_link_url('https://login.questdb.io@evil.example/'))

    def test_safe_link_url_rejects_nonascii_host(self):
        self.assertIsNone(_render._safe_link_url('https://аpple.com/'))

    def test_safe_link_url_rejects_bad_port_and_embedded_controls(self):
        self.assertIsNone(_render._safe_link_url('https://host:70000/'))
        self.assertIsNone(_render._safe_link_url('https://host\t/'))
        self.assertIsNone(_render._safe_link_url('https://ho\nst/'))

    def test_safe_link_url_accepts_plain_https_and_punycode(self):
        self.assertEqual(_render._safe_link_url('https://ok.example/verify'),
                         'https://ok.example/verify')
        self.assertEqual(_render._safe_link_url('https://xn--e1afmkfd.example/'),
                         'https://xn--e1afmkfd.example/')

    def test_safe_target_strips_control_before_vetting(self):
        # One value feeds the href, webbrowser.open() and the QR, so a control
        # char stripped from the shown link can't survive into the real target.
        self.assertEqual(
            _render._safe_target('https://ok.example/​verify'),
            'https://ok.example/verify')
        self.assertIsNone(_render._safe_target('javascript:​alert(1)'))

    # -- _display_url: what is shown as text (homograph defense) --
    def test_display_url_folds_fullwidth_dot_to_real_domain(self):
        # U+FF0E folds to '.', exposing the true registrable domain (evil.com).
        self.assertEqual(_render._display_url('https://exa．mple.com/x'),
                         'https://exa.mple.com/x')

    def test_display_url_shows_homograph_host_as_punycode(self):
        # Cyrillic look-alike host is shown IDNA/punycode, never raw.
        self.assertEqual(_render._display_url('https://аpple.com/x'),
                         'https://xn--pple-43d.com/x')

    def test_display_url_drops_userinfo(self):
        self.assertEqual(_render._display_url('https://trusted@evil.example/x'),
                         'https://evil.example/x')

    def test_display_url_escapes_delimiter_folding_confusable(self):
        # U+FF0F fullwidth solidus is shown \u-escaped, never as a bare '/'.
        shown = _render._display_url('https://exa／mple/x')
        self.assertNotIn('／', shown)
        self.assertIn('\\uff0f', shown)

    def test_display_url_escapes_idna_minted_backslash(self):
        # U+FF3C fullwidth reverse solidus PASSES urlparse (whose NFKC
        # delimiter-reject covers '/ @ :' but not '\') yet nameprep folds it to
        # a literal '\', which a WHATWG/browser parser treats as '/', ending the
        # host early. This exercises the post-IDNA '\\/@?#' guard -- the distinct
        # branch the U+FF0F test above does NOT reach (that one makes urlparse
        # raise before IDNA). The confusable must be shown \u-escaped, never as
        # a bare '\'.
        shown = _render._display_url('https://exa＼mple.com/x')
        self.assertNotIn('＼', shown)   # raw fullwidth char gone
        self.assertIn('\\uff3c', shown)     # shown as its visible escape

    def test_display_url_preserves_plain_host_and_port(self):
        self.assertEqual(_render._display_url('https://ok.example:9000/x'),
                         'https://ok.example:9000/x')

    # -- _matched_complete / _same_origin: origin-match the complete URL --
    def test_matched_complete_accepts_same_origin(self):
        self.assertEqual(
            _render._matched_complete({
                'verification_uri': 'https://idp.example/device',
                'verification_uri_complete': 'https://idp.example/device?code=X'}),
            'https://idp.example/device?code=X')

    def test_matched_complete_drops_different_host(self):
        # A trusted-looking uri paired with a complete on a DIFFERENT host would
        # steer the auto-open/QR/click to the attacker -> treat as absent.
        self.assertIsNone(
            _render._matched_complete({
                'verification_uri': 'https://idp.example/device',
                'verification_uri_complete': 'https://evil.example/device?code=X'}))

    def test_matched_complete_drops_different_port(self):
        self.assertIsNone(
            _render._matched_complete({
                'verification_uri': 'https://idp.example:9000/device',
                'verification_uri_complete':
                    'https://idp.example:9001/device?code=X'}))

    def test_matched_complete_accepts_explicit_default_port(self):
        # verification_uri omits the port while complete spells out :443 — the
        # scheme default is normalized, so this is the same origin and the
        # pre-filled URL is kept rather than dropped as a spoof.
        self.assertEqual(
            _render._matched_complete({
                'verification_uri': 'https://idp.example/device',
                'verification_uri_complete':
                    'https://idp.example:443/device?code=X'}),
            'https://idp.example:443/device?code=X')

    def test_same_origin(self):
        self.assertTrue(_render._same_origin(
            'https://a.example:9000/x', 'https://a.example:9000/y'))
        self.assertFalse(_render._same_origin(
            'https://a.example:9000/x', 'https://a.example:9001/y'))
        self.assertFalse(_render._same_origin(
            'https://a.example/x', 'http://a.example/y'))
        # An explicit default port equals an implicit one (443 https / 80 http),
        # so a legitimate complete URL that writes the port still matches.
        self.assertTrue(_render._same_origin(
            'https://a.example/x', 'https://a.example:443/y'))
        self.assertTrue(_render._same_origin(
            'http://a.example:80/x', 'http://a.example/y'))
        # A non-default explicit port still differs from an implicit one.
        self.assertFalse(_render._same_origin(
            'https://a.example/x', 'https://a.example:8443/y'))

    # -- _render_link: linkify only vetted URLs, escape the rest --
    def test_render_link_linkifies_safe_url(self):
        html = _render._render_link('https://ok.example/verify')
        self.assertIn('<a href="https://ok.example/verify"', html)
        self.assertIn('rel="noopener noreferrer"', html)

    def test_render_link_shows_rejected_url_as_inert_escaped_text(self):
        html = _render._render_link('javascript:"><img src=x onerror=alert(1)>')
        self.assertNotIn('<a ', html)    # never linkified
        self.assertNotIn('<img', html)   # markup is html-escaped, not live

    # -- _verification_target: the single canonical actionable URL --
    def test_verification_target_prefers_native_browser_target(self):
        self.assertEqual(
            _render._verification_target({
                'verification_uri': 'https://shown.example/v',
                'verification_uri_complete': 'https://shown.example/c',
                'browser_target': 'https://vetted.example/t'}),
            'https://vetted.example/t')

    def test_verification_target_drops_diverging_complete(self):
        # No native browser_target; a complete on a different host is not used.
        self.assertEqual(
            _render._verification_target({
                'verification_uri': 'https://idp.example/v',
                'verification_uri_complete': 'https://evil.example/c'}),
            'https://idp.example/v')

    def test_jupyter_renderer_sanitizes_and_escapes_untrusted_fields(self):
        # The "Jupyter-first" renderer writes untrusted, MITM-tamperable IdP
        # fields (user_code, verification_uri, JWT identity, error message) into
        # the notebook DOM. The per-field helpers are tested above; this drives
        # JupyterRenderer end-to-end with a fake IPython.display and pins that
        # it actually applies them -- stripping control/bidi/zero-width chars,
        # html-escaping injected markup, and never linkifying a dangerous scheme.
        captured = []

        class _FakeHTML:
            def __init__(self, data):
                self.data = data

        class _FakeHandle:
            def update(self, obj):
                captured.append(obj.data)

        def _fake_display(obj, display_id=None):
            captured.append(obj.data)
            return _FakeHandle()

        ipython = types.ModuleType('IPython')
        display_mod = types.ModuleType('IPython.display')
        display_mod.HTML = _FakeHTML
        display_mod.display = _fake_display
        ipython.display = display_mod
        with mock.patch.dict(
                sys.modules,
                {'IPython': ipython, 'IPython.display': display_mod}):
            renderer = _render.JupyterRenderer(qr=False)
            renderer.on_prompt({
                'user_code': 'AB​CD',                    # zero-width space
                'verification_uri': 'javascript:alert(1)'})   # dangerous scheme
            renderer.on_success('ev<script>il‮', 600)    # markup + bidi
            renderer.on_failure('boom <img src=x> ​')    # markup + zero-width
        html = '\n'.join(captured)
        self.assertTrue(captured, 'renderer emitted nothing')
        self.assertNotIn('​', html)              # zero-width stripped
        self.assertNotIn('‮', html)              # bidi override stripped
        self.assertIn('&lt;script&gt;', html)         # identity markup escaped
        self.assertNotIn('<script>', html)
        self.assertIn('&lt;img', html)                # message markup escaped
        self.assertNotIn('href="javascript:', html)   # dangerous scheme inert


class AdapterTest(unittest.TestCase):
    unsafe_urls = (
        'https://trusted.questdb.com@evil.example:9000',
        'https://trusted.questdb.com:secret@evil.example:9000',
        'file://evil.example',
    )

    def test_psycopg_uses_noninteractive_token(self):
        auth = mock.Mock()
        auth.token.return_value = 'TOKEN'
        driver = mock.Mock()
        with mock.patch.object(_adapters, '_pg_module', return_value=driver):
            _adapters.psycopg_connect(
                auth, 'https://questdb.example.com:9000')
        auth.token.assert_called_once_with()
        driver.connect.assert_called_once_with(
            host='questdb.example.com',
            port=8812,
            dbname='qdb',
            user='_sso',
            password='TOKEN')

    def test_bad_adapter_url_is_typed(self):
        with self.assertRaises(OidcConfigError):
            _adapters._require_host('https://host:invalid')

    def test_coerce_port_rejects_invalid(self):
        # bool (True/False is never a port), non-integral / non-finite float,
        # non-numeric, and out-of-range must all raise the typed error rather
        # than reach the driver as a bare ValueError / silently truncate.
        for bad in (True, False, 8812.9, float('inf'), float('nan'),
                    0, -1, 65536, 70000, 10 ** 100, 'x', None):
            with self.subTest(bad=bad), self.assertRaises(OidcConfigError):
                _adapters._coerce_port(bad)

    def test_coerce_port_accepts_valid(self):
        self.assertEqual(_adapters._coerce_port(8812), 8812)
        self.assertEqual(_adapters._coerce_port('8812'), 8812)  # e.g. from env
        self.assertEqual(_adapters._coerce_port(8812.0), 8812)  # integral float
        self.assertEqual(_adapters._coerce_port(1), 1)
        self.assertEqual(_adapters._coerce_port(65535), 65535)

    def test_require_host_rejects_connstring_metacharacters(self):
        # The libpq-conninfo-injection / connection-redirection defense: none of
        # these belong in a real host, so an explicit host= override carrying
        # one (bypassing urlparse) must be rejected before it reaches a driver.
        for bad in ('a,evil', 'a/evil', 'a;b', 'a=b', 'a b', 'a%b', 'a\tb'):
            with self.subTest(bad=bad), self.assertRaises(OidcConfigError):
                _adapters._require_host('https://ok.example:9000/', bad)

    def test_require_host_strips_ipv6_brackets(self):
        # The PG drivers take a bare address; a bracketed IPv6 literal (from an
        # override or a URL) is returned unbracketed.
        self.assertEqual(
            _adapters._require_host('https://ok.example/', '[::1]'), '::1')
        self.assertEqual(
            _adapters._require_host('https://[::1]:9000/'), '::1')

    def test_require_host_rejects_userinfo_nonhttp_and_hostless(self):
        with self.assertRaises(OidcConfigError):
            _adapters._require_host('https://trusted@evil.example/')
        with self.assertRaises(OidcConfigError):
            _adapters._require_host('file://evil.example/')
        with self.assertRaises(OidcConfigError):
            _adapters._require_host('localhost')  # no scheme/authority

    def test_require_host_rejects_non_string_override(self):
        # A truthy non-str host= override (int, bytes, arbitrary object) must
        # surface as the package's typed OidcConfigError, not the bare
        # AttributeError/TypeError it would otherwise raise on .startswith() /
        # the host regex -- mirroring _coerce_port's up-front type guard. An
        # empty string and None stay valid (they fall back to the URL host).
        for bad in (123, b'evil.example', object(), ['h']):
            with self.subTest(bad=bad), self.assertRaises(OidcConfigError):
                _adapters._require_host('https://ok.example:9000/', bad)
        self.assertEqual(
            _adapters._require_host('https://ok.example/', ''), 'ok.example')

    def test_psycopg_rejects_unsafe_url_before_token_or_connection(self):
        for url in self.unsafe_urls:
            auth = mock.Mock()
            driver = mock.Mock()
            with self.subTest(url=url), mock.patch.object(
                    _adapters, '_pg_module', return_value=driver):
                with self.assertRaises(OidcConfigError):
                    _adapters.psycopg_connect(auth, url)
            auth.token.assert_not_called()
            driver.connect.assert_not_called()

    def test_sqlalchemy_rejects_unsafe_url_before_token_or_engine(self):
        sqlalchemy = types.ModuleType('sqlalchemy')
        sqlalchemy.create_engine = mock.Mock()
        sqlalchemy.event = mock.Mock()
        sqlalchemy_engine = types.ModuleType('sqlalchemy.engine')
        sqlalchemy_engine.URL = mock.Mock()
        modules = {
            'sqlalchemy': sqlalchemy,
            'sqlalchemy.engine': sqlalchemy_engine,
        }
        for url in self.unsafe_urls:
            auth = mock.Mock()
            with self.subTest(url=url), mock.patch.dict(sys.modules, modules):
                with self.assertRaises(OidcConfigError):
                    _adapters.sqlalchemy_engine(auth, url)
            auth.token.assert_not_called()
            sqlalchemy.create_engine.assert_not_called()


if __name__ == '__main__':
    unittest.main()
