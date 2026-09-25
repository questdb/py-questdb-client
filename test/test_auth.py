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

import dataclasses
from decimal import Decimal
from fractions import Fraction
import gc
import io
import logging
import os
import platform
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import types
import unittest
import uuid
import weakref
from unittest import mock

try:  # POSIX only; the home-resolution guard it exercises is POSIX-shaped.
    import pwd
except ImportError:  # pragma: no cover - Windows
    pwd = None

import questdb
from questdb.auth import (
    FileTokenStore,
    OidcCancelledError,
    OidcConfig,
    OidcConfigError,
    OidcDeviceAuth,
    OidcDeviceFlowError,
    OidcError,
    OidcNetworkError,
    OidcInteractionRequired,
    OidcTimeoutError,
    Renderer,
    psycopg_connect,
    sanitize_display_text,
    sqlalchemy_engine,
)
from questdb import _client
from questdb.auth import _adapters
from questdb.auth import _render
from questdb.auth._render import (
    TerminalRenderer,
    _verification_target,
    in_ipython_kernel,
    make_renderer,
)
import pg_capture_server
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


# Upper bound on collections while waiting for a target state. CPython reaches
# it on the first pass; PyPy does not refcount and stages cpyext finalization
# across several collections.
_SETTLE_MAX_PASSES = 60


def _settle_until(done):
    """Collect until ``done()`` holds; return whether it ever did.

    Target-directed, never a plateau detector. "The reading stopped changing"
    is not evidence of anything on PyPy, whose staged finalization can hold a
    value for several passes and then move again -- and nothing bounds how long
    such a plateau lasts. Waiting for the state the assertion is about cannot
    be fooled that way, and an unreachable target still fails, after the last
    pass.
    """
    for _ in range(_SETTLE_MAX_PASSES):
        gc.collect()
        if done():
            return True
    return False


def _provider_id_watermark():
    """Every provider built after this call has an id above the returned one."""
    return _client._debug_oidc_last_provider_id()


def _registered_ids_since(watermark):
    """Registry ids of providers built after ``watermark``.

    Scoping to the test's own providers is what makes an exact assertion
    possible. The whole-registry size also counts whatever earlier tests left
    unreachable but not yet finalized, and on PyPy that count drifts while the
    test runs -- including under the *baseline* reading, which then poisoned
    every comparison against it (``3 != 5`` on the linux_x64_pypy wheel job).
    """
    providers, handles = _client._debug_oidc_registry_snapshot()
    if providers != handles:
        raise AssertionError(
            f'OIDC provider/native registries are out of sync: '
            f'{sorted(providers ^ handles)}')
    return {provider_id for provider_id in providers
            if provider_id > watermark}


def _settled_registered_ids_since(watermark, expected=frozenset()):
    """Collect until exactly ``expected`` of the post-``watermark`` providers
    remain registered, then return the ids that do.

    Callers assert the result equals ``expected``: a leaked entry never
    leaves, so it exhausts the passes and fails that assertion.
    """
    expected = set(expected)
    _settle_until(lambda: _registered_ids_since(watermark) == expected)
    return _registered_ids_since(watermark)


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
            # interactive and open_browser are tri-state: None means "decide
            # from the environment", so it is a valid value for them.
            if name not in ('interactive', 'open_browser'):
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

    def test_explicit_empty_identity_overrides_are_rejected(self):
        for name in ('scope', 'audience', 'issuer'):
            with self.subTest(constructor='direct', name=name):
                with self.assertRaisesRegex(OidcConfigError, name):
                    make_auth(**{name: ''})

        # Override validation happens before discovery network I/O. An empty
        # value means neither "inherit" nor a usable explicit setting.
        for name in ('client_id', 'scope', 'audience', 'issuer'):
            with self.subTest(constructor='from_questdb', name=name):
                with self.assertRaisesRegex(OidcConfigError, name):
                    OidcDeviceAuth.from_questdb(
                        'http://127.0.0.1:1', **{name: ''})

    def test_explicit_open_browser_overrides_the_kernel_guess(self):
        # Regression: open_browser was `open_browser is True and not
        # in_ipython_kernel()`, so an explicit True was silently dropped in ANY
        # kernel -- including a local `jupyter lab`, where the browser and the
        # reader are on the same machine and the guess is simply wrong. There
        # was no way to ask for the browser at all.
        #
        # The kernel probe is only consulted for the default, so whether it is
        # called is exactly the question: consulted => the caller's value was
        # overridden; not consulted => it was honoured.
        def build(open_browser):
            probe = mock.Mock(return_value=True)
            with mock.patch.object(
                    _render, 'in_ipython_kernel', probe):
                # An explicit renderer and interactive keep make_renderer() and
                # detect_interactive() from consulting the probe as well.
                make_auth(
                    open_browser=open_browser,
                    interactive=False,
                    renderer=Renderer())
            return probe.called

        self.assertTrue(
            build(None), 'the default must still defer to the kernel check')
        self.assertFalse(
            build(True), 'an explicit True must not be overridden')
        self.assertFalse(
            build(False), 'an explicit False must not be overridden')

    def test_optional_booleans_accept_none_on_both_constructors(self):
        # Renamed from test_documented_optional_booleans_retain_none, which
        # claimed more than it checked: OidcConfig exposes no `interactive`
        # field, so the only assertion was on client_id and the test passed
        # whether None was retained, coerced to False, or coerced to True.
        # What it can honestly pin is that the tri-state values are ACCEPTED by
        # both constructors -- the behaviour they select is covered by
        # test_explicit_open_browser_overrides_the_kernel_guess and the
        # detect_interactive tests.
        auth = make_auth(interactive=None, open_browser=None)
        self.assertEqual(auth.config.client_id, 'questdb')
        # Validation order: the booleans are checked before the url, so a bad
        # url still reports the url rather than masking an accepted None.
        with self.assertRaisesRegex(OidcConfigError, 'url'):
            OidcDeviceAuth.from_questdb(
                object(), groups_in_token=None, interactive=None,
                open_browser=None)

    def test_config_sanitizes_direct_construction(self):
        # OidcConfig documents that every string field is display-sanitized,
        # and it is exported with a public generated constructor -- so the
        # invariant has to hold for an instance nobody routed through
        # OidcDeviceAuth.config: a test double, or a copy rebuilt with
        # dataclasses.replace. Its repr reaches the same terminal / notebook /
        # logged-traceback sink either way. See
        # test_discovery_config_view_strips_control_chars for the native
        # boundary's half of the guarantee.
        # ESC + BEL (C0 control), U+202E RIGHT-TO-LEFT OVERRIDE (bidi),
        # U+200B ZERO WIDTH SPACE (zero-width), NUL.
        cfg = OidcConfig(
            client_id='cl\x1bient',
            token_endpoint='https://idp/to\x07ken',
            device_authorization_endpoint='https://idp/de‮vice',
            scope='open​id',
            audience='aud\x1bience',
            issuer='iss\x00uer')
        self.assertEqual(cfg.client_id, 'client')
        self.assertEqual(cfg.token_endpoint, 'https://idp/token')
        self.assertEqual(
            cfg.device_authorization_endpoint, 'https://idp/device')
        self.assertEqual(cfg.scope, 'openid')
        self.assertEqual(cfg.audience, 'audience')
        self.assertEqual(cfg.issuer, 'issuer')
        for ch in ('\x1b', '\x07', '\x00', '‮', '​'):
            self.assertNotIn(ch, repr(cfg))
        # An unset optional stays None: _strip_control maps None to '', and
        # collapsing absent into present-but-empty would misreport the config.
        bare = OidcConfig(
            client_id='c', token_endpoint='t',
            device_authorization_endpoint='d')
        self.assertIsNone(bare.audience)
        self.assertIsNone(bare.issuer)
        self.assertEqual(bare.scope, 'openid')
        self.assertFalse(bare.groups_in_token)
        # Sanitizing through object.__setattr__ must not unfreeze the instance.
        with self.assertRaises(dataclasses.FrozenInstanceError):
            cfg.client_id = 'other'
        # A replace()-built copy is sanitized too -- that is the path a plain
        # __init__-only guarantee would miss.
        self.assertEqual(
            dataclasses.replace(cfg, client_id='ne\x1bw').client_id, 'new')

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
        # them and the kernel's own stdin flag was never consulted. The bundled
        # tests missed it because they fake the shell as a name-only class with
        # no base.
        zmq_base = type('ZMQInteractiveShell', (), {})
        for name in ('Shell', 'SpyderShell'):
            with self.subTest(shell=name):
                shell = type(name, (zmq_base,), {})()
                ipython = types.ModuleType('IPython')
                ipython.get_ipython = lambda shell=shell: shell
                with mock.patch.dict(sys.modules, {'IPython': ipython}):
                    self.assertTrue(in_ipython_kernel())
                    # A real frontend allows stdin, so these stay interactive.
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

    def test_a_missing_tty_does_not_refuse_sign_in(self):
        # detect_interactive() used to end in `sys.stderr.isatty()`, which
        # refuses on the absence of evidence rather than evidence of absence:
        # `prog 2>&1 | tee log` at a real terminal, a process supervisor, or an
        # IDE run configuration all capture stderr and still show it to a human,
        # and all were turned away from a sign-in that works -- with
        # `interactive=True` only discoverable after the refusal. No stream
        # combination is non-interactive any more; the Java client has no such
        # detection either.
        ipython = types.ModuleType('IPython')
        ipython.get_ipython = lambda: None

        class Stream:
            def __init__(self, tty):
                self._tty = tty

            def isatty(self):
                return self._tty

        for out_tty, err_tty in [(False, True), (True, False),
                                 (True, True), (False, False)]:
            with self.subTest(stdout=out_tty, stderr=err_tty):
                with mock.patch.dict(sys.modules, {'IPython': ipython}), \
                        mock.patch.object(_render.sys, 'stdout', Stream(out_tty)), \
                        mock.patch.object(_render.sys, 'stderr', Stream(err_tty)):
                    self.assertTrue(_render.detect_interactive())

    def test_a_broken_stderr_does_not_refuse_sign_in(self):
        # The old isatty() call was wrapped in try/except returning False, so a
        # stream whose isatty() raises (or a None stderr under pythonw) also
        # refused. Nothing reads the stream to decide any more.
        ipython = types.ModuleType('IPython')
        ipython.get_ipython = lambda: None

        class Exploding:
            def isatty(self):
                raise OSError('detached')

        for stderr in (Exploding(), None):
            with self.subTest(stderr=type(stderr).__name__):
                with mock.patch.dict(sys.modules, {'IPython': ipython}), \
                        mock.patch.object(_render.sys, 'stderr', stderr):
                    self.assertTrue(_render.detect_interactive())

    def test_notebook_executor_without_stdin_is_non_interactive(self):
        # A notebook executor (papermill / nbclient / nbconvert --execute) runs a
        # real ZMQ kernel -- in_ipython_kernel() is True -- but with
        # allow_stdin=False: the frontend is stating at protocol level that no
        # human can authorize. This is the ONLY signal that now makes
        # detect_interactive() False, and unlike a TTY test it has no
        # false-refusal mode -- so sign-in fails fast with
        # OidcInteractionRequired instead of polling to the device-code deadline
        # with a prompt rendered into an output nobody will open.
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

    def test_default_interval_is_bounded_at_the_native_floor(self):
        # Native clamps the value to [5, 1800] (`questdb/oidc.h`), so 1..4 were
        # accepted here and silently became 5 -- the caller had no way to learn
        # their setting was ignored. Reject them instead, and keep the boundary.
        for value in (1, 2, 4):
            with self.subTest(value=value), self.assertRaises(OidcConfigError):
                make_auth(default_interval=value)
        make_auth(default_interval=5)

    def test_default_interval_is_bounded_at_the_native_ceiling(self):
        # The old bound was the full uint64 range, but native casts to i64
        # before clamping: 2**63 wrapped negative, floored to 0, and came back
        # as the 5s MINIMUM, so the largest accepted value produced the FASTEST
        # polling. Bound it where native clamps anyway.
        for value in (1801, 1 << 63, (1 << 64) - 1):
            with self.subTest(value=value), self.assertRaises(OidcConfigError):
                make_auth(default_interval=value)
        # The boundary itself is still accepted.
        make_auth(default_interval=1800)

    def test_config_errors_report_config_error_code(self):
        """A Python-raised OidcConfigError carries the same code as a native one.

        `OidcError` defaulted every directly constructed error to `AuthError`,
        and no raise site in `questdb.auth` passes `code=`, so a validation
        failure reported an auth failure -- something signing in again could
        clear -- while `oidc.pxi` gave the native error for the same condition
        `ConfigError`. The documented contract is that retry logic can key on
        `.code`, so the two routes have to agree.
        """
        for label, build in (
                ('empty client_id',
                 lambda: questdb.auth.OidcDeviceAuth(
                     '', 'https://idp.example/device',
                     'https://idp.example/token')),
                ('bad renderer', lambda: make_auth(renderer=object())),
                ('bad interval', lambda: make_auth(default_interval=0)),
                ('empty store dir', lambda: FileTokenStore('')),
        ):
            with self.subTest(label):
                with self.assertRaises(OidcConfigError) as ctx:
                    build()
                self.assertEqual(
                    ctx.exception.code, questdb.QuestDBErrorCode.ConfigError)
        # Only the config type changes default; the rest stay terminal-auth.
        self.assertEqual(
            OidcInteractionRequired('x').code,
            questdb.QuestDBErrorCode.AuthError)
        # An explicit code still wins, so a native error keeps its own
        # classification (notably the retryable SocketError).
        self.assertEqual(
            OidcConfigError(
                'x', code=questdb.QuestDBErrorCode.SocketError).code,
            questdb.QuestDBErrorCode.SocketError)

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
        # A provider built with __new__ but never __init__'d has no native handle.
        # That is *not* the same state as closed, and every error says so: its
        # own token-flow ops raise RuntimeError, and attaching it to a transport
        # raises ValueError -- never a native NULL deref. Both name the
        # uninitialized state rather than reporting it as "closed", which would
        # send the reader looking for a lifecycle call they never made. clear()
        # is a documented no-op on it.
        closed = OidcDeviceAuth.__new__(OidcDeviceAuth)
        for op in ('sign_in', 'token', 'headers'):
            with self.subTest(op=op), self.assertRaisesRegex(
                    RuntimeError, 'not initialized'):
                getattr(closed, op)()
        with self.assertRaisesRegex(RuntimeError, 'not initialized'):
            closed.config
        closed.clear()  # idempotent no-op on an uninitialized provider
        with self.assertRaisesRegex(ValueError, 'not initialized'):
            questdb.Sender(
                questdb.Protocol.Http, '127.0.0.1', 9000,
                oidc_auth=closed, auto_flush=False)
        with self.assertRaisesRegex(ValueError, 'not initialized'):
            questdb.connect(
                'ws::addr=127.0.0.1:9000;lazy_connect=true;',
                oidc_auth=closed)

    def test_explicitly_closed_provider_is_rejected(self):
        # The other closed state: a real provider that close() disabled. The
        # attach guards gained a `_closed` check that only this covers -- the
        # uninitialized case above exercises the null native handle instead.
        auth = make_auth()
        auth.close()
        with self.assertRaisesRegex(ValueError, 'closed'):
            questdb.Sender(
                questdb.Protocol.Http, '127.0.0.1', 9000,
                oidc_auth=auth, auto_flush=False)
        with self.assertRaisesRegex(ValueError, 'closed'):
            questdb.connect(
                'ws::addr=127.0.0.1:9000;lazy_connect=true;', oidc_auth=auth)
        # config stays readable: it is immutable native state that close does
        # not invalidate.
        self.assertEqual(auth.config.client_id, 'questdb')

    def test_connect_preserves_oidc_error_that_looks_like_duplicate_key(self):
        for err in (
                OidcNetworkError(
                    'identity provider said duplicate key "query_pool_min"',
                    status=503, retry_after=7),
                OidcConfigError(
                    'identity provider said duplicate key "query_pool_min"')):
            with self.subTest(error_type=type(err).__name__):
                fake_db = mock.Mock()
                fake_db.from_conf.side_effect = err
                with mock.patch.dict(
                        questdb.connect.__globals__, {'QuestDB': fake_db}):
                    with self.assertRaises(type(err)) as raised:
                        questdb.connect(
                            'ws::addr=127.0.0.1:9000;', query_pool_min=1)
                self.assertIs(raised.exception, err)
                if isinstance(err, OidcNetworkError):
                    self.assertEqual(raised.exception.status, 503)
                    self.assertEqual(raised.exception.retry_after, 7)
                else:
                    self.assertEqual(
                        err.code, questdb.QuestDBErrorCode.ConfigError)

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
        # specifically. Pin the hierarchy and the default code directly (the
        # transport-path behaviour is covered by the attachment tests).
        #
        # A misconfiguration defaults to ConfigError rather than AuthError:
        # native classifies it that way, and the package documents that retry
        # logic may key on `.code`, so a config failure must not look like an
        # auth failure that signing in again could clear. See
        # test_config_errors_report_config_error_code.
        self.assertTrue(issubclass(OidcError, questdb.QuestDBError))
        for exc_type, expected in (
                (OidcConfigError, questdb.QuestDBErrorCode.ConfigError),
                # Retryable, matching native's OidcErrorKind::Network ->
                # SocketError: a transient IdP/QuestDB network failure must not
                # look like a terminal auth failure to `.code`-keyed retry
                # logic.
                (OidcNetworkError, questdb.QuestDBErrorCode.SocketError),
                (OidcInteractionRequired, questdb.QuestDBErrorCode.AuthError),
                (OidcDeviceFlowError, questdb.QuestDBErrorCode.AuthError),
                (OidcTimeoutError, questdb.QuestDBErrorCode.AuthError)):
            with self.subTest(exc_type=exc_type.__name__):
                self.assertTrue(issubclass(exc_type, OidcError))
                err = exc_type('x')
                self.assertIsInstance(err, questdb.QuestDBError)
                self.assertIs(err.code, expected)

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

    @unittest.skipUnless(
        os.name == 'nt', 'non-Unix file-store durability guard')
    def test_windows_file_store_rejects_before_device_flow(self):
        renderer = RecordingRenderer()
        with tempfile.TemporaryDirectory() as directory:
            with OidcTestServer() as server:
                auth = make_discovered_auth(
                    server,
                    token_store=FileTokenStore.at(directory),
                    renderer=renderer)
                with self.assertRaisesRegex(
                        OidcConfigError, 'durable directory-entry'):
                    auth.sign_in()
                self.assertEqual(server.requests('/device', 'POST'), [])
                self.assertEqual(renderer.prompts, [])
                # No CREDENTIAL may be written: durability is exactly what
                # this platform cannot provide. The empty `.lock` files of the
                # cross-process protocol (`.store.lock`, `<identity>.lock`)
                # belong to the READS that deliberately stay available here;
                # they are released in place rather than unlinked (so a
                # departing holder cannot delete a successor's lock) and never
                # hold credential material. Assert on the entries, not on an
                # empty directory.
                entries = os.listdir(directory)
                self.assertEqual(
                    [name for name in entries if not name.endswith('.lock')],
                    [],
                    f'the rejected store wrote a non-lock entry: {entries}')

    def test_exit_hook_silences_diagnostics_without_closing_providers(self):
        self.addCleanup(_client._debug_oidc_reset_callback_shutdown)
        # The atexit hook exists to stop a persistence diagnostic entering a
        # finalizing interpreter. It must not achieve that by closing the
        # provider: every handle it can reach belongs to a provider the user
        # still holds, i.e. exactly the set attached to live transports, and
        # closing is terminal for all of them. A closed provider fails every
        # later token pull non-retryably, so a reconnect during the rest of
        # interpreter shutdown -- atexit runs BEFORE module clearing, so the
        # pool's bounded close-flush drain happens after this hook -- would
        # terminalize a QWP publication store and discard frames it had
        # already accepted.
        #
        # Running the hook and then using the provider is the whole test: a
        # closing hook makes every operation below raise OidcCancelledError.
        auth = make_auth()

        _client._oidc_detach_diagnostics_at_exit()

        # Still open: a closing hook fails each of these instead.
        self.assertEqual(auth.config.client_id, 'questdb')
        auth.clear()
        with self.assertRaises(OidcInteractionRequired):
            auth.token()

        # Idempotent, and harmless once the provider really is closed.
        _client._oidc_detach_diagnostics_at_exit()
        auth.close()
        _client._oidc_detach_diagnostics_at_exit()

    def test_exit_hook_silences_renderer_events_without_closing_provider(self):
        self.addCleanup(_client._debug_oidc_reset_callback_shutdown)
        renderer = RecordingRenderer()
        with OidcTestServer() as server:
            auth = make_discovered_auth(server, renderer=renderer)
            _client._oidc_detach_diagnostics_at_exit()
            # Clear only the Python shutdown flag, which makes sign_in() refuse
            # to prompt; the native handles stay detached, and that is what
            # this test drives through the native device flow.
            _client._debug_oidc_reset_callback_shutdown()

            # The hook suppresses every callback that would enter Python, but
            # deliberately keeps the provider usable for transports draining
            # after atexit. Sign-in therefore completes and commits its token
            # without invoking the detached renderer.
            auth.sign_in()
            self.assertEqual(auth.token(), 'AT-initial')
            self.assertEqual(renderer.prompts, [])
            self.assertEqual(renderer.waiting, [])
            self.assertEqual(renderer.successes, [])
            self.assertEqual(renderer.failures, [])

    def test_sign_in_after_exit_hook_fails_fast_instead_of_prompting(self):
        # A sign-in started after the exit hook (an atexit handler registered
        # before questdb was imported) cannot show its device code: it used to
        # poll silently until the code expired.
        self.addCleanup(_client._debug_oidc_reset_callback_shutdown)
        renderer = RecordingRenderer()
        pending = (400, {'error': 'authorization_pending'}, None)
        with OidcTestServer(
                device_token_response=pending, device_expires_in=30,
                device_interval=1) as server:
            auth = make_discovered_auth(server, renderer=renderer)
            _client._oidc_detach_diagnostics_at_exit()
            started = time.monotonic()
            with self.assertRaises(OidcInteractionRequired) as raised:
                auth.sign_in()
            self.assertLess(time.monotonic() - started, 5)
            self.assertIn('interpreter shutdown', str(raised.exception))
            self.assertEqual(renderer.prompts, [])
            # Nothing was closed: the provider is still usable.
            self.assertEqual(auth.config.client_id, 'discovered-client')
            with self.assertRaises(OidcInteractionRequired):
                auth.token()

    def test_sign_in_after_exit_hook_still_rejects_a_concurrent_sign_in(self):
        self.addCleanup(_client._debug_oidc_reset_callback_shutdown)
        prompt_entered = threading.Event()
        release_prompt = threading.Event()
        sign_in_errors = []

        class BlockingRenderer(RecordingRenderer):
            def on_prompt(self, challenge):
                prompt_entered.set()
                release_prompt.wait()
                super().on_prompt(challenge)

        with OidcTestServer() as server:
            auth = make_discovered_auth(
                server, renderer=BlockingRenderer(), open_browser=False)

            def sign_in():
                try:
                    auth.sign_in()
                except BaseException as exc:
                    sign_in_errors.append(exc)

            signer = threading.Thread(target=sign_in, daemon=True)
            hook = threading.Thread(
                target=_client._oidc_detach_diagnostics_at_exit, daemon=True)
            try:
                signer.start()
                self.assertTrue(prompt_entered.wait(5))
                hook.start()
                hook.join(2)
                if hook.is_alive():
                    self.fail('exit hook waited for a parked renderer callback')
                with self.assertRaises(OidcError) as raised:
                    auth.sign_in()
                self.assertEqual(
                    raised.exception.code,
                    questdb.QuestDBErrorCode.InvalidApiCall)
            finally:
                release_prompt.set()
                hook.join(5)
                signer.join(10)
                auth.close()
            self.assertFalse(signer.is_alive())
            self.assertEqual(sign_in_errors, [])

    def test_sign_in_after_exit_hook_is_served_from_the_cache(self):
        # A credential that needs no prompt still satisfies sign_in().
        self.addCleanup(_client._debug_oidc_reset_callback_shutdown)
        with OidcTestServer() as server:
            auth = make_discovered_auth(server)
            auth.sign_in()
            _client._oidc_detach_diagnostics_at_exit()
            auth.sign_in()
            self.assertEqual(auth.token(), 'AT-initial')

    @unittest.skipUnless(
        os.name == 'posix', 'durable file token store requires POSIX')
    def test_exit_hook_actually_silences_persistence_diagnostics(self):
        self.addCleanup(_client._debug_oidc_reset_callback_shutdown)
        # The companion test above proves detaching does not close providers;
        # this positive half proves the hook reaches the native diagnostic
        # sink. Reducing _oidc_detach_handle_callbacks to a no-op makes the
        # deterministic failed save below emit a WARNING and fail this test.
        class SabotageRenderer(RecordingRenderer):
            def __init__(self, directory):
                super().__init__()
                self.directory = directory

            def on_prompt(self, challenge):
                super().on_prompt(challenge)
                shutil.rmtree(self.directory)
                with open(self.directory, 'w', encoding='utf-8') as sink:
                    sink.write('not a directory')

        with tempfile.TemporaryDirectory() as parent:
            directory = os.path.join(parent, 'store')
            with OidcTestServer() as server:
                auth = make_discovered_auth(
                    server,
                    token_store=FileTokenStore.at(directory),
                    renderer=SabotageRenderer(directory))
                _client._oidc_detach_diagnostics_at_exit()
                # See test_exit_hook_silences_renderer_events_without_closing_
                # provider: keep native detached, let sign_in() reach it.
                _client._debug_oidc_reset_callback_shutdown()
                with self.assertNoLogs('questdb', level='WARNING'):
                    auth.sign_in()
                self.assertEqual(auth.token(), 'AT-initial')

    @unittest.skipUnless(
        os.name == 'posix', 'durable file token store requires POSIX')
    def test_exit_hook_does_not_wait_for_parked_persistence_diagnostic(self):
        self.addCleanup(_client._debug_oidc_reset_callback_shutdown)
        diagnostic_entered = threading.Event()
        release_diagnostic = threading.Event()

        class SabotageRenderer(RecordingRenderer):
            def __init__(self, directory):
                super().__init__()
                self.directory = directory

            def on_prompt(self, challenge):
                super().on_prompt(challenge)
                shutil.rmtree(self.directory)
                with open(self.directory, 'w', encoding='utf-8') as sink:
                    sink.write('not a directory')

        class BlockingHandler(logging.Handler):
            def emit(self, record):
                diagnostic_entered.set()
                release_diagnostic.wait()

        with tempfile.TemporaryDirectory() as parent:
            directory = os.path.join(parent, 'store')
            with OidcTestServer() as server:
                auth = make_discovered_auth(
                    server,
                    token_store=FileTokenStore.at(directory),
                    renderer=SabotageRenderer(directory))
                handler = BlockingHandler()
                logger = logging.getLogger('questdb')
                old_level = logger.level
                logger.addHandler(handler)
                logger.setLevel(logging.WARNING)
                sign_in_errors = []

                def sign_in():
                    try:
                        auth.sign_in()
                    except BaseException as exc:
                        sign_in_errors.append(exc)

                signer = threading.Thread(target=sign_in, daemon=True)
                hook = threading.Thread(
                    target=_client._oidc_detach_diagnostics_at_exit,
                    daemon=True)
                try:
                    signer.start()
                    self.assertTrue(
                        diagnostic_entered.wait(10),
                        'persistence diagnostic did not reach logging')

                    # Suppression must be published without draining arbitrary
                    # logging.Handler code already inside the native callback
                    # gate. The waiting diagnostic detach hangs here until
                    # release_diagnostic is set.
                    hook.start()
                    hook.join(2)
                    if hook.is_alive():
                        release_diagnostic.set()
                        hook.join(5)
                        signer.join(5)
                        self.fail(
                            'exit hook waited for a parked diagnostic callback')
                    self.assertFalse(release_diagnostic.is_set())
                    self.assertTrue(
                        _client._debug_oidc_callbacks_shutting_down())

                    release_diagnostic.set()
                    signer.join(10)
                    self.assertFalse(signer.is_alive())
                    self.assertEqual(sign_in_errors, [])
                    self.assertEqual(auth.token(), 'AT-initial')
                finally:
                    release_diagnostic.set()
                    hook.join(5)
                    signer.join(5)
                    logger.removeHandler(handler)
                    logger.setLevel(old_level)

    def test_exit_hook_detaches_provider_built_after_its_snapshot(self):
        self.addCleanup(_client._debug_oidc_reset_callback_shutdown)
        prompt_entered = threading.Event()
        release_prompt = threading.Event()

        class BlockingRenderer(RecordingRenderer):
            def on_prompt(self, challenge):
                super().on_prompt(challenge)
                prompt_entered.set()
                release_prompt.wait()

        first_renderer = BlockingRenderer()
        second_renderer = RecordingRenderer()
        sign_in_errors = []

        with OidcTestServer() as first_server, OidcTestServer() as second_server:
            first = make_discovered_auth(
                first_server, renderer=first_renderer)

            def sign_in_first():
                try:
                    first.sign_in()
                except BaseException as exc:
                    sign_in_errors.append(exc)

            signer = threading.Thread(target=sign_in_first, daemon=True)
            signer.start()
            self.assertTrue(prompt_entered.wait(5), 'first prompt did not start')

            # The hook must publish event suppression and return while arbitrary
            # renderer code remains parked. Running it in a helper lets a
            # regression cleanly release the callback instead of hanging the
            # whole test process forever.
            hook = threading.Thread(
                target=_client._oidc_detach_diagnostics_at_exit, daemon=True)
            hook.start()
            hook.join(2)
            if hook.is_alive():
                release_prompt.set()
                hook.join(5)
                signer.join(5)
                self.fail('exit hook waited for a parked renderer callback')
            self.assertFalse(release_prompt.is_set())
            self.assertTrue(_client._debug_oidc_callbacks_shutting_down())

            # A provider completed after the hook's snapshot observes the
            # shutdown marker and self-detaches before it is exposed.
            late = make_discovered_auth(
                second_server, renderer=second_renderer)

            release_prompt.set()
            signer.join(10)
            self.assertFalse(signer.is_alive())
            self.assertEqual(sign_in_errors, [])

            # Keep native detached, let sign_in() reach it (see
            # test_exit_hook_silences_renderer_events_without_closing_provider).
            _client._debug_oidc_reset_callback_shutdown()
            late.sign_in()
            self.assertEqual(late.token(), 'AT-initial')
            self.assertEqual(second_renderer.prompts, [])
            self.assertEqual(second_renderer.waiting, [])
            self.assertEqual(second_renderer.successes, [])
            self.assertEqual(second_renderer.failures, [])

    @unittest.skipUnless(
        os.name == 'posix', 'durable file token store requires POSIX')
    def test_persistence_diagnostic_can_close_its_provider(self):
        class SabotageRenderer(RecordingRenderer):
            def __init__(self, directory):
                super().__init__()
                self.directory = directory

            def on_prompt(self, challenge):
                super().on_prompt(challenge)
                shutil.rmtree(self.directory)
                with open(self.directory, 'w', encoding='utf-8') as sink:
                    sink.write('not a directory')

        class ClosingHandler(logging.Handler):
            def __init__(self):
                super().__init__()
                self.auth = None
                self.returned = threading.Event()

            def emit(self, record):
                # logging.Handler.handle owns this handler's lock here. Native
                # must not wait on the acquisition mutex held by the diagnostic
                # callback's own persistence stack.
                self.auth.close()
                self.returned.set()

        with tempfile.TemporaryDirectory() as parent:
            directory = os.path.join(parent, 'store')
            with OidcTestServer() as server:
                auth = make_discovered_auth(
                    server,
                    token_store=FileTokenStore.at(directory),
                    renderer=SabotageRenderer(directory))
                handler = ClosingHandler()
                handler.auth = auth
                logger = logging.getLogger('questdb')
                old_level = logger.level
                logger.addHandler(handler)
                logger.setLevel(logging.WARNING)
                outcome = []

                def sign_in():
                    try:
                        auth.sign_in()
                    except BaseException as exc:
                        outcome.append(exc)

                thread = threading.Thread(target=sign_in, daemon=True)
                try:
                    thread.start()
                    thread.join(10)
                    self.assertFalse(
                        thread.is_alive(),
                        'close() waited for its own persistence diagnostic')
                    self.assertTrue(
                        handler.returned.is_set(),
                        'the persistence diagnostic did not return from close()')
                    if outcome:
                        self.assertIsInstance(outcome[0], OidcCancelledError)
                finally:
                    logger.removeHandler(handler)
                    logger.setLevel(old_level)

    @unittest.skipUnless(
        os.name == 'posix', 'durable file token store requires POSIX')
    def test_persistence_diagnostic_rejects_acquisition_reentry(self):
        class SabotageRenderer(RecordingRenderer):
            def __init__(self, directory):
                super().__init__()
                self.directory = directory

            def on_prompt(self, challenge):
                super().on_prompt(challenge)
                shutil.rmtree(self.directory)
                with open(self.directory, 'w', encoding='utf-8') as sink:
                    sink.write('not a directory')

        class ReenteringHandler(logging.Handler):
            def __init__(self):
                super().__init__()
                self.auth = None
                self.results = []
                self.returned = threading.Event()

            def emit(self, record):
                try:
                    for operation in (self.auth.clear, self.auth.token):
                        try:
                            operation()
                        except BaseException as exc:
                            self.results.append(exc)
                        else:
                            self.results.append(None)
                finally:
                    self.returned.set()

        with tempfile.TemporaryDirectory() as parent:
            directory = os.path.join(parent, 'store')
            with OidcTestServer() as server:
                auth = make_discovered_auth(
                    server,
                    token_store=FileTokenStore.at(directory),
                    renderer=SabotageRenderer(directory))
                handler = ReenteringHandler()
                handler.auth = auth
                logger = logging.getLogger('questdb')
                old_level = logger.level
                logger.addHandler(handler)
                logger.setLevel(logging.WARNING)
                outcome = []

                def sign_in():
                    try:
                        auth.sign_in()
                    except BaseException as exc:
                        outcome.append(exc)

                thread = threading.Thread(target=sign_in, daemon=True)
                try:
                    thread.start()
                    thread.join(10)
                    self.assertFalse(
                        thread.is_alive(),
                        'callback acquisition waited for its own diagnostic')
                    self.assertTrue(handler.returned.is_set())
                    self.assertEqual(outcome, [])
                    self.assertEqual(len(handler.results), 2)
                    clear_error, token_error = handler.results
                    self.assertIsInstance(clear_error, questdb.QuestDBError)
                    self.assertEqual(
                        clear_error.code,
                        questdb.QuestDBErrorCode.InvalidApiCall)
                    self.assertIsInstance(
                        token_error, OidcInteractionRequired)
                    self.assertEqual(
                        token_error.code,
                        questdb.QuestDBErrorCode.SocketError)
                    for error in handler.results:
                        self.assertIn(
                            'persistence diagnostic callback', str(error))
                finally:
                    logger.removeHandler(handler)
                    logger.setLevel(old_level)

    @unittest.skipUnless(os.name == 'posix', 'POSIX bytes paths only')
    def test_non_utf8_file_store_path_is_typed(self):
        with tempfile.TemporaryDirectory() as directory:
            path = os.fsencode(directory) + b'/tokens-\xff'
            token_store = FileTokenStore.at(path)
            with self.assertRaisesRegex(
                    OidcConfigError, 'token_store directory'):
                make_auth(token_store=token_store)

    def test_file_store_directory_is_expanded_and_absolute(self):
        # The directory used to be handed to native verbatim and resolved
        # against the process CWD, so FileTokenStore('~/qdb-tokens') wrote a
        # long-lived plaintext refresh token into a directory literally named
        # '~' below the working directory, and a relative path followed a
        # chdir -- re-running the device flow and leaving a second copy of the
        # credential elsewhere.
        home = os.path.expanduser('~')
        self.assertEqual(
            FileTokenStore('~/qdb-tokens').directory,
            os.path.join(home, 'qdb-tokens'))
        self.assertNotIn('~', FileTokenStore('~/qdb-tokens').directory)

        store = FileTokenStore('rel-tokens')
        self.assertTrue(os.path.isabs(store.directory))
        cwd_relative = os.path.join(os.getcwd(), 'rel-tokens')
        self.assertEqual(store.directory, cwd_relative)

        # Pinned at construction: a later chdir must not move the store.
        with tempfile.TemporaryDirectory() as other:
            previous = os.getcwd()
            try:
                os.chdir(other)
                self.assertEqual(store.directory, cwd_relative)
            finally:
                os.chdir(previous)

        # An absolute path is already resolved and passes through untouched.
        with tempfile.TemporaryDirectory() as directory:
            self.assertEqual(FileTokenStore.at(directory).directory, directory)

    def test_unresolvable_home_is_refused_not_resolved_against_cwd(self):
        """`~` that cannot be expanded must not become a literal directory.

        `os.path.expanduser` returns the path UNCHANGED when `$HOME` is unset
        and the uid has no `pwd` entry -- the normal state in a container run
        under an arbitrary uid. `abspath` then resolved the leading `~` against
        the working directory, so `FileTokenStore('~/qdb-tokens')` wrote a
        long-lived plaintext refresh token into a directory literally named
        `~`, usually inside whatever the process happened to be started in.
        `at_default_location()` already refused this; the constructor did not.
        """
        if pwd is None:
            self.skipTest('no pwd module (Windows)')

        def no_passwd_entry(_uid):
            raise KeyError('no passwd entry for uid')

        with mock.patch.dict(os.environ, clear=False) as _env:
            os.environ.pop('HOME', None)
            os.environ.pop('USERPROFILE', None)
            with mock.patch.object(pwd, 'getpwuid', no_passwd_entry):
                # Precondition: expansion really does fail in this environment,
                # so the assertion below is testing the guard and not a
                # coincidentally-resolvable path.
                if not os.path.expanduser('~/qdb-tokens').startswith('~'):
                    self.skipTest('platform still resolves ~ without $HOME')
                with self.assertRaises(OidcConfigError) as ctx:
                    FileTokenStore('~/qdb-tokens')
                self.assertIn('home directory', str(ctx.exception))
                self.assertEqual(
                    ctx.exception.code, questdb.QuestDBErrorCode.ConfigError)
                # A path with no `~` is unaffected by the guard.
                with tempfile.TemporaryDirectory() as directory:
                    self.assertEqual(
                        FileTokenStore(directory).directory, directory)

    def test_default_file_store_environment_override(self):
        # A real temporary directory rather than a '/tmp/...' literal: the
        # override is resolved like any other directory, and a leading '/' is
        # drive-less on Windows, where abspath() resolves it against the
        # current drive. The literal comparison then failed as
        # 'C:\\tmp\\qdb-oidc-test' != '/tmp/qdb-oidc-test' -- a platform
        # artefact of the expected value, nothing to do with the override.
        with tempfile.TemporaryDirectory() as directory:
            with mock.patch.dict(
                    os.environ,
                    {'QUESTDB_CLIENT_OIDC_TOKEN_STORE_DIR': directory}):
                self.assertEqual(
                    FileTokenStore.at_default_location().directory, directory)

        # A non-absolute override is refused rather than normalized. This used
        # to expand '~' so the refresh token could not land in a directory
        # literally named '~' -- a real hazard, but the cure was worse: this
        # setting is shared with the native client, which does NOT expand '~'.
        # Expanding it here pointed Python at $HOME/x while native used ./~/x,
        # so the one setting meant to share a store silently produced two, each
        # with its own plaintext refresh token. Refusing names the problem
        # instead.
        for bad in (os.path.join('~', 'qdb-oidc-test'),
                    os.path.join('rel', 'qdb-oidc-test')):
            with self.subTest(override=bad):
                with mock.patch.dict(
                        os.environ,
                        {'QUESTDB_CLIENT_OIDC_TOKEN_STORE_DIR': bad}):
                    with self.assertRaises(OidcConfigError) as ctx:
                        FileTokenStore.at_default_location()
                    message = str(ctx.exception)
                    self.assertIn(
                        'QUESTDB_CLIENT_OIDC_TOKEN_STORE_DIR', message)
                    self.assertIn('absolute', message)

        # An explicit constructor path is a Python-supplied path, not the
        # shared setting, so it keeps the expansion.
        self.assertEqual(
            FileTokenStore(os.path.join('~', 'qdb-oidc-test')).directory,
            os.path.join(os.path.expanduser('~'), 'qdb-oidc-test'))

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

    def test_failed_construction_clears_renderer_back_reference(self):
        # Inspect the Cython slot directly through a test seam rather than
        # inferring it from immediate weakref destruction. The latter is a
        # CPython refcount property, not a language guarantee, and failed on
        # PyPy even when the provider no longer held the renderer.
        class BackReferencingRenderer(Renderer):
            provider = None

        # Early callback validation used to publish the renderer before the
        # validation loop, outside the native-build try/except.
        provider = OidcDeviceAuth.__new__(OidcDeviceAuth)
        renderer = BackReferencingRenderer()
        renderer.provider = provider
        renderer.on_failure = None
        with self.assertRaisesRegex(OidcConfigError, 'on_failure'):
            provider.__init__(
                'questdb', 'https://idp.example/device',
                'https://idp.example/token', interactive=False,
                open_browser=False, renderer=renderer)
        self.assertFalse(_client._debug_oidc_renderer_attached(provider))

        # A failure after callback registration takes the separate unwind path
        # and must clear the same edge.
        provider = OidcDeviceAuth.__new__(OidcDeviceAuth)
        renderer = BackReferencingRenderer()
        renderer.provider = provider
        with self.assertRaises(OidcError):
            provider.__init__(
                'questdb', 'https://idp.example/device',
                'https://idp.example/token', interactive=False,
                open_browser=False, renderer=renderer,
                ca_bundle=self._UNREADABLE_CA_BUNDLE)
        self.assertFalse(_client._debug_oidc_renderer_attached(provider))

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
        'PyPy leaks any reference cycle that passes through a C-extension '
        'object (PyPy issue #3848): cpyext pins the renderer through the '
        "provider's struct field, and PyPy's collector cannot see that the pin "
        'is itself only reachable from the dying cycle. Observed on the '
        'linux_x64_pypy wheel job, where the renderer survived every one of '
        'the collections below. What PyPy *can* do is covered there by '
        'test_close_detaches_renderer (deterministic release) and '
        'test_registry_weakref_released_on_success (acyclic collection).')
    def test_renderer_provider_cycle_is_collected(self):
        watermark = _provider_id_watermark()
        renderer = Renderer()
        auth = make_auth(renderer=renderer)
        renderer.auth = auth
        renderer_ref = weakref.ref(renderer)
        auth_ref = weakref.ref(auth)

        del renderer, auth
        # The provider has no finalizer-bearing child: the native handle lives in
        # a separate registry entry removed by the provider's weakref callback.
        # That keeps the cycle free of finalized C-extension edges, so CPython's
        # cyclic GC reclaims it and the native handle with it.
        _settle_until(
            lambda: renderer_ref() is None and auth_ref() is None)

        self.assertIsNone(renderer_ref())
        self.assertIsNone(auth_ref())
        self.assertEqual(_settled_registered_ids_since(watermark), set())

    def test_close_detaches_renderer(self):
        renderer = Renderer()
        auth = make_auth(renderer=renderer)
        renderer_ref = weakref.ref(renderer)
        del renderer
        self.assertIsNotNone(renderer_ref())

        auth.close()
        # Wait for the state asserted below. The old plateau detector was fed
        # this as a boolean, so it "settled" on consecutive True readings --
        # renderer not yet collected -- and the assertion then failed.
        _settle_until(lambda: renderer_ref() is None)
        self.assertIsNone(renderer_ref())

    # Native build() is ~100ms, so keep repeated construction tests modest.
    _LEAK_ITERS = 20

    def test_registry_weakref_released_on_success(self):
        # Each OidcDeviceAuth registers a weakref to itself in _OIDC_PROVIDERS
        # (oidc.pxi _finish_builder) under an integer key native keeps as
        # user_data. Native owns no Python reference; the provider weakref
        # callback removes both registry entries, and a missed callback strands
        # the weakref plus native-handle owner in module-global dictionaries.
        make_auth(renderer=Renderer())  # warm one-time module state
        watermark = _provider_id_watermark()
        refs = []
        for _ in range(self._LEAK_ITERS):
            auth = make_auth(renderer=Renderer())
            refs.append(weakref.ref(auth))
            del auth
        remaining = _settled_registered_ids_since(watermark)
        self.assertTrue(
            all(ref() is None for ref in refs),
            'a provider remained alive after its strong references were dropped')
        self.assertEqual(
            remaining, set(),
            f'the provider registry grew over {self._LEAK_ITERS} constructions')

    def test_registry_lock_keeps_concurrent_provider_builds_paired(self):
        # Build() releases the GIL, so constructors started together can
        # interleave between id allocation, paired registry insertion, native
        # construction and publication. The registry lock must give every
        # provider one unique id and keep the weakref/handle maps paired.
        workers = 8
        barrier = threading.Barrier(workers)
        providers = [None] * workers
        failures = []

        def build(index):
            try:
                barrier.wait()
                providers[index] = make_auth(renderer=Renderer())
            except BaseException as exc:
                failures.append(exc)

        watermark = _provider_id_watermark()
        threads = [
            threading.Thread(target=build, args=(index,), daemon=True)
            for index in range(workers)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(15)
        self.assertTrue(all(not thread.is_alive() for thread in threads))
        self.assertEqual(failures, [])
        self.assertTrue(all(provider is not None for provider in providers))

        # `_registered_ids_since` also asserts the two maps stay paired.
        new_ids = _registered_ids_since(watermark)
        self.assertEqual(len(new_ids), workers)

        providers.clear()
        self.assertEqual(_settled_registered_ids_since(watermark), set())

    # A path that reaches native build() rather than a Python pre-check: the
    # setter only stores it, and build() is what opens it. Anything rejected
    # earlier (an empty client_id, say, which `_oidc_required_utf8` refuses)
    # never reaches the registry insert and so cannot exercise this at all.
    _UNREADABLE_CA_BUNDLE = '/nonexistent/questdb-oidc-review/ca.pem'

    def _construct_and_fail_in_build(self, target=None):
        """Drive a native build() failure, optionally re-``__init__``-ing
        ``target``. Asserts the failure really came from build()."""
        args = ('questdb', 'https://idp.example/device',
                'https://idp.example/token')
        kwargs = dict(interactive=False, open_browser=False,
                      renderer=Renderer(),
                      ca_bundle=self._UNREADABLE_CA_BUNDLE)
        with self.assertRaises(OidcError) as caught:
            if target is None:
                OidcDeviceAuth(*args, **kwargs)
            else:
                target.__init__(*args, **kwargs)
        # Pin the trigger: if a future change starts rejecting `ca_bundle`
        # before the builder is populated, this test would silently stop
        # covering the post-registration path.
        self.assertIn('CA bundle', str(caught.exception))

    def test_ca_bundle_rejects_unexpanded_home_prefix(self):
        for bad in ('~', '~/ca.pem', '~\\ca.pem'):
            with self.subTest(path=bad):
                with self.assertRaisesRegex(
                        OidcError, 'already-expanded absolute path') as caught:
                    make_auth(ca_bundle=bad)
                self.assertIs(
                    caught.exception.code,
                    questdb.QuestDBErrorCode.ConfigError)
        # A tilde away from the leading path component is not expansion syntax;
        # it reaches the ordinary build-time file-open error instead.
        with self.assertRaises(OidcError) as caught:
            make_auth(ca_bundle='dir/has~tilde.pem')
        self.assertNotIn('already-expanded', str(caught.exception))

    def test_insecure_gates_plaintext_discovery(self):
        # `insecure` was only ever type-checked: nothing asserted it DOES
        # anything, so dropping the setter call would have left the suite green.
        # The test server is loopback, where plaintext is allowed either way, so
        # it cannot discriminate -- this uses TEST-NET-1 (RFC 5737), which is
        # non-loopback and guaranteed unroutable, and a 1s timeout to bound the
        # permitted case.
        common = dict(interactive=False, open_browser=False,
                      renderer=Renderer(), timeout=1)
        url = 'http://192.0.2.1:9000'

        # Refused at the config gate, before any socket is opened.
        with self.assertRaises(OidcConfigError) as refused:
            OidcDeviceAuth.from_questdb(url, insecure=False, **common)
        self.assertIn('insecure', str(refused.exception).lower())

        # Permitted: it gets as far as the network and fails there instead,
        # which is what proves the gate was lifted rather than merely moved.
        with self.assertRaises(OidcNetworkError):
            OidcDeviceAuth.from_questdb(url, insecure=True, **common)

    def test_registry_entry_is_dropped_the_moment_build_fails(self):
        # The invariant is *registered <=> built*, and it has to hold at the
        # moment of failure -- not merely by the time the object is collected.
        # Asserting it only after the failed object is dropped would prove
        # nothing because its weakref callback cleans up then. Holding the
        # half-built object alive is what makes this discriminate.
        self._construct_and_fail_in_build()  # warm one-time module state
        watermark = _provider_id_watermark()
        auth = OidcDeviceAuth.__new__(OidcDeviceAuth)
        self._construct_and_fail_in_build(target=auth)
        # No collection needed or wanted: `auth` is alive, so an entry left
        # behind here can only be the unwind's fault.
        self.assertEqual(
            _registered_ids_since(watermark), set(),
            'a failed native build() left its `_OIDC_PROVIDERS` entry behind; '
            'the registry must not hold an entry for a provider that was '
            'never built')
        del auth
        self.assertEqual(_settled_registered_ids_since(watermark), set())

    def test_registry_drains_when_init_is_retried_after_failed_build(self):
        # A failed build leaves the provider's borrowed raw handle NULL, so the
        # already-initialized guard does not fire on a retry. Without the unwind,
        # a second `__init__` would overwrite its provider id and strand both old
        # registry entries for the life of the process.
        watermark = _provider_id_watermark()
        for _ in range(self._LEAK_ITERS):
            auth = OidcDeviceAuth.__new__(OidcDeviceAuth)
            self._construct_and_fail_in_build(target=auth)
            # The retry succeeds, so the object ends up live and built.
            before_retry = _provider_id_watermark()
            auth.__init__(
                'questdb', 'https://idp.example/device',
                'https://idp.example/token',
                interactive=False, open_browser=False, renderer=Renderer())
            # Exactly the live retry is registered: every earlier iteration's
            # provider and this iteration's failed build are gone.
            live_id = _registered_ids_since(before_retry)
            self.assertEqual(len(live_id), 1)
            self.assertEqual(
                _settled_registered_ids_since(watermark, live_id), live_id)
            del auth
        self.assertEqual(
            _settled_registered_ids_since(watermark), set(),
            f'the provider registry grew over {self._LEAK_ITERS} '
            f'failed-then-retried initializations')

    @unittest.skipIf(
        platform.python_implementation() == 'PyPy',
        "Exact weakref bookkeeping (getweakrefcount over the binding's "
        'PyWeakref_NewRef) is CPython refcount semantics; PyPy cpyext does not '
        'guarantee the same count.')
    def test_provider_holds_exactly_one_registry_weakref(self):
        # Directly observe the registry bookkeeping: _finish_builder stores
        # exactly one weakref to the provider in _OIDC_PROVIDERS (oidc.pxi).
        # Nothing else references the provider weakly, so getweakrefcount sees
        # exactly that one -- its callback must later pop both registry entries.
        auth = make_auth(renderer=Renderer())
        self.assertEqual(weakref.getweakrefcount(auth), 1)
        (registry_ref,) = weakref.getweakrefs(auth)  # exactly one; unpack asserts it
        self.assertIs(registry_ref(), auth)
        # A second provider gets its own independent weakref, not a shared one.
        other = make_auth(renderer=Renderer())
        self.assertEqual(weakref.getweakrefcount(other), 1)
        self.assertIsNot(weakref.getweakrefs(other)[0], registry_ref)


class ProviderCycleSafetyTest(unittest.TestCase):
    """A provider reclaimed as part of a reference cycle stays usable.

    The cyclic collector runs weakref callbacks BEFORE finalizers, so while
    `_OIDC_NATIVE_HANDLES` was the sole owner of the native handle the registry
    callback freed it and any `__del__` in the same cycle then dereferenced a
    dangling pointer. Both shapes below segfaulted the interpreter with no
    traceback, so they run out-of-process and assert on the exit status: an
    in-process regression would take the whole test run down with it.
    """

    def _run(self, body, *, expect_empty_stderr=False):
        # `settle()` collects until the child reaches the state the test is
        # about, rather than until the count stops moving.
        #
        # A single `gc.collect()` is enough on CPython, where the last decref
        # runs the finalizer and the weakref callback back to back. PyPy does
        # not refcount and may stage collectable cpyext finalization across
        # several collections -- notably the extension-subclass self-cycle
        # below -- so the caller states the target rather than waiting for a
        # plateau. An uncollected cycle is itself a plateau.
        #
        # `expect` requires the registry to drain, and `done()` requires that
        # `__del__` actually ran: a collector may clear the weakref one pass
        # before running the finalizer. A cycle that genuinely cannot be
        # collected exhausts every pass and still returns a non-zero count.
        # PyPy's known cross-heap renderer/provider cycle is skipped separately
        # below; no number of collections can reclaim that cpyext shape.
        shutdown_stderr_marker = '__QDB_BEGIN_INTERPRETER_SHUTDOWN__'
        script = (
            'import gc, sys\n'
            'from questdb._client import OidcDeviceAuth\n'
            'from questdb._client import _debug_oidc_registry_size as sz\n'
            'def settle(done=None, expect=0):\n'
            '    count = None\n'
            '    for _ in range({max_passes}):\n'
            '        gc.collect()\n'
            '        count = sz()\n'
            '        if count == expect and (done is None or done()):\n'
            '            break\n'
            '    return count\n'.format(max_passes=_SETTLE_MAX_PASSES)
        ) + body
        if expect_empty_stderr:
            # Import/build warnings precede this marker (notably two known PyPy
            # cpyext/Cython warnings). Only diagnostics emitted by finalization
            # itself belong to these shutdown tests.
            script += (
                f'\nprint({shutdown_stderr_marker!r}, '
                'file=sys.stderr, flush=True)\n')
        # Resolve `questdb` the way this process did instead of assuming the
        # in-place `src/` build. Under cibuildwheel the package is installed
        # from the wheel and `src/questdb/` holds no compiled `_client`, so
        # putting `src` first shadowed the installed package with a source tree
        # that cannot import itself -- every child died in
        # `src/questdb/__init__.py` on `from questdb import _client` with a
        # partially-initialized-module ImportError, long before reaching the
        # cycle these tests are about. Same idiom as
        # `test_dataframe.TestNoPyarrow`.
        env = dict(os.environ)
        env['PYTHONPATH'] = os.pathsep.join(
            [os.path.dirname(os.path.dirname(os.path.abspath(questdb.__file__)))]
            + [p for p in env.get('PYTHONPATH', '').split(os.pathsep) if p])
        proc = subprocess.run(
            [sys.executable, '-c', script],
            capture_output=True, text=True, timeout=120, env=env)
        self.assertEqual(
            proc.returncode, 0,
            'provider cycle collection crashed the interpreter '
            '(exit {}): {}{}'.format(
                proc.returncode, proc.stdout, proc.stderr))
        if expect_empty_stderr:
            _, marker, shutdown_stderr = proc.stderr.rpartition(
                shutdown_stderr_marker + '\n')
            self.assertTrue(
                marker,
                'child exited without reaching the shutdown checkpoint: '
                f'{proc.stderr}')
            self.assertEqual(
                shutdown_stderr, '',
                'interpreter shutdown emitted diagnostics: '
                f'{shutdown_stderr}')
        return proc.stdout

    def test_collected_store_provider_drains_and_exits_cleanly(self):
        # A provider built with a token store is the only kind that installs
        # `_oidc_diagnostic_trampoline`, so it is the only kind whose
        # `_OidcNativeHandle.__dealloc__` reaches
        # `questdb_oidc_auth_detach_diagnostics`. That dealloc releases the GIL
        # around the native call, so this covers the plain path through it:
        # the provider is still collected, the registry still drains, and the
        # interpreter still exits 0 rather than hanging or crashing.
        #
        # Deliberately scoped to that. It does NOT prove the detach contract --
        # nothing here emits a diagnostic, so a dealloc that skipped the detach
        # entirely would pass. The contract itself (a callback in flight is
        # drained, later ones are suppressed, siblings keep delivering, and a
        # re-entrant call does not deadlock) is covered natively in
        # `questdb-rs-ffi/src/oidc.rs`, where a callback can be driven
        # synchronously; from Python it would need a real store failure racing
        # a collection.
        #
        # Out-of-process because the failure modes it does cover are a hang or
        # an interpreter crash, neither of which an in-process assertion
        # survives.
        out = self._run(
            'import tempfile\n'
            'from questdb.auth import FileTokenStore\n'
            'directory = tempfile.mkdtemp()\n'
            'auth = OidcDeviceAuth(\n'
            '    "questdb",\n'
            '    "https://idp.example/device",\n'
            '    "https://idp.example/token",\n'
            '    interactive=False, open_browser=False,\n'
            '    token_store=FileTokenStore.at(directory))\n'
            'assert sz() == 1, sz()\n'
            'del auth\n'
            'print(settle())\n')
        self.assertEqual(out.strip().splitlines()[-1], '0')

    def test_real_shutdown_with_live_stored_provider_and_sender(self):
        # Leave both objects live: this exercises the registered atexit hook at
        # Py_Finalize rather than calling it by hand or collecting first.
        out = self._run(
            'import tempfile, questdb\n'
            'from questdb.auth import FileTokenStore\n'
            'directory = tempfile.mkdtemp()\n'
            'auth = OidcDeviceAuth(\n'
            '    "questdb", "https://idp.example/device",\n'
            '    "https://idp.example/token", interactive=False,\n'
            '    open_browser=False,\n'
            '    token_store=FileTokenStore.at(directory))\n'
            'sender = questdb.Sender(\n'
            '    questdb.Protocol.Http, "127.0.0.1", 1,\n'
            '    oidc_auth=auth, auto_flush=False)\n'
            'print("ready")\n',
            expect_empty_stderr=True)
        self.assertIn('ready', out)

    def test_real_shutdown_with_renderer_callback_in_flight(self):
        # A daemon sign-in is parked inside managed renderer code when the main
        # thread exits. The atexit hook must publish non-waiting detach and let
        # Py_Finalize complete rather than draining a callback that cannot
        # return until after shutdown.
        out = self._run(
            'import http.server, json, threading\n'
            'entered = threading.Event()\n'
            'block = threading.Event()\n'
            'class H(http.server.BaseHTTPRequestHandler):\n'
            '    def log_message(self, *args): pass\n'
            '    def do_POST(self):\n'
            '        body = json.dumps({\n'
            '            "device_code": "dev", "user_code": "CODE",\n'
            '            "verification_uri": "http://127.0.0.1/verify",\n'
            '            "expires_in": 60, "interval": 5}).encode()\n'
            '        self.send_response(200)\n'
            '        self.send_header("Content-Type", "application/json")\n'
            '        self.send_header("Content-Length", str(len(body)))\n'
            '        self.end_headers(); self.wfile.write(body)\n'
            'server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), H)\n'
            'threading.Thread(target=server.serve_forever, daemon=True).start()\n'
            'class R:\n'
            '    def on_prompt(self, details): entered.set(); block.wait()\n'
            '    def on_waiting(self, seconds): pass\n'
            '    def on_success(self, identity, expires): pass\n'
            '    def on_failure(self, message): pass\n'
            'port = server.server_address[1]\n'
            'auth = OidcDeviceAuth(\n'
            '    "questdb", f"http://127.0.0.1:{port}/device",\n'
            '    f"http://127.0.0.1:{port}/token",\n'
            '    renderer=R(), open_browser=False, timeout=5)\n'
            'def run():\n'
            '    try: auth.sign_in()\n'
            '    except BaseException: pass\n'
            'threading.Thread(target=run, daemon=True).start()\n'
            'assert entered.wait(20), "renderer callback did not start"\n'
            'print("parked")\n',
            expect_empty_stderr=True)
        self.assertIn('parked', out)

    @unittest.skipIf(
        platform.python_implementation() == 'PyPy',
        'PyPy leaks reference cycles crossing a C-extension object (PyPy '
        'issue #3848), so the renderer finalizer cannot run for this shape. '
        'The equivalent renderer/provider collection test above is skipped '
        'for the same reason. The collectable extension-subclass cycle below '
        'still exercises finalizer/handle safety on PyPy; this cross-heap '
        'ordering case remains fully covered on CPython.')
    def test_renderer_cycle_close_from_del_is_safe(self):
        out = self._run(
            'closed = []\n'
            'class R:\n'
            '    def __init__(self): self.provider = None\n'
            '    def on_prompt(self, d): pass\n'
            '    def on_waiting(self, s): pass\n'
            '    def on_success(self, i, e): pass\n'
            '    def on_failure(self, m): pass\n'
            '    def __del__(self):\n'
            '        if self.provider is not None:\n'
            '            self.provider.close()\n'
            '            closed.append(1)\n'
            '            print("closed")\n'
            'r = R()\n'
            'a = OidcDeviceAuth("cid", "https://i/d", "https://i/t", renderer=r)\n'
            'r.provider = a\n'
            'del a, r\n'
            'print("registry", settle(lambda: closed))\n')
        self.assertIn('closed', out, 'the finalizer never ran')
        self.assertIn(
            'registry 0', out,
            'the cycle must still be collectable: owning the handle from the '
            'provider must not keep the cycle alive')

    def test_subclass_self_cycle_close_from_del_is_safe(self):
        out = self._run(
            'closed = []\n'
            'class Sub(OidcDeviceAuth):\n'
            '    def __del__(self):\n'
            '        self.close()\n'
            '        closed.append(1)\n'
            '        print("closed")\n'
            'a = Sub("cid", "https://i/d", "https://i/t")\n'
            'a.self_ref = a\n'
            'del a\n'
            'print("registry", settle(lambda: closed))\n')
        self.assertIn('closed', out, 'the finalizer never ran')
        self.assertIn('registry 0', out)

    def test_registry_bookkeeping_never_owns_the_native_handle(self):
        """Losing a registry entry must not free a live provider's handle.

        `_oidc_provider_collected` is a module-level `def`, and the weakref it
        is called with is reachable through `weakref.getweakrefs(provider)`, so
        a caller can drive it against a live provider. That used to release the
        native handle out from under it; now it is bookkeeping only.
        """
        # Out-of-process for the same reason as its siblings, and because the
        # id counter is module-global: in the aggregated run it has already
        # advanced past anything this test could sweep.
        out = self._run(
            'import weakref\n'
            'import questdb._client as c\n'
            'a = OidcDeviceAuth("cid", "https://i/d", "https://i/t")\n'
            'client_id = a.config.client_id\n'
            '(ref,) = weakref.getweakrefs(a)\n'
            'for pid in range(1, 8):\n'
            '    c._oidc_provider_collected(pid, ref)\n'
            'print("registry", sz())\n'
            'assert a.config.client_id == client_id\n'
            'a.close()\n'
            'print("usable")\n')
        # Bookkeeping is gone, the provider is not: reading the native config
        # view and closing both dereference the handle the registry used to own.
        self.assertIn('registry 0', out)
        self.assertIn('usable', out)


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

    def test_sign_in_in_flight_is_visible_to_the_retry_gate(self):
        # Regression: `_is_oidc_terminal_for_foreground` failed a foreground
        # `dataframe()` fast for EVERY `OidcInteractionRequired`. Three native
        # conditions share that class, and `classify_provider_error` gives them
        # all the same retryable `SocketError`, so neither the class nor the
        # code separates "nobody has signed in" -- where failing fast is right
        # -- from "a peer sign-in or a renderer paint is in flight", which
        # `oidc.h` says a transport must retry. Only the provider knows, and
        # this is what it is asked.
        waiting = threading.Event()

        class WaitingRenderer(RecordingRenderer):
            def on_waiting(self, seconds_left):
                super().on_waiting(seconds_left)
                waiting.set()

        pending = (400, {'error': 'authorization_pending'}, None)
        with OidcTestServer(
                device_token_response=pending, device_expires_in=20) as server:
            auth = make_discovered_auth(server, renderer=WaitingRenderer())
            self.assertFalse(auth._sign_in_in_progress)

            def sign_in():
                try:
                    auth.sign_in()
                except BaseException:
                    pass

            thread = threading.Thread(target=sign_in)
            thread.start()
            busy_error = None
            try:
                self.assertTrue(
                    waiting.wait(20), 'device flow never reached a poll')
                self.assertTrue(
                    auth._sign_in_in_progress,
                    'a sign_in() in flight must be visible to the gate')
                with self.assertRaises(OidcInteractionRequired) as raised:
                    auth.token()
                busy_error = raised.exception
                self.assertTrue(
                    getattr(busy_error, '_acquisition_busy', False),
                    'native busy classification was lost in error conversion')
            finally:
                auth.close()
                thread.join(20)
            self.assertFalse(thread.is_alive())
            # Provider state has now cleared, but the captured error still says
            # why that acquisition failed and remains retryable.
            self.assertFalse(auth._sign_in_in_progress)
            self.assertFalse(
                _client._debug_is_oidc_terminal_for_foreground(
                    busy_error, auth))

    def test_close_cancels_device_polling_and_is_permanent(self):
        waiting = threading.Event()

        class WaitingRenderer(RecordingRenderer):
            def on_waiting(self, seconds_left):
                super().on_waiting(seconds_left)
                waiting.set()

        renderer = WaitingRenderer()
        result = []
        pending = (400, {'error': 'authorization_pending'}, None)
        # Short device lifetime, per the fixture's own warning: this asserts a
        # cancellation, so if that ever regresses the sign-in must fail fast
        # rather than block for the 600s default -- which the CI watchdog would
        # re-arm past, turning a test failure into a stall. Both sibling
        # cancellation tests already pass it.
        with OidcTestServer(
                device_token_response=pending, device_expires_in=20) as server:
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
            # The regression is a stall until the 20s device-code lifetime
            # expires, so 10s still discriminates while leaving room for an
            # in-flight loopback request to unwind on a slow agent.
            self.assertLess(time.monotonic() - started, 10)
            worker.join(10)
            self.assertFalse(worker.is_alive())

        self.assertEqual(len(result), 1)
        self.assertIsInstance(result[0], OidcCancelledError)
        # clear() is excluded: it is pure teardown and must outlive close(),
        # which drops the in-memory credential but leaves the persisted entry.
        auth.clear()
        for op in (auth.sign_in, auth.token, auth.headers):
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
        # A short device-code lifetime so a regressed cancellation surfaces as a
        # fast failure rather than a multi-minute stall that only the job
        # timeout would notice.
        with OidcTestServer(
                device_token_response=pending, device_expires_in=20) as server:
            auth = make_discovered_auth(
                server, renderer=InterruptingRenderer())
            # Attach before the interruption: the regression was specifically
            # that cancelling a re-auth killed every pre-existing transport.
            sender = questdb.Sender.from_conf(
                f'http::addr=127.0.0.1:{server.port};',
                oidc_auth=auth, auto_flush=False)
            try:
                started = time.monotonic()
                with self.assertRaises(KeyboardInterrupt):
                    auth.sign_in()
                # Promptly, not after the device code expires.
                self.assertLess(time.monotonic() - started, 10)
                # The attempt ended, not the provider. With no token yet an
                # attached consumer sees the ordinary recoverable condition.
                with self.assertRaises(OidcInteractionRequired):
                    auth.token()

                # Authorize a retry on the SAME provider. It succeeds, and the
                # sender attached before Ctrl-C can still authenticate and send.
                server.device_token_response = None
                auth.sign_in()
                self.assertEqual(auth.token(), server.initial_access_token)
                sender.establish()
                sender.row(
                    'after_cancel', columns={'value': 1},
                    at=questdb.ServerTimestamp)
                sender.flush()
            finally:
                sender.close(flush=False)
        self.assertEqual(len(server.requests('/write', 'POST')), 1)

    def test_system_exit_in_renderer_aborts_sign_in(self):
        # The dispatch parks `(KeyboardInterrupt, SystemExit)`, but only the
        # Ctrl-C half was covered: narrowing that tuple to KeyboardInterrupt
        # left the suite green while a SystemExit raised from a renderer -- an
        # atexit-driven shutdown, or sys.exit() from a notebook Cancel button --
        # fell through to the `except BaseException` logger, was swallowed, and
        # left sign_in() polling to the device-code deadline.
        class ExitingRenderer(RecordingRenderer):
            def on_waiting(self, seconds_left):
                super().on_waiting(seconds_left)
                raise SystemExit(3)

        pending = (400, {'error': 'authorization_pending'}, None)
        with OidcTestServer(
                device_token_response=pending, device_expires_in=20) as server:
            auth = make_discovered_auth(server, renderer=ExitingRenderer())
            started = time.monotonic()
            with self.assertRaises(SystemExit) as ctx:
                auth.sign_in()
            # The original exception is re-raised, not a fresh one.
            self.assertEqual(ctx.exception.code, 3)
            self.assertLess(time.monotonic() - started, 10)
        # SystemExit cancels only this attempt, just like KeyboardInterrupt.
        # The provider remains open and reports the recoverable no-token state.
        with self.assertRaises(OidcInteractionRequired):
            auth.token()

    @unittest.skipUnless(
        hasattr(signal, 'setitimer'), 'needs signal.setitimer')
    def test_signal_handler_exception_interrupts_sign_in(self):
        # A signal-driven timeout (a SIGALRM handler, pytest-timeout's signal
        # method) raises from whatever Python bytecode runs next on the main
        # thread -- during sign_in() that is a renderer callback. Its exception
        # was taken for a renderer bug, logged, and swallowed: the timeout
        # never fired and sign_in() polled until the device code expired.
        class Timeout(Exception):
            pass

        def on_alarm(signum, frame):
            raise Timeout('deadline')

        pending = (400, {'error': 'authorization_pending'}, None)
        previous = signal.signal(signal.SIGALRM, on_alarm)
        try:
            with OidcTestServer(
                    device_token_response=pending, device_expires_in=20,
                    device_interval=1) as server:
                auth = make_discovered_auth(
                    server, renderer=RecordingRenderer())
                try:
                    started = time.monotonic()
                    signal.setitimer(signal.ITIMER_REAL, 0.5)
                    with self.assertRaises(Timeout):
                        auth.sign_in()
                    self.assertLess(time.monotonic() - started, 10)
                    # The attempt ended; the provider did not.
                    with self.assertRaises(OidcInteractionRequired):
                        auth.token()
                finally:
                    signal.setitimer(signal.ITIMER_REAL, 0)
                    auth.close()
        finally:
            signal.signal(signal.SIGALRM, previous)

    @unittest.skipUnless(hasattr(signal, 'SIGALRM'), 'SIGALRM required')
    def test_signal_raised_inside_renderer_is_not_a_renderer_failure(self):
        # A signal arriving *after* PyErr_CheckSignals, during user renderer
        # bytecode, must cancel sign-in even when its handler raises a regular
        # Exception. One-shot handlers may unregister themselves before raising.
        class Deadline(Exception):
            pass

        class AlarmRenderer(RecordingRenderer):
            def on_prompt(self, response):
                signal.raise_signal(signal.SIGALRM)
                super().on_prompt(response)

        for exc_type in (TimeoutError, Deadline):
            with self.subTest(exc_type=exc_type.__name__):
                previous = signal.getsignal(signal.SIGALRM)

                def on_alarm(signum, frame):
                    if exc_type is Deadline:
                        signal.signal(signum, signal.SIG_IGN)
                    raise exc_type('deadline inside renderer')

                signal.signal(signal.SIGALRM, on_alarm)
                try:
                    with OidcTestServer(device_expires_in=8) as server:
                        auth = make_discovered_auth(
                            server, renderer=AlarmRenderer())
                        try:
                            started = time.monotonic()
                            with self.assertRaises(exc_type):
                                auth.sign_in()
                            self.assertLess(time.monotonic() - started, 6)
                            with self.assertRaises(OidcInteractionRequired):
                                auth.token()
                        finally:
                            auth.close()
                finally:
                    signal.signal(signal.SIGALRM, previous)

    @unittest.skipUnless(hasattr(signal, 'SIGUSR1'), 'SIGUSR1 required')
    def test_renderer_sharing_signal_handler_code_does_not_cancel_sign_in(self):
        # Two callbacks from the same factory share __code__, but invoking
        # one as a renderer callback does not deliver a signal to the other.
        def make_callback():
            def callback(*args):
                raise TimeoutError('ordinary renderer failure; no signal')
            return callback

        installed_handler = make_callback()
        renderer = RecordingRenderer()
        renderer.on_prompt = make_callback()
        self.assertIs(
            installed_handler.__code__, renderer.on_prompt.__code__)
        previous = signal.signal(signal.SIGUSR1, installed_handler)
        try:
            with OidcTestServer() as server:
                auth = make_discovered_auth(server, renderer=renderer)
                try:
                    with self.assertLogs('questdb', level='ERROR'):
                        auth.sign_in()
                    self.assertEqual(auth.token(), 'AT-initial')
                finally:
                    auth.close()
        finally:
            signal.signal(signal.SIGUSR1, previous)

    @unittest.skipUnless(hasattr(signal, 'SIGUSR1'), 'SIGUSR1 required')
    def test_sign_in_does_not_inspect_unrelated_handler_properties(self):
        class SignalHandler:
            def __init__(self):
                self.lookups = 0
                self.called = False

            @property
            def __code__(self):
                self.lookups += 1
                raise RuntimeError('unrelated signal handler was inspected')

            def __call__(self, signum, frame):
                self.called = True
                raise TimeoutError('actual signal reached callable handler')

        class AlarmRenderer(RecordingRenderer):
            def on_prompt(self, response):
                signal.raise_signal(signal.SIGUSR1)

        handler = SignalHandler()
        previous = signal.signal(signal.SIGUSR1, handler)
        try:
            with OidcTestServer() as server:
                auth = make_discovered_auth(server)
                try:
                    auth.sign_in()
                    self.assertEqual(auth.token(), 'AT-initial')
                    self.assertFalse(handler.called)
                    self.assertEqual(handler.lookups, 0)
                finally:
                    auth.close()
            # Avoiding instance-property access must not hide a real signal
            # delivered to the same callable handler on another sign-in.
            with OidcTestServer() as server:
                auth = make_discovered_auth(
                    server, renderer=AlarmRenderer())
                try:
                    with self.assertRaisesRegex(
                            TimeoutError, 'actual signal reached'):
                        auth.sign_in()
                    self.assertTrue(handler.called)
                    self.assertEqual(handler.lookups, 0)
                finally:
                    auth.close()
        finally:
            signal.signal(signal.SIGUSR1, previous)

    @unittest.skipUnless(hasattr(signal, 'SIGUSR1'), 'SIGUSR1 required')
    def test_varargs_signal_handler_still_interrupts_sign_in(self):
        class AlarmRenderer(RecordingRenderer):
            def on_prompt(self, response):
                signal.raise_signal(signal.SIGUSR1)

        def on_alarm(*args):
            raise TimeoutError('actual signal delivered inside renderer')

        previous = signal.signal(signal.SIGUSR1, on_alarm)
        try:
            with OidcTestServer() as server:
                auth = make_discovered_auth(
                    server, renderer=AlarmRenderer())
                try:
                    with self.assertRaisesRegex(
                            TimeoutError, 'actual signal delivered'):
                        auth.sign_in()
                    with self.assertRaises(OidcInteractionRequired):
                        auth.token()
                finally:
                    auth.close()
        finally:
            signal.signal(signal.SIGUSR1, previous)

    def test_interrupt_raised_while_logging_a_renderer_failure_aborts_sign_in(
            self):
        # A failing renderer is logged, and the logging call is bytecode too:
        # a Ctrl-C delivered inside it (or raised by a handler) escaped the
        # `noexcept` callback as an unraisable exception and was lost.
        class FailingRenderer(RecordingRenderer):
            def on_waiting(self, seconds_left):
                super().on_waiting(seconds_left)
                raise TypeError('renderer bug')

        class InterruptingHandler(logging.Handler):
            def emit(self, record):
                raise KeyboardInterrupt

        pending = (400, {'error': 'authorization_pending'}, None)
        logger = logging.getLogger('questdb')
        handler = InterruptingHandler()
        logger.addHandler(handler)
        try:
            with OidcTestServer(
                    device_token_response=pending, device_expires_in=20,
                    device_interval=1) as server:
                auth = make_discovered_auth(server, renderer=FailingRenderer())
                try:
                    started = time.monotonic()
                    with self.assertRaises(KeyboardInterrupt):
                        auth.sign_in()
                    self.assertLess(time.monotonic() - started, 10)
                finally:
                    auth.close()
        finally:
            logger.removeHandler(handler)

    @unittest.skipUnless(hasattr(signal, 'SIGALRM'), 'SIGALRM required')
    def test_signal_inside_renderer_failure_logging_aborts_sign_in(self):
        class FailingRenderer(RecordingRenderer):
            def on_prompt(self, response):
                raise TypeError('renderer bug')

        class AlarmHandler(logging.Handler):
            def emit(self, record):
                signal.raise_signal(signal.SIGALRM)

        def on_alarm(signum, frame):
            raise TimeoutError('deadline during renderer logging')

        logger = logging.getLogger('questdb')
        handler = AlarmHandler()
        previous = signal.signal(signal.SIGALRM, on_alarm)
        logger.addHandler(handler)
        try:
            with OidcTestServer(device_expires_in=8) as server:
                auth = make_discovered_auth(server, renderer=FailingRenderer())
                try:
                    with self.assertRaises(TimeoutError):
                        auth.sign_in()
                finally:
                    auth.close()
        finally:
            logger.removeHandler(handler)
            signal.signal(signal.SIGALRM, previous)

    def test_concurrent_sign_in_cannot_steal_callback_interrupt(self):
        # The steal window opens when the renderer's KeyboardInterrupt is parked
        # on the provider and closes when the first sign_in() -- after native
        # has unwound the cancelled flow and it has re-taken the GIL -- reads
        # it back. A second sign_in() that entered native in between would
        # reset or consume that slot. The window cannot be entered on cue (no
        # user code runs inside it), so the second thread keeps attempting
        # sign_in() from before the interrupt is raised until the first thread
        # has finished: each attempt must be refused outright, and the first
        # must still get its own interrupt back.
        callback_entered = threading.Event()
        second_attempted = threading.Event()
        # Set when the second thread's sign_in() legitimately got in: the
        # first call had already released its lock and returned. That is not a
        # steal, so the loop stops there instead of recording it.
        late_entry = threading.Event()
        first_ident = []

        class InterruptingRenderer(RecordingRenderer):
            def on_waiting(self, seconds_left):
                super().on_waiting(seconds_left)
                if threading.get_ident() != first_ident[0]:
                    late_entry.set()
                    auth.cancel_sign_in()
                    return
                callback_entered.set()
                if not second_attempted.wait(5):
                    raise AssertionError('concurrent sign-in was never attempted')
                raise KeyboardInterrupt

        pending = (400, {'error': 'authorization_pending'}, None)
        first_result = []
        second_results = []
        attempts_after_interrupt = [0]
        with OidcTestServer(
                device_token_response=pending,
                device_expires_in=20) as server:
            auth = make_discovered_auth(
                server, renderer=InterruptingRenderer())

            def first_sign_in():
                first_ident.append(threading.get_ident())
                try:
                    auth.sign_in()
                except BaseException as exc:
                    first_result.append(exc)

            first = threading.Thread(target=first_sign_in, daemon=True)

            def second_sign_in():
                deadline = time.monotonic() + 10
                while time.monotonic() < deadline:
                    interrupted = second_attempted.is_set()
                    try:
                        auth.sign_in()
                    except OidcError as exc:
                        if late_entry.is_set():
                            # The first call had already released its lock,
                            # so this entry is legitimate rather than a
                            # steal. It is still an attempt that ran after
                            # the interrupt, so count it before stopping.
                            if interrupted:
                                attempts_after_interrupt[0] += 1
                            return
                        # The expected refusal. Keep only the anomalies: this
                        # loop can run many thousands of times.
                        if 'already in progress' not in str(exc):
                            second_results.append(exc)
                    except BaseException as exc:
                        second_results.append(exc)
                    else:
                        second_results.append(None)
                    if interrupted:
                        attempts_after_interrupt[0] += 1
                    second_attempted.set()
                    # Only the first iteration runs before the interrupt: it
                    # is the `second_attempted` above that releases the
                    # renderer to raise one. Stopping as soon as the first
                    # thread is gone therefore leaves nothing probed after
                    # the interrupt whenever this thread loses the CPU while
                    # that call unwinds -- which is how a loaded CI agent
                    # sees it. Keep going until at least one attempt has
                    # been made after the interrupt; once the first call is
                    # gone, that attempt gets in, trips `late_entry`, and
                    # returns above.
                    if not first.is_alive() and attempts_after_interrupt[0]:
                        return

            first.start()
            self.assertTrue(
                callback_entered.wait(5), 'first sign-in did not render')
            second = threading.Thread(target=second_sign_in, daemon=True)
            second.start()
            first.join(10)
            second.join(10)

        self.assertFalse(first.is_alive())
        self.assertFalse(second.is_alive())
        self.assertEqual(len(first_result), 1)
        self.assertIsInstance(first_result[0], KeyboardInterrupt)
        # Some attempts ran while the interrupt was being raised, parked and
        # handed back, not merely before it.
        self.assertGreater(attempts_after_interrupt[0], 0)
        self.assertEqual(
            second_results, [],
            'a concurrent sign_in() was not refused outright')

    def test_default_interval_reaches_native_when_idp_omits_interval(self):
        # Validation alone was covered, and every /device response carried an
        # `interval`, which takes precedence -- so deleting the native setter
        # call left the suite green. The prompt reports the interval the
        # polling loop will actually use; cancel from it rather than wait one.
        class CancellingRenderer(RecordingRenderer):
            auth = None

            def on_prompt(self, response):
                super().on_prompt(response)
                self.auth.cancel_sign_in()

        cases = (
            # (advertised by the IdP, default_interval=, used)
            (None, 11, 11),
            (None, 29, 29),
            (7, 29, 7),
        )
        for advertised, configured, expected in cases:
            with self.subTest(advertised=advertised, configured=configured):
                with OidcTestServer(device_interval=advertised) as server:
                    renderer = CancellingRenderer()
                    auth = make_discovered_auth(
                        server, renderer=renderer,
                        default_interval=configured)
                    renderer.auth = auth
                    try:
                        with self.assertRaises(OidcCancelledError):
                            auth.sign_in()
                    finally:
                        auth.close()
                self.assertEqual(len(renderer.prompts), 1)
                self.assertEqual(renderer.prompts[0]['interval'], expected)

    def _sabotaging_renderer(self, directory):
        class SabotageRenderer(RecordingRenderer):
            def on_prompt(self, challenge):
                super().on_prompt(challenge)
                # Replace the preflighted store directory with a regular file,
                # so the durable save after the device flow fails.
                shutil.rmtree(directory)
                with open(directory, 'w', encoding='utf-8') as sink:
                    sink.write('not a directory')
        return SabotageRenderer()

    class _RaisingHandler(logging.Handler):
        def __init__(self, exc):
            super().__init__()
            self.exc = exc
            self.fired = 0

        def emit(self, record):
            self.fired += 1
            raise self.exc

    def _with_questdb_handler(self, handler):
        logger = logging.getLogger('questdb')
        old_level = logger.level
        logger.addHandler(handler)
        logger.setLevel(logging.WARNING)

        def restore():
            logger.removeHandler(handler)
            logger.setLevel(old_level)
        self.addCleanup(restore)

    @unittest.skipUnless(
        os.name == 'posix', 'durable file token store requires POSIX')
    def test_interrupt_in_persistence_diagnostic_reaches_sign_in(self):
        # The persistence diagnostic runs `logging` -- Python bytecode -- on
        # the sign_in() thread while the GIL is otherwise released, so it is
        # where CPython delivers a pending Ctrl-C. Its `except BaseException:
        # pass` swallowed that and cleared the tripped-signal flag. A handler
        # raising the interrupt stands in for the signal deterministically.
        for exc in (KeyboardInterrupt(), SystemExit(4)):
            with self.subTest(exc=type(exc).__name__):
                with tempfile.TemporaryDirectory() as parent:
                    directory = os.path.join(parent, 'store')
                    with OidcTestServer() as server:
                        auth = make_discovered_auth(
                            server,
                            token_store=FileTokenStore.at(directory),
                            renderer=self._sabotaging_renderer(directory))
                        handler = self._RaisingHandler(exc)
                        self._with_questdb_handler(handler)
                        with self.assertRaises(type(exc)) as ctx:
                            auth.sign_in()
                        logging.getLogger('questdb').removeHandler(handler)
                        self.assertIs(ctx.exception, exc)
                        self.assertEqual(handler.fired, 1)
                        # The save failed after the token was committed; the
                        # interrupt reports the Ctrl-C, it does not undo that.
                        self.assertEqual(
                            auth.token(), server.initial_access_token)

    @unittest.skipUnless(
        os.name == 'posix', 'durable file token store requires POSIX')
    def test_interrupt_in_persistence_diagnostic_reaches_token(self):
        # Same as above for the other foreground call a diagnostic can run
        # inside: a synchronous refresh in token() whose save fails.
        credential_path = None
        sabotaged = threading.Event()

        def sabotage_after_refresh_request():
            # See test_background_token_provider_dispatches_persistence_
            # diagnostic: turning the credential into a directory makes the
            # atomic replace that persists the refreshed token fail.
            if not sabotaged.is_set():
                if os.path.isfile(credential_path):
                    os.remove(credential_path)
                os.mkdir(credential_path)
                sabotaged.set()

        with tempfile.TemporaryDirectory() as directory:
            with OidcTestServer(
                    initial_expires_in=4,
                    refresh_request_hook=sabotage_after_refresh_request
                    ) as server:
                auth = make_discovered_auth(
                    server, token_store=FileTokenStore.at(directory))
                auth.sign_in()
                credential_files = [
                    name for name in os.listdir(directory)
                    if name.endswith('.json')]
                self.assertEqual(len(credential_files), 1)
                credential_path = os.path.join(
                    directory, credential_files[0])
                handler = self._RaisingHandler(KeyboardInterrupt())
                self._with_questdb_handler(handler)
                # Serve the initial token until it crosses the refresh
                # threshold; the refreshing call must raise, not return the
                # refreshed token with the interrupt thrown away.
                deadline = time.monotonic() + 15
                with self.assertRaises(KeyboardInterrupt):
                    while time.monotonic() < deadline:
                        if auth.token() != server.initial_access_token:
                            break
                        time.sleep(0.05)
                self.assertTrue(sabotaged.is_set())
                self.assertEqual(handler.fired, 1)

    def test_clear_does_not_wait_behind_interactive_sign_in(self):
        # sign_in() holds the provider for the whole device flow. clear()
        # waited for all of it with the GIL released -- up to the device-code
        # lifetime, with Ctrl-C undeliverable -- whenever it landed between
        # polls rather than during a renderer callback, which was already
        # refused.
        waiting = threading.Event()

        class WaitingRenderer(RecordingRenderer):
            def on_waiting(self, seconds_left):
                super().on_waiting(seconds_left)
                waiting.set()

        pending = (400, {'error': 'authorization_pending'}, None)
        outcome = []
        with OidcTestServer(
                device_token_response=pending,
                device_expires_in=15) as server:
            auth = make_discovered_auth(server, renderer=WaitingRenderer())

            def sign_in():
                try:
                    auth.sign_in()
                except BaseException as exc:
                    outcome.append(exc)

            signer = threading.Thread(target=sign_in, daemon=True)
            signer.start()
            self.assertTrue(waiting.wait(5), 'sign-in did not start polling')
            # Let the callback return, so this lands in the poll wait (5s)
            # that the old code blocked through, not the callback window.
            time.sleep(0.2)
            started = time.monotonic()
            with self.assertRaises(questdb.QuestDBError) as ctx:
                auth.clear()
            self.assertLess(time.monotonic() - started, 2)
            self.assertIs(
                ctx.exception.code, questdb.QuestDBErrorCode.InvalidApiCall)
            self.assertIn('Nothing was cleared', str(ctx.exception))

            # The refused clear left the sign-in running; cancel it, and
            # clear now proceeds.
            self.assertTrue(signer.is_alive())
            auth.cancel_sign_in()
            signer.join(5)
            self.assertFalse(signer.is_alive())
            self.assertEqual(len(outcome), 1)
            self.assertIsInstance(outcome[0], OidcCancelledError)
            auth.clear()

    def test_renderer_can_cancel_sign_in_without_closing_provider(self):
        outcome = []
        holder = []

        class CancellingRenderer(RecordingRenderer):
            def on_waiting(self, seconds_left):
                super().on_waiting(seconds_left)
                try:
                    holder[0].cancel_sign_in()
                    outcome.append('cancelled')
                except BaseException as exc:  # noqa: BLE001
                    outcome.append(exc)

        pending = (400, {'error': 'authorization_pending'}, None)
        with OidcTestServer(
                device_token_response=pending, device_expires_in=20) as server:
            auth = make_discovered_auth(server, renderer=CancellingRenderer())
            holder.append(auth)
            started = time.monotonic()
            with self.assertRaises(OidcCancelledError):
                auth.sign_in()
            self.assertLess(time.monotonic() - started, 10)
            self.assertEqual(outcome[:1], ['cancelled'])
            with self.assertRaises(OidcInteractionRequired):
                auth.token()

            # Calling cancel while idle must not poison the next attempt.
            auth.cancel_sign_in()
            server.device_token_response = None
            auth.sign_in()
            self.assertEqual(auth.token(), server.initial_access_token)

    def test_renderer_can_close_the_provider_from_its_callback(self):
        # Permanent close remains callback-safe for renderers that explicitly
        # want to disable this provider and all attached transports. Native
        # publishes the close without waiting for the authentication critical
        # section and skips only the drain, so this must not deadlock.
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
        with OidcTestServer(
                device_token_response=pending, device_expires_in=20) as server:
            auth = make_discovered_auth(server, renderer=CancellingRenderer())
            holder.append(auth)
            started = time.monotonic()
            with self.assertRaises(OidcCancelledError):
                auth.sign_in()
            self.assertLess(time.monotonic() - started, 10)
        self.assertEqual(outcome[:1], ['closed'])

    def test_renderer_on_success_can_read_committed_token(self):
        holder = []

        class TokenReadingRenderer(RecordingRenderer):
            def on_success(self, identity, expires_in):
                super().on_success(identity, expires_in)
                holder.append(auth.token())

        with OidcTestServer() as server:
            auth = make_discovered_auth(
                server, renderer=TokenReadingRenderer())
            auth.sign_in()
            self.assertEqual(holder, [server.initial_access_token])

    def test_interrupt_on_success_does_not_discard_the_new_token(self):
        # oidc.pxi cancels the flow only for PROMPT/WAITING: on SUCCESS the
        # token has just been acquired and closing there would throw it away.
        # Dropping that condition passes the whole suite otherwise, because the
        # only interrupt test raises from on_waiting.
        class InterruptOnSuccess(RecordingRenderer):
            def on_success(self, identity, expires_in):
                super().on_success(identity, expires_in)
                raise KeyboardInterrupt

        renderer = InterruptOnSuccess()
        with OidcTestServer() as server:
            auth = make_discovered_auth(server, renderer=renderer)
            # The interrupt is still the user's, so it is re-raised...
            with self.assertRaises(KeyboardInterrupt):
                auth.sign_in()
            self.assertEqual(len(renderer.successes), 1)
            # ...but the provider must remain open and keep the token it just
            # acquired, rather than cancelling itself on the way out.
            self.assertEqual(auth.token(), server.initial_access_token)

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
                # Same class a signal handler commonly raises, but no signal
                # handler frame: ordinary renderer errors remain best-effort.
                raise TimeoutError('boom-prompt')

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

    @unittest.skipUnless(
        os.name == 'posix', 'durable file token store requires POSIX')
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

    @unittest.skipUnless(
        os.name == 'posix', 'durable file token store requires POSIX')
    def test_persistence_failure_logs_warning_and_keeps_token(self):
        class SabotageRenderer(RecordingRenderer):
            def __init__(self, directory):
                super().__init__()
                self.directory = directory

            def on_prompt(self, challenge):
                super().on_prompt(challenge)
                # Preflight has already accepted/created the empty directory.
                # Replace it with a regular file so the later durable save
                # deterministically fails on every platform (including root CI).
                shutil.rmtree(self.directory)
                with open(self.directory, 'w', encoding='utf-8') as sink:
                    sink.write('not a directory')

        with tempfile.TemporaryDirectory() as parent:
            directory = os.path.join(parent, 'store')
            with OidcTestServer() as server:
                renderer = SabotageRenderer(directory)
                auth = make_discovered_auth(
                    server,
                    token_store=FileTokenStore.at(directory),
                    renderer=renderer)
                with self.assertLogs('questdb', level='WARNING') as captured:
                    auth.sign_in()
                self.assertEqual(auth.token(), 'AT-initial')
                self.assertEqual(renderer.failures, [])
                self.assertEqual(len(captured.records), 1)
                self.assertIn('token store save failed', captured.output[0])
                self.assertNotIn('AT-initial', captured.output[0])

    @unittest.skipUnless(
        os.name == 'posix', 'durable file token store requires POSIX')
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

    @unittest.skipUnless(
        os.name == 'posix', 'durable file token store requires POSIX')
    def test_clear_after_close_removes_persisted_token(self):
        # close() drops the in-memory credential but deliberately leaves the
        # persisted entry, and clear() used to refuse on a closed provider --
        # both layers did. So the ordinary scoped form left a long-lived
        # plaintext refresh token on disk with no supported way to remove it:
        # the only recoveries were rebuilding an identical provider or deleting
        # the file by hand.
        with tempfile.TemporaryDirectory() as directory:
            with OidcTestServer() as server:
                with make_discovered_auth(
                        server,
                        token_store=FileTokenStore.at(directory)) as auth:
                    auth.sign_in()
                    self.assertEqual(auth.token(), 'AT-initial')
                self.assertTrue(os.listdir(directory), 'nothing was persisted')

                # __exit__ closed it; clearing must still work.
                auth.clear()
                # Lock release leaves zero-length, immediately reclaimable
                # marker files so a stale owner can never unlink a successor's
                # lock. They contain no credential; clear() must remove the
                # JSON token entry itself.
                credential_files = [
                    name for name in os.listdir(directory)
                    if name.endswith('.json')]
                self.assertEqual(
                    credential_files, [],
                    'the persisted credential survived clear() after close()')

                # And the credential is really gone: a fresh provider has to
                # run a second device flow.
                fresh = make_discovered_auth(
                    server, token_store=FileTokenStore.at(directory))
                fresh.sign_in()
                self.assertEqual(
                    len(server.requests('/device', 'POST')), 2)

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

    @unittest.skipUnless(
        os.name == 'posix', 'durable file token store requires POSIX')
    def test_background_token_provider_dispatches_persistence_diagnostic(self):
        # The foreground sign-in diagnostic path starts on a Python thread that
        # already has a thread state. Exercise the other boundary: QWP resolves
        # a near-expiry credential on Rust's isolated token-provider worker,
        # whose failed save enters _oidc_diagnostic_trampoline from that foreign
        # thread and acquires the GIL there.
        credential_path = None
        sabotaged = threading.Event()

        def sabotage_after_refresh_request():
            # The refresh request is sent only after native has acquired the
            # store lease and loaded the valid file. Replacing the target now
            # lets refresh succeed but makes the following atomic replace fail.
            # A reconnect may refresh again; sabotage only the first response.
            if not sabotaged.is_set():
                # Rotating-token safety removes the old JSON before sending the
                # request; a non-rotating IdP may leave it until replacement.
                # Either way, make the destination a directory so rename fails.
                if os.path.isfile(credential_path):
                    os.remove(credential_path)
                os.mkdir(credential_path)
                sabotaged.set()

        with tempfile.TemporaryDirectory() as directory:
            with OidcTestServer(
                    initial_expires_in=4,
                    refresh_request_hook=sabotage_after_refresh_request
                    ) as oidc_server:
                auth = make_discovered_auth(
                    oidc_server, token_store=FileTokenStore.at(directory))
                auth.sign_in()
                credential_files = [
                    name for name in os.listdir(directory)
                    if name.endswith('.json')]
                self.assertEqual(len(credential_files), 1)
                credential_path = os.path.join(
                    directory, credential_files[0])

                with QwpAckServer(
                        close_after_upgrade_unless_authorization=(
                            'Bearer AT-refreshed')) as qwp_server:
                    conf = (
                        f'ws::addr=127.0.0.1:{qwp_server.port};'
                        'lazy_connect=true;'
                        'reconnect_initial_backoff_millis=25;'
                        'reconnect_max_backoff_millis=25;'
                        'reconnect_max_duration_millis=10000;'
                        'close_flush_timeout_millis=10000;')
                    sender = questdb.Sender.from_conf(
                        conf, oidc_auth=auth, auto_flush=False)
                    try:
                        # Establish synchronously with the still-valid initial
                        # token. The mock completes that upgrade, then closes
                        # every session using the initial credential. The
                        # background reconnect loop therefore stays active until
                        # the short-lived token crosses its refresh threshold;
                        # only the refreshed credential is allowed to carry the
                        # frame. This synchronises on the ACK instead of sleeping
                        # for an assumed wall-clock threshold.
                        sender.establish()
                        main_thread = threading.get_ident()
                        with self.assertLogs(
                                'questdb', level='WARNING') as captured:
                            sender.row(
                                'events', columns={'value': 42},
                                at=questdb.ServerTimestamp)
                            fsn = sender.flush_and_get_fsn()
                            self.assertTrue(
                                sender.await_acked_fsn(fsn, 10000))
                    finally:
                        sender.close(flush=False)
                    stats = qwp_server.snapshot()

        self.assertTrue(sabotaged.is_set())
        warnings = [
            record for record in captured.records
            if 'token store save failed' in record.getMessage()]
        self.assertEqual(len(warnings), 1, captured.output)
        self.assertNotEqual(
            warnings[0].thread, main_thread,
            'diagnostic unexpectedly ran on the Python caller thread')
        self.assertEqual(stats['binary_frames'], 1)
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
                    drivername='postgresql+psycopg')
            # SQLAlchemy's dialect supplies the validated destination in
            # cparams; the mock listener must model those real driver args.
            params = {'host': '127.0.0.1', 'port': 8812}
            returned.listeners['do_connect'](None, None, [], params)
            token_requests = server.requests('/token', 'POST')

        self.assertIs(returned, engine)
        self.assertEqual(params['password'], 'AT-refreshed')
        self.assertEqual(params['sslmode'], 'prefer')
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

    def test_device_poll_reflection_does_not_expose_device_code(self):
        # Some IdPs, proxies and WAFs reflect the submitted device_code in the
        # OAuth error fields. Exercise both its exact and form-encoded spellings
        # through the Python FFI boundary: neither the renderer nor any public
        # exception surface may turn that credential into display or log text.
        device_code = 'DEV +/% exact credential'
        encoded_device_code = 'DEV+%2B%2F%25+exact+credential'
        renderer = RecordingRenderer()
        with OidcTestServer(
                device_code=device_code,
                device_token_response=(400, {
                    'error': device_code,
                    'error_description': (
                        'safe diagnostic before '
                        f'{encoded_device_code} after'),
                }, None)) as server:
            auth = make_discovered_auth(server, renderer=renderer)
            with self.assertRaises(OidcDeviceFlowError) as ctx:
                auth.sign_in()
            token_requests = server.requests('/token', 'POST')

        self.assertEqual(len(token_requests), 1)
        self.assertEqual(
            token_requests[0]['form']['device_code'], [device_code])
        self.assertIn(
            f'device_code={encoded_device_code}'.encode('ascii'),
            token_requests[0]['body'])
        err = ctx.exception
        self.assertIn('[redacted credential]', err.error)
        self.assertIn('[redacted credential]', err.error_description)
        surfaces = [
            *renderer.failures,
            str(err),
            repr(err),
            err.error,
            err.error_description,
            repr(vars(err)),
        ]
        for surface in surfaces:
            self.assertNotIn(device_code, surface)
            self.assertNotIn(encoded_device_code, surface)

    def test_refresh_reflection_does_not_expose_refresh_token(self):
        # The refresh grant has no renderer callback, but it shares the token
        # endpoint and exception conversion with device polling. Confirm the
        # request really carried the mutation-discriminating secret and that a
        # reflected raw/encoded value survives in no Python exception surface.
        refresh_token = 'RT +/% exact credential'
        encoded_refresh_token = 'RT+%2B%2F%25+exact+credential'
        renderer = RecordingRenderer()
        with OidcTestServer(
                initial_access_token=EXPIRED_ACCESS_TOKEN,
                refresh_token=refresh_token,
                refresh_token_response=(429, {
                    'error': 'slow_down',
                    'error_description': (
                        f'safe diagnostic {refresh_token}; '
                        f'wire={encoded_refresh_token}'),
                }, {'Retry-After': '7'})) as server:
            auth = make_discovered_auth(server, renderer=renderer)
            auth.sign_in()
            with self.assertRaises(OidcNetworkError) as ctx:
                auth.token()
            token_requests = server.requests('/token', 'POST')

        self.assertEqual(len(token_requests), 2)
        self.assertEqual(
            token_requests[1]['form']['refresh_token'], [refresh_token])
        self.assertIn(
            f'refresh_token={encoded_refresh_token}'.encode('ascii'),
            token_requests[1]['body'])
        err = ctx.exception
        self.assertEqual(err.status, 429)
        self.assertEqual(err.retry_after, 7)
        self.assertEqual(renderer.failures, [])
        surfaces = [str(err), repr(err), repr(vars(err))]
        for surface in surfaces:
            self.assertNotIn(refresh_token, surface)
            self.assertNotIn(encoded_refresh_token, surface)

    def test_oidc_error_strips_control_characters_from_idp_text(self):
        # Regression: OidcError.__init__ sanitizes every message argument
        # because an uncaught traceback reaches a terminal or a notebook, both
        # of which interpret ANSI, and the IdP fields interpolated into these
        # messages are attacker- or MITM-controllable. Native does NOT strip
        # them on this path -- only the device-flow *event* text is sanitized --
        # so the Python layer is the only sink guard. Every existing test uses
        # clean strings, so deleting that call passed the whole suite.
        hostile = 'The user \x1b[31mdenied‮​ it.'
        with OidcTestServer(device_token_response=(400, {
                'error': 'access_\x1b[32mdenied',
                'error_description': hostile}, None)) as server:
            auth = make_discovered_auth(server)
            with self.assertRaises(OidcDeviceFlowError) as ctx:
                auth.sign_in()
        err = ctx.exception
        for field in (str(err), repr(err), err.error, err.error_description):
            self.assertNotIn('\x1b', field, 'an ANSI escape survived')
            self.assertNotIn('‮', field, 'a bidi override survived')
            self.assertNotIn('​', field, 'a zero-width char survived')
        # Stripped, not dropped: the readable text must survive.
        self.assertIn('denied', err.error_description)
        self.assertIn('denied', err.error)

    def test_expired_device_code_maps_to_timeout_with_idp_error(self):
        # `expired_token` maps to OidcTimeoutError (a OidcDeviceFlowError), and
        # every native-attached IdP diagnostic must be carried through. This
        # special timeout branch used to drop description and HTTP status even
        # though the general device-flow error branch preserved both.
        with OidcTestServer(device_token_response=(400, {
                'error': 'expired_token',
                'error_description': 'The device code is no longer valid.'},
                None)) as server:
            auth = make_discovered_auth(server)
            with self.assertRaises(OidcTimeoutError) as ctx:
                auth.sign_in()
        self.assertIsInstance(ctx.exception, OidcDeviceFlowError)
        self.assertEqual(ctx.exception.error, 'expired_token')
        self.assertEqual(
            ctx.exception.error_description,
            'The device code is no longer valid.')
        self.assertEqual(ctx.exception.status, 400)

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
        # (questdb_oidc_token_free) and the _OIDC_PROVIDERS pop across the FULL
        # lifecycle, not just bare construction; a stranded registry weakref
        # would accumulate.
        with OidcTestServer() as server:
            make_discovered_auth(server).sign_in()  # warm
            watermark = _provider_id_watermark()
            refs = []
            for _ in range(12):
                auth = make_discovered_auth(server)
                auth.sign_in()
                self.assertEqual(auth.token(), 'AT-initial')
                refs.append(weakref.ref(auth))
                del auth
            remaining = _settled_registered_ids_since(watermark)
        self.assertTrue(
            all(ref() is None for ref in refs),
            'a provider remained alive after a full sign-in/token lifecycle')
        self.assertEqual(
            remaining, set(),
            'the provider registry grew over 12 sign-in/token lifecycles')


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

    def test_sender_rejects_basic_auth_and_rotating_token(self):
        # username/password are in the same mutual-exclusion list as token, on
        # both the kwarg and the conf-string path, but only token was ever
        # tested: dropping either from the Sender tuple or from connect()'s key
        # list left the suite green while the caller fell through to native's
        # internal "..._token_provider" message.
        for kwargs in (
                {'username': 'u'},
                {'password': 'p'},
                {'username': 'u', 'password': 'p'}):
            with self.subTest(kwargs=kwargs):
                with self.assertRaises(questdb.QuestDBError) as ctx:
                    questdb.Sender(
                        questdb.Protocol.Http, 'localhost', 9000,
                        oidc_auth=self.auth, **kwargs)
                message = str(ctx.exception)
                self.assertIn('oidc_auth', message)
                self.assertNotIn('token_provider', message)
                for key in kwargs:
                    self.assertIn(key, message)

    def test_sender_from_conf_rejects_basic_auth_and_rotating_token(self):
        with self.assertRaises(questdb.QuestDBError) as ctx:
            questdb.Sender.from_conf(
                'http::addr=localhost:9000;username=u;password=p;',
                oidc_auth=self.auth)
        message = str(ctx.exception)
        self.assertIn('oidc_auth', message)
        self.assertIn('username', message)
        self.assertIn('password', message)
        self.assertNotIn('token_provider', message)

    def test_pool_rejects_basic_auth_and_rotating_token(self):
        with self.assertRaises(questdb.QuestDBError) as ctx:
            questdb.connect(
                'ws::addr=localhost:9000;lazy_connect=true;'
                'username=u;password=p;',
                oidc_auth=self.auth)
        message = str(ctx.exception)
        self.assertIn('oidc_auth', message)
        self.assertIn('username', message)
        self.assertIn('password', message)
        self.assertNotIn('token_provider', message)

    def test_pool_conflict_from_keywords_does_not_blame_a_conf_string(self):
        # connect() folds keyword credentials into the configuration string
        # it builds, so a caller who never wrote one must not be told to edit
        # it.
        for args, kwargs in (
                ((), {'host': 'localhost', 'username': 'u', 'password': 'p'}),
                (('ws::addr=localhost:9000;lazy_connect=true;',),
                 {'token': 'fixed'})):
            with self.subTest(kwargs=kwargs):
                with self.assertRaises(questdb.QuestDBError) as ctx:
                    questdb.connect(*args, oidc_auth=self.auth, **kwargs)
                self.assertEqual(
                    ctx.exception.code, questdb.QuestDBErrorCode.ConfigError)
                message = str(ctx.exception)
                self.assertNotIn('configuration string', message)
                self.assertIn('remove the fixed credential', message)
                for key in kwargs:
                    if key != 'host':
                        self.assertIn(key, message)

    def test_token_conflict_names_the_parameters_the_caller_wrote(self):
        # Regression: only native enforced this, and it reports the internal
        # config key it knows -- "qwp_ws_token_provider" / "http_token_provider"
        # -- which exists in no public API, so a caller who passed oidc_auth=
        # and token= was told about a symbol they cannot find anywhere.
        with self.assertRaises(questdb.QuestDBError) as ctx:
            questdb.Sender(
                questdb.Protocol.Http, 'localhost', 9000,
                token='fixed', oidc_auth=self.auth)
        message = str(ctx.exception)
        self.assertIn('oidc_auth', message)
        self.assertIn('token', message)
        self.assertNotIn('token_provider', message)

        with self.assertRaises(questdb.QuestDBError) as ctx:
            questdb.connect(
                'ws::addr=localhost:9000;lazy_connect=true;token=fixed;',
                oidc_auth=self.auth)
        message = str(ctx.exception)
        self.assertIn('oidc_auth', message)
        self.assertNotIn('token_provider', message)

    def test_required_constructor_arguments_reject_none(self):
        # Regression: the three required positionals went through the
        # optional-string helper, which drops a None and leaves the field unset.
        # Native then reported it as missing from QuestDB's /settings and told
        # the caller to pass it explicitly -- advice that makes no sense for a
        # constructor that never contacts /settings, naming a builder method
        # this API does not have.
        cases = [
            (None, 'https://idp.example/device', 'https://idp.example/token',
             'client_id'),
            ('questdb', None, 'https://idp.example/token',
             'device_authorization_endpoint'),
            ('questdb', 'https://idp.example/device', None, 'token_endpoint'),
        ]
        for client_id, device_endpoint, token_endpoint, expected in cases:
            with self.subTest(missing=expected):
                with self.assertRaises(OidcConfigError) as ctx:
                    OidcDeviceAuth(
                        client_id, device_endpoint, token_endpoint,
                        interactive=False, open_browser=False)
                message = str(ctx.exception)
                self.assertIn(expected, message)
                self.assertIn('required', message)
                self.assertNotIn('/settings', message)

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
    def test_causally_oidc_error_keeps_the_transports_own_code(self):
        # `questdb_error_oidc_get_view` reports an OIDC failure anywhere in the
        # error's *causal chain*, not that the error is one. This pins that
        # contract from the Python side, because `c_err_to_py` keys the
        # exception type on it for every native error in the extension.
        #
        # The provider's InteractionRequired is re-classified to a retryable
        # SocketError on its way out (`classify_provider_error` exempts only
        # Config and Cancelled), so the outer code is the transport's while the
        # OIDC payload rides along. Two things must hold at once: the typed
        # class is available for auth-specific handling, and the transport's
        # own classification is not overwritten by it -- retry logic keying on
        # `.code` has to keep seeing SocketError, which is the whole reason
        # native re-classifies.
        auth = make_auth()
        with self.assertRaises(OidcError) as caught:
            with questdb.Sender(
                    questdb.Protocol.Http, '127.0.0.1', 9000,
                    oidc_auth=auth, protocol_version=2) as sender:
                sender.row('t', columns={'v': 1}, at=questdb.ServerTimestamp)
                sender.flush()
        self.assertIsInstance(caught.exception, OidcInteractionRequired)
        self.assertIs(caught.exception.code,
                      questdb.QuestDBErrorCode.SocketError)
        # And it stays catchable as the ordinary error type, which is what
        # keeps every existing `except QuestDBError` handler working once a
        # sender is given `oidc_auth=`.
        self.assertIsInstance(caught.exception, questdb.QuestDBError)

    @unittest.skipIf(pd is None, 'pandas not installed')
    def test_dataframe_auto_flush_preserves_oidc_error(self):
        auth = make_auth()
        with self.assertRaises(OidcInteractionRequired) as caught:
            with questdb.Sender(
                    questdb.Protocol.Http,
                    '127.0.0.1',
                    9000,
                    oidc_auth=auth,
                    auto_flush_rows=1,
                    auto_flush_bytes=False,
                    auto_flush_interval=False,
                    protocol_version=2) as sender:
                sender.dataframe(
                    pd.DataFrame({'value': [1]}),
                    table_name='oidc_auto_flush',
                    at=questdb.ServerTimestamp)
        self.assertIn(' - See https://', str(caught.exception))

    def test_native_holds_no_python_reference_for_its_callback(self):
        # Regression: the event handler's user_data used to be an INCREF'd
        # weakref, so the final native release ran a Py_DECREF -- possibly on an
        # abandoned token-acquisition worker, which the interpreter neither
        # manages nor joins, at an arbitrary later time. It was guarded by a
        # Py_IsFinalizing check, but no such check can be made safe: finalization
        # can begin between the test and PyGILState_Ensure.
        #
        # Native now receives an opaque integer key instead, so the release
        # callback owns nothing and never enters Python. The provider weakref
        # callback drops the registry entries and native owner on a managed
        # thread; a stale entry would be a slow leak that no ordinary weakref
        # assertion catches.
        watermark = _provider_id_watermark()
        provider = make_auth()
        self.assertEqual(len(_registered_ids_since(watermark)), 1)
        weak = weakref.ref(provider)
        del provider
        # Draining the weakref callback pops both entries, so take the reading
        # first and assert on both afterwards.
        remaining = _settled_registered_ids_since(watermark)
        self.assertIsNone(weak(), 'the registry must not keep a provider alive')
        self.assertEqual(
            remaining, set(), 'the registry entry outlived its provider')

    def test_provider_registry_does_not_grow_across_churn(self):
        watermark = _provider_id_watermark()
        refs = []
        for _ in range(200):
            provider = make_auth()
            refs.append(weakref.ref(provider))
            del provider
        remaining = _settled_registered_ids_since(watermark)
        self.assertTrue(all(ref() is None for ref in refs))
        self.assertEqual(remaining, set())

    def test_token_provider_failure_is_narrated_to_the_listener(self):
        # Regression: the Bearer header is resolved ABOVE the endpoint loop, so
        # its failure returned before auth_failed() (inside the loop) and
        # all_endpoints_unreachable() (after it) could run. A QWP/WS connect
        # round whose token pull failed therefore emitted no event and wrote no
        # log -- while the round is retryable and the reconnect budget restarts
        # each time, so the sender kept trying in complete silence and the
        # operator's first symptom was unrelated store backpressure.
        events = []
        auth = make_auth()  # never signed in
        db = questdb.connect(
            'ws::addr=127.0.0.1:19009;lazy_connect=true;',
            oidc_auth=auth,
            connection_listener=events.append,
            connection_event_inbox_capacity=32)
        try:
            # Keep the owning DB and its event dispatcher alive while the
            # background connection attempt narrates the synchronous token
            # failure. Eager construction used to unwind and close the
            # dispatcher immediately after queuing AuthFailed, so shutdown was
            # allowed to discard the backlog before the callback thread ran.
            sender = db.sender()
            try:
                deadline = time.monotonic() + 5
                while not events and time.monotonic() < deadline:
                    time.sleep(0.01)
            finally:
                sender.close(flush=False)
        finally:
            db.close()
        self.assertTrue(events, 'the failed token pull was never narrated')
        event = events[0]
        # CredentialUnavailable, not AuthFailed. AuthFailed is terminal and
        # means the server rejected a credential we presented; this round never
        # presented one. Overloading the two forced every listener to gate on
        # `host is not None` and made a silent-refresh blip indistinguishable
        # from a rejected credential. The Java client draws the same line with
        # QwpCredentialUnavailableException vs QwpAuthFailedException.
        self.assertIs(
            event.kind, questdb.ConnectionEventKind.CredentialUnavailable)
        self.assertIsNot(event.kind, questdb.ConnectionEventKind.AuthFailed)
        # No endpoint attribution: nothing was contacted, the credential failed.
        self.assertIsNone(event.host)
        self.assertIsNone(event.port)
        self.assertIs(
            event.cause_code, questdb.QuestDBErrorCode.SocketError)
        # The cause must carry the actionable detail, not just a code.
        self.assertIn('sign_in', event.cause_msg)

    def test_closed_provider_is_terminal_credential_unavailable(self):
        # Attach while the provider is open (the public API correctly rejects an
        # already-closed provider), then close it before lazy connect performs
        # its first token pull. close() is monotonic, so native must preserve the
        # provider's AuthError instead of reclassifying it as retryable and the
        # QWP runner must stop after narrating one CredentialUnavailable event.
        events = []
        auth = make_auth()
        db = questdb.connect(
            'ws::addr=127.0.0.1:19009;lazy_connect=true;'
            'reconnect_initial_backoff_millis=10;'
            'reconnect_max_backoff_millis=10;',
            oidc_auth=auth,
            connection_listener=events.append,
            connection_event_inbox_capacity=32)
        auth.close()
        try:
            sender = db.sender()
            try:
                deadline = time.monotonic() + 5
                while not events and time.monotonic() < deadline:
                    time.sleep(0.01)
                self.assertTrue(
                    events, 'the closed provider failure was never narrated')
                credential_events = [
                    event for event in events
                    if event.kind is
                    questdb.ConnectionEventKind.CredentialUnavailable]
                self.assertEqual(len(credential_events), 1)
                event = credential_events[0]
                self.assertIs(
                    event.cause_code, questdb.QuestDBErrorCode.AuthError)
                self.assertIsNone(event.host)
                self.assertIsNone(event.port)
                self.assertIn('closed', event.cause_msg.lower())

                # The cause code above pins the classification; this pins that
                # the runner acts on it and stops. A runner that kept retrying
                # at the 10ms backoff would narrate dozens more events in this
                # window, while a correct one narrates none, so the window can
                # only miss a regression, never fail a correct runner. Checking
                # the count the moment the first event arrived caught a
                # retrying runner only about half the time.
                time.sleep(0.5)
                self.assertEqual(
                    [
                        observed.kind for observed in events
                        if observed.kind is
                        questdb.ConnectionEventKind.CredentialUnavailable
                    ],
                    [questdb.ConnectionEventKind.CredentialUnavailable])
            finally:
                sender.close(flush=False)
        finally:
            db.close()

    @unittest.skipIf(pd is None, 'pandas not installed')
    def test_pool_dataframe_fails_fast_when_sign_in_is_required(self):
        # Regression: native classifies an OIDC token failure as a retryable
        # SocketError so a transport's background drainer keeps queued frames
        # alive while a human signs in. QuestDB.dataframe()'s reconnect gate
        # keyed on that code alone, so a foreground call re-polled a provider
        # that is documented never to prompt, stalling for the WHOLE reconnect
        # budget (300s by default) before raising the very same error.
        #
        # Keep a long budget to preserve the production shape, but make the
        # regression fail deterministically on its first dataframe replay. The
        # old gate re-serialized the dataframe until that budget elapsed; the
        # fixed gate probes the provider and raises without a second `.int`
        # read, so no wall-clock assertion is needed.
        class CountingUUID(uuid.UUID):
            int_reads = 0

            def __getattribute__(self, name):
                if name == 'int':
                    type(self).int_reads += 1
                    if type(self).int_reads > 1:
                        raise AssertionError(
                            'OIDC retry rebuilt the dataframe')
                return super().__getattribute__(name)

        value = CountingUUID('12345678-1234-5678-1234-567812345678')
        df = pd.DataFrame({
            'value': [value],
            'ts': pd.to_datetime([1700000000], unit='s')})
        CountingUUID.int_reads = 0
        auth = make_auth()  # never signed in
        db = questdb.connect(
            'ws::addr=127.0.0.1:19009;lazy_connect=on;'
            'reconnect_max_duration_millis=60000;',
            oidc_auth=auth)
        try:
            with self.assertRaises(OidcInteractionRequired):
                db.dataframe(df, table_name='oidc_ff', at='ts')
            self.assertEqual(
                CountingUUID.int_reads, 1,
                'the terminal OIDC probe rebuilt the whole dataframe instead '
                'of polling the provider directly')
        finally:
            db.close()

    @unittest.skipIf(pd is None, 'pandas not installed')
    def test_pool_dataframe_retry_fails_fast_when_the_credential_lapses(self):
        # The gate above only saw the FIRST attempt. After an ordinary
        # transport failure the retry went to the native `*_with_retry`
        # borrow, which classifies a non-busy InteractionRequired as a
        # retryable SocketError and polled the provider for the whole
        # remaining budget before Python could fail fast. Here the first
        # connect is refused (not OIDC), and the token then lapses with its
        # refresh rejected: the call must raise OidcInteractionRequired well
        # inside the 60s budget.
        probe = socket.socket()
        probe.bind(('127.0.0.1', 0))
        closed_port = probe.getsockname()[1]
        probe.close()
        df = pd.DataFrame({
            'value': [1], 'ts': pd.to_datetime([1700000000], unit='s')})
        with OidcTestServer(
                initial_expires_in=4,
                refresh_token_response=(
                    400, {'error': 'invalid_grant'}, None)) as server:
            auth = make_discovered_auth(server)
            auth.sign_in()
            db = questdb.connect(
                f'ws::addr=127.0.0.1:{closed_port};lazy_connect=on;'
                'reconnect_initial_backoff_millis=50;'
                'reconnect_max_backoff_millis=200;'
                'reconnect_max_duration_millis=60000;',
                oidc_auth=auth)
            try:
                started = time.monotonic()
                with self.assertRaises(OidcInteractionRequired) as caught:
                    db.dataframe(df, table_name='oidc_lapse', at='ts')
                elapsed = time.monotonic() - started
            finally:
                db.close()
                auth.close()
        self.assertFalse(caught.exception._acquisition_busy)
        self.assertLess(
            elapsed, 30.0,
            'the native re-borrow polled a lapsed provider for the whole '
            'reconnect budget')

    @unittest.skipIf(pd is None, 'pandas not installed')
    def test_pool_dataframe_waits_behind_a_peer_sign_in_found_by_the_probe(self):
        # The single probe can land while a peer sign_in() sits between
        # device-flow polls. A direct token() refusal there is busy but carries
        # AuthError -- only a transport's own pull is reclassified to
        # SocketError -- and the retryable-code check raised it at once, although
        # the gate documents a peer sign-in as transient. The call must instead
        # wait inside its budget and succeed once the sign-in completes.
        pending = (400, {'error': 'authorization_pending'}, None)
        polled = threading.Event()
        probe_errors = []
        peer = []

        class _Renderer(Renderer):
            def on_waiting(self, seconds_left):
                polled.set()

        class _PeerSignInBeforeProbe(OidcDeviceAuth):
            probes = 0

            def token(self):
                type(self).probes += 1
                if type(self).probes == 1:
                    def run():
                        try:
                            self.sign_in()
                            peer.append('ok')
                        except BaseException as e:  # pragma: no cover
                            peer.append(e)
                    threading.Thread(target=run, daemon=True).start()
                    polled.wait(20)
                    # on_waiting returned: native is now in its poll sleep.
                    time.sleep(0.3)
                try:
                    return super().token()
                except questdb.QuestDBError as e:
                    probe_errors.append(
                        (e.code, getattr(e, '_acquisition_busy', None)))
                    raise

        df = pd.DataFrame({
            'value': [1], 'ts': pd.to_datetime([1700000000], unit='s')})
        with OidcTestServer(
                device_token_responses=[pending], device_interval=2,
                device_expires_in=30) as server:
            cfg = make_discovered_auth(server).config
            auth = _PeerSignInBeforeProbe(
                cfg.client_id, cfg.device_authorization_endpoint,
                cfg.token_endpoint, scope=cfg.scope, audience=cfg.audience,
                issuer=cfg.issuer, interactive=True, open_browser=False,
                renderer=_Renderer())
            with QwpAckServer(
                    required_authorization='Bearer AT-initial') as qwp:
                db = questdb.connect(
                    f'ws::addr=127.0.0.1:{qwp.port};lazy_connect=true;'
                    'reconnect_initial_backoff_millis=50;'
                    'reconnect_max_backoff_millis=200;'
                    'reconnect_max_duration_millis=20000;',
                    oidc_auth=auth)
                try:
                    db.dataframe(df, table_name='busy_probe', at='ts')
                finally:
                    db.close()
                    auth.close()
        self.assertEqual(peer, ['ok'])
        self.assertIn((questdb.QuestDBErrorCode.AuthError, True), probe_errors)

    @unittest.skipIf(pd is None, 'pandas not installed')
    def test_pool_dataframe_surfaces_the_probe_failure_not_the_original(self):
        # The foreground gate probes the provider once before deciding, and
        # rebinds `exc` to the probe's error so "its own type and structured
        # OIDC detail" reach the caller. Every exit from that handler was a
        # BARE `raise`, which re-raises whatever the ENCLOSING `except` is
        # handling -- and the nested probe handler completes normally, so
        # CPython restores the outer exception state first. The rebinding
        # therefore steered the retry decision while the caller still got the
        # original error.
        #
        # Concretely: a provider closed on another thread makes the probe raise
        # OidcCancelledError (AuthError, so it fails the retryable-code check),
        # and the caller was told to run sign_in() on a provider that is closed
        # for good and can never produce a token -- with `except
        # OidcCancelledError` not matching, and .status/.retry_after/.error/
        # .error_description dropped.
        class ClosedDuringProbe(OidcDeviceAuth):
            probes = 0

            def token(self):
                type(self).probes += 1
                raise OidcCancelledError(
                    'the provider was closed while the flush was in flight')

        df = pd.DataFrame({
            'value': [1],
            'ts': pd.to_datetime([1700000000], unit='s')})
        auth = ClosedDuringProbe(  # never signed in
            'questdb',
            'https://idp.example/device',
            'https://idp.example/token',
            interactive=False,
            open_browser=False)
        db = questdb.connect(
            'ws::addr=127.0.0.1:19009;lazy_connect=on;'
            'reconnect_max_duration_millis=60000;',
            oidc_auth=auth)
        try:
            with self.assertRaises(OidcCancelledError) as caught:
                db.dataframe(df, table_name='oidc_probe', at='ts')
        finally:
            db.close()
        self.assertEqual(
            ClosedDuringProbe.probes, 1,
            'the foreground gate must probe the provider exactly once')
        self.assertNotIsInstance(caught.exception, OidcInteractionRequired)
        # The original failure is not lost: it is the context of the one that
        # is raised, so a traceback still shows both.
        self.assertIsInstance(
            caught.exception.__context__, OidcInteractionRequired)

    @unittest.skipIf(pd is None, 'pandas not installed')
    def test_pool_dataframe_replays_after_probe_acquires_token(self):
        # Deterministically close the race the foreground probe exists for: the
        # first native connect sees no token and raises InteractionRequired; the
        # immediate Python probe completes sign-in, so only replaying data
        # preparation can deliver the frame.
        class SignInOnProbe(OidcDeviceAuth):
            probes = 0

            def token(self):
                type(self).probes += 1
                if type(self).probes == 1:
                    self.sign_in()
                return super().token()

        with OidcTestServer() as oidc_server:
            config = make_discovered_auth(oidc_server).config
            auth = SignInOnProbe(
                config.client_id,
                config.device_authorization_endpoint,
                config.token_endpoint,
                scope=config.scope,
                audience=config.audience,
                issuer=config.issuer,
                interactive=True,
                open_browser=False,
                renderer=RecordingRenderer())
            with QwpAckServer(
                    required_authorization='Bearer AT-initial') as qwp_server:
                conf = (
                    f'ws::addr=127.0.0.1:{qwp_server.port};'
                    'lazy_connect=true;'
                    'reconnect_initial_backoff_millis=1;'
                    'reconnect_max_backoff_millis=1;'
                    'reconnect_max_duration_millis=5000;')
                db = questdb.connect(conf, oidc_auth=auth)
                try:
                    db.dataframe(
                        pd.DataFrame({
                            'value': [1],
                            'ts': pd.to_datetime([1700000000], unit='s'),
                        }),
                        table_name='oidc_probe_replay',
                        at='ts')
                finally:
                    db.close()
                stats = qwp_server.snapshot()

        self.assertEqual(SignInOnProbe.probes, 1)
        self.assertGreaterEqual(stats['binary_frames'], 1)
        self.assertEqual(stats['errors'], [])

    def test_flush_surfaces_typed_oidc_error(self):
        # The plain row()/flush() path (not the dataframe path) whose OIDC token
        # pull fails at connect surfaces the typed OidcError through c_err_to_py.
        # OidcError subclasses QuestDBError, so an existing `except QuestDBError`
        # flush handler still catches it while `except OidcInteractionRequired`
        # can react specifically. Complements
        # test_dataframe_auto_flush_preserves_oidc_error (the dataframe path).
        auth = make_auth()  # never signed in
        with self.assertRaises(OidcInteractionRequired) as ctx:
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
                sender.flush()
        self.assertIsInstance(ctx.exception, questdb.QuestDBError)

    def test_failed_flush_clears_the_internal_buffer(self):
        """A failed explicit flush clears the internal buffer, with no carve-out.

        The internal buffer is shared state: `may_flush_on_row_complete`,
        `SenderTransaction.__enter__` / `.commit()` and `close(flush=True)` all
        read a non-empty buffer as "unflushed rows, publish at the next
        opportunity". Retaining a failed batch there does not hand it back to
        the caller -- it hands it to whichever of those runs next. Leaving the
        `with` block must therefore stay silent rather than re-raising the
        error the caller already handled. See
        `test_caller_owned_buffer_survives_a_failed_flush_for_retry` for the
        supported way to republish a failed batch.
        """
        with OidcTestServer() as server:
            auth = make_discovered_auth(server)
            with questdb.Sender(
                    questdb.Protocol.Http,
                    '127.0.0.1',
                    server.port,
                    oidc_auth=auth,
                    auto_flush=False,
                    protocol_version=2) as sender:
                sender.row(
                    'oidc_retry_flush', columns={'value': 1},
                    at=questdb.ServerTimestamp)
                with self.assertRaises(OidcInteractionRequired) as caught:
                    sender.flush()
                self.assertFalse(caught.exception.in_doubt)
                self.assertEqual(
                    len(sender), 0,
                    'a failed flush must not leave rows in the internal '
                    'buffer; they would be published by the next auto-flush, '
                    'transaction or close() rather than by the caller')
                self.assertEqual(server.requests('/write', 'POST'), [])
                # The caller handled the error and gave up. Exiting the block
                # calls close(flush=True); with the buffer clear it finds
                # nothing to send and must not raise a second time.
            self.assertEqual(server.requests('/write', 'POST'), [])

    def test_caller_owned_buffer_survives_a_failed_flush_for_retry(self):
        """`flush(buf, clear=False)` is how a failed batch is republished.

        A caller-owned buffer is never touched by the flush error path, so it
        survives ANY failure -- not just a retryable OIDC one -- and it is
        invisible to auto-flush, `transaction()` and `close(flush=True)`,
        which only ever consult the sender's internal buffer. After
        `sign_in()` the same prefix publishes exactly once.
        """
        with OidcTestServer() as server:
            auth = make_discovered_auth(server)
            with questdb.Sender(
                    questdb.Protocol.Http,
                    '127.0.0.1',
                    server.port,
                    oidc_auth=auth,
                    auto_flush=False,
                    protocol_version=2) as sender:
                buf = sender.new_buffer()
                buf.row(
                    'oidc_retry_flush', columns={'value': 1},
                    at=questdb.ServerTimestamp)
                pending = len(buf)
                with self.assertRaises(OidcInteractionRequired):
                    sender.flush(buf, clear=False)
                self.assertEqual(
                    len(buf), pending,
                    'a caller-owned buffer must survive a failed flush')
                self.assertEqual(server.requests('/write', 'POST'), [])

                auth.sign_in()
                sender.flush(buf)
                self.assertEqual(
                    len(buf), 0, 'a successful flush clears the buffer')
                writes = server.requests('/write', 'POST')

        self.assertEqual(len(writes), 1, 'the prefix must publish exactly once')
        self.assertIn(b'oidc_retry_flush', writes[0]['body'])

    def test_failed_commit_discards_its_rows_and_completes(self):
        """A commit whose flush fails leaves nothing behind.

        `commit()` marks the transaction complete before flushing, so
        `__exit__` will not roll it back afterwards. The rows must therefore be
        discarded by `commit()` itself: left in the internal buffer they would
        be published by the next auto-flush, transaction or `close(flush=True)`
        -- without transactional framing, and after the caller was told the
        commit failed. The transaction is also over, so neither `commit()` nor
        `rollback()` may run again.
        """
        with OidcTestServer() as server:
            auth = make_discovered_auth(server)
            with questdb.Sender(
                    questdb.Protocol.Http,
                    '127.0.0.1',
                    server.port,
                    oidc_auth=auth,
                    auto_flush=False,
                    protocol_version=2) as sender:
                with self.assertRaises(OidcInteractionRequired):
                    with sender.transaction('oidc_txn_fail') as txn:
                        txn.row(
                            columns={'value': 1}, at=questdb.ServerTimestamp)
                self.assertEqual(
                    len(sender), 0,
                    'a failed commit must discard its rows rather than leave '
                    'them for the next flush to publish untransactionally')
                self.assertEqual(server.requests('/write', 'POST'), [])

                for method in (txn.commit, txn.rollback):
                    with self.assertRaises(questdb.QuestDBError) as ctx:
                        method()
                    self.assertEqual(
                        ctx.exception.code,
                        questdb.QuestDBErrorCode.InvalidApiCall)
                    self.assertIn(
                        'already completed', str(ctx.exception))

                # The sender is still usable, and the discarded rows do not
                # reappear in the next transaction.
                auth.sign_in()
                with sender.transaction('oidc_txn_ok') as txn2:
                    txn2.row(columns={'value': 2}, at=questdb.ServerTimestamp)
                writes = server.requests('/write', 'POST')

        self.assertEqual(len(writes), 1)
        self.assertIn(b'oidc_txn_ok', writes[0]['body'])
        self.assertNotIn(b'oidc_txn_fail', writes[0]['body'])

    def test_transaction_after_a_failed_flush_is_not_contaminated(self):
        """A transaction must publish only its own rows.

        `commit()` marks the transaction complete before its flush, so a
        failure there skips `__exit__`'s rollback. If a previously failed
        flush had left rows in the internal buffer, the next transaction's
        `__enter__` would publish them -- without the transactional framing
        the caller asked for, and after the caller was told they did not land.
        """
        with OidcTestServer() as server:
            auth = make_discovered_auth(server)
            with questdb.Sender(
                    questdb.Protocol.Http,
                    '127.0.0.1',
                    server.port,
                    oidc_auth=auth,
                    auto_flush=False,
                    protocol_version=2) as sender:
                sender.row(
                    'oidc_dropped', columns={'value': 1},
                    at=questdb.ServerTimestamp)
                with self.assertRaises(OidcInteractionRequired):
                    sender.flush()

                auth.sign_in()
                with sender.transaction('oidc_txn') as txn:
                    txn.row(
                        columns={'value': 2}, at=questdb.ServerTimestamp)
                writes = server.requests('/write', 'POST')

        self.assertEqual(len(writes), 1)
        body = writes[0]['body']
        self.assertIn(b'oidc_txn', body)
        self.assertNotIn(
            b'oidc_dropped', body,
            'the discarded row must not ride along in a later transaction')

    def test_retryable_oidc_row_auto_flush_clears_and_never_accumulates(self):
        """An auto-flush must clear, even for a retryable OIDC failure.

        ``last_flush_ms`` is not bumped on a failure, so ``should_auto_flush``
        stays true on a retained buffer and every subsequent row re-attempts
        the flush. A buffer left populated here therefore grows one row per
        raise until it breaches ``max_buf_size``, at which point the error
        becomes ``InvalidApiCall`` and the whole accumulation is discarded at
        once -- silent data loss behind a message that says nothing about
        authentication. A caller who needs to republish a failed batch owns
        the buffer instead; see
        ``test_caller_owned_buffer_survives_a_failed_flush_for_retry``.

        The single-row version of this test could not see any of that.
        """
        with OidcTestServer() as server:
            auth = make_discovered_auth(server)
            with questdb.Sender(
                    questdb.Protocol.Http,
                    '127.0.0.1',
                    server.port,
                    oidc_auth=auth,
                    auto_flush_rows=1,
                    auto_flush_bytes=False,
                    auto_flush_interval=False,
                    protocol_version=2) as sender:
                for value in range(5):
                    with self.assertRaises(OidcInteractionRequired):
                        sender.row(
                            'oidc_retry_row', columns={'value': value},
                            at=questdb.ServerTimestamp)
                    self.assertEqual(
                        len(sender), 0,
                        'a failed auto-flush must not retain the row; '
                        'retaining it makes the buffer grow one row per raise')
                self.assertEqual(server.requests('/write', 'POST'), [])

                # Nothing was retained, so there is nothing to recover: each
                # raise already told the caller its row did not land.
                auth.sign_in()
                sender.flush()

        self.assertEqual(server.requests('/write', 'POST'), [])

    @unittest.skipIf(pd is None, 'pandas not installed')
    def test_retryable_oidc_dataframe_auto_flush_clears_and_never_accumulates(self):
        """As the row case above, for the DataFrame auto-flush path."""
        with OidcTestServer() as server:
            auth = make_discovered_auth(server)
            with questdb.Sender(
                    questdb.Protocol.Http,
                    '127.0.0.1',
                    server.port,
                    oidc_auth=auth,
                    auto_flush_rows=1,
                    auto_flush_bytes=False,
                    auto_flush_interval=False,
                    protocol_version=2) as sender:
                for value in range(3):
                    with self.assertRaises(OidcInteractionRequired):
                        sender.dataframe(
                            pd.DataFrame({'value': [value]}),
                            table_name='oidc_retry_dataframe',
                            at=questdb.ServerTimestamp)
                    self.assertEqual(len(sender), 0)
                self.assertEqual(server.requests('/write', 'POST'), [])

                auth.sign_in()
                sender.flush()

        self.assertEqual(server.requests('/write', 'POST'), [])

    def test_oidc_error_carries_the_native_error_code(self):
        # The OIDC branch of c_err_to_py used to stamp AuthError on every native
        # error, discarding the code native had deliberately chosen. Native
        # reclassifies a recoverable token-provider failure as a retryable
        # SocketError so failover polls it again, and QuestDB.dataframe()'s
        # reconnect loop gates on exactly that code -- so `oidc_auth=` silently
        # lost the retry that a fixed `token=` still had. Two decoders in this
        # package also disagreed: the reader path reads the code directly and
        # reported SocketError for the very same error.
        auth = make_auth()  # never signed in -> InteractionRequired
        with self.assertRaises(OidcInteractionRequired) as ctx:
            with questdb.Sender(
                    questdb.Protocol.Http,
                    '127.0.0.1',
                    9000,
                    oidc_auth=auth,
                    auto_flush=False,
                    protocol_version=2) as sender:
                sender.row(
                    'oidc_code', columns={'value': 1},
                    at=questdb.ServerTimestamp)
                sender.flush()
        # Retryable, and recognised as such by the dataframe reconnect gate.
        self.assertIs(
            ctx.exception.code, questdb.QuestDBErrorCode.SocketError)
        self.assertIn(
            ctx.exception.code,
            (questdb.QuestDBErrorCode.FailoverRetry,
             questdb.QuestDBErrorCode.SocketError))

        # A native configuration failure keeps its own code rather than
        # collapsing to AuthError.
        with OidcTestServer(settings_config_overrides={
                'acl.oidc.enabled': False}) as server:
            with self.assertRaises(OidcConfigError) as cfg:
                OidcDeviceAuth.from_questdb(server.url, interactive=False)
        self.assertIs(cfg.exception.code, questdb.QuestDBErrorCode.ConfigError)

        # A directly constructed error still defaults to AuthError, so the
        # documented `except QuestDBError` contract is unchanged.
        self.assertIs(
            OidcError('x').code, questdb.QuestDBErrorCode.AuthError)

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

    def test_public_auth_surface_resolves_and_sanitizes(self):
        import questdb.auth as auth
        for name in auth.__all__:
            with self.subTest(name=name):
                self.assertTrue(hasattr(auth, name))
        self.assertEqual(
            auth.TOKEN_STORE_DIR_ENV, 'QUESTDB_CLIENT_OIDC_TOKEN_STORE_DIR')
        self.assertIs(auth.psycopg_connect, psycopg_connect)
        self.assertIs(auth.sqlalchemy_engine, sqlalchemy_engine)
        self.assertIs(auth.sanitize_display_text, sanitize_display_text)
        self.assertEqual(sanitize_display_text('\x1b[31mx\u200b'), '[31mx')

    # -- _strip_control: strip control / bidi / zero-width / format chars --
    def test_strip_control_printable_ascii_fast_path_and_unicode_slow_path(self):
        text = 'Token acquisition failed: retry later (HTTP 503).'
        self.assertIs(_render._strip_control(text), text)
        self.assertEqual(
            _render._strip_control('café\u00a0x\x1b\u200b'),
            'café x')

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

    def test_strip_control_is_stable_across_python_unicode_versions(self):
        # U+2EBF0 is a CJK Extension I letter assigned in Unicode 15.1. Older
        # supported Pythons bundle an earlier UCD and report it as Cn; it must
        # still survive instead of making the same identity version-dependent.
        newer_letter = chr(0x2EBF0)
        self.assertEqual(_render._strip_control(newer_letter), newer_letter)

        # The reserved Cn values that are intentionally invisible remain
        # stripped without relying on the interpreter's UCD classification.
        for codepoint in (0x2065, 0xFDD0, 0xFFF0, 0x1FFFE,
                          0xE0000, 0xE0002, 0xE0080, 0xE01F0):
            with self.subTest(codepoint=f'U+{codepoint:04X}'):
                self.assertEqual(_render._strip_control(chr(codepoint)), '')

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

    def test_safe_link_url_accepts_https_and_loopback_http(self):
        self.assertEqual(_render._safe_link_url('https://ok.example/verify'),
                         'https://ok.example/verify')
        self.assertEqual(_render._safe_link_url('https://xn--e1afmkfd.example/'),
                         'https://xn--e1afmkfd.example/')
        for url in (
                'http://localhost/verify',
                'http://LOCALHOST:9000/verify',
                'http://localhost./verify',
                'http://127.0.0.1/verify',
                'http://127.5.5.5/verify',
                'http://[::1]/verify'):
            with self.subTest(url=url):
                self.assertEqual(_render._safe_link_url(url), url)

    def test_safe_link_url_rejects_remote_plaintext(self):
        for url in (
                'http://idp.example.com/verify',
                'http://10.0.0.1/verify',
                'http://169.254.1.1/verify',
                'http://[fe80::1]/verify'):
            with self.subTest(url=url):
                self.assertIsNone(_render._safe_link_url(url))

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

    def test_verification_target_rejects_remote_plaintext_in_all_paths(self):
        remote = 'http://idp.example.com/device'
        self.assertIsNone(_render._verification_target({
            'verification_uri': 'https://shown.example/v',
            'browser_target': remote,
        }))
        self.assertIsNone(_render._verification_target({
            'verification_uri': remote,
            'verification_uri_complete': remote + '?code=ABCD',
        }))

    def test_verification_target_drops_diverging_complete(self):
        # No native browser_target; a complete on a different host is not used.
        self.assertEqual(
            _render._verification_target({
                'verification_uri': 'https://idp.example/v',
                'verification_uri_complete': 'https://evil.example/c'}),
            'https://idp.example/v')

    # -- a native refusal is final: nothing may re-promote the display text --
    #
    # Block-drawing glyphs are how a terminal QR is emitted, so their absence
    # is what "encoded no QR" actually means.
    _QR_GLYPHS = '█▀▄'

    def _refused_prompt(self):
        # The URL has to be one native refused for a reason the *local* vetting
        # would not reach on its own, or these tests cannot tell "honoured
        # native's verdict" from "rejected it here anyway".
        #
        # This fixture used to be an over-long URL paired with its truncated
        # display string. That could never discriminate: the display was
        # _MAX_ACTIONABLE_URL_CHARS + 1 long, so `_safe_link_url` refused it on
        # length whatever `browser_target` said, and the tests passed against
        # the very regression they name. (The over-long case has its own
        # coverage in `test_over_long_url_is_rejected_not_truncated`.)
        #
        # A punycode host is the clean separator: native refuses an IDNA
        # A-label as a confusable, while `_safe_link_url` deliberately accepts
        # one -- `_display_url` renders hosts in punycode precisely so a
        # homoglyph is visible rather than blocked. So the fallback here is
        # both willing and able to promote this URL, and only the native
        # verdict stops it.
        real = 'https://xn--80ak6aa92e.com/device'
        return real, {
            'user_code': 'ABCD-EFGH',
            'verification_uri': real,
            'verification_uri_complete': real,
            'browser_target': None,
            'expires_in': 600,
            'interval': 5}

    @unittest.skipIf(_render._qr_data_uri('https://x.example/') is None,
                     'qrcode not installed: the QR paths cannot be exercised')
    def test_jupyter_qr_encodes_only_a_vetted_target(self):
        # The Jupyter QR path had no coverage at all: every JupyterRenderer
        # test constructs it with qr=False, so `_qr_img`, `_qr_data_uri` and the
        # None/'' tri-state cache never ran -- in the renderer whose sink is raw
        # HTML built from an untrusted device response, and whose QR is followed
        # by a phone without anyone reading it first.
        real, refused = self._refused_prompt()

        renderer = _render.JupyterRenderer(qr=True)
        renderer._resp = refused
        html_out = ''.join(renderer._prompt_head())
        self.assertNotIn(
            'data:image/png;base64,', html_out,
            'a URL native refused to vet must not be encoded into a QR')
        # Control: the same renderer DOES draw one for a vetted target, so the
        # assertion above cannot pass merely because QR output is unavailable.
        vetted = _render.JupyterRenderer(qr=True)
        vetted._resp = dict(refused, browser_target=real)
        self.assertIn('data:image/png;base64,', ''.join(vetted._prompt_head()))

        # '' is the built-but-unavailable state and must be distinguishable
        # from None (not built yet), or a refused target would be retried on
        # every countdown tick.
        self.assertEqual(renderer._qr_html, '')
        # A second prompt rebuilds it: a re-sign-in has a fresh user_code, so a
        # cached image from the previous prompt would be stale. `_display` is
        # stubbed because on_prompt would otherwise emit an IPython display
        # object into the test runner's stdout.
        with mock.patch.object(vetted, '_display') as display:
            vetted.on_prompt(dict(refused, browser_target=real))
        display.assert_called_once()
        self.assertIn('data:image/png;base64,', display.call_args.args[0])

    def test_native_refusal_drops_the_open_directly_line(self):
        # format_prompt derived the "(or open directly: ...)" line straight from
        # _matched_complete, without consulting browser_target. So on a response
        # native had refused to vet, the terminal prompt printed the refused URL
        # as an instruction to open -- which many terminals hyperlink -- while
        # the QR path on the very same response correctly drew nothing.
        real, resp = self._refused_prompt()
        rendered = _render.format_prompt(resp)
        self.assertNotIn('open directly', rendered)
        # The primary line still shows the URL for transcription, but its
        # scheme is visibly defanged so terminal emulators do not auto-link it.
        self.assertIn('https[:]//xn--80ak6aa92e.com', rendered)
        self.assertNotIn('https://', rendered)
        # Control: with the verdict removed the same response DOES offer it, so
        # the assertion above is not passing because the fixture simply has no
        # origin-matched complete.
        without_verdict = {
            k: v for k, v in resp.items() if k != 'browser_target'}
        without_verdict_rendered = _render.format_prompt(without_verdict)
        self.assertIn('open directly', without_verdict_rendered)
        self.assertIn('https://', without_verdict_rendered)
        # And a vetted target keeps the line.
        self.assertIn(
            'open directly',
            _render.format_prompt(dict(resp, browser_target=real)))

    def test_native_refusal_yields_no_actionable_target(self):
        # Regression: the fallback chain used to re-promote the display string,
        # handing the user a live link and a QR for a URL native had explicitly
        # declined.
        real, resp = self._refused_prompt()
        self.assertIsNone(_render._verification_target(resp))
        # The control that makes the assertion above mean something: with the
        # verdict removed, the identical URL IS promoted. So the refusal is
        # what suppressed it, not local vetting.
        without_verdict = {
            k: v for k, v in resp.items() if k != 'browser_target'}
        self.assertFalse(_render._native_adjudicated(without_verdict))
        self.assertEqual(_render._verification_target(without_verdict), real)
        # The key being present at all is what marks the verdict as native's.
        self.assertTrue(_render._native_adjudicated(resp))

    def test_native_refusal_leaves_prompt_inert_but_visible(self):
        real, resp = self._refused_prompt()
        renderer = _render.JupyterRenderer(qr=False)
        renderer._resp = resp
        head = ''.join(renderer._prompt_head())
        self.assertNotIn('<a href', head)
        self.assertNotIn('authorize directly', head)
        # Still shown, escaped and copyable -- refusing to open is not hiding.
        self.assertIn('xn--80ak6aa92e.com', head)
        # And the control: the same URL, vetted, does get linkified.
        vetted = dict(resp, browser_target=real)
        renderer._resp = vetted
        self.assertIn('<a href', ''.join(renderer._prompt_head()))

    @unittest.skipIf(_render._qr_ascii('https://x.example/') is None,
                     'qrcode not installed: the QR paths cannot be exercised')
    def test_native_refusal_encodes_no_qr(self):
        real, resp = self._refused_prompt()
        stream = io.StringIO()
        _render.TerminalRenderer(stream=stream, qr=True).on_prompt(resp)
        rendered = stream.getvalue()
        # The actual assertion: no QR was drawn. Previously this test only
        # checked that the URL text appeared, so a renderer that encoded the
        # refused URL passed it.
        self.assertFalse(
            any(glyph in rendered for glyph in self._QR_GLYPHS),
            'a refused URL must not be encoded into a QR code')
        self.assertIn('xn--80ak6aa92e.com', rendered)
        # Control: the same renderer DOES draw one for a vetted target, so the
        # assertion above is not passing merely because QR output is disabled.
        vetted = io.StringIO()
        _render.TerminalRenderer(stream=vetted, qr=True).on_prompt(
            dict(resp, browser_target=real))
        self.assertTrue(
            any(glyph in vetted.getvalue() for glyph in self._QR_GLYPHS),
            'the fixture cannot detect a QR at all')

    def test_default_terminal_renderer_follows_stderr(self):
        # The renderer is built during OidcDeviceAuth construction, which can be
        # a long way from the sign_in() that renders through it. Binding
        # sys.stderr at construction sent the device code to whatever stream was
        # installed back then -- a redirect_stderr buffer, pytest's capture, the
        # pre-daemonize fd 2 -- and the bare `except Exception: pass` in _write
        # meant a closed one lost the prompt with no error and no log, leaving
        # sign_in() polling to the deadline with nothing on screen.
        renderer = _render.TerminalRenderer()
        late = io.StringIO()
        real_stderr = sys.stderr
        try:
            sys.stderr = late
            renderer.on_failure('after the swap')
        finally:
            sys.stderr = real_stderr
        self.assertIn('after the swap', late.getvalue())

        # An explicit stream is still honoured as given, not re-resolved.
        explicit = io.StringIO()
        pinned = _render.TerminalRenderer(stream=explicit)
        other = io.StringIO()
        try:
            sys.stderr = other
            pinned.on_failure('pinned')
        finally:
            sys.stderr = real_stderr
        self.assertIn('pinned', explicit.getvalue())
        self.assertEqual(other.getvalue(), '')

    def test_terminal_renderer_warns_once_when_it_cannot_write(self):
        closed = io.StringIO()
        closed.close()
        renderer = _render.TerminalRenderer(stream=closed)
        with self.assertLogs('questdb', level='WARNING') as captured:
            renderer.on_failure('first')
            renderer.on_failure('second')
        # Swallowing is still right -- a broken console must not abort a working
        # sign-in -- but it must not be silent, and must not spam once per
        # countdown tick.
        self.assertEqual(len(captured.records), 1, captured.output)
        self.assertIn('OIDC sign-in prompt', captured.output[0])
        self.assertNotIn('TerminalRenderer', captured.output[0])

    def test_native_vetted_target_still_drives_the_link(self):
        # The refusal path must not disturb the ordinary case.
        resp = {
            'user_code': 'A',
            'verification_uri': 'https://idp.example.com/device',
            'verification_uri_complete': 'https://idp.example.com/device?c=A',
            'browser_target': 'https://idp.example.com/device?c=A',
            'expires_in': 600, 'interval': 5}
        self.assertEqual(
            _render._verification_target(resp),
            'https://idp.example.com/device?c=A')
        renderer = _render.JupyterRenderer(qr=False)
        renderer._resp = resp
        self.assertIn('<a href', ''.join(renderer._prompt_head()))

    def test_custom_renderer_without_browser_target_keeps_fallback(self):
        # A pure-Python renderer builds its own dict with no browser_target
        # key; key ABSENCE (not a None value) selects the origin-matching
        # fallback, so custom renderers are unaffected.
        resp = {
            'verification_uri': 'https://idp.example.com/device',
            'verification_uri_complete': 'https://idp.example.com/device?c=A'}
        self.assertFalse(_render._native_adjudicated(resp))
        self.assertEqual(
            _render._verification_target(resp),
            'https://idp.example.com/device?c=A')

    def test_over_long_url_is_rejected_not_truncated(self):
        # The same bound native applies, enforced on the fallback path too.
        long_url = 'https://idp.example.com/d?p=' + 'a' * 400
        self.assertGreater(len(long_url), _render._MAX_ACTIONABLE_URL_CHARS)
        self.assertIsNone(_render._safe_link_url(long_url))
        self.assertIsNone(_render._verification_target({
            'verification_uri': long_url,
            'verification_uri_complete': long_url}))

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
            def update(self, obj, raw=False):
                captured.append(obj['text/html'])

        def _fake_display(obj, display_id=None, raw=False):
            captured.append(obj['text/html'])
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

    def test_jupyter_renderer_publishes_the_code_as_plain_text(self):
        # A kernel cannot tell what its frontend renders. `jupyter console`,
        # qtconsole and Spyder show only text/plain for display_data, which for
        # an IPython HTML object is the placeholder repr -- the user never saw
        # the URL or the code, and with the browser off by default in a kernel
        # sign_in() waited out the whole device code. Every render must carry a
        # plain-text twin with both.
        bundles = []

        class _FakeHandle:
            def update(self, obj, raw=False):
                self.raw = raw
                bundles.append((raw, dict(obj)))

        def _fake_display(obj, display_id=None, raw=False):
            bundles.append((raw, dict(obj)))
            return _FakeHandle()

        ipython = types.ModuleType('IPython')
        display_mod = types.ModuleType('IPython.display')
        display_mod.display = _fake_display
        ipython.display = display_mod
        with mock.patch.dict(
                sys.modules,
                {'IPython': ipython, 'IPython.display': display_mod}):
            renderer = _render.JupyterRenderer(qr=False)
            renderer.on_prompt({
                'user_code': 'WXYZ-1234',
                'verification_uri': 'https://idp.example.com/verify'})
            renderer.on_waiting(90)
            renderer.on_success('alice', 600)
        self.assertEqual(len(bundles), 3)
        for raw, bundle in bundles:
            self.assertTrue(raw, 'bundle must be published as raw MIME data')
            self.assertEqual(set(bundle), {'text/html', 'text/plain'})
            plain = bundle['text/plain']
            self.assertIn('WXYZ-1234', plain)
            self.assertIn('idp.example.com/verify', plain)
            self.assertNotIn('<', plain)
        self.assertIn('Signed in as alice', bundles[-1][1]['text/plain'])

    def test_status_only_render_before_prompt_omits_prompt_scaffold(self):
        # A terminal event without a preceding prompt has no URL and no user
        # code, so the panel must carry the status alone rather than an empty
        # "Open  and enter code:" scaffold.
        captured = []

        class _FakeHTML:
            def __init__(self, data):
                self.data = data

        class _FakeHandle:
            def update(self, obj, raw=False):
                captured.append(obj['text/html'])

        def _fake_display(obj, display_id=None, raw=False):
            captured.append(obj['text/html'])
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
            renderer.on_failure('boom')
        html = '\n'.join(captured)
        self.assertTrue(captured, 'renderer emitted nothing')
        self.assertIn('boom', html)
        self.assertNotIn('and enter code:', html)


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
            password='TOKEN',
            hostaddr='',  # Mask PGHOSTADDR without bypassing hostname lookup.
            # The token IS the password, so it must not reach the wire in the
            # clear or to an unauthenticated remote server.
            sslmode='verify-full')

    def test_adapters_default_to_authenticated_remote_pg_transport(self):
        # An explicit sslmode wins, and None opts out entirely for a caller
        # managing TLS through the environment or a service file.
        for override, expected in (
                ({}, 'verify-full'),
                ({'sslmode': 'verify-full'}, 'verify-full'),
                ({'sslmode': 'disable'}, 'disable')):
            with self.subTest(override=override):
                auth = mock.Mock()
                auth.token.return_value = 'TOKEN'
                driver = mock.Mock()
                with mock.patch.object(
                        _adapters, '_pg_module', return_value=driver):
                    _adapters.psycopg_connect(
                        auth, 'https://questdb.example.com:9000', **override)
                self.assertEqual(
                    driver.connect.call_args.kwargs['sslmode'], expected)

        auth = mock.Mock()
        auth.token.return_value = 'TOKEN'
        driver = mock.Mock()
        with mock.patch.object(_adapters, '_pg_module', return_value=driver):
            _adapters.psycopg_connect(
                auth, 'https://questdb.example.com:9000', sslmode=None)
        self.assertNotIn('sslmode', driver.connect.call_args.kwargs)
        self.assertEqual(driver.connect.call_args.kwargs['hostaddr'], '')

    def test_adapters_require_tls_for_localhost_names(self):
        for url in (
                'http://localhost:9000',
                'http://LOCALHOST.:9000'):
            with self.subTest(url=url):
                auth = mock.Mock()
                auth.token.return_value = 'TOKEN'
                driver = mock.Mock()
                with mock.patch.object(
                        _adapters, '_pg_module', return_value=driver):
                    _adapters.psycopg_connect(auth, url)
                self.assertEqual(
                    driver.connect.call_args.kwargs['sslmode'], 'verify-full')

    def test_adapters_require_tls_for_trailing_dot_loopback_spellings(self):
        # `127.0.0.1.` is resolved through DNS by libpq's getaddrinfo, not
        # parsed as a literal, so it must not get the numeric-loopback
        # `prefer` downgrade that would send the token in cleartext.
        for host in ('127.0.0.1.', '::1.'):
            with self.subTest(host=host):
                auth = mock.Mock()
                auth.token.return_value = 'TOKEN'
                driver = mock.Mock()
                with mock.patch.object(
                        _adapters, '_pg_module', return_value=driver):
                    _adapters.psycopg_connect(
                        auth, 'https://questdb.example.com:9000', host=host)
                self.assertEqual(
                    driver.connect.call_args.kwargs['sslmode'], 'verify-full')
        self.assertEqual(
            _adapters._effective_sslmode('127.0.0.1.', 'auto'), 'verify-full')

    def test_adapters_accept_numeric_loopback_without_tls(self):
        for url in (
                'http://127.0.0.1:9000',
                'http://127.5.5.5:9000',
                'http://[::1]:9000'):
            with self.subTest(url=url):
                auth = mock.Mock()
                auth.token.return_value = 'TOKEN'
                driver = mock.Mock()
                with mock.patch.object(
                        _adapters, '_pg_module', return_value=driver):
                    _adapters.psycopg_connect(auth, url)
                self.assertEqual(
                    driver.connect.call_args.kwargs['sslmode'], 'prefer')

    def test_bad_adapter_url_is_typed(self):
        with self.assertRaises(OidcConfigError):
            _adapters._require_host('https://host:invalid')

    def test_coerce_port_rejects_invalid(self):
        # bool (True/False is never a port), fractional/non-finite numerics,
        # non-numeric, and out-of-range must all raise the typed error rather
        # than reach the driver as a bare ValueError / silently truncate.
        for bad in (True, False, 8812.9, Decimal('8812.5'),
                    Fraction(17625, 2), float('inf'), float('nan'),
                    0, -1, 65536, 70000, 10 ** 100, 'x', None):
            with self.subTest(bad=bad), self.assertRaises(OidcConfigError):
                _adapters._coerce_port(bad)

    def test_coerce_port_accepts_valid(self):
        self.assertEqual(_adapters._coerce_port(8812), 8812)
        self.assertEqual(_adapters._coerce_port('8812'), 8812)  # e.g. from env
        self.assertEqual(_adapters._coerce_port(8812.0), 8812)  # integral float
        self.assertEqual(_adapters._coerce_port(Decimal('8812.0')), 8812)
        self.assertEqual(_adapters._coerce_port(Fraction(8812, 1)), 8812)
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

    def test_adapters_reject_a_destination_in_the_driver_passthrough(self):
        # The token is a bearer credential, so only the peer validated from
        # `url` / `host=` / `pg_port=` may receive it. SQLAlchemy merges
        # `connect_args` OVER the arguments built from that URL
        # (`cparams.update(connect_args)` in `create_engine`), so a destination
        # smuggled in there would redirect the connection -- and the password
        # -- to an unvetted host. Rejected up front, before any token exists.
        sqlalchemy = types.ModuleType('sqlalchemy')
        sqlalchemy.create_engine = mock.Mock()
        sqlalchemy.event = mock.Mock()
        sqlalchemy_engine_module = types.ModuleType('sqlalchemy.engine')
        sqlalchemy_engine_module.URL = mock.Mock()
        modules = {
            'sqlalchemy': sqlalchemy,
            'sqlalchemy.engine': sqlalchemy_engine_module,
        }
        url = 'https://questdb.example.com:9000'
        for key, value in (
                ('host', 'other.example'),
                ('hostaddr', '203.0.113.9'),
                ('port', 5432),
                ('service', 'elsewhere'),
                ('dsn', 'host=other.example'),
                ('conninfo', 'host=other.example'),
                ('HOST', 'other.example')):  # libpq keys are case-insensitive
            with self.subTest(key=key):
                auth = mock.Mock()
                with mock.patch.dict(sys.modules, modules):
                    with self.assertRaisesRegex(
                            OidcConfigError, 'connection destination'):
                        _adapters.sqlalchemy_engine(
                            auth, url, connect_args={key: value})
                auth.token.assert_not_called()
                sqlalchemy.create_engine.assert_not_called()

                if key == 'host':
                    # `host=` is psycopg_connect's OWN parameter (the
                    # documented, validated override), not a passthrough.
                    continue
                auth = mock.Mock()
                driver = mock.Mock()
                with mock.patch.object(
                        _adapters, '_pg_module', return_value=driver):
                    with self.assertRaisesRegex(
                            OidcConfigError, 'connection destination'):
                        _adapters.psycopg_connect(auth, url, **{key: value})
                auth.token.assert_not_called()
                driver.connect.assert_not_called()

        # A non-destination passthrough (the documented TLS escape hatch) is
        # untouched by the guard.
        auth = mock.Mock()
        auth.token.return_value = 'TOKEN'
        driver = mock.Mock()
        with mock.patch.object(_adapters, '_pg_module', return_value=driver):
            _adapters.psycopg_connect(
                auth, url, sslrootcert='/etc/ssl/questdb-ca.pem')
        self.assertEqual(
            driver.connect.call_args.kwargs['host'], 'questdb.example.com')
        self.assertEqual(
            driver.connect.call_args.kwargs['sslrootcert'],
            '/etc/ssl/questdb-ca.pem')

    def test_sqlalchemy_listener_refuses_to_token_a_redirected_connection(self):
        # Defence in depth for the arguments the driver actually dials: an
        # application's own `do_connect` listener registered before this one can
        # rewrite cparams after `connect_args` was vetted. The token must not be
        # attached to a destination that no longer matches the validated peer.
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
        auth = mock.Mock()
        auth.token.return_value = 'SECRET-BEARER'
        with mock.patch.dict(sys.modules, modules):
            returned = _adapters.sqlalchemy_engine(
                auth,
                'https://questdb.example.com:9000',
                drivername='postgresql+psycopg')
        listener = returned.listeners['do_connect']

        redirected = {'host': 'other.example', 'port': 8812}
        with self.assertRaisesRegex(OidcConfigError, 'refusing to send'):
            listener(None, None, [], redirected)
        auth.token.assert_not_called()
        self.assertNotIn('password', redirected)

        for cargs, cparams in (
                ([], {'port': 8812}),
                ([], {'host': 'questdb.example.com'}),
                (['host=other.example port=5432'], {
                    'host': 'questdb.example.com', 'port': 8812}),
                ([], {'host': 'questdb.example.com', 'port': 8812,
                      'hostaddr': '127.0.0.1'})):
            with self.subTest(cargs=cargs, cparams=cparams):
                with self.assertRaisesRegex(
                        OidcConfigError, 'refusing to send the OIDC token'):
                    listener(None, None, cargs, cparams)
                auth.token.assert_not_called()
                self.assertNotIn('password', cparams)

        # The validated destination still gets its token.
        expected = {'host': 'questdb.example.com', 'port': 8812}
        listener(None, None, [], expected)
        self.assertEqual(expected['password'], 'SECRET-BEARER')
        self.assertEqual(expected['hostaddr'], '')

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

    def test_sqlalchemy_rejects_non_libpq_driver_while_sslmode_is_set(self):
        # pg8000 and other non-libpq drivers take no `sslmode`: injecting it
        # failed every pooled connection with a bare TypeError. Refuse at
        # construction, and accept the driver once TLS is left to connect_args.
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

        sqlalchemy.event = Event
        sqlalchemy_engine = types.ModuleType('sqlalchemy.engine')
        sqlalchemy_engine.URL = mock.Mock()
        modules = {
            'sqlalchemy': sqlalchemy,
            'sqlalchemy.engine': sqlalchemy_engine,
        }
        url = 'https://questdb.example.com:9000'
        auth = mock.Mock()
        auth.token.return_value = 'SECRET-BEARER'
        with mock.patch.dict(sys.modules, modules):
            for sslmode in ('auto', 'require'):
                with self.subTest(sslmode=sslmode):
                    with self.assertRaisesRegex(
                            OidcConfigError, 'not a libpq driver'):
                        _adapters.sqlalchemy_engine(
                            auth, url, drivername='postgresql+pg8000',
                            sslmode=sslmode)
            sqlalchemy.create_engine.assert_not_called()
            auth.token.assert_not_called()

            # A bare `postgresql` URL selects psycopg2, a libpq driver.
            _adapters.sqlalchemy_engine(auth, url, drivername='postgresql')
            returned = _adapters.sqlalchemy_engine(
                auth, url, drivername='postgresql+pg8000', sslmode=None)
        params = {'host': 'questdb.example.com', 'port': 8812}
        returned.listeners['do_connect'](None, None, [], params)
        self.assertEqual(params['password'], 'SECRET-BEARER')
        self.assertNotIn('sslmode', params)
        self.assertNotIn('hostaddr', params)  # pg8000 is not libpq.


# A JWT (unsigned; native does not verify) whose payload carries only `sub`.
# `{}` header, `{"sub":"alice@example.com"}` payload.
ACCESS_TOKEN_WITH_SUB = 'e30.eyJzdWIiOiJhbGljZUBleGFtcGxlLmNvbSJ9.'


def _real_pg_driver_stack():
    """``(sqlalchemy, psycopg)`` if both really import, else ``None``.

    psycopg raises ImportError when it has no usable libpq, as does a missing
    package, so an importable module is a usable driver.
    """
    try:
        import psycopg
        import sqlalchemy
    except ImportError:
        return None
    return sqlalchemy, psycopg


class _TokenSequence:
    """An ``auth`` stand-in serving successive tokens, to observe rotation."""

    def __init__(self, *tokens):
        self._tokens = iter(tokens)
        self.calls = 0

    def token(self):
        self.calls += 1
        return next(self._tokens)


@unittest.skipIf(
    _real_pg_driver_stack() is None,
    'SQLAlchemy and psycopg (with libpq) are not both installed')
class AdapterRealDriverTest(unittest.TestCase):
    """The PG-wire adapters against a real SQLAlchemy and psycopg.

    `AdapterTest` drives the adapters through injected stand-in modules and
    calls the ``do_connect`` listener by hand, so neither SQLAlchemy's real
    ``(dialect, conn_rec, cargs, cparams)`` contract and its
    ``cparams.update(connect_args)`` merge order, nor what libpq actually puts
    on the wire, is exercised there. Here the real stack dials a loopback
    endpoint that records the startup message and the cleartext password and
    then refuses the login.
    """

    URL = 'http://127.0.0.1:9000'

    def _refused(self, connect):
        import psycopg
        import sqlalchemy.exc
        with self.assertRaises(
                (psycopg.OperationalError,
                 sqlalchemy.exc.OperationalError)) as ctx:
            connect()
        self.assertIn(pg_capture_server.REJECTION_MESSAGE, str(ctx.exception))

    def test_psycopg_connect_sends_a_real_token_to_the_vetted_peer(self):
        # End to end: the token a real provider signed in for is what libpq
        # sends, as `_sso`, to the host from the URL and the given pg_port.
        with OidcTestServer() as oidc_server, \
                pg_capture_server.PgCaptureServer() as pg:
            auth = make_discovered_auth(oidc_server)
            auth.sign_in()
            self._refused(lambda: psycopg_connect(
                auth, self.URL, pg_port=pg.port,
                application_name='qdb-oidc-test'))
        self.assertEqual(pg.errors, [])
        (login,) = pg.logins
        self.assertEqual(login['password'], oidc_server.initial_access_token)
        self.assertEqual(login['params']['user'], '_sso')
        self.assertEqual(login['params']['database'], 'qdb')
        # A passthrough kwarg reaches the wire.
        self.assertEqual(
            login['params']['application_name'], 'qdb-oidc-test')
        # sslmode "auto" on a numeric loopback resolves to "prefer": TLS is
        # attempted first, then plaintext is accepted.
        self.assertIn('ssl', login['encryption_requests'])

    def test_libpq_environment_hostaddr_cannot_redirect_the_token(self):
        # host= does not override PGHOSTADDR: libpq dials the latter while the
        # adapter validates the former. A numeric loopback host permits
        # plaintext, so this used to send the bearer password to this other
        # listener after it declined TLS. Construct the engine before setting
        # the environment: pooled connections must be protected at connect time.
        import psycopg
        import sqlalchemy.exc
        url = 'http://127.0.0.2:9000'
        with pg_capture_server.PgCaptureServer() as other:
            for sslmode in ('auto', None):
                with self.subTest(sslmode=sslmode):
                    auth = _TokenSequence('DIRECT-SECRET', 'POOLED-SECRET')
                    engine = sqlalchemy_engine(
                        auth, url, pg_port=other.port, sslmode=sslmode,
                        connect_args={'connect_timeout': 1})
                    try:
                        # Even if TLS is managed by libpq's environment, its
                        # destination must still come from the validated URL.
                        with mock.patch.dict(os.environ, {
                                'PGHOSTADDR': '127.0.0.1',
                                'PGSSLMODE': 'prefer'}):
                            with self.assertRaises((
                                    psycopg.OperationalError,
                                    sqlalchemy.exc.OperationalError,
                                    OidcConfigError)):
                                psycopg_connect(
                                    auth, url, pg_port=other.port,
                                    sslmode=sslmode, connect_timeout=1)
                            with self.assertRaises((
                                    psycopg.OperationalError,
                                    sqlalchemy.exc.OperationalError,
                                    OidcConfigError)):
                                engine.connect()
                    finally:
                        engine.dispose()
        self.assertEqual(other.errors, [])
        self.assertEqual(other.logins, [], 'token sent to PGHOSTADDR peer')

    def test_libpq_environment_hostaddr_does_not_block_the_vetted_peer(self):
        # An inherited address must not redirect the dial or prevent normal
        # connections when the validated numeric host itself is reachable.
        auth = _TokenSequence('DIRECT-SECRET', 'POOLED-SECRET')
        with pg_capture_server.PgCaptureServer() as pg, \
                mock.patch.dict(os.environ, {'PGHOSTADDR': '127.0.0.2'}):
            self._refused(lambda: psycopg_connect(
                auth, self.URL, pg_port=pg.port, connect_timeout=1))
            engine = sqlalchemy_engine(
                auth, self.URL, pg_port=pg.port,
                connect_args={'connect_timeout': 1})
            try:
                self._refused(engine.connect)
            finally:
                engine.dispose()
        self.assertEqual(pg.errors, [])
        self.assertEqual(
            [login['password'] for login in pg.logins],
            ['DIRECT-SECRET', 'POOLED-SECRET'])

    def test_sqlalchemy_engine_injects_a_fresh_token_per_connection(self):
        from sqlalchemy.pool import NullPool
        auth = _TokenSequence('TOKEN-1', 'TOKEN-2')
        with pg_capture_server.PgCaptureServer() as pg:
            engine = sqlalchemy_engine(
                auth, self.URL, pg_port=pg.port, poolclass=NullPool)
            try:
                self._refused(engine.connect)
                self._refused(engine.connect)
            finally:
                engine.dispose()
        self.assertEqual(pg.errors, [])
        self.assertEqual(
            [login['password'] for login in pg.logins],
            ['TOKEN-1', 'TOKEN-2'])
        self.assertEqual(auth.calls, 2)
        for login in pg.logins:
            self.assertEqual(login['params']['user'], '_sso')
            self.assertEqual(login['params']['database'], 'qdb')
            self.assertIn('ssl', login['encryption_requests'])

    def test_sqlalchemy_connect_args_merge_over_the_adapter_defaults(self):
        # SQLAlchemy merges `connect_args` into cparams before the do_connect
        # listener runs, and the listener only `setdefault`s sslmode -- so an
        # explicit sslmode wins, and other passthrough arguments survive to
        # the driver alongside the injected password.
        auth = _TokenSequence('TOKEN-1')
        with pg_capture_server.PgCaptureServer() as pg:
            engine = sqlalchemy_engine(
                auth, self.URL, pg_port=pg.port,
                connect_args={
                    'sslmode': 'disable',
                    'application_name': 'qdb-merge-test'})
            try:
                self._refused(engine.connect)
            finally:
                engine.dispose()
        self.assertEqual(pg.errors, [])
        (login,) = pg.logins
        self.assertEqual(login['password'], 'TOKEN-1')
        self.assertEqual(
            login['params']['application_name'], 'qdb-merge-test')
        # `disable`, not the adapter's `prefer`: no TLS negotiation at all.
        self.assertNotIn('ssl', login['encryption_requests'])

    def test_sqlalchemy_rejects_positional_connection_redirection(self):
        # An earlier listener can move host/port from cparams into cargs. The
        # destination check must run before auth.token(), even if a positional
        # conninfo string would otherwise send the bearer to a different peer.
        import sqlalchemy
        from sqlalchemy import event
        auth = _TokenSequence('SECRET-BEARER')

        with pg_capture_server.PgCaptureServer() as vetted, \
                pg_capture_server.PgCaptureServer() as other:
            def redirect(dialect, conn_rec, cargs, cparams):
                cparams.pop('host', None)
                cparams.pop('port', None)
                cargs[:] = [f'host=127.0.0.1 port={other.port}']

            event.listen(sqlalchemy.engine.Engine, 'do_connect', redirect)
            try:
                engine = sqlalchemy_engine(
                    auth, self.URL, pg_port=vetted.port)
                try:
                    with self.assertRaisesRegex(
                            OidcConfigError, 'refusing to send the OIDC token'):
                        engine.connect()
                finally:
                    engine.dispose()
            finally:
                event.remove(sqlalchemy.engine.Engine, 'do_connect', redirect)
        self.assertEqual(auth.calls, 0)
        self.assertEqual(vetted.logins, [])
        self.assertEqual(other.logins, [])

    def test_sqlalchemy_refuses_a_listener_that_redirects_the_connection(self):
        # A `do_connect` listener registered for every engine runs ahead of
        # the adapter's own. If it re-points the dial, the adapter must refuse
        # before attaching the token -- and nothing may reach any server.
        import sqlalchemy
        from sqlalchemy import event
        auth = _TokenSequence('TOKEN-1')

        def redirect(dialect, conn_rec, cargs, cparams):
            cparams['host'] = '127.0.0.2'

        event.listen(sqlalchemy.engine.Engine, 'do_connect', redirect)
        try:
            with pg_capture_server.PgCaptureServer() as pg:
                engine = sqlalchemy_engine(auth, self.URL, pg_port=pg.port)
                try:
                    with self.assertRaises(OidcConfigError) as ctx:
                        engine.connect()
                finally:
                    engine.dispose()
        finally:
            event.remove(sqlalchemy.engine.Engine, 'do_connect', redirect)
        self.assertIn('refusing to send the OIDC token', str(ctx.exception))
        self.assertEqual(auth.calls, 0)
        self.assertEqual(pg.logins, [])


class OidcReviewFixTest(unittest.TestCase):
    def test_success_event_carries_the_jwt_subject_as_identity(self):
        # `on_success`'s first argument is `event.identity`, which native fills
        # from the served token's `sub` claim. Every other test serves the
        # opaque 'AT-initial', so `sub` is absent and `identity` is NULL --
        # which left `assertIsNone` as the only assertion this field ever got,
        # and a regression reading `event.message` (also NULL on SUCCESS) in
        # its place would have passed unchanged.
        renderer = RecordingRenderer()
        with OidcTestServer(
                initial_access_token=ACCESS_TOKEN_WITH_SUB) as server:
            auth = make_discovered_auth(server, renderer=renderer)
            auth.sign_in()
            self.assertEqual(auth.token(), ACCESS_TOKEN_WITH_SUB)
        self.assertEqual(len(renderer.successes), 1)
        self.assertEqual(renderer.successes[0][0], 'alice@example.com')
        self.assertGreater(renderer.successes[0][1], 0)
        self.assertEqual(renderer.failures, [])

    def test_half_built_errors_module_is_treated_as_absent(self):
        # CPython publishes a module in `sys.modules` before running its body,
        # and `questdb/auth/_errors.py` imports `._render` and
        # `questdb._client` before defining a single class. Reading
        # `mod.OidcError` off a module observed in that window raises
        # AttributeError *over* the failure being reported, defeating every
        # `except QuestDBError` handler -- the exact substitution the untyped
        # fallback exists to prevent. A half-built module must count as absent
        # and must not be cached.
        half_built = types.ModuleType('questdb.auth._errors')
        self.assertFalse(hasattr(half_built, 'OidcError'))
        # Build while the real module is available; only native error
        # conversion belongs inside the simulated half-import window.
        native_failure_auth = make_auth()
        try:
            _client._debug_oidc_reset_errors_module()
            with mock.patch.dict(
                    sys.modules,
                    {'questdb.auth._errors': half_built}):
                self.assertFalse(_client._debug_oidc_errors_module_ready())
                # The importing resolver must not "fix" it by re-importing:
                # that returns the same partial object out of `sys.modules`.
                self.assertFalse(_client._debug_oidc_errors_module_resolved())
                # Drive a real native builder error through the untyped
                # conversion branch, not just the resolver predicates around
                # it. It must preserve the native code/message and remain
                # catchable by the ordinary QuestDBError base.
                with self.assertRaises(questdb.QuestDBError) as raised:
                    native_failure_auth.token()
                self.assertIs(type(raised.exception), questdb.QuestDBError)
                self.assertEqual(
                    raised.exception.code,
                    questdb.QuestDBErrorCode.AuthError)
                self.assertIn(
                    'No usable cached or refreshable OIDC token',
                    str(raised.exception))
            # Nothing was cached, so the finished module is picked up now.
            self.assertTrue(_client._debug_oidc_errors_module_resolved())
            self.assertTrue(_client._debug_oidc_errors_module_ready())
        finally:
            _client._debug_oidc_reset_errors_module()

    def test_foreground_gate_shares_the_error_class_identity(self):
        # The gate used to resolve `OidcInteractionRequired` through its own
        # `sys.modules` lookup while the exception was built from the cached
        # module. Two independent lookups disagree after any `sys.modules`
        # swap, leaving `isinstance` permanently False and the gate silently
        # disabled. Call the gate itself so these assertions discriminate that
        # regression rather than merely restating how the exception was built.
        _client._debug_oidc_reset_errors_module()
        try:
            self.assertTrue(_client._debug_oidc_errors_module_resolved())
            import questdb.auth._errors as errors
            exc = errors.OidcInteractionRequired('needs sign-in')
            self.assertTrue(
                _client._debug_is_oidc_terminal_for_foreground(exc, None))

            # Once resolved, a transient replacement in sys.modules must not
            # change the class identity used by the gate.
            half_built = types.ModuleType('questdb.auth._errors')
            with mock.patch.dict(
                    sys.modules, {'questdb.auth._errors': half_built}):
                self.assertTrue(
                    _client._debug_is_oidc_terminal_for_foreground(exc, None))

            # Classification belongs to this native error snapshot, not to a
            # later read of mutable provider state. A busy provider cannot make
            # a non-busy error transient, while the structured busy bit remains
            # transient even after the provider reports idle.
            busy_provider = types.SimpleNamespace(_sign_in_in_progress=True)
            self.assertTrue(
                _client._debug_is_oidc_terminal_for_foreground(
                    exc, busy_provider))
            exc._acquisition_busy = True
            idle_provider = types.SimpleNamespace(_sign_in_in_progress=False)
            self.assertFalse(
                _client._debug_is_oidc_terminal_for_foreground(
                    exc, idle_provider))
            self.assertFalse(_client._debug_is_oidc_terminal_for_foreground(
                errors.OidcNetworkError('network'), None))
        finally:
            _client._debug_oidc_reset_errors_module()

    def test_native_handle_publication_and_shutdown_import_order_are_pinned(self):
        # Synchronization bugs are not deterministically observable on a
        # GIL-holding CPython build. Pin the source ordering itself: native build
        # writes only a C local while the GIL is released, then publishes the
        # shared handle after reacquisition; logging must likewise import before
        # oidc.pxi registers its LIFO atexit hook.
        root = os.path.dirname(os.path.dirname(__file__))
        with open(os.path.join(root, 'src', 'questdb', 'oidc.pxi'),
                  encoding='utf-8') as source_file:
            oidc_source = source_file.read()
        build = oidc_source.index(
            'built_raw = questdb_oidc_builder_build(builder, &err)')
        reacquire = oidc_source.index('_ensure_has_gil(&gs)', build)
        publish = oidc_source.index('native.raw = built_raw', reacquire)
        self.assertLess(build, reacquire)
        self.assertLess(reacquire, publish)

        with open(os.path.join(root, 'src', 'questdb', '_client.pyx'),
                  encoding='utf-8') as source_file:
            client_source = source_file.read()
        self.assertLess(
            client_source.index('import logging'),
            client_source.index('include "oidc.pxi"'))

    def test_config_and_error_views_recheck_written_prefix(self):
        # The error view has two tiers, like the event struct: native's v1
        # prefix ends at `retry_after_seconds` and `acquisition_busy` was
        # appended after it. `questdb_error_oidc_get_view` accepts any capacity
        # from the v1 size up and writes back the prefix it knows, so a v1-only
        # library answers with exactly the v1 size.
        error_v1, error_full, config_size = _client._debug_oidc_view_sizes()
        self.assertLess(error_v1, error_full)
        self.assertEqual(
            _client._debug_oidc_view_prefix_support(
                error_v1 - 1, config_size - 1),
            (False, False, False))
        self.assertEqual(
            _client._debug_oidc_view_prefix_support(error_v1, config_size),
            (True, False, True))
        self.assertEqual(
            _client._debug_oidc_view_prefix_support(
                error_full - 1, config_size),
            (True, False, True))
        self.assertEqual(
            _client._debug_oidc_view_prefix_support(error_full, config_size),
            (True, True, True))

    def test_v1_error_view_keeps_its_classification(self):
        # A library that predates `acquisition_busy` reports a complete v1
        # prefix. Everything the typed exception needs is inside it, so the
        # conversion must keep the class and fields -- collapsing it to a bare
        # OidcError disabled `QuestDB.dataframe()`'s OidcInteractionRequired
        # fail-fast gate and every `except OidcTimeoutError` / `except
        # OidcConfigError`. Only the missing tail defaults.
        error_v1, error_full, _ = _client._debug_oidc_view_sizes()
        kinds = (
            (0, OidcConfigError),
            (1, OidcNetworkError),
            (2, OidcDeviceFlowError),
            (3, OidcTimeoutError),
            (4, OidcInteractionRequired),
            (5, OidcCancelledError),
        )
        for kind, cls in kinds:
            with self.subTest(kind=kind):
                exc = _client._debug_oidc_error_from_view_prefix(
                    error_v1, kind, True)
                self.assertIs(type(exc), cls)
                self.assertEqual(exc.status, 400)
                self.assertEqual(exc.retry_after, 7)
                # The unwritten tail is not read.
                self.assertFalse(exc._acquisition_busy)

        full = _client._debug_oidc_error_from_view_prefix(error_full, 4, True)
        self.assertIs(type(full), OidcInteractionRequired)
        self.assertTrue(full._acquisition_busy)

        # Shorter than v1: nothing is readable, so only the untyped base class.
        short = _client._debug_oidc_error_from_view_prefix(
            error_v1 - 1, 4, True)
        self.assertIs(type(short), OidcError)
        self.assertIsNone(short.status)
        self.assertFalse(short._acquisition_busy)

    def test_event_tail_fields_are_gated_by_struct_size(self):
        # A callback from an older shared library can provide the pre-interval
        # prefix or omit both appended URL fields. Static linkage makes that
        # impossible in wheels today, but the public ABI explicitly permits it.
        browser_size, interval_size = (
            _client._debug_oidc_event_tail_sizes())
        self.assertLess(browser_size, interval_size)
        self.assertEqual(_client._debug_oidc_event_tail_support(
            browser_size - 1), (False, False))
        self.assertEqual(_client._debug_oidc_event_tail_support(
            browser_size), (True, False))
        self.assertEqual(_client._debug_oidc_event_tail_support(
            interval_size - 1), (True, False))
        self.assertEqual(_client._debug_oidc_event_tail_support(
            interval_size), (True, True))



try:
    import pyarrow as _pa
except ImportError:
    _pa = None


class _RolePolicyServer(QwpAckServer):
    """QWP/WS server answering each upgrade per ``policy(index, t)``:
    ``'503'``, ``'401'``, ``'421'`` (with ``X-QuestDB-Role: REPLICA``) or
    ``'serve'``."""

    def __init__(self, policy, **kwargs):
        super().__init__(**kwargs)
        self._policy = policy
        self._t0 = None
        self._upgrades = 0

    def _handle_connection(self, conn, close_after):
        import qwp_ws_ack_server
        now = time.monotonic()
        with self._lock:
            if self._t0 is None:
                self._t0 = now
            index = self._upgrades
            self._upgrades += 1
        action = self._policy(index, now - self._t0)
        if action == 'serve':
            return super()._handle_connection(conn, close_after)
        try:
            conn.settimeout(5)
            qwp_ws_ack_server._read_until(conn, b'\r\n\r\n')
            if action == '503':
                conn.sendall(b'HTTP/1.1 503 Service Unavailable\r\n'
                             b'Content-Length: 0\r\nConnection: close\r\n\r\n')
            elif action == '401':
                conn.sendall(b'HTTP/1.1 401 Unauthorized\r\n'
                             b'Content-Length: 0\r\nConnection: close\r\n\r\n')
            else:
                conn.sendall(b'HTTP/1.1 421 Misdirected Request\r\n'
                             b'X-QuestDB-Role: REPLICA\r\n'
                             b'Content-Length: 0\r\nConnection: close\r\n\r\n')
        except OSError:
            pass
        finally:
            try:
                conn.close()
            except OSError:
                pass
            with self._lock:
                self.finished_count += 1


class _CountingArrowArray:
    """An ``__arrow_c_array__`` producer that counts its exports: one per
    prepared dataframe attempt."""

    def __init__(self, batch):
        self._batch = batch
        self.exports = 0

    def __arrow_c_array__(self, requested_schema=None):
        self.exports += 1
        return self._batch.__arrow_c_array__(requested_schema)


@unittest.skipIf(_pa is None, 'pyarrow not installed')
class OidcPoolDataframeFailoverTest(unittest.TestCase):
    """An OIDC-authenticated pool rides out failover like any other pool."""

    CONF = ('ws::addr=127.0.0.1:{port};lazy_connect=true;'
            'reconnect_max_duration_millis=20000;'
            'reconnect_initial_backoff_millis=100;'
            'reconnect_max_backoff_millis=5000;')

    def _run(self, policy, server_out=None, **connect_kwargs):
        frame = _CountingArrowArray(
            _pa.record_batch({'v': _pa.array([1, 2, 3], _pa.int64())}))
        with OidcTestServer() as idp:
            auth = make_discovered_auth(idp)
            auth.sign_in()
            with _RolePolicyServer(policy) as server:
                if server_out is not None:
                    server_out.append(server)
                with questdb.connect(
                        self.CONF.format(port=server.port),
                        oidc_auth=auth, **connect_kwargs) as db:
                    db.dataframe(
                        frame, table_name='t', at=questdb.ServerTimestamp)
        return frame

    def test_terminal_rejection_between_slices_is_not_redialled(self):
        # The first attempt fails with a retryable 503, so the retry runs in
        # slices. Native returns the 401 at once as terminal; the slice ladder
        # used to re-dial it with a doubled slice until the budget ran out,
        # re-presenting the rejected token and emitting one terminal
        # AuthFailed event per slice. Without a provider the same call dials
        # once and emits one.
        def policy(index, t):
            return '503' if index == 0 else '401'

        events = []
        servers = []
        with self.assertRaises(questdb.QuestDBError) as cm:
            self._run(
                policy, servers, connection_listener=events.append,
                connection_event_inbox_capacity=256)
        self.assertEqual(cm.exception.code, questdb.QuestDBErrorCode.AuthError)
        self.assertEqual(servers[0]._upgrades, 2)
        auth_failed = [
            e for e in events
            if e.kind is questdb.ConnectionEventKind.AuthFailed]
        self.assertEqual(len(auth_failed), 1, events)

    def test_role_election_longer_than_a_retry_slice_is_ridden_out(self):
        # The first attempt fails with a retryable 503; the reconnect then sees
        # every endpoint answer as a replica for longer than the 2s slice the
        # foreground OIDC gate retries in. That slice used to hand the
        # RoleMismatch back to Python, which raised it -- a pool without
        # `oidc_auth` waits for the promotion instead.
        def policy(index, t):
            if index == 0:
                return '503'
            return '421' if t < 3.5 else 'serve'

        self._run(policy)

    def test_outage_prepares_the_frame_once_per_attempt(self):
        # Slicing the reconnect must not re-run dataframe preparation (an
        # Arrow export, a LazyFrame collect) for every slice of an outage.
        def policy(index, t):
            return '503' if t < 4.5 else 'serve'

        frame = self._run(policy)
        self.assertEqual(frame.exports, 2)


@unittest.skipUnless(
    hasattr(signal, 'pthread_kill') and hasattr(signal, 'SIGALRM'),
    'POSIX signals required')
@unittest.skipIf(
    platform.python_implementation() == 'PyPy',
    'PyPy has no pending-call re-delivery; only Ctrl-C is re-armed there')
class OidcDiagnosticSignalTest(unittest.TestCase):
    """A signal handled while a persistence diagnostic runs on the main thread
    inside an attached transport's flush behaves as it would after the flush:
    its exception reaches the caller, and the handler runs once."""

    def _flush_with_signal(
            self, signum, handler, busy_worker=False,
            inside_logging=False, handler_failure=False):
        self._surfaced_after_flush = False
        self._handler_called = threading.Event()
        outer = self
        previous = signal.signal(signum, handler)
        sabotaged = threading.Event()
        credential = [None]
        worker_in_handler = threading.Event()
        release_worker = threading.Event()
        main = threading.main_thread()

        class _BlockingWorkerHandler(logging.Handler):
            # A slow handler (network, NFS) serving another thread's
            # diagnostic. It must not hold back the main thread's interrupt.
            def emit(self, record):
                if threading.current_thread() is main:
                    outer._handler_called.set()
                    if inside_logging:
                        signal.raise_signal(signum)
                    if handler_failure:
                        raise TimeoutError('ordinary logging handler bug')
                else:
                    worker_in_handler.set()
                    release_worker.wait(30)

        questdb_logger = logging.getLogger('questdb')
        blocking_handler = _BlockingWorkerHandler()
        worker = None

        def hook():
            # Make the refreshed token's save fail, so native reports a
            # persistence diagnostic during the flush, and deliver the signal
            # while that refresh is in flight. It targets this (server)
            # thread so the main thread's socket read is not interrupted;
            # CPython runs the Python handler on the main thread at its next
            # bytecode -- inside the diagnostic callback.
            if sabotaged.is_set():
                return
            if os.path.isfile(credential[0]):
                os.remove(credential[0])
            os.mkdir(credential[0])
            sabotaged.set()
            if not inside_logging and not handler_failure:
                signal.pthread_kill(threading.get_ident(), signum)

        try:
            with tempfile.TemporaryDirectory() as store_dir, \
                    tempfile.TemporaryDirectory() as worker_tmp, \
                    OidcTestServer(
                        initial_expires_in=4,
                        refresh_request_hook=hook) as server, \
                    OidcTestServer() as worker_server:
                auth = make_discovered_auth(
                    server, token_store=FileTokenStore.at(store_dir))
                auth.sign_in()
                credential[0] = os.path.join(store_dir, next(
                    n for n in os.listdir(store_dir) if n.endswith('.json')))
                if busy_worker:
                    worker_store = os.path.join(worker_tmp, 'store')

                    class _SabotagingRenderer(RecordingRenderer):
                        # Replace the store directory with a file, so the
                        # worker's save fails and emits a diagnostic.
                        def on_prompt(self, challenge):
                            super().on_prompt(challenge)
                            shutil.rmtree(worker_store)
                            with open(worker_store, 'w') as f:
                                f.write('not a directory')

                    worker_auth = make_discovered_auth(
                        worker_server,
                        token_store=FileTokenStore.at(worker_store),
                        renderer=_SabotagingRenderer())
                    questdb_logger.addHandler(blocking_handler)
                    worker = threading.Thread(
                        target=worker_auth.sign_in, daemon=True)
                    worker.start()
                    self.assertTrue(worker_in_handler.wait(15))
                elif inside_logging or handler_failure:
                    questdb_logger.addHandler(blocking_handler)
                sender = questdb.Sender.from_conf(
                    f'http::addr=127.0.0.1:{server.port};',
                    oidc_auth=auth, auto_flush=False)
                sender.establish()
                try:
                    deadline = time.monotonic() + 20
                    while not sabotaged.is_set():
                        self.assertLess(time.monotonic(), deadline)
                        sender.row(
                            't', columns={'v': 1},
                            at=questdb.ServerTimestamp)
                        sender.flush()
                        if not sabotaged.is_set():
                            time.sleep(0.05)
                    # A bytecode boundary after the flush that ran the refresh.
                    for _ in range(3):
                        pass
                except BaseException:
                    # Where it surfaced: here, right after the flush, or only
                    # later -- e.g. once the busy worker below is released.
                    self._surfaced_after_flush = True
                    raise
                finally:
                    sender.close(flush=False)
        finally:
            release_worker.set()
            if worker is not None:
                worker.join(15)
            questdb_logger.removeHandler(blocking_handler)
            signal.signal(signum, previous)

    def test_exception_from_a_signal_handler_reaches_the_flush_caller(self):
        def on_alarm(signum, frame):
            raise TimeoutError('deadline')

        with self.assertRaises(TimeoutError):
            self._flush_with_signal(signal.SIGALRM, on_alarm)

    def test_signal_raised_inside_diagnostic_logging_reaches_flush(self):
        class Deadline(Exception):
            pass

        for exc_type in (TimeoutError, Deadline):
            with self.subTest(exc_type=exc_type.__name__):
                def on_alarm(signum, frame):
                    raise exc_type('deadline inside logging')

                with self.assertRaises(exc_type):
                    self._flush_with_signal(
                        signal.SIGALRM, on_alarm, inside_logging=True)
                self.assertTrue(self._handler_called.is_set())
                self.assertTrue(self._surfaced_after_flush)

    def test_ordinary_logging_handler_failure_is_still_best_effort(self):
        def on_alarm(signum, frame):
            raise TimeoutError('unused alarm handler')

        self._flush_with_signal(
            signal.SIGALRM, on_alarm, handler_failure=True)
        self.assertTrue(self._handler_called.is_set())

    def test_other_threads_diagnostic_does_not_delay_the_exception(self):
        # Another thread is inside its own persistence diagnostic, blocked in
        # a slow logging handler, while the main thread's flush parks the
        # handler's exception. It must still surface right after the flush,
        # not when the unrelated handler eventually returns.
        def on_alarm(signum, frame):
            raise TimeoutError('deadline')

        with self.assertRaises(TimeoutError):
            self._flush_with_signal(
                signal.SIGALRM, on_alarm, busy_worker=True)
        self.assertTrue(
            self._surfaced_after_flush,
            'the exception was held back until the other thread\'s logging '
            'handler returned')

    def test_system_exit_from_a_signal_handler_reaches_the_flush_caller(self):
        def on_term(signum, frame):
            sys.exit(5)

        with self.assertRaises(SystemExit) as raised:
            self._flush_with_signal(signal.SIGTERM, on_term)
        self.assertEqual(raised.exception.code, 5)

    def test_custom_sigint_handler_runs_once(self):
        calls = []

        def on_int(signum, frame):
            calls.append(signum)
            raise KeyboardInterrupt('user handler')

        with self.assertRaises(KeyboardInterrupt):
            self._flush_with_signal(signal.SIGINT, on_int)
        # Let any re-delivered signal run before counting.
        for _ in range(3):
            time.sleep(0.05)
        self.assertEqual(calls, [signal.SIGINT])

    def _run_child(self, script):
        env = dict(os.environ)
        env['PYTHONPATH'] = os.pathsep.join(
            [os.path.dirname(os.path.dirname(os.path.abspath(questdb.__file__))),
             os.path.dirname(os.path.abspath(__file__))]
            + [p for p in env.get('PYTHONPATH', '').split(os.pathsep) if p])
        proc = subprocess.run(
            [sys.executable, '-c', script],
            capture_output=True, text=True, timeout=120, env=env)
        self.assertEqual(
            proc.returncode, 0,
            f'child failed (exit {proc.returncode}): '
            f'{proc.stdout}{proc.stderr}')
        return proc.stdout

    def test_two_pending_signals_both_reach_the_flush_caller(self):
        # Regression: with no foreground call, parking the first exception
        # asked `threading` for the main thread. That ran bytecode, where
        # CPython ran the second pending handler; its exception was swallowed
        # together with the first, which had not been parked yet, and the
        # flush returned normally as if neither signal had arrived. The first
        # handler must be C-level (the default SIGINT handler) for this: a
        # Python handler's own frame already clears the eval breaker. As with
        # two signals outside any callback, the second exception may surface
        # inside the handling of the first, so the check follows __context__.
        script = (
            'import os, signal, tempfile, threading, time\n'
            'import questdb\n'
            'from questdb.auth import FileTokenStore\n'
            'from oidc_test_server import OidcTestServer\n'
            'from test_auth import make_discovered_auth\n'
            'def on_alarm(signum, frame):\n'
            '    raise TimeoutError("deadline")\n'
            'signal.signal(signal.SIGALRM, on_alarm)\n'
            'signal.signal(signal.SIGINT, signal.default_int_handler)\n'
            'sabotaged = threading.Event()\n'
            'credential = [None]\n'
            'def hook():\n'
            '    if sabotaged.is_set():\n'
            '        return\n'
            '    if os.path.isfile(credential[0]):\n'
            '        os.remove(credential[0])\n'
            '    os.mkdir(credential[0])\n'
            '    sabotaged.set()\n'
            '    signal.pthread_kill(threading.get_ident(), signal.SIGINT)\n'
            '    signal.pthread_kill(threading.get_ident(), signal.SIGALRM)\n'
            'surfaced = []\n'
            'def chain(exc):\n'
            '    names = []\n'
            '    while exc is not None:\n'
            '        names.append(type(exc).__name__)\n'
            '        exc = exc.__context__\n'
            '    return names\n'
            'def attempt(fn):\n'
            '    try:\n'
            '        fn()\n'
            '    except BaseException as exc:\n'
            '        surfaced.extend(chain(exc))\n'
            'def spin():\n'
            '    for _ in range(5):\n'
            '        time.sleep(0.05)\n'
            'with tempfile.TemporaryDirectory() as store_dir, \\\n'
            '        OidcTestServer(initial_expires_in=4,\n'
            '                       refresh_request_hook=hook) as server:\n'
            '    auth = make_discovered_auth(\n'
            '        server, token_store=FileTokenStore.at(store_dir))\n'
            '    auth.sign_in()\n'
            '    credential[0] = os.path.join(store_dir, next(\n'
            '        n for n in os.listdir(store_dir) if n.endswith(".json")))\n'
            '    sender = questdb.Sender.from_conf(\n'
            '        f"http::addr=127.0.0.1:{server.port};",\n'
            '        oidc_auth=auth, auto_flush=False)\n'
            '    sender.establish()\n'
            '    def flush_until_sabotaged():\n'
            '        deadline = time.monotonic() + 20\n'
            '        while not sabotaged.is_set():\n'
            '            assert time.monotonic() < deadline\n'
            '            sender.row("t", columns={"v": 1},\n'
            '                       at=questdb.ServerTimestamp)\n'
            '            sender.flush()\n'
            '            if not sabotaged.is_set():\n'
            '                time.sleep(0.05)\n'
            '        spin()\n'
            '    stage = 0\n'
            '    while stage < 3:\n'
            '        try:\n'
            '            attempt(flush_until_sabotaged if stage == 0 else spin)\n'
            '        except BaseException as exc:\n'
            '            surfaced.extend(chain(exc))\n'
            '        stage += 1\n'
            '    sender.close(flush=False)\n'
            'print("SURFACED", sorted(set(surfaced)))\n')
        out = self._run_child(script)
        self.assertIn(
            "SURFACED ['KeyboardInterrupt', 'TimeoutError']", out, out)

    @unittest.skipUnless(hasattr(os, 'fork'), 'os.fork required')
    def test_child_forked_from_a_worker_thread_reanchors_the_main_thread(self):
        # Regression: the main-thread identity was captured once at import.
        # In a child forked from a worker thread -- which CPython makes the
        # child's main thread -- every diagnostic on that thread looked like a
        # worker's, so an exception its signal handler raised was parked and
        # never re-raised.
        script = (
            'import os, threading, warnings\n'
            'from questdb import _client\n'
            'parent_main = _client._debug_oidc_main_thread_ident()\n'
            'result = {}\n'
            'def fork_from_worker():\n'
            '    r, w = os.pipe()\n'
            '    with warnings.catch_warnings():\n'
            '        warnings.simplefilter("ignore", DeprecationWarning)\n'
            '        pid = os.fork()\n'
            '    if pid == 0:\n'
            '        ok = (_client._debug_oidc_main_thread_ident()\n'
            '              == threading.get_ident()\n'
            '              == threading.main_thread().ident)\n'
            '        os.write(w, b"1" if ok else b"0")\n'
            '        os._exit(0)\n'
            '    os.close(w)\n'
            '    result["child"] = os.read(r, 1)\n'
            '    os.close(r)\n'
            '    os.waitpid(pid, 0)\n'
            't = threading.Thread(target=fork_from_worker)\n'
            't.start()\n'
            't.join()\n'
            'assert parent_main == threading.main_thread().ident\n'
            'assert _client._debug_oidc_main_thread_ident() == parent_main\n'
            'print("CHILD", result["child"].decode())\n')
        out = self._run_child(script)
        self.assertIn('CHILD 1', out, out)


@unittest.skipUnless(hasattr(os, 'fork'), 'os.fork required')
class OidcForkSafetyTest(unittest.TestCase):
    def test_inherited_provider_never_enters_a_locked_native_auth(self):
        # Fork while the sign-in worker owns the native acquisition lock and
        # waits on the IdP's HTTP response. An inherited close used to block
        # forever waiting for that worker, which no longer exists in the child.
        # Run in a subprocess AND bound the inner child: a regression must fail
        # the test rather than hang the entire suite.
        script = '''
import gc, os, signal, threading, time, warnings
from questdb import QuestDBErrorCode, _client
from questdb.auth import OidcCancelledError, OidcConfigError, OidcDeviceAuth, Renderer
from oidc_test_server import OidcTestServer

entered, release = threading.Event(), threading.Event()
with OidcTestServer() as server:
    original = server._handle
    def hold_device_response(handler):
        if handler.path == '/device':
            entered.set()
            assert release.wait(20), 'device response was not released'
        original(handler)
    server._handle = hold_device_response
    auth = OidcDeviceAuth.from_questdb(
        server.url, renderer=Renderer(), interactive=True,
        open_browser=False, timeout=30)
    idle = OidcDeviceAuth(
        'questdb', server.url + '/device', server.url + '/token',
        interactive=False, open_browser=False)
    idle_id = _client._debug_oidc_last_provider_id()
    worker_error = []
    def sign_in():
        try:
            auth.sign_in()
        except OidcCancelledError:
            pass
        except BaseException as exc:
            worker_error.append(exc)
    worker = threading.Thread(target=sign_in, daemon=True)
    worker.start()
    assert entered.wait(5), 'sign-in never reached the blocked HTTP request'
    read_fd, write_fd = os.pipe()
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', DeprecationWarning)
        pid = os.fork()
    if pid == 0:
        os.close(read_fd)
        try:
            for method in ('close', 'token', 'clear', 'cancel_sign_in', 'config'):
                try:
                    getattr(auth, method)() if method != 'config' else auth.config
                except OidcConfigError as exc:
                    assert exc.code == QuestDBErrorCode.ConfigError
                    assert 'fork' in str(exc)
                else:
                    raise AssertionError(method + ' accepted an inherited auth')
            # Inherited finalization must not call into the parent's native
            # callback gates, even when the provider is otherwise idle.
            del idle
            gc.collect()
            assert idle_id not in _client._debug_oidc_registry_snapshot()[1]
            try:
                OidcDeviceAuth(
                    'questdb', server.url + '/device', server.url + '/token',
                    interactive=False, open_browser=False)
            except OidcConfigError as exc:
                assert exc.code == QuestDBErrorCode.ConfigError
                assert 'exec' in str(exc)
            else:
                raise AssertionError('new provider initialized after fork')
            os.write(write_fd, b'OK')
            os._exit(0)
        except BaseException as exc:
            os.write(write_fd, (type(exc).__name__ + ': ' + str(exc)).encode())
            os._exit(1)
    os.close(write_fd)
    try:
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            done, status = os.waitpid(pid, os.WNOHANG)
            if done:
                break
            time.sleep(.02)
        else:
            os.kill(pid, signal.SIGKILL)
            os.waitpid(pid, 0)
            raise AssertionError('child hung using inherited OIDC provider')
        result = os.read(read_fd, 4096)
        assert os.WIFEXITED(status) and os.WEXITSTATUS(status) == 0, (status, result)
        assert result == b'OK', result
    finally:
        os.close(read_fd)
        release.set()
        auth.cancel_sign_in()
        worker.join(5)
        assert not worker.is_alive()
        assert not worker_error, worker_error
        auth.close()
        idle.close()
'''
        env = dict(os.environ)
        env['PYTHONPATH'] = os.pathsep.join(
            [os.path.dirname(os.path.abspath(__file__)),
             os.path.dirname(os.path.dirname(os.path.abspath(questdb.__file__)))]
            + [p for p in env.get('PYTHONPATH', '').split(os.pathsep) if p])
        proc = subprocess.run(
            [sys.executable, '-c', script], env=env,
            capture_output=True, text=True, timeout=30)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)


class OidcApiContractTest(unittest.TestCase):
    def test_concurrent_sign_in_reports_invalid_api_call(self):
        # A second sign_in() while one runs is a busy refusal, not a terminal
        # auth failure: the provider stays open and the first call continues.
        # It reports InvalidApiCall, as a clear() refused under the same
        # condition does.
        waiting = threading.Event()

        class _Renderer(Renderer):
            def on_waiting(self, seconds_left):
                waiting.set()

        pending = (400, {'error': 'authorization_pending'}, None)
        with OidcTestServer(
                device_token_response=pending, device_expires_in=15,
                device_interval=1) as server:
            auth = make_discovered_auth(server, renderer=_Renderer())
            first = threading.Thread(
                target=lambda: self.assertRaises(
                    OidcCancelledError, auth.sign_in))
            first.start()
            try:
                self.assertTrue(waiting.wait(20))
                with self.assertRaises(OidcError) as raised:
                    auth.sign_in()
                self.assertEqual(
                    raised.exception.code,
                    questdb.QuestDBErrorCode.InvalidApiCall)
            finally:
                auth.cancel_sign_in()
                first.join(20)
            self.assertFalse(first.is_alive())

    def test_config_strings_over_1_mib_are_rejected_before_parsing(self):
        padding = 'a' * (1 << 20)
        for conf in (
                f'http::addr=localhost:9000;username={padding};password=p;',
                f'ws::addr=localhost:9000;token={padding};'):
            with self.subTest(protocol=conf.split('::')[0]):
                with self.assertRaises(questdb.QuestDBError) as raised:
                    questdb.Sender.from_conf(conf)
                self.assertEqual(
                    raised.exception.code,
                    questdb.QuestDBErrorCode.InvalidApiCall)
                self.assertIn('1 MiB', str(raised.exception))
        # Windows caps a real environment variable at 32767 characters, so
        # `os.environ` refuses a value this long there. `from_env` reads the
        # mapping, so substitute it rather than calling putenv().
        env = dict(os.environ)
        env['QDB_CLIENT_CONF'] = (
            f'http::addr=localhost:9000;username={padding};')
        with mock.patch.object(os, 'environ', env):
            with self.assertRaises(questdb.QuestDBError) as raised:
                questdb.Sender.from_env()
        self.assertEqual(
            raised.exception.code, questdb.QuestDBErrorCode.InvalidApiCall)
        self.assertIn('1 MiB', str(raised.exception))
        # Exactly at the cap is still accepted by the length check.
        head = 'http::addr=localhost:9000;username='
        tail = ';password=p;'
        exact = head + 'a' * ((1 << 20) - len(head) - len(tail)) + tail
        self.assertEqual(len(exact.encode()), 1 << 20)
        questdb.Sender.from_conf(exact)


if __name__ == '__main__':
    unittest.main()
