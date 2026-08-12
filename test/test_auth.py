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
import sys
import tempfile
import types
import unittest
import weakref
from unittest import mock

import questdb
from questdb.auth import (
    FileTokenStore,
    OidcConfigError,
    OidcDeviceAuth,
    OidcInteractionRequired,
    Renderer,
)
from questdb.auth import _adapters
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
        self.assertEqual(config.scope, 'openid groups')
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

    def test_invalid_unicode_is_typed(self):
        with self.assertRaises(OidcConfigError):
            OidcDeviceAuth(
                '\ud800',
                'https://idp.example/device',
                'https://idp.example/token',
                interactive=False)

    def test_custom_store_is_rejected(self):
        with self.assertRaisesRegex(OidcConfigError, 'FileTokenStore'):
            make_auth(token_store=object())

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
                {'QUESTDB_CLIENT_OIDC_TOKEN_STORE_DIR': '/tmp/qdb-oidc-test'}):
            self.assertEqual(
                FileTokenStore.at_default_location().directory,
                '/tmp/qdb-oidc-test')

    def test_renderer_must_implement_interface(self):
        with self.assertRaisesRegex(OidcConfigError, 'renderer'):
            make_auth(renderer=object())

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

    def test_renderer_provider_cycle_is_collected(self):
        renderer = Renderer()
        auth = make_auth(renderer=renderer)
        renderer.auth = auth
        renderer_ref = weakref.ref(renderer)
        auth_ref = weakref.ref(auth)

        del renderer, auth
        for _ in range(3):
            gc.collect()
            if renderer_ref() is None and auth_ref() is None:
                break

        self.assertIsNone(renderer_ref())
        self.assertIsNone(auth_ref())


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
            'browser_target': (
                server.url + '/verify?user_code=WXYZ-1234'),
        }])
        self.assertEqual(len(renderer.successes), 1)
        self.assertIsNone(renderer.successes[0][0])
        self.assertGreater(renderer.successes[0][1], 0)
        self.assertEqual(renderer.failures, [])

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


class NativeTransportAttachmentTest(unittest.TestCase):
    def setUp(self):
        self.auth = make_auth()

    def test_sender_accepts_shared_provider(self):
        sender = questdb.Sender(
            questdb.Protocol.Ws,
            'localhost',
            9000,
            oidc_auth=self.auth,
            auto_flush=False)
        sender.close(flush=False)

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
        sender = questdb.Sender.from_conf(
            'https::addr=localhost:9000;',
            oidc_auth=self.auth,
            auto_flush=False)
        sender.close(flush=False)

    def test_sender_rejects_fixed_and_rotating_token(self):
        with self.assertRaises(questdb.QuestDBError):
            questdb.Sender(
                questdb.Protocol.Http,
                'localhost',
                9000,
                token='fixed',
                oidc_auth=self.auth)

    def test_sender_rejects_wrong_auth_type(self):
        with self.assertRaisesRegex(TypeError, 'OidcDeviceAuth'):
            questdb.Sender(
                questdb.Protocol.Http,
                'localhost',
                9000,
                oidc_auth=object())

    def test_lazy_pool_accepts_shared_provider(self):
        with questdb.connect(
                'ws::addr=localhost:9000;lazy_connect=true;',
                oidc_auth=self.auth):
            pass

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
