"""Regression tests for the PostgreSQL login-capture fixture."""

import io
import socket
import struct
import unittest
from unittest import mock

from pg_capture_server import PgCaptureServer


class TestPgCaptureServer(unittest.TestCase):
    def _connection(self, *, with_password=True):
        startup = (struct.pack('!I', 196608)
                   + b'user\0_sso\0database\0qdb\0\0')
        data = struct.pack('!I', 4 + len(startup)) + startup
        if with_password:
            password = b'test-token\0'
            data += b'p' + struct.pack('!I', 4 + len(password)) + password
        conn = mock.MagicMock(spec=socket.socket)
        conn.__enter__.return_value = conn
        conn.recv.side_effect = io.BytesIO(data).read
        return conn

    def test_records_login_before_rejection_exactly_once(self):
        expected = [{
            'params': {'user': '_sso', 'database': 'qdb'},
            'password': 'test-token',
            'encryption_requests': [],
        }]
        for send_error in (None, OSError('rejection send failed')):
            with self.subTest(send_error=send_error):
                server = PgCaptureServer()
                conn = self._connection()
                at_rejection = []

                def sendall(data):
                    if data[:1] == b'E':
                        # Observe the exact point at which a driver could
                        # receive the rejection, without a scheduling race.
                        at_rejection.extend(server.logins)
                        if send_error is not None:
                            raise send_error

                conn.sendall.side_effect = sendall
                server._serve(conn)
                self.assertEqual(at_rejection, expected)
                self.assertEqual(server.logins, expected)
                self.assertEqual(
                    server.errors, [] if send_error is None else [send_error])

    def test_records_startup_when_password_read_fails(self):
        server = PgCaptureServer()
        server._serve(self._connection(with_password=False))
        self.assertEqual(server.logins, [{
            'params': {'user': '_sso', 'database': 'qdb'},
            'password': None,
            'encryption_requests': [],
        }])
        self.assertEqual(len(server.errors), 1)
        self.assertIsInstance(server.errors[0], ConnectionError)

    def test_does_not_record_without_startup(self):
        server = PgCaptureServer()
        conn = self._connection()
        conn.recv.side_effect = io.BytesIO().read
        server._serve(conn)
        self.assertEqual(server.logins, [])
        self.assertEqual(len(server.errors), 1)
        self.assertIsInstance(server.errors[0], ConnectionError)
