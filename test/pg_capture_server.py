"""Loopback PostgreSQL wire-protocol endpoint that records what a driver sends.

Just enough of the v3 startup handshake for a real libpq-based driver
(psycopg / psycopg2, directly or under SQLAlchemy) to deliver its startup
parameters and cleartext password: it declines SSL and GSS encryption, asks
for a cleartext password, records it, and then refuses the login with an
ordinary ``28P01`` error. The driver therefore raises its usual
``OperationalError`` after the password has crossed the wire, which is the
moment the OIDC adapters exist to get right. No query is ever answered.
"""

from __future__ import annotations

import socket
import struct
import threading

_SSL_REQUEST = 80877103
_GSSENC_REQUEST = 80877104
_CANCEL_REQUEST = 80877102
_PROTOCOL_3 = 196608

REJECTION_MESSAGE = 'pg capture server: login recorded and refused'


def _recv_exact(conn, n):
    buf = bytearray()
    while len(buf) < n:
        chunk = conn.recv(n - len(buf))
        if not chunk:
            raise ConnectionError('peer closed mid-message')
        buf += chunk
    return bytes(buf)


def _error_response(message):
    fields = b''
    for code, value in ((b'S', 'FATAL'), (b'V', 'FATAL'),
                        (b'C', '28P01'), (b'M', message)):
        fields += code + value.encode('utf-8') + b'\0'
    fields += b'\0'
    return b'E' + struct.pack('!I', 4 + len(fields)) + fields


class PgCaptureServer:
    """Records one ``dict`` per login attempt in :attr:`logins`.

    Each has ``params`` (the startup parameters, e.g. ``user`` / ``database``),
    ``password`` (``None`` if the driver never sent one) and
    ``encryption_requests`` (the SSL / GSS negotiation requests declined
    before startup, in order).
    """

    def __init__(self, host='127.0.0.1'):
        self.host = host
        self.port = None
        self._sock = None
        self._thread = None
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._logins = []
        self.errors = []

    @property
    def logins(self):
        with self._lock:
            return list(self._logins)

    def __enter__(self):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind((self.host, 0))
        self._sock.listen()
        self._sock.settimeout(0.2)
        self.port = self._sock.getsockname()[1]
        self._thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *_exc):
        self._stop.set()
        self._thread.join(5)
        self._sock.close()

    def _accept_loop(self):
        while not self._stop.is_set():
            try:
                conn, _ = self._sock.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            threading.Thread(
                target=self._serve, args=(conn,), daemon=True).start()

    def _serve(self, conn):
        login = {'params': None, 'password': None, 'encryption_requests': []}
        try:
            with conn:
                conn.settimeout(10)
                while True:
                    (length,) = struct.unpack('!I', _recv_exact(conn, 4))
                    body = _recv_exact(conn, length - 4)
                    (code,) = struct.unpack('!I', body[:4])
                    if code in (_SSL_REQUEST, _GSSENC_REQUEST):
                        login['encryption_requests'].append(
                            'ssl' if code == _SSL_REQUEST else 'gss')
                        conn.sendall(b'N')
                        continue
                    if code == _CANCEL_REQUEST:
                        return
                    if code != _PROTOCOL_3:
                        raise ValueError(f'unexpected startup code {code}')
                    parts = body[4:].split(b'\0')
                    params = {}
                    for key, value in zip(parts[0::2], parts[1::2]):
                        if not key:
                            break
                        params[key.decode()] = value.decode()
                    login['params'] = params
                    break
                # AuthenticationCleartextPassword.
                conn.sendall(b'R' + struct.pack('!II', 8, 3))
                tag = _recv_exact(conn, 1)
                (length,) = struct.unpack('!I', _recv_exact(conn, 4))
                body = _recv_exact(conn, length - 4)
                if tag != b'p':
                    raise ValueError(f'expected a password message, got {tag!r}')
                login['password'] = body.rstrip(b'\0').decode()
                conn.sendall(_error_response(REJECTION_MESSAGE))
        except Exception as exc:  # recorded for the test to assert on
            self.errors.append(exc)
        finally:
            if login['params'] is not None:
                with self._lock:
                    self._logins.append(login)
