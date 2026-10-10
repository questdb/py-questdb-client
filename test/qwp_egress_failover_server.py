"""Small QWP/WebSocket reader fixture: drop a query mid-stream, then serve its replay."""

import socket
import struct
import threading

from qwp_ws_ack_server import (
    _compute_accept, _fin_close, _header, _optional_header,
    _read_frame, _read_until, _request_path, _write_frame,
)


def _framed(payload, table_count=0):
    return b'QWP1' + bytes([1, 0]) + struct.pack('<HI', table_count, len(payload)) + payload


def _varint(value):
    result = bytearray()
    while value >= 128:
        result.append((value & 127) | 128)
        value >>= 7
    result.append(value)
    return bytes(result)


def _server_info(node):
    cluster = b'test-cluster'
    node = node.encode()
    return _framed(bytes([0x18, 0]) + struct.pack('<QIq', 0, 0, 0)
                   + struct.pack('<H', len(cluster)) + cluster
                   + struct.pack('<H', len(node)) + node)


def _result(request_id):
    batch = bytearray([0x11]) + struct.pack('<q', request_id)
    batch += _varint(0) + _varint(0) + _varint(3)
    batch += _varint(1) + _varint(1) + b'v' + bytes([0x05, 0])
    for value in (1, 2, 3):
        batch += struct.pack('<q', value)
    end = bytes([0x12]) + struct.pack('<q', request_id) + _varint(0) + _varint(0)
    return _framed(batch, 1), _framed(end)


class EgressFailoverServer:
    def __init__(self, deliver_first_batch=False):
        self.deliver_first_batch = deliver_first_batch
        self.first_batch_sent = threading.Event()
        self.release_first = threading.Event()
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._reader_count = 0
        self.authorizations = []
        self.errors = []
        self._threads = []

    def __enter__(self):
        self._sock = socket.socket()
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind(('127.0.0.1', 0))
        self._sock.listen()
        self._sock.settimeout(0.2)
        self.port = self._sock.getsockname()[1]
        self._thread = threading.Thread(target=self._accept, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *_args):
        self._stop.set()
        self.release_first.set()
        self._thread.join(timeout=2)
        for thread in self._threads:
            thread.join(timeout=2)
        self._sock.close()

    def _accept(self):
        while not self._stop.is_set():
            try:
                conn, _addr = self._sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            thread = threading.Thread(target=self._handle, args=(conn,), daemon=True)
            self._threads.append(thread)
            thread.start()

    def _handle(self, conn):
        try:
            conn.settimeout(30)
            request = _read_until(conn, b'\r\n\r\n')
            if not (_request_path(request) or '').startswith('/read'):
                conn.sendall(b'HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n')
                return
            with self._lock:
                self._reader_count += 1
                index = self._reader_count
                self.authorizations.append(_optional_header(request, 'Authorization'))
            key = _header(request, 'Sec-WebSocket-Key')
            conn.sendall((
                'HTTP/1.1 101 Switching Protocols\r\n'
                'Upgrade: websocket\r\nConnection: Upgrade\r\n'
                f'Sec-WebSocket-Accept: {_compute_accept(key)}\r\n'
                'X-QWP-Version: 1\r\n\r\n').encode('ascii'))
            _write_frame(conn, 0x2, _server_info(f'node{index}'))
            while True:
                frame = _read_frame(conn)
                if frame is None:
                    return
                _fin, opcode, payload = frame
                if opcode == 0x9:
                    _write_frame(conn, 0xA, payload)
                    continue
                if opcode == 0x2 and payload and payload[0] == 0x10:
                    request_id = struct.unpack('<q', payload[1:9])[0]
                    break
            if index == 1:
                if self.deliver_first_batch:
                    _write_frame(conn, 0x2, _result(request_id)[0])
                    self.first_batch_sent.set()
                self.release_first.wait(30)
                _fin_close(conn)
            else:
                for frame in _result(request_id):
                    _write_frame(conn, 0x2, frame)
                while not self._stop.is_set():
                    frame = _read_frame(conn)
                    if frame is None or frame[1] == 0x8:
                        return
                    if frame[1] == 0x9:
                        _write_frame(conn, 0xA, frame[2])
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError,
                socket.timeout, OSError):
            pass
        except Exception as exc:
            self.errors.append(repr(exc))
        finally:
            conn.close()
