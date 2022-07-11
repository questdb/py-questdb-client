import socket
import select
import re


NON_ESCAPED_NEW_LINE_RE = re.compile(rb'(?<!\\)\n')


class Server:
    def __init__(self):
        self._sock = None
        self._client_sock = None
        self.msgs = []

    def __enter__(self):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind(('', 0))
        self._sock.listen()
        self.port = self._sock.getsockname()[1]
        return self

    def accept(self):
        self._client_sock = self._sock.accept()[0]
        self._client_sock.setblocking(False)

    def recv(self, wait_timeout_sec=0.1):
        # Bail out early if there's no data.
        if not select.select([self._client_sock], [], [], wait_timeout_sec)[0]:
            return []

        # Read full lines.
        buf = b''
        while True:
            # Block for *some* data.
            select.select([self._client_sock], [], [])
            buf += self._client_sock.recv(1024)
            if len(buf) < 2:
                continue
            if (buf[-1] == ord('\n')) and (buf[-2] != ord('\\')):
                break
        self.last_buf = buf
        new_msgs = NON_ESCAPED_NEW_LINE_RE.split(buf)[:-1]
        self.msgs.extend(new_msgs)
        return new_msgs

    def __exit__(self, _ex_type, _ex_value, _ex_tb):
        if self._client_sock:
            self._client_sock.close()
            self._client_sock = None
        if self._sock:
            self._sock.close()
            self._sock = None
