import socket
import select
import re
import http.server as hs
import threading
import time

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
            res =  select.select([self._client_sock], [], [], 1.0)
            new_data = self._client_sock.recv(1024)
            if not new_data:
                return []
            buf += new_data
            if len(buf) < 2:
                continue
            if (buf[-1] == ord('\n')) and (buf[-2] != ord('\\')):
                break
        self.last_buf = buf
        new_msgs = NON_ESCAPED_NEW_LINE_RE.split(buf)[:-1]
        self.msgs.extend(new_msgs)
        return new_msgs

    def close(self):
        if self._client_sock:
            self._client_sock.close()
            self._client_sock = None
        if self._sock:
            self._sock.close()
            self._sock = None

    def __exit__(self, _ex_type, _ex_value, _ex_tb):
        self.close()

class HttpServer:
    def __init__(self, delay_seconds=0):
        self.delay_seconds = delay_seconds
        self.requests = []
        self.responses = []
        self.headers = []
        self._ready_event = None
        self._stop_event = None
        self._http_server = None
        self._http_server_thread = None

    def _serve(self):
        self._http_server.serve_forever()
        self._stop_event.set()

    def create_handler(self):
        delay_seconds = self.delay_seconds
        requests = self.requests
        headers = self.headers
        responses = self.responses

        class IlpHttpHandler(hs.BaseHTTPRequestHandler):
            def do_POST(self):
                time.sleep(delay_seconds)

                try:
                    headers.append({key: value for key, value in self.headers.items()})
                    content_length = int(self.headers['Content-Length'])
                    body = self.rfile.read(content_length)
                    requests.append(body)
                    try:
                        wait_ms, code, content_type, body = responses.pop(0)
                    except IndexError:
                        wait_ms, code, content_type, body = 0, 200, None, None
                    time.sleep(wait_ms / 1000)
                    self.send_response(code)
                    if content_type:
                        self.send_header('Content-Type', content_type)
                    if body:
                        self.send_header('Content-Length', len(body))
                    self.end_headers()
                    if body:
                        self.wfile.write(body)
                except BrokenPipeError:
                    pass

        return IlpHttpHandler

    def __enter__(self):
        self._stop_event = threading.Event()
        handler_class = self.create_handler()
        self._http_server = hs.HTTPServer(('', 0), handler_class, bind_and_activate=True)
        self._http_server_thread = threading.Thread(target=self._serve)
        self._http_server_thread.start()
        return self

    def __exit__(self, _ex_type, _ex_value, _ex_tb):
        self._http_server.shutdown()
        self._http_server.server_close()
        self._stop_event.set()

    @property
    def port(self):
        return self._http_server.server_port
