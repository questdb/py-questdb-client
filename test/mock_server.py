import json
import socket
import select
import re
import http.server as hs
import threading
import time
import struct

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
        if not select.select([self._client_sock], [], [], wait_timeout_sec)[0]:
            return []

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
        new_msgs = []
        head = 0
        index = 0

        while index < len(buf):
            if index < len(buf) - 1 and buf[index] == ord('=') and buf[index + 1] == ord('='):
                new_index = self._parse_binary_data(buf, index)
                if new_index > len(buf):
                    break
                index = new_index
                continue

            if index > 0 and buf[index] == ord('\n') and buf[index - 1] != ord('\\'):
                new_msgs.append(buf[head:index])
                head = index + 1

            index += 1

        self.msgs.extend(new_msgs)
        return new_msgs

    def _parse_binary_data(self, buf, index):
        if buf[index] != ord('=') or index + 1 >= len(buf) or buf[index + 1] != ord('='):
            return index

        index += 2  # skip "=="
        if index >= len(buf):
            return index
        binary_type = buf[index]
        index += 1

        if binary_type == 16:
            index += 8
        elif binary_type == 14:
            # dims
            if index + 1 >= len(buf):
                return index
            index += 1
            if index >= len(buf):
                return index
            dims = buf[index]
            index += 1

            total_elements = 1
            for _ in range(dims):
                if index + 4 > len(buf):
                    return index
                dim_size = struct.unpack('<i', buf[index:index + 4])[0]
                index += 4
                total_elements *= dim_size

            elem_size = 8
            index += total_elements * elem_size
        else:
            pass

        return index

    def close(self):
        if self._client_sock:
            self._client_sock.close()
            self._client_sock = None
        if self._sock:
            self._sock.close()
            self._sock = None

    def __exit__(self, _ex_type, _ex_value, _ex_tb):
        self.close()

SETTINGS_WITH_PROTOCOL_VERSION_V1 = '{"config":{"release.type":"OSS","release.version":"[DEVELOPMENT]","line.proto.support.versions":[1],"ilp.proto.transports":["tcp","http"],"posthog.enabled":false,"posthog.api.key":null,"cairo.max.file.name.length":127},"preferences.version":0,"preferences":{}}'
SETTINGS_WITH_PROTOCOL_VERSION_V2 = '{"config":{"release.type":"OSS","release.version":"[DEVELOPMENT]","line.proto.support.versions":[2],"ilp.proto.transports":["tcp","http"],"posthog.enabled":false,"posthog.api.key":null,"cairo.max.file.name.length":127},"preferences.version":0,"preferences":{}}'
SETTINGS_WITH_PROTOCOL_VERSION_V3 = '{"config":{"release.type":"OSS","release.version":"[DEVELOPMENT]","line.proto.support.versions":[3],"ilp.proto.transports":["tcp","http"],"posthog.enabled":false,"posthog.api.key":null,"cairo.max.file.name.length":127},"preferences.version":0,"preferences":{}}'
SETTINGS_WITH_PROTOCOL_VERSION_V1_V2 = '{"config":{"release.type":"OSS","release.version":"[DEVELOPMENT]","line.proto.support.versions":[1,2],"ilp.proto.transports":["tcp","http"],"posthog.enabled":false,"posthog.api.key":null,"cairo.max.file.name.length":127},"preferences.version":0,"preferences":{}}'
SETTINGS_WITHOUT_PROTOCOL_VERSION = '{ "release.type": "OSS", "release.version": "[DEVELOPMENT]", "acl.enabled": false, "posthog.enabled": false, "posthog.api.key": null }'

class HttpServer:
    def __init__(self, settings=SETTINGS_WITH_PROTOCOL_VERSION_V1_V2, delay_seconds=0):
        self.delay_seconds = delay_seconds
        self.requests = []
        self.responses = []
        self.headers = []
        self.settings = settings
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
        server_settings = self.settings.encode('utf-8')

        class IlpHttpHandler(hs.BaseHTTPRequestHandler):
            def do_GET(self):
                try:
                    time.sleep(delay_seconds)
                    headers.append(dict(self.headers.items()))
                    content_length = self.headers.get('Content-Length', 0)
                    if content_length:
                        self.rfile.read(int(content_length))

                    if len(server_settings) == 0:
                        self.send_error(404, "Endpoint not found")
                    else:
                        if self.path == '/settings':
                            response_data = server_settings
                            self.send_response(200)
                            self.send_header('Content-Type', 'application/json')
                            self.send_header('Content-Length', len(response_data))
                            self.end_headers()
                            self.wfile.write(response_data)
                            self.wfile.flush()
                        else:
                            self.send_error(404, "Endpoint not found")
                except BrokenPipeError:
                    pass

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
        print(f"HTTP server started on port {self._http_server.server_port}")
        return self

    def __exit__(self, _ex_type, _ex_value, _ex_tb):
        print(f"HTTP server exit on port {self._http_server.server_port}")
        self._http_server.shutdown()
        self._http_server.server_close()
        self._stop_event.set()

    @property
    def port(self):
        return self._http_server.server_port
