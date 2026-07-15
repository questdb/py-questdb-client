import base64
import hashlib
import socket
import struct
import threading
import time


WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
QWP_STATUS_OK = 0x00
QWP_FLAG_DEFER_COMMIT = 0x01


class QwpAckServer:
    def __init__(self, *, host="127.0.0.1", ack_delay_s=0.0,
                 close_plan=None, max_batch_size=0,
                 defer_aware_acks=False):
        """
        `close_plan`: iterable consumed one value per accepted connection;
        a connection with value N is closed after handling its Nth binary
        frame (0 = right after the upgrade). None (or an exhausted plan)
        means the connection is served normally.

        `max_batch_size`: when > 0, advertised as `X-QWP-Max-Batch-Size`
        in the upgrade response, capping the client's per-frame size.

        `defer_aware_acks`: when True, mimic the real server's commit
        semantics — frames flagged QWP_FLAG_DEFER_COMMIT are not acked
        until the next non-deferred (commit-boundary) frame arrives,
        which acks cumulatively. Default acks every frame immediately.
        """
        self.host = host
        self.ack_delay_s = ack_delay_s
        self._close_iter = iter(close_plan) if close_plan is not None else None
        self.max_batch_size = max_batch_size
        self.defer_aware_acks = defer_aware_acks
        self.port = None
        self._sock = None
        self._stop = threading.Event()
        self._thread = None
        self._handlers = []
        self._lock = threading.Lock()
        self.accept_count = 0
        self.finished_count = 0
        self.binary_frame_count = 0
        self.qwp1_frame_count = 0
        self.binary_bytes = 0
        self.binary_prefixes = []
        self.control_frame_count = 0
        self.errors = []

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.stop()

    def start(self):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind((self.host, 0))
        self._sock.listen()
        self._sock.settimeout(0.2)
        self.port = self._sock.getsockname()[1]
        self._thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        if self.port is not None:
            try:
                with socket.create_connection((self.host, self.port), timeout=0.2):
                    pass
            except OSError:
                pass
        if self._thread is not None:
            self._thread.join(timeout=2)
        for handler in list(self._handlers):
            handler.join(timeout=2)
        if self._sock is not None:
            try:
                self._sock.close()
            except OSError:
                pass

    def snapshot(self):
        with self._lock:
            return {
                "accepted_connections": self.accept_count,
                "finished_connections": self.finished_count,
                "binary_frames": self.binary_frame_count,
                "qwp1_frames": self.qwp1_frame_count,
                "binary_bytes": self.binary_bytes,
                "binary_prefixes": list(self.binary_prefixes),
                "control_frames": self.control_frame_count,
                "errors": list(self.errors),
            }

    def wait_binary_frames_settled(self, quiet_s=0.02, timeout_s=5.0):
        """Return the ``binary_frames`` count once no more are in flight.

        The count is incremented by each connection's handler thread as it
        reads frames off the wire, asynchronously to the client. On the
        happy path the client's commit waits for our ack (sent only after
        the frame is counted), so a snapshot is already settled. But a
        client that drops a *pipelined* connection — e.g. a mid-stream
        rejected dataframe that flushed a few deferred batches before
        raising — closes the socket without waiting; its handler keeps
        counting the already-sent frames after the client call returns.

        A dropped connection's socket is closed before the client call
        returns, so its handler reaches EOF and exits imminently. Once
        every accepted connection's handler has finished, no unread frame
        remains and the count is final — return it. If a connection is
        still open (an upfront reject leaves the pooled connection live,
        but then nothing was sent so the count cannot lag), fall back to
        returning once the count has held steady for ``quiet_s``.
        """
        deadline = time.monotonic() + timeout_s
        last = None
        stable_since = time.monotonic()
        while True:
            with self._lock:
                frames = self.binary_frame_count
                all_finished = self.finished_count == self.accept_count
            now = time.monotonic()
            if all_finished:
                return frames
            if frames != last:
                last = frames
                stable_since = now
            elif now - stable_since >= quiet_s:
                return frames
            if now >= deadline:
                return frames
            time.sleep(0.001)

    def _accept_loop(self):
        while not self._stop.is_set():
            try:
                conn, _addr = self._sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            if self._stop.is_set():
                conn.close()
                break
            with self._lock:
                self.accept_count += 1
            close_after = None
            if self._close_iter is not None:
                close_after = next(self._close_iter, None)
            handler = threading.Thread(
                target=self._handle_connection,
                args=(conn, close_after),
                daemon=True)
            self._handlers.append(handler)
            handler.start()

    def _handle_connection(self, conn, close_after):
        next_seq = 0
        frames_handled = 0
        try:
            conn.settimeout(5)
            request = _read_until(conn, b"\r\n\r\n")
            key = _header(request, "Sec-WebSocket-Key")
            accept = _compute_accept(key)
            response = (
                "HTTP/1.1 101 Switching Protocols\r\n"
                "Upgrade: websocket\r\n"
                "Connection: Upgrade\r\n"
                f"Sec-WebSocket-Accept: {accept}\r\n"
                "X-QWP-Version: 1\r\n")
            if self.max_batch_size > 0:
                response += f"X-QWP-Max-Batch-Size: {self.max_batch_size}\r\n"
            response += "\r\n"
            conn.sendall(response.encode("ascii"))
            if close_after is not None and close_after == 0:
                _fin_close(conn)
                return

            while not self._stop.is_set():
                frame = _read_frame(conn)
                if frame is None:
                    break
                _fin, opcode, payload = frame
                if opcode == 0x8:
                    with self._lock:
                        self.control_frame_count += 1
                    _write_frame(conn, 0x8, b"")
                    break
                if opcode == 0x9:
                    with self._lock:
                        self.control_frame_count += 1
                    _write_frame(conn, 0xA, payload)
                    continue
                if opcode != 0x2:
                    with self._lock:
                        self.control_frame_count += 1
                    continue

                with self._lock:
                    self.binary_frame_count += 1
                    if payload.startswith(b"QWP1"):
                        self.qwp1_frame_count += 1
                    self.binary_bytes += len(payload)
                    if len(self.binary_prefixes) < 16:
                        self.binary_prefixes.append(payload[:8].hex())
                if self.ack_delay_s:
                    time.sleep(self.ack_delay_s)
                seq = next_seq
                next_seq += 1
                if not (self.defer_aware_acks
                        and _is_deferred_qwp_frame(payload)):
                    # Cumulative: one OK for `seq` also completes every
                    # deferred frame held back before it.
                    _write_qwp_ok(conn, seq)
                frames_handled += 1
                if close_after is not None and frames_handled >= close_after:
                    _fin_close(conn)
                    break
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            # The peer dropping its connection (e.g. a failed dataframe
            # call discarding uncommitted frames) is a normal lifecycle
            # event, not a protocol error.
            pass
        except Exception as exc:
            with self._lock:
                self.errors.append(repr(exc))
        finally:
            try:
                conn.close()
            except OSError:
                pass
            with self._lock:
                self.finished_count += 1


def _fin_close(conn):
    # A bare close() with unread client frames in the receive buffer takes
    # the RST path, and macOS loopback occasionally loses that RST: the
    # client's socket stays silently ESTABLISHED until its request timeout
    # (30s). A FIN is sequenced and retransmitted, so shutdown(SHUT_WR)
    # notifies the peer reliably; the short drain afterwards keeps the
    # receive queue empty so the final close() doesn't reset first.
    try:
        conn.shutdown(socket.SHUT_WR)
    except OSError:
        return
    conn.settimeout(0.2)
    try:
        while conn.recv(65536):
            pass
    except OSError:
        pass


def _read_exact(conn, length):
    chunks = []
    remaining = length
    while remaining:
        chunk = conn.recv(remaining)
        if not chunk:
            return None
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _read_until(conn, marker):
    data = bytearray()
    while marker not in data:
        chunk = conn.recv(256)
        if not chunk:
            raise ConnectionError("connection closed during HTTP upgrade")
        data.extend(chunk)
    return bytes(data)


def _header(request, name):
    text = request.decode("iso-8859-1")
    prefix = name.lower() + ":"
    for line in text.split("\r\n"):
        if line.lower().startswith(prefix):
            return line.split(":", 1)[1].strip()
    raise ValueError(f"missing HTTP header {name}")


def _compute_accept(key):
    digest = hashlib.sha1((key + WS_GUID).encode("ascii")).digest()
    return base64.b64encode(digest).decode("ascii")


def _read_frame(conn):
    header = _read_exact(conn, 2)
    if header is None:
        return None
    fin = bool(header[0] & 0x80)
    opcode = header[0] & 0x0F
    masked = bool(header[1] & 0x80)
    short_len = header[1] & 0x7F
    if short_len == 126:
        ext = _read_exact(conn, 2)
        if ext is None:
            return None
        payload_len = struct.unpack("!H", ext)[0]
    elif short_len == 127:
        ext = _read_exact(conn, 8)
        if ext is None:
            return None
        payload_len = struct.unpack("!Q", ext)[0]
    else:
        payload_len = short_len

    mask = b""
    if masked:
        mask = _read_exact(conn, 4)
        if mask is None:
            return None
    payload = _read_exact(conn, payload_len)
    if payload is None:
        return None
    if masked:
        payload = bytes(byte ^ mask[index & 3]
                        for index, byte in enumerate(payload))
    return fin, opcode, payload


def _write_frame(conn, opcode, payload):
    frame = bytearray([0x80 | (opcode & 0x0F)])
    payload_len = len(payload)
    if payload_len <= 125:
        frame.append(payload_len)
    elif payload_len <= 0xFFFF:
        frame.append(126)
        frame.extend(struct.pack("!H", payload_len))
    else:
        frame.append(127)
        frame.extend(struct.pack("!Q", payload_len))
    frame.extend(payload)
    conn.sendall(frame)


def _is_deferred_qwp_frame(payload):
    return (len(payload) > 5
            and payload.startswith(b"QWP1")
            and payload[5] & QWP_FLAG_DEFER_COMMIT != 0)


def _write_qwp_ok(conn, wire_seq):
    payload = bytearray([QWP_STATUS_OK])
    payload.extend(struct.pack("<Q", wire_seq))
    payload.extend(struct.pack("<H", 0))
    _write_frame(conn, 0x2, payload)
