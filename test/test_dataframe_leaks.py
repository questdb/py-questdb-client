import sys
sys.dont_write_bytecode = True
import ctypes
import gc
import ipaddress
import sysconfig
import threading
import unittest
import uuid


def _limit_malloc_arenas():
    # Pin glibc to one arena before the sender's threads spawn; per-thread
    # arenas otherwise inflate RSS without a real leak.
    if not sys.platform.startswith('linux'):
        return
    try:
        ctypes.CDLL('libc.so.6', use_errno=False).mallopt(-8, 1)  # M_ARENA_MAX
    except (OSError, AttributeError):
        pass


_limit_malloc_arenas()

import patch_path

import questdb._client as qi

try:
    import numpy as np
    import pandas as pd
except ImportError:
    np = None
    pd = None

try:
    import pyarrow as pa
except ImportError:
    pa = None

try:
    import psutil
    _PROCESS = psutil.Process()
except ImportError:
    psutil = None


def _malloc_trim():
    # Return glibc per-thread arena free space to the OS so RSS reflects live
    # memory; a real leak survives the trim.
    if not sys.platform.startswith('linux'):
        return
    try:
        ctypes.CDLL('libc.so.6', use_errno=False).malloc_trim(0)
    except (OSError, AttributeError):
        pass


def _rss():
    _malloc_trim()
    return _PROCESS.memory_info().rss


_FREE_THREADED = bool(sysconfig.get_config_var('Py_GIL_DISABLED'))
_NO_RSS_ON_FREE_THREADED = (
    'RSS is not a leak signal on a free-threaded build: mimalloc pages '
    'are not returned by malloc_trim')


def _assert_no_leak(test, work, warmup, measure):
    # A real native leak grows in *every* window; glibc/obmalloc arena
    # retention bursts (sometimes across several consecutive windows) then
    # flattens. Take the smallest growth across the last half: if RSS held flat
    # for even one tail window, it has plateaued, so a transient multi-window
    # burst can't read as a leak — judging the shape, not an absolute size.
    windows = 6
    per = max(1, measure // windows)
    for _ in range(warmup):
        work()
    gc.collect()
    prev = _rss()
    growths = []
    for _ in range(windows):
        for _ in range(per):
            work()
        gc.collect()
        now = _rss()
        growths.append(now - prev)
        prev = now
    half = max(1, windows // 2)
    head = sorted(growths[:half])[half // 2]
    tail = min(growths[-half:])
    test.assertTrue(
        tail <= 3 * 1024 * 1024 or tail * 2 <= head,
        f'RSS not plateauing: per-window growth {growths} bytes over '
        f'{windows} windows of {per} iterations (head {head:.0f}, '
        f'tail {tail:.0f}); likely a leaked native buffer.')


class _MallocZoneStatistics(ctypes.Structure):
    _fields_ = [
        ('blocks_in_use', ctypes.c_uint),
        ('size_in_use', ctypes.c_size_t),
        ('max_size_in_use', ctypes.c_size_t),
        ('size_allocated', ctypes.c_size_t),
    ]


def _mallinfo_type(field_type):
    class MallInfo(ctypes.Structure):
        _fields_ = [
            (name, field_type) for name in (
                'arena', 'ordblks', 'smblks', 'hblks', 'hblkhd', 'usmblks',
                'fsmblks', 'uordblks', 'fordblks', 'keepcost')]
    return MallInfo


def _heap_reader():
    """A `(read, wrap)` pair, where `read()` returns the bytes the C
    allocator has handed out and not taken back, or None where this
    platform offers no such count.

    The client's Cython code and its Rust library both allocate through
    the C allocator, so this sees a native allocation that is never
    freed directly, where RSS sees it only once it outgrows allocator
    noise. On macOS it is `malloc_zone_statistics`; on glibc it is
    `mallinfo2`, or `mallinfo` before glibc 2.33, whose fields are
    `int`s that wrap at 2**32 (`wrap`). musl and Windows have no
    equivalent. PyPy is left out because its garbage collector, not
    the end of a call or a thread, decides when the objects owning
    native memory are freed."""
    if sys.implementation.name != 'cpython':
        return None
    if sys.platform == 'darwin':
        try:
            statistics = ctypes.CDLL(
                '/usr/lib/libSystem.B.dylib').malloc_zone_statistics
        except (OSError, AttributeError):
            return None
        statistics.argtypes = [
            ctypes.c_void_p, ctypes.POINTER(_MallocZoneStatistics)]
        statistics.restype = None

        def read_zones():
            stats = _MallocZoneStatistics()
            statistics(None, ctypes.byref(stats))
            return stats.size_in_use

        return read_zones, None
    if sys.platform.startswith('linux'):
        try:
            libc = ctypes.CDLL('libc.so.6', use_errno=False)
        except OSError:
            return None
        for name, field_type, wrap in (
                ('mallinfo2', ctypes.c_size_t, None),
                ('mallinfo', ctypes.c_int, 1 << 32)):
            mallinfo = getattr(libc, name, None)
            if mallinfo is None:
                continue
            mallinfo.argtypes = []
            mallinfo.restype = _mallinfo_type(field_type)

            def read_mallinfo(mallinfo=mallinfo):
                info = mallinfo()
                # `uordblks` counts the arena chunks in use, `hblkhd` the
                # large blocks glibc maps on their own.
                return info.uordblks + info.hblkhd

            return read_mallinfo, wrap
    return None


_HEAP_READER = _heap_reader()
_NO_HEAP_STATS = 'no C heap statistics on this platform or interpreter'


def _assert_no_heap_growth(test, work, per_window, windows=6, leak=16):
    """Fail if the C heap grows in every one of `windows` windows of
    `per_window` calls to `work`, after one window of warmup, by at
    least half of what a leak of `leak` bytes a call would add.

    `leak` is the smallest per-call leak the test exists to catch. A
    leak that size adds its full size every call, so it clears the
    half-size line in every window. The default is one block, the
    smallest allocation the C allocator hands out -- 16 bytes on macOS,
    32 on glibc -- which suits a test whose process does nothing else
    while it measures. A test that also runs a mock server and a network
    round trip shares the heap with the server's threads and the
    client's I/O, which move it by a few kilobytes in a typical window,
    so it passes the size of the allocation it guards instead.

    The smallest window is what is judged, so the occasional step of a
    few pages -- the allocator taking a new region, glibc's per-thread
    cache filling -- does not read as a leak unless it lands in every
    window."""
    read, wrap = _HEAP_READER
    for _ in range(per_window):
        work()
    gc.collect()
    prev = read()
    growths = []
    for _ in range(windows):
        for _ in range(per_window):
            work()
        gc.collect()
        now = read()
        growth = now - prev
        if wrap is not None:
            growth = (growth + wrap // 2) % wrap - wrap // 2
        growths.append(growth)
        prev = now
    threshold = per_window * leak // 2
    test.assertLess(
        min(growths), threshold,
        f'C heap grew by at least {threshold} bytes in every window: '
        f'{growths} bytes over {windows} windows of {per_window} calls; '
        f'an allocation of about {leak} bytes or more is likely never '
        f'freed on each call.')


@unittest.skipUnless(pd is not None, 'pandas not installed')
@unittest.skipUnless(pa is not None, 'pyarrow not installed')
@unittest.skipUnless(psutil is not None, 'psutil not installed')
class TestCategoricalArrowLeak(unittest.TestCase):
    """Guards the hand-built dictionary ``ArrowArray``/``ArrowSchema`` for
    pandas Categorical columns (``_dataframe_category_series_as_arrow``):
    every malloc'd buffer must be freed by its ``release`` callback on both
    the row path (``Buffer.dataframe`` -> ``col_t_release``) and the columnar
    path (``QuestDB.dataframe`` -> Rust import -> ``arrow_import_free``)."""

    ROWS = 4096

    def _cat(self, n_cats, code_dtype, null_step=0, large_string=False):
        codes = np.random.randint(0, n_cats, self.ROWS).astype(code_dtype)
        if null_step:
            codes[::null_step] = -1
        categories = [f'category_value_{i:05}' for i in range(n_cats)]
        if large_string:
            categories = pd.array(
                categories, dtype=pd.ArrowDtype(pa.large_string()))
        return pd.Series(pd.Categorical.from_codes(
            codes, categories=pd.Index(categories)))

    def _frames(self):
        ts = pd.Series(
            pd.to_datetime(np.arange(self.ROWS), unit='s'))
        v = pd.Series(np.arange(self.ROWS, dtype=np.int64))
        frames = [
            pd.DataFrame({'ts': ts, 'sym': self._cat(50, np.int8), 'v': v}),
            pd.DataFrame({'ts': ts, 'sym': self._cat(50, np.int8, null_step=7),
                          'v': v}),
            pd.DataFrame({'ts': ts, 'sym': self._cat(300, np.int16,
                          null_step=11), 'v': v}),
        ]
        if pa is not None:
            frames.append(pd.DataFrame({
                'ts': ts, 'sym': self._cat(40, np.int8, large_string=True),
                'v': v}))
        return frames

    def _assert_stable(self, work, warmup, measure):
        _assert_no_leak(self, work, warmup, measure)

    def test_row_path_no_leak(self):
        frames = self._frames()

        def work():
            for df in frames:
                qi.Buffer(protocol_version=2).dataframe(
                    df, table_name='t', at=qi.ServerTimestamp)

        self._assert_stable(work, warmup=200, measure=4000)

    @unittest.skipIf(_FREE_THREADED, _NO_RSS_ON_FREE_THREADED)
    def test_columnar_path_no_leak(self):
        from qwp_ws_ack_server import QwpAckServer
        frames = self._frames()
        with QwpAckServer() as server:
            conf = (f'ws::addr=127.0.0.1:{server.port};'
                    'sender_pool_min=1;sender_pool_max=1;pool_reap=manual;'
                    # Ingest-only mock server: skip the eager reader-pool
                    # connect, which would time out waiting for server info.
                    'query_pool_min=0;')
            with qi.QuestDB.from_conf(conf) as client:
                def work():
                    for df in frames:
                        client.dataframe(
                            df, table_name='t', at='ts', symbols='auto')

                self._assert_stable(work, warmup=150, measure=1800)


@unittest.skipUnless(pd is not None, 'pandas not installed')
@unittest.skipUnless(pa is not None, 'pyarrow not installed')
class TestFixedSizeBinaryClaimLeak(unittest.TestCase):

    def test_claimed_schema_is_released(self):
        rows = 8
        df = pd.DataFrame({
            'u': pd.Series(
                pa.array([bytes(16)] * rows, pa.binary(16)),
                dtype=pd.ArrowDtype(pa.binary(16))),
            'l': pd.Series(
                pa.array([bytes(32)] * rows, pa.binary(32)),
                dtype=pd.ArrowDtype(pa.binary(32))),
            'n': np.arange(rows)})
        # Width no longer implies a type; exercise the explicit-claim
        # planner that supersedes the schema-replacement path.
        df.attrs['questdb'] = {
            'version': 1,
            'columns': {'u': {'kind': 'uuid'}, 'l': {'kind': 'long256'}},
        }

        def plan(symbols):
            qi._bench_dataframe_plan_and_populate_column_chunks(
                df, table_name='t', at=qi.ServerTimestamp, symbols=symbols)

        for symbols, error in (('auto', None), (['n'], qi.QuestDBError)):
            with self.subTest(symbols=symbols):
                def work():
                    if error is None:
                        plan(symbols)
                    else:
                        with self.assertRaises(error):
                            plan(symbols)

                work()
                gc.collect()
                before = pa.total_allocated_bytes()
                for _ in range(100):
                    work()
                gc.collect()
                self.assertEqual(pa.total_allocated_bytes(), before)


@unittest.skipUnless(pd is not None, 'pandas not installed')
@unittest.skipUnless(psutil is not None, 'psutil not installed')
class TestPyobjColumnarLeak(unittest.TestCase):
    """Guards the calloc'd ``pyobj_built_t`` builders
    (``_dataframe_columnar_build_{str,int,float,bool,uuid,ipv4,bytes,
    long256,fsb}_pyobj``) reached by ``QuestDB.dataframe`` for
    object-dtype columns: every native buffer (data, validity bitmap, str
    byte arena) must be freed on the success and all-valid
    (bitmap-dropped) paths, borrowed BINARY memoryviews must be released
    on success and failure, and the pooled connection must be returned on
    every call.

    The last two builders are reached only through a
    ``df.attrs['questdb']`` claim -- object-dtype ints under ``long256``
    and object-dtype ``bytes`` under ``uuid`` or ``long256`` -- which is
    the shape ``to_pandas(dtype_backend='numpy_nullable')`` hands those
    columns back as. They allocate 16 or 32 bytes a row plus a bitmap, so
    the frames below carry the claim."""

    ROWS = 2048

    @staticmethod
    def _ts(n):
        return pd.Series(pd.to_datetime(np.arange(n), unit='s'))

    def _frames(self):
        n = self.ROWS
        ts = self._ts(n)

        def col(values, null_step):
            return pd.Series(
                [None if (null_step and i % null_step == 0) else v
                 for i, v in enumerate(values)],
                dtype=object)

        strs = [f'value_{i:06}' for i in range(n)]
        ints = list(range(n))
        floats = [i * 0.5 for i in range(n)]
        bools = pd.Series([bool(i & 1) for i in range(n)], dtype=object)
        uuids = [uuid.UUID(int=i) for i in range(n)]
        ips = [ipaddress.IPv4Address(i) for i in range(n)]
        blobs = []
        for i in range(n):
            value = b'value_%06d' % i
            kind = i % 3
            if kind == 0:
                blobs.append(value)
            elif kind == 1:
                blobs.append(bytearray(value))
            else:
                blobs.append(memoryview(value))
        long256_ints = [(1 << 255) + i for i in range(n)]
        uuid_bytes = [uuid.UUID(int=i).bytes for i in range(n)]
        long256_bytes = [
            (i).to_bytes(32, 'little') for i in range(n)]
        frames = []
        for null_step in (0, 7):
            frame = pd.DataFrame({
                'ts': ts,
                's': col(strs, null_step),
                'i': col(ints, null_step),
                'f': col(floats, null_step),
                'b': bools,
                'u': col(uuids, null_step),
                'ip': col(ips, null_step),
                'by': col(blobs, null_step),
                'l256': col(long256_ints, null_step),
                'u_fsb': col(uuid_bytes, null_step),
                'l_fsb': col(long256_bytes, null_step),
            })
            frame.attrs['questdb'] = {
                'version': 1, 'columns': {
                    'l256': {'kind': 'long256'},
                    'u_fsb': {'kind': 'uuid'},
                    'l_fsb': {'kind': 'long256'}}}
            frames.append(frame)
        return frames

    def _assert_stable(self, work, warmup, measure):
        _assert_no_leak(self, work, warmup, measure)

    @unittest.skipIf(_FREE_THREADED, _NO_RSS_ON_FREE_THREADED)
    def test_pyobj_columnar_path_no_leak(self):
        from qwp_ws_ack_server import QwpAckServer
        frames = self._frames()
        with QwpAckServer() as server:
            conf = (f'ws::addr=127.0.0.1:{server.port};'
                    'sender_pool_min=1;sender_pool_max=1;pool_reap=manual;'
                    # Ingest-only mock server: skip the eager reader-pool
                    # connect, which would time out waiting for server info.
                    'query_pool_min=0;')
            with qi.QuestDB.from_conf(conf) as client:
                def work():
                    for df in frames:
                        client.dataframe(
                            df, table_name='t', at='ts', symbols=False)

                self._assert_stable(work, warmup=150, measure=1800)

    def test_binary_memoryview_error_path_no_leak(self):
        """A frame rejected on its last BINARY cell must leave no native
        memory behind. Every cell before the bad one is borrowed, copied
        and released inside the row loop, so at most one buffer is open
        at a time; what this measures is the partially built column and
        its offset table, which the unwind has to free. The bad cell is
        caught by the itemsize / contiguity check, so this exercises the
        pre-`PyObject_GetBuffer` branch. ``PyBuffer_Release`` itself is a
        refcount question RSS cannot answer; it is asserted directly by
        ``TestBinaryBufferRelease`` below.
        """
        from qwp_ws_ack_server import QwpAckServer

        def invalid_frame():
            values = []
            for i in range(96):
                value = (b'value_%06d_' % i) + b'x' * 256
                kind = i % 3
                if kind == 0:
                    values.append(value)
                elif kind == 1:
                    values.append(bytearray(value))
                else:
                    values.append(memoryview(value))
            values.append(memoryview(b'invalid')[::2])
            return pd.DataFrame({
                'by': pd.Series(values, dtype=object),
            })

        with QwpAckServer() as server:
            conf = (f'ws::addr=127.0.0.1:{server.port};'
                    'sender_pool_min=1;sender_pool_max=1;pool_reap=manual;'
                    'query_pool_min=0;')
            with qi.QuestDB.from_conf(conf) as client:
                def work():
                    try:
                        client.dataframe(
                            invalid_frame(), table_name='t',
                            at=qi.ServerTimestamp, symbols=False)
                    except qi.QuestDBError as exc:
                        if exc.code != qi.QuestDBErrorCode.BadDataFrame:
                            raise
                        if "Bad column 'by' at row 96" not in str(exc):
                            raise AssertionError(
                                f'unexpected BINARY validation error: {exc}')
                    else:
                        raise AssertionError(
                            'invalid BINARY memoryview was accepted')

                self._assert_stable(work, warmup=200, measure=2400)

    @unittest.skipIf(_HEAP_READER is None, _NO_HEAP_STATS)
    def test_pyobj_error_after_a_partial_bitmap_no_leak(self):
        """A column rejected part-way through must free what it had
        already built, bitmap included.

        Every other error-path test puts the bad value at row 0, so
        ``pyobj_built_free`` only ever runs on a struct with nothing
        written yet -- the allocations it has to release are not there
        to be missed. Here the nulls before the bad row have grown the
        validity bitmap and the values before it have filled the data
        buffer, so the unwind runs with both live. One frame per
        builder that raises: the string arena, the fixed-width integer
        buffer, and the two claimed builders' 16- and 32-byte slots.

        The smallest of those is a 64-byte bitmap, which RSS would not
        show, so the C heap is measured, one frame at a time: a frame
        moves the heap by a few kilobytes a window on its own, and five
        together move it by enough to hide one builder's bitmap. Each
        frame is refused while it is built, before the lazy connection
        is ever opened, so no server runs in this process to move it.
        """
        n = 512
        bad_at = n - 1

        def col(good, bad, null_step=5):
            return pd.Series(
                [None if i % null_step == 0
                 else (bad if i == bad_at else good(i))
                 for i in range(n)],
                dtype=object)

        # A lone surrogate cannot be encoded as UTF-8; an integer past
        # the signed 64-bit range overflows; a claimed UUID or LONG256
        # cell has to be exactly 16 or 32 bytes wide.
        cases = [
            ('s', col(lambda i: f'value_{i:06}', '\ud800'), None),
            ('i', col(lambda i: i, 2 ** 63), None),
            ('l256', col(lambda i: (1 << 255) + i, -1), 'long256'),
            ('u_fsb', col(lambda i: uuid.UUID(int=i).bytes, b'short'),
             'uuid'),
            ('l_fsb', col(lambda i: (i).to_bytes(32, 'little'), b'short'),
             'long256'),
        ]
        frames = []
        for name, values, kind in cases:
            frame = pd.DataFrame({'ts': self._ts(n), name: values})
            if kind is not None:
                frame.attrs['questdb'] = {
                    'version': 1, 'columns': {name: {'kind': kind}}}
            frames.append((name, frame))

        with qi.QuestDB.from_conf(
                'ws::addr=127.0.0.1:1;lazy_connect=true;'
                'sender_pool_min=0;query_pool_min=0;'
                'pool_reap=manual;') as client:
            for name, frame in frames:
                def work(name=name, frame=frame):
                    try:
                        client.dataframe(
                            frame, table_name='t', at='ts', symbols=False)
                    except qi.QuestDBError as exc:
                        if repr(name) not in str(exc):
                            raise AssertionError(
                                f'error did not name {name!r}: {exc}')
                    else:
                        raise AssertionError(
                            f'column {name!r} was accepted')

                with self.subTest(column=name):
                    _assert_no_heap_growth(self, work, per_window=200)

    @unittest.skipUnless(pa is not None, 'pyarrow not installed')
    def test_promoted_columnar_path_no_leak(self):
        """Guards ``_dataframe_columnar_promote_cols``: the pybuf chunk
        release + Arrow re-export for object-Decimal and NaT-carrying
        ``datetime64[ns]`` field columns must not leak native buffers."""
        from decimal import Decimal
        from qwp_ws_ack_server import QwpAckServer
        n = self.ROWS
        df = pd.DataFrame({
            'ts': pd.Series(pd.to_datetime(np.arange(n), unit='s')),
            'dec': pd.Series(
                [None if i % 7 == 0 else Decimal(i) / 100
                 for i in range(n)], dtype=object),
            'vts': pd.Series(
                [pd.NaT if i % 5 == 0 else pd.Timestamp(i, unit='s')
                 for i in range(n)], dtype='datetime64[ns]'),
        })
        with QwpAckServer() as server:
            conf = (f'ws::addr=127.0.0.1:{server.port};'
                    'sender_pool_min=1;sender_pool_max=1;pool_reap=manual;'
                    # Ingest-only mock server: skip the eager reader-pool
                    # connect, which would time out waiting for server info.
                    'query_pool_min=0;')
            with qi.QuestDB.from_conf(conf) as client:
                def work():
                    client.dataframe(
                        df, table_name='t', at='ts', symbols=False)

                self._assert_stable(work, warmup=150, measure=1800)


class TestBinaryBufferRelease(unittest.TestCase):
    """``PyObject_GetBuffer`` on a memoryview BINARY cell must be paired
    with a ``PyBuffer_Release``. A missing release is a refcount leak of
    a fixed size, which the RSS harness above cannot see. It is visible
    directly instead: while any buffer is exported, the underlying
    ``bytearray`` refuses to be re-sized with

      BufferError: Existing exports of data: object cannot be re-sized

    so a ``bytearray`` that accepts ``append`` afterwards proves the
    client let go of it.
    """

    @staticmethod
    def _conf(port):
        return (f'ws::addr=127.0.0.1:{port};lazy_connect=true;'
                'sender_pool_min=1;sender_pool_max=1;pool_reap=manual;'
                'query_pool_min=0;')

    def _assert_released(self, backing, view):
        view.release()
        try:
            backing.append(0)
        except BufferError as exc:
            self.fail(f'client kept a Py_buffer on the cell: {exc}')

    @unittest.skipIf(pd is None, 'pandas not installed')
    def test_good_path_releases(self):
        from qwp_ws_ack_server import QwpAckServer
        backing = bytearray(b'value_0')
        view = memoryview(backing)
        frame = pd.DataFrame({'by': pd.Series([view], dtype=object)})
        with QwpAckServer() as server:
            with qi.QuestDB.from_conf(self._conf(server.port)) as client:
                client.dataframe(
                    frame, table_name='t', at=qi.ServerTimestamp,
                    symbols=False)
        del frame
        gc.collect()
        self._assert_released(backing, view)

    @unittest.skipIf(pd is None, 'pandas not installed')
    def test_error_path_releases_earlier_cells(self):
        """The bad cell sits after the good one, so the good cell's
        buffer is released by the ``finally`` inside the row loop rather
        than by reaching the end of the column."""
        from qwp_ws_ack_server import QwpAckServer
        backing = bytearray(b'value_0')
        view = memoryview(backing)
        frame = pd.DataFrame({
            'by': pd.Series(
                [view, memoryview(b'invalid')[::2]], dtype=object)})
        with QwpAckServer() as server:
            with qi.QuestDB.from_conf(self._conf(server.port)) as client:
                with self.assertRaises(qi.QuestDBError):
                    client.dataframe(
                        frame, table_name='t', at=qi.ServerTimestamp,
                        symbols=False)
        del frame
        gc.collect()
        self._assert_released(backing, view)

    def test_row_path_releases(self):
        """`Buffer.row` borrows the same way `dataframe()` does, through
        its own `PyObject_GetBuffer` / `PyBuffer_Release` pair in
        `_column_binary`. Both tests above drive `client.dataframe`, so
        without this one the row API -- which is where BINARY cells were
        added -- has nothing asserting it lets the buffer go."""
        backing = bytearray(b'value_0')
        view = memoryview(backing)
        buffer = qi.Buffer._new_qwp()
        buffer.row('t', columns={'by': view}, at=qi.TimestampNanos(1))
        del buffer
        gc.collect()
        self._assert_released(backing, view)

    def test_row_path_releases_on_a_rejected_row(self):
        """A row that fails after the BINARY cell has been borrowed
        unwinds through the `finally`, so the export is dropped there
        too. `Geohash` is rejected for mixing precisions within the
        column, which happens after the earlier cells are written."""
        backing = bytearray(b'value_0')
        view = memoryview(backing)
        buffer = qi.Buffer._new_qwp()
        buffer.row(
            't', columns={'by': memoryview(b'first'), 'g': qi.Geohash(1, 1)},
            at=qi.TimestampNanos(1))
        before = len(buffer)
        with self.assertRaises(qi.QuestDBError):
            buffer.row(
                't', columns={'by': view, 'g': qi.Geohash(1, 5)},
                at=qi.TimestampNanos(2))
        self.assertEqual(len(buffer), before)
        del buffer
        gc.collect()
        self._assert_released(backing, view)

    # The claimed kinds that send an object column of bytes-like cells
    # through `_dataframe_columnar_build_fsb_pyobj`, a second builder with
    # its own `PyObject_GetBuffer` / `PyBuffer_Release` pair.
    _FIXED_WIDTH_CLAIMS = (('uuid', 16), ('long256', 32))

    @staticmethod
    def _claimed_frame(cells, kind):
        frame = pd.DataFrame({'c': pd.Series(cells, dtype=object)})
        frame.attrs['questdb'] = {
            'version': 1, 'columns': {'c': {'kind': kind}}}
        return frame

    @unittest.skipIf(pd is None, 'pandas not installed')
    def test_claimed_fixed_width_good_path_releases(self):
        """A `uuid` or `long256` claim writes an object column of
        `bytes`-like cells at a fixed width. The `bytearray` cell after
        the memoryview takes the branch beside it, which borrows
        nothing."""
        from qwp_ws_ack_server import QwpAckServer
        for kind, width in self._FIXED_WIDTH_CLAIMS:
            with self.subTest(kind=kind):
                backing = bytearray(b'\x01' * width)
                view = memoryview(backing)
                frame = self._claimed_frame(
                    [view, bytearray(b'\x02' * width)], kind)
                with QwpAckServer() as server:
                    with qi.QuestDB.from_conf(
                            self._conf(server.port)) as client:
                        client.dataframe(
                            frame, table_name='t', at=qi.ServerTimestamp,
                            symbols=False)
                del frame
                gc.collect()
                self._assert_released(backing, view)

    @unittest.skipIf(pd is None, 'pandas not installed')
    def test_claimed_fixed_width_error_path_releases(self):
        """The short cell is refused after its buffer is borrowed, so the
        `finally` inside the row loop releases it, as it released the
        good cell before it."""
        from qwp_ws_ack_server import QwpAckServer
        for kind, width in self._FIXED_WIDTH_CLAIMS:
            with self.subTest(kind=kind):
                good_backing = bytearray(b'\x01' * width)
                good = memoryview(good_backing)
                short_backing = bytearray(b'\x02' * (width - 1))
                short = memoryview(short_backing)
                frame = self._claimed_frame([good, short], kind)
                with QwpAckServer() as server:
                    with qi.QuestDB.from_conf(
                            self._conf(server.port)) as client:
                        with self.assertRaisesRegex(
                                qi.QuestDBError,
                                f'exactly {width} bytes, got {width - 1}'):
                            client.dataframe(
                                frame, table_name='t',
                                at=qi.ServerTimestamp, symbols=False)
                del frame
                gc.collect()
                self._assert_released(good_backing, good)
                self._assert_released(short_backing, short)


@unittest.skipUnless(pd is not None, 'pandas not installed')
@unittest.skipUnless(psutil is not None, 'psutil not installed')
class TestClosedHandleDataframeLeak(unittest.TestCase):
    """`QuestDB.dataframe()` claims a `qdb_pystr_buf` for the call. The
    handle check that raises on a closed pool runs first, so an
    allocation made ahead of it has nothing to free it and a retry loop
    against a closed handle leaks one per attempt."""

    def test_a_closed_handle_leaks_nothing(self):
        frame = pd.DataFrame({
            'a': np.arange(1024, dtype=np.int64),
            'ts': pd.to_datetime(np.arange(1024), unit='s')})
        handle = qi.QuestDB.from_conf(
            'ws::addr=127.0.0.1:1;lazy_connect=true;'
            'sender_pool_min=0;pool_reap=manual;')
        handle.close()

        def work():
            with self.assertRaises(qi.QuestDBError):
                handle.dataframe(
                    frame, table_name='closed', at='ts')

        # Enough attempts that a per-call leak of a few dozen bytes
        # clears the 3 MiB-per-window floor `_assert_no_leak`
        # allows for allocator noise.
        _assert_no_leak(self, work, warmup=20000, measure=900000)


@unittest.skipUnless(pa is not None, 'pyarrow not installed')
@unittest.skipIf(_HEAP_READER is None, _NO_HEAP_STATS)
class TestCapsuleOverridesLeak(unittest.TestCase):
    """The Arrow capsule path allocates an override array and imports a
    schema on every call, and releases both on the way out. Cover a
    successful send with overrides and two streams the client refuses:
    one by the guarded FFI preflight, one by the 8-byte fixed-size
    binary check, which runs after the schema is imported and before
    the first batch is read."""

    # A leaked override array is one `qwp_arrow_override` an entry: a
    # pointer, a `size_t` and two 32-bit fields, 24 bytes on a 64-bit
    # build. The C heap shows that at any column count; a few columns
    # keep each call cheap.
    COLUMNS = 16
    OVERRIDE_ENTRY_BYTES = 2 * ctypes.sizeof(ctypes.c_void_p) + 8

    # The stream's exported schema is the smallest allocation the
    # refusals guard: pyarrow 25 puts 160 bytes on the macOS C heap for
    # each of their two-column schemas, released once the client is done
    # with it. glibc counts each block's header too, so its figure is no
    # smaller. Half of that is the line, so a smaller export still shows.
    SCHEMA_EXPORT_BYTES = 160

    @staticmethod
    def _conf(port):
        return (f'ws::addr=127.0.0.1:{port};'
                'sender_pool_min=1;sender_pool_max=1;pool_reap=manual;'
                # Ingest-only mock server: skip the eager reader-pool
                # connect, which would time out waiting for server info.
                'query_pool_min=0;')

    def test_overrides_leak_nothing(self):
        from qwp_ws_ack_server import QwpAckServer
        data = {}
        overrides = {}
        for i in range(self.COLUMNS):
            data[f'u{i}'] = pa.array([b'\x00' * 16], pa.binary(16))
            overrides[f'u{i}'] = 'uuid'
        data['gh'] = pa.array([1], pa.int32())
        overrides['gh'] = ('geohash', 20)
        data['ts'] = pa.array([0], pa.timestamp('us'))
        frame = pa.table(data)
        with QwpAckServer() as server:
            with qi.QuestDB.from_conf(self._conf(server.port)) as client:
                def work():
                    client.dataframe(
                        frame, table_name='caps', at='ts',
                        schema_overrides=overrides)

                # Every call waits for a WebSocket ack, which costs about
                # 40 ms on the Linux CI agents, so the windows are short.
                # The override array is the smallest allocation this
                # guards, and the line it sets clears the few kilobytes a
                # window the round trip moves the heap by.
                _assert_no_heap_growth(
                    self, work, per_window=50,
                    leak=self.OVERRIDE_ENTRY_BYTES * len(overrides))

    def _assert_refusals_leak_nothing(self, frame, code):
        from qwp_ws_ack_server import QwpAckServer
        with QwpAckServer() as server:
            with qi.QuestDB.from_conf(self._conf(server.port)) as client:
                def work():
                    with self.assertRaises(qi.QuestDBError) as raised:
                        client.dataframe(frame, table_name='caps', at='ts')
                    self.assertEqual(raised.exception.code, code)

                # Refused before anything is sent, so calls are cheap.
                _assert_no_heap_growth(
                    self, work, per_window=1000,
                    leak=self.SCHEMA_EXPORT_BYTES)
            frames = server.wait_binary_frames_settled()
            stats = server.snapshot()
        self.assertEqual(frames, 0)
        self.assertEqual(stats['errors'], [])

    def test_rejected_stream_leaks_nothing(self):
        nested = pa.StructArray.from_arrays(
            [pa.array([1], type=pa.int32())], names=['value'])
        self._assert_refusals_leak_nothing(
            pa.table({
                'nested': nested,
                'ts': pa.array([0], type=pa.timestamp('us')),
            }),
            qi.QuestDBErrorCode.ArrowUnsupportedColumnKind)

    def test_fsb8_refusal_leaks_nothing(self):
        """The 8-byte fixed-size binary refusal (the shape of a polars
        `Object` column) happens on the client, after the stream's schema
        is imported and before its first batch is read. The stream, its
        schema and the borrowed connection must all be released."""
        self._assert_refusals_leak_nothing(
            pa.table({
                'handles': pa.array([b'\x00' * 8], type=pa.binary(8)),
                'ts': pa.array([0], type=pa.timestamp('us')),
            }),
            qi.QuestDBErrorCode.ArrowUnsupportedColumnKind)


class _BottomReached(Exception):
    pass


class _NestingAddr(ipaddress.IPv4Address):
    """An IPV4 cell whose conversion to an int is pure Python, so it runs
    inside the `row()` writing it. From there it writes the next cell
    through the next lease, so every lease before it still has its
    `row()` in progress -- each on a handle of its own."""

    def __init__(self, leases, level, raise_at_bottom, reached):
        super().__init__('192.0.2.1')
        self._nest = (leases, level, raise_at_bottom, reached)

    def __int__(self):
        leases, level, raise_at_bottom, reached = self._nest
        reached.append(level)
        if level + 1 < len(leases):
            leases[level + 1].row(
                'nested',
                columns={'ip': _NestingAddr(
                    leases, level + 1, raise_at_bottom, reached)},
                at=qi.ServerTimestamp)
        elif raise_at_bottom:
            raise _BottomReached()
        return 0xC0000201


def _nested_rows(leases, raise_at_bottom=False):
    """Write one row through each lease, each from inside the one before
    it, and return the levels that ran. With `raise_at_bottom`, the
    innermost conversion raises `_BottomReached`, so every row is
    rewound and every lease is left with nothing buffered."""
    reached = []
    leases[0].row(
        'nested',
        columns={'ip': _NestingAddr(leases, 0, raise_at_bottom, reached)},
        at=qi.ServerTimestamp)
    return reached


class TestCallTableMemory(unittest.TestCase):
    """Each thread keeps a native table of the calls and callback
    dispatches it has in progress on each owner, such as a `QuestDB`
    handle (`_scoped_table_ensure`). The table starts
    with four slots and doubles when a fifth handle has a call in
    progress on the same thread, and it is freed when the thread's
    Python state ends (`_scoped_table_free`)."""

    HANDLES = 6

    @classmethod
    def _handles(cls, port):
        # One connection per handle, opened here and reused by every
        # lease, so the server accepts nothing while a test measures.
        conf = (f'ws::addr=127.0.0.1:{port};'
                'sender_pool_min=1;sender_pool_max=1;'
                'query_pool_min=0;pool_reap=manual;')
        return [qi.QuestDB.from_conf(conf) for _ in range(cls.HANDLES)]

    def _close_all(self, handles):
        # `close()` refuses while this thread still has a call counted on
        # the handle, so every handle closing is what shows the table's
        # counts went back to zero.
        refusals = []
        for db in handles:
            try:
                db.close()
            except qi.QuestDBError as exc:
                refusals.append(exc)
        self.assertEqual(refusals, [])

    def test_the_table_grows_past_its_first_slots(self):
        """Six handles with a call in progress at once take the table
        past its first four slots. The second pass runs on the grown
        table."""
        from qwp_ws_ack_server import QwpAckServer
        with QwpAckServer() as server:
            handles = self._handles(server.port)
            try:
                for _ in range(2):
                    leases = [db.sender() for db in handles]
                    try:
                        reached = _nested_rows(leases)
                    finally:
                        for lease in leases:
                            lease.close()
                    self.assertEqual(reached, list(range(self.HANDLES)))
            finally:
                self._close_all(handles)

    @unittest.skipIf(_HEAP_READER is None, _NO_HEAP_STATS)
    def test_a_threads_table_is_freed_when_it_exits(self):
        """Every thread grows its own table to eight slots and exits.
        Neither the table nor its slots may outlive the thread."""
        from qwp_ws_ack_server import QwpAckServer
        errors = []
        with QwpAckServer() as server:
            handles = self._handles(server.port)
            try:
                def nest_and_exit():
                    leases = [db.sender() for db in handles]
                    try:
                        _nested_rows(leases, raise_at_bottom=True)
                    except _BottomReached:
                        pass
                    except BaseException as exc:
                        errors.append(exc)
                    finally:
                        for lease in leases:
                            lease.close()

                def work():
                    worker = threading.Thread(target=nest_and_exit)
                    worker.start()
                    worker.join()

                accepted = server.snapshot()['accepted_connections']
                _assert_no_heap_growth(self, work, per_window=300)
                self.assertEqual(
                    server.snapshot()['accepted_connections'], accepted)
            finally:
                self._close_all(handles)
        self.assertEqual(errors, [])


@unittest.skipIf(_HEAP_READER is None, _NO_HEAP_STATS)
class TestNativeHeapLeak(unittest.TestCase):
    """Allocations made once per call outside the dataframe builders,
    each freed on the way out of the call. The C heap shows a missed
    free after a few hundred calls; RSS would need hundreds of
    thousands.

    None of these tests runs a mock server in this process:
    `QwpAckServer` keeps an object per connection it accepts, which
    would grow the heap on its own."""

    @unittest.skipIf(pd is None, 'pandas not installed')
    def test_ws_sender_dataframe_frees_its_options(self):
        """`Sender.dataframe()` over ws:: clones the sender's options for
        the length of the call and frees the clone in a `finally` that
        every outcome passes through. Here each call fails to connect,
        which is quick, and its `SocketError` shows it got as far as
        connecting, and so past the clone."""
        sender = qi.Sender.from_conf(
            'ws::addr=127.0.0.1:1;auto_flush=off;initial_connect_retry=off;')
        frame = pd.DataFrame({
            'v': np.arange(2, dtype=np.int64),
            'ts': pd.to_datetime([0, 1], unit='s')})

        def work():
            with self.assertRaises(qi.QuestDBError) as raised:
                sender.dataframe(frame, table_name='t', at='ts')
            self.assertEqual(
                raised.exception.code, qi.QuestDBErrorCode.SocketError)

        try:
            _assert_no_heap_growth(self, work, per_window=1000)
        finally:
            sender.close()

    @unittest.skipIf(pa is None, 'pyarrow not installed')
    def test_a_refused_nul_column_name_leaks_nothing(self):
        """A column name holding a NUL is refused with the error `row()`
        raises for it, which the check produces by converting the name
        in a string buffer of its own. The refusal comes before any
        connection is attempted, so no server is needed."""
        frame = pa.table({
            'a\x00b': pa.array([1]),
            'ts': pa.array([0], pa.timestamp('us')),
        })
        with qi.QuestDB.from_conf(
                'ws::addr=127.0.0.1:1;lazy_connect=true;'
                'sender_pool_min=0;query_pool_min=0;'
                'pool_reap=manual;') as client:
            def work():
                with self.assertRaises(qi.QuestDBError) as raised:
                    client.dataframe(frame, table_name='t', at='ts')
                self.assertEqual(
                    raised.exception.code, qi.QuestDBErrorCode.InvalidName)

            _assert_no_heap_growth(self, work, per_window=1000)

    @unittest.skipIf(pd is None, 'pandas not installed')
    def test_a_row_route_dataframe_frees_its_string_storage(self):
        """`dataframe()` on the row-serializing route -- `Buffer`,
        `SenderTransaction`, and `Sender` over the ILP protocols, which
        all reach `_dataframe` -- gives each call string storage of its
        own and frees it in a `finally`. A missed free is about a
        kilobyte a call. Writing into a `Buffer` needs no connection."""
        buf = qi.Buffer(protocol_version=2)
        frame = pd.DataFrame({
            'v': np.arange(2, dtype=np.int64),
            'ts': pd.to_datetime([0, 1], unit='s')})

        def work():
            buf.dataframe(frame, table_name='t', at='ts')
            buf.clear()

        _assert_no_heap_growth(self, work, per_window=1000)

    @unittest.skipIf(pd is None, 'pandas not installed')
    def test_a_refused_row_route_dataframe_frees_its_string_storage(self):
        """The same storage, on a frame refused while its plan is built:
        the `at` column it names does not exist."""
        buf = qi.Buffer(protocol_version=2)
        frame = pd.DataFrame({
            'v': np.arange(2, dtype=np.int64),
            'ts': pd.to_datetime([0, 1], unit='s')})

        def work():
            with self.assertRaises(qi.QuestDBError) as raised:
                buf.dataframe(frame, table_name='t', at='missing')
            self.assertEqual(
                raised.exception.code, qi.QuestDBErrorCode.InvalidApiCall)

        _assert_no_heap_growth(self, work, per_window=1000)


if __name__ == '__main__':
    unittest.main()
