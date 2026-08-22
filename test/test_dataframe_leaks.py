import sys
sys.dont_write_bytecode = True
import ctypes
import gc
import ipaddress
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
        """
        from qwp_ws_ack_server import QwpAckServer
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

        with QwpAckServer() as server:
            conf = (f'ws::addr=127.0.0.1:{server.port};'
                    'sender_pool_min=1;sender_pool_max=1;pool_reap=manual;'
                    'query_pool_min=0;')
            with qi.QuestDB.from_conf(conf) as client:
                def work():
                    for name, frame in frames:
                        try:
                            client.dataframe(
                                frame, table_name='t', at='ts',
                                symbols=False)
                        except qi.QuestDBError as exc:
                            if repr(name) not in str(exc):
                                raise AssertionError(
                                    f'error did not name {name!r}: {exc}')
                        else:
                            raise AssertionError(
                                f'column {name!r} was accepted')

                self._assert_stable(work, warmup=150, measure=1200)

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
@unittest.skipUnless(psutil is not None, 'psutil not installed')
class TestCapsuleOverridesLeak(unittest.TestCase):
    """The Arrow capsule path allocates an override array per call and
    imports a schema per stream. Every other frame in this module is
    object-dtype pandas and no test elsewhere passes `schema_overrides`,
    so nothing entered this path before."""

    def test_overrides_leak_nothing(self):
        from qwp_ws_ack_server import QwpAckServer
        frame = pa.table({
            'u': pa.array([b'\x00' * 16], pa.binary(16)),
            'gh': pa.array([1], pa.int32()),
            'ts': pa.array([0], pa.timestamp('us')),
        })
        overrides = {'u': 'uuid', 'gh': ('geohash', 20)}
        with QwpAckServer() as server:
            conf = (f'ws::addr=127.0.0.1:{server.port};'
                    'sender_pool_min=1;sender_pool_max=1;pool_reap=manual;'
                    # Ingest-only mock server: skip the eager reader-pool
                    # connect, which would time out waiting for server info.
                    'query_pool_min=0;')
            with qi.QuestDB.from_conf(conf) as client:
                def work():
                    client.dataframe(
                        frame, table_name='caps', at='ts',
                        schema_overrides=overrides)

                _assert_no_leak(self, work, warmup=400, measure=12000)


if __name__ == '__main__':
    unittest.main()
