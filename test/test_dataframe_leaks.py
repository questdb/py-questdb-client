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
                    'sender_pool_min=1;sender_pool_max=1;pool_reap=manual;')
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
    (``_dataframe_columnar_build_{str,int,float,bool,uuid,ipv4,bytes}_pyobj``)
    reached by ``QuestDB.dataframe`` for object-dtype columns: every native
    buffer (data, validity bitmap, str byte arena) must be freed on the
    success and all-valid (bitmap-dropped) paths, and the pooled connection
    must be returned on every call."""

    ROWS = 2048

    def _frames(self):
        n = self.ROWS
        ts = pd.Series(pd.to_datetime(np.arange(n), unit='s'))

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
        blobs = [b'value_%06d' % i for i in range(n)]
        frames = []
        for null_step in (0, 7):
            frames.append(pd.DataFrame({
                'ts': ts,
                's': col(strs, null_step),
                'i': col(ints, null_step),
                'f': col(floats, null_step),
                'b': bools,
                'u': col(uuids, null_step),
                'ip': col(ips, null_step),
                'by': col(blobs, null_step),
            }))
        return frames

    def _assert_stable(self, work, warmup, measure):
        _assert_no_leak(self, work, warmup, measure)

    def test_pyobj_columnar_path_no_leak(self):
        from qwp_ws_ack_server import QwpAckServer
        frames = self._frames()
        with QwpAckServer() as server:
            conf = (f'ws::addr=127.0.0.1:{server.port};'
                    'sender_pool_min=1;sender_pool_max=1;pool_reap=manual;')
            with qi.QuestDB.from_conf(conf) as client:
                def work():
                    for df in frames:
                        client.dataframe(
                            df, table_name='t', at='ts', symbols=False)

                self._assert_stable(work, warmup=150, measure=1800)


if __name__ == '__main__':
    unittest.main()
