import sys
sys.dont_write_bytecode = True
import gc
import unittest

import patch_path

import questdb.ingress as qi

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


def _rss():
    return _PROCESS.memory_info().rss


@unittest.skipUnless(pd is not None, 'pandas not installed')
@unittest.skipUnless(pa is not None, 'pyarrow not installed')
@unittest.skipUnless(psutil is not None, 'psutil not installed')
class TestCategoricalArrowLeak(unittest.TestCase):
    """Guards the hand-built dictionary ``ArrowArray``/``ArrowSchema`` for
    pandas Categorical columns (``_dataframe_category_series_as_arrow``):
    every malloc'd buffer must be freed by its ``release`` callback on both
    the row path (``Buffer.dataframe`` -> ``col_t_release``) and the columnar
    path (``Client.dataframe`` -> Rust import -> ``arrow_import_free``)."""

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
        for _ in range(warmup):
            work()
        gc.collect()
        before = _rss()
        for _ in range(measure):
            work()
        gc.collect()
        growth = _rss() - before
        self.assertLess(
            growth, 8 * 1024 * 1024,
            f'RSS grew by {growth} bytes over {measure} iterations; '
            'a native buffer is likely leaked.')

    def test_row_path_no_leak(self):
        frames = self._frames()

        def work():
            for df in frames:
                qi.Buffer.ilp(protocol_version=2).dataframe(
                    df, table_name='t', at=qi.ServerTimestamp)

        self._assert_stable(work, warmup=200, measure=4000)

    def test_columnar_path_no_leak(self):
        from qwp_ws_ack_server import QwpAckServer
        frames = self._frames()
        with QwpAckServer() as server:
            conf = (f'qwpws::addr=127.0.0.1:{server.port};'
                    'pool_size=1;pool_max=1;pool_reap=manual;')
            with qi.Client.from_conf(conf) as client:
                def work():
                    for df in frames:
                        client.dataframe(
                            df, table_name='t', at='ts', symbols='auto')

                self._assert_stable(work, warmup=50, measure=800)


@unittest.skipUnless(pd is not None, 'pandas not installed')
@unittest.skipUnless(psutil is not None, 'psutil not installed')
class TestPyobjColumnarLeak(unittest.TestCase):
    """Guards the calloc'd ``pyobj_built_t`` builders
    (``_dataframe_columnar_build_{str,int,float,bool}_pyobj``) reached by
    ``Client.dataframe`` for object-dtype columns: every native buffer
    (data, validity bitmap, str byte arena) must be freed on the success
    and all-valid (bitmap-dropped) paths, and the pooled connection must be
    returned on every call."""

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
        frames = []
        for null_step in (0, 7):
            frames.append(pd.DataFrame({
                'ts': ts,
                's': col(strs, null_step),
                'i': col(ints, null_step),
                'f': col(floats, null_step),
                'b': bools,
            }))
        return frames

    def _assert_stable(self, work, warmup, measure):
        for _ in range(warmup):
            work()
        gc.collect()
        before = _rss()
        for _ in range(measure):
            work()
        gc.collect()
        growth = _rss() - before
        self.assertLess(
            growth, 8 * 1024 * 1024,
            f'RSS grew by {growth} bytes over {measure} iterations; '
            'a native buffer is likely leaked.')

    def test_pyobj_columnar_path_no_leak(self):
        from qwp_ws_ack_server import QwpAckServer
        frames = self._frames()
        with QwpAckServer() as server:
            conf = (f'qwpws::addr=127.0.0.1:{server.port};'
                    'pool_size=1;pool_max=1;pool_reap=manual;')
            with qi.Client.from_conf(conf) as client:
                def work():
                    for df in frames:
                        client.dataframe(
                            df, table_name='t', at='ts', symbols=False)

                self._assert_stable(work, warmup=50, measure=800)


if __name__ == '__main__':
    unittest.main()
