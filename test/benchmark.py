#!/usr/bin/env python3

import sys
import os
sys.dont_write_bytecode = True
import unittest
import time
import numpy as np
import pandas as pd
from concurrent.futures import ThreadPoolExecutor

import patch_path
import questdb.ingress as qi


def _tp(buf, t0, t1):
    tp = len(buf) / (t1 - t0) / 1024 / 1024
    return f'{tp:.2f} MiB/s'


class TestBenchmarkPandas(unittest.TestCase):
    def test_pystr_i64_10m(self):
        # This is a benchmark, not a test.
        # It is useful to run it manually to check performance.
        slist = [f's{i:09}' for i in range(10_000_000)]
        df = pd.DataFrame({
            'a': slist,
            'b': list(range(len(slist)))})

        buf = qi.Buffer()

        # Warm up and pre-size buffer
        buf.dataframe(df, table_name='tbl1', symbols=True)
        buf.clear()

        # Run
        t0 = time.monotonic()
        buf.dataframe(df, table_name='tbl1', symbols=True)
        t1 = time.monotonic()
        print(f'Time: {t1 - t0}, size: {len(buf)}, tp: {_tp(buf, t0, t1)}')

    def test_mixed_10m(self):
        # This is a benchmark, not a test.
        # It is useful to run it manually to check performance.
        count = 10_000_000
        slist = [f's{i:09}' for i in range(count)]
        df = pd.DataFrame({
            'col1': pd.Series(slist, dtype='string[pyarrow]'),
            'col2': list(range(len(slist))),
            'col3': [float(i / 2) for i in range(len(slist))],
            'col4': [float(i / 2) + 1.0 for i in range(len(slist))],
            'col5': pd.Categorical(
                ['a', 'b', 'c', 'a', None, 'c', 'a', float('nan')] *
                (count // 8))})

        buf = qi.Buffer()

        # Warm up and pre-size buffer
        buf.dataframe(df, table_name='tbl1', symbols=True)
        buf.clear()

        # Run
        t0 = time.monotonic()
        buf.dataframe(df, table_name='tbl1', symbols=True)
        t1 = time.monotonic()
        print(f'Time: {t1 - t0}, size: {len(buf)}, tp: {_tp(buf, t0, t1)}')

    def test_string_escaping_10m(self):
        count = 10_000_000
        slist = [f's={i:09}==abc \\' for i in range(count)]
        series = pd.Series(slist, dtype='string[pyarrow]')
        df = pd.DataFrame({
            'col1': series,
            'col2': series,
            'col3': series,
            'col4': series,
            'col5': series,
            'col6': series})
        
        buf = qi.Buffer()

        # Warm up and pre-size buffer
        buf.dataframe(df, table_name='tbl1', symbols=True)
        buf.clear()

        # Run
        t0 = time.monotonic()
        buf.dataframe(df, table_name='tbl1', symbols=True)
        t1 = time.monotonic()
        print(f'Time: {t1 - t0}, size: {len(buf)}, tp: {_tp(buf, t0, t1)}')

    def test_string_encoding_10m(self):
        count = 10_000_000
        strs = ['a',                     # ASCII
                'q❤️p',                   # Mixed ASCII and UCS-2
                '❤️' * 12  ,              # UCS-2
                'Questo è un qualcosa',  # Non-ASCII UCS-1
                'щось',                  # UCS-2, 2 bytes for UTF-8.
                '',                      # Empty string
                '嚜꓂',                   # UCS-2, 3 bytes for UTF-8.
                '𐀀a𐀀b𐀀💩🦞c𐀀d𐀀ef']      # UCS-4, 4 bytes for UTF-8.
        slist = strs * (count // len(strs))
        self.assertEqual(len(slist), count)

        df = pd.DataFrame({
            'col1': slist,
            'col2': slist,
            'col3': slist,
            'col4': slist,
            'col5': slist})

        buf = qi.Buffer()

        # Warm up and pre-size buffer
        buf.dataframe(df, table_name='tbl1', symbols=False)
        buf.clear()

        # Run
        t0 = time.monotonic()
        buf.dataframe(df, table_name='tbl1', symbols=False)
        t1 = time.monotonic()
        print(f'Time: {t1 - t0}, size: {len(buf)}, tp: {_tp(buf, t0, t1)}')

    def _test_gil_release_10m(self, threads):
        count = 10_000_000
        series = pd.Series(np.arange(count), dtype='int64')
        df = pd.DataFrame({
            'col1': series,
            'col2': series,
            'col3': series,
            'col4': series,
            'col5': series,
            'col6': series})

        tpe = ThreadPoolExecutor(max_workers=threads)
        bufs = [qi.Buffer() for _ in range(threads)]

        def benchmark_run(buf):
            t0 = time.monotonic()
            buf.dataframe(df, table_name='tbl1', symbols=True)
            t1 = time.monotonic()
            return buf, (t0, t1)

        # Warm up and pre-size buffer
        futs = [
            tpe.submit(benchmark_run, buf)
            for buf in bufs]
        for fut in futs:
            fut.result()  # Wait for completion
        for buf in bufs:
            buf.clear()

        # Run
        futs = [
            tpe.submit(benchmark_run, buf)
            for buf in bufs]
        results = [
            fut.result()
            for fut in futs]
        print(f'\nSize: {len(bufs[0])}')
        total_time = 0
        min_time = 2 ** 64 -1  # Bigger than any `time.monotonic()` value
        max_time = 0
        print('Per-thread times:')
        for index, (_, (t0, t1)) in enumerate(results):
            if t0 < min_time:
                min_time = t0
            if t1 > max_time:
                max_time = t1
            elapsed = t1 - t0
            print(f'  [{index:02}]: Time: {elapsed}')
            total_time += elapsed
        avg_time = total_time / len(results)
        print(f'Avg time: {avg_time}')
        tp = (len(bufs[0]) * len(bufs)) / (max_time - min_time) / 1024 / 1024
        print(f'Wall time: {max_time - min_time}, tp: {tp:.2f} MiB/s')

    def test_gil_release_10m_1t(self):
        self._test_gil_release_10m(1)

    def test_gil_release_10m_10t(self):
        self._test_gil_release_10m(10)

    def test_gil_release_10m_16t(self):
        self._test_gil_release_10m(16)

    def test_gil_release_10m_32t(self):
        self._test_gil_release_10m(32)


if __name__ == '__main__':
    if os.environ.get('TEST_QUESTDB_PROFILE') == '1':
        import cProfile
        cProfile.run('unittest.main()', sort='cumtime')
    else:
        unittest.main()
