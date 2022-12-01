#!/usr/bin/env python3

import sys
import os
sys.dont_write_bytecode = True
import unittest
import time
import pandas as pd

import patch_path
import questdb.ingress as qi


class TestBencharkPandas(unittest.TestCase):
    def test_pystr_i64_10m(self):
        # This is a benchmark, not a test.
        # It is useful to run it manually to check performance.
        slist = [f's{i:09}' for i in range(10_000_000)]
        df = pd.DataFrame({
            'a': slist,
            'b': list(range(len(slist)))})

        buf = qi.Buffer()

        # Warm up and pre-size buffer
        buf.pandas(df, table_name='tbl1', symbols=True)
        buf.clear()

        # Run
        t0 = time.monotonic()
        buf.pandas(df, table_name='tbl1', symbols=True)
        t1 = time.monotonic()
        print(f'Time: {t1 - t0}, size: {len(buf)}')

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
        buf.pandas(df, table_name='tbl1', symbols=True)
        buf.clear()

        # Run
        t0 = time.monotonic()
        buf.pandas(df, table_name='tbl1', symbols=True)
        t1 = time.monotonic()
        print(f'Time: {t1 - t0}, size: {len(buf)}')

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
        buf.pandas(df, table_name='tbl1', symbols=True)
        buf.clear()

        # Run
        t0 = time.monotonic()
        buf.pandas(df, table_name='tbl1', symbols=True)
        t1 = time.monotonic()
        print(f'Time: {t1 - t0}, size: {len(buf)}')

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
        buf.pandas(df, table_name='tbl1', symbols=False)
        buf.clear()

        # Run
        t0 = time.monotonic()
        buf.pandas(df, table_name='tbl1', symbols=False)
        t1 = time.monotonic()
        print(f'Time: {t1 - t0}, size: {len(buf)}')


if __name__ == '__main__':
    if os.environ.get('TEST_QUESTDB_PROFILE') == '1':
        import cProfile
        cProfile.run('unittest.main()', sort='cumtime')
    else:
        unittest.main()
