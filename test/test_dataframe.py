#!/usr/bin/env python3

import sys
import os
sys.dont_write_bytecode = True
import unittest
import datetime as dt
import functools
import tempfile
import pathlib

BROKEN_TIMEZONES = True

try:
    import zoneinfo
    _TZ = zoneinfo.ZoneInfo('America/New_York')
    BROKEN_TIMEZONES = os.name == 'nt'
except ImportError:
    import pytz
    _TZ = pytz.timezone('America/New_York')

import patch_path

import questdb.ingress as qi
import pandas as pd
import numpy as np
import pyarrow as pa

try:
    import fastparquet
except ImportError:
    fastparquet = None


def _dataframe(*args, **kwargs):
    buf = qi.Buffer()
    buf.dataframe(*args, **kwargs)
    return str(buf)


DF1 = pd.DataFrame({
    'A': [1.0, 2.0, 3.0],
    'B': [1, 2, 3],
    'C': [
        pd.Timestamp('20180310'),
        pd.Timestamp('20180311'),
        pd.Timestamp('20180312')],
    'D': [True, 'foo', 'bar']})


DF2 = pd.DataFrame({
    'T': ['t1', 't2', 't1'],
    'A': ['a1', 'a2', 'a3'],
    'B': ['b1', None, 'b3'],
    'C': pd.Series(['b1', None, 'b3'], dtype='string'),
    'D': pd.Series(['a1', 'a2', 'a3'], dtype='string'),
    'E': [1.0, 2.0, 3.0],
    'F': [1, 2, 3],
    'G': [
        pd.Timestamp('20180310'),
        pd.Timestamp('20180311'),
        pd.Timestamp('20180312')]})


def with_tmp_dir(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        with tempfile.TemporaryDirectory(prefix='py-questdb-client_') as tmpdir:
            return func(self, *args, pathlib.Path(tmpdir), **kwargs)
    return wrapper


class TestPandas(unittest.TestCase):
    def test_bad_dataframe(self):
        with self.assertRaisesRegex(qi.IngressError,
                'Expected pandas'):
            _dataframe([])

    def test_no_table_name(self):
        with self.assertRaisesRegex(qi.IngressError,
                'Must specify at least one of'):
            _dataframe(DF1)

    def test_bad_table_name_type(self):
        with self.assertRaisesRegex(TypeError, "'table_name' has incorrect type"):
            _dataframe(DF1, table_name=1.5)

    def test_invalid_table_name(self):
        with self.assertRaisesRegex(qi.IngressError,
                '`table_name`: Bad string "."'):
            _dataframe(DF1, table_name='.')

    def test_invalid_column_dtype(self):
        with self.assertRaisesRegex(qi.IngressError,
                '`table_name_col`: Bad dtype'):
            _dataframe(DF1, table_name_col='B')
        with self.assertRaisesRegex(qi.IngressError,
                '`table_name_col`: Bad dtype'):
            _dataframe(DF1, table_name_col=1)
        with self.assertRaisesRegex(qi.IngressError,
                '`table_name_col`: Bad dtype'):
            _dataframe(DF1, table_name_col=-3)
        with self.assertRaisesRegex(qi.IngressError,
                '`table_name_col`: -5 index'):
            _dataframe(DF1, table_name_col=-5)

    def test_bad_str_obj_col(self):
        with self.assertRaisesRegex(qi.IngressError,
                "`table_name_col`: Bad.*`object`.*bool.*'D'.*Must.*strings"):
            _dataframe(DF1, table_name_col='D')
        with self.assertRaisesRegex(qi.IngressError,
                "`table_name_col`: Bad.*`object`.*bool.*'D'.*Must.*strings"):
            _dataframe(DF1, table_name_col=3)
        with self.assertRaisesRegex(qi.IngressError,
                "`table_name_col`: Bad.*`object`.*bool.*'D'.*Must.*strings"):
            _dataframe(DF1, table_name_col=-1)

    def test_bad_symbol(self):
        with self.assertRaisesRegex(qi.IngressError,
                '`symbols`.*bool.*tuple.*list'):
            _dataframe(DF1, table_name='tbl1', symbols=0)
        with self.assertRaisesRegex(qi.IngressError,
                '`symbols`.*bool.*tuple.*list'):
            _dataframe(DF1, table_name='tbl1', symbols={})
        with self.assertRaisesRegex(qi.IngressError,
                '`symbols`.*bool.*tuple.*list'):
            _dataframe(DF1, table_name='tbl1', symbols=None)
        with self.assertRaisesRegex(qi.IngressError,
                "`symbols`: Bad dtype `float64`.*'A'.*Must.*strings col"):
            _dataframe(DF1, table_name='tbl1', symbols=(0,))
        with self.assertRaisesRegex(qi.IngressError,
                "`symbols`: Bad dtype `int64`.*'B'.*Must be a strings column."):
            _dataframe(DF1, table_name='tbl1', symbols=[1])

    def test_bad_at(self):
        with self.assertRaisesRegex(qi.IngressError,
                '`at`.*2018.*not found in the'):
            _dataframe(DF1, table_name='tbl1', at='2018-03-10T00:00:00Z')
        with self.assertRaisesRegex(qi.IngressError,
                '`at`.*float64.*be a datetime'):
            _dataframe(DF1, table_name='tbl1', at='A')
        with self.assertRaisesRegex(qi.IngressError,
                '`at`.*int64.*be a datetime'):
            _dataframe(DF1, table_name='tbl1', at=1)
        with self.assertRaisesRegex(qi.IngressError,
                '`at`.*object.*be a datetime'):
            _dataframe(DF1, table_name='tbl1', at=-1)

    def test_empty_dataframe(self):
        buf = _dataframe(pd.DataFrame(), table_name='tbl1')
        self.assertEqual(buf, '')

    def test_zero_row_dataframe(self):
        buf = _dataframe(pd.DataFrame(columns=['A', 'B']), table_name='tbl1')
        self.assertEqual(buf, '')

    def test_zero_column_dataframe(self):
        df = pd.DataFrame(index=[0, 1, 2])
        self.assertEqual(len(df), 3)
        buf = _dataframe(df, table_name='tbl1')
        self.assertEqual(buf, '')

    def test_basic(self):
        buf = _dataframe(
            DF2,
            table_name_col='T',
            symbols=['A', 'B', 'C', 'D'],
            at=-1)
        self.assertEqual(
            buf,
            't1,A=a1,B=b1,C=b1,D=a1 E=1.0,F=1i 1520640000000000000\n' +
            't2,A=a2,D=a2 E=2.0,F=2i 1520726400000000000\n' +
            't1,A=a3,B=b3,C=b3,D=a3 E=3.0,F=3i 1520812800000000000\n')

    def test_named_dataframe(self):
        df = pd.DataFrame({
            'a': [1, 2, 3],
            'b': ['a', 'b', 'c']})
        df.index.name = 'table_name'
        buf = _dataframe(df)
        self.assertEqual(
            buf,
            'table_name a=1i,b="a"\n' +
            'table_name a=2i,b="b"\n' +
            'table_name a=3i,b="c"\n')

        buf = _dataframe(df, table_name='tbl1')
        self.assertEqual(
            buf,
            'tbl1 a=1i,b="a"\n' +
            'tbl1 a=2i,b="b"\n' +
            'tbl1 a=3i,b="c"\n')

        buf = _dataframe(df, table_name_col='b')
        self.assertEqual(
            buf,
            'a a=1i\n' +
            'b a=2i\n' +
            'c a=3i\n')

        df.index.name = 42  # bad type, not str
        with self.assertRaisesRegex(qi.IngressError,
                'Bad dataframe index name as table.*: Expected str, not.*int.'):
            _dataframe(df)

    @unittest.skipIf(BROKEN_TIMEZONES, 'requires accurate timezones')
    def test_at_good(self):
        df = pd.DataFrame({
            'a': [1, 2, 3],
            'b': ['a', 'b', 'c']})
        df.index.name = 'test_at_good'
        with self.assertRaisesRegex(qi.IngressError,
                'Bad argument `at`: Column .2018-03.* not found .* dataframe.'):
            _dataframe(df, at='2018-03-10T00:00:00Z')

        # Same timestamp, specified in various ways.
        t1_setup = dt.datetime(2018, 3, 10, 0, 0, 0, tzinfo=dt.timezone.utc)
        t1 = t1_setup.astimezone(tz=None).replace(tzinfo=None)  # naive, local
        t2 = dt.datetime(2018, 3, 10, 0, 0, 0, tzinfo=dt.timezone.utc)
        t3 = dt.datetime(2018, 3, 9, 19, 0, 0, tzinfo=_TZ)
        t4 = qi.TimestampNanos(1520640000000000000)
        t5 = qi.TimestampNanos.from_datetime(t1)
        t6 = qi.TimestampNanos.from_datetime(t2)
        t7 = qi.TimestampNanos.from_datetime(t3)
        timestamps = [t1, t2, t3, t4, t5, t6, t7]
        for ts in timestamps:
            buf = _dataframe(df, table_name='tbl1', at=ts)
            self.assertEqual(
                buf,
                'tbl1 a=1i,b="a" 1520640000000000000\n' +
                'tbl1 a=2i,b="b" 1520640000000000000\n' +
                'tbl1 a=3i,b="c" 1520640000000000000\n')

    @unittest.skipIf(BROKEN_TIMEZONES, 'requires accurate timezones')
    def test_at_neg(self):
        n1 = dt.datetime(1965, 1, 1, 0, 0, 0, tzinfo=dt.timezone.utc)
        n2 = dt.datetime(1965, 1, 1, 0, 0, 0, tzinfo=_TZ)
        n3 = dt.datetime(1965, 1, 1, 0, 0, 0)
        neg_timestamps = [n1, n2, n3]
        for ts in neg_timestamps:
            with self.assertRaisesRegex(qi.IngressError,
                    'Bad.*`at`: Cannot .* before the Unix epoch .1970-01-01.*'):
                _dataframe(DF2, at=ts, table_name='test_at_neg')

    @unittest.skipIf(BROKEN_TIMEZONES, 'requires accurate timezones')
    def test_at_ts_0(self):
        df = pd.DataFrame({
            'a': [1, 2, 3],
            'b': ['a', 'b', 'c']})
        df.index.name = 'test_at_ts_0'

        # Epoch 0, specified in various ways.
        e1_setup = dt.datetime(1970, 1, 1, 0, 0, 0, tzinfo=dt.timezone.utc)
        e1 = e1_setup.astimezone(tz=None).replace(tzinfo=None)  # naive, local
        e2 = dt.datetime(1970, 1, 1, 0, 0, 0, tzinfo=dt.timezone.utc)
        e3 = dt.datetime(1969, 12, 31, 19, 0, 0, tzinfo=_TZ)
        e4 = qi.TimestampNanos(0)
        e5 = qi.TimestampNanos.from_datetime(e1)
        e6 = qi.TimestampNanos.from_datetime(e2)
        e7 = qi.TimestampNanos.from_datetime(e3)
        edge_timestamps = [e1, e2, e3, e4, e5, e6, e7]

        for ts in edge_timestamps:
            buf = _dataframe(df, table_name='tbl1', at=ts)
            self.assertEqual(
                buf,
                'tbl1 a=1i,b="a" 0\n' +
                'tbl1 a=2i,b="b" 0\n' +
                'tbl1 a=3i,b="c" 0\n')

    def test_single_at_col(self):
        df = pd.DataFrame({'timestamp': pd.to_datetime(['2023-01-01'])})
        with self.assertRaisesRegex(qi.IngressError,
                'Bad dataframe row at index 0: All values are nulls.'):
            _dataframe(df, table_name='tbl1', at='timestamp')

    def test_row_of_nulls(self):
        df = pd.DataFrame({'a': ['a1', None, 'a3']})
        with self.assertRaisesRegex(
                qi.IngressError, 'Bad dataframe row.*1: All values are nulls.'):
            _dataframe(df, table_name='tbl1', symbols=['a'])

    def test_u8_numpy_col(self):
        df = pd.DataFrame({'a': pd.Series([
                1, 2, 3,
                0,
                255],  # u8 max
            dtype='uint8')})
        buf = _dataframe(df, table_name='tbl1')
        self.assertEqual(
            buf,
            'tbl1 a=1i\n' +
            'tbl1 a=2i\n' +
            'tbl1 a=3i\n' +
            'tbl1 a=0i\n' +
            'tbl1 a=255i\n')

    def test_i8_numpy_col(self):
        df = pd.DataFrame({'a': pd.Series([
                1, 2, 3,
                -128,  # i8 min
                127,   # i8 max
                0], dtype='int8')})
        buf = _dataframe(df, table_name='tbl1')
        self.assertEqual(
            buf,
            'tbl1 a=1i\n' +
            'tbl1 a=2i\n' +
            'tbl1 a=3i\n' +
            'tbl1 a=-128i\n' +
            'tbl1 a=127i\n' +
            'tbl1 a=0i\n')

    def test_u16_numpy_col(self):
        df = pd.DataFrame({'a': pd.Series([
                1, 2, 3,
                0,
                65535],  # u16 max
            dtype='uint16')})
        buf = _dataframe(df, table_name='tbl1')
        self.assertEqual(
            buf,
            'tbl1 a=1i\n' +
            'tbl1 a=2i\n' +
            'tbl1 a=3i\n' +
            'tbl1 a=0i\n' +
            'tbl1 a=65535i\n')

    def test_i16_numpy_col(self):
        df = pd.DataFrame({'a': pd.Series([
                1, 2, 3,
                -32768,  # i16 min
                32767,   # i16 max
                0], dtype='int16')})
        buf = _dataframe(df, table_name='tbl1')
        self.assertEqual(
            buf,
            'tbl1 a=1i\n' +
            'tbl1 a=2i\n' +
            'tbl1 a=3i\n' +
            'tbl1 a=-32768i\n' +
            'tbl1 a=32767i\n' +
            'tbl1 a=0i\n')

    def test_u32_numpy_col(self):
        df = pd.DataFrame({'a': pd.Series([
                1, 2, 3,
                0,
                4294967295],  # u32 max
            dtype='uint32')})
        buf = _dataframe(df, table_name='tbl1')
        self.assertEqual(
            buf,
            'tbl1 a=1i\n' +
            'tbl1 a=2i\n' +
            'tbl1 a=3i\n' +
            'tbl1 a=0i\n' +
            'tbl1 a=4294967295i\n')

    def test_i32_numpy_col(self):
        df = pd.DataFrame({'a': pd.Series([
                1, 2, 3,
                -2147483648,  # i32 min
                0,
                2147483647],  # i32 max
            dtype='int32')})
        buf = _dataframe(df, table_name='tbl1')
        self.assertEqual(
            buf,
            'tbl1 a=1i\n' +
            'tbl1 a=2i\n' +
            'tbl1 a=3i\n' +
            'tbl1 a=-2147483648i\n' +
            'tbl1 a=0i\n' +
            'tbl1 a=2147483647i\n')

    def test_u64_numpy_col(self):
        df = pd.DataFrame({'a': pd.Series([
                1, 2, 3,
                0,
                9223372036854775807],  # i64 max
            dtype='uint64')})
        buf = _dataframe(df, table_name='tbl1')
        self.assertEqual(
            buf,
            'tbl1 a=1i\n' +
            'tbl1 a=2i\n' +
            'tbl1 a=3i\n' +
            'tbl1 a=0i\n' +
            'tbl1 a=9223372036854775807i\n')

        buf = qi.Buffer()
        buf.dataframe(pd.DataFrame({'b': [.5, 1.0, 1.5]}), table_name='tbl2')
        exp1 = (
            'tbl2 b=0.5\n' +
            'tbl2 b=1.0\n' +
            'tbl2 b=1.5\n')
        self.assertEqual(
            str(buf),
            exp1)
        df2 = pd.DataFrame({'a': pd.Series([
                1, 2, 3,
                0,
                9223372036854775808],  # i64 max + 1
            dtype='uint64')})
        with self.assertRaisesRegex(
                qi.IngressError,
                'serialize .* column .a. .* 4 .9223372036854775808.*int64'):
            buf.dataframe(df2, table_name='tbl1')

        self.assertEqual(
            str(buf),
            exp1)  # No partial write of `df2`.

    def test_i64_numpy_col(self):
        df = pd.DataFrame({'a': pd.Series([
                1, 2, 3,
                -9223372036854775808,  # i64 min
                0,
                9223372036854775807],  # i64 max
            dtype='int64')})
        buf = _dataframe(df, table_name='tbl1')
        self.assertEqual(
            buf,
            'tbl1 a=1i\n' +
            'tbl1 a=2i\n' +
            'tbl1 a=3i\n' +
            'tbl1 a=-9223372036854775808i\n' +
            'tbl1 a=0i\n' +
            'tbl1 a=9223372036854775807i\n')

    def test_f32_numpy_col(self):
        df = pd.DataFrame({'a': pd.Series([
                1.0, 2.0, 3.0,
                0.0,
                float('inf'),
                float('-inf'),
                float('nan'),
                3.4028234663852886e38],  # f32 max
            dtype='float32')})
        buf = _dataframe(df, table_name='tbl1')
        self.assertEqual(
            buf,
            'tbl1 a=1.0\n' +
            'tbl1 a=2.0\n' +
            'tbl1 a=3.0\n' +
            'tbl1 a=0.0\n' +
            'tbl1 a=Infinity\n' +
            'tbl1 a=-Infinity\n' +
            'tbl1 a=NaN\n' +
            'tbl1 a=3.4028234663852886e38\n')

    def test_f64_numpy_col(self):
        df = pd.DataFrame({'a': pd.Series([
                1.0, 2.0, 3.0,
                0.0,
                float('inf'),
                float('-inf'),
                float('nan'),
                1.7976931348623157e308],  # f64 max
            dtype='float64')})
        buf = _dataframe(df, table_name='tbl1')
        self.assertEqual(
            buf,
            'tbl1 a=1.0\n' +
            'tbl1 a=2.0\n' +
            'tbl1 a=3.0\n' +
            'tbl1 a=0.0\n' +
            'tbl1 a=Infinity\n' +
            'tbl1 a=-Infinity\n' +
            'tbl1 a=NaN\n' +
            'tbl1 a=1.7976931348623157e308\n')

    def test_u8_arrow_col(self):
        df = pd.DataFrame({
            'a': pd.Series([
                    1, 2, 3,
                    0,
                    None,
                    255],  # u8 max
                dtype=pd.UInt8Dtype()),
            'b': ['a', 'b', 'c', 'd', 'e', 'f']})
        buf = _dataframe(df, table_name='tbl1')
        self.assertEqual(
            buf,
            'tbl1 a=1i,b="a"\n' +
            'tbl1 a=2i,b="b"\n' +
            'tbl1 a=3i,b="c"\n' +
            'tbl1 a=0i,b="d"\n' +
            'tbl1 b="e"\n' +
            'tbl1 a=255i,b="f"\n')

    def test_i8_arrow_col(self):
        df = pd.DataFrame({
            'a': pd.Series([
                    1, 2, 3,
                    -128,  # i8 min
                    0,
                    None,
                    127],  # i8 max
                dtype=pd.Int8Dtype()),
            'b': ['a', 'b', 'c', 'd', 'e', 'f', 'g']})
        buf = _dataframe(df, table_name='tbl1')
        self.assertEqual(
            buf,
            'tbl1 a=1i,b="a"\n' +
            'tbl1 a=2i,b="b"\n' +
            'tbl1 a=3i,b="c"\n' +
            'tbl1 a=-128i,b="d"\n' +
            'tbl1 a=0i,b="e"\n' +
            'tbl1 b="f"\n' +
            'tbl1 a=127i,b="g"\n')

    def test_u16_arrow_col(self):
        df = pd.DataFrame({
            'a': pd.Series([
                    1, 2, 3,
                    0,
                    None,
                    65535],  # u16 max
                dtype=pd.UInt16Dtype()),
            'b': ['a', 'b', 'c', 'd', 'e', 'f']})
        buf = _dataframe(df, table_name='tbl1')
        self.assertEqual(
            buf,
            'tbl1 a=1i,b="a"\n' +
            'tbl1 a=2i,b="b"\n' +
            'tbl1 a=3i,b="c"\n' +
            'tbl1 a=0i,b="d"\n' +
            'tbl1 b="e"\n' +
            'tbl1 a=65535i,b="f"\n')

    def test_i16_arrow_col(self):
        df = pd.DataFrame({
            'a': pd.Series([
                    1, 2, 3,
                    -32768,  # i16 min
                    0,
                    None,
                    32767],  # i16 max
                dtype=pd.Int16Dtype()),
            'b': ['a', 'b', 'c', 'd', 'e', 'f', 'g']})
        buf = _dataframe(df, table_name='tbl1')
        self.assertEqual(
            buf,
            'tbl1 a=1i,b="a"\n' +
            'tbl1 a=2i,b="b"\n' +
            'tbl1 a=3i,b="c"\n' +
            'tbl1 a=-32768i,b="d"\n' +
            'tbl1 a=0i,b="e"\n' +
            'tbl1 b="f"\n' +
            'tbl1 a=32767i,b="g"\n')

    def test_u32_arrow_col(self):
        df = pd.DataFrame({
            'a': pd.Series([
                    1, 2, 3,
                    0,
                    None,
                    4294967295],  # u32 max
                dtype=pd.UInt32Dtype()),
            'b': ['a', 'b', 'c', 'd', 'e', 'f']})
        buf = _dataframe(df, table_name='tbl1')
        self.assertEqual(
            buf,
            'tbl1 a=1i,b="a"\n' +
            'tbl1 a=2i,b="b"\n' +
            'tbl1 a=3i,b="c"\n' +
            'tbl1 a=0i,b="d"\n' +
            'tbl1 b="e"\n' +
            'tbl1 a=4294967295i,b="f"\n')

    def test_i32_arrow_col(self):
        df = pd.DataFrame({
            'a': pd.Series([
                    1, 2, 3,
                    -2147483648,  # i32 min
                    0,
                    None,
                    2147483647],  # i32 max
                dtype=pd.Int32Dtype()),
            'b': ['a', 'b', 'c', 'd', 'e', 'f', 'g']})
        buf = _dataframe(df, table_name='tbl1')
        self.assertEqual(
            buf,
            'tbl1 a=1i,b="a"\n' +
            'tbl1 a=2i,b="b"\n' +
            'tbl1 a=3i,b="c"\n' +
            'tbl1 a=-2147483648i,b="d"\n' +
            'tbl1 a=0i,b="e"\n' +
            'tbl1 b="f"\n' +
            'tbl1 a=2147483647i,b="g"\n')

    def test_u64_arrow_col(self):
        df = pd.DataFrame({
            'a': pd.Series([
                    1, 2, 3,
                    0,
                    None,
                    9223372036854775807],  # i64 max
                dtype=pd.UInt64Dtype()),
            'b': ['a', 'b', 'c', 'd', 'e', 'f']})
        buf = _dataframe(df, table_name='tbl1')
        self.assertEqual(
            buf,
            'tbl1 a=1i,b="a"\n' +
            'tbl1 a=2i,b="b"\n' +
            'tbl1 a=3i,b="c"\n' +
            'tbl1 a=0i,b="d"\n' +
            'tbl1 b="e"\n' +
            'tbl1 a=9223372036854775807i,b="f"\n')

        df2 = pd.DataFrame({'a': pd.Series([
                1, 2, 3,
                0,
                9223372036854775808],  # i64 max + 1
            dtype=pd.UInt64Dtype())})
        with self.assertRaisesRegex(
                qi.IngressError,
                'serialize .* column .a. .* 4 .9223372036854775808.*int64'):
            _dataframe(df2, table_name='tbl1')

    def test_i64_arrow_col(self):
        df = pd.DataFrame({
            'a': pd.Series([
                    1, 2, 3,
                    -9223372036854775808,  # i64 min
                    0,
                    None,
                    9223372036854775807],  # i64 max
                dtype=pd.Int64Dtype()),
            'b': ['a', 'b', 'c', 'd', 'e', 'f', 'g']})
        buf = _dataframe(df, table_name='tbl1')
        self.assertEqual(
            buf,
            'tbl1 a=1i,b="a"\n' +
            'tbl1 a=2i,b="b"\n' +
            'tbl1 a=3i,b="c"\n' +
            'tbl1 a=-9223372036854775808i,b="d"\n' +
            'tbl1 a=0i,b="e"\n' +
            'tbl1 b="f"\n' +
            'tbl1 a=9223372036854775807i,b="g"\n')

    def test_f32_arrow_col(self):
        df = pd.DataFrame({
            'a': pd.Series([
                    1.0, 2.0, 3.0,
                    0.0,
                    float('inf'),
                    float('-inf'),
                    float('nan'),
                    3.4028234663852886e38,  # f32 max
                    None],
                dtype=pd.Float32Dtype()),
            'b': ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i']})
        buf = _dataframe(df, table_name='tbl1')
        self.assertEqual(
            buf,
            'tbl1 a=1.0,b="a"\n' +
            'tbl1 a=2.0,b="b"\n' +
            'tbl1 a=3.0,b="c"\n' +
            'tbl1 a=0.0,b="d"\n' +
            'tbl1 a=Infinity,b="e"\n' +
            'tbl1 a=-Infinity,b="f"\n' +
            'tbl1 b="g"\n' +  # This one is wierd: `nan` gets 0 in the bitmask.
            'tbl1 a=3.4028234663852886e38,b="h"\n' +
            'tbl1 b="i"\n')

    def test_f64_arrow_col(self):
        df = pd.DataFrame({
            'a': pd.Series([
                    1.0, 2.0, 3.0,
                    0.0,
                    float('inf'),
                    float('-inf'),
                    float('nan'),
                    1.7976931348623157e308,  # f64 max
                    None],
                dtype=pd.Float64Dtype()),
            'b': ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i']})
        buf = _dataframe(df, table_name='tbl1')
        self.assertEqual(
            buf,
            'tbl1 a=1.0,b="a"\n' +
            'tbl1 a=2.0,b="b"\n' +
            'tbl1 a=3.0,b="c"\n' +
            'tbl1 a=0.0,b="d"\n' +
            'tbl1 a=Infinity,b="e"\n' +
            'tbl1 a=-Infinity,b="f"\n' +
            'tbl1 b="g"\n' +  # This one is wierd: `nan` gets 0 in the bitmask.
            'tbl1 a=1.7976931348623157e308,b="h"\n' +
            'tbl1 b="i"\n')

    def test_bool_numpy_col(self):
        df = pd.DataFrame({'a': pd.Series([
                True, False, False,
                False, True, False],
            dtype='bool')})
        buf = _dataframe(df, table_name='tbl1')
        self.assertEqual(
            buf,
            'tbl1 a=t\n' +
            'tbl1 a=f\n' +
            'tbl1 a=f\n' +
            'tbl1 a=f\n' +
            'tbl1 a=t\n' +
            'tbl1 a=f\n')

    def test_bool_arrow_col(self):
        df = pd.DataFrame({'a': pd.Series([
                True, False, False,
                False, True, False,
                True, True, True,
                False, False, False],
            dtype='boolean')})  # Note `boolean` != `bool`.
        buf = _dataframe(df, table_name='tbl1')
        self.assertEqual(
            buf,
            'tbl1 a=t\n' +
            'tbl1 a=f\n' +
            'tbl1 a=f\n' +
            'tbl1 a=f\n' +
            'tbl1 a=t\n' +
            'tbl1 a=f\n' +
            'tbl1 a=t\n' +
            'tbl1 a=t\n' +
            'tbl1 a=t\n' +
            'tbl1 a=f\n' +
            'tbl1 a=f\n' +
            'tbl1 a=f\n')

        df2 = pd.DataFrame({'a': pd.Series([
                True, False, False,
                None, True, False],
            dtype='boolean')})
        with self.assertRaisesRegex(
                qi.IngressError,
                'Failed.*at row index 3 .*<NA>.: .*insert null .*boolean col'):
            _dataframe(df2, table_name='tbl1')

    def test_bool_obj_col(self):
        df = pd.DataFrame({'a': pd.Series([
                True, False, False,
                False, True, False],
            dtype='object')})
        buf = _dataframe(df, table_name='tbl1')
        self.assertEqual(
            buf,
            'tbl1 a=t\n' +
            'tbl1 a=f\n' +
            'tbl1 a=f\n' +
            'tbl1 a=f\n' +
            'tbl1 a=t\n' +
            'tbl1 a=f\n')

        df2 = pd.DataFrame({'a': pd.Series([
                True, False, 'false'],
            dtype='object')})
        with self.assertRaisesRegex(
                qi.IngressError,
                'serialize .* column .a. .* 2 .*false.*bool'):
            _dataframe(df2, table_name='tbl1')

        df3 = pd.DataFrame({'a': pd.Series([
                None, True, False],
            dtype='object')})
        with self.assertRaisesRegex(
                qi.IngressError,
                'serialize.*\\(None\\): Cannot insert null.*boolean column'):
            _dataframe(df3, table_name='tbl1')

    def test_datetime64_numpy_col(self):
        df = pd.DataFrame({
            'a': pd.Series([
                    pd.Timestamp('2019-01-01 00:00:00'),
                    pd.Timestamp('2019-01-01 00:00:01'),
                    pd.Timestamp('2019-01-01 00:00:02'),
                    pd.Timestamp('2019-01-01 00:00:03'),
                    pd.Timestamp('2019-01-01 00:00:04'),
                    pd.Timestamp('2019-01-01 00:00:05'),
                    None,
                    float('nan'),
                    pd.NA],
                dtype='datetime64[ns]'),
            'b': ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i']})
        buf = _dataframe(df, table_name='tbl1')
        self.assertEqual(
            buf,
            'tbl1 a=1546300800000000t,b="a"\n' +
            'tbl1 a=1546300801000000t,b="b"\n' +
            'tbl1 a=1546300802000000t,b="c"\n' +
            'tbl1 a=1546300803000000t,b="d"\n' +
            'tbl1 a=1546300804000000t,b="e"\n' +
            'tbl1 a=1546300805000000t,b="f"\n' +
            'tbl1 b="g"\n' +
            'tbl1 b="h"\n' +
            'tbl1 b="i"\n')

        df = pd.DataFrame({'a': pd.Series([
                pd.Timestamp('1970-01-01 00:00:00'),
                pd.Timestamp('1970-01-01 00:00:01'),
                pd.Timestamp('1970-01-01 00:00:02')])})
        buf = _dataframe(df, table_name='tbl1')
        self.assertEqual(
            buf,
            'tbl1 a=0t\n' +
            'tbl1 a=1000000t\n' +
            'tbl1 a=2000000t\n')

    def test_datetime64_tz_arrow_col(self):
        df = pd.DataFrame({
            'a': [
                pd.Timestamp(
                    year=2019, month=1, day=1,
                    hour=0, minute=0, second=0, tz=_TZ),
                pd.Timestamp(
                    year=2019, month=1, day=1,
                    hour=0, minute=0, second=1, tz=_TZ),
                None,
                pd.Timestamp(
                    year=2019, month=1, day=1,
                    hour=0, minute=0, second=3, tz=_TZ)],
            'b': ['sym1', 'sym2', 'sym3', 'sym4']})
        buf = _dataframe(df, table_name='tbl1', symbols=['b'])
        self.assertEqual(
            buf,
            # Note how these are 5hr offset from `test_datetime64_numpy_col`.
            'tbl1,b=sym1 a=1546318800000000t\n' +
            'tbl1,b=sym2 a=1546318801000000t\n' +
            'tbl1,b=sym3\n' +
            'tbl1,b=sym4 a=1546318803000000t\n')

        # Not epoch 0.
        df = pd.DataFrame({
            'a': [
                pd.Timestamp(
                    year=1970, month=1, day=1,
                    hour=0, minute=0, second=0, tz=_TZ),
                pd.Timestamp(
                    year=1970, month=1, day=1,
                    hour=0, minute=0, second=1, tz=_TZ),
                pd.Timestamp(
                    year=1970, month=1, day=1,
                    hour=0, minute=0, second=2, tz=_TZ)],
            'b': ['sym1', 'sym2', 'sym3']})
        buf = _dataframe(df, table_name='tbl1', symbols=['b'])
        self.assertEqual(
            buf,
            # Note how these are 5hr offset from `test_datetime64_numpy_col`.
            'tbl1,b=sym1 a=18000000000t\n' +
            'tbl1,b=sym2 a=18001000000t\n' +
            'tbl1,b=sym3 a=18002000000t\n')

        # Actual epoch 0.
        df = pd.DataFrame({
            'a': [
                pd.Timestamp(
                    year=1969, month=12, day=31,
                    hour=19, minute=0, second=0, tz=_TZ),
                pd.Timestamp(
                    year=1969, month=12, day=31,
                    hour=19, minute=0, second=1, tz=_TZ),
                pd.Timestamp(
                    year=1969, month=12, day=31,
                    hour=19, minute=0, second=2, tz=_TZ)],
            'b': ['sym1', 'sym2', 'sym3']})
        buf = _dataframe(df, table_name='tbl1', symbols=['b'])
        self.assertEqual(
            buf,
            'tbl1,b=sym1 a=0t\n' +
            'tbl1,b=sym2 a=1000000t\n' +
            'tbl1,b=sym3 a=2000000t\n')

        df2 = pd.DataFrame({
            'a': [
                pd.Timestamp(
                    year=1900, month=1, day=1,
                    hour=0, minute=0, second=0, tz=_TZ)],
            'b': ['sym1']})
        buf = _dataframe(df2, table_name='tbl1', symbols=['b'])

        # Accounting for different datatime library differences.
        # Mostly, here assert that negative timestamps are allowed.
        self.assertIn(
            buf,
            ['tbl1,b=sym1 a=-2208970800000000t\n',
             'tbl1,b=sym1 a=-2208971040000000t\n'])

    def test_datetime64_numpy_at(self):
        df = pd.DataFrame({
            'a': pd.Series([
                    pd.Timestamp('2019-01-01 00:00:00'),
                    pd.Timestamp('2019-01-01 00:00:01'),
                    pd.Timestamp('2019-01-01 00:00:02'),
                    pd.Timestamp('2019-01-01 00:00:03'),
                    pd.Timestamp('2019-01-01 00:00:04'),
                    pd.Timestamp('2019-01-01 00:00:05'),
                    float('nan'),
                    None,
                    pd.NaT],
                dtype='datetime64[ns]'),
            'b': [1, 2, 3, 4, 5, 6, 7, 8, 9]})
        buf = _dataframe(df, table_name='tbl1', at='a')
        self.assertEqual(
            buf,
            'tbl1 b=1i 1546300800000000000\n' +
            'tbl1 b=2i 1546300801000000000\n' +
            'tbl1 b=3i 1546300802000000000\n' +
            'tbl1 b=4i 1546300803000000000\n' +
            'tbl1 b=5i 1546300804000000000\n' +
            'tbl1 b=6i 1546300805000000000\n' +
            'tbl1 b=7i\n' +
            'tbl1 b=8i\n' +
            'tbl1 b=9i\n')

        df = pd.DataFrame({
            'a': pd.Series([
                    pd.Timestamp('1970-01-01 00:00:00'),
                    pd.Timestamp('1970-01-01 00:00:01'),
                    pd.Timestamp('1970-01-01 00:00:02')],
                dtype='datetime64[ns]'),
            'b': [1, 2, 3]})
        buf = _dataframe(df, table_name='tbl1', at='a')
        self.assertEqual(
            buf,
            'tbl1 b=1i 0\n' +
            'tbl1 b=2i 1000000000\n' +
            'tbl1 b=3i 2000000000\n')

    def test_datetime64_tz_arrow_at(self):
        df = pd.DataFrame({
            'a': [
                pd.Timestamp(
                    year=2019, month=1, day=1,
                    hour=0, minute=0, second=0, tz=_TZ),
                pd.Timestamp(
                    year=2019, month=1, day=1,
                    hour=0, minute=0, second=1, tz=_TZ),
                None,
                pd.Timestamp(
                    year=2019, month=1, day=1,
                    hour=0, minute=0, second=3, tz=_TZ)],
            'b': ['sym1', 'sym2', 'sym3', 'sym4']})
        buf = _dataframe(df, table_name='tbl1', symbols=['b'], at='a')
        self.assertEqual(
            buf,
            # Note how these are 5hr offset from `test_datetime64_numpy_col`.
            'tbl1,b=sym1 1546318800000000000\n' +
            'tbl1,b=sym2 1546318801000000000\n' +
            'tbl1,b=sym3\n' +
            'tbl1,b=sym4 1546318803000000000\n')

        df2 = pd.DataFrame({
            'a': [
                pd.Timestamp(
                    year=1900, month=1, day=1,
                    hour=0, minute=0, second=0, tz=_TZ)],
            'b': ['sym1']})
        with self.assertRaisesRegex(
                qi.IngressError, "Failed.*'a'.*-220897.* is neg"):
            _dataframe(df2, table_name='tbl1', symbols=['b'], at='a')

    def _test_pyobjstr_table(self, dtype):
        df = pd.DataFrame({
            '../bad col name/../it does not matter...':
                pd.Series([
                    'a',                     # ASCII
                    'b' * 127,               # Max table name length.
                    'q❤️p',                   # Mixed ASCII and UCS-2
                    '嚜꓂',                   # UCS-2, 3 bytes for UTF-8.
                    '💩🦞'],                 # UCS-4, 4 bytes for UTF-8.
                dtype=dtype),
            'b': [1, 2, 3, 4, 5]})
        buf = _dataframe(df, table_name_col=0)
        self.assertEqual(
            buf,
            'a b=1i\n' +
            ('b' * 127) + ' b=2i\n' +
            'q❤️p b=3i\n' +
            '嚜꓂ b=4i\n' +
            '💩🦞 b=5i\n')

        with self.assertRaisesRegex(
                qi.IngressError, "Too long"):
            _dataframe(
                pd.DataFrame({'a': pd.Series(['b' * 128], dtype=dtype)}),
                table_name_col='a')

        with self.assertRaisesRegex(
                qi.IngressError, 'Failed.*Expected a table name, got a null.*'):
            _dataframe(
                pd.DataFrame({
                    '.': pd.Series(['x', None], dtype=dtype),
                    'b': [1, 2]}),
                table_name_col='.')

        with self.assertRaisesRegex(
                qi.IngressError, 'Failed.*Expected a table name, got a null.*'):
            _dataframe(
                pd.DataFrame({
                    '.': pd.Series(['x', float('nan')], dtype=dtype),
                    'b': [1, 2]}),
                table_name_col='.')

        with self.assertRaisesRegex(
                qi.IngressError, 'Failed.*Expected a table name, got a null.*'):
            _dataframe(
                pd.DataFrame({
                    '.': pd.Series(['x', pd.NA], dtype=dtype),
                    'b': [1, 2]}),
                table_name_col='.')

        with self.assertRaisesRegex(
                qi.IngressError, "''.*must have a non-zero length"):
            _dataframe(
                pd.DataFrame({
                    '/': pd.Series([''], dtype=dtype),
                    'b': [1]}),
                table_name_col='/')

        with self.assertRaisesRegex(
                qi.IngressError, "'tab..1'.*invalid dot `\\.` at position 4"):
            _dataframe(
                pd.DataFrame({
                    '/': pd.Series(['tab..1'], dtype=dtype),
                    'b': [1]}),
                table_name_col='/')

    def test_obj_str_table(self):
        self._test_pyobjstr_table('object')

        with self.assertRaisesRegex(
                qi.IngressError, 'table name .*got an object of type int'):
            _dataframe(
                pd.DataFrame({
                    '.': pd.Series(['x', 42], dtype='object'),
                    'z': [1, 2]}),
                table_name_col='.')

    def test_obj_string_table(self):
        self._test_pyobjstr_table('string')

        self.assertEqual(
            _dataframe(
                pd.DataFrame({
                    '.': pd.Series(['x', 42], dtype='string'),
                    'z': [1, 2]}),
                table_name_col='.'),
            'x z=1i\n' +
            '42 z=2i\n')

    def _test_pyobjstr_numpy_symbol(self, dtype):
        df = pd.DataFrame({'a': pd.Series([
                'a',                     # ASCII
                'q❤️p',                   # Mixed ASCII and UCS-2
                '❤️' * 1200,              # Over the 1024 buffer prealloc.
                'Questo è un qualcosa',  # Non-ASCII UCS-1
                'щось',                  # UCS-2, 2 bytes for UTF-8.
                '',                      # Empty string
                '嚜꓂',                   # UCS-2, 3 bytes for UTF-8.
                '💩🦞'],                 # UCS-4, 4 bytes for UTF-8.
            dtype=dtype)})
        buf = _dataframe(df, table_name='tbl1', symbols=True)
        self.assertEqual(
            buf,
            'tbl1,a=a\n' +
            'tbl1,a=q❤️p\n' +
            'tbl1,a=' + ('❤️' * 1200) + '\n' +
            'tbl1,a=Questo\\ è\\ un\\ qualcosa\n' +
            'tbl1,a=щось\n' +
            'tbl1,a=\n' +
            'tbl1,a=嚜꓂\n' +
            'tbl1,a=💩🦞\n')

        for null_obj in (None, float('nan'), pd.NA):
            self.assertEqual(
                _dataframe(
                    pd.DataFrame({
                        'x': pd.Series(['a', null_obj], dtype=dtype),
                        'y': [1, 2]}),
                    table_name='tbl1', symbols=[0]),
                'tbl1,x=a y=1i\n' +
                'tbl1 y=2i\n')

    def test_obj_str_numpy_symbol(self):
        self._test_pyobjstr_numpy_symbol('object')

        with self.assertRaisesRegex(
                qi.IngressError, 'Expected a string, got an .* type int'):
            _dataframe(
                pd.DataFrame({
                    'x': pd.Series(['x', 42], dtype='object'),
                    'y': [1, 2]}),
                table_name='tbl1', symbols=[0])

    def test_obj_string_numpy_symbol(self):
        self._test_pyobjstr_numpy_symbol('string')

        self.assertEqual(
            _dataframe(
                pd.DataFrame({
                    'x': pd.Series(['x', 42], dtype='string'),
                    'y': [1, 2]}),
                table_name='tbl1', symbols=[0]),
            'tbl1,x=x y=1i\n' +
            'tbl1,x=42 y=2i\n')

    def test_str_numpy_col(self):
        df = pd.DataFrame({'a': pd.Series([
                'a',                     # ASCII
                'q❤️p',                   # Mixed ASCII and UCS-2
                '❤️' * 1200,              # Over the 1024 buffer prealloc.
                'Questo è un qualcosa',  # Non-ASCII UCS-1
                'щось',                  # UCS-2, 2 bytes for UTF-8.
                '',                      # Empty string
                '嚜꓂',                   # UCS-2, 3 bytes for UTF-8.
                '💩🦞'],                 # UCS-4, 4 bytes for UTF-8.
            dtype='str')})
        buf = _dataframe(df, table_name='tbl1')
        self.assertEqual(
            buf,
            'tbl1 a="a"\n' +
            'tbl1 a="q❤️p"\n' +
            'tbl1 a="' + ('❤️' * 1200) + '"\n' +
            'tbl1 a="Questo è un qualcosa"\n' +
            'tbl1 a="щось"\n' +
            'tbl1 a=""\n' +
            'tbl1 a="嚜꓂"\n' +
            'tbl1 a="💩🦞"\n')

    def test_str_arrow_table(self):
        df = pd.DataFrame({
            '../bad col name/../it does not matter...': pd.Series([
                'a',                     # ASCII
                'b' * 127,               # Max table name length.
                'q❤️p',                   # Mixed ASCII and UCS-2
                '嚜꓂',                   # UCS-2, 3 bytes for UTF-8.
                '💩🦞'],                 # UCS-4, 4 bytes for UTF-8.
                dtype='string[pyarrow]'),
            'b': [1, 2, 3, 4, 5]})
        buf = _dataframe(df, table_name_col=0)
        self.assertEqual(
            buf,
            'a b=1i\n' +
            ('b' * 127) + ' b=2i\n' +
            'q❤️p b=3i\n' +
            '嚜꓂ b=4i\n' +
            '💩🦞 b=5i\n')

        with self.assertRaisesRegex(
                qi.IngressError, "Too long"):
            _dataframe(
                pd.DataFrame({
                    'a': pd.Series(['b' * 128], dtype='string[pyarrow]')}),
                table_name_col='a')

        with self.assertRaisesRegex(
                qi.IngressError, "Failed .*<NA>.*Table name cannot be null"):
            _dataframe(
                pd.DataFrame({
                    '.': pd.Series(['x', None], dtype='string[pyarrow]'),
                    'b': [1, 2]}),
                table_name_col='.')

        with self.assertRaisesRegex(
                qi.IngressError, "''.*must have a non-zero length"):
            _dataframe(
                pd.DataFrame({
                    '/': pd.Series([''], dtype='string[pyarrow]')}),
                table_name_col='/')

        with self.assertRaisesRegex(
                qi.IngressError, "'tab..1'.*invalid dot `\\.` at position 4"):
            _dataframe(
                pd.DataFrame({
                    '/': pd.Series(['tab..1'], dtype='string[pyarrow]')}),
                table_name_col='/')

    def test_str_arrow_symbol(self):
        df = pd.DataFrame({
            'a': pd.Series([
                'a',                     # ASCII
                'q❤️p',                   # Mixed ASCII and UCS-2
                '❤️' * 1200,              # Over the 1024 buffer prealloc.
                'Questo è un qualcosa',  # Non-ASCII UCS-1
                'щось',                  # UCS-2, 2 bytes for UTF-8.
                '',                      # Empty string
                None,
                '嚜꓂',                   # UCS-2, 3 bytes for UTF-8.
                '💩🦞'],                 # UCS-4, 4 bytes for UTF-8.
                dtype='string[pyarrow]'),
            'b': [1, 2, 3, 4, 5, 6, 7, 8, 9]})
        buf = _dataframe(df, table_name='tbl1', symbols=True)
        self.assertEqual(
            buf,
            'tbl1,a=a b=1i\n' +
            'tbl1,a=q❤️p b=2i\n' +
            'tbl1,a=' + ('❤️' * 1200) + ' b=3i\n' +
            'tbl1,a=Questo\\ è\\ un\\ qualcosa b=4i\n' +
            'tbl1,a=щось b=5i\n' +
            'tbl1,a= b=6i\n' +
            'tbl1 b=7i\n' +
            'tbl1,a=嚜꓂ b=8i\n' +
            'tbl1,a=💩🦞 b=9i\n')

    def test_str_arrow_col(self):
        df = pd.DataFrame({
            'a': pd.Series([
                'a',                     # ASCII
                'q❤️p',                   # Mixed ASCII and UCS-2
                '❤️' * 1200,              # Over the 1024 buffer prealloc.
                'Questo è un qualcosa',  # Non-ASCII UCS-1
                'щось',                  # UCS-2, 2 bytes for UTF-8.
                '',                      # Empty string
                None,
                '嚜꓂',                   # UCS-2, 3 bytes for UTF-8.
                '💩🦞'],                 # UCS-4, 4 bytes for UTF-8.
                dtype='string[pyarrow]'),
            'b': [1, 2, 3, 4, 5, 6, 7, 8, 9]})
        buf = _dataframe(df, table_name='tbl1', symbols=False)
        self.assertEqual(
            buf,
            'tbl1 a="a",b=1i\n' +
            'tbl1 a="q❤️p",b=2i\n' +
            'tbl1 a="' + ('❤️' * 1200) + '",b=3i\n' +
            'tbl1 a="Questo è un qualcosa",b=4i\n' +
            'tbl1 a="щось",b=5i\n' +
            'tbl1 a="",b=6i\n' +
            'tbl1 b=7i\n' +
            'tbl1 a="嚜꓂",b=8i\n' +
            'tbl1 a="💩🦞",b=9i\n')

    def test_pyobj_int_col(self):
        int64_min = -2**63
        int64_max = 2**63 - 1
        self.assertEqual(
            _dataframe(
                pd.DataFrame({
                    'a': pd.Series([
                        1, 2, 3, None, float('nan'), pd.NA, 7,
                        0,
                        int64_min,
                        int64_max], dtype='object'),
                    'b': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]}),
                table_name='tbl1'),
            'tbl1 a=1i,b=1i\n' +
            'tbl1 a=2i,b=2i\n' +
            'tbl1 a=3i,b=3i\n' +
            'tbl1 b=4i\n' +
            'tbl1 b=5i\n' +
            'tbl1 b=6i\n' +
            'tbl1 a=7i,b=7i\n' +
            'tbl1 a=0i,b=8i\n' +
            'tbl1 a=' + str(int64_min) + 'i,b=9i\n' +
            'tbl1 a=' + str(int64_max) + 'i,b=10i\n')

        with self.assertRaisesRegex(
                qi.IngressError, "1 \\('STRING'\\): .*type int, got.*str\\."):
            _dataframe(
                pd.DataFrame({
                    'a': pd.Series([1, 'STRING'], dtype='object'),
                    'b': [1, 2]}),
                table_name='tbl1')

        out_of_range = [int64_min - 1, int64_max + 1]
        for num in out_of_range:
            with self.assertRaisesRegex(
                    qi.IngressError, "index 1 .*922337203685477.*int too big"):
                _dataframe(
                    pd.DataFrame({
                        'a': pd.Series([1, num], dtype='object'),
                        'b': [1, 2]}),
                    table_name='tbl1')

    def test_pyobj_float_col(self):
        self.assertEqual(
            _dataframe(
                pd.DataFrame({
                    'a': pd.Series(
                        [1.0, 2.0, 3.0, None, float('nan'), pd.NA, 7.0],
                        dtype='object'),
                    'b': [1, 2, 3, 4, 5, 6, 7]}),
                table_name='tbl1'),
            'tbl1 a=1.0,b=1i\n' +
            'tbl1 a=2.0,b=2i\n' +
            'tbl1 a=3.0,b=3i\n' +
            'tbl1 b=4i\n' +
            'tbl1 a=NaN,b=5i\n' +
            'tbl1 b=6i\n' +
            'tbl1 a=7.0,b=7i\n')

        with self.assertRaisesRegex(
                qi.IngressError, "1 \\('STRING'\\): .*type float, got.*str\\."):
            _dataframe(
                pd.DataFrame({
                    'a': pd.Series([1.0, 'STRING'], dtype='object'),
                    'b': [1, 2]}),
                table_name='tbl1')

    def test_bad_category(self):
        # We only support string categories
        # (unless anyone asks for additional ones).
        # We want to test others are rejected.
        with self.assertRaisesRegex(
                qi.IngressError, "Bad column 'a'.*got a category of .*int64"):
            _dataframe(
                pd.DataFrame({'a': pd.Series([1, 2, 3, 2], dtype='category')}),
                table_name='tbl1')

    def _test_cat_table(self, count):
        slist = [f's{i}' for i in range(count)]

        df = pd.DataFrame({
            'a': pd.Series(slist, dtype='category'),
            'b': list(range(len(slist)))})

        buf = _dataframe(df, table_name_col=0)
        exp = ''.join(
            f'{s} b={i}i\n'
            for i, s in enumerate(slist))
        self.assertEqual(buf, exp)

        slist[2] = None
        df2 = pd.DataFrame({
            'a': pd.Series(slist, dtype='category'),
            'b': list(range(len(slist)))})
        with self.assertRaisesRegex(
                qi.IngressError, 'Table name cannot be null'):
            _dataframe(df2, table_name_col=0)

    def test_cat_i8_table(self):
        self._test_cat_table(30)
        self._test_cat_table(127)

    def test_cat_i16_table(self):
        self._test_cat_table(128)
        self._test_cat_table(4000)
        self._test_cat_table(32767)

    def test_cat_i32_table(self):
        self._test_cat_table(32768)
        self._test_cat_table(40000)

    def _test_cat_symbol(self, count):
        slist = [f's{i}' for i in range(count)]

        df = pd.DataFrame({
            'a': pd.Series(slist, dtype='category'),
            'b': list(range(len(slist)))})

        buf = _dataframe(df, table_name='tbl1', symbols=True)
        exp = ''.join(
            f'tbl1,a={s} b={i}i\n'
            for i, s in enumerate(slist))
        self.assertEqual(buf, exp)

        slist[2] = None
        df2 = pd.DataFrame({
            'a': pd.Series(slist, dtype='category'),
            'b': list(range(len(slist)))})

        exp2 = exp.replace('tbl1,a=s2 b=2i\n', 'tbl1 b=2i\n')
        buf2 = _dataframe(df2, table_name='tbl1', symbols=True)
        self.assertEqual(buf2, exp2)

    def test_cat_i8_symbol(self):
        self._test_cat_symbol(30)
        self._test_cat_symbol(127)

    def test_cat_i16_symbol(self):
        self._test_cat_symbol(128)
        self._test_cat_symbol(4000)
        self._test_cat_symbol(32767)

    def test_cat_i32_symbol(self):
        self._test_cat_symbol(32768)
        self._test_cat_symbol(40000)

    def _test_cat_str(self, count):
        slist = [f's{i}' for i in range(count)]

        df = pd.DataFrame({
            'a': pd.Series(slist, dtype='category'),
            'b': list(range(len(slist)))})

        buf = _dataframe(df, table_name='tbl1', symbols=False)
        exp = ''.join(
            f'tbl1 a="{s}",b={i}i\n'
            for i, s in enumerate(slist))
        self.assertEqual(buf, exp)

        slist[2] = None
        df2 = pd.DataFrame({
            'a': pd.Series(slist, dtype='category'),
            'b': list(range(len(slist)))})

        exp2 = exp.replace('tbl1 a="s2",b=2i\n', 'tbl1 b=2i\n')
        buf2 = _dataframe(df2, table_name='tbl1', symbols=False)
        self.assertEqual(buf2, exp2)

    def test_cat_i8_str(self):
        self._test_cat_str(30)
        self._test_cat_str(127)

    def test_cat_i16_str(self):
        self._test_cat_str(128)
        self._test_cat_str(4000)
        self._test_cat_str(32767)

    def test_cat_i32_str(self):
        self._test_cat_str(32768)
        self._test_cat_str(40000)

    def test_all_nulls_pyobj_col(self):
        df = pd.DataFrame({
            'a': [None, pd.NA, float('nan')],
            'b': [1, 2, 3]})
        buf = _dataframe(df, table_name='tbl1')
        self.assertEqual(
            buf,
            'tbl1 b=1i\n' +
            'tbl1 b=2i\n' +
            'tbl1 b=3i\n')

    def test_strided_numpy_column(self):
        two_d = np.array([
            [1, 10],
            [2, 20],
            [3, 30]], dtype='int64')
        col2 = two_d[:, 1]
        col2.flags['WRITEABLE'] = False

        # Checking our test case setup.
        mv = memoryview(col2)
        self.assertEqual(mv.contiguous, False)
        self.assertEqual(mv.strides, (16,))

        df = pd.DataFrame(col2, copy=False)
        df.columns = ['a']

        with self.assertRaisesRegex(
                qi.IngressError, "Bad column 'a': .*not.*contiguous"):
            _dataframe(df, table_name='tbl1')

    def test_serializing_in_chunks(self):
        df = pd.DataFrame({
            'a': pd.Series(np.arange(30), dtype='int64'),
            'b': pd.Series(np.arange(30), dtype='Int64')})
        parts = [
            df.iloc[:10],
            df.iloc[10:20],
            df.iloc[20:]]
        for index, part in enumerate(parts):
            buf = _dataframe(part, table_name='tbl1')
            exp = ''.join(
                f'tbl1 a={i}i,b={i}i\n'
                for i in range(index * 10, (index + 1) * 10))
            self.assertEqual(buf, exp)

    def test_arrow_chunked_array(self):
        # We build a table with chunked arrow arrays as columns.
        chunks_a = [
            pa.array([1, 2, 3], type=pa.int16()),
            pa.array([4, 5, 6], type=pa.int16()),
            pa.array([], type=pa.int16()),
            pa.array([7, 8, 9], type=pa.int16())]
        chunked_a = pa.chunked_array(chunks_a)
        chunks_b = [
            pa.array([10, 20], type=pa.int32()),
            pa.array([], type=pa.int32()),
            pa.array([30, 40, 50, 60], type=pa.int32()),
            pa.array([70, 80, 90], type=pa.int32())]
        chunked_b = pa.chunked_array(chunks_b)
        arr_tab = pa.Table.from_arrays([chunked_a, chunked_b], names=['a', 'b'])

        # NOTE!
        # This does *not* preserve the chunking of the arrow arrays.
        df = arr_tab.to_pandas()
        buf = _dataframe(df, table_name='tbl1')
        exp = (
            'tbl1 a=1i,b=10i\n' +
            'tbl1 a=2i,b=20i\n' +
            'tbl1 a=3i,b=30i\n' +
            'tbl1 a=4i,b=40i\n' +
            'tbl1 a=5i,b=50i\n' +
            'tbl1 a=6i,b=60i\n' +
            'tbl1 a=7i,b=70i\n' +
            'tbl1 a=8i,b=80i\n' +
            'tbl1 a=9i,b=90i\n')
        self.assertEqual(buf, exp)

        if not hasattr(pd, 'ArrowDtype'):
            # We don't have pandas ArrowDtype, so we can't test the rest.
            return

        # To preserve the chunking we need to use a special pandas type:
        pandarrow_a = pd.array(chunked_a, dtype='int16[pyarrow]')
        pandarrow_b = pd.array(chunked_b, dtype='int32[pyarrow]')
        df = pd.DataFrame({'a': pandarrow_a, 'b': pandarrow_b})

        # Note that this dtype is experimental (currently),
        # so we don't support it yet.. but we have everything in place should we
        # need to, so - as for now - we just test that we raise a nice error.
        with self.assertRaisesRegex(
                qi.IngressError,
                "Unsupported dtype int16\[pyarrow\] for column 'a'.*github"):
            _dataframe(df, table_name='tbl1')

    @unittest.skipIf(not fastparquet, 'fastparquet not installed')
    @with_tmp_dir
    def test_parquet_roundtrip(self, tmpdir):
        pa_parquet_path = tmpdir / 'test_pa.parquet'
        fp_parquet_path = tmpdir / 'test_fp.parquet'
        df = pd.DataFrame({
            's': pd.Categorical(['a', 'b', 'a', 'c', 'a']),
            'a': pd.Series([1, 2, 3, 4, 5], dtype='int16'),
            'b': pd.Series([10, 20, 30, None, 50], dtype='UInt8'),
            'c': [0.5, float('nan'), 2.5, 3.5, None]})
        df.to_parquet(pa_parquet_path, engine='pyarrow')
        df.to_parquet(fp_parquet_path, engine='fastparquet')
        pa2pa_df = pd.read_parquet(pa_parquet_path, engine='pyarrow')
        pa2fp_df = pd.read_parquet(pa_parquet_path, engine='fastparquet')
        fp2pa_df = pd.read_parquet(fp_parquet_path, engine='pyarrow')
        fp2fp_df = pd.read_parquet(fp_parquet_path, engine='fastparquet')

        exp_dtypes = ['category', 'int16', 'UInt8', 'float64']
        self.assertEqual(list(df.dtypes), exp_dtypes)

        def df_eq(exp_df, deser_df, exp_dtypes):
            self.assertEqual(list(deser_df.dtypes), exp_dtypes)
            if not exp_df.equals(deser_df):
                print('\nexp_df:')
                print(exp_df)
                print('\ndeser_df:')
                print(deser_df)
            self.assertTrue(exp_df.equals(deser_df))

        # fastparquet doesn't roundtrip with pyarrow parquet properly.
        # It decays categories to object and UInt8 to float64.
        # We need to set up special case expected results for that.
        fallback_exp_dtypes = [
            np.dtype('O'),
            np.dtype('int16'),
            np.dtype('float64'),
            np.dtype('float64')]
        fallback_df = df.astype({'s': 'object', 'b': 'float64'})

        df_eq(df, pa2pa_df, exp_dtypes)
        df_eq(df, pa2fp_df, exp_dtypes)
        df_eq(fallback_df, fp2pa_df, fallback_exp_dtypes)
        df_eq(df, fp2fp_df, exp_dtypes)

        exp = (
            'tbl1,s=a a=1i,b=10i,c=0.5\n' +
            'tbl1,s=b a=2i,b=20i,c=NaN\n' +
            'tbl1,s=a a=3i,b=30i,c=2.5\n' +
            'tbl1,s=c a=4i,c=3.5\n' +
            'tbl1,s=a a=5i,b=50i,c=NaN\n')

        fallback_exp = (
            'tbl1 s="a",a=1i,b=10.0,c=0.5\n' +
            'tbl1 s="b",a=2i,b=20.0,c=NaN\n' +
            'tbl1 s="a",a=3i,b=30.0,c=2.5\n' +
            'tbl1 s="c",a=4i,b=NaN,c=3.5\n' +
            'tbl1 s="a",a=5i,b=50.0,c=NaN\n')

        self.assertEqual(_dataframe(df, table_name='tbl1'), exp)
        self.assertEqual(_dataframe(pa2pa_df, table_name='tbl1'), exp)
        self.assertEqual(_dataframe(pa2fp_df, table_name='tbl1'), exp)
        self.assertEqual(_dataframe(fp2pa_df, table_name='tbl1'), fallback_exp)
        self.assertEqual(_dataframe(fp2fp_df, table_name='tbl1'), exp)


if __name__ == '__main__':
    if os.environ.get('TEST_QUESTDB_PROFILE') == '1':
        import cProfile
        cProfile.run('unittest.main()', sort='cumtime')
    else:
        unittest.main()
