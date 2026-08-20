#!/usr/bin/env python3

import os
import subprocess
import sys

sys.dont_write_bytecode = True
import unittest
import datetime as dt
import functools
import tempfile
import pathlib
from decimal import Decimal
from test_tools import _float_binary_bytes, _array_binary_bytes, TimestampEncodingMixin

BROKEN_TIMEZONES = True

try:
    import zoneinfo
    _TZ = zoneinfo.ZoneInfo('America/New_York')
    BROKEN_TIMEZONES = os.name == 'nt'
except ImportError:
    import pytz
    _TZ = pytz.timezone('America/New_York')

import questdb._client as qi
import pandas as pd
import numpy as np
import pyarrow as pa

# Pandas 3.x defaults tz-aware timestamps to microsecond resolution.
# Pin to nanoseconds where tests expect nanosecond precision.
_NS_TZ_DTYPE = pd.DatetimeTZDtype(tz=_TZ, unit='ns')
_US_TZ_DTYPE = pd.DatetimeTZDtype(tz=_TZ, unit='us')

try:
    import fastparquet
except ImportError:
    fastparquet = None


def _dataframe(protocol_version: int, *args, **kwargs):
    buf = qi.Buffer(protocol_version=protocol_version)
    buf.dataframe(*args, **kwargs)
    return bytes(buf)


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
    'G': pd.Series([
        pd.Timestamp('20180310'),
        pd.Timestamp('20180311'),
        pd.Timestamp('20180312')], dtype='datetime64[ns]')})

DF3 = pd.DataFrame({
    'T': ['t1', 't2', 't1'],
    'A': ['a1', 'a2', 'a3'],
    'B': ['b1', None, 'b3'],
    'C': pd.Series(['b1', None, 'b3'], dtype='string'),
    'D': pd.Series(['a1', 'a2', 'a3'], dtype='string'),
    'E': [1.0, 2.0, 3.0],
    'F': [1, 2, 3],
    "G": [
        np.array([1.0]),
        np.array([10.0]),
        np.array([100.0])],
    'H': pd.Series([
        pd.Timestamp('20180310'),
        pd.Timestamp('20180311'),
        pd.Timestamp('20180312')], dtype='datetime64[ns]')}
)

DECIMAL_BINARY_FORMAT_TYPE = 23


def _decode_decimal_payload(line: bytes, prefix: bytes = b'tbl dec=') -> tuple[int, bytes]:
    """Extract (scale, mantissa-bytes) from a serialized decimal line."""
    if not line.startswith(prefix):
        raise AssertionError(f'Unexpected decimal prefix in line: {line!r}')
    payload = line[len(prefix):]
    if len(payload) < 4:
        raise AssertionError(f'Invalid decimal payload length: {len(payload)}')
    if payload[0] != ord('='):
        raise AssertionError(f'Unexpected decimal type marker: {payload[0]}')
    if payload[1] != DECIMAL_BINARY_FORMAT_TYPE:
        raise AssertionError(f'Unexpected decimal format type: {payload[1]}')
    scale = payload[2]
    byte_width = payload[3]
    mantissa = payload[4:]
    if len(mantissa) != byte_width:
        raise AssertionError(
            f'Expected {byte_width} mantissa bytes, got {len(mantissa)}')
    return scale, mantissa

def _unwrap_decimal(decimal: Decimal):
    (sign, digits, exponent) = decimal.as_tuple()
    unscaled = 0
    scale = 0
    for digit in digits:
        unscaled  = unscaled * 10 + digit
    if exponent > 0:
        unscaled = unscaled * pow(10, exponent)
    else:
        scale = -exponent
    if sign == 1:
        unscaled = -unscaled
    return scale, unscaled

def _decimal_from_unscaled(unscaled, scale: int):
    if unscaled is None:
        return None
    return Decimal(unscaled).scaleb(-scale)


def _decimal_binary_payload(unscaled, scale: int, byte_width: int) -> bytes:
    if unscaled is None:
        return b'=' + bytes([DECIMAL_BINARY_FORMAT_TYPE, 0, 0])
    return (
        b'=' +
        bytes([DECIMAL_BINARY_FORMAT_TYPE, scale, byte_width]) +
        int(unscaled).to_bytes(byte_width, byteorder='big', signed=True)
    )


def with_tmp_dir(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        with tempfile.TemporaryDirectory(prefix='py-questdb-client_') as tmpdir:
            return func(self, *args, pathlib.Path(tmpdir), **kwargs)
    return wrapper

class TestPandasBase:
    class TestPandas(unittest.TestCase, TimestampEncodingMixin):
        def test_mandatory_at_dataframe(self):
            with self.assertRaisesRegex(TypeError, "needs keyword-only argument at"):
                _dataframe(self.version, [])
            with self.assertRaisesRegex(TypeError, "needs keyword-only argument at"):
                buf = qi.Buffer(protocol_version=self.version)
                buf.dataframe([])

            buf = qi.Buffer(protocol_version=self.version)
            buf.dataframe(pd.DataFrame(), at=qi.ServerTimestamp)

        def test_mandatory_at_row(self):
            with self.assertRaisesRegex(TypeError, "needs keyword-only argument at"):
                buf = qi.Buffer(protocol_version=self.version)
                buf.row(table_name="test_buffer")

            buf = qi.Buffer(protocol_version=self.version)
            buf.row(table_name="test_mandatory_at_row", at=qi.ServerTimestamp)

        def test_bad_dataframe(self):
            with self.assertRaisesRegex(qi.QuestDBError,
                    'Expected pandas'):
                _dataframe(self.version, [], at=qi.ServerTimestamp)

        def test_row_path_rejects_columnar_only_object_columns(self):
            import uuid
            import ipaddress
            cases = (
                ('bytes/bytearray/memoryview', b'\x00\x01'),
                ('bytes/bytearray/memoryview', bytearray(b'\x00\x01')),
                ('bytes/bytearray/memoryview', memoryview(b'\x00\x01')),
                ('UUID', uuid.uuid4()),
                ('IPv4Address', ipaddress.IPv4Address('1.2.3.4')),
            )
            for descr, value in cases:
                df = pd.DataFrame({'a': [value]})
                with self.assertRaisesRegex(
                        qi.QuestDBError,
                        f'{descr} objects, which are only supported on the '
                        'columnar QuestDB.dataframe'):
                    _dataframe(self.version, df, table_name='t',
                               at=qi.ServerTimestamp)

        def test_no_table_name(self):
            with self.assertRaisesRegex(qi.QuestDBError,
                    'Must specify at least one of'):
                _dataframe(self.version, DF1, at=qi.ServerTimestamp)

        def test_bad_table_name_type(self):
            with self.assertRaisesRegex(TypeError, "'table_name' has incorrect type"):
                _dataframe(self.version, DF1, table_name=1.5, at=qi.ServerTimestamp)

        def test_invalid_table_name(self):
            with self.assertRaisesRegex(qi.QuestDBError,
                    '`table_name`: Bad string "."'):
                _dataframe(self.version, DF1, table_name='.', at=qi.ServerTimestamp)

        def test_invalid_column_dtype(self):
            with self.assertRaisesRegex(qi.QuestDBError,
                    '`table_name_col`: Bad dtype'):
                _dataframe(self.version, DF1, table_name_col='B', at=qi.ServerTimestamp)
            with self.assertRaisesRegex(qi.QuestDBError,
                    '`table_name_col`: Bad dtype'):
                _dataframe(self.version, DF1, table_name_col=1, at=qi.ServerTimestamp)
            with self.assertRaisesRegex(qi.QuestDBError,
                    '`table_name_col`: Bad dtype'):
                _dataframe(self.version, DF1, table_name_col=-3, at=qi.ServerTimestamp)
            with self.assertRaisesRegex(qi.QuestDBError,
                    '`table_name_col`: -5 index'):
                _dataframe(self.version, DF1, table_name_col=-5, at=qi.ServerTimestamp)

        def test_bad_str_obj_col(self):
            with self.assertRaisesRegex(qi.QuestDBError,
                    "`table_name_col`: Bad.*`object`.*bool.*'D'.*Must.*strings"):
                _dataframe(self.version, DF1, table_name_col='D', at=qi.ServerTimestamp)
            with self.assertRaisesRegex(qi.QuestDBError,
                    "`table_name_col`: Bad.*`object`.*bool.*'D'.*Must.*strings"):
                _dataframe(self.version, DF1, table_name_col=3, at=qi.ServerTimestamp)
            with self.assertRaisesRegex(qi.QuestDBError,
                    "`table_name_col`: Bad.*`object`.*bool.*'D'.*Must.*strings"):
                _dataframe(self.version, DF1, table_name_col=-1, at=qi.ServerTimestamp)

        def test_bad_symbol(self):
            with self.assertRaisesRegex(qi.QuestDBError,
                    '`symbols`.*bool.*tuple.*list'):
                _dataframe(self.version, DF1, table_name='tbl1', symbols=0, at=qi.ServerTimestamp)
            with self.assertRaisesRegex(qi.QuestDBError,
                    '`symbols`.*bool.*tuple.*list'):
                _dataframe(self.version, DF1, table_name='tbl1', symbols={}, at=qi.ServerTimestamp)
            with self.assertRaisesRegex(qi.QuestDBError,
                    '`symbols`.*bool.*tuple.*list'):
                _dataframe(self.version, DF1, table_name='tbl1', symbols=None, at=qi.ServerTimestamp)
            with self.assertRaisesRegex(qi.QuestDBError,
                    "`symbols`: Bad dtype `float64`.*'A'.*Must.*strings col"):
                _dataframe(self.version, DF1, table_name='tbl1', symbols=(0,), at=qi.ServerTimestamp)
            with self.assertRaisesRegex(qi.QuestDBError,
                    "`symbols`: Bad dtype `int64`.*'B'.*Must be a strings column."):
                _dataframe(self.version, DF1, table_name='tbl1', symbols=[1], at=qi.ServerTimestamp)

        def test_bad_at(self):
            with self.assertRaisesRegex(qi.QuestDBError,
                    '`at`.*2018.*not found in the'):
                _dataframe(self.version, DF1, table_name='tbl1', at='2018-03-10T00:00:00Z')
            with self.assertRaisesRegex(qi.QuestDBError,
                    '`at`.*float64.*be a datetime'):
                _dataframe(self.version, DF1, table_name='tbl1', at='A')
            with self.assertRaisesRegex(qi.QuestDBError,
                    '`at`.*int64.*be a datetime'):
                _dataframe(self.version, DF1, table_name='tbl1', at=1)
            with self.assertRaisesRegex(qi.QuestDBError,
                    '`at`.*object.*be a datetime'):
                _dataframe(self.version, DF1, table_name='tbl1', at=-1)

        def test_empty_dataframe(self):
            buf = _dataframe(self.version, pd.DataFrame(), table_name='tbl1', at=qi.ServerTimestamp)
            self.assertEqual(buf, b'')

        def test_zero_row_dataframe(self):
            buf = _dataframe(self.version, pd.DataFrame(columns=['A', 'B']), table_name='tbl1', at=qi.ServerTimestamp)
            self.assertEqual(buf, b'')

        def test_zero_column_dataframe(self):
            df = pd.DataFrame(index=[0, 1, 2])
            self.assertEqual(len(df), 3)
            buf = _dataframe(self.version, df, table_name='tbl1', at=qi.ServerTimestamp)
            self.assertEqual(buf, b'')

        def test_basic(self):
            buf = _dataframe(
                self.version,
                DF2,
                table_name_col='T',
                symbols=['A', 'B', 'C', 'D'],
                at=-1)
            e = self.enc_des_ts_n
            exp = (
                b't1,A=a1,B=b1,C=b1,D=a1 E' +  _float_binary_bytes(1.0, self.version == 1) +  f',F=1i {e(1520640000000000000)}\n'.encode() +
                b't2,A=a2,D=a2 E' + _float_binary_bytes(2.0, self.version == 1) + f',F=2i {e(1520726400000000000)}\n'.encode() +
                b't1,A=a3,B=b3,C=b3,D=a3 E' + _float_binary_bytes(3.0, self.version == 1) + f',F=3i {e(1520812800000000000)}\n'.encode())
            self.assertEqual(buf, exp)

        def test_basic_with_arrays(self):
            if self.version == 1:
                self.skipTest('Protocol version v1 doesn\'t support arrays')
            buf = _dataframe(
                self.version,
                DF3,
                table_name_col='T',
                symbols=['A', 'B', 'C', 'D'],
                at=-1)
            e = self.enc_des_ts_n
            exp = (
                b't1,A=a1,B=b1,C=b1,D=a1 E' +  _float_binary_bytes(1.0, self.version == 1) +  b',F=1i,G=' + _array_binary_bytes(np.array([1.0])) + f' {e(1520640000000000000)}\n'.encode() +
                b't2,A=a2,D=a2 E' + _float_binary_bytes(2.0, self.version == 1) + b',F=2i,G=' + _array_binary_bytes(np.array([10.0])) + f' {e(1520726400000000000)}\n'.encode() +
                b't1,A=a3,B=b3,C=b3,D=a3 E' + _float_binary_bytes(3.0, self.version == 1) + b',F=3i,G=' + _array_binary_bytes(np.array([100.0])) + f' {e(1520812800000000000)}\n'.encode())
            self.assertEqual(buf, exp)

        def test_named_dataframe(self):
            df = pd.DataFrame({
                'a': [1, 2, 3],
                'b': ['a', 'b', 'c']})
            df.index.name = 'table_name'
            buf = _dataframe(self.version, df, at=qi.ServerTimestamp)
            self.assertEqual(
                buf,
                b'table_name a=1i,b="a"\n' +
                b'table_name a=2i,b="b"\n' +
                b'table_name a=3i,b="c"\n')

            buf = _dataframe(self.version, df, table_name='tbl1', at=qi.ServerTimestamp)
            self.assertEqual(
                buf,
                b'tbl1 a=1i,b="a"\n' +
                b'tbl1 a=2i,b="b"\n' +
                b'tbl1 a=3i,b="c"\n')

            buf = _dataframe(self.version, df, table_name_col='b', at=qi.ServerTimestamp)
            self.assertEqual(
                buf,
                b'a a=1i\n' +
                b'b a=2i\n' +
                b'c a=3i\n')

            df.index.name = 42  # bad type, not str
            with self.assertRaisesRegex(qi.QuestDBError,
                    'Bad dataframe index name as table.*: Expected str, not.*int.'):
                _dataframe(self.version, df, at=qi.ServerTimestamp)

        @unittest.skipIf(BROKEN_TIMEZONES, 'requires accurate timezones')
        def test_at_good(self):
            df = pd.DataFrame({
                'a': [1, 2, 3],
                'b': ['a', 'b', 'c']})
            df.index.name = 'test_at_good'
            with self.assertRaisesRegex(qi.QuestDBError,
                    'Bad argument `at`: Column .2018-03.* not found .* dataframe.'):
                _dataframe(self.version, df, at='2018-03-10T00:00:00Z')

            # Same timestamp, specified in various ways.
            t1_setup = dt.datetime(2018, 3, 10, 0, 0, 0, tzinfo=dt.timezone.utc)
            t1 = t1_setup.replace(tzinfo=None)  # naive, interpreted as UTC
            t2 = dt.datetime(2018, 3, 10, 0, 0, 0, tzinfo=dt.timezone.utc)
            t3 = dt.datetime(2018, 3, 9, 19, 0, 0, tzinfo=_TZ)
            t4 = qi.TimestampNanos(1520640000000000000)
            t5 = qi.TimestampNanos.from_datetime(t1)
            t6 = qi.TimestampNanos.from_datetime(t2)
            t7 = qi.TimestampNanos.from_datetime(t3)
            timestamps = [t1, t2, t3, t4, t5, t6, t7]
            e = self.enc_des_ts_n
            for ts in timestamps:
                buf = _dataframe(self.version, df, table_name='tbl1', at=ts)
                self.assertEqual(
                    buf,
                    f'tbl1 a=1i,b="a" {e(1520640000000000000)}\n'.encode() +
                    f'tbl1 a=2i,b="b" {e(1520640000000000000)}\n'.encode() +
                    f'tbl1 a=3i,b="c" {e(1520640000000000000)}\n'.encode())

        @unittest.skipIf(BROKEN_TIMEZONES, 'requires accurate timezones')
        def test_at_neg(self):
            n1 = dt.datetime(1965, 1, 1, 0, 0, 0, tzinfo=dt.timezone.utc)
            n2 = dt.datetime(1965, 1, 1, 0, 0, 0, tzinfo=_TZ)
            n3 = dt.datetime(1965, 1, 1, 0, 0, 0)
            neg_timestamps = [n1, n2, n3]
            for ts in neg_timestamps:
                with self.assertRaisesRegex(qi.QuestDBError,
                        'Bad.*`at`: Cannot .* before the Unix epoch .1970-01-01.*'):
                    _dataframe(self.version, DF2, at=ts, table_name='test_at_neg')

        @unittest.skipIf(BROKEN_TIMEZONES, 'requires accurate timezones')
        def test_at_ts_0(self):
            df = pd.DataFrame({
                'a': [1, 2, 3],
                'b': ['a', 'b', 'c']})
            df.index.name = 'test_at_ts_0'

            # Epoch 0, specified in various ways.
            e1_setup = dt.datetime(1970, 1, 1, 0, 0, 0, tzinfo=dt.timezone.utc)
            e1 = e1_setup.replace(tzinfo=None)  # naive, interpreted as UTC
            e2 = dt.datetime(1970, 1, 1, 0, 0, 0, tzinfo=dt.timezone.utc)
            e3 = dt.datetime(1969, 12, 31, 19, 0, 0, tzinfo=_TZ)
            e4 = qi.TimestampNanos(0)
            e5 = qi.TimestampNanos.from_datetime(e1)
            e6 = qi.TimestampNanos.from_datetime(e2)
            e7 = qi.TimestampNanos.from_datetime(e3)
            edge_timestamps = [e1, e2, e3, e4, e5, e6, e7]

            e = self.enc_des_ts_n
            for ts in edge_timestamps:
                buf = _dataframe(self.version, df, table_name='tbl1', at=ts)
                self.assertEqual(
                    buf,
                    f'tbl1 a=1i,b="a" {e(0)}\n'.encode() +
                    f'tbl1 a=2i,b="b" {e(0)}\n'.encode() +
                    f'tbl1 a=3i,b="c" {e(0)}\n'.encode())

        def test_single_at_col(self):
            df = pd.DataFrame({'timestamp': pd.to_datetime(['2023-01-01'])})
            with self.assertRaisesRegex(qi.QuestDBError,
                    'Bad dataframe row at index 0: All values are nulls.'):
                _dataframe(self.version, df, table_name='tbl1', at='timestamp')

        def test_row_of_nulls(self):
            df = pd.DataFrame({'a': ['a1', None, 'a3']})
            with self.assertRaisesRegex(
                    qi.QuestDBError, 'Bad dataframe row.*1: All values are nulls.'):
                _dataframe(self.version, df, table_name='tbl1', symbols=['a'], at=qi.ServerTimestamp)

        def test_planning_error_keeps_existing_buffer(self):
            buf = qi.Buffer(protocol_version=self.version)
            buf.dataframe(
                pd.DataFrame({'a': [1]}),
                table_name='tbl1',
                at=qi.ServerTimestamp)
            before = bytes(buf)

            with self.assertRaisesRegex(
                    qi.QuestDBError,
                    "`symbols`: Bad dtype `int64`.*'a'.*Must be a strings column."):
                buf.dataframe(
                    pd.DataFrame({'a': [1]}),
                    table_name='tbl2',
                    symbols=['a'],
                    at=qi.ServerTimestamp)

            self.assertEqual(bytes(buf), before)

        def test_debug_dataframe_plan_fixed_table_and_timestamp_column(self):
            df = pd.DataFrame({
                'ts': pd.Series([
                    pd.Timestamp('2024-01-01 00:00:00'),
                    pd.Timestamp('2024-01-01 00:00:01')], dtype='datetime64[ns]'),
                'seq': pd.Series([1, 2], dtype='int64'),
                'price': pd.Series([10.5, 11.5], dtype='float64'),
            })

            plan = qi._debug_dataframe_plan(
                df, table_name='trades', at='ts', symbols=False)
            cols = {col['orig_name']: col for col in plan['cols']}

            self.assertEqual(plan['row_count'], 2)
            self.assertEqual(plan['col_count'], 3)
            self.assertEqual(plan['fixed_table_name'], 'trades')
            self.assertEqual(plan['at_value'], 'column')
            self.assertEqual(cols['seq']['target'], 'integer')
            self.assertEqual(cols['seq']['target_name'], 'seq')
            self.assertEqual(cols['price']['target'], 'float')
            self.assertEqual(cols['price']['target_name'], 'price')
            self.assertEqual(cols['ts']['target'], 'designated timestamp')
            self.assertIsNone(cols['ts']['target_name'])
            self.assertEqual(
                _dataframe(1, df, table_name='trades', at='ts', symbols=False),
                b'trades seq=1i,price=10.5 1704067200000000000\n'
                b'trades seq=2i,price=11.5 1704067201000000000\n')

        def test_debug_dataframe_plan_handles_zero_row_dataframe(self):
            df = pd.DataFrame({
                'ts': pd.Series([], dtype='datetime64[ns]'),
                'seq': pd.Series([], dtype='int64'),
            })

            row_plan = qi._debug_dataframe_plan(
                df, table_name='trades', at='ts')
            columnar_plan = qi._debug_dataframe_columnar_plan(
                df, table_name='trades', at='ts')

            self.assertEqual(row_plan['row_count'], 0)
            self.assertEqual(row_plan['col_count'], 0)
            self.assertEqual(row_plan['cols'], [])
            self.assertTrue(columnar_plan['supported'])
            self.assertEqual(columnar_plan['failures'], [])
            self.assertEqual(columnar_plan['normalizations'], [])

        def test_debug_dataframe_plan_table_column_and_auto_symbol(self):
            df = pd.DataFrame({
                'tbl': ['t1', 't2'],
                'sym': pd.Categorical(['a', 'b']),
                'value': pd.Series([1, 2], dtype='int64'),
                'ts': pd.Series([
                    pd.Timestamp('2024-01-01 00:00:00'),
                    pd.Timestamp('2024-01-01 00:00:01')], dtype='datetime64[ns]'),
            })

            plan = qi._debug_dataframe_plan(df, table_name_col='tbl', at='ts')
            cols = {col['orig_name']: col for col in plan['cols']}

            self.assertIsNone(plan['fixed_table_name'])
            self.assertEqual(plan['at_value'], 'column')
            self.assertEqual(cols['tbl']['target'], 'table name')
            self.assertIsNone(cols['tbl']['target_name'])
            self.assertEqual(cols['sym']['target'], 'symbol')
            self.assertEqual(cols['sym']['target_name'], 'sym')
            self.assertEqual(cols['value']['target'], 'integer')
            self.assertEqual(cols['value']['target_name'], 'value')
            self.assertEqual(cols['ts']['target'], 'designated timestamp')
            self.assertEqual(
                _dataframe(1, df, table_name_col='tbl', at='ts'),
                b't1,sym=a value=1i 1704067200000000000\n'
                b't2,sym=b value=2i 1704067201000000000\n')

        def test_debug_dataframe_plan_reuses_row_path_validation(self):
            df = pd.DataFrame({'a': [1]})
            with self.assertRaisesRegex(
                    qi.QuestDBError,
                    "`symbols`: Bad dtype `int64`.*'a'.*Must be a strings column."):
                qi._debug_dataframe_plan(
                    df,
                    table_name='tbl1',
                    symbols=['a'],
                    at=qi.ServerTimestamp)

        def test_debug_dataframe_columnar_plan_accepts_v1_numeric_core(self):
            df = pd.DataFrame({
                'ts': pd.Series([
                    pd.Timestamp('2024-01-01 00:00:00'),
                    pd.Timestamp('2024-01-01 00:00:01')], dtype='datetime64[ns]'),
                'seq': pd.Series([1, 2], dtype='int64'),
                'price': pd.Series([10.5, 11.5], dtype='float64'),
            })

            plan = qi._debug_dataframe_columnar_plan(
                df, table_name='trades', at='ts', symbols=False)

            self.assertTrue(plan['supported'])
            self.assertEqual(plan['failures'], [])

        def test_columnar_plan_populates_plain_arrow_uint32_as_integer(self):
            df = pd.DataFrame({
                'ts': pd.Series([
                    pd.Timestamp('2024-01-01 00:00:00'),
                    pd.Timestamp('2024-01-01 00:00:01')],
                    dtype='datetime64[ns]'),
                'seq': pd.Series(
                    pa.array([1, 4294967295], type=pa.uint32()),
                    dtype=pd.ArrowDtype(pa.uint32())),
            })

            plan = qi._debug_dataframe_columnar_plan(
                df, table_name='trades', at='ts', symbols=False)
            self.assertTrue(plan['supported'], plan['failures'])
            result = qi._bench_dataframe_plan_and_populate_column_chunks(
                df, table_name='trades', at='ts', symbols=False)

            self.assertEqual(result['populated_rows_total'], 2)
            self.assertEqual(result['row_path_cell_emissions'], 0)

        def test_columnar_plan_accepts_arrow_wide_numeric_sources(self):
            df = pd.DataFrame({
                'ts': pd.Series([
                    pd.Timestamp('2024-01-01 00:00:00'),
                    pd.Timestamp('2024-01-01 00:00:01'),
                    pd.Timestamp('2024-01-01 00:00:02')],
                    dtype='datetime64[ns]'),
                'arrow_i64': pd.Series(
                    pa.array([1, None, -3], type=pa.int64()),
                    dtype=pd.ArrowDtype(pa.int64())),
                'nullable_i64': pd.Series(
                    [4, pd.NA, -6], dtype=pd.Int64Dtype()),
                'arrow_f64': pd.Series(
                    pa.array([1.5, None, -3.25], type=pa.float64()),
                    dtype=pd.ArrowDtype(pa.float64())),
                'nullable_f64': pd.Series(
                    [4.5, pd.NA, -6.25], dtype=pd.Float64Dtype()),
            })

            plan = qi._debug_dataframe_columnar_plan(
                df, table_name='trades', at='ts', symbols=False)
            self.assertTrue(plan['supported'], plan['failures'])
            result = qi._bench_dataframe_plan_and_populate_column_chunks(
                df, table_name='trades', at='ts', symbols=False)

            self.assertEqual(result['populated_rows_total'], 3)
            self.assertEqual(result['row_path_cell_emissions'], 0)

        def test_debug_dataframe_columnar_plan_accepts_narrow_numpy_dtypes(self):
            # Step 3 broadened the columnar planner to accept every
            # narrower NumPy numeric dtype + native bool. The shapes
            # that were rejected pre-Step-3 (int8/16/32, uint*, float32,
            # bool) all flow through the Rust widening / packing
            # appender now.
            df = pd.DataFrame({
                'ts': pd.Series([
                    pd.Timestamp('2024-01-01 00:00:00')], dtype='datetime64[ns]'),
                'narrow_int': pd.Series([1], dtype='int32'),
                'narrow_float': pd.Series([1.5], dtype='float32'),
                'native_bool': pd.Series([True], dtype='bool'),
                'u8': pd.Series([200], dtype='uint8'),
            })

            plan = qi._debug_dataframe_columnar_plan(
                df, table_name='trades', at='ts', symbols=False)

            self.assertTrue(plan['supported'])
            self.assertEqual(plan['failures'], [])

        def test_debug_dataframe_columnar_plan_accepts_tz_aware_timestamps(self):
            # The columnar v1 planner was originally restricted to bare
            # numpy datetime64[ns/us] for both the designated `at` column
            # and `ts` field columns. The row path (Buffer.dataframe /
            # Sender.dataframe) accepted tz-aware DatetimeTZDtype and
            # pyarrow timestamp(unit, tz=...) all along; columnar v1
            # was tightened by accident. This test pins the symmetric
            # contract: every datetime variant the row path accepts
            # also passes the columnar planner.
            cases = [
                # 1. pd.to_datetime(['...Z']) infers DatetimeTZDtype.
                pd.to_datetime(
                    ['2024-01-01T00:00:00Z', '2024-01-01T00:00:01Z']),
                # 2. Explicit DatetimeTZDtype with a non-UTC zone.
                pd.Series(
                    [pd.Timestamp('2024-01-01 00:00:00',
                                  tz='America/New_York'),
                     pd.Timestamp('2024-01-01 00:00:01',
                                  tz='America/New_York')]),
                # 3. ArrowDtype timestamp[us, tz=...].
                pd.Series(
                    [1700000000000000, 1700000001000000],
                    dtype=pd.ArrowDtype(
                        pa.timestamp('us', tz='UTC'))),
            ]
            for idx, ts_series in enumerate(cases):
                with self.subTest(case=idx, dtype=str(ts_series.dtype)):
                    df = pd.DataFrame({
                        'ts': ts_series,
                        'lg': pd.Series([1, 2], dtype='int64'),
                    })
                    plan = qi._debug_dataframe_columnar_plan(
                        df, table_name='t', at='ts')
                    self.assertTrue(
                        plan['supported'],
                        f'case={idx} dtype={ts_series.dtype!r} '
                        f'failures={plan["failures"]!r}')

            # 4. tz-aware as a field column (non-`at`), with tz-naive at=.
            df = pd.DataFrame({
                'ts': pd.Series(
                    [pd.Timestamp('2024-01-01'),
                     pd.Timestamp('2024-01-02')], dtype='datetime64[ns]'),
                'event_ts': pd.to_datetime(
                    ['2024-01-01T00:00:00Z', '2024-01-01T00:00:01Z']),
                'lg': pd.Series([1, 2], dtype='int64'),
            })
            plan = qi._debug_dataframe_columnar_plan(
                df, table_name='t', at='ts')
            self.assertTrue(
                plan['supported'],
                f'tz-aware field column failures={plan["failures"]!r}')

        def test_debug_dataframe_columnar_plan_accepts_object_datetime_field(self):
            # Object-dtype datetime cells targeting a (non-designated)
            # timestamp field column are supported on the columnar path.
            df = pd.DataFrame({
                'ts': pd.Series([
                    pd.Timestamp('2024-01-01 00:00:00'),
                    pd.Timestamp('2024-01-01 00:00:01')],
                    dtype='datetime64[ns]'),
                't': pd.Series([
                    dt.datetime(2024, 1, 1, 12, 0, 0),
                    dt.datetime(2024, 1, 1, 12, 0, 1)], dtype=object),
                'v': pd.Series([1.0, 2.0], dtype='float64'),
            })

            plan = qi._debug_dataframe_columnar_plan(
                df, table_name='trades', at='ts')

            self.assertTrue(
                plan['supported'],
                f'object-datetime field column failures={plan["failures"]!r}')
            self.assertEqual(plan['failures'], [])

        def test_debug_dataframe_columnar_plan_rejects_unsupported_shape(self):
            df = pd.DataFrame({
                'tbl': ['t1'],
                'sym': pd.Series(['a'], dtype='object'),
                'value': pd.Series([1], dtype='int64'),
                'ts': pd.Series([pd.NaT], dtype='datetime64[ns]'),
            })

            plan = qi._debug_dataframe_columnar_plan(
                df, table_name_col='tbl', symbols=['sym'], at='ts')
            reasons = [failure['reason'] for failure in plan['failures']]

            self.assertFalse(plan['supported'])
            self.assertTrue(any('fixed table_name' in reason
                                for reason in reasons))
            self.assertTrue(any('Categorical or string[pyarrow]' in reason
                                for reason in reasons))
            self.assertTrue(any('cannot contain NaT' in reason
                                for reason in reasons))

        def test_debug_dataframe_columnar_plan_promotes_ns_nat_field(self):
            df = pd.DataFrame({
                'ts': pd.Series([
                    pd.Timestamp('2024-01-01 00:00:00'),
                    pd.Timestamp('2024-01-01 00:00:01')],
                    dtype='datetime64[us]'),
                'vts': pd.Series(
                    [pd.Timestamp('1960-01-01'), pd.NaT],
                    dtype='datetime64[ns]'),
            })

            plan = qi._debug_dataframe_columnar_plan(
                df, table_name='tbl1', at='ts')

            self.assertTrue(plan['supported'], plan['failures'])
            self.assertEqual(plan['failures'], [])

        def test_debug_dataframe_columnar_plan_accepts_v1_mixed_fast_paths(self):
            df = pd.DataFrame({
                'ts': pd.Series([
                    pd.Timestamp('2024-01-01 00:00:00'),
                    pd.Timestamp('2024-01-01 00:00:01'),
                    pd.Timestamp('2024-01-01 00:00:02')],
                    dtype='datetime64[ns]'),
                'event_ts': pd.Series([
                    pd.Timestamp('2024-01-02 00:00:00'),
                    pd.Timestamp('2024-01-02 00:00:01'),
                    pd.Timestamp('2024-01-02 00:00:02')],
                    dtype='datetime64[ns]'),
                'sym': pd.Categorical(['a', None, 'b']),
                'label': pd.Series(
                    pa.array(['alpha', None, 'gamma'], type=pa.string()),
                    dtype='string[pyarrow]'),
                'seq': pd.Series([1, 2, 3], dtype='int64'),
                'price': pd.Series([10.5, 11.5, 12.5], dtype='float64'),
            })

            plan = qi._debug_dataframe_columnar_plan(
                df, table_name='trades', at='ts')

            self.assertTrue(plan['supported'])
            self.assertEqual(plan['failures'], [])

        def test_debug_dataframe_columnar_plan_rejects_timestamp_only_frame(self):
            df = pd.DataFrame({
                'ts': pd.Series([
                    pd.Timestamp('2024-01-01 00:00:00'),
                    pd.Timestamp('2024-01-01 00:00:01')],
                    dtype='datetime64[ns]'),
            })

            plan = qi._debug_dataframe_columnar_plan(
                df, table_name='trades', at='ts')

            self.assertFalse(plan['supported'])
            self.assertEqual(
                [failure['reason'] for failure in plan['failures']],
                ['v1 requires at least one non-timestamp data column.'])
            with self.assertRaises(qi.UnsupportedDataFrameShapeError) as cm:
                qi._bench_dataframe_plan_and_populate_column_chunks(
                    df,
                    table_name='trades',
                    at='ts')
            self.assertEqual(
                cm.exception.column_failures,
                ({'column': None,
                  'target': None,
                  'source_code': None,
                  'reason': 'v1 requires at least one non-timestamp data column.'},))

        def test_debug_dataframe_columnar_plan_preserves_large_string(self):
            df = pd.DataFrame({
                'ts': pd.Series([
                    pd.Timestamp('2024-01-01 00:00:00'),
                    pd.Timestamp('2024-01-01 00:00:01')],
                    dtype='datetime64[ns]'),
                'label': pd.Series(
                    pa.array(['alpha', 'beta'], type=pa.large_string()),
                    dtype=pd.ArrowDtype(pa.large_string())),
                'seq': pd.Series([1, 2], dtype='int64'),
            })

            row_plan = qi._debug_dataframe_plan(
                df, table_name='trades', at='ts')
            label_col = next(
                col for col in row_plan['cols']
                if col['orig_name'] == 'label')
            self.assertEqual(label_col['source_code'], 406000)

            plan = qi._debug_dataframe_columnar_plan(
                df, table_name='trades', at='ts')

            self.assertTrue(plan['supported'])
            self.assertEqual(plan['failures'], [])
            self.assertEqual(plan['normalizations'], [])

        def test_arrow_backed_column_nonzero_offset_slice(self):
            # An Arrow-backed column whose underlying buffer has a non-zero
            # offset (a positional slice) must serialize the surviving rows
            # rather than walking off the chunk into the spare blank chunk.
            ser = pd.Series(
                pa.array(['a', 'b', 'c', 'd', 'e']),
                dtype=pd.ArrowDtype(pa.string()))
            df = pd.DataFrame({'s': ser}).iloc[2:]
            buf = _dataframe(
                self.version, df, table_name='t', at=qi.ServerTimestamp)
            self.assertEqual(buf.count(b'\n'), 3)
            for keep in (b'"c"', b'"d"', b'"e"'):
                self.assertIn(keep, buf)
            for drop in (b'"a"', b'"b"'):
                self.assertNotIn(drop, buf)

        def test_arrow_backed_column_empty_leading_chunk(self):
            # A zero-length leading chunk must be stepped over: the
            # values from the following chunk must serialize, not come
            # out as empty strings.
            ser = pd.concat([
                pd.Series([], dtype='string[pyarrow]'),
                pd.Series(['a', 'b'], dtype='string[pyarrow]')])
            self.assertEqual(ser.array.__arrow_array__().num_chunks, 2)
            df = pd.DataFrame({'x': ser})
            buf = _dataframe(
                self.version, df, table_name='t', at=qi.ServerTimestamp)
            self.assertEqual(buf, b't x="a"\nt x="b"\n')
            self.assertNotIn(b'x=""', buf)

        def test_arrow_backed_column_empty_mid_frame_chunk(self):
            ser = pd.concat([
                pd.Series(['a'], dtype='string[pyarrow]'),
                pd.Series([], dtype='string[pyarrow]'),
                pd.Series(['b'], dtype='string[pyarrow]')])
            self.assertEqual(ser.array.__arrow_array__().num_chunks, 3)
            df = pd.DataFrame({'x': ser})
            buf = _dataframe(
                self.version, df, table_name='t', at=qi.ServerTimestamp)
            self.assertEqual(buf, b't x="a"\nt x="b"\n')
            self.assertNotIn(b'x=""', buf)

        def test_debug_dataframe_columnar_plan_preserves_large_string_category(self):
            symbols = pd.Series(
                pa.array(
                    ['alpha', 'beta', None, 'alpha'],
                    type=pa.large_string()),
                dtype=pd.ArrowDtype(pa.large_string())).astype('category')
            df = pd.DataFrame({
                'ts': pd.Series([
                    pd.Timestamp('2024-01-01 00:00:00'),
                    pd.Timestamp('2024-01-01 00:00:01'),
                    pd.Timestamp('2024-01-01 00:00:02'),
                    pd.Timestamp('2024-01-01 00:00:03')],
                    dtype='datetime64[ns]'),
                'sym': symbols,
                'seq': pd.Series([1, 2, 3, 4], dtype='int64'),
            })

            row_plan = qi._debug_dataframe_plan(
                df, table_name='trades', at='ts')
            sym_col = next(
                col for col in row_plan['cols']
                if col['orig_name'] == 'sym')
            self.assertEqual(sym_col['source_code'], 403000)

            plan = qi._debug_dataframe_columnar_plan(
                df, table_name='trades', at='ts')

            self.assertTrue(plan['supported'])
            self.assertEqual(plan['failures'], [])
            self.assertEqual(plan['normalizations'], [])

        def test_bench_dataframe_plan_and_populate_column_chunks(self):
            df = pd.DataFrame({
                'ts': pd.Series([
                    pd.Timestamp('2024-01-01 00:00:00'),
                    pd.Timestamp('2024-01-01 00:00:01')], dtype='datetime64[ns]'),
                'seq': pd.Series([1, 2], dtype='int64'),
                'price': pd.Series([10.5, 11.5], dtype='float64'),
            })

            result = qi._bench_dataframe_plan_and_populate_column_chunks(
                df,
                table_name='trades',
                at='ts',
                symbols=False,
                iterations=3)

            self.assertEqual(result['iterations'], 3)
            self.assertEqual(result['row_count'], 2)
            self.assertEqual(result['col_count'], 3)
            self.assertEqual(result['logical_cells'], 6)
            self.assertEqual(result['populated_chunks'], 3)
            self.assertEqual(result['last_populated_rows'], 2)
            self.assertEqual(result['row_path_cell_emissions'], 0)

        def test_bench_dataframe_plan_and_populate_splits_chunks(self):
            df = pd.DataFrame({
                'ts': pd.Series([
                    pd.Timestamp('2024-01-01 00:00:00'),
                    pd.Timestamp('2024-01-01 00:00:01'),
                    pd.Timestamp('2024-01-01 00:00:02')], dtype='datetime64[ns]'),
                'seq': pd.Series([1, 2, 3], dtype='int64'),
            })

            result = qi._bench_dataframe_plan_and_populate_column_chunks(
                df,
                table_name='trades',
                at='ts',
                symbols=False,
                iterations=2,
                max_rows_per_chunk=2)

            self.assertEqual(result['rows_per_chunk'], 2)
            self.assertEqual(result['populated_chunks'], 4)
            self.assertEqual(result['populated_rows_total'], 6)
            self.assertEqual(result['last_populated_rows'], 1)
            self.assertEqual(result['row_path_cell_emissions'], 0)

        def test_bench_dataframe_plan_and_populate_aligns_nullable_chunks(self):
            df = pd.DataFrame({
                'ts': pd.Series(
                    pd.date_range('2024-01-01', periods=10, freq='s'),
                    dtype='datetime64[ns]'),
                'sym': pd.Categorical(
                    ['a', None, 'b', 'c', None, 'a', 'b', 'c', 'a', None]),
                'seq': pd.Series(range(10), dtype='int64'),
            })

            result = qi._bench_dataframe_plan_and_populate_column_chunks(
                df,
                table_name='trades',
                at='ts',
                iterations=1,
                max_rows_per_chunk=3)

            self.assertEqual(result['rows_per_chunk'], 8)
            self.assertEqual(result['populated_chunks'], 2)
            self.assertEqual(result['populated_rows_total'], 10)
            self.assertEqual(result['last_populated_rows'], 2)
            self.assertEqual(result['row_path_cell_emissions'], 0)

        def test_bench_dataframe_plan_reuses_arrow_import_across_three_chunks(self):
            labels = [
                'alpha', None, 'beta', 'gamma',
                None, 'delta', 'epsilon', 'zeta',
                'eta', None, 'theta', 'iota',
                'kappa', 'lambda', None, 'mu',
                'nu', 'xi', None, 'omicron',
            ]
            df = pd.DataFrame({
                'ts': pd.Series(
                    pd.date_range('2024-01-01', periods=20, freq='s'),
                    dtype='datetime64[ns]'),
                'sym': pd.Categorical(labels),
                'label': pd.Series(
                    pa.array(labels, type=pa.string()),
                    dtype='string[pyarrow]'),
                'seq': pd.Series(range(20), dtype='int64'),
            })

            result = qi._bench_dataframe_plan_and_populate_column_chunks(
                df,
                table_name='trades',
                at='ts',
                iterations=1,
                max_rows_per_chunk=3)

            self.assertEqual(result['rows_per_chunk'], 8)
            self.assertEqual(result['populated_chunks'], 3)
            self.assertEqual(result['populated_rows_total'], 20)
            self.assertEqual(result['last_populated_rows'], 4)
            self.assertEqual(result['row_path_cell_emissions'], 0)

        def test_bench_dataframe_plan_and_populate_aligns_pyobj_chunks(self):
            # Regression: PyObject-sourced columns can carry nulls (or
            # always be bitmaps in the bool_pyobj case). The chunk-size
            # planner must align to 8 even though the wrapping ArrowArray
            # has null_count=0 (the pyobj wrapper hardcodes this).
            #
            # Without the fix, max_rows_per_chunk=3 would survive as 3
            # and the second chunk's row_offset=3 would trip the
            # byte-aligned-offset check in the emit branch.
            df = pd.DataFrame({
                'ts': pd.Series(
                    pd.date_range('2024-01-01', periods=10, freq='s'),
                    dtype='datetime64[ns]'),
                'obj_str': pd.Series(
                    ['a', None, 'b', 'c', None, 'a', 'b', 'c', 'a', None],
                    dtype='object'),
                'seq': pd.Series(range(10), dtype='int64'),
            })

            result = qi._bench_dataframe_plan_and_populate_column_chunks(
                df,
                table_name='trades',
                at='ts',
                iterations=1,
                max_rows_per_chunk=3)

            self.assertEqual(result['rows_per_chunk'], 8)
            self.assertEqual(result['populated_chunks'], 2)
            self.assertEqual(result['populated_rows_total'], 10)
            self.assertEqual(result['row_path_cell_emissions'], 0)

        def test_bench_dataframe_plan_and_populate_aligns_bool_pyobj_chunks(self):
            # bool_pyobj always builds a bitmap of values; the emit
            # offsets by row_offset // 8 regardless of nulls, so the
            # planner must require 8-row alignment for this source too.
            df = pd.DataFrame({
                'ts': pd.Series(
                    pd.date_range('2024-01-01', periods=10, freq='s'),
                    dtype='datetime64[ns]'),
                'flag': pd.Series(
                    [True, False, True, True, False] * 2,
                    dtype='object'),
                'seq': pd.Series(range(10), dtype='int64'),
            })

            result = qi._bench_dataframe_plan_and_populate_column_chunks(
                df,
                table_name='trades',
                at='ts',
                iterations=1,
                max_rows_per_chunk=5)

            self.assertEqual(result['rows_per_chunk'], 8)
            self.assertEqual(result['populated_chunks'], 2)
            self.assertEqual(result['populated_rows_total'], 10)
            self.assertEqual(result['row_path_cell_emissions'], 0)

        def test_bench_dataframe_plan_and_populate_binary_pyobj(self):
            # Regression: a pandas `bytes`/object column forces the manual
            # columnar planner, which previously rejected the binary target
            # even though the build/populate path fully supports it.
            df = pd.DataFrame({
                'ts': pd.Series(
                    pd.date_range('2024-01-01', periods=4, freq='s'),
                    dtype='datetime64[ns]'),
                'blob': pd.Series(
                    [b'hello', b'', b'\x00\x01\x02', None],
                    dtype='object'),
                'seq': pd.Series(range(4), dtype='int64'),
            })

            result = qi._bench_dataframe_plan_and_populate_column_chunks(
                df,
                table_name='trades',
                at='ts',
                iterations=1,
                max_rows_per_chunk=16384)

            self.assertEqual(result['populated_rows_total'], 4)
            self.assertEqual(result['row_path_cell_emissions'], 0)

        def test_bench_dataframe_plan_and_populate_rejects_unsupported_shape(self):
            # Step 3 made bool/int32/etc. supported. Pick a shape that
            # remains rejected: NaT in the designated timestamp.
            df = pd.DataFrame({
                'ts': pd.Series([pd.NaT], dtype='datetime64[ns]'),
                'seq': pd.Series([1], dtype='int64'),
            })

            with self.assertRaisesRegex(
                    qi.UnsupportedDataFrameShapeError,
                    'DataFrame is not supported'):
                qi._bench_dataframe_plan_and_populate_column_chunks(
                    df,
                    table_name='trades',
                    at='ts',
                    symbols=False)

        def test_bench_dataframe_plan_and_populate_mixed_fast_paths(self):
            df = pd.DataFrame({
                'ts': pd.Series([
                    pd.Timestamp('2024-01-01 00:00:00'),
                    pd.Timestamp('2024-01-01 00:00:01'),
                    pd.Timestamp('2024-01-01 00:00:02')],
                    dtype='datetime64[ns]'),
                'event_ts': pd.Series([
                    pd.Timestamp('2024-01-02 00:00:00'),
                    pd.Timestamp('2024-01-02 00:00:01'),
                    pd.Timestamp('2024-01-02 00:00:02')],
                    dtype='datetime64[ns]'),
                'sym': pd.Categorical(['a', None, 'b']),
                'label': pd.Series(
                    pa.array(['alpha', None, 'gamma'], type=pa.string()),
                    dtype='string[pyarrow]'),
                'seq': pd.Series([1, 2, 3], dtype='int64'),
                'price': pd.Series([10.5, 11.5, 12.5], dtype='float64'),
            })

            result = qi._bench_dataframe_plan_and_populate_column_chunks(
                df,
                table_name='trades',
                at='ts',
                iterations=2)

            self.assertEqual(result['iterations'], 2)
            self.assertEqual(result['row_count'], 3)
            self.assertEqual(result['col_count'], 6)
            self.assertEqual(result['populated_chunks'], 2)
            self.assertEqual(result['last_populated_rows'], 3)
            self.assertEqual(result['row_path_cell_emissions'], 0)

        def test_u8_numpy_col(self):
            df = pd.DataFrame({'a': pd.Series([
                    1, 2, 3,
                    0,
                    255],  # u8 max
                dtype='uint8')})
            buf = _dataframe(self.version, df, table_name='tbl1', at=qi.ServerTimestamp)
            self.assertEqual(
                buf,
                b'tbl1 a=1i\n' +
                b'tbl1 a=2i\n' +
                b'tbl1 a=3i\n' +
                b'tbl1 a=0i\n' +
                b'tbl1 a=255i\n')

        def test_i8_numpy_col(self):
            df = pd.DataFrame({'a': pd.Series([
                    1, 2, 3,
                    -128,  # i8 min
                    127,   # i8 max
                    0], dtype='int8')})
            buf = _dataframe(self.version, df, table_name='tbl1', at=qi.ServerTimestamp)
            self.assertEqual(
                buf,
                b'tbl1 a=1i\n' +
                b'tbl1 a=2i\n' +
                b'tbl1 a=3i\n' +
                b'tbl1 a=-128i\n' +
                b'tbl1 a=127i\n' +
                b'tbl1 a=0i\n')

        def test_u16_numpy_col(self):
            df = pd.DataFrame({'a': pd.Series([
                    1, 2, 3,
                    0,
                    65535],  # u16 max
                dtype='uint16')})
            buf = _dataframe(self.version, df, table_name='tbl1', at=qi.ServerTimestamp)
            self.assertEqual(
                buf,
                b'tbl1 a=1i\n' +
                b'tbl1 a=2i\n' +
                b'tbl1 a=3i\n' +
                b'tbl1 a=0i\n' +
                b'tbl1 a=65535i\n')

        def test_i16_numpy_col(self):
            df = pd.DataFrame({'a': pd.Series([
                    1, 2, 3,
                    -32768,  # i16 min
                    32767,   # i16 max
                    0], dtype='int16')})
            buf = _dataframe(self.version, df, table_name='tbl1', at=qi.ServerTimestamp)
            self.assertEqual(
                buf,
                b'tbl1 a=1i\n' +
                b'tbl1 a=2i\n' +
                b'tbl1 a=3i\n' +
                b'tbl1 a=-32768i\n' +
                b'tbl1 a=32767i\n' +
                b'tbl1 a=0i\n')

        def test_u32_numpy_col(self):
            df = pd.DataFrame({'a': pd.Series([
                    1, 2, 3,
                    0,
                    4294967295],  # u32 max
                dtype='uint32')})
            buf = _dataframe(self.version, df, table_name='tbl1', at=qi.ServerTimestamp)
            self.assertEqual(
                buf,
                b'tbl1 a=1i\n' +
                b'tbl1 a=2i\n' +
                b'tbl1 a=3i\n' +
                b'tbl1 a=0i\n' +
                b'tbl1 a=4294967295i\n')

        def test_i32_numpy_col(self):
            df = pd.DataFrame({'a': pd.Series([
                    1, 2, 3,
                    -2147483648,  # i32 min
                    0,
                    2147483647],  # i32 max
                dtype='int32')})
            buf = _dataframe(self.version, df, table_name='tbl1', at=qi.ServerTimestamp)
            self.assertEqual(
                buf,
                b'tbl1 a=1i\n' +
                b'tbl1 a=2i\n' +
                b'tbl1 a=3i\n' +
                b'tbl1 a=-2147483648i\n' +
                b'tbl1 a=0i\n' +
                b'tbl1 a=2147483647i\n')

        def test_u64_numpy_col(self):
            df = pd.DataFrame({'a': pd.Series([
                    1, 2, 3,
                    0,
                    9223372036854775807],  # i64 max
                dtype='uint64')})
            buf = _dataframe(self.version, df, table_name='tbl1', at=qi.ServerTimestamp)
            self.assertEqual(
                buf,
                b'tbl1 a=1i\n' +
                b'tbl1 a=2i\n' +
                b'tbl1 a=3i\n' +
                b'tbl1 a=0i\n' +
                b'tbl1 a=9223372036854775807i\n')

            buf = qi.Buffer(protocol_version=self.version)
            buf.dataframe(pd.DataFrame({'b': [.5, 1.0, 1.5]}), table_name='tbl2', at=qi.ServerTimestamp)
            exp1 = (
                b'tbl2 b' + _float_binary_bytes(0.5, self.version == 1) + b'\n' +
                b'tbl2 b' + _float_binary_bytes(1.0, self.version == 1) + b'\n' +
                b'tbl2 b' + _float_binary_bytes(1.5, self.version == 1) + b'\n')
            self.assertEqual(
                bytes(buf),
                exp1)
            df2 = pd.DataFrame({'a': pd.Series([
                    1, 2, 3,
                    0,
                    9223372036854775808],  # i64 max + 1
                dtype='uint64')})
            with self.assertRaisesRegex(
                    qi.QuestDBError,
                    '.* serialize .* column .a. .* 4 .*9223372036854775808.*int64.*'):
                buf.dataframe(df2, table_name='tbl1', at=qi.ServerTimestamp)

            self.assertEqual(
                bytes(buf),
                exp1)  # No partial write of `df2`.

        def test_i64_numpy_col(self):
            df = pd.DataFrame({'a': pd.Series([
                    1, 2, 3,
                    -9223372036854775808,  # i64 min
                    0,
                    9223372036854775807],  # i64 max
                dtype='int64')})
            buf = _dataframe(self.version, df, table_name='tbl1', at=qi.ServerTimestamp)
            self.assertEqual(
                buf,
                b'tbl1 a=1i\n' +
                b'tbl1 a=2i\n' +
                b'tbl1 a=3i\n' +
                b'tbl1 a=-9223372036854775808i\n' +
                b'tbl1 a=0i\n' +
                b'tbl1 a=9223372036854775807i\n')

        def test_f32_numpy_col(self):
            df = pd.DataFrame({'a': pd.Series([
                    1.0, 2.0, 3.0,
                    0.0,
                    float('inf'),
                    float('-inf'),
                    float('nan'),
                    3.4028234663852886e38],  # f32 max
                dtype='float32')})
            buf = _dataframe(self.version, df, table_name='tbl1', at=qi.ServerTimestamp)
            self.assertEqual(
                buf,
                b'tbl1 a' + _float_binary_bytes(1.0, self.version == 1) + b'\n' +
                b'tbl1 a' + _float_binary_bytes(2.0, self.version == 1) + b'\n' +
                b'tbl1 a' + _float_binary_bytes(3.0, self.version == 1) + b'\n' +
                b'tbl1 a' + _float_binary_bytes(0.0, self.version == 1) + b'\n' +
                b'tbl1 a' + _float_binary_bytes(float('inf'), self.version == 1) + b'\n' +
                b'tbl1 a' + _float_binary_bytes(float('-inf'), self.version == 1) + b'\n' +
                b'tbl1 a' + _float_binary_bytes(float('NaN'), self.version == 1) + b'\n' +
                b'tbl1 a' + _float_binary_bytes(3.4028234663852886e38, self.version == 1) + b'\n')

        def test_f64_numpy_col(self):
            df = pd.DataFrame({'a': pd.Series([
                    1.0, 2.0, 3.0,
                    0.0,
                    float('inf'),
                    float('-inf'),
                    float('nan'),
                    1.7976931348623157e308],  # f64 max
                dtype='float64')})
            buf = _dataframe(self.version, df, table_name='tbl1', at=qi.ServerTimestamp)
            self.assertEqual(
                buf,
                b'tbl1 a' + _float_binary_bytes(1.0, self.version == 1) + b'\n' +
                b'tbl1 a' + _float_binary_bytes(2.0, self.version == 1) + b'\n' +
                b'tbl1 a' + _float_binary_bytes(3.0, self.version == 1) + b'\n' +
                b'tbl1 a' + _float_binary_bytes(0.0, self.version == 1) + b'\n' +
                b'tbl1 a' + _float_binary_bytes(float('inf'), self.version == 1) + b'\n' +
                b'tbl1 a' + _float_binary_bytes(float('-inf'), self.version == 1) + b'\n' +
                b'tbl1 a' + _float_binary_bytes(float('NAN'), self.version == 1) + b'\n' +
                b'tbl1 a' + _float_binary_bytes(1.7976931348623157e308, self.version == 1) + b'\n')

        def test_datetime_pyobj_column_matches_numpy(self):
            ts = dt.datetime(2021, 1, 1, 12, 0, 0, 123456)
            obj_df = pd.DataFrame({'ts': pd.Series([ts], dtype=object)})
            np_df = pd.DataFrame(
                {'ts': pd.Series([ts]).astype('datetime64[us]')})
            obj_buf = _dataframe(
                self.version, obj_df, table_name='tbl', at=qi.ServerTimestamp)
            np_buf = _dataframe(
                self.version, np_df, table_name='tbl', at=qi.ServerTimestamp)
            self.assertNotEqual(obj_buf, b'')
            self.assertEqual(obj_buf, np_buf)

        def test_datetime_pyobj_column_with_null_matches_numpy(self):
            ts = dt.datetime(2021, 1, 1, 12, 0, 0)
            obj_df = pd.DataFrame({
                'sym': pd.Categorical(['a', 'b']),
                'ts': pd.Series([ts, None], dtype=object)})
            np_df = pd.DataFrame({
                'sym': pd.Categorical(['a', 'b']),
                'ts': pd.Series([ts, pd.NaT]).astype('datetime64[us]')})
            self.assertEqual(
                _dataframe(
                    self.version, obj_df, table_name='tbl',
                    at=qi.ServerTimestamp),
                _dataframe(
                    self.version, np_df, table_name='tbl',
                    at=qi.ServerTimestamp))

        def test_datetime_pyobj_column_with_nat_is_null(self):
            ts = dt.datetime(2021, 1, 1, 12, 0, 0)
            obj_df = pd.DataFrame({
                'sym': pd.Categorical(['a', 'b', 'c']),
                'ts': pd.Series([ts, pd.NaT, None], dtype=object)})
            np_df = pd.DataFrame({
                'sym': pd.Categorical(['a', 'b', 'c']),
                'ts': pd.Series([ts, pd.NaT, pd.NaT]).astype('datetime64[us]')})
            obj_buf = _dataframe(
                self.version, obj_df, table_name='tbl', at=qi.ServerTimestamp)
            self.assertNotIn(b'0001-01-01', obj_buf)
            self.assertEqual(
                obj_buf,
                _dataframe(
                    self.version, np_df, table_name='tbl',
                    at=qi.ServerTimestamp))

        def test_datetime_pyobj_column_all_nat_is_null(self):
            obj_df = pd.DataFrame({
                'sym': pd.Categorical(['a', 'b']),
                'ts': pd.Series([pd.NaT, pd.NaT], dtype=object)})
            buf = _dataframe(
                self.version, obj_df, table_name='tbl', at=qi.ServerTimestamp)
            self.assertNotIn(b'ts=', buf)
            self.assertNotIn(b'0001-01-01', buf)

        def test_decimal_pyobj_column(self):
            decimals = [
                Decimal('123.45'),
                Decimal('-0.5'),
                Decimal('0'),
                Decimal('57896044618658097711785492504343953926634992332820282019728792003956564819967'), # Maximum value: 2²⁵⁵-1
                Decimal('-57896044618658097711785492504343953926634992332820282019728792003956564819968'), # Minimum value: -2²⁵⁵
                Decimal('170141183460469231731687303715884105727'), # 2¹²⁷-1
                Decimal('-170141183460469231731687303715884105728'), # -2¹²⁷
                Decimal('9223372036854775807'), # 2⁶³-1
                Decimal('-9223372036854775808'), # -2⁶³
                Decimal('2147483647'), # 2³¹-1
                Decimal('-2147483648'), # -2³¹
            ]
            if self.version < 3:
                with self.assertRaisesRegex(
                        qi.QuestDBError,
                        'does not support the decimal datatype'):
                    _dataframe(self.version, pd.DataFrame({'dec': [Decimal('123')]}), table_name='tbl', at=qi.ServerTimestamp)
                return
            for decimal in decimals:
                df = pd.DataFrame({'dec': [decimal]})
                try:
                    buf = _dataframe(self.version, df, table_name='tbl', at=qi.ServerTimestamp)
                    (scale, mantissa) = _decode_decimal_payload(buf.splitlines()[0])
                    unscaled = int.from_bytes(mantissa, byteorder='big', signed=True)

                    (expected_scale, expected_unscaled) = _unwrap_decimal(decimal)

                    self.assertEqual(scale, expected_scale)
                    self.assertEqual(unscaled, expected_unscaled)
                except Exception as ex:
                    self.fail(f'Failed to serialize {decimal}: {ex}')

        def test_decimal_pyobj_null_cell(self):
            if self.version < 3:
                self.skipTest('decimal datatype requires ILP version 3 or later')
            df = pd.DataFrame({
                'dec': pd.Series([Decimal('1.5'), None], dtype=object),
                'other': [1, 2]})
            lines = _dataframe(
                self.version, df, table_name='tbl',
                at=qi.ServerTimestamp).splitlines()
            self.assertEqual(len(lines), 2)
            self.assertIn(b'dec=', lines[0])
            self.assertIn(b'other=1i', lines[0])
            self.assertNotIn(b'dec=', lines[1])
            self.assertIn(b'other=2i', lines[1])

        def test_decimal_pyobj_mixed_cell_types(self):
            if self.version < 3:
                self.skipTest('decimal datatype requires ILP version 3 or later')
            for bad_cell in ('oops', 3.0, [1]):
                with self.subTest(bad_cell=bad_cell):
                    df = pd.DataFrame({
                        'dec': pd.Series(
                            [Decimal('1.5'), bad_cell], dtype=object)})
                    with self.assertRaisesRegex(
                            qi.QuestDBError,
                            'Expected an object of type Decimal') as raised:
                        _dataframe(
                            self.version, df, table_name='tbl',
                            at=qi.ServerTimestamp)
                    self.assertIs(
                        raised.exception.code,
                        qi.QuestDBErrorCode.BadDataFrame)
                    self.assertIn('row index 1', str(raised.exception))

        def test_decimal_pyobj_trailing_zeros_and_integer(self):
            if self.version < 3:
                self.skipTest('decimal datatype requires ILP version 3 or later')
            df = pd.DataFrame({'dec': [Decimal('1.2300'), Decimal('1000')]})
            buf = _dataframe(self.version, df, table_name='tbl', at=qi.ServerTimestamp)
            decoded = [_decode_decimal_payload(line) for line in buf.splitlines()]
            expected = [Decimal('1.23'), Decimal('1000')]
            self.assertEqual(len(decoded), len(expected))
            for (scale, mantissa), expected_value in zip(decoded, expected):
                unscaled = int.from_bytes(mantissa, byteorder='big', signed=True)
                self.assertEqual(Decimal(unscaled).scaleb(-scale), expected_value)

        def test_decimal_pyobj_special_values(self):
            if self.version < 3:
                self.skipTest('decimal datatype requires ILP version 3 or later')
            df = pd.DataFrame({'dec': [Decimal('NaN'), Decimal('Infinity'), Decimal('-Infinity')]})
            try:
                _dataframe(self.version, df, table_name='tbl', at=qi.ServerTimestamp)
                self.fail("special values shouldn't be encoded")
            except qi.QuestDBError:
                pass

        def test_decimal_pyobj_overflow(self):
            if self.version < 3:
                self.skipTest('decimal datatype requires ILP version 3 or later')
            df = pd.DataFrame({'dec': [Decimal('57896044618658097711785492504343953926634992332820282019728792003956564819968')]})

            with self.assertRaisesRegex(
                    qi.QuestDBError,
                    '.*Decimal mantissa too large; maximum supported size is 32 bytes.*'):
                _dataframe(self.version, df, table_name='tbl', at=qi.ServerTimestamp)

        def test_decimal_pyobj_scale_too_big(self):
            if self.version < 3:
                self.skipTest('decimal datatype requires ILP version 3 or later')
            df = pd.DataFrame({'dec': [Decimal('1.2e-100')]})

            with self.assertRaisesRegex(
                    qi.QuestDBError,
                    '.*exceeds the maximum supported scale of 76.*'):
                _dataframe(self.version, df, table_name='tbl', at=qi.ServerTimestamp)

        def test_decimal_pyobj_positive_exponent(self):
            if self.version < 3:
                self.skipTest('decimal datatype requires ILP version 3 or later')
            # A representable positive exponent expands into the unscaled
            # mantissa with scale 0.
            df = pd.DataFrame({'dec': [Decimal('1E+20')]})
            buf = _dataframe(self.version, df, table_name='tbl', at=qi.ServerTimestamp)
            (scale, mantissa) = _decode_decimal_payload(buf.splitlines()[0])
            self.assertEqual(scale, 0)
            self.assertEqual(
                int.from_bytes(mantissa, byteorder='big', signed=True),
                10 ** 20)

            # An out-of-range exponent must raise cleanly instead of
            # being truncated to a bogus narrower value.
            df = pd.DataFrame({'dec': [Decimal('1E+100')]})
            with self.assertRaisesRegex(
                    qi.QuestDBError,
                    '.*Decimal exponent 100 exceeds the maximum supported'
                    ' value of 76.*'):
                _dataframe(self.version, df, table_name='tbl', at=qi.ServerTimestamp)

        def test_decimal_arrow_columns(self):
            if self.version < 3:
                arr = pd.array(
                    [Decimal('1.23')],
                    dtype=pd.ArrowDtype(pa.decimal128(10, 2)))
                df = pd.DataFrame({'dec': arr, 'count': [0]})
                with self.assertRaisesRegex(
                        qi.QuestDBError,
                        'does not support the decimal datatype'):
                    _dataframe(self.version, df, table_name='tbl', at=qi.ServerTimestamp)
                return

            arrow_cases = [
                (pa.decimal32(7, 2), [12345, -6789]),
                (pa.decimal64(14, 4), [123456789, -987654321]),
                (pa.decimal128(38, 6), [123456789012345, -987654321012345, None]),
                (pa.decimal256(76, 10), [1234567890123456789012345, -987654321098765432109876, None]),
            ]

            for arrow_type, unscaled_values in arrow_cases:
                values = [_decimal_from_unscaled(unscaled, arrow_type.scale) for unscaled in unscaled_values]
                arr = pd.array(values, dtype=pd.ArrowDtype(arrow_type))
                counts = list(range(len(values)))
                df = pd.DataFrame({'dec': arr, 'count': counts})
                buf = _dataframe(self.version, df, table_name='tbl', at=qi.ServerTimestamp)
                offset = 0
                prefix = b'tbl dec='
                for unscaled, count in zip(unscaled_values, counts):
                    suffix = f',count={count}i\n'.encode('ascii')
                    if unscaled is None:
                        # If the decimal is invalid, we shouldn't have encoded it
                        try:
                            buf.index(suffix, offset)
                            self.fail("There shouldn't be any other fields")
                        except ValueError:
                            continue
                
                    end = buf.index(suffix, offset)
                    line = buf[offset:end + len(suffix)]
                    self.assertTrue(line.startswith(prefix), line)
                    payload = line[len(prefix):len(line) - len(suffix)] if len(suffix) else line[len(prefix):]
                    expected_payload = _decimal_binary_payload(unscaled, arrow_type.scale, arrow_type.byte_width)
                    self.assertEqual(payload, expected_payload)
                    offset = end + len(suffix)

        def test_u8_arrow_col(self):
            df = pd.DataFrame({
                'a': pd.Series([
                        1, 2, 3,
                        0,
                        None,
                        255],  # u8 max
                    dtype=pd.UInt8Dtype()),
                'b': ['a', 'b', 'c', 'd', 'e', 'f']})
            buf = _dataframe(self.version, df, table_name='tbl1', at=qi.ServerTimestamp)
            self.assertEqual(
                buf,
                b'tbl1 a=1i,b="a"\n' +
                b'tbl1 a=2i,b="b"\n' +
                b'tbl1 a=3i,b="c"\n' +
                b'tbl1 a=0i,b="d"\n' +
                b'tbl1 b="e"\n' +
                b'tbl1 a=255i,b="f"\n')

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
            buf = _dataframe(self.version, df, table_name='tbl1', at=qi.ServerTimestamp)
            self.assertEqual(
                buf,
                b'tbl1 a=1i,b="a"\n' +
                b'tbl1 a=2i,b="b"\n' +
                b'tbl1 a=3i,b="c"\n' +
                b'tbl1 a=-128i,b="d"\n' +
                b'tbl1 a=0i,b="e"\n' +
                b'tbl1 b="f"\n' +
                b'tbl1 a=127i,b="g"\n')

        def test_u16_arrow_col(self):
            df = pd.DataFrame({
                'a': pd.Series([
                        1, 2, 3,
                        0,
                        None,
                        65535],  # u16 max
                    dtype=pd.UInt16Dtype()),
                'b': ['a', 'b', 'c', 'd', 'e', 'f']})
            buf = _dataframe(self.version, df, table_name='tbl1', at=qi.ServerTimestamp)
            self.assertEqual(
                buf,
                ('tbl1 a=1i,b="a"\n' +
                'tbl1 a=2i,b="b"\n' +
                'tbl1 a=3i,b="c"\n' +
                'tbl1 a=0i,b="d"\n' +
                'tbl1 b="e"\n' +
                'tbl1 a=65535i,b="f"\n').encode('utf-8'))

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
            buf = _dataframe(self.version, df, table_name='tbl1', at=qi.ServerTimestamp)
            self.assertEqual(
                buf,
                b'tbl1 a=1i,b="a"\n' +
                b'tbl1 a=2i,b="b"\n' +
                b'tbl1 a=3i,b="c"\n' +
                b'tbl1 a=-32768i,b="d"\n' +
                b'tbl1 a=0i,b="e"\n' +
                b'tbl1 b="f"\n' +
                b'tbl1 a=32767i,b="g"\n')

        def test_u32_arrow_col(self):
            df = pd.DataFrame({
                'a': pd.Series([
                        1, 2, 3,
                        0,
                        None,
                        4294967295],  # u32 max
                    dtype=pd.UInt32Dtype()),
                'b': ['a', 'b', 'c', 'd', 'e', 'f']})
            buf = _dataframe(self.version, df, table_name='tbl1', at=qi.ServerTimestamp)
            self.assertEqual(
                buf,
                b'tbl1 a=1i,b="a"\n' +
                b'tbl1 a=2i,b="b"\n' +
                b'tbl1 a=3i,b="c"\n' +
                b'tbl1 a=0i,b="d"\n' +
                b'tbl1 b="e"\n' +
                b'tbl1 a=4294967295i,b="f"\n')

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
            buf = _dataframe(self.version, df, table_name='tbl1', at=qi.ServerTimestamp)
            self.assertEqual(
                buf,
                b'tbl1 a=1i,b="a"\n' +
                b'tbl1 a=2i,b="b"\n' +
                b'tbl1 a=3i,b="c"\n' +
                b'tbl1 a=-2147483648i,b="d"\n' +
                b'tbl1 a=0i,b="e"\n' +
                b'tbl1 b="f"\n' +
                b'tbl1 a=2147483647i,b="g"\n')

        def test_u64_arrow_col(self):
            df = pd.DataFrame({
                'a': pd.Series([
                        1, 2, 3,
                        0,
                        None,
                        9223372036854775807],  # i64 max
                    dtype=pd.UInt64Dtype()),
                'b': ['a', 'b', 'c', 'd', 'e', 'f']})
            buf = _dataframe(self.version, df, table_name='tbl1', at=qi.ServerTimestamp)
            self.assertEqual(
                buf,
                b'tbl1 a=1i,b="a"\n' +
                b'tbl1 a=2i,b="b"\n' +
                b'tbl1 a=3i,b="c"\n' +
                b'tbl1 a=0i,b="d"\n' +
                b'tbl1 b="e"\n' +
                b'tbl1 a=9223372036854775807i,b="f"\n')

            df2 = pd.DataFrame({'a': pd.Series([
                    1, 2, 3,
                    0,
                    9223372036854775808],  # i64 max + 1
                dtype=pd.UInt64Dtype())})
            with self.assertRaisesRegex(
                    qi.QuestDBError,
                    '.* serialize .* column .a. .* 4 .*9223372036854775808.*int64.*'):
                _dataframe(self.version, df2, table_name='tbl1', at=qi.ServerTimestamp)

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
            buf = _dataframe(self.version, df, table_name='tbl1', at=qi.ServerTimestamp)
            self.assertEqual(
                buf,
                b'tbl1 a=1i,b="a"\n' +
                b'tbl1 a=2i,b="b"\n' +
                b'tbl1 a=3i,b="c"\n' +
                b'tbl1 a=-9223372036854775808i,b="d"\n' +
                b'tbl1 a=0i,b="e"\n' +
                b'tbl1 b="f"\n' +
                b'tbl1 a=9223372036854775807i,b="g"\n')

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
            buf = _dataframe(self.version, df, table_name='tbl1', at=qi.ServerTimestamp)
            self.assertEqual(
                buf,
                b'tbl1 a' + _float_binary_bytes(1.0, self.version == 1) + b',b="a"\n' +
                b'tbl1 a' + _float_binary_bytes(2.0, self.version == 1) + b',b="b"\n' +
                b'tbl1 a' + _float_binary_bytes(3.0, self.version == 1) + b',b="c"\n' +
                b'tbl1 a' + _float_binary_bytes(0.0, self.version == 1) + b',b="d"\n' +
                b'tbl1 a' + _float_binary_bytes(float('inf'), self.version == 1) + b',b="e"\n' +
                b'tbl1 a' + _float_binary_bytes(float('-inf'), self.version == 1) + b',b="f"\n' +
                b'tbl1 b="g"\n' +  # This one is wierd: `nan` gets 0 in the bitmask.
                b'tbl1 a' + _float_binary_bytes(3.4028234663852886e38, self.version == 1) + b',b="h"\n' +
                b'tbl1 b="i"\n')

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
            buf = _dataframe(self.version, df, table_name='tbl1', at=qi.ServerTimestamp)
            self.assertEqual(
                buf,
                b'tbl1 a' + _float_binary_bytes(1.0, self.version == 1) + b',b="a"\n' +
                b'tbl1 a' + _float_binary_bytes(2.0, self.version == 1) + b',b="b"\n' +
                b'tbl1 a' + _float_binary_bytes(3.0, self.version == 1) + b',b="c"\n' +
                b'tbl1 a' + _float_binary_bytes(0.0, self.version == 1) + b',b="d"\n' +
                b'tbl1 a' + _float_binary_bytes(float('inf'), self.version == 1) + b',b="e"\n' +
                b'tbl1 a' + _float_binary_bytes(float('-inf'), self.version == 1) + b',b="f"\n' +
                b'tbl1 b="g"\n' +  # This one is wierd: `nan` gets 0 in the bitmask.
                b'tbl1 a' + _float_binary_bytes(1.7976931348623157e308, self.version == 1) + b',b="h"\n' +
                b'tbl1 b="i"\n')

        def test_bool_numpy_col(self):
            df = pd.DataFrame({'a': pd.Series([
                    True, False, False,
                    False, True, False],
                dtype='bool')})
            buf = _dataframe(self.version, df, table_name='tbl1', at=qi.ServerTimestamp)
            self.assertEqual(
                buf,
                b'tbl1 a=t\n' +
                b'tbl1 a=f\n' +
                b'tbl1 a=f\n' +
                b'tbl1 a=f\n' +
                b'tbl1 a=t\n' +
                b'tbl1 a=f\n')

        def test_bool_arrow_col(self):
            df = pd.DataFrame({'a': pd.Series([
                    True, False, False,
                    False, True, False,
                    True, True, True,
                    False, False, False],
                dtype='boolean')})  # Note `boolean` != `bool`.
            buf = _dataframe(self.version, df, table_name='tbl1', at=qi.ServerTimestamp)
            self.assertEqual(
                buf,
                b'tbl1 a=t\n' +
                b'tbl1 a=f\n' +
                b'tbl1 a=f\n' +
                b'tbl1 a=f\n' +
                b'tbl1 a=t\n' +
                b'tbl1 a=f\n' +
                b'tbl1 a=t\n' +
                b'tbl1 a=t\n' +
                b'tbl1 a=t\n' +
                b'tbl1 a=f\n' +
                b'tbl1 a=f\n' +
                b'tbl1 a=f\n')

            df2 = pd.DataFrame({'a': pd.Series([
                    True, False, False,
                    None, True, False],
                dtype='boolean')})
            with self.assertRaisesRegex(
                    qi.QuestDBError,
                    'Failed.*at row index 3 .*<NA>.: .*insert null .*boolean col'):
                _dataframe(self.version, df2, table_name='tbl1', at=qi.ServerTimestamp)

        def test_bool_obj_col(self):
            df = pd.DataFrame({'a': pd.Series([
                    True, False, False,
                    False, True, False],
                dtype='object')})
            buf = _dataframe(self.version, df, table_name='tbl1', at=qi.ServerTimestamp)
            self.assertEqual(
                buf,
                b'tbl1 a=t\n' +
                b'tbl1 a=f\n' +
                b'tbl1 a=f\n' +
                b'tbl1 a=f\n' +
                b'tbl1 a=t\n' +
                b'tbl1 a=f\n')

            df2 = pd.DataFrame({'a': pd.Series([
                    True, False, 'false'],
                dtype='object')})
            with self.assertRaisesRegex(
                    qi.QuestDBError,
                    'serialize .* column .a. .* 2 .*false.*bool'):
                _dataframe(self.version, df2, table_name='tbl1', at=qi.ServerTimestamp)

            df3 = pd.DataFrame({'a': pd.Series([
                    None, True, False],
                dtype='object')})
            with self.assertRaisesRegex(
                    qi.QuestDBError,
                    'serialize.*\\(None\\): Cannot insert null.*boolean column'):
                _dataframe(self.version, df3, table_name='tbl1', at=qi.ServerTimestamp)

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
            buf = _dataframe(self.version, df, table_name='tbl1', at=qi.ServerTimestamp)
            e = self.enc_ts_n
            exp = (
                f'tbl1 a={e(1546300800000000000)},b="a"\n'.encode() +
                f'tbl1 a={e(1546300801000000000)},b="b"\n'.encode() +
                f'tbl1 a={e(1546300802000000000)},b="c"\n'.encode() +
                f'tbl1 a={e(1546300803000000000)},b="d"\n'.encode() +
                f'tbl1 a={e(1546300804000000000)},b="e"\n'.encode() +
                f'tbl1 a={e(1546300805000000000)},b="f"\n'.encode() +
                b'tbl1 b="g"\n' +
                b'tbl1 b="h"\n' +
                b'tbl1 b="i"\n')
            self.assertEqual(buf, exp)

            df = pd.DataFrame({'a': pd.Series([
                    pd.Timestamp('1970-01-01 00:00:00'),
                    pd.Timestamp('1970-01-01 00:00:01'),
                    pd.Timestamp('1970-01-01 00:00:02')],
                    dtype='datetime64[ns]')})
            buf = _dataframe(self.version, df, table_name='tbl1', at=qi.ServerTimestamp)
            self.assertEqual(
                buf,
                f'tbl1 a={e(0)}\n'.encode() +
                f'tbl1 a={e(1000000000)}\n'.encode() +
                f'tbl1 a={e(2000000000)}\n'.encode())

        def test_datetime64_numpy_seconds_col(self):
            df = pd.DataFrame({
                'a': pd.Series([
                        pd.Timestamp('2024-01-01 00:00:00'),
                        pd.Timestamp('2024-01-01 00:00:01'),
                        None,
                        pd.Timestamp('2024-01-01 00:00:03')],
                    dtype='datetime64[s]'),
                'b': ['a', 'b', 'c', 'd']})
            buf = _dataframe(self.version, df, table_name='tbl1', at=qi.ServerTimestamp)
            e = self.enc_ts_t
            exp = (
                f'tbl1 a={e(1704067200000000)},b="a"\n'.encode() +
                f'tbl1 a={e(1704067201000000)},b="b"\n'.encode() +
                b'tbl1 b="c"\n' +
                f'tbl1 a={e(1704067203000000)},b="d"\n'.encode())
            self.assertEqual(buf, exp)

        def test_datetime64_tz_arrow_col(self):
            df = pd.DataFrame({
                'a': pd.array([
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
                    dtype=_NS_TZ_DTYPE),
                'b': ['sym1', 'sym2', 'sym3', 'sym4']})
            buf = _dataframe(self.version, df, table_name='tbl1', symbols=['b'], at=qi.ServerTimestamp)
            e = self.enc_ts_n
            self.assertEqual(
                buf,
                # Note how these are 5hr offset from `test_datetime64_numpy_col`.
                f'tbl1,b=sym1 a={e(1546318800000000000)}\n'.encode() +
                f'tbl1,b=sym2 a={e(1546318801000000000)}\n'.encode() +
                b'tbl1,b=sym3\n' +
                f'tbl1,b=sym4 a={e(1546318803000000000)}\n'.encode())

            # Not epoch 0.
            df = pd.DataFrame({
                'a': pd.array([
                    pd.Timestamp(
                        year=1970, month=1, day=1,
                        hour=0, minute=0, second=0, tz=_TZ),
                    pd.Timestamp(
                        year=1970, month=1, day=1,
                        hour=0, minute=0, second=1, tz=_TZ),
                    pd.Timestamp(
                        year=1970, month=1, day=1,
                        hour=0, minute=0, second=2, tz=_TZ)],
                    dtype=_NS_TZ_DTYPE),
                'b': ['sym1', 'sym2', 'sym3']})
            buf = _dataframe(self.version, df, table_name='tbl1', symbols=['b'], at=qi.ServerTimestamp)
            self.assertEqual(
                buf,
                # Note how these are 5hr offset from `test_datetime64_numpy_col`.
                f'tbl1,b=sym1 a={e(18000000000000)}\n'.encode() +
                f'tbl1,b=sym2 a={e(18001000000000)}\n'.encode() +
                f'tbl1,b=sym3 a={e(18002000000000)}\n'.encode())

            # Actual epoch 0.
            df = pd.DataFrame({
                'a': pd.array([
                    pd.Timestamp(
                        year=1969, month=12, day=31,
                        hour=19, minute=0, second=0, tz=_TZ),
                    pd.Timestamp(
                        year=1969, month=12, day=31,
                        hour=19, minute=0, second=1, tz=_TZ),
                    pd.Timestamp(
                        year=1969, month=12, day=31,
                        hour=19, minute=0, second=2, tz=_TZ)],
                    dtype=_NS_TZ_DTYPE),
                'b': ['sym1', 'sym2', 'sym3']})
            buf = _dataframe(self.version, df, table_name='tbl1', symbols=['b'], at=qi.ServerTimestamp)
            self.assertEqual(
                buf,
                f'tbl1,b=sym1 a={e(0)}\n'.encode() +
                f'tbl1,b=sym2 a={e(1000000000)}\n'.encode() +
                f'tbl1,b=sym3 a={e(2000000000)}\n'.encode())

            df2 = pd.DataFrame({
                'a': pd.array([
                    pd.Timestamp(
                        year=1900, month=1, day=1,
                        hour=0, minute=0, second=0, tz=_TZ)],
                    dtype=_NS_TZ_DTYPE),
                'b': ['sym1']})
            buf = _dataframe(self.version, df2, table_name='tbl1', symbols=['b'], at=qi.ServerTimestamp)

            # Accounting for different datatime library differences.
            # Mostly, here assert that negative timestamps are allowed.
            self.assertIn(
                buf,
                [f'tbl1,b=sym1 a={e(-2208970800000000000)}\n'.encode(),
                 f'tbl1,b=sym1 a={e(-2208971040000000000)}\n'.encode()])

        def test_datetime64_tz_arrow_micros_col(self):
            df = pd.DataFrame({
                'a': pd.array([
                    pd.Timestamp(
                        year=2024, month=1, day=1,
                        hour=0, minute=0, second=0, microsecond=123456, tz=_TZ),
                    pd.Timestamp(
                        year=2024, month=1, day=1,
                        hour=0, minute=0, second=1, microsecond=654321, tz=_TZ),
                    None,
                    pd.Timestamp(
                        year=2024, month=1, day=1,
                        hour=0, minute=0, second=3, microsecond=111111, tz=_TZ)],
                    dtype=_US_TZ_DTYPE),
                'b': ['sym1', 'sym2', 'sym3', 'sym4']})
            buf = _dataframe(self.version, df, table_name='tbl1', symbols=['b'], at=qi.ServerTimestamp)
            e = self.enc_ts_t
            self.assertEqual(
                buf,
                # Note how these are 5hr offset from `test_arrow_micros_col`.
                f'tbl1,b=sym1 a={e(1704085200123456)}\n'.encode() +
                f'tbl1,b=sym2 a={e(1704085201654321)}\n'.encode() +
                b'tbl1,b=sym3\n' +
                f'tbl1,b=sym4 a={e(1704085203111111)}\n'.encode())

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
            buf = _dataframe(self.version, df, table_name='tbl1', at='a')
            e = self.enc_des_ts_n
            exp = (
                f'tbl1 b=1i {e(1546300800000000000)}\n'.encode() +
                f'tbl1 b=2i {e(1546300801000000000)}\n'.encode() +
                f'tbl1 b=3i {e(1546300802000000000)}\n'.encode() +
                f'tbl1 b=4i {e(1546300803000000000)}\n'.encode() +
                f'tbl1 b=5i {e(1546300804000000000)}\n'.encode() +
                f'tbl1 b=6i {e(1546300805000000000)}\n'.encode() +
                b'tbl1 b=7i\n' +
                b'tbl1 b=8i\n' +
                b'tbl1 b=9i\n')
            self.assertEqual(buf, exp)
            df = pd.DataFrame({
                'a': pd.Series([
                        pd.Timestamp('1970-01-01 00:00:00'),
                        pd.Timestamp('1970-01-01 00:00:01'),
                        pd.Timestamp('1970-01-01 00:00:02')],
                    dtype='datetime64[ns]'),
                'b': [1, 2, 3]})
            buf = _dataframe(self.version, df, table_name='tbl1', at='a')
            self.assertEqual(
                buf,
                f'tbl1 b=1i {e(0)}\n'.encode() +
                f'tbl1 b=2i {e(1000000000)}\n'.encode() +
                f'tbl1 b=3i {e(2000000000)}\n'.encode())

        def test_datetime64_numpy_seconds_at(self):
            df = pd.DataFrame({
                'a': pd.Series([
                        pd.Timestamp('2024-01-01 00:00:00'),
                        pd.Timestamp('2024-01-01 00:00:01'),
                        None,
                        pd.Timestamp('2024-01-01 00:00:03')],
                    dtype='datetime64[s]'),
                'b': [1, 2, 3, 4]})
            buf = _dataframe(self.version, df, table_name='tbl1', at='a')
            e = self.enc_des_ts_t
            exp = (
                f'tbl1 b=1i {e(1704067200000000)}\n'.encode() +
                f'tbl1 b=2i {e(1704067201000000)}\n'.encode() +
                b'tbl1 b=3i\n' +
                f'tbl1 b=4i {e(1704067203000000)}\n'.encode())
            self.assertEqual(buf, exp)

        def test_datetime64_tz_arrow_at(self):
            df = pd.DataFrame({
                'a': pd.array([
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
                    dtype=_NS_TZ_DTYPE),
                'b': ['sym1', 'sym2', 'sym3', 'sym4']})
            buf = _dataframe(self.version, df, table_name='tbl1', symbols=['b'], at='a')
            e = self.enc_des_ts_n
            exp = (
                # Note how these are 5hr offset from `test_datetime64_numpy_col`.
                f'tbl1,b=sym1 {e(1546318800000000000)}\n'.encode() +
                f'tbl1,b=sym2 {e(1546318801000000000)}\n'.encode() +
                b'tbl1,b=sym3\n' +
                f'tbl1,b=sym4 {e(1546318803000000000)}\n'.encode())
            self.assertEqual(buf, exp)

            df2 = pd.DataFrame({
                'a': pd.array([
                    pd.Timestamp(
                        year=1900, month=1, day=1,
                        hour=0, minute=0, second=0, tz=_TZ)],
                    dtype=_NS_TZ_DTYPE),
                'b': ['sym1']})
            with self.assertRaisesRegex(
                    qi.QuestDBError, "Failed.*'a'.*-220897.* is neg"):
                _dataframe(self.version, df2, table_name='tbl1', symbols=['b'], at='a')

        def test_datetime64_tz_arrow_micros_at(self):
            df = pd.DataFrame({
                'a': pd.array([
                    pd.Timestamp(
                        year=2024, month=1, day=1,
                        hour=0, minute=0, second=0, microsecond=123456, tz=_TZ),
                    pd.Timestamp(
                        year=2024, month=1, day=1,
                        hour=0, minute=0, second=1, microsecond=654321, tz=_TZ),
                    None,
                    pd.Timestamp(
                        year=2024, month=1, day=1,
                        hour=0, minute=0, second=3, microsecond=111111, tz=_TZ)],
                    dtype=_US_TZ_DTYPE),
                'b': ['sym1', 'sym2', 'sym3', 'sym4']})
            buf = _dataframe(self.version, df, table_name='tbl1', symbols=['b'], at='a')
            e = self.enc_des_ts_t
            exp = (
                # Note how these are 5hr offset from `test_arrow_micros_col`.
                f'tbl1,b=sym1 {e(1704085200123456)}\n'.encode() +
                f'tbl1,b=sym2 {e(1704085201654321)}\n'.encode() +
                b'tbl1,b=sym3\n' +
                f'tbl1,b=sym4 {e(1704085203111111)}\n'.encode())
            self.assertEqual(buf, exp)

            df2 = pd.DataFrame({
                'a': pd.array([
                    pd.Timestamp(
                        year=1900, month=1, day=1,
                        hour=0, minute=0, second=0, microsecond=123456, tz=_TZ)],
                    dtype=_US_TZ_DTYPE),
                'b': ['sym1']})
            with self.assertRaisesRegex(
                    qi.QuestDBError, "Failed.*'a'.*-220897.* is neg"):
                _dataframe(self.version, df2, table_name='tbl1', symbols=['b'], at='a')

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
            buf = _dataframe(self.version, df, table_name_col=0, at=qi.ServerTimestamp)
            self.assertEqual(
                buf,
                ('a b=1i\n' +
                ('b' * 127) + ' b=2i\n' +
                'q❤️p b=3i\n' +
                '嚜꓂ b=4i\n' +
                '💩🦞 b=5i\n').encode("utf-8"))

            with self.assertRaisesRegex(
                    qi.QuestDBError, "Too long"):
                _dataframe(self.version,
                    pd.DataFrame({'a': pd.Series(['b' * 128], dtype=dtype)}),
                    table_name_col='a', at=qi.ServerTimestamp)

            with self.assertRaisesRegex(
                    qi.QuestDBError, 'Failed.*(Expected a table name, got a null|Table name cannot be null).*'):
                _dataframe(self.version,
                    pd.DataFrame({
                        '.': pd.Series(['x', None], dtype=dtype),
                        'b': [1, 2]}),
                    table_name_col='.', at=qi.ServerTimestamp)

            with self.assertRaisesRegex(
                    qi.QuestDBError, 'Failed.*(Expected a table name, got a null|Table name cannot be null).*'):
                _dataframe(self.version,
                    pd.DataFrame({
                        '.': pd.Series(['x', float('nan')], dtype=dtype),
                        'b': [1, 2]}),
                    table_name_col='.', at=qi.ServerTimestamp)

            with self.assertRaisesRegex(
                    qi.QuestDBError, 'Failed.*(Expected a table name, got a null|Table name cannot be null).*'):
                _dataframe(self.version,
                    pd.DataFrame({
                        '.': pd.Series(['x', pd.NA], dtype=dtype),
                        'b': [1, 2]}),
                    table_name_col='.', at=qi.ServerTimestamp)

            with self.assertRaisesRegex(
                    qi.QuestDBError, "''.*must have a non-zero length"):
                _dataframe(self.version,
                    pd.DataFrame({
                        '/': pd.Series([''], dtype=dtype),
                        'b': [1]}),
                    table_name_col='/', at=qi.ServerTimestamp)

            with self.assertRaisesRegex(
                    qi.QuestDBError, "'tab..1'.*invalid dot `\\.` at position 4"):
                _dataframe(self.version,
                    pd.DataFrame({
                        '/': pd.Series(['tab..1'], dtype=dtype),
                        'b': [1]}),
                    table_name_col='/', at=qi.ServerTimestamp)

        def test_obj_str_table(self):
            self._test_pyobjstr_table('object')

            with self.assertRaisesRegex(
                    qi.QuestDBError, 'table name .*got an object of type int'):
                _dataframe(self.version,
                    pd.DataFrame({
                        '.': pd.Series(['x', 42], dtype='object'),
                        'z': [1, 2]}),
                    table_name_col='.', at=qi.ServerTimestamp)

        def test_obj_string_table(self):
            self._test_pyobjstr_table('string')

            self.assertEqual(
                _dataframe(self.version,
                    pd.DataFrame({
                        '.': pd.Series(['x', 42], dtype='string'),
                        'z': [1, 2]}),
                    table_name_col='.', at=qi.ServerTimestamp),
                b'x z=1i\n' +
                b'42 z=2i\n')

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
            buf = _dataframe(self.version, df, table_name='tbl1', symbols=True, at=qi.ServerTimestamp)
            self.assertEqual(
                buf,
                ('tbl1,a=a\n' +
                'tbl1,a=q❤️p\n' +
                'tbl1,a=' + ('❤️' * 1200) + '\n' +
                'tbl1,a=Questo\\ è\\ un\\ qualcosa\n' +
                'tbl1,a=щось\n' +
                'tbl1,a=\n' +
                'tbl1,a=嚜꓂\n' +
                'tbl1,a=💩🦞\n').encode("utf-8"))

            for null_obj in (None, float('nan'), pd.NA):
                self.assertEqual(
                    _dataframe(
                        self.version,
                        pd.DataFrame({
                            'x': pd.Series(['a', null_obj], dtype=dtype),
                            'y': [1, 2]}),
                        table_name='tbl1', symbols=[0], at=qi.ServerTimestamp),
                    b'tbl1,x=a y=1i\n' +
                    b'tbl1 y=2i\n')

        def test_obj_str_numpy_symbol(self):
            self._test_pyobjstr_numpy_symbol('object')

            with self.assertRaisesRegex(
                    qi.QuestDBError, 'Expected a string, got an .* type int'):
                _dataframe(
                    self.version,
                    pd.DataFrame({
                        'x': pd.Series(['x', 42], dtype='object'),
                        'y': [1, 2]}),
                    table_name='tbl1', symbols=[0], at=qi.ServerTimestamp)

        def test_obj_string_numpy_symbol(self):
            self._test_pyobjstr_numpy_symbol('string')

            self.assertEqual(
                _dataframe(
                    self.version,
                    pd.DataFrame({
                        'x': pd.Series(['x', 42], dtype='string'),
                        'y': [1, 2]}),
                    table_name='tbl1', symbols=[0], at=qi.ServerTimestamp),
                b'tbl1,x=x y=1i\n' +
                b'tbl1,x=42 y=2i\n')

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
            buf = _dataframe(self.version, df, table_name='tbl1', at=qi.ServerTimestamp)
            self.assertEqual(
                buf,
                ('tbl1 a="a"\n' +
                'tbl1 a="q❤️p"\n' +
                'tbl1 a="' + ('❤️' * 1200) + '"\n' +
                'tbl1 a="Questo è un qualcosa"\n' +
                'tbl1 a="щось"\n' +
                'tbl1 a=""\n' +
                'tbl1 a="嚜꓂"\n' +
                'tbl1 a="💩🦞"\n').encode("utf-8"))

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
            buf = _dataframe(self.version, df, table_name_col=0, at=qi.ServerTimestamp)
            self.assertEqual(
                buf,
                ('a b=1i\n' +
                ('b' * 127) + ' b=2i\n' +
                'q❤️p b=3i\n' +
                '嚜꓂ b=4i\n' +
                '💩🦞 b=5i\n').encode("utf-8"))

            with self.assertRaisesRegex(
                    qi.QuestDBError, "Too long"):
                _dataframe(
                    self.version,
                    pd.DataFrame({
                        'a': pd.Series(['b' * 128], dtype='string[pyarrow]')}),
                    table_name_col='a', at = qi.ServerTimestamp)

            with self.assertRaisesRegex(
                    qi.QuestDBError, "Failed .*<NA>.*Table name cannot be null"):
                _dataframe(
                    self.version,
                    pd.DataFrame({
                        '.': pd.Series(['x', None], dtype='string[pyarrow]'),
                        'b': [1, 2]}),
                    table_name_col='.', at = qi.ServerTimestamp)

            with self.assertRaisesRegex(
                    qi.QuestDBError, "''.*must have a non-zero length"):
                _dataframe(
                    self.version,
                    pd.DataFrame({
                        '/': pd.Series([''], dtype='string[pyarrow]')}),
                    table_name_col='/', at = qi.ServerTimestamp)

            with self.assertRaisesRegex(
                    qi.QuestDBError, "'tab..1'.*invalid dot `\\.` at position 4"):
                _dataframe(
                    self.version,
                    pd.DataFrame({
                        '/': pd.Series(['tab..1'], dtype='string[pyarrow]')}),
                    table_name_col='/', at = qi.ServerTimestamp)

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
            buf = _dataframe(self.version, df, table_name='tbl1', symbols=True, at = qi.ServerTimestamp)
            self.assertEqual(
                buf,
                ('tbl1,a=a b=1i\n' +
                'tbl1,a=q❤️p b=2i\n' +
                'tbl1,a=' + ('❤️' * 1200) + ' b=3i\n' +
                'tbl1,a=Questo\\ è\\ un\\ qualcosa b=4i\n' +
                'tbl1,a=щось b=5i\n' +
                'tbl1,a= b=6i\n' +
                'tbl1 b=7i\n' +
                'tbl1,a=嚜꓂ b=8i\n' +
                'tbl1,a=💩🦞 b=9i\n').encode('utf-8'))

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
            buf = _dataframe(self.version, df, table_name='tbl1', symbols=False, at = qi.ServerTimestamp)
            self.assertEqual(
                buf,
                ('tbl1 a="a",b=1i\n' +
                'tbl1 a="q❤️p",b=2i\n' +
                'tbl1 a="' + ('❤️' * 1200) + '",b=3i\n' +
                'tbl1 a="Questo è un qualcosa",b=4i\n' +
                'tbl1 a="щось",b=5i\n' +
                'tbl1 a="",b=6i\n' +
                'tbl1 b=7i\n' +
                'tbl1 a="嚜꓂",b=8i\n' +
                'tbl1 a="💩🦞",b=9i\n').encode('utf-8'))

        def test_pyobj_int_col(self):
            int64_min = -2**63
            int64_max = 2**63 - 1
            self.assertEqual(
                _dataframe(
                    self.version,
                    pd.DataFrame({
                        'a': pd.Series([
                            1, 2, 3, None, float('nan'), pd.NA, 7,
                            0,
                            int64_min,
                            int64_max], dtype='object'),
                        'b': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]}),
                    table_name='tbl1', at = qi.ServerTimestamp),
                ('tbl1 a=1i,b=1i\n' +
                'tbl1 a=2i,b=2i\n' +
                'tbl1 a=3i,b=3i\n' +
                'tbl1 b=4i\n' +
                'tbl1 b=5i\n' +
                'tbl1 b=6i\n' +
                'tbl1 a=7i,b=7i\n' +
                'tbl1 a=0i,b=8i\n' +
                'tbl1 a=' + str(int64_min) + 'i,b=9i\n' +
                'tbl1 a=' + str(int64_max) + 'i,b=10i\n').encode('utf-8'))

            with self.assertRaisesRegex(
                    qi.QuestDBError, "1 \\('STRING'\\): .*type int, got.*str\\."):
                _dataframe(
                    self.version,
                    pd.DataFrame({
                        'a': pd.Series([1, 'STRING'], dtype='object'),
                        'b': [1, 2]}),
                    table_name='tbl1', at = qi.ServerTimestamp)

            out_of_range = [int64_min - 1, int64_max + 1]
            for num in out_of_range:
                with self.assertRaisesRegex(
                        qi.QuestDBError, "index 1 .*922337203685477.*int too big"):
                    _dataframe(
                        self.version,
                        pd.DataFrame({
                            'a': pd.Series([1, num], dtype='object'),
                            'b': [1, 2]}),
                        table_name='tbl1', at = qi.ServerTimestamp)

        def test_pyobj_float_col(self):
            self.assertEqual(
                _dataframe(
                    self.version,
                    pd.DataFrame({
                        'a': pd.Series(
                            [1.0, 2.0, 3.0, None, float('nan'), pd.NA, 7.0],
                            dtype='object'),
                        'b': [1, 2, 3, 4, 5, 6, 7]}),
                    table_name='tbl1', at = qi.ServerTimestamp),
                b'tbl1 a' + _float_binary_bytes(1.0, self.version == 1) + b',b=1i\n' +
                b'tbl1 a' + _float_binary_bytes(2.0, self.version == 1) + b',b=2i\n' +
                b'tbl1 a' + _float_binary_bytes(3.0, self.version == 1) + b',b=3i\n' +
                b'tbl1 b=4i\n' +
                b'tbl1 a' + _float_binary_bytes(float('NaN'), self.version == 1) + b',b=5i\n' +
                b'tbl1 b=6i\n' +
                b'tbl1 a' + _float_binary_bytes(7.0, self.version == 1) + b',b=7i\n')

            with self.assertRaisesRegex(
                    qi.QuestDBError, "1 \\('STRING'\\): .*type float, got.*str\\."):
                _dataframe(
                    self.version,
                    pd.DataFrame({
                        'a': pd.Series([1.0, 'STRING'], dtype='object'),
                        'b': [1, 2]}),
                    table_name='tbl1', at = qi.ServerTimestamp)

        def test_bad_category(self):
            # We only support string categories
            # (unless anyone asks for additional ones).
            # We want to test others are rejected.
            with self.assertRaisesRegex(
                    qi.QuestDBError, "Bad column 'a'.*got a category of .*int64"):
                _dataframe(
                    self.version,
                    pd.DataFrame({'a': pd.Series([1, 2, 3, 2], dtype='category')}),
                    table_name='tbl1', at = qi.ServerTimestamp)

        def _test_cat_table(self, count):
            slist = [f's{i}' for i in range(count)]

            df = pd.DataFrame({
                'a': pd.Series(slist, dtype='category'),
                'b': list(range(len(slist)))})

            buf = _dataframe(self.version, df, table_name_col=0, at = qi.ServerTimestamp)
            exp = ''.join(
                f'{s} b={i}i\n'
                for i, s in enumerate(slist))
            self.assertEqual(buf, exp.encode("utf-8"))

            slist[2] = None
            df2 = pd.DataFrame({
                'a': pd.Series(slist, dtype='category'),
                'b': list(range(len(slist)))})
            with self.assertRaisesRegex(
                    qi.QuestDBError, 'Table name cannot be null'):
                _dataframe(self.version, df2, table_name_col=0, at = qi.ServerTimestamp)

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

            buf = _dataframe(self.version, df, table_name='tbl1', symbols=True, at = qi.ServerTimestamp)
            exp = ''.join(
                f'tbl1,a={s} b={i}i\n'
                for i, s in enumerate(slist))
            self.assertEqual(buf, exp.encode("utf-8"))

            slist[2] = None
            df2 = pd.DataFrame({
                'a': pd.Series(slist, dtype='category'),
                'b': list(range(len(slist)))})

            exp2 = exp.replace('tbl1,a=s2 b=2i\n', 'tbl1 b=2i\n')
            buf2 = _dataframe(self.version, df2, table_name='tbl1', symbols=True, at = qi.ServerTimestamp)
            self.assertEqual(buf2, exp2.encode("utf-8"))

        def test_cat_i8_symbol(self):
            self._test_cat_symbol(30)
            self._test_cat_symbol(127)

        def test_cat_large_string_symbol(self):
            df = pd.DataFrame({
                'a': pd.Series(
                    pa.array(
                        ['alpha', 'beta', None, 'alpha'],
                        type=pa.large_string()),
                    dtype=pd.ArrowDtype(pa.large_string())).astype('category'),
                'b': [1, 2, 3, 4],
            })

            buf = _dataframe(
                self.version,
                df,
                table_name='tbl1',
                symbols=True,
                at=qi.ServerTimestamp)

            self.assertEqual(
                buf,
                b'tbl1,a=alpha b=1i\n'
                b'tbl1,a=beta b=2i\n'
                b'tbl1 b=3i\n'
                b'tbl1,a=alpha b=4i\n')

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

            buf = _dataframe(self.version, df, table_name='tbl1', symbols=False, at = qi.ServerTimestamp)
            exp = ''.join(
                f'tbl1 a="{s}",b={i}i\n'
                for i, s in enumerate(slist))
            self.assertEqual(buf, exp.encode("utf-8"))

            slist[2] = None
            df2 = pd.DataFrame({
                'a': pd.Series(slist, dtype='category'),
                'b': list(range(len(slist)))})

            exp2 = exp.replace('tbl1 a="s2",b=2i\n', 'tbl1 b=2i\n')
            buf2 = _dataframe(self.version, df2, table_name='tbl1', symbols=False, at = qi.ServerTimestamp)
            self.assertEqual(buf2, exp2.encode("utf-8"))

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
            buf = _dataframe(self.version, df, table_name='tbl1', at = qi.ServerTimestamp)
            self.assertEqual(
                buf,
                b'tbl1 b=1i\n' +
                b'tbl1 b=2i\n' +
                b'tbl1 b=3i\n')

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
                    qi.QuestDBError, "Bad column 'a': .*not.*contiguous"):
                _dataframe(self.version, df, table_name='tbl1', at = qi.ServerTimestamp)

        def test_serializing_in_chunks(self):
            df = pd.DataFrame({
                'a': pd.Series(np.arange(30), dtype='int64'),
                'b': pd.Series(np.arange(30), dtype='Int64')})
            parts = [
                df.iloc[:10],
                df.iloc[10:20],
                df.iloc[20:]]
            for index, part in enumerate(parts):
                buf = _dataframe(self.version, part, table_name='tbl1', at = qi.ServerTimestamp)
                exp = ''.join(
                    f'tbl1 a={i}i,b={i}i\n'
                    for i in range(index * 10, (index + 1) * 10))
                self.assertEqual(buf, exp.encode("utf-8"))

        def test_auto_flush_error_msg(self):
            header = ["x", "y"]
            x = list(range(10000))
            y = list(range(10000))

            df = pd.DataFrame(zip(x, y), columns=header)

            with self.assertRaisesRegex(qi.QuestDBError, 'Could not flush buffer: Buffer size of 21780 exceeds maximum configured allowed size of 1024 bytes'):
                with qi.Sender.from_conf("http::addr=localhost:9000;auto_flush_rows=1000;max_buf_size=1024;protocol_version=2;") as sender:
                    sender.dataframe(df, table_name='test_df', at=qi.ServerTimestamp)
                    sender.flush()

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
            buf = _dataframe(self.version, df, table_name='tbl1', at = qi.ServerTimestamp)
            exp = (
                b'tbl1 a=1i,b=10i\n' +
                b'tbl1 a=2i,b=20i\n' +
                b'tbl1 a=3i,b=30i\n' +
                b'tbl1 a=4i,b=40i\n' +
                b'tbl1 a=5i,b=50i\n' +
                b'tbl1 a=6i,b=60i\n' +
                b'tbl1 a=7i,b=70i\n' +
                b'tbl1 a=8i,b=80i\n' +
                b'tbl1 a=9i,b=90i\n')
            self.assertEqual(buf, exp)

            if not hasattr(pd, 'ArrowDtype'):
                # We don't have pandas ArrowDtype, so we can't test the rest.
                return

            # To preserve the chunking we need to use a special pandas type:
            pandarrow_a = pd.array(chunked_a, dtype='int16[pyarrow]')
            pandarrow_b = pd.array(chunked_b, dtype='int32[pyarrow]')
            df = pd.DataFrame({'a': pandarrow_a, 'b': pandarrow_b})

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
            try:
                df.to_parquet(fp_parquet_path, engine='fastparquet')
                fp_wrote = True
            except (ValueError, TypeError):
                # fastparquet may not support pandas 3.x arrow-backed strings.
                fp_wrote = False
            pa2pa_df = pd.read_parquet(pa_parquet_path, engine='pyarrow')

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
            # It decays categories to object/string and UInt8 to float64.
            # We need to set up special case expected results for that.
            fallback_exp_dtypes = [
                np.dtype('O'),
                np.dtype('int16'),
                np.dtype('float64'),
                np.dtype('float64')]
            fallback_df = df.astype({'s': 'object', 'b': 'float64'})

            def fastparquet_pyarrow_expected(deser_df):
                actual_dtypes = list(deser_df.dtypes)
                if not isinstance(actual_dtypes[0], pd.StringDtype):
                    return fallback_df, fallback_exp_dtypes

                exp_dtypes = list(fallback_exp_dtypes)
                exp_dtypes[0] = actual_dtypes[0]
                return fallback_df.astype({'s': actual_dtypes[0]}), exp_dtypes

            df_eq(df, pa2pa_df, exp_dtypes)
            if fp_wrote:
                pa2fp_df = pd.read_parquet(pa_parquet_path, engine='fastparquet')
                fp2pa_df = pd.read_parquet(fp_parquet_path, engine='pyarrow')
                fp2fp_df = pd.read_parquet(fp_parquet_path, engine='fastparquet')
                df_eq(df, pa2fp_df, exp_dtypes)
                fp2pa_exp_df, fp2pa_exp_dtypes = fastparquet_pyarrow_expected(fp2pa_df)
                df_eq(fp2pa_exp_df, fp2pa_df, fp2pa_exp_dtypes)
                df_eq(df, fp2fp_df, exp_dtypes)

            exp = (
                b'tbl1,s=a a=1i,b=10i,c' + _float_binary_bytes(0.5, self.version == 1) + b'\n' +
                b'tbl1,s=b a=2i,b=20i,c' + _float_binary_bytes(float('NaN'), self.version == 1) + b'\n' +
                b'tbl1,s=a a=3i,b=30i,c' + _float_binary_bytes(2.5, self.version == 1) + b'\n' +
                b'tbl1,s=c a=4i,c' + _float_binary_bytes(3.5, self.version == 1) + b'\n' +
                b'tbl1,s=a a=5i,b=50i,c' + _float_binary_bytes(float('NaN'), self.version == 1) + b'\n')

            fallback_exp = (
                b'tbl1 s="a",a=1i,b' + _float_binary_bytes(10.0, self.version == 1) + b',c' +
                _float_binary_bytes(0.5, self.version == 1) + b'\n' +
                b'tbl1 s="b",a=2i,b' + _float_binary_bytes(20.0, self.version == 1) + b',c' +
                _float_binary_bytes(float('NaN'), self.version == 1) + b'\n' +
                b'tbl1 s="a",a=3i,b' + _float_binary_bytes(30.0, self.version == 1) + b',c' +
                _float_binary_bytes(2.5, self.version == 1) + b'\n' +
                b'tbl1 s="c",a=4i,b' + _float_binary_bytes(float('NaN'), self.version == 1) + b',c' +
                _float_binary_bytes(3.5, self.version == 1) + b'\n' +
                b'tbl1 s="a",a=5i,b' + _float_binary_bytes(50.0, self.version == 1) + b',c' +
                _float_binary_bytes(float('NaN'), self.version == 1) + b'\n')

            self.assertEqual(_dataframe(self.version, df, table_name='tbl1', at=qi.ServerTimestamp), exp)
            self.assertEqual(_dataframe(self.version, pa2pa_df, table_name='tbl1', at=qi.ServerTimestamp), exp)
            if fp_wrote:
                self.assertEqual(_dataframe(self.version, pa2fp_df, table_name='tbl1', at=qi.ServerTimestamp), exp)
                self.assertEqual(_dataframe(self.version, fp2pa_df, table_name='tbl1', at=qi.ServerTimestamp), fallback_exp)
                self.assertEqual(_dataframe(self.version, fp2fp_df, table_name='tbl1', at=qi.ServerTimestamp), exp)

        def test_f64_np_array(self):
            df = pd.DataFrame({
                'a': [np.array([1.0], np.float64), np.array([2.0], np.float64), np.array([3.0], np.float64)]})

            if self.version == 1:
                with self.assertRaisesRegex(
                        qi.QuestDBError,
                        "Protocol version v1 does not support array datatype"):
                    _ = _dataframe(self.version, df, table_name='tbl1', at=qi.ServerTimestamp)
            else:
                buf = _dataframe(self.version, df, table_name='tbl1', at=qi.ServerTimestamp)
                self.assertEqual(
                    buf,
                    b'tbl1 a=' + _array_binary_bytes(np.array([1.0], np.float64)) + b'\n' +
                    b'tbl1 a=' + _array_binary_bytes(np.array([2.0], np.float64)) + b'\n' +
                    b'tbl1 a=' + _array_binary_bytes(np.array([3.0], np.float64)) + b'\n')

        def test_f64_np_array_null_cell(self):
            if self.version == 1:
                self.skipTest('Protocol version v1 does not support arrays')
            df = pd.DataFrame({
                'a': pd.Series(
                    [np.array([1.0], np.float64), None], dtype=object),
                'other': [1, 2]})
            buf = _dataframe(
                self.version, df, table_name='tbl1',
                at=qi.ServerTimestamp)
            self.assertEqual(
                buf,
                b'tbl1 a=' +
                _array_binary_bytes(np.array([1.0], np.float64)) +
                b',other=1i\n'
                b'tbl1 other=2i\n')

        def test_f64_np_array_mixed_cell_types(self):
            if self.version == 1:
                self.skipTest('Protocol version v1 does not support arrays')
            for bad_cell in ('oops', [2.0], 3.0):
                with self.subTest(bad_cell=bad_cell):
                    df = pd.DataFrame({
                        'a': pd.Series([
                            np.array([1.0], np.float64),
                            bad_cell], dtype=object)})
                    with self.assertRaisesRegex(
                            qi.QuestDBError,
                            'Expected an object of type numpy.ndarray') as raised:
                        _dataframe(
                            self.version, df, table_name='tbl1',
                            at=qi.ServerTimestamp)
                    self.assertIs(
                        raised.exception.code,
                        qi.QuestDBErrorCode.BadDataFrame)
                    self.assertIn('row index 1', str(raised.exception))

        def test_numpy_micros_col(self):
            df = pd.DataFrame({
                'x': [1, 2, 3],
                'ts1': pd.Series([
                    pd.Timestamp(2023, 2, 1, 10, 0, 0),
                    pd.Timestamp(2023, 2, 2, 12, 30, 15),
                    pd.Timestamp(2023, 2, 3, 15, 45, 30)
                ], dtype='datetime64[us]'),
                'ts2': pd.Series([
                    pd.Timestamp(2023, 2, 1, 10, 0, 0),
                    None,
                    pd.Timestamp(2023, 2, 3, 15, 45, 30)
                ], dtype='datetime64[us]')
            })

            act = _dataframe(self.version, df, table_name='tbl1', at='ts2')

            # format designated timestamp micros
            def fdtm(value):
                if self.version >= 2:
                    return f'{value}t\n'.encode()
                else:
                    value = value * 1000
                    return f'{value}\n'.encode()
                
            exp = (
                b'tbl1 x=1i,ts1=1675245600000000t ' + fdtm(1675245600000000) +
                b'tbl1 x=2i,ts1=1675341015000000t\n' +
                b'tbl1 x=3i,ts1=1675439130000000t ' + fdtm(1675439130000000))
            self.assertEqual(exp, act)
                
        def test_arrow_micros_col(self):
            df = pd.DataFrame({
                'x': [1, 2, 3],
                'ts1': pd.Series(
                    pa.array(
                        [
                            pd.Timestamp("2024-01-01 00:00:00.123456"),
                            pd.Timestamp("2024-01-01 00:00:01.654321"),
                            pd.Timestamp("2024-01-01 00:00:02.111111"),
                        ],
                        type=pa.timestamp("us")
                    ),
                    dtype="timestamp[us][pyarrow]"),
                'ts2': pd.Series(
                    pa.array(
                        [
                            pd.Timestamp("2024-01-01 00:00:00.123456"),
                            pd.Timestamp("2024-01-01 00:00:01.654321"),
                            None
                        ],
                        type=pa.timestamp("us")
                    ),
                    dtype="timestamp[us][pyarrow]"),
            })
            act = _dataframe(self.version, df, table_name='tbl1', at='ts2')

            # format designated timestamp micros
            def fdtm(value):
                if self.version >= 2:
                    return f'{value}t\n'.encode()
                else:
                    value = value * 1000
                    return f'{value}\n'.encode()
                
            exp = (
                b'tbl1 x=1i,ts1=1704067200123456t ' + fdtm(1704067200123456) +
                b'tbl1 x=2i,ts1=1704067201654321t ' + fdtm(1704067201654321) +
                b'tbl1 x=3i,ts1=1704067202111111t\n')
            self.assertEqual(exp, act)

        def test_arrow_types(self):
            df = pd.DataFrame({
                "ts": pd.Series(
                    pa.array(
                        pd.date_range("2024-01-01", periods=5, freq="s"),
                        type=pa.timestamp("ns")
                    ),
                    dtype="timestamp[ns][pyarrow]"
                ),

                "ts2": pd.Series(
                    pa.array(
                        pd.date_range("2024-01-01", periods=5, freq="s"),
                        type=pa.timestamp("ns")
                    ),
                    dtype="timestamp[ns][pyarrow]"
                ),

                "b": pd.Series(
                    pa.array([True, False, True, True, False], type=pa.bool_()),
                    dtype="bool[pyarrow]"
                ),

                "sensor_large": pd.Series(
                    pa.LargeStringArray.from_pandas(
                        ["alpha", None, "gamma", "delta", "epsilon"]
                    ),
                    dtype="large_string[pyarrow]"
                ),

                "sensor_small": pd.Series(
                    pa.array(["foo", "bar", None, "baz", "qux"], type=pa.string()),
                    dtype="string[pyarrow]"
                ),

                "value_f32": pd.Series(
                    pa.array([None, 20.0, 30.25, 40.5, 50.75], type=pa.float32()),
                    dtype="float32[pyarrow]"
                ),

                "value_f64": pd.Series(
                    pa.array([1.1, 2.2, 3.3, None, 5.5], type=pa.float64()),
                    dtype="float64[pyarrow]"
                ),

                "value_i8": pd.Series(
                    pa.array([1, None, 3, 4, 5], type=pa.int8()),
                    dtype="int8[pyarrow]"
                ),

                "value_i16": pd.Series(
                    pa.array([100, 200, 300, 400, None], type=pa.int16()),
                    dtype="int16[pyarrow]"
                ),

                "value_i32": pd.Series(
                    pa.array([1000, 2000, None, 4000, 5000], type=pa.int32()),
                    dtype="int32[pyarrow]"
                ),

                "value_i64": pd.Series(
                    pa.array([10, 20, 30, 40, None], type=pa.int64()),
                    dtype="int64[pyarrow]"
                ),
            })

            # format a timestamp
            def fts(value):
                if self.version >= 2:
                    return f'{value}n'.encode()
                else:
                    value = value // 1000
                    return f'{value}t'.encode()

            # designated timestamp suffix and line ending
            tsls = b'n\n' if self.version >= 2 else b'\n'

            exp = (
                b'tbl1 ts2=' + fts(1704067200000000000) +
                b',b=t,sensor_large="alpha",sensor_small="foo",value_f64' +
                _float_binary_bytes(1.1, self.version == 1) +
                b',value_i8=1i,value_i16=100i,value_i32=1000i,value_i64=10i 1704067200000000000' +
                tsls +

                b'tbl1 ts2=' + fts(1704067201000000000) +
                b',b=f,sensor_small="bar",value_f32' +
                _float_binary_bytes(20.0, self.version == 1) +
                b',value_f64' +
                _float_binary_bytes(2.2, self.version == 1) +
                b',value_i16=200i,value_i32=2000i,value_i64=20i 1704067201000000000' +
                tsls +

                b'tbl1 ts2=' + fts(1704067202000000000) +
                b',b=t,sensor_large="gamma",value_f32' +
                _float_binary_bytes(30.25, self.version == 1) +
                b',value_f64' +
                _float_binary_bytes(3.3, self.version == 1) +
                b',value_i8=3i,value_i16=300i,value_i64=30i 1704067202000000000' +
                tsls +

                b'tbl1 ts2=' + fts(1704067203000000000) +
                b',b=t,sensor_large="delta",sensor_small="baz",value_f32' +
                _float_binary_bytes(40.5, self.version == 1) +
                b',value_i8=4i,value_i16=400i,value_i32=4000i,value_i64=40i 1704067203000000000' +
                tsls +

                b'tbl1 ts2=' + fts(1704067204000000000) +
                b',b=f,sensor_large="epsilon",sensor_small="qux",value_f32' +
                _float_binary_bytes(50.75, self.version == 1) +
                b',value_f64' +
                _float_binary_bytes(5.5, self.version == 1) +
                b',value_i8=5i,value_i32=5000i 1704067204000000000' +
                tsls)
            act = _dataframe(self.version, df, table_name='tbl1', at='ts')
            self.assertEqual(act, exp)

        def test_arrow_strings_as_symbols(self):
            df = pd.DataFrame({
                "sym_large": pd.Series(
                    pa.LargeStringArray.from_pandas(
                        ["alpha", None, "gamma", "delta", "epsilon"]
                    ),
                    dtype="large_string[pyarrow]"
                ),

                "sym_small": pd.Series(
                    pa.array(["foo", "bar", None, "baz", "qux"], type=pa.string()),
                    dtype="string[pyarrow]"
                )
            })

            act = _dataframe(self.version, df, table_name='tbl1', symbols=('sym_large', 'sym_small'), at=qi.ServerTimestamp)
            exp = (
                b'tbl1,sym_large=alpha,sym_small=foo\n'
                b'tbl1,sym_small=bar\n'
                b'tbl1,sym_large=gamma\n'
                b'tbl1,sym_large=delta,sym_small=baz\n'
                b'tbl1,sym_large=epsilon,sym_small=qux\n'
            )
            self.assertEqual(exp, act)


class TestPandasProtocolVersionV1(TestPandasBase.TestPandas):
    name = 'protocol version 1'
    version = 1


class TestPandasProtocolVersionV2(TestPandasBase.TestPandas):
    name = 'protocol version 2'
    version = 2


class TestPandasProtocolVersionV3(TestPandasBase.TestPandas):
    name = 'protocol version 3'
    version = 3


class TestNaTScalarDatetime(unittest.TestCase):
    def test_from_datetime_nat_raises_value_error(self):
        for cls in (qi.TimestampNanos, qi.TimestampMicros):
            with self.assertRaisesRegex(
                    ValueError, 'NaT is not a valid timestamp'):
                cls.from_datetime(pd.NaT)

    def test_row_at_nat_raises_value_error(self):
        buf = qi.Buffer(protocol_version=2)
        with self.assertRaisesRegex(
                ValueError, 'NaT is not a valid timestamp'):
            buf.row('t', columns={'x': 1}, at=pd.NaT)

    def test_dataframe_at_nat_raises(self):
        buf = qi.Buffer(protocol_version=2)
        df = pd.DataFrame({'x': [1]})
        with self.assertRaisesRegex(
                qi.QuestDBError, 'NaT is not a valid timestamp'):
            buf.dataframe(df, table_name='t', at=pd.NaT)


class TestColumnarPlanWithoutPyarrow(unittest.TestCase):
    """The columnar planner's pyarrow-optional fallbacks, exercised in a
    subprocess with the pyarrow import blocked."""

    _SCRIPT = """
import sys


class _BlockPyarrow:
    def find_spec(self, name, path=None, target=None):
        if name == 'pyarrow' or name.startswith('pyarrow.'):
            raise ImportError('pyarrow blocked for this test')


sys.meta_path.insert(0, _BlockPyarrow())
import pandas as pd
from decimal import Decimal
import questdb._client as qi

df = pd.DataFrame({
    'ts': pd.Series([pd.Timestamp('2024-01-01')], dtype='datetime64[us]'),
    'vts': pd.Series([pd.NaT], dtype='datetime64[ns]'),
})
plan = qi._debug_dataframe_columnar_plan(df, table_name='t', at='ts')
assert not plan['supported'], plan
reasons = ' '.join(f['reason'] for f in plan['failures'])
assert 'without pyarrow' in reasons, plan

df = pd.DataFrame({
    'ts': pd.Series([pd.Timestamp('2024-01-01')], dtype='datetime64[us]'),
    'amt': pd.Series([Decimal('1.5')], dtype=object),
})
plan = qi._debug_dataframe_columnar_plan(df, table_name='t', at='ts')
assert not plan['supported'], plan
reasons = ' '.join(f['reason'] for f in plan['failures'])
assert 'require pyarrow' in reasons, plan
print('OK')
"""

    def test_planner_rejects_pyarrow_dependent_columns(self):
        env = dict(os.environ)
        env['PYTHONPATH'] = os.pathsep.join(
            [str(pathlib.Path(qi.__file__).parent.parent)]
            + [p for p in env.get('PYTHONPATH', '').split(os.pathsep) if p])
        result = subprocess.run(
            [sys.executable, '-c', self._SCRIPT],
            capture_output=True, text=True, env=env)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('OK', result.stdout)


class TestDecimalWithoutCAccelerator(unittest.TestCase):
    """`Decimal` cells are read through CPython's `_decimal` object layout, so
    the pure-Python `decimal` module has to be turned away rather than punned."""

    _SCRIPT = """
import sys

# `decimal` falls back to its pure-Python implementation when the accelerator
# cannot be imported, and keeps the `Decimal` name.
sys.modules['_decimal'] = None

from decimal import Decimal
import questdb.ingress as qi

assert hasattr(Decimal, '_int'), 'expected the pure-Python Decimal'

buf = qi.Buffer._new_qwp()
try:
    buf.row('t', columns={'d': Decimal('1.25')}, at=qi.ServerTimestamp)
except ValueError as ve:
    assert '_decimal' in str(ve), ve
else:
    raise AssertionError('a punned Decimal was accepted')
print('OK')
"""

    _FOREIGN_INTERPRETER_SCRIPT = """
import sys, types

# Stand in for an interpreter that is not CPython but does ship a
# module named `_decimal`, so `Decimal is _decimal.Decimal` holds and
# the module check alone would let the reinterpretation through.
sys.implementation = types.SimpleNamespace(
    name='pypy', version=sys.implementation.version,
    hexversion=sys.implementation.hexversion,
    cache_tag=sys.implementation.cache_tag)

from decimal import Decimal
import _decimal
import questdb.ingress as qi

assert Decimal is _decimal.Decimal, 'expected the accelerated Decimal'

buf = qi.Buffer._new_qwp()
try:
    buf.row('t', columns={'d': Decimal('1.25')}, at=qi.ServerTimestamp)
except ValueError as ve:
    message = str(ve)
    assert 'pypy' in message, message
    assert 'float' in message and 'string' in message, message
else:
    raise AssertionError('a non-CPython Decimal layout was assumed')
print('OK')
"""

    def test_a_non_cpython_interpreter_is_refused(self):
        """`Decimal is _decimal.Decimal` only says the `decimal` module
        is backed by something called `_decimal`. An interpreter that
        ships its own under that name passes the module check while
        laying its objects out differently -- which is the case the
        guard exists for -- so the interpreter is checked too."""
        self._run_script(self._FOREIGN_INTERPRETER_SCRIPT)

    def test_pure_python_decimal_is_refused(self):
        self._run_script(self._SCRIPT)

    def _run_script(self, script):
        env = dict(os.environ)
        env['PYTHONPATH'] = os.pathsep.join(
            [str(pathlib.Path(qi.__file__).parent.parent)]
            + [p for p in env.get('PYTHONPATH', '').split(os.pathsep) if p])
        env['PYTHONWARNINGS'] = 'ignore'
        result = subprocess.run(
            [sys.executable, '-c', script],
            capture_output=True, text=True, env=env)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('OK', result.stdout)


if __name__ == '__main__':
    if os.environ.get('TEST_QUESTDB_PROFILE') == '1':
        import cProfile
        cProfile.run('unittest.main()', sort='cumtime')
    else:
        unittest.main()
