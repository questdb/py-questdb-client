#!/usr/bin/env python3

import sys
sys.dont_write_bytecode = True
import os
import unittest
import datetime
import time
import numpy as np
import pandas as pd
import zoneinfo

import patch_path
from mock_server import Server

import questdb.ingress as qi

if os.environ.get('TEST_QUESTDB_INTEGRATION') == '1':
    from system_test import TestWithDatabase


class TestBuffer(unittest.TestCase):
    def test_new(self):
        buf = qi.Buffer()
        self.assertEqual(len(buf), 0)
        self.assertEqual(buf.capacity(), 64 * 1024)

    def test_basic(self):
        buf = qi.Buffer()
        buf.row('tbl1', symbols={'sym1': 'val1', 'sym2': 'val2'})
        self.assertEqual(len(buf), 25)
        self.assertEqual(str(buf), 'tbl1,sym1=val1,sym2=val2\n')

    def test_bad_table(self):
        buf = qi.Buffer()
        with self.assertRaisesRegex(
                qi.IngressError,
                'Table names must have a non-zero length'):
            buf.row('', symbols={'sym1': 'val1'})
        with self.assertRaisesRegex(
                qi.IngressError,
                'Bad string "x..y": Found invalid dot `.` at position 2.'):
            buf.row('x..y', symbols={'sym1': 'val1'})

    def test_symbol(self):
        buf = qi.Buffer()
        buf.row('tbl1', symbols={'sym1': 'val1', 'sym2': 'val2'})
        self.assertEqual(str(buf), 'tbl1,sym1=val1,sym2=val2\n')

    def test_bad_symbol_column_name(self):
        buf = qi.Buffer()
        with self.assertRaisesRegex(
                qi.IngressError,
                'Column names must have a non-zero length.'):
            buf.row('tbl1', symbols={'': 'val1'})
        with self.assertRaisesRegex(
                qi.IngressError,
                'Bad string "sym.bol": '
                'Column names can\'t contain a \'.\' character, '
                'which was found at byte position 3.'):
            buf.row('tbl1', symbols={'sym.bol': 'val1'})

    def test_column(self):
        two_h_after_epoch = datetime.datetime(
            1970, 1, 1, 2, tzinfo=datetime.timezone.utc)
        buf = qi.Buffer()
        buf.row('tbl1', columns={
            'col1': True,
            'col2': False,
            'col3': -1,
            'col4': 0.5,
            'col5': 'val',
            'col6': qi.TimestampMicros(12345),
            'col7': two_h_after_epoch,
            'col8': None})
        exp = (
            'tbl1 col1=t,col2=f,col3=-1i,col4=0.5,'
            'col5="val",col6=12345t,col7=7200000000t\n')
        self.assertEqual(str(buf), exp)

    def test_none_symbol(self):
        buf = qi.Buffer()
        buf.row('tbl1', symbols={'sym1': 'val1', 'sym2': None})
        exp = 'tbl1,sym1=val1\n'
        self.assertEqual(str(buf), exp)
        self.assertEqual(len(buf), len(exp))

        # No fields to write, no fields written, therefore a no-op.
        buf.row('tbl1', symbols={'sym1': None, 'sym2': None})
        self.assertEqual(str(buf), exp)
        self.assertEqual(len(buf), len(exp))

    def test_none_column(self):
        buf = qi.Buffer()
        buf.row('tbl1', columns={'col1': 1})
        exp = 'tbl1 col1=1i\n'
        self.assertEqual(str(buf), exp)
        self.assertEqual(len(buf), len(exp))

        # No fields to write, no fields written, therefore a no-op.
        buf.row('tbl1', columns={'col1': None, 'col2': None})
        self.assertEqual(str(buf), exp)
        self.assertEqual(len(buf), len(exp))

    def test_no_symbol_or_col_args(self):
        buf = qi.Buffer()
        buf.row('table_name')
        self.assertEqual(str(buf), '')

    def test_unicode(self):
        buf = qi.Buffer()
        buf.row(
            'tbl1',                            # ASCII
            symbols={'questdb1': 'q❤️p'},       # Mixed ASCII and UCS-2
            columns={'questdb2': '❤️' * 1200})  # Over the 1024 buffer prealloc.
        buf.row(
            'tbl1',
            symbols={
                'Questo è il nome di una colonna':  # Non-ASCII UCS-1
                'Це символьне значення'},  # UCS-2, 2 bytes for UTF-8.
            columns={
                'questdb1': '',  # Empty string
                'questdb2': '嚜꓂',  # UCS-2, 3 bytes for UTF-8.
                'questdb3': '💩🦞'})  # UCS-4, 4 bytes for UTF-8.
        self.assertEqual(str(buf),
            f'tbl1,questdb1=q❤️p questdb2="{"❤️" * 1200}"\n' +
            'tbl1,Questo\\ è\\ il\\ nome\\ di\\ una\\ colonna=' +
            'Це\\ символьне\\ значення ' +
            'questdb1="",questdb2="嚜꓂",questdb3="💩🦞"\n')

        buf.clear()
        buf.row('tbl1', symbols={'questdb1': 'q❤️p'})
        self.assertEqual(str(buf), 'tbl1,questdb1=q❤️p\n')

        # A bad char in Python.
        with self.assertRaisesRegex(
                qi.IngressError,
                '.*codepoint 0xd800 in string .*'):
            buf.row('tbl1', symbols={'questdb1': 'a\ud800'})

        # Strong exception safety: no partial writes.
        # Ensure we can continue using the buffer after an error.
        buf.row('tbl1', symbols={'questdb1': 'another line of input'})
        self.assertEqual(
            str(buf),
            'tbl1,questdb1=q❤️p\n' +
            # Note: No partially written failed line here.
            'tbl1,questdb1=another\\ line\\ of\\ input\n')

    def test_float(self):
        buf = qi.Buffer()
        buf.row('tbl1', columns={'num': 1.2345678901234567})
        self.assertEqual(str(buf), f'tbl1 num=1.2345678901234567\n')

    def test_int_range(self):
        buf = qi.Buffer()
        buf.row('tbl1', columns={'num': 0})
        self.assertEqual(str(buf), f'tbl1 num=0i\n')
        buf.clear()

        # 32-bit int range.
        buf.row('tbl1', columns={'min': -2**31, 'max': 2**31-1})
        self.assertEqual(str(buf), f'tbl1 min=-2147483648i,max=2147483647i\n')
        buf.clear()

        # 64-bit int range.
        buf.row('tbl1', columns={'min': -2**63, 'max': 2**63-1})
        self.assertEqual(str(buf), f'tbl1 min=-9223372036854775808i,max=9223372036854775807i\n')
        buf.clear()

        # Overflow.
        with self.assertRaises(OverflowError):
            buf.row('tbl1', columns={'num': 2**63})

        # Underflow.
        with self.assertRaises(OverflowError):
            buf.row('tbl1', columns={'num': -2**63-1})


class TestSender(unittest.TestCase):
    def test_basic(self):
        with Server() as server, qi.Sender('localhost', server.port) as sender:
            server.accept()
            self.assertEqual(server.recv(), [])
            sender.row(
                'tab1',
                symbols={
                    't1': 'val1',
                    't2': 'val2'},
                columns={
                    'f1': True,
                    'f2': 12345,
                    'f3': 10.75,
                    'f4': 'val3'},
                at=qi.TimestampNanos(111222233333))
            sender.row(
                'tab1',
                symbols={
                    'tag3': 'value 3',
                    'tag4': 'value:4'},
                columns={
                    'field5': False})
            sender.flush()
            msgs = server.recv()
            self.assertEqual(msgs, [
                (b'tab1,t1=val1,t2=val2 '
                 b'f1=t,f2=12345i,f3=10.75,f4="val3" '
                 b'111222233333'),
                b'tab1,tag3=value\\ 3,tag4=value:4 field5=f'])

    def test_connect_close(self):
        with Server() as server:
            sender = None
            try:
                sender = qi.Sender('localhost', server.port)
                sender.connect()
                server.accept()
                self.assertEqual(server.recv(), [])
                sender.row('tbl1', symbols={'sym1': 'val1'})
                sender.flush()
                msgs = server.recv()
                self.assertEqual(msgs, [b'tbl1,sym1=val1'])
            finally:
                sender.close()

    def test_row_before_connect(self):
        try:
            sender = qi.Sender('localhost', 12345)
            sender.row('tbl1', symbols={'sym1': 'val1'})
            with self.assertRaisesRegex(qi.IngressError, 'Not connected'):
                sender.flush()
        finally:
            sender.close()

    def test_flush_1(self):
        with Server() as server:
            with qi.Sender('localhost', server.port) as sender:
                server.accept()
                with self.assertRaisesRegex(qi.IngressError, 'Column names'):
                    sender.row('tbl1', symbols={'...bad name..': 'val1'})
                self.assertEqual(str(sender), '')
                sender.flush()
                self.assertEqual(str(sender), '')
            msgs = server.recv()
            self.assertEqual(msgs, [])

    def test_flush_2(self):
        with Server() as server:
            with qi.Sender('localhost', server.port) as sender:
                server.accept()
                server.close()

                # We enter a bad state where we can't flush again.
                with self.assertRaises(qi.IngressError):
                    for _ in range(1000):
                        time.sleep(0.01)
                        sender.row('tbl1', symbols={'a': 'b'})
                        sender.flush()

                # We should still be in a bad state.
                with self.assertRaises(qi.IngressError):
                    sender.row('tbl1', symbols={'a': 'b'})
                    sender.flush()

            # Leaving the `with` scope will call __exit__ and here we test
            # that a prior exception will not cause subsequent problems.

    def test_flush_3(self):
        # Same as test_flush_2, but we catch the exception _outside_ the
        # sender's `with` block, to ensure no exceptions get trapped.
        with Server() as server:
            with self.assertRaises(qi.IngressError):
                with qi.Sender('localhost', server.port) as sender:
                    server.accept()
                    server.close()
                    for _ in range(1000):
                        time.sleep(0.01)
                        sender.row('tbl1', symbols={'a': 'b'})
                        sender.flush()

    def test_flush_4(self):
        # Clearing of the internal buffer is not allowed.
        with Server() as server:
            with self.assertRaises(ValueError):
                with qi.Sender('localhost', server.port) as sender:
                    server.accept()
                    sender.row('tbl1', symbols={'a': 'b'})
                    sender.flush(buffer=None, clear=False)

    def test_two_rows_explicit_buffer(self):
        with Server() as server, qi.Sender('localhost', server.port) as sender:
            server.accept()
            self.assertEqual(server.recv(), [])
            buffer = sender.new_buffer()
            buffer.row(
                'line_sender_buffer_example2',
                symbols={'id': 'Hola'},
                columns={'price': '111222233333i', 'qty': 3.5},
                at=qi.TimestampNanos(111222233333))
            buffer.row(
                'line_sender_example',
                symbols={'id': 'Adios'},
                columns={'price': '111222233343i', 'qty': 2.5},
                at=qi.TimestampNanos(111222233343))
            exp = (
                'line_sender_buffer_example2,id=Hola price="111222233333i",qty=3.5 111222233333\n'
                'line_sender_example,id=Adios price="111222233343i",qty=2.5 111222233343\n')
            self.assertEqual(str(buffer), exp)
            sender.flush(buffer)
            msgs = server.recv()
            bexp = [msg.encode('utf-8') for msg in exp.rstrip().split('\n')]
            self.assertEqual(msgs, bexp)

    def test_independent_buffer(self):
        buf = qi.Buffer()
        buf.row('tbl1', symbols={'sym1': 'val1'})
        exp = 'tbl1,sym1=val1\n'
        bexp = exp[:-1].encode('utf-8')
        self.assertEqual(str(buf), exp)

        with Server() as server1, Server() as server2:
            with qi.Sender('localhost', server1.port) as sender1, \
                 qi.Sender('localhost', server2.port) as sender2:
                    server1.accept()
                    server2.accept()

                    sender1.flush(buf, clear=False)
                    self.assertEqual(str(buf), exp)

                    sender2.flush(buf, clear=False)
                    self.assertEqual(str(buf), exp)

                    msgs1 = server1.recv()
                    msgs2 = server2.recv()
                    self.assertEqual(msgs1, [bexp])
                    self.assertEqual(msgs2, [bexp])

                    sender1.flush(buf)
                    self.assertEqual(server1.recv(), [bexp])

                    # The buffer is now auto-cleared.
                    self.assertEqual(str(buf), '')

    def test_auto_flush(self):
        with Server() as server:
            with qi.Sender('localhost', server.port, auto_flush=4) as sender:
                server.accept()
                sender.row('tbl1', symbols={'sym1': 'val1'})
                self.assertEqual(len(sender), 0)  # auto-flushed buffer.
                msgs = server.recv()
                self.assertEqual(msgs, [b'tbl1,sym1=val1'])

    def test_immediate_auto_flush(self):
        with Server() as server:
            with qi.Sender('localhost', server.port, auto_flush=True) as sender:
                server.accept()
                sender.row('tbl1', symbols={'sym1': 'val1'})
                self.assertEqual(len(sender), 0)  # auto-flushed buffer.
                msgs = server.recv()
                self.assertEqual(msgs, [b'tbl1,sym1=val1'])

    def test_auto_flush_on_closed_socket(self):
        with Server() as server:
            with qi.Sender('localhost', server.port, auto_flush=True) as sender:
                server.accept()
                server.close()
                exp_err = 'Could not flush buffer.* - See https'
                with self.assertRaisesRegex(qi.IngressError, exp_err):
                    for _ in range(1000):
                        time.sleep(0.01)
                        sender.row('tbl1', symbols={'a': 'b'})

    def test_dont_auto_flush(self):
        msg_counter = 0
        with Server() as server:
            with qi.Sender('localhost', server.port, auto_flush=0) as sender:
                server.accept()
                while len(sender) < 32768:  # 32KiB
                    sender.row('tbl1', symbols={'sym1': 'val1'})
                    msg_counter += 1
                msgs = server.recv()
                self.assertEqual(msgs, [])
            start = time.monotonic()
            msgs = []
            while len(msgs) < msg_counter:
                msgs += server.recv()
                elapsed = time.monotonic() - start
                if elapsed > 30.0:
                    raise TimeoutError()

    def test_dont_flush_on_exception(self):
        with Server() as server:
            with self.assertRaises(RuntimeError):
                with qi.Sender('localhost', server.port) as sender:
                    server.accept()
                    sender.row('tbl1', symbols={'sym1': 'val1'})
                    self.assertEqual(str(sender), 'tbl1,sym1=val1\n')
                    raise RuntimeError('Test exception')
            msgs = server.recv()
            self.assertEqual(msgs, [])

    def test_new_buffer(self):
        sender = qi.Sender(
            host='localhost',
            port=9009,
            init_capacity=1024,
            max_name_len=10)
        buffer = sender.new_buffer()
        self.assertEqual(buffer.init_capacity, 1024)
        self.assertEqual(buffer.max_name_len, 10)
        self.assertEqual(buffer.init_capacity, sender.init_capacity)
        self.assertEqual(buffer.max_name_len, sender.max_name_len)

    def test_connect_after_close(self):
        with Server() as server, qi.Sender('localhost', server.port) as sender:
            server.accept()
            sender.row('tbl1', symbols={'sym1': 'val1'})
            sender.close()
            with self.assertRaises(qi.IngressError):
                sender.connect()

    def test_bad_init_args(self):
        with self.assertRaises(OverflowError):
            qi.Sender(host='localhost', port=9009, read_timeout=-1)

        with self.assertRaises(OverflowError):
            qi.Sender(host='localhost', port=9009, init_capacity=-1)

        with self.assertRaises(OverflowError):
            qi.Sender(host='localhost', port=9009, max_name_len=-1)


def _pandas(*args, **kwargs):
    buf = qi.Buffer()
    buf.pandas(*args, **kwargs)
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


class TestPandas(unittest.TestCase):
    def test_bad_dataframe(self):
        with self.assertRaisesRegex(TypeError, 'Expected pandas'):
            _pandas([])

    def test_no_table_name(self):
        with self.assertRaisesRegex(ValueError, 'Must specify at least one of'):
            _pandas(DF1)

    def test_bad_table_name_type(self):
        with self.assertRaisesRegex(TypeError, 'Must be str'):
            _pandas(DF1, table_name=1.5)

    def test_invalid_table_name(self):
        with self.assertRaisesRegex(ValueError, '`table_name`: Bad string "."'):
            _pandas(DF1, table_name='.')

    def test_invalid_column_dtype(self):
        with self.assertRaisesRegex(TypeError, '`table_name_col`: Bad dtype'):
            _pandas(DF1, table_name_col='B')
        with self.assertRaisesRegex(TypeError, '`table_name_col`: Bad dtype'):
            _pandas(DF1, table_name_col=1)
        with self.assertRaisesRegex(TypeError, '`table_name_col`: Bad dtype'):
            _pandas(DF1, table_name_col=-3)
        with self.assertRaisesRegex(IndexError, '`table_name_col`: -5 index'):
            _pandas(DF1, table_name_col=-5)

    def test_bad_str_obj_col(self):
        with self.assertRaisesRegex(TypeError, 'Found non-string value'):
            _pandas(DF1, table_name_col='D')
        with self.assertRaisesRegex(TypeError, 'Found non-string value'):
            _pandas(DF1, table_name_col=3)
        with self.assertRaisesRegex(TypeError, 'Found non-string value'):
            _pandas(DF1, table_name_col=-1)

    def test_bad_symbol(self):
        with self.assertRaisesRegex(TypeError, '`symbols`.*bool.*tuple.*list'):
            _pandas(DF1, table_name='tbl1', symbols=0)
        with self.assertRaisesRegex(TypeError, '`symbols`.*bool.*tuple.*list'):
            _pandas(DF1, table_name='tbl1', symbols={})
        with self.assertRaisesRegex(TypeError, '`symbols`.*bool.*tuple.*list'):
            _pandas(DF1, table_name='tbl1', symbols=None)
        with self.assertRaisesRegex(TypeError, '.*element.*symbols.*float.*0'):
            _pandas(DF1, table_name='tbl1', symbols=(0,))
        with self.assertRaisesRegex(TypeError, '.*element.*symbols.*int.*1'):
            _pandas(DF1, table_name='tbl1', symbols=[1])

    def test_bad_at(self):
        with self.assertRaisesRegex(KeyError, '`at`.*2018.*not found in the'):
            _pandas(DF1, table_name='tbl1', at='2018-03-10T00:00:00Z')
        with self.assertRaisesRegex(TypeError, '`at`.*float64.*be a datetime'):
            _pandas(DF1, table_name='tbl1', at='A')
        with self.assertRaisesRegex(TypeError, '`at`.*int64.*be a datetime'):
            _pandas(DF1, table_name='tbl1', at=1)
        with self.assertRaisesRegex(TypeError, '`at`.*object.*be a datetime'):
            _pandas(DF1, table_name='tbl1', at=-1)
    
    def test_basic(self):
        buf = _pandas(
            DF2,
            table_name_col='T',
            symbols=['A', 'B', 'C', 'D'],
            at=-1)
        self.assertEqual(
            buf,
            't1,A=a1,B=b1,C=b1,D=a1 E=1.0,F=1i 1520640000000000000\n' +
            't2,A=a2,D=a2 E=2.0,F=2i 1520726400000000000\n' +
            't1,A=a3,B=b3,C=b3,D=a3 E=3.0,F=3i 1520812800000000000\n')

    def test_row_of_nulls(self):
        df = pd.DataFrame({'a': ['a1', None, 'a3']})
        with self.assertRaisesRegex(
                qi.IngressError, 'State error: Bad call to `at`'):
            _pandas(df, table_name='tbl1', symbols=['a'])

    def test_u8_numpy_col(self):
        df = pd.DataFrame({'a': pd.Series([
                1, 2, 3,
                0,
                255],  # u8 max
            dtype='uint8')})
        buf = _pandas(df, table_name='tbl1')
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
        buf = _pandas(df, table_name='tbl1')
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
        buf = _pandas(df, table_name='tbl1')
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
        buf = _pandas(df, table_name='tbl1')
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
        buf = _pandas(df, table_name='tbl1')
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
        buf = _pandas(df, table_name='tbl1')
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
        buf = _pandas(df, table_name='tbl1')
        self.assertEqual(
            buf,
            'tbl1 a=1i\n' +
            'tbl1 a=2i\n' +
            'tbl1 a=3i\n' +
            'tbl1 a=0i\n' +
            'tbl1 a=9223372036854775807i\n')

        buf = qi.Buffer()
        buf.pandas(pd.DataFrame({'b': [.5, 1.0, 1.5]}), table_name='tbl2')
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
            buf.pandas(df2, table_name='tbl1')

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
        buf = _pandas(df, table_name='tbl1')
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
        buf = _pandas(df, table_name='tbl1')
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
        buf = _pandas(df, table_name='tbl1')
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
        buf = _pandas(df, table_name='tbl1')
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
        buf = _pandas(df, table_name='tbl1')
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
        buf = _pandas(df, table_name='tbl1')
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
        buf = _pandas(df, table_name='tbl1')
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
        buf = _pandas(df, table_name='tbl1')
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
        buf = _pandas(df, table_name='tbl1')
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
        buf = _pandas(df, table_name='tbl1')
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
            _pandas(df2, table_name='tbl1')

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
        buf = _pandas(df, table_name='tbl1')
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
        buf = _pandas(df, table_name='tbl1')
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
        buf = _pandas(df, table_name='tbl1')
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
        buf = _pandas(df, table_name='tbl1')
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
        buf = _pandas(df, table_name='tbl1')
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
            _pandas(df2, table_name='tbl1')

    def test_bool_obj_col(self):
        df = pd.DataFrame({'a': pd.Series([
                True, False, False,
                False, True, False],
            dtype='object')})
        buf = _pandas(df, table_name='tbl1')
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
            _pandas(df2, table_name='tbl1')

        df3 = pd.DataFrame({'a': pd.Series([
                None, True, False],
            dtype='object')})
        with self.assertRaisesRegex(
                qi.IngressError,
                'serialize.*\\(None\\): Cannot insert null.*boolean column'):
            _pandas(df3, table_name='tbl1')

    def test_datetime64_numpy_col(self):
        df = pd.DataFrame({'a': pd.Series([
                pd.Timestamp('2019-01-01 00:00:00'),
                pd.Timestamp('2019-01-01 00:00:01'),
                pd.Timestamp('2019-01-01 00:00:02'),
                pd.Timestamp('2019-01-01 00:00:03'),
                pd.Timestamp('2019-01-01 00:00:04'),
                pd.Timestamp('2019-01-01 00:00:05')],
            dtype='datetime64[ns]')})
        buf = _pandas(df, table_name='tbl1')
        self.assertEqual(
            buf,
            'tbl1 a=1546300800000000t\n' +
            'tbl1 a=1546300801000000t\n' +
            'tbl1 a=1546300802000000t\n' +
            'tbl1 a=1546300803000000t\n' +
            'tbl1 a=1546300804000000t\n' +
            'tbl1 a=1546300805000000t\n')

        # TODO: Test 0-epoch.

    def test_datetime64_tz_arrow_col(self):
        # Currently broken, find `TODO: datetime[ns]+tz`.
        # We're just casting `PyObject*`` to `int64_t` at the moment.
        tz = zoneinfo.ZoneInfo('America/New_York')
        df = pd.DataFrame({
            'a': [
                pd.Timestamp(
                    year=2019, month=1, day=1,
                    hour=0, minute=0, second=0, tz=tz),
                pd.Timestamp(
                    year=2019, month=1, day=1,
                    hour=0, minute=0, second=1, tz=tz),
                None,
                pd.Timestamp(
                    year=2019, month=1, day=1,
                    hour=0, minute=0, second=3, tz=tz)],
            'b': ['sym1', 'sym2', 'sym3', 'sym4']})
        buf = _pandas(df, table_name='tbl1', symbols=['b'])
        self.assertEqual(
            buf,
            # Note how these are 5hr offset from `test_datetime64_numpy_col`.
            'tbl1,b=sym1 a=1546318800000000t\n' +
            'tbl1,b=sym2 a=1546318801000000t\n' +
            'tbl1,b=sym3\n' +
            'tbl1,b=sym4 a=1546318803000000t\n')

        # TODO: Test 0-epoch.

        df2 = pd.DataFrame({
            'a': [
                pd.Timestamp(
                    year=1900, month=1, day=1,
                    hour=0, minute=0, second=0, tz=tz)],
            'b': ['sym1']})
        with self.assertRaisesRegex(
                qi.IngressError, "Failed.*'a'.*-2208970800000000 is negative."):
            _pandas(df2, table_name='tbl1', symbols=['b'])

    def test_datetime64_numpy_at(self):
        df = pd.DataFrame({
            'a': pd.Series([
                    pd.Timestamp('2019-01-01 00:00:00'),
                    pd.Timestamp('2019-01-01 00:00:01'),
                    pd.Timestamp('2019-01-01 00:00:02'),
                    pd.Timestamp('2019-01-01 00:00:03'),
                    pd.Timestamp('2019-01-01 00:00:04'),
                    pd.Timestamp('2019-01-01 00:00:05')],
                dtype='datetime64[ns]'),
            'b': [1, 2, 3, 4, 5, 6]})
        buf = _pandas(df, table_name='tbl1', at='a')
        self.assertEqual(
            buf,
            'tbl1 b=1i 1546300800000000000\n' +
            'tbl1 b=2i 1546300801000000000\n' +
            'tbl1 b=3i 1546300802000000000\n' +
            'tbl1 b=4i 1546300803000000000\n' +
            'tbl1 b=5i 1546300804000000000\n' +
            'tbl1 b=6i 1546300805000000000\n')

        # TODO: Test 0-epoch.

    def test_datetime64_tz_arrow_at(self):
        # Currently broken, find `TODO: datetime[ns]+tz`.
        # We're just casting `PyObject*`` to `int64_t` at the moment.
        tz = zoneinfo.ZoneInfo('America/New_York')
        df = pd.DataFrame({
            'a': [
                pd.Timestamp(
                    year=2019, month=1, day=1,
                    hour=0, minute=0, second=0, tz=tz),
                pd.Timestamp(
                    year=2019, month=1, day=1,
                    hour=0, minute=0, second=1, tz=tz),
                None,
                pd.Timestamp(
                    year=2019, month=1, day=1,
                    hour=0, minute=0, second=3, tz=tz)],
            'b': ['sym1', 'sym2', 'sym3', 'sym4']})
        buf = _pandas(df, table_name='tbl1', symbols=['b'], at='a')
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
                    hour=0, minute=0, second=0, tz=tz)],
            'b': ['sym1']})
        with self.assertRaisesRegex(
                qi.IngressError, "Failed.*'a'.*-2208970800000000000 is neg"):
            _pandas(df2, table_name='tbl1', symbols=['b'], at='a')

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
        buf = _pandas(df, table_name_col=0)
        self.assertEqual(
            buf,
            'a b=1i\n' +
            ('b' * 127) + ' b=2i\n' +
            'q❤️p b=3i\n' +
            '嚜꓂ b=4i\n' +
            '💩🦞 b=5i\n')

        with self.assertRaisesRegex(
                qi.IngressError, "Too long"):
            _pandas(
                pd.DataFrame({'a': pd.Series(['b' * 128], dtype=dtype)}),
                table_name_col='a')

        with self.assertRaisesRegex(
                qi.IngressError, 'Failed.*Expected a table name, got a null.*'):
            _pandas(
                pd.DataFrame({
                    '.': pd.Series(['x', None], dtype=dtype),
                    'b': [1, 2]}),
                table_name_col='.')

        with self.assertRaisesRegex(
                qi.IngressError, 'Failed.*Expected a table name, got a null.*'):
            _pandas(
                pd.DataFrame({
                    '.': pd.Series(['x', float('nan')], dtype=dtype),
                    'b': [1, 2]}),
                table_name_col='.')

        with self.assertRaisesRegex(
                qi.IngressError, 'Failed.*Expected a table name, got a null.*'):
            _pandas(
                pd.DataFrame({
                    '.': pd.Series(['x', pd.NA], dtype=dtype),
                    'b': [1, 2]}),
                table_name_col='.')

        with self.assertRaisesRegex(
                qi.IngressError, "''.*must have a non-zero length"):
            _pandas(
                pd.DataFrame({
                    '/': pd.Series([''], dtype=dtype),
                    'b': [1]}),
                table_name_col='/')

        with self.assertRaisesRegex(
                qi.IngressError, "'tab..1'.*invalid dot `\.` at position 4"):
            _pandas(
                pd.DataFrame({
                    '/': pd.Series(['tab..1'], dtype=dtype),
                    'b': [1]}),
                table_name_col='/')

    def test_obj_str_table(self):
        self._test_pyobjstr_table('object')

        with self.assertRaisesRegex(
                qi.IngressError, 'table name .*got an object of type int'):
            _pandas(
                pd.DataFrame({
                    '.': pd.Series(['x', 42], dtype='object'),
                    'z': [1, 2]}),
                table_name_col='.')

    def test_obj_string_table(self):
        self._test_pyobjstr_table('string')

        self.assertEqual(
            _pandas(
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
        buf = _pandas(df, table_name='tbl1', symbols=True)
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
                _pandas(
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
            _pandas(
                pd.DataFrame({
                    'x': pd.Series(['x', 42], dtype='object'),
                    'y': [1, 2]}),
                table_name='tbl1', symbols=[0])

    def test_obj_string_numpy_symbol(self):
        self._test_pyobjstr_numpy_symbol('string')

        self.assertEqual(
            _pandas(
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
        buf = _pandas(df, table_name='tbl1')
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
        buf = _pandas(df, table_name='tbl1', symbols=True)
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
        buf = _pandas(df, table_name_col=0)
        self.assertEqual(
            buf,
            'a b=1i\n' +
            ('b' * 127) + ' b=2i\n' +
            'q❤️p b=3i\n' +
            '嚜꓂ b=4i\n' +
            '💩🦞 b=5i\n')

        with self.assertRaisesRegex(
                qi.IngressError, "Too long"):
            _pandas(
                pd.DataFrame({
                    'a': pd.Series(['b' * 128], dtype='string[pyarrow]')}),
                table_name_col='a')

        with self.assertRaisesRegex(
                qi.IngressError, "Failed .*<NA>.*Table name cannot be null"):
            _pandas(
                pd.DataFrame({
                    '.': pd.Series(['x', None], dtype='string[pyarrow]'),
                    'b': [1, 2]}),
                table_name_col='.')

        with self.assertRaisesRegex(
                qi.IngressError, "''.*must have a non-zero length"):
            _pandas(
                pd.DataFrame({
                    '/': pd.Series([''], dtype='string[pyarrow]')}),
                table_name_col='/')

        with self.assertRaisesRegex(
                qi.IngressError, "'tab..1'.*invalid dot `\.` at position 4"):
            _pandas(
                pd.DataFrame({
                    '/': pd.Series(['tab..1'], dtype='string[pyarrow]')}),
                table_name_col='/')

    def test_pyobj_int_col(self):
        self.assertEqual(
            _pandas(
                pd.DataFrame({
                    'a': pd.Series([1, 2, 3, None, float('nan'), pd.NA, 7], dtype='object'),
                    'b': [1, 2, 3, 4, 5, 6, 7]}),
                table_name='tbl1'),
            'tbl1 a=1i,b=1i\n' +
            'tbl1 a=2i,b=2i\n' +
            'tbl1 a=3i,b=3i\n' +
            'tbl1 b=4i\n' +
            'tbl1 b=5i\n' +
            'tbl1 b=6i\n' +
            'tbl1 a=7i,b=7i\n')
        
        with self.assertRaisesRegex(
                qi.IngressError, "1 \\('STRING'\\): .*type int, got.*str\."):
            _pandas(
                pd.DataFrame({
                    'a': pd.Series([1, 'STRING'], dtype='object'),
                    'b': [1, 2]}),
                table_name='tbl1')

    def test_pyobj_float_col(self):
        self.assertEqual(
            _pandas(
                pd.DataFrame({
                    'a': pd.Series([1.0, 2.0, 3.0, None, float('nan'), pd.NA, 7.0], dtype='object'),
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
                qi.IngressError, "1 \\('STRING'\\): .*type float, got.*str\."):
            _pandas(
                pd.DataFrame({
                    'a': pd.Series([1.0, 'STRING'], dtype='object'),
                    'b': [1, 2]}),
                table_name='tbl1')


if __name__ == '__main__':
    unittest.main()
