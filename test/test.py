#!/usr/bin/env python3

import sys
sys.dont_write_bytecode = True
import os
import unittest
import datetime
import time

import patch_path
from mock_server import Server


import questdb.ingress as qi

if os.environ.get('TEST_QUESTDB_INTEGRATION') == '1':
    from system_test import TestWithDatabase

try:
    import pandas as pd
    import numpy
    import pyarrow
except ImportError:
    pd = None


if pd is not None:
    from test_dataframe import TestPandas
else:
    class TestNoPandas(unittest.TestCase):
        def test_no_pandas(self):
            buf = qi.Buffer()
            exp = 'Missing.*`pandas.*pyarrow`.*readthedocs.*installation.html.'
            with self.assertRaisesRegex(ImportError, exp):
                buf.dataframe(None)


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

    @unittest.skipIf(not pd, 'pandas not installed')
    def test_dataframe(self):
        with Server() as server:
            with qi.Sender('localhost', server.port) as sender:
                server.accept()
                df = pd.DataFrame({'a': [1, 2], 'b': [3.0, 4.0]})
                sender.dataframe(df, table_name='tbl1')
            msgs = server.recv()
            self.assertEqual(
                msgs,
                [b'tbl1 a=1i,b=3.0',
                    b'tbl1 a=2i,b=4.0'])

    @unittest.skipIf(not pd, 'pandas not installed')
    def test_dataframe_auto_flush(self):
        with Server() as server:
            # An auto-flush size of 20 bytes is enough to auto-flush the first
            # row, but not the second.
            with qi.Sender('localhost', server.port, auto_flush=20) as sender:
                server.accept()
                df = pd.DataFrame({'a': [100000, 2], 'b': [3.0, 4.0]})
                sender.dataframe(df, table_name='tbl1')
                msgs = server.recv()
                self.assertEqual(
                    msgs,
                    [b'tbl1 a=100000i,b=3.0'])

                # The second row is still pending send.
                self.assertEqual(len(sender), 16)

                # So we give it some more data and we should see it flush.
                sender.row('tbl1', columns={'a': 3, 'b': 5.0})
                msgs = server.recv()
                self.assertEqual(
                    msgs,
                    [b'tbl1 a=2i,b=4.0',
                     b'tbl1 a=3i,b=5.0'])

                self.assertEqual(len(sender), 0)

                # We can now disconnect the server and see auto flush failing.
                server.close()

                exp_err = 'Could not flush buffer.* - See https'
                with self.assertRaisesRegex(qi.IngressError, exp_err):
                    for _ in range(1000):
                        time.sleep(0.01)
                        sender.dataframe(df.head(1), table_name='tbl1')

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


class TestBases:
    class Timestamp(unittest.TestCase):
        def test_from_int(self):
            ns = 1670857929778202000
            num = ns // self.ns_scale
            ts = self.timestamp_cls(num)
            self.assertEqual(ts.value, num)

            ts0 = self.timestamp_cls(0)
            self.assertEqual(ts0.value, 0)

            with self.assertRaisesRegex(ValueError, 'value must be a positive'):
                self.timestamp_cls(-1)

        def test_from_datetime(self):
            utc = datetime.timezone.utc

            dt1 = datetime.datetime(2022, 1, 1, 12, 0, 0, 0, tzinfo=utc)
            ts1 = self.timestamp_cls.from_datetime(dt1)
            self.assertEqual(ts1.value, 1641038400000000000 // self.ns_scale)
            self.assertEqual(
                ts1.value,
                int(dt1.timestamp() * 1000000000 // self.ns_scale))

            dt2 = datetime.datetime(1970, 1, 1, tzinfo=utc)
            ts2 = self.timestamp_cls.from_datetime(dt2)
            self.assertEqual(ts2.value, 0)

            with self.assertRaisesRegex(ValueError, 'value must be a positive'):
                self.timestamp_cls.from_datetime(
                    datetime.datetime(1969, 12, 31, tzinfo=utc))

            dt_naive = datetime.datetime(2022, 1, 1, 12, 0, 0, 0,
                tzinfo=utc).astimezone(None).replace(tzinfo=None)
            ts3 = self.timestamp_cls.from_datetime(dt_naive)
            self.assertEqual(ts3.value, 1641038400000000000 // self.ns_scale)

        def test_now(self):
            expected = time.time_ns() // self.ns_scale
            actual = self.timestamp_cls.now().value
            delta = abs(expected - actual)
            one_sec = 1000000000 // self.ns_scale
            self.assertLess(delta, one_sec)


class TestTimestampMicros(TestBases.Timestamp):
    timestamp_cls = qi.TimestampMicros
    ns_scale = 1000


class TestTimestampNanos(TestBases.Timestamp):
    timestamp_cls = qi.TimestampNanos
    ns_scale = 1


if __name__ == '__main__':
    if os.environ.get('TEST_QUESTDB_PROFILE') == '1':
        import cProfile
        cProfile.run('unittest.main()', sort='cumtime')
    else:
        unittest.main()
