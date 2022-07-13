#!/usr/bin/env python3

import sys
sys.dont_write_bytecode = True
import os
import unittest
import datetime

import patch_path
from mock_server import Server

import questdb.ilp as ilp

if os.environ.get('TEST_QUESTDB_INTEGRATION') == '1':
    from system_test import TestWithDatabase


class TestBuffer(unittest.TestCase):
    def test_new(self):
        buf = ilp.Buffer()
        self.assertEqual(len(buf), 0)
        self.assertEqual(buf.capacity(), 64 * 1024)

    def test_basic(self):
        buf = ilp.Buffer()
        buf.row('tbl1', symbols={'sym1': 'val1', 'sym2': 'val2'})
        self.assertEqual(len(buf), 25)
        self.assertEqual(str(buf), 'tbl1,sym1=val1,sym2=val2\n')

    def test_bad_table(self):
        buf = ilp.Buffer()
        with self.assertRaisesRegex(
                ilp.IlpError,
                'Table names must have a non-zero length'):
            buf.row('', symbols={'sym1': 'val1'})
        with self.assertRaisesRegex(
                ilp.IlpError,
                'Bad string "x..y": Found invalid dot `.` at position 2.'):
            buf.row('x..y', symbols={'sym1': 'val1'})

    def test_symbol(self):
        buf = ilp.Buffer()
        buf.row('tbl1', symbols={'sym1': 'val1', 'sym2': 'val2'})
        self.assertEqual(str(buf), 'tbl1,sym1=val1,sym2=val2\n')

    def test_bad_symbol_column_name(self):
        buf = ilp.Buffer()
        with self.assertRaisesRegex(
                ilp.IlpError,
                'Column names must have a non-zero length.'):
            buf.row('tbl1', symbols={'': 'val1'})
        with self.assertRaisesRegex(
                ilp.IlpError,
                'Bad string "sym.bol": '
                'Column names can\'t contain a \'.\' character, '
                'which was found at byte position 3.'):
            buf.row('tbl1', symbols={'sym.bol': 'val1'})

    def test_column(self):
        two_h_after_epoch = datetime.datetime(
            1970, 1, 1, 2, tzinfo=datetime.timezone.utc)
        buf = ilp.Buffer()
        buf.row('tbl1', columns={
            'col1': True,
            'col2': False,
            'col3': -1,
            'col4': 0.5,
            'col5': 'val',
            'col6': ilp.TimestampMicros(12345),
            'col7': two_h_after_epoch})
        exp = (
            'tbl1 col1=t,col2=f,col3=-1i,col4=0.5,'
            'col5="val",col6=12345t,col7=7200000000t\n')
        self.assertEqual(str(buf), exp)


class TestSender(unittest.TestCase):
    def test_basic(self):
        with Server() as server, ilp.Sender('localhost', server.port) as sender:
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
                at=ilp.TimestampNanos(111222233333))
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
                sender = ilp.Sender('localhost', server.port)
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
            sender = ilp.Sender('localhost', 12345)
            sender.row('tbl1', symbols={'sym1': 'val1'})
            with self.assertRaisesRegex(ilp.IlpError, 'Not connected'):
                sender.flush()
        finally:
            sender.close()


if __name__ == '__main__':
    unittest.main()
