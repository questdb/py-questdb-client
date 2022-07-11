import sys
sys.dont_write_bytecode = True
import unittest
import pathlib
import datetime

PROJ_ROOT = pathlib.Path(__file__).parent.parent
sys.path.append(str(PROJ_ROOT / 'src'))

import questdb.ilp as ilp


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


if __name__ == '__main__':
    unittest.main()
