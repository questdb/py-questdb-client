import sys
sys.dont_write_bytecode = True
import unittest
import pathlib
import datetime

PROJ_ROOT = pathlib.Path(__file__).parent.parent
sys.path.append(str(PROJ_ROOT / 'src'))

import questdb.ilp as ilp


class TestLineSenderBuffer(unittest.TestCase):
    def test_new(self):
        buf = ilp.LineSenderBuffer()
        self.assertEqual(len(buf), 0)
        self.assertEqual(buf.capacity(), 64 * 1024)

    def test_basic(self):
        buf = ilp.LineSenderBuffer()
        buf.row('tbl1', symbols={'sym1': 'val1', 'sym2': 'val2'})
        self.assertEqual(len(buf), 25)
        self.assertEqual(str(buf), 'tbl1,sym1=val1,sym2=val2\n')

    # def test_bad_table(self):
    #     buf = ilp.LineSenderBuffer()
    #     with self.assertRaisesRegex(
    #             ilp.IlpError,
    #             'Table names must have a non-zero length'):
    #         buf.table('')
    #     with self.assertRaisesRegex(
    #             ilp.IlpError,
    #             'Bad string "x..y": Found invalid dot `.` at position 2.'):
    #         buf.table('x..y')

    # def test_symbol(self):
    #     buf = ilp.LineSenderBuffer()
    #     buf.table('tbl1')
    #     buf.symbol('sym1', 'val1')
    #     buf.symbol('sym2', 'val2')
    #     self.assertEqual(str(buf), 'tbl1,sym1=val1,sym2=val2')

    # def test_bad_symbol_column_name(self):
    #     buf = ilp.LineSenderBuffer()
    #     buf.table('tbl1')
    #     with self.assertRaisesRegex(
    #             ilp.IlpError,
    #             'Column names must have a non-zero length.'):
    #         buf.symbol('', 'val1')
    #     with self.assertRaisesRegex(
    #             ilp.IlpError,
    #             'Bad string "sym.bol": '
    #             'Column names can\'t contain a \'.\' character, '
    #             'which was found at byte position 3.'):
    #         buf.symbol('sym.bol', 'val1')

    # def test_column(self):
    #     two_h_after_epoch = datetime.datetime(
    #         1970, 1, 1, 2, tzinfo=datetime.timezone.utc)
    #     buf = ilp.LineSenderBuffer()
    #     buf.table('tbl1')
    #     buf.column('col1', True)
    #     buf.column('col2', False)
    #     buf.column('col3', -1)
    #     buf.column('col4', 0.5)
    #     buf.column('col5', 'val')
    #     buf.column('col6', ilp.TimestampMicros(12345))
    #     buf.column('col7', two_h_after_epoch)
    #     exp = (
    #         'tbl1 col1=t,col2=f,col3=-1i,col4=0.5,'
    #         'col5="val",col6=12345t,col7=7200000000t')
    #     self.assertEqual(str(buf), exp)


if __name__ == '__main__':
    unittest.main()
