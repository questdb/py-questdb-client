#!/usr/bin/env python3
"""Tests for the row egress API (`QueryResult.iter_rows` / `.columns`),
`QueryResult.exec_done`, and the per-query timeout.

Split in two:

- `TestTimeoutArgument` needs no server. The timeout argument is validated
  before a connection is borrowed, precisely so a programming error costs no
  round trip, and that is what these pin.
- Everything else drives a real QuestDB: the one at `QDB_HTTP_ADDR`, or
  under `TEST_QUESTDB_INTEGRATION=1` (the `test.py` integration run) the
  system-test fixture's server.
  The wire encoding of `timeout_ms` and the capability gating are covered
  exhaustively in the Rust and C++ suites of c-questdb-client; what needs a
  live server is the half those cannot reach: the Python value mapping for
  every column kind, the exact QuestDB type names, and the server's own
  null and timeout behaviour.

Run the live half with:

    QDB_HTTP_ADDR=127.0.0.1:9000 python3 test/test_client_row_egress.py
"""

import sys

sys.dont_write_bytecode = True
import atexit
import datetime
import decimal
import ipaddress
import os
import unittest
import uuid

import patch_path  # noqa: F401  (puts `src` on sys.path)

import questdb as qdb


ADDR = os.environ.get('QDB_HTTP_ADDR')
TABLE = 'py_row_egress_test'

_fixture = None


def _live_addr():
    global _fixture
    if ADDR:
        return ADDR
    if os.environ.get('TEST_QUESTDB_INTEGRATION') != '1':
        raise unittest.SkipTest(
            'set QDB_HTTP_ADDR=host:port for a running QuestDB')
    if _fixture is None:
        import system_test
        system_test.may_install_questdb()
        _fixture = system_test.QuestDbFixture(
            system_test.QUESTDB_PLAIN_INSTALL_PATH, http=True)
        _fixture.start()
        atexit.register(_fixture.stop)
    return f'{_fixture.host}:{_fixture.http_server_port}'


class _LiveCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.addr = _live_addr()
        cls.db = qdb.connect(f'ws::addr={cls.addr};')

    @classmethod
    def tearDownClass(cls):
        cls.db.close()


class TestTimeoutArgument(unittest.TestCase):
    """`timeout=` validation, which must not need a server."""

    def _db(self):
        # A closed port with `lazy_connect`: reaching the network at all
        # would hang or raise a transport error, so any exception these
        # tests see is the argument check doing its job.
        return qdb.connect('ws::addr=127.0.0.1:1;lazy_connect=on;')

    def test_negative_int_rejected(self):
        with self._db() as db:
            with self.assertRaises(ValueError):
                db.query('select 1', timeout=-1)

    def test_negative_timedelta_rejected(self):
        with self._db() as db:
            with self.assertRaises(ValueError):
                db.query(
                    'select 1', timeout=datetime.timedelta(seconds=-1))

    def test_non_integer_rejected(self):
        with self._db() as db:
            for bad in ('5s', 1.5, b'5', [5], object()):
                with self.subTest(bad=bad):
                    with self.assertRaises(TypeError):
                        db.query('select 1', timeout=bad)

    def test_bool_rejected(self):
        # `True` is an int in Python; accepting it would silently mean a
        # 1 ms timeout, which is never what a caller writing `timeout=True`
        # intended.
        with self._db() as db:
            with self.assertRaises(TypeError):
                db.query('select 1', timeout=True)

    def test_execute_validates_too(self):
        with self._db() as db:
            with self.assertRaises(ValueError):
                db.execute('select 1', timeout=-1)

    def test_conversion(self):
        conv = qdb._client._debug_query_timeout_to_millis
        td = datetime.timedelta
        cases = [
            (None, -1),
            (0, 0),
            (250, 250),
            (td(0), 0),
            (td(microseconds=1), 1),
            (td(microseconds=999), 1),
            (td(milliseconds=1001), 1001),
            (td(milliseconds=1001, microseconds=999), 1001),
            (td(days=1, milliseconds=3), 86_400_003),
            (2 ** 63 - 1, 2 ** 63 - 1),
        ]
        for value, expected in cases:
            with self.subTest(value=value):
                self.assertEqual(conv(value), expected)

    def test_conversion_is_exact_for_every_millisecond(self):
        conv = qdb._client._debug_query_timeout_to_millis
        for ms in range(1, 100_000):
            self.assertEqual(
                conv(datetime.timedelta(milliseconds=ms)), ms)

    def test_timeout_beyond_int64_rejected(self):
        with self._db() as db:
            for bad in (2 ** 63, 2 ** 64, 2 ** 70):
                with self.subTest(bad=bad):
                    with self.assertRaises(ValueError):
                        db.query('select 1', timeout=bad)

    def test_unknown_connect_string_key_still_rejected(self):
        # The new key must not have widened the parser.
        with self.assertRaises(qdb.QuestDBError):
            qdb.connect('ws::addr=127.0.0.1:1;query_timeout=1000;')

    def test_connect_string_key_accepted(self):
        # `query_timeout_ms` has to be accepted by BOTH roles: one connect
        # string configures the sender pool and the reader pool.
        with qdb.connect(
                'ws::addr=127.0.0.1:1;query_timeout_ms=1000;'
                'lazy_connect=on;') as db:
            self.assertIsNotNone(db)


class TestRowEgressLive(_LiveCase):
    """Value mapping and type names against a real server."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.db.execute(f'drop table if exists {TABLE}')
        cls.db.execute(
            f'create table {TABLE} ('
            'ts timestamp, sym symbol, px double, n long, u uuid, c char, '
            'v varchar, b boolean, d date, g geohash(8c), gb geohash(3b), '
            'ip ipv4, dec decimal(10,2), l256 long256, f float, sh short, '
            'by byte, i int'
            ') timestamp(ts) partition by day wal')
        cls.db.execute(
            f"insert into {TABLE} values "
            "('2024-01-01T00:00:00.000001Z','AAPL',1.5,7,"
            "'123e4567-e89b-12d3-a456-426614174000','X','hello',true,"
            "'2024-01-01T00:00:00.000Z',#u4yuquw4,##101,'10.0.0.1',"
            "cast(12.34 as decimal(10,2)),cast('0x123456789a' as long256),"
            "2.5,3,4,5)")
        cls.db.execute(
            f"insert into {TABLE} values "
            "('2024-01-02T00:00:00.000000Z',null,null,null,null,null,null,"
            "null,null,null,null,null,null,null,null,null,null,null)")
        cls.db.query(f"select wait_wal_table('{TABLE}')")._drain()

    @classmethod
    def tearDownClass(cls):
        try:
            cls.db.execute(f'drop table if exists {TABLE}')
        finally:
            super().tearDownClass()

    def _one(self, sql, binds=None):
        with self.db.query(sql, binds) as result:
            rows = list(result.iter_rows())
        self.assertEqual(len(rows), 1, sql)
        return rows[0]

    # --- columns() -------------------------------------------------------

    def test_column_names_and_types(self):
        with self.db.query(f'select * from {TABLE} limit 0') as result:
            cols = result.columns()
            self.assertEqual(list(result.iter_rows()), [])
        self.assertEqual(cols, [
            ('ts', 'TIMESTAMP'),
            ('sym', 'SYMBOL'),
            ('px', 'DOUBLE'),
            ('n', 'LONG'),
            ('u', 'UUID'),
            ('c', 'CHAR'),
            ('v', 'VARCHAR'),
            ('b', 'BOOLEAN'),
            ('d', 'DATE'),
            ('g', 'GEOHASH(8c)'),
            ('gb', 'GEOHASH(3b)'),
            ('ip', 'IPv4'),
            # The wire carries only the scale, so the declared
            # DECIMAL(10,2) is reported at its storage width's precision.
            ('dec', 'DECIMAL(18,2)'),
            ('l256', 'LONG256'),
            ('f', 'FLOAT'),
            ('sh', 'SHORT'),
            ('by', 'BYTE'),
            ('i', 'INT'),
        ])

    def test_columns_then_iter_rows_shares_one_stream(self):
        # A DB-API caller reads `description` before fetching; asking for
        # the columns must not consume the rows.
        with self.db.query(f'select n from {TABLE} order by ts') as result:
            self.assertEqual(result.columns(), [('n', 'LONG')])
            self.assertEqual(list(result.iter_rows()), [(7,), (None,)])

    def test_columns_is_repeatable(self):
        with self.db.query('select 1 as a') as result:
            self.assertEqual(result.columns(), result.columns())

    def test_timestamp_ns_type_name(self):
        row_type = self.db.query(
            "select cast(0 as timestamp_ns) as t").columns()
        self.assertEqual(row_type, [('t', 'TIMESTAMP_NS')])

    def test_array_type_name(self):
        self.assertEqual(
            self.db.query(
                "select ARRAY[1.0, 2.0] as a").columns(),
            [('a', 'DOUBLE[]')])

    # --- iter_rows() values ---------------------------------------------

    def test_values_round_trip(self):
        row = self._one(f'select * from {TABLE} where n = 7')
        (ts, sym, px, n, u, c, v, b, d, g, gb, ip, dec, l256, f, sh, by,
         i) = row
        self.assertEqual(ts, datetime.datetime(
            2024, 1, 1, 0, 0, 0, 1, tzinfo=datetime.timezone.utc))
        self.assertEqual(sym, 'AAPL')
        self.assertEqual(px, 1.5)
        self.assertEqual(n, 7)
        # Canonical RFC-4122 order, matching the server's own text output
        # and this driver's Arrow path.
        self.assertEqual(
            u, uuid.UUID('123e4567-e89b-12d3-a456-426614174000'))
        self.assertEqual(c, 'X')
        self.assertEqual(v, 'hello')
        self.assertIs(b, True)
        self.assertEqual(d, datetime.datetime(
            2024, 1, 1, tzinfo=datetime.timezone.utc))
        self.assertEqual(g, 'u4yuquw4')
        self.assertEqual(gb, '101')
        self.assertEqual(ip, ipaddress.IPv4Address('10.0.0.1'))
        self.assertEqual(dec, decimal.Decimal('12.34'))
        self.assertEqual(l256, 0x123456789a)
        self.assertEqual(f, 2.5)
        self.assertEqual(sh, 3)
        self.assertEqual(by, 4)
        self.assertEqual(i, 5)

    def test_value_python_types(self):
        row = self._one(f'select * from {TABLE} where n = 7')
        for value, want in zip(row, [
                datetime.datetime, str, float, int, uuid.UUID, str, str,
                bool, datetime.datetime, str, str,
                ipaddress.IPv4Address, decimal.Decimal, int, float, int,
                int, int]):
            self.assertIsInstance(value, want)

    def test_timestamps_are_utc_aware(self):
        ts, d = self._one(f'select ts, d from {TABLE} where n = 7')
        for value in (ts, d):
            self.assertIsNotNone(value.tzinfo)
            self.assertEqual(value.utcoffset(), datetime.timedelta(0))

    def test_decimal_keeps_its_scale(self):
        # `Decimal('12.34')` and `Decimal('12.340')` are equal but not
        # identical as text; the scale is what a contract check compares.
        dec, = self._one(f'select dec from {TABLE} where n = 7')
        self.assertEqual(str(dec), '12.34')

    def test_timestamp_ns_rounds_down_to_microseconds(self):
        # `datetime` has no nanosecond field. The value is rounded towards
        # the past on both sides of the epoch, never to nearest.
        utc = datetime.timezone.utc
        value, = self._one("select cast(1500 as timestamp_ns) as t")
        self.assertEqual(
            value, datetime.datetime(1970, 1, 1, 0, 0, 0, 1, tzinfo=utc))
        value, = self._one("select cast(-1500 as timestamp_ns) as t")
        self.assertEqual(
            value,
            datetime.datetime(1969, 12, 31, 23, 59, 59, 999998, tzinfo=utc))

    def test_pre_epoch_temporals(self):
        utc = datetime.timezone.utc
        ts, d = self._one(
            "select cast(-1 as timestamp) as ts, cast(-1 as date) as d")
        self.assertEqual(
            ts,
            datetime.datetime(1969, 12, 31, 23, 59, 59, 999999, tzinfo=utc))
        self.assertEqual(
            d,
            datetime.datetime(1969, 12, 31, 23, 59, 59, 999000, tzinfo=utc))

    def test_datetime_bounds_decode(self):
        utc = datetime.timezone.utc
        lo, hi = self._one(
            'select cast(-62135596800000000 as timestamp) as lo, '
            'cast(253402300799999999 as timestamp) as hi')
        self.assertEqual(lo, datetime.datetime(1, 1, 1, tzinfo=utc))
        self.assertEqual(
            hi, datetime.datetime(9999, 12, 31, 23, 59, 59, 999999, tzinfo=utc))
        lo, hi = self._one(
            'select cast(-62135596800000 as date) as lo, '
            'cast(253402300799999 as date) as hi')
        self.assertEqual(lo, datetime.datetime(1, 1, 1, tzinfo=utc))
        self.assertEqual(
            hi, datetime.datetime(9999, 12, 31, 23, 59, 59, 999000, tzinfo=utc))

    def test_temporal_outside_datetime_range_is_a_clear_error(self):
        # Year 10000 is a valid QuestDB timestamp but not a `datetime`; the
        # int64 extremes exercise the overflow guards on the way to it.
        for sql in (
                'select cast(253402300800000000 as timestamp) as t',
                'select cast(-62135596800000001 as timestamp) as t',
                'select cast(253402300800000 as date) as t',
                'select cast(-62135596800001 as date) as t',
                'select cast(9223372036854775807 as timestamp) as t',
                'select cast(-9223372036854775807 as timestamp) as t',
                'select cast(9223372036854775807 as date) as t',
                'select cast(-9223372036854775807 as date) as t'):
            with self.subTest(sql=sql):
                with self.assertRaises(qdb.QuestDBError) as ctx:
                    self._one(sql)
                self.assertEqual(
                    ctx.exception.code, qdb.QuestDBErrorCode.InvalidTimestamp)
                self.assertIn("'t'", str(ctx.exception))
        self.assertEqual(self._one('select 1 as a'), (1,))

    def test_edge_values(self):
        cases = [
            ('select cast(-12.34 as decimal(10,2)) as v',
             decimal.Decimal('-12.34')),
            ('select cast(7 as decimal(10,0)) as v', decimal.Decimal('7')),
            ("select cast('123456789012345678901234567.8' "
             "as decimal(30,1)) as v",
             decimal.Decimal('123456789012345678901234567.8')),
            ("select cast('-1234567890123456789012345678901234567890"
             "12345678901234.5' as decimal(60,1)) as v",
             decimal.Decimal('-1234567890123456789012345678901234567890'
                             '12345678901234.5')),
            ("select cast('0x0102030405060708090a0b0c0d0e0f101112131415"
             "161718191a1b1c1d1e1f20' as long256) as v",
             0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20),
            ("select cast('1.2.3.4' as ipv4) as v",
             ipaddress.IPv4Address('1.2.3.4')),
            ('select cast(-5 as byte) as v', -5),
            ('select #sp052w92bcde as v', 'sp052w92bcde'),
            ('select ##1 as v', '1'),
            ("select 'héllo, 🌍' as v", 'héllo, 🌍'),
            ("select cast('é' as char) as v", 'é'),
        ]
        for sql, expected in cases:
            with self.subTest(sql=sql):
                value, = self._one(sql)
                self.assertEqual(value, expected)
                self.assertIs(type(value), type(expected))

    def test_geohash_bit_width_renders_as_bits(self):
        value, = self._one('select ##1011 as g')
        self.assertEqual(value, '1011')

    def test_geohash_char_width_renders_as_base32(self):
        value, = self._one('select #sp052w92 as g')
        self.assertEqual(value, 'sp052w92')

    def test_binary_is_bytes(self):
        value, = self._one("select cast(null as binary) as b")
        self.assertIsNone(value)
        value, = self._one("select rnd_bin(4, 4, 0) as b")
        self.assertIsInstance(value, bytes)
        self.assertEqual(len(value), 4)

    def test_array_is_ndarray(self):
        import numpy as np
        value, = self._one('select ARRAY[1.0, 2.0, 3.0] as a')
        self.assertIsInstance(value, np.ndarray)
        self.assertEqual(list(value), [1.0, 2.0, 3.0])

    # --- nulls -----------------------------------------------------------

    def test_nullable_columns_yield_none(self):
        row = self._one(
            f'select sym, px, n, u, c, v, d, g, ip, dec, l256, f, i '
            f'from {TABLE} where sym is null')
        for value in row:
            self.assertIsNone(value)

    def test_types_without_a_wire_null(self):
        # BOOLEAN, BYTE and SHORT have no NULL representation in QuestDB
        # (the egress spec marks the row non-null and ships the zero), so
        # an inserted `null` reads back as the zero value, not None. Pinned
        # because it surprises people, not because it is wrong.
        b, by, sh = self._one(
            f'select b, by, sh from {TABLE} where sym is null')
        self.assertIs(b, False)
        self.assertEqual(by, 0)
        self.assertEqual(sh, 0)

    def test_char_zero_reads_as_none(self):
        # QuestDB's CHAR null is code point 0 and rides as a non-null row;
        # PGWire renders it as NULL, so the row path matches that.
        value, = self._one(f'select c from {TABLE} where sym is null')
        self.assertIsNone(value)

    def test_float_null_is_none_not_nan(self):
        # The server turns NaN into a validity bit, so a NaN never reaches
        # the values buffer and null/NaN are one thing on the wire.
        px, f = self._one(
            f'select px, f from {TABLE} where sym is null')
        self.assertIsNone(px)
        self.assertIsNone(f)

    # --- stream semantics -----------------------------------------------

    def test_empty_result_has_columns_but_no_rows(self):
        with self.db.query(f'select * from {TABLE} where false') as result:
            self.assertTrue(result.columns())
            self.assertEqual(list(result.iter_rows()), [])

    def test_iter_rows_is_single_use(self):
        with self.db.query('select 1 as a') as result:
            self.assertEqual(list(result.iter_rows()), [(1,)])
            # The same exhausted iterator, not a fresh stream.
            self.assertEqual(list(result.iter_rows()), [])

    def test_iter_rows_after_another_materialisation_raises(self):
        with self.db.query('select 1 as a') as result:
            result._drain()
            with self.assertRaises(qdb.QuestDBError):
                list(result.iter_rows())

    def test_iter_rows_streams_many_batches(self):
        # Enough rows to cross batch boundaries, so the per-batch decode
        # and the symbol-dictionary carry-over both get exercised.
        n = 20_000
        with self.db.query(
                "select x, cast(x % 97 as symbol) as s "
                f"from long_sequence({n})") as result:
            total = 0
            last = None
            for x, s in result.iter_rows():
                total += 1
                last = (x, s)
        self.assertEqual(total, n)
        self.assertEqual(last, (n, str(n % 97)))

    def test_abandoned_iterator_releases_the_connection(self):
        # Walking away mid-stream must not wedge the pool: the next query
        # has to work.
        result = self.db.query(f'select x from long_sequence(100000)')
        rows = result.iter_rows()
        next(rows)
        del rows
        result.close()
        self.assertEqual(list(self.db.query('select 1 as a').iter_rows()),
                         [(1,)])

    def test_iterator_is_invalid_after_close(self):
        result = self.db.query('select x from long_sequence(3)')
        rows = result.iter_rows()
        result.close()
        with self.assertRaises(qdb.QuestDBError):
            next(rows)

    def test_iterator_is_invalid_after_close_mid_stream(self):
        # Rows decoded ahead of the consumer must not keep flowing after
        # `close()`, whichever batch they sit in.
        result = self.db.query('select x from long_sequence(100000)')
        rows = result.iter_rows()
        for _ in range(50000):
            next(rows)
        result.close()
        with self.assertRaises(qdb.QuestDBError):
            next(rows)

    def test_columns_do_not_fail_on_undecodable_data(self):
        # A schema probe must not depend on the values; the decode error
        # belongs to iteration.
        sql = ('select case when x = 3 '
               'then cast(253402300800000000 as timestamp) '
               'else cast(x as timestamp) end as t from long_sequence(5)')
        with self.db.query(sql) as result:
            self.assertEqual(result.columns(), [('t', 'TIMESTAMP')])
            with self.assertRaises(qdb.QuestDBError) as ctx:
                list(result.iter_rows())
        self.assertEqual(
            ctx.exception.code, qdb.QuestDBErrorCode.InvalidTimestamp)

    def test_schema_probe_keeps_the_lease_usable(self):
        # `columns()` on a `LIMIT 0` probe must read the result to its end,
        # or closing it tears the lease's connection down.
        with self.db.reader() as r:
            for _ in range(3):
                with r.query(f'select * from {TABLE} limit 0') as result:
                    self.assertEqual(len(result.columns()), 18)
            self.assertEqual(
                list(r.query('select 1 as a').iter_rows()), [(1,)])

    def test_dropped_row_result_does_not_wedge_the_lease(self):
        # Dropping an un-iterated row result must free its cursor, so the
        # lease reports a torn-down connection rather than a result that
        # is "still open" forever.
        import gc
        with self.db.reader() as r:
            r.query('select x from long_sequence(100000)').columns()
            gc.collect()
            with self.assertRaises(qdb.QuestDBError) as ctx:
                r.query('select 1 as a')
            self.assertIn('terminal', str(ctx.exception))

    def test_close_is_idempotent_after_a_non_select(self):
        # A non-SELECT reaches its terminal inside `columns()`, which frees
        # the cursor; `close()` must still be a no-op rather than a crash.
        result = self.db.query('drop table if exists py_no_such_table')
        self.assertEqual(result.columns(), [])
        self.assertIsNotNone(result.exec_done)
        result.close()
        result.close()

    def test_cancel_then_close_mid_stream(self):
        result = self.db.query('select x from long_sequence(100000)')
        rows = result.iter_rows()
        next(rows)
        result.cancel()
        result.close()
        self.assertEqual(
            list(self.db.query('select 1 as a').iter_rows()), [(1,)])

    def test_close_mid_stream_frees_the_connection(self):
        result = self.db.query('select x from long_sequence(100000)')
        rows = result.iter_rows()
        next(rows)
        result.close()
        self.assertEqual(
            list(self.db.query('select 1 as a').iter_rows()), [(1,)])

    def test_no_pyarrow_needed(self):
        # The row path must not import pyarrow — that is the whole point of
        # building it on the raw batch API.
        import subprocess
        code = (
            'import sys; sys.path.insert(0, "src");\n'
            'import builtins\n'
            'real = builtins.__import__\n'
            'def guard(name, *a, **k):\n'
            '    if name.split(".")[0] == "pyarrow":\n'
            '        raise AssertionError("pyarrow imported")\n'
            '    return real(name, *a, **k)\n'
            'builtins.__import__ = guard\n'
            'import questdb\n'
            f'db = questdb.connect("ws::addr={self.addr};")\n'
            'with db.query("select 1 as a") as r:\n'
            '    assert r.columns() == [("a", "INT")]\n'
            '    assert list(r.iter_rows()) == [(1,)]\n'
            'db.close()\n')
        subprocess.run(
            [sys.executable, '-c', code], check=True,
            cwd=str(patch_path.PROJ_ROOT))

    # --- exec_done -------------------------------------------------------

    def test_exec_done_is_none_for_a_select(self):
        with self.db.query('select 1 as a') as result:
            list(result.iter_rows())
            self.assertIsNone(result.exec_done)

    def test_exec_done_before_drain_is_none(self):
        with self.db.query('select 1 as a') as result:
            self.assertIsNone(result.exec_done)
            list(result.iter_rows())

    def test_execute_returns_insert_row_count(self):
        self.db.execute('drop table if exists py_exec_done_test')
        self.db.execute(
            'create table py_exec_done_test (a int, b int)')
        try:
            done = self.db.execute(
                'insert into py_exec_done_test values (1,1),(2,2),(3,3)')
            self.assertIsNotNone(done)
            op_type, rows = done
            self.assertIsInstance(op_type, int)
            self.assertEqual(rows, 3)

            done = self.db.execute(
                'update py_exec_done_test set b = 9 where a > 1')
            self.assertEqual(done[1], 2)
        finally:
            self.db.execute('drop table if exists py_exec_done_test')

    def test_exec_done_survives_close(self):
        self.db.execute('drop table if exists py_exec_done_close')
        self.db.execute('create table py_exec_done_close (a int)')
        try:
            with self.db.query(
                    'insert into py_exec_done_close values (1),(2)') as result:
                list(result.iter_rows())
            self.assertEqual(result.exec_done[1], 2)
            result.close()
            self.assertEqual(result.exec_done[1], 2)
        finally:
            self.db.execute('drop table if exists py_exec_done_close')

    def test_exec_done_through_every_drain_path(self):
        try:
            import pandas  # noqa: F401
            import pyarrow  # noqa: F401
        except ImportError:
            self.skipTest('needs pandas and pyarrow')
        self.db.execute('drop table if exists py_exec_done_paths')
        self.db.execute('create table py_exec_done_paths (a int)')
        try:
            drains = {
                'to_pandas': lambda r: r.to_pandas(),
                'to_arrow': lambda r: r.to_arrow(),
                'iter_arrow': lambda r: list(r.iter_arrow()),
                'iter_pandas': lambda r: list(r.iter_pandas()),
                'cancel': lambda r: r.cancel(),
            }
            for name, drain in drains.items():
                with self.subTest(path=name):
                    with self.db.query(
                            'insert into py_exec_done_paths values (1)'
                            ) as result:
                        drain(result)
                    self.assertEqual(result.exec_done[1], 1)
        finally:
            self.db.execute('drop table if exists py_exec_done_paths')

    def test_rows_affected_is_none_when_the_server_reports_none(self):
        self.db.execute('drop table if exists py_exec_done_truncate')
        self.db.execute('create table py_exec_done_truncate (a int)')
        try:
            op_type, rows = self.db.execute(
                'truncate table py_exec_done_truncate')
            self.assertIsInstance(op_type, int)
            self.assertIsNone(rows)
        finally:
            self.db.execute('drop table if exists py_exec_done_truncate')

    def test_execute_on_a_select_returns_none(self):
        self.assertIsNone(self.db.execute('select 1'))

    def test_non_select_has_no_columns(self):
        with self.db.query(
                'drop table if exists py_no_such_table') as result:
            self.assertEqual(result.columns(), [])
            self.assertEqual(list(result.iter_rows()), [])
            self.assertIsNotNone(result.exec_done)

    def test_pooled_reader_execute_returns_exec_done(self):
        with self.db.reader() as r:
            r.execute('drop table if exists py_lease_exec_test')
            r.execute('create table py_lease_exec_test (a int)')
            try:
                done = r.execute(
                    'insert into py_lease_exec_test values (1),(2)')
                self.assertEqual(done[1], 2)
            finally:
                r.execute('drop table if exists py_lease_exec_test')

class TestQueryTimeoutLive(_LiveCase):
    """`timeout=` against a server advertising ``CAP_QUERY_TIMEOUT``."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        caps = cls.db.server_info().capabilities
        if not caps & 0x08:
            cls.db.close()
            raise unittest.SkipTest(
                'server does not advertise CAP_QUERY_TIMEOUT (capabilities '
                f'0x{caps:08X}); it predates questdb/questdb#7768')

    def test_timeout_expiry_raises_query_timeout(self):
        with self.assertRaises(qdb.QuestDBError) as ctx:
            with self.db.query('select * from sleep(60000)', timeout=200) as r:
                list(r.iter_rows())
        self.assertEqual(ctx.exception.code, qdb.QuestDBErrorCode.QueryTimeout)

    def test_connection_survives_a_timeout(self):
        # QUERY_TIMEOUT is per-query: the pooled connection stays usable,
        # which is what keeps a timeout from poisoning a dbt thread.
        with self.assertRaises(qdb.QuestDBError):
            with self.db.query('select * from sleep(60000)', timeout=200) as r:
                list(r.iter_rows())
        self.assertEqual(
            list(self.db.query('select 1 as a').iter_rows()), [(1,)])

    def test_timedelta_timeout_accepted(self):
        with self.assertRaises(qdb.QuestDBError) as ctx:
            with self.db.query(
                    'select * from sleep(60000)',
                    timeout=datetime.timedelta(milliseconds=200)) as r:
                list(r.iter_rows())
        self.assertEqual(ctx.exception.code, qdb.QuestDBErrorCode.QueryTimeout)

    def test_generous_timeout_does_not_interfere(self):
        # A timeout the query comfortably beats must be invisible: same
        # rows, no error. Guards against the field being mis-encoded into a
        # value the server reads as "already expired".
        with self.db.query('select 1 as a', timeout=30_000) as r:
            self.assertEqual(list(r.iter_rows()), [(1,)])
        with self.db.query(
                'select count() from long_sequence(1000)',
                timeout=30_000) as r:
            self.assertEqual(list(r.iter_rows()), [(1000,)])

    def test_no_timeout_leaves_the_query_alone(self):
        with self.db.query('select 1 as a') as r:
            self.assertEqual(list(r.iter_rows()), [(1,)])

    def test_connect_string_timeout_applies_to_every_query(self):
        with qdb.connect(f'ws::addr={self.addr};query_timeout_ms=200;') as db:
            for _ in range(2):
                with self.assertRaises(qdb.QuestDBError) as ctx:
                    with db.query('select * from sleep(60000)') as r:
                        list(r.iter_rows())
                self.assertEqual(
                    ctx.exception.code, qdb.QuestDBErrorCode.QueryTimeout)
            # A short query is unaffected.
            self.assertEqual(list(db.query('select 1 as a').iter_rows()),
                             [(1,)])

    def test_per_query_timeout_overrides_the_connect_string(self):
        # `sleep` is an interruptible cursor that never finishes, so the
        # observable signal is *when* it is cut off. A connect-string
        # default of 300 ms with a per-query 3 s must be cut off at ~3 s:
        # the per-query value replaced the default rather than being
        # ignored or combined with it.
        import time
        with qdb.connect(f'ws::addr={self.addr};query_timeout_ms=300;') as db:
            started = time.monotonic()
            with self.assertRaises(qdb.QuestDBError) as ctx:
                with db.query(
                        'select * from sleep(60000)', timeout=3_000) as r:
                    list(r.iter_rows())
            elapsed = time.monotonic() - started
            self.assertEqual(
                ctx.exception.code, qdb.QuestDBErrorCode.QueryTimeout)
            self.assertGreater(
                elapsed, 1.5,
                'the 300 ms connect-string default was applied instead of '
                f'the per-query 3 s (cut off after {elapsed:.2f}s)')
            self.assertLess(elapsed, 20.0)

    def test_per_query_timeout_zero_clears_the_connect_string(self):
        # A query of a few hundred milliseconds against a 20 ms default:
        # `timeout=None` keeps the default and is cut off, `timeout=0` lifts
        # it and the query completes.
        sql = ('select count() from long_sequence(100000000) '
               'where x % 7 = 0')
        with qdb.connect(f'ws::addr={self.addr};query_timeout_ms=20;') as db:
            with self.assertRaises(qdb.QuestDBError) as ctx:
                with db.query(sql, timeout=None) as r:
                    list(r.iter_rows())
            self.assertEqual(
                ctx.exception.code, qdb.QuestDBErrorCode.QueryTimeout)
            with db.query(sql, timeout=0) as r:
                self.assertEqual(list(r.iter_rows()), [(14285714,)])
            self.assertIsNone(db.execute(sql, timeout=0))

    def test_lease_survives_a_timeout(self):
        # The server answers a timed-out query with a terminal frame on a
        # healthy connection, so the lease must stay usable: a dbt thread
        # keeps its lease across a model that ran over its budget.
        with self.db.reader() as r:
            for _ in range(2):
                with self.assertRaises(qdb.QuestDBError) as ctx:
                    r.execute('select * from sleep(60000)', timeout=200)
                self.assertEqual(
                    ctx.exception.code, qdb.QuestDBErrorCode.QueryTimeout)
                self.assertEqual(
                    list(r.query('select 1 as a').iter_rows()), [(1,)])

    def test_timeout_returns_the_reader_to_the_pool(self):
        stats = qdb._client._debug_egress_pool_stats
        with qdb.connect(f'ws::addr={self.addr};') as db:
            list(db.query('select 1 as a').iter_rows())
            self.assertEqual(stats(db), (0, 1))
            with self.assertRaises(qdb.QuestDBError):
                db.execute('select * from sleep(60000)', timeout=200)
            self.assertEqual(stats(db), (0, 1))

    def test_timeout_on_execute(self):
        with self.assertRaises(qdb.QuestDBError) as ctx:
            self.db.execute('select * from sleep(60000)', timeout=200)
        self.assertEqual(ctx.exception.code, qdb.QuestDBErrorCode.QueryTimeout)

    def test_pooled_reader_rejects_a_bad_timeout_and_stays_usable(self):
        with self.db.reader() as r:
            with self.assertRaises(ValueError):
                r.query('select 1', timeout=-1)
            with self.assertRaises(TypeError):
                r.execute('select 1', timeout='1s')
            self.assertEqual(
                list(r.query('select 1 as a').iter_rows()), [(1,)])

    def test_timeout_on_a_pooled_reader(self):
        with self.db.reader() as r:
            with self.assertRaises(qdb.QuestDBError) as ctx:
                with r.query('select * from sleep(60000)', timeout=200) as result:
                    list(result.iter_rows())
            self.assertEqual(
                ctx.exception.code, qdb.QuestDBErrorCode.QueryTimeout)


class TestUuidByteOrder(_LiveCase):
    """Every route a UUID takes has to agree on canonical RFC-4122 order.

    The native client took standard byte order in c-questdb-client #186 and
    byte-swaps to QWP wire order itself. Three Python-side paths kept
    pre-swapping on top of that, which reversed the 16 bytes: the bind, the
    numpy reader behind `to_pandas`, and a dataframe UUID column. Each test
    here checks against the server's own text form, the one arbiter that
    cannot itself be byte-reversed.
    """

    U = uuid.UUID('123e4567-e89b-12d3-a456-426614174000')
    def _new_table(self):
        table = f'{TABLE}_uuid_{uuid.uuid4().hex[:8]}'
        self.addCleanup(self.db.execute, f'drop table if exists {table}')
        return table

    def _fresh_table(self, columns):
        table = self._new_table()
        self.db.execute(
            f'create table {table} ({columns}, ts timestamp) '
            'timestamp(ts) partition by day wal')
        return table

    def _wait(self, table):
        self.db.query(f"select wait_wal_table('{table}')")._drain()

    def test_bound_uuid_reaches_the_server_unchanged(self):
        rows = list(self.db.query(
            'select cast($1 as varchar) as s', [self.U]).iter_rows())
        self.assertEqual(rows[0][0], str(self.U))

    def test_bound_uuid_round_trips(self):
        rows = list(self.db.query('select $1 as u', [self.U]).iter_rows())
        self.assertEqual(rows[0][0], self.U)

    def test_a_bound_uuid_matches_the_same_value_in_sql(self):
        # The predicate an incremental merge or a seed lookup is built on.
        rows = list(self.db.query(
            f"select $1 = cast('{self.U}' as uuid) as eq",
            [self.U]).iter_rows())
        self.assertIs(rows[0][0], True)

    def test_egress_uuid_matches_the_server_text_form(self):
        rows = list(self.db.query(
            f"select cast('{self.U}' as uuid) as u, "
            f"cast(cast('{self.U}' as uuid) as varchar) as s").iter_rows())
        self.assertEqual(str(rows[0][0]), rows[0][1])

    def test_to_pandas_agrees_with_the_row_path(self):
        try:
            import pandas  # noqa: F401
            import pyarrow  # noqa: F401
        except ImportError:
            self.skipTest('needs pandas and pyarrow')
        sql = f"select cast('{self.U}' as uuid) as u"
        df = self.db.query(sql).to_pandas()
        self.assertEqual(df['u'][0], self.U)
        self.assertEqual(
            df['u'][0], list(self.db.query(sql).iter_rows())[0][0])

    def test_dataframe_uuid_column_round_trips(self):
        try:
            import pandas as pd
        except ImportError:
            self.skipTest('needs pandas')
        table = self._fresh_table('u uuid')
        self.db.dataframe(
            pd.DataFrame({'u': [self.U, None]}),
            table_name=table, at=qdb.ServerTimestamp)
        self._wait(table)
        got = dict(self.db.query(
            f'select u, cast(u as varchar) as s from {table}').iter_rows())
        self.assertEqual(got.get(self.U), str(self.U))
        self.assertIn(None, got)

    def test_arrow_fixed_size_binary_uuid_column(self):
        # `pa.binary(16)` holds `UUID.bytes` and lands as that UUID.
        try:
            import pandas as pd
            import pyarrow as pa
        except ImportError:
            self.skipTest('needs pandas and pyarrow')
        table = self._fresh_table('u uuid')
        col = pd.Series(
            pa.array([self.U.bytes, None], type=pa.binary(16)),
            dtype=pd.ArrowDtype(pa.binary(16)))
        self.db.dataframe(
            pd.DataFrame({'u': col}),
            table_name=table, at=qdb.ServerTimestamp)
        self._wait(table)
        got = dict(self.db.query(
            f'select u, cast(u as varchar) as s from {table}').iter_rows())
        self.assertEqual(got.get(self.U), str(self.U))
        self.assertIn(None, got)
        table = self.db.query(f'select u from {table}').to_arrow()
        storage = table.column('u').combine_chunks()
        if isinstance(storage.type, pa.BaseExtensionType):
            storage = storage.storage
        self.assertIn(self.U.bytes, storage.to_pylist())

    def test_arrow_fixed_size_binary_long256_column(self):
        try:
            import pandas as pd
            import pyarrow as pa
        except ImportError:
            self.skipTest('needs pandas and pyarrow')
        table = self._fresh_table('v long256')
        raw = bytes(range(1, 33))
        col = pd.Series(
            pa.array([raw], type=pa.binary(32)),
            dtype=pd.ArrowDtype(pa.binary(32)))
        self.db.dataframe(
            pd.DataFrame({'v': col}),
            table_name=table, at=qdb.ServerTimestamp)
        self._wait(table)
        value, = list(self.db.query(f'select v from {table}').iter_rows())
        self.assertEqual(value[0], int.from_bytes(raw, 'little'))

    def test_dataframe_planner_path_claims_fixed_size_binary(self):
        # A numpy column alongside the Arrow one routes the frame through
        # the per-column planner rather than the capsule path.
        try:
            import pandas as pd
            import pyarrow as pa
        except ImportError:
            self.skipTest('needs pandas and pyarrow')
        table = self._fresh_table('u uuid, v long256, n long')
        raw = bytes(range(32, 64))
        self.db.dataframe(
            pd.DataFrame({
                'u': pd.Series(
                    pa.array([self.U.bytes], type=pa.binary(16)),
                    dtype=pd.ArrowDtype(pa.binary(16))),
                'v': pd.Series(
                    pa.array([raw], type=pa.binary(32)),
                    dtype=pd.ArrowDtype(pa.binary(32))),
                'n': [1]}),
            table_name=table, at=qdb.ServerTimestamp)
        self._wait(table)
        rows = list(self.db.query(
            f'select cast(u as varchar), v, n from {table}').iter_rows())
        self.assertEqual(
            rows, [(str(self.U), int.from_bytes(raw, 'little'), 1)])

    def test_arrow_table_without_a_claim_lands_as_binary(self):
        try:
            import pyarrow as pa
        except ImportError:
            self.skipTest('needs pyarrow')
        table = self._new_table()
        frame = pa.table({'u': pa.array([self.U.bytes], type=pa.binary(16))})
        self.db.dataframe(frame, table_name=table, at=qdb.ServerTimestamp)
        self._wait(table)
        with self.db.query(f'select u from {table}') as result:
            self.assertEqual(result.columns(), [('u', 'BINARY')])
            self.assertEqual(list(result.iter_rows()), [(self.U.bytes,)])

    def test_arrow_table_needs_a_uuid_claim(self):
        # Passed straight through, an unlabelled `binary(16)` is opaque
        # bytes; `schema_overrides` claims it as UUID.
        try:
            import pyarrow as pa
        except ImportError:
            self.skipTest('needs pyarrow')
        table = self._fresh_table('u uuid')
        frame = pa.table({'u': pa.array([self.U.bytes], type=pa.binary(16))})
        self.db.dataframe(
            frame, table_name=table, at=qdb.ServerTimestamp,
            schema_overrides={'u': 'uuid'})
        self._wait(table)
        rows = list(self.db.query(
            f'select cast(u as varchar) from {table}').iter_rows())
        self.assertEqual(rows, [(str(self.U),)])

    def test_a_uuid_whose_bytes_are_the_wrong_width_is_refused(self):
        # `UUID.bytes` is an overridable property and the bind reads a flat
        # 16 bytes from the pointer, so the width is checked first.
        class Narrow(uuid.UUID):
            @property
            def bytes(self):
                return b'\x00' * 15

        with self.assertRaises(ValueError):
            self.db.query('select $1 as u', [Narrow(int=0)])


if __name__ == '__main__':
    unittest.main()
