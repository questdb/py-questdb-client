#!/usr/bin/env python3
"""Tests for the row egress API (`QueryResult.iter_rows` / `.columns`),
`QueryResult.exec_done`, and the per-query timeout.

Split in two:

- `TestTimeoutArgument` needs no server. The timeout argument is validated
  before a connection is borrowed, precisely so a programming error costs no
  round trip, and that is what these pin.
- Everything else is gated on `QDB_HTTP_ADDR` and drives a real QuestDB.
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
import datetime
import decimal
import ipaddress
import os
import unittest
import uuid

import patch_path  # noqa: F401  (puts `src` on sys.path)

import questdb as qdb


ADDR = os.environ.get('QDB_HTTP_ADDR')
live = unittest.skipUnless(
    ADDR, 'set QDB_HTTP_ADDR=host:port for a running QuestDB')

TABLE = 'py_row_egress_test'


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


@live
class TestRowEgressLive(unittest.TestCase):
    """Value mapping and type names against a real server."""

    @classmethod
    def setUpClass(cls):
        cls.db = qdb.connect(f'ws::addr={ADDR};')
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
            cls.db.close()

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

    def test_timestamp_ns_truncates_to_microseconds(self):
        # `datetime` has no nanosecond field. Truncation is documented;
        # this pins it so it cannot silently become rounding.
        value, = self._one(
            "select cast(1500 as timestamp_ns) as t")
        self.assertEqual(value, datetime.datetime(
            1970, 1, 1, 0, 0, 0, 1, tzinfo=datetime.timezone.utc))

    def test_geohash_bit_width_renders_as_bits(self):
        value, = self._one('select ##1011 as g')
        self.assertEqual(value, '1011')

    def test_geohash_char_width_renders_as_base32(self):
        value, = self._one('select #sp052w92 as g')
        self.assertEqual(value, 'sp052w92')

    def test_binary_is_bytes(self):
        value, = self._one("select cast(null as binary) as b")
        self.assertIsNone(value)

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
            f'db = questdb.connect("ws::addr={ADDR};")\n'
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

    # --- per-query timeout ----------------------------------------------

    def test_server_advertises_the_timeout_capability(self):
        # Everything below depends on it; fail here with a clear message
        # rather than as a confusing timeout error.
        caps = self.db.server_info().capabilities
        self.assertTrue(
            caps & 0x08,
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
                f'select count() from {TABLE}', timeout=30_000) as r:
            self.assertEqual(len(list(r.iter_rows())), 1)

    def test_no_timeout_leaves_the_query_alone(self):
        with self.db.query('select 1 as a') as r:
            self.assertEqual(list(r.iter_rows()), [(1,)])

    def test_connect_string_timeout_applies_to_every_query(self):
        with qdb.connect(f'ws::addr={ADDR};query_timeout_ms=200;') as db:
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
        with qdb.connect(f'ws::addr={ADDR};query_timeout_ms=300;') as db:
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

    def test_per_query_timeout_zero_is_accepted(self):
        # `0` clears the default, leaving the query under the server-wide
        # `query.timeout`. That it puts no field on the wire is pinned
        # byte-exactly in the C++ mock suite; here it just has to not be
        # rejected, and a short query must still run.
        with qdb.connect(f'ws::addr={ADDR};query_timeout_ms=300;') as db:
            with db.query('select 1 as a', timeout=0) as r:
                self.assertEqual(list(r.iter_rows()), [(1,)])
            with db.query('select 1 as a', timeout=None) as r:
                self.assertEqual(list(r.iter_rows()), [(1,)])

    def test_timeout_on_execute(self):
        with self.assertRaises(qdb.QuestDBError) as ctx:
            self.db.execute('select * from sleep(60000)', timeout=200)
        self.assertEqual(ctx.exception.code, qdb.QuestDBErrorCode.QueryTimeout)

    def test_timeout_on_a_pooled_reader(self):
        with self.db.reader() as r:
            with self.assertRaises(qdb.QuestDBError) as ctx:
                with r.query('select * from sleep(60000)', timeout=200) as result:
                    list(result.iter_rows())
            self.assertEqual(
                ctx.exception.code, qdb.QuestDBErrorCode.QueryTimeout)


@live
class TestUuidByteOrder(unittest.TestCase):
    """Every route a UUID takes has to agree on canonical RFC-4122 order.

    The native client took standard byte order in c-questdb-client #186 and
    byte-swaps to QWP wire order itself. Three Python-side paths kept
    pre-swapping on top of that, which reversed the 16 bytes: the bind, the
    numpy reader behind `to_pandas`, and a dataframe UUID column. Each test
    here checks against the server's own text form, the one arbiter that
    cannot itself be byte-reversed.
    """

    U = uuid.UUID('123e4567-e89b-12d3-a456-426614174000')
    TBL = TABLE + '_uuid'

    @classmethod
    def setUpClass(cls):
        cls.db = qdb.connect(f'ws::addr={ADDR};')

    @classmethod
    def tearDownClass(cls):
        try:
            cls.db.execute(f'drop table if exists {cls.TBL}')
        finally:
            cls.db.close()

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
        self.db.execute(f'drop table if exists {self.TBL}')
        self.db.execute(
            f'create table {self.TBL} (u uuid, ts timestamp) '
            'timestamp(ts) partition by day wal')
        self.db.dataframe(
            pd.DataFrame({'u': [self.U, None]}),
            table_name=self.TBL, at=qdb.ServerTimestamp)
        self.db.query(f"select wait_wal_table('{self.TBL}')")._drain()
        got = dict(self.db.query(
            f'select u, cast(u as varchar) as s from {self.TBL}').iter_rows())
        self.assertEqual(got.get(self.U), str(self.U))
        self.assertIn(None, got)

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
