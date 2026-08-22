import sys
import time
import uuid

import pandas as pd
import pyarrow as pa

import questdb
from questdb import QuestDBError, TimestampNanos


def _query_when_applied(db, sql, attempts=100):
    """Rows land through the WAL, so a read issued straight after the
    write acknowledges can still be ahead of them."""
    frame = db.query(sql).to_pandas()
    for _ in range(attempts):
        if not frame.empty:
            break
        time.sleep(0.05)
        frame = db.query(sql).to_pandas()
    return frame


def example(host: str = 'localhost', port: int = 9000):
    try:
        with questdb.connect(f'ws::addr={host}:{port};') as db:
            db.execute(
                'CREATE TABLE IF NOT EXISTS devices_df ('
                '  device_id UUID,'
                '  address IPV4,'
                '  grade CHAR,'
                '  last_seen DATE,'
                '  checksum LONG256,'
                '  location GEOHASH(5c),'
                '  timestamp TIMESTAMP_NS'
                ') TIMESTAMP(timestamp) PARTITION BY DAY WAL')

            device_ids = [
                uuid.UUID('123e4567-e89b-12d3-a456-426614174000'),
                uuid.UUID('00000000-0000-0000-0000-0000000000ff')]
            frame = pd.DataFrame({
                # A DataFrame states these types through the column's
                # own Arrow type. UUID and LONG256 are fixed-size
                # binary of 16 and 32 bytes; IPV4 is uint32 and CHAR is
                # uint16, both of which need naming through
                # `schema_overrides` because the integer alone does not
                # say which type it is.
                'device_id': pd.Series(
                    [value.bytes for value in device_ids],
                    dtype=pd.ArrowDtype(pa.binary(16))),
                'address': pd.Series(
                    [0x0A000007, 0x0A000008],
                    dtype=pd.ArrowDtype(pa.uint32())),
                'grade': pd.Series(
                    [ord('A'), ord('B')],
                    dtype=pd.ArrowDtype(pa.uint16())),
                # A millisecond Arrow timestamp is itself the DATE
                # claim; `pa.date32()` and `pa.date64()` work too.
                'last_seen': pd.Series(
                    [1704164645678, 1704164645679],
                    dtype=pd.ArrowDtype(pa.timestamp('ms'))),
                'checksum': pd.Series(
                    [(0xdeadbeef).to_bytes(32, 'little'),
                     (1).to_bytes(32, 'little')],
                    dtype=pd.ArrowDtype(pa.binary(32))),
                # A GEOHASH rides on a signed integer wide enough for
                # its precision, and `schema_overrides` names the bits.
                # 27364744 is the base32 string 'u33d8' packed into 25
                # bits -- `Geohash.from_string` does that conversion for
                # the row API.
                'location': pd.Series(
                    [27364744, 27364745],
                    dtype=pd.ArrowDtype(pa.int32())),
            })
            db.dataframe(
                frame,
                table_name='devices_df',
                schema_overrides={
                    'device_id': 'uuid',
                    'address': 'ipv4',
                    'grade': 'char',
                    'checksum': 'long256',
                    'location': ('geohash', 25),
                },
                at=TimestampNanos.now())

            # Reading a result back and writing it out again keeps
            # every one of these types. A pandas dtype holds an Arrow
            # type and no field, so the claim travels in
            # `df.attrs['questdb']`, which `dataframe()` reads. It works
            # the same on all three `to_pandas` backends, and needs no
            # `schema_overrides` second time round.
            read_back = _query_when_applied(
                db, 'SELECT * FROM devices_df LIMIT -2')
            print(read_back.attrs['questdb'])

            # Written back into the table it came from: the server
            # refuses a column whose type does not match the one the
            # table holds, so this append landing is the claim doing its
            # job. Written to a new table name instead, the same frame
            # auto-creates it with these types rather than with BINARY
            # and plain integers.
            db.dataframe(read_back, table_name='devices_df', at='timestamp')

            print(_query_when_applied(
                db,
                'SELECT device_id, address, grade, last_seen, checksum, '
                'location FROM devices_df'))

    except QuestDBError as e:
        sys.stderr.write(
            f'Got error: {e} (code={e.code}, in_doubt={e.in_doubt})\n')


if __name__ == '__main__':
    example()
