import datetime
import ipaddress
import sys
import time
import uuid

import questdb
from questdb import (
    Char,
    DateMillis,
    Geohash,
    Long256,
    QuestDBError,
    TimestampNanos,
)


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


def example(
        host: str = 'localhost',
        port: int = 9000,
        table_name: str = 'devices'):
    with questdb.connect(f'ws::addr={host}:{port};') as db:
        # These column types are auto-created like any other, but a
        # GEOHASH column's precision is fixed when the column is
        # created, so this table states it up front.
        db.execute(
            f'CREATE TABLE IF NOT EXISTS {table_name} ('
            '  device_id UUID,'
            '  address IPV4,'
            '  payload BINARY,'
            '  grade CHAR,'
            '  last_seen DATE,'
            '  checksum LONG256,'
            '  location GEOHASH(5c),'
            '  timestamp TIMESTAMP_NS'
            ') TIMESTAMP(timestamp) PARTITION BY DAY WAL')

        with db.sender() as sender:
            sender.row(
                table_name,
                columns={
                    # UUID, IPV4 and BINARY are written with the
                    # ordinary Python types.
                    'device_id': uuid.UUID(
                        '123e4567-e89b-12d3-a456-426614174000'),
                    'address': ipaddress.IPv4Address('10.0.0.7'),
                    'payload': b'\x00\x01\x02\x03',
                    # The rest need a wrapper, because the Python
                    # type they would otherwise arrive as already
                    # means another QuestDB column type.
                    'grade': Char('A'),
                    # A DATE is a millisecond timestamp rather than
                    # a civil date; `DateMillis.now()` and
                    # `DateMillis(millis)` build one too.
                    'last_seen': DateMillis.from_datetime(
                        datetime.datetime(
                            2024, 1, 2, 3, 4, 5, 678000,
                            tzinfo=datetime.timezone.utc)),
                    'checksum': Long256(0xdeadbeef),
                    # 5 characters of base32, so 25 bits of
                    # precision -- the GEOHASH(5c) the column holds.
                    'location': Geohash.from_string('u33d8'),
                },
                at=TimestampNanos.now())
            sender.flush(wait=True)

        print(_query_when_applied(
            db,
            'SELECT device_id, address, grade, last_seen, checksum, '
            f'location FROM {table_name} LIMIT -1'))


if __name__ == '__main__':
    try:
        example()
    except QuestDBError as e:
        sys.stderr.write(
            f'Got error: {e} (code={e.code}, in_doubt={e.in_doubt})\n')
        sys.exit(1)
