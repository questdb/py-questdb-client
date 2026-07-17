import random
import sys
import time
import uuid

import questdb
from questdb import QuestDBError, TimestampNanos

FLUSH_ROWS = 100        # publish once this many rows are buffered ...
FLUSH_INTERVAL = 5.0    # ... or once this many seconds have passed


def example(host: str = 'localhost', port: int = 9000, total_rows=None):
    table_name = str(uuid.uuid1())
    try:
        with questdb.connect(f'ws::addr={host}:{port};') as db:
            with db.sender() as sender:
                # Pooled auto-flush is off by default: accumulate rows on
                # your own cadence and flush explicitly.
                sent = 0
                last_flush = time.monotonic()
                print('Ctrl^C to terminate...')
                while total_rows is None or sent < total_rows:
                    time.sleep(random.randint(0, 750) / 1000)

                    sender.row(
                        table_name,
                        symbols={
                            'src': random.choice(('ALPHA', 'BETA', 'OMEGA')),
                            'dst': random.choice(('ALPHA', 'BETA', 'OMEGA'))},
                        columns={
                            'price': random.randint(200, 500),
                            'qty': random.randint(1, 5)},
                        at=TimestampNanos.now())
                    sent += 1

                    if (len(sender) >= FLUSH_ROWS or
                            time.monotonic() - last_flush >= FLUSH_INTERVAL):
                        print(f'Flushing {len(sender)} rows...')
                        sender.flush()
                        last_flush = time.monotonic()

                # Closing the lease publishes any remaining rows.
                print(f'table: {table_name}, total rows sent: {sent}')

    except KeyboardInterrupt:
        print('bye!')
    except QuestDBError as e:
        sys.stderr.write(
            f'Got error: {e} (code={e.code}, in_doubt={e.in_doubt})\n')


if __name__ == '__main__':
    # Bounded run so the example terminates; pass total_rows=None to run
    # until Ctrl^C, as a real ticking-data loop would.
    example(total_rows=25)
