import sys

import questdb
from questdb import QuestDBError, TimestampNanos


def example(host: str = 'localhost', port: int = 9000):
    try:
        conf = f'ws::addr={host}:{port};'
        with questdb.connect(conf) as db:
            # Lease a pooled sender for row-by-row ingestion.
            # Take one lease per thread; leaving the `with` block
            # returns it to the pool.
            with db.sender() as sender:
                sender.row(
                    'trades',
                    symbols={
                        'symbol': 'ETH-USD',
                        'side': 'sell'},
                    columns={
                        'price': 2615.54,
                        'amount': 0.00044},
                    at=TimestampNanos.now())

                # Rows accumulate on the lease until you flush; there is
                # no auto-flush on the pooled sender. `wait=True` blocks
                # until the server acknowledges everything published on
                # this lease.
                sender.flush(wait=True)

            # Query with positional bind parameters ($1..$N) and read the
            # result straight into pandas. Fully draining the result
            # returns its connection to the pool.
            frame = db.query(
                'SELECT symbol, price, amount FROM trades '
                'WHERE symbol = $1 LIMIT 5',
                ['ETH-USD'],
            ).to_pandas()
            print(frame)

    except QuestDBError as e:
        sys.stderr.write(
            f'Got error: {e} (code={e.code}, in_doubt={e.in_doubt})\n')


if __name__ == '__main__':
    example()
