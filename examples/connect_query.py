import sys

import questdb
from questdb import QuestDBError


def example(host: str = 'localhost', port: int = 9000):
    try:
        with questdb.connect(f'ws::addr={host}:{port};') as db:
            # query() also runs DDL and DML statements; close the result
            # even when no rows are expected.
            db.query(
                'CREATE TABLE IF NOT EXISTS trades ('
                '  timestamp TIMESTAMP, symbol SYMBOL,'
                '  price DOUBLE, amount DOUBLE'
                ') TIMESTAMP(timestamp) PARTITION BY DAY WAL').close()

            # Bind positional parameters to $1..$N placeholders instead of
            # interpolating values into the SQL.
            frame = db.query(
                'SELECT timestamp, price, amount FROM trades '
                'WHERE symbol = $1 AND price > $2 LIMIT 10',
                ['ETH-USD', 1000.0],
            ).to_pandas()
            print(frame)

            # Stream a large result batch by batch instead of
            # materializing all of it; fully draining the iterator
            # returns the connection to the pool.
            total = 0
            with db.query('SELECT * FROM trades') as result:
                for batch in result.iter_pandas():
                    total += len(batch)
            print('rows streamed:', total)

            # Lease one reader connection for a run of queries;
            # reset_symbol_dict=False keeps the SYMBOL dictionary warm
            # because every query shares the connection.
            with db.reader() as r:
                latest = r.query(
                    'SELECT * FROM trades LIMIT -5').to_pandas()
                per_symbol = r.query(
                    'SELECT symbol, count() FROM trades').to_pandas()
            print(latest)
            print(per_symbol)

    except QuestDBError as e:
        sys.stderr.write(
            f'Got error: {e} (code={e.code}, in_doubt={e.in_doubt})\n')


if __name__ == '__main__':
    example()
