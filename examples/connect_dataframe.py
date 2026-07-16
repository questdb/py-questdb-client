import sys

import pandas as pd

import questdb
from questdb import QuestDBError


def example(host: str = 'localhost', port: int = 9000):
    df = pd.DataFrame({
        'symbol': pd.Categorical(['ETH-USD', 'BTC-USD']),
        'side': pd.Categorical(['sell', 'sell']),
        'price': [2615.54, 39269.98],
        'amount': [0.00044, 0.001],
        'timestamp': pd.to_datetime(
            ['2021-01-01T00:00:00Z', '2021-01-01T00:00:01Z'])})
    try:
        conf = f'ws::addr={host}:{port};'
        with questdb.connect(conf) as db:
            # Bulk-load the whole frame over the direct columnar path.
            # Categorical columns become SYMBOL automatically
            # (symbols='auto'); the call blocks until the server
            # acknowledges the final batch.
            db.dataframe(df, table_name='trades', at='timestamp')

            # Lease one reader connection and run queries on it
            # sequentially. `reset_symbol_dict=False` keeps the SYMBOL
            # dictionary warm across queries because they share the
            # connection.
            with db.query() as q:
                recent = q.query('SELECT * FROM trades LIMIT 5').to_pandas()
                print(recent)

                by_symbol = q.query(
                    'SELECT symbol, count() FROM trades',
                    reset_symbol_dict=False,
                ).to_pandas()
                print(by_symbol)

    except QuestDBError as e:
        sys.stderr.write(
            f'Got error: {e} (code={e.code}, in_doubt={e.in_doubt})\n')


if __name__ == '__main__':
    example()
