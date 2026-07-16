import sys

import pandas as pd
import pyarrow as pa

import questdb
from questdb import QuestDBError, TimestampNanos


def example(host: str = 'localhost', port: int = 9000):
    # Arrow decimal columns carry their width and scale in the type,
    # so they map onto DECIMAL columns without per-cell inspection.
    df = pd.DataFrame({
        'symbol': pd.Categorical(['BTC-USD', 'ETH-USD']),
        'price': pd.Series(
            [50123.456789, 2615.123456],
            dtype=pd.ArrowDtype(pa.decimal128(18, 6))),
        'quantity': pd.Series(
            [1.2345, 10.5678],
            dtype=pd.ArrowDtype(pa.decimal128(12, 4))),
    })
    try:
        with questdb.connect(f'ws::addr={host}:{port};') as db:
            db.query(
                'CREATE TABLE IF NOT EXISTS financial_data ('
                '  symbol SYMBOL,'
                '  price DECIMAL(18, 6),'
                '  quantity DECIMAL(12, 4),'
                '  timestamp TIMESTAMP_NS'
                ') TIMESTAMP(timestamp) PARTITION BY DAY WAL').close()

            db.dataframe(
                df, table_name='financial_data', symbols=['symbol'],
                at=TimestampNanos.now())

    except QuestDBError as e:
        sys.stderr.write(
            f'Got error: {e} (code={e.code}, in_doubt={e.in_doubt})\n')


if __name__ == '__main__':
    example()
