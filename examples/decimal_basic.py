import sys
from decimal import Decimal

import pandas as pd

import questdb
from questdb import QuestDBError, TimestampNanos


def example(host: str = 'localhost', port: int = 9000):
    try:
        with questdb.connect(f'ws::addr={host}:{port};') as db:
            # DECIMAL columns must be created ahead of time; the server
            # does not auto-create them.
            db.execute(
                'CREATE TABLE IF NOT EXISTS financial_data ('
                '  symbol SYMBOL,'
                '  price DECIMAL(18, 6),'
                '  quantity DECIMAL(12, 4),'
                '  timestamp TIMESTAMP_NS'
                ') TIMESTAMP(timestamp) PARTITION BY DAY WAL')

            # Row-by-row ingestion with Python Decimal values.
            with db.sender() as sender:
                sender.row(
                    'financial_data',
                    symbols={'symbol': 'BTC-USD'},
                    columns={
                        'price': Decimal('50123.456789'),
                        'quantity': Decimal('1.2345')},
                    at=TimestampNanos.now())
                sender.flush(wait=True)

            # Bulk load with object-dtype Decimal columns.
            df = pd.DataFrame({
                'symbol': pd.Categorical(['BTC-USD', 'ETH-USD']),
                'price': [Decimal('50123.456789'), Decimal('2615.123456')],
                'quantity': [Decimal('1.2345'), Decimal('10.5678')],
            })
            db.dataframe(
                df, table_name='financial_data', symbols=['symbol'],
                at=TimestampNanos.now())

            frame = db.query(
                'SELECT symbol, price, quantity FROM financial_data '
                'LIMIT -3').to_pandas()
            print(frame)

    except QuestDBError as e:
        sys.stderr.write(
            f'Got error: {e} (code={e.code}, in_doubt={e.in_doubt})\n')


if __name__ == '__main__':
    example()
