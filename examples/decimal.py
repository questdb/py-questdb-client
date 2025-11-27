from decimal import Decimal
from questdb.ingress import Sender, TimestampNanos
import pandas as pd

# First, create the table with DECIMAL columns using SQL:
#
# CREATE TABLE financial_data (
#     symbol SYMBOL,
#     price DECIMAL(18, 6),
#     quantity DECIMAL(12, 4),
#     timestamp TIMESTAMP
# ) TIMESTAMP(timestamp) PARTITION BY DAY;

conf = 'http::addr=localhost:9000;'
with Sender.from_conf(conf) as sender:
    # Using row() method with Python Decimal
    sender.row(
        'financial_data',
        symbols={'symbol': 'BTC-USD'},
        columns={
            'price': Decimal('50123.456789'),
            'quantity': Decimal('1.2345')
        },
        at=TimestampNanos.now())
    
    # Using dataframe() with Python Decimal objects
    df = pd.DataFrame({
        'symbol': ['BTC-USD', 'ETH-USD'],
        'price': [Decimal('50123.456789'), Decimal('2615.123456')],
        'quantity': [Decimal('1.2345'), Decimal('10.5678')]
    })
    sender.dataframe(df, table_name='financial_data',
                    symbols=['symbol'], at=TimestampNanos.now())
