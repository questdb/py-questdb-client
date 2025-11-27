from questdb.ingress import Sender, TimestampNanos
import pandas as pd
import pyarrow as pa

# First, create the table with DECIMAL columns using SQL:
#
# CREATE TABLE financial_data (
#     symbol SYMBOL,
#     price DECIMAL(18, 6),
#     quantity DECIMAL(12, 4),
#     timestamp TIMESTAMP
# ) TIMESTAMP(timestamp) PARTITION BY DAY;

df = pd.DataFrame({
    'symbol': ['BTC-USD', 'ETH-USD'],
    'price': pd.Series(
        [50123.456789, 2615.123456],
        dtype=pd.ArrowDtype(pa.decimal128(18, 6))),
    'quantity': pd.Series(
        [1.2345, 10.5678],
        dtype=pd.ArrowDtype(pa.decimal128(12, 4)))
})

conf = 'http::addr=localhost:9000;'
with Sender.from_conf(conf) as sender:
    sender.dataframe(df, table_name='financial_data',
                    symbols=['symbol'], at=TimestampNanos.now())
