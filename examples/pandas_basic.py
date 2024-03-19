from questdb.ingress import Sender, IngressError

import sys
import pandas as pd


def example(host: str = 'localhost', port: int = 9000):
    df = pd.DataFrame({
            'pair': ['USDGBP', 'EURJPY'],
            'traded_price': [0.83, 142.62],
            'qty': [100, 400],
            'limit_price': [0.84, None],
            'timestamp': [
                pd.Timestamp('2022-08-06 07:35:23.189062', tz='UTC'),
                pd.Timestamp('2022-08-06 07:35:23.189062', tz='UTC')]})
    try:
        with Sender.from_conf(f"http::addr={host}:{port};") as sender:
            sender.dataframe(
                df,
                table_name='trades',  # Table name to insert into.
                symbols=['pair'],  # Columns to be inserted as SYMBOL types.
                at='timestamp')  # Column containing the designated timestamps.

    except IngressError as e:
        sys.stderr.write(f'Got error: {e}\n')


if __name__ == '__main__':
    example()
