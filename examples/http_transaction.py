from questdb.ingress import Sender, TimestampNanos
import pandas as pd


def example():
    conf = 'http::addr=localhost:9000;'
    with Sender.from_conf(conf) as sender:
        # Force a whole dataframe to be written in a single transaction.
        # This temporarily suspends auto-flushing:
        # The dataframe-generated buffer must thus fit in memory.
        with sender.transaction('trades_python') as txn:
            df = pd.DataFrame({
                'pair': ['USDGBP', 'EURJPY'],
                'traded_price': [0.83, 142.62],
                'qty': [100, 400],
                'limit_price': [0.84, None],
                'timestamp': [
                    pd.Timestamp('2022-08-06 07:35:23.189062', tz='UTC'),
                    pd.Timestamp('2022-08-06 07:35:23.189062', tz='UTC')]})
            txn.dataframe(
                df,
                symbols=['pair'],
                at='timestamp')

            # You can write additional dataframes or rows,
            # but they must all be for the same table.
            txn.row(
                symbols={'pair': 'EURUSD'},
                columns={'traded_price': 0.86, 'qty': 1000},
                at=TimestampNanos.now())

        # The transaction is flushed when the `with` block ends.


if __name__ == '__main__':
    example()
