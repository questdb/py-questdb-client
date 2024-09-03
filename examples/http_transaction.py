from questdb.ingress import Sender, TimestampNanos
import pandas as pd


def example():
    conf = 'http::addr=localhost:9000;'
    with Sender.from_conf(conf) as sender:
        # Force a whole dataframe to be written in a single transaction.
        # This temporarily suspends auto-flushing:
        # The dataframe-generated buffer must thus fit in memory.
        with sender.transaction('trades') as txn:
            df = pd.DataFrame({
                    'symbol': pd.Categorical(['ETH-USD', 'BTC-USD']),
                    'side': pd.Categorical(['sell', 'sell']),
                    'price': [2615.54, 39269.98],
                    'amount': [0.00044, 0.001],
                    'timestamp': pd.to_datetime(['2021-01-01', '2021-01-02'])})
            txn.dataframe(
                df,
                symbols=['pair'],
                at='timestamp')

            # You can write additional dataframes or rows,
            # but they must all be for the same table.
            txn.row(
                symbols={'symbol': 'ETH-USD', 'side': 'sell'},
                columns={'price': 2615.54, 'amount': 0.00044},
                at=TimestampNanos.now())

        # The transaction is flushed when the `with` block ends.


if __name__ == '__main__':
    example()
