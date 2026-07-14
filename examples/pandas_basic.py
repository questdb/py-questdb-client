from questdb import Client, QuestDBError

import sys
import pandas as pd


def example(host: str = 'localhost', port: int = 9000):
    df = pd.DataFrame({
            'symbol': pd.Categorical(['ETH-USD', 'BTC-USD']),
            'side': pd.Categorical(['sell', 'sell']),
            'price': [2615.54, 39269.98],
            'amount': [0.00044, 0.001],
            'timestamp': pd.to_datetime(['2021-01-01', '2021-01-02'])})
    try:
        with Client.from_conf(f"ws::addr={host}:{port};") as client:
            # Ingress: publish a Pandas DataFrame into QuestDB.
            client.dataframe(
                df,
                table_name='trades',  # Table name to insert into.
                symbols=['symbol', 'side'],  # Columns to be inserted as SYMBOL types.
                at='timestamp')  # Column containing the designated timestamps.

            # Egress: query QuestDB and materialise the result as Pandas.
            with client.query(
                    "SELECT x AS trade_id, "
                    "x * 10.0 AS price "
                    "FROM long_sequence(3)") as result:
                queried = result.to_pandas()
            print(queried)

    except QuestDBError as e:
        sys.stderr.write(f'Got error: {e}\n')


if __name__ == '__main__':
    example()
