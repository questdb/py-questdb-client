from questdb.ingress import Sender, IngressError

import sys
import pandas as pd


def example(host: str = 'localhost', port: int = 9000):
    df = pd.DataFrame({
            'metric': pd.Categorical(
                ['humidity', 'temp_c', 'voc_index', 'temp_c']),
            'sensor': pd.Categorical(
                ['paris-01', 'london-02', 'london-01', 'paris-01']),
            'value': [
                0.83, 22.62, 100.0, 23.62],
            'ts': [
                pd.Timestamp('2022-08-06 07:35:23.189062'),
                pd.Timestamp('2022-08-06 07:35:23.189062'),
                pd.Timestamp('2022-08-06 07:35:23.189062'),
                pd.Timestamp('2022-08-06 07:35:23.189062')]})
    try:
        with Sender.from_conf(f"http::addr={host}:{port};") as sender:
            sender.dataframe(
                df,
                table_name_col='metric',  # Table name from 'metric' column.
                symbols='auto',  # Category columns as SYMBOL. (Default)
                at=-1)  # Last column contains the designated timestamps.

    except IngressError as e:
        sys.stderr.write(f'Got error: {e}\n')


if __name__ == '__main__':
    example()
