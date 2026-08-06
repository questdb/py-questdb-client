import questdb
from questdb import QuestDBError

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
        with questdb.connect(f"ws::addr={host}:{port};") as db:
            # Ingress: one DataFrame destined for multiple tables.
            # The columnar path loads one table per call, so split by the
            # table-naming column and bulk-load each group.
            for metric, group in df.groupby('metric', observed=True):
                db.dataframe(
                    group.drop(columns=['metric']),
                    table_name=str(metric),
                    symbols='auto',  # Category columns as SYMBOL. (Default)
                    at='ts')

            # Egress: query QuestDB and materialise the result as Pandas.
            with db.query(
                    "SELECT x AS sample_id, "
                    "x / 10.0 AS value "
                    "FROM long_sequence(3)") as result:
                queried = result.to_pandas()
            print(queried)

    except QuestDBError as e:
        sys.stderr.write(f'Got error: {e}\n')


if __name__ == '__main__':
    example()
