from questdb import Client, Sender
import pandas as pd


def write_parquet_file():
    df = pd.DataFrame({
        'location': pd.Categorical(
            ['BP-5541', 'UB-3355', 'SL-0995', 'BP-6653']),
        'provider': pd.Categorical(
            ['BP Pulse', 'Ubitricity', 'Source London', 'BP Pulse']),
        'speed_kwh': pd.Categorical(
            [50, 7, 7, 120]),
        'connector_type': pd.Categorical(
            ['Type 2 & 2+CCS', 'Type 1 & 2', 'Type 1 & 2', 'Type 2 & 2+CCS']),
        'current_type': pd.Categorical(
            ['dc', 'ac', 'ac', 'dc']),
        'price_pence':
            [54, 34, 32, 59],
        'in_use':
            [True, False, False, True],
        'ts': [
            pd.Timestamp('2022-12-30 12:15:00'),
            pd.Timestamp('2022-12-30 12:16:00'),
            pd.Timestamp('2022-12-30 12:18:00'),
            pd.Timestamp('2022-12-30 12:19:00')]})
    name = 'ev_chargers'
    df.index.name = name  # We set the dataframe's index name here!
    filename = f'{name}.parquet'
    df.to_parquet(filename)
    return filename


def example(host: str = 'localhost', port: int = 9000):
    filename = write_parquet_file()

    df = pd.read_parquet(filename)
    with Sender.from_conf(f"http::addr={host}:{port};") as sender:
        # Ingress: publish a Pandas DataFrame into QuestDB.
        # Note: Table name is looked up from the dataframe's index name.
        sender.dataframe(df, at='ts')

    with Client.from_conf(f"ws::addr={host}:{port};") as client:
        # Egress: query QuestDB and materialise the result as Pandas.
        with client.query(
                "SELECT x AS charger_id, "
                "x * 25 AS speed_kwh "
                "FROM long_sequence(3)") as result:
            queried = result.to_pandas()
        print(queried)


if __name__ == '__main__':
    example()
