"""Polars DataFrame ingest example.

`Client.dataframe()` accepts polars `DataFrame` and `LazyFrame`
directly, riding the Arrow PyCapsule Interface (`__arrow_c_stream__`)
straight into `column_sender_flush_arrow_batch`. No pyarrow dependency
unless `schema_overrides` is used.
"""

from questdb.ingress import Client, IngressError
import datetime
import sys


def example(host: str = 'localhost', port: int = 9000):
    import polars as pl

    df = pl.DataFrame({
        'symbol': ['ETH-USD', 'BTC-USD', 'ETH-USD'],
        'side':   ['sell', 'buy', 'buy'],
        'price':  [2615.54, 67234.12, 2620.88],
        'amount': [0.00044, 0.0012, 0.00033],
        'ts': [
            datetime.datetime(2025, 1, 1, 12, 0, 0),
            datetime.datetime(2025, 1, 1, 12, 0, 1),
            datetime.datetime(2025, 1, 1, 12, 0, 2),
        ],
    })

    try:
        conf = f'qwpws::addr={host}:{port};'
        with Client.from_conf(conf) as client:
            client.dataframe(df, table_name='trades', at='ts')

            client.dataframe(
                df,
                table_name='trades_chunked',
                at='ts',
                max_rows_per_batch=2)
    except IngressError as e:
        sys.stderr.write(f'Got error: {e}\n')


def schema_overrides_example(host: str = 'localhost', port: int = 9000):
    """B-class wire types (IPv4 / Geohash / etc.) need an explicit hint
    because `arr.dtype` alone cannot disambiguate them from plain
    integer columns. `schema_overrides` injects the corresponding
    `questdb.*` Arrow Field metadata. Requires pyarrow.
    """
    import polars as pl

    df = pl.DataFrame({
        'addr': [0x0A000001, 0xC0A80101, 0x7F000001],
        'price': [100, 200, 300],
        'ts': [
            datetime.datetime(2025, 1, 1, 12, 0, 0),
            datetime.datetime(2025, 1, 1, 12, 0, 1),
            datetime.datetime(2025, 1, 1, 12, 0, 2),
        ],
    }, schema={'addr': pl.UInt32, 'price': pl.Int64, 'ts': pl.Datetime('us')})

    try:
        conf = f'qwpws::addr={host}:{port};'
        with Client.from_conf(conf) as client:
            client.dataframe(
                df,
                table_name='ipv4_log',
                at='ts',
                schema_overrides={'addr': 'ipv4'})
    except IngressError as e:
        sys.stderr.write(f'Got error: {e}\n')


if __name__ == '__main__':
    example()
    schema_overrides_example()
