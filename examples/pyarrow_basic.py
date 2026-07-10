"""PyArrow Table / RecordBatch ingest and query example.

`Client.dataframe()` accepts any object exposing the Arrow PyCapsule
Interface (`__arrow_c_stream__`) — pyarrow Table, RecordBatch, DuckDB
relations, cudf, etc. — and routes through
`column_sender_flush_arrow_batch` one-shot. No per-column Cython
dispatch, no chunk lifecycle. `Client.query()` can materialise query
results as a pyarrow `Table` with `QueryResult.to_arrow()`.
"""

from questdb import Client, QuestDBError
import sys


def example(host: str = 'localhost', port: int = 9000):
    import pyarrow as pa

    schema = pa.schema([
        pa.field('symbol', pa.string()),
        pa.field('side', pa.string()),
        pa.field('price', pa.float64()),
        pa.field('amount', pa.float64()),
        pa.field('ts', pa.timestamp('us')),
    ])
    table = pa.Table.from_pydict({
        'symbol': ['ETH-USD', 'BTC-USD'],
        'side':   ['sell', 'buy'],
        'price':  [2615.54, 67234.12],
        'amount': [0.00044, 0.0012],
        'ts':     [1735732800_000_000, 1735732801_000_000],
    }, schema=schema)

    try:
        conf = f'ws::addr={host}:{port};'
        with Client.from_conf(conf) as client:
            # Ingress: publish a PyArrow Table into QuestDB.
            client.dataframe(table, table_name='trades', at='ts')

            # Egress: query QuestDB and materialise the result as PyArrow.
            with client.query(
                    "SELECT x AS trade_id, "
                    "x * 10.0 AS price, "
                    "timestamp_sequence("
                    "'2025-01-01T12:00:00.000000Z', 1000000) AS ts "
                    "FROM long_sequence(3)") as result:
                queried = result.to_arrow()
            print(queried)
    except QuestDBError as e:
        sys.stderr.write(f'Got error: {e}\n')


def schema_metadata_example(host: str = 'localhost', port: int = 9000):
    """B-class wire types can be selected either via `schema_overrides`
    (wrapper injects metadata for you) or by attaching the metadata
    directly on the pyarrow Field. Both are equivalent; this example
    shows the direct-attach form.
    """
    import pyarrow as pa

    schema = pa.schema([
        pa.field('addr', pa.uint32(),
                 metadata={b'questdb.column_type': b'ipv4'}),
        pa.field('loc', pa.int32(),
                 metadata={b'questdb.geohash_bits': b'20'}),
        pa.field('ts', pa.timestamp('us')),
    ])
    table = pa.Table.from_pydict({
        'addr': [0x0A000001, 0xC0A80101],
        'loc':  [0x12345, 0x67890],
        'ts':   [1735732800_000_000, 1735732801_000_000],
    }, schema=schema)

    try:
        conf = f'ws::addr={host}:{port};'
        with Client.from_conf(conf) as client:
            client.dataframe(table, table_name='locations', at='ts')
    except QuestDBError as e:
        sys.stderr.write(f'Got error: {e}\n')


if __name__ == '__main__':
    example()
    schema_metadata_example()
