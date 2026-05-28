"""Smoke test for Client.query → to_pandas / to_arrow round-trip.

Run with the local QuestDB repo path:

    QDB_REPO_PATH=/home/jara/devel/oss/questdb-http2 \
        venv/bin/python test/smoke_egress.py
"""

from __future__ import annotations

import os
import pathlib
import sys
import time

import patch_path  # noqa: F401
PROJ_ROOT = patch_path.PROJ_ROOT
sys.path.append(str(PROJ_ROOT / 'c-questdb-client' / 'system_test'))

import numpy as np
import pandas as pd
import pyarrow as pa

from fixture import QuestDbFixture, install_questdb, install_questdb_from_repo

import questdb.ingress as qi


def _install_path():
    if not os.environ.get('QDB_REPO_PATH'):
        raise SystemExit(
            'Set QDB_REPO_PATH to a built QuestDB checkout '
            '(e.g. /home/jara/devel/oss/questdb-http2)')
    repo = pathlib.Path(os.environ['QDB_REPO_PATH'])
    return install_questdb_from_repo(repo)


def _wait_for_rows(qdb, table_name, expected, timeout_s=30):
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            res = qdb.http_sql_query(f'SELECT count() FROM {table_name}')
        except Exception:
            time.sleep(0.1)
            continue
        rows = res.get('dataset') or []
        if rows and rows[0][0] >= expected:
            return
        time.sleep(0.1)
    raise RuntimeError(
        f'WAL apply timed out: {expected} rows expected on {table_name}')


def main():
    install_path = _install_path()
    plain_dir = PROJ_ROOT / 'build' / 'questdb' / 'plain'
    plain_dir.mkdir(parents=True, exist_ok=True)
    import shutil
    shutil.copytree(install_path, plain_dir, dirs_exist_ok=True)

    qdb = QuestDbFixture(plain_dir, auth=False, http=True)
    qdb.start()
    try:
        addr = f'{qdb.host}:{qdb.http_server_port}'
        conf = f'qwpws::addr={addr};'
        table_name = 'smoke_egress'
        # Drop pre-existing table to keep the test deterministic.
        try:
            qdb.http_sql_query(f'DROP TABLE IF EXISTS {table_name}')
        except Exception:
            pass

        df_in = pd.DataFrame({
            'ts': pd.to_datetime([
                '2024-01-01T00:00:00',
                '2024-01-01T00:00:01',
                '2024-01-01T00:00:02']),
            'id': np.array([1, 2, 3], dtype=np.int64),
            'price': np.array([10.5, 20.25, 30.125], dtype=np.float64),
            'sym': pd.Categorical(['AAA', 'BBB', 'AAA']),
            's': pd.Series(['hello', 'world', 'foo'], dtype='string[pyarrow]'),
        })
        print(f'[smoke] ingesting {len(df_in)} rows...')
        with qi.Client.from_conf(conf) as client:
            client.dataframe(df_in, table_name=table_name, at='ts')

        _wait_for_rows(qdb, table_name, len(df_in))
        print('[smoke] WAL applied; reading back via Client.query...')

        with qi.Client.from_conf(conf) as client:
            # The designated-ts column is stored under the name `timestamp`
            # by QuestDB regardless of the source column name.
            result = client.query(
                f'SELECT * FROM {table_name} ORDER BY timestamp')
            table = result.to_arrow()
            print('[smoke] schema:')
            print(table.schema)
            print('[smoke] table:')
            print(table)

            # Second call gets its own QueryResult; the first was consumed.
            result2 = client.query(
                f'SELECT id, price, sym, s FROM {table_name} ORDER BY id')
            pdf = result2.to_pandas()
            print('[smoke] pandas frame:')
            print(pdf)
            print('[smoke] dtypes:')
            print(pdf.dtypes)

            # Third call: dtype_backend="pyarrow" path.
            result3 = client.query(
                f'SELECT id, price FROM {table_name} ORDER BY id')
            pdf_arrow = result3.to_pandas(dtype_backend='pyarrow')
            print('[smoke] arrow-backed dtypes:')
            print(pdf_arrow.dtypes)

            # Fourth call: iter_arrow streaming path.
            result4 = client.query(f'SELECT id FROM {table_name}')
            batch_count = 0
            row_count = 0
            for batch in result4.iter_arrow():
                batch_count += 1
                row_count += batch.num_rows
            print(f'[smoke] iter_arrow: {batch_count} batches, '
                  f'{row_count} rows')

        print('[smoke] PASS')
    finally:
        qdb.stop()


if __name__ == '__main__':
    main()
