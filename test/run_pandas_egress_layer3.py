#!/usr/bin/env python3
"""Step 2 egress real-server fixture (QWP_DATAFRAME_BENCH_PLAN.md s5.2/s5.4).

Starts a local QuestDB, ingests the s1-narrow table (DEDUP UPSERT KEYS(ts),
monotonic-unique microsecond ts), waits for the WAL to apply and asserts
count() == rows, then reads the table back through the egress paths
(``benchmark_pandas_egress``) and emits the contract-conformant JSON.

This reuses the Step 1 DEDUP spine end to end: write in Step 1, read in Step 2,
on the same server. No git-mutation of any QuestDB repo -- the fixture only
copies the prebuilt jar.
"""

import argparse
import contextlib
import json
import pathlib
import sys

sys.dont_write_bytecode = True

import patch_path

PROJ_ROOT = patch_path.PROJ_ROOT
sys.path.append(str(PROJ_ROOT / "c-questdb-client" / "system_test"))

from fixture import QuestDbFixture, install_questdb_from_repo

import questdb.ingress as qi
from benchmark_pandas_columnar import (
    DEFAULT_SYM_CARD,
    DEFAULT_VARCHAR_LEN,
    build_schema_df,
    run_real_client_path,
    schema_sql_report,
)
from run_pandas_columnar_layer3 import fetch_http_endpoint, fetch_row_count
from benchmark_pandas_egress import (
    ALL_PATHS,
    build_egress_report,
    measure_egress_wire_bytes,
    verify_zero_copy,
)


def run_layer3(args):
    with contextlib.redirect_stdout(sys.stderr):
        questdb_root = install_questdb_from_repo(pathlib.Path(args.questdb_repo))
    qdb = QuestDbFixture(questdb_root, auth=False, http=True, qwp_udp=False)
    with contextlib.redirect_stdout(sys.stderr):
        qdb.start()
    try:
        http_base = f"http://{qdb.host}:{qdb.http_server_port}"
        conf = (
            f"qwpws::addr={qdb.host}:{qdb.http_server_port};"
            "pool_size=1;pool_max=1;pool_reap=manual;")
        schema = "s1-narrow"
        df = build_schema_df(
            schema, args.rows,
            sym_card=args.sym_card, varchar_len=args.varchar_len)
        sql = schema_sql_report(schema)
        table_name = sql["table_name"]
        setup_sqls = [sql["drop_sql"], sql["create_sql"]]

        # --- Ingest the S1 table (chunked real-client path; DEDUP-correct). ---
        # No reset between iterations: we ingest once (warmups=0, iterations=1)
        # so the table holds exactly `rows` to read back.
        run_real_client_path(
            df, args.rows, 1, 0,
            conf=conf, table_name=table_name, http_base=http_base,
            setup_sqls=setup_sqls, reset_sqls=())

        # --- DEDUP gate: WAL-aware count() == rows (plan s3.4). ---
        count_check = fetch_row_count(
            http_base, table_name, expected=args.rows)
        if not count_check["ok"]:
            raise AssertionError(
                f"egress fixture: count() mismatch on {table_name}: "
                f"expected {count_check['expected']}, got "
                f"{count_check['actual']} "
                f"(inflated={count_check.get('inflated')})")

        # --- Read it back through the egress paths. ---
        paths = args.path or ALL_PATHS
        with qi.Client.from_conf(conf) as client:
            read_sql = f"SELECT * FROM {table_name}"
            wire_bytes = measure_egress_wire_bytes(client, read_sql)
            zero_copy = verify_zero_copy(client, read_sql)
            report = build_egress_report(
                client=client,
                table_name=table_name,
                rows=args.rows,
                columns=len(df.columns),
                iterations=args.iterations,
                warmups=args.warmups,
                run_mode=args.run_mode,
                paths=paths,
                wire_bytes=wire_bytes,
                zero_copy=zero_copy,
                extra={
                    "questdb_version": ".".join(
                        str(part) for part in qdb.version),
                    "questdb_repo": str(
                        pathlib.Path(args.questdb_repo).resolve()),
                    "http_base": http_base,
                    "real_conf": conf,
                    "schema_sql": sql,
                    "row_count_check": count_check,
                    "settings": fetch_http_endpoint(http_base, "/settings"),
                })
        return report
    finally:
        qdb.stop()


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Start a local QuestDB, ingest the s1-narrow table, then run the "
            "pandas egress read-back benchmark against it."))
    parser.add_argument(
        "--questdb-repo",
        default="../questdb",
        help=(
            "Path to a built QuestDB repo containing "
            "core/target/questdb-*-SNAPSHOT.jar."))
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--iterations", type=int, default=10)
    parser.add_argument("--warmups", type=int, default=2)
    parser.add_argument(
        "--sym-card", type=int, default=DEFAULT_SYM_CARD)
    parser.add_argument(
        "--varchar-len", type=int, default=DEFAULT_VARCHAR_LEN)
    parser.add_argument(
        "--run-mode", choices=["quick", "full"], default="full")
    parser.add_argument(
        "--path",
        choices=ALL_PATHS,
        action="append",
        help="Egress path(s) to run. Defaults to all paths.")
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args()

    report = run_layer3(args)
    print(json.dumps(
        report, indent=2 if args.pretty else None, sort_keys=True))


if __name__ == "__main__":
    main()
