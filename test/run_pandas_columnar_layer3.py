#!/usr/bin/env python3

import argparse
import contextlib
import json
import pathlib
import sys
import urllib.error
import urllib.request

sys.dont_write_bytecode = True

import patch_path

PROJ_ROOT = patch_path.PROJ_ROOT
sys.path.append(str(PROJ_ROOT / "c-questdb-client" / "system_test"))

from fixture import QuestDbFixture, install_questdb_from_repo

from benchmark_pandas_columnar import (
    SUPPORTED_SCHEMAS,
    add_cpu_summary,
    add_rates,
    execute_sql,
    run_real_client_path,
    run_real_row_path,
    schema_sql_report,
    summarize,
)


def run_layer3(args):
    with contextlib.redirect_stdout(sys.stderr):
        questdb_root = install_questdb_from_repo(pathlib.Path(args.questdb_repo))
    qdb = QuestDbFixture(
        questdb_root,
        auth=False,
        http=True,
        qwp_udp=False)
    with contextlib.redirect_stdout(sys.stderr):
        qdb.start()
    try:
        http_base = f"http://{qdb.host}:{qdb.http_server_port}"
        conf = (
            f"qwpws::addr={qdb.host}:{qdb.http_server_port};"
            "pool_size=1;"
            "pool_max=1;"
            "pool_reap=manual;")
        df = SUPPORTED_SCHEMAS[args.schema](args.rows)
        schema_sql = schema_sql_report(args.schema)
        setup_sqls = [schema_sql["drop_sql"], schema_sql["create_sql"]]
        reset_sqls = [schema_sql["truncate_sql"]]
        settings = fetch_http_endpoint(http_base, "/settings")
        version = qdb.version

        paths = {}
        for path_name, runner in (
                ("real-row", run_real_row_path),
                ("real-client", run_real_client_path)):
            samples, cpu_samples, last = runner(
                df,
                args.rows,
                args.iterations,
                args.warmups,
                conf=conf,
                table_name=schema_sql["table_name"],
                http_base=http_base,
                setup_sqls=setup_sqls,
                reset_sqls=reset_sqls)
            summary = summarize(samples)
            add_rates(summary, args.rows, len(df.columns))
            add_cpu_summary(summary, cpu_samples, args.rows, len(df.columns))
            summary["row_count_check"] = fetch_row_count(
                http_base,
                schema_sql["table_name"],
                expected=args.rows)
            summary["last"] = last
            paths[path_name] = summary

        return {
            "schema": args.schema,
            "rows": args.rows,
            "columns": len(df.columns),
            "iterations": args.iterations,
            "warmups": args.warmups,
            "questdb_version": ".".join(str(part) for part in version),
            "questdb_repo": str(pathlib.Path(args.questdb_repo).resolve()),
            "http_base": http_base,
            "real_conf": conf,
            "schema_sql": schema_sql,
            "settings": settings,
            "paths": paths,
        }
    finally:
        qdb.stop()


def fetch_http_endpoint(http_base, path):
    url = http_base.rstrip("/") + path
    try:
        with urllib.request.urlopen(url, timeout=30) as response:
            body = response.read().decode("utf-8", errors="replace")
            try:
                parsed = json.loads(body)
            except json.JSONDecodeError:
                parsed = body
            return {
                "url": url,
                "status": response.status,
                "body": parsed,
            }
    except urllib.error.HTTPError as error:
        return {
            "url": url,
            "status": error.code,
            "body": error.read().decode("utf-8", errors="replace"),
        }


def fetch_row_count(http_base, table_name, *, expected):
    result = execute_sql(http_base, f"SELECT count() FROM {table_name}")
    parsed = json.loads(result["body"])
    actual = parsed["dataset"][0][0]
    return {
        "actual": actual,
        "expected": expected,
        "ok": actual == expected,
        "status": result["status"],
    }


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Start a local QuestDB fixture and run pandas columnar Layer 3 "
            "real-row / real-client benchmarks."))
    parser.add_argument(
        "--questdb-repo",
        default="../questdb",
        help=(
            "Path to a built QuestDB repo containing "
            "core/target/questdb-*-SNAPSHOT.jar."))
    parser.add_argument(
        "--schema",
        choices=sorted(SUPPORTED_SCHEMAS),
        default="numeric-core")
    parser.add_argument("--rows", type=int, default=10000)
    parser.add_argument("--iterations", type=int, default=3)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args()

    result = run_layer3(args)
    print(json.dumps(
        result,
        indent=2 if args.pretty else None,
        sort_keys=True))


if __name__ == "__main__":
    main()
