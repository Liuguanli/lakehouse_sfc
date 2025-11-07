#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Execute DBGEN TPCH query streams against previously materialised tpch_all tables.
Each query file is executed individually and metrics are recorded per engine/stream.
"""

from __future__ import annotations

import argparse
import csv
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Iterable, List

from pyspark.sql import SparkSession

from lakehouse_op import run_queries as rq
from lakehouse_op.tpch_all_schemas import TABLE_LIST


def _split_sql_statements(sql_text: str) -> list[str]:
    statements, buf = [], []
    in_single = in_double = False
    i, n = 0, len(sql_text)
    while i < n:
        ch = sql_text[i]
        nxt = sql_text[i + 1] if i + 1 < n else ""
        if ch == "-" and nxt == "-" and not in_single and not in_double:
            # skip comment until newline
            while i < n and sql_text[i] != "\n":
                i += 1
            i += 1
            continue
        if ch == "'" and not in_double:
            in_single = not in_single
        elif ch == '"' and not in_single:
            in_double = not in_double
        if ch == ";" and not in_single and not in_double:
            stmt = "".join(buf).strip()
            if stmt:
                statements.append(stmt)
            buf = []
        else:
            buf.append(ch)
        i += 1
    tail = "".join(buf).strip()
    if tail:
        statements.append(tail)
    return statements


def _main_statement_index(statements: list[str]) -> int:
    idx = -1
    for i, stmt in enumerate(statements):
        if stmt.lstrip().lower().startswith("select"):
            idx = i
    return idx if idx >= 0 else len(statements) - 1


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser("TPCH all query runner")
    ap.add_argument("--engine", required=True, choices=["delta", "hudi", "iceberg"])
    ap.add_argument("--data-root", default="./data/tpch_all",
                    help="Root directory where tpch_all tables were materialised.")
    ap.add_argument("--streams-root", default="/datasets/tpch_1/workload",
                    help="Directory holding DBGEN stream_* folders.")
    ap.add_argument("--streams", default="auto",
                    help="Comma-separated list of stream_* folders (default: auto-detect).")
    ap.add_argument("--results-root", default="results/tpch_all",
                    help="Base directory for result CSVs.")
    ap.add_argument("--timestamp", default="",
                    help="Optional timestamp suffix (default: generated).")
    ap.add_argument("--action", choices=["count", "collect", "show"], default="count",
                    help="Spark action to trigger for each query.")
    ap.add_argument("--no-warmup", action="store_true",
                    help="Disable warmup run before measuring metrics.")
    ap.add_argument("--rest-wait-ms", type=int, default=2000,
                    help="REST wait window for metrics.")
    ap.add_argument("--rest-poll-ms", type=int, default=250,
                    help="REST polling interval.")

    # Iceberg specifics
    ap.add_argument("--iceberg-catalog", default="tpchall",
                    help="Catalog name used during table creation.")
    ap.add_argument("--iceberg-namespace", default="tpch_all",
                    help="Iceberg namespace used for tables.")
    ap.add_argument("--iceberg-warehouse", default="./data/tpch_all/iceberg_wh",
                    help="Warehouse path (needed to configure Spark).")
    return ap.parse_args()


def detect_streams(streams_root: Path) -> List[Path]:
    return sorted(p for p in streams_root.iterdir() if p.is_dir() and p.name.startswith("stream_"))


def build_spark(args: argparse.Namespace) -> SparkSession:
    builder = (
        SparkSession.builder
        .appName(f"tpch-all-runner-{args.engine}")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.files.maxPartitionBytes", "256m")
    )
    if args.engine == "delta":
        builder = (
            builder
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )
    elif args.engine == "hudi":
        builder = builder.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    elif args.engine == "iceberg":
        warehouse = str(Path(args.iceberg_warehouse).expanduser().resolve())
        catalog = args.iceberg_catalog
        builder = (
            builder
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
            .config(f"spark.sql.catalog.{catalog}.type", "hadoop")
            .config(f"spark.sql.catalog.{catalog}.warehouse", warehouse)
        )
    return builder.getOrCreate()


def register_tables(spark: SparkSession, args: argparse.Namespace, data_root: Path) -> None:
    if args.engine == "delta":
        fmt = "delta"
        for table in TABLE_LIST:
            path = data_root / "delta" / table
            spark.read.format(fmt).load(str(path)).createOrReplaceTempView(table)
    elif args.engine == "hudi":
        fmt = "hudi"
        for table in TABLE_LIST:
            path = data_root / "hudi" / table
            spark.read.format(fmt).load(str(path)).createOrReplaceTempView(table)
    else:  # iceberg
        catalog = args.iceberg_catalog
        namespace = args.iceberg_namespace
        for table in TABLE_LIST:
            ident = f"{catalog}.{namespace}.{table}"
            spark.read.table(ident).createOrReplaceTempView(table)


def iter_queries(stream_dir: Path) -> Iterable[Path]:
    return sorted(stream_dir.glob("query_*.sql"))


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def update_latest_symlink(target_dir: Path, latest_dir: Path) -> None:
    try:
        if latest_dir.is_symlink() or latest_dir.exists():
            latest_dir.unlink()
        latest_dir.symlink_to(target_dir, target_is_directory=True)
    except OSError:
        pass


def main():
    args = parse_args()
    data_root = Path(args.data_root)
    streams_root = Path(args.streams_root)
    results_root = Path(args.results_root)

    timestamp = args.timestamp or datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    if args.streams.lower() == "auto":
        stream_dirs = detect_streams(streams_root)
    else:
        names = [s.strip() for s in args.streams.replace(",", " ").split() if s.strip()]
        stream_dirs = [streams_root / n for n in names]

    spark = build_spark(args)
    try:
        register_tables(spark, args, data_root)

        for stream_dir in stream_dirs:
            if not stream_dir.exists():
                print(f"[WARN] stream directory not found: {stream_dir}")
                continue
            qfiles = list(iter_queries(stream_dir))
            if not qfiles:
                print(f"[WARN] no queries in {stream_dir}")
                continue

            out_dir = results_root / args.engine / stream_dir.name / timestamp
            ensure_dir(out_dir)
            csv_path = out_dir / "results.csv"
            with csv_path.open("w", newline="", encoding="utf-8") as fh:
                writer = csv.writer(fh)
                writer.writerow([
                    "engine","stream","query",
                    "bytesRead","elapsedTime_s","executorRunTime_s","executorCpuTime_s",
                    "bytes_input_files","files_scanned","bytes_scanned",
                    "bytesRead_ev","files_scanned_ev","bytes_scanned_ev",
                    "executorRunTime_s_ev","executorCpuTime_s_ev",
                ])
                for qf in qfiles:
                    sql_text = Path(qf).read_text(encoding="utf-8").strip()
                    statements = _split_sql_statements(sql_text)
                    if not statements:
                        continue
                    main_idx = _main_statement_index(statements)
                    metrics = None
                    for idx, stmt in enumerate(statements):
                        if idx == main_idx:
                            if not args.no_warmup:
                                rq.run_one_query(
                                    spark,
                                    stmt,
                                    f"warmup-{stream_dir.name}-{qf.name}-{time.time_ns()}",
                                    args.action,
                                    args.rest_wait_ms,
                                    args.rest_poll_ms,
                                )
                            group_id = f"{stream_dir.name}-{qf.name}-{time.time_ns()}"
                            metrics = rq.run_one_query(
                                spark,
                                stmt,
                                group_id,
                                args.action,
                                args.rest_wait_ms,
                                args.rest_poll_ms,
                            )
                        else:
                            spark.sql(stmt)
                    if metrics is None:
                        continue
                    writer.writerow([
                        args.engine,
                        stream_dir.name,
                        qf.name,
                        metrics["bytesRead"],
                        f"{metrics['elapsedTime_s']:.3f}",
                        f"{metrics['executorRunTime_s']:.3f}",
                        f"{metrics['executorCpuTime_s']:.3f}",
                        f"{metrics['bytes_input_files']:.3f}",
                        metrics["files_scanned"],
                        f"{metrics['bytes_scanned']:.3f}",
                        f"{metrics['bytesRead_ev']:.3f}",
                        metrics["files_scanned_ev"],
                        f"{metrics['bytes_scanned_ev']:.3f}",
                        f"{metrics['executorRunTime_s_ev']:.3f}",
                        f"{metrics['executorCpuTime_s_ev']:.3f}",
                    ])

            latest = results_root / args.engine / stream_dir.name / "latest"
            update_latest_symlink(out_dir, latest)
            print(f"[OK] Results for {args.engine} {stream_dir.name} -> {csv_path}")

    finally:
        spark.stop()

    print("[DONE] TPCH all queries executed.")


if __name__ == "__main__":
    main()
