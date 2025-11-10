#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Ingest all TPCH tables (CSV) into Delta, Hudi, and Iceberg lakehouse formats.

This loader is standalone (does not depend on WLG workloads) so external
benchmarks can materialize multi-table datasets easily.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession

from lakehouse_op.tpch_all_schemas import TPCH_TABLES, TABLE_LIST


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser("TPCH ALL table loader")
    ap.add_argument("--source", default="/datasets/tpch_1/data",
                    help="Directory containing TPCH CSV files (customer.csv, ...).")
    ap.add_argument("--tables", default=",".join(TABLE_LIST),
                    help="Comma-separated subset of tables to load (default: all).")
    ap.add_argument("--engines", default="delta,hudi,iceberg",
                    help="Comma-separated target engines.")
    ap.add_argument("--data-root", default="./data/tpch_all",
                    help="Root directory for materialised tables.")
    ap.add_argument("--overwrite", action="store_true",
                    help="Overwrite existing outputs (default: skip existing tables).")

    # Iceberg specific
    ap.add_argument("--iceberg-catalog", default="tpchall",
                    help="Spark catalog name for Iceberg (default: tpchall).")
    ap.add_argument("--iceberg-namespace", default="tpch_all",
                    help="Iceberg namespace/database for tables.")
    ap.add_argument("--iceberg-warehouse", default="./data/tpch_all/iceberg_wh",
                    help="Iceberg warehouse directory (Hadoop catalog).")

    return ap.parse_args()


def build_spark(args: argparse.Namespace) -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("tpch-all-loader")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    )

    # Delta + Iceberg extensions are harmless even if not used for a run
    builder = (
        builder
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension,"
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config(f"spark.sql.catalog.{args.iceberg_catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{args.iceberg_catalog}.type", "hadoop")
        .config(f"spark.sql.catalog.{args.iceberg_catalog}.warehouse",
                str(Path(args.iceberg_warehouse).expanduser().resolve()))
    )

    return builder.getOrCreate()


def read_table(spark: SparkSession, src_root: Path, table: str) -> DataFrame:
    spec = TPCH_TABLES[table]
    csv_path = src_root / spec["filename"]
    if not csv_path.exists():
        raise FileNotFoundError(f"Missing source file for table {table}: {csv_path}")
    df = (
        spark.read
        .option("sep", "|")
        .option("header", "false")
        .option("dateFormat", "yyyy-MM-dd")
        .schema(spec["schema"])
        .csv(str(csv_path))
        .drop("_dummy")
    )
    return df


def ensure_local_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def write_delta(df: DataFrame, dst: Path, overwrite: bool) -> None:
    dst_abs = dst.expanduser().resolve()
    ensure_local_dir(dst_abs.parent)
    mode = "overwrite" if overwrite else "errorifexists"
    df.write.format("delta").mode(mode).save(str(dst_abs))


def write_hudi(df: DataFrame, table: str, dst: Path, spec: Dict, overwrite: bool) -> None:
    dst_abs = dst.expanduser().resolve()
    ensure_local_dir(dst_abs.parent)
    record_keys = spec["hudi"]["record_key"]
    partition_field = spec["hudi"]["partition_field"]
    if len(record_keys) > 1:
        keygen = "org.apache.hudi.keygen.ComplexKeyGenerator"
    else:
        keygen = "org.apache.hudi.keygen.SimpleKeyGenerator" if partition_field else \
            "org.apache.hudi.keygen.NonpartitionedKeyGenerator"

    hudi_conf = {
        "hoodie.table.name": f"tpch_all_{table}",
        "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
        "hoodie.datasource.write.operation": "bulk_insert",
        "hoodie.datasource.write.recordkey.field": ",".join(record_keys),
        "hoodie.datasource.write.precombine.field": spec["hudi"]["precombine_field"],
        "hoodie.datasource.write.partitionpath.field": partition_field,
        "hoodie.datasource.write.keygenerator.class": keygen,
        "hoodie.datasource.hive_sync.enable": "false",
        "hoodie.datasource.write.hive_style_partitioning": "true",
        "hoodie.write.markers.type": "DIRECT",
        "hoodie.timeline.service.enabled": "false",
    }
    mode = "overwrite" if overwrite else "errorifexists"
    (df.write.format("hudi")
       .options(**hudi_conf)
       .mode(mode)
       .save(str(dst_abs)))


def write_iceberg(
    df: DataFrame,
    spark: SparkSession,
    table: str,
    catalog: str,
    namespace: str,
    overwrite: bool,
) -> None:
    table_ident = f"{catalog}.{namespace}.{table}"
    writer = df.writeTo(table_ident).using("iceberg")
    if overwrite:
        writer.createOrReplace()
    else:
        writer.create()


def main():
    args = parse_args()
    wanted_tables = [t.strip() for t in args.tables.replace(",", " ").split() if t.strip()]
    engines = [e.strip().lower() for e in args.engines.replace(",", " ").split() if e.strip()]
    data_root = Path(args.data_root)
    src_root = Path(args.source)

    spark = build_spark(args)
    try:
        for table in wanted_tables:
            if table not in TPCH_TABLES:
                print(f"[WARN] Unknown table '{table}', skipping.")
                continue
            print(f"[LOAD] Reading table {table}")
            df = read_table(spark, src_root, table)

            if "delta" in engines:
                dst = data_root / "delta" / table
                print(f"  -> Delta: {dst}")
                write_delta(df, dst, args.overwrite)

            if "hudi" in engines:
                dst = data_root / "hudi" / table
                print(f"  -> Hudi: {dst}")
                write_hudi(df, table, dst, TPCH_TABLES[table], args.overwrite)

            if "iceberg" in engines:
                print(f"  -> Iceberg: {args.iceberg_catalog}.{args.iceberg_namespace}.{table}")
                write_iceberg(df, spark, table, args.iceberg_catalog, args.iceberg_namespace, args.overwrite)
    finally:
        spark.stop()

    print("[DONE] TPCH all tables materialised.")


if __name__ == "__main__":
    main()
