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
import json

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
    ap.add_argument("--hudi-layouts", default="no_layout,linear,zorder,hilbert",
                    help="Comma-separated Hudi layouts to materialise (default: no_layout,linear,zorder,hilbert).")
    ap.add_argument("--hudi-table-config", default="",
                    help="Optional JSON file overriding per-table Hudi configs.")

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


def write_hudi(df: DataFrame, table: str, layout: str, cfg: Dict, dst: Path, overwrite: bool) -> None:
    dst_abs = dst.expanduser().resolve()
    ensure_local_dir(dst_abs.parent)

    record_keys = cfg["record_key"]                 # list
    precombine_field = cfg["precombine_field"]      # str
    partition_field = cfg.get("partition_field", "")  # str, e.g. "a,b" or ""
    sort_columns = cfg.get("sort_columns", [])      # list
    raw_layout_strategy = cfg.get("layout_strategy", "").strip()

    layout_norm = layout.strip().lower()
    strategy_map = {
        "linear": "linear",
        "zorder": "z-order",
        "z-order": "z-order",
        "hilbert": "hilbert",
    }
    layout_enable = layout_norm != "no_layout"
    layout_strategy = raw_layout_strategy or strategy_map.get(layout_norm, "")
    if layout_enable and not sort_columns:
        # Fallback: keep clustering deterministic if scenario forgot sort columns.
        sort_columns = record_keys

    # 关键改动：按 partition 字段数量选择 key generator
    partition_fields = [f.strip() for f in partition_field.split(",") if f.strip()]

    if len(partition_fields) == 0:
        # 无分区
        keygen = "org.apache.hudi.keygen.NonpartitionedKeyGenerator"
        partition_path_opt = ""
    elif len(partition_fields) == 1:
        # 单字段分区 → SimpleKeyGenerator
        keygen = "org.apache.hudi.keygen.SimpleKeyGenerator"
        partition_path_opt = partition_fields[0]
    else:
        # 多字段分区 → ComplexKeyGenerator
        keygen = "org.apache.hudi.keygen.ComplexKeyGenerator"
        partition_path_opt = ",".join(partition_fields)

    hudi_conf = {
        "hoodie.table.name": f"tpch_all_{table}_{layout}",
        "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
        "hoodie.datasource.write.operation": "bulk_insert",
        "hoodie.datasource.write.recordkey.field": ",".join(record_keys),
        "hoodie.datasource.write.precombine.field": precombine_field,
        "hoodie.datasource.write.partitionpath.field": partition_path_opt,
        "hoodie.datasource.write.keygenerator.class": keygen,
        # "hoodie.datasource.hive_sync.enable": "false",
        # "hoodie.datasource.write.hive_style_partitioning": "true",
        # "hoodie.write.markers.type": "DIRECT",
        # "hoodie.timeline.service.enabled": "false",
        # Clustering knobs: shared defaults for all layouts.
        "hoodie.clustering.inline": os.getenv("HUDI_CLUSTERING_INLINE", "true"),
        "hoodie.clustering.inline.max.commits": os.getenv("HUDI_CLUSTERING_MAX_COMMITS", "1"),
        "hoodie.clustering.plan.strategy.small.file.limit": os.getenv(
            "HUDI_CLUSTERING_SMALL_FILE_LIMIT", str(256 * 1024 * 1024)
        ),
        "hoodie.clustering.plan.strategy.target.file.max.bytes": os.getenv(
            "HUDI_CLUSTERING_TARGET_FILE_MAX_BYTES", str(256 * 1024 * 1024)
        ),
        "hoodie.clustering.plan.strategy.max.num.groups": os.getenv(
            "HUDI_CLUSTERING_MAX_NUM_GROUPS", "4096"
        ),
    }

    if layout_enable and layout_strategy:
        hudi_conf.update({
            "hoodie.clustering.plan.strategy.sort.columns": ",".join(sort_columns),
            "hoodie.clustering.layout.optimize.enable": "true",
            "hoodie.clustering.layout.optimize.strategy": layout_strategy,
        })
    else:
        # no_layout: no layout optimization and no explicit ordering columns.
        hudi_conf["hoodie.clustering.layout.optimize.enable"] = "false"
        hudi_conf["hoodie.clustering.inline"] = "false"

    # mode = "overwrite" if overwrite else "errorifexists"
    (
        df.write
        .format("hudi")
        .options(**hudi_conf)
        .mode("overwrite")
        .save(str(dst_abs))
    )


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
    hudi_layouts = [l.strip() for l in args.hudi_layouts.replace(",", " ").split() if l.strip()]
    wanted_tables = [t.strip() for t in args.tables.replace(",", " ").split() if t.strip()]
    engines = [e.strip().lower() for e in args.engines.replace(",", " ").split() if e.strip()]
    data_root = Path(args.data_root)
    src_root = Path(args.source)

    # Hudi layout-aware configs (defaults fall back to tpch_all_schemas)
    HUDI_TABLE_CONFIGS = {
        "lineitem": {
            "record_key": ["l_orderkey", "l_linenumber"],
            "precombine_field": "l_commitdate",
            "partition_field": "l_returnflag,l_linestatus",
            "sort_columns": ["l_shipdate", "l_receiptdate"],
        },
        "orders": {
            "record_key": ["o_orderkey"],
            "precombine_field": "o_orderdate",
            "partition_field": "o_orderstatus,o_orderpriority",
            "sort_columns": ["o_orderdate", "o_custkey"],
        },
    }

    if args.hudi_table_config:
        config_path = Path(args.hudi_table_config)
        if not config_path.exists():
            raise FileNotFoundError(f"Hudi table config not found: {config_path}")
        with config_path.open() as f:
            overrides = json.load(f)
        if not isinstance(overrides, dict):
            raise ValueError("Hudi table config JSON must be an object keyed by table name.")
        for tbl, cfg in overrides.items():
            if not isinstance(cfg, dict):
                continue
            HUDI_TABLE_CONFIGS[tbl] = {
                "record_key": cfg.get("record_key", HUDI_TABLE_CONFIGS.get(tbl, {}).get("record_key", TPCH_TABLES.get(tbl, {}).get("hudi", {}).get("record_key", []))),
                "precombine_field": cfg.get("precombine_field", HUDI_TABLE_CONFIGS.get(tbl, {}).get("precombine_field", "")),
                "partition_field": cfg.get("partition_field", HUDI_TABLE_CONFIGS.get(tbl, {}).get("partition_field", "")),
                "sort_columns": cfg.get("sort_columns", HUDI_TABLE_CONFIGS.get(tbl, {}).get("sort_columns", [])),
            }

    def hudi_config_for(table: str) -> Dict:
        base = TPCH_TABLES[table]["hudi"]
        cfg = HUDI_TABLE_CONFIGS.get(table, {})
        return {
            "record_key": cfg.get("record_key", base["record_key"]),
            "precombine_field": cfg.get("precombine_field", base["precombine_field"]),
            "partition_field": cfg.get("partition_field", base["partition_field"]),
            "sort_columns": cfg.get("sort_columns", []),
        }

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
                cfg = hudi_config_for(table)
                for layout in hudi_layouts:
                    dst = data_root / "hudi" / layout / table
                    print(f"  -> Hudi: {dst} (layout={layout})")
                    write_hudi(df, table, layout, cfg, dst, args.overwrite)

            if "iceberg" in engines:
                print(f"  -> Iceberg: {args.iceberg_catalog}.{args.iceberg_namespace}.{table}")
                write_iceberg(df, spark, table, args.iceberg_catalog, args.iceberg_namespace, args.overwrite)
    finally:
        spark.stop()

    print("[DONE] TPCH all tables materialised.")


if __name__ == "__main__":
    main()
