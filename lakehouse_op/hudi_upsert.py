#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Upsert/insert a batch of TPCH rows into an existing Hudi table.

Example:
  spark-submit --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:1.0.2 \
    lakehouse_op/hudi_upsert.py \
      --input ./data/tmp/rq7_tpch4_seed42/batch_01 \
      --table-path ./data/tpch_16/hudi/hudi_no_layout \
      --table-name events_hudi_no_layout \
      --record-key l_orderkey,l_linenumber \
      --precombine-field l_commitdate \
      --partition-field l_returnflag,l_linestatus
"""

import argparse
from pathlib import Path
from typing import List

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from io_loader import load_input_df, parse_kv_options


def build_spark(app_name: str, shuffle_partitions: int) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .getOrCreate()
    )


def str_bool(v: str) -> bool:
    return v.lower() in ("1", "true", "yes", "y", "on")


def parse_csv_list(s: str) -> List[str]:
    return [c.strip() for c in s.split(",") if c.strip()]


def main() -> None:
    p = argparse.ArgumentParser(description="Upsert/insert data into an existing Hudi table.")
    p.add_argument("--input", required=True, help="Input path (parquet/csv/etc.)")
    p.add_argument("--input-format", default="auto", help="spark.read format (default: auto detect)")
    p.add_argument(
        "--input-option",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Additional spark.read options (repeatable)",
    )

    p.add_argument("--table-path", required=True, help="Target Hudi table path")
    p.add_argument("--table-name", required=True, help="Target Hudi table name")
    p.add_argument("--record-key", required=True, help="Hudi record key(s), comma-separated")
    p.add_argument("--precombine-field", required=True, help="Precombine field")
    p.add_argument("--partition-field", default="", help="Partition field(s), comma-separated (empty for none)")
    p.add_argument("--operation", choices=["upsert", "insert"], default="upsert", help="Write operation (default: upsert)")

    p.add_argument("--sort-columns", default="", help="Optional sort columns (comma-separated)")
    p.add_argument("--target-file-mb", type=int, default=128, help="Target file size MB (default: 128)")
    p.add_argument("--small-file-limit-mb", type=int, default=256, help="Small file limit MB (default: 256)")
    p.add_argument("--max-num-groups", type=int, default=4096, help="Max clustering groups (default: 4096)")
    p.add_argument("--compression", default="snappy", help="Parquet compression (default: snappy)")

    p.add_argument("--coalesce", type=int, default=0, help="Optional coalesce before write (0 = disabled)")
    p.add_argument("--date-cols", default="l_shipdate,l_commitdate,l_receiptdate", help="Columns to cast to DATE if present")

    p.add_argument("--app-name", default="hudi-upsert", help="Spark app name")
    p.add_argument("--shuffle-partitions", type=int, default=200, help="spark.sql.shuffle.partitions")
    args = p.parse_args()

    spark = build_spark(args.app_name, args.shuffle_partitions)

    df = load_input_df(spark, args.input, fmt=args.input_format, options=parse_kv_options(args.input_option))

    # Keep common TPCH date columns consistent
    for c in parse_csv_list(args.date_cols):
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast("date"))

    if args.coalesce and args.coalesce > 0:
        df = df.coalesce(args.coalesce)

    partition_field = args.partition_field.strip()
    partition_field_csv = ",".join(parse_csv_list(partition_field)) if partition_field else ""
    record_keys_csv = ",".join(parse_csv_list(args.record_key))
    sort_cols_csv = ",".join(parse_csv_list(args.sort_columns))

    keygen = "org.apache.hudi.keygen.ComplexKeyGenerator" if partition_field_csv else "org.apache.hudi.keygen.NonpartitionedKeyGenerator"

    options = {
        "hoodie.table.name": args.table_name,
        "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
        "hoodie.datasource.write.operation": args.operation,
        "hoodie.datasource.write.recordkey.field": record_keys_csv,
        "hoodie.datasource.write.precombine.field": args.precombine_field,
        "hoodie.datasource.write.partitionpath.field": partition_field_csv,
        "hoodie.datasource.write.keygenerator.class": keygen,
        "hoodie.datasource.write.hive_style_partitioning": "true",
        "hoodie.datasource.write.payload.class": "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload",
        # Clustering hints
        "hoodie.clustering.plan.strategy.small.file.limit": str(args.small_file_limit_mb * 1024 * 1024),
        "hoodie.clustering.plan.strategy.target.file.max.bytes": str(args.target_file_mb * 1024 * 1024),
        "hoodie.clustering.plan.strategy.max.num.groups": str(args.max_num_groups),
        # File format
        "hoodie.parquet.compression.codec": args.compression,
        # Shuffle parallelism
        "hoodie.upsert.shuffle.parallelism": str(args.shuffle_partitions),
        "hoodie.insert.shuffle.parallelism": str(args.shuffle_partitions),
    }
    if sort_cols_csv:
        options["hoodie.clustering.plan.strategy.sort.columns"] = sort_cols_csv

    options = {k: v for k, v in options.items() if v is not None}

    print(f"[HUDI] Writing to {args.table_path} with operation={args.operation}")
    df.write.format("hudi").options(**options).mode("append").save(args.table_path)

    spark.stop()
    print("[HUDI] Upsert complete.")


if __name__ == "__main__":
    main()
