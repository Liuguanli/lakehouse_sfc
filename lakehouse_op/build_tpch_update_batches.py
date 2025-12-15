#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Shuffle TPCH data and split into N batches for incremental inserts.

Example:
  spark-submit lakehouse_op/build_tpch_update_batches.py \
    --source /datasets/tpch_4.parquet \
    --output-dir ./data/tmp/rq7_tpch4_seed42 \
    --batches 10 --seed 42
"""

import argparse
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from io_loader import load_input_df, parse_kv_options


def build_spark(app_name: str, shuffle_partitions: int) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )


def main() -> None:
    p = argparse.ArgumentParser(description="Shuffle TPCH data and split into N batches.")
    p.add_argument("--source", required=True, help="Input dataset path (e.g., /datasets/tpch_4.parquet)")
    p.add_argument("--output-dir", required=True, help="Output root for batches (one subdir per batch)")
    p.add_argument("--batches", type=int, default=10, help="Number of batches to create (default: 10)")
    p.add_argument("--seed", type=int, default=42, help="Random seed for shuffling (default: 42)")
    p.add_argument("--input-format", default="auto", help="spark.read format (default: auto detect)")
    p.add_argument(
        "--input-option",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Additional spark.read options (repeatable)",
    )
    p.add_argument("--coalesce", type=int, default=0, help="Optional coalesce before write (0 = disabled)")
    p.add_argument("--app-name", default="tpch-batch-builder", help="Spark app name")
    p.add_argument("--shuffle-partitions", type=int, default=200, help="spark.sql.shuffle.partitions")
    args = p.parse_args()

    if args.batches <= 0:
        raise SystemExit("Batches must be > 0")

    out_dir = Path(args.output_dir).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    spark = build_spark(args.app_name, args.shuffle_partitions)
    df = load_input_df(spark, args.source, fmt=args.input_format, options=parse_kv_options(args.input_option))

    # Shuffle once and assign batch_id evenly via ntile
    df = df.withColumn("_rnd", F.rand(args.seed))
    df = df.withColumn("batch_id", F.ntile(args.batches).over(Window.orderBy(F.col("_rnd"))))
    df = df.drop("_rnd").cache()

    total = df.count()
    print(f"[BATCH] Total rows: {total}")
    for batch_id in range(1, args.batches + 1):
        batch_df = df.filter(F.col("batch_id") == batch_id).drop("batch_id")
        if args.coalesce and args.coalesce > 0:
            batch_df = batch_df.coalesce(args.coalesce)
        target = out_dir / f"batch_{batch_id:02d}"
        print(f"[BATCH] Writing batch {batch_id}/{args.batches} -> {target}")
        batch_df.write.mode("overwrite").parquet(str(target))

    spark.stop()
    print("[BATCH] Done.")


if __name__ == "__main__":
    main()
