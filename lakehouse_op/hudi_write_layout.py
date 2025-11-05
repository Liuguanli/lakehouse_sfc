#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Hudi writer for TPC-H lineitem (COPY_ON_WRITE + bulk_insert)

Defaults tailored for TPC-H lineitem:
- record key:        l_orderkey,l_linenumber
- partition field:   l_shipdate
- preCombine field:  l_shipdate
- layout columns:    l_shipdate,l_orderkey
- auto-cast date columns: l_shipdate,l_commitdate,l_receiptdate

All paths and names come from CLI args (no hard-coded paths).
You can override any default via CLI flags.
"""

import argparse
from typing import List
from pyspark.sql import SparkSession

# --- add imports at the top ---
from pathlib import Path
from urllib.parse import urlparse

from lakehouse_op.io_loader import load_input_df, parse_kv_options

def normalize_uri(p: str) -> str:
    """
    Convert any local path (relative or absolute) to an absolute file URI (file:///...).
    If the input already has a non-local scheme (hdfs://, s3a://, abfs://), keep it.
    """
    u = urlparse(p)
    if u.scheme in ("hdfs", "s3a", "abfs", "abfss", "gs"):
        return p  # remote FS: leave as-is
    # treat as local path (relative or absolute)
    return Path(p).expanduser().resolve().as_uri()


def ensure_local_parent(uri: str) -> None:
    """Create parent dirs for local file:// URIs."""
    u = urlparse(uri)
    if u.scheme == "file":
        Path(u.path).parent.mkdir(parents=True, exist_ok=True)


def build_spark(app_name: str, shuffle_partitions: int) -> SparkSession:
    """Build a SparkSession suitable for Hudi bulk_insert + clustering."""
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .getOrCreate()
    )


def str_bool(v: str) -> bool:
    """Parse boolean from CLI string."""
    return v.lower() in ("1", "true", "yes", "y", "on")


def parse_csv_list(s: str) -> List[str]:
    return [c.strip() for c in s.split(",") if c.strip()]


def main():
    p = argparse.ArgumentParser(
        description="Hudi writer (TPC-H lineitem focused; layout & file-size controls; COW bulk_insert)"
    )

    # Required (no hard-coded paths)
    p.add_argument("--input", required=True, help="Source Parquet directory")
    p.add_argument("--table-path", required=True, help="Target Hudi table path")
    p.add_argument("--table-name", required=True, help="Target Hudi table name")

    p.add_argument(
        "--input-format",
        default="auto",
        help="spark.read format for the input (parquet/csv/..., default: auto detect).",
    )
    p.add_argument(
        "--input-option",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Additional spark.read options (repeatable).",
    )

    # Keys & partitioning (defaults for TPCH lineitem)
    p.add_argument("--record-key", default="l_orderkey,l_linenumber",
                   help="Hudi record key, comma-separated (default: l_orderkey,l_linenumber)")
    p.add_argument("--precombine-field", default="l_shipdate",
                   help="Hudi preCombine field (default: l_shipdate)")
    p.add_argument("--partition-field", default="l_shipdate",
                   help="Partition field (default: l_shipdate; empty for non-partitioned)")

    # Spark session knobs
    p.add_argument("--app-name", default="hudi-writer-lineitem", help="Spark application name")
    p.add_argument("--shuffle-partitions", type=int, default=200, help="spark.sql.shuffle.partitions")

    # Layout materialization (clustering)
    p.add_argument("--clustering-inline", type=str_bool, default=True,
                   help="Run clustering inline with the write (default: true)")
    p.add_argument("--clustering-every-n-commits", type=int, default=1,
                   help="Run clustering every N commits when inline (default: 1)")
    p.add_argument("--sort-columns", default="l_shipdate,l_orderkey",
                   help="Ordering columns for clustering, comma-separated (default: l_shipdate,l_orderkey)")

    # Layout optimization strategy (SFC/linear)
    p.add_argument("--layout-enable", type=str_bool, default=False,
                   help="Enable layout optimization in clustering (default: false)")
    p.add_argument("--layout-strategy", default="default",
                   choices=["default", "hilbert", "z-order", "linear"],
                   help="Layout optimization strategy (default/hilbert/z-order/linear)")

    # File size & compression
    p.add_argument("--target-file-mb", type=int, default=128,
                   help="Target data file size (MB) for clustering (default: 128)")
    p.add_argument("--small-file-limit-mb", type=int, default=256,
                   help="Small file limit (MB) for clustering plan (default: 256)")
    p.add_argument("--max-num-groups", type=int, default=4096,
                   help="Max number of clustering groups (default: 4096)")
    p.add_argument("--compression", default="snappy",
                   choices=["snappy", "zstd", "gzip", "lz4"],
                   help="Parquet compression codec (default: snappy)")
    p.add_argument("--parquet-max-mb", type=int, default=1024,
                   help="Upper bound of a single Parquet file (MB) (default: 1024)")
    p.add_argument("--parquet-block-mb", type=int, default=128,
                   help="Parquet block size (MB) (default: 128)")

    # Optional: coalesce before write
    p.add_argument("--coalesce", type=int, default=0,
                   help="Coalesce to N partitions before writing (0 = disabled)")

    # Optional: date columns to cast (lineitem)
    p.add_argument("--date-cols", default="l_shipdate,l_commitdate,l_receiptdate",
                   help="Columns to cast to DATE if present (default: l_shipdate,l_commitdate,l_receiptdate)")

    args = p.parse_args()

    input_options = parse_kv_options(args.input_option)
    spark = build_spark(args.app_name, args.shuffle_partitions)

    input_uri = normalize_uri(args.input)
    df = load_input_df(spark, input_uri, fmt=args.input_format, options=input_options)

    # Cast common TPCH lineitem date columns – keeps types consistent for partitioning & filters
    from pyspark.sql import functions as F
    for c in parse_csv_list(args.date_cols):
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast("date"))

    if args.coalesce and args.coalesce > 0:
        df = df.coalesce(args.coalesce)

    # Key generator
    if args.partition_field:
        keygen = "org.apache.hudi.keygen.ComplexKeyGenerator"
        partition_field = args.partition_field
    else:
        keygen = "org.apache.hudi.keygen.NonpartitionedKeyGenerator"
        partition_field = ""

    # Parse lists and to-bytes
    sort_cols = ",".join(parse_csv_list(args.sort_columns))
    record_keys = ",".join(parse_csv_list(args.record_key))

    target_bytes = str(args.target_file_mb * 1024 * 1024)
    small_limit_bytes = str(args.small_file_limit_mb * 1024 * 1024)
    parquet_max_bytes = str(args.parquet_max_mb * 1024 * 1024)
    parquet_block_bytes = str(args.parquet_block_mb * 1024 * 1024)

    # Hudi options (COPY_ON_WRITE + bulk_insert + clustering)
    options = {
        # Table identity & write mode
        "hoodie.table.name": args.table_name,
        "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
        "hoodie.datasource.write.operation": "bulk_insert",

        # Keys & partitioning
        "hoodie.datasource.write.recordkey.field": record_keys,
        "hoodie.datasource.write.precombine.field": args.precombine_field,
        "hoodie.datasource.write.partitionpath.field": partition_field,
        "hoodie.datasource.write.keygenerator.class": keygen,

        # Clustering triggers
        "hoodie.clustering.inline": str(args.clustering_inline).lower(),
        "hoodie.clustering.inline.max.commits": str(args.clustering_every_n_commits),

        # Clustering plan: file sizes & ordering
        "hoodie.clustering.plan.strategy.small.file.limit": small_limit_bytes,
        "hoodie.clustering.plan.strategy.target.file.max.bytes": target_bytes,
        "hoodie.clustering.plan.strategy.max.num.groups": str(args.max_num_groups),

        # # Layout optimization (SFC / linear)
        # "hoodie.clustering.plan.strategy.sort.columns": sort_cols,
        # "hoodie.clustering.layout.optimize.enable": str(args.layout_enable).lower(),
        # "hoodie.clustering.layout.optimize.strategy": args.layout_strategy,

        # Parquet & compression
        "hoodie.parquet.compression.codec": args.compression,
        "hoodie.parquet.max.file.size": parquet_max_bytes,
        "hoodie.parquet.block.size": parquet_block_bytes,
    }


    if args.layout_enable:
        options.update({
            "hoodie.clustering.plan.strategy.sort.columns": sort_cols,           # e.g. "l_shipdate,l_commitdate"
            "hoodie.clustering.layout.optimize.enable": "true",
            "hoodie.clustering.layout.optimize.strategy": args.layout_strategy,  # "z-order" | "hilbert" | "linear"
        })
    else:
        options.update({
            "hoodie.clustering.layout.optimize.enable": "false",
        })
        options.pop("hoodie.clustering.layout.optimize.strategy", None)

    table_uri = normalize_uri(args.table_path)
    ensure_local_parent(table_uri)

    (
        df.write.format("hudi")
        .options(**options)
        .mode("overwrite")
        .save(table_uri)
    )

    print(f"[OK] input={input_uri}  table={table_uri}")

    print(
        f"✅ Done. table={args.table_name} | path={args.table_path} | "
        f"key={record_keys} | partition={partition_field or '(none)'} | "
        f"layout.enable={args.layout_enable} | strategy={args.layout_strategy} | sort={sort_cols}"
    )
    spark.stop()


if __name__ == "__main__":
    main()
