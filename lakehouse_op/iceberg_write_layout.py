#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Iceberg write + layout for TPC-H style data (aligned with Delta/Hudi scripts).

Layout:
  - baseline: no ordering
  - linear  : pre-write sortWithinPartitions (optionally with repartitionByRange)
  - zorder  : post-write rewrite with sort strategy using zorder(...)

Optimize (post-write rewrite):
  - none / binpack / sort / zorder / both
  - 'both' means compaction + zorder
  - If layout=linear and optimize=none, we still trigger a sort rewrite once to align historical data.

Other:
  - --repartition / --range-cols / --coalesce to influence initial file layout
  - --partition-by for CTAS/append partitioning
  - --date-cols to cast common TPCH date columns
  - --rewrite-where to limit rewrite scope
"""

import argparse
import os
from typing import Iterable, List

from pyspark.sql import SparkSession, DataFrame, functions as F

from lakehouse_op.io_loader import load_input_df, parse_kv_options


def _abs_path(p: str) -> str:
    return os.path.abspath(os.path.expanduser(p))


def _normalize_cols(v: Iterable[str] | str | None) -> List[str]:
    if v is None:
        return []
    if isinstance(v, str):
        parts = v.replace(",", " ").split()
    else:
        parts = []
        for tok in v:
            parts.extend(tok.replace(",", " ").split())
    return [x for x in (p.strip() for p in parts) if x]


def _validate_columns(df_cols: List[str], want: List[str], label: str):
    missing = [c for c in want if c not in df_cols]
    if missing:
        raise ValueError(f"{label} not in schema: {missing}. Available: {df_cols}")


def build_spark(warehouse: str) -> SparkSession:
    wh = _abs_path(warehouse)
    return (
        SparkSession.builder
        .appName("iceberg-write-layout")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", wh)
        .getOrCreate()
    )


def repartition_df(df: DataFrame, num_parts: int, range_cols: List[str], coalesce: int) -> DataFrame:
    if coalesce > 0 and num_parts > 0:
        raise ValueError("--coalesce and --repartition are mutually exclusive")
    if coalesce > 0:
        return df.coalesce(coalesce)
    if num_parts > 0:
        if range_cols:
            return df.repartitionByRange(num_parts, *[F.col(c) for c in range_cols])
        return df.repartition(num_parts)
    return df


def apply_linear(df: DataFrame, layout_cols: List[str]) -> DataFrame:
    if not layout_cols:
        return df
    return df.sortWithinPartitions(*[F.col(c) for c in layout_cols])


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Iceberg write + layout (baseline/linear/zorder) + rewrite (binpack/sort/zorder/both)"
    )
    p.add_argument("--input", required=True, help="Source Parquet directory")
    p.add_argument("--warehouse", required=True, help="HadoopCatalog warehouse directory")
    p.add_argument("--table-identifier", required=True, help="Iceberg table id, e.g., local.db.table")

    p.add_argument(
        "--input-format",
        default="auto",
        help="Input format for spark.read (parquet/csv/..., default: auto detect).",
    )
    p.add_argument(
        "--input-option",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Additional spark.read options (repeatable).",
    )

    p.add_argument("--mode", default="overwrite", choices=["overwrite", "append"], help="Write mode")

    p.add_argument("--partition-by", nargs="*", default=["l_returnflag", "l_linestatus"],
                   help="Partition columns (space/comma separated)")

    p.add_argument("--layout", choices=["baseline", "linear", "zorder"], default="baseline",
                   help="baseline=no ordering; linear=sortWithinPartitions; zorder=post-write zorder(...)")
    p.add_argument("--layout-cols", nargs="*", default=["l_shipdate", "l_receiptdate"],
                   help="Columns used by linear/zorder")

    p.add_argument("--repartition", type=int, default=0, help="Repartition before write (0=keep)")
    p.add_argument("--range-cols", nargs="*", default=["l_shipdate", "l_receiptdate"],
                   help="Columns for repartitionByRange")
    p.add_argument("--coalesce", type=int, default=0, help="Coalesce before write")

    p.add_argument("--date-cols", nargs="*", default=["l_shipdate", "l_commitdate", "l_receiptdate"],
                   help="Columns to cast to DATE if present")

    p.add_argument("--optimize", choices=["none", "binpack", "sort", "zorder", "both"], default="none",
                   help="Post-write rewrite strategy")
    p.add_argument("--target-file-mb", type=int, default=128, help="Target file size for rewrite (MB)")
    p.add_argument("--rewrite-where", default="", help="WHERE filter for rewrite procedures")

    return p.parse_args()


def main() -> int:
    args = parse_args()
    spark = build_spark(args.warehouse)

    # Normalize lists
    args.partition_by = _normalize_cols(args.partition_by)
    args.layout_cols  = _normalize_cols(args.layout_cols)
    args.range_cols   = _normalize_cols(args.range_cols)
    args.date_cols    = _normalize_cols(args.date_cols)

    input_path = _abs_path(args.input)
    table_id = args.table_identifier

    input_options = parse_kv_options(args.input_option)
    df = load_input_df(spark, input_path, fmt=args.input_format, options=input_options)

    # Validate columns
    if args.partition_by:
        _validate_columns(df.columns, args.partition_by, "partition-by")
    if args.range_cols:
        _validate_columns(df.columns, args.range_cols, "range-cols")
    if args.layout_cols:
        _validate_columns(df.columns, args.layout_cols, "layout-cols")

    # Cast date columns
    for c in args.date_cols:
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast("date"))

    # Pre-layout shaping
    df = repartition_df(df, args.repartition, args.range_cols, args.coalesce)

    # Linear in-partition ordering
    if args.layout == "linear":
        df = apply_linear(df, args.layout_cols)

    # Namespace
    ns = ".".join(table_id.split(".")[:-1])
    if ns:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ns}")

    # Use a temp view so CTAS/INSERT uses the shaped DataFrame
    df.createOrReplaceTempView("_tmp_src")

    # Partitioning clause
    partition_clause = ""
    if args.partition_by:
        cols = ", ".join(args.partition_by)
        partition_clause = f"PARTITIONED BY ({cols})"

    # Overwrite/append
    if args.mode == "overwrite":
        spark.sql(f"""
            CREATE OR REPLACE TABLE {table_id}
            USING iceberg
            {partition_clause}
            AS SELECT * FROM _tmp_src
        """)
    else:
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {table_id}
            USING iceberg
            {partition_clause}
        """)
        spark.sql(f"INSERT INTO {table_id} SELECT * FROM _tmp_src")

    # Write ordering (helps future incremental writes/rewrite)
    if args.layout in ("linear", "zorder") and args.layout_cols:
        cols = ", ".join(args.layout_cols)
        spark.sql(f"ALTER TABLE {table_id} WRITE ORDERED BY {cols}")

    # -------- Post-write rewrite --------
    target_bytes = max(1, args.target_file_mib if hasattr(args, "target_file_mib") else args.target_file_mb) * 1024 * 1024
    where_clause = f", where => '{args.rewrite_where}'" if args.rewrite_where else ""

    do_binpack = args.optimize in ("binpack", "both")
    do_sort_flag = args.optimize in ("sort", "both")
    do_zorder = (args.optimize in ("zorder", "both")) or (args.layout == "zorder")
    # If layout=linear and optimize=none, still run a single sort rewrite
    do_sort = do_sort_flag or (args.layout == "linear")

    # 1) binpack only
    if do_binpack and not (do_sort or do_zorder):
        spark.sql(f"""
          CALL local.system.rewrite_data_files(
            table => '{table_id}',
            options => map(
              'min-file-size-bytes', '{max(1, target_bytes // 2)}',
              'target-file-size-bytes', '{target_bytes}'
            ){where_clause}
          )
        """)

    # 2) sort only
    if do_sort and not do_zorder:
        if args.layout_cols:
            sort_expr = ",".join(args.layout_cols)
            spark.sql(f"""
              CALL local.system.rewrite_data_files(
                table => '{table_id}',
                strategy => 'sort',
                sort_order => '{sort_expr}',
                options => map(
                  'target-file-size-bytes', '{target_bytes}'
                ){where_clause}
              )
            """)
        else:
            spark.sql(f"""
              CALL local.system.rewrite_data_files(
                table => '{table_id}',
                strategy => 'sort',
                options => map(
                  'target-file-size-bytes', '{target_bytes}'
                ){where_clause}
              )
            """)

    # 3) zorder (SFC)
    if do_zorder:
        if not args.layout_cols:
            raise SystemExit("zorder requires --layout-cols, e.g., --layout-cols 'l_shipdate,l_receiptdate'")
        zexpr = f"zorder({', '.join(args.layout_cols)})"
        spark.sql(f"""
          CALL local.system.rewrite_data_files(
            table => '{table_id}',
            strategy => 'sort',
            sort_order => '{zexpr}',
            options => map(
              'target-file-size-bytes', '{target_bytes}'
            ){where_clause}
          )
        """)

    print(f"[OK] Iceberg write complete | layout={args.layout} | optimize={args.optimize} "
          f"| cols={args.layout_cols} | table={table_id}")
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
