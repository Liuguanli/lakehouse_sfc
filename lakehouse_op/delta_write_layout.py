#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Delta write + layout for TPC-H style data.

Features
- Read from a Parquet directory; write to a Delta *path-based* table.
- Layout strategies:
    * baseline: no ordering
    * linear:   in-partition linear ordering before write
    * zorder:   post-write Z-Order (via OPTIMIZE)
- Post-write OPTIMIZE: none / compact / zorder / both (compact then zorder)
- Optional: WHERE clause to optimize only a subset
- Optional: pre-write repartition/coalesce to influence initial layout
- TPC-H helpers: auto-cast common date columns if present.

Tested with Delta 3.x on Spark 3.5.x.
"""

import argparse
import sys
from pathlib import Path
from typing import Iterable, List, Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

try:
    from delta.tables import DeltaTable
except Exception:
    DeltaTable = None

from io_loader import load_input_df, parse_kv_options


# ---------- path utilities (avoid relative path surprises) ----------
def abs_path(p: str) -> str:
    """Return an absolute, normalized local filesystem path (no scheme)."""
    return str(Path(p).expanduser().resolve())


def ensure_parent_dir(p: str) -> None:
    """Create parent dir for a local filesystem path."""
    Path(p).parent.mkdir(parents=True, exist_ok=True)


# ---------- column list helpers ----------
def _normalize_cols(v: Iterable[str] | str | None) -> List[str]:
    """
    Accept either:
      - a list of tokens (possibly containing commas), or
      - a single comma/space separated string,
    and normalize to a clean list of column names.
    """
    if v is None:
        return []
    if isinstance(v, str):
        parts = v.replace(",", " ").split()
    else:
        parts = []
        for tok in v:
            parts.extend(tok.replace(",", " ").split())
    return [p for p in (p.strip() for p in parts) if p]


def _validate_columns(df_cols: List[str], want: List[str], label: str):
    missing = [c for c in want if c not in df_cols]
    if missing:
        raise ValueError(f"{label} not in schema: {missing}. Available: {df_cols}")


# ---------- CLI ----------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Delta write + layout (baseline/linear/zorder) + OPTIMIZE (compact/zorder/both)"
    )
    p.add_argument("--input", required=True, help="Source Parquet directory (local/HDFS-compatible path)")
    p.add_argument("--output", required=True, help="Target Delta path-table directory")

    p.add_argument(
        "--input-format",
        default="auto",
        help="Input format for spark.read (e.g. parquet, csv, json). Defaults to auto-detect.",
    )
    p.add_argument(
        "--input-option",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Additional spark.read options (repeatable, overrides defaults).",
    )

    p.add_argument("--mode", default="overwrite", choices=["overwrite", "append"], help="Save mode")
    p.add_argument(
        "--partition-by",
        nargs="*",
        default=["l_shipdate"],  # sensible TPC-H default; override as needed
        help="Partition columns (space or comma separated)",
    )

    # Layout strategy: baseline / linear / zorder
    p.add_argument(
        "--layout",
        default="baseline",
        choices=["baseline", "linear", "zorder"],
        help="baseline=no ordering; linear=sortWithinPartitions before write; zorder=OPTIMIZE ZORDER after write",
    )
    p.add_argument(
        "--layout-cols",
        nargs="*",
        default=["l_shipdate", "l_orderkey"],  # good TPC-H defaults
        help="Columns used by linear sorting or z-order (space/comma separated)",
    )

    # Pre-write soft layout
    p.add_argument("--repartition", type=int, default=0, help="Repartition before write (0 = keep)")
    p.add_argument(
        "--range-cols",
        nargs="*",
        default=["l_shipdate", "l_orderkey"],
        help="Columns for repartitionByRange (space/comma separated, optional)",
    )
    p.add_argument("--coalesce", type=int, default=0, help="Coalesce before write (mutually exclusive with --repartition)")
    p.add_argument("--overwrite-schema", action="store_true", help="Overwrite schema when mode=overwrite")

    # Post-write OPTIMIZE
    p.add_argument(
        "--optimize",
        default="none",
        choices=["none", "compact", "zorder", "both"],
        help="OPTIMIZE behavior: compact (merge small files), zorder, both, or none",
    )
    p.add_argument(
        "--optimize-where",
        default="",
        help='Optional WHERE clause to restrict OPTIMIZE scope (e.g. "l_shipdate >= \'1996-01-01\'")',
    )

    # TPC-H convenience: auto-cast date-like columns if present
    p.add_argument(
        "--date-cols",
        nargs="*",
        default=["l_shipdate", "l_commitdate", "l_receiptdate"],
        help="Columns to cast to DATE if present (space/comma separated)",
    )

    return p.parse_args()


# ---------- Spark ----------
def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("delta-write-layout")
        # Delta extensions (safety net even if not provided on spark-submit)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )


# ---------- Dataframe transforms ----------
def repartition_df(df, num_parts: int, range_cols: List[str], coalesce: int):
    if coalesce > 0 and num_parts > 0:
        raise ValueError("--coalesce and --repartition are mutually exclusive")
    if coalesce > 0:
        return df.coalesce(coalesce)
    if num_parts > 0:
        if range_cols:
            return df.repartitionByRange(num_parts, *[F.col(c) for c in range_cols])
        return df.repartition(num_parts)
    return df


def apply_linear_ordering(df, layout_cols: List[str]):
    # Stable in-partition ordering; for stronger global effects, combine with repartitionByRange.
    if not layout_cols:
        return df
    return df.sortWithinPartitions(*[F.col(c) for c in layout_cols])


# ---------- IO ----------
def write_delta(df, path: str, mode: str, partition_by: List[str], overwrite_schema: bool):
    writer = df.write.format("delta").mode(mode)
    if partition_by:
        writer = writer.partitionBy(partition_by)
    if overwrite_schema and mode == "overwrite":
        writer = writer.option("overwriteSchema", "true")
    writer.save(path)


# ---------- OPTIMIZE ----------
def optimize_with_api(spark: SparkSession, path: str, where: Optional[str],
                      zcols: List[str], do_compact: bool, do_zorder: bool):
    if DeltaTable is None:
        raise RuntimeError("delta.tables.DeltaTable not found (ensure delta-spark package is provided).")
    dt = DeltaTable.forPath(spark, path)
    if do_compact:
        (dt.optimize().where(where).executeCompaction() if where else dt.optimize().executeCompaction())
    if do_zorder and zcols:
        (dt.optimize().where(where).executeZOrderBy(*zcols) if where else dt.optimize().executeZOrderBy(*zcols))


def optimize_with_sql(spark: SparkSession, path: str, where: Optional[str],
                      zcols: List[str], do_compact: bool, do_zorder: bool):
    if do_compact and not do_zorder:
        spark.sql(f"OPTIMIZE delta.`{path}`" + (f" WHERE {where}" if where else ""))
    elif do_zorder:
        zlist = ", ".join(zcols) if zcols else ""
        if not zlist:
            spark.sql(f"OPTIMIZE delta.`{path}`" + (f" WHERE {where}" if where else ""))
        else:
            spark.sql(
                f"OPTIMIZE delta.`{path}`"
                + (f" WHERE {where}" if where else "")
                + f" ZORDER BY ({zlist})"
            )


# ---------- main ----------
def main():
    args = parse_args()
    spark = build_spark()

    # Normalize possibly-quoted/comma-separated inputs into clean lists
    args.partition_by = _normalize_cols(args.partition_by)
    args.layout_cols  = _normalize_cols(args.layout_cols)
    args.range_cols   = _normalize_cols(args.range_cols)
    args.date_cols    = _normalize_cols(args.date_cols)

    input_path = abs_path(args.input)
    output_path = abs_path(args.output)
    ensure_parent_dir(output_path)

    input_options = parse_kv_options(args.input_option)
    df = load_input_df(spark, input_path, fmt=args.input_format, options=input_options)

    # Validate columns (early, friendlier messages)
    _validate_columns(df.columns, args.partition_by, "partition-by")
    _validate_columns(df.columns, args.range_cols, "range-cols")
    _validate_columns(df.columns, args.layout_cols, "layout-cols (before pruning)")

    # TPC-H: cast common date-like columns if present
    for c in args.date_cols:
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast("date"))

    # Pre-write soft layout
    df = repartition_df(df, args.repartition, args.range_cols, args.coalesce)

    # Pre-write layout: linear -> in-partition sort; baseline -> no-op; zorder -> post-write
    layout_cols = args.layout_cols or []
    if args.layout == "linear":
        df = apply_linear_ordering(df, layout_cols)

    # Write Delta
    write_delta(df, output_path, args.mode, args.partition_by, args.overwrite_schema)

    # Post-write OPTIMIZE
    do_compact = args.optimize in ("compact", "both")
    do_zorder_flag = args.optimize in ("zorder", "both")
    do_zorder_layout = (args.layout == "zorder")
    do_zorder = do_zorder_flag or do_zorder_layout

    # IMPORTANT: Delta cannot Z-Order on partition columns -> prune them
    partition_set = set(args.partition_by or [])
    zcols_effective = [c for c in layout_cols if c not in partition_set]
    if do_zorder and not zcols_effective:
        print("[WARN] Z-Order requested but all columns provided are partition columns; skipping Z-Order.")
        do_zorder = False

    if do_compact or do_zorder:
        try:
            optimize_with_api(spark, output_path, args.optimize_where or None,
                              zcols_effective, do_compact, do_zorder)
        except Exception:
            optimize_with_sql(spark, output_path, args.optimize_where or None,
                              zcols_effective, do_compact, do_zorder)

    print(
        f"[OK] Delta write complete | layout={args.layout} | optimize={args.optimize} "
        f"| zcols={zcols_effective} | output={output_path}"
    )
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
