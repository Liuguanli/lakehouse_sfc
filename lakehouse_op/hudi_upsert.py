#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Upsert/insert a batch of TPCH rows into an existing Hudi table.

Key features in this "final" version:
- Align incoming DataFrame schema to existing Hudi table schema.
- Works even if .hoodie/.schema is missing (common).
- Adds missing fields (e.g., record_id) and casts types to match table schema.
- Keeps hive_style_partitioning consistent (default false here).
"""

import argparse
import json
from pathlib import Path
from typing import Dict, List, Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from io_loader import load_input_df, parse_kv_options


# -------------------------
# Spark
# -------------------------

def build_spark(app_name: str, shuffle_partitions: int) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .getOrCreate()
    )


def parse_csv_list(s: str) -> List[str]:
    return [c.strip() for c in s.split(",") if c.strip()]


# -------------------------
# Schema alignment
# -------------------------

def get_hudi_table_spark_schema(spark: SparkSession, table_path: str) -> Optional[T.StructType]:
    """
    Most reliable way to get the target table schema:
    Spark reads Hudi table schema from metadata.
    """
    try:
        return spark.read.format("hudi").load(table_path).schema
    except Exception:
        return None


def _avro_type_to_spark(avro_type) -> Optional[T.DataType]:
    # union like ["null","long"] or ["null", {...}]
    if isinstance(avro_type, list):
        non_null = [t for t in avro_type if t != "null"]
        if len(non_null) == 1:
            return _avro_type_to_spark(non_null[0])
        return None

    if isinstance(avro_type, str):
        return {
            "string": T.StringType(),
            "int": T.IntegerType(),
            "long": T.LongType(),
            "float": T.FloatType(),
            "double": T.DoubleType(),
            "boolean": T.BooleanType(),
            "bytes": T.BinaryType(),
        }.get(avro_type)

    if isinstance(avro_type, dict):
        if avro_type.get("logicalType") == "date":
            return T.DateType()
        inner = avro_type.get("type")
        if isinstance(inner, str):
            return _avro_type_to_spark(inner)

    return None


def read_hudi_table_schema_from_file(table_path: str) -> Optional[Dict[str, T.DataType]]:
    """
    Fallback: try read .hoodie/.schema (may not exist in some setups).
    Return mapping: col_name -> Spark DataType
    """
    schema_file = Path(table_path) / ".hoodie" / ".schema"
    if not schema_file.exists():
        return None

    try:
        schema_json = json.loads(schema_file.read_text())
    except Exception:
        return None

    fields = schema_json.get("fields", [])
    out: Dict[str, T.DataType] = {}
    for f in fields:
        name = f.get("name")
        avro_t = f.get("type")
        if not name:
            continue
        spark_t = _avro_type_to_spark(avro_t)
        if spark_t is not None:
            out[name] = spark_t
    return out or None


def align_df_to_spark_schema(df, target_schema: T.StructType):
    """
    Align df to Spark StructType:
    - add missing columns as null casted to target types
    - cast existing columns to target types
    - reorder columns to match target schema order
    """
    target_fields = {f.name: f.dataType for f in target_schema.fields}

    for name, dtype in target_fields.items():
        if name not in df.columns:
            df = df.withColumn(name, F.lit(None).cast(dtype))
        else:
            df = df.withColumn(name, F.col(name).cast(dtype))

    ordered = [f.name for f in target_schema.fields]
    return df.select(*ordered)


def align_df_to_dtype_map(df, dtype_map: Dict[str, T.DataType]):
    """
    Align df to a mapping col->dtype (fallback method):
    - add missing as null casted
    - cast existing
    - reorder (mapped cols first)
    """
    for name, dtype in dtype_map.items():
        if name not in df.columns:
            df = df.withColumn(name, F.lit(None).cast(dtype))
        else:
            df = df.withColumn(name, F.col(name).cast(dtype))

    ordered = [c for c in dtype_map.keys() if c in df.columns] + [c for c in df.columns if c not in dtype_map]
    return df.select(*ordered)


def enforce_last_resort_fixes(df):
    """
    Last-resort safety net for known issues in your logs:
    - record_id must exist (nullable long)
    - l_quantity often mismatches (double vs long)
    """
    if "record_id" not in df.columns:
        df = df.withColumn("record_id", F.lit(None).cast("long"))

    if "l_quantity" in df.columns:
        df = df.withColumn("l_quantity", F.col("l_quantity").cast("long"))

    return df


# -------------------------
# Main
# -------------------------

def main() -> None:
    p = argparse.ArgumentParser(description="Upsert/insert data into an existing Hudi table.")
    p.add_argument("--input", required=True, help="Input path (parquet/csv/etc.)")
    p.add_argument("--input-format", default="auto", help="spark.read format (default: auto detect)")
    p.add_argument("--input-option", action="append", default=[], metavar="KEY=VALUE",
                   help="Additional spark.read options (repeatable)")

    p.add_argument("--table-path", required=True, help="Target Hudi table path")
    p.add_argument("--table-name", required=True, help="Target Hudi table name")
    p.add_argument("--record-key", required=True, help="Hudi record key(s), comma-separated")
    p.add_argument("--precombine-field", required=True, help="Precombine field")
    p.add_argument("--partition-field", default="", help="Partition field(s), comma-separated (empty for none)")
    p.add_argument("--operation", choices=["upsert", "insert"], default="upsert",
                   help="Write operation (default: upsert)")

    p.add_argument("--sort-columns", default="", help="Optional sort columns (comma-separated)")
    p.add_argument("--target-file-mb", type=int, default=128, help="Target file size MB (default: 128)")
    p.add_argument("--small-file-limit-mb", type=int, default=256, help="Small file limit MB (default: 256)")
    p.add_argument("--max-num-groups", type=int, default=4096, help="Max clustering groups (default: 4096)")
    p.add_argument("--compression", default="snappy", help="Parquet compression (default: snappy)")

    p.add_argument("--coalesce", type=int, default=0, help="Optional coalesce before write (0 = disabled)")
    p.add_argument("--date-cols", default="l_shipdate,l_commitdate,l_receiptdate",
                   help="Columns to cast to DATE if present")
    p.add_argument(
        "--metadata-enable",
        choices=["true", "false"],
        default="true",
        help="Enable Hudi metadata table updates (default: true)",
    )

    p.add_argument("--app-name", default="hudi-upsert", help="Spark app name")
    p.add_argument("--shuffle-partitions", type=int, default=200, help="spark.sql.shuffle.partitions")
    args = p.parse_args()

    spark = build_spark(args.app_name, args.shuffle_partitions)

    # Load batch input
    df = load_input_df(spark, args.input, fmt=args.input_format, options=parse_kv_options(args.input_option))

    # Normalize common TPCH date columns
    for c in parse_csv_list(args.date_cols):
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast("date"))

    if args.coalesce and args.coalesce > 0:
        df = df.coalesce(args.coalesce)

    # 1) Align to existing table schema (Spark-based, most reliable)
    target_schema = get_hudi_table_spark_schema(spark, args.table_path)
    if target_schema is not None:
        df = align_df_to_spark_schema(df, target_schema)
    else:
        # 2) Fallback: try .hoodie/.schema if it exists
        dtype_map = read_hudi_table_schema_from_file(args.table_path)
        if dtype_map:
            df = align_df_to_dtype_map(df, dtype_map)

    # 3) Last-resort fixes for your known failures
    df = enforce_last_resort_fixes(df)

    # Debug: ensure schema contains record_id
    print(f"[DEBUG] table_path={args.table_path}")
    print(f"[DEBUG] df columns (first 30)={df.columns[:30]}")
    print(f"[DEBUG] has record_id={'record_id' in df.columns}")
    df.printSchema()

    partition_field = args.partition_field.strip()
    partition_field_csv = ",".join(parse_csv_list(partition_field)) if partition_field else ""
    record_keys_csv = ",".join(parse_csv_list(args.record_key))
    sort_cols_csv = ",".join(parse_csv_list(args.sort_columns))

    keygen = (
        "org.apache.hudi.keygen.ComplexKeyGenerator"
        if partition_field_csv
        else "org.apache.hudi.keygen.NonpartitionedKeyGenerator"
    )

    options = {
        "hoodie.table.name": args.table_name,
        "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
        "hoodie.datasource.write.operation": args.operation,
        "hoodie.datasource.write.recordkey.field": record_keys_csv,
        "hoodie.datasource.write.precombine.field": args.precombine_field,
        "hoodie.datasource.write.partitionpath.field": partition_field_csv,
        "hoodie.datasource.write.keygenerator.class": keygen,

        # Must match existing table config (your table existing value is false)
        "hoodie.datasource.write.hive_style_partitioning": "false",

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

        # Metadata table can fail on mixed/corrupted metadata logs; allow disabling.
        "hoodie.metadata.enable": args.metadata_enable,
    }
    if sort_cols_csv:
        options["hoodie.clustering.plan.strategy.sort.columns"] = sort_cols_csv

    print(f"[HUDI] Writing to {args.table_path} with operation={args.operation}")
    df.write.format("hudi").options(**options).mode("append").save(args.table_path)

    spark.stop()
    print("[HUDI] Upsert complete.")


if __name__ == "__main__":
    main()
