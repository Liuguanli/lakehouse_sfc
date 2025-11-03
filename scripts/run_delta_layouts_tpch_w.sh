#!/usr/bin/env bash
set -euo pipefail

# ========= Config (override via env) =========
: "${DELTA_PKG:=io.delta:delta-spark_2.12:3.2.0}"
: "${INPUT:=/datasets/tpch_16.parquet}"     # e.g. ../lakehouse-lab/data/parquet_src
: "${OUT_BASE:=./data/tpch_16/delta}"               # e.g. /media/DATA/lakehouse

# TPC-H friendly defaults
# NOTE: space- or comma-separated are both OK; we normalize below into arrays.
# : "${PARTITION_BY:=l_commitdate}"
: "${PARTITION_BY:=l_returnflag l_linestatus}"
: "${RANGE_COLS:=l_shipdate l_receiptdate}"
: "${LAYOUT_COLS:=l_shipdate l_receiptdate}"

# Resource knobs (tune as needed)
: "${SPARK_SHUF:=400}"
: "${DRIVER_MEM:=48g}"      # adjust up for large datasets
: "${EXEC_MEM:=64g}"
: "${EXEC_OVH:=16g}"
: "${MAX_PART_BYTES:=256m}"
: "${ADAPTIVE:=true}"
: "${VEC_READER:=true}"

# Auto-detect SPARK_HOME if not set (prefer spark-submit on PATH)
if [ -z "${SPARK_HOME:-}" ]; then
  if command -v spark-submit >/dev/null 2>&1; then
    export SPARK_HOME="$(dirname "$(dirname "$(command -v spark-submit)")")"
  else
    echo "spark-submit not found. Please install Spark or set SPARK_HOME." >&2
    exit 1
  fi
fi

mkdir -p "$OUT_BASE"

COMMON_SUBMIT_ARGS=(
  --packages "$DELTA_PKG"
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
  --conf spark.sql.shuffle.partitions="$SPARK_SHUF"
  --conf spark.driver.memory="$DRIVER_MEM"
  --conf spark.executor.memory="$EXEC_MEM"
  --conf spark.executor.memoryOverhead="$EXEC_OVH"
  --conf spark.sql.files.maxPartitionBytes="$MAX_PART_BYTES"
  --conf spark.sql.adaptive.enabled="$ADAPTIVE"
  --conf spark.sql.parquet.enableVectorizedReader="$VEC_READER"
)

# Turn possibly comma/space-separated strings into arrays
_split_cols() {
  local s="$1"
  # replace commas with spaces, squeeze spaces
  s="${s//,/ }"
  # shellcheck disable=SC2206
  COL_ARR=($s)
}

# Prepare arrays for --partition-by/--range-cols/--layout-cols
_split_cols "$PARTITION_BY"; PARTITION_ARR=("${COL_ARR[@]}")
_split_cols "$RANGE_COLS";    RANGE_ARR=("${COL_ARR[@]}")
_split_cols "$LAYOUT_COLS";   LAYOUT_ARR=("${COL_ARR[@]}")

run_job () {
  local layout="$1"      # baseline | linear | zorder
  local optimize="$2"    # none | compact | zorder | both
  local out_dir="$3"

  echo "=== Running Delta write: layout=${layout}, optimize=${optimize}, out=${out_dir} ==="
  "$SPARK_HOME/bin/spark-submit" \
    "${COMMON_SUBMIT_ARGS[@]}" \
    ./lakehouse_op/delta_write_layout.py \
      --input "$INPUT" \
      --output "${OUT_BASE}/${out_dir}" \
      --mode overwrite \
      --overwrite-schema \
      --partition-by "${PARTITION_ARR[@]}" \
      --repartition "$SPARK_SHUF" \
      --range-cols "${RANGE_ARR[@]}" \
      --layout "$layout" \
      --layout-cols "${LAYOUT_ARR[@]}" \
      --optimize "$optimize"
  echo "=== Done: ${out_dir} ==="
}

# 1) baseline: no ordering, no OPTIMIZE
run_job "baseline" "none" "delta_baseline"

# 2) linear: pre-write sort + compact small files
run_job "linear" "none" "delta_linear"

# 3) zorder: post-write z-order + compact (both)
# Make sure LAYOUT_COLS do NOT include partition columns; however, the Python
# script will auto-prune partition cols for Z-Order if you forget.
run_job "zorder" "zorder" "delta_zorder"
