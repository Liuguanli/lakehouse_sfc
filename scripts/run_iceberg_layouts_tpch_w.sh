#!/usr/bin/env bash
set -euo pipefail

# =======================
# Config (override by env)
# =======================
: "${INPUT:=/datasets/tpch_16.parquet}"
: "${WAREHOUSE:=./data/tpch_16/iceberg_wh}"          # HadoopCatalog warehouse
: "${NAMESPACE:=local.demo}"                 # catalog.namespace
: "${BASE_NAME:=events_iceberg}"             # final table: ${NAMESPACE}.${BASE_NAME}_<suffix>

# Spark/Iceberg/resources (aligned with Delta/Hudi)
: "${ICEBERG_PKG:=org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0}"
: "${SPARK_TIMEZONE:=UTC}"
: "${SPARK_SHUF:=400}"
: "${DRIVER_MEM:=48g}"
: "${EXEC_MEM:=64g}"
: "${EXEC_OVH:=16g}"
: "${MAX_PART_BYTES:=256m}"
: "${VEC_READER:=true}"
: "${ADAPTIVE:=true}"

# TPC-H style columns (comma or space-separated)
: "${PARTITION_BY:=l_returnflag l_linestatus}"
: "${RANGE_COLS:=l_shipdate l_receiptdate}"
: "${LAYOUT_COLS:=l_shipdate l_receiptdate}"

# File-size / rewrite scope
: "${TARGET_FILE_MB:=128}"
: "${REWRITE_WHERE:=}"    # e.g. "l_returnflag = 'R'"

# Detect SPARK_HOME (prefer spark-submit)
if [ -z "${SPARK_HOME:-}" ]; then
  if command -v spark-submit >/dev/null 2>&1; then
    export SPARK_HOME="$(dirname "$(dirname "$(command -v spark-submit)")")"
  else
    echo "spark-submit not found. Please install Spark or set SPARK_HOME." >&2
    exit 1
  fi
fi

# Normalize lists -> arrays
_split_cols() { local s="$1"; s="${s//,/ }"; COL_ARR=($s); }
_split_cols "$PARTITION_BY"; PARTITION_ARR=("${COL_ARR[@]}")
_split_cols "$RANGE_COLS";    RANGE_ARR=("${COL_ARR[@]}")
_split_cols "$LAYOUT_COLS";   LAYOUT_ARR=("${COL_ARR[@]}")

COMMON_SUBMIT_ARGS=(
  --packages "$ICEBERG_PKG"
  --conf "spark.sql.session.timeZone=${SPARK_TIMEZONE}"
  --conf spark.sql.shuffle.partitions="$SPARK_SHUF"
  --conf spark.driver.memory="$DRIVER_MEM"
  --conf spark.executor.memory="$EXEC_MEM"
  --conf spark.executor.memoryOverhead="$EXEC_OVH"
  --conf spark.sql.files.maxPartitionBytes="$MAX_PART_BYTES"
  --conf spark.sql.adaptive.enabled="$ADAPTIVE"
  --conf spark.sql.parquet.enableVectorizedReader="$VEC_READER"
  # Iceberg HadoopCatalog
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog
  --conf spark.sql.catalog.local.type=hadoop
  --conf spark.sql.catalog.local.warehouse="$(python3 - <<'PY'
import os; print(os.path.abspath(os.environ.get("WAREHOUSE","./data/iceberg_wh")))
PY
)"
)

run_job () {
  local layout="$1"      # baseline | linear | zorder
  local optimize="$2"    # none | binpack | sort | zorder | both
  local suffix="$3"      # baseline | linear | zorder

  local TABLE_ID="${NAMESPACE}.${BASE_NAME}_${suffix}"

  echo "=== Iceberg write: layout=${layout}, optimize=${optimize}, table=${TABLE_ID} ==="
  "$SPARK_HOME/bin/spark-submit" \
    "${COMMON_SUBMIT_ARGS[@]}" \
    ./lakehouse_op/iceberg_write_layout.py \
      --input "$INPUT" \
      --warehouse "$WAREHOUSE" \
      --table-identifier "$TABLE_ID" \
      --mode overwrite \
      --partition-by "${PARTITION_ARR[@]}" \
      --repartition "$SPARK_SHUF" \
      --range-cols "${RANGE_ARR[@]}" \
      --layout "$layout" \
      --layout-cols "${LAYOUT_ARR[@]}" \
      --optimize "$optimize" \
      --target-file-mb "$TARGET_FILE_MB" \
      --rewrite-where "$REWRITE_WHERE"
  echo "=== Done: ${TABLE_ID} ==="
}

# 1) baseline: no ordering, no rewrite
run_job "baseline" "none" "baseline"

# 2) linear: in-partition sort, then sort rewrite
run_job "linear" "sort" "linear"

# 3) zorder: SFC ordering via zorder(...) rewrite + compaction
run_job "zorder" "both" "zorder"
