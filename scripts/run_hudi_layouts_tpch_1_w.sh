#!/usr/bin/env bash
set -euo pipefail

# =======================
# Config (override by env)
# =======================
: "${INPUT:=/datasets/tpch_1.parquet}"
: "${BASE_DIR:=./data/tpch_1/hudi}"

# ===== Spark/Hudi defaults (override via env) =====
: "${HUDI_PKG:=org.apache.hudi:hudi-spark3.5-bundle_2.12:1.0.2}"
: "${SPARK_TIMEZONE:=UTC}"
: "${SPARK_SHUFFLE_PARTITIONS:=400}"
: "${EXEC_CORES:=10}"
: "${EXEC_MEM:=64g}"
: "${EXEC_OVERHEAD:=16g}"
: "${DRIVER_MEM:=64g}"
: "${MAX_PART_BYTES:=256m}"
: "${VEC_READER:=false}"               # parquet vectorized reader
: "${ADAPTIVE:=true}"
: "${PARQUET_POOL_RATIO:=0.3}"
: "${SPARK_APP_NAME:=hudi-writer-lineitem}"


# Common table parameters (TPC-H lineitem defaults)
: "${RECORD_KEY:=l_orderkey,l_linenumber}"
: "${PRECOMBINE_FIELD:=l_orderkey}"
# : "${PARTITION_FIELD:="l_commitdate"}"
: "${PARTITION_FIELD:="l_returnflag,l_linestatus"}"
: "${SORT_COLUMNS="l_shipdate,l_receiptdate"}"
: "${TARGET_FILE_MB:=128}"


# Detect SPARK_HOME if missing (prefer spark-sql on PATH)
if [ -z "${SPARK_HOME:-}" ]; then
  if command -v spark-sql >/dev/null 2>&1; then
    export SPARK_HOME="$(dirname "$(dirname "$(readlink -f "$(command -v spark-sql)")")")"
  elif command -v spark-submit >/dev/null 2>&1; then
    export SPARK_HOME="$(dirname "$(dirname "$(readlink -f "$(command -v spark-submit)")")")"
  else
    echo "ERROR: SPARK_HOME not set and spark-sql/spark-submit not found in PATH." >&2
    exit 1
  fi
fi

COMMON_ARGS=(
  --packages "$HUDI_PKG"
  --conf "spark.sql.session.timeZone=${SPARK_TIMEZONE}"
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer
  --conf "spark.sql.shuffle.partitions=${SPARK_SHUFFLE_PARTITIONS}"
  --conf "spark.executor.cores=${EXEC_CORES}"
  --conf "spark.executor.memory=${EXEC_MEM}"
  --conf "spark.executor.memoryOverhead=${EXEC_OVERHEAD}"
  --conf "spark.driver.memory=${DRIVER_MEM}"
  --conf "spark.sql.files.maxPartitionBytes=${MAX_PART_BYTES}"
  --conf "spark.sql.parquet.enableVectorizedReader=${VEC_READER}"
  --conf "spark.sql.adaptive.enabled=${ADAPTIVE}"
  --conf "spark.hadoop.parquet.memory.pool.ratio=${PARQUET_POOL_RATIO}"
)

run_job () {

  local enable_layout="$1"     # true | false
  local layout_strategy="$2"  # z-order | hilbert | linear
  local table_suffix="$3"     # zorder | hilbert | linear

  local TABLE_PATH="${BASE_DIR}/hudi_${table_suffix}"
  local TABLE_NAME="events_hudi_${table_suffix}"

  echo "=== Running Hudi write: strategy=${layout_strategy}, path=${TABLE_PATH}, name=${TABLE_NAME} ==="
  "$SPARK_HOME/bin/spark-submit" \
    "${COMMON_ARGS[@]}" \
    ./lakehouse_op/hudi_write_layout.py \
      --input "$INPUT" \
      --table-path "$TABLE_PATH" \
      --table-name "$TABLE_NAME" \
      --record-key "$RECORD_KEY" \
      --precombine-field "$PRECOMBINE_FIELD" \
      --partition-field "$PARTITION_FIELD" \
      --layout-enable "$enable_layout" --layout-strategy "$layout_strategy" \
      --sort-columns "$SORT_COLUMNS" \
      --target-file-mb "$TARGET_FILE_MB"
  echo "=== Done: ${TABLE_NAME} ==="
}

# ======================
# 0) No layout
# ======================
run_job false "default" "no_layout" 

# ======================
# 1) Z-Order (SFC)
# ======================
run_job true "z-order" "zorder" 

# ======================
# 2) Hilbert (SFC)
# ======================
run_job true "hilbert" "hilbert" 

# ======================
# 3) Linear
# ======================
run_job true "linear" "linear" 
