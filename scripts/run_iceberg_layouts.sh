#!/usr/bin/env bash
set -euo pipefail

# =======================
# Defaults (override via CLI/env/--config)
# =======================
: "${INPUT:=/datasets/tpch_16.parquet}"
: "${WAREHOUSE:=./data/tpch_16/iceberg_wh}"
: "${NAMESPACE:=local.demo}"
: "${BASE_NAME:=events_iceberg}"

# Spark/Iceberg/resources
: "${ICEBERG_PKG:=org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0}"
: "${SPARK_TIMEZONE:=UTC}"
: "${SPARK_SHUF:=400}"
: "${DRIVER_MEM:=48g}"
: "${EXEC_MEM:=64g}"
: "${EXEC_OVH:=16g}"
: "${MAX_PART_BYTES:=256m}"
: "${VEC_READER:=true}"
: "${ADAPTIVE:=true}"

# TPC-H style columns (accept comma or space)
: "${PARTITION_BY:=l_returnflag l_linestatus}"
: "${RANGE_COLS:=l_shipdate l_receiptdate}"
: "${LAYOUT_COLS:=l_shipdate l_receiptdate}"

# File-size / rewrite scope
: "${TARGET_FILE_MB:=128}"
: "${REWRITE_WHERE:=}"   # e.g., "l_returnflag = 'R'"

CONFIG_FILE=""
ONLY="baseline,linear,zorder"   # which variants to run

usage() {
  cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Core:
  --input PATH                 Input Parquet (default: \$INPUT)
  --warehouse PATH             Iceberg HadoopCatalog warehouse dir (default: \$WAREHOUSE)
  --namespace NAME             Catalog namespace (default: \$NAMESPACE)
  --base-name NAME             Base table name (default: \$BASE_NAME)
  --only LIST                  Variants to run: baseline,linear,zorder (comma/space) (default: "$ONLY")
  --config FILE                Source an env file before running

Layout/partition:
  --partition-by COLS          Partition cols (comma/space) (default: "\$PARTITION_BY")
  --range-cols COLS            Range cols for pre-sort (default: "\$RANGE_COLS")
  --layout-cols COLS           Columns for z-order/linear sort (default: "\$LAYOUT_COLS")

Rewrite/files:
  --target-file-mb N           Target file size in MB (default: \$TARGET_FILE_MB)
  --rewrite-where SQLPRED      Optional filter for rewrite (default: empty)

Spark:
  --shuffle N                  spark.sql.shuffle.partitions (default: \$SPARK_SHUF)
  --driver-mem SIZE            spark.driver.memory (default: \$DRIVER_MEM)
  --exec-mem SIZE              spark.executor.memory (default: \$EXEC_MEM)
  --exec-overhead SIZE         spark.executor.memoryOverhead (default: \$EXEC_OVH)
  --max-part-bytes SIZE        spark.sql.files.maxPartitionBytes (default: \$MAX_PART_BYTES)
  --vec-reader true|false      parquet vectorized reader (default: \$VEC_READER)
  --adaptive true|false        AQE (default: \$ADAPTIVE)
  --timezone ZONE              spark.sql.session.timeZone (default: \$SPARK_TIMEZONE)

Misc:
  -h, --help                   Show help
EOF
}

# =======================
# Parse CLI
# =======================
while [[ $# -gt 0 ]]; do
  case "$1" in
    --input)            INPUT="$2"; shift 2;;
    --warehouse)        WAREHOUSE="$2"; shift 2;;
    --namespace)        NAMESPACE="$2"; shift 2;;
    --base-name)        BASE_NAME="$2"; shift 2;;
    --only)             ONLY="$2"; shift 2;;
    --config)           CONFIG_FILE="$2"; shift 2;;

    --partition-by)     PARTITION_BY="$2"; shift 2;;
    --range-cols)       RANGE_COLS="$2"; shift 2;;
    --layout-cols)      LAYOUT_COLS="$2"; shift 2;;

    --target-file-mb)   TARGET_FILE_MB="$2"; shift 2;;
    --rewrite-where)    REWRITE_WHERE="$2"; shift 2;;

    --shuffle)          SPARK_SHUF="$2"; shift 2;;
    --driver-mem)       DRIVER_MEM="$2"; shift 2;;
    --exec-mem)         EXEC_MEM="$2"; shift 2;;
    --exec-overhead)    EXEC_OVH="$2"; shift 2;;
    --max-part-bytes)   MAX_PART_BYTES="$2"; shift 2;;
    --vec-reader)       VEC_READER="$2"; shift 2;;
    --adaptive)         ADAPTIVE="$2"; shift 2;;
    --timezone)         SPARK_TIMEZONE="$2"; shift 2;;

    -h|--help)          usage; exit 0;;
    *) echo "Unknown option: $1"; usage; exit 2;;
  esac
done

# Load optional config last so it can override env but not CLI
if [[ -n "$CONFIG_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$CONFIG_FILE"
fi

# =======================
# Helpers
# =======================
_norm_list() { local s="${1//,/ }"; # commas->spaces
  # shellcheck disable=SC2206
  LIST_ARR=($s)
}

_norm_list "$PARTITION_BY"; PARTITION_ARR=("${LIST_ARR[@]}")
_norm_list "$RANGE_COLS";    RANGE_ARR=("${LIST_ARR[@]}")
_norm_list "$LAYOUT_COLS";   LAYOUT_ARR=("${LIST_ARR[@]}")
_norm_list "$ONLY";          ONLY_ARR=("${LIST_ARR[@]}")

# Detect SPARK_HOME
if [ -z "${SPARK_HOME:-}" ]; then
  if command -v spark-submit >/dev/null 2>&1; then
    export SPARK_HOME="$(dirname "$(dirname "$(command -v spark-submit)")")"
  else
    echo "spark-submit not found. Please install Spark or set SPARK_HOME." >&2
    exit 1
  fi
fi

mkdir -p "$WAREHOUSE"

COMMON_SUBMIT_ARGS=(
  --packages "$ICEBERG_PKG"
  --conf "spark.sql.session.timeZone=${SPARK_TIMEZONE}"
  --conf "spark.sql.shuffle.partitions=${SPARK_SHUF}"
  --conf "spark.driver.memory=${DRIVER_MEM}"
  --conf "spark.executor.memory=${EXEC_MEM}"
  --conf "spark.executor.memoryOverhead=${EXEC_OVH}"
  --conf "spark.sql.files.maxPartitionBytes=${MAX_PART_BYTES}"
  --conf "spark.sql.adaptive.enabled=${ADAPTIVE}"
  --conf "spark.sql.parquet.enableVectorizedReader=${VEC_READER}"

  # Iceberg HadoopCatalog
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog
  --conf spark.sql.catalog.local.type=hadoop
  --conf "spark.sql.catalog.local.warehouse=$(python3 - <<'PY'
import os; print(os.path.abspath(os.environ.get("WAREHOUSE","./data/iceberg_wh")))
PY
)"
)

run_job () {
  local layout="$1"      # baseline | linear | zorder
  local optimize="$2"    # none | sort | zorder | both
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
  echo "[OK] ${TABLE_ID}"
}

run_variant() {
  case "$1" in
    baseline) run_job "baseline" "none"  "baseline" ;;
    linear)   run_job "linear"   "sort"  "linear"   ;;
    zorder)   run_job "zorder"   "both"  "zorder"   ;;
    *) echo "Unknown variant: $1" >&2; exit 2;;
  esac
}

for v in "${ONLY_ARR[@]}"; do
  run_variant "$v"
done
