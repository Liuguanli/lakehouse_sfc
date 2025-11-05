#!/usr/bin/env bash
set -euo pipefail

# =========================
# Defaults (override by env)
# =========================
: "${INPUT:=/datasets/tpch_16.parquet}"
: "${BASE_DIR:=./data/tpch_16/hudi}"

# Spark/Hudi
: "${HUDI_PKG:=org.apache.hudi:hudi-spark3.5-bundle_2.12:1.0.2}"
: "${SPARK_TIMEZONE:=UTC}"
: "${SPARK_SHUFFLE_PARTITIONS:=400}"
: "${EXEC_CORES:=10}"
: "${EXEC_MEM:=64g}"
: "${EXEC_OVERHEAD:=16g}"
: "${DRIVER_MEM:=64g}"
: "${MAX_PART_BYTES:=256m}"
: "${VEC_READER:=false}"
: "${ADAPTIVE:=true}"
: "${PARQUET_POOL_RATIO:=0.3}"
: "${SPARK_APP_NAME:=hudi-writer-lineitem}"

# Table defaults (TPC-H lineitem-like)
: "${RECORD_KEY:=l_orderkey,l_linenumber}"
: "${PRECOMBINE_FIELD:=l_receiptdate}"     # better as a time column
: "${PARTITION_FIELD:=l_returnflag,l_linestatus}"
: "${SORT_COLUMNS:=l_shipdate,l_receiptdate}"
: "${TARGET_FILE_MB:=128}"

CONFIG_FILE=""

usage() {
  cat <<'EOF'
Usage:
  run_hudi_layouts.sh [OPTIONS]

Options:
  --input PATH                 Input Parquet path (default: $INPUT)
  --base-dir PATH              Output base dir for Hudi tables (default: $BASE_DIR)

  # Table options
  --record-key COLS            Hudi record key(s), comma/space separated (default: $RECORD_KEY)
  --precombine-field COL       Precombine field (default: $PRECOMBINE_FIELD)
  --partition-field COLS       Partition field(s), comma/space separated (default: $PARTITION_FIELD)
  --sort-columns COLS          Sort columns (comma/space separated) (default: $SORT_COLUMNS)
  --target-file-mb N           Target file size MB (default: $TARGET_FILE_MB)

  # Spark options
  --shuffle N                  spark.sql.shuffle.partitions (default: $SPARK_SHUFFLE_PARTITIONS)
  --exec-cores N               spark.executor.cores (default: $EXEC_CORES)
  --exec-mem SIZE              spark.executor.memory (default: $EXEC_MEM)
  --exec-overhead SIZE         spark.executor.memoryOverhead (default: $EXEC_OVERHEAD)
  --driver-mem SIZE            spark.driver.memory (default: $DRIVER_MEM)
  --max-part-bytes SIZE        spark.sql.files.maxPartitionBytes (default: $MAX_PART_BYTES)
  --vec-reader true|false      Parquet vectorized reader (default: $VEC_READER)
  --adaptive true|false        Adaptive query exec (default: $ADAPTIVE)
  --timezone ZONE              spark.sql.session.timeZone (default: $SPARK_TIMEZONE)
  --pq-pool-ratio R            spark.hadoop.parquet.memory.pool.ratio (default: $PARQUET_POOL_RATIO)
  --app-name NAME              Spark app name (default: $SPARK_APP_NAME)

  --config FILE                Optional env file to source (overrides defaults)
  -h, --help                   Show this help

Notes:
- We explicitly use ComplexKeyGenerator for multi-column record/partition keys.
- All column lists are normalized to comma-separated strings for Hudi.
- The script runs 4 variants: no_layout, zorder, hilbert, linear.
EOF
}

# ============= Parse CLI =============
while [[ $# -gt 0 ]]; do
  case "$1" in
    --input)            INPUT="$2"; shift 2;;
    --base-dir)         BASE_DIR="$2"; shift 2;;
    --record-key)       RECORD_KEY="$2"; shift 2;;
    --precombine-field) PRECOMBINE_FIELD="$2"; shift 2;;
    --partition-field)  PARTITION_FIELD="$2"; shift 2;;
    --sort-columns)     SORT_COLUMNS="$2"; shift 2;;
    --target-file-mb)   TARGET_FILE_MB="$2"; shift 2;;

    --shuffle)          SPARK_SHUFFLE_PARTITIONS="$2"; shift 2;;
    --exec-cores)       EXEC_CORES="$2"; shift 2;;
    --exec-mem)         EXEC_MEM="$2"; shift 2;;
    --exec-overhead)    EXEC_OVERHEAD="$2"; shift 2;;
    --driver-mem)       DRIVER_MEM="$2"; shift 2;;
    --max-part-bytes)   MAX_PART_BYTES="$2"; shift 2;;
    --vec-reader)       VEC_READER="$2"; shift 2;;
    --adaptive)         ADAPTIVE="$2"; shift 2;;
    --timezone)         SPARK_TIMEZONE="$2"; shift 2;;
    --pq-pool-ratio)    PARQUET_POOL_RATIO="$2"; shift 2;;
    --app-name)         SPARK_APP_NAME="$2"; shift 2;;

    --config)           CONFIG_FILE="$2"; shift 2;;
    -h|--help)          usage; exit 0;;
    *) echo "Unknown option: $1"; usage; exit 2;;
  esac
done

# ============= Load optional config =============
if [[ -n "$CONFIG_FILE" ]]; then
  [[ -f "$CONFIG_FILE" ]] || { echo "Config not found: $CONFIG_FILE" >&2; exit 2; }
  # shellcheck disable=SC1090
  source "$CONFIG_FILE"
fi

# ============= Helpers =============
_norm_to_csv() {
  # Input may be comma/space separated; output comma-separated without spaces
  local s="$1"
  s="${s//,/ }"
  # shellcheck disable=SC2206
  local arr=($s)
  local IFS=,
  echo "${arr[*]}"
}

RECORD_KEY_CSV="$(_norm_to_csv "$RECORD_KEY")"
PARTITION_FIELD_CSV="$(_norm_to_csv "$PARTITION_FIELD")"
SORT_COLUMNS_CSV="$(_norm_to_csv "$SORT_COLUMNS")"

# ============= Detect SPARK_HOME =============
if [ -z "${SPARK_HOME:-}" ]; then
  if command -v spark-submit >/dev/null 2>&1; then
    export SPARK_HOME="$(dirname "$(dirname "$(command -v spark-submit)")")"
  elif command -v spark-sql >/dev/null 2>&1; then
    export SPARK_HOME="$(dirname "$(dirname "$(command -v spark-sql)")")"
  else
    echo "ERROR: spark-submit not found and SPARK_HOME is unset." >&2
    exit 1
  fi
fi

mkdir -p "$BASE_DIR"

COMMON_ARGS=(
  --packages "$HUDI_PKG"
  --name "$SPARK_APP_NAME"
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
  local enable_layout="$1"     # true|false
  local layout_strategy="$2"   # z-order|hilbert|linear|default
  local table_suffix="$3"      # zorder|hilbert|linear|no_layout

  local TABLE_PATH="${BASE_DIR}/hudi_${table_suffix}"
  local TABLE_NAME="events_hudi_${table_suffix}"

  echo "=== Hudi write: strategy=${layout_strategy}, table=${TABLE_NAME} ==="

  # Ensure ComplexKeyGenerator for multi-column keys/partitions.
  KEYGEN_CLASS=org.apache.hudi.keygen.ComplexKeyGenerator \
  "$SPARK_HOME/bin/spark-submit" \
    "${COMMON_ARGS[@]}" \
    ./lakehouse_op/hudi_write_layout.py \
      --input "$INPUT" \
      --table-path "$TABLE_PATH" \
      --table-name "$TABLE_NAME" \
      --record-key "$RECORD_KEY_CSV" \
      --precombine-field "$PRECOMBINE_FIELD" \
      --partition-field "$PARTITION_FIELD_CSV" \
      --layout-enable "$enable_layout" \
      --layout-strategy "$layout_strategy" \
      --sort-columns "$SORT_COLUMNS_CSV" \
      --target-file-mb "$TARGET_FILE_MB"

  echo "[OK] ${TABLE_NAME}"
}

# ======================
# Run all variants
# ======================
# run_job false "default" "no_layout"
# run_job true  "z-order" "zorder"
# run_job true  "hilbert" "hilbert"
run_job true  "linear"  "linear"
