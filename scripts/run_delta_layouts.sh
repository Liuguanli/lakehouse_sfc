#!/usr/bin/env bash
set -euo pipefail

# ========= Default Config (overridable by env or CLI) =========
: "${DELTA_PKG:=io.delta:delta-spark_2.12:3.2.0}"
: "${INPUT:=/datasets/tpch_16.parquet}"
: "${OUT_BASE:=./data/tpch_16/delta}"

: "${PARTITION_BY:=l_returnflag l_linestatus}"
: "${RANGE_COLS:=l_shipdate l_receiptdate}"
: "${LAYOUT_COLS:=l_shipdate l_receiptdate}"

: "${SPARK_SHUF:=400}"
: "${DRIVER_MEM:=48g}"
: "${EXEC_MEM:=64g}"
: "${EXEC_OVH:=16g}"
: "${MAX_PART_BYTES:=256m}"
: "${ADAPTIVE:=true}"
: "${VEC_READER:=true}"

CONFIG_FILE=""

usage() {
  cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Options:
  --input PATH             Input Parquet file path (default: \$INPUT)
  --out-base PATH          Output Delta base directory (default: \$OUT_BASE)
  -p, --partition-by COLS  Partition columns (space- or comma-separated)
  -r, --range-cols COLS    Range columns (space- or comma-separated)
  -l, --layout-cols COLS   Layout columns (space- or comma-separated)
  --shuffle N              spark.sql.shuffle.partitions (default: \$SPARK_SHUF)
  --driver-mem SIZE        Spark driver memory (default: \$DRIVER_MEM)
  --exec-mem SIZE          Spark executor memory (default: \$EXEC_MEM)
  --exec-ovh SIZE          Spark executor memory overhead (default: \$EXEC_OVH)
  --max-part-bytes SIZE    spark.sql.files.maxPartitionBytes (default: \$MAX_PART_BYTES)
  --adaptive true|false    Enable Adaptive Query Execution (default: \$ADAPTIVE)
  --vec-reader true|false  Enable Parquet vectorized reader (default: \$VEC_READER)
  --config FILE            Optional env config file to source
  -h, --help               Show this help message
EOF
}

# ========= Parse CLI Arguments =========
while [[ $# -gt 0 ]]; do
  case "$1" in
    --input)            INPUT="$2"; shift 2;;
    --out-base)         OUT_BASE="$2"; shift 2;;
    -p|--partition-by)  PARTITION_BY="$2"; shift 2;;
    -r|--range-cols)    RANGE_COLS="$2"; shift 2;;
    -l|--layout-cols)   LAYOUT_COLS="$2"; shift 2;;
    --shuffle)          SPARK_SHUF="$2"; shift 2;;
    --driver-mem)       DRIVER_MEM="$2"; shift 2;;
    --exec-mem)         EXEC_MEM="$2"; shift 2;;
    --exec-ovh)         EXEC_OVH="$2"; shift 2;;
    --max-part-bytes)   MAX_PART_BYTES="$2"; shift 2;;
    --adaptive)         ADAPTIVE="$2"; shift 2;;
    --vec-reader)       VEC_READER="$2"; shift 2;;
    --config)           CONFIG_FILE="$2"; shift 2;;
    -h|--help)          usage; exit 0;;
    *) echo "Unknown option: $1"; usage; exit 2;;
  esac
done

# ========= Load Config (if provided) =========
if [[ -n "$CONFIG_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$CONFIG_FILE"
fi

# ========= Helpers =========
_split_cols() {
  local s="${1//,/ }"
  # shellcheck disable=SC2206
  COL_ARR=($s)
}

# ========= Detect SPARK_HOME =========
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
  --conf "spark.sql.shuffle.partitions=$SPARK_SHUF"
  --conf "spark.driver.memory=$DRIVER_MEM"
  --conf "spark.executor.memory=$EXEC_MEM"
  --conf "spark.executor.memoryOverhead=$EXEC_OVH"
  --conf "spark.sql.files.maxPartitionBytes=$MAX_PART_BYTES"
  --conf "spark.sql.adaptive.enabled=$ADAPTIVE"
  --conf "spark.sql.parquet.enableVectorizedReader=$VEC_READER"
)

# Convert string lists to arrays
_split_cols "$PARTITION_BY"; PARTITION_ARR=("${COL_ARR[@]}")
_split_cols "$RANGE_COLS";  RANGE_ARR=("${COL_ARR[@]}")
_split_cols "$LAYOUT_COLS"; LAYOUT_ARR=("${COL_ARR[@]}")

# ========= Main Runner =========
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

# ========= Selective Delta layout runner =========
# Normalize to a single layout token
LAYOUT_RAW="${LAYOUTS:-}"
LAYOUT_ONE="${LAYOUT_RAW%%,*}"
LAYOUT_ONE="$(echo "$LAYOUT_ONE" | awk '{print $1}')"  # first word only
LAYOUT_ONE="${LAYOUT_ONE,,}"

# Map to the three variants; only the chosen one runs
case "$LAYOUT_ONE" in
  "")
    echo "[INFO] No layout provided; nothing will run for Delta."
    ;;
  baseline)
    run_job "baseline" "none"   "delta_baseline"
    ;;
  linear)
    run_job "linear"   "none"   "delta_linear"
    ;;
  zorder|z-order)
    run_job "zorder"   "zorder" "delta_zorder"
    ;;
  *)
    echo "WARN: unknown Delta layout '$LAYOUT_ONE' (skipped)"
    ;;
esac
