#!/usr/bin/env bash
set -euo pipefail

source ~/.lakehouse/env

# Usage:
#   ./run_all.sh                         # DATASET=tpch_16, SQL_DIR=workloads/tpch_16 (fallback to demo)
#   ./run_all.sh tpch_16                 # same as above
#   ./run_all.sh tpch_16 workloads/tpch_16_Q1
#   ./run_all.sh tpch_4  workloads/tpch_4_Q1

DATASET="${1:-tpch_16}"
SQL_DIR_INPUT="${2:-}"

# Optional environment file
[[ -f ~/.lakehouse/env ]] && source ~/.lakehouse/env

# Detect SPARK_HOME automatically if not set
if [[ -z "${SPARK_HOME:-}" ]]; then
  if command -v spark-submit >/dev/null 2>&1; then
    export SPARK_HOME="$(dirname "$(dirname "$(command -v spark-submit)")")"
  else
    echo "spark-submit not found and SPARK_HOME is not set" >&2
    exit 1
  fi
fi

# Resolve SQL (workload) directory
if [[ -n "$SQL_DIR_INPUT" ]]; then
  WORKLOAD_DIR="$SQL_DIR_INPUT"
else
  WORKLOAD_DIR="workloads/${DATASET}"
  [[ -d "$WORKLOAD_DIR" ]] || WORKLOAD_DIR="workloads/demo"
fi

if [[ ! -d "$WORKLOAD_DIR" ]]; then
  echo "Workload directory not found: $WORKLOAD_DIR" >&2
  exit 1
fi

ICEBERG_WH="$(pwd)/data//${DATASET}/iceberg_wh"
HUDI_ROOT="$(pwd)/data/${DATASET}/hudi"
DELTA_ROOT="$(pwd)/data/${DATASET}/delta"



WORKLOAD_NAME="$(basename "$WORKLOAD_DIR")"
TS="$(date +%Y%m%d_%H%M%S)"
RESULTS_DIR="results/${WORKLOAD_NAME}/${TS}"
LOG_DIR="log/${DATASET}/${WORKLOAD_NAME}/${TS}"
mkdir -p "$RESULTS_DIR" "$LOG_DIR"

echo "DATASET     = $DATASET"
echo "WORKLOAD_DIR= $WORKLOAD_DIR"
echo "RESULTS_DIR = $RESULTS_DIR"

# Common Spark configurations
COMMON_CONF=(
  --conf spark.sql.shuffle.partitions=200
  --conf spark.sql.files.maxPartitionBytes=256m
  --conf spark.driver.memory=16g
  --conf spark.executor.memory=16g
  --conf spark.executor.memoryOverhead=4g
)

# ---------- Iceberg ----------
ICEBERG_PKGS="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0"
ICEBERG_CONF=(
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog
  --conf spark.sql.catalog.local.type=hadoop
  --conf "spark.sql.catalog.local.warehouse=${ICEBERG_WH}"
)

run_iceberg () {
  local layout="$1"  # baseline | linear | zorder
  local table="local.demo.events_iceberg_${layout}"
  "$SPARK_HOME/bin/spark-submit" \
    --packages "$ICEBERG_PKGS" \
    "${ICEBERG_CONF[@]}" \
    "${COMMON_CONF[@]}" \
    lakehouse_op/run_queries.py \
      --engine iceberg \
      --table "$table" \
      --queries_dir "$WORKLOAD_DIR" \
      --warmup --cache none --action count \
      --output_csv "${RESULTS_DIR}/results_iceberg_${layout}_${WORKLOAD_NAME}.csv"
}

# ---------- Delta ----------
DELTA_PKGS="io.delta:delta-spark_2.12:3.2.0"
DELTA_CONF=(
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
)

run_delta () {
  local layout="$1"  # baseline | linear | zorder
  local table_path
  case "$layout" in
    baseline) table_path="${DELTA_ROOT}/delta_baseline" ;;
    linear)   table_path="${DELTA_ROOT}/delta_linear" ;;
    zorder)   table_path="${DELTA_ROOT}/delta_zorder" ;;
    *) echo "Unknown delta layout: $layout" >&2; exit 1 ;;
  esac
  "$SPARK_HOME/bin/spark-submit" \
    --packages "$DELTA_PKGS" \
    "${DELTA_CONF[@]}" \
    "${COMMON_CONF[@]}" \
    lakehouse_op/run_queries.py \
      --engine delta \
      --table "$table_path" \
      --queries_dir "$WORKLOAD_DIR" \
      --warmup --cache none --action count \
      --output_csv "${RESULTS_DIR}/results_delta_${layout}_${WORKLOAD_NAME}.csv"
}

# ---------- Hudi ----------
HUDI_PKGS="org.apache.hudi:hudi-spark3.5-bundle_2.12:1.0.2"

run_hudi () {
  local layout="$1"  # no_layout | linear | zorder | hilbert
  local table_path="${HUDI_ROOT}/hudi_${layout}"
  "$SPARK_HOME/bin/spark-submit" \
    --packages "$HUDI_PKGS" \
    "${COMMON_CONF[@]}" \
    lakehouse_op/run_queries.py \
      --engine hudi \
      --table "$table_path" \
      --queries_dir "$WORKLOAD_DIR" \
      --warmup --cache none --action count \
      --output_csv "${RESULTS_DIR}/results_hudi_${layout}_${WORKLOAD_NAME}.csv"
}


# ---------- Execute All ----------
run_iceberg baseline
run_iceberg linear
run_iceberg zorder

run_delta baseline
run_delta linear
run_delta zorder

run_hudi no_layout
run_hudi linear
run_hudi zorder
run_hudi hilbert

echo "All done -> ${RESULTS_DIR}"
