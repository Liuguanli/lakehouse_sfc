#!/usr/bin/env bash
set -euo pipefail

# --- usage ---
# ./run_all.sh [WORKLOAD_DIR]
# example: ./run_all.sh workloads/tpch_16_Q1
# default: workloads/demo

# --- env & inputs ---
source ~/.lakehouse/env

WORKLOAD_DIR="${1:-workloads/demo}"                 # queries_dir passed to runner
if [[ ! -d "$WORKLOAD_DIR" ]]; then
  echo "Workload directory not found: $WORKLOAD_DIR" >&2
  exit 1
fi

WORKLOAD_NAME="$(basename "$WORKLOAD_DIR")"         # last path component, e.g. tpch_16_Q1
TS="$(date +%Y%m%d_%H%M%S)"

RESULTS_DIR="results/${WORKLOAD_NAME}/${TS}"        # central results folder
LOG_DIR="log/${WORKLOAD_NAME}/${TS}"                # central log folder
mkdir -p "$RESULTS_DIR"
mkdir -p "$LOG_DIR"

echo "WORKLOAD_DIR = $WORKLOAD_DIR"
echo "WORKLOAD_NAME = $WORKLOAD_NAME"
echo "RESULTS_DIR   = $RESULTS_DIR"
echo "LOG_DIR      = $LOG_DIR"
echo "TS            = $TS"

# --- common Spark knobs ---
COMMON_CONF=(
  --conf spark.sql.shuffle.partitions=200
  --conf spark.sql.files.maxPartitionBytes=256m
  --conf spark.driver.memory=16g
  --conf spark.executor.memory=16g
  --conf spark.executor.memoryOverhead=4g
)

# ---------- ICEBERG ----------
ICEBERG_PKGS="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0"
ICEBERG_CONF=(
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog
  --conf spark.sql.catalog.local.type=hadoop
  --conf spark.sql.catalog.local.warehouse="$(pwd)/data/iceberg_wh"
)

run_iceberg() {
  local layout="$1"   # baseline | linear | zorder
  local table="local.demo.events_iceberg_${layout}"
  "$SPARK_HOME/bin/spark-submit" \
    --packages "$ICEBERG_PKGS" \
    "${ICEBERG_CONF[@]}" \
    "${COMMON_CONF[@]}" \
    lakehouse_op/run_queries.py \
      --engine iceberg \
      --table "$table" \
      --queries_dir "$WORKLOAD_DIR" \
      --warmup \
      --cache none \
      --action count \
      --output_csv "${RESULTS_DIR}/results_iceberg_${layout}_${WORKLOAD_NAME}.csv" \
      # --output_log "${LOG_DIR}/results_iceberg_${layout}_${WORKLOAD_NAME}.log"
}

run_iceberg baseline
run_iceberg linear
run_iceberg zorder

# ---------- DELTA ----------
DELTA_PKGS="io.delta:delta-spark_2.12:3.2.0"
DELTA_CONF=(
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
)

run_delta() {
  local layout="$1"   # baseline | linear | zorder
  local table_path
  case "$layout" in
    baseline) table_path="$(pwd)/data/delta/delta_baseline" ;;
    linear)   table_path="$(pwd)/data/delta/delta_linear" ;;
    zorder)   table_path="$(pwd)/data/delta/delta_zorder" ;;
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
      --warmup \
      --cache none \
      --action count \
      --output_csv "${RESULTS_DIR}/results_delta_${layout}_${WORKLOAD_NAME}.csv" \
      # --output_log "${LOG_DIR}/results_delta_${layout}_${WORKLOAD_NAME}.log"
}

run_delta baseline
run_delta linear
run_delta zorder

# ---------- HUDI ----------
HUDI_PKGS="org.apache.hudi:hudi-spark3.5-bundle_2.12:1.0.2"

run_hudi() {
  local layout="$1"   # no_layout | linear | zorder | hilbert
  local table_path="$(pwd)/data/hudi/hudi_${layout}"
  "$SPARK_HOME/bin/spark-submit" \
    --packages "$HUDI_PKGS" \
    "${COMMON_CONF[@]}" \
    lakehouse_op/run_queries.py \
      --engine hudi \
      --table "$table_path" \
      --queries_dir "$WORKLOAD_DIR" \
      --warmup \
      --cache none \
      --action count \
      --output_csv "${RESULTS_DIR}/results_hudi_${layout}_${WORKLOAD_NAME}.csv" \
      # --output_log "${LOG_DIR}/results_hudi_${layout}_${WORKLOAD_NAME}.log"
}

run_hudi no_layout
run_hudi linear
run_hudi zorder
run_hudi hilbert

echo "All done. Results are under: ${RESULTS_DIR}"
