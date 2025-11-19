#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# Usage:
#   bash scripts/run_query.sh tpch_16 workloads/tpch_16_Q1
#   # Run only specific engines:
#   bash scripts/run_query.sh tpch_16 workloads/tpch_16_Q1 --delta --hudi
#
# Flags:
#   --delta     Run Delta engine only
#   --hudi      Run Hudi engine only
#   --iceberg   Run Iceberg engine only
#   --tag STR   Optional label appended to results/log directory names
#   (default)   Run all three engines if none specified
# ============================================================

DATASET="${1:-}"
WORKLOAD_DIR="${2:-}"
shift 2 || true

RUN_DELTA=0
RUN_HUDI=0
RUN_ICEBERG=0
HUDI_LAYOUTS="${HUDI_LAYOUTS:-no_layout,linear,zorder,hilbert}"
DELTA_LAYOUTS="${DELTA_LAYOUTS:-baseline,linear,zorder}"
ICEBERG_LAYOUTS="${ICEBERG_LAYOUTS:-baseline,linear,zorder}"
declare -a RUN_TAGS=()

# ---------- Parse optional engine flags ----------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --delta)   RUN_DELTA=1; shift;;
    --hudi)    RUN_HUDI=1; shift;;
    --iceberg) RUN_ICEBERG=1; shift;;
    --hudi-layouts)          # NEW: comma-separated layouts
      HUDI_LAYOUTS="$2"; shift 2;;
    --delta-layouts)
      DELTA_LAYOUTS="$2"; shift 2;;
    --iceberg-layouts)
      ICEBERG_LAYOUTS="$2"; shift 2;;
    --tag)
      RUN_TAGS+=("$2"); shift 2;;
    -h|--help)
      sed -n '1,30p' "$0"; exit 0;;
    *) echo "Unknown option: $1" >&2; exit 2;;
  esac
done

# Default to all engines if none specified
if [[ $RUN_DELTA -eq 0 && $RUN_HUDI -eq 0 && $RUN_ICEBERG -eq 0 ]]; then
  RUN_DELTA=1; RUN_HUDI=1; RUN_ICEBERG=1
fi

# ---------- Environment setup ----------
[[ -f "${HOME}/.lakehouse/env" ]] && source "${HOME}/.lakehouse/env"

# Auto-detect SPARK_HOME if not set
if [[ -z "${SPARK_HOME:-}" ]]; then
  if command -v spark-submit >/dev/null 2>&1; then
    export SPARK_HOME="$(dirname "$(dirname "$(command -v spark-submit)")")"
  else
    echo "spark-submit not found and SPARK_HOME is not set" >&2
    exit 1
  fi
fi

# ---------- Validate input ----------
if [[ -z "$DATASET" || -z "$WORKLOAD_DIR" ]]; then
  echo "Usage: $0 <DATASET> <WORKLOAD_DIR> [--delta] [--hudi] [--iceberg]" >&2
  exit 1
fi

if [[ ! -d "$WORKLOAD_DIR" ]]; then
  echo "Workload directory not found: $WORKLOAD_DIR" >&2
  exit 1
fi

# ---------- Define directories ----------
ROOT="$(pwd)"
ICEBERG_WH="${ROOT}/data/${DATASET}/iceberg_wh"
HUDI_ROOT="${ROOT}/data/${DATASET}/hudi"
DELTA_ROOT="${ROOT}/data/${DATASET}/delta"

WORKLOAD_NAME="$(basename "$WORKLOAD_DIR")"
TS="$(date +%Y%m%d_%H%M%S)"
tag_suffix=""
if [[ ${#RUN_TAGS[@]} -gt 0 ]]; then
  sanitize_tag() {
    local t="$1"
    t="${t// /_}"
    t="${t//[^A-Za-z0-9._=-]/_}"
    printf '%s' "$t"
  }
  tag_join=""
  for tag in "${RUN_TAGS[@]}"; do
    norm="$(sanitize_tag "$tag")"
    if [[ -n "$norm" ]]; then
      if [[ -n "$tag_join" ]]; then
        tag_join+="_"
      fi
      tag_join+="$norm"
    fi
  done
  if [[ -n "$tag_join" ]]; then
    tag_suffix="__${tag_join}"
  fi
fi
RESULTS_DIR="results/${WORKLOAD_NAME}/${TS}${tag_suffix}"
LOG_DIR="log/${DATASET}/${WORKLOAD_NAME}/${TS}${tag_suffix}"
mkdir -p "$RESULTS_DIR" "$LOG_DIR"

echo "DATASET     = $DATASET"
echo "WORKLOAD_DIR= $WORKLOAD_DIR"
echo "RESULTS_DIR = $RESULTS_DIR"

# ---------- Common Spark configuration ----------
COMMON_CONF=(
  --conf spark.sql.shuffle.partitions=200
  --conf spark.sql.files.maxPartitionBytes=256m
  --conf spark.driver.memory=16g
  --conf spark.executor.memory=16g
  --conf spark.executor.memoryOverhead=4g
)

# ============================================================
# Iceberg Runner
# ============================================================
ICEBERG_PKGS="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0"
ICEBERG_CONF=(
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog
  --conf spark.sql.catalog.local.type=hadoop
  --conf "spark.sql.catalog.local.warehouse=${ICEBERG_WH}"
)

run_iceberg() {
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

# ============================================================
# Delta Runner
# ============================================================
DELTA_PKGS="io.delta:delta-spark_2.12:3.2.0"
DELTA_CONF=(
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
)

run_delta() {
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

# ============================================================
# Hudi Runner
# ============================================================
HUDI_PKGS="org.apache.hudi:hudi-spark3.5-bundle_2.12:1.0.2"

run_hudi() {
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

# ============================================================
# Execute Selected Engines
# ============================================================

# if [[ $RUN_ICEBERG -eq 1 ]]; then
#   echo ">>> Running Iceberg queries"
#   run_iceberg baseline
#   run_iceberg linear
#   run_iceberg zorder
# fi

# if [[ $RUN_DELTA -eq 1 ]]; then
#   echo ">>> Running Delta queries"
#   run_delta baseline
#   run_delta linear
#   run_delta zorder
# fi

# if [[ $RUN_HUDI -eq 1 ]]; then
#   echo ">>> Running Hudi queries"
#   run_hudi no_layout
#   run_hudi zorder
#   run_hudi hilbert
#   run_hudi linear
# fi

if [[ $RUN_ICEBERG -eq 1 ]]; then
  echo ">>> Running Iceberg queries"
  IFS=',' read -r -a _iceberg_layouts <<< "$ICEBERG_LAYOUTS"
  for lay in "${_iceberg_layouts[@]}"; do
    run_iceberg "$lay"
  done
fi

if [[ $RUN_DELTA -eq 1 ]]; then
  echo ">>> Running Delta queries"
  IFS=',' read -r -a _delta_layouts <<< "$DELTA_LAYOUTS"
  for lay in "${_delta_layouts[@]}"; do
    run_delta "$lay"
  done
fi

if [[ $RUN_HUDI -eq 1 ]]; then
  echo ">>> Running Hudi queries"
  IFS=',' read -r -a _hudi_layouts <<< "$HUDI_LAYOUTS"
  for lay in "${_hudi_layouts[@]}"; do
    run_hudi "$lay"
  done
fi

echo "✅ All executions complete → Results stored in: ${RESULTS_DIR}"

# ---------- Final mirror (copy contents into FINAL, overwrite same names) ----------
FINAL_BASE="results/${WORKLOAD_NAME}/final"
FINAL_DIR="${FINAL_BASE}/${TS}"
mkdir -p "$FINAL_DIR"

if command -v rsync >/dev/null 2>&1; then
  # Mirror contents; overwrite changed files; remove stale files in FINAL
  rsync -a --checksum --inplace --delete "${RESULTS_DIR}/" "$FINAL_DIR/"
else
  # Fallback: copy contents (including dotfiles); always overwrite
  cp -af "${RESULTS_DIR}/." "$FINAL_DIR/"
fi

ln -sfn "$FINAL_DIR" "${FINAL_BASE}/latest"
echo "📎 Mirrored full results to: ${FINAL_DIR} (and updated ${FINAL_BASE}/latest)"
