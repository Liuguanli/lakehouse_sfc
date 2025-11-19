#!/usr/bin/env bash
set -euo pipefail
#
# High-level RQ1 driver: build Hudi layouts (data write) and then
# invoke scripts/run_RQ_query.sh to generate/execute the workloads.
#
# Usage:
#   bash scripts/run_RQ_1.sh                       # build + query defaults
#   bash scripts/run_RQ_1.sh --scales "16" --layouts "no_layout,zorder"
#   bash scripts/run_RQ_1.sh --skip-load --limit 5
#   bash scripts/run_RQ_1.sh --query-only -- --spec spec_tpch_RQ1_Q1_N1_1_S1_C1_N2_O1.yaml
#   bash scripts/run_RQ_1.sh --scales 16 --layouts zorder --tag this_is_tag --query-only -- --spec spec_tpch_RQ1_Q1_N1_1_S1_C1_N2_O1.yaml
#

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOAD_SCRIPT=""
QUERY_SCRIPT="${ROOT_DIR}/scripts/run_RQ_query.sh"

DEFAULT_SCALES="16"
DEFAULT_LAYOUTS="no_layout,linear,zorder,hilbert"
DEFAULT_LAYOUTS="linear,zorder,hilbert"

LOAD_SCALES="${SCALES:-$DEFAULT_SCALES}"
LOAD_LAYOUTS="${RQ1_LAYOUTS:-$DEFAULT_LAYOUTS}"
LOAD_RECORD_KEY=""
LOAD_PRECOMBINE_FIELD=""
LOAD_PARTITION_FIELD=""
LOAD_SORT_COLUMNS=""
LOAD_TARGET_FILE_MB=""
LOAD_INPUT_TEMPLATE=""
LOAD_BASE_TEMPLATE=""
DATASET_KIND="tpch"
DATASET_NAME_OVERRIDE=""
DEFAULT_QUERY_DATASET=""
QUERY_DATASET_NAME=""

SKIP_LOAD=0
SKIP_QUERY=0
declare -a LOAD_EXTRA_ARGS=()
declare -a QUERY_ARGS=()
declare -a RUN_TAGS=()
QUERY_OUTPUT_ROOT=""

usage() {
  cat <<'EOF'
run_RQ_1.sh [options] [-- extra run_RQ_query.sh args]
  --scales "16"               Space/comma separated scale factors to build
  --layouts "no_layout,..."   Hudi layout list for load + query stages
  --dataset NAME              Workload dataset: tpch (default) or amazon
  --dataset-name STRING       Dataset identifier passed to run_RQ_query (default per dataset)
  --record-key COLS           Override RECORD_KEY env for loader
  --precombine-field COL      Override PRECOMBINE_FIELD env for loader
  --partition-field COLS      Override PARTITION_FIELD env for loader
  --sort-columns COLS         Override SORT_COLUMNS env for loader
  --target-file-mb N          Override TARGET_FILE_MB env for loader
  --input-template STR        Override HUDI_INPUT_TEMPLATE for loader
  --base-template STR         Override HUDI_BASE_TEMPLATE for loader
  --load-extra \"--arg val\"   Pass through extra flag to loader (repeatable)
  --output-root DIR           Write query outputs under DIR (default derived)
  --skip-load / --skip-query  Skip respective stages
  --load-only / --query-only  Convenience flags equivalent to skip opposite
  --                          Separator before args meant for run_RQ_query.sh
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --scales)
      LOAD_SCALES="$2"; shift 2;;
    --layouts)
      LOAD_LAYOUTS="$2"; shift 2;;
    --dataset)
      DATASET_KIND="$2"; shift 2;;
    --dataset-name)
      DATASET_NAME_OVERRIDE="$2"; shift 2;;
    --record-key)
      LOAD_RECORD_KEY="$2"; shift 2;;
    --precombine-field)
      LOAD_PRECOMBINE_FIELD="$2"; shift 2;;
    --partition-field)
      LOAD_PARTITION_FIELD="$2"; shift 2;;
    --sort-columns)
      LOAD_SORT_COLUMNS="$2"; shift 2;;
    --target-file-mb)
      LOAD_TARGET_FILE_MB="$2"; shift 2;;
    --input-template)
      LOAD_INPUT_TEMPLATE="$2"; shift 2;;
    --base-template)
      LOAD_BASE_TEMPLATE="$2"; shift 2;;
    --skip-load)
      SKIP_LOAD=1; shift;;
    --skip-query)
      SKIP_QUERY=1; shift;;
    --load-only)
      SKIP_QUERY=1; shift;;
    --query-only)
      SKIP_LOAD=1; shift;;
    --load-extra)
      LOAD_EXTRA_ARGS+=("$2"); shift 2;;
    --tag)
      RUN_TAGS+=("$2"); shift 2;;
    --output-root)
      QUERY_OUTPUT_ROOT="$2"; shift 2;;
    -h|--help)
      usage; exit 0;;
    --)
      shift
      QUERY_ARGS+=("$@")
      break;;
    *)
      QUERY_ARGS+=("$1"); shift;;
  esac
done

set_dataset_defaults() {
  case "$DATASET_KIND" in
    tpch)
      LOAD_SCRIPT="${ROOT_DIR}/scripts/load_data_spec/run_hudi_layouts_tpch.sh"
      DEFAULT_QUERY_DATASET="tpch_16"
      ;;
    amazon)
      LOAD_SCRIPT="${ROOT_DIR}/scripts/load_data_spec/run_hudi_layouts_amazon.sh"
      DEFAULT_QUERY_DATASET="amazon"
      ;;
    *)
      echo "Unsupported dataset type: ${DATASET_KIND}" >&2
      exit 2
      ;;
  }
  QUERY_DATASET_NAME="${DATASET_NAME_OVERRIDE:-$DEFAULT_QUERY_DATASET}"
}

set_dataset_defaults

if [[ $SKIP_LOAD -eq 0 ]]; then
  if [[ ! -x "$LOAD_SCRIPT" ]]; then
    echo "Load script not found or not executable: $LOAD_SCRIPT" >&2
    exit 1
  fi
  echo ">>> Building ${DATASET_KIND} Hudi layouts for scales=[$LOAD_SCALES], layouts=[$LOAD_LAYOUTS]"
  (
    export SCALES="$LOAD_SCALES"
    export HUDI_LAYOUTS="$LOAD_LAYOUTS"
    [[ -n "$LOAD_RECORD_KEY" ]] && export RECORD_KEY="$LOAD_RECORD_KEY"
    [[ -n "$LOAD_PRECOMBINE_FIELD" ]] && export PRECOMBINE_FIELD="$LOAD_PRECOMBINE_FIELD"
    [[ -n "$LOAD_PARTITION_FIELD" ]] && export PARTITION_FIELD="$LOAD_PARTITION_FIELD"
    [[ -n "$LOAD_SORT_COLUMNS" ]] && export SORT_COLUMNS="$LOAD_SORT_COLUMNS"
    [[ -n "$LOAD_TARGET_FILE_MB" ]] && export TARGET_FILE_MB="$LOAD_TARGET_FILE_MB"
    [[ -n "$LOAD_INPUT_TEMPLATE" ]] && export HUDI_INPUT_TEMPLATE="$LOAD_INPUT_TEMPLATE"
    [[ -n "$LOAD_BASE_TEMPLATE" ]] && export HUDI_BASE_TEMPLATE="$LOAD_BASE_TEMPLATE"
    bash "$LOAD_SCRIPT" "${LOAD_EXTRA_ARGS[@]}"
  )
else
  echo "[SKIP] Data load stage"
fi

if [[ $SKIP_QUERY -eq 0 ]]; then
  if [[ ! -x "$QUERY_SCRIPT" ]]; then
    echo "Query runner not found or not executable: $QUERY_SCRIPT" >&2
    exit 1
  fi
  echo ">>> Running RQ1 queries (HUDI_LAYOUTS=$LOAD_LAYOUTS)"
  CMD=( "$QUERY_SCRIPT" --workload-type "$DATASET_KIND" --dataset-name "$QUERY_DATASET_NAME" )
  if [[ -n "$QUERY_OUTPUT_ROOT" ]]; then
    CMD+=(--output-root "$QUERY_OUTPUT_ROOT")
  fi
  CMD+=("${QUERY_ARGS[@]}")
  for tag in "${RUN_TAGS[@]}"; do
    CMD+=(--tag "$tag")
  done
  HUDI_LAYOUTS="$LOAD_LAYOUTS" \
    bash "${CMD[@]}"
else
  echo "[SKIP] Query execution stage"
fi

echo "[DONE] RQ1 pipeline complete."
