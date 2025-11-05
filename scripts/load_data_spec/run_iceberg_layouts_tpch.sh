#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------------------------------------
# TPC-H Iceberg loader wrapper.
# Handles default namespace/base-name/layouts while deferring execution to
# load_data_core/run_iceberg_layouts.sh.
# ---------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPTS_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
CORE_SCRIPT="${SCRIPTS_ROOT}/load_data_core/run_iceberg_layouts.sh"

[[ -f "${CORE_SCRIPT}" ]] || { echo "Missing core script: ${CORE_SCRIPT}" >&2; exit 1; }

PARENT_LAYOUTS="${LAYOUTS:-}"

DEFAULT_SCALES="1 4 16"
SCALES_RAW="${SCALES:-$DEFAULT_SCALES}"
LAYOUTS_OVERRIDE=""

INPUT_TEMPLATE_DEFAULT="/datasets/tpch_%s.parquet"
WAREHOUSE_TEMPLATE_DEFAULT="./data/tpch_%s/iceberg_wh"
INPUT_TEMPLATE="${ICEBERG_INPUT_TEMPLATE:-$INPUT_TEMPLATE_DEFAULT}"
WAREHOUSE_TEMPLATE="${ICEBERG_WH_TEMPLATE:-$WAREHOUSE_TEMPLATE_DEFAULT}"

NAMESPACE="${ICEBERG_NS:-${NAMESPACE:-local.demo}}"
BASE_NAME="${ICEBERG_BASENAME:-${BASE_NAME:-events_iceberg}}"
PARTITION_BY="${PARTITION_BY:-l_returnflag,l_linestatus}"
RANGE_COLS="${RANGE_COLS:-l_shipdate,l_receiptdate}"
LAYOUT_COLS="${LAYOUT_COLS:-l_shipdate,l_receiptdate}"
SPARK_SHUF="${SPARK_SHUF:-400}"
TARGET_FILE_MB="${TARGET_FILE_MB:-128}"

CORE_EXTRA_ARGS=()

CORE_SUPPORTS_COL_FLAGS=false
if bash "$CORE_SCRIPT" --help 2>/dev/null | grep -q -- '--partition-by'; then
  CORE_SUPPORTS_COL_FLAGS=true
fi

usage() {
  cat <<'EOF'
run_iceberg_layouts_tpch.sh
  Wrapper around load_data_core/run_iceberg_layouts.sh with TPC-H defaults.

Options:
  --scales "1 4 16"          Space/comma separated scale factors (default: 1 4 16)
  --layouts "baseline,zorder" Layout names to materialise (default: ICEBERG_LAYOUTS or baseline,linear,zorder)
  --input-template STRING    printf-style template for source parquet (default: /datasets/tpch_%s.parquet)
  --warehouse-template STRING printf-style template for target warehouse dir (default: ./data/tpch_%s/iceberg_wh)
  --namespace NAME           Override Iceberg namespace (default: local.demo)
  --base-name NAME           Override base table name (default: events_iceberg)
  --partition-by COLS        Override partition columns
  --range-cols COLS          Override range columns
  --layout-cols COLS         Override layout columns
  --shuffle N                Override spark.sql.shuffle.partitions (default: 400)
  --target-file-mb N         Override target file size MB (default: 128)
  --help                     Show this help and exit

Notes:
  * Additional arguments can be passed to the core script after `--`.
  * Environment overrides follow the option names (ICEBERG_LAYOUTS, ICEBERG_NS, etc.).
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --scales)
      SCALES_RAW="$2"; shift 2;;
    --layouts)
      LAYOUTS_OVERRIDE="$2"; shift 2;;
    --input-template)
      INPUT_TEMPLATE="$2"; shift 2;;
    --warehouse-template)
      WAREHOUSE_TEMPLATE="$2"; shift 2;;
    --namespace)
      NAMESPACE="$2"; shift 2;;
    --base-name)
      BASE_NAME="$2"; shift 2;;
    --partition-by)
      PARTITION_BY="$2"; shift 2;;
    --range-cols)
      RANGE_COLS="$2"; shift 2;;
    --layout-cols)
      LAYOUT_COLS="$2"; shift 2;;
    --shuffle)
      SPARK_SHUF="$2"; shift 2;;
    --target-file-mb)
      TARGET_FILE_MB="$2"; shift 2;;
    -h|--help)
      usage; exit 0;;
    --)
      shift
      CORE_EXTRA_ARGS=("$@")
      break;;
    *)
      echo "Unknown option: $1" >&2
      usage
      exit 2;;
  esac
done

normalize_list() {
  local raw="$1"
  local -a tmp=()
  raw="${raw//,/ }"
  for token in $raw; do
    [[ -n "$token" ]] && tmp+=("$token")
  done
  printf '%s\n' "${tmp[@]}"
}

mapfile -t SCALES_ARR < <(normalize_list "$SCALES_RAW")

LAYOUT_SRC="${LAYOUTS_OVERRIDE:-${ICEBERG_LAYOUTS:-${PARENT_LAYOUTS:-baseline,linear,zorder}}}"
mapfile -t LAYOUT_ARR < <(normalize_list "$LAYOUT_SRC")

if [[ ${#SCALES_ARR[@]} -eq 0 ]]; then
  echo "[WARN] No scales provided; nothing to do." >&2
  exit 0
fi

if [[ ${#LAYOUT_ARR[@]} -eq 0 ]]; then
  echo "[WARN] No layouts selected; nothing to do." >&2
  exit 0
fi

for scale in "${SCALES_ARR[@]}"; do
  input_path=$(printf "$INPUT_TEMPLATE" "$scale")
  warehouse_dir=$(printf "$WAREHOUSE_TEMPLATE" "$scale")
  echo "===> Iceberg build for tpch_${scale}"
  for layout in "${LAYOUT_ARR[@]}"; do
    layout_token="${layout,,}"
    layout_token="${layout_token// /}"
    if [[ -z "$layout_token" ]]; then
      continue
    fi
    echo "  -> layout=${layout_token} input=${input_path} warehouse=${warehouse_dir}"
    if $CORE_SUPPORTS_COL_FLAGS; then
      LAYOUTS="$layout_token" \
      PARTITION_BY="$PARTITION_BY" \
      RANGE_COLS="$RANGE_COLS" \
      LAYOUT_COLS="$LAYOUT_COLS" \
      WAREHOUSE="$warehouse_dir" \
        bash "$CORE_SCRIPT" \
          --input "$input_path" \
          --warehouse "$warehouse_dir" \
          --namespace "$NAMESPACE" \
          --base-name "$BASE_NAME" \
          --partition-by "$PARTITION_BY" \
          --range-cols "$RANGE_COLS" \
          --layout-cols "$LAYOUT_COLS" \
          --shuffle "$SPARK_SHUF" \
          --target-file-mb "$TARGET_FILE_MB" \
          "${CORE_EXTRA_ARGS[@]}"
    else
      LAYOUTS="$layout_token" \
      PARTITION_BY="$PARTITION_BY" \
      RANGE_COLS="$RANGE_COLS" \
      LAYOUT_COLS="$LAYOUT_COLS" \
      WAREHOUSE="$warehouse_dir" \
        bash "$CORE_SCRIPT" \
          --input "$input_path" \
          --warehouse "$warehouse_dir" \
          --namespace "$NAMESPACE" \
          --base-name "$BASE_NAME" \
          --shuffle "$SPARK_SHUF" \
          --target-file-mb "$TARGET_FILE_MB" \
          "${CORE_EXTRA_ARGS[@]}"
    fi
  done
done
