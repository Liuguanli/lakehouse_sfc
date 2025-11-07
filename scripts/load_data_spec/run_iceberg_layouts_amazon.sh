#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------------------------------------
# Amazon reviews Iceberg loader wrapper.
# Handles layout iteration with reasonable defaults for the CSV dataset.
# ---------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPTS_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
CORE_SCRIPT="${SCRIPTS_ROOT}/load_data_core/run_iceberg_layouts.sh"

[[ -f "${CORE_SCRIPT}" ]] || { echo "Missing core script: ${CORE_SCRIPT}" >&2; exit 1; }

PARENT_LAYOUTS="${LAYOUTS:-}"

INPUT="${INPUT:-/datasets/amazon_review_all.csv}"
WAREHOUSE="${WAREHOUSE:-./data/amazon/iceberg_wh}"
NAMESPACE="${ICEBERG_NS:-local.demo}"
BASE_NAME="${ICEBERG_BASENAME:-reviews_iceberg}"

PARTITION_BY="${PARTITION_BY:-category}"
RANGE_COLS="${RANGE_COLS:-record_timestamp}"
LAYOUT_COLS="${LAYOUT_COLS:-record_timestamp,rating}"
SPARK_SHUF="${SPARK_SHUF:-800}"
TARGET_FILE_MB="${TARGET_FILE_MB:-256}"

LAYOUTS_OVERRIDE=""
CORE_EXTRA_ARGS=()

usage() {
  cat <<'EOF'
run_iceberg_layouts_amazon.sh
  Wrapper around load_data_core/run_iceberg_layouts.sh for the Amazon dataset.

Options:
  --input PATH            Source CSV path (default: /datasets/amazon_review_all.csv)
  --warehouse DIR         Iceberg warehouse directory (default: ./data/amazon/iceberg_wh)
  --namespace NAME        Iceberg namespace (default: local.demo)
  --base-name NAME        Base table name (default: reviews_iceberg)
  --layouts LIST          Layouts to build (default: ICEBERG_LAYOUTS or baseline,linear,zorder)
  --partition-by COLS     Partition columns (comma/space separated)
  --range-cols COLS       Range columns for repartition (comma/space separated)
  --layout-cols COLS      Layout/order columns (comma/space separated)
  --shuffle N             spark.sql.shuffle.partitions (default: 800)
  --target-file-mb N      Target rewrite file size in MB (default: 256)
  --help                  Show help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --input)          INPUT="$2"; shift 2;;
    --warehouse)      WAREHOUSE="$2"; shift 2;;
    --namespace)      NAMESPACE="$2"; shift 2;;
    --base-name)      BASE_NAME="$2"; shift 2;;
    --layouts)        LAYOUTS_OVERRIDE="$2"; shift 2;;
    --partition-by)   PARTITION_BY="$2"; shift 2;;
    --range-cols)     RANGE_COLS="$2"; shift 2;;
    --layout-cols)    LAYOUT_COLS="$2"; shift 2;;
    --shuffle)        SPARK_SHUF="$2"; shift 2;;
    --target-file-mb) TARGET_FILE_MB="$2"; shift 2;;
    -h|--help)        usage; exit 0;;
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

[[ -n "$INPUT" ]] || { echo "Input path required (--input)"; exit 2; }

normalize_list() {
  local raw="$1"
  local -a tmp=()
  raw="${raw//,/ }"
  for token in $raw; do
    [[ -n "$token" ]] && tmp+=("$token")
  done
  printf '%s\n' "${tmp[@]}"
}

LAYOUT_SRC="${LAYOUTS_OVERRIDE:-${ICEBERG_LAYOUTS:-${PARENT_LAYOUTS:-baseline,linear,zorder}}}"
mapfile -t LAYOUT_ARR < <(normalize_list "$LAYOUT_SRC")

if [[ ${#LAYOUT_ARR[@]} -eq 0 ]]; then
  echo "[WARN] No Iceberg layouts selected; nothing to do." >&2
  exit 0
fi

for layout in "${LAYOUT_ARR[@]}"; do
  layout_token="${layout,,}"
  layout_token="${layout_token// /}"
  [[ -z "$layout_token" ]] && continue
  echo "===> Iceberg layout=${layout_token}"
  LAYOUTS="$layout_token" \
  PARTITION_BY="$PARTITION_BY" \
  RANGE_COLS="$RANGE_COLS" \
  LAYOUT_COLS="$LAYOUT_COLS" \
  WAREHOUSE="$WAREHOUSE" \
    bash "$CORE_SCRIPT" \
      --input "$INPUT" \
      --warehouse "$WAREHOUSE" \
      --namespace "$NAMESPACE" \
      --base-name "$BASE_NAME" \
      --partition-by "$PARTITION_BY" \
      --range-cols "$RANGE_COLS" \
      --layout-cols "$LAYOUT_COLS" \
      --shuffle "$SPARK_SHUF" \
      --target-file-mb "$TARGET_FILE_MB" \
      --layout "$layout_token" \
      "${CORE_EXTRA_ARGS[@]}"
done
