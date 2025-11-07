#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------------------------------------
# Amazon reviews Hudi loader wrapper.
# Executes the core runner for the configured layouts with sensible defaults.
# ---------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPTS_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
CORE_SCRIPT="${SCRIPTS_ROOT}/load_data_core/run_hudi_layouts.sh"

[[ -f "${CORE_SCRIPT}" ]] || { echo "Missing core script: ${CORE_SCRIPT}" >&2; exit 1; }

PARENT_LAYOUTS="${LAYOUTS:-}"

INPUT="${INPUT:-/datasets/amazon_review_all.csv}"
BASE_DIR="${BASE_DIR:-./data/amazon/hudi}"

RECORD_KEY="${RECORD_KEY:-user_id,asin}"
PRECOMBINE_FIELD="${PRECOMBINE_FIELD:-record_timestamp}"
PARTITION_FIELD="${PARTITION_FIELD:-category}"
SORT_COLUMNS="${SORT_COLUMNS:-record_timestamp,rating}"
TARGET_FILE_MB="${TARGET_FILE_MB:-256}"

LAYOUTS_OVERRIDE=""
CORE_EXTRA_ARGS=()

usage() {
  cat <<'EOF'
run_hudi_layouts_amazon.sh
  Wrapper around load_data_core/run_hudi_layouts.sh for the Amazon reviews CSV.

Options:
  --input PATH            Source CSV path (default: /datasets/amazon_review_all.csv)
  --base-dir DIR          Output base dir (default: ./data/amazon/hudi)
  --layouts LIST          Layouts to build (default: HUDI_LAYOUTS or no_layout,linear,zorder,hilbert)
  --record-key COLS       Hudi record key columns (default: user_id,asin)
  --precombine-field COL  Hudi precombine field (default: record_timestamp)
  --partition-field COLS  Partition field(s) (default: category)
  --sort-columns COLS     Sort/layout columns (default: record_timestamp,rating)
  --target-file-mb N      Target file size in MB (default: 256)
  --help                  Show help

Pass additional core-script arguments after `--`.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --input)            INPUT="$2"; shift 2;;
    --base-dir)         BASE_DIR="$2"; shift 2;;
    --layouts)          LAYOUTS_OVERRIDE="$2"; shift 2;;
    --record-key)       RECORD_KEY="$2"; shift 2;;
    --precombine-field) PRECOMBINE_FIELD="$2"; shift 2;;
    --partition-field)  PARTITION_FIELD="$2"; shift 2;;
    --sort-columns)     SORT_COLUMNS="$2"; shift 2;;
    --target-file-mb)   TARGET_FILE_MB="$2"; shift 2;;
    -h|--help)          usage; exit 0;;
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

LAYOUT_SRC="${LAYOUTS_OVERRIDE:-${HUDI_LAYOUTS:-${PARENT_LAYOUTS:-no_layout,linear,zorder,hilbert}}}"
mapfile -t LAYOUT_ARR < <(normalize_list "$LAYOUT_SRC")

if [[ ${#LAYOUT_ARR[@]} -eq 0 ]]; then
  echo "[WARN] No Hudi layouts selected; nothing to do." >&2
  exit 0
fi

for layout in "${LAYOUT_ARR[@]}"; do
  layout_token="${layout,,}"
  layout_token="${layout_token// /}"
  [[ -z "$layout_token" ]] && continue
  echo "===> Hudi layout=${layout_token}"
  LAYOUTS="$layout_token" \
    bash "$CORE_SCRIPT" \
      --input "$INPUT" \
      --base-dir "$BASE_DIR" \
      --record-key "$RECORD_KEY" \
      --precombine-field "$PRECOMBINE_FIELD" \
      --partition-field "$PARTITION_FIELD" \
      --sort-columns "$SORT_COLUMNS" \
      --target-file-mb "$TARGET_FILE_MB" \
      "${CORE_EXTRA_ARGS[@]}"
done
