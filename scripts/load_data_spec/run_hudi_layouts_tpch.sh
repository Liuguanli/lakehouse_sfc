#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------------------------------------
# TPC-H Hudi loader wrapper.
# Encapsulates layout defaults while delegating execution to
# load_data_core/run_hudi_layouts.sh.
#
# Example:
#   bash scripts/load_data_spec/run_hudi_layouts_tpch.sh \
#     --scales "16" --layouts "linear,zorder"
# ---------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPTS_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
CORE_SCRIPT="${SCRIPTS_ROOT}/load_data_core/run_hudi_layouts.sh"

[[ -f "${CORE_SCRIPT}" ]] || { echo "Missing core script: ${CORE_SCRIPT}" >&2; exit 1; }

PARENT_LAYOUTS="${LAYOUTS:-}"

DEFAULT_SCALES="1 4 16"
SCALES_RAW="${SCALES:-$DEFAULT_SCALES}"
LAYOUTS_OVERRIDE=""

INPUT_TEMPLATE_DEFAULT="/datasets/tpch_%s.parquet"
BASE_TEMPLATE_DEFAULT="./data/tpch_%s/hudi"
INPUT_TEMPLATE="${HUDI_INPUT_TEMPLATE:-$INPUT_TEMPLATE_DEFAULT}"
BASE_TEMPLATE="${HUDI_BASE_TEMPLATE:-$BASE_TEMPLATE_DEFAULT}"

RECORD_KEY="${RECORD_KEY:-l_orderkey,l_linenumber}"
PRECOMBINE_FIELD="${PRECOMBINE_FIELD:-l_receiptdate}"
PARTITION_FIELD="${PARTITION_FIELD:-l_returnflag,l_linestatus}"
SORT_COLUMNS="${SORT_COLUMNS:-l_shipdate,l_receiptdate}"
TARGET_FILE_MB="${TARGET_FILE_MB:-128}"

CORE_EXTRA_ARGS=()

usage() {
  cat <<'EOF'
run_hudi_layouts_tpch.sh
  Wrapper around load_data_core/run_hudi_layouts.sh with TPC-H defaults.

Options:
  --scales "1 4 16"          Space/comma separated scale factors (default: 1 4 16)
  --layouts "no_layout,zorder" Layout names to materialise (default: HUDI_LAYOUTS or no_layout,linear,zorder,hilbert)
  --input-template STRING    printf-style template for source parquet (default: /datasets/tpch_%s.parquet)
  --base-template STRING     printf-style template for target base dir (default: ./data/tpch_%s/hudi)
  --record-key COLS          Override record key columns (default: l_orderkey,l_linenumber)
  --precombine-field COL     Override precombine field (default: l_receiptdate)
  --partition-field COLS     Override partition columns (default: l_returnflag,l_linestatus)
  --sort-columns COLS        Override sort columns (default: l_shipdate,l_receiptdate)
  --target-file-mb N         Override target file size MB (default: 128)
  --help                     Show this help and exit

Notes:
  * Additional arguments can be passed to the core script after `--`.
  * Environment overrides:
      SCALES="1 4"               -> same as --scales
      HUDI_LAYOUTS="linear"      -> same as --layouts
      HUDI_INPUT_TEMPLATE=...    -> override input template
      HUDI_BASE_TEMPLATE=...     -> override base template
      RECORD_KEY=...             -> override record key, etc.
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
    --base-template)
      BASE_TEMPLATE="$2"; shift 2;;
    --record-key)
      RECORD_KEY="$2"; shift 2;;
    --precombine-field)
      PRECOMBINE_FIELD="$2"; shift 2;;
    --partition-field)
      PARTITION_FIELD="$2"; shift 2;;
    --sort-columns)
      SORT_COLUMNS="$2"; shift 2;;
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

LAYOUT_SRC="${LAYOUTS_OVERRIDE:-${HUDI_LAYOUTS:-${PARENT_LAYOUTS:-no_layout,linear,zorder,hilbert}}}"
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
  base_dir=$(printf "$BASE_TEMPLATE" "$scale")
  echo "===> Hudi build for tpch_${scale}"
  for layout in "${LAYOUT_ARR[@]}"; do
    layout_token="${layout,,}"
    layout_token="${layout_token// /}"
    if [[ -z "$layout_token" ]]; then
      continue
    fi
    echo "  -> layout=${layout_token} input=${input_path} base=${base_dir}"
    LAYOUTS="$layout_token" \
      bash "$CORE_SCRIPT" \
        --input "$input_path" \
        --base-dir "$base_dir" \
        --record-key "$RECORD_KEY" \
        --precombine-field "$PRECOMBINE_FIELD" \
        --partition-field "$PARTITION_FIELD" \
        --sort-columns "$SORT_COLUMNS" \
        --target-file-mb "$TARGET_FILE_MB" \
        "${CORE_EXTRA_ARGS[@]}"
  done
done
