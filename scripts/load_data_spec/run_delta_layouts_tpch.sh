#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------------------------------------
# TPC-H Delta loader wrapper.
# Provides opinionated defaults for layouts, columns, and data locations,
# while delegating the actual Spark submission to load_data_core/run_delta_layouts.sh.
#
# Example:
#   bash scripts/load_data_spec/run_delta_layouts_tpch.sh \
#     --scales "1 4 16" \
#     --layouts "baseline,zorder" \
#     --input-template "/datasets/tpch_%s.parquet"
#
# Additional arguments after `--` are forwarded verbatim to the core script.
# ---------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPTS_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
CORE_SCRIPT="${SCRIPTS_ROOT}/load_data_core/run_delta_layouts.sh"

[[ -f "${CORE_SCRIPT}" ]] || { echo "Missing core script: ${CORE_SCRIPT}" >&2; exit 1; }

# Capture parent-provided layout list before we start overriding $LAYOUTS per job.
PARENT_LAYOUTS="${LAYOUTS:-}"

# Defaults (overridable by env or CLI)
DEFAULT_SCALES="1 4 16"
SCALES_RAW="${SCALES:-$DEFAULT_SCALES}"
LAYOUTS_OVERRIDE=""

PARTITION_BY="${PARTITION_BY:-l_returnflag,l_linestatus}"
RANGE_COLS="${RANGE_COLS:-l_shipdate l_receiptdate}"
LAYOUT_COLS="${LAYOUT_COLS:-l_shipdate,l_receiptdate}"

INPUT_TEMPLATE_DEFAULT="/datasets/tpch_%s.parquet"
OUTPUT_TEMPLATE_DEFAULT="./data/tpch_%s/delta"
INPUT_TEMPLATE="${DELTA_INPUT_TEMPLATE:-$INPUT_TEMPLATE_DEFAULT}"
OUTPUT_TEMPLATE="${DELTA_OUTPUT_TEMPLATE:-$OUTPUT_TEMPLATE_DEFAULT}"

CORE_EXTRA_ARGS=()

usage() {
  cat <<'EOF'
run_delta_layouts_tpch.sh
  Opinionated wrapper around load_data_core/run_delta_layouts.sh for TPC-H data.

Options:
  --scales "1 4 16"          Space/comma separated scale factors (default: 1 4 16)
  --layouts "a,b,c"          Layout list to materialise (default: DELTA_LAYOUTS or baseline,linear,zorder)
  --input-template STRING    printf-style template for source parquet (default: /datasets/tpch_%s.parquet)
  --output-template STRING   printf-style template for Delta output root (default: ./data/tpch_%s/delta)
  --partition-by COLS        Override partition columns (comma/space separated)
  --range-cols COLS          Override range columns (comma/space separated)
  --layout-cols COLS         Override layout columns (comma/space separated)
  --help                     Show this help and exit

Notes:
  * Additional arguments can be passed to the core script after a literal `--`.
  * Environment overrides:
      SCALES="1 4"                -> same as --scales
      DELTA_LAYOUTS="baseline"    -> same as --layouts
      DELTA_INPUT_TEMPLATE=...    -> override input template
      DELTA_OUTPUT_TEMPLATE=...   -> override output template
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
    --output-template)
      OUTPUT_TEMPLATE="$2"; shift 2;;
    --partition-by)
      PARTITION_BY="$2"; shift 2;;
    --range-cols)
      RANGE_COLS="$2"; shift 2;;
    --layout-cols)
      LAYOUT_COLS="$2"; shift 2;;
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

LAYOUT_SRC="${LAYOUTS_OVERRIDE:-${DELTA_LAYOUTS:-${PARENT_LAYOUTS:-baseline,linear,zorder}}}"
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
  out_base=$(printf "$OUTPUT_TEMPLATE" "$scale")
  echo "===> Delta build for tpch_${scale}"
  for layout in "${LAYOUT_ARR[@]}"; do
    layout_token="${layout,,}"
    layout_token="${layout_token// /}"
    if [[ -z "$layout_token" ]]; then
      continue
    fi
    echo "  -> layout=${layout_token} input=${input_path} out=${out_base}"
    LAYOUTS="$layout_token" \
      bash "$CORE_SCRIPT" \
        --input "$input_path" \
        --out-base "$out_base" \
        --partition-by "$PARTITION_BY" \
        --range-cols "$RANGE_COLS" \
        --layout-cols "$LAYOUT_COLS" \
        "${CORE_EXTRA_ARGS[@]}"
  done
done
