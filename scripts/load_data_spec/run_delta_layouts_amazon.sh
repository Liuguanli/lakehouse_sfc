#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------------------------------------
# Amazon reviews Delta loader wrapper.
# Mirrors the TPC-H helpers but targets a single CSV dataset (no scales).
# Iterates over requested layouts and forwards standard column settings.
# ---------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPTS_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
CORE_SCRIPT="${SCRIPTS_ROOT}/load_data_core/run_delta_layouts.sh"

[[ -f "${CORE_SCRIPT}" ]] || { echo "Missing core script: ${CORE_SCRIPT}" >&2; exit 1; }

PARENT_LAYOUTS="${LAYOUTS:-}"

INPUT="${INPUT:-/datasets/amazon_review_all.csv}"
OUT_BASE="${OUT_BASE:-./data/amazon/delta}"

PARTITION_BY="${PARTITION_BY:-category}"
RANGE_COLS="${RANGE_COLS:-record_timestamp}"
LAYOUT_COLS="${LAYOUT_COLS:-record_timestamp,rating}"

LAYOUTS_OVERRIDE=""
CORE_EXTRA_ARGS=()

usage() {
  cat <<'EOF'
run_delta_layouts_amazon.sh
  Opinionated wrapper around load_data_core/run_delta_layouts.sh for the
  Amazon reviews CSV dataset.

Options:
  --input PATH             Source CSV path (default: /datasets/amazon_review_all.csv)
  --out-base DIR           Output base directory (default: ./data/amazon/delta)
  --layouts list           Layouts to build (default: DELTA_LAYOUTS or baseline,linear,zorder)
  --partition-by COLS      Partition columns (comma/space separated)
  --range-cols COLS        Range/repartition columns (comma/space separated)
  --layout-cols COLS       Layout/order columns (comma/space separated)
  --help                   Show this help

Notes:
  * Additional arguments for the core script can be passed after a literal `--`.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --input)        INPUT="$2"; shift 2;;
    --out-base)     OUT_BASE="$2"; shift 2;;
    --layouts)      LAYOUTS_OVERRIDE="$2"; shift 2;;
    --partition-by) PARTITION_BY="$2"; shift 2;;
    --range-cols)   RANGE_COLS="$2"; shift 2;;
    --layout-cols)  LAYOUT_COLS="$2"; shift 2;;
    -h|--help)      usage; exit 0;;
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

LAYOUT_SRC="${LAYOUTS_OVERRIDE:-${DELTA_LAYOUTS:-${PARENT_LAYOUTS:-baseline,linear,zorder}}}"
mapfile -t LAYOUT_ARR < <(normalize_list "$LAYOUT_SRC")

if [[ ${#LAYOUT_ARR[@]} -eq 0 ]]; then
  echo "[WARN] No Delta layouts selected; nothing to do." >&2
  exit 0
fi

for layout in "${LAYOUT_ARR[@]}"; do
  layout_token="${layout,,}"
  layout_token="${layout_token// /}"
  [[ -z "$layout_token" ]] && continue
  echo "===> Delta layout=${layout_token}"
  LAYOUTS="$layout_token" \
    bash "$CORE_SCRIPT" \
      --input "$INPUT" \
      --out-base "$OUT_BASE" \
      --partition-by "$PARTITION_BY" \
      --range-cols "$RANGE_COLS" \
      --layout-cols "$LAYOUT_COLS" \
      "${CORE_EXTRA_ARGS[@]}"
done
