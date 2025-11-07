#!/usr/bin/env bash
set -euo pipefail

# --------------------------
# Usage:
#   bash scripts/run_reviews_write.sh
#   bash scripts/run_reviews_write.sh --engines delta,hudi
#   bash scripts/run_reviews_write.sh --delta --overwrite
#
# Optional env overrides:
#   INPUT=/datasets/amazon_review_all.csv
#   PARTITION_BY="col1,col2"     # comma-separated
#   RANGE_COLS="colA colB"       # space-separated (only if needed)
#   LAYOUT_COLS="colX,colY"      # comma-separated (only if needed)
#   ICEBERG_NS=local.demo
#   ICEBERG_BASENAME=reviews_iceberg
# --------------------------

# ----- Defaults -----
ENG_DELTA=false
ENG_HUDI=false
ENG_ICEBERG=false
OVERWRITE=false

# Default input file (override via env INPUT)
INPUT="${INPUT:-/datasets/amazon_review_all.csv}"

# record_timestamp, rating — current default, balancing temporal and quality dimensions

# record_timestamp, asin — time + product identifier

# category, record_timestamp — partition by category first, then sort by time

# record_timestamp, helpful_vote — time + user engagement level

# Column arguments with dataset-aware defaults
PARTITION_BY="${PARTITION_BY:-category}"
RANGE_COLS="${RANGE_COLS:-record_timestamp}"
LAYOUT_COLS="${LAYOUT_COLS:-record_timestamp,rating}"

# Per-engine layout defaults
DELTA_LAYOUTS="${DELTA_LAYOUTS:-baseline,linear,zorder}"
HUDI_LAYOUTS="${HUDI_LAYOUTS:-no_layout,linear,zorder,hilbert}"
ICEBERG_LAYOUTS="${ICEBERG_LAYOUTS:-baseline,linear,zorder}"

# Iceberg naming (overridable via env)
ICEBERG_NS="${ICEBERG_NS:-local.demo}"
ICEBERG_BASENAME="${ICEBERG_BASENAME:-events_iceberg}"

# Script locations (resolved relative to this file)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DELTA_SPEC_SCRIPT="${SCRIPT_DIR}/load_data_spec/run_delta_layouts_amazon.sh"
HUDI_SPEC_SCRIPT="${SCRIPT_DIR}/load_data_spec/run_hudi_layouts_amazon.sh"
ICEBERG_SPEC_SCRIPT="${SCRIPT_DIR}/load_data_spec/run_iceberg_layouts_amazon.sh"

DATA_ROOT="./data/amazon"
DELTA_ROOT="${DATA_ROOT}/delta"
HUDI_ROOT="${DATA_ROOT}/hudi"
ICEBERG_WH="${DATA_ROOT}/iceberg_wh"

# ----- Helpers -----
die()   { echo "ERROR: $*" >&2; exit 1; }
need()  { [[ -f "$1" ]] || die "Missing script: $1"; }
banner(){ echo -e "\n============================================================\n  $1\n============================================================"; }

print_help() {
  cat <<'EOF'
run_reviews_write.sh
  --engines delta,hudi,iceberg   Comma-separated engines to run
  --delta | --hudi | --iceberg   Enable engine(s) individually
  --overwrite                    Remove target output dir before writing
  --input PATH                   Override input CSV path
  --partition-by COLS            Override partition columns
  --range-cols COLS              Override range columns
  --layout-cols COLS             Override layout/sort columns
  --delta-layouts LIST           Override Delta layouts (comma-separated)
  --hudi-layouts LIST            Override Hudi layouts
  --iceberg-layouts LIST         Override Iceberg layouts
  -h|--help                      Show this help

Environment overrides:
  INPUT=/datasets/amazon_review_all.csv
  PARTITION_BY="col1,col2"    RANGE_COLS="colA colB"   LAYOUT_COLS="colX,colY"
  ICEBERG_NS=local.demo       ICEBERG_BASENAME=reviews_iceberg
EOF
}

# ----- CLI parsing -----
while [[ $# -gt 0 ]]; do
  case "$1" in
    --engines)
      IFS=',' read -r -a engs <<< "$2"
      ENG_DELTA=false; ENG_HUDI=false; ENG_ICEBERG=false
      for e in "${engs[@]}"; do
        case "${e}" in
          delta)   ENG_DELTA=true ;;
          hudi)    ENG_HUDI=true ;;
          iceberg) ENG_ICEBERG=true ;;
          *) die "Unknown engine: ${e}" ;;
        esac
      done
      shift 2
      ;;
    --delta)     ENG_DELTA=true;     shift ;;
    --hudi)      ENG_HUDI=true;      shift ;;
    --iceberg)   ENG_ICEBERG=true;   shift ;;
    --overwrite) OVERWRITE=true;     shift ;;
    --input)     INPUT="$2"; shift 2;;
    --partition-by) PARTITION_BY="$2"; shift 2;;
    --range-cols)   RANGE_COLS="$2"; shift 2;;
    --layout-cols)  LAYOUT_COLS="$2"; shift 2;;
    --delta-layouts)   DELTA_LAYOUTS="$2"; shift 2;;
    --hudi-layouts)    HUDI_LAYOUTS="$2"; shift 2;;
    --iceberg-layouts) ICEBERG_LAYOUTS="$2"; shift 2;;
    -h|--help)   print_help; exit 0 ;;
    *) die "Unknown arg: $1 (use --help)";;
  esac
done

# Enable all engines if none explicitly selected
if ! $ENG_DELTA && ! $ENG_HUDI && ! $ENG_ICEBERG; then
  ENG_DELTA=true; ENG_HUDI=true; ENG_ICEBERG=true
fi

# Ensure sub-scripts exist for selected engines
$ENG_DELTA   && need "$DELTA_SPEC_SCRIPT"
$ENG_HUDI    && need "$HUDI_SPEC_SCRIPT"
$ENG_ICEBERG && need "$ICEBERG_SPEC_SCRIPT"

# Safe delete helper: restrict deletions to data/amazon/*
safe_rm() {
  local path="$1"
  [[ "$path" == ${DATA_ROOT}/* ]] || die "Refusing to delete outside ${DATA_ROOT}/ : $path"
  rm -rf "$path"
}

# ----- Engine runners -----
run_delta() {
  $OVERWRITE && { echo "[overwrite] removing ${DELTA_ROOT}"; safe_rm "${DELTA_ROOT}"; }
  banner "Delta layouts (amazon)"
  local -a args=(
    --input "${INPUT}"
    --out-base "${DELTA_ROOT}"
    --partition-by "${PARTITION_BY}"
    --range-cols "${RANGE_COLS}"
    --layout-cols "${LAYOUT_COLS}"
  )
  [[ -n "${DELTA_LAYOUTS:-}" ]] && args+=(--layouts "${DELTA_LAYOUTS}")
  bash "$DELTA_SPEC_SCRIPT" "${args[@]}"
}

run_hudi() {
  $OVERWRITE && { echo "[overwrite] removing ${HUDI_ROOT}"; safe_rm "${HUDI_ROOT}"; }
  banner "Hudi layouts (amazon)"
  local -a args=(
    --input "${INPUT}"
    --base-dir "${HUDI_ROOT}"
    --record-key "user_id,asin"
    --precombine-field "record_timestamp"
    --partition-field "${PARTITION_BY}"
    --sort-columns "${LAYOUT_COLS}"
    --target-file-mb "256"
  )
  [[ -n "${HUDI_LAYOUTS:-}" ]] && args+=(--layouts "${HUDI_LAYOUTS}")
  bash "$HUDI_SPEC_SCRIPT" "${args[@]}"
}

run_iceberg() {
  $OVERWRITE && { echo "[overwrite] removing ${ICEBERG_WH}"; safe_rm "${ICEBERG_WH}"; }
  banner "Iceberg layouts (amazon)"
  local -a args=(
    --input "${INPUT}"
    --warehouse "${ICEBERG_WH}"
    --namespace "${ICEBERG_NS}"
    --base-name "${ICEBERG_BASENAME}"
    --partition-by "${PARTITION_BY}"
    --range-cols "${RANGE_COLS}"
    --layout-cols "${LAYOUT_COLS}"
    --shuffle "800"
    --target-file-mb "256"
  )
  [[ -n "${ICEBERG_LAYOUTS:-}" ]] && args+=(--layouts "${ICEBERG_LAYOUTS}")
  bash "$ICEBERG_SPEC_SCRIPT" "${args[@]}"
}

# ----- Main -----
echo "Input    : ${INPUT}"
echo "Engines  : delta=${ENG_DELTA} hudi=${ENG_HUDI} iceberg=${ENG_ICEBERG} overwrite=${OVERWRITE}"
echo "Columns  : PARTITION_BY='${PARTITION_BY}' RANGE_COLS='${RANGE_COLS}' LAYOUT_COLS='${LAYOUT_COLS}'"
echo "Layouts  : DELTA='${DELTA_LAYOUTS}' | HUDI='${HUDI_LAYOUTS}' | ICEBERG='${ICEBERG_LAYOUTS}'"

mkdir -p "$DATA_ROOT"

$ENG_DELTA   && run_delta
$ENG_HUDI    && run_hudi
$ENG_ICEBERG && run_iceberg

echo
echo "[DONE] Completed for: ${INPUT}"
