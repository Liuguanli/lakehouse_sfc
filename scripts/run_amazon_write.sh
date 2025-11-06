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

# Column arguments (customize as needed)
PARTITION_BY="${PARTITION_BY:-}"   # e.g., user_id,product_id
RANGE_COLS="${RANGE_COLS:-}"       # e.g., review_date
LAYOUT_COLS="${LAYOUT_COLS:-}"     # e.g., review_date,star_rating

# Iceberg naming (overridable via env)
ICEBERG_NS="${ICEBERG_NS:-local.demo}"
ICEBERG_BASENAME="${ICEBERG_BASENAME:-reviews_iceberg}"

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
    -h|--help)   print_help; exit 0 ;;
    *) die "Unknown arg: $1 (use --help)";;
  esac
done

# Enable all engines if none explicitly selected
if ! $ENG_DELTA && ! $ENG_HUDI && ! $ENG_ICEBERG; then
  ENG_DELTA=true; ENG_HUDI=true; ENG_ICEBERG=true
fi

# Ensure sub-scripts exist for selected engines
$ENG_DELTA   && need "./scripts/run_delta_layouts.sh"
$ENG_HUDI    && need "./scripts/run_hudi_layouts.sh"
$ENG_ICEBERG && need "./scripts/run_iceberg_layouts.sh"

# Safe delete helper: restrict deletions to ./data/reviews/*
safe_rm() {
  local path="$1"
  [[ "$path" == ./data/reviews/* ]] || die "Refusing to delete outside ./data/reviews/: $path"
  rm -rf "$path"
}

# ----- Engine runners -----
run_delta() {
  local root="./data/reviews/delta"
  $OVERWRITE && { echo "[overwrite] removing ${root}"; safe_rm "$root"; }
  banner "Delta build for amazon_review_all.csv"
  bash ./scripts/run_delta_layouts.sh \
    --input "${INPUT}" \
    --out-base "${root}" \
    ${PARTITION_BY:+--partition-by "${PARTITION_BY}"} \
    ${RANGE_COLS:+--range-cols "${RANGE_COLS}"} \
    ${LAYOUT_COLS:+--layout-cols "${LAYOUT_COLS}"}
}

run_hudi() {
  local root="./data/reviews/hudi"
  $OVERWRITE && { echo "[overwrite] removing ${root}"; safe_rm "$root"; }
  banner "Hudi build for amazon_review_all.csv"
  bash ./scripts/run_hudi_layouts.sh \
    --input "${INPUT}" \
    --base-dir "${root}" \
    --record-key "id" \
    --precombine-field "event_time" \
    ${PARTITION_BY:+--partition-field "${PARTITION_BY}"} \
    ${LAYOUT_COLS:+--sort-columns "${LAYOUT_COLS}"}
}

run_iceberg() {
  local wh="./data/reviews/iceberg_wh"
  $OVERWRITE && { echo "[overwrite] removing ${wh}"; safe_rm "$wh"; }
  banner "Iceberg build for amazon_review_all.csv"
  bash ./scripts/run_iceberg_layouts.sh \
    --input "${INPUT}" \
    --warehouse "${wh}" \
    --namespace "${ICEBERG_NS}" \
    --base-name "${ICEBERG_BASENAME}" \
    ${PARTITION_BY:+--partition-by "${PARTITION_BY}"} \
    ${RANGE_COLS:+--range-cols "${RANGE_COLS}"} \
    ${LAYOUT_COLS:+--layout-cols "${LAYOUT_COLS}"}
}

# ----- Main -----
echo "Input : ${INPUT}"
echo "Engines: delta=${ENG_DELTA} hudi=${ENG_HUDI} iceberg=${ENG_ICEBERG} overwrite=${OVERWRITE}"
echo "Columns: PARTITION_BY='${PARTITION_BY}' RANGE_COLS='${RANGE_COLS}' LAYOUT_COLS='${LAYOUT_COLS}'"

$ENG_DELTA   && run_delta
$ENG_HUDI    && run_hudi
$ENG_ICEBERG && run_iceberg

echo
echo "[DONE] Completed for: ${INPUT}"
