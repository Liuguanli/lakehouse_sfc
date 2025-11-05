#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# TPCH writer: build Delta/Hudi/Iceberg tables for given scales
#
# Examples
#   bash scripts/run_tpch_write.sh --scales "1 4 16" --delta
#   bash scripts/run_tpch_write.sh --engines delta,hudi --scales "64" --overwrite
#   # Per-engine layouts (CLI > ENV > defaults)
#   bash scripts/run_tpch_write.sh --iceberg --scales "64" \
#        --iceberg-layouts baseline,linear
#
# Env overrides (optional)
#   PARTITION_BY="l_returnflag,l_linestatus"
#   RANGE_COLS="l_shipdate l_receiptdate"
#   LAYOUT_COLS="l_shipdate,l_receiptdate"
#   DELTA_LAYOUTS="baseline,linear,zorder"
#   HUDI_LAYOUTS="no_layout,linear,zorder,hilbert"
#   ICEBERG_LAYOUTS="baseline,linear,zorder"
# ============================================================

# ---------- Defaults ----------
ENG_DELTA=false
ENG_HUDI=false
ENG_ICEBERG=false
OVERWRITE=false

# Default scales (space-separated)
if [[ -n "${SCALES:-}" ]]; then
  read -r -a SCALES_ARR <<< "${SCALES}"
else
  SCALES_ARR=(1 4 16)
fi

# Common column settings
PARTITION_BY="${PARTITION_BY:-l_returnflag,l_linestatus}"
RANGE_COLS="${RANGE_COLS:-l_shipdate l_receiptdate}"       # space-separated
LAYOUT_COLS="${LAYOUT_COLS:-l_shipdate,l_receiptdate}"     # comma-separated

# Per-engine layout defaults (comma-separated)
DELTA_LAYOUTS="${DELTA_LAYOUTS:-baseline,linear,zorder}"
HUDI_LAYOUTS="${HUDI_LAYOUTS:-no_layout,linear,zorder,hilbert}"
ICEBERG_LAYOUTS="${ICEBERG_LAYOUTS:-baseline,linear,zorder}"

# Iceberg namespace/base (for table naming inside the sub-script)
ICEBERG_NS="${ICEBERG_NS:-local.demo}"
ICEBERG_BASENAME="${ICEBERG_BASENAME:-events_iceberg}"

# ---------- Helpers ----------
die()  { echo "ERROR: $*" >&2; exit 1; }
need() { [[ -f "$1" ]] || die "Missing script: $1"; }
banner(){ echo -e "\n============================================================\n  $1\n============================================================"; }

print_help() {
  cat <<'EOF'
run_tpch_write.sh
  --engines delta,hudi,iceberg     Select engines (comma list). If omitted and no flags given, run all.
  --delta | --hudi | --iceberg     Enable engine(s) individually.
  --scales "1 4 16"                Space-separated TPCH scales. Default: "1 4 16".
  --overwrite                      Remove target data dirs before writing.
  --delta-layouts "..."            Overwrite DELTA layout list (comma-separated).
  --hudi-layouts  "..."            Overwrite HUDI  layout list.
  --iceberg-layouts "..."          Overwrite ICEBERG layout list.
  -h | --help                      Show this help.

Precedence for layouts: CLI > ENV (e.g., DELTA_LAYOUTS) > script defaults.
Sub-scripts may honor LAYOUTS env; if not, they will build their own defaults.
EOF
}

# Parse ENGINES from environment first (optional)
if [[ -n "${ENGINES:-}" ]]; then
  IFS=',' read -r -a _engs <<< "${ENGINES}"
  for e in "${_engs[@]}"; do
    case "${e}" in
      delta)   ENG_DELTA=true ;;
      hudi)    ENG_HUDI=true ;;
      iceberg) ENG_ICEBERG=true ;;
      *) die "Unknown engine in ENGINES: ${e}";;
    esac
  done
fi

# ---------- CLI parsing ----------
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
    --scales)    read -r -a SCALES_ARR <<< "$2"; shift 2 ;;
    --overwrite) OVERWRITE=true;     shift ;;
    --delta-layouts)   DELTA_LAYOUTS="$2";   shift 2 ;;
    --hudi-layouts)    HUDI_LAYOUTS="$2";    shift 2 ;;
    --iceberg-layouts) ICEBERG_LAYOUTS="$2"; shift 2 ;;
    -h|--help)   print_help; exit 0 ;;
    *) die "Unknown arg: $1 (use --help)";;
  esac
done

# If no engine chosen explicitly anywhere, enable all
if ! $ENG_DELTA && ! $ENG_HUDI && ! $ENG_ICEBERG; then
  ENG_DELTA=true; ENG_HUDI=true; ENG_ICEBERG=true
fi

# ---------- Sub-scripts check ----------
$ENG_DELTA   && need "./scripts/run_delta_layouts.sh"
$ENG_HUDI    && need "./scripts/run_hudi_layouts.sh"
$ENG_ICEBERG && need "./scripts/run_iceberg_layouts.sh"

# Safe delete helper: only under ./data/tpch_${s}/...
safe_rm() {
  local path="$1"
  [[ "$path" == ./data/tpch_*/* ]] || die "Refusing to delete outside ./data/tpch_*/ : $path"
  rm -rf "$path"
}

# ---------- Engine runners ----------
run_delta_for_scale() {
  local s="$1"
  local root="./data/tpch_${s}/delta"
  $OVERWRITE && { echo "[overwrite] removing ${root}"; safe_rm "$root"; }
  banner "Delta build for tpch_${s}"
  # Pass LAYOUTS to sub-script; it should honor this (comma-separated)
  LAYOUTS="${DELTA_LAYOUTS}" \
  bash ./scripts/run_delta_layouts.sh \
    --input "/datasets/tpch_${s}.parquet" \
    --out-base "${root}" \
    --partition-by "${PARTITION_BY}" \
    --range-cols  "${RANGE_COLS}" \
    --layout-cols "${LAYOUT_COLS}"
}

run_hudi_for_scale() {
  local s="$1"
  local root="./data/tpch_${s}/hudi"
  $OVERWRITE && { echo "[overwrite] removing ${root}"; safe_rm "$root"; }
  banner "Hudi build for tpch_${s}"
  LAYOUTS="${HUDI_LAYOUTS}" \
  bash ./scripts/run_hudi_layouts.sh \
    --input "/datasets/tpch_${s}.parquet" \
    --base-dir "${root}" \
    --record-key "l_orderkey,l_linenumber" \
    --precombine-field "l_receiptdate" \
    --partition-field "${PARTITION_BY}" \
    --sort-columns "${LAYOUT_COLS}"
}

run_iceberg_for_scale() {
  local s="$1"
  local wh="./data/tpch_${s}/iceberg_wh"
  $OVERWRITE && { echo "[overwrite] removing ${wh}"; safe_rm "$wh"; }
  banner "Iceberg build for tpch_${s}"
  LAYOUTS="${ICEBERG_LAYOUTS}" \
  bash ./scripts/run_iceberg_layouts.sh \
    --input "/datasets/tpch_${s}.parquet" \
    --warehouse "${wh}" \
    --namespace "${ICEBERG_NS}" \
    --base-name "${ICEBERG_BASENAME}" \
    --partition-by "${PARTITION_BY}" \
    --range-cols  "${RANGE_COLS}" \
    --layout-cols "${LAYOUT_COLS}"
}

# ---------- Main ----------
echo "Scales    : ${SCALES_ARR[*]}"
echo "Engines   : delta=${ENG_DELTA} hudi=${ENG_HUDI} iceberg=${ENG_ICEBERG} overwrite=${OVERWRITE}"
echo "Columns   : PARTITION_BY='${PARTITION_BY}' RANGE_COLS='${RANGE_COLS}' LAYOUT_COLS='${LAYOUT_COLS}'"
echo "Layouts   : DELTA='${DELTA_LAYOUTS}' | HUDI='${HUDI_LAYOUTS}' | ICEBERG='${ICEBERG_LAYOUTS}'"

for s in "${SCALES_ARR[@]}"; do
  $ENG_DELTA   && run_delta_for_scale   "$s"
  $ENG_HUDI    && run_hudi_for_scale    "$s"
  $ENG_ICEBERG && run_iceberg_for_scale "$s"
done

echo
echo "[DONE] Completed for scales: ${SCALES_ARR[*]}"
