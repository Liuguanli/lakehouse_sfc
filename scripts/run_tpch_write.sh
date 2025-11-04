# bash ./scripts/run_delta_layouts.sh \
#   --input /datasets/tpch_1.parquet \
#   --out-base ./data/tpch_1/delta \
#   --partition-by "l_returnflag,l_linestatus" \
#   --range-cols  "l_shipdate l_receiptdate" \
#   --layout-cols "l_shipdate,l_receiptdate"

# bash ./scripts/run_hudi_layouts.sh \
#   --input /datasets/tpch_1.parquet \
#   --base-dir ./data/tpch_1/hudi \
#   --record-key l_orderkey,l_linenumber \
#   --precombine-field l_receiptdate \
#   --partition-field l_returnflag,l_linestatus \
#   --sort-columns l_shipdate,l_receiptdate

# bash ./scripts/run_iceberg_layouts.sh \
#   --input /datasets/tpch_1.parquet \
#   --warehouse ./data/tpch_1/iceberg_wh \
#   --namespace local.demo --base-name events_iceberg \
#   --partition-by "l_returnflag,l_linestatus" \
#   --range-cols "l_shipdate,l_receiptdate" \
#   --layout-cols "l_shipdate,l_receiptdate"

#!/usr/bin/env bash
set -euo pipefail

# --------------------------
# Usage:
#   bash scripts/run_tpch_write.sh
#   bash scripts/run_tpch_write.sh --engines delta,hudi --scales "1 4 16 64"
#   bash scripts/run_tpch_write.sh --delta --iceberg --scales "1 64"
#   bash scripts/run_tpch_write.sh --overwrite --engines delta --scales "16"
#
# Notes:
#   - If neither --engines nor any of --delta/--hudi/--iceberg is provided,
#     all three engines run by default.
#   - Scales default to "1 4 16".
#   - Sub-scripts required:
#       ./scripts/run_delta_layouts.sh
#       ./scripts/run_hudi_layouts.sh
#       ./scripts/run_iceberg_layouts.sh
# --------------------------

# ----- Defaults -----
ENG_DELTA=false
ENG_HUDI=false
ENG_ICEBERG=false
OVERWRITE=false

if [[ -n "${ENGINES:-}" ]]; then
  IFS=',' read -r -a _engs <<< "${ENGINES}"
  for e in "${_engs[@]}"; do
    case "${e}" in
      delta)   ENG_DELTA=true ;;
      hudi)    ENG_HUDI=true ;;
      iceberg) ENG_ICEBERG=true ;;
      *) echo "Unknown engine in ENGINES: ${e}" >&2; exit 1;;
    esac
  done
fi

# If CLI provides --scales, it will override env SCALES below
if [[ -n "${SCALES:-}" ]]; then
  read -r -a SCALES_ARR <<< "${SCALES}"
else
  SCALES_ARR=(1 4 16)  # default
fi

# ----- Common columns/args (shared across engines) -----
PARTITION_BY="${PARTITION_BY:-l_returnflag,l_linestatus}"
RANGE_COLS="${RANGE_COLS:-l_shipdate l_receiptdate}"     # space-separated
LAYOUT_COLS="${LAYOUT_COLS:-l_shipdate,l_receiptdate}"   # comma-separated

# Iceberg naming (overridable via env)
ICEBERG_NS="${ICEBERG_NS:-local.demo}"
ICEBERG_BASENAME="${ICEBERG_BASENAME:-events_iceberg}"

# ----- Helpers -----
die() { echo "ERROR: $*" >&2; exit 1; }
need() { [[ -f "$1" ]] || die "Missing script: $1"; }
banner() { echo -e "\n============================================================\n  $1\n============================================================"; }

print_help() {
  cat <<'EOF'
write_tpch.sh
  --engines delta,hudi,iceberg   Comma-separated engines to run. If omitted and no flags passed, run all.
  --delta | --hudi | --iceberg   Enable engine(s) individually.
  --scales "1 4 16"              Space-separated TPCH scales. Default: "1 4 16".
  --overwrite                    Delete target data directories for selected engines/scales before writing.
  -h|--help                      Show this help.

Examples:
  bash scripts/write_tpch.sh
  bash scripts/write_tpch.sh --engines delta,hudi --scales "1 64"
  bash scripts/write_tpch.sh --delta --iceberg --scales "1 4 16"
  bash scripts/write_tpch.sh --overwrite --engines delta --scales "16"
EOF
}

# ----- Parse CLI -----
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
    -h|--help)   print_help; exit 0 ;;
    *) die "Unknown arg: $1 (use --help)";;
  esac
done

# If no engine chosen explicitly anywhere, enable all
if ! $ENG_DELTA && ! $ENG_HUDI && ! $ENG_ICEBERG; then
  ENG_DELTA=true; ENG_HUDI=true; ENG_ICEBERG=true
fi

# Ensure sub-scripts exist for enabled engines
$ENG_DELTA   && need "./scripts/run_delta_layouts.sh"
$ENG_HUDI    && need "./scripts/run_hudi_layouts.sh"
$ENG_ICEBERG && need "./scripts/run_iceberg_layouts.sh"

# Safe delete helper: only remove under ./data/tpch_${s}/...
safe_rm() {
  local path="$1"
  [[ "$path" == ./data/tpch_*/* ]] || die "Refusing to delete outside ./data/tpch_*/ : $path"
  rm -rf "$path"
}

# ----- Engine runners -----
run_delta_for_scale() {
  local s="$1"
  local root="./data/tpch_${s}/delta"
  $OVERWRITE && { echo "[overwrite] removing ${root}"; safe_rm "$root"; }
  banner "Delta build for tpch_${s}"
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
  bash ./scripts/run_iceberg_layouts.sh \
    --input "/datasets/tpch_${s}.parquet" \
    --warehouse "${wh}" \
    --namespace "${ICEBERG_NS}" \
    --base-name "${ICEBERG_BASENAME}" \
    --partition-by "${PARTITION_BY}" \
    --range-cols "${LAYOUT_COLS}" \
    --layout-cols "${LAYOUT_COLS}"
}

# ----- Main loop -----
echo "Scales: ${SCALES_ARR[*]}"
echo "Engines: delta=${ENG_DELTA} hudi=${ENG_HUDI} iceberg=${ENG_ICEBERG} overwrite=${OVERWRITE}"

for s in "${SCALES_ARR[@]}"; do
  $ENG_DELTA   && run_delta_for_scale   "$s"
  $ENG_HUDI    && run_hudi_for_scale    "$s"
  $ENG_ICEBERG && run_iceberg_for_scale "$s"
done

echo
echo "[DONE] Completed for scales: ${SCALES_ARR[*]}"
