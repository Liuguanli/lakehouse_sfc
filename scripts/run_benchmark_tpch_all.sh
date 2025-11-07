#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# Hard-coded TPCH-all benchmark (mirrors the Amazon flow):
#   - For each engine: load data -> run DBGEN queries -> clean outputs.
# ============================================================

bash ./scripts/lakehouse_setup.sh --repair
source ~/.lakehouse/env

SOURCE_DIR="${SOURCE_DIR:-/datasets/tpch_1/data}"
STREAMS_ROOT="${STREAMS_ROOT:-/datasets/tpch_1/workload}"
DATA_ROOT="${DATA_ROOT:-./data/tpch_all}"
RESULTS_ROOT="${RESULTS_ROOT:-results/tpch_all}"
ICEBERG_WH="${ICEBERG_WH:-${DATA_ROOT}/iceberg_wh}"

clean_tpch_all_engine() {
  local eng="$1"
  case "$eng" in
    iceberg) rm -rf "${ICEBERG_WH}" || true ;;
    delta)   rm -rf "${DATA_ROOT}/delta" || true ;;
    hudi)    rm -rf "${DATA_ROOT}/hudi" || true ;;
  esac
}

run_engine_block() {
  local eng="$1"
  local banner="$2"

  echo "===== TPCH-all write: ${banner} ====="
  bash ./scripts/tpch_all/load_data.sh \
    --engines "$eng" \
    --source "$SOURCE_DIR" \
    --data-root "$DATA_ROOT" \
    --iceberg-warehouse "$ICEBERG_WH" \
    --overwrite

  echo "===== TPCH-all query: ${banner} ====="
  bash ./scripts/tpch_all/run_queries.sh \
    --engines "$eng" \
    --streams auto \
    --streams-root "$STREAMS_ROOT" \
    --data-root "$DATA_ROOT" \
    --results-root "$RESULTS_ROOT"

  echo "===== TPCH-all clean: ${banner} ====="
  clean_tpch_all_engine "$eng"
}

run_engine_block iceberg "Iceberg"
run_engine_block delta   "Delta"
run_engine_block hudi    "Hudi"

echo "[DONE] TPCH-all benchmark pipeline finished."
