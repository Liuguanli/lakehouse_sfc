# bash ./scripts/run_hudi_layouts.sh \
#   --input /datasets/tpch_1.parquet \
#   --base-dir ./data/tpch_1/hudi \
#   --record-key l_orderkey,l_linenumber \
#   --precombine-field l_receiptdate \
#   --partition-field l_returnflag,l_linestatus \
#   --sort-columns l_shipdate,l_receiptdate


#!/usr/bin/env bash
set -euo pipefail

# SCALES=(1 4 16 64)
SCALES=(1 4 16)

COMMON_ARGS=(
  --record-key l_orderkey,l_linenumber
  --precombine-field l_receiptdate
  --partition-field l_returnflag,l_linestatus
  --sort-columns l_shipdate,l_receiptdate
)

for s in "${SCALES[@]}"; do
  echo "===> Running scale tpch_${s}"
  bash ./scripts/run_hudi_layouts.sh \
    --input "/datasets/tpch_${s}.parquet" \
    --base-dir "./data/tpch_${s}/hudi" \
    "${COMMON_ARGS[@]}"
done
