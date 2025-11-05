# bash ./scripts/run_delta_layouts.sh \
#   --input /datasets/tpch_1.parquet \
#   --out-base ./data/tpch_1/delta \
#   --partition-by "l_returnflag,l_linestatus" \
#   --range-cols  "l_shipdate l_receiptdate" \
#   --layout-cols "l_shipdate,l_receiptdate"

#!/usr/bin/env bash
set -euo pipefail

# SCALES=(1 4 16 64)
SCALES=(1 4 16)

COMMON_ARGS=(
  --partition-by "l_returnflag,l_linestatus"
  --range-cols  "l_shipdate l_receiptdate"   # space-separated
  --layout-cols "l_shipdate,l_receiptdate"   # comma-separated
)

for s in "${SCALES[@]}"; do
  echo "===> Delta build for tpch_${s}"
  bash ./scripts/run_delta_layouts.sh \
    --input "/datasets/tpch_${s}.parquet" \
    --out-base "./data/tpch_${s}/delta" \
    "${COMMON_ARGS[@]}"
done
