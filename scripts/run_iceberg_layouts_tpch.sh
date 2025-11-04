# bash ./scripts/run_iceberg_layouts.sh \
#   --input /datasets/tpch_1.parquet \
#   --warehouse ./data/tpch_1/iceberg_wh \
#   --namespace local.demo --base-name events_iceberg \
#   --partition-by "l_returnflag,l_linestatus" \
#   --range-cols "l_shipdate,l_receiptdate" \
#   --layout-cols "l_shipdate,l_receiptdate"

#!/usr/bin/env bash
set -euo pipefail

# SCALES=(1 4 16 64)
SCALES=(1 4 16)

NS="local.demo"
BASE_NAME="events_iceberg"
COMMON_ARGS=(
  --namespace "$NS"
  --base-name "$BASE_NAME"
  --partition-by "l_returnflag,l_linestatus"
  --range-cols "l_shipdate,l_receiptdate"   
  --layout-cols "l_shipdate,l_receiptdate" 
)

for s in "${SCALES[@]}"; do
  echo "===> Iceberg build for tpch_${s}"
  bash ./scripts/run_iceberg_layouts.sh \
    --input "/datasets/tpch_${s}.parquet" \
    --warehouse "./data/tpch_${s}/iceberg_wh" \
    "${COMMON_ARGS[@]}"
done
