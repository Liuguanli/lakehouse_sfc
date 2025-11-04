bash ./scripts/run_delta_layouts.sh \
  --input /datasets/tpch_64.parquet \
  --out-base ./data/tpch_64/delta \
  --partition-by "l_returnflag,l_linestatus" \
  --range-cols  "l_shipdate l_receiptdate" \
  --layout-cols "l_shipdate,l_receiptdate"