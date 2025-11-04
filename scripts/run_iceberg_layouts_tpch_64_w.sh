bash ./scripts/run_iceberg_layouts.sh \
  --input /datasets/tpch_64.parquet \
  --warehouse ./data/tpch_64/iceberg_wh \
  --namespace local.demo --base-name events_iceberg \
  --partition-by "l_returnflag,l_linestatus" \
  --range-cols "l_shipdate,l_receiptdate" \
  --layout-cols "l_shipdate,l_receiptdate"