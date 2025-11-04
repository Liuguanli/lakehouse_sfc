bash ./scripts/run_hudi_layouts.sh \
  --input /datasets/tpch_64.parquet \
  --base-dir ./data/tpch_64/hudi \
  --partition-field "l_returnflag l_linestatus" \
  --sort-columns "l_shipdate l_receiptdate"