# TPCH scale 4
bash ./scripts/run_hudi_layouts.sh \
  --input /datasets/tpch_4.parquet \
  --base-dir ./data/tpch_4/hudi \
  --record-key l_orderkey,l_linenumber \
  --precombine-field l_receiptdate \
  --partition-field l_returnflag,l_linestatus \
  --sort-columns l_shipdate,l_receiptdate


# # TPCH scale 16
# bash run_hudi_layouts.sh \
#   --input /datasets/tpch_16.parquet \
#   --base-dir ./data/tpch_16/hudi \
#   --partition-field "l_returnflag l_linestatus" \
#   --sort-columns "l_shipdate l_receiptdate"

# # Batch both
# for n in 1 16; do
#   bash run_hudi_layouts.sh \
#     --input "/datasets/tpch_${n}.parquet" \
#     --base-dir "./data/tpch_${n}/hudi" \
#     --partition-field "l_returnflag,l_linestatus" \
#     --sort-columns "l_shipdate,l_receiptdate"
# done
