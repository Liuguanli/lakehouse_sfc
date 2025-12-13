# cp workload_spec/tpch_rq1/spec_tpch_RQ1_*_C1_N2_O1.yaml workload_spec/tpch_rq3/
# # cp workload_spec/tpch_rq2/spec_tpch_RQ2_*_O1.yaml workload_spec/tpch_rq3/


# bash scripts/copy_rename_specs.sh \
#   --src workload_spec/tpch_rq1 \
#   --dst workload_spec/tpch_rq3 \
#   --old-prefix spec_tpch_RQ1_ \
#   --new-prefix spec_tpch_RQ3_ \
#   --old-rq RQ1 --new-rq RQ3 \
#   --glob "spec_tpch_RQ1_*_C1_N2_O1.yaml"