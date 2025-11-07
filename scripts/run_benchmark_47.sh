

# bash ./scripts/lakehouse_setup.sh --repair

# source ~/.lakehouse/env


bash ./scripts/lakehouse_setup.sh --repair

source ~/.lakehouse/env

# bash scripts/clean_data.sh --scales "64" --yes

# bash scripts/run_tpch_write.sh --hudi --scales "64" --hudi-layouts no_layout,zorder
# RUNNER_ARGS="--hudi --hudi-layouts no_layout,zorder" bash ./scripts/query_data_spec/run_tpch_query.sh 64
# bash scripts/clean_data.sh --scales "64" --yes

# bash scripts/run_tpch_write.sh --hudi --scales "64" --hudi-layouts hilbert,linear
# RUNNER_ARGS="--hudi --hudi-layouts hilbert,linear" bash ./scripts/query_data_spec/run_tpch_query.sh 64
# bash scripts/clean_data.sh --scales "64" --yes

bash scripts/run_tpch_write.sh --hudi --scales "16"
RUNNER_ARGS="--hudi" bash ./scripts/query_data_spec/run_tpch_query.sh 16

bash scripts/run_tpch_write.sh --iceberg --scales "16"
RUNNER_ARGS="--iceberg" bash scripts/query_data_spec/run_tpch_query.sh 16

bash scripts/run_tpch_write.sh --delta --scales "16"
RUNNER_ARGS="--delta" bash scripts/query_data_spec/run_tpch_query.sh 16

