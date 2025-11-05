#!/usr/bin/env bash

# bash ./scripts/lakehouse_setup.sh --repair

# bash ./scripts/run_tpch_write.sh

# bash scripts/run_tpch_write.sh # Overall
# bash scripts/run_tpch_write.sh --scales "1 4 16" # 172.26.147.188
# bash scripts/run_tpch_write.sh --engines delta,iceberg --scales "64" # 172.26.147.188
# bash scripts/run_tpch_write.sh --hudi --scales "64"  # 172.26.146.47
RUNNER_ARGS="--hudi" bash ./scripts/run_tpch_query.sh 64
# bash scripts/clean_data.sh --scales "64" --yes

# bash scripts/run_tpch_write.sh --engines delta,hudi --scales "1 64"
# bash scripts/run_tpch_write.sh --delta --iceberg --scales "1 4 16 64"



# bash run_tpch_query.sh 4


# RUNNER_ARGS="--delta --hudi" bash run_tpch_query.sh 4
# RUNNER_ARGS="--delta --hudi" bash run_tpch_query.sh 4


# RUNNER_ARGS="--iceberg" bash run_tpch_query.sh --force 16
