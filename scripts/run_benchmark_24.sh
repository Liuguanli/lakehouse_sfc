

bash ./scripts/lakehouse_setup.sh --repair

source ~/.lakehouse/env

pip install -r requirements.txt

# bash scripts/clean_data.sh --scales "64" --yes
# bash scripts/run_tpch_write.sh --delta --scales "64"
RUNNER_ARGS="--delta" bash scripts/query_data_spec/run_tpch_query.sh 64