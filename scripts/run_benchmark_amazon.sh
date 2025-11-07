#!/usr/bin/env bash
set -euo pipefail

bash ./scripts/lakehouse_setup.sh --repair
source ~/.lakehouse/env

DELTA_LAYOUTS="baseline,linear,zorder"
HUDI_LAYOUTS_PHASE1="no_layout,linear"
HUDI_LAYOUTS_PHASE2="zorder,hilbert"
ICEBERG_LAYOUTS="baseline,linear,zorder"


# echo "===== Amazon write: Iceberg ====="
# bash ./scripts/run_amazon_write.sh --iceberg --overwrite --iceberg-layouts "$ICEBERG_LAYOUTS"
echo "===== Amazon query: Iceberg ====="
RUNNER_ARGS="--iceberg --iceberg-layouts $ICEBERG_LAYOUTS" \
  bash ./scripts/query_data_spec/run_amazon_query.sh --force

bash ./scripts/clean_data.sh --amazon --yes

echo "===== Amazon write: Delta ====="
bash ./scripts/run_amazon_write.sh --delta --overwrite --delta-layouts "$DELTA_LAYOUTS"
echo "===== Amazon query: Delta ====="
RUNNER_ARGS="--delta --delta-layouts $DELTA_LAYOUTS" \
  bash ./scripts/query_data_spec/run_amazon_query.sh --force

bash ./scripts/clean_data.sh --amazon --yes

echo "===== Amazon write: Hudi (phase 1) ====="
bash ./scripts/run_amazon_write.sh --hudi --overwrite --hudi-layouts "$HUDI_LAYOUTS_PHASE1"
echo "===== Amazon query: Hudi (phase 1) ====="
RUNNER_ARGS="--hudi --hudi-layouts $HUDI_LAYOUTS_PHASE1" \
  bash ./scripts/query_data_spec/run_amazon_query.sh --force

bash ./scripts/clean_data.sh --amazon --yes

echo "===== Amazon write: Hudi (phase 2) ====="
bash ./scripts/run_amazon_write.sh --hudi --overwrite --hudi-layouts "$HUDI_LAYOUTS_PHASE2"
echo "===== Amazon query: Hudi (phase 2) ====="
RUNNER_ARGS="--hudi --hudi-layouts $HUDI_LAYOUTS_PHASE2" \
  bash ./scripts/query_data_spec/run_amazon_query.sh --force

bash ./scripts/clean_data.sh --amazon --yes
