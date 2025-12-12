#!/usr/bin/env bash
set -euo pipefail

# RQ5 matrix (Delta): run RQ1-style specs on Delta layouts for TPCH and Amazon.
[[ -f "${HOME}/.lakehouse/env" ]] && source "${HOME}/.lakehouse/env"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_SCRIPT="${ROOT_DIR}/scripts/run_RQ_query.sh"
[[ -x "$RUN_SCRIPT" ]] || { echo "Missing run script: $RUN_SCRIPT" >&2; exit 1; }

clean_spark_eventlogs() {
  local ev_dir="${HOME}/Documents/lakehouse/spark_eventlogs"
  if [[ -d "$ev_dir" ]]; then
    echo "[CLEAN] Removing Spark event logs under $ev_dir"
    find "$ev_dir" -mindepth 1 -maxdepth 1 -exec rm -rf {} + 2>/dev/null || true
  else
    echo "[SKIP CLEAN] Spark eventlog dir not found: $ev_dir"
  fi
}

# ---------------------------------------------------------------------------
# Scenarios (Delta, Amazon) – mirror RQ1 Amazon scenarios; layouts limited to baseline/linear/zorder.

declare -A SCENARIO_AMAZON_DEFAULT=(
  [name]="amazon_default"
  [layouts]="baseline,linear,zorder"
  [record_key]="user_id,asin"
  [precombine]="record_timestamp"
  [partition]="category"
  [sort]="asin,parent_asin"
  [target_mb]="128"
  [skip_load]=1
  [query_args]="--workload-type custom --spec-dir workload_spec/amazon_rq1 --spec-glob spec_amazon_RQ1_*_C1.yaml"
  [output_root]="workloads/amazon_rq5_delta/amazon_default"
  [dataset_name]="amazon"
)

declare -A SCENARIO_AMAZON_DEFAULT_V1=(
  [name]="amazon_default_v1"
  [layouts]="baseline,linear,zorder"
  [record_key]="user_id,asin"
  [precombine]="record_timestamp"
  [partition]="category"
  [sort]="parent_asin,asin"
  [target_mb]="128"
  [query_args]="--workload-type custom --spec-dir workload_spec/amazon_rq1 --spec-glob spec_amazon_RQ1_*_C1.yaml"
  [output_root]="workloads/amazon_rq5_delta/amazon_default_v1"
  [dataset_name]="amazon"
)

declare -A SCENARIO_AMAZON_O2_V1=(
  [name]="amazon_sort_variant_o2_v1"
  [layouts]="baseline,linear,zorder"
  [record_key]="user_id,asin"
  [precombine]="record_timestamp"
  [partition]="category"
  [sort]="asin,user_id"
  [target_mb]="128"
  [query_args]="--workload-type custom --spec-dir workload_spec/amazon_rq1 --spec-glob spec_amazon_RQ1_*_C1.yaml"
  [output_root]="workloads/amazon_rq5_delta/amazon_sort_variant_o2_v1"
  [dataset_name]="amazon"
)

declare -A SCENARIO_AMAZON_O2_V2=(
  [name]="amazon_sort_variant_o2_v2"
  [layouts]="baseline,linear,zorder"
  [record_key]="user_id,asin"
  [precombine]="record_timestamp"
  [partition]="category"
  [sort]="user_id,asin"
  [target_mb]="128"
  [query_args]="--workload-type custom --spec-dir workload_spec/amazon_rq1 --spec-glob spec_amazon_RQ1_*_C1.yaml"
  [output_root]="workloads/amazon_rq5_delta/amazon_sort_variant_o2_v2"
  [dataset_name]="amazon"
)

declare -A SCENARIO_AMAZON_O3_V1=(
  [name]="amazon_sort_variant_o3_v1"
  [layouts]="baseline,linear,zorder"
  [record_key]="user_id,asin"
  [precombine]="record_timestamp"
  [partition]="category"
  [sort]="user_id,record_timestamp"
  [target_mb]="128"
  [query_args]="--workload-type custom --spec-dir workload_spec/amazon_rq1 --spec-glob spec_amazon_RQ1_*_C1.yaml"
  [output_root]="workloads/amazon_rq5_delta/amazon_sort_variant_o3_v1"
  [dataset_name]="amazon"
)

declare -A SCENARIO_AMAZON_O3_V2=(
  [name]="amazon_sort_variant_o3_v2"
  [layouts]="baseline,linear,zorder"
  [record_key]="user_id,asin"
  [precombine]="record_timestamp"
  [partition]="category"
  [sort]="record_timestamp,user_id"
  [target_mb]="128"
  [query_args]="--workload-type custom --spec-dir workload_spec/amazon_rq1 --spec-glob spec_amazon_RQ1_*_C1.yaml"
  [output_root]="workloads/amazon_rq5_delta/amazon_sort_variant_o3_v2"
  [dataset_name]="amazon"
)

SCENARIOS=(
  SCENARIO_AMAZON_DEFAULT
  SCENARIO_AMAZON_DEFAULT_V1
  SCENARIO_AMAZON_O2_V1
  SCENARIO_AMAZON_O2_V2
  SCENARIO_AMAZON_O3_V1
  SCENARIO_AMAZON_O3_V2
)

# ---------------------------------------------------------------------------

for scenario_var in "${SCENARIOS[@]}"; do
  declare -n scenario="$scenario_var"
  name="${scenario[name]:-scenario}"
  layouts="${scenario[layouts]:-baseline,linear,zorder}"
  dataset_name="${scenario[dataset_name]:-amazon}"
  output_root="${scenario[output_root]:-${ROOT_DIR}/workloads/rq5_delta/${name}}"
  query_args_str="${scenario[query_args]:---workload-type custom --spec-dir workload_spec/amazon_rq1 --spec-glob spec_amazon_RQ1_*.yaml}"

  echo "===== Running scenario: $name (Delta) ====="

  cmd=( "$RUN_SCRIPT"
        --workload-type "amazon"
        --dataset-name "$dataset_name"
        --output-root "$output_root"
        --delta
        --delta-layouts "$layouts"
        --tag "scenario=${name}" )

  read -r -a query_args <<<"$query_args_str"
  if [[ ${#query_args[@]} -gt 0 ]]; then
    cmd+=("${query_args[@]}")
  fi

  DELTA_LAYOUTS="$layouts" bash "${cmd[@]}"

  clean_spark_eventlogs
done

echo "[DONE] Delta RQ5 scenarios completed."
